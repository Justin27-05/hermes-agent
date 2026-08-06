from __future__ import annotations

import asyncio
from dataclasses import replace
import gc
import hashlib
from pathlib import Path

import pytest

from gateway.project_surface_lifecycle import (
    SurfaceLifecycleProjector,
    discord_surface_binding_id,
)
from gateway.project_surfaces import (
    DiscordProjectSurface,
    ProjectLifecycleSnapshot,
    discord_project_channel_name,
    project_channel_spec_for_lifecycle_event,
)
from hermes_cli import project_runtime_db as runtime_db
from hermes_cli import project_surface_operations as surface_ops
from hermes_cli import projects_db
from hermes_cli.project_events import ProjectEvent
from plugins.platforms.discord.project_channels import (
    DiscordProjectErrorCode,
    DiscordProjectPortError,
    ProjectChannelSpec,
    ProjectChannelState,
    project_channel_marker,
)


class _GatewaySurfacePortFake:
    """Hermetic Discord capability used only to prove gateway wiring."""

    async def ensure_channel(self, *args, **kwargs):
        raise AssertionError("the supervisor must not perform an effect at startup")

    async def read_channel(self, *args, **kwargs):
        raise AssertionError("the supervisor must not perform a read at startup")


def _surface() -> DiscordProjectSurface:
    return DiscordProjectSurface(
        guild_id="444",
        owner_user_id="111",
        active_category_id="10",
        completed_category_id="20",
    )


def test_gateway_starts_surface_supervisor_before_discord_reconnects():
    """A configured workspace must survive a temporarily missing adapter."""
    from gateway.config import GatewayConfig, Platform
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig.from_dict(
        {
            "platforms": {
                "discord": {
                    "enabled": True,
                    "allow_from": ["111"],
                    "project_workspaces": {
                        "enabled": True,
                        "guild_id": "444",
                        "owner_user_id": "111",
                        "active_category_id": "10",
                        "completed_category_id": "20",
                    },
                }
            }
        }
    )
    runner.adapters = {}
    scheduled: list[tuple[object, str]] = []
    runner._spawn_supervised = lambda factory, name: scheduled.append(
        (factory, name)
    )

    assert runner._start_discord_project_surface_supervisor() is True
    assert [name for _, name in scheduled] == [
        "discord_project_surface_supervisor"
    ]


@pytest.mark.asyncio
async def test_surface_supervisor_picks_up_a_reconnected_discord_adapter(
    monkeypatch,
):
    """Reconnect must make the already-started worker usable without respawn."""
    import gateway.project_surface_lifecycle as lifecycle_module
    import gateway.run as run_module
    from gateway.config import Platform
    from gateway.run import GatewayRunner
    from hermes_cli import projects_db as projects_db_module

    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {}
    port = _GatewaySurfacePortFake()
    seen_ports: list[object] = []

    class _Connection:
        closed = False

        def close(self):
            self.closed = True

    connection = _Connection()

    class _Projector:
        def __init__(self, _conn, *, surface, port, worker_id):
            del surface, worker_id
            seen_ports.append(port)

        async def project_pending(self, *, limit):
            assert limit == 25
            runner._running = False
            return 1

    async def _poll_once(_seconds):
        runner.adapters[Platform.DISCORD] = port

    monkeypatch.setattr(
        lifecycle_module, "SurfaceLifecycleProjector", _Projector
    )
    monkeypatch.setattr(projects_db_module, "connect", lambda: connection)
    monkeypatch.setattr(run_module.asyncio, "sleep", _poll_once)

    await runner._run_discord_project_surface_supervisor(
        _surface(), poll_seconds=0.1
    )

    assert seen_ports == [port]
    assert connection.closed is True


@pytest.mark.asyncio
async def test_legacy_lifecycle_event_is_blocked_once_and_does_not_poison_replay(
    tmp_path,
):
    conn = _runtime_db(tmp_path / "projects.db")
    _append_lifecycle_event(
        conn,
        event_id="event-legacy-rename",
        kind="project.renamed",
        created_at=2,
        payload_json='{"command_fingerprint":"legacy"}',
    )
    projector = _projector(conn, _LifecyclePortFake())

    blocked = await projector.project_event("event-legacy-rename")

    assert blocked is not None
    assert blocked.status == "blocked"
    assert blocked.blocked_reason == "legacy_missing_surface_state"
    changes = conn.total_changes
    assert await projector.project_event("event-legacy-rename") == blocked
    assert conn.total_changes == changes
    assert conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE kind = 'surface.sync_blocked'"
    ).fetchone()[0] == 1
    conn.close()


def _event(kind: str) -> ProjectEvent:
    return ProjectEvent(
        event_id=f"event-{kind}",
        project_id="project-1",
        sequence=1,
        kind=kind,
        turn_id=None,
        payload={"command_fingerprint": "fingerprint"},
        created_at="2026-07-29T00:00:00Z",
    )


def _snapshot(
    *,
    name: str = "project-alpha",
    lifecycle: str = "active",
    channel_id: str | None = None,
) -> ProjectLifecycleSnapshot:
    return ProjectLifecycleSnapshot(
        project_id="project-1",
        name=name,
        lifecycle=lifecycle,
        channel_id=channel_id,
    )


@pytest.mark.parametrize(
    "kind,lifecycle,category_id,owner_can_send",
    [
        ("project.created", "completed", "10", True),
        ("project.renamed", "active", "10", True),
        ("project.renamed", "awaiting_acceptance", "10", True),
        ("project.renamed", "completed", "20", False),
        ("project.technically_completed", "completed", "10", True),
        ("project.completion_accepted", "active", "20", False),
        ("project.reopened", "completed", "10", True),
    ],
)
def test_lifecycle_event_maps_to_exact_discord_channel_spec(
    kind,
    lifecycle,
    category_id,
    owner_can_send,
):
    spec = project_channel_spec_for_lifecycle_event(
        _event(kind),
        _snapshot(lifecycle=lifecycle, channel_id="9001"),
        _surface(),
    )

    assert spec is not None
    assert spec.project_id == "project-1"
    assert spec.guild_id == "444"
    assert spec.owner_user_id == "111"
    assert spec.channel_id == "9001"
    assert spec.name == "project-alpha"
    assert spec.category_id == category_id
    assert spec.owner_can_send is owner_can_send


def test_surface_events_and_unrelated_events_are_not_projected_recursively():
    snapshot = _snapshot()

    assert (
        project_channel_spec_for_lifecycle_event(
            _event("surface.sync_pending"),
            snapshot,
            _surface(),
        )
        is None
    )
    assert (
        project_channel_spec_for_lifecycle_event(
            _event("turn.queued"),
            snapshot,
            _surface(),
        )
        is None
    )


def test_lifecycle_mapping_rejects_event_snapshot_identity_drift():
    with pytest.raises(ValueError):
        project_channel_spec_for_lifecycle_event(
            _event("project.created"),
            replace(_snapshot(), project_id="another-project"),
            _surface(),
        )


@pytest.mark.parametrize(
    "name,expected",
    [
        ("project-alpha", "project-alpha"),
        ("Project Alpha", "project-alpha"),
        ("Café Déjà Vu", "cafe-deja-vu"),
        (
            "🔥✨",
            "project-"
            + hashlib.sha256(b"project-1").hexdigest()[:12],
        ),
    ],
)
def test_discord_project_channel_name_is_safe_and_deterministic(
    name,
    expected,
):
    assert discord_project_channel_name("project-1", name) == expected


def test_long_rename_stays_within_discord_channel_name_limit():
    name = discord_project_channel_name(
        "project-1",
        "A very long renamed project " * 10,
    )

    assert len(name) == 100
    assert not name.endswith("-")


class _Crash(BaseException):
    pass


class _LifecyclePortFake:
    def __init__(self) -> None:
        self.channels: dict[str, ProjectChannelState] = {}
        self.ensure_calls: list[tuple[ProjectChannelSpec, str]] = []
        self.read_calls: list[str] = []
        self.create_count = 0
        self.mode: str | None = None

    @staticmethod
    def _state(
        spec: ProjectChannelSpec,
        channel_id: str,
    ) -> ProjectChannelState:
        return ProjectChannelState(
            guild_id=spec.guild_id,
            channel_id=channel_id,
            name=spec.name,
            category_id=spec.category_id,
            ownership_marker=project_channel_marker(spec.project_id),
            only_owner_and_bot_can_view=True,
            owner_can_view=True,
            owner_can_send=spec.owner_can_send,
            owner_can_read_history=True,
            bot_can_view=True,
            bot_can_send=True,
            bot_can_read_history=True,
        )

    async def ensure_channel(
        self,
        spec: ProjectChannelSpec,
        *,
        operation_id: str,
    ) -> ProjectChannelState:
        self.ensure_calls.append((spec, operation_id))
        await asyncio.sleep(0)
        if self.mode == "collision":
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.CONFLICT,
                operation_id=operation_id,
            )
        current = self.channels.get(spec.project_id)
        if current is None:
            self.create_count += 1
            current = self._state(
                spec, spec.channel_id or f"channel-{self.create_count}"
            )
            self.channels[spec.project_id] = current
        if self.mode == "crash_after_effect":
            self.mode = None
            raise _Crash
        if self.mode == "transient_once":
            self.mode = None
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.TRANSIENT,
                retryable=True,
                operation_id=operation_id,
            )
        if self.mode == "partial_once":
            self.mode = None
            self.channels[spec.project_id] = replace(
                current, name=spec.name
            )
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.STATE_MISMATCH,
                retryable=True,
                operation_id=operation_id,
            )
        current = self._state(spec, current.channel_id)
        self.channels[spec.project_id] = current
        return current

    async def read_channel(
        self,
        *,
        guild_id: str,
        channel_id: str,
    ) -> ProjectChannelState | None:
        del guild_id
        self.read_calls.append(channel_id)
        return next(
            (
                state
                for state in self.channels.values()
                if state.channel_id == channel_id
            ),
            None,
        )

    async def find_event_message(
        self,
        *,
        channel_id: str,
        event_id: str,
        before_segment=None,
    ) -> str | None:
        if before_segment is not None:
            await before_segment()
        del channel_id, event_id
        return None

    async def publish_event(self, **_kwargs):
        raise AssertionError("lifecycle projection does not publish chat events")


def _runtime_db(path: Path):
    conn = projects_db.connect(path)
    project_id = projects_db.create_project(
        conn,
        project_id="project-1",
        name="Project Alpha",
    )
    runtime_db.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="session-project-1",
        current_phase="planning",
        now=1,
    )
    runtime_db.bind_surface(
        conn,
        binding_id="desktop-binding",
        project_id=project_id,
        surface="desktop",
        external_binding_id="desktop-window",
        actor_id="owner-1",
        now=1,
    )
    with runtime_db.write_transaction(conn):
        runtime_db._append_runtime_event(
            conn,
            event_id="event-created",
            project_id=project_id,
            kind="project.created",
            turn_id=None,
            payload_json=(
                '{"command_fingerprint":"created",'
                '"surface":{"lifecycle":"active",'
                '"name":"Project Alpha"}}'
            ),
            created_at=1,
        )
    return conn


def _projector(
    conn,
    port,
    *,
    worker_id: str = "projector-1",
    now: list[int] | None = None,
) -> SurfaceLifecycleProjector:
    clock = now or [10]
    return SurfaceLifecycleProjector(
        conn,
        surface=_surface(),
        port=port,
        worker_id=worker_id,
        lease_seconds=5,
        clock=lambda: clock[0],
    )


def _append_lifecycle_event(
    conn,
    *,
    event_id: str,
    kind: str,
    created_at: int,
    project_id: str = "project-1",
    payload_json: str = (
        '{"command_fingerprint":"transition",'
        '"surface":{"lifecycle":"completed",'
        '"name":"Project Alpha"}}'
    ),
) -> None:
    with runtime_db.write_transaction(conn):
        runtime_db._append_runtime_event(
            conn,
            event_id=event_id,
            project_id=project_id,
            kind=kind,
            turn_id=None,
            payload_json=payload_json,
            created_at=created_at,
        )


def _add_project(
    conn,
    *,
    project_id: str,
    name: str,
    now: int,
) -> None:
    projects_db.create_project(
        conn,
        project_id=project_id,
        name=name,
    )
    runtime_db.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id=f"session-{project_id}",
        current_phase="planning",
        now=now,
    )


@pytest.mark.asyncio
async def test_created_event_converges_binding_principal_and_terminal_event(
    tmp_path,
):
    conn = _runtime_db(tmp_path / "projects.db")
    port = _LifecyclePortFake()
    projector = _projector(conn, port)

    operation = await projector.project_event("event-created")

    assert operation is not None
    assert operation.status == "synchronized"
    assert port.create_count == 1
    bindings = runtime_db.bindings_for_project(
        conn, project_id="project-1"
    )
    discord_bindings = [
        binding for binding in bindings if binding.surface == "discord"
    ]
    assert len(discord_bindings) == 1
    binding = discord_bindings[0]
    assert binding.binding_id == discord_surface_binding_id(
        "project-1", "444", "channel-1"
    )
    assert binding.external_binding_id == "channel-1"
    assert (
        runtime_db.principal_for_surface_binding(
            conn,
            project_id="project-1",
            binding_id=binding.binding_id,
        )
        == "111"
    )
    terminal = conn.execute(
        """
        SELECT kind, payload_json FROM project_events
        WHERE project_id = ? AND kind = 'surface.synchronized'
        """,
        ("project-1",),
    ).fetchall()
    assert len(terminal) == 1

    changes = conn.total_changes
    replay = await projector.project_event("event-created")
    assert replay == operation
    assert conn.total_changes == changes
    assert port.create_count == 1
    conn.close()


@pytest.mark.asyncio
async def test_sync_pending_retries_to_synchronized_without_second_channel(
    tmp_path,
):
    conn = _runtime_db(tmp_path / "projects.db")
    port = _LifecyclePortFake()
    port.mode = "transient_once"
    now = [10]
    projector = _projector(conn, port, now=now)

    pending = await projector.project_event("event-created")

    assert pending is not None
    assert pending.status == "sync_pending"
    assert port.create_count == 1
    assert [
        row["kind"]
        for row in conn.execute(
            """
            SELECT kind FROM project_events
            WHERE kind LIKE 'surface.%'
            ORDER BY sequence
            """
        ).fetchall()
    ] == ["surface.sync_pending"]

    now[0] = 11
    synchronized = await projector.project_next()

    assert synchronized is not None
    assert synchronized.status == "synchronized"
    assert port.create_count == 1
    assert [
        row["kind"]
        for row in conn.execute(
            """
            SELECT kind FROM project_events
            WHERE kind LIKE 'surface.%'
            ORDER BY sequence
            """
        ).fetchall()
    ] == ["surface.sync_pending", "surface.synchronized"]
    conn.close()


@pytest.mark.asyncio
async def test_restart_after_remote_success_reads_marker_before_create(
    tmp_path,
):
    db_path = tmp_path / "projects.db"
    first_conn = _runtime_db(db_path)
    port = _LifecyclePortFake()
    port.mode = "crash_after_effect"
    first = _projector(first_conn, port, now=[10])

    with pytest.raises(_Crash):
        await first.project_event("event-created")

    assert port.create_count == 1
    first_conn.close()
    restarted_conn = projects_db.connect(db_path)
    restarted = _projector(
        restarted_conn,
        port,
        worker_id="projector-restarted",
        now=[15],
    )

    recovered = await restarted.project_next()

    assert recovered is not None
    assert recovered.status == "synchronized"
    assert port.create_count == 1
    assert len(port.ensure_calls) == 2
    restarted_conn.close()


@pytest.mark.asyncio
async def test_foreign_marker_collision_blocks_without_binding(tmp_path):
    conn = _runtime_db(tmp_path / "projects.db")
    port = _LifecyclePortFake()
    port.mode = "collision"

    operation = await _projector(conn, port).project_event("event-created")

    assert operation is not None
    assert operation.status == "blocked"
    assert runtime_db.binding_for_surface_identity(
        conn,
        surface="discord",
        external_binding_id="channel-1",
    ) is None
    assert (
        conn.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE kind = 'surface.sync_blocked'
            """
        ).fetchone()[0]
        == 1
    )
    conn.close()


@pytest.mark.asyncio
async def test_partial_completed_move_stays_pending_until_exact_readback(
    tmp_path,
):
    conn = _runtime_db(tmp_path / "projects.db")
    port = _LifecyclePortFake()
    projector = _projector(conn, port, now=[10])
    created = await projector.project_event("event-created")
    assert created is not None and created.status == "synchronized"
    _append_lifecycle_event(
        conn,
        event_id="event-completed",
        kind="project.completion_accepted",
        created_at=2,
    )
    port.mode = "partial_once"

    pending = await projector.project_event("event-completed")

    assert pending is not None
    assert pending.status == "sync_pending"
    assert pending.external_channel_id == "channel-1"
    assert port.channels["project-1"].category_id == "10"

    exact = await projector.project_event("event-completed")

    assert exact is not None
    assert exact.status == "synchronized"
    state = port.channels["project-1"]
    assert state.channel_id == "channel-1"
    assert state.category_id == "20"
    assert state.owner_can_send is False
    assert port.create_count == 1
    conn.close()


@pytest.mark.asyncio
async def test_queued_rename_uses_its_immutable_name_not_a_later_snapshot(
    tmp_path,
):
    """A delayed rename must not be projected with a later rename's name."""
    conn = _runtime_db(tmp_path / "projects.db")
    port = _LifecyclePortFake()
    projector = _projector(conn, port)

    await projector.project_event("event-created")
    with runtime_db.write_transaction(conn):
        assert projects_db.update_project(
            conn,
            "project-1",
            name="First durable rename",
            caller_owns_transaction=True,
        )
        runtime_db._append_runtime_event(
            conn,
            event_id="event-rename-first",
            project_id="project-1",
            kind="project.renamed",
            turn_id=None,
            payload_json=(
                '{"command_fingerprint":"rename-first",'
                '"surface":{"lifecycle":"active",'
                '"name":"First durable rename"}}'
            ),
            created_at=2,
        )
        assert projects_db.update_project(
            conn,
            "project-1",
            name="Second later rename",
            caller_owns_transaction=True,
        )
        runtime_db._append_runtime_event(
            conn,
            event_id="event-rename-second",
            project_id="project-1",
            kind="project.renamed",
            turn_id=None,
            payload_json=(
                '{"command_fingerprint":"rename-second",'
                '"surface":{"lifecycle":"active",'
                '"name":"Second later rename"}}'
            ),
            created_at=3,
        )

    first = await projector.project_event("event-rename-first")

    assert first is not None and first.status == "synchronized"
    assert port.channels["project-1"].name == "first-durable-rename"
    conn.close()


@pytest.mark.asyncio
async def test_two_projectors_share_one_durable_effect_claim(tmp_path):
    db_path = tmp_path / "projects.db"
    first_conn = _runtime_db(db_path)
    second_conn = projects_db.connect(db_path)
    port = _LifecyclePortFake()
    first = _projector(first_conn, port, worker_id="projector-1")
    second = _projector(second_conn, port, worker_id="projector-2")

    await asyncio.gather(
        first.project_event("event-created"),
        second.project_event("event-created"),
    )

    operation = first_conn.execute(
        """
        SELECT status FROM project_surface_operations
        WHERE lifecycle_event_id = 'event-created'
        """
    ).fetchone()
    assert operation["status"] == "synchronized"
    assert port.create_count == 1
    assert len(port.ensure_calls) == 1
    first_conn.close()
    second_conn.close()


@pytest.mark.asyncio
async def test_heartbeat_prevents_takeover_while_slow_effect_is_in_flight(
    tmp_path,
):
    """A second connection cannot create while the first renews its lease."""
    db_path = tmp_path / "projects.db"
    first_conn = _runtime_db(db_path)
    second_conn = projects_db.connect(db_path)
    started = asyncio.Event()
    release = asyncio.Event()
    clock = [10]

    class _SlowPort(_LifecyclePortFake):
        def __init__(self):
            super().__init__()
            self.effect_starts = 0

        async def ensure_channel(self, spec, *, operation_id):
            self.effect_starts += 1
            started.set()
            await release.wait()
            return await super().ensure_channel(spec, operation_id=operation_id)

    port = _SlowPort()
    first = _projector(first_conn, port, worker_id="first", now=clock)
    first._lease_heartbeat_seconds = 0.01
    second = _projector(second_conn, port, worker_id="second", now=clock)

    first_task = asyncio.create_task(first.project_event("event-created"))
    await asyncio.wait_for(started.wait(), timeout=1)
    clock[0] = 14
    await asyncio.sleep(0.05)
    clock[0] = 15

    assert await second.project_event("event-created") is not None
    assert port.effect_starts == 1

    release.set()
    completed = await first_task
    assert completed is not None and completed.status == "synchronized"
    assert port.create_count == 1
    first_conn.close()
    second_conn.close()


@pytest.mark.asyncio
async def test_recovery_renews_before_mutation_can_cross_old_lease_boundary(
    tmp_path,
):
    db_path = tmp_path / "projects.db"
    first_conn = _runtime_db(db_path)
    second_conn = projects_db.connect(db_path)
    clock = [1]
    pre_read_started = asyncio.Event()
    release_pre_read = asyncio.Event()
    mutation_started = asyncio.Event()
    release_mutation = asyncio.Event()

    class _BoundaryPort(_LifecyclePortFake):
        def __init__(self):
            super().__init__()
            self.recovery_state: ProjectChannelState | None = None
            self.read_count = 0
            self.effect_starts = 0

        async def read_channel(self, *, guild_id, channel_id):
            del guild_id, channel_id
            self.read_count += 1
            if self.read_count == 1:
                pre_read_started.set()
                await release_pre_read.wait()
            return self.recovery_state

        async def ensure_channel(self, spec, *, operation_id):
            del operation_id
            self.effect_starts += 1
            if self.effect_starts == 1:
                mutation_started.set()
                await release_mutation.wait()
            state = self._state(spec, "channel-existing")
            self.channels[spec.project_id] = state
            return state

    port = _BoundaryPort()
    seed = _projector(first_conn, port, worker_id="seed", now=clock)
    event = seed._event("event-created")
    assert event is not None
    prepared = seed._prepare(event)
    assert prepared is not None
    operation, spec = prepared
    seed_claim = surface_ops.claim_effect(
        first_conn,
        operation.operation_id,
        holder_id="seed",
        now=1,
        lease_seconds=5,
    )
    assert seed_claim is not None
    surface_ops.mark_effect_started(
        first_conn,
        operation.operation_id,
        claim=seed_claim,
        now=1,
    )
    pending = surface_ops.reconcile(
        first_conn,
        operation.operation_id,
        claim=seed_claim,
        now=1,
        readback_json='{"channel_id":"channel-existing","exact":false}',
        external_channel_id="channel-existing",
        outcome="partial",
    )
    assert pending.status == "sync_pending"
    port.recovery_state = replace(
        port._state(
            replace(spec, channel_id="channel-existing"),
            "channel-existing",
        ),
        name="outdated-name",
    )
    clock[0] = 10
    first = _projector(first_conn, port, worker_id="first", now=clock)
    first._lease_heartbeat_seconds = 0.5
    second = _projector(second_conn, port, worker_id="second", now=clock)
    second._lease_heartbeat_seconds = 0.5

    first_task = asyncio.create_task(first.project_event("event-created"))
    await asyncio.wait_for(pre_read_started.wait(), timeout=1)
    clock[0] = 14
    release_pre_read.set()
    await asyncio.wait_for(mutation_started.wait(), timeout=1)
    clock[0] = 15

    second_result = await second.project_event("event-created")
    release_mutation.set()
    first_result = (await asyncio.gather(
        first_task,
        return_exceptions=True,
    ))[0]

    assert second_result is not None
    assert second_result.status == "sync_pending"
    assert not isinstance(first_result, BaseException)
    assert first_result.status == "synchronized"
    assert port.effect_starts == 1
    first_conn.close()
    second_conn.close()


@pytest.mark.asyncio
async def test_stale_initial_renewal_never_starts_remote_or_blocks_operation(
    tmp_path,
    recwarn,
):
    conn = _runtime_db(tmp_path / "projects.db")
    clock = [10]
    projector = _projector(conn, _LifecyclePortFake(), now=clock)
    event = projector._event("event-created")
    assert event is not None
    prepared = projector._prepare(event)
    assert prepared is not None
    operation, _spec = prepared
    stale = surface_ops.claim_effect(
        conn,
        operation.operation_id,
        holder_id="stale",
        now=10,
        lease_seconds=5,
    )
    assert stale is not None
    surface_ops.mark_effect_started(
        conn,
        operation.operation_id,
        claim=stale,
        now=10,
    )
    replacement = surface_ops.claim_effect(
        conn,
        operation.operation_id,
        holder_id="replacement",
        now=15,
        lease_seconds=5,
    )
    assert replacement is not None
    surface_ops.mark_effect_started(
        conn,
        operation.operation_id,
        claim=replacement,
        now=15,
    )
    clock[0] = 15
    projector._lease_heartbeat_seconds = 0.01
    remote_starts = 0

    async def _remote_effect():
        nonlocal remote_starts
        remote_starts += 1
        await asyncio.Event().wait()

    remote = _remote_effect()
    with pytest.raises(surface_ops.SurfaceOperationConflict):
        await projector._with_lease_heartbeat(
            operation.operation_id,
            [stale],
            remote,
        )
    del remote
    gc.collect()

    assert remote_starts == 0
    assert not [
        warning
        for warning in recwarn
        if "was never awaited" in str(warning.message)
    ]
    current = surface_ops.operation_for_lifecycle_event(
        conn,
        project_id="project-1",
        lifecycle_event_id="event-created",
    )
    assert current is not None and current.status == "effect_started"
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE kind = 'surface.sync_blocked'
        """
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.asyncio
async def test_remote_error_after_renewal_reconciles_with_the_renewed_claim(
    tmp_path,
):
    conn = _runtime_db(tmp_path / "projects.db")
    started = asyncio.Event()
    release = asyncio.Event()
    clock = [10]

    class _SlowFailingPort(_LifecyclePortFake):
        async def ensure_channel(self, spec, *, operation_id):
            self.ensure_calls.append((spec, operation_id))
            started.set()
            await release.wait()
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.TRANSIENT,
                retryable=True,
                operation_id=operation_id,
            )

    projector = _projector(conn, _SlowFailingPort(), now=clock)
    projector._lease_heartbeat_seconds = 0.01
    task = asyncio.create_task(projector.project_event("event-created"))
    await asyncio.wait_for(started.wait(), timeout=1)
    clock[0] = 14
    await asyncio.sleep(0.05)

    release.set()
    pending = await task

    assert pending is not None
    assert pending.status == "sync_pending"
    conn.close()


@pytest.mark.asyncio
async def test_renewal_failure_is_retryable_and_joins_the_cancelled_remote(
    tmp_path,
    monkeypatch,
):
    conn = _runtime_db(tmp_path / "projects.db")
    started = asyncio.Event()
    remote_finished = asyncio.Event()

    class _NeverFinishingPort(_LifecyclePortFake):
        async def ensure_channel(self, spec, *, operation_id):
            del spec, operation_id
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                remote_finished.set()

    real_renewal = surface_ops.renew_effect_claim
    periodic_failure_injected = False

    def _fail_renewal(*args, **kwargs):
        nonlocal periodic_failure_injected
        if started.is_set() and not periodic_failure_injected:
            periodic_failure_injected = True
            raise surface_ops.SurfaceOperationConflict(
                "injected renewal failure"
            )
        return real_renewal(*args, **kwargs)

    monkeypatch.setattr(surface_ops, "renew_effect_claim", _fail_renewal)
    projector = _projector(conn, _NeverFinishingPort())
    projector._lease_heartbeat_seconds = 0.01

    pending = await asyncio.wait_for(
        projector.project_event("event-created"),
        timeout=1,
    )

    assert started.is_set()
    assert remote_finished.is_set()
    assert pending is not None
    assert pending.status == "sync_pending"
    conn.close()


@pytest.mark.asyncio
async def test_projector_cancellation_cancels_and_joins_remote_effect(tmp_path):
    conn = _runtime_db(tmp_path / "projects.db")
    started = asyncio.Event()
    remote_finished = asyncio.Event()

    class _NeverFinishingPort(_LifecyclePortFake):
        async def ensure_channel(self, spec, *, operation_id):
            del spec, operation_id
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                remote_finished.set()

    projector = _projector(conn, _NeverFinishingPort())
    projector._lease_heartbeat_seconds = 0.01
    task = asyncio.create_task(projector.project_event("event-created"))
    await asyncio.wait_for(started.wait(), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert remote_finished.is_set()
    conn.close()


@pytest.mark.asyncio
async def test_slow_failure_readback_renews_lease_and_prevents_takeover(
    tmp_path,
):
    db_path = tmp_path / "projects.db"
    first_conn = _runtime_db(db_path)
    second_conn = projects_db.connect(db_path)
    clock = [1]
    readback_started = asyncio.Event()
    release_readback = asyncio.Event()

    class _SlowFailureReadbackPort(_LifecyclePortFake):
        def __init__(self):
            super().__init__()
            self.fail_next_ensure = False
            self.block_next_read = False
            self.rename_effects = 0

        async def ensure_channel(self, spec, *, operation_id):
            if spec.name == "renamed-during-failure":
                self.rename_effects += 1
            if self.fail_next_ensure:
                self.fail_next_ensure = False
                raise DiscordProjectPortError(
                    DiscordProjectErrorCode.TRANSIENT,
                    retryable=True,
                    operation_id=operation_id,
                )
            return await super().ensure_channel(spec, operation_id=operation_id)

        async def read_channel(self, *, guild_id, channel_id):
            if self.block_next_read:
                self.block_next_read = False
                readback_started.set()
                await release_readback.wait()
            return await super().read_channel(
                guild_id=guild_id,
                channel_id=channel_id,
            )

    port = _SlowFailureReadbackPort()
    first = _projector(first_conn, port, worker_id="first", now=clock)
    first._lease_heartbeat_seconds = 0.01
    second = _projector(second_conn, port, worker_id="second", now=clock)
    created = await first.project_event("event-created")
    assert created is not None and created.status == "synchronized"
    _append_lifecycle_event(
        first_conn,
        event_id="event-rename-failure",
        kind="project.renamed",
        created_at=2,
        payload_json=(
            '{"command_fingerprint":"rename",'
            '"surface":{"lifecycle":"active",'
            '"name":"Renamed During Failure"}}'
        ),
    )
    clock[0] = 10
    port.fail_next_ensure = True
    port.block_next_read = True

    first_task = asyncio.create_task(
        first.project_event("event-rename-failure")
    )
    await asyncio.wait_for(readback_started.wait(), timeout=1)
    clock[0] = 14
    await asyncio.sleep(0.05)
    clock[0] = 15

    leased = await second.project_event("event-rename-failure")

    assert leased is not None and leased.status == "effect_started"
    assert port.rename_effects == 1
    release_readback.set()
    pending = await first_task
    assert pending is not None and pending.status == "sync_pending"
    first_conn.close()
    second_conn.close()


@pytest.mark.asyncio
async def test_pending_batch_chooses_only_oldest_event_per_project(tmp_path):
    conn = _runtime_db(tmp_path / "projects.db")
    projector = _projector(conn, _LifecyclePortFake())
    event = projector._event("event-created")
    assert event is not None
    prepared = projector._prepare(event)
    assert prepared is not None
    operation, _spec = prepared
    active_claim = surface_ops.claim_effect(
        conn,
        operation.operation_id,
        holder_id="active-worker",
        now=10,
        lease_seconds=100,
    )
    assert active_claim is not None
    surface_ops.mark_effect_started(
        conn,
        operation.operation_id,
        claim=active_claim,
        now=10,
    )
    for index in range(25):
        _append_lifecycle_event(
            conn,
            event_id=f"event-project-1-{index}",
            kind="project.renamed",
            created_at=11 + index,
        )
    _add_project(
        conn,
        project_id="project-2",
        name="Project Beta",
        now=40,
    )
    _append_lifecycle_event(
        conn,
        event_id="event-project-2-created",
        project_id="project-2",
        kind="project.created",
        created_at=40,
        payload_json=(
            '{"command_fingerprint":"created-beta",'
            '"surface":{"lifecycle":"active","name":"Project Beta"}}'
        ),
    )

    projected = await projector.project_pending(limit=25)

    project_2 = surface_ops.operation_for_lifecycle_event(
        conn,
        project_id="project-2",
        lifecycle_event_id="event-project-2-created",
    )
    assert projected == 2
    assert project_2 is not None and project_2.status == "synchronized"
    assert [
        spec.project_id for spec, _operation_id in projector._port.ensure_calls
    ] == ["project-2"]
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_surface_operations
        WHERE project_id = 'project-1'
        """
    ).fetchone()[0] == 1
    conn.close()


@pytest.mark.asyncio
async def test_legacy_operation_resumes_after_lease_expiry_and_blocks_once(
    tmp_path,
):
    db_path = tmp_path / "projects.db"
    first_conn = _runtime_db(db_path)
    _append_lifecycle_event(
        first_conn,
        event_id="event-legacy-recovery",
        kind="project.renamed",
        created_at=2,
        payload_json='{"command_fingerprint":"legacy"}',
    )
    operation = surface_ops.prepare_or_replay(
        first_conn,
        operation_id="surface-operation-legacy-recovery",
        project_id="project-1",
        lifecycle_event_id="event-legacy-recovery",
        kind="discord.unprojectable_legacy_event",
        desired_json="{}",
        prestate_json="{}",
        ownership_marker=project_channel_marker("project-1"),
    )
    claim = surface_ops.claim_effect(
        first_conn,
        operation.operation_id,
        holder_id="crashed-worker",
        now=10,
        lease_seconds=5,
    )
    assert claim is not None
    started = surface_ops.mark_effect_started(
        first_conn,
        operation.operation_id,
        claim=claim,
        now=10,
    )
    first_conn.close()
    restarted_conn = projects_db.connect(db_path)
    now = [14]
    restarted = _projector(
        restarted_conn,
        _LifecyclePortFake(),
        worker_id="restarted",
        now=now,
    )

    changes = restarted_conn.total_changes
    assert await restarted.project_event("event-legacy-recovery") == started
    assert restarted_conn.total_changes == changes
    now[0] = 15
    blocked = await restarted.project_event("event-legacy-recovery")

    assert blocked is not None and blocked.status == "blocked"
    assert blocked.blocked_reason == "legacy_missing_surface_state"
    changes = restarted_conn.total_changes
    assert await restarted.project_event("event-legacy-recovery") == blocked
    assert restarted_conn.total_changes == changes
    assert restarted_conn.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE kind = 'surface.sync_blocked'
          AND project_id = 'project-1'
        """
    ).fetchone()[0] == 1
    restarted_conn.close()


class _SharedChannelPort(_LifecyclePortFake):
    async def ensure_channel(self, spec, *, operation_id):
        self.ensure_calls.append((spec, operation_id))
        state = self._state(spec, "channel-shared")
        self.channels[spec.project_id] = state
        return state


@pytest.mark.asyncio
async def test_same_external_channel_for_two_projects_blocks_second(tmp_path):
    conn = _runtime_db(tmp_path / "projects.db")
    _add_project(
        conn,
        project_id="project-2",
        name="Project Beta",
        now=2,
    )
    _append_lifecycle_event(
        conn,
        event_id="event-project-2-created",
        project_id="project-2",
        kind="project.created",
        created_at=2,
        payload_json=(
            '{"command_fingerprint":"created-beta",'
            '"surface":{"lifecycle":"active","name":"Project Beta"}}'
        ),
    )
    projector = _projector(conn, _SharedChannelPort())
    first = await projector.project_event("event-created")
    assert first is not None and first.status == "synchronized"

    second = await projector.project_event("event-project-2-created")

    assert second is not None and second.status == "blocked"
    assert second.blocked_reason == "local_channel_claim_collision"
    conn.close()


@pytest.mark.asyncio
async def test_binding_identity_conflict_blocks_with_specific_reason(tmp_path):
    conn = _runtime_db(tmp_path / "projects.db")
    _add_project(
        conn,
        project_id="project-2",
        name="Project Beta",
        now=2,
    )
    runtime_db.bind_surface(
        conn,
        binding_id="preexisting-discord-binding",
        project_id="project-2",
        surface="discord",
        external_binding_id="channel-shared",
        actor_id="owner-2",
        principal_id="222",
        now=2,
    )

    blocked = await _projector(
        conn,
        _SharedChannelPort(),
    ).project_event("event-created")

    assert blocked is not None and blocked.status == "blocked"
    assert blocked.blocked_reason == "local_binding_identity_collision"
    assert conn.execute(
        "SELECT COUNT(*) FROM project_surface_channel_claims"
    ).fetchone()[0] == 0
    conn.close()


def test_stale_exact_claim_is_not_masked_as_local_collision(tmp_path):
    conn = _runtime_db(tmp_path / "projects.db")
    clock = [10]
    port = _LifecyclePortFake()
    projector = _projector(conn, port, worker_id="stale", now=clock)
    event = projector._event("event-created")
    assert event is not None
    prepared = projector._prepare(event)
    assert prepared is not None
    operation, spec = prepared
    stale = surface_ops.claim_effect(
        conn,
        operation.operation_id,
        holder_id="stale",
        now=10,
        lease_seconds=5,
    )
    assert stale is not None
    surface_ops.mark_effect_started(
        conn,
        operation.operation_id,
        claim=stale,
        now=10,
    )
    replacement = surface_ops.claim_effect(
        conn,
        operation.operation_id,
        holder_id="replacement",
        now=15,
        lease_seconds=5,
    )
    assert replacement is not None
    surface_ops.mark_effect_started(
        conn,
        operation.operation_id,
        claim=replacement,
        now=15,
    )
    clock[0] = 15

    with pytest.raises(surface_ops.SurfaceOperationConflict):
        projector._record_exact(
            event,
            operation,
            stale,
            port._state(spec, "channel-1"),
        )

    current = surface_ops.operation_for_lifecycle_event(
        conn,
        project_id="project-1",
        lifecycle_event_id="event-created",
    )
    assert current is not None and current.status == "effect_started"
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE kind = 'surface.sync_blocked'
        """
    ).fetchone()[0] == 0
    conn.close()
