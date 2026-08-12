"""Crash-safe delivery of canonical project events to Discord."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest

from gateway.platforms.base import SendResult
from hermes_cli import project_runtime_db as runtime_db
from hermes_cli import projects_db
from hermes_cli.project_events import ProjectEvent
from plugins.platforms.discord.project_channels import (
    DiscordProjectErrorCode,
    DiscordProjectPortError,
)


def _delivery_db(path: Path):
    conn = projects_db.connect(path)
    project_id = projects_db.create_project(
        conn,
        project_id="project-delivery",
        name="Delivery project",
    )
    runtime_db.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="delivery-session",
        current_phase="implementation",
        now=1,
    )
    runtime_db.bind_surface(
        conn,
        binding_id="discord-binding",
        project_id=project_id,
        surface="discord",
        external_binding_id="discord-channel",
        actor_id="owner",
        principal_id="discord-owner",
        now=1,
    )
    return conn, project_id


class _DiscordDeliveryFake:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []
        self.messages: dict[tuple[str, str], tuple[str, ...]] = {}
        self.published_events: list[ProjectEvent] = []

    async def find_event_message(
        self,
        *,
        channel_id: str,
        event_id: str,
        before_segment=None,
    ) -> str | None:
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("find", event_id, None))
        ids = self.messages.get((channel_id, event_id))
        return ids[0] if ids else None

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment=None,
    ) -> SendResult:
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("publish", event.event_id, nonce))
        self.published_events.append(event)
        ids = self.messages.setdefault(
            (channel_id, event.event_id),
            ("discord-message-1",),
        )
        return SendResult(
            success=True,
            message_id=ids[0],
            raw_response={"message_ids": list(ids)},
        )


class _FailingDiscordDeliveryFake(_DiscordDeliveryFake):
    def __init__(self, result: SendResult) -> None:
        super().__init__()
        self.result = result

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment=None,
    ) -> SendResult:
        del channel_id
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("publish", event.event_id, nonce))
        return self.result


class _CrashAfterSend(BaseException):
    pass


class _CrashAfterSendFake(_DiscordDeliveryFake):
    def __init__(self) -> None:
        super().__init__()
        self.crash_once = True
        self.logical_send_count = 0

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment=None,
    ) -> SendResult:
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("publish", event.event_id, nonce))
        key = (channel_id, event.event_id)
        ids = self.messages.get(key)
        if ids is None:
            self.logical_send_count += 1
            ids = ("discord-message-after-crash",)
            self.messages[key] = ids
        if self.crash_once:
            self.crash_once = False
            raise _CrashAfterSend
        return SendResult(
            success=True,
            message_id=ids[0],
            raw_response={"message_ids": list(ids)},
        )


class _ResponseLostAfterSendFake(_DiscordDeliveryFake):
    def __init__(self) -> None:
        super().__init__()
        self.lose_response_once = True
        self.logical_send_count = 0

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment=None,
    ) -> SendResult:
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("publish", event.event_id, nonce))
        key = (channel_id, event.event_id)
        ids = self.messages.get(key)
        if ids is None:
            self.logical_send_count += 1
            ids = ("discord-message-after-loss",)
            self.messages[key] = ids
        if self.lose_response_once:
            self.lose_response_once = False
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.TRANSIENT,
                retryable=True,
            )
        return SendResult(
            success=True,
            message_id=ids[0],
            raw_response={"message_ids": list(ids)},
        )


class _PartialChunkFake(_DiscordDeliveryFake):
    def __init__(self) -> None:
        super().__init__()
        self.partial_once = True
        self.logical_groups = 0

    async def find_event_message(
        self,
        *,
        channel_id: str,
        event_id: str,
        before_segment=None,
    ) -> str | None:
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("find", event_id, None))
        ids = self.messages.get((channel_id, event_id))
        if ids is not None and len(ids) < 2:
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.PARTIAL_DELIVERY,
                retryable=True,
            )
        return ids[0] if ids else None

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment=None,
    ) -> SendResult:
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("publish", event.event_id, nonce))
        key = (channel_id, event.event_id)
        ids = self.messages.get(key)
        if ids is None:
            self.logical_groups += 1
            ids = ("discord-part-1",)
            self.messages[key] = ids
        if self.partial_once:
            self.partial_once = False
            return SendResult(
                success=False,
                message_id=ids[0],
                error=DiscordProjectErrorCode.PARTIAL_DELIVERY.value,
                retryable=True,
                error_kind="transient",
                raw_response={"message_ids": list(ids)},
            )
        complete = ("discord-part-1", "discord-part-2")
        self.messages[key] = complete
        return SendResult(
            success=True,
            message_id=complete[0],
            continuation_message_ids=(complete[1],),
            raw_response={"message_ids": list(complete)},
        )


class _BlockingDiscordDeliveryFake(_DiscordDeliveryFake):
    def __init__(self) -> None:
        super().__init__()
        self.publish_started = asyncio.Event()
        self.release = asyncio.Event()

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment=None,
    ) -> SendResult:
        del channel_id
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("publish", event.event_id, nonce))
        self.publish_started.set()
        await self.release.wait()
        raise AssertionError("cancelled delivery must not resume")


class _SegmentFenceLeaseLossFake(_DiscordDeliveryFake):
    def __init__(self, *, after_first_chunk) -> None:
        super().__init__()
        self.after_first_chunk = after_first_chunk
        self.first_chunk_hook_pending = True
        self.sent_parts: list[int] = []

    async def find_event_message(
        self,
        *,
        channel_id: str,
        event_id: str,
        before_segment=None,
    ) -> str | None:
        assert callable(before_segment)
        await before_segment()
        self.calls.append(("find", event_id, None))
        ids = self.messages.get((channel_id, event_id))
        if ids is not None and len(ids) == 1:
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.PARTIAL_DELIVERY,
                retryable=True,
            )
        return ids[0] if ids else None

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment=None,
    ) -> SendResult:
        self.calls.append(("publish", event.event_id, nonce))
        assert callable(before_segment)
        key = (channel_id, event.event_id)
        await before_segment()
        ids = self.messages.get(key, ())
        if not ids:
            await before_segment()
            self.sent_parts.append(1)
            ids = ("discord-fenced-part-1",)
            self.messages[key] = ids
            if self.first_chunk_hook_pending:
                self.first_chunk_hook_pending = False
                self.after_first_chunk()
        if len(ids) == 1:
            await before_segment()
            self.sent_parts.append(2)
            ids = (*ids, "discord-fenced-part-2")
            self.messages[key] = ids
        await before_segment()
        return SendResult(
            success=True,
            message_id=ids[0],
            continuation_message_ids=tuple(ids[1:]),
            raw_response={"message_ids": list(ids)},
        )


class _ReadFenceLeaseLossFake(_DiscordDeliveryFake):
    def __init__(self, *, after_channel_lookup) -> None:
        super().__init__()
        self.after_channel_lookup = after_channel_lookup
        self.remote_events: list[str] = []

    async def find_event_message(
        self,
        *,
        channel_id: str,
        event_id: str,
        before_segment=None,
    ) -> str | None:
        del channel_id, event_id
        assert callable(before_segment)
        await before_segment()
        self.remote_events.append("channel")
        self.after_channel_lookup()
        await before_segment()
        self.remote_events.append("history")
        return None

    async def publish_event(self, **_kwargs) -> SendResult:
        self.remote_events.append("send")
        raise AssertionError("lease-lost marker read must not publish")


@pytest.mark.asyncio
async def test_one_canonical_event_is_delivered_once_and_acknowledged(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "projects.db")
    event = ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"delivery-{kind}",
    ).append_event(
        project_id,
        "project.changed",
        {"field": "phase"},
    )
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
        lease_seconds=30,
        heartbeat_interval_seconds=10,
    )

    result = await worker.deliver_next()

    assert result is not None
    assert (
        result.delivery_id,
        result.event_id,
        result.binding_id,
        result.status,
    ) == (
        conn.execute(
            """
            SELECT delivery_id FROM project_deliveries
            WHERE binding_id = 'discord-binding'
            """
        ).fetchone()[0],
        event.event_id,
        "discord-binding",
        "delivered",
    )
    assert [call[0] for call in port.calls] == ["find", "publish"]
    assert port.calls[1][2]
    row = conn.execute(
        """
        SELECT status, cursor, lease_expires_at,
               remote_message_ids_json
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == (
        "delivered",
        event.sequence,
        None,
        '["discord-message-1"]',
    )
    conn.close()


@pytest.mark.asyncio
async def test_delivered_replay_performs_no_remote_or_durable_write(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "projects.db")
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"replay-{kind}",
    ).append_event(project_id, "project.changed", {"field": "phase"})
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
    )
    assert await worker.deliver_next() is not None
    port.calls.clear()
    durable_changes = conn.total_changes

    assert await worker.deliver_next() is None
    assert port.calls == []
    assert conn.total_changes == durable_changes
    conn.close()


@pytest.mark.parametrize(
    ("send_result", "expected_status", "next_attempt_at", "error_code"),
    (
        (
            SendResult(
                success=False,
                error="raw transient detail",
                retryable=True,
                error_kind="transient",
            ),
            "deferred",
            101,
            "transient",
        ),
        (
            SendResult(
                success=False,
                error="raw permission detail",
                retryable=False,
                error_kind="forbidden",
            ),
            "blocked",
            None,
            "forbidden",
        ),
    ),
)
@pytest.mark.asyncio
async def test_worker_defers_transient_and_blocks_permanent_failure(
    tmp_path,
    send_result,
    expected_status,
    next_attempt_at,
    error_code,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(
        tmp_path / f"{expected_status}.db"
    )
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"{expected_status}-{kind}",
    ).append_event(project_id, "project.changed", {"field": "phase"})
    port = _FailingDiscordDeliveryFake(send_result)
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
    )

    result = await worker.deliver_next()

    assert result is not None
    assert result.status == expected_status
    row = conn.execute(
        """
        SELECT status, cursor, lease_expires_at,
               next_attempt_at, last_error_code
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == (
        "pending" if expected_status == "deferred" else "blocked",
        None,
        None,
        next_attempt_at,
        error_code,
    )
    assert "raw" not in row["last_error_code"]
    conn.close()


@pytest.mark.asyncio
async def test_restart_after_send_before_ack_recovers_marker_without_duplicate(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    database_path = tmp_path / "projects.db"
    conn, project_id = _delivery_db(database_path)
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"crash-{kind}",
    ).append_event(project_id, "project.changed", {"field": "phase"})
    port = _CrashAfterSendFake()
    first = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="worker-before-crash",
        clock=lambda: 100,
        lease_seconds=30,
    )

    with pytest.raises(_CrashAfterSend):
        await first.deliver_next()
    before_restart = conn.execute(
        """
        SELECT status, attempt_count, lease_expires_at
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(before_restart) == ("in_flight", 1, 130)
    conn.close()

    restarted = projects_db.connect(database_path)
    second = ProjectEventDeliveryWorker(
        restarted,
        port=port,
        worker_id="worker-after-crash",
        clock=lambda: 130,
        lease_seconds=30,
    )
    result = await second.deliver_next()

    assert result is not None
    assert result.status == "delivered"
    assert port.logical_send_count == 1
    assert [call[0] for call in port.calls] == [
        "find",
        "publish",
        "find",
        "publish",
    ]
    nonces = [
        call[2] for call in port.calls if call[0] == "publish"
    ]
    assert len(set(nonces)) == 1
    row = restarted.execute(
        """
        SELECT status, attempt_count, remote_message_ids_json
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == (
        "delivered",
        2,
        '["discord-message-after-crash"]',
    )
    restarted.close()


@pytest.mark.asyncio
async def test_response_loss_after_send_is_read_back_in_same_attempt(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "response-lost.db")
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"response-lost-{kind}",
    ).append_event(project_id, "project.changed", {"field": "phase"})
    port = _ResponseLostAfterSendFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
    )

    result = await worker.deliver_next()

    assert result is not None
    assert result.status == "delivered"
    assert port.logical_send_count == 1
    assert [call[0] for call in port.calls] == [
        "find",
        "publish",
        "find",
        "publish",
    ]
    nonces = [
        call[2] for call in port.calls if call[0] == "publish"
    ]
    assert len(set(nonces)) == 1
    row = conn.execute(
        """
        SELECT status, attempt_count, remote_message_ids_json
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == (
        "delivered",
        1,
        '["discord-message-after-loss"]',
    )
    conn.close()


@pytest.mark.asyncio
async def test_cancellation_is_preserved_without_terminal_ledger_write(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "cancelled.db")
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"cancelled-{kind}",
    ).append_event(project_id, "project.changed", {"field": "phase"})
    port = _BlockingDiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
        lease_seconds=30,
        heartbeat_interval_seconds=10,
    )
    delivery = asyncio.create_task(worker.deliver_next())
    await port.publish_started.wait()

    delivery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivery

    row = conn.execute(
        """
        SELECT status, attempt_count, lease_expires_at,
               next_attempt_at, last_error_code
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == ("in_flight", 1, 130, None, None)
    conn.close()


@pytest.mark.asyncio
async def test_segment_fence_loss_stops_chunks_and_successor_completes(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    database_path = tmp_path / "segment-fence-loss.db"
    now = [100]
    conn, project_id = _delivery_db(database_path)
    ProjectEventOutbox(
        conn,
        clock=lambda: now[0],
        id_factory=lambda kind: f"segment-fence-{kind}",
    ).append_event(
        project_id,
        "project.changed",
        {"body": "long logical event"},
    )
    port = _SegmentFenceLeaseLossFake(
        after_first_chunk=lambda: now.__setitem__(0, 130)
    )
    first = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="worker-before-fence-loss",
        clock=lambda: now[0],
        lease_seconds=30,
        heartbeat_interval_seconds=10,
    )

    with pytest.raises(RuntimeError, match="delivery lease lost"):
        await first.deliver_next()

    row = conn.execute(
        """
        SELECT status, attempt_count, lease_expires_at,
               cursor, remote_message_ids_json,
               next_attempt_at, last_error_code
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == (
        "in_flight",
        1,
        130,
        None,
        None,
        None,
        None,
    )
    assert port.sent_parts == [1]
    conn.close()

    restarted = projects_db.connect(database_path)
    successor = ProjectEventDeliveryWorker(
        restarted,
        port=port,
        worker_id="worker-after-fence-loss",
        clock=lambda: now[0],
        lease_seconds=30,
        heartbeat_interval_seconds=10,
    )
    result = await successor.deliver_next()

    assert result is not None
    assert result.status == "delivered"
    assert port.sent_parts == [1, 2]
    terminal = restarted.execute(
        """
        SELECT status, attempt_count, remote_message_ids_json
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(terminal) == (
        "delivered",
        2,
        '["discord-fenced-part-1","discord-fenced-part-2"]',
    )
    restarted.close()


@pytest.mark.asyncio
async def test_read_fence_loss_stops_history_and_preserves_stale_claim(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    now = [100]
    conn, project_id = _delivery_db(tmp_path / "read-fence-loss.db")
    ProjectEventOutbox(
        conn,
        clock=lambda: now[0],
        id_factory=lambda kind: f"read-fence-{kind}",
    ).append_event(
        project_id,
        "project.changed",
        {"body": "read fence"},
    )
    port = _ReadFenceLeaseLossFake(
        after_channel_lookup=lambda: now.__setitem__(0, 130)
    )
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="worker-before-read-fence-loss",
        clock=lambda: now[0],
        lease_seconds=30,
        heartbeat_interval_seconds=10,
    )

    with pytest.raises(RuntimeError, match="delivery lease lost"):
        await worker.deliver_next()

    row = conn.execute(
        """
        SELECT status, attempt_count, lease_expires_at,
               cursor, remote_message_ids_json,
               next_attempt_at, last_error_code
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == (
        "in_flight",
        1,
        130,
        None,
        None,
        None,
        None,
    )
    assert port.remote_events == ["channel"]
    conn.close()


@pytest.mark.asyncio
async def test_partial_chunk_group_retries_with_stable_nonce_and_acks_group(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "partial.db")
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"partial-{kind}",
    ).append_event(project_id, "project.changed", {"field": "phase"})
    port = _PartialChunkFake()
    first = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="worker-first-part",
        clock=lambda: 100,
    )

    first_result = await first.deliver_next()

    assert first_result is not None
    assert first_result.status == "deferred"
    second = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="worker-finish-parts",
        clock=lambda: 101,
    )
    second_result = await second.deliver_next()

    assert second_result is not None
    assert second_result.status == "delivered"
    assert port.logical_groups == 1
    nonces = [
        call[2] for call in port.calls if call[0] == "publish"
    ]
    assert len(nonces) == 2
    assert len(set(nonces)) == 1
    row = conn.execute(
        """
        SELECT status, attempt_count, remote_message_ids_json
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == (
        "delivered",
        2,
        '["discord-part-1","discord-part-2"]',
    )
    conn.close()


@pytest.mark.asyncio
async def test_one_event_is_delivered_independently_per_discord_binding(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "two-bindings.db")
    runtime_db.bind_surface(
        conn,
        binding_id="discord-binding-2",
        project_id=project_id,
        surface="discord",
        external_binding_id="discord-channel-2",
        actor_id="owner",
        principal_id="discord-owner",
        now=2,
    )
    event = ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"two-bindings-{kind}",
    ).append_event(project_id, "project.changed", {"field": "phase"})
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
    )

    results = (
        await worker.deliver_next(),
        await worker.deliver_next(),
    )

    assert {
        (result.event_id, result.binding_id, result.status)
        for result in results
        if result is not None
    } == {
        (event.event_id, "discord-binding", "delivered"),
        (event.event_id, "discord-binding-2", "delivered"),
    }
    assert set(port.messages) == {
        ("discord-channel", event.event_id),
        ("discord-channel-2", event.event_id),
    }
    rows = conn.execute(
        """
        SELECT binding_id, status, cursor
        FROM project_deliveries
        ORDER BY binding_id
        """
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("discord-binding", "delivered", event.sequence),
        ("discord-binding-2", "delivered", event.sequence),
    ]
    conn.close()


@pytest.mark.asyncio
async def test_origin_turn_queued_is_suppressed_without_remote_echo(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_policy import ActorContext
    from hermes_cli.project_runtime import ProjectRuntime

    conn, project_id = _delivery_db(tmp_path / "origin.db")
    state = runtime_db.runtime_state_for_project(conn, project_id)
    assert state is not None
    ProjectRuntime(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"origin-{kind}",
    ).enqueue_turn(
        project_id,
        {"message": "already visible"},
        ActorContext(
            "owner",
            "discord",
            "discord-binding",
            True,
        ),
        idempotency_key="origin-message",
        expected_version=state.version,
    )
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
    )

    result = await worker.deliver_next()

    assert result is not None
    assert result.status == "suppressed"
    assert port.calls == []
    row = conn.execute(
        """
        SELECT status, cursor, remote_message_ids_json
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == ("suppressed", 1, "[]")
    conn.close()


@pytest.mark.asyncio
async def test_verified_artifact_delivery_contains_only_safe_metadata(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "artifact.db")
    project_root = tmp_path / "project-root"
    project_root.mkdir()
    projects_db.add_folder(
        conn,
        project_id,
        str(project_root),
        is_primary=True,
    )
    artifact_path = project_root / "report.txt"
    contents = b"verified report"
    artifact_path.write_bytes(contents)
    outbox = ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"artifact-{kind}",
    )
    outbox.register_verified_artifact(
        project_id,
        artifact_id="artifact-report",
        path=artifact_path,
        metadata={
            "kind": "report",
            "raw_payload": "never deliver",
        },
    )
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
    )

    result = await worker.deliver_next()

    assert result is not None
    assert result.status == "delivered"
    assert len(port.published_events) == 1
    delivered = port.published_events[0]
    assert delivered.kind == "artifact.verified"
    assert dict(delivered.payload) == {
        "artifact_id": "artifact-report",
        "basename": "report.txt",
        "sha256": hashlib.sha256(contents).hexdigest(),
        "size": len(contents),
        "status": "verified",
    }
    serialized = repr(dict(delivered.payload))
    assert str(artifact_path.resolve()) not in serialized
    assert "never deliver" not in serialized
    conn.close()


@pytest.mark.asyncio
async def test_unverified_artifact_event_is_blocked_before_remote_io(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "unverified.db")
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"unverified-{kind}",
    ).append_event(
        project_id,
        "artifact.verified",
        {
            "artifact_id": "not-in-registry",
            "path": "C:/private/raw.bin",
            "bytes": "never deliver",
        },
    )
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
    )

    result = await worker.deliver_next()

    assert result is not None
    assert result.status == "blocked"
    assert port.calls == []
    row = conn.execute(
        """
        SELECT status, last_error_code
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == ("blocked", "artifact_unverified")
    conn.close()


@pytest.mark.asyncio
async def test_approval_event_publishes_exact_restart_safe_typed_actions(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "approval.db")
    approval_id = "approval-durable-1"
    runtime_db.create_approval_request(
        conn,
        runtime_db.ApprovalRequest(
            approval_id=approval_id,
            project_id=project_id,
            requester_actor_id="owner",
            authorization_actor_id="owner",
            canonical_action="publish",
            approval_class="publish",
            command_revision=1,
            expected_runtime_version=0,
            expected_lifecycle="active",
            expected_phase="implementation",
            targets=("C:/work/runtime/release",),
            batch_id="approval-batch",
            batch_items=("publish",),
            status="pending",
            expires_at=4_000_000_000,
        ),
        now=10,
    )
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"approval-{kind}",
    ).append_event(
        project_id,
        "approval.requested",
        {"approval_id": approval_id},
    )
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="delivery-worker",
        clock=lambda: 100,
    )

    result = await worker.deliver_next()

    assert result is not None
    assert result.status == "delivered"
    assert len(port.published_events) == 1
    delivered = port.published_events[0]
    assert dict(delivered.payload) == {
        "approval_id": approval_id,
        "status": "pending",
        "commands": {
            "approve": (
                "/project approval approval-durable-1 approve"
            ),
            "deny": "/project approval approval-durable-1 deny",
        },
    }
    assert "C:/work/runtime/release" not in repr(dict(delivered.payload))
    conn.close()


def _bind_additional_discord_surface(
    conn,
    project_id: str,
    *,
    suffix: str,
    now: int,
) -> None:
    runtime_db.bind_surface(
        conn,
        binding_id=f"discord-binding-{suffix}",
        project_id=project_id,
        surface="discord",
        external_binding_id=f"discord-channel-{suffix}",
        actor_id="owner",
        principal_id=f"discord-owner-{suffix}",
        now=now,
    )


@pytest.mark.asyncio
async def test_fair_batch_delivers_once_per_binding_before_hot_binding_repeats(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "fair-batch.db")
    outbox = ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"fair-{kind}",
    )
    for sequence in range(3):
        outbox.append_event(
            project_id,
            "project.changed",
            {"hot_sequence": sequence},
            event_id=f"hot-event-{sequence}",
        )
    _bind_additional_discord_surface(
        conn,
        project_id,
        suffix="b",
        now=2,
    )
    _bind_additional_discord_surface(
        conn,
        project_id,
        suffix="c",
        now=3,
    )
    shared = outbox.append_event(
        project_id,
        "project.changed",
        {"shared": True},
        event_id="shared-event",
    )
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="fair-worker",
        clock=lambda: 100,
    )

    results = await worker.deliver_pending(limit=25)

    assert [
        (result.binding_id, result.event_id)
        for result in results
    ] == [
        ("discord-binding", "hot-event-0"),
        ("discord-binding-b", shared.event_id),
        ("discord-binding-c", shared.event_id),
    ]
    rows = conn.execute(
        """
        SELECT binding_id, status, COUNT(*) AS amount
        FROM project_deliveries
        GROUP BY binding_id, status
        ORDER BY binding_id, status
        """
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("discord-binding", "delivered", 1),
        ("discord-binding", "pending", 3),
        ("discord-binding-b", "delivered", 1),
        ("discord-binding-c", "delivered", 1),
    ]
    conn.close()


@pytest.mark.parametrize(
    ("barrier", "expected_status"),
    (
        ("blocked", "blocked"),
        ("live", "in_flight"),
        ("not_due", "pending"),
    ),
)
@pytest.mark.asyncio
async def test_one_binding_barrier_does_not_block_other_fair_batch_heads(
    tmp_path,
    barrier,
    expected_status,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(
        tmp_path / f"barrier-{barrier}.db"
    )
    _bind_additional_discord_surface(
        conn,
        project_id,
        suffix="b",
        now=2,
    )
    _bind_additional_discord_surface(
        conn,
        project_id,
        suffix="c",
        now=3,
    )
    outbox = ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"barrier-{barrier}-{kind}",
    )
    outbox.append_event(
        project_id,
        "project.changed",
        {"barrier": barrier},
    )
    head = outbox.claim_delivery(
        project_id,
        "discord-binding",
        lease_seconds=30,
    )
    assert head is not None
    if barrier == "blocked":
        outbox.block_delivery(head, error_code="forbidden")
    elif barrier == "not_due":
        outbox.defer_delivery(
            head,
            error_code="transient",
            delay_seconds=10,
        )
    port = _DiscordDeliveryFake()
    worker = ProjectEventDeliveryWorker(
        conn,
        port=port,
        worker_id="fair-worker",
        clock=lambda: 100,
    )

    results = await worker.deliver_pending(limit=2)

    assert [result.binding_id for result in results] == [
        "discord-binding-b",
        "discord-binding-c",
    ]
    assert conn.execute(
        """
        SELECT status FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()[0] == expected_status
    conn.close()


@pytest.mark.asyncio
async def test_fair_batch_limit_is_positive_and_deterministically_bounded(
    tmp_path,
):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    conn, project_id = _delivery_db(tmp_path / "fair-limit.db")
    _bind_additional_discord_surface(
        conn,
        project_id,
        suffix="b",
        now=2,
    )
    _bind_additional_discord_surface(
        conn,
        project_id,
        suffix="c",
        now=3,
    )
    ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"fair-limit-{kind}",
    ).append_event(
        project_id,
        "project.changed",
        {"field": "phase"},
    )
    worker = ProjectEventDeliveryWorker(
        conn,
        port=_DiscordDeliveryFake(),
        worker_id="fair-worker",
        clock=lambda: 100,
    )
    changes = conn.total_changes

    for invalid in (0, -1, True, "2"):
        with pytest.raises(
            ValueError,
            match="positive integer",
        ):
            await worker.deliver_pending(limit=invalid)
    assert conn.total_changes == changes

    results = await worker.deliver_pending(limit=2)

    assert [result.binding_id for result in results] == [
        "discord-binding",
        "discord-binding-b",
    ]
    assert conn.execute(
        """
        SELECT status FROM project_deliveries
        WHERE binding_id = 'discord-binding-c'
        """
    ).fetchone()[0] == "pending"
    conn.close()


@pytest.mark.asyncio
async def test_two_connections_claim_one_logical_external_send(tmp_path):
    from gateway.project_event_delivery import ProjectEventDeliveryWorker
    from hermes_cli.project_events import ProjectEventOutbox

    database_path = tmp_path / "projects.db"
    first_conn, project_id = _delivery_db(database_path)
    ProjectEventOutbox(
        first_conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"concurrent-{kind}",
    ).append_event(project_id, "project.changed", {"field": "phase"})
    second_conn = projects_db.connect(database_path)
    port = _DiscordDeliveryFake()
    workers = (
        ProjectEventDeliveryWorker(
            first_conn,
            port=port,
            worker_id="worker-a",
            clock=lambda: 100,
        ),
        ProjectEventDeliveryWorker(
            second_conn,
            port=port,
            worker_id="worker-b",
            clock=lambda: 100,
        ),
    )

    results = await asyncio.gather(
        *(worker.deliver_next() for worker in workers)
    )

    assert sorted(
        result.status if result is not None else "idle"
        for result in results
    ) == ["delivered", "idle"]
    assert [call[0] for call in port.calls].count("publish") == 1
    assert len(port.messages) == 1
    row = first_conn.execute(
        """
        SELECT status, attempt_count, remote_message_ids_json
        FROM project_deliveries
        WHERE binding_id = 'discord-binding'
        """
    ).fetchone()
    assert tuple(row) == (
        "delivered",
        1,
        '["discord-message-1"]',
    )
    first_conn.close()
    second_conn.close()


def test_gateway_starts_one_delivery_supervisor_before_discord_connects():
    from gateway.config import GatewayConfig
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

    assert runner._start_discord_project_event_delivery_supervisor() is True
    assert runner._start_discord_project_event_delivery_supervisor() is False
    assert [name for _, name in scheduled] == [
        "discord_project_event_delivery_supervisor"
    ]


@pytest.mark.asyncio
async def test_delivery_supervisor_waits_reconnects_and_uses_fresh_state(
    monkeypatch,
):
    import gateway.project_event_delivery as delivery_module
    import gateway.run as run_module
    from gateway.config import Platform
    from gateway.run import GatewayRunner
    from hermes_cli import projects_db as projects_db_module

    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {}
    first_port = _DiscordDeliveryFake()
    second_port = _DiscordDeliveryFake()
    connections: list[object] = []
    seen: list[tuple[object, object, str]] = []
    batch_limits: list[int] = []

    class _Connection:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    def _connect():
        connection = _Connection()
        connections.append(connection)
        return connection

    class _Worker:
        def __init__(self, conn, *, port, worker_id):
            seen.append((conn, port, worker_id))

        async def deliver_pending(self, *, limit):
            batch_limits.append(limit)
            return ()

    sleeps: list[float] = []

    async def _poll(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 1:
            runner.adapters[Platform.DISCORD] = first_port
        elif len(sleeps) == 2:
            runner.adapters[Platform.DISCORD] = second_port
        else:
            runner._running = False

    monkeypatch.setattr(
        delivery_module,
        "ProjectEventDeliveryWorker",
        _Worker,
    )
    monkeypatch.setattr(projects_db_module, "connect", _connect)
    monkeypatch.setattr(run_module.asyncio, "sleep", _poll)

    await runner._run_discord_project_event_delivery_supervisor(
        poll_seconds=0.1
    )

    assert sleeps == [0.1, 0.1, 0.1]
    assert [port for _, port, _ in seen] == [
        first_port,
        second_port,
    ]
    assert len({id(conn) for conn, _, _ in seen}) == 2
    assert all(connection.closed for connection in connections)
    assert len({worker_id for _, _, worker_id in seen}) == 1
    assert batch_limits == [25, 25]
