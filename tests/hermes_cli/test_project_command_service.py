"""Contract tests for the thin shared ProjectCommandService boundary."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import ActorContext


def _owner() -> ActorContext:
    return ActorContext("owner-1", "desktop", "desktop-binding", True)


def test_dispatch_table_contains_every_task9_canonical_command():
    from hermes_cli.project_command_service import ProjectCommandService

    assert set(ProjectCommandService.command_names()) == {
        "project.create",
        "project.rename",
        "project.status",
        "turn.enqueue",
        "queue.status",
        "run.stop",
        "run.resume",
        "approval.resolve",
        "artifact.get",
        "project.mark_technically_complete",
        "project.accept_completion",
        "project.reopen",
    }


@pytest.mark.parametrize(
    ("name", "project_id", "payload", "actor"),
    (
        ("project.create", None, {"name": "Created"}, _owner()),
        ("project.rename", "project-1", {"name": "Renamed"}, _owner()),
        ("project.status", "project-1", {}, _owner()),
        ("turn.enqueue", "project-1", {"message": "work"}, _owner()),
        ("queue.status", "project-1", {}, _owner()),
        (
            "run.stop",
            "project-1",
            {"turn_id": "turn-1", "expected_control_version": 1},
            _owner(),
        ),
        (
            "run.resume",
            "project-1",
            {"turn_id": "turn-1", "expected_control_version": 2},
            _owner(),
        ),
        (
            "approval.resolve",
            "project-1",
            {"approval_id": "approval-1", "outcome": "approved"},
            _owner(),
        ),
        (
            "artifact.get",
            "project-1",
            {"artifact_id": "artifact-1"},
            _owner(),
        ),
        (
            "project.mark_technically_complete",
            "project-1",
            {},
            ActorContext("hermes", "system", "core", False),
        ),
        ("project.accept_completion", "project-1", {}, _owner()),
        ("project.reopen", "project-1", {}, _owner()),
    ),
)
def test_every_canonical_command_returns_one_structured_result(
    name, project_id, payload, actor
):
    from hermes_cli.project_command_service import (
        ProjectCommandError,
        ProjectCommandService,
        ProjectSnapshot,
    )

    class Runtime:
        def create_managed_project(self, actor, **kwargs):
            return SimpleNamespace(project_id="project-1")

        def rename_project(self, *args, **kwargs):
            return None

        def list_queue(self, *args, **kwargs):
            return ()

        def enqueue_turn(self, *args, **kwargs):
            return SimpleNamespace(turn_id="turn-1")

        def request_stop(self, *args, **kwargs):
            return None

        def request_resume(self, *args, **kwargs):
            return None

        def resolve_approval(self, *args, **kwargs):
            return None

        def artifact_for_id(self, project_id, artifact_id):
            return {"artifact_id": artifact_id}

        def mark_technically_complete(self, *args, **kwargs):
            return None

        def accept_completion(self, *args, **kwargs):
            return None

        def reopen_project(self, *args, **kwargs):
            return None

    def snapshot(value, *, artifact=None):
        return ProjectSnapshot(
            value,
            "active",
            4,
            "session-1",
            0,
            None,
            None,
            None,
            7,
            artifact,
        )

    mutation = name not in {
        "project.status",
        "queue.status",
        "artifact.get",
    }
    result = ProjectCommandService(
        runtime=Runtime(),
        snapshot_reader=snapshot,
        hermes_authority_factory=lambda: object(),
    ).dispatch(
        name,
        project_id=project_id,
        payload=payload,
        actor=actor,
        idempotency_key="command-1" if mutation else None,
        expected_version=0 if mutation else None,
    )

    assert isinstance(result, (ProjectSnapshot, ProjectCommandError))
    assert isinstance(result, ProjectSnapshot), result


def test_request_is_immutable_and_detaches_payload_mapping():
    from hermes_cli.project_command_service import ProjectCommandRequest

    payload = {"turn_id": "turn-1"}
    request = ProjectCommandRequest(
        name="run.stop",
        project_id="project-1",
        payload=payload,
        actor=_owner(),
        idempotency_key="stop-1",
        expected_version=4,
    )
    payload["turn_id"] = "forged"

    assert request.payload == {"turn_id": "turn-1"}
    with pytest.raises((FrozenInstanceError, AttributeError)):
        request.name = "run.resume"


@pytest.mark.parametrize(
    "payload",
    (
        {"name": "Unsafe", "folders": [""]},
        {"name": "Unsafe", "folders": ["."]},
        {"name": "Unsafe", "primary_path": "relative/repo"},
    ),
)
def test_project_create_rejects_process_relative_roots(payload):
    from hermes_cli.project_command_service import (
        ProjectCommandError,
        ProjectCommandService,
    )

    class Runtime:
        def create_managed_project(self, *_args, **_kwargs):
            raise AssertionError("invalid root reached persistence")

    result = ProjectCommandService(runtime=Runtime()).dispatch(
        "project.create",
        project_id=None,
        payload=payload,
        actor=_owner(),
        idempotency_key="unsafe-root",
        expected_version=0,
    )

    assert type(result) is ProjectCommandError
    assert result.code == "PROJECT_COMMAND_INVALID_ARGUMENT"


@pytest.mark.parametrize(
    "name",
    (
        "project.create",
        "project.rename",
        "turn.enqueue",
        "run.stop",
        "run.resume",
        "approval.resolve",
        "project.mark_technically_complete",
        "project.accept_completion",
        "project.reopen",
    ),
)
def test_mutating_command_requires_actor_idempotency_and_expected_version(
    name,
):
    from hermes_cli.project_command_service import (
        ProjectCommandError,
        ProjectCommandService,
    )

    result = ProjectCommandService().dispatch(
        name,
        project_id="project-1",
        payload={},
        actor=None,
        idempotency_key=None,
        expected_version=None,
    )

    assert type(result) is ProjectCommandError
    assert result.code == "PROJECT_COMMAND_MUTATION_PRECONDITION_REQUIRED"


def test_stop_and_resume_use_runtime_without_local_idempotency_state():
    from hermes_cli.project_command_service import (
        ProjectCommandService,
        ProjectSnapshot,
    )

    calls = []

    class Runtime:
        def request_stop(self, project_id, turn_id, actor, **kwargs):
            calls.append(("stop", project_id, turn_id, actor, kwargs))

        def request_resume(self, project_id, turn_id, actor, **kwargs):
            calls.append(("resume", project_id, turn_id, actor, kwargs))

    def snapshot(project_id, *, artifact=None):
        return ProjectSnapshot(
            project_id=project_id,
            lifecycle="active",
            version=7,
            canonical_session_id="session-1",
            queue_depth=0,
            active_turn_id="turn-1",
            active_run_control="stop_requested",
            pending_approval_id=None,
            last_event_sequence=9,
            artifact=artifact,
        )

    service = ProjectCommandService(runtime=Runtime(), snapshot_reader=snapshot)
    stop = service.dispatch(
        "run.stop",
        project_id="project-1",
        payload={"turn_id": "turn-1", "expected_control_version": 2},
        actor=_owner(),
        idempotency_key="same-control-key",
        expected_version=6,
    )
    resume = service.dispatch(
        "run.resume",
        project_id="project-1",
        payload={"turn_id": "turn-1", "expected_control_version": 3},
        actor=_owner(),
        idempotency_key="same-control-key",
        expected_version=7,
    )

    assert stop.project_id == resume.project_id == "project-1"
    assert [call[0] for call in calls] == ["stop", "resume"]
    assert calls[0][4] == {
        "idempotency_key": "same-control-key",
        "expected_version": 6,
        "expected_control_version": 2,
    }
    assert calls[1][4] == {
        "idempotency_key": "same-control-key",
        "expected_version": 7,
        "expected_control_version": 3,
    }


def test_create_uses_an_explicit_creation_version_without_a_project_id():
    from hermes_cli.project_command_service import (
        ProjectCommandError,
        ProjectCommandService,
    )

    result = ProjectCommandService().dispatch(
        "project.create",
        project_id=None,
        payload={"name": "New project"},
        actor=_owner(),
        idempotency_key="create-1",
        expected_version=0,
    )

    assert type(result) is ProjectCommandError
    assert result.code == "PROJECT_COMMAND_PORT_UNAVAILABLE"


def test_status_and_artifact_reads_use_runtime_owner_authorization():
    from hermes_cli.project_command_service import (
        ProjectCommandService,
        ProjectSnapshot,
    )

    calls = []

    class Runtime:
        def list_queue(self, project_id, actor):
            calls.append(("list_queue", project_id, actor))
            return ()

        def artifact_for_id(self, project_id, artifact_id):
            calls.append(("artifact_for_id", project_id, artifact_id))
            return {"artifact_id": artifact_id}

    def snapshot(project_id, *, artifact=None):
        return ProjectSnapshot(
            project_id, "active", 1, "session-1", 0, None, None, None, 1,
            artifact,
        )

    service = ProjectCommandService(runtime=Runtime(), snapshot_reader=snapshot)
    status = service.dispatch(
        "project.status", project_id="project-1", payload={}, actor=_owner(),
    )
    artifact = service.dispatch(
        "artifact.get", project_id="project-1", payload={"artifact_id": "a-1"},
        actor=_owner(),
    )

    assert status.project_id == artifact.project_id == "project-1"
    assert [call[0] for call in calls] == [
        "list_queue", "list_queue", "artifact_for_id",
    ]


@pytest.mark.parametrize(
    "name",
    (
        "approval.resolve",
        "project.mark_technically_complete",
        "project.accept_completion",
        "project.reopen",
    ),
)
def test_missing_public_runtime_ports_fail_closed(name):
    from hermes_cli.project_command_service import (
        ProjectCommandError,
        ProjectCommandService,
    )

    result = ProjectCommandService().dispatch(
        name,
        project_id="project-1",
        payload={},
        actor=_owner(),
        idempotency_key="key-1",
        expected_version=1,
    )

    assert type(result) is ProjectCommandError
    assert result.code == "PROJECT_COMMAND_PORT_UNAVAILABLE"
    assert result.message == "canonical runtime port is unavailable"


@pytest.fixture
def managed_command_env(tmp_path):
    from hermes_cli.project_runtime import ProjectRuntime

    conn = projects_db.connect(tmp_path / "projects.db")
    project_id = projects_db.create_project(
        conn, name="Managed", folders=(str(tmp_path),)
    )
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="session-root",
        current_phase="implementation",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="desktop-owner",
        project_id=project_id,
        surface="desktop",
        external_binding_id="window-1",
        actor_id="owner-1",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="discord-owner",
        project_id=project_id,
        surface="discord",
        external_binding_id="channel-1",
        actor_id="owner-1",
        now=1,
    )
    runtime = ProjectRuntime(conn, clock=lambda: 100)
    dispatcher = runtime.acquire_dispatcher_lease(
        "b7090781-62d6-4e63-91ac-d32efad3b99e",
        lease_seconds=1000,
    )
    assert dispatcher is not None
    yield {
        "conn": conn,
        "project_id": project_id,
        "runtime": runtime,
        "root": tmp_path,
        "desktop": ActorContext(
            "owner-1", "desktop", "desktop-owner", True
        ),
        "discord": ActorContext(
            "owner-1", "discord", "discord-owner", True
        ),
        "hermes": ActorContext("hermes", "system", "core", False),
        "dispatcher": dispatcher,
    }
    conn.close()


def test_runtime_snapshot_is_actor_bound_and_contains_canonical_projection(
    managed_command_env,
):
    env = managed_command_env

    snapshot = env["runtime"].snapshot_for_actor(
        env["project_id"], env["desktop"]
    )

    assert snapshot.project_id == env["project_id"]
    assert snapshot.lifecycle == "active"
    assert snapshot.current_phase == "implementation"
    assert snapshot.version == 0
    assert snapshot.canonical_session_id == "session-root"
    assert snapshot.queue_depth == 0
    assert snapshot.last_event_sequence == 0
    assert snapshot.active_control_version is None
    with pytest.raises(Exception) as raised:
        env["runtime"].snapshot_for_actor(
            env["project_id"],
            ActorContext("owner-1", "discord", "desktop-owner", True),
        )
    assert raised.value.code.value == "actor_not_authorized"


def test_enqueue_receipt_replays_the_same_accepted_turn_id(
    managed_command_env,
):
    from hermes_cli.project_command_service import ProjectCommandService

    env = managed_command_env
    service = ProjectCommandService(runtime=env["runtime"])
    request = {
        "name": "turn.enqueue",
        "project_id": env["project_id"],
        "payload": {"message": "ship once"},
        "actor": env["desktop"],
        "idempotency_key": "enqueue-receipt-replay",
        "expected_version": 0,
    }

    first = service.dispatch(**request)
    replay = service.dispatch(**request)

    assert first.accepted_turn_id is not None
    assert replay.accepted_turn_id == first.accepted_turn_id
    assert first.active_control_version is None
    assert replay.active_control_version is None
    assert env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_turns
        WHERE project_id = ?
        """,
        (env["project_id"],),
    ).fetchone()[0] == 1
    assert env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'turn.queued'
        """,
        (env["project_id"],),
    ).fetchone()[0] == 1


def test_control_conflict_preserves_current_control_version(
    managed_command_env,
):
    from hermes_cli.project_command_service import (
        ProjectCommandError,
        ProjectCommandService,
    )

    env = managed_command_env
    service = ProjectCommandService(runtime=env["runtime"])
    enqueued = service.dispatch(
        "turn.enqueue",
        project_id=env["project_id"],
        payload={"message": "control me"},
        actor=env["desktop"],
        idempotency_key="enqueue-control-version",
        expected_version=0,
    )
    claim = env["runtime"].claim_next_turn(
        env["project_id"],
        "worker-control-version",
        lease_seconds=100,
    )
    assert claim is not None

    conflict = service.dispatch(
        "run.stop",
        project_id=env["project_id"],
        payload={
            "turn_id": enqueued.accepted_turn_id,
            "expected_control_version": 0,
        },
        actor=env["desktop"],
        idempotency_key="stop-control-conflict",
        expected_version=2,
    )

    assert type(conflict) is ProjectCommandError
    assert conflict.code == "PROJECT_RUNTIME_CONTROL_VERSION_CONFLICT"
    assert conflict.current_version is None
    assert conflict.current_control_version == 1


def test_technical_completion_requires_hermes_and_an_empty_queue(
    managed_command_env,
):
    env = managed_command_env
    runtime = env["runtime"]
    project_id = env["project_id"]
    runtime.enqueue_turn(
        project_id,
        {"message": "still queued"},
        env["desktop"],
        idempotency_key="turn-1",
        expected_version=0,
    )
    before = prdb.runtime_state_for_project(env["conn"], project_id)
    event_count = env["conn"].execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0]

    with pytest.raises(Exception) as queue_error:
        runtime.mark_technically_complete(
            project_id,
            env["dispatcher"],
            idempotency_key="complete-1",
            expected_version=before.version,
        )
    assert queue_error.value.code.value == "project_queue_not_empty"
    assert prdb.runtime_state_for_project(env["conn"], project_id) == before
    assert env["conn"].execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == event_count

    env["conn"].execute(
        """
        UPDATE project_turns SET status = 'cancelled'
        WHERE project_id = ?
        """,
        (project_id,),
    )
    env["conn"].execute(
        """
        UPDATE project_run_controls SET control_state = 'terminal'
        WHERE project_id = ?
        """,
        (project_id,),
    )
    env["conn"].commit()

    with pytest.raises(Exception) as actor_error:
        runtime.mark_technically_complete(
            project_id,
            env["desktop"],
            idempotency_key="complete-owner",
            expected_version=before.version,
        )
    assert actor_error.value.code.value == "actor_not_authorized"

    completed = runtime.mark_technically_complete(
        project_id,
        env["dispatcher"],
        idempotency_key="complete-1",
        expected_version=before.version,
    )
    assert completed.lifecycle == "awaiting_acceptance"


def test_technical_completion_rejects_forged_or_stale_core_authority(
    managed_command_env,
):
    from hermes_cli.project_runtime import DispatcherLease

    env = managed_command_env
    runtime = env["runtime"]
    project_id = env["project_id"]

    with pytest.raises(Exception) as forged:
        runtime.mark_technically_complete(
            project_id,
            env["hermes"],
            idempotency_key="forged-core-actor",
            expected_version=0,
        )
    assert forged.value.code.value == "actor_not_authorized"

    stale = DispatcherLease(
        env["dispatcher"].instance_id,
        env["dispatcher"].generation,
        env["dispatcher"].fencing_token + 1,
        env["dispatcher"].expires_at,
    )
    with pytest.raises(Exception) as stale_error:
        runtime.mark_technically_complete(
            project_id,
            stale,
            idempotency_key="stale-core-lease",
            expected_version=0,
        )
    assert stale_error.value.code.value == "stale_dispatcher_lease"
    assert prdb.runtime_state_for_project(
        env["conn"], project_id
    ).lifecycle == "active"


@pytest.mark.parametrize("nonterminal_state", ("claimed", "stopped"))
def test_technical_completion_rejects_active_or_stopped_turn(
    managed_command_env, nonterminal_state
):
    env = managed_command_env
    runtime = env["runtime"]
    project_id = env["project_id"]
    turn = runtime.enqueue_turn(
        project_id,
        {"message": nonterminal_state},
        env["desktop"],
        idempotency_key=f"turn-{nonterminal_state}",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id, "worker-1", lease_seconds=30
    )
    if nonterminal_state == "stopped":
        runtime.request_stop(
            project_id,
            turn.turn_id,
            env["desktop"],
            idempotency_key="stop-for-completion",
            expected_version=2,
            expected_control_version=1,
        )
        runtime.acknowledge_stopped(claim)
    state = prdb.runtime_state_for_project(env["conn"], project_id)

    with pytest.raises(Exception) as raised:
        runtime.mark_technically_complete(
            project_id,
            env["dispatcher"],
            idempotency_key=f"complete-{nonterminal_state}",
            expected_version=state.version,
        )

    assert raised.value.code.value == "project_queue_not_empty"
    assert prdb.runtime_state_for_project(
        env["conn"], project_id
    ) == state


def test_accept_and_reopen_are_owner_only_idempotent_and_preserve_lineage(
    managed_command_env,
):
    env = managed_command_env
    runtime = env["runtime"]
    conn = env["conn"]
    project_id = env["project_id"]
    awaiting = runtime.mark_technically_complete(
        project_id,
        env["dispatcher"],
        idempotency_key="technical-1",
        expected_version=0,
    )
    lineage_before = prdb.lineage_for_project(conn, project_id=project_id)
    artifact_path = env["root"] / "completion-artifact.txt"
    artifact_path.write_text("preserve me", encoding="utf-8")
    artifact_before = runtime.register_verified_artifact(
        project_id,
        artifact_id="completion-artifact",
        path=artifact_path,
        metadata={"kind": "completion"},
    )

    with pytest.raises(Exception) as actor_error:
        runtime.accept_completion(
            project_id,
            env["hermes"],
            idempotency_key="accept-system",
            expected_version=awaiting.version,
        )
    assert actor_error.value.code.value == "actor_not_authorized"

    completed = runtime.accept_completion(
        project_id,
        env["discord"],
        idempotency_key="accept-1",
        expected_version=awaiting.version,
    )
    assert completed.lifecycle == "completed"

    reopened = runtime.reopen_project(
        project_id,
        env["desktop"],
        idempotency_key="reopen-1",
        expected_version=completed.version,
    )
    duplicate = runtime.reopen_project(
        project_id,
        env["discord"],
        idempotency_key="reopen-1",
        expected_version=completed.version,
    )

    assert reopened.lifecycle == duplicate.lifecycle == "active"
    assert reopened.version == duplicate.version
    assert prdb.lineage_for_project(conn, project_id=project_id) == lineage_before
    assert runtime.artifact_for_id(
        project_id, "completion-artifact"
    ) == artifact_before
    with pytest.raises(Exception) as stale:
        runtime.reopen_project(
            project_id,
            env["desktop"],
            idempotency_key="reopen-stale",
            expected_version=completed.version,
        )
    assert stale.value.code.value == "project_version_conflict"


def test_reopen_from_awaiting_acceptance_is_supported(managed_command_env):
    env = managed_command_env
    awaiting = env["runtime"].mark_technically_complete(
        env["project_id"],
        env["dispatcher"],
        idempotency_key="technical-awaiting",
        expected_version=0,
    )

    reopened = env["runtime"].reopen_project(
        env["project_id"],
        env["discord"],
        idempotency_key="reopen-awaiting",
        expected_version=awaiting.version,
    )

    assert reopened.lifecycle == "active"


def test_command_service_dispatches_completion_lifecycle(
    managed_command_env,
):
    from hermes_cli.project_command_service import ProjectCommandService

    env = managed_command_env
    service = ProjectCommandService(
        runtime=env["runtime"],
        hermes_authority_factory=lambda: env["dispatcher"],
    )
    technical = service.dispatch(
        "project.mark_technically_complete",
        project_id=env["project_id"],
        payload={},
        actor=env["hermes"],
        idempotency_key="technical-service",
        expected_version=0,
    )
    accepted = service.dispatch(
        "project.accept_completion",
        project_id=env["project_id"],
        payload={},
        actor=env["desktop"],
        idempotency_key="accept-service",
        expected_version=technical.version,
    )
    reopened = service.dispatch(
        "project.reopen",
        project_id=env["project_id"],
        payload={},
        actor=env["discord"],
        idempotency_key="reopen-service",
        expected_version=accepted.version,
    )

    assert technical.lifecycle == "awaiting_acceptance"
    assert accepted.lifecycle == "completed"
    assert reopened.lifecycle == "active"


def test_desktop_and_discord_replay_same_stop_and_resume_commands(
    managed_command_env,
):
    from hermes_cli.project_command_service import ProjectCommandService

    env = managed_command_env
    runtime = env["runtime"]
    project_id = env["project_id"]
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "cross-surface control"},
        env["desktop"],
        idempotency_key="cross-surface-turn",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id, "worker-cross-surface", lease_seconds=30
    )
    service = ProjectCommandService(runtime=runtime)
    stop_payload = {
        "turn_id": turn.turn_id,
        "expected_control_version": 1,
    }

    desktop_stop = service.dispatch(
        "run.stop",
        project_id=project_id,
        payload=stop_payload,
        actor=env["desktop"],
        idempotency_key="cross-surface-stop",
        expected_version=2,
    )
    discord_stop = service.dispatch(
        "run.stop",
        project_id=project_id,
        payload=stop_payload,
        actor=env["discord"],
        idempotency_key="cross-surface-stop",
        expected_version=2,
    )
    runtime.acknowledge_stopped(claim)
    resume_payload = {
        "turn_id": turn.turn_id,
        "expected_control_version": 3,
    }
    discord_resume = service.dispatch(
        "run.resume",
        project_id=project_id,
        payload=resume_payload,
        actor=env["discord"],
        idempotency_key="cross-surface-resume",
        expected_version=4,
    )
    desktop_resume = service.dispatch(
        "run.resume",
        project_id=project_id,
        payload=resume_payload,
        actor=env["desktop"],
        idempotency_key="cross-surface-resume",
        expected_version=4,
    )
    event_count = env["conn"].execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0]
    delayed_desktop_stop = service.dispatch(
        "run.stop",
        project_id=project_id,
        payload=stop_payload,
        actor=env["desktop"],
        idempotency_key="cross-surface-stop",
        expected_version=2,
    )

    assert desktop_stop.version == discord_stop.version == 3
    assert desktop_stop.active_turn_id == discord_stop.active_turn_id
    assert discord_resume.version == desktop_resume.version == 5
    assert discord_resume.queue_depth == desktop_resume.queue_depth == 1
    assert delayed_desktop_stop.version == 5
    assert delayed_desktop_stop.queue_depth == 1
    assert env["conn"].execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == event_count
    stored_turn = prdb._runtime_turn_for_project(
        env["conn"], project_id=project_id, turn_id=turn.turn_id
    )
    assert stored_turn.turn_id == turn.turn_id
    assert stored_turn.sequence == turn.sequence


def test_command_service_resolves_approval_and_reads_artifact(
    managed_command_env, tmp_path
):
    from hermes_cli.project_command_service import ProjectCommandService

    env = managed_command_env
    approval = prdb.ApprovalRequest(
        approval_id="approval-service",
        project_id=env["project_id"],
        requester_actor_id="owner-1",
        authorization_actor_id="owner-1",
        canonical_action="publish",
        approval_class="publish",
        command_revision=1,
        expected_runtime_version=0,
        expected_lifecycle="active",
        expected_phase="implementation",
        targets=("C:/work/runtime/release",),
        batch_id="batch-service",
        batch_items=("release",),
        status="pending",
        expires_at=1000,
    )
    prdb.create_approval_request(env["conn"], approval, now=10)
    artifact_path = tmp_path / "artifact.txt"
    artifact_path.write_text("verified", encoding="utf-8")
    env["runtime"].register_verified_artifact(
        env["project_id"],
        artifact_id="artifact-service",
        path=artifact_path,
        metadata={"kind": "report"},
    )
    service = ProjectCommandService(runtime=env["runtime"])

    resolved = service.dispatch(
        "approval.resolve",
        project_id=env["project_id"],
        payload={
            "approval_id": "approval-service",
            "outcome": "approved",
        },
        actor=env["desktop"],
        idempotency_key="approval-resolve-service",
        expected_version=0,
    )
    artifact = service.dispatch(
        "artifact.get",
        project_id=env["project_id"],
        payload={"artifact_id": "artifact-service"},
        actor=env["discord"],
    )

    assert resolved.project_id == env["project_id"]
    assert resolved.pending_approval_id is None
    assert artifact.artifact["artifact_id"] == "artifact-service"
    assert artifact.artifact["status"] == "verified"


def test_command_service_routes_operation_approval_to_guard():
    from enum import Enum

    from hermes_cli.project_command_service import (
        ProjectCommandService,
        ProjectSnapshot,
    )

    class Code(str, Enum):
        VALUE = "operation_approval_required"

    class RuntimeErrorWithCode(RuntimeError):
        code = Code.VALUE

    class Runtime:
        def resolve_approval(self, *_args, **_kwargs):
            raise RuntimeErrorWithCode

        def snapshot_for_actor(self, project_id, _actor):
            return SimpleNamespace(
                project_id=project_id,
                lifecycle="active",
                current_phase="implementation",
                version=1,
                canonical_session_id="session-1",
                queue_depth=0,
                active_turn_id=None,
                active_run_control=None,
                pending_approval_id=None,
                last_event_sequence=1,
            )

    class Operations:
        def __init__(self):
            self.calls = []

        def resolve_operation_approval(
            self, approval_id, actor, *, outcome
        ):
            self.calls.append((approval_id, actor, outcome))
            return SimpleNamespace(project_id="project-1")

    operations = Operations()
    result = ProjectCommandService(
        runtime=Runtime(), operations=operations
    ).dispatch(
        "approval.resolve",
        project_id="project-1",
        payload={
            "approval_id": "operation-approval-1",
            "outcome": "approved",
        },
        actor=_owner(),
        idempotency_key="operation-approval-command",
        expected_version=0,
    )

    assert type(result) is ProjectSnapshot
    assert operations.calls == [
        ("operation-approval-1", _owner(), "approved")
    ]


def test_command_service_creates_and_renames_through_runtime(tmp_path):
    from hermes_cli.project_command_service import ProjectCommandService
    from hermes_cli.project_runtime import ProjectRuntime

    conn = projects_db.connect(tmp_path / "projects.db")
    try:
        runtime = ProjectRuntime(conn, clock=lambda: 100)
        actor = ActorContext(
            "owner-1", "desktop", "desktop-local", True
        )
        service = ProjectCommandService(runtime=runtime)

        created = service.dispatch(
            "project.create",
            project_id=None,
            payload={
                "name": "New managed project",
                "current_phase": "planning",
            },
            actor=actor,
            idempotency_key="create-1",
            expected_version=0,
        )
        replay = service.dispatch(
            "project.create",
            project_id=None,
            payload={
                "name": "New managed project",
                "current_phase": "planning",
            },
            actor=actor,
            idempotency_key="create-1",
            expected_version=0,
        )
        from hermes_cli.project_command_service import ProjectCommandError

        assert not isinstance(created, ProjectCommandError), created
        assert not isinstance(replay, ProjectCommandError), replay
        renamed = service.dispatch(
            "project.rename",
            project_id=created.project_id,
            payload={"name": "Renamed project"},
            actor=actor,
            idempotency_key="rename-1",
            expected_version=created.version,
        )

        assert replay.project_id == created.project_id
        assert renamed.project_id == created.project_id
        assert renamed.version == created.version + 1
        assert projects_db.get_project(conn, created.project_id).name == (
            "Renamed project"
        )
    finally:
        conn.close()
