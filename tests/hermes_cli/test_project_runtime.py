"""Behavioral contract tests for the durable per-project FIFO runtime."""

from __future__ import annotations

import importlib
import math
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import ActorContext


def _event_count(conn, project_id):
    return conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?", (project_id,)
    ).fetchone()[0]


def _adopt_bound_project(conn, *, name, root, binding, external):
    project_id = projects_db.create_project(conn, name=name)
    prdb.create_project_conversation(
        conn, project_id=project_id, conversation_id=root,
        current_phase="implementation", now=1,
    )
    prdb.bind_surface(
        conn, binding_id=binding, project_id=project_id, surface="desktop",
        external_binding_id=external, actor_id="owner-1", now=1,
    )
    return project_id, ActorContext("owner-1", "desktop", binding, True)


@pytest.fixture
def runtime_env(tmp_path):
    conn = projects_db.connect(tmp_path / "projects.db")
    project_id = projects_db.create_project(
        conn, name="Runtime", folders=("C:/work/runtime",)
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
    module = importlib.import_module("hermes_cli.project_runtime")
    yield {
        "conn": conn,
        "project_id": project_id,
        "runtime": module.ProjectRuntime(conn, clock=lambda: 100),
        "desktop": ActorContext("owner-1", "desktop", "desktop-owner", True),
        "discord": ActorContext("owner-1", "discord", "discord-owner", True),
        "module": module,
    }
    conn.close()


def test_enqueue_returns_a_canonical_queued_turn_for_an_exact_owner_binding(runtime_env):
    runtime = runtime_env["runtime"]
    turn = runtime.enqueue_turn(
        runtime_env["project_id"],
        {"message": "first", "client_timestamp": 999999},
        runtime_env["desktop"],
        idempotency_key="enqueue-1",
        expected_version=0,
    )

    assert turn.sequence == 1
    assert turn.status == "queued"
    assert turn.origin_binding_id == "desktop-owner"
    assert turn.payload == {"client_timestamp": 999999, "message": "first"}
    control = prdb.runtime_control_for_turn(
        runtime_env["conn"], project_id=runtime_env["project_id"], turn_id=turn.turn_id
    )
    assert control.control_state == "running"
    assert control.control_version == 0


def test_enqueue_is_canonical_idempotent_and_rejects_changed_origin_or_payload(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    first = runtime.enqueue_turn(
        project_id,
        {"b": [1, {"a": True}], "a": None},
        runtime_env["desktop"],
        idempotency_key="same-key",
        expected_version=0,
    )
    replay = runtime.enqueue_turn(
        project_id,
        {"a": None, "b": [1, {"a": True}]},
        runtime_env["desktop"],
        idempotency_key="same-key",
        expected_version=999,
    )
    assert replay == first
    assert prdb.runtime_state_for_project(runtime_env["conn"], project_id).version == 1
    assert _event_count(runtime_env["conn"], project_id) == 1
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as payload_conflict:
        runtime.enqueue_turn(
            project_id,
            {"a": "changed"},
            runtime_env["desktop"],
            idempotency_key="same-key",
            expected_version=1,
        )
    assert payload_conflict.value.code is runtime_env["module"].RuntimeErrorCode.IDEMPOTENCY_CONFLICT
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as origin_conflict:
        runtime.enqueue_turn(
            project_id,
            {"a": None, "b": [1, {"a": True}]},
            runtime_env["discord"],
            idempotency_key="same-key",
            expected_version=1,
        )
    assert origin_conflict.value.code is runtime_env["module"].RuntimeErrorCode.IDEMPOTENCY_CONFLICT


def test_fifo_sequences_are_project_scoped_and_ignore_client_timestamps(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turns = [
        runtime.enqueue_turn(project_id, {"client_timestamp": timestamp}, runtime_env["desktop"], idempotency_key=f"time-{timestamp}", expected_version=index)
        for index, timestamp in enumerate((300, 100, 200))
    ]
    other_id, other_actor = _adopt_bound_project(
        runtime_env["conn"], name="Other", root="other-root",
        binding="other-binding", external="other-window",
    )
    other = runtime.enqueue_turn(other_id, {"client_timestamp": 1}, other_actor, idempotency_key="other", expected_version=0)
    assert [turn.sequence for turn in turns] == [1, 2, 3]
    assert other.sequence == 1
    assert [turn.sequence for turn in runtime.list_queue(project_id, runtime_env["desktop"])] == [1, 2, 3]


def test_queue_reads_require_the_exact_durable_owner_binding(runtime_env):
    runtime = runtime_env["runtime"]
    runtime.enqueue_turn(runtime_env["project_id"], {"x": 1}, runtime_env["desktop"], idempotency_key="queued", expected_version=0)
    forged = ActorContext("owner-1", "discord", "desktop-owner", True)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as denied:
        runtime.list_queue(runtime_env["project_id"], forged)
    assert denied.value.code is runtime_env["module"].RuntimeErrorCode.ACTOR_NOT_AUTHORIZED


@pytest.mark.parametrize(
    "payload",
    [
        {"number": math.nan},
        {"number": math.inf},
        {1: "not-a-string-key"},
        {"tuple": ("not", "json")},
        {"set": {"not", "json"}},
    ],
)
def test_enqueue_rejects_noncanonical_json_before_writing(runtime_env, payload):
    runtime = runtime_env["runtime"]
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as error:
        runtime.enqueue_turn(
            runtime_env["project_id"], payload, runtime_env["desktop"],
            idempotency_key="invalid-json", expected_version=0,
        )
    assert error.value.code is runtime_env["module"].RuntimeErrorCode.INVALID_ARGUMENT
    assert _event_count(runtime_env["conn"], runtime_env["project_id"]) == 0


def test_enqueue_rejects_a_cyclic_json_object_before_sql(runtime_env):
    payload = {"cycle": []}
    payload["cycle"].append(payload)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as error:
        runtime_env["runtime"].enqueue_turn(
            runtime_env["project_id"], payload, runtime_env["desktop"],
            idempotency_key="cycle", expected_version=0,
        )
    assert error.value.code is runtime_env["module"].RuntimeErrorCode.INVALID_ARGUMENT
    assert _event_count(runtime_env["conn"], runtime_env["project_id"]) == 0


def test_project_fifo_claim_cancel_and_cross_project_isolation(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    first = runtime.enqueue_turn(project_id, {"order": 3}, runtime_env["desktop"], idempotency_key="q-3", expected_version=0)
    second = runtime.enqueue_turn(project_id, {"order": 1}, runtime_env["desktop"], idempotency_key="q-1", expected_version=1)
    queued = runtime.list_queue(project_id, runtime_env["desktop"])
    assert [turn.turn_id for turn in queued] == [first.turn_id, second.turn_id]
    claim = runtime.claim_next_turn(project_id, "worker-one", lease_seconds=30)
    assert claim is not None and claim.turn_id == first.turn_id and claim.sequence == 1
    assert runtime.claim_next_turn(project_id, "worker-two", lease_seconds=30) is None
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as not_queued:
        runtime.cancel_queued_turn(project_id, first.turn_id, runtime_env["desktop"], expected_version=3)
    assert not_queued.value.code is runtime_env["module"].RuntimeErrorCode.TURN_NOT_QUEUED
    cancelled = runtime.cancel_queued_turn(project_id, second.turn_id, runtime_env["desktop"], expected_version=3)
    assert cancelled.status == "cancelled"
    assert prdb.runtime_control_for_turn(runtime_env["conn"], project_id=project_id, turn_id=second.turn_id).control_state == "terminal"


def test_stop_ack_resume_preserves_logical_turn_and_rotates_next_claim(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"message": "work"}, runtime_env["desktop"], idempotency_key="enqueue", expected_version=0)
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    stopped = runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"], idempotency_key="stop", expected_version=2, expected_control_version=1,
    )
    assert stopped.control_state == "stop_requested"
    replay = runtime.request_stop(
        project_id, turn.turn_id, runtime_env["discord"], idempotency_key="stop", expected_version=2, expected_control_version=1,
    )
    assert replay == stopped
    assert _event_count(runtime_env["conn"], project_id) == 3
    acknowledged = runtime.acknowledge_stopped(claim)
    assert acknowledged.control_state == "stopped"
    resumed = runtime.request_resume(
        project_id, turn.turn_id, runtime_env["discord"], idempotency_key="resume", expected_version=4, expected_control_version=3,
    )
    assert resumed.control_state == "resume_requested"
    same_turn = prdb.runtime_turn_for_project(
        runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id
    )
    assert same_turn.sequence == turn.sequence and same_turn.attempt_id == claim.attempt_id
    renewed = runtime.claim_next_turn(project_id, "worker-two", lease_seconds=30)
    assert renewed.turn_id == turn.turn_id
    assert renewed.attempt_id != claim.attempt_id
    assert renewed.lease_generation == claim.lease_generation + 1
    assert renewed.fencing_token == claim.fencing_token + 1


def test_approval_request_is_atomic_nonwaiting_and_blocks_the_fifo_head(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"message": "approve"}, runtime_env["desktop"], idempotency_key="approval-turn", expected_version=0)
    runtime.enqueue_turn(project_id, {"message": "later"}, runtime_env["desktop"], idempotency_key="later-turn", expected_version=1)
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = prdb.ApprovalRequest(
        approval_id="approval-1", project_id=project_id,
        requester_actor_id="owner-1", authorization_actor_id="owner-1",
        canonical_action="publish", approval_class="publish", command_revision=1,
        expected_runtime_version=3, expected_lifecycle="active",
        expected_phase="implementation", targets=("C:/work/runtime/release",),
        batch_id="batch-1", batch_items=("release",), status="pending",
        expires_at=1000,
    )
    approval = runtime.request_turn_approval(project_id, turn.turn_id, request, runtime_env["desktop"])
    assert approval.approval_id == request.approval_id
    assert approval.targets == ("c:/work/runtime/release",)
    assert prdb.runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).status == "awaiting_approval"
    assert runtime.claim_next_turn(project_id, "other-worker", lease_seconds=30) is None
    assert runtime_env["conn"].execute("SELECT turn_id FROM project_approvals WHERE approval_id = 'approval-1'").fetchone()[0] == turn.turn_id
    assert runtime_env["conn"].in_transaction is False


def test_stop_crash_state_is_durable_and_recovery_blocked_resume_fails_closed(runtime_env, tmp_path):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"message": "stop"}, runtime_env["desktop"], idempotency_key="stop-turn", expected_version=0)
    runtime.enqueue_turn(project_id, {"message": "later"}, runtime_env["desktop"], idempotency_key="later-turn", expected_version=1)
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(project_id, turn.turn_id, runtime_env["desktop"], idempotency_key="stop", expected_version=3, expected_control_version=1)
    runtime_env["conn"].execute(
        """INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key, approval_id,
            command_revision, targets_json, payload_json, status, receipt_json,
            created_at, updated_at
        ) VALUES ('op-1', ?, ?, 'op-key', NULL, 1, '[]', '{}', 'unknown', NULL, 1, 1)""",
        (project_id, turn.turn_id),
    )
    runtime_env["conn"].commit()
    claim = runtime.claim_next_turn(project_id, "other", lease_seconds=30)
    assert claim is None
    original_claim = runtime_env["module"].TurnClaim(
        turn_id=turn.turn_id, project_id=project_id, sequence=1,
        lease_generation=1, fencing_token=1, canonical_session_id="session-root",
        attempt_id=prdb.runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).attempt_id,
        worker_id="worker",
    )
    runtime.acknowledge_stopped(original_claim)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as blocked:
        runtime.request_resume(project_id, turn.turn_id, runtime_env["desktop"], idempotency_key="resume", expected_version=5, expected_control_version=3)
    assert blocked.value.code is runtime_env["module"].RuntimeErrorCode.TURN_RECOVERY_BLOCKED


def test_forged_actor_and_boolean_cas_are_rejected_without_runtime_writes(runtime_env):
    runtime = runtime_env["runtime"]
    forged = ActorContext("owner-1", "desktop", "desktop-owner", False)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as unauthorized:
        runtime.enqueue_turn(runtime_env["project_id"], {"x": 1}, forged, idempotency_key="x", expected_version=0)
    assert unauthorized.value.code is runtime_env["module"].RuntimeErrorCode.ACTOR_NOT_AUTHORIZED
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as invalid:
        runtime.enqueue_turn(runtime_env["project_id"], {"x": 1}, runtime_env["desktop"], idempotency_key="x", expected_version=True)
    assert invalid.value.code is runtime_env["module"].RuntimeErrorCode.INVALID_ARGUMENT
    assert _event_count(runtime_env["conn"], runtime_env["project_id"]) == 0


def test_exact_approval_retry_uses_canonical_immutable_request_identity(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"message": "approve"}, runtime_env["desktop"], idempotency_key="queued", expected_version=0)
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = prdb.ApprovalRequest(
        approval_id="approval-retry", project_id=project_id,
        requester_actor_id="owner-1", authorization_actor_id="owner-1",
        canonical_action="publish", approval_class="publish", command_revision=1,
        expected_runtime_version=2, expected_lifecycle="active", expected_phase="implementation",
        targets=("C:/work/runtime/release",), batch_id="batch", batch_items=("release",),
        status="pending", expires_at=1000,
    )
    first = runtime.request_turn_approval(project_id, turn.turn_id, request, runtime_env["desktop"])
    replay = runtime.request_turn_approval(project_id, turn.turn_id, request, runtime_env["discord"])
    assert replay == first
    assert _event_count(runtime_env["conn"], project_id) == 3


def test_event_conflict_rolls_back_turn_control_and_runtime_version(runtime_env):
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    conn.execute(
        """INSERT INTO project_events (
            event_id, project_id, sequence, kind, turn_id, payload_json, created_at
        ) VALUES ('fixed-event', ?, 1, 'prior', NULL, '{}', 1)""",
        (project_id,),
    )
    conn.commit()
    module = runtime_env["module"]
    runtime = module.ProjectRuntime(
        conn, clock=lambda: 100,
        id_factory=lambda kind: "fixed-event" if kind == "event" else "fixed-turn",
    )
    with pytest.raises(sqlite3.IntegrityError):
        runtime.enqueue_turn(project_id, {"message": "must-roll-back"}, runtime_env["desktop"], idempotency_key="rollback", expected_version=0)
    assert prdb.runtime_state_for_project(conn, project_id).version == 0
    assert conn.execute("SELECT COUNT(*) FROM project_turns WHERE project_id = ?", (project_id,)).fetchone()[0] == 0
    assert _event_count(conn, project_id) == 1


def test_frozen_payload_and_stale_worker_acknowledgement_are_fail_closed(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"outer": {"items": [1, 2]}}, runtime_env["desktop"], idempotency_key="freeze", expected_version=0)
    with pytest.raises(TypeError):
        turn.payload["new"] = "nope"
    assert turn.payload["outer"]["items"] == (1, 2)
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(project_id, turn.turn_id, runtime_env["desktop"], idempotency_key="stop", expected_version=2, expected_control_version=1)
    forged = runtime_env["module"].TurnClaim(
        turn_id=claim.turn_id, project_id=claim.project_id, sequence=claim.sequence,
        lease_generation=claim.lease_generation, fencing_token=claim.fencing_token,
        canonical_session_id=claim.canonical_session_id, attempt_id=claim.attempt_id,
        worker_id="forged-worker",
    )
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.acknowledge_stopped(forged)
    assert rejected.value.code is runtime_env["module"].RuntimeErrorCode.TURN_NOT_STOP_REQUESTED
    assert prdb.runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).status == "stop_requested"


def test_two_file_backed_connections_claim_one_oldest_turn_in_25_races(tmp_path):
    module = importlib.import_module("hermes_cli.project_runtime")
    path = tmp_path / "race.db"
    for index in range(25):
        bootstrap = projects_db.connect(path)
        project_id = projects_db.create_project(bootstrap, name=f"Race {index}")
        prdb.create_project_conversation(bootstrap, project_id=project_id, conversation_id=f"root-{index}", current_phase="implementation", now=1)
        prdb.bind_surface(bootstrap, binding_id=f"binding-{index}", project_id=project_id, surface="desktop", external_binding_id=f"window-{index}", actor_id="owner", now=1)
        owner = ActorContext("owner", "desktop", f"binding-{index}", True)
        service = module.ProjectRuntime(bootstrap, clock=lambda: 100)
        service.enqueue_turn(project_id, {"n": 1}, owner, idempotency_key=f"first-{index}", expected_version=0)
        service.enqueue_turn(project_id, {"n": 2}, owner, idempotency_key=f"second-{index}", expected_version=1)
        bootstrap.close()
        barrier = threading.Barrier(2)
        def claim(worker):
            conn = projects_db.connect(path)
            try:
                barrier.wait(timeout=5)
                return module.ProjectRuntime(conn, clock=lambda: 101).claim_next_turn(project_id, worker, lease_seconds=30)
            finally:
                conn.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [future.result(timeout=10) for future in (pool.submit(claim, "a"), pool.submit(claim, "b"))]
        winners = [claim for claim in results if claim is not None]
        assert len(winners) == 1 and winners[0].sequence == 1
        check = projects_db.connect(path)
        try:
            assert check.execute("SELECT COUNT(*) FROM project_turns WHERE project_id = ? AND status = 'claimed'", (project_id,)).fetchone()[0] == 1
            assert check.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'turn.claimed'", (project_id,)).fetchone()[0] == 1
            assert check.execute("SELECT COUNT(*) FROM project_worker_leases WHERE project_id = ?", (project_id,)).fetchone()[0] == 1
        finally:
            check.close()


def test_stop_request_survives_reopen_and_blocks_later_fifo_work(tmp_path):
    module = importlib.import_module("hermes_cli.project_runtime")
    path = tmp_path / "stop-crash.db"
    conn = projects_db.connect(path)
    project_id, owner = _adopt_bound_project(
        conn, name="Stop crash", root="stop-root", binding="stop-binding", external="stop-window"
    )
    runtime = module.ProjectRuntime(conn, clock=lambda: 100)
    first = runtime.enqueue_turn(project_id, {"message": "first"}, owner, idempotency_key="first", expected_version=0)
    runtime.enqueue_turn(project_id, {"message": "later"}, owner, idempotency_key="later", expected_version=1)
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(project_id, first.turn_id, owner, idempotency_key="stop", expected_version=3, expected_control_version=1)
    conn.close()

    reopened = projects_db.connect(path)
    try:
        state = prdb.runtime_turn_for_project(reopened, project_id=project_id, turn_id=first.turn_id)
        control = prdb.runtime_control_for_turn(reopened, project_id=project_id, turn_id=first.turn_id)
        assert state.status == "stop_requested"
        assert control.control_state == "stop_requested"
        assert reopened.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'run.stop_requested'", (project_id,)).fetchone()[0] == 1
        assert reopened.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'run.stopped'", (project_id,)).fetchone()[0] == 0
        assert module.ProjectRuntime(reopened, clock=lambda: 101).claim_next_turn(project_id, "after-crash", lease_seconds=30) is None
    finally:
        reopened.close()
