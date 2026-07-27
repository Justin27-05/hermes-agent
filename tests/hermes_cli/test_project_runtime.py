"""Behavioral contract tests for the durable per-project FIFO runtime."""

from __future__ import annotations

import importlib
import inspect
import math
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import fields, replace
from typing import Literal, Mapping, get_type_hints

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import ActorContext


def _event_count(conn, project_id):
    return conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?", (project_id,)
    ).fetchone()[0]


def _runtime_mutation_snapshot(conn, project_id, turn_id):
    return (
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_approvals
                WHERE project_id = ?
                ORDER BY approval_id
                """,
                (project_id,),
            )
        ),
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?", (turn_id,)
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ?
                ORDER BY sequence
                """,
                (project_id,),
            )
        ),
    )


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


def _approval_request(
    project_id,
    *,
    approval_id="approval",
    expected_runtime_version=2,
    expected_lifecycle="active",
    expires_at=1000,
):
    return prdb.ApprovalRequest(
        approval_id=approval_id,
        project_id=project_id,
        requester_actor_id="owner-1",
        authorization_actor_id="owner-1",
        canonical_action="publish",
        approval_class="publish",
        command_revision=1,
        expected_runtime_version=expected_runtime_version,
        expected_lifecycle=expected_lifecycle,
        expected_phase="implementation",
        targets=("C:/work/runtime/release",),
        batch_id="batch",
        batch_items=("release",),
        status="pending",
        expires_at=expires_at,
    )


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


def test_public_values_and_control_method_signatures_match_the_task4_contract():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert module.JSONScalar == str | int | float | bool | None
    assert module.JSONValue == (
        module.JSONScalar
        | tuple["JSONValue", ...]
        | Mapping[str, "JSONValue"]
    )
    assert module.TurnStatus == Literal[
        "queued",
        "claimed",
        "awaiting_approval",
        "stop_requested",
        "stopped",
        "reconciling",
        "succeeded",
        "failed",
        "cancelled",
    ]
    assert module.ControlState == Literal[
        "running",
        "stop_requested",
        "stopped",
        "resume_requested",
        "terminal",
    ]
    assert module.ApprovalRequest is prdb.ApprovalRequest
    assert tuple(field.name for field in fields(module.ProjectTurn)) == (
        "turn_id", "project_id", "sequence", "idempotency_key", "payload",
        "origin_binding_id", "status", "attempt_id", "lease_generation",
        "fencing_token", "created_at", "updated_at",
    )
    assert tuple(field.name for field in fields(module.RunControl)) == (
        "turn_id", "project_id", "control_state", "control_version",
        "last_idempotency_key", "attempt_id", "updated_at",
    )
    assert tuple(field.name for field in fields(module.TurnClaim)) == (
        "turn_id", "project_id", "sequence", "worker_id", "attempt_id",
        "lease_generation", "fencing_token", "lease_expires_at",
        "canonical_session_id",
    )
    assert tuple(field.name for field in fields(module.TurnApproval)) == (
        "turn_id", "approval",
    )
    assert tuple(inspect.signature(module.ProjectRuntime.cancel_queued_turn).parameters) == (
        "self", "project_id", "turn_id", "actor", "idempotency_key",
        "expected_version", "expected_control_version",
    )
    assert tuple(inspect.signature(module.ProjectRuntime.request_turn_approval).parameters) == (
        "self", "turn_id", "request", "actor", "expected_control_version",
    )
    turn_hints = get_type_hints(module.ProjectTurn)
    control_hints = get_type_hints(module.RunControl)
    approval_hints = get_type_hints(module.TurnApproval)
    request_hints = get_type_hints(module.ProjectRuntime.request_turn_approval)
    assert (
        module.ProjectTurn.__annotations__["payload"]
        == "Mapping[str, JSONValue]"
    )
    assert turn_hints["status"] == module.TurnStatus
    assert control_hints["control_state"] == module.ControlState
    assert approval_hints["approval"] is module.ApprovalRequest
    assert request_hints["request"] is module.ApprovalRequest
    assert request_hints["return"] is module.TurnApproval


def test_claim_and_controls_expose_exact_creation_and_lease_identity(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "identity"}, runtime_env["desktop"],
        idempotency_key="identity", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    stopped = runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )

    assert (turn.created_at, turn.updated_at) == (100, 100)
    assert claim.lease_expires_at == 130
    assert stopped.last_idempotency_key == "stop"
    assert stopped.updated_at == 100


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
    control = prdb._runtime_control_for_turn(
        runtime_env["conn"], project_id=runtime_env["project_id"], turn_id=turn.turn_id
    )
    assert control.control_state == "running"
    assert control.control_version == 0
    assert turn.created_at == 100
    assert turn.updated_at == 100


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
        runtime.cancel_queued_turn(
            project_id,
            first.turn_id,
            runtime_env["desktop"],
            idempotency_key="cancel-first",
            expected_version=3,
            expected_control_version=1,
        )
    assert not_queued.value.code is runtime_env["module"].RuntimeErrorCode.TURN_NOT_QUEUED
    cancelled = runtime.cancel_queued_turn(
        project_id,
        second.turn_id,
        runtime_env["desktop"],
        idempotency_key="cancel-second",
        expected_version=3,
        expected_control_version=0,
    )
    assert cancelled.status == "cancelled"
    assert prdb._runtime_control_for_turn(runtime_env["conn"], project_id=project_id, turn_id=second.turn_id).control_state == "terminal"


def test_cancel_is_exactly_replayable_and_binds_both_cas_versions(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "cancel"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )

    first = runtime.cancel_queued_turn(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="cancel", expected_version=1,
        expected_control_version=0,
    )
    snapshot = (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE project_id = ? AND turn_id = ?",
            (project_id, turn.turn_id),
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE project_id = ? AND turn_id = ?",
            (project_id, turn.turn_id),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )
    replay = runtime.cancel_queued_turn(
        project_id, turn.turn_id, runtime_env["discord"],
        idempotency_key="cancel", expected_version=1,
        expected_control_version=0,
    )

    assert replay == first
    assert snapshot == (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE project_id = ? AND turn_id = ?",
            (project_id, turn.turn_id),
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE project_id = ? AND turn_id = ?",
            (project_id, turn.turn_id),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as conflict:
        runtime.cancel_queued_turn(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="cancel", expected_version=2,
            expected_control_version=1,
        )
    assert conflict.value.code is runtime_env["module"].RuntimeErrorCode.IDEMPOTENCY_CONFLICT


def test_cancel_rejects_control_split_brain_without_mutation(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "cancel"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    conn.execute(
        """UPDATE project_run_controls SET control_state = 'stopped'
           WHERE project_id = ? AND turn_id = ?""",
        (project_id, turn.turn_id),
    )
    conn.commit()
    before = (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises((runtime_env["module"].ProjectRuntimeError, RuntimeError)):
        runtime.cancel_queued_turn(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="cancel", expected_version=1,
            expected_control_version=0,
        )

    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "queued"
    assert before == (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


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
    same_turn = prdb._runtime_turn_for_project(
        runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id
    )
    assert same_turn.sequence == turn.sequence and same_turn.attempt_id == claim.attempt_id
    renewed = runtime.claim_next_turn(project_id, "worker-two", lease_seconds=30)
    assert renewed.turn_id == turn.turn_id
    assert renewed.attempt_id != claim.attempt_id
    assert renewed.lease_generation == claim.lease_generation + 1
    assert renewed.fencing_token == claim.fencing_token + 1


@pytest.mark.parametrize(
    "corruption_sql",
    [
        pytest.param(
            "DELETE FROM project_worker_leases WHERE project_id = ? AND turn_id = ?",
            id="missing-lease",
        ),
        pytest.param(
            """UPDATE project_run_controls SET attempt_id = 'forged-attempt'
               WHERE project_id = ? AND turn_id = ?""",
            id="control-attempt",
        ),
        pytest.param(
            """UPDATE project_worker_leases SET worker_id = 'forged-worker'
               WHERE project_id = ? AND turn_id = ?""",
            id="lease-worker",
        ),
    ],
)
def test_stop_requires_one_structurally_complete_current_claim(
    runtime_env, corruption_sql
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    conn.execute(corruption_sql, (project_id, turn.turn_id))
    conn.commit()
    before = (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises((runtime_env["module"].ProjectRuntimeError, RuntimeError)):
        runtime.request_stop(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="stop", expected_version=2,
            expected_control_version=1,
        )

    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "claimed"
    assert before == (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


@pytest.mark.parametrize(
    "field_name",
    [
        "sequence",
        "worker_id",
        "canonical_session_id",
        "lease_expires_at",
    ],
)
def test_stopped_acknowledgement_rejects_each_forged_claim_field(
    runtime_env, field_name
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )
    forged_value = (
        "forged"
        if field_name in {"worker_id", "canonical_session_id"}
        else getattr(claim, field_name) + 1
    )
    forged = replace(claim, **{field_name: forged_value})

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.acknowledge_stopped(forged)

    assert (
        rejected.value.code
        is runtime_env["module"].RuntimeErrorCode.TURN_NOT_STOP_REQUESTED
    )
    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "stop_requested"
    assert prdb.runtime_state_for_project(conn, project_id).version == 3
    assert _event_count(conn, project_id) == 3


def test_stopped_acknowledgement_rejects_a_split_brain_control_attempt(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )
    conn.execute(
        """UPDATE project_run_controls SET attempt_id = 'forged-attempt'
           WHERE project_id = ? AND turn_id = ?""",
        (project_id, turn.turn_id),
    )
    conn.commit()

    with pytest.raises(runtime_env["module"].ProjectRuntimeError):
        runtime.acknowledge_stopped(claim)

    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "stop_requested"
    assert prdb.runtime_state_for_project(conn, project_id).version == 3
    assert _event_count(conn, project_id) == 3


def test_stopped_acknowledgement_exact_replay_uses_durable_full_claim_identity(
    runtime_env,
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )
    first = runtime.acknowledge_stopped(claim)
    assert tuple(conn.execute(
        """SELECT claim_worker_id, claim_lease_expires_at,
                  claim_canonical_session_id
           FROM project_run_controls
           WHERE project_id = ? AND turn_id = ?""",
        (project_id, turn.turn_id),
    ).fetchone()) == (
        claim.worker_id,
        claim.lease_expires_at,
        claim.canonical_session_id,
    )
    assert conn.execute(
        """SELECT 1 FROM project_worker_leases
           WHERE project_id = ? AND turn_id = ?""",
        (project_id, turn.turn_id),
    ).fetchone() is None
    snapshot = (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    for field_name in (
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    ):
        value = getattr(claim, field_name)
        forged_value = f"{value}-forged" if type(value) is str else value + 1
        with pytest.raises(runtime_env["module"].ProjectRuntimeError):
            runtime.acknowledge_stopped(
                replace(claim, **{field_name: forged_value})
            )

    replay = runtime.acknowledge_stopped(claim)
    assert replay == first
    assert snapshot == (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


@pytest.mark.parametrize(
    "corruption_sql",
    [
        pytest.param(
            """UPDATE project_turns SET status = 'illegal'
               WHERE project_id = ? AND sequence = 1""",
            id="illegal-oldest-turn",
        ),
        pytest.param(
            """UPDATE project_turns SET status = 'cancelled'
               WHERE project_id = ? AND sequence = 1;
               UPDATE project_run_controls SET control_state = 'illegal'
               WHERE project_id = ? AND turn_id = (
                   SELECT turn_id FROM project_turns
                   WHERE project_id = ? AND sequence = 1
               )""",
            id="illegal-terminal-control",
        ),
    ],
)
def test_claim_fails_closed_on_any_corrupt_row_before_a_later_queued_turn(
    runtime_env, corruption_sql
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    first = runtime.enqueue_turn(
        project_id, {"order": 1}, runtime_env["desktop"],
        idempotency_key="first", expected_version=0,
    )
    second = runtime.enqueue_turn(
        project_id, {"order": 2}, runtime_env["desktop"],
        idempotency_key="second", expected_version=1,
    )
    if ";" in corruption_sql:
        statements = [part.strip() for part in corruption_sql.split(";")]
        conn.execute(statements[0], (project_id,))
        conn.execute(statements[1], (project_id, project_id))
    else:
        conn.execute(corruption_sql, (project_id,))
    conn.commit()
    before = (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises(RuntimeError):
        runtime.claim_next_turn(project_id, "worker", lease_seconds=30)

    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (second.turn_id,)
    ).fetchone()[0] == "queued"
    assert conn.execute(
        """SELECT COUNT(*) FROM project_worker_leases
           WHERE project_id = ?""",
        (project_id,),
    ).fetchone()[0] == 0
    assert before == (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )
    assert first.turn_id != second.turn_id


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed", "cancelled"])
def test_resume_rejects_every_terminal_status_without_mutation(
    runtime_env, terminal_status
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "terminal"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    conn.execute(
        "UPDATE project_turns SET status = ? WHERE turn_id = ?",
        (terminal_status, turn.turn_id),
    )
    conn.execute(
        """UPDATE project_run_controls SET control_state = 'terminal'
           WHERE turn_id = ?""",
        (turn.turn_id,),
    )
    conn.commit()
    before = (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.request_resume(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="resume", expected_version=1,
            expected_control_version=0,
        )

    assert rejected.value.code in {
        runtime_env["module"].RuntimeErrorCode.TURN_TERMINAL,
        runtime_env["module"].RuntimeErrorCode.TURN_NOT_RESUMABLE,
    }
    assert before == (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


@pytest.mark.parametrize(
    "inactive_lifecycle",
    ["awaiting_acceptance", "completed", "archived"],
)
def test_resume_requires_an_active_project_lifecycle_without_mutation(
    runtime_env, inactive_lifecycle
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )
    runtime.acknowledge_stopped(claim)
    conn.execute("PRAGMA ignore_check_constraints=ON")
    conn.execute(
        """UPDATE project_runtime_state
           SET lifecycle = ?, version = version + 1, updated_at = 101
           WHERE project_id = ?""",
        (inactive_lifecycle, project_id),
    )
    conn.execute("PRAGMA ignore_check_constraints=OFF")
    conn.commit()
    before = (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.request_resume(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="resume", expected_version=5,
            expected_control_version=3,
        )

    assert rejected.value.code in {
        runtime_env["module"].RuntimeErrorCode.PROJECT_NOT_ACTIVE,
        runtime_env["module"].RuntimeErrorCode.PROJECT_LINEAGE_INVALID,
    }
    assert before == (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


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
    result = runtime.request_turn_approval(
        turn.turn_id,
        request,
        runtime_env["desktop"],
        expected_control_version=1,
    )
    assert result.turn_id == turn.turn_id
    assert result.approval.approval_id == request.approval_id
    assert result.approval.targets == ("c:/work/runtime/release",)
    assert prdb._runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).status == "awaiting_approval"
    assert runtime.claim_next_turn(project_id, "other-worker", lease_seconds=30) is None
    assert runtime_env["conn"].execute("SELECT turn_id FROM project_approvals WHERE approval_id = 'approval-1'").fetchone()[0] == turn.turn_id
    assert runtime_env["conn"].in_transaction is False


def test_turn_approval_keeps_preversion_identity_and_is_immediately_resolvable_and_consumable(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = _approval_request(project_id)

    result = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["desktop"],
        expected_control_version=1,
    )
    snapshot = conn.execute(
        """SELECT expected_runtime_version, effective_runtime_version,
                  turn_expected_control_version
           FROM project_approvals WHERE approval_id = ?""",
        (request.approval_id,),
    ).fetchone()

    assert result.approval.expected_runtime_version == 2
    assert tuple(snapshot) == (2, 3, 1)
    assert prdb.runtime_state_for_project(conn, project_id).version == 3
    resolved = prdb.resolve_approval(
        conn, approval_id=request.approval_id,
        resolver=runtime_env["desktop"], outcome="approved", now=101,
    )
    assert resolved is not None and resolved.status == "approved"
    assert prdb.consume_approval_authorization(
        conn,
        approval_id=request.approval_id,
        project_id=project_id,
        authorization_actor_id="owner-1",
        canonical_action="publish",
        approval_class="publish",
        command_revision=1,
        expected_runtime_version=2,
        expected_lifecycle="active",
        expected_phase="implementation",
        targets=("C:/work/runtime/release",),
        batch_id="batch",
        batch_items=("release",),
        now=102,
    )


def test_exact_turn_approval_replays_after_lifecycle_drift_resolution_and_expiry_without_writes(runtime_env):
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    now = [100]
    runtime = runtime_env["module"].ProjectRuntime(conn, clock=lambda: now[0])
    turn = runtime.enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = _approval_request(project_id)
    first = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["desktop"],
        expected_control_version=1,
    )
    resolved = prdb.resolve_approval(
        conn, approval_id=request.approval_id,
        resolver=runtime_env["desktop"], outcome="approved", now=101,
    )
    assert resolved is not None
    drifted = prdb.transition_lifecycle(
        conn,
        project_id=project_id,
        expected_version=3,
        lifecycle="awaiting_acceptance",
        updated_at=102,
    )
    assert drifted is not None
    conn.commit()
    before = _runtime_mutation_snapshot(conn, project_id, turn.turn_id)
    now[0] = 2000

    replay = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["discord"],
        expected_control_version=1,
    )

    assert first.turn_id == replay.turn_id
    assert replay.approval.status == "approved"
    assert before == _runtime_mutation_snapshot(conn, project_id, turn.turn_id)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as changed_cas:
        runtime.request_turn_approval(
            turn.turn_id,
            request,
            runtime_env["desktop"],
            expected_control_version=99,
        )
    assert (
        changed_cas.value.code
        is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT
    )
    assert before == _runtime_mutation_snapshot(conn, project_id, turn.turn_id)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as changed:
        runtime.request_turn_approval(
            turn.turn_id,
            replace(request, batch_id="changed"),
            runtime_env["desktop"],
            expected_control_version=1,
        )
    assert changed.value.code is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as wrong_turn:
        runtime.request_turn_approval(
            "another-turn", request, runtime_env["desktop"],
            expected_control_version=1,
        )
    assert wrong_turn.value.code is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT


@pytest.mark.parametrize(
    ("lifecycle", "transitions"),
    [
        pytest.param(
            "awaiting_acceptance",
            ("awaiting_acceptance",),
            id="awaiting-acceptance",
        ),
        pytest.param(
            "completed",
            ("awaiting_acceptance", "completed"),
            id="completed",
        ),
    ],
)
def test_fresh_turn_approval_requires_an_active_project_without_writes(
    runtime_env, lifecycle, transitions
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "approve"},
        runtime_env["desktop"],
        idempotency_key="enqueue",
        expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    state = prdb.runtime_state_for_project(conn, project_id)
    assert state is not None
    for target in transitions:
        state = prdb.transition_lifecycle(
            conn,
            project_id=project_id,
            expected_version=state.version,
            lifecycle=target,
            updated_at=99,
        )
        assert state is not None
    conn.commit()
    request = _approval_request(
        project_id,
        expected_runtime_version=state.version,
        expected_lifecycle=lifecycle,
    )
    before = _runtime_mutation_snapshot(conn, project_id, turn.turn_id)

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.request_turn_approval(
            turn.turn_id,
            request,
            runtime_env["desktop"],
            expected_control_version=1,
        )

    assert (
        rejected.value.code
        is runtime_env["module"].RuntimeErrorCode.PROJECT_NOT_ACTIVE
    )
    assert before == _runtime_mutation_snapshot(conn, project_id, turn.turn_id)


def test_migrated_linked_turn_approval_without_control_cas_fails_closed(
    runtime_env,
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "approve"},
        runtime_env["desktop"],
        idempotency_key="enqueue",
        expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = _approval_request(project_id)
    runtime.request_turn_approval(
        turn.turn_id,
        request,
        runtime_env["desktop"],
        expected_control_version=1,
    )
    conn.execute(
        """
        UPDATE project_approvals
        SET turn_expected_control_version = NULL
        WHERE approval_id = ?
        """,
        (request.approval_id,),
    )
    conn.commit()
    before = _runtime_mutation_snapshot(conn, project_id, turn.turn_id)

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.request_turn_approval(
            turn.turn_id,
            request,
            runtime_env["discord"],
            expected_control_version=1,
        )

    assert (
        rejected.value.code
        is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT
    )
    assert before == _runtime_mutation_snapshot(conn, project_id, turn.turn_id)


@pytest.mark.parametrize(
    "corruption_sql",
    [
        pytest.param(
            "DELETE FROM project_worker_leases WHERE project_id = ? AND turn_id = ?",
            id="missing-lease",
        ),
        pytest.param(
            """UPDATE project_run_controls SET attempt_id = 'forged-attempt'
               WHERE project_id = ? AND turn_id = ?""",
            id="control-attempt",
        ),
        pytest.param(
            """UPDATE project_worker_leases SET fencing_token = fencing_token + 1
               WHERE project_id = ? AND turn_id = ?""",
            id="lease-fence",
        ),
    ],
)
def test_turn_approval_requires_the_exact_current_claim_triple(
    runtime_env, corruption_sql
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    conn.execute(corruption_sql, (project_id, turn.turn_id))
    conn.commit()

    with pytest.raises((runtime_env["module"].ProjectRuntimeError, RuntimeError)):
        runtime.request_turn_approval(
            turn.turn_id,
            _approval_request(project_id),
            runtime_env["desktop"],
            expected_control_version=1,
        )

    assert conn.execute(
        "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "claimed"
    assert prdb.runtime_state_for_project(conn, project_id).version == 2
    assert _event_count(conn, project_id) == 2


def test_turn_approval_rejects_wrong_preversion_and_control_version(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as wrong_state:
        runtime.request_turn_approval(
            turn.turn_id,
            _approval_request(project_id, expected_runtime_version=1),
            runtime_env["desktop"],
            expected_control_version=1,
        )
    assert wrong_state.value.code is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as wrong_control:
        runtime.request_turn_approval(
            turn.turn_id,
            _approval_request(project_id),
            runtime_env["desktop"],
            expected_control_version=0,
        )
    assert wrong_control.value.code is runtime_env["module"].RuntimeErrorCode.CONTROL_VERSION_CONFLICT
    assert conn.execute(
        "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == 0


def test_turn_approval_event_conflict_rolls_back_approval_link_and_version(runtime_env):
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime_env["runtime"].enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime_env["runtime"].claim_next_turn(project_id, "worker", lease_seconds=30)
    conn.execute(
        """INSERT INTO project_events (
            event_id, project_id, sequence, kind, turn_id, payload_json, created_at
        ) VALUES ('approval-event-conflict', ?, 3, 'prior', NULL, '{}', 99)""",
        (project_id,),
    )
    conn.commit()
    runtime = runtime_env["module"].ProjectRuntime(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: (
            "approval-event-conflict" if kind == "event" else f"{kind}-fixed"
        ),
    )

    with pytest.raises(sqlite3.IntegrityError):
        runtime.request_turn_approval(
            turn.turn_id,
            _approval_request(project_id),
            runtime_env["desktop"],
            expected_control_version=1,
        )

    assert conn.execute(
        "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "claimed"
    assert prdb.runtime_state_for_project(conn, project_id).version == 2


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
        attempt_id=prdb._runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).attempt_id,
        worker_id="worker",
        lease_expires_at=130,
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
    first = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["desktop"], expected_control_version=1
    )
    replay = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["discord"], expected_control_version=1
    )
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
        lease_expires_at=claim.lease_expires_at,
    )
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.acknowledge_stopped(forged)
    assert rejected.value.code is runtime_env["module"].RuntimeErrorCode.TURN_NOT_STOP_REQUESTED
    assert prdb._runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).status == "stop_requested"


def test_two_file_backed_connections_claim_one_oldest_turn_in_25_races(tmp_path):
    module = importlib.import_module("hermes_cli.project_runtime")
    path = tmp_path / "race.db"

    def persisted_project_snapshot(conn, project_id):
        queries = (
            """SELECT * FROM project_runtime_state
               WHERE project_id = ? ORDER BY project_id""",
            """SELECT * FROM project_turns
               WHERE project_id = ? ORDER BY sequence, turn_id""",
            """SELECT * FROM project_run_controls
               WHERE project_id = ? ORDER BY turn_id""",
            """SELECT * FROM project_events
               WHERE project_id = ? ORDER BY sequence, event_id""",
            """SELECT * FROM project_worker_leases
               WHERE project_id = ? ORDER BY turn_id, lease_id""",
        )
        return tuple(
            tuple(tuple(row) for row in conn.execute(query, (project_id,)))
            for query in queries
        )

    unrelated_conn = projects_db.connect(path)
    unrelated_id, unrelated_owner = _adopt_bound_project(
        unrelated_conn,
        name="Unrelated race isolation",
        root="unrelated-root",
        binding="unrelated-binding",
        external="unrelated-window",
    )
    module.ProjectRuntime(unrelated_conn, clock=lambda: 50).enqueue_turn(
        unrelated_id,
        {"scope": "must-not-change"},
        unrelated_owner,
        idempotency_key="unrelated-turn",
        expected_version=0,
    )
    unrelated_baseline = persisted_project_snapshot(
        unrelated_conn, unrelated_id
    )
    unrelated_conn.close()

    for index in range(25):
        bootstrap = projects_db.connect(path)
        project_id = projects_db.create_project(bootstrap, name=f"Race {index}")
        prdb.create_project_conversation(bootstrap, project_id=project_id, conversation_id=f"root-{index}", current_phase="implementation", now=1)
        prdb.bind_surface(bootstrap, binding_id=f"binding-{index}", project_id=project_id, surface="desktop", external_binding_id=f"window-{index}", actor_id="owner", now=1)
        owner = ActorContext("owner", "desktop", f"binding-{index}", True)
        service = module.ProjectRuntime(bootstrap, clock=lambda: 100)
        first = service.enqueue_turn(project_id, {"n": 1}, owner, idempotency_key=f"first-{index}", expected_version=0)
        second = service.enqueue_turn(project_id, {"n": 2}, owner, idempotency_key=f"second-{index}", expected_version=1)
        unrelated_before = persisted_project_snapshot(bootstrap, unrelated_id)
        assert unrelated_before == unrelated_baseline
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
        assert (
            len(winners) == 1
            and winners[0].turn_id == first.turn_id
            and winners[0].sequence == 1
        )
        check = projects_db.connect(path)
        try:
            assert check.execute("SELECT COUNT(*) FROM project_turns WHERE project_id = ? AND status = 'claimed'", (project_id,)).fetchone()[0] == 1
            assert check.execute(
                """SELECT status FROM project_turns
                   WHERE project_id = ? AND turn_id = ?""",
                (project_id, second.turn_id),
            ).fetchone()[0] == "queued"
            assert check.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'turn.claimed'", (project_id,)).fetchone()[0] == 1
            assert check.execute("SELECT COUNT(*) FROM project_worker_leases WHERE project_id = ?", (project_id,)).fetchone()[0] == 1
            assert (
                persisted_project_snapshot(check, unrelated_id)
                == unrelated_before
            )
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
        state = prdb._runtime_turn_for_project(reopened, project_id=project_id, turn_id=first.turn_id)
        control = prdb._runtime_control_for_turn(reopened, project_id=project_id, turn_id=first.turn_id)
        assert state.status == "stop_requested"
        assert control.control_state == "stop_requested"
        assert reopened.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'run.stop_requested'", (project_id,)).fetchone()[0] == 1
        assert reopened.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'run.stopped'", (project_id,)).fetchone()[0] == 0
        assert module.ProjectRuntime(reopened, clock=lambda: 101).claim_next_turn(project_id, "after-crash", lease_seconds=30) is None
    finally:
        reopened.close()
