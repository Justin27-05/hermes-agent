"""Task 5 contract and recovery tests for the durable project runtime."""

from __future__ import annotations

import importlib
import inspect
import json
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import fields, replace
from pathlib import Path
from typing import Literal, Protocol, get_type_hints

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import ActorContext


def _make_runtime(path, *, now=100, clock=None):
    conn = projects_db.connect(path)
    project_id = projects_db.create_project(conn, name="Recovery")
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="session-root",
        current_phase="implementation",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="owner-binding",
        project_id=project_id,
        surface="desktop",
        external_binding_id=f"window-{project_id}",
        actor_id="owner",
        now=1,
    )
    module = importlib.import_module("hermes_cli.project_runtime")
    runtime = module.ProjectRuntime(conn, clock=clock or (lambda: now))
    actor = ActorContext("owner", "desktop", "owner-binding", True)
    return module, conn, runtime, project_id, actor


def _claim_snapshot(conn, project_id, turn_id):
    return (
        tuple(
            conn.execute(
                """
                SELECT * FROM project_turns
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        ),
        tuple(
            conn.execute(
                """
                SELECT * FROM project_run_controls
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        ),
        tuple(
            conn.execute(
                """
                SELECT * FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        )
        if conn.execute(
            """
            SELECT 1 FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn_id),
        ).fetchone()
        is not None
        else None,
        prdb.runtime_state_for_project(conn, project_id),
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? ORDER BY sequence
                """,
                (project_id,),
            )
        ),
    )


def _enqueue_and_claim(runtime, project_id, actor, *, key="turn"):
    turn = runtime.enqueue_turn(
        project_id,
        {"message": key},
        actor,
        idempotency_key=key,
        expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    assert claim is not None
    return turn, claim


def _communicate_probe(process, *, timeout=15):
    try:
        return process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
        raise


class _RecordingReadback:
    def __init__(self, conn, result=None, *, error=None, barrier=None):
        self.conn = conn
        self.result = result
        self.error = error
        self.barrier = barrier
        self.calls = []

    def read_turn(self, request):
        assert self.conn.in_transaction is False
        self.calls.append(request)
        if self.barrier is not None:
            self.barrier.wait(timeout=5)
        if self.error is not None:
            raise self.error
        return self.result


def _legacy_task4_turn_database(path):
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO projects VALUES ('legacy', 'legacy', 'Legacy', 1, 0);
        CREATE TABLE project_turns (
            turn_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL
                REFERENCES projects(id) ON DELETE RESTRICT,
            sequence INTEGER NOT NULL,
            idempotency_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            origin_binding_id TEXT,
            status TEXT NOT NULL,
            attempt_id TEXT,
            lease_generation INTEGER NOT NULL DEFAULT 0,
            fencing_token INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE (project_id, turn_id),
            UNIQUE (project_id, sequence),
            UNIQUE (project_id, idempotency_key)
        );
        INSERT INTO project_turns VALUES
            ('queued', 'legacy', 1, 'q', '{}', NULL, 'queued',
             NULL, 0, 0, 1, 1),
            ('claimed', 'legacy', 2, 'c', '{}', NULL, 'claimed',
             'attempt-c', 1, 1, 2, 2),
            ('stopped', 'legacy', 3, 's', '{}', NULL, 'stopped',
             'attempt-s', 2, 2, 3, 3),
            ('terminal', 'legacy', 4, 't', '{}', NULL, 'succeeded',
             'attempt-t', 3, 3, 4, 4);
        """
    )
    conn.commit()
    return conn


def test_task4_public_values_remain_exact_and_task9_surface_stays_absent():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert tuple(field.name for field in fields(module.ProjectTurn)) == (
        "turn_id",
        "project_id",
        "sequence",
        "idempotency_key",
        "payload",
        "origin_binding_id",
        "status",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "created_at",
        "updated_at",
    )
    assert tuple(field.name for field in fields(module.RunControl)) == (
        "turn_id",
        "project_id",
        "control_state",
        "control_version",
        "last_idempotency_key",
        "attempt_id",
        "updated_at",
    )
    assert tuple(field.name for field in fields(module.TurnClaim)) == (
        "turn_id",
        "project_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    )
    assert not hasattr(module, "ProjectSnapshot")
    assert not hasattr(module.ProjectRuntime, "execute_command")


def test_task5_aliases_and_frozen_dtos_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert module.TerminalTurnStatus == Literal["succeeded", "failed"]
    assert module.ExecutionState == Literal["not_started", "started"]
    assert module.RecoverySourceStatus == Literal["claimed", "stop_requested"]
    assert module.ReadbackOutcome == Literal[
        "succeeded", "failed", "stopped", "unknown"
    ]
    assert tuple(field.name for field in fields(module.CanonicalTurnResult)) == (
        "status",
        "result_id",
    )
    assert tuple(field.name for field in fields(module.TurnReadbackRequest)) == (
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
        "source_status",
        "execution_state",
    )
    assert tuple(field.name for field in fields(module.TurnReadbackResult)) == (
        "outcome",
        "result_id",
    )
    assert module.CanonicalTurnResult.__dataclass_params__.frozen is True
    assert module.TurnReadbackRequest.__dataclass_params__.frozen is True
    assert module.TurnReadbackResult.__dataclass_params__.frozen is True
    assert get_type_hints(module.CanonicalTurnResult) == {
        "status": module.TerminalTurnStatus,
        "result_id": str,
    }
    assert get_type_hints(module.TurnReadbackResult) == {
        "outcome": module.ReadbackOutcome,
        "result_id": str | None,
    }


def test_task5_readback_protocol_has_one_exact_method():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert issubclass(module.TurnReadbackPort, Protocol)
    assert {
        name
        for name in module.TurnReadbackPort.__dict__
        if not name.startswith("_")
    } == {"read_turn"}
    signature = inspect.signature(module.TurnReadbackPort.read_turn)
    assert tuple(signature.parameters) == ("self", "request")
    assert get_type_hints(module.TurnReadbackPort.read_turn) == {
        "request": module.TurnReadbackRequest,
        "return": module.TurnReadbackResult,
    }


def test_task5_service_signatures_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")
    runtime = module.ProjectRuntime

    expected_parameters = {
        "heartbeat_turn": ("self", "claim", "lease_seconds"),
        "mark_turn_started": ("self", "claim"),
        "commit_turn": ("self", "claim", "result"),
        "reconcile_inflight_turns": ("self", "readback", "limit"),
    }
    expected_hints = {
        "heartbeat_turn": {
            "claim": module.TurnClaim,
            "lease_seconds": int,
            "return": module.TurnClaim,
        },
        "mark_turn_started": {
            "claim": module.TurnClaim,
            "return": module.TurnClaim,
        },
        "commit_turn": {
            "claim": module.TurnClaim,
            "result": module.CanonicalTurnResult,
            "return": module.ProjectTurn,
        },
        "reconcile_inflight_turns": {
            "readback": module.TurnReadbackPort,
            "limit": int,
            "return": tuple[module.ProjectTurn, ...],
        },
    }

    for name, parameters in expected_parameters.items():
        method = getattr(runtime, name)
        signature = inspect.signature(method)
        assert tuple(signature.parameters) == parameters
        assert get_type_hints(method) == expected_hints[name]
    assert (
        inspect.signature(runtime.heartbeat_turn).parameters[
            "lease_seconds"
        ].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    assert (
        inspect.signature(runtime.reconcile_inflight_turns).parameters[
            "limit"
        ].kind
        is inspect.Parameter.KEYWORD_ONLY
    )


def test_task5_stable_error_codes_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert module.RuntimeErrorCode.STALE_TURN_CLAIM.value == "stale_turn_claim"
    assert (
        module.RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED.value
        == "turn_execution_not_started"
    )
    assert (
        module.RuntimeErrorCode.TERMINAL_RESULT_CONFLICT.value
        == "terminal_result_conflict"
    )


def test_fresh_task5_turn_schema_enforces_metadata_and_indexes(tmp_path):
    conn = projects_db.connect(tmp_path / "fresh.db")
    try:
        columns = {
            row["name"]: row
            for row in conn.execute("PRAGMA table_info(project_turns)")
        }
        indexes = {
            row["name"]: row
            for row in conn.execute("PRAGMA index_list(project_turns)")
        }
        lease_indexes = {
            row["name"]
            for row in conn.execute("PRAGMA index_list(project_worker_leases)")
        }

        assert columns["execution_state"]["notnull"] == 0
        assert columns["terminal_result_id"]["notnull"] == 0
        assert indexes["idx_project_turns_terminal_result"]["unique"] == 1
        assert "idx_project_turns_project_sequence" in indexes
        assert "idx_project_worker_leases_expiry" in lease_indexes

        project_id = projects_db.create_project(conn, name="Schema checks")
        common = (
            project_id,
            "{}",
            "claimed",
            "attempt",
            1,
            1,
            1,
            1,
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, created_at, updated_at, execution_state
                ) VALUES ('bad-execution', ?, 1, 'bad-execution', ?, ?, ?, ?,
                          ?, ?, ?, 'unknown')
                """,
                common,
            )
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, created_at, updated_at, terminal_result_id
                ) VALUES ('bad-result', ?, 1, 'bad-result', ?, ?, ?, ?, ?, ?,
                          ?, '')
                """,
                common,
            )
    finally:
        conn.close()


def test_task4_turn_rows_migrate_additively_without_backfill_or_events(tmp_path):
    conn = _legacy_task4_turn_database(tmp_path / "legacy.db")
    old_columns = (
        "turn_id",
        "project_id",
        "sequence",
        "idempotency_key",
        "payload_json",
        "origin_binding_id",
        "status",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "created_at",
        "updated_at",
    )
    before = tuple(
        tuple(row)
        for row in conn.execute(
            f"SELECT {', '.join(old_columns)} FROM project_turns ORDER BY sequence"
        )
    )

    prdb.ensure_schema(conn)
    prdb.ensure_schema(conn)

    after = tuple(
        tuple(row)
        for row in conn.execute(
            f"SELECT {', '.join(old_columns)} FROM project_turns ORDER BY sequence"
        )
    )
    task5_values = tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT execution_state, terminal_result_id
            FROM project_turns ORDER BY sequence
            """
        )
    )
    assert after == before
    assert task5_values == ((None, None),) * 4
    assert conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE kind = 'turn.recovery_blocked'"
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.parametrize(
    ("execution_state", "terminal_result_id"),
    [
        pytest.param("unknown", None, id="execution-enum"),
        pytest.param(None, "", id="empty-terminal-result"),
    ],
)
def test_task5_turn_mapper_fails_closed_on_malformed_metadata(
    execution_state, terminal_result_id
):
    row = {
        "turn_id": "turn",
        "project_id": "project",
        "sequence": 1,
        "idempotency_key": "key",
        "payload_json": "{}",
        "origin_binding_id": "binding",
        "status": "queued",
        "attempt_id": None,
        "lease_generation": 0,
        "fencing_token": 0,
        "created_at": 1,
        "updated_at": 1,
        "execution_state": execution_state,
        "terminal_result_id": terminal_result_id,
    }

    with pytest.raises(RuntimeError):
        prdb.runtime_turn_from_row(row)


def test_new_claim_persists_not_started_in_the_atomic_claim(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(tmp_path / "claim.db")
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "claim"},
            actor,
            idempotency_key="claim",
            expected_version=0,
        )
        claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)

        assert claim is not None
        row = conn.execute(
            """
            SELECT execution_state, terminal_result_id
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()
        assert tuple(row) == ("not_started", None)
    finally:
        conn.close()


def test_two_connections_racing_task5_migration_converge(tmp_path):
    path = tmp_path / "migration-race.db"
    _legacy_task4_turn_database(path).close()
    barrier = threading.Barrier(2)

    def migrate():
        conn = sqlite3.connect(path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            barrier.wait(timeout=5)
            prdb.ensure_schema(conn)
            conn.commit()
            return {
                row["name"]
                for row in conn.execute("PRAGMA table_info(project_turns)")
            }
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(migrate) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]

    assert all(
        {"execution_state", "terminal_result_id"} <= columns
        for columns in results
    )


def test_heartbeat_extends_both_horizons_without_versions_or_events(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 110

        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)

        assert renewed == replace(claim, lease_expires_at=160)
        lease = prdb._current_worker_lease_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        control = prdb._runtime_control_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        assert lease is not None and lease.expires_at == 160
        assert control is not None and control.claim_lease_expires_at == 160
        assert control.control_version == 1
        assert prdb.runtime_state_for_project(conn, project_id).version == 2
        assert len(
            conn.execute(
                "SELECT 1 FROM project_events WHERE project_id = ?",
                (project_id,),
            ).fetchall()
        ) == 2
        assert before[0][:-1] == _claim_snapshot(
            conn, project_id, turn.turn_id
        )[0][:-1]
        assert module.RuntimeErrorCode.STALE_TURN_CLAIM.value == "stale_turn_claim"
    finally:
        conn.close()


def test_heartbeat_lost_response_retry_uses_observed_horizon_and_never_shortens(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat-retry.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = 110
        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)
        first_snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 120

        replay = runtime.heartbeat_turn(claim, lease_seconds=10)

        assert replay == renewed
        assert _claim_snapshot(conn, project_id, turn.turn_id) == first_snapshot
        with pytest.raises(module.ProjectRuntimeError) as greater:
            runtime.heartbeat_turn(
                replace(renewed, lease_expires_at=161),
                lease_seconds=50,
            )
        assert greater.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == first_snapshot
    finally:
        conn.close()


@pytest.mark.parametrize(
    "field_name",
    [
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    ],
)
def test_heartbeat_rejects_each_forged_authority_field_without_writes(
    tmp_path, field_name
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-forged-{field_name}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        value = getattr(claim, field_name)
        forged_value = f"{value}-forged" if type(value) is str else value + 1
        forged = replace(claim, **{field_name: forged_value})
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(forged, lease_seconds=60)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize("lease_seconds", [True, 0, -1, 1.5])
def test_heartbeat_rejects_invalid_ttl_before_writing(tmp_path, lease_seconds):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-invalid-{lease_seconds}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=lease_seconds)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_heartbeat_rejects_sqlite_integer_overflow_without_writes(tmp_path):
    now = (1 << 63) - 2
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat-overflow.db", clock=lambda: now
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "overflow"},
            actor,
            idempotency_key="overflow",
            expected_version=0,
        )
        conn.execute(
            """
            UPDATE project_turns
            SET status = 'claimed', attempt_id = 'attempt', lease_generation = 1,
                fencing_token = 1, execution_state = 'not_started'
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        )
        conn.execute(
            """
            UPDATE project_run_controls
            SET control_state = 'running', control_version = 1,
                attempt_id = 'attempt', claim_worker_id = 'worker',
                claim_lease_expires_at = ?,
                claim_canonical_session_id = 'session-root'
            WHERE project_id = ? AND turn_id = ?
            """,
            ((1 << 63) - 1, project_id, turn.turn_id),
        )
        conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id, lease_generation,
                fencing_token, expires_at, updated_at
            ) VALUES ('attempt', ?, ?, 'worker', 1, 1, ?, 1)
            """,
            (project_id, turn.turn_id, (1 << 63) - 1),
        )
        conn.commit()
        claim = module.TurnClaim(
            turn.turn_id,
            project_id,
            turn.sequence,
            "worker",
            "attempt",
            1,
            1,
            (1 << 63) - 1,
            "session-root",
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=2)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("turn_status", "control_state"),
    [
        pytest.param("stop_requested", "stop_requested", id="stop-requested"),
        pytest.param("awaiting_approval", "running", id="awaiting-approval"),
    ],
)
def test_heartbeat_is_allowed_for_the_exact_live_task5_status_set(
    tmp_path, turn_status, control_state
):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-{turn_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        conn.execute(
            "UPDATE project_turns SET status = ? WHERE turn_id = ?",
            (turn_status, turn.turn_id),
        )
        conn.execute(
            """
            UPDATE project_run_controls SET control_state = ?
            WHERE turn_id = ?
            """,
            (control_state, turn.turn_id),
        )
        conn.commit()

        renewed = runtime.heartbeat_turn(claim, lease_seconds=60)

        assert renewed.lease_expires_at == 160
    finally:
        conn.close()


@pytest.mark.parametrize(
    "condition",
    ["expired", "reconciling", "legacy", "inactive"],
)
def test_heartbeat_rejects_expired_or_nonlive_claim_state_without_writes(
    tmp_path, condition
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-{condition}.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if condition == "expired":
            now[0] = claim.lease_expires_at
        elif condition == "reconciling":
            conn.execute(
                "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "legacy":
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
        else:
            state = prdb.runtime_state_for_project(conn, project_id)
            prdb.transition_lifecycle(
                conn,
                project_id=project_id,
                expected_version=state.version,
                lifecycle="awaiting_acceptance",
                updated_at=101,
            )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=60)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_mark_started_is_exact_idempotent_and_metadata_only(tmp_path):
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "started.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 101

        first = runtime.mark_turn_started(claim)
        after = _claim_snapshot(conn, project_id, turn.turn_id)
        replay = runtime.mark_turn_started(claim)

        assert first == replay == claim
        assert conn.execute(
            "SELECT execution_state FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "started"
        assert after == _claim_snapshot(conn, project_id, turn.turn_id)
        assert after[1][3] == before[1][3]
        assert after[3].version == before[3].version
        assert after[4] == before[4]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "condition",
    ["expired", "awaiting_approval", "stopped", "reconciling", "legacy"],
)
def test_mark_started_rejects_every_nonlive_execution_state_without_writes(
    tmp_path, condition
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"started-{condition}.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if condition == "expired":
            now[0] = claim.lease_expires_at
        elif condition == "awaiting_approval":
            conn.execute(
                "UPDATE project_turns SET status = 'awaiting_approval' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "stopped":
            conn.execute(
                "UPDATE project_turns SET status = 'stopped' WHERE turn_id = ?",
                (turn.turn_id,),
            )
            conn.execute(
                "UPDATE project_run_controls SET control_state = 'stopped' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "reconciling":
            conn.execute(
                "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        else:
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.mark_turn_started(claim)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_expired_worker_cannot_request_approval_or_acknowledge_stop(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "existing-expiry-gaps.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        request = prdb.ApprovalRequest(
            approval_id="expired-approval",
            project_id=project_id,
            requester_actor_id="owner",
            authorization_actor_id="owner",
            canonical_action="publish",
            approval_class="publish",
            command_revision=1,
            expected_runtime_version=2,
            expected_lifecycle="active",
            expected_phase="implementation",
            targets=("C:/work/release",),
            batch_id="batch",
            batch_items=("release",),
            status="pending",
            expires_at=1000,
        )
        before_approval = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError):
            runtime.request_turn_approval(
                turn.turn_id,
                request,
                actor,
                expected_control_version=1,
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before_approval
        assert conn.execute(
            "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
            (project_id,),
        ).fetchone()[0] == 0

        stopped = runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop-at-expiry",
            expected_version=2,
            expected_control_version=1,
        )
        assert stopped.control_state == "stop_requested"
        before_ack = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError):
            runtime.acknowledge_stopped(claim)

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before_ack
    finally:
        conn.close()


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
def test_commit_started_turn_is_one_atomic_terminal_transition(
    tmp_path, terminal_status
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-{terminal_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        result = module.CanonicalTurnResult(
            status=terminal_status,
            result_id=f"result-{terminal_status}",
        )

        committed = runtime.commit_turn(claim, result)

        assert committed.status == terminal_status
        stored = conn.execute(
            """
            SELECT status, execution_state, terminal_result_id
            FROM project_turns WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()
        control = prdb._runtime_control_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        event = conn.execute(
            """
            SELECT kind, payload_json FROM project_events
            WHERE project_id = ? ORDER BY sequence DESC LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        assert tuple(stored) == (
            terminal_status,
            "started",
            f"result-{terminal_status}",
        )
        assert control.control_state == "terminal"
        assert control.control_version == before[1][3] + 1
        assert prdb.runtime_state_for_project(conn, project_id).version == (
            before[3].version + 1
        )
        assert conn.execute(
            """
            SELECT 1 FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone() is None
        assert event["kind"] == f"turn.{terminal_status}"
        assert f"result-{terminal_status}" not in event["payload_json"]
    finally:
        conn.close()


def test_commit_exact_replay_is_write_free_and_conflicts_on_changed_result(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-replay.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        result = module.CanonicalTurnResult("succeeded", "result-1")
        first = runtime.commit_turn(claim, result)
        snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 10_000

        replay = runtime.commit_turn(claim, result)

        assert replay == first
        assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
        for changed in (
            module.CanonicalTurnResult("failed", "result-1"),
            module.CanonicalTurnResult("succeeded", "result-2"),
        ):
            with pytest.raises(module.ProjectRuntimeError) as conflict:
                runtime.commit_turn(claim, changed)
            assert (
                conflict.value.code
                is module.RuntimeErrorCode.TERMINAL_RESULT_CONFLICT
            )
            assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
    finally:
        conn.close()


def test_commit_before_start_has_a_distinct_write_free_error(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-before-start.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                claim, module.CanonicalTurnResult("succeeded", "result")
            )

        assert (
            rejected.value.code
            is module.RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED
        )
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "result_factory",
    [
        pytest.param(
            lambda module: module.CanonicalTurnResult("unknown", "result"),
            id="status",
        ),
        pytest.param(
            lambda module: module.CanonicalTurnResult("succeeded", ""),
            id="empty-result",
        ),
        pytest.param(lambda module: {"status": "succeeded"}, id="mapping"),
    ],
)
def test_commit_rejects_malformed_results_without_writes(tmp_path, result_factory):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-invalid-result.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(claim, result_factory(module))

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "field_name",
    [
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    ],
)
def test_commit_rejects_each_forged_claim_field_without_writes(
    tmp_path, field_name
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-forged-{field_name}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        value = getattr(claim, field_name)
        forged_value = f"{value}-forged" if type(value) is str else value + 1
        forged = replace(claim, **{field_name: forged_value})
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                forged, module.CanonicalTurnResult("succeeded", "result")
            )

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("turn_status", "control_state"),
    [
        pytest.param("stop_requested", "stop_requested", id="stop-requested"),
        pytest.param("awaiting_approval", "running", id="approval"),
        pytest.param("reconciling", "running", id="reconciling"),
    ],
)
def test_commit_rejects_nonclaimed_statuses_without_writes(
    tmp_path, turn_status, control_state
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-{turn_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        conn.execute(
            "UPDATE project_turns SET status = ? WHERE turn_id = ?",
            (turn_status, turn.turn_id),
        )
        conn.execute(
            "UPDATE project_run_controls SET control_state = ? WHERE turn_id = ?",
            (control_state, turn.turn_id),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                claim, module.CanonicalTurnResult("succeeded", "result")
            )

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_commit_at_expiry_is_stale_but_old_observed_heartbeat_horizon_is_valid(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-expiry.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = 110
        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)
        runtime.mark_turn_started(claim)
        committed = runtime.commit_turn(
            claim, module.CanonicalTurnResult("succeeded", "old-horizon-result")
        )
        assert committed.status == "succeeded"

        second = runtime.enqueue_turn(
            project_id,
            {"message": "expired"},
            actor,
            idempotency_key="expired",
            expected_version=3,
        )
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        now[0] = second_claim.lease_expires_at
        before = _claim_snapshot(conn, project_id, second.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as stale:
            runtime.commit_turn(
                second_claim,
                module.CanonicalTurnResult("failed", "expired-result"),
            )

        assert stale.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, second.turn_id) == before
        assert renewed.lease_expires_at == 160
    finally:
        conn.close()


def test_duplicate_terminal_result_id_rolls_back_the_second_commit(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "duplicate-result.db"
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, project_id, actor, key="first"
        )
        runtime.mark_turn_started(first_claim)
        runtime.commit_turn(
            first_claim,
            module.CanonicalTurnResult("succeeded", "shared-result"),
        )
        second = runtime.enqueue_turn(
            project_id,
            {"message": "second"},
            actor,
            idempotency_key="second",
            expected_version=3,
        )
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        before = _claim_snapshot(conn, project_id, second.turn_id)

        with pytest.raises(sqlite3.IntegrityError):
            runtime.commit_turn(
                second_claim,
                module.CanonicalTurnResult("succeeded", "shared-result"),
            )

        assert _claim_snapshot(conn, project_id, second.turn_id) == before
        assert first.turn_id != second.turn_id
    finally:
        conn.close()


def test_terminal_event_conflict_rolls_back_result_control_lease_and_version(
    tmp_path,
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-event-conflict.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        event_id = conn.execute(
            """
            SELECT event_id FROM project_events
            WHERE project_id = ? ORDER BY sequence LIMIT 1
            """,
            (project_id,),
        ).fetchone()[0]
        conflict_runtime = module.ProjectRuntime(
            conn,
            clock=lambda: 100,
            id_factory=lambda kind: event_id if kind == "event" else "unused",
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(sqlite3.IntegrityError):
            conflict_runtime.commit_turn(
                claim, module.CanonicalTurnResult("failed", "result")
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_requeues_expired_not_started_claim_without_readback(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-not-started.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1
        assert recovered[0].turn_id == turn.turn_id
        assert recovered[0].status == "queued"
        assert port.calls == []
        row = conn.execute(
            """
            SELECT status, attempt_id, lease_generation, fencing_token,
                   execution_state
            FROM project_turns WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()
        control = conn.execute(
            """
            SELECT control_state, control_version, attempt_id,
                   claim_worker_id, claim_lease_expires_at,
                   claim_canonical_session_id
            FROM project_run_controls WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()
        assert tuple(row) == ("queued", None, 1, 1, None)
        assert tuple(control) == ("running", before[1][3] + 2, None, None, None, None)
        assert prdb.runtime_state_for_project(conn, project_id).version == (
            before[3].version + 2
        )
        assert [
            row[0]
            for row in conn.execute(
                """
                SELECT kind FROM project_events
                WHERE project_id = ? ORDER BY sequence DESC LIMIT 2
                """,
                (project_id,),
            ).fetchall()[::-1]
        ] == ["turn.reconciling", "turn.requeued"]
        replacement = runtime.claim_next_turn(
            project_id, "worker-b", lease_seconds=30
        )
        assert replacement.turn_id == claim.turn_id
        assert replacement.attempt_id != claim.attempt_id
        assert replacement.lease_generation == claim.lease_generation + 1
        assert replacement.fencing_token == claim.fencing_token + 1
    finally:
        conn.close()


def test_recovery_stops_expired_not_started_stop_request_without_readback(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-stop.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == "stopped"
        assert port.calls == []
        assert conn.execute(
            "SELECT control_state FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "stopped"
        assert [
            row[0]
            for row in conn.execute(
                """
                SELECT kind FROM project_events
                WHERE project_id = ? ORDER BY sequence DESC LIMIT 2
                """,
                (project_id,),
            ).fetchall()[::-1]
        ] == ["turn.reconciling", "run.stopped"]
    finally:
        conn.close()


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("execution_state", ["started", None])
def test_recovery_readback_terminalizes_started_and_legacy_attempts(
    tmp_path, execution_state, outcome
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"recover-{execution_state}-{outcome}.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if execution_state == "started":
            runtime.mark_turn_started(claim)
        else:
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
            conn.commit()
        port = _RecordingReadback(
            conn,
            module.TurnReadbackResult(outcome, f"result-{outcome}"),
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == outcome
        assert len(port.calls) == 1
        request = port.calls[0]
        assert request == module.TurnReadbackRequest(
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            sequence=claim.sequence,
            worker_id=claim.worker_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            lease_expires_at=claim.lease_expires_at,
            canonical_session_id=claim.canonical_session_id,
            source_status="claimed",
            execution_state=execution_state,
        )
        assert conn.execute(
            "SELECT terminal_result_id FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == f"result-{outcome}"
    finally:
        conn.close()


def test_recovery_accepts_stopped_readback_only_for_stop_source(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-stopped-proof.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("stopped")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == "stopped"
        assert port.calls[0].source_status == "stop_requested"
    finally:
        conn.close()


@pytest.mark.parametrize(
    "port_factory",
    [
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            id="unknown",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("succeeded")
            ),
            id="missing-result",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("unknown", "extra")
            ),
            id="extra-result",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(conn, object()),
            id="wrong-type",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, error=RuntimeError("private readback detail")
            ),
            id="exception",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("stopped")
            ),
            id="source-outcome-mismatch",
        ),
    ],
)
def test_unknown_malformed_and_illegal_readback_blocks_once_per_attempt(
    tmp_path, port_factory
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-block.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        port = port_factory(module, conn)
        now[0] = claim.lease_expires_at

        first = runtime.reconcile_inflight_turns(port, limit=10)
        snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        second = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(first) == 1 and first[0].status == "reconciling"
        assert second == ()
        assert len(port.calls) == 1
        assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
        events = conn.execute(
            """
            SELECT event_id, payload_json FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert len(events) == 1
        assert "private readback detail" not in events[0]["payload_json"]
    finally:
        conn.close()


def test_recovery_block_event_identity_allows_a_later_attempt_to_block(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-block-attempt-scope.db", clock=lambda: now[0]
    )
    try:
        turn, first_claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(first_claim)
        first_port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = first_claim.lease_expires_at
        runtime.reconcile_inflight_turns(first_port, limit=10)

        conn.execute(
            """
            UPDATE project_turns
            SET status = 'queued', attempt_id = NULL, execution_state = NULL
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        )
        conn.execute(
            """
            UPDATE project_run_controls
            SET control_state = 'running', attempt_id = NULL,
                claim_worker_id = NULL, claim_lease_expires_at = NULL,
                claim_canonical_session_id = NULL
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        )
        conn.commit()
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        second_port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = second_claim.lease_expires_at

        runtime.reconcile_inflight_turns(second_port, limit=10)

        events = conn.execute(
            """
            SELECT event_id FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            ORDER BY sequence
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert len(events) == 2
        assert events[0]["event_id"] != events[1]["event_id"]
    finally:
        conn.close()


def test_awaiting_approval_expiry_is_inert_for_task5_recovery(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-approval-inert.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        conn.execute(
            "UPDATE project_turns SET status = 'awaiting_approval' WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert recovered == ()
        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_inactive_not_started_claim_blocks_but_terminal_proof_can_close(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-inactive.db", clock=lambda: now[0]
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, project_id, actor, key="inactive-not-started"
        )
        state = prdb.runtime_state_for_project(conn, project_id)
        with prdb.write_transaction(conn):
            state = prdb.transition_lifecycle(
                conn,
                project_id=project_id,
                expected_version=state.version,
                lifecycle="awaiting_acceptance",
                updated_at=101,
            )
        now[0] = first_claim.lease_expires_at
        no_call = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "unused")
        )

        blocked = runtime.reconcile_inflight_turns(no_call, limit=10)

        assert blocked[0].status == "reconciling"
        assert no_call.calls == []

        second_project = projects_db.create_project(conn, name="Inactive proof")
        prdb.create_project_conversation(
            conn,
            project_id=second_project,
            conversation_id="second-root",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="second-owner",
            project_id=second_project,
            surface="desktop",
            external_binding_id="second-window",
            actor_id="owner",
            now=1,
        )
        second_actor = ActorContext("owner", "desktop", "second-owner", True)
        second_runtime = module.ProjectRuntime(conn, clock=lambda: now[0])
        now[0] = 200
        second = second_runtime.enqueue_turn(
            second_project,
            {"message": "terminal proof"},
            second_actor,
            idempotency_key="terminal-proof",
            expected_version=0,
        )
        second_claim = second_runtime.claim_next_turn(
            second_project, "worker-two", lease_seconds=30
        )
        second_runtime.mark_turn_started(second_claim)
        second_state = prdb.runtime_state_for_project(conn, second_project)
        with prdb.write_transaction(conn):
            prdb.transition_lifecycle(
                conn,
                project_id=second_project,
                expected_version=second_state.version,
                lifecycle="awaiting_acceptance",
                updated_at=201,
            )
        proof = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "inactive-result")
        )
        now[0] = second_claim.lease_expires_at

        closed = second_runtime.reconcile_inflight_turns(proof, limit=10)

        assert closed[0].turn_id == second.turn_id
        assert closed[0].status == "succeeded"
    finally:
        conn.close()


def test_preexisting_reconciling_attempt_resumes_without_a_lease(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-preexisting.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        conn.execute(
            "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.execute(
            "DELETE FROM project_worker_leases WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.commit()
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("failed", "resumed-result")
        )

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == "failed"
        assert len(port.calls) == 1
    finally:
        conn.close()


@pytest.mark.parametrize("limit", [True, 0, 101, 1.5])
def test_recovery_rejects_invalid_limit_before_port_or_write(tmp_path, limit):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"recover-limit-{limit}.db"
    )
    try:
        turn, _ = _enqueue_and_claim(runtime, project_id, actor)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.reconcile_inflight_turns(port, limit=limit)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_rejects_outer_transaction_before_port_or_write(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-outer-transaction.db"
    )
    try:
        turn, _ = _enqueue_and_claim(runtime, project_id, actor)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with prdb.write_transaction(conn):
            with pytest.raises(module.ProjectRuntimeError) as rejected:
                runtime.reconcile_inflight_turns(port, limit=10)
            assert (
                rejected.value.code
                is module.RuntimeErrorCode.INVALID_ARGUMENT
            )

        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_two_reconcilers_may_read_but_commit_one_terminal_outcome(tmp_path):
    path = tmp_path / "recover-race.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    now[0] = claim.lease_expires_at
    bootstrap.close()
    barrier = threading.Barrier(2)

    def reconcile():
        conn = projects_db.connect(path)
        try:
            port = _RecordingReadback(
                conn,
                module.TurnReadbackResult("succeeded", "race-result"),
                barrier=barrier,
            )
            result = module.ProjectRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(port, limit=10)
            return result, len(port.calls)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(reconcile) for _ in range(2)]
        results = [future.result(timeout=15) for future in futures]

    check = projects_db.connect(path)
    try:
        assert sum(calls for _, calls in results) == 2
        assert all(result[0].status == "succeeded" for result, _ in results)
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ? AND kind = 'turn.succeeded'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 1
    finally:
        check.close()


def test_recovery_takeover_rotates_fence_and_stale_worker_cannot_write(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-takeover-fence.db", clock=lambda: now[0]
    )
    try:
        turn, stale_claim = _enqueue_and_claim(
            runtime, project_id, actor
        )
        now[0] = stale_claim.lease_expires_at
        runtime.reconcile_inflight_turns(
            _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            limit=10,
        )
        current_claim = runtime.claim_next_turn(
            project_id, "worker-current", lease_seconds=30
        )
        assert current_claim is not None
        assert current_claim.turn_id == turn.turn_id
        assert current_claim.attempt_id != stale_claim.attempt_id
        assert (
            current_claim.lease_generation
            == stale_claim.lease_generation + 1
        )
        assert current_claim.fencing_token == stale_claim.fencing_token + 1
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        stale_calls = (
            lambda: runtime.heartbeat_turn(
                stale_claim, lease_seconds=30
            ),
            lambda: runtime.mark_turn_started(stale_claim),
            lambda: runtime.commit_turn(
                stale_claim,
                module.CanonicalTurnResult("succeeded", "stale-result"),
            ),
            lambda: runtime.acknowledge_stopped(stale_claim),
        )
        for call in stale_calls:
            with pytest.raises(module.ProjectRuntimeError):
                call()
            assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_fresh_processes_serialize_claim_and_take_over_after_expiry(tmp_path):
    path = tmp_path / "recover-process-takeover.db"
    module, conn, runtime, project_id, actor = _make_runtime(path)
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "cross-process"},
            actor,
            idempotency_key="cross-process",
            expected_version=0,
        )
    finally:
        conn.close()

    probe = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "project_runtime_worker_probe.py"
    )
    command = [
        sys.executable,
        str(probe),
        "claim",
        str(path),
        project_id,
    ]
    workers = [
        subprocess.Popen(
            [*command, worker_id, "100"],
            cwd=Path(__file__).resolve().parents[2],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for worker_id in ("process-a", "process-b")
    ]
    outputs = [
        _communicate_probe(worker)
        for worker in workers
    ]
    assert [worker.returncode for worker in workers] == [0, 0], outputs
    claims = [
        json.loads(stdout)["claim"]
        for stdout, _ in outputs
        if json.loads(stdout)["claim"] is not None
    ]
    assert len(claims) == 1
    stale_claim = claims[0]
    assert stale_claim["turn_id"] == turn.turn_id

    takeover = subprocess.run(
        [
            sys.executable,
            str(probe),
            "recover-claim",
            str(path),
            project_id,
            "process-takeover",
            str(stale_claim["lease_expires_at"]),
        ],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert takeover.returncode == 0, takeover.stderr
    payload = json.loads(takeover.stdout)
    assert payload["recovered"] == [
        {"status": "queued", "turn_id": turn.turn_id}
    ]
    current_claim = payload["claim"]
    assert current_claim["attempt_id"] != stale_claim["attempt_id"]
    assert (
        current_claim["lease_generation"]
        == stale_claim["lease_generation"] + 1
    )
    assert (
        current_claim["fencing_token"]
        == stale_claim["fencing_token"] + 1
    )


def test_fresh_process_claim_race_repeats_25_times_and_winner_commits(
    tmp_path,
):
    path = tmp_path / "recover-process-race-25.db"
    module, conn, runtime, first_project, first_actor = _make_runtime(path)
    probe = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "project_runtime_worker_probe.py"
    )
    repo_root = Path(__file__).resolve().parents[2]
    try:
        for iteration in range(25):
            if iteration == 0:
                project_id = first_project
                actor = first_actor
                project_runtime = runtime
            else:
                project_id = projects_db.create_project(
                    conn, name=f"Race {iteration}"
                )
                session_id = f"race-root-{iteration}"
                binding_id = f"race-owner-{iteration}"
                prdb.create_project_conversation(
                    conn,
                    project_id=project_id,
                    conversation_id=session_id,
                    current_phase="implementation",
                    now=1,
                )
                prdb.bind_surface(
                    conn,
                    binding_id=binding_id,
                    project_id=project_id,
                    surface="desktop",
                    external_binding_id=f"window-{iteration}",
                    actor_id="owner",
                    now=1,
                )
                actor = ActorContext(
                    "owner", "desktop", binding_id, True
                )
                project_runtime = module.ProjectRuntime(
                    conn, clock=lambda: 100
                )
            turn = project_runtime.enqueue_turn(
                project_id,
                {"iteration": iteration},
                actor,
                idempotency_key=f"race-{iteration}",
                expected_version=0,
            )
            command = [
                sys.executable,
                str(probe),
                "claim",
                str(path),
                project_id,
            ]
            workers = [
                subprocess.Popen(
                    [*command, worker_id, "100"],
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                )
                for worker_id in (
                    f"worker-a-{iteration}",
                    f"worker-b-{iteration}",
                )
            ]
            outputs = [
                _communicate_probe(worker)
                for worker in workers
            ]
            assert [worker.returncode for worker in workers] == [0, 0], (
                iteration,
                outputs,
            )
            claims = [
                payload["claim"]
                for stdout, _ in outputs
                if (payload := json.loads(stdout))["claim"] is not None
            ]
            assert len(claims) == 1
            claim = claims[0]
            claim_json = json.dumps(
                claim, sort_keys=True, separators=(",", ":")
            )
            started = subprocess.run(
                [
                    sys.executable,
                    str(probe),
                    "start",
                    str(path),
                    project_id,
                    claim["worker_id"],
                    "100",
                    claim_json,
                ],
                cwd=repo_root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=15,
                check=False,
            )
            assert started.returncode == 0, started.stderr
            assert json.loads(started.stdout)["claim"] == claim
            committed = subprocess.run(
                [
                    sys.executable,
                    str(probe),
                    "commit",
                    str(path),
                    project_id,
                    claim["worker_id"],
                    "100",
                    claim_json,
                    "succeeded",
                    f"result-{iteration}",
                ],
                cwd=repo_root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=15,
                check=False,
            )
            assert committed.returncode == 0, committed.stderr
            assert json.loads(committed.stdout) == {
                "claim": None,
                "recovered": [],
                "status": "succeeded",
                "turn_id": turn.turn_id,
            }
            assert conn.execute(
                """
                SELECT COUNT(*) FROM project_worker_leases
                WHERE project_id = ?
                """,
                (project_id,),
            ).fetchone()[0] == 0
            assert conn.execute(
                """
                SELECT COUNT(*) FROM project_events
                WHERE project_id = ? AND turn_id = ?
                  AND kind = 'turn.succeeded'
                """,
                (project_id, turn.turn_id),
            ).fetchone()[0] == 1
    finally:
        conn.close()


def test_claim_scan_is_set_based_and_bounded_by_the_fifo_head(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "bounded-claim-scan.db"
    )
    try:
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, origin_binding_id, status, attempt_id,
                    lease_generation, fencing_token, execution_state,
                    terminal_result_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, '{}', 'owner-binding', 'cancelled',
                          NULL, 0, 0, NULL, NULL, 1, 1)
                """,
                [
                    (
                        f"historical-{sequence}",
                        project_id,
                        sequence,
                        f"historical-{sequence}",
                    )
                    for sequence in range(1, 251)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    idempotency_key, command_fingerprint, attempt_id,
                    claim_worker_id, claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (?, ?, 'terminal', 0, NULL, NULL, NULL,
                          NULL, NULL, NULL, 1)
                """,
                [
                    (f"historical-{sequence}", project_id)
                    for sequence in range(1, 251)
                ],
            )
        target = runtime.enqueue_turn(
            project_id,
            {"message": "bounded"},
            actor,
            idempotency_key="bounded",
            expected_version=0,
        )
        statements = []
        conn.set_trace_callback(statements.append)
        try:
            claim = runtime.claim_next_turn(
                project_id, "bounded-worker", lease_seconds=30
            )
        finally:
            conn.set_trace_callback(None)

        assert claim is not None and claim.turn_id == target.turn_id
        normalized = [
            " ".join(statement.lower().split())
            for statement in statements
        ]
        turn_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_turns" in statement
        ]
        control_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_run_controls" in statement
        ]
        lease_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_worker_leases" in statement
        ]
        assert len(turn_selects) <= 4
        assert len(control_selects) <= 3
        assert len(lease_selects) <= 3
        assert all(
            "order by sequence, turn_id" not in statement
            or "limit 1" in statement
            for statement in turn_selects
        )
    finally:
        conn.close()


def test_recovery_parks_the_whole_batch_before_first_readback(tmp_path):
    now = [100]
    module, conn, runtime, first_project, first_actor = _make_runtime(
        tmp_path / "recover-whole-batch.db", clock=lambda: now[0]
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, first_project, first_actor, key="first"
        )
        runtime.mark_turn_started(first_claim)
        second_project = projects_db.create_project(conn, name="Second")
        prdb.create_project_conversation(
            conn,
            project_id=second_project,
            conversation_id="second-root",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="second-owner",
            project_id=second_project,
            surface="desktop",
            external_binding_id="second-window",
            actor_id="owner",
            now=1,
        )
        second_actor = ActorContext(
            "owner", "desktop", "second-owner", True
        )
        second_runtime = module.ProjectRuntime(
            conn, clock=lambda: now[0]
        )
        second = second_runtime.enqueue_turn(
            second_project,
            {"message": "second"},
            second_actor,
            idempotency_key="second",
            expected_version=0,
        )
        second_claim = second_runtime.claim_next_turn(
            second_project, "second-worker", lease_seconds=30
        )
        assert second_claim is not None
        second_runtime.mark_turn_started(second_claim)
        now[0] = first_claim.lease_expires_at

        class _BatchReadback:
            def __init__(self):
                self.calls = []

            def read_turn(self, request):
                assert conn.in_transaction is False
                if not self.calls:
                    assert {
                        row["status"]
                        for row in conn.execute(
                            """
                            SELECT status FROM project_turns
                            WHERE turn_id IN (?, ?)
                            """,
                            (first.turn_id, second.turn_id),
                        )
                    } == {"reconciling"}
                    assert conn.execute(
                        """
                        SELECT COUNT(*) FROM project_worker_leases
                        WHERE turn_id IN (?, ?)
                        """,
                        (first.turn_id, second.turn_id),
                    ).fetchone()[0] == 0
                self.calls.append(request)
                return module.TurnReadbackResult(
                    "succeeded", f"result-{request.turn_id}"
                )

        port = _BatchReadback()
        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(port.calls) == 2
        assert {turn.status for turn in recovered} == {"succeeded"}
    finally:
        conn.close()


def test_claim_counter_overflow_is_rejected_without_writes(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "claim-counter-overflow.db"
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "overflow"},
            actor,
            idempotency_key="overflow",
            expected_version=0,
        )
        conn.execute(
            """
            UPDATE project_turns
            SET lease_generation = ?, fencing_token = ?
            WHERE turn_id = ?
            """,
            (module.SQLITE_INT_MAX, module.SQLITE_INT_MAX, turn.turn_id),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(RuntimeError, match="counter"):
            runtime.claim_next_turn(
                project_id, "overflow-worker", lease_seconds=30
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_claim_expiry_overflow_is_invalid_without_writes(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "claim-expiry-overflow.db",
        clock=lambda: (1 << 63) - 1,
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "expiry overflow"},
            actor,
            idempotency_key="expiry-overflow",
            expected_version=0,
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.claim_next_turn(
                project_id, "overflow-worker", lease_seconds=1
            )

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_candidate_query_selects_the_expiry_index(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-expiry-plan.db"
    )
    try:
        _enqueue_and_claim(runtime, project_id, actor)
        statements = []
        conn.set_trace_callback(statements.append)
        try:
            assert runtime.reconcile_inflight_turns(
                _RecordingReadback(
                    conn, module.TurnReadbackResult("unknown")
                ),
                limit=10,
            ) == ()
        finally:
            conn.set_trace_callback(None)
        candidate_query = next(
            statement
            for statement in statements
            if "FROM project_turns AS turn" in statement
            and "turn.status IN ('claimed', 'stop_requested')" in statement
        )
        details = " ".join(
            row["detail"]
            for row in conn.execute(
                f"EXPLAIN QUERY PLAN {candidate_query}"
            )
        )
        assert "idx_project_worker_leases_expiry" in details
    finally:
        conn.close()


def test_heartbeat_after_recovery_discovery_invalidates_candidate_cleanly(
    tmp_path, monkeypatch
):
    path = tmp_path / "recover-heartbeat-wins.db"
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    heartbeat_conn = None
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        heartbeat_conn = projects_db.connect(path)
        heartbeat_runtime = module.ProjectRuntime(
            heartbeat_conn, clock=lambda: claim.lease_expires_at - 1
        )
        original_candidates = prdb._recovery_candidates
        renewed = []

        def discover_then_heartbeat(*args, **kwargs):
            candidates = original_candidates(*args, **kwargs)
            renewed.append(
                heartbeat_runtime.heartbeat_turn(
                    claim, lease_seconds=30
                )
            )
            return candidates

        monkeypatch.setattr(
            prdb, "_recovery_candidates", discover_then_heartbeat
        )
        before_events = tuple(
            conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? ORDER BY sequence
                """,
                (project_id,),
            )
        )

        recovered = runtime.reconcile_inflight_turns(
            _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            limit=10,
        )

        assert recovered == ()
        assert len(renewed) == 1
        assert conn.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "claimed"
        assert tuple(
            conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? ORDER BY sequence
                """,
                (project_id,),
            )
        ) == before_events
    finally:
        if heartbeat_conn is not None:
            heartbeat_conn.close()
        conn.close()
