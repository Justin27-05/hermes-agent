"""Contract tests for the durable ProjectOperationGuard."""

from __future__ import annotations

import importlib
import inspect
import json
import queue
import sqlite3
import subprocess
import sys
import threading
from dataclasses import FrozenInstanceError, fields, replace
from enum import Enum
from types import MappingProxyType
from pathlib import Path
from typing import Literal

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import (
    ActorContext,
    CRITICAL_ACTION_RULES,
    Decision,
    PolicyDecision,
)


TASK6_COLUMNS = {
    "guard_revision",
    "canonical_action",
    "batch_items_json",
    "readback_kind",
    "attempt_id",
    "lease_generation",
    "fencing_token",
    "receipt_id",
    "readback_json",
    "blocked_reason",
    "remote_idempotency_supported",
    "approval_fingerprint_json",
    "guard_validated",
}

TASK6_INDEXES = {
    "idx_project_operations_one_approval",
    "idx_project_approvals_one_operation",
    "idx_project_operations_receipt",
    "idx_project_operations_turn_status",
    "idx_project_operations_recovery",
    "idx_project_operations_approved_rehydrate",
    "idx_project_operations_turn_unresolved",
    "idx_project_operations_turn_unsafe",
}

OPERATION_PROBE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "project_operation_probe.py"
)

CRITICAL_ACTION_CASES = tuple(
    (action, rule.approval_class)
    for action, rule in CRITICAL_ACTION_RULES.items()
)


@pytest.fixture
def operation_conn(tmp_path):
    conn = projects_db.connect(tmp_path / "projects.db")
    try:
        yield conn
    finally:
        conn.close()


def _build_operation_env(tmp_path, *, started):
    conn = projects_db.connect(tmp_path / "guard.db")
    project_id = projects_db.create_project(
        conn,
        name="Operation Guard",
        folders=("C:/work/operations",),
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
    runtime_module = importlib.import_module("hermes_cli.project_runtime")
    operation_module = importlib.import_module(
        "hermes_cli.project_operations"
    )
    now = [100]
    runtime = runtime_module.ProjectRuntime(
        conn,
        clock=lambda: now[0],
    )
    actor = ActorContext(
        "owner-1", "desktop", "desktop-owner", True
    )
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "perform one operation"},
        actor,
        idempotency_key="turn-key",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id, "worker-1", lease_seconds=30
    )
    assert claim is not None
    if started:
        claim = runtime.mark_turn_started(claim)
    return {
        "conn": conn,
        "project_id": project_id,
        "turn": turn,
        "claim": claim,
        "actor": actor,
        "discord_actor": ActorContext(
            "owner-1", "discord", "discord-owner", True
        ),
        "runtime": runtime,
        "module": operation_module,
        "guard": operation_module.ProjectOperationGuard(runtime),
        "now": now,
    }


@pytest.fixture
def operation_env(tmp_path):
    env = _build_operation_env(tmp_path, started=True)
    try:
        yield env
    finally:
        env["conn"].close()


@pytest.fixture
def operation_env_not_started(tmp_path):
    env = _build_operation_env(tmp_path, started=False)
    try:
        yield env
    finally:
        env["conn"].close()


def _insert_project(conn, project_id="project-1"):
    conn.execute(
        """
        INSERT INTO projects (id, slug, name, created_at, archived)
        VALUES (?, ?, ?, 1, 0)
        """,
        (project_id, project_id, project_id),
    )
    conn.commit()
    return project_id


def _legacy_operation_values(conn, project_id):
    conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key, approval_id,
            command_revision, targets_json, payload_json, status, receipt_json,
            created_at, updated_at
        ) VALUES (
            'legacy-operation', ?, NULL, 'legacy-key', NULL,
            7, '["C:/legacy"]', '{"secret":"unchanged"}', 'completed',
            '{"receipt":"legacy"}', 11, 12
        )
        """,
        (project_id,),
    )
    conn.commit()
    return tuple(
        conn.execute(
            """
            SELECT operation_id, project_id, turn_id, idempotency_key,
                   approval_id, command_revision, targets_json, payload_json,
                   status, receipt_json, created_at, updated_at
            FROM project_operations
            WHERE operation_id = 'legacy-operation'
            """
        ).fetchone()
    )


def test_public_operation_contract_is_exact_frozen_and_secret_safe():
    module = importlib.import_module("hermes_cli.project_operations")

    assert module.OperationStatus == Literal[
        "awaiting_approval",
        "approved",
        "effect_started",
        "receipt_recorded",
        "unknown",
        "reconciled",
        "blocked",
    ]
    assert tuple(member.value for member in module.OperationErrorCode) == (
        "invalid_operation_argument",
        "operation_policy_denied",
        "operation_not_found",
        "operation_idempotency_conflict",
        "operation_state_conflict",
        "operation_approval_conflict",
        "operation_capability_unsupported",
        "operation_receipt_conflict",
        "legacy_operation_unmanaged",
    )
    assert tuple(field.name for field in fields(module.OperationApprovalSpec)) == (
        "approval_id",
        "approval_class",
        "expires_at",
        "authorization",
    )
    assert tuple(field.name for field in fields(module.OperationIntent)) == (
        "operation_id",
        "project_id",
        "turn_id",
        "idempotency_key",
        "canonical_action",
        "command_revision",
        "targets",
        "batch_items",
        "payload",
        "readback_kind",
        "remote_idempotency_supported",
    )
    assert tuple(field.name for field in fields(module.OperationReceipt)) == (
        "receipt_id",
        "payload",
    )
    assert tuple(field.name for field in fields(module.ProjectOperation)) == (
        "operation_id",
        "project_id",
        "turn_id",
        "idempotency_key",
        "canonical_action",
        "command_revision",
        "targets",
        "batch_items",
        "status",
        "approval_id",
        "readback_kind",
        "receipt_id",
        "blocked_reason",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "created_at",
        "updated_at",
    )
    assert tuple(field.name for field in fields(module.OperationReadbackRequest)) == (
        "operation_id",
        "project_id",
        "turn_id",
        "canonical_action",
        "targets",
        "batch_items",
        "idempotency_key",
        "readback_kind",
        "receipt",
        "attempt_id",
        "lease_generation",
        "fencing_token",
    )
    assert tuple(field.name for field in fields(module.OperationReadbackResult)) == (
        "outcome",
        "evidence",
        "receipt",
    )
    assert tuple(
        inspect.signature(module.ProjectOperationGuard.prepare).parameters
    ) == ("self", "claim", "intent", "policy", "approval")
    assert tuple(
        inspect.signature(module.ProjectOperationGuard.resolve_operation_approval).parameters
    ) == ("self", "approval_id", "resolver", "outcome")
    assert tuple(
        inspect.signature(module.ProjectOperationGuard.expire_due_operation_approvals).parameters
    ) == ("self", "limit")
    assert tuple(
        inspect.signature(module.ProjectOperationGuard.mark_started).parameters
    ) == ("self", "claim", "operation_id")
    assert tuple(
        inspect.signature(module.ProjectOperationGuard.record_receipt).parameters
    ) == ("self", "claim", "operation_id", "receipt")
    assert tuple(
        inspect.signature(module.ProjectOperationGuard.reconcile).parameters
    ) == ("self", "claim", "operation_id", "readback")
    assert tuple(
        inspect.signature(module.ProjectOperationGuard.block_unknown).parameters
    ) == ("self", "claim", "operation_id")
    assert tuple(
        inspect.signature(module.ProjectOperationGuard.disposition_for_turn).parameters
    ) == ("self", "project_id", "turn_id")

    payload = {"nested": [1, {"ok": True}]}
    intent = module.OperationIntent(
        operation_id="operation-1",
        project_id="project-1",
        turn_id="turn-1",
        idempotency_key="remote-key",
        canonical_action="local_code_edit",
        command_revision=1,
        targets=("C:/work/file.py",),
        batch_items=("write-file",),
        payload=payload,
        readback_kind="remote-ledger",
        remote_idempotency_supported=True,
    )
    payload["nested"][1]["ok"] = False
    assert intent.payload == {"nested": (1, MappingProxyType({"ok": True}))}
    assert isinstance(intent.payload, MappingProxyType)
    with pytest.raises(FrozenInstanceError):
        intent.operation_id = "changed"
    assert "payload" not in {
        field.name for field in fields(module.ProjectOperation)
    }
    assert "evidence" not in {
        field.name for field in fields(module.ProjectOperation)
    }


def test_task6_schema_is_additive_indexed_and_rejects_raw_invalid_managed_rows(
    operation_conn,
):
    columns = {
        row["name"]
        for row in operation_conn.execute(
            "PRAGMA table_info(project_operations)"
        )
    }
    indexes = {
        row["name"]
        for row in operation_conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'index' AND tbl_name IN (
                'project_operations', 'project_approvals'
            )
            """
        )
    }
    triggers = {
        row["name"]
        for row in operation_conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'trigger' AND tbl_name = 'project_operations'
            """
        )
    }

    assert TASK6_COLUMNS <= columns
    assert TASK6_INDEXES <= indexes
    assert {
        "trg_project_operations_task6_insert",
        "trg_project_operations_task6_update",
    } <= triggers

    project_id = _insert_project(operation_conn)
    with pytest.raises(sqlite3.IntegrityError):
        operation_conn.execute(
            """
            INSERT INTO project_operations (
                operation_id, project_id, turn_id, idempotency_key,
                command_revision, targets_json, payload_json, status,
                created_at, updated_at, guard_revision, canonical_action,
                batch_items_json, readback_kind, attempt_id,
                lease_generation, fencing_token, guard_validated
            ) VALUES (
                'invalid-managed', ?, 'turn', 'remote-key', 1, '[]', '{}',
                'invented', 1, 1, 1, 'local_code_edit', '["item"]',
                'ledger', 'attempt', 1, 1, 1
            )
            """,
            (project_id,),
        )
    operation_conn.rollback()


def _unsafe_index_shape(conn):
    rows = conn.execute(
        """
        SELECT name, sql, rootpage
        FROM sqlite_master
        WHERE type = 'index'
          AND name IN (
              'idx_project_operations_turn_unsafe',
              'idx_project_operations_turn_unsafe_blocked'
          )
        ORDER BY name
        """
    ).fetchall()
    index_list = {
        row["name"]: row
        for row in conn.execute(
            "PRAGMA index_list('project_operations')"
        )
    }
    xinfo = conn.execute(
        "PRAGMA index_xinfo('idx_project_operations_turn_unsafe')"
    ).fetchall()
    return rows, index_list, xinfo


def _assert_canonical_unsafe_expression_index(conn):
    rows, index_list, xinfo = _unsafe_index_shape(conn)
    assert [row["name"] for row in rows] == [
        "idx_project_operations_turn_unsafe"
    ]
    assert prdb._OPERATION_UNSAFE_CLASS_SQL in rows[0]["sql"]
    assert index_list["idx_project_operations_turn_unsafe"][
        "partial"
    ] == 0
    key_rows = [row for row in xinfo if row["key"] == 1]
    assert [(row["seqno"], row["cid"]) for row in key_rows] == [
        (0, 1),
        (1, 2),
        (2, -2),
        (3, 0),
    ]
    return rows[0]["sql"], rows[0]["rootpage"]


@pytest.mark.parametrize(
    (
        "guard_validated",
        "guard_revision",
        "status",
        "blocked_reason",
        "expected",
    ),
    (
        *((1, 1, status, None, 0) for status in (
            "approved",
            "effect_started",
            "receipt_recorded",
            "unknown",
            "reconciled",
        )),
        *((1, 1, "blocked", reason, 0) for reason in (
            "operation_capability_unsupported",
            "approval_denied",
            "approval_time_expired",
            "approval_stale_boundary",
        )),
        (None, 1, "approved", None, 1),
        ("1", 1, "approved", None, 1),
        (0, 1, "approved", None, 1),
        (1, None, "approved", None, 1),
        (1, "1", "approved", None, 1),
        (1, 0, "approved", None, 1),
        (1, 1, None, None, 1),
        (1, 1, 1, None, 1),
        (1, 1, "invented", None, 1),
        (1, 1, "blocked", None, 1),
        (1, 1, "blocked", 1, 1),
        (1, 1, "blocked", "operation_readback_ambiguous", 1),
    ),
)
def test_operation_unsafe_classifier_is_total_exact_integer(
    operation_conn,
    guard_validated,
    guard_revision,
    status,
    blocked_reason,
    expected,
):
    result = operation_conn.execute(
        f"""
        SELECT ({prdb._OPERATION_UNSAFE_CLASS_SQL}) AS unsafe
        FROM (
            SELECT ? AS guard_validated,
                   ? AS guard_revision,
                   ? AS status,
                   ? AS blocked_reason
        )
        """,
        (
            guard_validated,
            guard_revision,
            status,
            blocked_reason,
        ),
    ).fetchone()["unsafe"]
    assert type(result) is int
    assert result == expected


def test_operation_unsafe_expression_index_and_hot_probes_are_exact(
    operation_conn,
):
    assert sqlite3.sqlite_version_info >= (3, 9, 0)
    _assert_canonical_unsafe_expression_index(operation_conn)

    standalone = operation_conn.execute(
        "EXPLAIN QUERY PLAN " + prdb._OPERATION_TURN_UNSAFE_SQL,
        ("project", "turn"),
    ).fetchall()
    correlated = operation_conn.execute(
        "EXPLAIN QUERY PLAN "
        + prdb._OPERATION_PENDING_BRANCH_SQL,
        ("approved", 1),
    ).fetchall()
    for plan in (standalone, correlated):
        details = " ".join(row["detail"] for row in plan)
        assert (
            "SEARCH "
            in details
            and "idx_project_operations_turn_unsafe" in details
            and "<expr>=?" in details
        )
        assert "SCAN project_operations" not in details
        assert "sqlite_autoindex_project_operations_3" not in details
        assert "USE TEMP B-TREE" not in details
        assert "UNION ALL" not in details
        assert "idx_project_operations_turn_unsafe_blocked" not in details


@pytest.mark.parametrize("obsolete_shape", ("partial", "partial_helper"))
def test_operation_unsafe_index_upgrade_and_repeat_converge(
    operation_conn,
    obsolete_shape,
):
    operation_conn.execute(
        "DROP INDEX idx_project_operations_turn_unsafe"
    )
    operation_conn.execute(
        "DROP INDEX IF EXISTS idx_project_operations_turn_unsafe_blocked"
    )
    operation_conn.execute(
        """
        CREATE INDEX idx_project_operations_turn_unsafe
        ON project_operations(project_id, turn_id, operation_id)
        WHERE guard_validated IS NOT 1
           OR guard_revision IS NOT 1
           OR status IS NULL
        """
    )
    if obsolete_shape == "partial_helper":
        operation_conn.execute(
            """
            CREATE INDEX idx_project_operations_turn_unsafe_blocked
            ON project_operations(project_id, turn_id, operation_id)
            WHERE status = 'blocked'
            """
        )
    operation_conn.commit()

    traced = []
    operation_conn.set_trace_callback(traced.append)
    try:
        prdb.ensure_schema(operation_conn)
    finally:
        operation_conn.set_trace_callback(None)
    assert not any("sqlite_schema" in sql.lower() for sql in traced)
    canonical = _assert_canonical_unsafe_expression_index(
        operation_conn
    )

    prdb.ensure_schema(operation_conn)
    assert _assert_canonical_unsafe_expression_index(
        operation_conn
    ) == canonical


def test_operation_unsafe_index_preflight_precedes_any_drop(
    operation_conn,
    monkeypatch,
):
    operation_conn.execute(
        "DROP INDEX idx_project_operations_turn_unsafe"
    )
    operation_conn.execute(
        """
        CREATE INDEX idx_project_operations_turn_unsafe
        ON project_operations(project_id, turn_id, operation_id)
        WHERE guard_validated IS NOT 1
        """
    )
    operation_conn.commit()
    before = _unsafe_index_shape(operation_conn)[0]
    monkeypatch.setattr(
        prdb.sqlite3, "sqlite_version_info", (3, 8, 11)
    )

    with pytest.raises(
        prdb.OperationMigrationError,
        match="expression index",
    ):
        prdb.ensure_schema(operation_conn)

    after = _unsafe_index_shape(operation_conn)[0]
    assert [(row["name"], row["sql"]) for row in after] == [
        (row["name"], row["sql"]) for row in before
    ]


def test_operation_unsafe_index_replacement_failure_rolls_back(
    operation_conn,
    monkeypatch,
):
    operation_conn.execute(
        "DROP INDEX idx_project_operations_turn_unsafe"
    )
    operation_conn.execute(
        """
        CREATE INDEX idx_project_operations_turn_unsafe
        ON project_operations(project_id, turn_id, operation_id)
        WHERE guard_validated IS NOT 1
        """
    )
    operation_conn.commit()
    before = _unsafe_index_shape(operation_conn)[0]
    original_execute = prdb.execute_schema_statements

    def fail_unsafe_create(conn, schema_sql):
        if "idx_project_operations_turn_unsafe" in schema_sql:
            raise sqlite3.OperationalError(
                "simulated expression-index storage failure"
            )
        return original_execute(conn, schema_sql)

    monkeypatch.setattr(
        prdb, "execute_schema_statements", fail_unsafe_create
    )
    with pytest.raises(
        prdb.OperationMigrationError,
        match="unsafe operation index",
    ):
        prdb.ensure_schema(operation_conn)

    after = _unsafe_index_shape(operation_conn)[0]
    assert [(row["name"], row["sql"]) for row in after] == [
        (row["name"], row["sql"]) for row in before
    ]


def test_operation_unsafe_index_concurrent_initializers_converge(
    tmp_path,
):
    path = tmp_path / "concurrent-operation-schema.db"
    initial = projects_db.connect(path)
    project_id = _insert_project(
        initial, "concurrent-operation-schema"
    )
    initial.execute(
        "DROP INDEX idx_project_operations_turn_unsafe"
    )
    initial.execute(
        """
        CREATE INDEX idx_project_operations_turn_unsafe
        ON project_operations(project_id, turn_id, operation_id)
        WHERE guard_validated IS NOT 1
        """
    )
    _drop_operation_sequence_guards(initial)
    initial.commit()
    initial.execute("PRAGMA foreign_keys=OFF")
    for ordinal in (1, 2):
        _insert_migration_operation(
            initial,
            project_id,
            f"concurrent-operation-{ordinal}",
            f"concurrent-approval-{ordinal}",
        )
        _insert_migration_approval(
            initial,
            project_id,
            f"concurrent-approval-{ordinal}",
            f"concurrent-operation-{ordinal}",
            ordinal,
        )
    initial.commit()
    initial.execute("PRAGMA foreign_keys=ON")
    initial.close()
    barrier = threading.Barrier(2)
    outcomes = queue.Queue()

    def initialize():
        barrier.wait()
        try:
            conn = projects_db.connect(path)
            conn.close()
            outcomes.put(None)
        except Exception as exc:
            outcomes.put(exc)

    threads = [threading.Thread(target=initialize) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()
    assert [outcomes.get_nowait(), outcomes.get_nowait()] == [
        None,
        None,
    ]
    final = projects_db.connect(path)
    try:
        _assert_canonical_unsafe_expression_index(final)
        assert tuple(
            tuple(row)
            for row in final.execute(
                """
                SELECT approval_id, operation_maintenance_seq
                FROM project_approvals
                WHERE project_id = ?
                ORDER BY approval_id
                """,
                (project_id,),
            )
        ) == (
            ("concurrent-approval-1", 1),
            ("concurrent-approval-2", 2),
        )
        assert _maintenance_state(final)[-1] == 3
        assert final.execute(
            """
            SELECT COUNT(*)
            FROM project_operation_maintenance
            """
        ).fetchone()[0] == 1
    finally:
        final.close()


def test_task1_operation_rows_remain_byte_exact_revision_zero_and_fail_closed(
    operation_conn,
):
    project_id = _insert_project(operation_conn)
    before = _legacy_operation_values(operation_conn, project_id)

    prdb.ensure_schema(operation_conn)
    after = tuple(
        operation_conn.execute(
            """
            SELECT operation_id, project_id, turn_id, idempotency_key,
                   approval_id, command_revision, targets_json, payload_json,
                   status, receipt_json, created_at, updated_at
            FROM project_operations
            WHERE operation_id = 'legacy-operation'
            """
        ).fetchone()
    )
    revision = operation_conn.execute(
        """
        SELECT guard_revision FROM project_operations
        WHERE operation_id = 'legacy-operation'
        """
    ).fetchone()[0]

    assert after == before
    assert revision == 0
    with pytest.raises(prdb.LegacyOperationUnmanagedError):
        prdb._project_operation_for_id(
            operation_conn,
            project_id=project_id,
            operation_id="legacy-operation",
        )


def test_managed_operation_mapper_rejects_noncanonical_json(operation_conn):
    project_id = _insert_project(operation_conn)
    operation_conn.execute("PRAGMA foreign_keys=OFF")
    operation_conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            command_revision, targets_json, payload_json, status,
            created_at, updated_at, guard_revision, canonical_action,
            batch_items_json, readback_kind, attempt_id,
            lease_generation, fencing_token,
            remote_idempotency_supported
        ) VALUES (
            'malformed-managed', ?, 'turn', 'remote-key', 1,
            '[ "C:/work" ]', '{}', 'approved', 1, 1, 1,
            'local_code_edit', '["item"]', 'ledger', 'attempt', 1, 1, 1
        )
        """,
        (project_id,),
    )
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=ON")

    with pytest.raises(RuntimeError, match="malformed persisted project operation"):
        prdb._project_operation_for_id(
            operation_conn,
            project_id=project_id,
            operation_id="malformed-managed",
        )


@pytest.mark.parametrize(
    ("mutation_sql", "parameters"),
    (
        (
            "approval_fingerprint_json = ?",
            ("{",),
        ),
        (
            "targets_json = ?",
            ("[",),
        ),
        (
            "payload_json = ?",
            ("{",),
        ),
        (
            """
            status = 'receipt_recorded',
            receipt_id = 'receipt-malformed',
            receipt_json = ?
            """,
            ("{",),
        ),
        (
            """
            status = 'reconciled',
            receipt_id = 'receipt-valid',
            receipt_json = '{}',
            readback_json = ?
            """,
            ("{",),
        ),
    ),
    ids=(
        "fingerprint",
        "targets",
        "payload",
        "receipt",
        "readback",
    ),
)
def test_validation_migration_quarantines_semantic_json_invalidity_once(
    operation_env,
    monkeypatch,
    mutation_sql,
    parameters,
):
    conn = operation_env["conn"]
    conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            command_revision, targets_json, payload_json, status,
            created_at, updated_at, guard_revision, canonical_action,
            batch_items_json, readback_kind, attempt_id,
            lease_generation, fencing_token,
            remote_idempotency_supported
        ) VALUES (
            'migration-invalid', ?, ?, 'migration-invalid-key',
            1, '["c:/work/operations/file.py"]', '{}', 'approved',
            1, 1, 1, 'local_code_edit', '["item"]', 'ledger', ?,
            1, 1, 1
        )
        """,
        (
            operation_env["project_id"],
            operation_env["turn"].turn_id,
            operation_env["claim"].attempt_id,
        ),
    )
    conn.execute("DROP TRIGGER trg_project_operations_task6_update")
    conn.execute(
        f"""
        UPDATE project_operations
        SET {mutation_sql}
        WHERE operation_id = 'migration-invalid'
        """,
        parameters,
    )
    conn.execute(
        """
        UPDATE project_operation_maintenance
        SET operation_validation_migration_complete = 0
        WHERE singleton = 1
        """
    )
    conn.commit()

    prdb.ensure_schema(conn)

    assert conn.execute(
        """
        SELECT guard_validated
        FROM project_operations
        WHERE operation_id = 'migration-invalid'
        """
    ).fetchone()[0] == 0
    assert conn.execute(
        """
        SELECT operation_validation_migration_complete
        FROM project_operation_maintenance
        WHERE singleton = 1
        """
    ).fetchone()[0] == 1
    assert not prdb._turn_allows_new_unresolved_operation(
        conn,
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    before = _operation_snapshot(operation_env)
    with pytest.raises(
        operation_env["module"].ProjectOperationError
    ) as conflict:
        operation_env["guard"].prepare(
            operation_env["claim"],
            _intent(
                operation_env,
                operation_id="after-invalid",
                idempotency_key="after-invalid-key",
            ),
            policy=PolicyDecision(
                Decision.ALLOW, "policy.allow.local", "allowed"
            ),
            approval=None,
        )
    assert (
        conflict.value.code
        is operation_env[
            "module"
        ].OperationErrorCode.OPERATION_STATE_CONFLICT
    )
    assert before == _operation_snapshot(operation_env)

    mapped = []
    original_certifier = prdb._certify_project_operation_row

    def recording_certifier(conn, row):
        mapped.append(row["operation_id"])
        return original_certifier(conn, row)

    monkeypatch.setattr(
        prdb,
        "_certify_project_operation_row",
        recording_certifier,
    )
    prdb.ensure_schema(conn)
    assert mapped == []


def test_raw_marker_zero_critical_row_is_durable_and_blocks_authority(
    operation_env,
):
    conn = operation_env["conn"]
    fingerprint = (
        '{"approval_class":"publish","approval_id":"provisional-approval",'
        '"authorization_actor_id":"owner-1","expires_at":1000,'
        '"requires_owner":true}'
    )
    conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            command_revision, targets_json, payload_json, status,
            created_at, updated_at, guard_revision, guard_validated,
            canonical_action, batch_items_json, readback_kind, attempt_id,
            lease_generation, fencing_token,
            remote_idempotency_supported, approval_fingerprint_json
        ) VALUES (
            'provisional-critical', ?, ?, 'provisional-critical-key',
            1, '["c:/work/operations/file.py"]', '{}', 'approved',
            1, 1, 1, 0, 'publish', '["publish"]', 'ledger', ?,
            1, 1, 1, ?
        )
        """,
        (
            operation_env["project_id"],
            operation_env["turn"].turn_id,
            operation_env["claim"].attempt_id,
            fingerprint,
        ),
    )
    conn.commit()
    before = _operation_snapshot(operation_env)

    with pytest.raises(
        operation_env["module"].ProjectOperationError
    ) as conflict:
        operation_env["guard"].prepare(
            operation_env["claim"],
            _intent(
                operation_env,
                operation_id="after-provisional",
                idempotency_key="after-provisional-key",
            ),
            policy=PolicyDecision(
                Decision.ALLOW, "policy.allow.local", "allowed"
            ),
            approval=None,
        )

    assert (
        conflict.value.code
        is operation_env[
            "module"
        ].OperationErrorCode.OPERATION_STATE_CONFLICT
    )
    assert before == _operation_snapshot(operation_env)
    _park_raw_operation_turn(operation_env)
    assert prdb._operation_pending_candidates(conn, limit=1) == ()
    reopened = projects_db.connect(
        Path(
            conn.execute("PRAGMA database_list").fetchone()["file"]
        )
    )
    try:
        assert reopened.execute(
            """
            SELECT guard_validated
            FROM project_operations
            WHERE operation_id = 'provisional-critical'
            """
        ).fetchone()[0] == 0
    finally:
        reopened.close()


def test_validation_migration_quarantines_malformed_linked_approval_pair(
    operation_env,
):
    _prepare_critical(operation_env)
    conn = operation_env["conn"]
    operation = prdb._project_operation_for_id(
        conn,
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    assert operation is not None
    prdb._decertify_project_operation(conn, operation)
    conn.execute(
        """
        UPDATE project_approvals
        SET turn_expected_control_version = NULL
        WHERE approval_id = 'approval-1'
          AND operation_id = 'operation-1'
        """
    )
    conn.execute(
        """
        UPDATE project_operation_maintenance
        SET operation_validation_migration_complete = 0
        WHERE singleton = 1
        """
    )
    conn.commit()

    prdb.ensure_schema(conn)

    assert conn.execute(
        """
        SELECT guard_validated
        FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()[0] == 0
    assert not prdb._turn_allows_new_unresolved_operation(
        conn,
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )


def test_validation_migration_storage_failure_rolls_back_completion(
    operation_env,
    monkeypatch,
):
    _insert_raw_unresolved_operation(operation_env)
    conn = operation_env["conn"]
    conn.execute(
        """
        UPDATE project_operation_maintenance
        SET operation_validation_migration_complete = 0
        WHERE singleton = 1
        """
    )
    conn.commit()

    def fail_certification(conn, row):
        raise sqlite3.OperationalError(
            "simulated operation certification storage failure"
        )

    monkeypatch.setattr(
        prdb,
        "_certify_project_operation_row",
        fail_certification,
    )
    with pytest.raises(
        prdb.OperationMigrationError,
        match="operation validation migration",
    ):
        prdb.ensure_schema(conn)

    assert conn.execute(
        """
        SELECT operation_validation_migration_complete
        FROM project_operation_maintenance
        WHERE singleton = 1
        """
    ).fetchone()[0] == 0
    assert conn.execute(
        """
        SELECT guard_validated
        FROM project_operations
        WHERE operation_id = 'current-unresolved'
        """
    ).fetchone()[0] == 0


def test_migration_rejects_duplicate_legacy_operation_approval_links(
    operation_conn,
):
    project_id = _insert_project(operation_conn)
    operation_conn.execute(
        "DROP INDEX idx_project_approvals_one_operation"
    )
    operation_conn.execute("PRAGMA foreign_keys=OFF")
    operation_conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, command_revision, targets_json,
            payload_json, status, created_at, updated_at
        ) VALUES ('legacy-operation', ?, 1, '[]', '{}', 'intent', 1, 1)
        """,
        (project_id,),
    )
    for ordinal in (1, 2):
        operation_conn.execute(
            """
            INSERT INTO project_approvals (
                approval_id, project_id, turn_id, operation_id,
                operation_maintenance_seq, actor_id,
                authorization_actor_id, canonical_action, approval_class,
                command_revision, expected_runtime_version,
                effective_runtime_version, turn_expected_control_version,
                expected_lifecycle, expected_phase, targets_json,
                batch_boundary_json, status, expires_at, resolved_at,
                resolved_by_actor_id, consumed_at, created_at
            ) VALUES (
                ?, ?, NULL, 'legacy-operation', ?, 'owner', 'owner', 'publish',
                'publish', ?, 0, 0, NULL, 'active', 'implementation',
                ?, ?, 'pending', 100, NULL, NULL, NULL, 1
            )
            """,
            (
                    f"approval-{ordinal}",
                    project_id,
                    ordinal,
                    ordinal,
                f'["C:/target-{ordinal}"]',
                (
                    '{"authorization_actor_id":"owner",'
                    '"batch_id":"batch-'
                    f'{ordinal}",'
                    '"batch_items":["item"],'
                    '"canonical_action":"publish",'
                    '"expected_lifecycle":"active",'
                    '"expected_phase":"implementation",'
                    '"expected_runtime_version":0}'
                ),
            ),
        )
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=ON")

    with pytest.raises(
        prdb.OperationMigrationError,
        match="duplicate approval operation links",
    ):
        prdb.ensure_schema(operation_conn)


class _StringImpostor(str):
    pass


class _StatusImpostor(str, Enum):
    APPROVED = "approved"


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param({"status": _StatusImpostor.APPROVED}, id="enum-status"),
        pytest.param({"lease_generation": True}, id="boolean-generation"),
        pytest.param(
            {"operation_id": _StringImpostor("operation")},
            id="string-subclass-id",
        ),
    ],
)
def test_operation_mapper_rejects_exact_type_impostors(mutation):
    row = {
        "operation_id": "operation",
        "project_id": "project",
        "turn_id": "turn",
        "idempotency_key": "remote-key",
        "approval_id": None,
        "command_revision": 1,
        "targets_json": '["C:/work"]',
        "payload_json": "{}",
        "status": "approved",
        "receipt_json": None,
        "created_at": 1,
        "updated_at": 1,
        "guard_revision": 1,
        "guard_validated": 1,
        "canonical_action": "local_code_edit",
        "batch_items_json": '["item"]',
        "readback_kind": "ledger",
        "attempt_id": "attempt",
        "lease_generation": 1,
        "fencing_token": 1,
        "receipt_id": None,
        "readback_json": None,
        "blocked_reason": None,
        "remote_idempotency_supported": 1,
        "approval_fingerprint_json": None,
    }
    row.update(mutation)

    with pytest.raises(RuntimeError, match="malformed persisted project operation"):
        prdb.project_operation_from_row(row)


def _intent(
    operation_env,
    *,
    operation_id="operation-1",
    idempotency_key="remote-operation-1",
    canonical_action="local_code_edit",
    targets=("C:/work/operations/file.py",),
    batch_items=("write-file",),
    payload=None,
    readback_kind="remote-ledger",
    remote_idempotency_supported=True,
):
    module = operation_env["module"]
    return module.OperationIntent(
        operation_id=operation_id,
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
        idempotency_key=idempotency_key,
        canonical_action=canonical_action,
        command_revision=1,
        targets=targets,
        batch_items=batch_items,
        payload=payload or {"content_digest": "sha256:abc"},
        readback_kind=readback_kind,
        remote_idempotency_supported=(
            remote_idempotency_supported
        ),
    )


def _operation_snapshot(operation_env):
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    return (
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_operations
                WHERE project_id = ?
                ORDER BY operation_id
                """,
                (project_id,),
            )
        ),
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
        tuple(
            conn.execute(
                """
                SELECT * FROM project_turns
                WHERE project_id = ?
                """,
                (project_id,),
            ).fetchone()
        ),
        tuple(
            conn.execute(
                """
                SELECT * FROM project_run_controls
                WHERE project_id = ?
                """,
                (project_id,),
            ).fetchone()
        ),
    )


def test_allowed_prepare_persists_one_canonical_intent_and_exact_replay_is_write_free(
    operation_env,
):
    guard = operation_env["guard"]
    intent = _intent(operation_env)
    policy = PolicyDecision(
        Decision.ALLOW,
        "policy.allow.local",
        "inside the approved project plan",
    )

    prepared = guard.prepare(
        operation_env["claim"],
        intent,
        policy=policy,
        approval=None,
    )
    after_first = _operation_snapshot(operation_env)
    replay = guard.prepare(
        operation_env["claim"],
        replace(intent),
        policy=policy,
        approval=None,
    )

    assert prepared == replay
    assert prepared.status == "approved"
    assert prepared.targets == ("c:/work/operations/file.py",)
    assert prepared.batch_items == ("write-file",)
    assert prepared.attempt_id == operation_env["claim"].attempt_id
    assert prepared.lease_generation == 1
    assert prepared.fencing_token == 1
    assert prepared.approval_id is None
    assert prepared.receipt_id is None
    assert prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version == 3
    event = operation_env["conn"].execute(
        """
        SELECT kind, payload_json
        FROM project_events
        WHERE project_id = ?
        ORDER BY sequence DESC
        LIMIT 1
        """,
        (operation_env["project_id"],),
    ).fetchone()
    assert tuple(event) == (
        "operation.intent_recorded",
        (
            '{"operation_id":"operation-1","status":"approved",'
            f'"turn_id":"{operation_env["turn"].turn_id}","version":3}}'
        ),
    )
    assert after_first == _operation_snapshot(operation_env)


@pytest.mark.parametrize(
    ("case", "expected_code"),
    (
        ("deny", "OPERATION_POLICY_DENIED"),
        ("none-policy", "INVALID_OPERATION_ARGUMENT"),
        ("malformed-policy", "INVALID_OPERATION_ARGUMENT"),
        ("allow-with-spec", "INVALID_OPERATION_ARGUMENT"),
        ("allow-with-class", "INVALID_OPERATION_ARGUMENT"),
        ("require-without-spec", "INVALID_OPERATION_ARGUMENT"),
        ("require-class-mismatch", "INVALID_OPERATION_ARGUMENT"),
        ("require-for-noncritical", "INVALID_OPERATION_ARGUMENT"),
        ("require-nonowner", "INVALID_OPERATION_ARGUMENT"),
        ("require-malformed-spec", "INVALID_OPERATION_ARGUMENT"),
    ),
)
def test_existing_allow_replay_validates_policy_before_return_write_free(
    operation_env,
    monkeypatch,
    case,
    expected_code,
):
    module = operation_env["module"]
    intent = _intent(operation_env)
    operation_env["guard"].prepare(
        operation_env["claim"],
        intent,
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    before = _operation_snapshot(operation_env)
    valid_spec = module.OperationApprovalSpec(
        "approval-replay",
        "publish",
        1_000,
        operation_env["actor"],
    )
    policy: object = PolicyDecision(
        Decision.ALLOW, "policy.allow.local", "allowed"
    )
    approval: object = None
    if case == "deny":
        policy = PolicyDecision(
            Decision.DENY, "policy.deny", "denied"
        )
    elif case == "none-policy":
        policy = None
    elif case == "malformed-policy":
        policy = PolicyDecision(
            "allow", "policy.allow.local", "allowed"
        )
    elif case == "allow-with-spec":
        approval = valid_spec
    elif case == "allow-with-class":
        policy = PolicyDecision(
            Decision.ALLOW,
            "policy.allow.local",
            "allowed",
            "publish",
        )
    elif case == "require-without-spec":
        policy = PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            "policy.approval.publish",
            "approval required",
            "publish",
        )
    elif case == "require-class-mismatch":
        policy = PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            "policy.approval.publish",
            "approval required",
            "publish",
        )
        approval = replace(valid_spec, approval_class="credentials")
    elif case == "require-for-noncritical":
        policy = PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            "policy.approval.publish",
            "approval required",
            "publish",
        )
        approval = valid_spec
    elif case == "require-nonowner":
        policy = PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            "policy.approval.publish",
            "approval required",
            "publish",
        )
        approval = replace(
            valid_spec,
            authorization=ActorContext(
                "owner-1", "desktop", "desktop-owner", False
            ),
        )
    else:
        policy = PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            "policy.approval.publish",
            "approval required",
            "publish",
        )
        approval = object()

    def unexpected_lookup(**_kwargs):
        raise AssertionError("invalid replay reached operation lookup")

    monkeypatch.setattr(
        operation_env["guard"],
        "_existing_operation",
        unexpected_lookup,
    )
    with pytest.raises(module.ProjectOperationError) as failure:
        operation_env["guard"].prepare(
            operation_env["claim"],
            intent,
            policy=policy,
            approval=approval,
        )

    assert failure.value.code is getattr(
        module.OperationErrorCode, expected_code
    )
    assert before == _operation_snapshot(operation_env)


def test_prepare_compares_the_full_immutable_fingerprint_without_writes(
    operation_env,
):
    module = operation_env["module"]
    guard = operation_env["guard"]
    policy = PolicyDecision(
        Decision.ALLOW, "policy.allow.local", "allowed"
    )
    intent = _intent(operation_env)
    guard.prepare(
        operation_env["claim"],
        intent,
        policy=policy,
        approval=None,
    )
    unchanged = _operation_snapshot(operation_env)
    changed_intents = (
        replace(intent, operation_id="different-operation"),
        replace(intent, turn_id="different-turn"),
        replace(intent, idempotency_key="different-remote-key"),
        replace(intent, canonical_action="local_test"),
        replace(intent, command_revision=2),
        replace(intent, targets=("C:/work/operations/other.py",)),
        replace(intent, batch_items=("different-item",)),
        replace(intent, payload={"content_digest": "sha256:different"}),
        replace(intent, readback_kind="different-ledger"),
    )
    for changed in changed_intents:
        with pytest.raises(module.ProjectOperationError) as conflict:
            guard.prepare(
                operation_env["claim"],
                changed,
                policy=policy,
                approval=None,
            )
        assert (
            conflict.value.code
            is module.OperationErrorCode.OPERATION_IDEMPOTENCY_CONFLICT
        )
        assert unchanged == _operation_snapshot(operation_env)

    changed_claim = replace(
        operation_env["claim"],
        fencing_token=operation_env["claim"].fencing_token + 1,
    )
    with pytest.raises(module.ProjectOperationError) as claim_conflict:
        guard.prepare(
            changed_claim,
            intent,
            policy=policy,
            approval=None,
        )
    assert (
        claim_conflict.value.code
        is module.OperationErrorCode.OPERATION_IDEMPOTENCY_CONFLICT
    )
    assert unchanged == _operation_snapshot(operation_env)


def test_critical_prepare_atomically_links_one_approval_and_blocks_fifo_head(
    operation_env,
):
    module = operation_env["module"]
    intent = _intent(
        operation_env,
        canonical_action="publish",
        batch_items=("backup", "publish", "healthcheck"),
    )
    spec = module.OperationApprovalSpec(
        approval_id="approval-operation-1",
        approval_class="publish",
        expires_at=1000,
        authorization=operation_env["actor"],
    )
    policy = PolicyDecision(
        Decision.REQUIRE_APPROVAL,
        "policy.approval.publish",
        "publish is critical",
        "publish",
    )

    prepared = operation_env["guard"].prepare(
        operation_env["claim"],
        intent,
        policy=policy,
        approval=spec,
    )
    after_first = _operation_snapshot(operation_env)
    replay = operation_env["guard"].prepare(
        operation_env["claim"],
        replace(intent),
        policy=policy,
        approval=spec,
    )

    assert prepared == replay
    assert prepared.status == "awaiting_approval"
    assert prepared.approval_id == "approval-operation-1"
    links = operation_env["conn"].execute(
        """
        SELECT operation.approval_id, approval.operation_id,
               approval.status, approval.batch_boundary_json,
               approval.turn_expected_control_version
        FROM project_operations AS operation
        JOIN project_approvals AS approval
          ON approval.project_id = operation.project_id
         AND approval.approval_id = operation.approval_id
        WHERE operation.operation_id = 'operation-1'
        """
    ).fetchone()
    assert tuple(links[:3]) == (
        "approval-operation-1",
        "operation-1",
        "pending",
    )
    assert (
        '"batch_id":"operation-1"'
        in links["batch_boundary_json"]
    )
    assert links["turn_expected_control_version"] == 1
    assert prdb._runtime_turn_for_project(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    ).status == "awaiting_approval"
    assert prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version == 3
    assert [
        row["kind"]
        for row in operation_env["conn"].execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ?
            ORDER BY sequence DESC LIMIT 2
            """,
            (operation_env["project_id"],),
        )
    ] == ["operation.intent_recorded", "approval.requested"]
    assert after_first == _operation_snapshot(operation_env)


def test_prepare_binds_approval_id_into_the_idempotency_fingerprint(
    operation_env,
):
    module = operation_env["module"]
    intent = _intent(operation_env, canonical_action="publish")
    policy = PolicyDecision(
        Decision.REQUIRE_APPROVAL,
        "policy.approval.publish",
        "publish is critical",
        "publish",
    )
    spec = module.OperationApprovalSpec(
        "approval-1", "publish", 1000, operation_env["actor"]
    )
    operation_env["guard"].prepare(
        operation_env["claim"],
        intent,
        policy=policy,
        approval=spec,
    )
    before = _operation_snapshot(operation_env)

    with pytest.raises(module.ProjectOperationError) as conflict:
        operation_env["guard"].prepare(
            operation_env["claim"],
            intent,
            policy=policy,
            approval=replace(spec, approval_id="approval-2"),
        )

    assert (
        conflict.value.code
        is module.OperationErrorCode.OPERATION_IDEMPOTENCY_CONFLICT
    )
    assert before == _operation_snapshot(operation_env)

    with pytest.raises(module.ProjectOperationError) as expiry_conflict:
        operation_env["guard"].prepare(
            operation_env["claim"],
            intent,
            policy=policy,
            approval=replace(spec, expires_at=2000),
        )
    assert (
        expiry_conflict.value.code
        is module.OperationErrorCode.OPERATION_IDEMPOTENCY_CONFLICT
    )
    assert before == _operation_snapshot(operation_env)


def test_missing_readback_capability_blocks_before_effect_started(
    operation_env,
):
    intent = _intent(operation_env, readback_kind=None)
    prepared = operation_env["guard"].prepare(
        operation_env["claim"],
        intent,
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )

    assert prepared.status == "blocked"
    assert prepared.blocked_reason == "operation_capability_unsupported"
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.effect_started'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 0


def test_denied_or_malformed_prepare_fails_before_any_operation_write(
    operation_env,
):
    module = operation_env["module"]
    before = _operation_snapshot(operation_env)
    denied = PolicyDecision(
        Decision.DENY, "policy.deny", "outside contract"
    )

    with pytest.raises(module.ProjectOperationError) as policy_error:
        operation_env["guard"].prepare(
            operation_env["claim"],
            _intent(operation_env),
            policy=denied,
            approval=None,
        )
    assert (
        policy_error.value.code
        is module.OperationErrorCode.OPERATION_POLICY_DENIED
    )
    assert before == _operation_snapshot(operation_env)

    with pytest.raises(module.ProjectOperationError) as invalid_key:
        operation_env["guard"].prepare(
            operation_env["claim"],
            replace(_intent(operation_env), idempotency_key=None),
            policy=PolicyDecision(
                Decision.ALLOW, "policy.allow.local", "allowed"
            ),
            approval=None,
        )
    assert (
        invalid_key.value.code
        is module.OperationErrorCode.INVALID_OPERATION_ARGUMENT
    )
    assert before == _operation_snapshot(operation_env)

    with pytest.raises(module.ProjectOperationError) as invalid_id:
        operation_env["guard"].prepare(
            operation_env["claim"],
            replace(
                _intent(operation_env),
                operation_id=_StringImpostor("operation-1"),
            ),
            policy=PolicyDecision(
                Decision.ALLOW, "policy.allow.local", "allowed"
            ),
            approval=None,
        )
    assert (
        invalid_id.value.code
        is module.OperationErrorCode.INVALID_OPERATION_ARGUMENT
    )
    assert before == _operation_snapshot(operation_env)


def _prepare_critical(
    operation_env,
    *,
    expires_at=1000,
    operation_id="operation-1",
    approval_id="approval-1",
):
    module = operation_env["module"]
    intent = _intent(
        operation_env,
        operation_id=operation_id,
        idempotency_key=f"remote-{operation_id}",
        canonical_action="publish",
    )
    spec = module.OperationApprovalSpec(
        approval_id,
        "publish",
        expires_at,
        operation_env["actor"],
    )
    prepared = operation_env["guard"].prepare(
        operation_env["claim"],
        intent,
        policy=PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            "policy.approval.publish",
            "publish is critical",
            "publish",
        ),
        approval=spec,
    )
    return prepared


def _generic_approval_request(
    operation_env,
    *,
    approval_id,
    expires_at=1_000,
):
    state = prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    )
    assert state is not None
    return prdb.ApprovalRequest(
        approval_id=approval_id,
        project_id=operation_env["project_id"],
        requester_actor_id="owner-1",
        authorization_actor_id="owner-1",
        canonical_action="publish",
        approval_class="publish",
        command_revision=1,
        expected_runtime_version=state.version,
        expected_lifecycle=state.lifecycle,
        expected_phase=state.current_phase,
        targets=(f"C:/work/operations/{approval_id}",),
        batch_id=f"batch-{approval_id}",
        batch_items=(f"item-{approval_id}",),
        status="pending",
        expires_at=expires_at,
    )


def _generic_authorization_args(request, *, now):
    return {
        "approval_id": request.approval_id,
        "project_id": request.project_id,
        "authorization_actor_id": request.authorization_actor_id,
        "canonical_action": request.canonical_action,
        "approval_class": request.approval_class,
        "command_revision": request.command_revision,
        "expected_runtime_version": request.expected_runtime_version,
        "expected_lifecycle": request.expected_lifecycle,
        "expected_phase": request.expected_phase,
        "targets": request.targets,
        "batch_id": request.batch_id,
        "batch_items": request.batch_items,
        "now": now,
    }


def _linked_authority_snapshot(operation_env):
    conn = operation_env["conn"]
    return (
        tuple(
            conn.execute(
                """
                SELECT * FROM project_operations
                WHERE operation_id = 'operation-1'
                """
            ).fetchone()
        ),
        tuple(
            conn.execute(
                """
                SELECT * FROM project_approvals
                WHERE approval_id = 'approval-1'
                """
            ).fetchone()
        ),
        prdb.runtime_state_for_project(
            conn, operation_env["project_id"]
        ),
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ?
                ORDER BY sequence
                """,
                (operation_env["project_id"],),
            )
        ),
    )


def test_generic_bulk_expiry_owns_only_unlinked_approvals(
    operation_env,
):
    _prepare_critical(operation_env, expires_at=105)
    pending = _generic_approval_request(
        operation_env,
        approval_id="generic-pending",
        expires_at=105,
    )
    approved = _generic_approval_request(
        operation_env,
        approval_id="generic-approved",
        expires_at=105,
    )
    prdb.create_approval_request(
        operation_env["conn"], pending, now=100
    )
    prdb.create_approval_request(
        operation_env["conn"], approved, now=100
    )
    assert prdb.resolve_approval(
        operation_env["conn"],
        approval_id=approved.approval_id,
        resolver=operation_env["actor"],
        outcome="approved",
        now=101,
    ) is not None
    linked_before = _linked_authority_snapshot(operation_env)

    with prdb.write_transaction(operation_env["conn"]):
        prdb._expire_approvals(operation_env["conn"], 110)

    assert tuple(
        tuple(row)
        for row in operation_env["conn"].execute(
            """
            SELECT approval_id, status
            FROM project_approvals
            WHERE approval_id LIKE 'generic-%'
            ORDER BY approval_id
            """
        )
    ) == (
        ("generic-approved", "expired"),
        ("generic-pending", "expired"),
    )
    assert _linked_authority_snapshot(operation_env) == linked_before


def test_generic_resolve_and_consume_are_filtered_from_linked_authority(
    operation_env,
):
    _prepare_critical(operation_env)
    linked_before = _linked_authority_snapshot(operation_env)
    assert prdb.resolve_approval(
        operation_env["conn"],
        approval_id="approval-1",
        resolver=operation_env["actor"],
        outcome="approved",
        now=100,
    ) is None
    assert _linked_authority_snapshot(operation_env) == linked_before

    generic = _generic_approval_request(
        operation_env, approval_id="generic-target"
    )
    prdb.create_approval_request(
        operation_env["conn"], generic, now=100
    )
    traced = []
    operation_env["conn"].set_trace_callback(traced.append)
    try:
        assert prdb.resolve_approval(
            operation_env["conn"],
            approval_id=generic.approval_id,
            resolver=operation_env["actor"],
            outcome="approved",
            now=101,
        ) is not None
        assert prdb.consume_approval_authorization(
            operation_env["conn"],
            **_generic_authorization_args(generic, now=102),
        )
    finally:
        operation_env["conn"].set_trace_callback(None)
    approval_statements = [
        " ".join(statement.lower().split())
        for statement in traced
        if "project_approvals" in statement.lower()
        and (
            statement.lstrip().upper().startswith("UPDATE")
            or statement.lstrip().upper().startswith("SELECT")
        )
    ]
    assert approval_statements
    assert all(
        "operation_id is null" in statement
        for statement in approval_statements
    )
    assert _linked_authority_snapshot(operation_env) == linked_before


def test_linked_approval_id_never_replays_through_generic_apis(
    operation_env,
):
    _prepare_critical(operation_env)
    conn = operation_env["conn"]
    row = conn.execute(
        """
        SELECT * FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()
    request = prdb._approval_from_row(row)
    linked_before = _linked_authority_snapshot(operation_env)

    with pytest.raises(prdb.ApprovalConflictError):
        prdb.create_approval_request(conn, request, now=100)

    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )
    with pytest.raises(
        runtime_module.ProjectRuntimeError
    ) as conflict:
        operation_env["runtime"].request_turn_approval(
            operation_env["turn"].turn_id,
            request,
            operation_env["actor"],
            expected_control_version=(
                row["turn_expected_control_version"]
            ),
        )
    assert (
        conflict.value.code
        is runtime_module.RuntimeErrorCode.APPROVAL_CONFLICT
    )
    assert _linked_authority_snapshot(operation_env) == linked_before


def test_generic_consume_cannot_consume_linked_approved_authority(
    operation_env,
):
    _prepare_critical(operation_env)
    operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["actor"],
        outcome="approved",
    )
    row = operation_env["conn"].execute(
        """
        SELECT * FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()
    request = prdb._approval_from_row(row)
    before = _transition_authority_snapshot(operation_env)

    assert not prdb.consume_approval_authorization(
        operation_env["conn"],
        **_generic_authorization_args(request, now=101),
    )
    assert _transition_authority_snapshot(operation_env) == before


def test_private_linker_refuses_linked_approval_on_valid_claimed_turn(
    operation_env_not_started,
):
    _prepare_critical(operation_env_not_started)
    claim = operation_env_not_started["claim"]
    operation_env_not_started["conn"].execute(
        """
        UPDATE project_turns SET status = 'claimed'
        WHERE project_id = ? AND turn_id = ?
        """,
        (
            operation_env_not_started["project_id"],
            operation_env_not_started["turn"].turn_id,
        ),
    )
    operation_env_not_started["conn"].commit()
    turn = prdb._runtime_turn_for_project(
        operation_env_not_started["conn"],
        project_id=operation_env_not_started["project_id"],
        turn_id=operation_env_not_started["turn"].turn_id,
    )
    assert turn is not None
    assert turn.status == "claimed"
    before = _transition_authority_snapshot(
        operation_env_not_started
    )

    assert not prdb._link_approval_to_claimed_turn(
        operation_env_not_started["conn"],
        approval_id="approval-1",
        project_id=operation_env_not_started["project_id"],
        turn_id=turn.turn_id,
        expected_attempt_id=claim.attempt_id,
        expected_lease_generation=claim.lease_generation,
        expected_fencing_token=claim.fencing_token,
        now=101,
    )
    assert (
        _transition_authority_snapshot(operation_env_not_started)
        == before
    )


def _secondary_claimed_turn(operation_env):
    conn = operation_env["conn"]
    project_id = projects_db.create_project(
        conn,
        name="Secondary claimed project",
        folders=("C:/work/secondary-claimed",),
    )
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="secondary-session",
        current_phase="implementation",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="secondary-owner",
        project_id=project_id,
        surface="desktop",
        external_binding_id="secondary-window",
        actor_id="owner-1",
        now=1,
    )
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )
    runtime = runtime_module.ProjectRuntime(
        conn, clock=lambda: operation_env["now"][0]
    )
    actor = ActorContext(
        "owner-1", "desktop", "secondary-owner", True
    )
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "secondary approval"},
        actor,
        idempotency_key="secondary-turn",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id, "secondary-worker", lease_seconds=30
    )
    assert claim is not None
    return runtime, actor, turn, claim


def _claimed_project_snapshot(conn, project_id, turn_id):
    return (
        prdb.runtime_state_for_project(conn, project_id),
        prdb._runtime_turn_for_project(
            conn, project_id=project_id, turn_id=turn_id
        ),
        prdb._runtime_control_for_turn(
            conn, project_id=project_id, turn_id=turn_id
        ),
        prdb._current_worker_lease_for_turn(
            conn, project_id=project_id, turn_id=turn_id
        ),
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
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_approvals
                WHERE project_id = ? ORDER BY approval_id
                """,
                (project_id,),
            )
        ),
    )


def test_linked_id_conflict_precedes_separate_valid_claimed_turn(
    operation_env,
    monkeypatch,
):
    _prepare_critical(operation_env)
    conn = operation_env["conn"]
    linked_row = conn.execute(
        """
        SELECT * FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()
    linked_request = prdb._approval_from_row(linked_row)
    runtime, actor, turn, claim = _secondary_claimed_turn(
        operation_env
    )
    state = prdb.runtime_state_for_project(conn, claim.project_id)
    assert state is not None
    request = replace(
        linked_request,
        project_id=claim.project_id,
        expected_runtime_version=state.version,
        expected_lifecycle=state.lifecycle,
        expected_phase=state.current_phase,
    )
    linked_before = _linked_authority_snapshot(operation_env)
    secondary_before = _claimed_project_snapshot(
        conn, claim.project_id, turn.turn_id
    )
    original_mapper = prdb._approval_from_row

    def reject_linked_mapping(row):
        if row["operation_id"] is not None:
            raise AssertionError(
                "generic request replay mapped a linked approval"
            )
        return original_mapper(row)

    monkeypatch.setattr(
        prdb, "_approval_from_row", reject_linked_mapping
    )
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )
    with pytest.raises(
        runtime_module.ProjectRuntimeError
    ) as conflict:
        runtime.request_turn_approval(
            turn.turn_id,
            request,
            actor,
            expected_control_version=1,
        )

    assert (
        conflict.value.code
        is runtime_module.RuntimeErrorCode.APPROVAL_CONFLICT
    )
    assert _linked_authority_snapshot(operation_env) == linked_before
    assert (
        _claimed_project_snapshot(
            conn, claim.project_id, turn.turn_id
        )
        == secondary_before
    )


def _assert_policy_failure(
    operation_env,
    *,
    approval_status,
    blocked_reason,
    event_reason,
    expected_execution_state,
    prior_version,
    prior_control_version=1,
):
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    turn_id = operation_env["turn"].turn_id
    approval = conn.execute(
        """
        SELECT status, resolved_at, resolved_by_actor_id, consumed_at
        FROM project_approvals WHERE approval_id = 'approval-1'
        """
    ).fetchone()
    operation = conn.execute(
        """
        SELECT status, blocked_reason FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    turn = prdb._runtime_turn_for_project(
        conn, project_id=project_id, turn_id=turn_id
    )
    control = prdb._runtime_control_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    )
    terminal_result_id = (
        "approval-blocked:approval-1:operation-1:"
        f"{event_reason}"
    )

    assert approval["status"] == approval_status
    if approval_status == "denied":
        assert tuple(approval)[1:] == (operation_env["now"][0], "owner-1", None)
        expected_approval_event = "approval.denied"
    else:
        assert tuple(approval)[1:] == (None, None, None)
        expected_approval_event = "approval.expired"
    assert tuple(operation) == ("blocked", blocked_reason)
    assert turn.status == "failed"
    assert turn.execution_state == expected_execution_state
    assert turn.terminal_result_id == terminal_result_id
    assert control.control_state == "terminal"
    assert control.control_version == prior_control_version + 1
    assert prdb._current_worker_lease_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    ) is None
    assert prdb.runtime_state_for_project(
        conn, project_id
    ).version == prior_version + 1
    events = list(
        conn.execute(
            """
            SELECT kind, payload_json FROM project_events
            WHERE project_id = ?
            ORDER BY sequence DESC LIMIT 3
            """,
            (project_id,),
        )
    )[::-1]
    assert [row["kind"] for row in events] == [
        expected_approval_event,
        "operation.blocked",
        "turn.failed",
    ]
    payloads = [json.loads(row["payload_json"]) for row in events]
    assert payloads[0]["reason"] == event_reason
    assert payloads[1]["reason"] == blocked_reason
    assert payloads[2]["reason"] == blocked_reason
    assert payloads[2]["terminal_result_id"] == terminal_result_id


def test_approved_operation_resolution_consumes_authority_and_duplicate_is_write_free(
    operation_env,
):
    module = operation_env["module"]
    _prepare_critical(operation_env)
    operation_env["now"][0] = 101

    approved = operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["discord_actor"],
        outcome="approved",
    )
    after_first = _operation_snapshot(operation_env)
    replay = operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["actor"],
        outcome="approved",
    )

    assert approved == replay
    assert approved.status == "approved"
    approval = operation_env["conn"].execute(
        """
        SELECT status, resolved_at, resolved_by_actor_id, consumed_at
        FROM project_approvals WHERE approval_id = 'approval-1'
        """
    ).fetchone()
    assert tuple(approval) == ("approved", 101, "owner-1", 101)
    turn = prdb._runtime_turn_for_project(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    assert turn.status == "claimed"
    assert prdb._current_worker_lease_for_turn(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    ) is not None
    assert prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version == 4
    assert [
        row["kind"]
        for row in operation_env["conn"].execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ?
            ORDER BY sequence DESC LIMIT 3
            """,
            (operation_env["project_id"],),
        )
    ][::-1] == [
        "approval.approved",
        "operation.approved",
        "turn.approval_released",
    ]
    assert after_first == _operation_snapshot(operation_env)

    with pytest.raises(module.ProjectOperationError) as opposite:
        operation_env["guard"].resolve_operation_approval(
            "approval-1",
            operation_env["actor"],
            outcome="denied",
        )
    assert (
        opposite.value.code
        is module.OperationErrorCode.OPERATION_APPROVAL_CONFLICT
    )
    assert after_first == _operation_snapshot(operation_env)


def test_live_pending_wrong_actor_binding_and_outcome_are_write_free(
    operation_env,
):
    module = operation_env["module"]
    _prepare_critical(operation_env)
    before = _operation_snapshot(operation_env)
    invalid_clicks = (
        (
            ActorContext(
                "owner-1", "desktop", "missing-binding", True
            ),
            "approved",
        ),
        (
            ActorContext(
                "other-actor", "desktop", "desktop-owner", True
            ),
            "approved",
        ),
        (
            operation_env["actor"],
            _StringImpostor("approved"),
        ),
    )

    for resolver, outcome in invalid_clicks:
        with pytest.raises(module.ProjectOperationError) as conflict:
            operation_env["guard"].resolve_operation_approval(
                "approval-1",
                resolver,
                outcome=outcome,
            )
        assert (
            conflict.value.code
            is module.OperationErrorCode.OPERATION_APPROVAL_CONFLICT
        )
        assert before == _operation_snapshot(operation_env)


def test_denied_started_operation_blocks_and_fails_turn_atomically(
    operation_env,
):
    _prepare_critical(operation_env)
    prior_version = prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version
    operation_env["now"][0] = 102

    blocked = operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["actor"],
        outcome="denied",
    )

    assert blocked.status == "blocked"
    _assert_policy_failure(
        operation_env,
        approval_status="denied",
        blocked_reason="approval_denied",
        event_reason="denied",
        expected_execution_state="started",
        prior_version=prior_version,
    )
    after = _operation_snapshot(operation_env)
    replay = operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["discord_actor"],
        outcome="denied",
    )
    assert replay == blocked
    assert after == _operation_snapshot(operation_env)


def test_denied_not_started_operation_clears_execution_metadata_legally(
    operation_env_not_started,
):
    _prepare_critical(operation_env_not_started)
    prior_version = prdb.runtime_state_for_project(
        operation_env_not_started["conn"],
        operation_env_not_started["project_id"],
    ).version
    operation_env_not_started["now"][0] = 102

    operation_env_not_started[
        "guard"
    ].resolve_operation_approval(
        "approval-1",
        operation_env_not_started["actor"],
        outcome="denied",
    )

    _assert_policy_failure(
        operation_env_not_started,
        approval_status="denied",
        blocked_reason="approval_denied",
        event_reason="denied",
        expected_execution_state=None,
        prior_version=prior_version,
    )


def test_time_expiry_via_resolve_precedes_actor_and_all_later_clicks_conflict(
    operation_env,
):
    module = operation_env["module"]
    _prepare_critical(operation_env, expires_at=110)
    prior_version = prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version
    operation_env["now"][0] = 110
    wrong_actor = ActorContext(
        "not-owner", "desktop", "missing-binding", False
    )

    expired = operation_env["guard"].resolve_operation_approval(
        "approval-1",
        wrong_actor,
        outcome="approved",
    )

    assert expired.status == "blocked"
    _assert_policy_failure(
        operation_env,
        approval_status="expired",
        blocked_reason="approval_time_expired",
        event_reason="time_expired",
        expected_execution_state="started",
        prior_version=prior_version,
    )
    final = _operation_snapshot(operation_env)
    for outcome in ("approved", "denied"):
        with pytest.raises(module.ProjectOperationError) as conflict:
            operation_env["guard"].resolve_operation_approval(
                "approval-1",
                operation_env["actor"],
                outcome=outcome,
            )
        assert (
            conflict.value.code
            is module.OperationErrorCode.OPERATION_APPROVAL_CONFLICT
        )
        assert final == _operation_snapshot(operation_env)


def test_bounded_expiry_poll_finalizes_not_started_operation_once(
    operation_env_not_started,
):
    _prepare_critical(operation_env_not_started, expires_at=105)
    prior_version = prdb.runtime_state_for_project(
        operation_env_not_started["conn"],
        operation_env_not_started["project_id"],
    ).version
    operation_env_not_started["now"][0] = 105

    expired = operation_env_not_started[
        "guard"
    ].expire_due_operation_approvals(limit=1)
    after_first = _operation_snapshot(operation_env_not_started)
    replay = operation_env_not_started[
        "guard"
    ].expire_due_operation_approvals(limit=1)

    assert len(expired) == 1
    assert expired[0].status == "blocked"
    assert replay == ()
    _assert_policy_failure(
        operation_env_not_started,
        approval_status="expired",
        blocked_reason="approval_time_expired",
        event_reason="time_expired",
        expected_execution_state=None,
        prior_version=prior_version,
    )
    assert after_first == _operation_snapshot(operation_env_not_started)


@pytest.mark.parametrize(
    "drift",
    ("runtime_version", "lifecycle", "phase", "control_version"),
)
def test_stale_boundary_finalizes_before_actor_validation(
    operation_env,
    drift,
):
    _prepare_critical(operation_env)
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    turn_id = operation_env["turn"].turn_id
    statements = {
        "runtime_version": (
            """
            UPDATE project_runtime_state SET version = version + 1
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "lifecycle": (
            """
            UPDATE project_runtime_state SET lifecycle = 'awaiting_acceptance'
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "phase": (
            """
            UPDATE project_runtime_state SET current_phase = 'verification'
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "control_version": (
            """
            UPDATE project_run_controls
            SET control_version = control_version + 1
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn_id),
        ),
    }
    statement, parameters = statements[drift]
    conn.execute(statement, parameters)
    conn.commit()
    prior_version = prdb.runtime_state_for_project(
        conn, project_id
    ).version
    prior_control_version = prdb._runtime_control_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    ).control_version
    operation_env["now"][0] = 101

    result = operation_env["guard"].resolve_operation_approval(
        "approval-1",
        ActorContext(
            "not-owner", "desktop", "missing-binding", False
        ),
        outcome="approved",
    )

    assert result.status == "blocked"
    _assert_policy_failure(
        operation_env,
        approval_status="expired",
        blocked_reason="approval_stale_boundary",
        event_reason="stale_boundary",
        expected_execution_state="started",
        prior_version=prior_version,
        prior_control_version=prior_control_version,
    )


class _OperationProbeHandle:
    def __init__(self, process):
        self.process = process
        self.stdout = queue.Queue()
        self.stderr = []
        self.threads = [
            threading.Thread(
                target=self._pump_stdout,
                name=f"operation-probe-stdout-{process.pid}",
                daemon=True,
            ),
            threading.Thread(
                target=self._pump_stderr,
                name=f"operation-probe-stderr-{process.pid}",
                daemon=True,
            ),
        ]
        for thread in self.threads:
            thread.start()

    def _pump_stdout(self):
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.stdout.put(line)
        self.stdout.put(None)

    def _pump_stderr(self):
        assert self.process.stderr is not None
        self.stderr.extend(self.process.stderr)

    def expect_ready_line(self, *, timeout):
        try:
            line = self.stdout.get(timeout=timeout)
        except queue.Empty as exc:
            raise AssertionError(
                "operation probe timed out waiting for ready"
            ) from exc
        if line is None:
            raise AssertionError(
                "operation probe exited before ready; "
                f"returncode={self.process.poll()}; "
                f"stderr={''.join(self.stderr)!r}"
            )
        return line

    def join_readers(self):
        for thread in self.threads:
            thread.join(timeout=5)
            assert not thread.is_alive(), (
                f"operation probe reader {thread.name} did not stop"
            )

    def drain_stdout(self):
        lines = []
        while True:
            try:
                line = self.stdout.get_nowait()
            except queue.Empty:
                break
            if line is not None:
                lines.append(line)
        return lines


class _OperationProbeSet:
    def __init__(self):
        self.processes = []
        self.handles = {}

    def register(self, process):
        self.processes.append(process)
        self.handles[process] = None
        handle = _OperationProbeHandle(process)
        self.handles[process] = handle
        return handle

    @staticmethod
    def _close_streams(process):
        for stream in (
            process.stdin,
            process.stdout,
            process.stderr,
        ):
            if stream is not None and not stream.closed:
                stream.close()

    def _remove(self, process):
        if process in self.processes:
            self.processes.remove(process)
        self.handles.pop(process, None)

    def discard_finished(self):
        for process in tuple(self.processes):
            if process.poll() is not None:
                self.collect(process, timeout=5)

    def collect(self, process, *, timeout):
        handle = self.handles[process]
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(
                "operation probe timed out while finishing"
            ) from exc
        if handle is not None:
            handle.join_readers()
            lines = handle.drain_stdout()
            stderr = "".join(handle.stderr)
        else:
            lines = []
            stderr = ""
        self._close_streams(process)
        self._remove(process)
        return returncode, lines, stderr

    def cleanup(self, primary_exception=None):
        errors = []
        processes = tuple(self.processes)

        for index, process in enumerate(processes):
            try:
                alive = process.poll() is None
            except BaseException as exc:
                errors.append(f"process {index} poll: {exc!r}")
                alive = True
            if alive:
                try:
                    process.kill()
                except BaseException as exc:
                    errors.append(f"process {index} kill: {exc!r}")

        for index, process in enumerate(processes):
            try:
                process.wait(timeout=5)
            except BaseException as exc:
                errors.append(f"process {index} wait: {exc!r}")

        for index, process in enumerate(processes):
            try:
                self._close_streams(process)
            except BaseException as exc:
                errors.append(f"process {index} stream close: {exc!r}")

        for index, process in enumerate(processes):
            handle = self.handles.get(process)
            if handle is None:
                continue
            for thread_index, thread in enumerate(handle.threads):
                try:
                    thread.join(timeout=5)
                    if thread.is_alive():
                        errors.append(
                            f"process {index} reader {thread_index} "
                            "did not stop"
                        )
                except BaseException as exc:
                    errors.append(
                        f"process {index} reader {thread_index}: {exc!r}"
                    )

        for process in processes:
            try:
                if process.poll() is not None:
                    self._remove(process)
            except BaseException as exc:
                errors.append(f"process final poll: {exc!r}")

        if errors:
            message = "operation probe cleanup failed: " + "; ".join(
                errors
            )
            if primary_exception is not None:
                primary_exception.add_note(message)
            else:
                raise AssertionError(message)


_OPERATION_PROBES = _OperationProbeSet()


def _start_operation_probe(
    operation_env,
    *,
    mode,
    now,
    outcome="approved",
    binding_id="desktop-owner",
    ready_timeout=15,
):
    try:
        _OPERATION_PROBES.discard_finished()
        process = subprocess.Popen(
            [
                sys.executable,
                str(OPERATION_PROBE),
                str(
                    operation_env["conn"]
                    .execute("PRAGMA database_list")
                    .fetchone()["file"]
                ),
                mode,
                str(now),
                outcome,
                binding_id,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        handle = _OPERATION_PROBES.register(process)
        line = handle.expect_ready_line(timeout=ready_timeout)
        try:
            ready = json.loads(line)
        except json.JSONDecodeError as exc:
            raise AssertionError(
                f"operation probe returned invalid ready payload: {line!r}"
            ) from exc
        if not isinstance(ready, dict) or ready.get("phase") != "ready":
            raise AssertionError(
                "operation probe returned unexpected ready payload: "
                f"{ready!r}"
            )
        return process
    except BaseException as exc:
        _OPERATION_PROBES.cleanup(exc)
        raise


def _release_operation_probe(process):
    try:
        if process.stdin is None or process.stdin.closed:
            raise ValueError("operation probe stdin is unavailable")
        process.stdin.write('{"command":"go"}\n')
        process.stdin.flush()
    except BaseException as exc:
        failure = AssertionError(
            f"operation probe release failed: {exc!r}"
        )
        _OPERATION_PROBES.cleanup(failure)
        raise failure from exc


def _finish_operation_probe(process, *, timeout=15):
    try:
        returncode, lines, stderr = _OPERATION_PROBES.collect(
            process, timeout=timeout
        )
        assert returncode == 0, stderr
        assert len(lines) == 1, lines
        try:
            return json.loads(lines[0])
        except json.JSONDecodeError as exc:
            raise AssertionError(
                "operation probe returned invalid result payload: "
                f"{lines[0]!r}"
            ) from exc
    except BaseException as exc:
        _OPERATION_PROBES.cleanup(exc)
        raise


def _run_operation_probe_race(operation_env, specifications):
    processes = []
    try:
        for specification in specifications:
            processes.append(
                _start_operation_probe(
                    operation_env,
                    mode=specification["mode"],
                    now=specification["now"],
                    outcome=specification.get(
                        "outcome", "approved"
                    ),
                    binding_id=specification.get(
                        "binding_id", "desktop-owner"
                    ),
                )
            )
        for process in processes:
            _release_operation_probe(process)
        return tuple(
            _finish_operation_probe(process)
            for process in processes
        )
    except BaseException as exc:
        _OPERATION_PROBES.cleanup(exc)
        raise


@pytest.mark.parametrize("_iteration", range(25))
@pytest.mark.parametrize(
    ("race_case", "generic_mode", "linked_mode", "now"),
    (
        ("due", "generic_expire", "expire", 105),
        ("stale", "generic_resolve", "resolve", 101),
        ("owner", "generic_consume", "resolve", 101),
    ),
)
def test_fresh_generic_helpers_cannot_cross_linked_finalization(
    operation_env,
    race_case,
    generic_mode,
    linked_mode,
    now,
    _iteration,
):
    _prepare_critical(
        operation_env,
        expires_at=105 if race_case == "due" else 1_000,
    )
    conn = operation_env["conn"]
    if race_case == "stale":
        conn.execute(
            """
            UPDATE project_runtime_state SET version = version + 1
            WHERE project_id = ?
            """,
            (operation_env["project_id"],),
        )
        conn.commit()
    before_version = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version
    before_events = conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0]

    generic_result, linked_result = _run_operation_probe_race(
        operation_env,
        (
            {"mode": generic_mode, "now": now},
            {
                "mode": linked_mode,
                "now": now,
                "binding_id": (
                    "missing-binding"
                    if race_case == "stale"
                    else "desktop-owner"
                ),
            },
        ),
    )

    if generic_mode == "generic_expire":
        assert generic_result == {"generic_completed": True}
    elif generic_mode == "generic_resolve":
        assert generic_result == {"generic_resolved": False}
    else:
        assert generic_result == {"generic_consumed": False}
    assert linked_result["operation_status"] in {
        "approved",
        "blocked",
    }
    approval = conn.execute(
        """
        SELECT status, consumed_at, operation_id
        FROM project_approvals WHERE approval_id = 'approval-1'
        """
    ).fetchone()
    operation = conn.execute(
        """
        SELECT status, blocked_reason, guard_validated
        FROM project_operations WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    if race_case == "owner":
        assert tuple(approval) == (
            "approved",
            now,
            "operation-1",
        )
        assert tuple(operation) == ("approved", None, 1)
    else:
        assert tuple(approval) == (
            "expired",
            None,
            "operation-1",
        )
        assert tuple(operation) == (
            "blocked",
            (
                "approval_time_expired"
                if race_case == "due"
                else "approval_stale_boundary"
            ),
            1,
        )
    assert prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version == before_version + 1
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == before_events + 3


@pytest.mark.parametrize("_iteration", range(5))
def test_two_fresh_generic_creators_share_one_unlinked_request(
    operation_env,
    _iteration,
):
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    before_state = prdb.runtime_state_for_project(conn, project_id)
    before_turn = prdb._runtime_turn_for_project(
        conn,
        project_id=project_id,
        turn_id=operation_env["turn"].turn_id,
    )
    before_control = prdb._runtime_control_for_turn(
        conn,
        project_id=project_id,
        turn_id=operation_env["turn"].turn_id,
    )
    before_lease = prdb._current_worker_lease_for_turn(
        conn,
        project_id=project_id,
        turn_id=operation_env["turn"].turn_id,
    )
    before_events = conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()[0]

    results = _run_operation_probe_race(
        operation_env,
        (
            {
                "mode": "generic_create",
                "now": 100,
                "outcome": "generic-race",
            },
            {
                "mode": "generic_create",
                "now": 100,
                "outcome": "generic-race",
            },
        ),
    )

    assert results == (
        {"approval_id": "generic-race"},
        {"approval_id": "generic-race"},
    )
    assert tuple(
        conn.execute(
            """
            SELECT approval_id, operation_id, turn_id, status
            FROM project_approvals
            WHERE approval_id = 'generic-race'
            """
        ).fetchone()
    ) == ("generic-race", None, None, "pending")
    assert prdb.runtime_state_for_project(conn, project_id) == before_state
    assert prdb._runtime_turn_for_project(
        conn,
        project_id=project_id,
        turn_id=operation_env["turn"].turn_id,
    ) == before_turn
    assert prdb._runtime_control_for_turn(
        conn,
        project_id=project_id,
        turn_id=operation_env["turn"].turn_id,
    ) == before_control
    assert prdb._current_worker_lease_for_turn(
        conn,
        project_id=project_id,
        turn_id=operation_env["turn"].turn_id,
    ) == before_lease
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()[0] == before_events
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_operations WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()[0] == 0


@pytest.mark.parametrize("_iteration", range(5))
@pytest.mark.parametrize(
    "generic_mode", ("generic_create", "request_turn")
)
def test_fresh_generic_create_or_request_races_guard_staging_safely(
    operation_env,
    generic_mode,
    _iteration,
):
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    turn_id = operation_env["turn"].turn_id
    before_version = prdb.runtime_state_for_project(
        conn, project_id
    ).version
    before_turn = prdb._runtime_turn_for_project(
        conn, project_id=project_id, turn_id=turn_id
    )
    before_control = prdb._runtime_control_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    )
    before_lease = prdb._current_worker_lease_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    )
    assert before_turn is not None
    assert before_control is not None
    assert before_lease is not None
    before_events = conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()[0]
    generic_specification = {
        "mode": generic_mode,
        "now": 100,
    }
    if generic_mode == "generic_create":
        generic_specification["outcome"] = "approval-1"

    generic_result, guard_result = _run_operation_probe_race(
        operation_env,
        (
            generic_specification,
            {"mode": "stage_critical", "now": 100},
        ),
    )

    approval = conn.execute(
        """
        SELECT approval_id, project_id, turn_id, operation_id,
               operation_maintenance_seq
        FROM project_approvals WHERE approval_id = 'approval-1'
        """
    ).fetchone()
    assert approval is not None
    operation = conn.execute(
        """
        SELECT approval_id, status, guard_validated
        FROM project_operations WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    successes = sum(
        "error_code" not in result
        for result in (generic_result, guard_result)
    )
    assert successes == 1
    if operation is not None:
        assert tuple(operation) == (
            "approval-1",
            "awaiting_approval",
            1,
        )
        assert tuple(approval)[2:] == (
            turn_id,
            "operation-1",
            approval["operation_maintenance_seq"],
        )
        assert (
            type(approval["operation_maintenance_seq"]) is int
            and approval["operation_maintenance_seq"] > 0
        )
        expected_event_delta = 2
        expected_version_delta = 1
        assert generic_result == {"error_code": "approval_conflict"}
        assert guard_result == {
            "operation_status": "awaiting_approval"
        }
    else:
        assert approval["operation_id"] is None
        assert approval["operation_maintenance_seq"] is None
        if generic_mode == "generic_create":
            assert approval["turn_id"] is None
            expected_event_delta = 0
            expected_version_delta = 0
        else:
            assert approval["turn_id"] == turn_id
            expected_event_delta = 1
            expected_version_delta = 1
        assert generic_result == {"approval_id": "approval-1"}
        if generic_mode == "generic_create":
            assert guard_result["error_code"] in {
                "operation_approval_conflict",
                "operation_state_conflict",
            }
        else:
            assert guard_result == {
                "error_code": "stale_turn_claim"
            }
    assert prdb.runtime_state_for_project(
        conn, project_id
    ).version == before_version + expected_version_delta
    final_turn = prdb._runtime_turn_for_project(
        conn, project_id=project_id, turn_id=turn_id
    )
    if operation is None and generic_mode == "generic_create":
        assert final_turn == before_turn
    else:
        assert final_turn == replace(
            before_turn, status="awaiting_approval"
        )
    assert prdb._runtime_control_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    ) == before_control
    assert prdb._current_worker_lease_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    ) == before_lease
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()[0] == before_events + expected_event_delta


@pytest.fixture
def recorded_operation_probe_processes(monkeypatch):
    original_popen = subprocess.Popen
    record = {"processes": [], "fail_next": False}

    def recording_popen(*args, **kwargs):
        if record["fail_next"]:
            record["fail_next"] = False
            raise OSError("operation probe spawn failed")
        process = original_popen(*args, **kwargs)
        record["processes"].append(process)
        return process

    monkeypatch.setattr(subprocess, "Popen", recording_popen)
    try:
        yield record
    finally:
        for process in record["processes"]:
            if process.poll() is None:
                process.kill()
        for process in record["processes"]:
            process.wait(timeout=5)
            for stream in (
                process.stdin,
                process.stdout,
                process.stderr,
            ):
                if stream is not None and not stream.closed:
                    stream.close()


def _assert_operation_probes_reaped(record, *, expected_count):
    processes = record["processes"]
    assert len(processes) == expected_count
    for process in processes:
        assert process.poll() is not None
        assert process.wait(timeout=5) == process.returncode
        assert all(
            stream is None or stream.closed
            for stream in (
                process.stdin,
                process.stdout,
                process.stderr,
            )
        )


def test_probe_ready_timeout_reaps_no_ready_child(
    operation_env, recorded_operation_probe_processes
):
    with pytest.raises(
        AssertionError, match="timed out waiting for ready"
    ):
        _start_operation_probe(
            operation_env,
            mode="no_ready",
            now=100,
            ready_timeout=0.2,
        )

    _assert_operation_probes_reaped(
        recorded_operation_probe_processes,
        expected_count=1,
    )


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        pytest.param(
            "early_exit", "exited before ready", id="early-exit"
        ),
        pytest.param(
            "malformed_ready",
            "invalid ready payload",
            id="malformed",
        ),
        pytest.param(
            "wrong_ready",
            "unexpected ready payload",
            id="wrong-phase",
        ),
    ],
)
def test_probe_ready_failure_reaps_existing_sibling_and_failed_child(
    operation_env,
    recorded_operation_probe_processes,
    mode,
    message,
):
    _start_operation_probe(
        operation_env,
        mode="resolve",
        now=100,
    )

    with pytest.raises(AssertionError, match=message):
        _start_operation_probe(
            operation_env,
            mode=mode,
            now=100,
            ready_timeout=2,
        )

    _assert_operation_probes_reaped(
        recorded_operation_probe_processes,
        expected_count=2,
    )


def test_probe_partial_spawn_failure_reaps_existing_sibling(
    operation_env, recorded_operation_probe_processes
):
    _start_operation_probe(
        operation_env,
        mode="resolve",
        now=100,
    )
    recorded_operation_probe_processes["fail_next"] = True

    with pytest.raises(OSError, match="operation probe spawn failed"):
        _start_operation_probe(
            operation_env,
            mode="expire",
            now=100,
        )

    _assert_operation_probes_reaped(
        recorded_operation_probe_processes,
        expected_count=1,
    )


def test_probe_release_failure_reaps_whole_childset(
    operation_env, recorded_operation_probe_processes
):
    processes = [
        _start_operation_probe(
            operation_env,
            mode="resolve",
            now=100,
        ),
        _start_operation_probe(
            operation_env,
            mode="expire",
            now=100,
        ),
    ]
    assert processes[0].stdin is not None
    processes[0].stdin.close()

    with pytest.raises(AssertionError, match="release failed"):
        _release_operation_probe(processes[0])

    _assert_operation_probes_reaped(
        recorded_operation_probe_processes,
        expected_count=2,
    )


def test_probe_finish_timeout_reaps_whole_childset(
    operation_env, recorded_operation_probe_processes
):
    processes = [
        _start_operation_probe(
            operation_env,
            mode="resolve",
            now=100,
        ),
        _start_operation_probe(
            operation_env,
            mode="expire",
            now=100,
        ),
    ]

    with pytest.raises(
        AssertionError, match="timed out while finishing"
    ):
        _finish_operation_probe(processes[0], timeout=0.2)

    _assert_operation_probes_reaped(
        recorded_operation_probe_processes,
        expected_count=2,
    )


@pytest.mark.parametrize("_iteration", range(25))
def test_resolve_versus_expiry_process_race_has_one_terminal_policy(
    operation_env, _iteration
):
    _prepare_critical(operation_env, expires_at=105)
    conn = operation_env["conn"]
    before_version = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version
    before_events = conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0]
    processes = [
        _start_operation_probe(
            operation_env, mode="resolve", now=105
        ),
        _start_operation_probe(
            operation_env, mode="expire", now=105
        ),
    ]
    try:
        for process in processes:
            _release_operation_probe(process)
        results = [
            _finish_operation_probe(process)
            for process in processes
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    approval = conn.execute(
        """
        SELECT status FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()[0]
    assert approval == "expired"
    assert sum(
        result.get("operation_status") == "blocked"
        for result in results
    ) == 1
    assert prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version == before_version + 1
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == before_events + 3


@pytest.mark.parametrize("_iteration", range(25))
def test_two_fresh_stale_pollers_finalize_once_per_epoch(
    operation_env,
    _iteration,
):
    _prepare_critical(operation_env, expires_at=1_000)
    conn = operation_env["conn"]
    conn.execute(
        """
        UPDATE project_runtime_state
        SET version = version + 1
        WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    )
    conn.execute(
        """
        UPDATE project_operation_maintenance
        SET next_lane = 1,
            approval_scan_after_seq = 0,
            approval_scan_high_water_seq = 0
        WHERE singleton = 1
        """
    )
    conn.commit()
    before_version = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version
    before_events = conn.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0]
    processes = [
        _start_operation_probe(
            operation_env, mode="expire", now=100
        )
        for _ in range(2)
    ]
    try:
        for process in processes:
            _release_operation_probe(process)
        results = [
            _finish_operation_probe(process)
            for process in processes
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    assert sum(result["count"] for result in results) == 1
    assert conn.execute(
        """
        SELECT status FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()[0] == "expired"
    assert tuple(
        conn.execute(
            """
            SELECT status, blocked_reason
            FROM project_operations
            WHERE operation_id = 'operation-1'
            """
        ).fetchone()
    ) == ("blocked", "approval_stale_boundary")
    assert prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version == before_version + 1
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == before_events + 3


def test_resolve_versus_stale_process_race_has_one_terminal_policy(
    operation_env,
):
    _prepare_critical(operation_env)
    conn = operation_env["conn"]
    conn.execute(
        """
        UPDATE project_runtime_state SET version = version + 1
        WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    )
    conn.commit()
    before_version = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version
    before_events = conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0]
    processes = [
        _start_operation_probe(
            operation_env, mode="resolve", now=101
        ),
        _start_operation_probe(
            operation_env,
            mode="resolve",
            now=101,
            binding_id="missing-binding",
        ),
    ]
    try:
        for process in processes:
            _release_operation_probe(process)
        results = [
            _finish_operation_probe(process)
            for process in processes
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    assert sum(
        result.get("operation_status") == "blocked"
        for result in results
    ) == 1
    assert conn.execute(
        """
        SELECT status FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()[0] == "expired"
    assert prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version == before_version + 1
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == before_events + 3


def test_resolution_process_crash_before_and_after_commit_is_durable(
    operation_env,
):
    _prepare_critical(operation_env)
    conn = operation_env["conn"]
    before = _operation_snapshot(operation_env)
    before_crash = _start_operation_probe(
        operation_env, mode="crash_before", now=101
    )
    _release_operation_probe(before_crash)
    _finish_crashed_probe(before_crash, 71)
    assert before == _operation_snapshot(operation_env)

    after_crash = _start_operation_probe(
        operation_env, mode="crash_after", now=101
    )
    _release_operation_probe(after_crash)
    _finish_crashed_probe(after_crash, 72)
    operation = prdb._project_operation_for_id(
        conn,
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    assert operation is not None
    assert operation.status == "approved"
    approval = conn.execute(
        """
        SELECT status, consumed_at FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()
    assert tuple(approval) == ("approved", 101)


def test_mapper_accepts_authoritative_not_applied_evidence_on_approved_operation():
    row = {
        "operation_id": "operation",
        "project_id": "project",
        "turn_id": "turn",
        "idempotency_key": "remote-key",
        "approval_id": None,
        "command_revision": 1,
        "targets_json": '["c:/work"]',
        "payload_json": "{}",
        "status": "approved",
        "receipt_json": None,
        "created_at": 1,
        "updated_at": 2,
        "guard_revision": 1,
        "guard_validated": 1,
        "canonical_action": "local_code_edit",
        "batch_items_json": '["item"]',
        "readback_kind": "ledger",
        "attempt_id": "attempt",
        "lease_generation": 1,
        "fencing_token": 1,
        "receipt_id": None,
        "readback_json": (
            '{"evidence":{"remote_history":"complete_absence"},'
            '"outcome":"not_applied"}'
        ),
        "blocked_reason": None,
        "remote_idempotency_supported": 1,
        "approval_fingerprint_json": None,
    }

    operation = prdb.project_operation_from_row(row)

    assert operation.status == "approved"
    assert operation.readback_json == row["readback_json"]


def test_live_approved_operation_lease_is_not_stolen(operation_env):
    _prepare_critical(operation_env)
    operation_env["now"][0] = 101
    operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["actor"],
        outcome="approved",
    )
    before = _operation_snapshot(operation_env)

    replacement = operation_env[
        "guard"
    ]._rehydrate_approved_operation(
        operation_env["project_id"],
        "operation-1",
        worker_id="replacement",
        lease_seconds=30,
    )

    assert replacement is None
    assert before == _operation_snapshot(operation_env)


def test_expired_approved_operation_gets_only_one_fresh_fenced_claim(
    operation_env,
):
    module = importlib.import_module("hermes_cli.project_runtime")
    old_claim = operation_env["claim"]
    _prepare_critical(operation_env)
    operation_env["now"][0] = 131
    approved = operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["actor"],
        outcome="approved",
    )
    assert approved.status == "approved"
    assert prdb._runtime_turn_for_project(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    ).status == "awaiting_approval"

    fresh = operation_env["guard"]._rehydrate_approved_operation(
        operation_env["project_id"],
        "operation-1",
        worker_id="worker-2",
        lease_seconds=30,
    )

    assert fresh is not None
    assert fresh.attempt_id != old_claim.attempt_id
    assert fresh.worker_id == "worker-2"
    assert fresh.lease_generation == old_claim.lease_generation + 1
    assert fresh.fencing_token == old_claim.fencing_token + 1
    assert fresh.lease_expires_at == 161
    turn = prdb._runtime_turn_for_project(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    operation = prdb._project_operation_for_id(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    assert turn.status == "claimed"
    assert turn.execution_state == "not_started"
    assert operation is not None
    assert (
        operation.attempt_id,
        operation.lease_generation,
        operation.fencing_token,
    ) == (
        fresh.attempt_id,
        fresh.lease_generation,
        fresh.fencing_token,
    )
    assert [
        row["kind"]
        for row in operation_env["conn"].execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ?
            ORDER BY sequence DESC LIMIT 2
            """,
            (operation_env["project_id"],),
        )
    ][::-1] == ["operation.rehydrated", "turn.claimed"]
    with pytest.raises(module.ProjectRuntimeError) as stale:
        operation_env["runtime"].heartbeat_turn(
            old_claim, lease_seconds=30
        )
    assert stale.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM


def test_not_applied_evidence_rehydrates_without_receipt_or_send(
    operation_env,
):
    intent = _intent(operation_env)
    operation_env["guard"].prepare(
        operation_env["claim"],
        intent,
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    conn = operation_env["conn"]
    operation = prdb._project_operation_for_id(
        conn,
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    assert operation is not None
    prdb._decertify_project_operation(conn, operation)
    conn.execute(
        """
        UPDATE project_operations
        SET readback_json = ?
        WHERE operation_id = 'operation-1'
        """,
        (
            '{"evidence":{"remote_history":"complete_absence"},'
            '"outcome":"not_applied"}',
        ),
    )
    conn.execute(
        """
        UPDATE project_turns SET status = 'reconciling'
        WHERE project_id = ? AND turn_id = ?
        """,
        (operation_env["project_id"], operation_env["turn"].turn_id),
    )
    conn.execute(
        """
        DELETE FROM project_worker_leases
        WHERE project_id = ? AND turn_id = ?
        """,
        (operation_env["project_id"], operation_env["turn"].turn_id),
    )
    prdb._certify_project_operation(
        conn,
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    conn.commit()
    before_receipts = conn.execute(
        """
        SELECT COUNT(*) FROM project_operations
        WHERE receipt_id IS NOT NULL OR receipt_json IS NOT NULL
        """
    ).fetchone()[0]

    fresh = operation_env["guard"]._rehydrate_approved_operation(
        operation_env["project_id"],
        "operation-1",
        worker_id="worker-after-readback",
        lease_seconds=20,
    )

    assert fresh is not None
    assert fresh.lease_generation == 2
    assert fresh.fencing_token == 2
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_operations
        WHERE receipt_id IS NOT NULL OR receipt_json IS NOT NULL
        """
    ).fetchone()[0] == before_receipts
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE kind = 'operation.effect_started'
        """
    ).fetchone()[0] == 0


def test_rehydration_refuses_to_skip_an_older_fifo_turn(operation_env):
    module = operation_env["module"]
    operation_env["guard"].prepare(
        operation_env["claim"],
        _intent(operation_env),
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    conn = operation_env["conn"]
    turn_id = operation_env["turn"].turn_id
    conn.execute(
        """
        UPDATE project_turns SET sequence = 2, status = 'reconciling'
        WHERE project_id = ? AND turn_id = ?
        """,
        (operation_env["project_id"], turn_id),
    )
    conn.execute(
        """
        DELETE FROM project_worker_leases
        WHERE project_id = ? AND turn_id = ?
        """,
        (operation_env["project_id"], turn_id),
    )
    conn.execute(
        """
        INSERT INTO project_turns (
            turn_id, project_id, sequence, idempotency_key, payload_json,
            origin_binding_id, status, attempt_id, lease_generation,
            fencing_token, execution_state, terminal_result_id,
            recovery_block_key, created_at, updated_at
        ) VALUES (
            'older-turn', ?, 1, 'older-key', '{}', 'desktop-owner',
            'queued', NULL, 0, 0, NULL, NULL, NULL, 1, 1
        )
        """,
        (operation_env["project_id"],),
    )
    conn.execute(
        """
        INSERT INTO project_run_controls (
            turn_id, project_id, control_state, control_version,
            idempotency_key, command_fingerprint, attempt_id,
            claim_worker_id, claim_lease_expires_at,
            claim_canonical_session_id, updated_at
        ) VALUES (
            'older-turn', ?, 'running', 0, NULL, NULL, NULL,
            NULL, NULL, NULL, 1
        )
        """,
        (operation_env["project_id"],),
    )
    conn.commit()
    before = _operation_snapshot(operation_env)

    with pytest.raises(module.ProjectOperationError) as conflict:
        operation_env["guard"]._rehydrate_approved_operation(
            operation_env["project_id"],
            "operation-1",
            worker_id="skipping-worker",
            lease_seconds=30,
        )

    assert (
        conflict.value.code
        is module.OperationErrorCode.OPERATION_STATE_CONFLICT
    )
    assert before == _operation_snapshot(operation_env)


def test_two_process_rehydrators_have_one_fenced_winner(operation_env):
    old_claim = operation_env["claim"]
    _prepare_critical(operation_env)
    operation_env["now"][0] = 131
    operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["actor"],
        outcome="approved",
    )
    conn = operation_env["conn"]
    before_version = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version
    before_events = conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0]
    processes = [
        _start_operation_probe(
            operation_env,
            mode="rehydrate",
            now=131,
            outcome=f"worker-{index}",
        )
        for index in (1, 2)
    ]
    try:
        for process in processes:
            _release_operation_probe(process)
        results = [
            _finish_operation_probe(process)
            for process in processes
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    claims = [result.get("claim") for result in results]
    assert sum(claim is not None for claim in claims) == 1
    winner = next(claim for claim in claims if claim is not None)
    assert winner["attempt_id"] != old_claim.attempt_id
    assert winner["lease_generation"] == 2
    assert winner["fencing_token"] == 2
    operation = prdb._project_operation_for_id(
        conn,
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    lease = prdb._current_worker_lease_for_turn(
        conn,
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    assert operation is not None and lease is not None
    assert operation.attempt_id == lease.lease_id == winner["attempt_id"]
    assert prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version == before_version + 1
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == before_events + 2


def _prepare_allowed(operation_env):
    return operation_env["guard"].prepare(
        operation_env["claim"],
        _intent(operation_env),
        policy=PolicyDecision(
            Decision.ALLOW,
            "policy.allow.local",
            "allowed",
        ),
        approval=None,
    )


def test_operation_marker_transitions_are_marker_only(
    operation_env,
):
    _prepare_allowed(operation_env)
    conn = operation_env["conn"]

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            UPDATE project_operations
            SET status = 'effect_started'
            WHERE operation_id = 'operation-1'
              AND guard_validated = 1
            """
        )
    conn.rollback()

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            UPDATE project_operations
            SET guard_validated = 0, status = 'effect_started'
            WHERE operation_id = 'operation-1'
              AND guard_validated = 1
            """
        )
    conn.rollback()

    conn.execute(
        """
        UPDATE project_operations
        SET guard_validated = 0
        WHERE operation_id = 'operation-1'
          AND guard_validated = 1
        """
    )
    conn.execute(
        """
        UPDATE project_operations
        SET status = 'effect_started'
        WHERE operation_id = 'operation-1'
          AND guard_validated = 0
        """
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            UPDATE project_operations
            SET guard_validated = 1, updated_at = updated_at + 1
            WHERE operation_id = 'operation-1'
              AND guard_validated = 0
            """
        )
    conn.rollback()
    assert tuple(
        conn.execute(
            """
            SELECT status, guard_validated
            FROM project_operations
            WHERE operation_id = 'operation-1'
            """
        ).fetchone()
    ) == ("approved", 1)


def test_linked_approval_mutation_requires_operation_decertification(
    operation_env,
):
    _prepare_critical(operation_env)
    conn = operation_env["conn"]

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            UPDATE project_approvals
            SET status = 'denied', resolved_at = 100,
                resolved_by_actor_id = 'owner-1'
            WHERE approval_id = 'approval-1'
              AND operation_id = 'operation-1'
            """
        )
    conn.rollback()
    assert conn.execute(
        """
        SELECT status FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()[0] == "pending"


class _Readback:
    def __init__(self, conn, result):
        self.conn = conn
        self.result = result
        self.requests = []

    def read_operation(self, request):
        assert self.conn.in_transaction is False
        self.requests.append(request)
        return self.result


def _receipt(operation_env, *, receipt_id="receipt-1", payload=None):
    return operation_env["module"].OperationReceipt(
        receipt_id,
        payload or {"provider_sequence": 7},
    )


def _readback_result(
    operation_env,
    outcome,
    *,
    evidence=None,
    receipt=None,
):
    return operation_env["module"].OperationReadbackResult(
        outcome,
        evidence,
        receipt,
    )


class _InjectedCertificationFault(RuntimeError):
    pass


def _transition_authority_snapshot(operation_env):
    return (
        _operation_snapshot(operation_env),
        tuple(
            tuple(row)
            for row in operation_env["conn"].execute(
                """
                SELECT * FROM project_worker_leases
                WHERE project_id = ?
                ORDER BY lease_id
                """,
                (operation_env["project_id"],),
            )
        ),
    )


@pytest.mark.parametrize(
    ("action", "fault_call"),
    (
        ("start", 1),
        ("receipt", 1),
        ("reconcile_phase_a", 1),
        ("reconcile_phase_c", 2),
        ("block", 1),
        ("rehydrate", 1),
        ("approve", 1),
        ("deny", 1),
        ("expire", 1),
        ("stale", 1),
    ),
)
@pytest.mark.parametrize("fault_stage", ("decertify", "certify"))
def test_operation_transition_certification_fault_rolls_back_everything(
    operation_env,
    monkeypatch,
    action,
    fault_call,
    fault_stage,
):
    guard = operation_env["guard"]
    if action in {
        "start",
        "receipt",
        "reconcile_phase_a",
        "reconcile_phase_c",
        "block",
    }:
        _prepare_allowed(operation_env)
    if action in {
        "receipt",
        "reconcile_phase_a",
        "reconcile_phase_c",
        "block",
    }:
        guard.mark_started(
            operation_env["claim"], "operation-1"
        )
    if action == "block":
        class PhaseACrash(BaseException):
            pass

        class CrashReadback:
            def read_operation(self, request):
                raise PhaseACrash

        with pytest.raises(PhaseACrash):
            guard.reconcile(
                operation_env["claim"],
                "operation-1",
                CrashReadback(),
            )
    if action in {
        "rehydrate",
        "approve",
        "deny",
        "expire",
        "stale",
    }:
        _prepare_critical(
            operation_env,
            expires_at=105 if action == "expire" else 1_000,
        )
    if action == "rehydrate":
        operation_env["now"][0] = 131
        guard.resolve_operation_approval(
            "approval-1",
            operation_env["actor"],
            outcome="approved",
        )
    if action == "expire":
        operation_env["now"][0] = 110
    if action == "stale":
        operation_env["conn"].execute(
            """
            UPDATE project_runtime_state SET version = version + 1
            WHERE project_id = ?
            """,
            (operation_env["project_id"],),
        )
        operation_env["conn"].commit()

    before = _transition_authority_snapshot(operation_env)
    phase_c_before = [None]
    calls = [0]
    if fault_stage == "decertify":
        original = prdb._decertify_project_operation

        def fail_after_decertify(conn, operation):
            result = original(conn, operation)
            calls[0] += 1
            if calls[0] == fault_call:
                raise _InjectedCertificationFault(
                    "after decertification"
                )
            return result

        monkeypatch.setattr(
            prdb,
            "_decertify_project_operation",
            fail_after_decertify,
        )
    else:
        original = prdb._certify_project_operation

        def fail_before_certify(
            conn, *, project_id, operation_id
        ):
            calls[0] += 1
            if calls[0] == fault_call:
                raise _InjectedCertificationFault(
                    "before certification"
                )
            return original(
                conn,
                project_id=project_id,
                operation_id=operation_id,
            )

        monkeypatch.setattr(
            prdb,
            "_certify_project_operation",
            fail_before_certify,
        )

    module = operation_env["module"]

    def invoke():
        if action == "start":
            return guard.mark_started(
                operation_env["claim"], "operation-1"
            )
        elif action == "receipt":
            return guard.record_receipt(
                operation_env["claim"],
                "operation-1",
                _receipt(operation_env),
            )
        elif action.startswith("reconcile"):
            class BoundaryReadback(_Readback):
                def read_operation(self, request):
                    if action == "reconcile_phase_c":
                        phase_c_before[0] = (
                            _transition_authority_snapshot(
                                operation_env
                            )
                        )
                    return super().read_operation(request)

            return guard.reconcile(
                operation_env["claim"],
                "operation-1",
                BoundaryReadback(
                    operation_env["conn"],
                    _readback_result(
                        operation_env,
                        "applied",
                        evidence={"ledger": "complete"},
                        receipt=_receipt(operation_env),
                    ),
                ),
            )
        elif action == "block":
            return guard.block_unknown(
                operation_env["claim"], "operation-1"
            )
        elif action == "rehydrate":
            return guard._rehydrate_approved_operation(
                operation_env["project_id"],
                "operation-1",
                worker_id="replacement-worker",
                lease_seconds=30,
            )
        elif action == "approve":
            return guard.resolve_operation_approval(
                "approval-1",
                operation_env["actor"],
                outcome="approved",
            )
        elif action == "deny":
            return guard.resolve_operation_approval(
                "approval-1",
                operation_env["actor"],
                outcome="denied",
            )
        elif action == "stale":
            return guard.resolve_operation_approval(
                "approval-1",
                operation_env["actor"],
                outcome="approved",
            )
        return guard.expire_due_operation_approvals(limit=1)

    if action == "expire":
        assert invoke() == ()
    else:
        with pytest.raises(module.ProjectOperationError) as conflict:
            invoke()
        expected_code = (
            module.OperationErrorCode.OPERATION_APPROVAL_CONFLICT
            if action in {"approve", "deny", "stale"}
            else module.OperationErrorCode.OPERATION_STATE_CONFLICT
        )
        assert conflict.value.code is expected_code
    assert calls[0] == fault_call
    expected_before = (
        phase_c_before[0]
        if action == "reconcile_phase_c"
        else before
    )
    assert expected_before is not None
    assert (
        _transition_authority_snapshot(operation_env)
        == expected_before
    )
    assert operation_env["conn"].execute(
        """
        SELECT guard_validated FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()[0] == 1


class _InjectedMutationFault(RuntimeError):
    pass


_AUTHORITY_MUTATION_TABLES = frozenset(
    {
        "project_operations",
        "project_approvals",
        "project_turns",
        "project_run_controls",
        "project_worker_leases",
        "project_runtime_state",
        "project_events",
    }
)


def _authority_mutation_table(statement):
    normalized = " ".join(statement.lower().split())
    for table in _AUTHORITY_MUTATION_TABLES:
        if (
            normalized.startswith(f"update {table}")
            or normalized.startswith(f"delete from {table}")
            or f" into {table} " in f" {normalized} "
        ):
            return table
    return None


class _MutationBoundaryConnection:
    def __init__(self, conn, *, fail_at=None):
        self._conn = conn
        self.fail_at = fail_at
        self.boundaries = []
        self.failed = False

    def execute(self, statement, parameters=()):
        cursor = self._conn.execute(statement, parameters)
        table = _authority_mutation_table(statement)
        if table is not None:
            self.boundaries.append(table)
            if len(self.boundaries) == self.fail_at:
                self.failed = True
                raise _InjectedMutationFault(
                    f"after {table} mutation"
                )
        return cursor

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _BoundaryPhaseACrash(BaseException):
    pass


class _BoundaryCrashReadback:
    def read_operation(self, request):
        raise _BoundaryPhaseACrash


_MUTATION_BOUNDARY_ACTIONS = (
    "prepare_allowed",
    "prepare_critical",
    "start",
    "receipt",
    "reconcile_phase_a",
    "reconcile_phase_c",
    "block",
    "rehydrate",
    "approve",
    "deny",
    "expire",
    "stale",
)


def _setup_mutation_boundary_action(operation_env, action):
    guard = operation_env["guard"]
    if action in {
        "start",
        "receipt",
        "reconcile_phase_a",
        "reconcile_phase_c",
        "block",
    }:
        _prepare_allowed(operation_env)
    if action in {
        "receipt",
        "reconcile_phase_a",
        "reconcile_phase_c",
        "block",
    }:
        guard.mark_started(operation_env["claim"], "operation-1")
    if action in {"reconcile_phase_c", "block"}:
        with pytest.raises(_BoundaryPhaseACrash):
            guard.reconcile(
                operation_env["claim"],
                "operation-1",
                _BoundaryCrashReadback(),
            )
    if action in {
        "rehydrate",
        "approve",
        "deny",
        "expire",
        "stale",
    }:
        _prepare_critical(
            operation_env,
            expires_at=105 if action == "expire" else 1_000,
        )
    if action == "rehydrate":
        operation_env["now"][0] = 131
        guard.resolve_operation_approval(
            "approval-1",
            operation_env["actor"],
            outcome="approved",
        )
    elif action == "expire":
        operation_env["now"][0] = 110
    elif action == "stale":
        operation_env["conn"].execute(
            """
            UPDATE project_runtime_state SET version = version + 1
            WHERE project_id = ?
            """,
            (operation_env["project_id"],),
        )
        operation_env["conn"].commit()
        operation_env["now"][0] = 101


def _invoke_mutation_boundary_action(operation_env, action):
    guard = operation_env["guard"]
    if action == "prepare_allowed":
        return _prepare_allowed(operation_env)
    if action == "prepare_critical":
        return _prepare_critical(operation_env)
    if action == "start":
        return guard.mark_started(
            operation_env["claim"], "operation-1"
        )
    if action == "receipt":
        return guard.record_receipt(
            operation_env["claim"],
            "operation-1",
            _receipt(operation_env),
        )
    if action == "reconcile_phase_a":
        try:
            return guard.reconcile(
                operation_env["claim"],
                "operation-1",
                _BoundaryCrashReadback(),
            )
        except _BoundaryPhaseACrash:
            return None
    if action == "reconcile_phase_c":
        return guard.reconcile(
            operation_env["claim"],
            "operation-1",
            _Readback(
                operation_env["conn"],
                _readback_result(
                    operation_env,
                    "applied",
                    evidence={"ledger": "complete"},
                    receipt=_receipt(operation_env),
                ),
            ),
        )
    if action == "block":
        return guard.block_unknown(
            operation_env["claim"], "operation-1"
        )
    if action == "rehydrate":
        return guard._rehydrate_approved_operation(
            operation_env["project_id"],
            "operation-1",
            worker_id="replacement-worker",
            lease_seconds=30,
        )
    if action in {"approve", "deny", "stale"}:
        return guard.resolve_operation_approval(
            "approval-1",
            operation_env["actor"],
            outcome="denied" if action == "deny" else "approved",
        )
    return guard.expire_due_operation_approvals(limit=1)


def _build_mutation_boundary_env(tmp_path, label, action):
    path = tmp_path / label
    path.mkdir()
    operation_env = _build_operation_env(path, started=True)
    _setup_mutation_boundary_action(operation_env, action)
    return operation_env


@pytest.mark.parametrize("action", _MUTATION_BOUNDARY_ACTIONS)
def test_every_authority_mutation_boundary_rolls_back_atomically(
    tmp_path,
    action,
):
    recorded = _build_mutation_boundary_env(
        tmp_path, f"{action}-record", action
    )
    try:
        recorder = _MutationBoundaryConnection(recorded["conn"])
        recorded["runtime"]._conn = recorder
        recorded["guard"]._conn = recorder
        _invoke_mutation_boundary_action(recorded, action)
        boundaries = tuple(recorder.boundaries)
    finally:
        recorded["conn"].close()
    assert boundaries

    for ordinal in range(1, len(boundaries) + 1):
        operation_env = _build_mutation_boundary_env(
            tmp_path, f"{action}-fault-{ordinal}", action
        )
        try:
            before = _transition_authority_snapshot(operation_env)
            faulting = _MutationBoundaryConnection(
                operation_env["conn"], fail_at=ordinal
            )
            operation_env["runtime"]._conn = faulting
            operation_env["guard"]._conn = faulting
            try:
                result = _invoke_mutation_boundary_action(
                    operation_env, action
                )
            except (
                _InjectedMutationFault,
                operation_env["module"].ProjectOperationError,
            ):
                pass
            else:
                assert action == "expire"
                assert result == ()
            assert faulting.failed
            assert (
                _transition_authority_snapshot(operation_env)
                == before
            )
            marker = operation_env["conn"].execute(
                """
                SELECT guard_validated FROM project_operations
                WHERE operation_id = 'operation-1'
                """
            ).fetchone()
            if action.startswith("prepare_"):
                assert marker is None
            else:
                assert marker[0] == 1
        finally:
            operation_env["conn"].close()


def test_critical_prepare_certification_failure_rolls_back_whole_pair(
    operation_env,
    monkeypatch,
):
    before = _transition_authority_snapshot(operation_env)

    def fail_certification(
        conn, *, project_id, operation_id
    ):
        raise _InjectedCertificationFault(
            "prepare certification failed"
        )

    monkeypatch.setattr(
        prdb,
        "_certify_project_operation",
        fail_certification,
    )
    with pytest.raises(
        operation_env["module"].ProjectOperationError
    ) as conflict:
        _prepare_critical(operation_env)
    assert (
        conflict.value.code
        is operation_env[
            "module"
        ].OperationErrorCode.OPERATION_STATE_CONFLICT
    )
    assert _transition_authority_snapshot(operation_env) == before


def test_mark_started_is_fenced_atomic_and_exact_replay_is_write_free(
    operation_env,
):
    _prepare_allowed(operation_env)
    guard = operation_env["guard"]
    conn = operation_env["conn"]
    before_version = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version

    started = guard.mark_started(
        operation_env["claim"], "operation-1"
    )
    after_first = _operation_snapshot(operation_env)
    replay = guard.mark_started(
        operation_env["claim"], "operation-1"
    )

    assert started == replay
    assert started.status == "effect_started"
    assert prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version == before_version + 1
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.effect_started'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 1
    assert after_first == _operation_snapshot(operation_env)


def test_stale_start_and_receipt_are_write_free(operation_env):
    _prepare_allowed(operation_env)
    operation_env["now"][0] = 131
    before = _operation_snapshot(operation_env)

    with pytest.raises(
        importlib.import_module(
            "hermes_cli.project_runtime"
        ).ProjectRuntimeError
    ) as stale_start:
        operation_env["guard"].mark_started(
            operation_env["claim"], "operation-1"
        )
    assert (
        stale_start.value.code
        is importlib.import_module(
            "hermes_cli.project_runtime"
        ).RuntimeErrorCode.STALE_TURN_CLAIM
    )
    assert before == _operation_snapshot(operation_env)

    with pytest.raises(
        importlib.import_module(
            "hermes_cli.project_runtime"
        ).ProjectRuntimeError
    ):
        operation_env["guard"].record_receipt(
            operation_env["claim"],
            "operation-1",
            _receipt(operation_env),
        )
    assert before == _operation_snapshot(operation_env)


def test_receipt_exact_replay_is_write_free_and_changed_receipt_conflicts(
    operation_env,
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    receipt = _receipt(operation_env)
    recorded = operation_env["guard"].record_receipt(
        operation_env["claim"], "operation-1", receipt
    )
    after_first = _operation_snapshot(operation_env)

    replay = operation_env["guard"].record_receipt(
        operation_env["claim"],
        "operation-1",
        _receipt(operation_env),
    )
    assert recorded == replay
    assert replay.status == "receipt_recorded"
    assert replay.receipt_id == "receipt-1"
    assert after_first == _operation_snapshot(operation_env)

    with pytest.raises(
        operation_env["module"].ProjectOperationError
    ) as conflict:
        operation_env["guard"].record_receipt(
            operation_env["claim"],
            "operation-1",
            _receipt(
                operation_env,
                receipt_id="receipt-2",
                payload={"provider_sequence": 8},
            ),
        )
    assert (
        conflict.value.code
        is operation_env[
            "module"
        ].OperationErrorCode.OPERATION_RECEIPT_CONFLICT
    )
    assert after_first == _operation_snapshot(operation_env)


def test_reconcile_applied_runs_port_outside_transaction_and_commits_once(
    operation_env,
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    receipt = _receipt(operation_env)
    operation_env["guard"].record_receipt(
        operation_env["claim"], "operation-1", receipt
    )
    conn = operation_env["conn"]
    port = _Readback(
        conn,
        _readback_result(
            operation_env,
            "applied",
            evidence={"ledger": "complete"},
            receipt=_receipt(operation_env),
        ),
    )
    before_version = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version

    reconciled = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-1", port
    )
    after_first = _operation_snapshot(operation_env)
    replay = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-1", port
    )

    assert reconciled == replay
    assert reconciled.status == "reconciled"
    assert reconciled.receipt_id == "receipt-1"
    assert len(port.requests) == 1
    assert port.requests[0].receipt == receipt
    assert prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    ).version == before_version + 2
    assert [
        row["kind"]
        for row in conn.execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ?
              AND kind IN ('operation.unknown', 'operation.reconciled')
            ORDER BY sequence
            """,
            (operation_env["project_id"],),
        )
    ] == ["operation.unknown", "operation.reconciled"]
    assert after_first == _operation_snapshot(operation_env)


def test_authoritative_not_applied_returns_to_approved_without_resend(
    operation_env,
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    port = _Readback(
        operation_env["conn"],
        _readback_result(
            operation_env,
            "not_applied",
            evidence={"ledger": "complete", "present": False},
            receipt=None,
        ),
    )

    approved = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-1", port
    )
    replay = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-1", port
    )

    assert approved == replay
    assert approved.status == "approved"
    assert len(port.requests) == 1
    assert approved.receipt_id is None
    assert operation_env["conn"].execute(
        """
        SELECT readback_json FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()[0] == (
        '{"evidence":{"ledger":"complete","present":false},'
        '"outcome":"not_applied"}'
    )
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.effect_started'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 1


def test_applied_readback_can_supply_the_only_canonical_receipt(
    operation_env,
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    supplied = _receipt(
        operation_env,
        receipt_id="provider-ref-1",
        payload={"nested": {"b": 2, "a": 1}},
    )
    port = _Readback(
        operation_env["conn"],
        _readback_result(
            operation_env,
            "applied",
            evidence={"ledger": "complete"},
            receipt=supplied,
        ),
    )

    reconciled = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-1", port
    )

    assert reconciled.status == "reconciled"
    assert reconciled.receipt_id == "provider-ref-1"
    assert all(
        field.name not in {"payload", "receipt", "readback"}
        for field in fields(reconciled)
    )
    row = operation_env["conn"].execute(
        """
        SELECT receipt_id, receipt_json
        FROM project_operations WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    assert tuple(row) == (
        "provider-ref-1",
        '{"nested":{"a":1,"b":2}}',
    )


class _ExplosiveOutcome(str):
    def __eq__(self, other):
        raise AssertionError("subclass equality must not run")

    def __hash__(self):
        raise AssertionError("subclass hashing must not run")


def test_readback_outcome_exact_type_is_checked_before_equality_or_hash(
    operation_env,
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    port = _Readback(
        operation_env["conn"],
        _readback_result(
            operation_env,
            _ExplosiveOutcome("applied"),
            evidence={"ledger": "complete"},
            receipt=_receipt(operation_env),
        ),
    )

    blocked = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-1", port
    )

    assert blocked.status == "blocked"
    assert blocked.blocked_reason == "operation_readback_ambiguous"


@pytest.mark.parametrize(
    "result_factory",
    [
        pytest.param(
            lambda env: _readback_result(
                env, "unknown", evidence={"ledger": "ambiguous"}
            ),
            id="unknown",
        ),
        pytest.param(
            lambda env: _readback_result(
                env, "applied", evidence={}, receipt=_receipt(env)
            ),
            id="empty-evidence",
        ),
        pytest.param(
            lambda env: _readback_result(
                env,
                _StatusImpostor.APPROVED,
                evidence={"ledger": "complete"},
                receipt=_receipt(env),
            ),
            id="outcome-type-impostor",
        ),
        pytest.param(
            lambda env: _readback_result(
                env,
                "applied",
                evidence={"ledger": "complete"},
                receipt=_receipt(env, receipt_id="different"),
            ),
            id="receipt-mismatch",
        ),
    ],
)
def test_ambiguous_or_malformed_readback_blocks_exactly_once(
    operation_env, result_factory
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    receipt = _receipt(operation_env)
    operation_env["guard"].record_receipt(
        operation_env["claim"], "operation-1", receipt
    )
    port = _Readback(
        operation_env["conn"], result_factory(operation_env)
    )

    blocked = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-1", port
    )
    after_first = _operation_snapshot(operation_env)
    replay = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-1", port
    )

    assert blocked == replay
    assert blocked.status == "blocked"
    assert blocked.blocked_reason == "operation_readback_ambiguous"
    assert len(port.requests) == 1
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.blocked'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 1
    assert after_first == _operation_snapshot(operation_env)


def test_readback_exception_is_normalized_to_one_durable_block(
    operation_env,
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )

    class ExplodingReadback:
        def read_operation(self, request):
            assert operation_env["conn"].in_transaction is False
            raise TimeoutError("provider timeout must not escape")

    blocked = operation_env["guard"].reconcile(
        operation_env["claim"],
        "operation-1",
        ExplodingReadback(),
    )

    assert blocked.status == "blocked"
    assert blocked.blocked_reason == "operation_readback_ambiguous"


def test_stale_phase_c_cannot_write_after_unknown_parking(operation_env):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )

    class ExpiringReadback:
        def read_operation(self, request):
            assert operation_env["conn"].in_transaction is False
            operation_env["now"][0] = 131
            return _readback_result(
                operation_env,
                "not_applied",
                evidence={"ledger": "complete", "present": False},
            )

    with pytest.raises(
        importlib.import_module(
            "hermes_cli.project_runtime"
        ).ProjectRuntimeError
    ) as stale:
        operation_env["guard"].reconcile(
            operation_env["claim"],
            "operation-1",
            ExpiringReadback(),
        )

    assert (
        stale.value.code
        is importlib.import_module(
            "hermes_cli.project_runtime"
        ).RuntimeErrorCode.STALE_TURN_CLAIM
    )
    row = operation_env["conn"].execute(
        """
        SELECT status, readback_json, blocked_reason
        FROM project_operations WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    assert tuple(row) == ("unknown", None, None)
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.unknown'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 1
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.blocked'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 0


def test_block_unknown_is_fenced_and_exactly_once(operation_env):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    with pytest.raises(_PhaseACrash):
        operation_env["guard"].reconcile(
            operation_env["claim"],
            "operation-1",
            _CrashAfterPhaseA(),
        )
    before_version = prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version

    blocked = operation_env["guard"].block_unknown(
        operation_env["claim"], "operation-1"
    )
    after_first = _operation_snapshot(operation_env)
    replay = operation_env["guard"].block_unknown(
        operation_env["claim"], "operation-1"
    )

    assert blocked == replay
    assert blocked.status == "blocked"
    assert prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version == before_version + 1
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.blocked'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 1
    assert after_first == _operation_snapshot(operation_env)


def _turn_result(operation_env, status, result_id):
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )
    return runtime_module.CanonicalTurnResult(status, result_id)


@pytest.mark.parametrize(
    ("operation_state", "expected"),
    [
        pytest.param("absent", "clear", id="absent"),
        pytest.param("approved", "unresolved", id="approved"),
        pytest.param("effect_started", "unresolved", id="effect-started"),
        pytest.param(
            "receipt_recorded", "unresolved", id="receipt-recorded"
        ),
        pytest.param("unknown", "unresolved", id="unknown"),
        pytest.param("reconciled", "reconciled", id="reconciled"),
        pytest.param("pre_blocked", "blocked", id="pre-effect-blocked"),
        pytest.param(
            "post_blocked", "blocked", id="post-effect-blocked"
        ),
        pytest.param("legacy", "blocked", id="legacy"),
        pytest.param("malformed", "blocked", id="malformed"),
    ],
)
def test_disposition_for_turn_is_positive_and_fail_closed(
    operation_env, operation_state, expected
):
    guard = operation_env["guard"]
    conn = operation_env["conn"]
    if operation_state == "legacy":
        conn.execute(
            """
            INSERT INTO project_operations (
                operation_id, project_id, turn_id, idempotency_key,
                command_revision, targets_json, payload_json, status,
                created_at, updated_at
            ) VALUES (
                'legacy-operation', ?, ?, 'legacy-key', 1,
                '["C:/legacy"]', '{}', 'intent', 100, 100
            )
            """,
            (
                operation_env["project_id"],
                operation_env["turn"].turn_id,
            ),
        )
        conn.commit()
    else:
        _prepare_allowed(
            operation_env
            if operation_state != "pre_blocked"
            else {
                **operation_env,
                "guard": guard,
            }
        ) if operation_state not in {
            "absent",
            "pre_blocked",
        } else None
        if operation_state == "pre_blocked":
            guard.prepare(
                operation_env["claim"],
                _intent(operation_env, readback_kind=None),
                policy=PolicyDecision(
                    Decision.ALLOW,
                    "policy.allow.local",
                    "allowed",
                ),
                approval=None,
            )
        elif operation_state in {
            "effect_started",
            "receipt_recorded",
            "unknown",
            "reconciled",
            "post_blocked",
        }:
            guard.mark_started(
                operation_env["claim"], "operation-1"
            )
            if operation_state in {
                "receipt_recorded",
                "reconciled",
            }:
                guard.record_receipt(
                    operation_env["claim"],
                    "operation-1",
                    _receipt(operation_env),
                )
            if operation_state == "unknown":
                with pytest.raises(_PhaseACrash):
                    guard.reconcile(
                        operation_env["claim"],
                        "operation-1",
                        _CrashAfterPhaseA(),
                    )
            elif operation_state == "reconciled":
                guard.reconcile(
                    operation_env["claim"],
                    "operation-1",
                    _Readback(
                        conn,
                        _readback_result(
                            operation_env,
                            "applied",
                            evidence={"ledger": "complete"},
                            receipt=_receipt(operation_env),
                        ),
                    ),
                )
            elif operation_state == "post_blocked":
                guard.reconcile(
                    operation_env["claim"],
                    "operation-1",
                    _Readback(
                        conn,
                        _readback_result(
                            operation_env,
                            "unknown",
                            evidence={"ledger": "ambiguous"},
                        ),
                    ),
                )
        if operation_state == "malformed":
            operation = prdb._project_operation_for_id(
                conn,
                project_id=operation_env["project_id"],
                operation_id="operation-1",
            )
            assert operation is not None
            prdb._decertify_project_operation(conn, operation)
            conn.execute(
                """
                UPDATE project_operations
                SET targets_json = '["not-canonical", "not-canonical"]'
                WHERE operation_id = 'operation-1'
                """
            )
            conn.commit()

    assert guard.disposition_for_turn(
        operation_env["project_id"],
        operation_env["turn"].turn_id,
    ) == expected


def test_commit_rejects_unresolved_operations_for_both_outcomes_without_writes(
    operation_env,
):
    _prepare_allowed(operation_env)
    before = _operation_snapshot(operation_env)
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )

    for status in ("succeeded", "failed"):
        with pytest.raises(
            runtime_module.ProjectRuntimeError
        ) as unresolved:
            operation_env["runtime"].commit_turn(
                operation_env["claim"],
                _turn_result(
                    operation_env,
                    status,
                    f"result-{status}",
                ),
            )
        assert (
            unresolved.value.code
            is runtime_module.RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED
        )
        assert before == _operation_snapshot(operation_env)


@pytest.mark.parametrize(
    ("terminal_status", "allowed"),
    [
        pytest.param("failed", True, id="failed"),
        pytest.param("succeeded", False, id="succeeded"),
    ],
)
def test_pre_effect_blocked_operation_only_allows_failed_commit(
    operation_env, terminal_status, allowed
):
    operation_env["guard"].prepare(
        operation_env["claim"],
        _intent(operation_env, readback_kind=None),
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )
    result = _turn_result(
        operation_env, terminal_status, f"result-{terminal_status}"
    )

    if allowed:
        turn = operation_env["runtime"].commit_turn(
            operation_env["claim"], result
        )
        assert turn.status == "failed"
    else:
        with pytest.raises(
            runtime_module.ProjectRuntimeError
        ) as unresolved:
            operation_env["runtime"].commit_turn(
                operation_env["claim"], result
            )
        assert (
            unresolved.value.code
            is runtime_module.RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED
        )


def test_reconciled_operation_allows_terminal_commit(operation_env):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    receipt = _receipt(operation_env)
    operation_env["guard"].record_receipt(
        operation_env["claim"], "operation-1", receipt
    )
    operation_env["guard"].reconcile(
        operation_env["claim"],
        "operation-1",
        _Readback(
            operation_env["conn"],
            _readback_result(
                operation_env,
                "applied",
                evidence={"ledger": "complete"},
                receipt=receipt,
            ),
        ),
    )

    turn = operation_env["runtime"].commit_turn(
        operation_env["claim"],
        _turn_result(operation_env, "succeeded", "result-1"),
    )

    assert turn.status == "succeeded"


def test_post_effect_blocked_commit_parks_recovery_proof(
    operation_env,
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    operation_env["guard"].reconcile(
        operation_env["claim"],
        "operation-1",
        _Readback(
            operation_env["conn"],
            _readback_result(
                operation_env,
                "unknown",
                evidence={"ledger": "ambiguous"},
            ),
        ),
    )
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )

    with pytest.raises(
        runtime_module.ProjectRuntimeError
    ) as unresolved:
        operation_env["runtime"].commit_turn(
            operation_env["claim"],
            _turn_result(operation_env, "failed", "result-1"),
        )

    assert (
        unresolved.value.code
        is runtime_module.RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED
    )
    turn = prdb._runtime_turn_for_project(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    assert turn is not None
    assert turn.status == "reconciling"
    assert turn.recovery_block_key is not None
    assert prdb._current_worker_lease_for_turn(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    ) is None


def _stop_operation_turn(operation_env):
    runtime = operation_env["runtime"]
    conn = operation_env["conn"]
    state = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    )
    control = prdb._runtime_control_for_turn(
        conn,
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    assert state is not None and control is not None
    runtime.request_stop(
        operation_env["project_id"],
        operation_env["turn"].turn_id,
        operation_env["actor"],
        idempotency_key="stop-operation",
        expected_version=state.version,
        expected_control_version=control.control_version,
    )
    runtime.acknowledge_stopped(operation_env["claim"])


def test_resume_uses_positive_operation_guard_without_readback(
    operation_env,
):
    _prepare_allowed(operation_env)
    _stop_operation_turn(operation_env)
    conn = operation_env["conn"]
    state = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    )
    control = prdb._runtime_control_for_turn(
        conn,
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    assert state is not None and control is not None
    before = _operation_snapshot(operation_env)
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )

    with pytest.raises(
        runtime_module.ProjectRuntimeError
    ) as unresolved:
        operation_env["runtime"].request_resume(
            operation_env["project_id"],
            operation_env["turn"].turn_id,
            operation_env["actor"],
            idempotency_key="resume-operation",
            expected_version=state.version,
            expected_control_version=control.control_version,
        )

    assert (
        unresolved.value.code
        is runtime_module.RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED
    )
    assert before == _operation_snapshot(operation_env)


@pytest.mark.parametrize(
    ("operation_state", "expected_status"),
    [
        pytest.param("approved", "reconciling", id="unresolved"),
        pytest.param("pre_blocked", "failed", id="pre-effect-blocked"),
        pytest.param("post_blocked", "reconciling", id="post-effect-blocked"),
    ],
)
def test_task5_recovery_uses_operation_guard_without_turn_readback(
    operation_env, operation_state, expected_status
):
    if operation_state == "pre_blocked":
        operation_env["guard"].prepare(
            operation_env["claim"],
            _intent(operation_env, readback_kind=None),
            policy=PolicyDecision(
                Decision.ALLOW, "policy.allow.local", "allowed"
            ),
            approval=None,
        )
    else:
        _prepare_allowed(operation_env)
        if operation_state == "post_blocked":
            operation_env["guard"].mark_started(
                operation_env["claim"], "operation-1"
            )
            operation_env["guard"].reconcile(
                operation_env["claim"],
                "operation-1",
                _Readback(
                    operation_env["conn"],
                    _readback_result(
                        operation_env,
                        "unknown",
                        evidence={"ledger": "ambiguous"},
                    ),
                ),
            )
    operation_env["now"][0] = 131

    class NoTurnReadback:
        def read_turn(self, request):
            raise AssertionError("Task-5 must not read operation turns")

    recovered = operation_env["runtime"].reconcile_inflight_turns(
        NoTurnReadback(), limit=10
    )

    assert len(recovered) == 1
    assert recovered[0].status == expected_status
    if expected_status == "reconciling":
        stored = prdb._runtime_turn_for_project(
            operation_env["conn"],
            project_id=operation_env["project_id"],
            turn_id=operation_env["turn"].turn_id,
        )
        assert stored is not None
        if operation_state == "approved":
            assert stored.recovery_block_key is None
        else:
            assert stored.recovery_block_key is not None


def _operation_db_path(operation_env):
    return Path(
        operation_env["conn"].execute(
            "PRAGMA database_list"
        ).fetchone()["file"]
    )


def _remote_ledger(path, *, with_effect=False):
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE remote_effects (
                idempotency_key TEXT PRIMARY KEY,
                receipt_id TEXT NOT NULL,
                receipt_json TEXT NOT NULL
            )
            """
        )
        if with_effect:
            conn.execute(
                """
                INSERT INTO remote_effects
                    (idempotency_key, receipt_id, receipt_json)
                VALUES (
                    'remote-operation-1', 'remote-receipt-1',
                    '{"provider_sequence":1}'
                )
                """
            )
        conn.commit()
    finally:
        conn.close()


def _start_config_probe(
    operation_env,
    *,
    mode,
    now,
    config,
):
    return _start_operation_probe(
        operation_env,
        mode=mode,
        now=now,
        outcome=json.dumps(
            config,
            sort_keys=True,
            separators=(",", ":"),
        ),
        binding_id="-",
    )


def _finish_crashed_probe(process, expected_code):
    try:
        returncode, lines, stderr = _OPERATION_PROBES.collect(
            process, timeout=15
        )
        assert returncode == expected_code, (lines, stderr)
        assert lines == []
    except BaseException as exc:
        _OPERATION_PROBES.cleanup(exc)
        raise


@pytest.mark.parametrize(
    ("boundary", "crash_code"),
    [
        pytest.param("before_marker", 73, id="before-send"),
        pytest.param("after_marker", 74, id="after-marker"),
        pytest.param("after_send", 75, id="after-send"),
        pytest.param("after_receipt", 76, id="after-receipt"),
        pytest.param("after_readback", 77, id="after-readback"),
    ],
)
def test_fresh_process_crash_boundaries_recover_one_remote_effect(
    operation_env, boundary, crash_code
):
    _prepare_allowed(operation_env)
    ledger_path = _operation_db_path(
        operation_env
    ).with_name("remote-ledger.db")
    _remote_ledger(ledger_path)
    config = {
        "boundary": boundary,
        "ledger_path": str(ledger_path),
        "operation_id": "operation-1",
    }
    crashing = _start_config_probe(
        operation_env,
        mode="execute",
        now=100,
        config=config,
    )
    try:
        _release_operation_probe(crashing)
        _finish_crashed_probe(crashing, crash_code)
    finally:
        if crashing.poll() is None:
            crashing.kill()
            crashing.wait(timeout=5)

    completing = _start_config_probe(
        operation_env,
        mode="complete",
        now=131,
        config=config,
    )
    try:
        _release_operation_probe(completing)
        result = _finish_operation_probe(completing)
    finally:
        if completing.poll() is None:
            completing.kill()
            completing.wait(timeout=5)

    assert result["operation_status"] == "reconciled"
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()[0] == 1
    ledger = sqlite3.connect(ledger_path)
    try:
        assert ledger.execute(
            "SELECT COUNT(*) FROM remote_effects"
        ).fetchone()[0] == 1
    finally:
        ledger.close()
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.reconciled'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 1


@pytest.mark.parametrize("_iteration", range(25))
def test_two_fresh_process_starters_commit_one_start_event(
    operation_env, _iteration
):
    _prepare_allowed(operation_env)
    before_version = prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version
    config = {"operation_id": "operation-1"}
    processes = [
        _start_config_probe(
            operation_env,
            mode="start",
            now=100,
            config=config,
        )
        for _ in range(2)
    ]
    try:
        for process in processes:
            _release_operation_probe(process)
        results = [
            _finish_operation_probe(process)
            for process in processes
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    assert {
        result["operation_status"] for result in results
    } == {"effect_started"}
    assert prdb.runtime_state_for_project(
        operation_env["conn"], operation_env["project_id"]
    ).version == before_version + 1
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.effect_started'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 1


@pytest.mark.parametrize("_iteration", range(25))
def test_two_fresh_process_reconcilers_commit_one_canonical_result(
    operation_env, _iteration
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    ledger_path = _operation_db_path(
        operation_env
    ).with_name("remote-ledger.db")
    _remote_ledger(ledger_path, with_effect=True)
    config = {
        "ledger_path": str(ledger_path),
        "operation_id": "operation-1",
    }
    processes = [
        _start_config_probe(
            operation_env,
            mode="reconcile",
            now=100,
            config=config,
        )
        for _ in range(2)
    ]
    try:
        for process in processes:
            _release_operation_probe(process)
        results = [
            _finish_operation_probe(process)
            for process in processes
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    assert {
        result["operation_status"] for result in results
    } == {"reconciled"}
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ?
          AND kind IN ('operation.unknown', 'operation.reconciled')
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 2


@pytest.mark.parametrize("_iteration", range(25))
def test_stale_starter_loses_to_fresh_rehydration_process(
    operation_env, _iteration
):
    old_claim = operation_env["claim"]
    _prepare_critical(operation_env)
    operation_env["now"][0] = 131
    operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["actor"],
        outcome="approved",
    )
    stale_config = {
        "claim": {
            field.name: getattr(old_claim, field.name)
            for field in fields(old_claim)
        },
        "operation_id": "operation-1",
    }
    fresh_config = {
        "operation_id": "operation-1",
        "worker_id": "fresh-worker",
    }
    processes = [
        _start_config_probe(
            operation_env,
            mode="start",
            now=131,
            config=stale_config,
        ),
        _start_config_probe(
            operation_env,
            mode="rehydrate_config",
            now=131,
            config=fresh_config,
        ),
    ]
    try:
        for process in processes:
            _release_operation_probe(process)
        results = [
            _finish_operation_probe(process)
            for process in processes
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    assert sum(
        result.get("error_code") == "stale_turn_claim"
        for result in results
    ) == 1
    claims = [
        result.get("claim")
        for result in results
        if result.get("claim") is not None
    ]
    assert len(claims) == 1
    assert claims[0]["lease_generation"] == 2
    assert claims[0]["fencing_token"] == 2


class _PhaseACrash(BaseException):
    pass


class _CrashAfterPhaseA:
    def read_operation(self, request):
        raise _PhaseACrash


@pytest.mark.parametrize(
    "readback_case",
    [
        pytest.param("equal", id="equal"),
        pytest.param("mismatch", id="mismatch"),
        pytest.param("missing", id="missing"),
        pytest.param("not_applied", id="not-applied"),
    ],
)
def test_phase_a_crash_preserves_receipt_authority_across_restart(
    operation_env, readback_case
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    original = _receipt(
        operation_env,
        receipt_id="receipt-original",
        payload={"provider_sequence": 1},
    )
    operation_env["guard"].record_receipt(
        operation_env["claim"], "operation-1", original
    )

    with pytest.raises(_PhaseACrash):
        operation_env["guard"].reconcile(
            operation_env["claim"],
            "operation-1",
            _CrashAfterPhaseA(),
        )

    stored = operation_env["conn"].execute(
        """
        SELECT status, receipt_id, receipt_json
        FROM project_operations WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    assert tuple(stored) == (
        "unknown",
        "receipt-original",
        '{"provider_sequence":1}',
    )
    db_path = _operation_db_path(operation_env)
    operation_env["conn"].close()
    reopened = projects_db.connect(db_path)
    operation_env["conn"] = reopened
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )
    runtime = runtime_module.ProjectRuntime(
        reopened, clock=lambda: 101
    )
    guard = operation_env["module"].ProjectOperationGuard(runtime)
    if readback_case == "equal":
        result = _readback_result(
            operation_env,
            "applied",
            evidence={"ledger": "complete"},
            receipt=original,
        )
    elif readback_case == "mismatch":
        result = _readback_result(
            operation_env,
            "applied",
            evidence={"ledger": "complete"},
            receipt=_receipt(
                operation_env,
                receipt_id="receipt-conflicting",
                payload={"provider_sequence": 999},
            ),
        )
    elif readback_case == "missing":
        result = _readback_result(
            operation_env,
            "applied",
            evidence={"ledger": "complete"},
            receipt=None,
        )
    else:
        result = _readback_result(
            operation_env,
            "not_applied",
            evidence={"ledger": "complete", "present": False},
            receipt=None,
        )
    port = _Readback(reopened, result)

    resolved = guard.reconcile(
        operation_env["claim"], "operation-1", port
    )

    assert port.requests[0].receipt == original
    assert resolved.status == (
        "reconciled" if readback_case == "equal" else "blocked"
    )
    assert resolved.receipt_id == "receipt-original"
    persisted = reopened.execute(
        """
        SELECT receipt_id, receipt_json
        FROM project_operations WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    assert tuple(persisted) == (
        "receipt-original",
        '{"provider_sequence":1}',
    )
    if readback_case != "equal":
        assert reopened.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'operation.blocked'
            """,
            (operation_env["project_id"],),
        ).fetchone()[0] == 1


def test_not_applied_revokes_old_claim_and_only_fresh_fence_restarts(
    operation_env,
):
    _prepare_allowed(operation_env)
    first = operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    before_control = prdb._runtime_control_for_turn(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    assert before_control is not None

    approved = operation_env["guard"].reconcile(
        operation_env["claim"],
        "operation-1",
        _Readback(
            operation_env["conn"],
            _readback_result(
                operation_env,
                "not_applied",
                evidence={"ledger": "complete", "present": False},
                receipt=None,
            ),
        ),
    )

    assert approved.status == "approved"
    turn = prdb._runtime_turn_for_project(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    control = prdb._runtime_control_for_turn(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    assert turn is not None and control is not None
    assert turn.status == "reconciling"
    assert turn.recovery_block_key is None
    assert control.control_version == before_control.control_version + 1
    assert prdb._current_worker_lease_for_turn(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    ) is None
    with pytest.raises(
        operation_env["module"].ProjectOperationError
    ) as old_start:
        operation_env["guard"].mark_started(
            operation_env["claim"], "operation-1"
        )
    assert (
        old_start.value.code
        is operation_env[
            "module"
        ].OperationErrorCode.OPERATION_STATE_CONFLICT
    )

    fresh = operation_env[
        "guard"
    ]._rehydrate_approved_operation(
        operation_env["project_id"],
        "operation-1",
        worker_id="worker-fresh",
        lease_seconds=30,
    )
    assert fresh is not None
    assert fresh.attempt_id != first.attempt_id
    assert fresh.lease_generation == first.lease_generation + 1
    assert fresh.fencing_token == first.fencing_token + 1
    restarted = operation_env["guard"].mark_started(
        fresh, "operation-1"
    )
    assert restarted.status == "effect_started"
    assert restarted.attempt_id == fresh.attempt_id


def _prepare_reconciled_receipt_owner(
    operation_env, *, receipt_id="shared-receipt"
):
    _prepare_allowed(operation_env)
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    receipt = _receipt(
        operation_env,
        receipt_id=receipt_id,
        payload={"provider_sequence": 1},
    )
    operation_env["guard"].record_receipt(
        operation_env["claim"], "operation-1", receipt
    )
    operation_env["guard"].reconcile(
        operation_env["claim"],
        "operation-1",
        _Readback(
            operation_env["conn"],
            _readback_result(
                operation_env,
                "applied",
                evidence={"ledger": "complete"},
                receipt=receipt,
            ),
        ),
    )
    return receipt


def _prepare_second_allowed_operation(operation_env):
    operation_env["guard"].prepare(
        operation_env["claim"],
        _intent(
            operation_env,
            operation_id="operation-2",
            idempotency_key="remote-operation-2",
            batch_items=("write-second",),
        ),
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-2"
    )


def test_cross_operation_receipt_collision_is_stable_and_write_free(
    operation_env,
):
    shared = _prepare_reconciled_receipt_owner(operation_env)
    _prepare_second_allowed_operation(operation_env)
    before = _operation_snapshot(operation_env)

    with pytest.raises(
        operation_env["module"].ProjectOperationError
    ) as collision:
        operation_env["guard"].record_receipt(
            operation_env["claim"], "operation-2", shared
        )

    assert (
        collision.value.code
        is operation_env[
            "module"
        ].OperationErrorCode.OPERATION_RECEIPT_CONFLICT
    )
    assert before == _operation_snapshot(operation_env)


def test_readback_receipt_collision_blocks_once_without_raw_sqlite(
    operation_env,
):
    shared = _prepare_reconciled_receipt_owner(operation_env)
    _prepare_second_allowed_operation(operation_env)
    port = _Readback(
        operation_env["conn"],
        _readback_result(
            operation_env,
            "applied",
            evidence={"ledger": "complete"},
            receipt=shared,
        ),
    )

    blocked = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-2", port
    )
    after_first = _operation_snapshot(operation_env)
    replay = operation_env["guard"].reconcile(
        operation_env["claim"], "operation-2", port
    )

    assert blocked == replay
    assert blocked.status == "blocked"
    assert blocked.blocked_reason == "operation_readback_ambiguous"
    assert blocked.receipt_id is None
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'operation.blocked'
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 1
    assert after_first == _operation_snapshot(operation_env)


def _operation_to_pending_status(operation_env, status):
    _prepare_allowed(operation_env)
    if status == "approved":
        return
    operation_env["guard"].mark_started(
        operation_env["claim"], "operation-1"
    )
    if status == "effect_started":
        return
    if status == "receipt_recorded":
        operation_env["guard"].record_receipt(
            operation_env["claim"],
            "operation-1",
            _receipt(operation_env),
        )
        return
    with pytest.raises(_PhaseACrash):
        operation_env["guard"].reconcile(
            operation_env["claim"],
            "operation-1",
            _CrashAfterPhaseA(),
        )


@pytest.mark.parametrize(
    "status",
    ["approved", "effect_started", "receipt_recorded", "unknown"],
)
def test_task5_parks_operation_pending_without_owning_recovery(
    operation_env, status
):
    _operation_to_pending_status(operation_env, status)
    before_operation = prdb._project_operation_for_id(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    operation_env["now"][0] = 131

    class NoTurnReadback:
        calls = 0

        def read_turn(self, request):
            self.calls += 1
            raise AssertionError(
                "Task-5 must not read an operation-pending turn"
            )

    readback = NoTurnReadback()
    recovered = operation_env["runtime"].reconcile_inflight_turns(
        readback, limit=10
    )

    assert len(recovered) == 1
    assert recovered[0].status == "reconciling"
    stored_turn = prdb._runtime_turn_for_project(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    )
    stored_operation = prdb._project_operation_for_id(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    assert stored_turn is not None and stored_operation is not None
    assert stored_turn.recovery_block_key is None
    assert prdb._current_worker_lease_for_turn(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    ) is None
    assert stored_operation == before_operation
    assert readback.calls == 0
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind IN (
            'turn.recovery_blocked', 'turn.requeued',
            'turn.succeeded', 'turn.failed'
        )
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 0


def _prepare_and_reconcile_named_operation(
    operation_env,
    *,
    operation_id,
    receipt_id,
):
    intent = _intent(
        operation_env,
        operation_id=operation_id,
        idempotency_key=f"remote-{operation_id}",
    )
    operation_env["guard"].prepare(
        operation_env["claim"],
        intent,
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    operation_env["guard"].mark_started(
        operation_env["claim"], operation_id
    )
    receipt = _receipt(
        operation_env,
        receipt_id=receipt_id,
        payload={"provider_sequence": receipt_id},
    )
    operation_env["guard"].record_receipt(
        operation_env["claim"], operation_id, receipt
    )
    operation_env["guard"].reconcile(
        operation_env["claim"],
        operation_id,
        _Readback(
            operation_env["conn"],
            _readback_result(
                operation_env,
                "applied",
                evidence={"ledger": "complete"},
                receipt=receipt,
            ),
        ),
    )


def test_prepare_allows_terminal_history_but_second_unresolved_is_write_free(
    operation_env,
):
    for ordinal in (1, 2):
        _prepare_and_reconcile_named_operation(
            operation_env,
            operation_id=f"history-{ordinal}",
            receipt_id=f"history-receipt-{ordinal}",
        )
    pre_effect = operation_env["guard"].prepare(
        operation_env["claim"],
        _intent(
            operation_env,
            operation_id="history-pre-effect",
            idempotency_key="history-pre-effect-key",
            readback_kind=None,
        ),
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    assert (
        pre_effect.status,
        pre_effect.blocked_reason,
    ) == ("blocked", "operation_capability_unsupported")
    unresolved = operation_env["guard"].prepare(
        operation_env["claim"],
        _intent(
            operation_env,
            operation_id="current-unresolved",
            idempotency_key="current-unresolved-key",
        ),
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    assert unresolved.status == "approved"
    before = _operation_snapshot(operation_env)

    with pytest.raises(
        operation_env["module"].ProjectOperationError
    ) as conflict:
        operation_env["guard"].prepare(
            operation_env["claim"],
            _intent(
                operation_env,
                operation_id="second-unresolved",
                idempotency_key="second-unresolved-key",
            ),
            policy=PolicyDecision(
                Decision.ALLOW, "policy.allow.local", "allowed"
            ),
            approval=None,
        )

    assert (
        conflict.value.code
        is operation_env["module"].OperationErrorCode.OPERATION_STATE_CONFLICT
    )
    assert before == _operation_snapshot(operation_env)


def _add_parked_operation_candidate(
    conn,
    *,
    ordinal,
    status,
    updated_at,
):
    project_id = projects_db.create_project(
        conn,
        name=f"Pending {ordinal}",
        folders=(f"C:/work/pending-{ordinal}",),
    )
    conversation_id = f"pending-session-{ordinal}"
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id=conversation_id,
        current_phase="implementation",
        now=1,
    )
    binding_id = f"pending-binding-{ordinal}"
    prdb.bind_surface(
        conn,
        binding_id=binding_id,
        project_id=project_id,
        surface="desktop",
        external_binding_id=f"window-{ordinal}",
        actor_id="owner-1",
        now=1,
    )
    runtime_module = importlib.import_module(
        "hermes_cli.project_runtime"
    )
    module = importlib.import_module("hermes_cli.project_operations")
    clock = [updated_at]
    runtime = runtime_module.ProjectRuntime(
        conn, clock=lambda: clock[0]
    )
    actor = ActorContext("owner-1", "desktop", binding_id, True)
    turn = runtime.enqueue_turn(
        project_id,
        {"message": f"pending {ordinal}"},
        actor,
        idempotency_key=f"pending-turn-{ordinal}",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id, f"pending-worker-{ordinal}", lease_seconds=30
    )
    assert claim is not None
    claim = runtime.mark_turn_started(claim)
    guard = module.ProjectOperationGuard(runtime)
    operation_id = f"pending-operation-{ordinal}"
    intent = module.OperationIntent(
        operation_id=operation_id,
        project_id=project_id,
        turn_id=turn.turn_id,
        idempotency_key=f"pending-remote-{ordinal}",
        canonical_action="local_code_edit",
        command_revision=1,
        targets=(f"C:/work/pending-{ordinal}/file.py",),
        batch_items=("write-file",),
        payload={"ordinal": ordinal},
        readback_kind="remote-ledger",
        remote_idempotency_supported=True,
    )
    guard.prepare(
        claim,
        intent,
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )
    if status != "approved":
        guard.mark_started(claim, operation_id)
    if status == "receipt_recorded":
        guard.record_receipt(
            claim,
            operation_id,
            module.OperationReceipt(
                f"pending-receipt-{ordinal}",
                {"provider_sequence": ordinal},
            ),
        )
    elif status == "unknown":
        with pytest.raises(_PhaseACrash):
            guard.reconcile(claim, operation_id, _CrashAfterPhaseA())
    selected = next(
        candidate
        for candidate in prdb._recovery_candidates(
            conn, now=updated_at + 31, limit=100
        )
        if candidate.project_id == project_id
        and candidate.turn_id == turn.turn_id
    )
    assert runtime._park_recovery_candidate(
        selected, now=updated_at + 31
    ) is not None
    return operation_id


def _insert_large_allowed_operation_history(
    operation_env,
    *,
    count=10_000,
):
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    turn_id = operation_env["turn"].turn_id
    attempt_id = operation_env["claim"].attempt_id
    conn.executemany(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            command_revision, targets_json, payload_json, status,
            created_at, updated_at, guard_revision, canonical_action,
            batch_items_json, readback_kind, attempt_id,
            lease_generation, fencing_token, blocked_reason,
            remote_idempotency_supported
        ) VALUES (
            ?, ?, ?, ?, 1, '["c:/work/history"]', '{}', 'blocked',
            1, 1, 1, 'local_code_edit', '["history"]', NULL, ?,
            1, 1, 'operation_capability_unsupported', 0
        )
        """,
        (
            (
                f"history-{ordinal:05d}",
                project_id,
                turn_id,
                f"history-key-{ordinal:05d}",
                attempt_id,
            )
            for ordinal in range(count)
        ),
    )
    conn.commit()


def _certify_operation_fixture_rows(operation_env):
    conn = operation_env["conn"]
    conn.execute(
        """
        UPDATE project_operation_maintenance
        SET operation_validation_migration_complete = 0
        WHERE singleton = 1
        """
    )
    conn.commit()
    prdb.ensure_schema(conn)


def _insert_raw_unresolved_operation(
    operation_env,
    *,
    operation_id="current-unresolved",
    status="approved",
    updated_at=100,
    guard_validated=0,
):
    conn = operation_env["conn"]
    conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            command_revision, targets_json, payload_json, status,
            created_at, updated_at, guard_revision, canonical_action,
            batch_items_json, readback_kind, attempt_id,
            lease_generation, fencing_token,
            remote_idempotency_supported, guard_validated
        ) VALUES (
            ?, ?, ?, ?, 1, '["c:/work/current"]', '{}', ?,
            1, ?, 1, 'local_code_edit', '["current"]', 'ledger', ?,
            1, 1, 1, ?
        )
        """,
        (
            operation_id,
            operation_env["project_id"],
            operation_env["turn"].turn_id,
            f"{operation_id}-key",
            status,
            updated_at,
            operation_env["claim"].attempt_id,
            guard_validated,
        ),
    )
    conn.commit()


def _park_raw_operation_turn(operation_env):
    conn = operation_env["conn"]
    conn.execute(
        """
        DELETE FROM project_worker_leases
        WHERE project_id = ? AND turn_id = ?
        """,
        (
            operation_env["project_id"],
            operation_env["turn"].turn_id,
        ),
    )
    conn.execute(
        """
        UPDATE project_turns
        SET status = 'reconciling', recovery_block_key = NULL
        WHERE project_id = ? AND turn_id = ?
        """,
        (
            operation_env["project_id"],
            operation_env["turn"].turn_id,
        ),
    )
    conn.commit()


def _with_sql_progress_budget(conn, *, budget, action):
    steps = [0]

    def count_progress():
        steps[0] += 1
        return int(steps[0] > budget)

    conn.set_progress_handler(count_progress, 1)
    try:
        result = action()
    finally:
        conn.set_progress_handler(None, 0)
    return result, steps[0]


def test_operation_pending_large_history_is_bounded_and_maps_candidate_only(
    operation_env, monkeypatch
):
    _insert_large_allowed_operation_history(operation_env)
    _insert_raw_unresolved_operation(operation_env)
    _certify_operation_fixture_rows(operation_env)
    _park_raw_operation_turn(operation_env)
    mapped = []
    original_mapper = prdb._project_operation_for_id

    def recording_mapper(
        conn,
        *,
        project_id,
        operation_id,
    ):
        mapped.append(operation_id)
        if operation_id.startswith("history-"):
            raise AssertionError("allowed history must not be mapped")
        return original_mapper(
            conn,
            project_id=project_id,
            operation_id=operation_id,
        )

    monkeypatch.setattr(
        prdb, "_project_operation_for_id", recording_mapper
    )

    candidates, steps = _with_sql_progress_budget(
        operation_env["conn"],
        budget=2_000,
        action=lambda: prdb._operation_pending_candidates(
            operation_env["conn"], limit=1
        ),
    )

    assert [candidate.operation_id for candidate in candidates] == [
        "current-unresolved"
    ]
    assert mapped == ["current-unresolved"]
    assert steps < 2_000
    for status in (
        "approved",
        "effect_started",
        "receipt_recorded",
        "unknown",
    ):
        plan = operation_env["conn"].execute(
            "EXPLAIN QUERY PLAN "
            + prdb._OPERATION_PENDING_BRANCH_SQL,
            (status, 1),
        ).fetchall()
        details = " ".join(row["detail"] for row in plan)
        assert "idx_project_operations_recovery" in details
        assert "idx_project_operations_turn_unresolved" in details
        assert "idx_project_operations_turn_unsafe" in details
        assert "USE TEMP B-TREE" not in details


def test_prepare_gate_is_bounded_and_does_not_map_allowed_history(
    operation_env, monkeypatch
):
    _insert_large_allowed_operation_history(operation_env)
    _certify_operation_fixture_rows(operation_env)
    mapped = []
    original_mapper = prdb._project_operation_for_id

    def recording_mapper(
        conn,
        *,
        project_id,
        operation_id,
    ):
        mapped.append(operation_id)
        if operation_id.startswith("history-"):
            raise AssertionError("prepare must not map allowed history")
        return original_mapper(
            conn,
            project_id=project_id,
            operation_id=operation_id,
        )

    monkeypatch.setattr(
        prdb, "_project_operation_for_id", recording_mapper
    )
    prepared, steps = _with_sql_progress_budget(
        operation_env["conn"],
        budget=4_000,
        action=lambda: operation_env["guard"].prepare(
            operation_env["claim"],
            _intent(operation_env),
            policy=PolicyDecision(
                Decision.ALLOW, "policy.allow.local", "allowed"
            ),
            approval=None,
        ),
    )

    assert prepared.status == "approved"
    assert all(
        not operation_id.startswith("history-")
        for operation_id in mapped
    )
    assert steps < 4_000


_UNSAFE_LARGE_HISTORY_SHAPES = (
    "marker0_malformed",
    "revision0",
    "post_effect_blocked",
    "marker_null",
    "marker_wrong_type",
    "revision_null",
    "revision_wrong_type",
)


def _relax_operation_not_null_for_corruption(
    operation_env,
    column,
):
    conn = operation_env["conn"]
    database_path = conn.execute(
        "PRAGMA database_list"
    ).fetchone()["file"]
    row = conn.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'table' AND name = 'project_operations'
        """
    ).fetchone()
    assert row is not None
    original_sql = row["sql"]
    search_after = 0
    constraint_offset = None
    while True:
        column_offset = original_sql.find(column, search_after)
        assert column_offset >= 0
        suffix_offset = column_offset + len(column)
        suffix = original_sql[suffix_offset:]
        whitespace = len(suffix) - len(suffix.lstrip())
        candidate = suffix_offset + whitespace
        if original_sql.startswith("INTEGER NOT NULL", candidate):
            constraint_offset = candidate
            break
        search_after = suffix_offset
    assert constraint_offset is not None
    relaxed_sql = (
        original_sql[:constraint_offset]
        + "INTEGER"
        + original_sql[constraint_offset + len("INTEGER NOT NULL") :]
    )
    schema_version = conn.execute(
        "PRAGMA schema_version"
    ).fetchone()[0]
    conn.execute("PRAGMA writable_schema=ON")
    try:
        conn.execute(
            """
            UPDATE sqlite_master SET sql = ?
            WHERE type = 'table' AND name = 'project_operations'
            """,
            (relaxed_sql,),
        )
    finally:
        conn.execute("PRAGMA writable_schema=OFF")
    conn.commit()
    conn.execute(f"PRAGMA schema_version = {schema_version + 1}")
    conn.close()

    reopened = sqlite3.connect(database_path)
    reopened.row_factory = sqlite3.Row
    reopened.execute("PRAGMA foreign_keys=ON")
    operation_env["conn"] = reopened
    operation_env["runtime"]._conn = reopened
    operation_env["guard"]._conn = reopened


def _insert_large_history_unsafe_row(operation_env, shape):
    if shape in {"marker_null", "revision_null"}:
        _relax_operation_not_null_for_corruption(
            operation_env,
            (
                "guard_validated"
                if shape == "marker_null"
                else "guard_revision"
            ),
        )
    marker = {
        "marker0_malformed": 0,
        "revision0": 0,
        "post_effect_blocked": 1,
        "marker_null": None,
        "marker_wrong_type": "wrong",
        "revision_null": 1,
        "revision_wrong_type": 1,
    }[shape]
    revision = {
        "marker0_malformed": 1,
        "revision0": 0,
        "post_effect_blocked": 1,
        "marker_null": 1,
        "marker_wrong_type": 1,
        "revision_null": None,
        "revision_wrong_type": "wrong",
    }[shape]
    post_effect = shape == "post_effect_blocked"
    targets_json = (
        "{"
        if shape == "marker0_malformed"
        else '["c:/work/unsafe"]'
    )
    conn = operation_env["conn"]
    conn.execute("PRAGMA ignore_check_constraints=ON")
    try:
        conn.execute(
            """
            INSERT INTO project_operations (
                operation_id, project_id, turn_id, idempotency_key,
                approval_id, command_revision, targets_json,
                payload_json, status, receipt_json, created_at,
                updated_at, guard_revision, guard_validated,
                canonical_action, batch_items_json, readback_kind,
                attempt_id, lease_generation, fencing_token,
                receipt_id, readback_json, blocked_reason,
                remote_idempotency_supported,
                approval_fingerprint_json
            ) VALUES (
                ?, ?, ?, ?, NULL, 1, ?, '{}', 'blocked', ?,
                1, 1, ?, ?, 'local_code_edit', '["unsafe"]', ?,
                ?, 1, 1, ?, NULL, ?, ?, NULL
            )
            """,
            (
                f"unsafe-{shape}",
                operation_env["project_id"],
                operation_env["turn"].turn_id,
                f"unsafe-key-{shape}",
                targets_json,
                (
                    '{"provider_sequence":1}'
                    if post_effect
                    else None
                ),
                revision,
                marker,
                "ledger" if post_effect else None,
                operation_env["claim"].attempt_id,
                "unsafe-receipt" if post_effect else None,
                (
                    "operation_readback_ambiguous"
                    if post_effect
                    else "operation_capability_unsupported"
                ),
                1 if post_effect else 0,
            ),
        )
    finally:
        conn.execute("PRAGMA ignore_check_constraints=OFF")
    conn.commit()


@pytest.mark.parametrize("unsafe_shape", _UNSAFE_LARGE_HISTORY_SHAPES)
def test_unsafe_large_history_is_indexed_bounded_and_fail_closed(
    operation_env,
    monkeypatch,
    unsafe_shape,
):
    _insert_large_allowed_operation_history(operation_env)
    _certify_operation_fixture_rows(operation_env)
    _insert_large_history_unsafe_row(operation_env, unsafe_shape)
    conn = operation_env["conn"]
    before = _transition_authority_snapshot(operation_env)

    def rejected_prepare():
        with pytest.raises(
            operation_env["module"].ProjectOperationError
        ) as conflict:
            operation_env["guard"].prepare(
                operation_env["claim"],
                _intent(operation_env),
                policy=PolicyDecision(
                    Decision.ALLOW,
                    "policy.allow.local",
                    "allowed",
                ),
                approval=None,
            )
        return conflict.value.code

    code, prepare_steps = _with_sql_progress_budget(
        conn,
        budget=4_000,
        action=rejected_prepare,
    )
    assert (
        code
        is operation_env[
            "module"
        ].OperationErrorCode.OPERATION_STATE_CONFLICT
    )
    assert prepare_steps < 4_000
    assert _transition_authority_snapshot(operation_env) == before

    _insert_raw_unresolved_operation(
        operation_env,
        guard_validated=1,
    )
    _park_raw_operation_turn(operation_env)
    mapped = []
    original_mapper = prdb._project_operation_for_id

    def recording_mapper(
        selected_conn,
        *,
        project_id,
        operation_id,
    ):
        mapped.append(operation_id)
        return original_mapper(
            selected_conn,
            project_id=project_id,
            operation_id=operation_id,
        )

    monkeypatch.setattr(
        prdb, "_project_operation_for_id", recording_mapper
    )
    candidates, recovery_steps = _with_sql_progress_budget(
        conn,
        budget=2_000,
        action=lambda: prdb._operation_pending_candidates(
            conn, limit=1
        ),
    )

    assert candidates == ()
    assert mapped == []
    assert recovery_steps < 2_000
    standalone_plan = conn.execute(
        "EXPLAIN QUERY PLAN " + prdb._OPERATION_TURN_UNSAFE_SQL,
        (
            operation_env["project_id"],
            operation_env["turn"].turn_id,
        ),
    ).fetchall()
    details = " ".join(row["detail"] for row in standalone_plan)
    assert "SEARCH project_operations" in details
    assert "idx_project_operations_turn_unsafe" in details
    assert "<expr>=?" in details
    assert "SCAN project_operations" not in details
    assert "USE TEMP B-TREE" not in details


def test_operation_pending_selector_is_four_branch_bounded_and_global(
    operation_conn,
):
    expected = []
    for ordinal, (status, updated_at) in enumerate(
        (
            ("approved", 40),
            ("effect_started", 10),
            ("receipt_recorded", 30),
            ("unknown", 20),
        ),
        start=1,
    ):
        expected.append(
            (
                updated_at,
                _add_parked_operation_candidate(
                    operation_conn,
                    ordinal=ordinal,
                    status=status,
                    updated_at=updated_at,
                ),
            )
        )

    candidates = prdb._operation_pending_candidates(
        operation_conn, limit=4
    )
    assert [item.operation_id for item in candidates] == [
        operation_id
        for _, operation_id in sorted(expected)
    ]
    assert len(candidates) <= 4
    for status in (
        "approved",
        "effect_started",
        "receipt_recorded",
        "unknown",
    ):
        plan = operation_conn.execute(
            "EXPLAIN QUERY PLAN "
            + prdb._OPERATION_PENDING_BRANCH_SQL,
            (status, 4),
        ).fetchall()
        details = " ".join(row["detail"] for row in plan)
        assert "idx_project_operations_recovery" in details
        assert "USE TEMP B-TREE" not in details


@pytest.mark.parametrize(
    "status",
    ["approved", "effect_started", "receipt_recorded", "unknown"],
)
def test_task6_poll_owns_operation_pending_after_either_poll_order(
    operation_env, status
):
    _operation_to_pending_status(operation_env, status)
    receipt = (
        _receipt(operation_env)
        if status == "receipt_recorded"
        else None
    )
    port = _Readback(
        operation_env["conn"],
        _readback_result(
            operation_env,
            "applied" if receipt is not None else "not_applied",
            evidence={"ledger": "complete"},
            receipt=receipt,
        ),
    )

    assert operation_env["guard"]._recover_pending_operations(
        port,
        worker_id="recovery-worker",
        lease_seconds=30,
        limit=10,
    ) == ()

    operation_env["now"][0] = 131

    class NoTurnReadback:
        def read_turn(self, request):
            raise AssertionError("Task-5 cannot own operation readback")

    operation_env["runtime"].reconcile_inflight_turns(
        NoTurnReadback(), limit=10
    )
    results = operation_env["guard"]._recover_pending_operations(
        port,
        worker_id="recovery-worker",
        lease_seconds=30,
        limit=10,
    )

    assert len(results) == 1
    operation, fresh_claim = results[0]
    if status == "approved":
        assert fresh_claim is not None
        assert operation.status == "approved"
        assert port.requests == []
    elif status == "receipt_recorded":
        assert fresh_claim is None
        assert operation.status == "reconciled"
        assert len(port.requests) == 1
        assert port.requests[0].receipt == receipt
    else:
        assert fresh_claim is None
        assert operation.status == "approved"
        assert len(port.requests) == 1


def test_task5_pending_projection_keeps_query_work_bounded(
    operation_conn,
):
    project_id = projects_db.create_project(
        operation_conn,
        name="Bounded Task 5",
        folders=("C:/work/bounded-task5",),
    )
    pending_count = 2_000
    operation_conn.executemany(
        """
        INSERT INTO project_turns (
            turn_id, project_id, sequence, idempotency_key,
            payload_json, status, attempt_id, lease_generation,
            fencing_token, execution_state, created_at, updated_at
        ) VALUES (?, ?, ?, ?, '{}', 'reconciling', ?, 1, 1,
                  'started', 1, 1)
        """,
        (
            (
                f"pending-turn-{ordinal}",
                project_id,
                ordinal,
                f"pending-turn-key-{ordinal}",
                f"pending-attempt-{ordinal}",
            )
            for ordinal in range(1, pending_count + 1)
        ),
    )
    operation_conn.executemany(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            command_revision, targets_json, payload_json, status,
            created_at, updated_at, guard_revision, canonical_action,
            batch_items_json, readback_kind, attempt_id,
            lease_generation, fencing_token,
            remote_idempotency_supported
        ) VALUES (?, ?, ?, ?, 1, '["c:/work/bounded-task5"]', '{}',
                  'approved', 1, 1, 1, 'local_code_edit', '["item"]',
                  'ledger', ?, 1, 1, 1)
        """,
        (
            (
                f"pending-operation-{ordinal}",
                project_id,
                f"pending-turn-{ordinal}",
                f"pending-operation-key-{ordinal}",
                f"pending-attempt-{ordinal}",
            )
            for ordinal in range(1, pending_count + 1)
        ),
    )
    for ordinal in range(1, pending_count + 1):
        prdb._certify_project_operation(
            operation_conn,
            project_id=project_id,
            operation_id=f"pending-operation-{ordinal}",
        )
    operation_conn.execute(
        """
        INSERT INTO project_turns (
            turn_id, project_id, sequence, idempotency_key,
            payload_json, status, attempt_id, lease_generation,
            fencing_token, execution_state, created_at, updated_at
        ) VALUES (
            'ordinary-turn', ?, ?, 'ordinary-turn-key', '{}',
            'reconciling', 'ordinary-attempt', 1, 1,
            'started', 1, 1
        )
        """,
        (project_id, pending_count + 1),
    )
    operation_conn.commit()
    progress = [0]

    def count_progress():
        progress[0] += 1
        return 0

    operation_conn.set_progress_handler(count_progress, 1)
    try:
        rows = operation_conn.execute(
            prdb._RECOVERY_RECONCILING_SQL, (1,)
        ).fetchall()
    finally:
        operation_conn.set_progress_handler(None, 0)

    assert len(rows) == 1
    assert rows[0]["has_pending_operation"] == 1
    assert progress[0] < 500
    plan = operation_conn.execute(
        "EXPLAIN QUERY PLAN " + prdb._RECOVERY_RECONCILING_SQL,
        (1,),
    ).fetchall()
    details = " ".join(row["detail"] for row in plan)
    assert "idx_project_turns_actionable_recovery" in details
    assert "idx_project_operations_recovery" in details
    assert "USE TEMP B-TREE" not in details


def _maintenance_state(conn):
    row = conn.execute(
        """
        SELECT singleton, approval_scan_after, next_lane,
               approval_scan_after_seq,
               approval_scan_high_water_seq,
               approval_scan_epoch,
               next_operation_approval_seq
        FROM project_operation_maintenance
        """
    ).fetchone()
    return tuple(row) if row is not None else None


def _stage_critical_link(operation_env):
    conn = operation_env["conn"]
    state = prdb.runtime_state_for_project(
        conn, operation_env["project_id"]
    )
    assert state is not None
    fingerprint = _raw_critical_fingerprint(
        "publish", approval_id="approval-staging"
    )
    assert prdb._insert_project_operation(
        conn,
        operation_id="operation-staging",
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
        idempotency_key="operation-staging-key",
        command_revision=1,
        targets_json='["c:/work/operations/file.py"]',
        payload_json='{"content_digest":"sha256:staging"}',
        status="approved",
        canonical_action="publish",
        batch_items_json='["publish"]',
        readback_kind="remote-ledger",
        attempt_id=operation_env["claim"].attempt_id,
        lease_generation=operation_env["claim"].lease_generation,
        fencing_token=operation_env["claim"].fencing_token,
        blocked_reason=None,
        remote_idempotency_supported=True,
        approval_fingerprint_json=fingerprint,
        now=operation_env["now"][0],
    )
    conn.commit()
    request = prdb.ApprovalRequest(
        approval_id="approval-staging",
        project_id=operation_env["project_id"],
        requester_actor_id="owner-1",
        authorization_actor_id="owner-1",
        canonical_action="publish",
        approval_class="publish",
        command_revision=1,
        expected_runtime_version=state.version,
        expected_lifecycle=state.lifecycle,
        expected_phase=state.current_phase,
        targets=("c:/work/operations/file.py",),
        batch_id="operation-staging",
        batch_items=("publish",),
        status="pending",
        expires_at=1_000,
    )
    operation_env["runtime"].request_turn_approval(
        operation_env["turn"].turn_id,
        request,
        operation_env["actor"],
        expected_control_version=1,
    )


def _insert_live_replenishment_approval(operation_env, sequence):
    conn = operation_env["conn"]
    project_id = projects_db.create_project(
        conn,
        name=f"Live Replenishment {sequence}",
        folders=(f"C:/work/live-replenishment/{sequence}",),
    )
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id=f"live-session-{sequence}",
        current_phase="implementation",
        now=1,
    )
    binding_id = f"live-binding-{sequence}"
    actor_id = f"live-owner-{sequence}"
    prdb.bind_surface(
        conn,
        binding_id=binding_id,
        project_id=project_id,
        surface="desktop",
        external_binding_id=f"live-window-{sequence}",
        actor_id=actor_id,
        now=1,
    )
    runtime = operation_env["runtime"].__class__(
        conn, clock=lambda: operation_env["now"][0]
    )
    actor = ActorContext(
        actor_id, "desktop", binding_id, True
    )
    turn = runtime.enqueue_turn(
        project_id,
        {"message": f"live replenishment {sequence}"},
        actor,
        idempotency_key=f"live-turn-key-{sequence}",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id,
        f"live-worker-{sequence}",
        lease_seconds=30,
    )
    assert claim is not None
    claim = runtime.mark_turn_started(claim)
    tail_env = {
        "conn": conn,
        "project_id": project_id,
        "turn": turn,
        "claim": claim,
        "actor": actor,
        "runtime": runtime,
        "module": operation_env["module"],
        "guard": operation_env["module"].ProjectOperationGuard(
            runtime
        ),
        "now": operation_env["now"],
    }
    approval_id = f"live-approval-{sequence}"
    operation_id = f"live-operation-{sequence}"

    _prepare_critical(
        tail_env,
        expires_at=1_000,
        operation_id=operation_id,
        approval_id=approval_id,
    )

    row = conn.execute(
        """
        SELECT approval.status, approval.expires_at,
               approval.operation_maintenance_seq,
               operation.guard_validated
        FROM project_approvals AS approval
        JOIN project_operations AS operation
          ON operation.project_id = approval.project_id
         AND operation.operation_id = approval.operation_id
        WHERE approval.approval_id = ?
        """,
        (approval_id,),
    ).fetchone()
    assert tuple(row) == ("pending", 1_000, sequence, 1)
    return approval_id


def test_approval_maintenance_singleton_alternates_and_live_advances_cursor(
    operation_env,
):
    _prepare_critical(operation_env, expires_at=1_000)
    assert _maintenance_state(operation_env["conn"]) == (
        1,
        "",
        0,
        0,
        0,
        0,
        2,
    )

    assert operation_env[
        "guard"
    ].expire_due_operation_approvals(limit=1) == ()
    assert _maintenance_state(operation_env["conn"]) == (
        1,
        "",
        1,
        0,
        0,
        0,
        2,
    )

    assert operation_env[
        "guard"
    ].expire_due_operation_approvals(limit=1) == ()
    assert _maintenance_state(operation_env["conn"]) == (
        1,
        "",
        0,
        0,
        0,
        1,
        2,
    )


def test_linked_approval_sequence_is_positive_immutable_and_generic_is_null(
    operation_env,
):
    _prepare_critical(operation_env, expires_at=1_000)
    generic = _generic_approval_request(
        operation_env, approval_id="generic-sequence"
    )
    prdb.create_approval_request(
        operation_env["conn"], generic, now=100
    )
    assert tuple(
        operation_env["conn"].execute(
            """
            SELECT
                (
                    SELECT operation_maintenance_seq
                    FROM project_approvals
                    WHERE approval_id = 'approval-1'
                ),
                (
                    SELECT operation_maintenance_seq
                    FROM project_approvals
                    WHERE approval_id = 'generic-sequence'
                )
            """
        ).fetchone()
    ) == (1, None)

    for statement in (
        """
        UPDATE project_approvals
        SET operation_maintenance_seq = 2
        WHERE approval_id = 'approval-1'
        """,
        """
        UPDATE project_approvals
        SET operation_id = NULL, operation_maintenance_seq = NULL
        WHERE approval_id = 'approval-1'
        """,
    ):
        operation = prdb._project_operation_for_id(
            operation_env["conn"],
            project_id=operation_env["project_id"],
            operation_id="operation-1",
        )
        assert operation is not None
        prdb._decertify_project_operation(
            operation_env["conn"], operation
        )
        with pytest.raises(sqlite3.IntegrityError):
            operation_env["conn"].execute(statement)
        operation_env["conn"].rollback()

    assert operation_env["conn"].execute(
        """
        SELECT operation_maintenance_seq
        FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()[0] == 1


def _insert_epoch_approvals(
    conn,
    project_id,
    sequences,
    *,
    reset_maintenance,
):
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.executemany(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            approval_id, command_revision, targets_json, payload_json,
            status, created_at, updated_at, guard_revision,
            guard_validated
        ) VALUES (
            ?, ?, NULL, ?, ?, 1, '[]', '{}', 'intent', 1, 1, 0, 0
        )
        """,
        (
            (
                f"epoch-operation-{sequence}",
                project_id,
                f"epoch-operation-key-{sequence}",
                f"epoch-approval-{sequence}",
            )
            for sequence in sequences
        ),
    )
    conn.executemany(
        """
        INSERT INTO project_approvals (
            approval_id, project_id, turn_id, operation_id,
            operation_maintenance_seq, actor_id,
            authorization_actor_id, canonical_action, approval_class,
            command_revision, expected_runtime_version,
            effective_runtime_version, turn_expected_control_version,
            expected_lifecycle, expected_phase, targets_json,
            batch_boundary_json, status, expires_at, resolved_at,
            resolved_by_actor_id, consumed_at, created_at
        ) VALUES (
            ?, ?, NULL, ?, ?, 'owner-1', 'owner-1',
            'publish', 'publish', ?, 0, 0, NULL, 'active',
            'implementation', ?, ?, 'pending', 10000,
            NULL, NULL, NULL, 1
        )
        """,
        (
            (
                f"epoch-approval-{sequence}",
                project_id,
                f"epoch-operation-{sequence}",
                sequence,
                sequence,
                f'["c:/epoch/{sequence}"]',
                (
                    '{"batch_id":"epoch-operation-'
                    f'{sequence}","batch_items":["item-{sequence}"]'
                    "}"
                ),
            )
            for sequence in sequences
        ),
    )
    if reset_maintenance:
        conn.execute(
            """
            UPDATE project_operation_maintenance
            SET approval_scan_after_seq = 0,
                approval_scan_high_water_seq = 0,
                approval_scan_epoch = 0,
                next_operation_approval_seq = ?,
                next_lane = 1
            WHERE singleton = 1
            """,
            (max(sequences, default=0) + 1,),
        )
    else:
        conn.execute(
            """
            UPDATE project_operation_maintenance
            SET next_operation_approval_seq = ?
            WHERE singleton = 1
            """,
            (max(sequences, default=0) + 1,),
        )
    conn.commit()
    conn.execute("PRAGMA foreign_keys=ON")


def _select_approval_maintenance(conn, *, now=100, limit=1):
    with prdb.write_transaction(conn):
        return prdb._select_operation_approval_maintenance(
            conn, now=now, limit=limit
        )


def test_stale_maintenance_epoch_has_finite_high_water_and_revisits(
    operation_conn,
):
    project_id = projects_db.create_project(
        operation_conn,
        name="Finite Approval Epoch",
        folders=("C:/work/finite-approval-epoch",),
    )
    _insert_epoch_approvals(
        operation_conn,
        project_id,
        (1, 2, 3),
        reset_maintenance=True,
    )

    assert _select_approval_maintenance(operation_conn) == (
        "epoch-approval-1",
    )
    assert _maintenance_state(operation_conn) == (
        1,
        "",
        0,
        1,
        3,
        0,
        4,
    )

    _insert_epoch_approvals(
        operation_conn,
        project_id,
        (4,),
        reset_maintenance=False,
    )
    assert _select_approval_maintenance(
        operation_conn, now=0
    ) == ()
    assert _select_approval_maintenance(operation_conn) == (
        "epoch-approval-2",
    )
    assert _select_approval_maintenance(
        operation_conn, now=0
    ) == ()
    assert _select_approval_maintenance(operation_conn) == (
        "epoch-approval-3",
    )
    assert _maintenance_state(operation_conn) == (
        1,
        "",
        0,
        0,
        0,
        1,
        5,
    )

    assert _select_approval_maintenance(
        operation_conn, now=0
    ) == ()
    assert _select_approval_maintenance(operation_conn) == (
        "epoch-approval-1",
    )
    state = _maintenance_state(operation_conn)
    assert state[3:6] == (1, 4, 1)


def test_stale_epoch_restart_and_unfinalized_page_defer_only_one_epoch(
    operation_conn,
):
    project_id = projects_db.create_project(
        operation_conn,
        name="Restart Approval Epoch",
        folders=("C:/work/restart-approval-epoch",),
    )
    _insert_epoch_approvals(
        operation_conn,
        project_id,
        (1, 2),
        reset_maintenance=True,
    )
    assert _select_approval_maintenance(operation_conn) == (
        "epoch-approval-1",
    )

    path = Path(
        operation_conn.execute("PRAGMA database_list").fetchone()["file"]
    )
    restarted = projects_db.connect(path)
    try:
        assert _select_approval_maintenance(
            restarted, now=0
        ) == ()
        assert _select_approval_maintenance(restarted) == (
            "epoch-approval-2",
        )
        assert _maintenance_state(restarted)[3:6] == (0, 0, 1)
        assert _select_approval_maintenance(
            restarted, now=0
        ) == ()
        assert _select_approval_maintenance(restarted) == (
            "epoch-approval-1",
        )
    finally:
        restarted.close()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("approval_scan_after", "raced"),
        ("next_lane", 0),
        ("operation_validation_migration_complete", 0),
        ("approval_scan_after_seq", 1),
        ("approval_scan_high_water_seq", 1),
        ("approval_scan_epoch", 1),
        ("next_operation_approval_seq", 2),
    ),
)
def test_approval_maintenance_every_field_cas_loss_is_write_free(
    operation_conn,
    field,
    value,
):
    if field == "next_lane":
        operation_conn.execute(
            """
            UPDATE project_operation_maintenance
            SET next_lane = 1
            WHERE singleton = 1
            """
        )
        operation_conn.commit()
    columns = (
        "approval_scan_after",
        "next_lane",
        "operation_validation_migration_complete",
        "approval_scan_after_seq",
        "approval_scan_high_water_seq",
        "approval_scan_epoch",
        "next_operation_approval_seq",
    )
    before = tuple(
        operation_conn.execute(
            f"""
            SELECT {", ".join(columns)}
            FROM project_operation_maintenance
            WHERE singleton = 1
            """
        ).fetchone()
    )
    expected = list(before)
    expected[columns.index(field)] = value
    path = Path(
        operation_conn.execute("PRAGMA database_list").fetchone()["file"]
    )
    racer = projects_db.connect(path)
    fired = [False]

    def race_before_update(statement):
        normalized = " ".join(statement.lower().split())
        if (
            not fired[0]
            and normalized.startswith(
                "update project_operation_maintenance"
            )
        ):
            fired[0] = True
            racer.execute(
                f"""
                UPDATE project_operation_maintenance
                SET {field} = ?
                WHERE singleton = 1
                """,
                (value,),
            )
            racer.commit()

    operation_conn.set_trace_callback(race_before_update)
    try:
        assert prdb._select_operation_approval_maintenance(
            operation_conn, now=100, limit=1
        ) == ()
        operation_conn.commit()
    finally:
        operation_conn.set_trace_callback(None)
        racer.close()
    assert fired == [True]
    assert tuple(
        operation_conn.execute(
            f"""
        SELECT {", ".join(columns)}
        FROM project_operation_maintenance
        WHERE singleton = 1
        """
        ).fetchone()
    ) == tuple(expected)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("approval_scan_after", "raced"),
        ("next_lane", 1),
        ("operation_validation_migration_complete", 0),
        ("approval_scan_after_seq", 1),
        ("approval_scan_high_water_seq", 1),
        ("approval_scan_epoch", 1),
    ),
)
def test_link_sequence_all_nonsequence_singleton_cas_losses_are_write_free(
    operation_env,
    field,
    value,
):
    _stage_critical_link(operation_env)
    conn = operation_env["conn"]
    columns = (
        "singleton",
        "approval_scan_after",
        "next_lane",
        "operation_validation_migration_complete",
        "approval_scan_after_seq",
        "approval_scan_high_water_seq",
        "approval_scan_epoch",
        "next_operation_approval_seq",
    )
    maintenance_before = tuple(
        conn.execute(
            f"""
            SELECT {", ".join(columns)}
            FROM project_operation_maintenance
            WHERE singleton = 1
            """
        ).fetchone()
    )
    expected = list(maintenance_before)
    expected[columns.index(field)] = value
    operation_before = _operation_snapshot(operation_env)
    path = Path(
        conn.execute("PRAGMA database_list").fetchone()["file"]
    )
    racer = projects_db.connect(path)
    fired = [False]

    def race_before_allocation(statement):
        normalized = " ".join(statement.lower().split())
        if (
            not fired[0]
            and normalized.startswith(
                "update project_operation_maintenance"
            )
        ):
            fired[0] = True
            racer.execute(
                f"""
                UPDATE project_operation_maintenance
                SET {field} = ?
                WHERE singleton = 1
                """,
                (value,),
            )
            racer.commit()

    conn.set_trace_callback(race_before_allocation)
    try:
        with pytest.raises(
            RuntimeError,
            match="maintenance sequence changed",
        ):
            prdb._link_project_operation_approval(
                conn,
                project_id=operation_env["project_id"],
                turn_id=operation_env["turn"].turn_id,
                operation_id="operation-staging",
                approval_id="approval-staging",
                attempt_id=operation_env["claim"].attempt_id,
                lease_generation=(
                    operation_env["claim"].lease_generation
                ),
                fencing_token=operation_env["claim"].fencing_token,
                now=operation_env["now"][0],
            )
        conn.commit()
    finally:
        conn.set_trace_callback(None)
        racer.close()

    assert fired == [True]
    assert tuple(
        conn.execute(
            f"""
            SELECT {", ".join(columns)}
            FROM project_operation_maintenance
            WHERE singleton = 1
            """
        ).fetchone()
    ) == tuple(expected)
    assert operation_before == _operation_snapshot(operation_env)
    assert conn.execute(
        """
        SELECT guard_validated, approval_id, status
        FROM project_operations
        WHERE operation_id = 'operation-staging'
        """
    ).fetchone()[:] == (0, None, "approved")
    assert conn.execute(
        """
        SELECT operation_id, operation_maintenance_seq
        FROM project_approvals
        WHERE approval_id = 'approval-staging'
        """
    ).fetchone()[:] == (None, None)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("singleton", 2),
        ("approval_scan_after", sqlite3.Binary(b"invalid")),
        ("next_lane", 2),
        ("operation_validation_migration_complete", 2),
        ("approval_scan_after_seq", -1),
        ("approval_scan_high_water_seq", -1),
        ("approval_scan_epoch", -1),
        ("next_operation_approval_seq", 0),
    ),
)
def test_link_sequence_validates_the_complete_singleton_before_allocation(
    operation_env,
    field,
    value,
):
    _stage_critical_link(operation_env)
    conn = operation_env["conn"]
    operation_before = _operation_snapshot(operation_env)
    conn.execute("PRAGMA ignore_check_constraints=ON")
    conn.execute(
        f"""
        UPDATE project_operation_maintenance
        SET {field} = ?
        """,
        (value,),
    )
    conn.execute("PRAGMA ignore_check_constraints=OFF")
    conn.commit()

    with pytest.raises(
        RuntimeError, match="invalid operation maintenance state"
    ):
        prdb._link_project_operation_approval(
            conn,
            project_id=operation_env["project_id"],
            turn_id=operation_env["turn"].turn_id,
            operation_id="operation-staging",
            approval_id="approval-staging",
            attempt_id=operation_env["claim"].attempt_id,
            lease_generation=(
                operation_env["claim"].lease_generation
            ),
            fencing_token=operation_env["claim"].fencing_token,
            now=operation_env["now"][0],
        )

    assert operation_before == _operation_snapshot(operation_env)
    assert conn.execute(
        """
        SELECT guard_validated, approval_id, status
        FROM project_operations
        WHERE operation_id = 'operation-staging'
        """
    ).fetchone()[:] == (0, None, "approved")
    assert conn.execute(
        """
        SELECT operation_id, operation_maintenance_seq
        FROM project_approvals
        WHERE approval_id = 'approval-staging'
        """
    ).fetchone()[:] == (None, None)


@pytest.mark.parametrize(
    "drift",
    ("runtime_version", "lifecycle", "phase", "control_version"),
)
def test_bounded_stale_lane_finalizes_all_boundary_drift(
    operation_env, drift
):
    _prepare_critical(operation_env, expires_at=1_000)
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    turn_id = operation_env["turn"].turn_id
    statement, parameters = {
        "runtime_version": (
            """
            UPDATE project_runtime_state SET version = version + 1
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "lifecycle": (
            """
            UPDATE project_runtime_state
            SET lifecycle = 'awaiting_acceptance'
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "phase": (
            """
            UPDATE project_runtime_state
            SET current_phase = 'verification'
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "control_version": (
            """
            UPDATE project_run_controls
            SET control_version = control_version + 1
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn_id),
        ),
    }[drift]
    conn.execute(statement, parameters)
    conn.commit()
    prior_version = prdb.runtime_state_for_project(
        conn, project_id
    ).version
    prior_control = prdb._runtime_control_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    )
    assert prior_control is not None

    assert operation_env[
        "guard"
    ].expire_due_operation_approvals(limit=1) == ()
    stale = operation_env[
        "guard"
    ].expire_due_operation_approvals(limit=1)

    assert len(stale) == 1
    assert stale[0].blocked_reason == "approval_stale_boundary"
    _assert_policy_failure(
        operation_env,
        approval_status="expired",
        blocked_reason="approval_stale_boundary",
        event_reason="stale_boundary",
        expected_execution_state="started",
        prior_version=prior_version,
        prior_control_version=prior_control.control_version,
    )


@pytest.mark.parametrize(
    "drift",
    ("runtime_version", "lifecycle", "phase", "control_version"),
)
def test_replenished_stale_epoch_finalizes_old_approval_within_bound(
    operation_env,
    drift,
):
    limit = 1
    _prepare_critical(operation_env, expires_at=1_000)
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    turn_id = operation_env["turn"].turn_id
    guard = operation_env["guard"]

    assert guard.expire_due_operation_approvals(limit=limit) == ()
    assert _maintenance_state(conn)[2] == 1
    _insert_live_replenishment_approval(operation_env, 2)
    assert guard.expire_due_operation_approvals(limit=limit) == ()
    maintenance = _maintenance_state(conn)
    assert maintenance[2:6] == (0, 1, 2, 0)
    assert conn.execute(
        """
        SELECT status FROM project_approvals
        WHERE approval_id = 'approval-1'
        """
    ).fetchone()[0] == "pending"

    statement, parameters = {
        "runtime_version": (
            """
            UPDATE project_runtime_state SET version = version + 1
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "lifecycle": (
            """
            UPDATE project_runtime_state
            SET lifecycle = 'awaiting_acceptance'
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "phase": (
            """
            UPDATE project_runtime_state
            SET current_phase = 'verification'
            WHERE project_id = ?
            """,
            (project_id,),
        ),
        "control_version": (
            """
            UPDATE project_run_controls
            SET control_version = control_version + 1
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn_id),
        ),
    }[drift]
    conn.execute(statement, parameters)
    conn.commit()
    prior_version = prdb.runtime_state_for_project(
        conn, project_id
    ).version
    prior_control = prdb._runtime_control_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    )
    assert prior_control is not None
    prior_events = conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()[0]
    remaining_in_epoch = conn.execute(
        """
        SELECT COUNT(*) FROM project_approvals
        WHERE status = 'pending'
          AND operation_id IS NOT NULL
          AND operation_maintenance_seq > ?
          AND operation_maintenance_seq <= ?
        """,
        (maintenance[3], maintenance[4]),
    ).fetchone()[0]
    stale_lane_bound = (
        remaining_in_epoch + limit - 1
    ) // limit + 1
    assert stale_lane_bound == 2

    finalized = []
    maintenance_calls = 0
    for stale_lane in range(1, stale_lane_bound + 1):
        assert _maintenance_state(conn)[2] == 0
        assert guard.expire_due_operation_approvals(
            limit=limit
        ) == ()
        maintenance_calls += 1
        assert _maintenance_state(conn)[2] == 1
        _insert_live_replenishment_approval(
            operation_env, 2 + stale_lane
        )
        finalized.extend(
            guard.expire_due_operation_approvals(limit=limit)
        )
        maintenance_calls += 1
        assert _maintenance_state(conn)[2] == 0

    assert maintenance_calls == 2 * stale_lane_bound
    assert len(finalized) == 1
    assert finalized[0].operation_id == "operation-1"
    assert (
        finalized[0].blocked_reason
        == "approval_stale_boundary"
    )
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()[0] == prior_events + 3
    _assert_policy_failure(
        operation_env,
        approval_status="expired",
        blocked_reason="approval_stale_boundary",
        event_reason="stale_boundary",
        expected_execution_state="started",
        prior_version=prior_version,
        prior_control_version=prior_control.control_version,
    )


def test_approval_maintenance_queries_are_indexed_and_population_bounded(
    operation_conn,
):
    indexes = {
        row["name"]
        for row in operation_conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'index' AND tbl_name = 'project_approvals'
            """
        )
    }
    assert {
        "idx_project_approvals_operation_due",
        "idx_project_approvals_operation_maintenance_seq",
        "idx_project_approvals_operation_stale_epoch",
    } <= indexes
    due_plan = operation_conn.execute(
        "EXPLAIN QUERY PLAN " + prdb._APPROVAL_DUE_MAINTENANCE_SQL,
        (100, 10),
    ).fetchall()
    high_water_plan = operation_conn.execute(
        "EXPLAIN QUERY PLAN "
        + prdb._APPROVAL_STALE_HIGH_WATER_SQL
    ).fetchall()
    stale_plan = operation_conn.execute(
        "EXPLAIN QUERY PLAN " + prdb._APPROVAL_STALE_MAINTENANCE_SQL,
        (0, 10_000, 10),
    ).fetchall()
    remaining_plan = operation_conn.execute(
        "EXPLAIN QUERY PLAN "
        + prdb._APPROVAL_STALE_REMAINING_SQL,
        (0, 10_000),
    ).fetchall()
    due_details = " ".join(row["detail"] for row in due_plan)
    high_water_details = " ".join(
        row["detail"] for row in high_water_plan
    )
    stale_details = " ".join(row["detail"] for row in stale_plan)
    remaining_details = " ".join(
        row["detail"] for row in remaining_plan
    )
    assert "idx_project_approvals_operation_due" in due_details
    for details in (
        high_water_details,
        stale_details,
        remaining_details,
    ):
        assert "idx_project_approvals_operation_stale_epoch" in details
    assert "USE TEMP B-TREE" not in (
        due_details
        + high_water_details
        + stale_details
        + remaining_details
    )

    project_id = projects_db.create_project(
        operation_conn,
        name="Approval Maintenance Scale",
        folders=("C:/work/approval-maintenance",),
    )
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=OFF")
    approval_rows = []
    for ordinal in range(5_000):
        approval_rows.append(
            (
                f"final-{ordinal:05d}",
                project_id,
                f"final-operation-{ordinal:05d}",
                ordinal + 1,
                ordinal + 1,
                f'["C:/final/{ordinal}"]',
                f'{{"batch_id":"final-{ordinal}"}}',
                "approved",
                1,
                1,
                "owner-1",
                1,
            )
        )
        approval_rows.append(
            (
                f"live-{ordinal:05d}",
                project_id,
                f"live-operation-{ordinal:05d}",
                ordinal + 5_001,
                ordinal + 10_001,
                f'["C:/live/{ordinal}"]',
                f'{{"batch_id":"live-{ordinal}"}}',
                "pending",
                10_000,
                None,
                None,
                None,
            )
        )
    approval_insert_trigger = (
        _approval_task6_insert_trigger_definition(operation_conn)
    )
    operation_conn.execute(
        "DROP TRIGGER trg_project_approvals_task6_insert"
    )
    try:
        operation_conn.executemany(
            """
            INSERT INTO project_approvals (
                approval_id, project_id, turn_id, operation_id,
                operation_maintenance_seq, actor_id,
                authorization_actor_id, canonical_action, approval_class,
                command_revision, expected_runtime_version,
                effective_runtime_version, turn_expected_control_version,
                expected_lifecycle, expected_phase, targets_json,
                batch_boundary_json, status, expires_at, resolved_at,
                resolved_by_actor_id, consumed_at, created_at
            ) VALUES (
                ?, ?, NULL, ?, ?, 'owner-1', 'owner-1',
                'publish', 'publish',
                ?, 0, 0, NULL, 'active', 'implementation', ?, ?, ?, ?,
                ?, ?, ?, 1
            )
            """,
            approval_rows,
        )
    finally:
        operation_conn.execute(approval_insert_trigger)
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=ON")

    def progress_for(sql, parameters):
        progress = [0]

        def count_progress():
            progress[0] += 1
            return 0

        operation_conn.set_progress_handler(count_progress, 1)
        try:
            rows = operation_conn.execute(
                sql, parameters
            ).fetchall()
        finally:
            operation_conn.set_progress_handler(None, 0)
        return rows, progress[0]

    due, due_steps = progress_for(
        prdb._APPROVAL_DUE_MAINTENANCE_SQL, (100, 10)
    )
    high_water, high_water_steps = progress_for(
        prdb._APPROVAL_STALE_HIGH_WATER_SQL, ()
    )
    stale, stale_steps = progress_for(
        prdb._APPROVAL_STALE_MAINTENANCE_SQL, (0, 10_000, 10)
    )
    remaining, remaining_steps = progress_for(
        prdb._APPROVAL_STALE_REMAINING_SQL, (0, 10_000)
    )
    assert due == []
    assert high_water[0]["operation_maintenance_seq"] == 10_000
    assert len(stale) == 10
    assert len(remaining) == 1
    assert due_steps < 500
    assert high_water_steps < 500
    assert stale_steps < 500
    assert remaining_steps < 500


def _insert_migration_operation(
    conn,
    project_id,
    operation_id,
    approval_id,
):
    conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            approval_id, command_revision, targets_json, payload_json,
            status, receipt_json, created_at, updated_at, guard_revision
        ) VALUES (?, ?, NULL, ?, ?, 1, '[]', '{}', 'intent', NULL, 1, 1, 0)
        """,
        (
            operation_id,
            project_id,
            f"migration-key-{operation_id}",
            approval_id,
        ),
    )


def _insert_migration_approval(
    conn,
    project_id,
    approval_id,
    operation_id,
    revision,
    *,
    maintenance_seq=None,
):
    conn.execute(
        """
        INSERT INTO project_approvals (
            approval_id, project_id, turn_id, operation_id,
            operation_maintenance_seq, actor_id,
            authorization_actor_id, canonical_action, approval_class,
            command_revision, expected_runtime_version,
            effective_runtime_version, turn_expected_control_version,
            expected_lifecycle, expected_phase, targets_json,
            batch_boundary_json, status, expires_at, resolved_at,
            resolved_by_actor_id, consumed_at, created_at
        ) VALUES (
            ?, ?, NULL, ?, ?, 'owner', 'owner', 'publish', 'publish', ?,
            0, 0, NULL, 'active', 'implementation', ?, ?,
            'pending', 100, NULL, NULL, NULL, 1
        )
        """,
        (
            approval_id,
            project_id,
            operation_id,
            maintenance_seq,
            revision,
            f'["C:/migration/{approval_id}"]',
            f'{{"batch_id":"{approval_id}"}}',
        ),
    )


def _drop_operation_sequence_guards(conn):
    conn.execute(
        "DROP TRIGGER IF EXISTS trg_project_approvals_task6_insert"
    )
    conn.execute(
        "DROP TRIGGER IF EXISTS trg_project_approvals_task6_update"
    )
    conn.execute(
        """
        DROP INDEX IF EXISTS
        idx_project_approvals_operation_maintenance_seq
        """
    )
    conn.execute(
        """
        DROP INDEX IF EXISTS
        idx_project_approvals_operation_stale_epoch
        """
    )


def test_sequence_migration_dense_remaps_signed_rowids_and_is_idempotent(
    operation_conn,
):
    project_id = _insert_project(
        operation_conn, "signed-sequence-migration"
    )
    _drop_operation_sequence_guards(operation_conn)
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=OFF")
    signed = (
        ("approval-positive", "operation-positive", 9, 3),
        ("approval-negative", "operation-negative", -4, 1),
        ("approval-zero", "operation-zero", 0, 2),
    )
    for approval_id, operation_id, rowid, revision in signed:
        _insert_migration_operation(
            operation_conn,
            project_id,
            operation_id,
            approval_id,
        )
        _insert_migration_approval(
            operation_conn,
            project_id,
            approval_id,
            operation_id,
            revision,
        )
        operation_conn.execute(
            """
            UPDATE project_approvals
            SET rowid = ?
            WHERE approval_id = ?
            """,
            (rowid, approval_id),
        )
    operation_conn.execute(
        """
        UPDATE project_operation_maintenance
        SET approval_scan_after_seq = 7,
            approval_scan_high_water_seq = 9,
            approval_scan_epoch = 11,
            next_operation_approval_seq = 1
        WHERE singleton = 1
        """
    )
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=ON")

    prdb.ensure_schema(operation_conn)
    expected = (
        ("approval-negative", -4, 1),
        ("approval-zero", 0, 2),
        ("approval-positive", 9, 3),
    )
    assert tuple(
        tuple(row)
        for row in operation_conn.execute(
            """
            SELECT approval_id, rowid, operation_maintenance_seq
            FROM project_approvals
            WHERE project_id = ?
            ORDER BY rowid, approval_id
            """,
            (project_id,),
        )
    ) == expected
    assert _maintenance_state(operation_conn)[3:] == (
        0,
        0,
        0,
        4,
    )

    prdb.ensure_schema(operation_conn)
    assert tuple(
        tuple(row)
        for row in operation_conn.execute(
            """
            SELECT approval_id, rowid, operation_maintenance_seq
            FROM project_approvals
            WHERE project_id = ?
            ORDER BY rowid, approval_id
            """,
            (project_id,),
        )
    ) == expected


@pytest.mark.parametrize("invalid_shape", ("zero", "duplicate"))
def test_sequence_migration_rejects_invalid_stored_values_atomically(
    operation_conn,
    invalid_shape,
):
    project_id = _insert_project(
        operation_conn, f"invalid-sequence-{invalid_shape}"
    )
    _drop_operation_sequence_guards(operation_conn)
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=OFF")
    for ordinal in (1, 2):
        _insert_migration_operation(
            operation_conn,
            project_id,
            f"operation-{ordinal}",
            f"approval-{ordinal}",
        )
        _insert_migration_approval(
            operation_conn,
            project_id,
            f"approval-{ordinal}",
            f"operation-{ordinal}",
            ordinal,
        )
    operation_conn.execute("PRAGMA ignore_check_constraints=ON")
    values = (0, 2) if invalid_shape == "zero" else (1, 1)
    for ordinal, value in enumerate(values, start=1):
        operation_conn.execute(
            """
            UPDATE project_approvals
            SET operation_maintenance_seq = ?
            WHERE approval_id = ?
            """,
            (value, f"approval-{ordinal}"),
        )
    operation_conn.execute("PRAGMA ignore_check_constraints=OFF")
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=ON")
    before = tuple(
        tuple(row)
        for row in operation_conn.execute(
            """
            SELECT approval_id, operation_maintenance_seq
            FROM project_approvals
            WHERE project_id = ?
            ORDER BY approval_id
            """,
            (project_id,),
        )
    )

    with pytest.raises(prdb.OperationMigrationError):
        prdb.ensure_schema(operation_conn)

    assert tuple(
        tuple(row)
        for row in operation_conn.execute(
            """
            SELECT approval_id, operation_maintenance_seq
            FROM project_approvals
            WHERE project_id = ?
            ORDER BY approval_id
            """,
            (project_id,),
        )
    ) == before


@pytest.mark.parametrize(
    "shape",
    (
        "one_way_operation",
        "one_way_approval",
        "dangling_operation",
        "dangling_approval",
        "crossed",
    ),
)
def test_migration_rejects_non_inverse_operation_approval_links(
    operation_conn, shape
):
    project_id = _insert_project(operation_conn, f"migration-{shape}")
    operation_conn.execute(
        "DROP INDEX idx_project_operations_one_approval"
    )
    operation_conn.execute(
        "DROP INDEX idx_project_approvals_one_operation"
    )
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=OFF")
    if shape == "one_way_operation":
        _insert_migration_operation(
            operation_conn, project_id, "operation-1", "approval-1"
        )
        _insert_migration_approval(
            operation_conn, project_id, "approval-1", None, 1
        )
    elif shape == "one_way_approval":
        _insert_migration_operation(
            operation_conn, project_id, "operation-1", None
        )
        _insert_migration_approval(
            operation_conn,
            project_id,
            "approval-1",
            "operation-1",
            1,
            maintenance_seq=1,
        )
    elif shape == "dangling_operation":
        _insert_migration_operation(
            operation_conn, project_id, "operation-1", "approval-missing"
        )
    elif shape == "dangling_approval":
        approval_insert_trigger = (
            _approval_task6_insert_trigger_definition(operation_conn)
        )
        operation_conn.execute(
            "DROP TRIGGER trg_project_approvals_task6_insert"
        )
        try:
            _insert_migration_approval(
                operation_conn,
                project_id,
                "approval-1",
                "operation-missing",
                1,
                maintenance_seq=1,
            )
        finally:
            operation_conn.execute(approval_insert_trigger)
    else:
        _insert_migration_operation(
            operation_conn, project_id, "operation-1", "approval-1"
        )
        _insert_migration_operation(
            operation_conn, project_id, "operation-2", "approval-2"
        )
        _insert_migration_approval(
            operation_conn,
            project_id,
            "approval-1",
            "operation-2",
            1,
            maintenance_seq=1,
        )
        _insert_migration_approval(
            operation_conn,
            project_id,
            "approval-2",
            "operation-1",
            2,
            maintenance_seq=2,
        )
    operation_conn.commit()
    operation_conn.execute("PRAGMA foreign_keys=ON")
    before_operations = tuple(
        tuple(row)
        for row in operation_conn.execute(
            """
            SELECT operation_id, approval_id
            FROM project_operations
            WHERE project_id = ?
            ORDER BY operation_id
            """,
            (project_id,),
        )
    )
    before_approvals = tuple(
        tuple(row)
        for row in operation_conn.execute(
            """
            SELECT approval_id, operation_id
            FROM project_approvals
            WHERE project_id = ?
            ORDER BY approval_id
            """,
            (project_id,),
        )
    )

    with pytest.raises(prdb.OperationMigrationError):
        prdb.ensure_schema(operation_conn)

    assert before_operations == tuple(
        tuple(row)
        for row in operation_conn.execute(
            """
            SELECT operation_id, approval_id
            FROM project_operations
            WHERE project_id = ?
            ORDER BY operation_id
            """,
            (project_id,),
        )
    )
    assert before_approvals == tuple(
        tuple(row)
        for row in operation_conn.execute(
            """
            SELECT approval_id, operation_id
            FROM project_approvals
            WHERE project_id = ?
            ORDER BY approval_id
            """,
            (project_id,),
        )
    )


def test_approval_maintenance_schema_is_idempotent(operation_conn):
    prdb.ensure_schema(operation_conn)
    prdb.ensure_schema(operation_conn)
    assert _maintenance_state(operation_conn) == (
        1,
        "",
        0,
        0,
        0,
        0,
        1,
    )
    assert {
        row["name"]
        for row in operation_conn.execute(
            "PRAGMA table_info(project_approvals)"
        )
    } >= {"operation_maintenance_seq"}
    assert operation_conn.execute(
        """
        SELECT COUNT(*) FROM project_operation_maintenance
        """
    ).fetchone()[0] == 1


def test_operation_intent_requires_exact_builtin_remote_capability(
    operation_env,
):
    intent = _intent(operation_env)
    assert intent.remote_idempotency_supported is True
    with pytest.raises(TypeError):
        replace(intent, remote_idempotency_supported=1)
    with pytest.raises(TypeError):
        replace(intent, remote_idempotency_supported=None)


def test_task6_static_critical_map_exactly_matches_task2_policy_authority(
    operation_conn,
):
    assert prdb.TASK6_CRITICAL_ACTION_APPROVAL_CLASSES == (
        CRITICAL_ACTION_CASES
    )
    assert tuple(
        tuple(row)
        for row in operation_conn.execute(
            f"""
            SELECT column1, column2
            FROM (
                VALUES
                {prdb._TASK6_CRITICAL_ACTION_VALUES_SQL}
            )
            """
        )
    ) == CRITICAL_ACTION_CASES


def _operation_task6_trigger_definitions(conn):
    return {
        row["name"]: row["sql"]
        for row in conn.execute(
            """
            SELECT name, sql FROM sqlite_master
            WHERE type = 'trigger'
              AND name IN (
                  'trg_project_operations_task6_insert',
                  'trg_project_operations_task6_update'
              )
            ORDER BY name
            """
        )
    }


def _approval_task6_insert_trigger_definition(conn):
    row = conn.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'trigger'
          AND name = 'trg_project_approvals_task6_insert'
        """
    ).fetchone()
    assert row is not None
    return row["sql"]


def _without_inverse_approval_clause(trigger_sql):
    fingerprint_match = trigger_sql.index(
        "NEW.approval_id = json_extract"
    )
    exists_start = trigger_sql.index(
        "AND EXISTS (", fingerprint_match
    )
    opening = trigger_sql.index("(", exists_start)
    depth = 0
    exists_end = None
    for offset in range(opening, len(trigger_sql)):
        character = trigger_sql[offset]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                exists_end = offset + 1
                break
    assert exists_end is not None
    old_sql = (
        trigger_sql[:exists_start]
        + "AND 1"
        + trigger_sql[exists_end:]
    )
    assert "inverse_approval.operation_id" not in old_sql
    return old_sql


def _without_no_incoming_approval_clause(trigger_sql):
    incoming_match = trigger_sql.index(
        "incoming_approval.operation_id"
    )
    exists_start = trigger_sql.rfind(
        "NOT EXISTS (", 0, incoming_match
    )
    assert exists_start >= 0
    opening = trigger_sql.index("(", exists_start)
    depth = 0
    exists_end = None
    for offset in range(opening, len(trigger_sql)):
        character = trigger_sql[offset]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                exists_end = offset + 1
                break
    assert exists_end is not None
    old_sql = (
        trigger_sql[:exists_start]
        + "1"
        + trigger_sql[exists_end:]
    )
    assert "incoming_approval.operation_id" not in old_sql
    return old_sql


def _without_approval_insert_target_guard(trigger_sql):
    target_match = trigger_sql.index(
        "operation.guard_validated = 0"
    )
    exists_start = trigger_sql.rfind(
        "AND EXISTS (", 0, target_match
    )
    assert exists_start >= 0
    opening = trigger_sql.index("(", exists_start)
    depth = 0
    exists_end = None
    for offset in range(opening, len(trigger_sql)):
        character = trigger_sql[offset]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                exists_end = offset + 1
                break
    assert exists_end is not None
    old_sql = trigger_sql[:exists_start] + trigger_sql[exists_end:]
    assert "operation.guard_validated = 0" not in old_sql
    return old_sql


@pytest.mark.parametrize(
    "missing_clause", ("inverse", "no-incoming")
)
def test_ensure_replaces_old_task6_triggers_and_repeat_is_stable(
    operation_conn,
    missing_clause,
):
    canonical = _operation_task6_trigger_definitions(operation_conn)
    assert set(canonical) == {
        "trg_project_operations_task6_insert",
        "trg_project_operations_task6_update",
    }
    assert all(
        "inverse_approval.operation_id" in sql
        for sql in canonical.values()
    )
    assert all(
        "incoming_approval.operation_id" in sql
        for sql in canonical.values()
    )
    stripper = (
        _without_inverse_approval_clause
        if missing_clause == "inverse"
        else _without_no_incoming_approval_clause
    )
    old = {
        name: stripper(sql)
        for name, sql in canonical.items()
    }
    for name in canonical:
        operation_conn.execute(f"DROP TRIGGER {name}")
    for sql in old.values():
        operation_conn.execute(sql)
    operation_conn.commit()
    assert _operation_task6_trigger_definitions(operation_conn) == old

    prdb.ensure_schema(operation_conn)

    upgraded = _operation_task6_trigger_definitions(operation_conn)
    assert upgraded == canonical
    assert all(
        "inverse_approval.operation_id" in sql
        for sql in upgraded.values()
    )
    assert all(
        "incoming_approval.operation_id" in sql
        for sql in upgraded.values()
    )
    prdb.ensure_schema(operation_conn)
    assert _operation_task6_trigger_definitions(
        operation_conn
    ) == upgraded


def test_ensure_replaces_old_approval_insert_trigger_and_repeat_is_stable(
    operation_conn,
):
    canonical = _approval_task6_insert_trigger_definition(
        operation_conn
    )
    assert "operation.guard_validated = 0" in canonical
    old = _without_approval_insert_target_guard(canonical)
    operation_conn.execute(
        "DROP TRIGGER trg_project_approvals_task6_insert"
    )
    operation_conn.execute(old)
    operation_conn.commit()
    assert (
        _approval_task6_insert_trigger_definition(operation_conn)
        == old
    )

    prdb.ensure_schema(operation_conn)

    upgraded = _approval_task6_insert_trigger_definition(
        operation_conn
    )
    assert upgraded == canonical
    assert "operation.guard_validated = 0" in upgraded
    prdb.ensure_schema(operation_conn)
    assert (
        _approval_task6_insert_trigger_definition(operation_conn)
        == upgraded
    )


@pytest.mark.parametrize(
    ("action", "approval_class"),
    CRITICAL_ACTION_CASES,
)
@pytest.mark.parametrize(
    "capability_supported",
    (True, False),
    ids=("linked", "capability-blocked"),
)
def test_every_critical_action_requires_its_exact_owner_approval_class(
    operation_env,
    action,
    approval_class,
    capability_supported,
):
    module = operation_env["module"]
    approval_id = f"approval-{action}"
    prepared = operation_env["guard"].prepare(
        operation_env["claim"],
        _intent(
            operation_env,
            canonical_action=action,
            batch_items=(action,),
            remote_idempotency_supported=capability_supported,
        ),
        policy=PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            CRITICAL_ACTION_RULES[action].rule_id,
            "critical action requires owner approval",
            approval_class,
        ),
        approval=module.OperationApprovalSpec(
            approval_id,
            approval_class,
            1_000,
            operation_env["actor"],
        ),
    )
    row = operation_env["conn"].execute(
        """
        SELECT approval_id, approval_fingerprint_json, status,
               blocked_reason
        FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    fingerprint = json.loads(row["approval_fingerprint_json"])

    assert fingerprint == {
        "approval_class": approval_class,
        "approval_id": approval_id,
        "authorization_actor_id": "owner-1",
        "expires_at": 1_000,
        "requires_owner": True,
    }
    if capability_supported:
        assert prepared.status == "awaiting_approval"
        assert tuple(row[::2]) == (
            approval_id,
            "awaiting_approval",
        )
        assert operation_env["conn"].execute(
            """
            SELECT operation_id
            FROM project_approvals
            WHERE project_id = ? AND approval_id = ?
            """,
            (operation_env["project_id"], approval_id),
        ).fetchone()[0] == "operation-1"
    else:
        assert prepared.status == "blocked"
        assert tuple(row[::2]) == (None, "blocked")
        assert row["blocked_reason"] == (
            "operation_capability_unsupported"
        )
        assert operation_env["conn"].execute(
            """
            SELECT COUNT(*)
            FROM project_approvals
            WHERE project_id = ?
            """,
            (operation_env["project_id"],),
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    ("action", "approval_class"),
    CRITICAL_ACTION_CASES,
)
def test_every_critical_action_rejects_allow_before_any_lookup_or_write(
    operation_env,
    monkeypatch,
    action,
    approval_class,
):
    module = operation_env["module"]
    before = _operation_snapshot(operation_env)

    def unexpected_lookup(**_kwargs):
        raise AssertionError("critical ALLOW reached operation lookup")

    monkeypatch.setattr(
        operation_env["guard"],
        "_existing_operation",
        unexpected_lookup,
    )
    with pytest.raises(module.ProjectOperationError) as denied:
        operation_env["guard"].prepare(
            operation_env["claim"],
            _intent(operation_env, canonical_action=action),
            policy=PolicyDecision(
                Decision.ALLOW,
                f"policy.invalid.allow.{approval_class}",
                "critical action cannot be allowed directly",
            ),
            approval=None,
        )

    assert (
        denied.value.code
        is module.OperationErrorCode.OPERATION_POLICY_DENIED
    )
    assert before == _operation_snapshot(operation_env)


def _raw_critical_fingerprint(
    approval_class: str,
    *,
    approval_id: str,
) -> str:
    return json.dumps(
        {
            "approval_class": approval_class,
            "approval_id": approval_id,
            "authorization_actor_id": "owner-1",
            "expires_at": 1_000,
            "requires_owner": True,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _stage_noninverse_critical_approval(
    operation_env,
    *,
    approval_id,
    link_shape,
):
    conn = operation_env["conn"]
    project_id = operation_env["project_id"]
    if link_shape == "crossed":
        other_operation_id = f"{approval_id}-other-operation"
        assert prdb._insert_project_operation(
            conn,
            operation_id=other_operation_id,
            project_id=project_id,
            turn_id=operation_env["turn"].turn_id,
            idempotency_key=f"{other_operation_id}-key",
            command_revision=1,
            targets_json='["c:/work/operations/other.py"]',
            payload_json="{}",
            status="approved",
            canonical_action="local_code_edit",
            batch_items_json='["other"]',
            readback_kind="remote-ledger",
            attempt_id=operation_env["claim"].attempt_id,
            lease_generation=(
                operation_env["claim"].lease_generation
            ),
            fencing_token=operation_env["claim"].fencing_token,
            blocked_reason=None,
            remote_idempotency_supported=True,
            approval_fingerprint_json=None,
            now=operation_env["now"][0],
        )
        _insert_migration_approval(
            conn,
            project_id,
            approval_id,
            other_operation_id,
            1,
            maintenance_seq=10_000,
        )
    else:
        _insert_migration_approval(
            conn,
            project_id,
            approval_id,
            None,
            1,
        )
    conn.commit()
    return _raw_critical_fingerprint(
        "publish", approval_id=approval_id
    )


@pytest.mark.parametrize("authority_path", ("insert", "update"))
@pytest.mark.parametrize("link_shape", ("generic", "crossed"))
def test_task6_trigger_rejects_noninverse_critical_approval_authority(
    operation_env,
    authority_path,
    link_shape,
):
    conn = operation_env["conn"]
    approval_id = f"{authority_path}-{link_shape}-approval"
    if authority_path == "update":
        _prepare_critical(operation_env)
    fingerprint = _stage_noninverse_critical_approval(
        operation_env,
        approval_id=approval_id,
        link_shape=link_shape,
    )
    if authority_path == "update":
        operation = prdb._project_operation_for_id(
            conn,
            project_id=operation_env["project_id"],
            operation_id="operation-1",
        )
        assert operation is not None
        prdb._decertify_project_operation(conn, operation)
        conn.execute(
            """
            UPDATE project_operations
            SET approval_id = ?, approval_fingerprint_json = ?
            WHERE operation_id = 'operation-1'
              AND guard_validated = 0
            """,
            (approval_id, fingerprint),
        )
        conn.commit()
    before = _operation_snapshot(operation_env)

    conn.execute("SAVEPOINT noninverse_critical_authority")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            if authority_path == "insert":
                conn.execute(
                    """
                    INSERT INTO project_operations (
                        operation_id, project_id, turn_id,
                        idempotency_key, approval_id,
                        command_revision, targets_json, payload_json,
                        status, created_at, updated_at, guard_revision,
                        guard_validated, canonical_action,
                        batch_items_json, readback_kind, attempt_id,
                        lease_generation, fencing_token,
                        remote_idempotency_supported,
                        approval_fingerprint_json
                    ) VALUES (
                        'raw-noninverse-insert', ?, ?,
                        'raw-noninverse-insert-key', ?,
                        1, '["c:/work/operations/file.py"]', '{}',
                        'approved', 100, 100, 1,
                        1, 'publish', '["publish"]',
                        'remote-ledger', ?, 1, 1, 1, ?
                    )
                    """,
                    (
                        operation_env["project_id"],
                        operation_env["turn"].turn_id,
                        approval_id,
                        operation_env["claim"].attempt_id,
                        fingerprint,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE project_operations
                    SET guard_validated = 1
                    WHERE operation_id = 'operation-1'
                      AND guard_validated = 0
                    """
                )
    finally:
        conn.execute(
            "ROLLBACK TO noninverse_critical_authority"
        )
        conn.execute("RELEASE noninverse_critical_authority")

    assert before == _operation_snapshot(operation_env)
    if authority_path == "insert":
        assert conn.execute(
            """
            SELECT COUNT(*) FROM project_operations
            WHERE operation_id = 'raw-noninverse-insert'
            """
        ).fetchone()[0] == 0
    else:
        assert conn.execute(
            """
            SELECT guard_validated FROM project_operations
            WHERE operation_id = 'operation-1'
            """
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    "authority_shape",
    ("exact-inverse", "capability-blocked", "noncritical"),
)
def test_marker_certification_accepts_only_valid_critical_exceptions(
    operation_env,
    authority_shape,
):
    if authority_shape == "exact-inverse":
        _prepare_critical(operation_env)
    elif authority_shape == "capability-blocked":
        _critical_capability_block(operation_env)
    else:
        _prepare_allowed(operation_env)
    conn = operation_env["conn"]
    operation = prdb._project_operation_for_id(
        conn,
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    assert operation is not None
    prdb._decertify_project_operation(conn, operation)

    assert conn.execute(
        """
        UPDATE project_operations
        SET guard_validated = 1
        WHERE operation_id = 'operation-1'
          AND guard_validated = 0
        """
    ).rowcount == 1
    assert prdb._project_operation_for_id(
        conn,
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    ) is not None


def _prepare_no_link_operation(operation_env, authority_shape):
    if authority_shape == "capability-blocked":
        _critical_capability_block(operation_env)
    else:
        _prepare_allowed(operation_env)
    operation = prdb._project_operation_for_id(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    assert operation is not None
    assert operation.approval_id is None
    prdb._decertify_project_operation(
        operation_env["conn"], operation
    )


@pytest.mark.parametrize(
    "authority_shape", ("capability-blocked", "noncritical")
)
def test_marker_certification_rejects_one_way_incoming_approval(
    operation_env,
    authority_shape,
):
    _prepare_no_link_operation(operation_env, authority_shape)
    conn = operation_env["conn"]
    _insert_migration_approval(
        conn,
        operation_env["project_id"],
        f"incoming-{authority_shape}",
        "operation-1",
        1,
        maintenance_seq=20_000,
    )
    conn.commit()
    before = _operation_snapshot(operation_env)

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            UPDATE project_operations
            SET guard_validated = 1
            WHERE operation_id = 'operation-1'
              AND guard_validated = 0
            """
        )

    assert _operation_snapshot(operation_env) == before
    assert conn.execute(
        """
        SELECT guard_validated FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()[0] == 0


@pytest.mark.parametrize(
    "authority_shape",
    ("capability-blocked", "noncritical", "executable-critical"),
)
def test_linked_approval_insert_rejects_certified_operation(
    operation_env,
    authority_shape,
):
    if authority_shape == "capability-blocked":
        _critical_capability_block(operation_env)
    elif authority_shape == "noncritical":
        _prepare_allowed(operation_env)
    else:
        _prepare_critical(operation_env)
    conn = operation_env["conn"]
    assert conn.execute(
        """
        SELECT guard_validated FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()[0] == 1
    before = _operation_snapshot(operation_env)

    with pytest.raises(sqlite3.IntegrityError):
        _insert_migration_approval(
            conn,
            operation_env["project_id"],
            f"late-{authority_shape}",
            "operation-1",
            2,
            maintenance_seq=22_000,
        )

    assert _operation_snapshot(operation_env) == before


def test_approval_insert_accepts_generic_unlinked_request(
    operation_env,
):
    conn = operation_env["conn"]

    _insert_migration_approval(
        conn,
        operation_env["project_id"],
        "generic-unlinked",
        None,
        1,
    )

    assert tuple(
        conn.execute(
            """
            SELECT operation_id, operation_maintenance_seq
            FROM project_approvals
            WHERE approval_id = 'generic-unlinked'
            """
        ).fetchone()
    ) == (None, None)


def test_approval_insert_accepts_exact_decertified_operation_target(
    operation_env,
):
    conn = operation_env["conn"]
    _insert_migration_operation(
        conn,
        operation_env["project_id"],
        "marker-zero-operation",
        None,
    )

    _insert_migration_approval(
        conn,
        operation_env["project_id"],
        "marker-zero-approval",
        "marker-zero-operation",
        1,
        maintenance_seq=22_001,
    )

    assert tuple(
        conn.execute(
            """
            SELECT approval.operation_id,
                   operation.guard_validated
            FROM project_approvals AS approval
            JOIN project_operations AS operation
              ON operation.project_id = approval.project_id
             AND operation.operation_id = approval.operation_id
            WHERE approval.approval_id = 'marker-zero-approval'
            """
        ).fetchone()
    ) == ("marker-zero-operation", 0)


@pytest.mark.parametrize(
    "authority_shape", ("capability-blocked", "noncritical")
)
def test_raw_insert_rejects_deferred_one_way_incoming_approval(
    operation_env,
    authority_shape,
):
    conn = operation_env["conn"]
    operation_id = f"raw-incoming-{authority_shape}"
    approval_id = f"incoming-insert-{authority_shape}"
    durable_before = _operation_snapshot(operation_env)
    conn.execute("PRAGMA defer_foreign_keys=ON")
    conn.execute("BEGIN")
    try:
        conn.execute(
            "DROP TRIGGER trg_project_approvals_task6_insert"
        )
        _insert_migration_approval(
            conn,
            operation_env["project_id"],
            approval_id,
            operation_id,
            1,
            maintenance_seq=21_000,
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO project_operations (
                    operation_id, project_id, turn_id,
                    idempotency_key, command_revision,
                    targets_json, payload_json, status,
                    created_at, updated_at, guard_revision,
                    guard_validated, canonical_action,
                    batch_items_json, readback_kind, attempt_id,
                    lease_generation, fencing_token, blocked_reason,
                    remote_idempotency_supported,
                    approval_fingerprint_json
                ) VALUES (
                    ?, ?, ?, ?, 1,
                    '["c:/work/operations/file.py"]', '{}', ?,
                    100, 100, 1, 1, ?, '["authority"]', ?, ?,
                    1, 1, ?, ?, ?
                )
                """,
                (
                    operation_id,
                    operation_env["project_id"],
                    operation_env["turn"].turn_id,
                    f"{operation_id}-key",
                    (
                        "blocked"
                        if authority_shape == "capability-blocked"
                        else "approved"
                    ),
                    (
                        "publish"
                        if authority_shape == "capability-blocked"
                        else "local_code_edit"
                    ),
                    (
                        None
                        if authority_shape == "capability-blocked"
                        else "remote-ledger"
                    ),
                    operation_env["claim"].attempt_id,
                    (
                        "operation_capability_unsupported"
                        if authority_shape == "capability-blocked"
                        else None
                    ),
                    (
                        0
                        if authority_shape == "capability-blocked"
                        else 1
                    ),
                    (
                        _raw_critical_fingerprint(
                            "publish",
                            approval_id=(
                                f"fingerprint-{authority_shape}"
                            ),
                        )
                        if authority_shape == "capability-blocked"
                        else None
                    ),
                ),
            )
    finally:
        conn.rollback()

    assert _operation_snapshot(operation_env) == durable_before
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_approvals
        WHERE approval_id = ?
        """,
        (approval_id,),
    ).fetchone()[0] == 0
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_operations
        WHERE operation_id = ?
        """,
        (operation_id,),
    ).fetchone()[0] == 0


def test_static_trigger_rejects_every_uncertified_critical_authority_shape(
    operation_env,
):
    conn = operation_env["conn"]
    accepted: list[tuple[str, str]] = []
    for action, approval_class in CRITICAL_ACTION_CASES:
        for shape, fingerprint in (
            ("no-fingerprint", None),
            ("malformed-json", "not json"),
            (
                "wrong-class",
                _raw_critical_fingerprint(
                    f"wrong-{approval_class}",
                    approval_id=f"raw-{action}-wrong-class",
                ),
            ),
            (
                "missing-link",
                _raw_critical_fingerprint(
                    approval_class,
                    approval_id=f"raw-{action}-missing-link",
                ),
            ),
        ):
            operation_id = f"raw-{action}-{shape}"
            conn.execute("SAVEPOINT raw_critical_shape")
            try:
                conn.execute(
                    """
                    INSERT INTO project_operations (
                        operation_id, project_id, turn_id,
                        idempotency_key, command_revision,
                        targets_json, payload_json, status,
                        created_at, updated_at, guard_revision,
                        guard_validated, canonical_action,
                        batch_items_json, readback_kind, attempt_id,
                        lease_generation, fencing_token,
                        remote_idempotency_supported,
                        approval_fingerprint_json
                    ) VALUES (
                        ?, ?, ?, ?, 1,
                        '["c:/work/operations/file.py"]', '{}',
                        'approved', 100, 100, 1,
                        1, ?, '["critical"]', 'remote-ledger', ?,
                        1, 1, 1, ?
                    )
                    """,
                    (
                        operation_id,
                        operation_env["project_id"],
                        operation_env["turn"].turn_id,
                        f"{operation_id}-key",
                        action,
                        operation_env["claim"].attempt_id,
                        fingerprint,
                    ),
                )
            except sqlite3.IntegrityError:
                pass
            else:
                accepted.append((action, shape))
            finally:
                conn.execute("ROLLBACK TO raw_critical_shape")
                conn.execute("RELEASE raw_critical_shape")

        operation_id = f"raw-{action}-malformed-update"
        conn.execute("SAVEPOINT raw_critical_update")
        try:
            conn.execute(
                """
                INSERT INTO project_operations (
                    operation_id, project_id, turn_id,
                    idempotency_key, command_revision,
                    targets_json, payload_json, status,
                    created_at, updated_at, guard_revision,
                    guard_validated, canonical_action,
                    batch_items_json, readback_kind, attempt_id,
                    lease_generation, fencing_token,
                    remote_idempotency_supported,
                    approval_fingerprint_json
                ) VALUES (
                    ?, ?, ?, ?, 1,
                    '["c:/work/operations/file.py"]', '{}',
                    'approved', 100, 100, 1,
                    0, ?, '["critical"]', 'remote-ledger', ?,
                    1, 1, 1, 'not json'
                )
                """,
                (
                    operation_id,
                    operation_env["project_id"],
                    operation_env["turn"].turn_id,
                    f"{operation_id}-key",
                    action,
                    operation_env["claim"].attempt_id,
                ),
            )
            try:
                conn.execute(
                    """
                    UPDATE project_operations
                    SET guard_validated = 1
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                )
            except sqlite3.IntegrityError:
                pass
            else:
                accepted.append((action, "malformed-update"))
        finally:
            conn.execute("ROLLBACK TO raw_critical_update")
            conn.execute("RELEASE raw_critical_update")

    assert accepted == []


@pytest.mark.parametrize(
    ("remote_supported", "readback_kind"),
    (
        (False, "remote-ledger"),
        (True, None),
        (False, None),
    ),
)
def test_remote_capability_matrix_blocks_before_effect_authority(
    operation_env,
    remote_supported,
    readback_kind,
):
    blocked = operation_env["guard"].prepare(
        operation_env["claim"],
        _intent(
            operation_env,
            readback_kind=readback_kind,
            remote_idempotency_supported=remote_supported,
        ),
        policy=PolicyDecision(
            Decision.ALLOW, "policy.allow.local", "allowed"
        ),
        approval=None,
    )

    assert blocked.status == "blocked"
    assert blocked.blocked_reason == "operation_capability_unsupported"
    row = operation_env["conn"].execute(
        """
        SELECT remote_idempotency_supported, approval_fingerprint_json,
               idempotency_key
        FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    assert tuple(row) == (
        int(remote_supported),
        None,
        "remote-operation-1",
    )


def _critical_capability_block(
    operation_env,
    *,
    approval_id="approval-1",
    approval_class="publish",
    expires_at=1_000,
    authorization=None,
):
    authorization = authorization or operation_env["actor"]
    intent = _intent(
        operation_env,
        canonical_action="publish",
        remote_idempotency_supported=False,
    )
    policy = PolicyDecision(
        Decision.REQUIRE_APPROVAL,
        "policy.approval.publish",
        "publish is critical",
        approval_class,
    )
    spec = operation_env["module"].OperationApprovalSpec(
        approval_id,
        approval_class,
        expires_at,
        authorization,
    )
    return operation_env["guard"].prepare(
        operation_env["claim"],
        intent,
        policy=policy,
        approval=spec,
    )


def test_capability_blocked_critical_persists_full_fingerprint_without_link(
    operation_env,
):
    first = _critical_capability_block(operation_env)
    expected = (
        '{"approval_class":"publish","approval_id":"approval-1",'
        '"authorization_actor_id":"owner-1","expires_at":1000,'
        '"requires_owner":true}'
    )

    assert first.status == "blocked"
    row = operation_env["conn"].execute(
        """
        SELECT approval_id, approval_fingerprint_json,
               remote_idempotency_supported
        FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()
    assert tuple(row) == (None, expected, 0)
    assert operation_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_approvals
        WHERE project_id = ?
        """,
        (operation_env["project_id"],),
    ).fetchone()[0] == 0

    replay = _critical_capability_block(
        operation_env,
        authorization=operation_env["discord_actor"],
    )
    assert replay == first


@pytest.mark.parametrize(
    "drift",
    (
        "approval_id",
        "approval_class",
        "expires_at",
        "authorization_actor_id",
        "requires_owner",
    ),
)
def test_capability_blocked_critical_replay_rejects_every_fingerprint_drift(
    operation_env, drift
):
    _critical_capability_block(operation_env)
    values = {
        "approval_id": "approval-1",
        "approval_class": "publish",
        "expires_at": 1_000,
        "authorization": operation_env["actor"],
    }
    if drift == "approval_id":
        values["approval_id"] = "approval-changed"
    elif drift == "approval_class":
        values["approval_class"] = "publish-changed"
    elif drift == "expires_at":
        values["expires_at"] = 2_000
    elif drift == "authorization_actor_id":
        values["authorization"] = ActorContext(
            "owner-2", "desktop", "desktop-owner", True
        )
    else:
        values["authorization"] = ActorContext(
            "owner-1", "desktop", "desktop-owner", False
        )
    before = _operation_snapshot(operation_env)

    with pytest.raises(
        operation_env["module"].ProjectOperationError
    ) as conflict:
        _critical_capability_block(operation_env, **values)

    expected_code = (
        operation_env[
            "module"
        ].OperationErrorCode.INVALID_OPERATION_ARGUMENT
        if drift in {"approval_class", "requires_owner"}
        else operation_env[
            "module"
        ].OperationErrorCode.OPERATION_IDEMPOTENCY_CONFLICT
    )
    assert conflict.value.code is expected_code
    assert before == _operation_snapshot(operation_env)


def test_normal_critical_fingerprint_matches_inverse_and_other_binding_resolves(
    operation_env,
):
    _prepare_critical(operation_env)
    row = operation_env["conn"].execute(
        """
        SELECT operation.approval_fingerprint_json,
               approval.approval_id, approval.approval_class,
               approval.expires_at, approval.authorization_actor_id
        FROM project_operations AS operation
        JOIN project_approvals AS approval
          ON approval.project_id = operation.project_id
         AND approval.approval_id = operation.approval_id
        WHERE operation.operation_id = 'operation-1'
        """
    ).fetchone()
    assert row["approval_fingerprint_json"] == (
        '{"approval_class":"publish","approval_id":"approval-1",'
        '"authorization_actor_id":"owner-1","expires_at":1000,'
        '"requires_owner":true}'
    )

    approved = operation_env["guard"].resolve_operation_approval(
        "approval-1",
        operation_env["discord_actor"],
        outcome="approved",
    )
    assert approved.status == "approved"


def test_revision_one_null_capability_is_not_backfilled_and_fails_closed(
    operation_env,
):
    _prepare_allowed(operation_env)
    operation = prdb._project_operation_for_id(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        operation_id="operation-1",
    )
    assert operation is not None
    prdb._decertify_project_operation(
        operation_env["conn"], operation
    )
    operation_env["conn"].execute(
        """
        UPDATE project_operations
        SET remote_idempotency_supported = NULL
        WHERE operation_id = 'operation-1'
        """
    )
    operation_env["conn"].commit()

    prdb.ensure_schema(operation_env["conn"])

    assert operation_env["conn"].execute(
        """
        SELECT remote_idempotency_supported
        FROM project_operations
        WHERE operation_id = 'operation-1'
        """
    ).fetchone()[0] is None
    with pytest.raises(
        RuntimeError, match="malformed persisted project operation"
    ):
        prdb._project_operation_for_id(
            operation_env["conn"],
            project_id=operation_env["project_id"],
            operation_id="operation-1",
        )
    assert prdb._project_operation_disposition_for_turn(
        operation_env["conn"],
        project_id=operation_env["project_id"],
        turn_id=operation_env["turn"].turn_id,
    ) == "post_effect_blocked"


@pytest.mark.parametrize(
    "statement",
    (
        "SET remote_idempotency_supported = NULL",
        "SET remote_idempotency_supported = 2",
        "SET remote_idempotency_supported = 0",
        "SET approval_fingerprint_json = '{}'",
    ),
)
def test_operation_trigger_rejects_capability_fingerprint_cross_breaks(
    operation_env, statement
):
    _prepare_allowed(operation_env)

    with pytest.raises(sqlite3.IntegrityError):
        operation_env["conn"].execute(
            f"""
            UPDATE project_operations
            {statement}
            WHERE operation_id = 'operation-1'
            """
        )
    operation_env["conn"].rollback()
