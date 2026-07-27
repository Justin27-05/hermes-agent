"""Contract tests for the durable ProjectOperationGuard."""

from __future__ import annotations

import importlib
import inspect
import json
import sqlite3
import subprocess
import sys
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
}

TASK6_INDEXES = {
    "idx_project_operations_one_approval",
    "idx_project_approvals_one_operation",
    "idx_project_operations_receipt",
    "idx_project_operations_turn_status",
    "idx_project_operations_recovery",
    "idx_project_operations_approved_rehydrate",
}

OPERATION_PROBE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "project_operation_probe.py"
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
                lease_generation, fencing_token
            ) VALUES (
                'invalid-managed', ?, 'turn', 'remote-key', 1, '[]', '{}',
                'invented', 1, 1, 1, 'local_code_edit', '["item"]',
                'ledger', 'attempt', 1, 1
            )
            """,
            (project_id,),
        )
    operation_conn.rollback()


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
            lease_generation, fencing_token
        ) VALUES (
            'malformed-managed', ?, 'turn', 'remote-key', 1,
            '[ "C:/work" ]', '{}', 'approved', 1, 1, 1,
            'local_code_edit', '["item"]', 'ledger', 'attempt', 1, 1
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
                approval_id, project_id, turn_id, operation_id, actor_id,
                authorization_actor_id, canonical_action, approval_class,
                command_revision, expected_runtime_version,
                effective_runtime_version, turn_expected_control_version,
                expected_lifecycle, expected_phase, targets_json,
                batch_boundary_json, status, expires_at, resolved_at,
                resolved_by_actor_id, consumed_at, created_at
            ) VALUES (
                ?, ?, NULL, 'legacy-operation', 'owner', 'owner', 'publish',
                'publish', ?, 0, 0, NULL, 'active', 'implementation',
                ?, ?, 'pending', 100, NULL, NULL, NULL, 1
            )
            """,
            (
                f"approval-{ordinal}",
                project_id,
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
        "canonical_action": "local_code_edit",
        "batch_items_json": '["item"]',
        "readback_kind": "ledger",
        "attempt_id": "attempt",
        "lease_generation": 1,
        "fencing_token": 1,
        "receipt_id": None,
        "readback_json": None,
        "blocked_reason": None,
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


def _prepare_critical(operation_env, *, expires_at=1000):
    module = operation_env["module"]
    intent = _intent(operation_env, canonical_action="publish")
    spec = module.OperationApprovalSpec(
        "approval-1",
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


def _start_operation_probe(
    operation_env,
    *,
    mode,
    now,
    outcome="approved",
    binding_id="desktop-owner",
):
    process = subprocess.Popen(
        [
            sys.executable,
            str(OPERATION_PROBE),
            str(operation_env["conn"].execute(
                "PRAGMA database_list"
            ).fetchone()["file"]),
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
    assert process.stdout is not None
    ready = json.loads(process.stdout.readline())
    assert ready["phase"] == "ready"
    return process


def _release_operation_probe(process):
    assert process.stdin is not None
    process.stdin.write('{"command":"go"}\n')
    process.stdin.flush()


def _finish_operation_probe(process):
    try:
        stdout, stderr = process.communicate(timeout=15)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert process.returncode == 0, stderr
    lines = [line for line in stdout.splitlines() if line]
    assert len(lines) == 1
    return json.loads(lines[0])


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
    before_crash.wait(timeout=15)
    assert before_crash.returncode == 71
    assert before == _operation_snapshot(operation_env)

    after_crash = _start_operation_probe(
        operation_env, mode="crash_after", now=101
    )
    _release_operation_probe(after_crash)
    after_crash.wait(timeout=15)
    assert after_crash.returncode == 72
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
    operation_env["conn"].execute(
        """
        UPDATE project_operations
        SET status = 'unknown', updated_at = 100
        WHERE operation_id = 'operation-1'
        """
    )
    operation_env["conn"].commit()
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
                conn.execute(
                    """
                    UPDATE project_operations SET status = 'unknown'
                    WHERE operation_id = 'operation-1'
                    """
                )
                conn.commit()
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
            conn.execute(
                """
                DROP TRIGGER trg_project_operations_task6_update
                """
            )
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
        stdout, stderr = process.communicate(timeout=15)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert process.returncode == expected_code, (stdout, stderr)


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
