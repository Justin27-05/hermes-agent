"""Durable SQLite schema primitives for per-project runtime state.

This module owns persistence structure only. Runtime policy, queueing,
delivery, worker, and provider behavior belong to later service layers.
"""

from __future__ import annotations

import contextlib
import json
import math
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Iterator, Literal, Optional

from hermes_cli.project_policy import (
    ActorContext,
    approval_class_for_action,
    canonicalize_targets,
)
from hermes_cli.project_lineage import (
    ProjectConversation,
    SurfaceBinding,
    make_child_conversation,
    make_root_conversation,
    make_surface_binding,
)
from hermes_cli.sqlite_util import add_column_if_missing, write_txn


_SQLITE_INT_MAX = (1 << 63) - 1

TASK6_CRITICAL_ACTION_APPROVAL_CLASSES = (
    ("credentials", "credentials"),
    ("money_quota", "money_quota"),
    ("money", "money_quota"),
    ("quota", "money_quota"),
    ("external_communication", "external_communication"),
    ("publish", "publish"),
    ("push", "publish"),
    ("pull_request", "publish"),
    ("release", "publish"),
    ("production", "production"),
    ("admin_service", "admin_service"),
    ("admin", "admin_service"),
    ("service", "admin_service"),
    ("startup", "admin_service"),
    ("destructive", "destructive"),
    ("live_canary", "live_canary"),
    ("final_acceptance", "final_acceptance"),
)


def _task6_sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


_TASK6_CRITICAL_ACTION_VALUES_SQL = ",\n".join(
    "                ({}, {})".format(
        _task6_sql_text(action),
        _task6_sql_text(approval_class),
    )
    for action, approval_class
    in TASK6_CRITICAL_ACTION_APPROVAL_CLASSES
)
_TASK6_CRITICAL_APPROVAL_CLASS_BY_ACTION = dict(
    TASK6_CRITICAL_ACTION_APPROVAL_CLASSES
)


RUNTIME_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS project_contracts (
    contract_id    TEXT PRIMARY KEY,
    project_id     TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    revision       INTEGER NOT NULL,
    contract_json  TEXT NOT NULL,
    status         TEXT NOT NULL,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    UNIQUE (project_id, contract_id),
    UNIQUE (project_id, revision)
);

CREATE TABLE IF NOT EXISTS project_runtime_state (
    project_id              TEXT PRIMARY KEY
                            REFERENCES projects(id) ON DELETE RESTRICT,
    lifecycle               TEXT NOT NULL
                            CHECK (
                                lifecycle IN (
                                    'active',
                                    'awaiting_acceptance',
                                    'completed'
                                )
                            ),
    current_phase           TEXT NOT NULL
                            CHECK (length(current_phase) > 0),
    version                 INTEGER NOT NULL,
    conversation_root_id    TEXT,
    conversation_tip_id     TEXT,
    updated_at              INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS project_conversations (
    conversation_id         TEXT PRIMARY KEY,
    project_id              TEXT NOT NULL
                            REFERENCES projects(id) ON DELETE RESTRICT,
    parent_conversation_id  TEXT,
    root_conversation_id    TEXT,
    created_at              INTEGER NOT NULL,
    UNIQUE (project_id, conversation_id),
    FOREIGN KEY (project_id, parent_conversation_id)
        REFERENCES project_conversations(project_id, conversation_id),
    FOREIGN KEY (project_id, root_conversation_id)
        REFERENCES project_conversations(project_id, conversation_id)
);

CREATE TABLE IF NOT EXISTS project_surface_bindings (
    binding_id           TEXT PRIMARY KEY,
    project_id           TEXT NOT NULL
                         REFERENCES projects(id) ON DELETE RESTRICT,
    surface              TEXT NOT NULL,
    external_binding_id  TEXT NOT NULL,
    actor_id             TEXT,
    created_at           INTEGER NOT NULL,
    UNIQUE (project_id, binding_id),
    UNIQUE (surface, external_binding_id)
);

CREATE TABLE IF NOT EXISTS project_turns (
    turn_id           TEXT PRIMARY KEY,
    project_id        TEXT NOT NULL
                      REFERENCES projects(id) ON DELETE RESTRICT,
    sequence          INTEGER NOT NULL,
    idempotency_key   TEXT NOT NULL,
    payload_json      TEXT NOT NULL,
    origin_binding_id TEXT,
    status            TEXT NOT NULL,
    attempt_id        TEXT,
    lease_generation  INTEGER NOT NULL DEFAULT 0,
    fencing_token     INTEGER NOT NULL DEFAULT 0,
    execution_state   TEXT
                      CHECK (
                          execution_state IS NULL
                          OR execution_state IN ('not_started', 'started')
                      ),
    terminal_result_id TEXT
                      CHECK (
                          terminal_result_id IS NULL
                          OR length(terminal_result_id) > 0
                      ),
    recovery_block_key TEXT
                      REFERENCES project_events(event_id)
                      CHECK (
                          recovery_block_key IS NULL
                          OR length(recovery_block_key) > 0
                      ),
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    UNIQUE (project_id, turn_id),
    UNIQUE (project_id, sequence),
    UNIQUE (project_id, idempotency_key),
    FOREIGN KEY (project_id, origin_binding_id)
        REFERENCES project_surface_bindings(project_id, binding_id)
);

CREATE TABLE IF NOT EXISTS project_run_controls (
    turn_id          TEXT PRIMARY KEY,
    project_id       TEXT NOT NULL
                     REFERENCES projects(id) ON DELETE RESTRICT,
    control_state    TEXT NOT NULL,
    control_version  INTEGER NOT NULL,
    idempotency_key  TEXT,
    command_fingerprint TEXT,
    attempt_id       TEXT,
    claim_worker_id  TEXT,
    claim_lease_expires_at INTEGER,
    claim_canonical_session_id TEXT,
    updated_at       INTEGER NOT NULL,
    UNIQUE (project_id, turn_id),
    UNIQUE (project_id, idempotency_key),
    FOREIGN KEY (project_id, turn_id)
        REFERENCES project_turns(project_id, turn_id)
);

CREATE TABLE IF NOT EXISTS project_events (
    event_id      TEXT PRIMARY KEY,
    project_id    TEXT NOT NULL
                  REFERENCES projects(id) ON DELETE RESTRICT,
    sequence      INTEGER NOT NULL,
    kind          TEXT NOT NULL,
    turn_id       TEXT,
    payload_json  TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    UNIQUE (project_id, event_id),
    UNIQUE (project_id, sequence),
    FOREIGN KEY (project_id, turn_id)
        REFERENCES project_turns(project_id, turn_id)
);

CREATE TABLE IF NOT EXISTS project_deliveries (
    delivery_id   TEXT PRIMARY KEY,
    project_id    TEXT NOT NULL
                  REFERENCES projects(id) ON DELETE RESTRICT,
    binding_id    TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    status        TEXT NOT NULL,
    cursor        INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    updated_at    INTEGER NOT NULL,
    UNIQUE (project_id, delivery_id),
    UNIQUE (binding_id, event_id),
    FOREIGN KEY (project_id, binding_id)
        REFERENCES project_surface_bindings(project_id, binding_id),
    FOREIGN KEY (project_id, event_id)
        REFERENCES project_events(project_id, event_id)
);

CREATE TABLE IF NOT EXISTS project_approvals (
    approval_id         TEXT PRIMARY KEY,
    project_id          TEXT NOT NULL
                        REFERENCES projects(id) ON DELETE RESTRICT,
    turn_id             TEXT,
    operation_id        TEXT,
    operation_maintenance_seq INTEGER
                        CHECK (
                            operation_maintenance_seq IS NULL
                            OR (
                                typeof(operation_maintenance_seq)
                                    = 'integer'
                                AND operation_maintenance_seq > 0
                            )
                        ),
    actor_id            TEXT NOT NULL,
    authorization_actor_id TEXT NOT NULL,
    canonical_action    TEXT NOT NULL,
    approval_class      TEXT NOT NULL,
    command_revision    INTEGER NOT NULL,
    expected_runtime_version INTEGER NOT NULL
                        CHECK (
                            typeof(expected_runtime_version) = 'integer'
                            AND expected_runtime_version >= 0
                        ),
    effective_runtime_version INTEGER NOT NULL
                        CHECK (
                            typeof(effective_runtime_version) = 'integer'
                            AND effective_runtime_version >= 0
                        ),
    turn_expected_control_version INTEGER
                        CHECK (
                            turn_expected_control_version IS NULL
                            OR (
                                typeof(turn_expected_control_version)
                                    = 'integer'
                                AND turn_expected_control_version >= 0
                            )
                        ),
    expected_lifecycle  TEXT NOT NULL
                        CHECK (
                            expected_lifecycle IN (
                                'active',
                                'awaiting_acceptance',
                                'completed'
                            )
                        ),
    expected_phase      TEXT NOT NULL
                        CHECK (length(expected_phase) > 0),
    targets_json        TEXT NOT NULL,
    batch_boundary_json TEXT NOT NULL,
    status              TEXT NOT NULL
                        CHECK (
                            status IN (
                                'pending', 'approved', 'denied', 'expired'
                            )
                        ),
    expires_at          INTEGER NOT NULL,
    resolved_at         INTEGER,
    resolved_by_actor_id TEXT,
    consumed_at         INTEGER,
    created_at          INTEGER NOT NULL,
    CHECK (
        (
            status = 'pending'
            AND resolved_at IS NULL
            AND resolved_by_actor_id IS NULL
            AND consumed_at IS NULL
        )
        OR (
            status = 'approved'
            AND resolved_at IS NOT NULL
            AND resolved_by_actor_id IS NOT NULL
        )
        OR (
            status = 'denied'
            AND resolved_at IS NOT NULL
            AND resolved_by_actor_id IS NOT NULL
            AND consumed_at IS NULL
        )
        OR (
            status = 'expired'
            AND consumed_at IS NULL
            AND (
                (
                    resolved_at IS NULL
                    AND resolved_by_actor_id IS NULL
                )
                OR (
                    resolved_at IS NOT NULL
                    AND resolved_by_actor_id IS NOT NULL
                )
            )
        )
    ),
    UNIQUE (project_id, approval_id),
    UNIQUE (
        project_id,
        command_revision,
        approval_class,
        targets_json,
        batch_boundary_json
    ),
    FOREIGN KEY (project_id, turn_id)
        REFERENCES project_turns(project_id, turn_id),
    FOREIGN KEY (project_id, operation_id)
        REFERENCES project_operations(project_id, operation_id)
);

CREATE TABLE IF NOT EXISTS project_artifacts (
    artifact_id   TEXT PRIMARY KEY,
    project_id    TEXT NOT NULL
                  REFERENCES projects(id) ON DELETE RESTRICT,
    turn_id       TEXT,
    path          TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    status        TEXT NOT NULL,
    verified_at   INTEGER,
    created_at    INTEGER NOT NULL,
    UNIQUE (project_id, artifact_id),
    UNIQUE (project_id, path),
    FOREIGN KEY (project_id, turn_id)
        REFERENCES project_turns(project_id, turn_id)
);

CREATE TABLE IF NOT EXISTS project_operations (
    operation_id     TEXT PRIMARY KEY,
    project_id       TEXT NOT NULL
                     REFERENCES projects(id) ON DELETE RESTRICT,
    turn_id          TEXT,
    idempotency_key  TEXT,
    approval_id      TEXT,
    command_revision INTEGER NOT NULL,
    targets_json     TEXT NOT NULL,
    payload_json     TEXT NOT NULL,
    status           TEXT NOT NULL
                     CHECK (
                         guard_revision = 0
                         OR status IN (
                             'awaiting_approval',
                             'approved',
                             'effect_started',
                             'receipt_recorded',
                             'unknown',
                             'reconciled',
                             'blocked'
                         )
                     ),
    receipt_json     TEXT,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    guard_revision   INTEGER NOT NULL DEFAULT 0
                     CHECK (
                         typeof(guard_revision) = 'integer'
                         AND guard_revision IN (0, 1)
                     ),
    guard_validated  INTEGER NOT NULL DEFAULT 0
                     CHECK (
                         typeof(guard_validated) = 'integer'
                         AND guard_validated IN (0, 1)
                     ),
    canonical_action TEXT,
    batch_items_json TEXT,
    readback_kind    TEXT,
    attempt_id       TEXT,
    lease_generation INTEGER,
    fencing_token    INTEGER,
    receipt_id       TEXT,
    readback_json    TEXT,
    blocked_reason   TEXT,
    remote_idempotency_supported INTEGER,
    approval_fingerprint_json TEXT,
    UNIQUE (project_id, operation_id),
    UNIQUE (project_id, idempotency_key),
    FOREIGN KEY (project_id, turn_id)
        REFERENCES project_turns(project_id, turn_id),
    FOREIGN KEY (project_id, approval_id)
        REFERENCES project_approvals(project_id, approval_id)
);

CREATE TABLE IF NOT EXISTS project_worker_leases (
    lease_id          TEXT PRIMARY KEY,
    project_id        TEXT NOT NULL
                      REFERENCES projects(id) ON DELETE RESTRICT,
    turn_id           TEXT,
    worker_id         TEXT NOT NULL,
    lease_generation  INTEGER NOT NULL,
    fencing_token     INTEGER NOT NULL,
    expires_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    UNIQUE (project_id, lease_id),
    UNIQUE (project_id, turn_id),
    FOREIGN KEY (project_id, turn_id)
        REFERENCES project_turns(project_id, turn_id)
);

CREATE TABLE IF NOT EXISTS project_operation_maintenance (
    singleton           INTEGER PRIMARY KEY
                        CHECK (singleton = 1),
    approval_scan_after TEXT NOT NULL DEFAULT '',
    next_lane           INTEGER NOT NULL DEFAULT 0
                        CHECK (next_lane IN (0, 1)),
    approval_scan_after_seq INTEGER NOT NULL DEFAULT 0
                        CHECK (
                            typeof(approval_scan_after_seq) = 'integer'
                            AND approval_scan_after_seq >= 0
                        ),
    approval_scan_high_water_seq INTEGER NOT NULL DEFAULT 0
                        CHECK (
                            typeof(approval_scan_high_water_seq) = 'integer'
                            AND approval_scan_high_water_seq >= 0
                        ),
    approval_scan_epoch INTEGER NOT NULL DEFAULT 0
                        CHECK (
                            typeof(approval_scan_epoch) = 'integer'
                            AND approval_scan_epoch >= 0
                        ),
    next_operation_approval_seq INTEGER NOT NULL DEFAULT 1
                        CHECK (
                            typeof(next_operation_approval_seq) = 'integer'
                            AND next_operation_approval_seq > 0
                        ),
    operation_validation_migration_complete
                        INTEGER NOT NULL DEFAULT 0
                        CHECK (
                            typeof(
                                operation_validation_migration_complete
                            ) = 'integer'
                            AND operation_validation_migration_complete
                                IN (0, 1)
                        )
);
"""

LINEAGE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_conversations_one_root
ON project_conversations(project_id)
WHERE parent_conversation_id IS NULL;
"""

TASK4_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_project_turns_claim_fifo
ON project_turns(project_id, status, sequence);

CREATE INDEX IF NOT EXISTS idx_project_run_controls_project_turn
ON project_run_controls(project_id, turn_id);
"""

TASK5_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_turns_terminal_result
ON project_turns(project_id, terminal_result_id)
WHERE terminal_result_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_project_worker_leases_expiry
ON project_worker_leases(expires_at, project_id, turn_id);

CREATE INDEX IF NOT EXISTS idx_project_turns_project_sequence
ON project_turns(project_id, sequence, turn_id);

CREATE INDEX IF NOT EXISTS idx_project_turns_actionable_recovery
ON project_turns(project_id, sequence, turn_id)
WHERE status = 'reconciling' AND recovery_block_key IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_project_events_recovery_block_attempt
ON project_events(
    project_id,
    turn_id,
    json_array(
        json_extract(payload_json, '$.attempt_id'),
        json_extract(payload_json, '$.lease_generation'),
        json_extract(payload_json, '$.fencing_token')
    )
)
WHERE kind = 'turn.recovery_blocked';
"""

_OPERATION_UNSAFE_CLASS_SQL = """CASE WHEN
     typeof(guard_validated) IS NOT 'integer'
  OR guard_validated IS NOT 1
  OR typeof(guard_revision) IS NOT 'integer'
  OR guard_revision IS NOT 1
  OR typeof(status) IS NOT 'text'
  OR status NOT IN (
       'approved','effect_started','receipt_recorded','unknown',
       'reconciled','blocked'
     )
  OR (
       status = 'blocked'
       AND (
         typeof(blocked_reason) IS NOT 'text'
         OR blocked_reason NOT IN (
           'operation_capability_unsupported','approval_denied',
           'approval_time_expired','approval_stale_boundary'
         )
       )
     )
THEN 1 ELSE 0 END"""

_OPERATION_UNSAFE_INDEX_SQL = f"""
CREATE INDEX idx_project_operations_turn_unsafe
ON project_operations(
  project_id,
  turn_id,
  (
{_OPERATION_UNSAFE_CLASS_SQL}
  ),
  operation_id
);
"""

TASK6_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_operations_one_approval
ON project_operations(project_id, approval_id)
WHERE approval_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_project_approvals_one_operation
ON project_approvals(project_id, operation_id)
WHERE operation_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_project_operations_receipt
ON project_operations(project_id, receipt_id)
WHERE guard_revision = 1
  AND guard_validated = 1
  AND receipt_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_project_operations_turn_status
ON project_operations(project_id, turn_id, status, operation_id)
WHERE guard_revision = 1 AND guard_validated = 1;

CREATE INDEX IF NOT EXISTS idx_project_operations_recovery
ON project_operations(
    status, updated_at, project_id, operation_id, turn_id
)
WHERE guard_revision = 1
  AND guard_validated = 1
  AND status IN (
      'approved', 'effect_started', 'receipt_recorded', 'unknown'
  );

CREATE INDEX IF NOT EXISTS idx_project_operations_approved_rehydrate
ON project_operations(project_id, turn_id, operation_id)
WHERE guard_revision = 1
  AND guard_validated = 1
  AND status = 'approved';

CREATE INDEX IF NOT EXISTS idx_project_operations_turn_unresolved
ON project_operations(project_id, turn_id, status, operation_id)
WHERE guard_revision = 1
  AND guard_validated = 1
  AND status IN (
      'approved', 'effect_started', 'receipt_recorded', 'unknown'
  );

CREATE INDEX IF NOT EXISTS idx_project_approvals_operation_due
ON project_approvals(expires_at, project_id, approval_id)
WHERE status = 'pending' AND operation_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS
idx_project_approvals_operation_maintenance_seq
ON project_approvals(operation_maintenance_seq)
WHERE operation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_project_approvals_operation_stale_epoch
ON project_approvals(operation_maintenance_seq)
WHERE status = 'pending' AND operation_id IS NOT NULL;
"""

_TASK6_CRITICAL_AUTHORITY_PREDICATE_SQL = """
COALESCE(
    (
        SELECT
            CASE
            WHEN (
                typeof(NEW.approval_fingerprint_json) = 'text'
                AND json_valid(
                    NEW.approval_fingerprint_json
                ) = 1
            )
            THEN (
            NEW.approval_fingerprint_json = json_object(
                'approval_class',
                json_extract(
                    NEW.approval_fingerprint_json,
                    '$.approval_class'
                ),
                'approval_id',
                json_extract(
                    NEW.approval_fingerprint_json,
                    '$.approval_id'
                ),
                'authorization_actor_id',
                json_extract(
                    NEW.approval_fingerprint_json,
                    '$.authorization_actor_id'
                ),
                'expires_at',
                json_extract(
                    NEW.approval_fingerprint_json,
                    '$.expires_at'
                ),
                'requires_owner',
                json('true')
            )
            AND json_extract(
                NEW.approval_fingerprint_json,
                '$.approval_class'
            ) = critical.column2
            AND typeof(json_extract(
                NEW.approval_fingerprint_json,
                '$.approval_id'
            )) = 'text'
            AND length(json_extract(
                NEW.approval_fingerprint_json,
                '$.approval_id'
            )) > 0
            AND typeof(json_extract(
                NEW.approval_fingerprint_json,
                '$.authorization_actor_id'
            )) = 'text'
            AND length(json_extract(
                NEW.approval_fingerprint_json,
                '$.authorization_actor_id'
            )) > 0
            AND json_type(
                NEW.approval_fingerprint_json,
                '$.expires_at'
            ) = 'integer'
            AND json_extract(
                NEW.approval_fingerprint_json,
                '$.expires_at'
            ) >= 0
            AND json_type(
                NEW.approval_fingerprint_json,
                '$.requires_owner'
            ) = 'true'
            AND (
                (
                    NEW.status = 'blocked'
                    AND NEW.blocked_reason
                        = 'operation_capability_unsupported'
                    AND (
                        NEW.remote_idempotency_supported = 0
                        OR NEW.readback_kind IS NULL
                    )
                    AND NEW.approval_id IS NULL
                )
                OR (
                    NOT (
                        NEW.status = 'blocked'
                        AND NEW.blocked_reason
                            = 'operation_capability_unsupported'
                    )
                    AND typeof(NEW.approval_id) = 'text'
                    AND length(NEW.approval_id) > 0
                    AND NEW.approval_id = json_extract(
                        NEW.approval_fingerprint_json,
                        '$.approval_id'
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM project_approvals AS inverse_approval
                        WHERE inverse_approval.project_id
                            = NEW.project_id
                          AND inverse_approval.approval_id
                            = NEW.approval_id
                          AND inverse_approval.operation_id
                            = NEW.operation_id
                    )
                )
            )
            )
            ELSE 0
            END
        FROM (
            VALUES
__TASK6_CRITICAL_ACTION_VALUES__
        ) AS critical
        WHERE critical.column1 = NEW.canonical_action
    ),
    (
        NEW.approval_fingerprint_json IS NULL
        AND NEW.approval_id IS NULL
    )
)
""".replace(
    "__TASK6_CRITICAL_ACTION_VALUES__",
    _TASK6_CRITICAL_ACTION_VALUES_SQL,
)

TASK6_TRIGGER_SQL = """
CREATE TRIGGER IF NOT EXISTS trg_project_operations_task6_insert
BEFORE INSERT ON project_operations
WHEN NEW.guard_revision = 1
 AND NEW.guard_validated = 1
 AND NOT (
    typeof(NEW.operation_id) = 'text' AND length(NEW.operation_id) > 0
    AND typeof(NEW.project_id) = 'text' AND length(NEW.project_id) > 0
    AND typeof(NEW.turn_id) = 'text' AND length(NEW.turn_id) > 0
    AND typeof(NEW.idempotency_key) = 'text'
        AND length(NEW.idempotency_key) > 0
    AND typeof(NEW.command_revision) = 'integer'
        AND NEW.command_revision > 0
    AND typeof(NEW.targets_json) = 'text' AND length(NEW.targets_json) > 0
    AND typeof(NEW.payload_json) = 'text' AND length(NEW.payload_json) > 0
    AND typeof(NEW.canonical_action) = 'text'
        AND length(NEW.canonical_action) > 0
    AND typeof(NEW.batch_items_json) = 'text'
        AND length(NEW.batch_items_json) > 0
    AND typeof(NEW.attempt_id) = 'text' AND length(NEW.attempt_id) > 0
    AND typeof(NEW.lease_generation) = 'integer'
        AND NEW.lease_generation > 0
    AND typeof(NEW.fencing_token) = 'integer'
        AND NEW.fencing_token > 0
    AND typeof(NEW.remote_idempotency_supported) = 'integer'
        AND NEW.remote_idempotency_supported IN (0, 1)
    AND typeof(NEW.created_at) = 'integer' AND NEW.created_at >= 0
    AND typeof(NEW.updated_at) = 'integer' AND NEW.updated_at >= 0
    AND NEW.status IN (
        'awaiting_approval', 'approved', 'effect_started',
        'receipt_recorded', 'unknown', 'reconciled', 'blocked'
    )
    AND (
        NEW.status != 'awaiting_approval'
        OR (
            typeof(NEW.approval_id) = 'text'
            AND length(NEW.approval_id) > 0
        )
    )
    AND (
        NEW.status = 'blocked'
        OR (
            typeof(NEW.readback_kind) = 'text'
            AND length(NEW.readback_kind) > 0
        )
    )
    AND (
        (
            (
                NEW.remote_idempotency_supported = 0
                OR NEW.readback_kind IS NULL
            )
            AND NEW.status = 'blocked'
            AND NEW.blocked_reason
                = 'operation_capability_unsupported'
            AND NEW.approval_id IS NULL
        )
        OR (
            NEW.remote_idempotency_supported = 1
            AND typeof(NEW.readback_kind) = 'text'
            AND length(NEW.readback_kind) > 0
            AND NOT (
                NEW.status = 'blocked'
                AND NEW.blocked_reason
                    = 'operation_capability_unsupported'
            )
        )
    )
    AND (__TASK6_CRITICAL_AUTHORITY__)
    AND (
        (
            (
                NEW.status IN (
                    'receipt_recorded', 'unknown', 'reconciled'
                )
                OR (
                    NEW.status = 'blocked'
                    AND NEW.blocked_reason
                        = 'operation_readback_ambiguous'
                )
            )
            AND typeof(NEW.receipt_id) = 'text'
            AND length(NEW.receipt_id) > 0
            AND typeof(NEW.receipt_json) = 'text'
            AND length(NEW.receipt_json) > 0
        )
        OR (
            NEW.status NOT IN ('receipt_recorded', 'reconciled')
            AND NEW.receipt_id IS NULL
            AND NEW.receipt_json IS NULL
        )
    )
    AND (
        (
            NEW.status = 'reconciled'
            AND typeof(NEW.readback_json) = 'text'
            AND length(NEW.readback_json) > 0
        )
        OR (
            NEW.status = 'approved'
            AND (
                NEW.readback_json IS NULL
                OR (
                    typeof(NEW.readback_json) = 'text'
                    AND length(NEW.readback_json) > 0
                )
            )
        )
        OR (
            NEW.status NOT IN ('approved', 'reconciled')
            AND NEW.readback_json IS NULL
        )
    )
    AND (
        (
            NEW.status = 'blocked'
            AND typeof(NEW.blocked_reason) = 'text'
            AND length(NEW.blocked_reason) > 0
        )
        OR (
            NEW.status != 'blocked'
            AND NEW.blocked_reason IS NULL
        )
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'invalid managed project operation');
END;

CREATE TRIGGER IF NOT EXISTS trg_project_operations_task6_update
BEFORE UPDATE ON project_operations
WHEN NEW.guard_revision = 1
 AND NEW.guard_validated = 1
 AND NOT (
    typeof(NEW.operation_id) = 'text' AND length(NEW.operation_id) > 0
    AND typeof(NEW.project_id) = 'text' AND length(NEW.project_id) > 0
    AND typeof(NEW.turn_id) = 'text' AND length(NEW.turn_id) > 0
    AND typeof(NEW.idempotency_key) = 'text'
        AND length(NEW.idempotency_key) > 0
    AND typeof(NEW.command_revision) = 'integer'
        AND NEW.command_revision > 0
    AND typeof(NEW.targets_json) = 'text' AND length(NEW.targets_json) > 0
    AND typeof(NEW.payload_json) = 'text' AND length(NEW.payload_json) > 0
    AND typeof(NEW.canonical_action) = 'text'
        AND length(NEW.canonical_action) > 0
    AND typeof(NEW.batch_items_json) = 'text'
        AND length(NEW.batch_items_json) > 0
    AND typeof(NEW.attempt_id) = 'text' AND length(NEW.attempt_id) > 0
    AND typeof(NEW.lease_generation) = 'integer'
        AND NEW.lease_generation > 0
    AND typeof(NEW.fencing_token) = 'integer'
        AND NEW.fencing_token > 0
    AND typeof(NEW.remote_idempotency_supported) = 'integer'
        AND NEW.remote_idempotency_supported IN (0, 1)
    AND typeof(NEW.created_at) = 'integer' AND NEW.created_at >= 0
    AND typeof(NEW.updated_at) = 'integer' AND NEW.updated_at >= 0
    AND NEW.status IN (
        'awaiting_approval', 'approved', 'effect_started',
        'receipt_recorded', 'unknown', 'reconciled', 'blocked'
    )
    AND (
        NEW.status != 'awaiting_approval'
        OR (
            typeof(NEW.approval_id) = 'text'
            AND length(NEW.approval_id) > 0
        )
    )
    AND (
        NEW.status = 'blocked'
        OR (
            typeof(NEW.readback_kind) = 'text'
            AND length(NEW.readback_kind) > 0
        )
    )
    AND (
        (
            (
                NEW.remote_idempotency_supported = 0
                OR NEW.readback_kind IS NULL
            )
            AND NEW.status = 'blocked'
            AND NEW.blocked_reason
                = 'operation_capability_unsupported'
            AND NEW.approval_id IS NULL
        )
        OR (
            NEW.remote_idempotency_supported = 1
            AND typeof(NEW.readback_kind) = 'text'
            AND length(NEW.readback_kind) > 0
            AND NOT (
                NEW.status = 'blocked'
                AND NEW.blocked_reason
                    = 'operation_capability_unsupported'
            )
        )
    )
    AND (__TASK6_CRITICAL_AUTHORITY__)
    AND (
        (
            (
                NEW.status IN (
                    'receipt_recorded', 'unknown', 'reconciled'
                )
                OR (
                    NEW.status = 'blocked'
                    AND NEW.blocked_reason
                        = 'operation_readback_ambiguous'
                )
            )
            AND typeof(NEW.receipt_id) = 'text'
            AND length(NEW.receipt_id) > 0
            AND typeof(NEW.receipt_json) = 'text'
            AND length(NEW.receipt_json) > 0
        )
        OR (
            NEW.status NOT IN ('receipt_recorded', 'reconciled')
            AND NEW.receipt_id IS NULL
            AND NEW.receipt_json IS NULL
        )
    )
    AND (
        (
            NEW.status = 'reconciled'
            AND typeof(NEW.readback_json) = 'text'
            AND length(NEW.readback_json) > 0
        )
        OR (
            NEW.status = 'approved'
            AND (
                NEW.readback_json IS NULL
                OR (
                    typeof(NEW.readback_json) = 'text'
                    AND length(NEW.readback_json) > 0
                )
            )
        )
        OR (
            NEW.status NOT IN ('approved', 'reconciled')
            AND NEW.readback_json IS NULL
        )
    )
    AND (
        (
            NEW.status = 'blocked'
            AND typeof(NEW.blocked_reason) = 'text'
            AND length(NEW.blocked_reason) > 0
        )
        OR (
            NEW.status != 'blocked'
            AND NEW.blocked_reason IS NULL
        )
    )
 )
BEGIN
    SELECT RAISE(ABORT, 'invalid managed project operation');
END;

CREATE TRIGGER IF NOT EXISTS trg_project_operations_guard_update
BEFORE UPDATE ON project_operations
WHEN (
       OLD.guard_validated = 1
       OR (
            OLD.guard_validated = 0
            AND NEW.guard_validated = 1
       )
     )
 AND (
       NEW.operation_id IS NOT OLD.operation_id
    OR NEW.project_id IS NOT OLD.project_id
    OR NEW.turn_id IS NOT OLD.turn_id
    OR NEW.idempotency_key IS NOT OLD.idempotency_key
    OR NEW.approval_id IS NOT OLD.approval_id
    OR NEW.command_revision IS NOT OLD.command_revision
    OR NEW.targets_json IS NOT OLD.targets_json
    OR NEW.payload_json IS NOT OLD.payload_json
    OR NEW.status IS NOT OLD.status
    OR NEW.receipt_json IS NOT OLD.receipt_json
    OR NEW.created_at IS NOT OLD.created_at
    OR NEW.updated_at IS NOT OLD.updated_at
    OR NEW.guard_revision IS NOT OLD.guard_revision
    OR NEW.canonical_action IS NOT OLD.canonical_action
    OR NEW.batch_items_json IS NOT OLD.batch_items_json
    OR NEW.readback_kind IS NOT OLD.readback_kind
    OR NEW.attempt_id IS NOT OLD.attempt_id
    OR NEW.lease_generation IS NOT OLD.lease_generation
    OR NEW.fencing_token IS NOT OLD.fencing_token
    OR NEW.receipt_id IS NOT OLD.receipt_id
    OR NEW.readback_json IS NOT OLD.readback_json
    OR NEW.blocked_reason IS NOT OLD.blocked_reason
    OR NEW.remote_idempotency_supported
       IS NOT OLD.remote_idempotency_supported
    OR NEW.approval_fingerprint_json
       IS NOT OLD.approval_fingerprint_json
 )
BEGIN
    SELECT RAISE(
        ABORT,
        'certified project operation requires marker-only transition'
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_project_approvals_task6_insert
BEFORE INSERT ON project_approvals
WHEN NOT (
       (
           NEW.operation_id IS NULL
           AND NEW.operation_maintenance_seq IS NULL
       )
    OR (
           typeof(NEW.operation_id) = 'text'
           AND length(NEW.operation_id) > 0
           AND typeof(NEW.operation_maintenance_seq) = 'integer'
           AND NEW.operation_maintenance_seq > 0
       )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'linked project approval requires positive maintenance sequence'
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_project_approvals_task6_update
BEFORE UPDATE ON project_approvals
WHEN (
       OLD.operation_id IS NOT NULL
       AND (
            NEW.operation_id IS NOT OLD.operation_id
         OR NEW.operation_maintenance_seq
            IS NOT OLD.operation_maintenance_seq
       )
     )
  OR (
       OLD.operation_id IS NOT NULL
       AND (
            NEW.approval_id IS NOT OLD.approval_id
         OR NEW.project_id IS NOT OLD.project_id
         OR NEW.turn_id IS NOT OLD.turn_id
         OR NEW.actor_id IS NOT OLD.actor_id
         OR NEW.authorization_actor_id
            IS NOT OLD.authorization_actor_id
         OR NEW.canonical_action IS NOT OLD.canonical_action
         OR NEW.approval_class IS NOT OLD.approval_class
         OR NEW.command_revision IS NOT OLD.command_revision
         OR NEW.expected_runtime_version
            IS NOT OLD.expected_runtime_version
         OR NEW.effective_runtime_version
            IS NOT OLD.effective_runtime_version
         OR NEW.turn_expected_control_version
            IS NOT OLD.turn_expected_control_version
         OR NEW.expected_lifecycle IS NOT OLD.expected_lifecycle
         OR NEW.expected_phase IS NOT OLD.expected_phase
         OR NEW.targets_json IS NOT OLD.targets_json
         OR NEW.batch_boundary_json IS NOT OLD.batch_boundary_json
         OR NEW.status IS NOT OLD.status
         OR NEW.expires_at IS NOT OLD.expires_at
         OR NEW.resolved_at IS NOT OLD.resolved_at
         OR NEW.resolved_by_actor_id
            IS NOT OLD.resolved_by_actor_id
         OR NEW.consumed_at IS NOT OLD.consumed_at
         OR NEW.created_at IS NOT OLD.created_at
       )
       AND NOT EXISTS (
           SELECT 1
           FROM project_operations AS operation
           WHERE operation.project_id = OLD.project_id
             AND operation.operation_id = OLD.operation_id
             AND operation.guard_validated = 0
       )
     )
  OR (
       OLD.operation_id IS NULL
       AND NEW.operation_id IS NOT NULL
       AND (
           typeof(NEW.operation_maintenance_seq) != 'integer'
           OR NEW.operation_maintenance_seq <= 0
           OR NOT EXISTS (
               SELECT 1
               FROM project_operations AS operation
               WHERE operation.project_id = NEW.project_id
                 AND operation.operation_id = NEW.operation_id
                 AND operation.guard_validated = 0
           )
       )
     )
  OR (
       OLD.operation_id IS NULL
       AND NEW.operation_id IS NULL
       AND NEW.operation_maintenance_seq IS NOT NULL
     )
BEGIN
    SELECT RAISE(
        ABORT,
        'linked project approval requires decertified operation'
    );
END;
""".replace(
    "__TASK6_CRITICAL_AUTHORITY__",
    _TASK6_CRITICAL_AUTHORITY_PREDICATE_SQL,
)

_RECOVERY_EXPIRED_LEASES_SQL = """
SELECT turn.project_id, turn.turn_id, turn.sequence
FROM project_worker_leases AS lease
     INDEXED BY idx_project_worker_leases_expiry
JOIN project_turns AS turn
  ON turn.project_id = lease.project_id
 AND turn.turn_id = lease.turn_id
WHERE lease.expires_at <= ?
  AND turn.status IN ('claimed', 'stop_requested')
ORDER BY lease.expires_at, lease.project_id, lease.turn_id
LIMIT ?
"""

_RECOVERY_RECONCILING_SQL = """
SELECT turn.project_id, turn.turn_id, turn.sequence,
       EXISTS (
           SELECT 1
           FROM project_operations AS operation
                INDEXED BY idx_project_operations_recovery
           WHERE operation.project_id = turn.project_id
             AND operation.turn_id = turn.turn_id
             AND operation.guard_revision = 1
             AND operation.guard_validated = 1
             AND operation.status IN (
                 'approved', 'effect_started',
                 'receipt_recorded', 'unknown'
             )
           LIMIT 1
       ) AS has_pending_operation
FROM project_turns AS turn
     INDEXED BY idx_project_turns_actionable_recovery
WHERE turn.status = 'reconciling'
  AND turn.recovery_block_key IS NULL
ORDER BY turn.project_id, turn.sequence, turn.turn_id
LIMIT ?
"""

_OPERATION_PENDING_BRANCH_SQL = f"""
SELECT operation.project_id, operation.turn_id, operation.operation_id
FROM project_operations AS operation
     INDEXED BY idx_project_operations_recovery
JOIN project_turns AS turn
  ON turn.project_id = operation.project_id
 AND turn.turn_id = operation.turn_id
WHERE operation.status = ?
  AND operation.guard_revision = 1
  AND operation.guard_validated = 1
  AND operation.status IN (
      'approved', 'effect_started', 'receipt_recorded', 'unknown'
  )
  AND turn.status = 'reconciling'
  AND turn.recovery_block_key IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM project_worker_leases AS lease
      WHERE lease.project_id = turn.project_id
        AND lease.turn_id = turn.turn_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM project_operations AS unsafe
           INDEXED BY idx_project_operations_turn_unsafe
      WHERE unsafe.project_id = turn.project_id
        AND unsafe.turn_id = turn.turn_id
        AND ({_OPERATION_UNSAFE_CLASS_SQL}) = 1
      LIMIT 1
  )
  AND NOT EXISTS (
      SELECT 1
      FROM project_operations AS unresolved
           INDEXED BY idx_project_operations_turn_unresolved
      WHERE unresolved.project_id = turn.project_id
        AND unresolved.turn_id = turn.turn_id
        AND unresolved.guard_revision = 1
        AND unresolved.guard_validated = 1
        AND unresolved.status IN (
            'approved', 'effect_started',
            'receipt_recorded', 'unknown'
        )
        AND unresolved.operation_id != operation.operation_id
      LIMIT 1
  )
ORDER BY operation.updated_at, operation.project_id,
         operation.operation_id, operation.turn_id
LIMIT ?
"""

_OPERATION_TURN_UNSAFE_SQL = f"""
SELECT 1
FROM project_operations INDEXED BY idx_project_operations_turn_unsafe
WHERE project_id = ?
  AND turn_id = ?
  AND ({_OPERATION_UNSAFE_CLASS_SQL}) = 1
LIMIT 1
"""

_OPERATION_TURN_UNRESOLVED_SQL = """
SELECT operation_id
FROM project_operations AS unresolved
     INDEXED BY idx_project_operations_turn_unresolved
WHERE unresolved.project_id = ?
  AND unresolved.turn_id = ?
  AND unresolved.guard_revision = 1
  AND unresolved.guard_validated = 1
  AND unresolved.status IN (
      'approved', 'effect_started', 'receipt_recorded', 'unknown'
  )
LIMIT ?
"""

_APPROVAL_DUE_MAINTENANCE_SQL = """
SELECT approval_id
FROM project_approvals
     INDEXED BY idx_project_approvals_operation_due
WHERE status = 'pending'
  AND operation_id IS NOT NULL
  AND expires_at <= ?
ORDER BY expires_at, project_id, approval_id
LIMIT ?
"""

_APPROVAL_STALE_HIGH_WATER_SQL = """
SELECT operation_maintenance_seq
FROM project_approvals
     INDEXED BY idx_project_approvals_operation_stale_epoch
WHERE status = 'pending'
  AND operation_id IS NOT NULL
ORDER BY operation_maintenance_seq DESC
LIMIT 1
"""

_APPROVAL_STALE_MAINTENANCE_SQL = """
SELECT approval_id, operation_maintenance_seq
FROM project_approvals
     INDEXED BY idx_project_approvals_operation_stale_epoch
WHERE status = 'pending'
  AND operation_id IS NOT NULL
  AND operation_maintenance_seq > ?
  AND operation_maintenance_seq <= ?
ORDER BY operation_maintenance_seq
LIMIT ?
"""

_APPROVAL_STALE_REMAINING_SQL = """
SELECT 1
FROM project_approvals
     INDEXED BY idx_project_approvals_operation_stale_epoch
WHERE status = 'pending'
  AND operation_id IS NOT NULL
  AND operation_maintenance_seq > ?
  AND operation_maintenance_seq <= ?
LIMIT 1
"""

_RECOVERY_BLOCK_LOOKUP_SQL = """
SELECT event_id, payload_json
FROM project_events INDEXED BY idx_project_events_recovery_block_attempt
WHERE project_id = ? AND turn_id = ?
  AND kind = 'turn.recovery_blocked'
  AND json_array(
        json_extract(payload_json, '$.attempt_id'),
        json_extract(payload_json, '$.lease_generation'),
        json_extract(payload_json, '$.fencing_token')
      ) = json_array(?, ?, ?)
LIMIT 1
"""

_RECOVERY_BLOCK_KEY_LOOKUP_SQL = """
SELECT event_id, payload_json
FROM project_events
WHERE event_id = ? AND project_id = ? AND turn_id = ?
  AND kind = 'turn.recovery_blocked'
LIMIT 1
"""


Lifecycle = Literal["active", "awaiting_acceptance", "completed"]
ApprovalStatus = Literal["pending", "approved", "denied", "expired"]


@dataclass(frozen=True)
class RuntimeState:
    project_id: str
    lifecycle: Lifecycle
    current_phase: str | None
    version: int
    conversation_root_id: Optional[str]
    conversation_tip_id: Optional[str]
    updated_at: int


@dataclass(frozen=True)
class RuntimeTurnRecord:
    """One raw durable turn record; the service owns public JSON decoding."""

    turn_id: str
    project_id: str
    sequence: int
    idempotency_key: str
    payload_json: str
    origin_binding_id: str | None
    status: str
    attempt_id: str | None
    lease_generation: int
    fencing_token: int
    execution_state: str | None
    terminal_result_id: str | None
    recovery_block_key: str | None
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class RuntimeControlRecord:
    turn_id: str
    project_id: str
    control_state: str
    control_version: int
    idempotency_key: str | None
    command_fingerprint: str | None
    attempt_id: str | None
    claim_worker_id: str | None
    claim_lease_expires_at: int | None
    claim_canonical_session_id: str | None
    updated_at: int


@dataclass(frozen=True)
class WorkerLeaseRecord:
    lease_id: str
    project_id: str
    turn_id: str
    worker_id: str
    lease_generation: int
    fencing_token: int
    expires_at: int
    updated_at: int


@dataclass(frozen=True)
class RecoveryCandidateRecord:
    """One exact expired attempt parked for out-of-transaction readback."""

    project_id: str
    turn_id: str
    sequence: int
    source_status: str
    worker_id: str
    attempt_id: str
    lease_generation: int
    fencing_token: int
    lease_expires_at: int
    canonical_session_id: str
    execution_state: str | None
    lifecycle: str
    control_version: int


@dataclass(frozen=True)
class ProjectOperationRecord:
    """One strictly decoded Task-6 operation ledger row."""

    operation_id: str
    project_id: str
    turn_id: str
    idempotency_key: str
    approval_id: str | None
    command_revision: int
    targets_json: str
    payload_json: str
    status: str
    receipt_json: str | None
    created_at: int
    updated_at: int
    guard_revision: int
    guard_validated: int
    canonical_action: str
    batch_items_json: str
    readback_kind: str | None
    attempt_id: str
    lease_generation: int
    fencing_token: int
    receipt_id: str | None
    readback_json: str | None
    blocked_reason: str | None
    remote_idempotency_supported: bool
    approval_fingerprint_json: str | None


@dataclass(frozen=True)
class ApprovalRequest:
    """A bounded, project-scoped approval for one exact command batch."""

    approval_id: str
    project_id: str
    requester_actor_id: str
    authorization_actor_id: str
    canonical_action: str
    approval_class: str
    command_revision: int
    expected_runtime_version: int | None
    expected_lifecycle: Lifecycle | None
    expected_phase: str | None
    targets: tuple[str, ...]
    batch_id: str
    batch_items: tuple[str, ...]
    status: ApprovalStatus
    expires_at: int
    resolved_by_actor_id: str | None = None
    resolved_at: int | None = None
    consumed_at: int | None = None


class ApprovalConflictError(ValueError):
    """An approval id or immutable batch boundary conflicts with stored state."""


class LegacyOperationUnmanagedError(RuntimeError):
    """A Task-1 placeholder row has no Task-6 execution authority."""


class OperationMigrationError(RuntimeError):
    """Task-6 cannot safely add uniqueness to malformed legacy links."""


class _InvalidOperationCertification(RuntimeError):
    """One row-local canonical/inverse-pair certification failure."""


class LineageConflictError(ValueError):
    """A project already has lineage or the requested lineage conflicts."""


class LineageMigrationError(RuntimeError):
    """Persisted lineage is malformed and cannot be selected or repaired."""


class BindingConflictError(ValueError):
    """A binding identity collides with a different immutable tuple."""


class _StaleConversationTip(RuntimeError):
    """Internal rollback sentinel for a failed child-tip compare-and-swap."""


def runtime_state_from_row(row: sqlite3.Row) -> RuntimeState:
    """Map a runtime-state SQLite row to its immutable representation."""
    return RuntimeState(
        project_id=row["project_id"],
        lifecycle=row["lifecycle"],
        current_phase=row["current_phase"],
        version=row["version"],
        conversation_root_id=row["conversation_root_id"],
        conversation_tip_id=row["conversation_tip_id"],
        updated_at=row["updated_at"],
    )


_OPERATION_STATUSES = frozenset(
    {
        "awaiting_approval",
        "approved",
        "effect_started",
        "receipt_recorded",
        "unknown",
        "reconciled",
        "blocked",
    }
)


def _canonical_operation_json(
    value: object,
    *,
    require_object: bool,
) -> str:
    """Validate exact finite JSON and return its canonical representation."""
    seen: set[int] = set()

    def copy_json(item: object) -> object:
        item_type = type(item)
        if item is None or item_type in {str, bool, int}:
            return item
        if item_type is float:
            if not math.isfinite(item):
                raise ValueError("non-finite JSON number")
            return item
        if item_type is list:
            identity = id(item)
            if identity in seen:
                raise ValueError("cyclic JSON")
            seen.add(identity)
            try:
                return [copy_json(value) for value in item]
            finally:
                seen.remove(identity)
        if item_type is dict:
            identity = id(item)
            if identity in seen:
                raise ValueError("cyclic JSON")
            seen.add(identity)
            try:
                if any(type(key) is not str for key in item):
                    raise ValueError("non-string JSON key")
                return {
                    key: copy_json(value)
                    for key, value in item.items()
                }
            finally:
                seen.remove(identity)
        raise ValueError("unsupported JSON type")

    copied = copy_json(value)
    if require_object and type(copied) is not dict:
        raise ValueError("JSON object required")
    return json.dumps(
        copied,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _decode_operation_json(
    raw: object,
    *,
    require_object: bool,
) -> object:
    if type(raw) is not str or not raw:
        raise ValueError("stored JSON must be non-empty text")
    decoded = json.loads(
        raw,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(value)
        ),
    )
    if _canonical_operation_json(
        decoded, require_object=require_object
    ) != raw:
        raise ValueError("stored JSON is not canonical")
    return decoded


def _project_operation_from_row(
    row: sqlite3.Row | dict[str, object],
    *,
    expected_guard_validated: int,
) -> ProjectOperationRecord:
    """Map one operation at one explicit durable-certification boundary."""
    try:
        guard_revision = row["guard_revision"]
        guard_validated = row["guard_validated"]
    except (IndexError, KeyError) as exc:
        raise RuntimeError(
            "malformed persisted project operation"
        ) from exc
    if type(guard_revision) is int and guard_revision == 0:
        raise LegacyOperationUnmanagedError(
            "legacy operation has no managed execution authority"
        )
    try:
        targets = _decode_operation_json(
            row["targets_json"], require_object=False
        )
        batch_items = _decode_operation_json(
            row["batch_items_json"], require_object=False
        )
        _decode_operation_json(row["payload_json"], require_object=True)
        receipt_json = row["receipt_json"]
        readback_json = row["readback_json"]
        approval_fingerprint_json = row[
            "approval_fingerprint_json"
        ]
        if receipt_json is not None:
            _decode_operation_json(receipt_json, require_object=True)
        decoded_readback = None
        if readback_json is not None:
            decoded_readback = _decode_operation_json(
                readback_json, require_object=True
            )
        decoded_fingerprint = None
        if approval_fingerprint_json is not None:
            decoded_fingerprint = _decode_operation_json(
                approval_fingerprint_json, require_object=True
            )
        status = row["status"]
        approval_id = row["approval_id"]
        readback_kind = row["readback_kind"]
        receipt_id = row["receipt_id"]
        blocked_reason = row["blocked_reason"]
        remote_idempotency_supported = row[
            "remote_idempotency_supported"
        ]
        canonical_action = row["canonical_action"]
        valid_targets = (
            type(targets) is list
            and bool(targets)
            and all(type(value) is str and value for value in targets)
            and canonicalize_targets(tuple(targets)) == tuple(targets)
        )
        valid_batch = (
            type(batch_items) is list
            and bool(batch_items)
            and all(
                type(value) is str and value for value in batch_items
            )
            and len(set(batch_items)) == len(batch_items)
        )
        receipt_present = (
            _stored_text(receipt_id) and type(receipt_json) is str
        )
        receipt_absent = receipt_id is None and receipt_json is None
        authoritative_not_applied = (
            type(decoded_readback) is dict
            and set(decoded_readback) == {"evidence", "outcome"}
            and decoded_readback["outcome"] == "not_applied"
            and type(decoded_readback["outcome"]) is str
            and type(decoded_readback["evidence"]) is dict
            and bool(decoded_readback["evidence"])
        )
        authoritative_applied = (
            type(decoded_readback) is dict
            and set(decoded_readback) == {"evidence", "outcome"}
            and decoded_readback["outcome"] == "applied"
            and type(decoded_readback["outcome"]) is str
            and type(decoded_readback["evidence"]) is dict
            and bool(decoded_readback["evidence"])
        )
        expected_approval_class = (
            _TASK6_CRITICAL_APPROVAL_CLASS_BY_ACTION.get(
                canonical_action
            )
        )
        fingerprint_present = (
            type(decoded_fingerprint) is dict
            and set(decoded_fingerprint)
            == {
                "approval_class",
                "approval_id",
                "authorization_actor_id",
                "expires_at",
                "requires_owner",
            }
            and _stored_text(decoded_fingerprint["approval_class"])
            and _stored_text(decoded_fingerprint["approval_id"])
            and _stored_text(
                decoded_fingerprint["authorization_actor_id"]
            )
            and _stored_int(decoded_fingerprint["expires_at"])
            and decoded_fingerprint["requires_owner"] is True
        )
        fingerprint_absent = approval_fingerprint_json is None
        capability_missing = (
            remote_idempotency_supported == 0
            or readback_kind is None
        )
        capability_blocked = (
            status == "blocked"
            and blocked_reason == "operation_capability_unsupported"
        )
        valid = (
            guard_revision == 1
            and type(guard_validated) is int
            and guard_validated == expected_guard_validated
            and _stored_text(row["operation_id"])
            and _stored_text(row["project_id"])
            and _stored_text(row["turn_id"])
            and _stored_text(row["idempotency_key"])
            and _stored_text(approval_id, optional=True)
            and _stored_int(row["command_revision"], minimum=1)
            and valid_targets
            and valid_batch
            and type(status) is str
            and status in _OPERATION_STATUSES
            and _stored_int(row["created_at"])
            and _stored_int(row["updated_at"])
            and _stored_text(canonical_action)
            and _stored_text(readback_kind, optional=True)
            and _stored_text(row["attempt_id"])
            and _stored_int(row["lease_generation"], minimum=1)
            and _stored_int(row["fencing_token"], minimum=1)
            and _stored_text(receipt_id, optional=True)
            and _stored_text(blocked_reason, optional=True)
            and type(remote_idempotency_supported) is int
            and remote_idempotency_supported in {0, 1}
            and (
                (
                    capability_missing
                    and capability_blocked
                    and approval_id is None
                )
                or (
                    not capability_missing
                    and not capability_blocked
                )
            )
            and (
                (
                    expected_approval_class is None
                    and fingerprint_absent
                    and approval_id is None
                )
                or (
                    type(expected_approval_class) is str
                    and fingerprint_present
                    and decoded_fingerprint["approval_class"]
                    == expected_approval_class
                    and (
                        (
                            approval_id is not None
                            and decoded_fingerprint["approval_id"]
                            == approval_id
                        )
                        or (
                            approval_id is None
                            and capability_blocked
                        )
                    )
                )
            )
            and (
                status != "awaiting_approval"
                or approval_id is not None
            )
            and (
                status == "blocked"
                or readback_kind is not None
            )
            and (
                (
                    (
                        status
                        in {
                            "receipt_recorded",
                            "unknown",
                            "reconciled",
                        }
                        or (
                            status == "blocked"
                            and blocked_reason
                            == "operation_readback_ambiguous"
                        )
                    )
                    and receipt_present
                )
                or (
                    status not in {"receipt_recorded", "reconciled"}
                    and receipt_absent
                )
            )
            and (
                (
                    status == "reconciled"
                    and type(readback_json) is str
                    and authoritative_applied
                )
                or (
                    status == "approved"
                    and (
                        readback_json is None
                        or authoritative_not_applied
                    )
                )
                or (
                    status not in {"approved", "reconciled"}
                    and readback_json is None
                )
            )
            and (
                (
                    status == "blocked"
                    and blocked_reason is not None
                )
                or (
                    status != "blocked"
                    and blocked_reason is None
                )
            )
        )
    except (
        IndexError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise RuntimeError(
            "malformed persisted project operation"
        ) from exc
    if not valid:
        raise RuntimeError("malformed persisted project operation")
    return ProjectOperationRecord(
        operation_id=row["operation_id"],
        project_id=row["project_id"],
        turn_id=row["turn_id"],
        idempotency_key=row["idempotency_key"],
        approval_id=approval_id,
        command_revision=row["command_revision"],
        targets_json=row["targets_json"],
        payload_json=row["payload_json"],
        status=status,
        receipt_json=receipt_json,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        guard_revision=guard_revision,
        guard_validated=guard_validated,
        canonical_action=canonical_action,
        batch_items_json=row["batch_items_json"],
        readback_kind=readback_kind,
        attempt_id=row["attempt_id"],
        lease_generation=row["lease_generation"],
        fencing_token=row["fencing_token"],
        receipt_id=receipt_id,
        readback_json=readback_json,
        blocked_reason=blocked_reason,
        remote_idempotency_supported=bool(
            remote_idempotency_supported
        ),
        approval_fingerprint_json=approval_fingerprint_json,
    )


def project_operation_from_row(
    row: sqlite3.Row | dict[str, object],
) -> ProjectOperationRecord:
    """Map one certified managed operation row."""
    return _project_operation_from_row(
        row, expected_guard_validated=1
    )


_TURN_STATUSES = frozenset(
    {
        "queued",
        "claimed",
        "awaiting_approval",
        "stop_requested",
        "stopped",
        "reconciling",
        "succeeded",
        "failed",
        "cancelled",
    }
)
_CONTROL_STATES = frozenset(
    {"running", "stop_requested", "stopped", "resume_requested", "terminal"}
)
_TERMINAL_TURN_STATUSES = frozenset({"succeeded", "failed", "cancelled"})


def _stored_text(value: object, *, optional: bool = False) -> bool:
    return (optional and value is None) or (
        type(value) is str and bool(value)
    )


def _stored_int(
    value: object, *, minimum: int = 0, optional: bool = False
) -> bool:
    return (optional and value is None) or (
        type(value) is int and value >= minimum
    )


def _recovery_block_key(
    *,
    project_id: str,
    turn_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
) -> str:
    """Return the opaque deterministic key for one exact blocked attempt."""
    identity = json.dumps(
        {
            "attempt_id": attempt_id,
            "fencing_token": fencing_token,
            "lease_generation": lease_generation,
            "project_id": project_id,
            "turn_id": turn_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return (
        "recovery-blocked-"
        f"{uuid.uuid5(uuid.NAMESPACE_URL, identity).hex}"
    )


def _decode_recovery_block_payload(payload_json: object) -> dict[str, object]:
    """Decode one canonical recovery-block audit payload exactly."""
    if type(payload_json) is not str:
        raise RuntimeError("recovery block event payload is malformed")
    try:
        payload = json.loads(
            payload_json,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(value)
            ),
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "recovery block event payload is malformed"
        ) from exc
    if (
        type(payload) is not dict
        or set(payload)
        != {
            "attempt_id",
            "fencing_token",
            "lease_generation",
            "source_status",
            "turn_id",
            "version",
        }
        or not _stored_text(payload["attempt_id"])
        or not _stored_int(payload["fencing_token"], minimum=1)
        or not _stored_int(payload["lease_generation"], minimum=1)
        or type(payload["source_status"]) is not str
        or payload["source_status"] not in {"claimed", "stop_requested"}
        or not _stored_text(payload["turn_id"])
        or not _stored_int(payload["version"])
    ):
        raise RuntimeError("recovery block event payload is malformed")
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    if canonical != payload_json:
        raise RuntimeError("recovery block event payload is not canonical")
    return payload


def _validate_recovery_block_payload(
    payload_json: object,
    *,
    turn_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
) -> None:
    payload = _decode_recovery_block_payload(payload_json)
    if (
        payload["turn_id"] != turn_id
        or payload["attempt_id"] != attempt_id
        or payload["lease_generation"] != lease_generation
        or payload["fencing_token"] != fencing_token
    ):
        raise RuntimeError(
            "recovery block event payload has inconsistent identity"
        )


def _valid_task5_turn_metadata(
    *,
    status: str,
    attempt_id: str | None,
    lease_generation: int,
    fencing_token: int,
    execution_state: str | None,
    terminal_result_id: str | None,
    recovery_block_key: str | None,
    project_id: str,
    turn_id: str,
) -> bool:
    """Check cross-column states reachable by Task 5 or its migration."""
    if attempt_id is None and execution_state is not None:
        return False
    if recovery_block_key is not None:
        if attempt_id is None or status != "reconciling":
            return False
        if recovery_block_key != _recovery_block_key(
            project_id=project_id,
            turn_id=turn_id,
            attempt_id=attempt_id,
            lease_generation=lease_generation,
            fencing_token=fencing_token,
        ):
            return False
    if terminal_result_id is None:
        return (
            status not in {"succeeded", "failed"}
            or execution_state is None
        )
    return (
        status in {"succeeded", "failed"}
        and attempt_id is not None
        and execution_state in {None, "started"}
    )


def execute_schema_statements(
    conn: sqlite3.Connection, schema_sql: str
) -> None:
    """Execute a SQL schema without committing a caller-owned transaction."""
    statement_chars: list[str] = []
    for char in schema_sql:
        statement_chars.append(char)
        if char != ";":
            continue
        statement = "".join(statement_chars)
        if not sqlite3.complete_statement(statement):
            continue
        if statement.strip():
            conn.execute(statement)
        statement_chars.clear()

    remainder = "".join(statement_chars)
    if not remainder.strip():
        return
    try:
        # Valid SQL need not end in a semicolon. SQLite also accepts
        # whitespace/comment-only input as a no-op.
        conn.execute(remainder)
    except sqlite3.OperationalError as exc:
        if "incomplete input" in str(exc).lower():
            raise ValueError("incomplete schema SQL") from exc
        raise


def _normalized_schema_sql(value: object) -> str | None:
    if type(value) is not str:
        return None
    return " ".join(value.strip().rstrip(";").split())


def _operation_unsafe_index_is_canonical(
    conn: sqlite3.Connection,
) -> bool:
    row = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'index'
          AND name = 'idx_project_operations_turn_unsafe'
        """
    ).fetchone()
    if row is None or _normalized_schema_sql(
        row["sql"]
    ) != _normalized_schema_sql(_OPERATION_UNSAFE_INDEX_SQL):
        return False
    index_rows = [
        index
        for index in conn.execute(
            "PRAGMA index_list('project_operations')"
        )
        if index["name"] == "idx_project_operations_turn_unsafe"
    ]
    if len(index_rows) != 1 or index_rows[0]["partial"] != 0:
        return False
    key_rows = [
        index
        for index in conn.execute(
            "PRAGMA index_xinfo('idx_project_operations_turn_unsafe')"
        )
        if index["key"] == 1
    ]
    return [
        (index["seqno"], index["cid"], index["name"])
        for index in key_rows
    ] == [
        (0, 1, "project_id"),
        (1, 2, "turn_id"),
        (2, -2, None),
        (3, 0, "operation_id"),
    ]


def _ensure_operation_unsafe_index(
    conn: sqlite3.Connection,
) -> None:
    """Replace every stale unsafe classifier with one canonical expression."""
    version = sqlite3.sqlite_version_info
    if not (
        type(version) is tuple
        and len(version) >= 3
        and all(type(part) is int for part in version[:3])
        and version[:3] >= (3, 9, 0)
    ):
        raise OperationMigrationError(
            "SQLite expression index support requires version 3.9.0"
        )
    try:
        helper = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'index'
              AND name =
                  'idx_project_operations_turn_unsafe_blocked'
            """
        ).fetchone()
        if helper is not None:
            conn.execute(
                "DROP INDEX idx_project_operations_turn_unsafe_blocked"
            )
        if not _operation_unsafe_index_is_canonical(conn):
            stale = conn.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'index'
                  AND name = 'idx_project_operations_turn_unsafe'
                """
            ).fetchone()
            if stale is not None:
                conn.execute(
                    "DROP INDEX idx_project_operations_turn_unsafe"
                )
            execute_schema_statements(
                conn, _OPERATION_UNSAFE_INDEX_SQL
            )
        if not _operation_unsafe_index_is_canonical(conn):
            raise OperationMigrationError(
                "unsafe operation index did not converge"
            )
    except OperationMigrationError:
        raise
    except sqlite3.Error as exc:
        raise OperationMigrationError(
            "unsafe operation index replacement failed"
        ) from exc


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create all additive ProjectRuntime tables without adopting projects."""
    conn.execute("PRAGMA foreign_keys=ON")
    execute_schema_statements(conn, RUNTIME_SCHEMA_SQL)
    _ensure_runtime_state_columns(conn)
    _ensure_approval_columns(conn)
    _ensure_run_control_columns(conn)
    _ensure_turn_columns(conn)
    _ensure_operation_columns(conn)
    _validate_existing_lineage(conn)
    try:
        execute_schema_statements(conn, LINEAGE_INDEX_SQL)
        execute_schema_statements(conn, TASK4_INDEX_SQL)
        execute_schema_statements(conn, TASK5_INDEX_SQL)
    except sqlite3.IntegrityError as exc:
        raise LineageMigrationError(
            "multiple conversation roots exist for one project"
        ) from exc


def _ensure_operation_columns(conn: sqlite3.Connection) -> None:
    """Install and migrate durable Task-6 operation certification."""
    with write_transaction(conn):
        conn.execute(
            """
            INSERT OR IGNORE INTO project_operation_maintenance (
                singleton, approval_scan_after, next_lane
            ) VALUES (1, '', 0)
            """
        )
        for name, ddl in (
            (
                "guard_revision",
                """
                guard_revision INTEGER NOT NULL DEFAULT 0
                CHECK (
                    typeof(guard_revision) = 'integer'
                    AND guard_revision IN (0, 1)
                )
                """,
            ),
            (
                "guard_validated",
                """
                guard_validated INTEGER NOT NULL DEFAULT 0
                CHECK (
                    typeof(guard_validated) = 'integer'
                    AND guard_validated IN (0, 1)
                )
                """,
            ),
            ("canonical_action", "canonical_action TEXT"),
            ("batch_items_json", "batch_items_json TEXT"),
            ("readback_kind", "readback_kind TEXT"),
            ("attempt_id", "attempt_id TEXT"),
            ("lease_generation", "lease_generation INTEGER"),
            ("fencing_token", "fencing_token INTEGER"),
            ("receipt_id", "receipt_id TEXT"),
            ("readback_json", "readback_json TEXT"),
            ("blocked_reason", "blocked_reason TEXT"),
            (
                "remote_idempotency_supported",
                "remote_idempotency_supported INTEGER",
            ),
            (
                "approval_fingerprint_json",
                "approval_fingerprint_json TEXT",
            ),
        ):
            add_column_if_missing(conn, "project_operations", name, ddl)
        add_column_if_missing(
            conn,
            "project_approvals",
            "operation_maintenance_seq",
            """
            operation_maintenance_seq INTEGER
                CHECK (
                    operation_maintenance_seq IS NULL
                    OR (
                        typeof(operation_maintenance_seq) = 'integer'
                        AND operation_maintenance_seq > 0
                    )
                )
            """,
        )
        add_column_if_missing(
            conn,
            "project_operation_maintenance",
            "operation_validation_migration_complete",
            """
            operation_validation_migration_complete
                INTEGER NOT NULL DEFAULT 0
                CHECK (
                    typeof(
                        operation_validation_migration_complete
                    ) = 'integer'
                    AND operation_validation_migration_complete IN (0, 1)
                )
            """,
        )
        for name, ddl in (
            (
                "approval_scan_after_seq",
                """
                approval_scan_after_seq INTEGER NOT NULL DEFAULT 0
                    CHECK (
                        typeof(approval_scan_after_seq) = 'integer'
                        AND approval_scan_after_seq >= 0
                    )
                """,
            ),
            (
                "approval_scan_high_water_seq",
                """
                approval_scan_high_water_seq
                    INTEGER NOT NULL DEFAULT 0
                    CHECK (
                        typeof(approval_scan_high_water_seq) = 'integer'
                        AND approval_scan_high_water_seq >= 0
                    )
                """,
            ),
            (
                "approval_scan_epoch",
                """
                approval_scan_epoch INTEGER NOT NULL DEFAULT 0
                    CHECK (
                        typeof(approval_scan_epoch) = 'integer'
                        AND approval_scan_epoch >= 0
                    )
                """,
            ),
            (
                "next_operation_approval_seq",
                """
                next_operation_approval_seq
                    INTEGER NOT NULL DEFAULT 1
                    CHECK (
                        typeof(next_operation_approval_seq) = 'integer'
                        AND next_operation_approval_seq > 0
                    )
                """,
            ),
        ):
            add_column_if_missing(
                conn,
                "project_operation_maintenance",
                name,
                ddl,
            )
        maintenance_rows = conn.execute(
            """
            SELECT singleton, approval_scan_after, next_lane,
                   operation_validation_migration_complete,
                   approval_scan_after_seq,
                   approval_scan_high_water_seq,
                   approval_scan_epoch,
                   next_operation_approval_seq
            FROM project_operation_maintenance
            """
        ).fetchall()
        if len(maintenance_rows) != 1:
            raise OperationMigrationError(
                "invalid operation maintenance singleton"
            )
        maintenance = maintenance_rows[0]
        if not (
            type(maintenance["singleton"]) is int
            and maintenance["singleton"] == 1
            and type(maintenance["approval_scan_after"]) is str
            and type(maintenance["next_lane"]) is int
            and maintenance["next_lane"] in {0, 1}
            and _stored_int(maintenance["approval_scan_after_seq"])
            and _stored_int(
                maintenance["approval_scan_high_water_seq"]
            )
            and _stored_int(maintenance["approval_scan_epoch"])
            and _stored_int(
                maintenance["next_operation_approval_seq"],
                minimum=1,
            )
            and type(
                maintenance[
                    "operation_validation_migration_complete"
                ]
            )
            is int
            and maintenance[
                "operation_validation_migration_complete"
            ]
            in {0, 1}
        ):
            raise OperationMigrationError(
                "invalid operation maintenance singleton"
            )
        non_inverse_operation_link = conn.execute(
            """
            SELECT operation.project_id, operation.operation_id
            FROM project_operations AS operation
            LEFT JOIN project_approvals AS approval
              ON approval.project_id = operation.project_id
             AND approval.approval_id = operation.approval_id
            WHERE operation.approval_id IS NOT NULL
              AND (
                  approval.approval_id IS NULL
                  OR approval.operation_id IS NULL
                  OR approval.operation_id != operation.operation_id
              )
            LIMIT 1
            """
        ).fetchone()
        non_inverse_approval_link = conn.execute(
            """
            SELECT approval.project_id, approval.approval_id
            FROM project_approvals AS approval
            LEFT JOIN project_operations AS operation
              ON operation.project_id = approval.project_id
             AND operation.operation_id = approval.operation_id
            WHERE approval.operation_id IS NOT NULL
              AND (
                  operation.operation_id IS NULL
                  OR operation.approval_id IS NULL
                  OR operation.approval_id != approval.approval_id
              )
            LIMIT 1
            """
        ).fetchone()
        duplicate_operation_link = conn.execute(
            """
            SELECT project_id, operation_id
            FROM project_approvals
            WHERE operation_id IS NOT NULL
            GROUP BY project_id, operation_id
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        ).fetchone()
        duplicate_approval_link = conn.execute(
            """
            SELECT project_id, approval_id
            FROM project_operations
            WHERE approval_id IS NOT NULL
            GROUP BY project_id, approval_id
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        ).fetchone()
        if (
            duplicate_operation_link is not None
            or duplicate_approval_link is not None
        ):
            raise OperationMigrationError(
                "duplicate approval operation links"
            )
        if (
            non_inverse_operation_link is not None
            or non_inverse_approval_link is not None
        ):
            raise OperationMigrationError(
                "operation approval links are not inverse"
            )

        conn.execute(
            "DROP TRIGGER IF EXISTS trg_project_approvals_task6_insert"
        )
        conn.execute(
            "DROP TRIGGER IF EXISTS trg_project_approvals_task6_update"
        )
        invalid_approval_sequence = conn.execute(
            """
            SELECT approval_id
            FROM project_approvals
            WHERE (
                    operation_id IS NULL
                    AND operation_maintenance_seq IS NOT NULL
                  )
               OR (
                    operation_id IS NOT NULL
                    AND operation_maintenance_seq IS NOT NULL
                    AND (
                        typeof(operation_maintenance_seq) != 'integer'
                        OR operation_maintenance_seq <= 0
                    )
                  )
            LIMIT 1
            """
        ).fetchone()
        duplicate_approval_sequence = conn.execute(
            """
            SELECT operation_maintenance_seq
            FROM project_approvals
            WHERE operation_id IS NOT NULL
              AND operation_maintenance_seq IS NOT NULL
            GROUP BY operation_maintenance_seq
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        ).fetchone()
        if (
            invalid_approval_sequence is not None
            or duplicate_approval_sequence is not None
        ):
            raise OperationMigrationError(
                "invalid operation approval maintenance sequence"
            )
        unsequenced_approvals = conn.execute(
            """
            SELECT rowid AS approval_rowid, approval_id
            FROM project_approvals
            WHERE operation_id IS NOT NULL
              AND operation_maintenance_seq IS NULL
            ORDER BY rowid, approval_id
            """
        ).fetchall()
        maximum_sequence = conn.execute(
            """
            SELECT COALESCE(MAX(operation_maintenance_seq), 0)
            FROM project_approvals
            WHERE operation_id IS NOT NULL
            """
        ).fetchone()[0]
        if not _stored_int(maximum_sequence):
            raise OperationMigrationError(
                "invalid operation approval maintenance sequence"
            )
        if (
            maximum_sequence > _SQLITE_INT_MAX
            - len(unsequenced_approvals)
        ):
            raise OperationMigrationError(
                "operation approval maintenance sequence exhausted"
            )
        for offset, approval in enumerate(
            unsequenced_approvals, start=1
        ):
            assigned = maximum_sequence + offset
            if conn.execute(
                """
                UPDATE project_approvals
                SET operation_maintenance_seq = ?
                WHERE rowid = ? AND approval_id = ?
                  AND operation_id IS NOT NULL
                  AND operation_maintenance_seq IS NULL
                """,
                (
                    assigned,
                    approval["approval_rowid"],
                    approval["approval_id"],
                ),
            ).rowcount != 1:
                raise OperationMigrationError(
                    "operation approval sequence migration changed"
                )
        final_maximum = maximum_sequence + len(
            unsequenced_approvals
        )
        if final_maximum >= _SQLITE_INT_MAX:
            raise OperationMigrationError(
                "operation approval maintenance sequence exhausted"
            )
        target_next_sequence = max(
            maintenance["next_operation_approval_seq"],
            final_maximum + 1,
        )
        if conn.execute(
            """
            UPDATE project_operation_maintenance
            SET next_operation_approval_seq = ?,
                approval_scan_after_seq = CASE
                    WHEN ? THEN 0 ELSE approval_scan_after_seq
                END,
                approval_scan_high_water_seq = CASE
                    WHEN ? THEN 0 ELSE approval_scan_high_water_seq
                END,
                approval_scan_epoch = CASE
                    WHEN ? THEN 0 ELSE approval_scan_epoch
                END
            WHERE singleton = 1
              AND next_operation_approval_seq = ?
              AND approval_scan_after_seq = ?
              AND approval_scan_high_water_seq = ?
              AND approval_scan_epoch = ?
            """,
            (
                target_next_sequence,
                bool(unsequenced_approvals),
                bool(unsequenced_approvals),
                bool(unsequenced_approvals),
                maintenance["next_operation_approval_seq"],
                maintenance["approval_scan_after_seq"],
                maintenance["approval_scan_high_water_seq"],
                maintenance["approval_scan_epoch"],
            ),
        ).rowcount != 1:
            raise OperationMigrationError(
                "operation approval maintenance state changed"
            )
        maintenance = conn.execute(
            """
            SELECT singleton, approval_scan_after, next_lane,
                   operation_validation_migration_complete,
                   approval_scan_after_seq,
                   approval_scan_high_water_seq,
                   approval_scan_epoch,
                   next_operation_approval_seq
            FROM project_operation_maintenance
            WHERE singleton = 1
            """
        ).fetchone()
        if maintenance is None:
            raise OperationMigrationError(
                "operation approval maintenance state disappeared"
            )

        for trigger_name in (
            "trg_project_operations_task6_insert",
            "trg_project_operations_task6_update",
        ):
            trigger = conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'trigger' AND name = ?
                """,
                (trigger_name,),
            ).fetchone()
            if trigger is not None and (
                "guard_validated" not in trigger["sql"]
                or "remote_idempotency_supported"
                not in trigger["sql"]
                or "approval_fingerprint_json"
                not in trigger["sql"]
                or "critical.column2" not in trigger["sql"]
                or "inverse_approval.operation_id"
                not in trigger["sql"]
            ):
                conn.execute(f"DROP TRIGGER {trigger_name}")

        if (
            maintenance[
                "operation_validation_migration_complete"
            ]
            == 0
        ):
            conn.execute(
                """
                UPDATE project_operations
                SET guard_validated = 0
                WHERE guard_validated IS NOT 0
                """
            )
            rows = conn.execute(
                """
                SELECT *
                FROM project_operations
                WHERE guard_revision = 1
                  AND guard_validated = 0
                ORDER BY rowid, operation_id
                """
            ).fetchall()
            for row in rows:
                try:
                    _certify_project_operation_row(conn, row)
                except _InvalidOperationCertification:
                    continue
                except (RuntimeError, sqlite3.Error) as exc:
                    raise OperationMigrationError(
                        "operation validation migration failed"
                    ) from exc
            if conn.execute(
                """
                UPDATE project_operation_maintenance
                SET operation_validation_migration_complete = 1
                WHERE singleton = 1
                  AND operation_validation_migration_complete = 0
                """
            ).rowcount != 1:
                raise OperationMigrationError(
                    "operation validation migration changed"
                )

        index_requirements = {
            "idx_project_operations_receipt": (
                "guard_validated = 1",
            ),
            "idx_project_operations_turn_status": (
                "guard_validated = 1",
            ),
            "idx_project_operations_recovery": (
                "status, updated_at, project_id, operation_id, turn_id",
                "guard_validated = 1",
            ),
            "idx_project_operations_approved_rehydrate": (
                "guard_validated = 1",
            ),
            "idx_project_operations_turn_unresolved": (
                "guard_validated = 1",
            ),
            "idx_project_approvals_operation_maintenance_seq": (
                "create unique index",
                "operation_maintenance_seq",
                "where operation_id is not null",
            ),
            "idx_project_approvals_operation_stale_epoch": (
                "operation_maintenance_seq",
                "where status = 'pending' and operation_id is not null",
            ),
        }
        for index_name, required_fragments in index_requirements.items():
            index = conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'index' AND name = ?
                """,
                (index_name,),
            ).fetchone()
            if index is None:
                continue
            normalized = " ".join(index["sql"].lower().split())
            if not all(
                fragment in normalized
                for fragment in required_fragments
            ):
                conn.execute(f"DROP INDEX {index_name}")
        conn.execute(
            "DROP INDEX IF EXISTS idx_project_operations_turn_guard"
        )
        conn.execute(
            "DROP INDEX IF EXISTS idx_project_approvals_operation_scan"
        )
        execute_schema_statements(conn, TASK6_INDEX_SQL)
        _ensure_operation_unsafe_index(conn)
        execute_schema_statements(conn, TASK6_TRIGGER_SQL)


def _validate_existing_lineage(conn: sqlite3.Connection) -> None:
    """Fail closed when additive migration encounters malformed lineage."""
    malformed_conversation = conn.execute(
        """
        SELECT conversation.conversation_id
        FROM project_conversations AS conversation
        LEFT JOIN project_conversations AS root
          ON root.project_id = conversation.project_id
         AND root.conversation_id = conversation.root_conversation_id
         AND root.parent_conversation_id IS NULL
         AND root.root_conversation_id = root.conversation_id
        LEFT JOIN project_conversations AS parent
          ON parent.project_id = conversation.project_id
         AND parent.conversation_id = conversation.parent_conversation_id
         AND parent.root_conversation_id = conversation.root_conversation_id
        WHERE (
                conversation.parent_conversation_id IS NULL
                AND (
                    conversation.root_conversation_id IS NULL
                    OR conversation.root_conversation_id
                       <> conversation.conversation_id
                )
              )
           OR (
                conversation.parent_conversation_id IS NOT NULL
                AND (
                    conversation.conversation_id
                       = conversation.parent_conversation_id
                    OR conversation.root_conversation_id IS NULL
                    OR root.conversation_id IS NULL
                    OR parent.conversation_id IS NULL
                )
              )
        LIMIT 1
        """
    ).fetchone()
    if malformed_conversation is not None:
        raise LineageMigrationError("malformed project conversation lineage")

    unreachable_conversation = conn.execute(
        """
        WITH RECURSIVE reachable (
            project_id, conversation_id, root_conversation_id
        ) AS (
            SELECT project_id, conversation_id, root_conversation_id
            FROM project_conversations
            WHERE parent_conversation_id IS NULL
              AND root_conversation_id = conversation_id
            UNION
            SELECT child.project_id,
                   child.conversation_id,
                   child.root_conversation_id
            FROM project_conversations AS child
            JOIN reachable AS parent
              ON parent.project_id = child.project_id
             AND parent.conversation_id = child.parent_conversation_id
             AND parent.root_conversation_id = child.root_conversation_id
        )
        SELECT conversation.conversation_id
        FROM project_conversations AS conversation
        LEFT JOIN reachable
          ON reachable.project_id = conversation.project_id
         AND reachable.conversation_id = conversation.conversation_id
         AND reachable.root_conversation_id
             = conversation.root_conversation_id
        WHERE reachable.conversation_id IS NULL
        LIMIT 1
        """
    ).fetchone()
    if unreachable_conversation is not None:
        raise LineageMigrationError(
            "project conversation is not reachable from its root"
        )

    malformed_state = conn.execute(
        """
        SELECT state.project_id
        FROM project_runtime_state AS state
        LEFT JOIN project_conversations AS root
          ON root.project_id = state.project_id
         AND root.conversation_id = state.conversation_root_id
         AND root.parent_conversation_id IS NULL
         AND root.root_conversation_id = root.conversation_id
        LEFT JOIN project_conversations AS tip
          ON tip.project_id = state.project_id
         AND tip.conversation_id = state.conversation_tip_id
         AND tip.root_conversation_id = state.conversation_root_id
        WHERE state.conversation_root_id IS NULL
           OR state.conversation_tip_id IS NULL
           OR root.conversation_id IS NULL
           OR tip.conversation_id IS NULL
        LIMIT 1
        """
    ).fetchone()
    if malformed_state is not None:
        raise LineageMigrationError("runtime state has dangling project lineage")


def _ensure_runtime_state_columns(conn: sqlite3.Connection) -> None:
    """Expose phase authority on legacy state rows without inferring a value."""
    add_column_if_missing(
        conn,
        "project_runtime_state",
        "current_phase",
        "current_phase TEXT",
    )


def _ensure_approval_columns(conn: sqlite3.Connection) -> None:
    """Add Task-2 approval fields to a Task-1 database without data loss."""
    for name, ddl in (
        ("authorization_actor_id", "authorization_actor_id TEXT"),
        ("canonical_action", "canonical_action TEXT"),
        ("resolved_by_actor_id", "resolved_by_actor_id TEXT"),
        ("consumed_at", "consumed_at INTEGER"),
        ("expected_runtime_version", "expected_runtime_version INTEGER"),
        ("effective_runtime_version", "effective_runtime_version INTEGER"),
        (
            "turn_expected_control_version",
            """
            turn_expected_control_version INTEGER
            CHECK (
                turn_expected_control_version IS NULL
                OR (
                    typeof(turn_expected_control_version) = 'integer'
                    AND turn_expected_control_version >= 0
                )
            )
            """,
        ),
        ("expected_lifecycle", "expected_lifecycle TEXT"),
        ("expected_phase", "expected_phase TEXT"),
    ):
        add_column_if_missing(
            conn,
            "project_approvals",
            name,
            ddl,
        )
    with write_transaction(conn):
        conn.execute(
            """
            UPDATE project_approvals
            SET effective_runtime_version = expected_runtime_version
            WHERE effective_runtime_version IS NULL
              AND expected_runtime_version IS NOT NULL
            """
        )


def _ensure_run_control_columns(conn: sqlite3.Connection) -> None:
    """Add Task-4's immutable control-command fingerprint additively."""
    for name, ddl in (
        ("command_fingerprint", "command_fingerprint TEXT"),
        ("claim_worker_id", "claim_worker_id TEXT"),
        ("claim_lease_expires_at", "claim_lease_expires_at INTEGER"),
        (
            "claim_canonical_session_id",
            "claim_canonical_session_id TEXT",
        ),
    ):
        add_column_if_missing(conn, "project_run_controls", name, ddl)


def _ensure_turn_columns(conn: sqlite3.Connection) -> None:
    """Add Task-5 evidence and migrate its exact block projection."""
    with write_transaction(conn):
        block_key_added = False
        for name, ddl in (
            (
                "execution_state",
                """
                execution_state TEXT
                CHECK (
                    execution_state IS NULL
                    OR execution_state IN ('not_started', 'started')
                )
                """,
            ),
            (
                "terminal_result_id",
                """
                terminal_result_id TEXT
                CHECK (
                    terminal_result_id IS NULL
                    OR length(terminal_result_id) > 0
                )
                """,
            ),
            (
                "recovery_block_key",
                """
                recovery_block_key TEXT
                REFERENCES project_events(event_id)
                CHECK (
                    recovery_block_key IS NULL
                    OR length(recovery_block_key) > 0
                )
                """,
            ),
        ):
            added = add_column_if_missing(
                conn, "project_turns", name, ddl
            )
            if name == "recovery_block_key":
                block_key_added = added
        if block_key_added:
            _backfill_recovery_block_keys(conn)


def _backfill_recovery_block_keys(conn: sqlite3.Connection) -> None:
    """Project canonical pre-column block events during one-time migration."""
    rows = conn.execute(
        """
        SELECT turn.project_id, turn.turn_id, turn.attempt_id,
               turn.lease_generation, turn.fencing_token,
               event.event_id, event.payload_json
        FROM project_turns AS turn
        JOIN project_events AS event
          ON event.project_id = turn.project_id
         AND event.turn_id = turn.turn_id
         AND event.kind = 'turn.recovery_blocked'
        WHERE turn.status = 'reconciling'
          AND turn.recovery_block_key IS NULL
        ORDER BY turn.project_id, turn.turn_id, event.event_id
        """
    ).fetchall()
    migrated: set[tuple[str, str]] = set()
    for row in rows:
        if not (
            _stored_text(row["project_id"])
            and _stored_text(row["turn_id"])
            and _stored_text(row["attempt_id"])
            and _stored_int(row["lease_generation"], minimum=1)
            and _stored_int(row["fencing_token"], minimum=1)
        ):
            raise RuntimeError("malformed recovery block during migration")
        payload = _decode_recovery_block_payload(row["payload_json"])
        if payload["turn_id"] != row["turn_id"]:
            raise RuntimeError(
                "recovery block event payload has inconsistent identity"
            )
        if (
            payload["attempt_id"] != row["attempt_id"]
            or payload["lease_generation"] != row["lease_generation"]
            or payload["fencing_token"] != row["fencing_token"]
        ):
            continue
        identity = (row["project_id"], row["turn_id"])
        block_key = _recovery_block_key(
            project_id=row["project_id"],
            turn_id=row["turn_id"],
            attempt_id=row["attempt_id"],
            lease_generation=row["lease_generation"],
            fencing_token=row["fencing_token"],
        )
        if identity in migrated or row["event_id"] != block_key:
            raise RuntimeError(
                "ambiguous recovery block during migration"
            )
        migrated.add(identity)
        if conn.execute(
            """
            UPDATE project_turns SET recovery_block_key = ?
            WHERE project_id = ? AND turn_id = ?
              AND status = 'reconciling'
              AND recovery_block_key IS NULL
            """,
            (block_key, row["project_id"], row["turn_id"]),
        ).rowcount != 1:
            raise RuntimeError(
                "recovery block changed during migration"
            )


@contextlib.contextmanager
def write_transaction(
    conn: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    """Use IMMEDIATE ownership or a rollback-isolating nested savepoint."""
    if conn.in_transaction:
        savepoint = "project_runtime_nested"
        conn.execute(f"SAVEPOINT {savepoint}")
        try:
            yield conn
        except Exception:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            finally:
                conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        else:
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        return
    with write_txn(conn):
        yield conn


def _runtime_state_for_project(
    conn: sqlite3.Connection, project_id: str
) -> Optional[RuntimeState]:
    row = conn.execute(
        "SELECT * FROM project_runtime_state WHERE project_id = ?",
        (project_id,),
    ).fetchone()
    return runtime_state_from_row(row) if row is not None else None


def runtime_state_for_project(
    conn: sqlite3.Connection, project_id: str
) -> Optional[RuntimeState]:
    """Read one project's runtime state, if it has been explicitly adopted."""
    return _runtime_state_for_project(conn, project_id)


def runtime_turn_from_row(row: sqlite3.Row) -> RuntimeTurnRecord:
    """Map a turn row without interpreting its caller payload."""
    if not (
        _stored_text(row["turn_id"])
        and _stored_text(row["project_id"])
        and _stored_int(row["sequence"], minimum=1)
        and _stored_text(row["idempotency_key"])
        and _stored_text(row["payload_json"])
        and _stored_text(row["origin_binding_id"], optional=True)
        and type(row["status"]) is str
        and row["status"] in _TURN_STATUSES
        and _stored_text(row["attempt_id"], optional=True)
        and _stored_int(row["lease_generation"])
        and _stored_int(row["fencing_token"])
        and (
            row["execution_state"] is None
            or (
                type(row["execution_state"]) is str
                and row["execution_state"] in {"not_started", "started"}
            )
        )
        and _stored_text(row["terminal_result_id"], optional=True)
        and _stored_text(row["recovery_block_key"], optional=True)
        and _stored_int(row["created_at"])
        and _stored_int(row["updated_at"])
        and _valid_task5_turn_metadata(
            status=row["status"],
            attempt_id=row["attempt_id"],
            lease_generation=row["lease_generation"],
            fencing_token=row["fencing_token"],
            execution_state=row["execution_state"],
            terminal_result_id=row["terminal_result_id"],
            recovery_block_key=row["recovery_block_key"],
            project_id=row["project_id"],
            turn_id=row["turn_id"],
        )
    ):
        raise RuntimeError("malformed persisted runtime turn")
    if (
        row["attempt_id"] is None
        and not (
            (row["lease_generation"] == 0 and row["fencing_token"] == 0)
            or (
                row["lease_generation"] > 0
                and row["fencing_token"] > 0
            )
        )
    ) or (
        row["attempt_id"] is not None
        and (
            row["lease_generation"] <= 0
            or row["fencing_token"] <= 0
        )
    ):
        raise RuntimeError("persisted turn has an invalid attempt identity")
    return RuntimeTurnRecord(
        turn_id=row["turn_id"],
        project_id=row["project_id"],
        sequence=row["sequence"],
        idempotency_key=row["idempotency_key"],
        payload_json=row["payload_json"],
        origin_binding_id=row["origin_binding_id"],
        status=row["status"],
        attempt_id=row["attempt_id"],
        lease_generation=row["lease_generation"],
        fencing_token=row["fencing_token"],
        execution_state=row["execution_state"],
        terminal_result_id=row["terminal_result_id"],
        recovery_block_key=row["recovery_block_key"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def runtime_control_from_row(row: sqlite3.Row) -> RuntimeControlRecord:
    """Map the durable control lane for one exact project turn."""
    audit_values = (
        row["claim_worker_id"],
        row["claim_lease_expires_at"],
        row["claim_canonical_session_id"],
    )
    audit_is_empty = all(value is None for value in audit_values)
    audit_is_complete = (
        _stored_text(row["claim_worker_id"])
        and _stored_int(row["claim_lease_expires_at"])
        and _stored_text(row["claim_canonical_session_id"])
    )
    if not (
        _stored_text(row["turn_id"])
        and _stored_text(row["project_id"])
        and type(row["control_state"]) is str
        and row["control_state"] in _CONTROL_STATES
        and _stored_int(row["control_version"])
        and _stored_text(row["idempotency_key"], optional=True)
        and _stored_text(row["command_fingerprint"], optional=True)
        and _stored_text(row["attempt_id"], optional=True)
        and (audit_is_empty or audit_is_complete)
        and not (row["attempt_id"] is None and audit_is_complete)
        and _stored_int(row["updated_at"])
    ):
        raise RuntimeError("malformed persisted runtime control")
    return RuntimeControlRecord(
        turn_id=row["turn_id"],
        project_id=row["project_id"],
        control_state=row["control_state"],
        control_version=row["control_version"],
        idempotency_key=row["idempotency_key"],
        command_fingerprint=row["command_fingerprint"],
        attempt_id=row["attempt_id"],
        claim_worker_id=row["claim_worker_id"],
        claim_lease_expires_at=row["claim_lease_expires_at"],
        claim_canonical_session_id=row["claim_canonical_session_id"],
        updated_at=row["updated_at"],
    )


def worker_lease_from_row(row: sqlite3.Row) -> WorkerLeaseRecord:
    """Map a current worker identity; expiry interpretation remains Task 5."""
    if not (
        _stored_text(row["lease_id"])
        and _stored_text(row["project_id"])
        and _stored_text(row["turn_id"])
        and _stored_text(row["worker_id"])
        and _stored_int(row["lease_generation"], minimum=1)
        and _stored_int(row["fencing_token"], minimum=1)
        and _stored_int(row["expires_at"])
        and _stored_int(row["updated_at"])
    ):
        raise RuntimeError("malformed persisted worker lease")
    return WorkerLeaseRecord(
        lease_id=row["lease_id"],
        project_id=row["project_id"],
        turn_id=row["turn_id"],
        worker_id=row["worker_id"],
        lease_generation=row["lease_generation"],
        fencing_token=row["fencing_token"],
        expires_at=row["expires_at"],
        updated_at=row["updated_at"],
    )


def _runtime_turn_for_project(
    conn: sqlite3.Connection, *, project_id: str, turn_id: str
) -> RuntimeTurnRecord | None:
    row = conn.execute(
        """
        SELECT * FROM project_turns
        WHERE project_id = ? AND turn_id = ?
        """,
        (project_id, turn_id),
    ).fetchone()
    return runtime_turn_from_row(row) if row is not None else None


_OPERATION_CERTIFICATION_COLUMNS = (
    "operation_id",
    "project_id",
    "turn_id",
    "idempotency_key",
    "approval_id",
    "command_revision",
    "targets_json",
    "payload_json",
    "status",
    "receipt_json",
    "created_at",
    "updated_at",
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
)

_APPROVAL_CERTIFICATION_COLUMNS = (
    "approval_id",
    "project_id",
    "turn_id",
    "operation_id",
    "operation_maintenance_seq",
    "actor_id",
    "authorization_actor_id",
    "canonical_action",
    "approval_class",
    "command_revision",
    "expected_runtime_version",
    "effective_runtime_version",
    "turn_expected_control_version",
    "expected_lifecycle",
    "expected_phase",
    "targets_json",
    "batch_boundary_json",
    "status",
    "expires_at",
    "resolved_at",
    "resolved_by_actor_id",
    "consumed_at",
    "created_at",
)


def _operation_approval_certification_row(
    conn: sqlite3.Connection,
    operation: ProjectOperationRecord,
) -> sqlite3.Row | None:
    """Validate and return the complete inverse approval, when required."""
    rows = conn.execute(
        """
        SELECT *
        FROM project_approvals
        WHERE project_id = ? AND operation_id = ?
        ORDER BY approval_id
        """,
        (operation.project_id, operation.operation_id),
    ).fetchall()
    expected_class = _TASK6_CRITICAL_APPROVAL_CLASS_BY_ACTION.get(
        operation.canonical_action
    )
    capability_blocked = (
        operation.status == "blocked"
        and operation.blocked_reason
        == "operation_capability_unsupported"
    )
    if expected_class is None or capability_blocked:
        if operation.approval_id is not None or rows:
            raise RuntimeError(
                "malformed persisted project operation approval link"
            )
        return None
    if operation.approval_id is None or len(rows) != 1:
        raise RuntimeError(
            "malformed persisted project operation approval link"
        )
    row = rows[0]
    try:
        targets = _decode_operation_json(
            row["targets_json"], require_object=False
        )
        if not _stored_text(row["batch_boundary_json"]):
            raise ValueError("approval boundary must be text")
        boundary = json.loads(
            row["batch_boundary_json"],
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(value)
            ),
        )
        fingerprint = _decode_operation_json(
            operation.approval_fingerprint_json,
            require_object=True,
        )
        valid_targets = (
            type(targets) is list
            and bool(targets)
            and all(type(value) is str and value for value in targets)
            and canonicalize_targets(tuple(targets)) == tuple(targets)
        )
        valid_boundary = (
            type(boundary) is dict
            and set(boundary)
            == {
                "authorization_actor_id",
                "batch_id",
                "batch_items",
                "canonical_action",
                "expected_lifecycle",
                "expected_phase",
                "expected_runtime_version",
            }
            and _stored_text(boundary["authorization_actor_id"])
            and _stored_text(boundary["batch_id"])
            and type(boundary["batch_items"]) is list
            and bool(boundary["batch_items"])
            and all(
                type(value) is str and value
                for value in boundary["batch_items"]
            )
            and len(set(boundary["batch_items"]))
            == len(boundary["batch_items"])
            and _stored_text(boundary["canonical_action"])
            and boundary["expected_lifecycle"]
            in {"active", "awaiting_acceptance", "completed"}
            and _stored_text(boundary["expected_phase"])
            and _stored_int(boundary["expected_runtime_version"])
            and row["batch_boundary_json"]
            == _canonical_boundary_json(
                authorization_actor_id=(
                    boundary["authorization_actor_id"]
                ),
                canonical_action=boundary["canonical_action"],
                batch_id=boundary["batch_id"],
                batch_items=tuple(boundary["batch_items"]),
                expected_runtime_version=(
                    boundary["expected_runtime_version"]
                ),
                expected_lifecycle=boundary["expected_lifecycle"],
                expected_phase=boundary["expected_phase"],
            )
        )
        valid_fingerprint = (
            type(fingerprint) is dict
            and fingerprint["approval_class"] == expected_class
            and fingerprint["approval_id"] == operation.approval_id
            and fingerprint["authorization_actor_id"]
            == row["authorization_actor_id"]
            and fingerprint["expires_at"] == row["expires_at"]
            and fingerprint["requires_owner"] is True
        )
        approval_status = row["status"]
        resolved_at = row["resolved_at"]
        resolved_by_actor_id = row["resolved_by_actor_id"]
        consumed_at = row["consumed_at"]
        valid_resolution = (
            (
                approval_status == "pending"
                and resolved_at is None
                and resolved_by_actor_id is None
                and consumed_at is None
            )
            or (
                approval_status == "approved"
                and _stored_int(resolved_at)
                and _stored_text(resolved_by_actor_id)
                and _stored_int(consumed_at)
                and consumed_at >= resolved_at
            )
            or (
                approval_status == "denied"
                and _stored_int(resolved_at)
                and _stored_text(resolved_by_actor_id)
                and consumed_at is None
            )
            or (
                approval_status == "expired"
                and consumed_at is None
                and (
                    (
                        resolved_at is None
                        and resolved_by_actor_id is None
                    )
                    or (
                        _stored_int(resolved_at)
                        and _stored_text(resolved_by_actor_id)
                    )
                )
            )
        )
        approval_status_for_operation = {
            "awaiting_approval": {"pending"},
            "approved": {"approved"},
            "effect_started": {"approved"},
            "receipt_recorded": {"approved"},
            "unknown": {"approved"},
            "reconciled": {"approved"},
            "blocked": (
                {"denied"}
                if operation.blocked_reason == "approval_denied"
                else (
                    {"expired"}
                    if operation.blocked_reason
                    in {
                        "approval_time_expired",
                        "approval_stale_boundary",
                    }
                    else {"approved"}
                )
            ),
        }
        valid = (
            valid_targets
            and valid_boundary
            and valid_fingerprint
            and valid_resolution
            and row["approval_id"] == operation.approval_id
            and row["project_id"] == operation.project_id
            and row["turn_id"] == operation.turn_id
            and row["operation_id"] == operation.operation_id
            and _stored_int(
                row["operation_maintenance_seq"], minimum=1
            )
            and _stored_text(row["actor_id"])
            and row["actor_id"] == row["authorization_actor_id"]
            and _stored_text(row["authorization_actor_id"])
            and row["canonical_action"] == operation.canonical_action
            and row["approval_class"] == expected_class
            and _stored_int(row["command_revision"], minimum=1)
            and row["command_revision"] == operation.command_revision
            and _stored_int(row["expected_runtime_version"])
            and _stored_int(row["effective_runtime_version"])
            and _stored_int(
                row["turn_expected_control_version"]
            )
            and row["expected_lifecycle"]
            in {"active", "awaiting_acceptance", "completed"}
            and _stored_text(row["expected_phase"])
            and row["targets_json"] == operation.targets_json
            and boundary["authorization_actor_id"]
            == row["authorization_actor_id"]
            and boundary["batch_id"] == operation.operation_id
            and json.dumps(
                boundary["batch_items"],
                separators=(",", ":"),
                ensure_ascii=False,
            )
            == operation.batch_items_json
            and boundary["canonical_action"]
            == operation.canonical_action
            and boundary["expected_runtime_version"]
            == row["expected_runtime_version"]
            and boundary["expected_lifecycle"]
            == row["expected_lifecycle"]
            and boundary["expected_phase"] == row["expected_phase"]
            and row["status"]
            in approval_status_for_operation[operation.status]
            and _stored_int(row["expires_at"])
            and _stored_int(row["created_at"])
            and row["expires_at"] > row["created_at"]
            and (
                resolved_at is None
                or resolved_at >= row["created_at"]
            )
        )
    except (
        IndexError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise RuntimeError(
            "malformed persisted project operation approval link"
        ) from exc
    if not valid:
        raise RuntimeError(
            "malformed persisted project operation approval link"
        )
    return row


def _certify_project_operation_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
) -> ProjectOperationRecord:
    """Certify one marker-zero snapshot with one marker-only tuple CAS."""
    try:
        operation = _project_operation_from_row(
            row, expected_guard_validated=0
        )
        approval = _operation_approval_certification_row(
            conn, operation
        )
    except (
        LegacyOperationUnmanagedError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as exc:
        raise _InvalidOperationCertification(
            "operation is not canonically certifiable"
        ) from exc
    operation_predicate = " AND ".join(
        f"{column} IS ?" for column in _OPERATION_CERTIFICATION_COLUMNS
    )
    parameters: list[object] = [
        operation.project_id,
        operation.operation_id,
        *(row[column] for column in _OPERATION_CERTIFICATION_COLUMNS),
    ]
    if approval is None:
        approval_predicate = """
          AND NOT EXISTS (
              SELECT 1
              FROM project_approvals AS approval
              WHERE approval.project_id = project_operations.project_id
                AND approval.operation_id
                    = project_operations.operation_id
              LIMIT 1
          )
        """
    else:
        approval_columns = " AND ".join(
            f"approval.{column} IS ?"
            for column in _APPROVAL_CERTIFICATION_COLUMNS
        )
        approval_predicate = f"""
          AND EXISTS (
              SELECT 1
              FROM project_approvals AS approval
              WHERE {approval_columns}
              LIMIT 1
          )
        """
        parameters.extend(
            approval[column]
            for column in _APPROVAL_CERTIFICATION_COLUMNS
        )
    cursor = conn.execute(
        f"""
        UPDATE project_operations
        SET guard_validated = 1
        WHERE project_id = ? AND operation_id = ?
          AND guard_validated = 0
          AND {operation_predicate}
          {approval_predicate}
        """,
        parameters,
    )
    if cursor.rowcount != 1:
        raise RuntimeError(
            "project operation changed during certification"
        )
    certified = conn.execute(
        """
        SELECT *
        FROM project_operations
        WHERE project_id = ? AND operation_id = ?
        """,
        (operation.project_id, operation.operation_id),
    ).fetchone()
    if certified is None:
        raise RuntimeError(
            "project operation disappeared during certification"
        )
    result = project_operation_from_row(certified)
    _operation_approval_certification_row(conn, result)
    return result


def _certify_project_operation(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    operation_id: str,
) -> ProjectOperationRecord:
    row = conn.execute(
        """
        SELECT *
        FROM project_operations
        WHERE project_id = ? AND operation_id = ?
          AND guard_revision = 1 AND guard_validated = 0
        """,
        (project_id, operation_id),
    ).fetchone()
    if row is None:
        raise RuntimeError("project operation is not certifiable")
    return _certify_project_operation_row(conn, row)


def _decertify_project_operation(
    conn: sqlite3.Connection,
    operation: ProjectOperationRecord,
) -> ProjectOperationRecord:
    """CAS one exact certified snapshot to marker zero only."""
    row = conn.execute(
        """
        SELECT *
        FROM project_operations
        WHERE project_id = ? AND operation_id = ?
          AND guard_revision = 1 AND guard_validated = 1
        """,
        (operation.project_id, operation.operation_id),
    ).fetchone()
    if row is None:
        raise RuntimeError("project operation is not certified")
    stored = project_operation_from_row(row)
    _operation_approval_certification_row(conn, stored)
    if stored != operation:
        raise RuntimeError(
            "project operation changed before decertification"
        )
    operation_predicate = " AND ".join(
        f"{column} IS ?" for column in _OPERATION_CERTIFICATION_COLUMNS
    )
    cursor = conn.execute(
        f"""
        UPDATE project_operations
        SET guard_validated = 0
        WHERE project_id = ? AND operation_id = ?
          AND guard_validated = 1
          AND {operation_predicate}
        """,
        (
            operation.project_id,
            operation.operation_id,
            *(
                row[column]
                for column in _OPERATION_CERTIFICATION_COLUMNS
            ),
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeError(
            "project operation changed during decertification"
        )
    return operation


def _project_operation_for_id(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    operation_id: str,
) -> ProjectOperationRecord | None:
    """Read one managed operation and verify its one-to-one approval link."""
    row = conn.execute(
        """
        SELECT * FROM project_operations
        WHERE project_id = ? AND operation_id = ?
        """,
        (project_id, operation_id),
    ).fetchone()
    if row is None:
        return None
    operation = project_operation_from_row(row)
    _operation_approval_certification_row(conn, operation)
    return operation


def _project_operation_for_idempotency_key(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    idempotency_key: str,
) -> ProjectOperationRecord | None:
    row = conn.execute(
        """
        SELECT operation_id FROM project_operations
        WHERE project_id = ? AND idempotency_key = ?
        """,
        (project_id, idempotency_key),
    ).fetchone()
    if row is None:
        return None
    return _project_operation_for_id(
        conn,
        project_id=project_id,
        operation_id=row["operation_id"],
    )


_PRE_EFFECT_BLOCK_REASONS = frozenset(
    {
        "operation_capability_unsupported",
        "approval_denied",
        "approval_time_expired",
        "approval_stale_boundary",
    }
)

_OPERATION_PENDING_STATUSES = frozenset(
    {"approved", "effect_started", "receipt_recorded", "unknown"}
)


def _operation_pending_for_turn(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
) -> ProjectOperationRecord | None:
    """Return the sole unresolved operation for one exact derived lane."""
    turn = _runtime_turn_for_project(
        conn, project_id=project_id, turn_id=turn_id
    )
    if not (
        turn is not None
        and turn.status == "reconciling"
        and turn.recovery_block_key is None
        and _current_worker_lease_for_turn(
            conn, project_id=project_id, turn_id=turn_id
        )
        is None
    ):
        return None
    if conn.execute(
        _OPERATION_TURN_UNSAFE_SQL, (project_id, turn_id)
    ).fetchone() is not None:
        return None
    unresolved = conn.execute(
        _OPERATION_TURN_UNRESOLVED_SQL,
        (project_id, turn_id, 2),
    ).fetchall()
    if len(unresolved) != 1:
        return None
    try:
        return _project_operation_for_id(
            conn,
            project_id=project_id,
            operation_id=unresolved[0]["operation_id"],
        )
    except (LegacyOperationUnmanagedError, RuntimeError):
        return None


def _turn_allows_new_unresolved_operation(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
) -> bool:
    """Allow history, but reject a second or unsafe unresolved authority."""
    unsafe = conn.execute(
        _OPERATION_TURN_UNSAFE_SQL, (project_id, turn_id)
    ).fetchone()
    if unsafe is not None:
        return False
    unresolved = conn.execute(
        _OPERATION_TURN_UNRESOLVED_SQL,
        (project_id, turn_id, 1),
    ).fetchone()
    return unresolved is None


def _operation_pending_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int,
) -> tuple[ProjectOperationRecord, ...]:
    """Merge four status-indexed branches into one bounded global order."""
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("invalid operation recovery limit")
    selected: dict[
        tuple[str, str], ProjectOperationRecord
    ] = {}
    for status in (
        "approved",
        "effect_started",
        "receipt_recorded",
        "unknown",
    ):
        rows = conn.execute(
            _OPERATION_PENDING_BRANCH_SQL, (status, limit)
        ).fetchall()
        for row in rows:
            try:
                operation = _project_operation_for_id(
                    conn,
                    project_id=row["project_id"],
                    operation_id=row["operation_id"],
                )
            except (LegacyOperationUnmanagedError, RuntimeError):
                operation = None
            if not (
                operation is not None
                and operation.turn_id == row["turn_id"]
                and operation.operation_id == row["operation_id"]
                and operation.status == status
            ):
                continue
            selected.setdefault(
                (operation.project_id, operation.operation_id),
                operation,
            )
    return tuple(
        sorted(
            selected.values(),
            key=lambda operation: (
                operation.updated_at,
                operation.project_id,
                operation.operation_id,
                operation.turn_id,
            ),
        )[:limit]
    )


def _validated_operation_maintenance_state(
    conn: sqlite3.Connection,
) -> sqlite3.Row:
    rows = conn.execute(
        """
        SELECT singleton, approval_scan_after, next_lane,
               operation_validation_migration_complete,
               approval_scan_after_seq,
               approval_scan_high_water_seq,
               approval_scan_epoch,
               next_operation_approval_seq
        FROM project_operation_maintenance
        """
    ).fetchall()
    if len(rows) != 1:
        raise RuntimeError("invalid operation maintenance state")
    state = rows[0]
    if not (
        type(state["singleton"]) is int
        and state["singleton"] == 1
        and type(state["approval_scan_after"]) is str
        and type(state["next_lane"]) is int
        and state["next_lane"] in {0, 1}
        and type(
            state["operation_validation_migration_complete"]
        )
        is int
        and state["operation_validation_migration_complete"] in {0, 1}
        and _stored_int(state["approval_scan_after_seq"])
        and _stored_int(state["approval_scan_high_water_seq"])
        and _stored_int(state["approval_scan_epoch"])
        and _stored_int(
            state["next_operation_approval_seq"], minimum=1
        )
    ):
        raise RuntimeError("invalid operation maintenance state")
    return state


def _select_operation_approval_maintenance(
    conn: sqlite3.Connection,
    *,
    now: int,
    limit: int,
) -> tuple[str, ...]:
    """Choose one durable alternating lane inside caller write scope."""
    state = _validated_operation_maintenance_state(conn)
    lane = state["next_lane"]
    after_sequence = state["approval_scan_after_seq"]
    high_water = state["approval_scan_high_water_seq"]
    epoch = state["approval_scan_epoch"]
    if lane == 0:
        rows = conn.execute(
            _APPROVAL_DUE_MAINTENANCE_SQL, (now, limit)
        ).fetchall()
        next_after_sequence = after_sequence
        next_high_water = high_water
        next_epoch = epoch
    else:
        if high_water == 0:
            if after_sequence != 0:
                raise RuntimeError(
                    "invalid operation maintenance epoch"
                )
            captured = conn.execute(
                _APPROVAL_STALE_HIGH_WATER_SQL
            ).fetchone()
            high_water = (
                captured["operation_maintenance_seq"]
                if captured is not None
                else 0
            )
            after_sequence = 0
        if not _stored_int(high_water):
            raise RuntimeError("invalid operation maintenance epoch")
        rows = (
            conn.execute(
                _APPROVAL_STALE_MAINTENANCE_SQL,
                (after_sequence, high_water, limit),
            ).fetchall()
            if high_water > 0
            else []
        )
        last_sequence = (
            rows[-1]["operation_maintenance_seq"]
            if rows
            else after_sequence
        )
        if not _stored_int(last_sequence):
            raise RuntimeError("invalid operation maintenance epoch")
        complete = len(rows) < limit
        if not complete:
            complete = (
                conn.execute(
                    _APPROVAL_STALE_REMAINING_SQL,
                    (last_sequence, high_water),
                ).fetchone()
                is None
            )
        if complete:
            if epoch >= _SQLITE_INT_MAX:
                raise RuntimeError(
                    "operation maintenance epoch exhausted"
                )
            next_after_sequence = 0
            next_high_water = 0
            next_epoch = epoch + 1
        else:
            next_after_sequence = last_sequence
            next_high_water = high_water
            next_epoch = epoch
    updated = conn.execute(
        """
        UPDATE project_operation_maintenance
        SET approval_scan_after_seq = ?,
            approval_scan_high_water_seq = ?,
            approval_scan_epoch = ?,
            next_lane = ?
        WHERE singleton IS ?
          AND approval_scan_after IS ?
          AND next_lane IS ?
          AND operation_validation_migration_complete IS ?
          AND approval_scan_after_seq IS ?
          AND approval_scan_high_water_seq IS ?
          AND approval_scan_epoch IS ?
          AND next_operation_approval_seq IS ?
        """,
        (
            next_after_sequence,
            next_high_water,
            next_epoch,
            1 - lane,
            state["singleton"],
            state["approval_scan_after"],
            state["next_lane"],
            state["operation_validation_migration_complete"],
            state["approval_scan_after_seq"],
            state["approval_scan_high_water_seq"],
            state["approval_scan_epoch"],
            state["next_operation_approval_seq"],
        ),
    )
    if updated.rowcount != 1:
        return ()
    return tuple(row["approval_id"] for row in rows)


def _project_operation_disposition_for_turn(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
) -> Literal[
    "clear",
    "reconciled",
    "unresolved",
    "pre_effect_blocked",
    "post_effect_blocked",
]:
    """Classify every operation on one turn, failing closed on bad data."""
    rows = conn.execute(
        """
        SELECT operation_id
        FROM project_operations
        WHERE project_id = ? AND turn_id = ?
        ORDER BY operation_id
        """,
        (project_id, turn_id),
    ).fetchall()
    if not rows:
        return "clear"
    dispositions: set[str] = set()
    for row in rows:
        try:
            operation = _project_operation_for_id(
                conn,
                project_id=project_id,
                operation_id=row["operation_id"],
            )
        except (LegacyOperationUnmanagedError, RuntimeError):
            return "post_effect_blocked"
        if operation is None:
            return "post_effect_blocked"
        if operation.status == "reconciled":
            dispositions.add("reconciled")
        elif operation.status == "blocked":
            dispositions.add(
                "pre_effect_blocked"
                if operation.blocked_reason
                in _PRE_EFFECT_BLOCK_REASONS
                else "post_effect_blocked"
            )
        else:
            dispositions.add("unresolved")
    if "post_effect_blocked" in dispositions:
        return "post_effect_blocked"
    if "unresolved" in dispositions:
        return "unresolved"
    if "pre_effect_blocked" in dispositions:
        return "pre_effect_blocked"
    return "reconciled"


def _insert_project_operation(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    project_id: str,
    turn_id: str,
    idempotency_key: str,
    command_revision: int,
    targets_json: str,
    payload_json: str,
    status: str,
    canonical_action: str,
    batch_items_json: str,
    readback_kind: str | None,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
    blocked_reason: str | None,
    remote_idempotency_supported: bool,
    approval_fingerprint_json: str | None,
    now: int,
) -> bool:
    cursor = conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key, approval_id,
            command_revision, targets_json, payload_json, status, receipt_json,
            created_at, updated_at, guard_revision, canonical_action,
            batch_items_json, readback_kind, attempt_id, lease_generation,
            fencing_token, receipt_id, readback_json, blocked_reason,
            remote_idempotency_supported, approval_fingerprint_json,
            guard_validated
        ) VALUES (
            ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?, ?, 1, ?, ?, ?, ?, ?, ?,
            NULL, NULL, ?, ?, ?, 0
        )
        ON CONFLICT DO NOTHING
        """,
        (
            operation_id,
            project_id,
            turn_id,
            idempotency_key,
            command_revision,
            targets_json,
            payload_json,
            status,
            now,
            now,
            canonical_action,
            batch_items_json,
            readback_kind,
            attempt_id,
            lease_generation,
            fencing_token,
            blocked_reason,
            int(remote_idempotency_supported),
            approval_fingerprint_json,
        ),
    )
    return cursor.rowcount == 1


def _link_project_operation_approval(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    operation_id: str,
    approval_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
    now: int,
) -> bool:
    maintenance = _validated_operation_maintenance_state(conn)
    operation_sequence = maintenance[
        "next_operation_approval_seq"
    ]
    if operation_sequence >= _SQLITE_INT_MAX:
        raise RuntimeError(
            "operation approval maintenance sequence exhausted"
        )
    if conn.execute(
        """
        UPDATE project_operation_maintenance
        SET next_operation_approval_seq = ?
        WHERE singleton IS ?
          AND approval_scan_after IS ?
          AND next_lane IS ?
          AND operation_validation_migration_complete IS ?
          AND approval_scan_after_seq IS ?
          AND approval_scan_high_water_seq IS ?
          AND approval_scan_epoch IS ?
          AND next_operation_approval_seq IS ?
        """,
        (
            operation_sequence + 1,
            maintenance["singleton"],
            maintenance["approval_scan_after"],
            maintenance["next_lane"],
            maintenance[
                "operation_validation_migration_complete"
            ],
            maintenance["approval_scan_after_seq"],
            maintenance["approval_scan_high_water_seq"],
            maintenance["approval_scan_epoch"],
            operation_sequence,
        ),
    ).rowcount != 1:
        raise RuntimeError(
            "operation approval maintenance sequence changed"
        )
    approval = conn.execute(
        """
        UPDATE project_approvals
        SET operation_id = ?, operation_maintenance_seq = ?
        WHERE project_id = ? AND approval_id = ?
          AND turn_id = ? AND operation_id IS NULL
          AND operation_maintenance_seq IS NULL
          AND status = 'pending' AND consumed_at IS NULL
        """,
        (
            operation_id,
            operation_sequence,
            project_id,
            approval_id,
            turn_id,
        ),
    )
    if approval.rowcount != 1:
        return False
    operation = conn.execute(
        """
        UPDATE project_operations
        SET approval_id = ?, status = 'awaiting_approval', updated_at = ?
        WHERE project_id = ? AND operation_id = ? AND turn_id = ?
          AND guard_validated = 0
          AND approval_id IS NULL AND status = 'approved'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
        """,
        (
            approval_id,
            now,
            project_id,
            operation_id,
            turn_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    return operation.rowcount == 1


def _approve_project_operation_approval(
    conn: sqlite3.Connection,
    *,
    approval_id: str,
    project_id: str,
    operation_id: str,
    turn_id: str,
    authorization_actor_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
    expected_control_version: int,
    release_live_turn: bool,
    now: int,
) -> bool | None:
    """Consume one operation approval and optionally release its live waiter."""
    approval = conn.execute(
        """
        UPDATE project_approvals
        SET status = 'approved', resolved_at = ?,
            resolved_by_actor_id = ?, consumed_at = ?
        WHERE project_id = ? AND approval_id = ? AND operation_id = ?
          AND turn_id = ? AND authorization_actor_id = ?
          AND status = 'pending' AND resolved_at IS NULL
          AND resolved_by_actor_id IS NULL AND consumed_at IS NULL
        """,
        (
            now,
            authorization_actor_id,
            now,
            project_id,
            approval_id,
            operation_id,
            turn_id,
            authorization_actor_id,
        ),
    )
    if approval.rowcount != 1:
        return None
    operation = conn.execute(
        """
        UPDATE project_operations
        SET status = 'approved', updated_at = ?
        WHERE project_id = ? AND operation_id = ? AND turn_id = ?
          AND guard_validated = 0
          AND approval_id = ? AND status = 'awaiting_approval'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
        """,
        (
            now,
            project_id,
            operation_id,
            turn_id,
            approval_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    if operation.rowcount != 1:
        raise RuntimeError(
            "operation changed during approval consumption"
        )
    if not release_live_turn:
        return False
    turn = conn.execute(
        """
        UPDATE project_turns
        SET status = 'claimed', updated_at = ?
        WHERE project_id = ? AND turn_id = ?
          AND status = 'awaiting_approval'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
          AND execution_state IN ('not_started', 'started')
        """,
        (
            now,
            project_id,
            turn_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    if turn.rowcount != 1:
        raise RuntimeError("turn changed during approval release")
    control = conn.execute(
        """
        SELECT 1 FROM project_run_controls
        WHERE project_id = ? AND turn_id = ? AND control_state = 'running'
          AND control_version = ? AND attempt_id = ?
        """,
        (
            project_id,
            turn_id,
            expected_control_version,
            attempt_id,
        ),
    ).fetchone()
    if control is None:
        raise RuntimeError("control changed during approval release")
    return True


def _finalize_project_operation_approval_policy(
    conn: sqlite3.Connection,
    *,
    approval_id: str,
    approval_status: Literal["denied", "expired"],
    resolved_by_actor_id: str | None,
    project_id: str,
    operation_id: str,
    turn_id: str,
    blocked_reason: str,
    terminal_result_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
    control_version: int,
    worker_id: str,
    lease_expires_at: int,
    canonical_session_id: str,
    now: int,
) -> bool:
    """Fail one denied/expired operation turn and fence its exact waiter."""
    if approval_status == "denied":
        approval = conn.execute(
            """
            UPDATE project_approvals
            SET status = 'denied', resolved_at = ?,
                resolved_by_actor_id = ?, consumed_at = NULL
            WHERE project_id = ? AND approval_id = ? AND operation_id = ?
              AND turn_id = ? AND status = 'pending'
              AND resolved_at IS NULL AND resolved_by_actor_id IS NULL
              AND consumed_at IS NULL
            """,
            (
                now,
                resolved_by_actor_id,
                project_id,
                approval_id,
                operation_id,
                turn_id,
            ),
        )
    else:
        approval = conn.execute(
            """
            UPDATE project_approvals
            SET status = 'expired', resolved_at = NULL,
                resolved_by_actor_id = NULL, consumed_at = NULL
            WHERE project_id = ? AND approval_id = ? AND operation_id = ?
              AND turn_id = ? AND status = 'pending'
              AND resolved_at IS NULL AND resolved_by_actor_id IS NULL
              AND consumed_at IS NULL
            """,
            (
                project_id,
                approval_id,
                operation_id,
                turn_id,
            ),
        )
    if approval.rowcount != 1:
        return False
    operation = conn.execute(
        """
        UPDATE project_operations
        SET status = 'blocked', blocked_reason = ?, updated_at = ?
        WHERE project_id = ? AND operation_id = ? AND turn_id = ?
          AND guard_validated = 0
          AND approval_id = ? AND status = 'awaiting_approval'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
        """,
        (
            blocked_reason,
            now,
            project_id,
            operation_id,
            turn_id,
            approval_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    if operation.rowcount != 1:
        raise RuntimeError(
            "operation changed during approval policy finalization"
        )
    turn = conn.execute(
        """
        UPDATE project_turns
        SET status = 'failed',
            execution_state = CASE
                WHEN execution_state = 'not_started' THEN NULL
                ELSE execution_state
            END,
            terminal_result_id = ?, updated_at = ?
        WHERE project_id = ? AND turn_id = ?
          AND status = 'awaiting_approval'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
          AND execution_state IN ('not_started', 'started')
          AND terminal_result_id IS NULL
        """,
        (
            terminal_result_id,
            now,
            project_id,
            turn_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    if turn.rowcount != 1:
        raise RuntimeError(
            "turn changed during approval policy finalization"
        )
    control = conn.execute(
        """
        UPDATE project_run_controls
        SET control_state = 'terminal',
            control_version = control_version + 1, updated_at = ?
        WHERE project_id = ? AND turn_id = ?
          AND control_state = 'running' AND control_version = ?
          AND attempt_id = ? AND claim_worker_id = ?
          AND claim_lease_expires_at = ?
          AND claim_canonical_session_id = ?
        """,
        (
            now,
            project_id,
            turn_id,
            control_version,
            attempt_id,
            worker_id,
            lease_expires_at,
            canonical_session_id,
        ),
    )
    if control.rowcount != 1:
        raise RuntimeError(
            "control changed during approval policy finalization"
        )
    if not _delete_current_worker_lease(
        conn,
        project_id=project_id,
        turn_id=turn_id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_generation=lease_generation,
        fencing_token=fencing_token,
        lease_expires_at=lease_expires_at,
    ):
        raise RuntimeError(
            "lease changed during approval policy finalization"
        )
    return True


def _rehydrate_project_operation_claim(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    operation_id: str,
    turn_id: str,
    source_turn_status: Literal["awaiting_approval", "reconciling"],
    old_attempt_id: str,
    old_lease_generation: int,
    old_fencing_token: int,
    old_control_version: int,
    old_worker_id: str,
    old_lease_expires_at: int,
    old_canonical_session_id: str,
    old_lease_present: bool,
    new_attempt_id: str,
    new_worker_id: str,
    new_lease_generation: int,
    new_fencing_token: int,
    new_lease_expires_at: int,
    canonical_session_id: str,
    now: int,
) -> tuple[RuntimeTurnRecord, WorkerLeaseRecord]:
    """Rotate one safe approved operation onto a fresh Task-5 fence."""
    if old_lease_present:
        if not _delete_current_worker_lease(
            conn,
            project_id=project_id,
            turn_id=turn_id,
            attempt_id=old_attempt_id,
            worker_id=old_worker_id,
            lease_generation=old_lease_generation,
            fencing_token=old_fencing_token,
            lease_expires_at=old_lease_expires_at,
        ):
            raise RuntimeError(
                "operation rehydration lost its old lease"
            )
    turn = conn.execute(
        """
        UPDATE project_turns
        SET status = 'claimed', attempt_id = ?, lease_generation = ?,
            fencing_token = ?, execution_state = 'not_started',
            terminal_result_id = NULL, recovery_block_key = NULL,
            updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND status = ?
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
          AND terminal_result_id IS NULL
          AND (
                status != 'reconciling'
                OR recovery_block_key IS NULL
          )
        """,
        (
            new_attempt_id,
            new_lease_generation,
            new_fencing_token,
            now,
            project_id,
            turn_id,
            source_turn_status,
            old_attempt_id,
            old_lease_generation,
            old_fencing_token,
        ),
    )
    if turn.rowcount != 1:
        raise RuntimeError("turn changed during operation rehydration")
    control = conn.execute(
        """
        UPDATE project_run_controls
        SET control_state = 'running',
            control_version = control_version + 1,
            attempt_id = ?, claim_worker_id = ?,
            claim_lease_expires_at = ?,
            claim_canonical_session_id = ?, updated_at = ?
        WHERE project_id = ? AND turn_id = ?
          AND control_state = 'running' AND control_version = ?
          AND attempt_id = ? AND claim_worker_id = ?
          AND claim_lease_expires_at = ?
          AND claim_canonical_session_id = ?
        """,
        (
            new_attempt_id,
            new_worker_id,
            new_lease_expires_at,
            canonical_session_id,
            now,
            project_id,
            turn_id,
            old_control_version,
            old_attempt_id,
            old_worker_id,
            old_lease_expires_at,
            old_canonical_session_id,
        ),
    )
    if control.rowcount != 1:
        raise RuntimeError(
            "control changed during operation rehydration"
        )
    operation = conn.execute(
        """
        UPDATE project_operations
        SET attempt_id = ?, lease_generation = ?, fencing_token = ?,
            readback_json = NULL, updated_at = ?
        WHERE project_id = ? AND operation_id = ? AND turn_id = ?
          AND guard_validated = 0
          AND status = 'approved'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
        """,
        (
            new_attempt_id,
            new_lease_generation,
            new_fencing_token,
            now,
            project_id,
            operation_id,
            turn_id,
            old_attempt_id,
            old_lease_generation,
            old_fencing_token,
        ),
    )
    if operation.rowcount != 1:
        raise RuntimeError(
            "operation changed during fenced rehydration"
        )
    conn.execute(
        """
        INSERT INTO project_worker_leases (
            lease_id, project_id, turn_id, worker_id, lease_generation,
            fencing_token, expires_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_attempt_id,
            project_id,
            turn_id,
            new_worker_id,
            new_lease_generation,
            new_fencing_token,
            new_lease_expires_at,
            now,
        ),
    )
    stored_turn = _runtime_turn_for_project(
        conn, project_id=project_id, turn_id=turn_id
    )
    stored_lease = _current_worker_lease_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    )
    if stored_turn is None or stored_lease is None:
        raise RuntimeError("rehydrated claim disappeared")
    return stored_turn, stored_lease


def _mark_project_operation_started(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    operation_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
    now: int,
) -> bool:
    cursor = conn.execute(
        """
        UPDATE project_operations
        SET status = 'effect_started', updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND operation_id = ?
          AND guard_validated = 0
          AND status = 'approved'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
        """,
        (
            now,
            project_id,
            turn_id,
            operation_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    return cursor.rowcount == 1


def _record_project_operation_receipt(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    operation_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
    receipt_id: str,
    receipt_json: str,
    now: int,
) -> bool:
    cursor = conn.execute(
        """
        UPDATE project_operations
        SET status = 'receipt_recorded', receipt_id = ?,
            receipt_json = ?, updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND operation_id = ?
          AND guard_validated = 0
          AND status = 'effect_started'
          AND receipt_id IS NULL AND receipt_json IS NULL
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
        """,
        (
            receipt_id,
            receipt_json,
            now,
            project_id,
            turn_id,
            operation_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    return cursor.rowcount == 1


def _project_operation_receipt_owner(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    receipt_id: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT operation_id
        FROM project_operations
        WHERE project_id = ? AND receipt_id = ?
          AND guard_revision = 1
        """,
        (project_id, receipt_id),
    ).fetchone()
    return row["operation_id"] if row is not None else None


def _park_project_operation_unknown(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    operation_id: str,
    source_status: Literal["effect_started", "receipt_recorded"],
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
    now: int,
) -> bool:
    cursor = conn.execute(
        """
        UPDATE project_operations
        SET status = 'unknown', readback_json = NULL,
            blocked_reason = NULL, updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND operation_id = ?
          AND guard_validated = 0
          AND status = ?
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
        """,
        (
            now,
            project_id,
            turn_id,
            operation_id,
            source_status,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    return cursor.rowcount == 1


def _finalize_project_operation_readback(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    operation_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
    target_status: Literal["approved", "reconciled", "blocked"],
    receipt_id: str | None,
    receipt_json: str | None,
    readback_json: str | None,
    blocked_reason: str | None,
    now: int,
) -> bool:
    cursor = conn.execute(
        """
        UPDATE project_operations
        SET status = ?, receipt_id = ?, receipt_json = ?,
            readback_json = ?, blocked_reason = ?, updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND operation_id = ?
          AND guard_validated = 0
          AND status = 'unknown'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
        """,
        (
            target_status,
            receipt_id,
            receipt_json,
            readback_json,
            blocked_reason,
            now,
            project_id,
            turn_id,
            operation_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    return cursor.rowcount == 1


def _runtime_control_for_turn(
    conn: sqlite3.Connection, *, project_id: str, turn_id: str
) -> RuntimeControlRecord | None:
    row = conn.execute(
        """
        SELECT * FROM project_run_controls
        WHERE project_id = ? AND turn_id = ?
        """,
        (project_id, turn_id),
    ).fetchone()
    return runtime_control_from_row(row) if row is not None else None


def _queued_turns_for_project(
    conn: sqlite3.Connection, *, project_id: str
) -> tuple[RuntimeTurnRecord, ...]:
    rows = conn.execute(
        """
        SELECT * FROM project_turns
        WHERE project_id = ? AND status = 'queued'
        ORDER BY sequence, turn_id
        """,
        (project_id,),
    ).fetchall()
    return tuple(runtime_turn_from_row(row) for row in rows)


def _current_worker_lease_for_turn(
    conn: sqlite3.Connection, *, project_id: str, turn_id: str
) -> WorkerLeaseRecord | None:
    row = conn.execute(
        """
        SELECT * FROM project_worker_leases
        WHERE project_id = ? AND turn_id = ?
        """,
        (project_id, turn_id),
    ).fetchone()
    return worker_lease_from_row(row) if row is not None else None


def _allocate_project_sequence(
    conn: sqlite3.Connection, *, table: Literal["turns", "events"], project_id: str
) -> int:
    """Allocate one per-project sequence while the caller owns a write transaction."""
    table_name = {
        "turns": "project_turns",
        "events": "project_events",
    }.get(table)
    if table_name is None:
        raise ValueError("unsupported runtime sequence table")
    return conn.execute(
        f"SELECT COALESCE(MAX(sequence), 0) + 1 FROM {table_name} WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0]


def _append_runtime_event(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    project_id: str,
    kind: str,
    turn_id: str | None,
    payload_json: str,
    created_at: int,
) -> int:
    """Append one canonical event under the caller's existing write boundary."""
    sequence = _allocate_project_sequence(conn, table="events", project_id=project_id)
    conn.execute(
        """
        INSERT INTO project_events (
            event_id, project_id, sequence, kind, turn_id, payload_json,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (event_id, project_id, sequence, kind, turn_id, payload_json, created_at),
    )
    return sequence


def _advance_runtime_version(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    expected_version: int,
    updated_at: int,
) -> RuntimeState | None:
    """CAS the project-wide token without starting or committing a transaction."""
    cursor = conn.execute(
        """
        UPDATE project_runtime_state
        SET version = version + 1, updated_at = ?
        WHERE project_id = ? AND version = ?
        """,
        (updated_at, project_id, expected_version),
    )
    if cursor.rowcount != 1:
        return None
    return _runtime_state_for_project(conn, project_id)


def _insert_queued_runtime_turn(
    conn: sqlite3.Connection,
    *,
    turn_id: str,
    project_id: str,
    sequence: int,
    idempotency_key: str,
    payload_json: str,
    origin_binding_id: str,
    now: int,
) -> RuntimeTurnRecord:
    """Create the inseparable initial queued turn and its control row."""
    conn.execute(
        """
        INSERT INTO project_turns (
            turn_id, project_id, sequence, idempotency_key, payload_json,
            origin_binding_id, status, attempt_id, lease_generation,
            fencing_token, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'queued', NULL, 0, 0, ?, ?)
        """,
        (turn_id, project_id, sequence, idempotency_key, payload_json, origin_binding_id, now, now),
    )
    conn.execute(
        """
        INSERT INTO project_run_controls (
            turn_id, project_id, control_state, control_version,
            idempotency_key, command_fingerprint, attempt_id, updated_at
        ) VALUES (?, ?, 'running', 0, NULL, NULL, NULL, ?)
        """,
        (turn_id, project_id, now),
    )
    result = _runtime_turn_for_project(conn, project_id=project_id, turn_id=turn_id)
    assert result is not None
    return result


def _validate_runtime_turn_pair(
    conn: sqlite3.Connection,
    *,
    turn: RuntimeTurnRecord,
) -> RuntimeControlRecord:
    """Fail closed unless one persisted turn has its exact control/lease pair."""
    if not _valid_task5_turn_metadata(
        status=turn.status,
        attempt_id=turn.attempt_id,
        lease_generation=turn.lease_generation,
        fencing_token=turn.fencing_token,
        execution_state=turn.execution_state,
        terminal_result_id=turn.terminal_result_id,
        recovery_block_key=turn.recovery_block_key,
        project_id=turn.project_id,
        turn_id=turn.turn_id,
    ):
        raise RuntimeError("runtime turn has inconsistent Task-5 metadata")
    event_block_key = (
        _recovery_block_event_key(
            conn,
            project_id=turn.project_id,
            turn_id=turn.turn_id,
            attempt_id=turn.attempt_id,
            lease_generation=turn.lease_generation,
            fencing_token=turn.fencing_token,
        )
        if turn.attempt_id is not None
        else None
    )
    if event_block_key != turn.recovery_block_key:
        raise RuntimeError(
            "runtime turn recovery block projection is inconsistent"
        )
    control = _runtime_control_for_turn(
        conn, project_id=turn.project_id, turn_id=turn.turn_id
    )
    if control is None or not (
        control.project_id == turn.project_id
        and control.turn_id == turn.turn_id
    ):
        raise RuntimeError("runtime turn has no matching control row")
    lease = _current_worker_lease_for_turn(
        conn, project_id=turn.project_id, turn_id=turn.turn_id
    )
    audit_complete = (
        control.claim_worker_id is not None
        and control.claim_lease_expires_at is not None
        and control.claim_canonical_session_id is not None
    )
    lease_matches = (
        lease is not None
        and audit_complete
        and lease.lease_id == turn.attempt_id
        and lease.worker_id == control.claim_worker_id
        and lease.lease_generation == turn.lease_generation
        and lease.fencing_token == turn.fencing_token
        and lease.expires_at == control.claim_lease_expires_at
    )
    attempt_matches = (
        turn.attempt_id is not None
        and control.attempt_id == turn.attempt_id
        and audit_complete
    )
    if turn.status == "queued":
        if turn.attempt_id is None:
            valid = (
                control.control_state == "running"
                and control.attempt_id is None
                and not audit_complete
                and lease is None
            )
        else:
            valid = (
                control.control_state == "resume_requested"
                and attempt_matches
                and lease is None
            )
    elif turn.status in {"claimed", "awaiting_approval"}:
        valid = (
            control.control_state == "running"
            and attempt_matches
            and lease_matches
        )
    elif turn.status == "stop_requested":
        valid = (
            control.control_state == "stop_requested"
            and attempt_matches
            and lease_matches
        )
    elif turn.status == "stopped":
        valid = (
            control.control_state == "stopped"
            and attempt_matches
            and lease is None
        )
    elif turn.status == "reconciling":
        valid = (
            control.control_state in {"running", "stop_requested"}
            and attempt_matches
            and (lease is None or lease_matches)
        )
    else:
        assert turn.status in _TERMINAL_TURN_STATUSES
        valid = (
            control.control_state == "terminal"
            and control.attempt_id == turn.attempt_id
            and lease is None
        )
    if not valid:
        raise RuntimeError("runtime turn/control/lease pair is inconsistent")
    return control


def _claim_oldest_queued_runtime_turn(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    worker_id: str,
    attempt_id: str,
    canonical_session_id: str,
    now: int,
    lease_seconds: int,
) -> RuntimeTurnRecord | None:
    """Validate history set-wise, then claim the exact nonterminal FIFO head."""
    row = conn.execute(
        """
        SELECT turn.*
        FROM project_turns AS turn
        LEFT JOIN project_run_controls AS control
          ON control.project_id = turn.project_id
         AND control.turn_id = turn.turn_id
        LEFT JOIN project_worker_leases AS lease
          ON lease.project_id = turn.project_id
         AND lease.turn_id = turn.turn_id
        WHERE turn.project_id = ?
          AND (
                turn.status NOT IN (
                    'queued', 'claimed', 'awaiting_approval',
                    'stop_requested', 'stopped', 'reconciling',
                    'succeeded', 'failed', 'cancelled'
                )
                OR (
                    turn.status IN ('succeeded', 'failed', 'cancelled')
                    AND NOT (
                typeof(turn.turn_id) = 'text'
                AND length(turn.turn_id) > 0
                AND typeof(turn.project_id) = 'text'
                AND length(turn.project_id) > 0
                AND typeof(turn.sequence) = 'integer'
                AND turn.sequence >= 1
                AND typeof(turn.idempotency_key) = 'text'
                AND length(turn.idempotency_key) > 0
                AND typeof(turn.payload_json) = 'text'
                AND length(turn.payload_json) > 0
                AND (
                    turn.origin_binding_id IS NULL
                    OR (
                        typeof(turn.origin_binding_id) = 'text'
                        AND length(turn.origin_binding_id) > 0
                    )
                )
                AND (
                    turn.attempt_id IS NULL
                    OR (
                        typeof(turn.attempt_id) = 'text'
                        AND length(turn.attempt_id) > 0
                    )
                )
                AND typeof(turn.lease_generation) = 'integer'
                AND typeof(turn.fencing_token) = 'integer'
                AND (
                    (
                        turn.attempt_id IS NULL
                        AND turn.lease_generation = 0
                        AND turn.fencing_token = 0
                    )
                    OR (
                        turn.attempt_id IS NOT NULL
                        AND turn.lease_generation > 0
                        AND turn.fencing_token > 0
                    )
                )
                AND (
                    turn.execution_state IS NULL
                    OR turn.execution_state IN ('not_started', 'started')
                )
                AND (
                    turn.terminal_result_id IS NULL
                    OR (
                        typeof(turn.terminal_result_id) = 'text'
                        AND length(turn.terminal_result_id) > 0
                    )
                )
                AND (
                    turn.attempt_id IS NOT NULL
                    OR turn.execution_state IS NULL
                )
                AND (
                    turn.terminal_result_id IS NULL
                    OR (
                        turn.status IN ('succeeded', 'failed')
                        AND turn.attempt_id IS NOT NULL
                        AND (
                            turn.execution_state IS NULL
                            OR turn.execution_state = 'started'
                        )
                    )
                )
                AND (
                    turn.status NOT IN ('succeeded', 'failed')
                    OR turn.terminal_result_id IS NOT NULL
                    OR turn.execution_state IS NULL
                )
                AND turn.recovery_block_key IS NULL
                AND typeof(turn.created_at) = 'integer'
                AND turn.created_at >= 0
                AND typeof(turn.updated_at) = 'integer'
                AND turn.updated_at >= 0
                AND control.turn_id IS NOT NULL
                AND control.control_state = 'terminal'
                AND typeof(control.control_version) = 'integer'
                AND control.control_version >= 0
                AND (
                    control.idempotency_key IS NULL
                    OR (
                        typeof(control.idempotency_key) = 'text'
                        AND length(control.idempotency_key) > 0
                    )
                )
                AND (
                    control.command_fingerprint IS NULL
                    OR (
                        typeof(control.command_fingerprint) = 'text'
                        AND length(control.command_fingerprint) > 0
                    )
                )
                AND control.attempt_id IS turn.attempt_id
                AND (
                    (
                        control.claim_worker_id IS NULL
                        AND control.claim_lease_expires_at IS NULL
                        AND control.claim_canonical_session_id IS NULL
                    )
                    OR (
                        control.attempt_id IS NOT NULL
                        AND typeof(control.claim_worker_id) = 'text'
                        AND length(control.claim_worker_id) > 0
                        AND typeof(control.claim_lease_expires_at) = 'integer'
                        AND control.claim_lease_expires_at >= 0
                        AND typeof(
                            control.claim_canonical_session_id
                        ) = 'text'
                        AND length(
                            control.claim_canonical_session_id
                        ) > 0
                    )
                )
                AND typeof(control.updated_at) = 'integer'
                AND control.updated_at >= 0
                AND lease.turn_id IS NULL
                    )
                )
          )
        ORDER BY turn.sequence, turn.turn_id
        LIMIT 1
        """,
        (project_id,),
    ).fetchone()
    if row is not None:
        malformed_turn = runtime_turn_from_row(row)
        _validate_runtime_turn_pair(conn, turn=malformed_turn)
        raise RuntimeError("malformed terminal runtime history")

    row = conn.execute(
        """
        SELECT turn.*
        FROM project_turns AS turn
        JOIN project_run_controls AS control
          ON control.project_id = turn.project_id
         AND control.turn_id = turn.turn_id
        LEFT JOIN project_worker_leases AS lease
          ON lease.project_id = turn.project_id
         AND lease.turn_id = turn.turn_id
        WHERE turn.project_id = ?
          AND turn.status NOT IN ('succeeded', 'failed', 'cancelled')
        ORDER BY turn.sequence, turn.turn_id
        LIMIT 1
        """,
        (project_id,),
    ).fetchone()
    if row is None:
        return None
    turn = runtime_turn_from_row(row)
    control = _validate_runtime_turn_pair(conn, turn=turn)
    if turn.status != "queued":
        return None
    if (
        turn.lease_generation >= _SQLITE_INT_MAX
        or turn.fencing_token >= _SQLITE_INT_MAX
    ):
        raise RuntimeError("runtime claim counter exhausted")
    generation = turn.lease_generation + 1
    fence = turn.fencing_token + 1
    lease_expires_at = now + lease_seconds
    if conn.execute(
        """
        UPDATE project_turns
        SET status = 'claimed', attempt_id = ?, lease_generation = ?,
            fencing_token = ?, execution_state = 'not_started',
            terminal_result_id = NULL, recovery_block_key = NULL,
            updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND status = 'queued'
          AND lease_generation = ? AND fencing_token = ?
          AND (
                (attempt_id IS NULL AND ? IS NULL)
                OR attempt_id = ?
          )
        """,
        (
            attempt_id,
            generation,
            fence,
            now,
            project_id,
            turn.turn_id,
            turn.lease_generation,
            turn.fencing_token,
            turn.attempt_id,
            turn.attempt_id,
        ),
    ).rowcount != 1:
        return None
    if conn.execute(
        """
        UPDATE project_run_controls
        SET control_state = 'running', control_version = control_version + 1,
            attempt_id = ?, claim_worker_id = ?,
            claim_lease_expires_at = ?,
            claim_canonical_session_id = ?, updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND control_version = ?
          AND control_state = ?
          AND (
                (attempt_id IS NULL AND ? IS NULL)
                OR attempt_id = ?
          )
        """,
        (
            attempt_id,
            worker_id,
            lease_expires_at,
            canonical_session_id,
            now,
            project_id,
            turn.turn_id,
            control.control_version,
            control.control_state,
            control.attempt_id,
            control.attempt_id,
        ),
    ).rowcount != 1:
        raise RuntimeError("runtime control changed during guarded turn claim")
    conn.execute(
        """
        INSERT INTO project_worker_leases (
            lease_id, project_id, turn_id, worker_id, lease_generation,
            fencing_token, expires_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            attempt_id,
            project_id,
            turn.turn_id,
            worker_id,
            generation,
            fence,
            lease_expires_at,
            now,
        ),
    )
    result = _runtime_turn_for_project(conn, project_id=project_id, turn_id=turn.turn_id)
    assert result is not None
    return result


_TASK4_TRANSITIONS = {
    ("queued", "running", "cancelled", "terminal"),
    ("claimed", "running", "stop_requested", "stop_requested"),
    ("stop_requested", "stop_requested", "stopped", "stopped"),
    ("stopped", "stopped", "queued", "resume_requested"),
}


def _transition_runtime_turn_and_control(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    expected_turn_status: str,
    next_turn_status: str,
    expected_control_state: str,
    expected_attempt_id: str | None,
    expected_control_version: int,
    next_control_state: str,
    now: int,
    idempotency_key: str | None = None,
    command_fingerprint: str | None = None,
) -> RuntimeControlRecord | None:
    """Atomically move a legal turn/control pair under caller-owned SQL scope."""
    if (
        expected_turn_status,
        expected_control_state,
        next_turn_status,
        next_control_state,
    ) not in _TASK4_TRANSITIONS:
        raise ValueError("unsupported Task-4 turn/control transition")
    if conn.execute(
        """
        UPDATE project_turns SET status = ?, updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND status = ?
          AND (
                (attempt_id IS NULL AND ? IS NULL)
                OR attempt_id = ?
          )
        """,
        (
            next_turn_status,
            now,
            project_id,
            turn_id,
            expected_turn_status,
            expected_attempt_id,
            expected_attempt_id,
        ),
    ).rowcount != 1:
        return None
    if conn.execute(
        """
        UPDATE project_run_controls
        SET control_state = ?, control_version = control_version + 1,
            idempotency_key = COALESCE(?, idempotency_key),
            command_fingerprint = COALESCE(?, command_fingerprint), updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND control_version = ?
          AND control_state = ?
          AND (
                (attempt_id IS NULL AND ? IS NULL)
                OR attempt_id = ?
          )
        """,
        (
            next_control_state,
            idempotency_key,
            command_fingerprint,
            now,
            project_id,
            turn_id,
            expected_control_version,
            expected_control_state,
            expected_attempt_id,
            expected_attempt_id,
        ),
    ).rowcount != 1:
        raise RuntimeError("runtime control changed after durable turn transition")
    return _runtime_control_for_turn(conn, project_id=project_id, turn_id=turn_id)


def _delete_current_worker_lease(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    attempt_id: str,
    worker_id: str,
    lease_generation: int,
    fencing_token: int,
    lease_expires_at: int,
) -> bool:
    """Close exactly the current worker identity; never delete a stale lease."""
    return conn.execute(
        """
        DELETE FROM project_worker_leases
        WHERE project_id = ? AND turn_id = ? AND lease_id = ? AND worker_id = ?
          AND lease_generation = ? AND fencing_token = ? AND expires_at = ?
        """,
        (
            project_id,
            turn_id,
            attempt_id,
            worker_id,
            lease_generation,
            fencing_token,
            lease_expires_at,
        ),
    ).rowcount == 1


def _heartbeat_runtime_turn(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    sequence: int,
    turn_status: str,
    control_state: str,
    attempt_id: str,
    worker_id: str,
    lease_generation: int,
    fencing_token: int,
    canonical_session_id: str,
    old_expires_at: int,
    new_expires_at: int,
    now: int,
) -> WorkerLeaseRecord | None:
    """Extend one exact live lease and its control audit under caller scope."""
    if new_expires_at == old_expires_at:
        return _current_worker_lease_for_turn(
            conn, project_id=project_id, turn_id=turn_id
        )
    lease_cursor = conn.execute(
        """
        UPDATE project_worker_leases
        SET expires_at = ?,
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ? AND lease_id = ?
          AND worker_id = ? AND lease_generation = ? AND fencing_token = ?
          AND expires_at = ? AND expires_at > ?
          AND EXISTS (
              SELECT 1
              FROM project_turns AS turn
              WHERE turn.project_id = project_worker_leases.project_id
                AND turn.turn_id = project_worker_leases.turn_id
                AND turn.sequence = ?
                AND turn.status = ?
                AND turn.attempt_id = ?
                AND turn.lease_generation = ?
                AND turn.fencing_token = ?
                AND turn.execution_state IN ('not_started', 'started')
          )
        """,
        (
            new_expires_at,
            now,
            now,
            project_id,
            turn_id,
            attempt_id,
            worker_id,
            lease_generation,
            fencing_token,
            old_expires_at,
            now,
            sequence,
            turn_status,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    if lease_cursor.rowcount != 1:
        return None
    control_cursor = conn.execute(
        """
        UPDATE project_run_controls
        SET claim_lease_expires_at = ?,
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ? AND control_state = ?
          AND attempt_id = ? AND claim_worker_id = ?
          AND claim_lease_expires_at = ?
          AND claim_canonical_session_id = ?
        """,
        (
            new_expires_at,
            now,
            now,
            project_id,
            turn_id,
            control_state,
            attempt_id,
            worker_id,
            old_expires_at,
            canonical_session_id,
        ),
    )
    if control_cursor.rowcount != 1:
        raise RuntimeError("runtime control changed during heartbeat")
    return _current_worker_lease_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    )


def _mark_runtime_turn_started(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    sequence: int,
    attempt_id: str,
    worker_id: str,
    lease_generation: int,
    fencing_token: int,
    canonical_session_id: str,
    expires_at: int,
    now: int,
) -> bool:
    """Persist the exact live attempt's execution boundary without an event."""
    cursor = conn.execute(
        """
        UPDATE project_turns
        SET execution_state = 'started',
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ? AND sequence = ?
          AND status = 'claimed' AND attempt_id = ?
          AND lease_generation = ? AND fencing_token = ?
          AND execution_state = 'not_started'
          AND EXISTS (
              SELECT 1
              FROM project_run_controls AS control
              WHERE control.project_id = project_turns.project_id
                AND control.turn_id = project_turns.turn_id
                AND control.control_state = 'running'
                AND control.attempt_id = ?
                AND control.claim_worker_id = ?
                AND control.claim_lease_expires_at = ?
                AND control.claim_canonical_session_id = ?
          )
          AND EXISTS (
              SELECT 1
              FROM project_worker_leases AS lease
              WHERE lease.project_id = project_turns.project_id
                AND lease.turn_id = project_turns.turn_id
                AND lease.lease_id = ?
                AND lease.worker_id = ?
                AND lease.lease_generation = ?
                AND lease.fencing_token = ?
                AND lease.expires_at = ?
                AND lease.expires_at > ?
          )
        """,
        (
            now,
            now,
            project_id,
            turn_id,
            sequence,
            attempt_id,
            lease_generation,
            fencing_token,
            attempt_id,
            worker_id,
            expires_at,
            canonical_session_id,
            attempt_id,
            worker_id,
            lease_generation,
            fencing_token,
            expires_at,
            now,
        ),
    )
    return cursor.rowcount == 1


def _commit_runtime_turn(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    sequence: int,
    terminal_status: str,
    terminal_result_id: str,
    attempt_id: str,
    worker_id: str,
    lease_generation: int,
    fencing_token: int,
    canonical_session_id: str,
    expires_at: int,
    expected_control_version: int,
    now: int,
) -> RuntimeTurnRecord | None:
    """Terminalize one exact started live attempt under caller write scope."""
    turn_cursor = conn.execute(
        """
        UPDATE project_turns
        SET status = ?, terminal_result_id = ?,
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ? AND sequence = ?
          AND status = 'claimed' AND attempt_id = ?
          AND lease_generation = ? AND fencing_token = ?
          AND execution_state = 'started' AND terminal_result_id IS NULL
        """,
        (
            terminal_status,
            terminal_result_id,
            now,
            now,
            project_id,
            turn_id,
            sequence,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    if turn_cursor.rowcount != 1:
        return None
    control_cursor = conn.execute(
        """
        UPDATE project_run_controls
        SET control_state = 'terminal',
            control_version = control_version + 1,
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ?
          AND control_state = 'running'
          AND control_version = ? AND attempt_id = ?
          AND claim_worker_id = ? AND claim_lease_expires_at = ?
          AND claim_canonical_session_id = ?
        """,
        (
            now,
            now,
            project_id,
            turn_id,
            expected_control_version,
            attempt_id,
            worker_id,
            expires_at,
            canonical_session_id,
        ),
    )
    if control_cursor.rowcount != 1:
        raise RuntimeError("runtime control changed during terminal commit")
    if not _delete_current_worker_lease(
        conn,
        project_id=project_id,
        turn_id=turn_id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_generation=lease_generation,
        fencing_token=fencing_token,
        lease_expires_at=expires_at,
    ):
        raise RuntimeError("runtime lease changed during terminal commit")
    result = _runtime_turn_for_project(
        conn, project_id=project_id, turn_id=turn_id
    )
    assert result is not None
    return result


def _recovery_block_event_key(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
) -> str | None:
    """Return and validate the canonical block event for one exact attempt."""
    block_key = _recovery_block_key(
        project_id=project_id,
        turn_id=turn_id,
        attempt_id=attempt_id,
        lease_generation=lease_generation,
        fencing_token=fencing_token,
    )
    identity_row = conn.execute(
        _RECOVERY_BLOCK_LOOKUP_SQL,
        (
            project_id,
            turn_id,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    ).fetchone()
    key_row = conn.execute(
        _RECOVERY_BLOCK_KEY_LOOKUP_SQL,
        (block_key, project_id, turn_id),
    ).fetchone()
    row = identity_row if identity_row is not None else key_row
    if row is None:
        return None
    _validate_recovery_block_payload(
        row["payload_json"],
        attempt_id=attempt_id,
        turn_id=turn_id,
        lease_generation=lease_generation,
        fencing_token=fencing_token,
    )
    if row["event_id"] != block_key:
        raise RuntimeError("recovery block event identity is inconsistent")
    return block_key


def _recovery_block_exists(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
) -> bool:
    """Return whether this exact attempt already has a canonical block."""
    return (
        _recovery_block_event_key(
            conn,
            project_id=project_id,
            turn_id=turn_id,
            attempt_id=attempt_id,
            lease_generation=lease_generation,
            fencing_token=fencing_token,
        )
        is not None
    )


def _set_recovery_block_key(
    conn: sqlite3.Connection,
    *,
    candidate: RecoveryCandidateRecord,
    block_key: str,
) -> bool:
    """Materialize one canonical block event on its exact parked attempt."""
    event_key = _recovery_block_event_key(
        conn,
        project_id=candidate.project_id,
        turn_id=candidate.turn_id,
        attempt_id=candidate.attempt_id,
        lease_generation=candidate.lease_generation,
        fencing_token=candidate.fencing_token,
    )
    if event_key != block_key:
        raise RuntimeError("recovery block event is missing")
    return (
        conn.execute(
            """
            UPDATE project_turns SET recovery_block_key = ?
            WHERE project_id = ? AND turn_id = ? AND sequence = ?
              AND status = 'reconciling' AND attempt_id = ?
              AND lease_generation = ? AND fencing_token = ?
              AND recovery_block_key IS NULL
            """,
            (
                block_key,
                candidate.project_id,
                candidate.turn_id,
                candidate.sequence,
                candidate.attempt_id,
                candidate.lease_generation,
                candidate.fencing_token,
            ),
        ).rowcount
        == 1
    )


def _recovery_candidate_for_attempt(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    attempt_id: str,
    lease_generation: int,
    fencing_token: int,
) -> RecoveryCandidateRecord | None:
    """Load and validate one lease-less reconciling attempt."""
    turn = _runtime_turn_for_project(
        conn, project_id=project_id, turn_id=turn_id
    )
    if turn is None or not (
        turn.status == "reconciling"
        and turn.attempt_id == attempt_id
        and turn.lease_generation == lease_generation
        and turn.fencing_token == fencing_token
    ):
        return None
    control = _validate_runtime_turn_pair(conn, turn=turn)
    lease = _current_worker_lease_for_turn(
        conn, project_id=project_id, turn_id=turn_id
    )
    if lease is not None:
        raise RuntimeError("reconciling runtime turn still has a worker lease")
    state = _runtime_state_for_project(conn, project_id)
    if state is None or state.lifecycle not in {
        "active",
        "awaiting_acceptance",
        "completed",
    }:
        raise RuntimeError("recovery candidate has invalid runtime state")
    source_status = {
        "running": "claimed",
        "stop_requested": "stop_requested",
    }.get(control.control_state)
    if (
        source_status is None
        or control.attempt_id != attempt_id
        or not _stored_text(control.claim_worker_id)
        or not _stored_int(control.claim_lease_expires_at)
        or not _stored_text(control.claim_canonical_session_id)
    ):
        raise RuntimeError("recovery candidate has incomplete attempt audit")
    return RecoveryCandidateRecord(
        project_id=project_id,
        turn_id=turn_id,
        sequence=turn.sequence,
        source_status=source_status,
        worker_id=control.claim_worker_id,
        attempt_id=attempt_id,
        lease_generation=lease_generation,
        fencing_token=fencing_token,
        lease_expires_at=control.claim_lease_expires_at,
        canonical_session_id=control.claim_canonical_session_id,
        execution_state=turn.execution_state,
        lifecycle=state.lifecycle,
        control_version=control.control_version,
    )


def _recovery_candidates(
    conn: sqlite3.Connection,
    *,
    now: int,
    limit: int,
) -> tuple[RecoveryCandidateRecord, ...]:
    """Select a bounded deterministic batch of expired or parked attempts."""
    branch_rows = (
        conn.execute(
            _RECOVERY_EXPIRED_LEASES_SQL, (now, limit)
        ).fetchall(),
        conn.execute(_RECOVERY_RECONCILING_SQL, (limit,)).fetchall(),
    )
    unique_rows: dict[tuple[str, str], sqlite3.Row] = {}
    for rows in branch_rows:
        for row in rows:
            unique_rows.setdefault(
                (row["project_id"], row["turn_id"]), row
            )
    selected_rows = sorted(
        unique_rows.values(),
        key=lambda row: (
            row["project_id"],
            row["sequence"],
            row["turn_id"],
        ),
    )[:limit]
    candidates: list[RecoveryCandidateRecord] = []
    for row in selected_rows:
        turn = _runtime_turn_for_project(
            conn,
            project_id=row["project_id"],
            turn_id=row["turn_id"],
        )
        if turn is None:
            raise RuntimeError("recovery candidate disappeared during scan")
        control = _validate_runtime_turn_pair(conn, turn=turn)
        state = _runtime_state_for_project(conn, turn.project_id)
        if state is None or state.lifecycle not in {
            "active",
            "awaiting_acceptance",
            "completed",
        }:
            raise RuntimeError("recovery candidate has invalid runtime state")
        if turn.attempt_id is None:
            raise RuntimeError("recovery candidate has no attempt identity")
        if turn.status == "reconciling":
            candidate = _recovery_candidate_for_attempt(
                conn,
                project_id=turn.project_id,
                turn_id=turn.turn_id,
                attempt_id=turn.attempt_id,
                lease_generation=turn.lease_generation,
                fencing_token=turn.fencing_token,
            )
            assert candidate is not None
            candidates.append(candidate)
            continue
        source_status = turn.status
        expected_control_state = {
            "claimed": "running",
            "stop_requested": "stop_requested",
        }.get(source_status)
        lease = _current_worker_lease_for_turn(
            conn, project_id=turn.project_id, turn_id=turn.turn_id
        )
        if (
            expected_control_state is None
            or control.control_state != expected_control_state
            or lease is None
            or lease.expires_at > now
            or control.claim_lease_expires_at != lease.expires_at
            or control.attempt_id != turn.attempt_id
            or control.claim_worker_id != lease.worker_id
            or not _stored_text(control.claim_canonical_session_id)
        ):
            raise RuntimeError("expired recovery candidate is inconsistent")
        candidates.append(
            RecoveryCandidateRecord(
                project_id=turn.project_id,
                turn_id=turn.turn_id,
                sequence=turn.sequence,
                source_status=source_status,
                worker_id=lease.worker_id,
                attempt_id=turn.attempt_id,
                lease_generation=turn.lease_generation,
                fencing_token=turn.fencing_token,
                lease_expires_at=lease.expires_at,
                canonical_session_id=control.claim_canonical_session_id,
                execution_state=turn.execution_state,
                lifecycle=state.lifecycle,
                control_version=control.control_version,
            )
        )
    return tuple(candidates)


def _park_expired_runtime_turn(
    conn: sqlite3.Connection,
    *,
    candidate: RecoveryCandidateRecord,
    now: int,
) -> RuntimeTurnRecord | None:
    """CAS an expired attempt into the durable lease-less recovery state."""
    expected_control_state = {
        "claimed": "running",
        "stop_requested": "stop_requested",
    }.get(candidate.source_status)
    if expected_control_state is None:
        raise ValueError("unsupported recovery source status")
    turn_cursor = conn.execute(
        """
        UPDATE project_turns
        SET status = 'reconciling',
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ? AND sequence = ?
          AND status = ? AND attempt_id = ?
          AND lease_generation = ? AND fencing_token = ?
          AND recovery_block_key IS NULL
          AND (
                (execution_state IS NULL AND ? IS NULL)
                OR execution_state = ?
          )
          AND EXISTS (
              SELECT 1
              FROM project_worker_leases AS lease
              WHERE lease.project_id = project_turns.project_id
                AND lease.turn_id = project_turns.turn_id
                AND lease.lease_id = ?
                AND lease.worker_id = ?
                AND lease.lease_generation = ?
                AND lease.fencing_token = ?
                AND lease.expires_at = ?
                AND lease.expires_at <= ?
          )
        """,
        (
            now,
            now,
            candidate.project_id,
            candidate.turn_id,
            candidate.sequence,
            candidate.source_status,
            candidate.attempt_id,
            candidate.lease_generation,
            candidate.fencing_token,
            candidate.execution_state,
            candidate.execution_state,
            candidate.attempt_id,
            candidate.worker_id,
            candidate.lease_generation,
            candidate.fencing_token,
            candidate.lease_expires_at,
            now,
        ),
    )
    if turn_cursor.rowcount != 1:
        return None
    control_cursor = conn.execute(
        """
        UPDATE project_run_controls
        SET control_version = control_version + 1,
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ?
          AND control_state = ? AND control_version = ?
          AND attempt_id = ? AND claim_worker_id = ?
          AND claim_lease_expires_at = ?
          AND claim_canonical_session_id = ?
        """,
        (
            now,
            now,
            candidate.project_id,
            candidate.turn_id,
            expected_control_state,
            candidate.control_version,
            candidate.attempt_id,
            candidate.worker_id,
            candidate.lease_expires_at,
            candidate.canonical_session_id,
        ),
    )
    if control_cursor.rowcount != 1:
        raise RuntimeError("runtime control changed while parking recovery")
    lease_cursor = conn.execute(
        """
        DELETE FROM project_worker_leases
        WHERE project_id = ? AND turn_id = ? AND lease_id = ?
          AND worker_id = ? AND lease_generation = ? AND fencing_token = ?
          AND expires_at = ? AND expires_at <= ?
        """,
        (
            candidate.project_id,
            candidate.turn_id,
            candidate.attempt_id,
            candidate.worker_id,
            candidate.lease_generation,
            candidate.fencing_token,
            candidate.lease_expires_at,
            now,
        ),
    )
    if lease_cursor.rowcount != 1:
        raise RuntimeError("runtime lease changed while parking recovery")
    return _runtime_turn_for_project(
        conn,
        project_id=candidate.project_id,
        turn_id=candidate.turn_id,
    )


def _park_live_runtime_turn_for_operation_block(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    turn_id: str,
    sequence: int,
    attempt_id: str,
    worker_id: str,
    lease_generation: int,
    fencing_token: int,
    lease_expires_at: int,
    canonical_session_id: str,
    control_version: int,
    now: int,
) -> RecoveryCandidateRecord:
    """Fence a live post-effect block into Task-5 recovery authority."""
    turn = conn.execute(
        """
        UPDATE project_turns
        SET status = 'reconciling',
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ? AND sequence = ?
          AND status = 'claimed' AND execution_state = 'started'
          AND attempt_id = ? AND lease_generation = ? AND fencing_token = ?
          AND recovery_block_key IS NULL
        """,
        (
            now,
            now,
            project_id,
            turn_id,
            sequence,
            attempt_id,
            lease_generation,
            fencing_token,
        ),
    )
    if turn.rowcount != 1:
        raise RuntimeError(
            "operation block lost its live runtime turn"
        )
    control = conn.execute(
        """
        UPDATE project_run_controls
        SET control_version = control_version + 1,
            updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
        WHERE project_id = ? AND turn_id = ?
          AND control_state = 'running' AND control_version = ?
          AND attempt_id = ? AND claim_worker_id = ?
          AND claim_lease_expires_at = ?
          AND claim_canonical_session_id = ?
        """,
        (
            now,
            now,
            project_id,
            turn_id,
            control_version,
            attempt_id,
            worker_id,
            lease_expires_at,
            canonical_session_id,
        ),
    )
    if control.rowcount != 1:
        raise RuntimeError(
            "operation block lost its live runtime control"
        )
    if not _delete_current_worker_lease(
        conn,
        project_id=project_id,
        turn_id=turn_id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_generation=lease_generation,
        fencing_token=fencing_token,
        lease_expires_at=lease_expires_at,
    ):
        raise RuntimeError(
            "operation block lost its live runtime lease"
        )
    candidate = _recovery_candidate_for_attempt(
        conn,
        project_id=project_id,
        turn_id=turn_id,
        attempt_id=attempt_id,
        lease_generation=lease_generation,
        fencing_token=fencing_token,
    )
    if candidate is None:
        raise RuntimeError(
            "operation block recovery candidate disappeared"
        )
    return candidate


def _apply_recovery_outcome(
    conn: sqlite3.Connection,
    *,
    candidate: RecoveryCandidateRecord,
    outcome: str,
    terminal_result_id: str | None,
    now: int,
) -> RuntimeTurnRecord | None:
    """Finalize one exact parked attempt under caller-owned write scope."""
    expected_control_state = {
        "claimed": "running",
        "stop_requested": "stop_requested",
    }.get(candidate.source_status)
    transition = {
        "queued": ("queued", "running"),
        "stopped": ("stopped", "stopped"),
        "succeeded": ("succeeded", "terminal"),
        "failed": ("failed", "terminal"),
    }.get(outcome)
    if expected_control_state is None or transition is None:
        raise ValueError("unsupported recovery transition")
    next_turn_status, next_control_state = transition
    if outcome == "queued":
        turn_sql = """
            UPDATE project_turns
            SET status = 'queued', attempt_id = NULL,
                execution_state = NULL, terminal_result_id = NULL,
                recovery_block_key = NULL,
                updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
            WHERE project_id = ? AND turn_id = ? AND sequence = ?
              AND status = 'reconciling' AND attempt_id = ?
              AND lease_generation = ? AND fencing_token = ?
              AND terminal_result_id IS NULL
              AND recovery_block_key IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM project_runtime_state AS state
                  WHERE state.project_id = project_turns.project_id
                    AND state.lifecycle = 'active'
              )
        """
    else:
        turn_sql = """
            UPDATE project_turns
            SET status = ?, terminal_result_id = ?,
                updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
            WHERE project_id = ? AND turn_id = ? AND sequence = ?
              AND status = 'reconciling' AND attempt_id = ?
              AND lease_generation = ? AND fencing_token = ?
              AND terminal_result_id IS NULL
              AND recovery_block_key IS NULL
        """
    if outcome == "queued":
        turn_parameters = (
            now,
            now,
            candidate.project_id,
            candidate.turn_id,
            candidate.sequence,
            candidate.attempt_id,
            candidate.lease_generation,
            candidate.fencing_token,
        )
    else:
        turn_parameters = (
            next_turn_status,
            terminal_result_id,
            now,
            now,
            candidate.project_id,
            candidate.turn_id,
            candidate.sequence,
            candidate.attempt_id,
            candidate.lease_generation,
            candidate.fencing_token,
        )
    turn_cursor = conn.execute(turn_sql, turn_parameters)
    if turn_cursor.rowcount != 1:
        return None
    if outcome == "queued":
        control_sql = """
            UPDATE project_run_controls
            SET control_state = 'running',
                control_version = control_version + 1,
                attempt_id = NULL, claim_worker_id = NULL,
                claim_lease_expires_at = NULL,
                claim_canonical_session_id = NULL,
                updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
            WHERE project_id = ? AND turn_id = ?
              AND control_state = ? AND attempt_id = ?
              AND claim_worker_id = ? AND claim_lease_expires_at = ?
              AND claim_canonical_session_id = ?
        """
        control_parameters = (
            now,
            now,
            candidate.project_id,
            candidate.turn_id,
            expected_control_state,
            candidate.attempt_id,
            candidate.worker_id,
            candidate.lease_expires_at,
            candidate.canonical_session_id,
        )
    else:
        control_sql = """
            UPDATE project_run_controls
            SET control_state = ?,
                control_version = control_version + 1,
                updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
            WHERE project_id = ? AND turn_id = ?
              AND control_state = ? AND attempt_id = ?
              AND claim_worker_id = ? AND claim_lease_expires_at = ?
              AND claim_canonical_session_id = ?
        """
        control_parameters = (
            next_control_state,
            now,
            now,
            candidate.project_id,
            candidate.turn_id,
            expected_control_state,
            candidate.attempt_id,
            candidate.worker_id,
            candidate.lease_expires_at,
            candidate.canonical_session_id,
        )
    control_cursor = conn.execute(control_sql, control_parameters)
    if control_cursor.rowcount != 1:
        raise RuntimeError("runtime control changed during recovery outcome")
    if _current_worker_lease_for_turn(
        conn,
        project_id=candidate.project_id,
        turn_id=candidate.turn_id,
    ) is not None:
        raise RuntimeError("runtime lease reappeared during recovery outcome")
    return _runtime_turn_for_project(
        conn,
        project_id=candidate.project_id,
        turn_id=candidate.turn_id,
    )


def _link_approval_to_claimed_turn(
    conn: sqlite3.Connection,
    *,
    approval_id: str,
    project_id: str,
    turn_id: str,
    expected_attempt_id: str,
    expected_lease_generation: int,
    expected_fencing_token: int,
    now: int,
) -> bool:
    """Bind one newly-created approval to one claimed FIFO head atomically."""
    if conn.execute(
        """
        UPDATE project_approvals SET turn_id = ?
        WHERE approval_id = ? AND project_id = ? AND turn_id IS NULL
          AND operation_id IS NULL
        """,
        (turn_id, approval_id, project_id),
    ).rowcount != 1:
        return False
    if conn.execute(
        """
        UPDATE project_turns SET status = 'awaiting_approval', updated_at = ?
        WHERE project_id = ? AND turn_id = ? AND status = 'claimed'
          AND attempt_id = ?
          AND lease_generation = ?
          AND fencing_token = ?
        """,
        (
            now,
            project_id,
            turn_id,
            expected_attempt_id,
            expected_lease_generation,
            expected_fencing_token,
        ),
    ).rowcount != 1:
        raise RuntimeError("claimed turn changed while linking approval")
    return True


def _conversation_from_row(row: sqlite3.Row) -> ProjectConversation:
    if row["parent_conversation_id"] is None:
        conversation = make_root_conversation(
            project_id=row["project_id"],
            conversation_id=row["conversation_id"],
            created_at=row["created_at"],
        )
    else:
        conversation = make_child_conversation(
            project_id=row["project_id"],
            conversation_id=row["conversation_id"],
            parent_conversation_id=row["parent_conversation_id"],
            root_conversation_id=row["root_conversation_id"],
            created_at=row["created_at"],
        )
    if conversation.root_conversation_id != row["root_conversation_id"]:
        raise LineageMigrationError("stored root is not a canonical self-root")
    return conversation


def lineage_for_project(
    conn: sqlite3.Connection, *, project_id: str
) -> tuple[ProjectConversation, ...]:
    """Read one project's validated lineage as immutable records."""
    if type(project_id) is not str or not project_id:
        raise ValueError("project_id must be a non-empty string")
    rows = conn.execute(
        """
        SELECT * FROM project_conversations
        WHERE project_id = ?
        ORDER BY created_at, conversation_id
        """,
        (project_id,),
    ).fetchall()
    return tuple(_conversation_from_row(row) for row in rows)


def create_project_conversation(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    conversation_id: str,
    current_phase: str,
    now: int,
) -> ProjectConversation:
    """Atomically adopt a project with exactly one self-root conversation."""
    root = make_root_conversation(
        project_id=project_id,
        conversation_id=conversation_id,
        created_at=now,
    )
    if type(current_phase) is not str or not current_phase:
        raise ValueError("current_phase must be a non-empty string")
    with write_transaction(conn):
        if _runtime_state_for_project(conn, project_id) is not None:
            raise LineageConflictError("project runtime state already exists")
        if conn.execute(
            """
            SELECT 1 FROM project_conversations
            WHERE project_id = ?
            LIMIT 1
            """,
            (project_id,),
        ).fetchone() is not None:
            raise LineageConflictError("project conversation lineage already exists")
        conn.execute(
            """
            INSERT INTO project_conversations (
                conversation_id, project_id, parent_conversation_id,
                root_conversation_id, created_at
            ) VALUES (?, ?, NULL, ?, ?)
            """,
            (
                root.conversation_id,
                root.project_id,
                root.root_conversation_id,
                root.created_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO project_runtime_state (
                project_id, lifecycle, current_phase, version,
                conversation_root_id, conversation_tip_id, updated_at
            ) VALUES (?, 'active', ?, 0, ?, ?, ?)
            """,
            (
                root.project_id,
                current_phase,
                root.conversation_id,
                root.conversation_id,
                now,
            ),
        )
    return root


def create_runtime_state(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    current_phase: str,
    conversation_root_id: str,
    conversation_tip_id: str,
    updated_at: int,
) -> RuntimeState:
    """Compatibility wrapper for canonical self-root project adoption."""
    if not (
        type(conversation_root_id) is str
        and conversation_root_id
        and type(conversation_tip_id) is str
        and conversation_tip_id
        and conversation_root_id == conversation_tip_id
    ):
        raise ValueError(
            "conversation root and tip must be the same non-empty string"
        )
    create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id=conversation_root_id,
        current_phase=current_phase,
        now=updated_at,
    )
    state = _runtime_state_for_project(conn, project_id)
    assert state is not None
    return state


def advance_conversation_tip(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    expected_tip_id: str,
    child_conversation_id: str,
    now: int,
) -> ProjectConversation | None:
    """Publish one compression child and atomically move only the current tip."""
    if not (
        type(project_id) is str
        and project_id
        and type(expected_tip_id) is str
        and expected_tip_id
    ):
        raise ValueError("project_id and expected_tip_id must be non-empty strings")
    try:
        with write_transaction(conn):
            state = _runtime_state_for_project(conn, project_id)
            if not (
                state is not None
                and state.lifecycle == "active"
                and type(state.current_phase) is str
                and bool(state.current_phase)
                and type(state.conversation_root_id) is str
                and bool(state.conversation_root_id)
                and state.conversation_tip_id == expected_tip_id
            ):
                return None
            expected = conn.execute(
                """
                SELECT * FROM project_conversations
                WHERE project_id = ?
                  AND conversation_id = ?
                  AND root_conversation_id = ?
                """,
                (
                    project_id,
                    expected_tip_id,
                    state.conversation_root_id,
                ),
            ).fetchone()
            if expected is None:
                return None
            _conversation_from_row(expected)
            child = make_child_conversation(
                project_id=project_id,
                conversation_id=child_conversation_id,
                parent_conversation_id=expected_tip_id,
                root_conversation_id=state.conversation_root_id,
                created_at=now,
            )
            try:
                conn.execute(
                    """
                    INSERT INTO project_conversations (
                        conversation_id, project_id, parent_conversation_id,
                        root_conversation_id, created_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        child.conversation_id,
                        child.project_id,
                        child.parent_conversation_id,
                        child.root_conversation_id,
                        child.created_at,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise LineageConflictError(
                    "child conversation identity already exists"
                ) from exc
            cursor = conn.execute(
                """
                UPDATE project_runtime_state
                SET conversation_tip_id = ?,
                    version = version + 1,
                    updated_at = ?
                WHERE project_id = ?
                  AND lifecycle = 'active'
                  AND conversation_root_id = ?
                  AND conversation_tip_id = ?
                  AND version = ?
                """,
                (
                    child.conversation_id,
                    now,
                    project_id,
                    state.conversation_root_id,
                    expected_tip_id,
                    state.version,
                ),
            )
            if cursor.rowcount != 1:
                raise _StaleConversationTip
        return child
    except _StaleConversationTip:
        return None


def _binding_from_row(row: sqlite3.Row) -> SurfaceBinding:
    return make_surface_binding(
        binding_id=row["binding_id"],
        project_id=row["project_id"],
        surface=row["surface"],
        external_binding_id=row["external_binding_id"],
        actor_id=row["actor_id"],
        created_at=row["created_at"],
    )


def _same_binding_identity(
    stored: SurfaceBinding, requested: SurfaceBinding
) -> bool:
    return (
        stored.binding_id == requested.binding_id
        and stored.project_id == requested.project_id
        and stored.surface == requested.surface
        and stored.external_binding_id == requested.external_binding_id
        and stored.actor_id == requested.actor_id
    )


def binding_for_id(
    conn: sqlite3.Connection, *, project_id: str, binding_id: str
) -> SurfaceBinding | None:
    """Read a binding only through its owning project scope."""
    if not all(type(value) is str and value for value in (project_id, binding_id)):
        raise ValueError("project_id and binding_id must be non-empty strings")
    row = conn.execute(
        """
        SELECT * FROM project_surface_bindings
        WHERE project_id = ? AND binding_id = ?
        """,
        (project_id, binding_id),
    ).fetchone()
    return _binding_from_row(row) if row is not None else None


def binding_for_external_identity(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    surface: str,
    external_binding_id: str,
) -> SurfaceBinding | None:
    """Read an external binding only within the expected project scope."""
    probe = make_surface_binding(
        binding_id="probe",
        project_id=project_id,
        surface=surface,
        external_binding_id=external_binding_id,
        actor_id="probe",
        created_at=0,
    )
    row = conn.execute(
        """
        SELECT * FROM project_surface_bindings
        WHERE project_id = ?
          AND surface = ?
          AND external_binding_id = ?
        """,
        (probe.project_id, probe.surface, probe.external_binding_id),
    ).fetchone()
    return _binding_from_row(row) if row is not None else None


def bindings_for_project(
    conn: sqlite3.Connection, *, project_id: str
) -> tuple[SurfaceBinding, ...]:
    """Read all immutable surface identities for one project."""
    if type(project_id) is not str or not project_id:
        raise ValueError("project_id must be a non-empty string")
    rows = conn.execute(
        """
        SELECT * FROM project_surface_bindings
        WHERE project_id = ?
        ORDER BY created_at, binding_id
        """,
        (project_id,),
    ).fetchall()
    return tuple(_binding_from_row(row) for row in rows)


def bind_surface(
    conn: sqlite3.Connection,
    *,
    binding_id: str,
    project_id: str,
    surface: str,
    external_binding_id: str,
    actor_id: str,
    now: int,
) -> SurfaceBinding:
    """Insert-or-read one immutable Desktop/Discord binding identity."""
    binding = make_surface_binding(
        binding_id=binding_id,
        project_id=project_id,
        surface=surface,
        external_binding_id=external_binding_id,
        actor_id=actor_id,
        created_at=now,
    )
    with write_transaction(conn):
        conn.execute(
            """
            INSERT INTO project_surface_bindings (
                binding_id, project_id, surface, external_binding_id,
                actor_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            (
                binding.binding_id,
                binding.project_id,
                binding.surface,
                binding.external_binding_id,
                binding.actor_id,
                binding.created_at,
            ),
        )
        rows = conn.execute(
            """
            SELECT * FROM project_surface_bindings
            WHERE binding_id = ?
               OR (surface = ? AND external_binding_id = ?)
            """,
            (
                binding.binding_id,
                binding.surface,
                binding.external_binding_id,
            ),
        ).fetchall()
        stored = _binding_from_row(rows[0]) if len(rows) == 1 else None
        if stored is None or not _same_binding_identity(stored, binding):
            raise BindingConflictError(
                "binding id or external identity already has another tuple"
            )
    return stored


_ALLOWED_SOURCES_BY_TARGET: dict[Lifecycle, tuple[Lifecycle, ...]] = {
    "active": ("awaiting_acceptance", "completed"),
    "awaiting_acceptance": ("active",),
    "completed": ("awaiting_acceptance",),
}


def transition_lifecycle(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    expected_version: int,
    lifecycle: Lifecycle,
    updated_at: int,
) -> Optional[RuntimeState]:
    """Apply one legal lifecycle edge with optimistic compare-and-swap."""
    if type(lifecycle) is not str or not lifecycle:
        return None
    allowed_sources = _ALLOWED_SOURCES_BY_TARGET.get(lifecycle)
    if allowed_sources is None:
        return None
    placeholders = ", ".join("?" for _ in allowed_sources)
    cursor = conn.execute(
        f"""
        UPDATE project_runtime_state
        SET lifecycle = ?, version = version + 1, updated_at = ?
        WHERE project_id = ?
          AND version = ?
          AND lifecycle IN ({placeholders})
        """,
        (
            lifecycle,
            updated_at,
            project_id,
            expected_version,
            *allowed_sources,
        ),
    )
    if cursor.rowcount != 1:
        return None
    return _runtime_state_for_project(conn, project_id)


def transition_current_phase(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    expected_version: int,
    current_phase: str,
    updated_at: int,
) -> Optional[RuntimeState]:
    """Initialize or change the durable phase through an exact version CAS."""
    if not (
        type(project_id) is str
        and bool(project_id)
        and type(expected_version) is int
        and expected_version >= 0
        and type(current_phase) is str
        and bool(current_phase)
        and type(updated_at) is int
    ):
        return None
    with write_transaction(conn):
        cursor = conn.execute(
            """
            UPDATE project_runtime_state
            SET current_phase = ?, version = version + 1, updated_at = ?
            WHERE project_id = ?
              AND version = ?
              AND (current_phase IS NULL OR current_phase <> ?)
            """,
            (
                current_phase,
                updated_at,
                project_id,
                expected_version,
                current_phase,
            ),
        )
        if cursor.rowcount != 1:
            return None
        return _runtime_state_for_project(conn, project_id)


def _canonical_items(values: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(values, tuple) or not values:
        raise ValueError(f"{field_name} must be a non-empty tuple")
    if any(type(value) is not str or not value for value in values):
        raise ValueError(f"{field_name} must contain non-empty strings")
    if len(set(values)) != len(values):
        raise ValueError(f"{field_name} must not contain duplicates")
    return values


def _canonical_json_array(values: tuple[str, ...]) -> str:
    return json.dumps(values, separators=(",", ":"), ensure_ascii=False)


def _canonical_boundary_json(
    *,
    authorization_actor_id: str,
    canonical_action: str,
    batch_id: str,
    batch_items: tuple[str, ...],
    expected_runtime_version: int,
    expected_lifecycle: Lifecycle,
    expected_phase: str,
) -> str:
    return json.dumps(
        {
            "authorization_actor_id": authorization_actor_id,
            "canonical_action": canonical_action,
            "batch_id": batch_id,
            "batch_items": batch_items,
            "expected_runtime_version": expected_runtime_version,
            "expected_lifecycle": expected_lifecycle,
            "expected_phase": expected_phase,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _approval_identity_storage_values(
    request: ApprovalRequest,
) -> tuple[tuple[str, ...], str, str]:
    """Validate and encode immutable request identity without reading the clock."""
    if not isinstance(request, ApprovalRequest):
        raise ValueError("request must be an ApprovalRequest")
    if not all(
        type(value) is str and bool(value)
        for value in (
            request.approval_id,
            request.project_id,
            request.requester_actor_id,
            request.authorization_actor_id,
            request.canonical_action,
            request.approval_class,
            request.batch_id,
        )
    ):
        raise ValueError("approval identity fields must be non-empty strings")
    if request.requester_actor_id != request.authorization_actor_id:
        raise ValueError("requester and authorization actor must match")
    if (
        approval_class_for_action(request.canonical_action)
        != request.approval_class
    ):
        raise ValueError("canonical action and approval class do not match")
    if type(request.command_revision) is not int or request.command_revision <= 0:
        raise ValueError("command_revision must be a positive integer")
    if (
        type(request.expected_runtime_version) is not int
        or request.expected_runtime_version < 0
    ):
        raise ValueError("expected_runtime_version must be a non-negative integer")
    if (
        type(request.expected_lifecycle) is not str
        or not request.expected_lifecycle
        or request.expected_lifecycle not in {
            "active",
            "awaiting_acceptance",
            "completed",
        }
    ):
        raise ValueError("expected_lifecycle must be a valid lifecycle")
    if type(request.expected_phase) is not str or not request.expected_phase:
        raise ValueError("expected_phase must be a non-empty string")
    if request.status != "pending":
        raise ValueError("new approvals must be pending")
    if any(
        value is not None
        for value in (
            request.resolved_by_actor_id,
            request.resolved_at,
            request.consumed_at,
        )
    ):
        raise ValueError("new approvals cannot contain resolved or consumed state")
    if type(request.expires_at) is not int or request.expires_at < 0:
        raise ValueError("approval expiry must be a non-negative integer")
    canonical_targets = canonicalize_targets(request.targets)
    if canonical_targets is None or not canonical_targets:
        raise ValueError("targets must be non-empty canonical paths")
    batch_items = _canonical_items(request.batch_items, field_name="batch_items")
    return (
        canonical_targets,
        _canonical_json_array(canonical_targets),
        _canonical_boundary_json(
            authorization_actor_id=request.authorization_actor_id,
            canonical_action=request.canonical_action,
            batch_id=request.batch_id,
            batch_items=batch_items,
            expected_runtime_version=request.expected_runtime_version,
            expected_lifecycle=request.expected_lifecycle,
            expected_phase=request.expected_phase,
        ),
    )


def _approval_storage_values(
    request: ApprovalRequest, now: int
) -> tuple[tuple[str, ...], str, str]:
    values = _approval_identity_storage_values(request)
    if type(now) is not int:
        raise ValueError("now must be an integer timestamp")
    if request.expires_at <= now:
        raise ValueError("approval expiry must be in the future")
    return values


def _approval_from_row(row: sqlite3.Row) -> ApprovalRequest:
    boundary = json.loads(row["batch_boundary_json"])
    return ApprovalRequest(
        approval_id=row["approval_id"],
        project_id=row["project_id"],
        requester_actor_id=row["actor_id"],
        authorization_actor_id=row["authorization_actor_id"],
        canonical_action=row["canonical_action"],
        approval_class=row["approval_class"],
        command_revision=row["command_revision"],
        expected_runtime_version=row["expected_runtime_version"],
        expected_lifecycle=row["expected_lifecycle"],
        expected_phase=row["expected_phase"],
        targets=tuple(json.loads(row["targets_json"])),
        batch_id=boundary["batch_id"],
        batch_items=tuple(boundary["batch_items"]),
        status=row["status"],
        expires_at=row["expires_at"],
        resolved_by_actor_id=row["resolved_by_actor_id"],
        resolved_at=row["resolved_at"],
        consumed_at=row["consumed_at"],
    )


def _row_matches_immutable_request(
    row: sqlite3.Row,
    request: ApprovalRequest,
    *,
    targets_json: str,
    boundary_json: str,
) -> bool:
    return (
        row["approval_id"] == request.approval_id
        and row["project_id"] == request.project_id
        and row["actor_id"] == request.requester_actor_id
        and row["authorization_actor_id"] == request.authorization_actor_id
        and row["canonical_action"] == request.canonical_action
        and row["approval_class"] == request.approval_class
        and row["command_revision"] == request.command_revision
        and row["expected_runtime_version"] == request.expected_runtime_version
        and row["expected_lifecycle"] == request.expected_lifecycle
        and row["expected_phase"] == request.expected_phase
        and row["targets_json"] == targets_json
        and row["batch_boundary_json"] == boundary_json
        and row["expires_at"] == request.expires_at
    )


def _create_approval_request(
    conn: sqlite3.Connection,
    request: ApprovalRequest,
    *,
    now: int,
    effective_runtime_version: int,
    turn_expected_control_version: int | None = None,
) -> ApprovalRequest:
    """Persist one request with distinct immutable and live authority versions."""
    canonical_targets, targets_json, boundary_json = _approval_storage_values(
        request, now
    )
    if (
        type(effective_runtime_version) is not int
        or effective_runtime_version < 0
    ):
        raise ValueError(
            "effective_runtime_version must be a non-negative integer"
        )
    if turn_expected_control_version is not None and (
        type(turn_expected_control_version) is not int
        or turn_expected_control_version < 0
    ):
        raise ValueError(
            "turn_expected_control_version must be a non-negative integer"
        )
    with write_transaction(conn):
        state = _runtime_state_for_project(conn, request.project_id)
        if not (
            state is not None
            and state.version == request.expected_runtime_version
            and state.lifecycle == request.expected_lifecycle
            and state.current_phase == request.expected_phase
        ):
            raise ApprovalConflictError(
                "runtime state does not match approval snapshot"
            )
        try:
            conn.execute(
                """
                INSERT INTO project_approvals (
                    approval_id, project_id, actor_id, authorization_actor_id,
                    canonical_action, approval_class, command_revision,
                    expected_runtime_version, effective_runtime_version,
                    turn_expected_control_version, expected_lifecycle,
                    expected_phase,
                    targets_json, batch_boundary_json, status, expires_at,
                    resolved_at, resolved_by_actor_id, consumed_at, created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?,
                    NULL, NULL, NULL, ?
                )
                ON CONFLICT DO NOTHING
                """,
                (
                    request.approval_id,
                    request.project_id,
                    request.requester_actor_id,
                    request.authorization_actor_id,
                    request.canonical_action,
                    request.approval_class,
                    request.command_revision,
                    request.expected_runtime_version,
                    effective_runtime_version,
                    turn_expected_control_version,
                    request.expected_lifecycle,
                    request.expected_phase,
                    targets_json,
                    boundary_json,
                    request.expires_at,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ApprovalConflictError("approval persistence conflict") from exc
        row = conn.execute(
            """
            SELECT * FROM project_approvals
            WHERE approval_id = ? AND operation_id IS NULL
            """,
            (request.approval_id,),
        ).fetchone()
        if row is None or not _row_matches_immutable_request(
            row,
            request,
            targets_json=targets_json,
            boundary_json=boundary_json,
        ) or row["effective_runtime_version"] != effective_runtime_version:
            raise ApprovalConflictError(
                "approval id or immutable batch boundary already exists"
            )
        if (
            row["turn_expected_control_version"]
            != turn_expected_control_version
        ):
            raise ApprovalConflictError(
                "approval id or immutable control boundary already exists"
            )
    result = _approval_from_row(row)
    if result.targets != canonical_targets:
        raise ApprovalConflictError("stored approval targets are not canonical")
    return result


def create_approval_request(
    conn: sqlite3.Connection, request: ApprovalRequest, *, now: int
) -> ApprovalRequest:
    """Insert-or-read a generic approval whose live version is unchanged."""
    if not isinstance(request, ApprovalRequest):
        raise ValueError("request must be an ApprovalRequest")
    return _create_approval_request(
        conn,
        request,
        now=now,
        effective_runtime_version=request.expected_runtime_version,
    )


def _expire_approvals(conn: sqlite3.Connection, now: int) -> None:
    conn.execute(
        """
        UPDATE project_approvals
        SET status = 'expired'
        WHERE operation_id IS NULL
          AND expires_at <= ?
          AND consumed_at IS NULL
          AND status IN ('pending', 'approved')
        """,
        (now,),
    )


def resolve_approval(
    conn: sqlite3.Connection,
    *,
    approval_id: str,
    resolver: ActorContext,
    outcome: Literal["approved", "denied"],
    now: int,
) -> ApprovalRequest | None:
    """Resolve once through an exact durable Desktop/Discord owner binding."""
    if not (
        isinstance(resolver, ActorContext)
        and type(resolver.surface) is str
        and bool(resolver.surface)
        and resolver.surface in {"desktop", "discord"}
        and resolver.is_owner is True
        and type(resolver.actor_id) is str
        and bool(resolver.actor_id)
        and type(resolver.binding_id) is str
        and bool(resolver.binding_id)
        and type(approval_id) is str
        and bool(approval_id)
        and type(outcome) is str
        and bool(outcome)
        and outcome in {"approved", "denied"}
        and type(now) is int
    ):
        return None
    with write_transaction(conn):
        _expire_approvals(conn, now)
        cursor = conn.execute(
            """
            UPDATE project_approvals AS approval
            SET status = ?, resolved_at = ?, resolved_by_actor_id = ?
            WHERE approval_id = ?
              AND approval.operation_id IS NULL
              AND actor_id = ?
              AND authorization_actor_id = ?
              AND canonical_action IS NOT NULL
              AND (
                  approval.turn_id IS NULL
                  OR approval.turn_expected_control_version IS NOT NULL
              )
              AND status = 'pending'
              AND expires_at > ?
              AND EXISTS (
                  SELECT 1
                  FROM project_runtime_state AS state
                  WHERE state.project_id = approval.project_id
                    AND state.version = approval.effective_runtime_version
                    AND state.lifecycle = approval.expected_lifecycle
                    AND state.current_phase = approval.expected_phase
              )
              AND EXISTS (
                  SELECT 1
                  FROM project_surface_bindings AS binding
                  WHERE binding.project_id = approval.project_id
                    AND binding.binding_id = ?
                    AND binding.surface = ?
                    AND binding.actor_id = ?
              )
            """,
            (
                outcome,
                now,
                resolver.actor_id,
                approval_id,
                resolver.actor_id,
                resolver.actor_id,
                now,
                resolver.binding_id,
                resolver.surface,
                resolver.actor_id,
            ),
        )
        if cursor.rowcount != 1:
            return None
        row = conn.execute(
            """
            SELECT * FROM project_approvals
            WHERE approval_id = ? AND operation_id IS NULL
            """,
            (approval_id,),
        ).fetchone()
    assert row is not None
    return _approval_from_row(row)


def _approval_match_parameters(
    *,
    approval_id: str,
    project_id: str,
    authorization_actor_id: str,
    canonical_action: str,
    approval_class: str,
    command_revision: int,
    expected_runtime_version: int,
    expected_lifecycle: Lifecycle,
    expected_phase: str,
    targets: tuple[str, ...],
    batch_id: str,
    batch_items: tuple[str, ...],
) -> tuple[object, ...] | None:
    if not all(
        type(value) is str and bool(value)
        for value in (
            approval_id,
            project_id,
            authorization_actor_id,
            canonical_action,
            approval_class,
            batch_id,
        )
    ):
        return None
    if type(command_revision) is not int or command_revision <= 0:
        return None
    if (
        type(expected_runtime_version) is not int
        or expected_runtime_version < 0
        or type(expected_lifecycle) is not str
        or not expected_lifecycle
        or expected_lifecycle not in {
            "active",
            "awaiting_acceptance",
            "completed",
        }
        or type(expected_phase) is not str
        or not expected_phase
    ):
        return None
    canonical_targets = canonicalize_targets(targets)
    if canonical_targets is None or not canonical_targets:
        return None
    try:
        valid_items = _canonical_items(batch_items, field_name="batch_items")
    except ValueError:
        return None
    return (
        approval_id,
        project_id,
        authorization_actor_id,
        canonical_action,
        approval_class,
        command_revision,
        expected_runtime_version,
        expected_lifecycle,
        expected_phase,
        _canonical_json_array(canonical_targets),
        _canonical_boundary_json(
            authorization_actor_id=authorization_actor_id,
            canonical_action=canonical_action,
            batch_id=batch_id,
            batch_items=valid_items,
            expected_runtime_version=expected_runtime_version,
            expected_lifecycle=expected_lifecycle,
            expected_phase=expected_phase,
        ),
    )


def consume_approval_authorization(
    conn: sqlite3.Connection,
    *,
    approval_id: str,
    project_id: str,
    authorization_actor_id: str,
    canonical_action: str,
    approval_class: str,
    command_revision: int,
    expected_runtime_version: int,
    expected_lifecycle: Lifecycle,
    expected_phase: str,
    targets: tuple[str, ...],
    batch_id: str,
    batch_items: tuple[str, ...],
    now: int,
) -> bool:
    """Atomically consume execution authority for one exact actor/action batch."""
    if type(now) is not int:
        return False
    parameters = _approval_match_parameters(
        approval_id=approval_id,
        project_id=project_id,
        authorization_actor_id=authorization_actor_id,
        canonical_action=canonical_action,
        approval_class=approval_class,
        command_revision=command_revision,
        expected_runtime_version=expected_runtime_version,
        expected_lifecycle=expected_lifecycle,
        expected_phase=expected_phase,
        targets=targets,
        batch_id=batch_id,
        batch_items=batch_items,
    )
    if parameters is None:
        return False
    with write_transaction(conn):
        _expire_approvals(conn, now)
        cursor = conn.execute(
            """
            UPDATE project_approvals AS approval
            SET consumed_at = ?
            WHERE approval_id = ?
              AND approval.operation_id IS NULL
              AND project_id = ?
              AND authorization_actor_id = ?
              AND canonical_action = ?
              AND approval_class = ?
              AND command_revision = ?
              AND expected_runtime_version = ?
              AND expected_lifecycle = ?
              AND expected_phase = ?
              AND targets_json = ?
              AND batch_boundary_json = ?
              AND (
                  approval.turn_id IS NULL
                  OR approval.turn_expected_control_version IS NOT NULL
              )
              AND status = 'approved'
              AND expires_at > ?
              AND consumed_at IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM project_runtime_state AS state
                  WHERE state.project_id = approval.project_id
                    AND state.version = approval.effective_runtime_version
                    AND state.lifecycle = approval.expected_lifecycle
                    AND state.current_phase = approval.expected_phase
              )
            """,
            (
                now,
                *parameters,
                now,
            ),
        )
    return cursor.rowcount == 1
