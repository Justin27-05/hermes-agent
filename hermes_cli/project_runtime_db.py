"""Durable SQLite schema primitives for per-project runtime state.

This module owns persistence structure only. Runtime policy, queueing,
delivery, worker, and provider behavior belong to later service layers.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
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
    status           TEXT NOT NULL,
    receipt_json     TEXT,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
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


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create all additive ProjectRuntime tables without adopting projects."""
    conn.execute("PRAGMA foreign_keys=ON")
    execute_schema_statements(conn, RUNTIME_SCHEMA_SQL)
    _ensure_runtime_state_columns(conn)
    _ensure_approval_columns(conn)
    _ensure_run_control_columns(conn)
    _validate_existing_lineage(conn)
    try:
        execute_schema_statements(conn, LINEAGE_INDEX_SQL)
        execute_schema_statements(conn, TASK4_INDEX_SQL)
    except sqlite3.IntegrityError as exc:
        raise LineageMigrationError(
            "multiple conversation roots exist for one project"
        ) from exc


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
        and _stored_int(row["created_at"])
        and _stored_int(row["updated_at"])
    ):
        raise RuntimeError("malformed persisted runtime turn")
    if (
        row["attempt_id"] is None
        and (
            row["lease_generation"] != 0
            or row["fencing_token"] != 0
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
    """Claim only the exact FIFO head; Task 5 owns expiry and takeover."""
    rows = conn.execute(
        """
        SELECT * FROM project_turns
        WHERE project_id = ?
        ORDER BY sequence, turn_id
        """,
        (project_id,),
    ).fetchall()
    turns: list[RuntimeTurnRecord] = []
    controls: dict[str, RuntimeControlRecord] = {}
    for row in rows:
        stored_turn = runtime_turn_from_row(row)
        turns.append(stored_turn)
        controls[stored_turn.turn_id] = _validate_runtime_turn_pair(
            conn, turn=stored_turn
        )
    turn = next(
        (
            stored_turn
            for stored_turn in turns
            if stored_turn.status not in _TERMINAL_TURN_STATUSES
        ),
        None,
    )
    if turn is None:
        return None
    if turn.status != "queued":
        return None
    control = controls[turn.turn_id]
    generation = turn.lease_generation + 1
    fence = turn.fencing_token + 1
    lease_expires_at = now + lease_seconds
    if conn.execute(
        """
        UPDATE project_turns
        SET status = 'claimed', attempt_id = ?, lease_generation = ?,
            fencing_token = ?, updated_at = ?
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
            "SELECT * FROM project_approvals WHERE approval_id = ?",
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
        WHERE expires_at <= ?
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
              AND actor_id = ?
              AND authorization_actor_id = ?
              AND canonical_action IS NOT NULL
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
            "SELECT * FROM project_approvals WHERE approval_id = ?",
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
