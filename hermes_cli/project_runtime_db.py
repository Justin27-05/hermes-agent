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
    attempt_id       TEXT,
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
        ("expected_lifecycle", "expected_lifecycle TEXT"),
        ("expected_phase", "expected_phase TEXT"),
    ):
        add_column_if_missing(
            conn,
            "project_approvals",
            name,
            ddl,
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


def create_runtime_state(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    current_phase: str,
    conversation_root_id: str,
    conversation_tip_id: str,
    updated_at: int,
) -> RuntimeState:
    """Explicitly adopt a catalog project into the active runtime lifecycle."""
    if type(current_phase) is not str or not current_phase:
        raise ValueError("current_phase must be a non-empty string")
    conn.execute(
        """
        INSERT INTO project_runtime_state (
            project_id,
            lifecycle,
            current_phase,
            version,
            conversation_root_id,
            conversation_tip_id,
            updated_at
        ) VALUES (?, 'active', ?, 0, ?, ?, ?)
        """,
        (
            project_id,
            current_phase,
            conversation_root_id,
            conversation_tip_id,
            updated_at,
        ),
    )
    state = _runtime_state_for_project(conn, project_id)
    assert state is not None
    return state


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


def _approval_storage_values(
    request: ApprovalRequest, now: int
) -> tuple[tuple[str, ...], str, str]:
    if not isinstance(request, ApprovalRequest):
        raise ValueError("request must be an ApprovalRequest")
    if type(now) is not int:
        raise ValueError("now must be an integer timestamp")
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
    if type(request.expires_at) is not int or request.expires_at <= now:
        raise ValueError("approval expiry must be in the future")
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


def create_approval_request(
    conn: sqlite3.Connection, request: ApprovalRequest, *, now: int
) -> ApprovalRequest:
    """Insert-or-read one immutable approval ID; reject every payload collision."""
    canonical_targets, targets_json, boundary_json = _approval_storage_values(
        request, now
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
                    expected_runtime_version, expected_lifecycle, expected_phase,
                    targets_json, batch_boundary_json, status, expires_at,
                    resolved_at, resolved_by_actor_id, consumed_at, created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?,
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
        ):
            raise ApprovalConflictError(
                "approval id or immutable batch boundary already exists"
            )
    result = _approval_from_row(row)
    if result.targets != canonical_targets:
        raise ApprovalConflictError("stored approval targets are not canonical")
    return result


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
                    AND state.version = approval.expected_runtime_version
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
                    AND state.version = ?
                    AND state.lifecycle = ?
                    AND state.current_phase = ?
              )
            """,
            (
                now,
                *parameters,
                now,
                expected_runtime_version,
                expected_lifecycle,
                expected_phase,
            ),
        )
    return cursor.rowcount == 1
