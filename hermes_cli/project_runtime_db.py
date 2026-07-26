"""Durable SQLite schema primitives for per-project runtime state.

This module owns persistence structure only. Runtime policy, queueing,
delivery, worker, and provider behavior belong to later service layers.
"""

from __future__ import annotations

import contextlib
import json
import re
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator, Literal, Optional

from hermes_cli.sqlite_util import write_txn

if TYPE_CHECKING:
    from hermes_cli.project_policy import ActorContext


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
    approval_class      TEXT NOT NULL,
    command_revision    INTEGER NOT NULL,
    targets_json        TEXT NOT NULL,
    batch_boundary_json TEXT NOT NULL,
    status              TEXT NOT NULL,
    expires_at          INTEGER NOT NULL,
    resolved_at         INTEGER,
    resolved_by_actor_id TEXT,
    consumed_at         INTEGER,
    created_at          INTEGER NOT NULL,
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
    approval_class: str
    command_revision: int
    targets: tuple[str, ...]
    batch_id: str
    batch_items: tuple[str, ...]
    status: ApprovalStatus
    expires_at: int
    resolved_by_actor_id: str | None = None
    resolved_at: int | None = None
    consumed_at: int | None = None


def runtime_state_from_row(row: sqlite3.Row) -> RuntimeState:
    """Map a runtime-state SQLite row to its immutable representation."""
    return RuntimeState(
        project_id=row["project_id"],
        lifecycle=row["lifecycle"],
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
    _ensure_approval_columns(conn)


def _ensure_approval_columns(conn: sqlite3.Connection) -> None:
    """Add Task-2 approval fields to a Task-1 database without data loss."""
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(project_approvals)")
    }
    for name, definition in (
        ("resolved_by_actor_id", "TEXT"),
        ("consumed_at", "INTEGER"),
    ):
        if name not in columns:
            conn.execute(f"ALTER TABLE project_approvals ADD COLUMN {name} {definition}")


@contextlib.contextmanager
def write_transaction(
    conn: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    """Use the shared IMMEDIATE transaction, without nesting another BEGIN."""
    if conn.in_transaction:
        yield conn
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
    conversation_root_id: str,
    conversation_tip_id: str,
    updated_at: int,
) -> RuntimeState:
    """Explicitly adopt a catalog project into the active runtime lifecycle."""
    conn.execute(
        """
        INSERT INTO project_runtime_state (
            project_id,
            lifecycle,
            version,
            conversation_root_id,
            conversation_tip_id,
            updated_at
        ) VALUES (?, 'active', 0, ?, ?, ?)
        """,
        (
            project_id,
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


def _canonical_string_array(
    values: object, *, field_name: str, require_items: bool
) -> tuple[str, ...]:
    if not isinstance(values, tuple) or (require_items and not values):
        raise ValueError(f"{field_name} must be a non-empty tuple")
    if any(not isinstance(value, str) or not value for value in values):
        raise ValueError(f"{field_name} must contain non-empty strings")
    if len(set(values)) != len(values):
        raise ValueError(f"{field_name} must not contain duplicates")
    if field_name == "targets" and any(
        not _is_canonical_target(value) for value in values
    ):
        raise ValueError("targets must be canonical paths")
    return values


def _is_canonical_target(value: str) -> bool:
    if not re.fullmatch(
        r"(?:[A-Za-z]:/(?:[^/]+(?:/[^/]+)*)?|/(?:[^/]+(?:/[^/]+)*)?)", value
    ):
        return False
    path_parts = value[3:] if len(value) > 1 and value[1:3] == ":/" else value[1:]
    return all(part not in {".", ".."} for part in path_parts.split("/"))


def _canonical_json_array(values: tuple[str, ...]) -> str:
    return json.dumps(values, separators=(",", ":"), ensure_ascii=False)


def _validate_approval_request(request: ApprovalRequest, now: int) -> None:
    if not isinstance(request, ApprovalRequest):
        raise ValueError("request must be an ApprovalRequest")
    if not all(
        isinstance(value, str) and value
        for value in (
            request.approval_id,
            request.project_id,
            request.requester_actor_id,
            request.approval_class,
            request.batch_id,
        )
    ):
        raise ValueError("approval identity fields must be non-empty strings")
    if not isinstance(request.command_revision, int) or request.command_revision <= 0:
        raise ValueError("command_revision must be a positive integer")
    if request.status != "pending":
        raise ValueError("new approvals must be pending")
    if not isinstance(request.expires_at, int) or request.expires_at <= now:
        raise ValueError("approval expiry must be in the future")
    _canonical_string_array(request.targets, field_name="targets", require_items=True)
    _canonical_string_array(
        request.batch_items, field_name="batch_items", require_items=True
    )


def _approval_from_row(row: sqlite3.Row) -> ApprovalRequest:
    return ApprovalRequest(
        approval_id=row["approval_id"],
        project_id=row["project_id"],
        requester_actor_id=row["actor_id"],
        approval_class=row["approval_class"],
        command_revision=row["command_revision"],
        targets=tuple(json.loads(row["targets_json"])),
        batch_id=json.loads(row["batch_boundary_json"])["batch_id"],
        batch_items=tuple(json.loads(row["batch_boundary_json"])["batch_items"]),
        status=row["status"],
        expires_at=row["expires_at"],
        resolved_by_actor_id=row["resolved_by_actor_id"],
        resolved_at=row["resolved_at"],
        consumed_at=row["consumed_at"],
    )


def create_approval_request(
    conn: sqlite3.Connection, request: ApprovalRequest, *, now: int
) -> ApprovalRequest:
    """Persist one pending approval with its complete, ordered batch boundary."""
    _validate_approval_request(request, now)
    targets_json = _canonical_json_array(request.targets)
    boundary_json = json.dumps(
        {"batch_id": request.batch_id, "batch_items": request.batch_items},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    with write_transaction(conn):
        conn.execute(
            """
            INSERT INTO project_approvals (
                approval_id, project_id, actor_id, approval_class,
                command_revision, targets_json, batch_boundary_json, status,
                expires_at, resolved_at, resolved_by_actor_id, consumed_at,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL, ?)
            """,
            (
                request.approval_id,
                request.project_id,
                request.requester_actor_id,
                request.approval_class,
                request.command_revision,
                targets_json,
                boundary_json,
                request.expires_at,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM project_approvals WHERE approval_id = ?",
            (request.approval_id,),
        ).fetchone()
    assert row is not None
    return _approval_from_row(row)


def _expire_pending_approvals(conn: sqlite3.Connection, now: int) -> None:
    conn.execute(
        """
        UPDATE project_approvals
        SET status = 'expired'
        WHERE status = 'pending' AND expires_at <= ?
        """,
        (now,),
    )


def resolve_approval(
    conn: sqlite3.Connection,
    *,
    approval_id: str,
    resolver: "ActorContext",
    outcome: Literal["approved", "denied"],
    now: int,
) -> ApprovalRequest | None:
    """Resolve a pending approval once, only by its requesting project owner."""
    if (
        outcome not in {"approved", "denied"}
        or not isinstance(approval_id, str)
        or not approval_id
        or getattr(resolver, "is_owner", False) is not True
        or not isinstance(getattr(resolver, "actor_id", None), str)
        or not resolver.actor_id
    ):
        return None
    with write_transaction(conn):
        _expire_pending_approvals(conn, now)
        cursor = conn.execute(
            """
            UPDATE project_approvals
            SET status = ?, resolved_at = ?, resolved_by_actor_id = ?
            WHERE approval_id = ?
              AND actor_id = ?
              AND status = 'pending'
              AND expires_at > ?
            """,
            (outcome, now, resolver.actor_id, approval_id, resolver.actor_id, now),
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
    approval_class: str,
    command_revision: int,
    targets: tuple[str, ...],
    batch_id: str,
    batch_items: tuple[str, ...],
) -> tuple[object, ...] | None:
    if not all(isinstance(value, str) and value for value in (approval_id, project_id, approval_class, batch_id)):
        return None
    if not isinstance(command_revision, int) or command_revision <= 0:
        return None
    try:
        valid_targets = _canonical_string_array(
            targets, field_name="targets", require_items=True
        )
        valid_items = _canonical_string_array(
            batch_items, field_name="batch_items", require_items=True
        )
    except ValueError:
        return None
    boundary_json = json.dumps(
        {"batch_id": batch_id, "batch_items": valid_items},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return (
        approval_id,
        project_id,
        approval_class,
        command_revision,
        _canonical_json_array(valid_targets),
        boundary_json,
    )


def approval_authorizes(
    conn: sqlite3.Connection,
    *,
    approval_id: str,
    project_id: str,
    approval_class: str,
    command_revision: int,
    targets: tuple[str, ...],
    batch_id: str,
    batch_items: tuple[str, ...],
    now: int,
) -> bool:
    """Check whether an unconsumed approval exactly authorizes this batch."""
    parameters = _approval_match_parameters(
        approval_id=approval_id,
        project_id=project_id,
        approval_class=approval_class,
        command_revision=command_revision,
        targets=targets,
        batch_id=batch_id,
        batch_items=batch_items,
    )
    if parameters is None:
        return False
    with write_transaction(conn):
        _expire_pending_approvals(conn, now)
        row = conn.execute(
            """
            SELECT 1 FROM project_approvals
            WHERE approval_id = ? AND project_id = ? AND approval_class = ?
              AND command_revision = ? AND targets_json = ?
              AND batch_boundary_json = ? AND status = 'approved'
              AND expires_at > ? AND consumed_at IS NULL
            """,
            (*parameters, now),
        ).fetchone()
    return row is not None


def consume_approval_authorization(
    conn: sqlite3.Connection,
    *,
    approval_id: str,
    project_id: str,
    approval_class: str,
    command_revision: int,
    targets: tuple[str, ...],
    batch_id: str,
    batch_items: tuple[str, ...],
    now: int,
) -> bool:
    """Atomically consume exactly one approval bound to the supplied batch."""
    parameters = _approval_match_parameters(
        approval_id=approval_id,
        project_id=project_id,
        approval_class=approval_class,
        command_revision=command_revision,
        targets=targets,
        batch_id=batch_id,
        batch_items=batch_items,
    )
    if parameters is None:
        return False
    with write_transaction(conn):
        _expire_pending_approvals(conn, now)
        cursor = conn.execute(
            """
            UPDATE project_approvals
            SET consumed_at = ?
            WHERE approval_id = ? AND project_id = ? AND approval_class = ?
              AND command_revision = ? AND targets_json = ?
              AND batch_boundary_json = ? AND status = 'approved'
              AND expires_at > ? AND consumed_at IS NULL
            """,
            (now, *parameters, now),
        )
    return cursor.rowcount == 1
