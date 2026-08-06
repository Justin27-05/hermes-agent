"""Durable, capability-free reconciliation ledger for project surfaces."""

from __future__ import annotations

import contextlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Iterator, Literal

from hermes_cli.sqlite_util import write_txn


_PENDING_STATUSES = frozenset(
    {"prepared", "effect_started", "sync_pending"}
)
_OUTCOMES = frozenset(
    {
        "exact",
        "pending",
        "partial",
        "ambiguous",
        "foreign",
        "collision",
        "blocked",
    }
)
_TERMINAL_STATUSES = frozenset({"synchronized", "blocked"})
_SELECT_OPERATION = """
SELECT
    operation_id,
    project_id,
    lifecycle_event_id,
    kind,
    desired_json,
    prestate_json,
    ownership_marker,
    status,
    external_channel_id,
    readback_json,
    last_outcome,
    blocked_reason,
    created_at,
    updated_at
FROM project_surface_operations
"""


class SurfaceOperationConflict(ValueError):
    """An immutable operation contract or lifecycle transition was violated."""


class SurfaceChannelCollision(SurfaceOperationConflict):
    """A durable project/channel identity is already owned elsewhere."""


@dataclass(frozen=True)
class SurfaceOperation:
    operation_id: str
    project_id: str
    lifecycle_event_id: str
    kind: str
    desired_json: str
    prestate_json: str
    ownership_marker: str
    status: str
    external_channel_id: str | None
    readback_json: str | None
    last_outcome: str | None
    blocked_reason: str | None
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class SurfaceEffectClaim:
    operation_id: str
    project_id: str
    holder_id: str
    fencing_token: int
    lease_expires_at: int


def _identity(value: object, name: str) -> str:
    if type(value) is not str or not value.strip():
        raise SurfaceOperationConflict(f"{name} must be non-empty text")
    return value.strip()


def _reject_json_constant(token: str) -> object:
    raise ValueError(f"non-finite JSON constant: {token}")


def _reject_duplicate_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _canonical_json_object(value: object, name: str) -> str:
    if type(value) is not str:
        raise SurfaceOperationConflict(f"{name} must be JSON text")
    try:
        parsed = json.loads(
            value,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError) as exc:
        raise SurfaceOperationConflict(
            f"{name} must be strict valid JSON"
        ) from exc
    if not isinstance(parsed, dict):
        raise SurfaceOperationConflict(f"{name} must be a JSON object")
    return json.dumps(
        parsed,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def init_schema(conn: sqlite3.Connection) -> None:
    """Install the additive ledger after projects and runtime events exist."""
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS project_surface_channel_claims (
            external_channel_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL UNIQUE,
            created_at INTEGER NOT NULL DEFAULT (unixepoch()),
            UNIQUE (project_id, external_channel_id),
            FOREIGN KEY (project_id)
                REFERENCES projects(id) ON DELETE RESTRICT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS project_surface_operations (
            operation_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            lifecycle_event_id TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL,
            desired_json TEXT NOT NULL,
            prestate_json TEXT NOT NULL,
            ownership_marker TEXT NOT NULL,
            status TEXT NOT NULL CHECK (
                status IN (
                    'prepared',
                    'effect_started',
                    'sync_pending',
                    'synchronized',
                    'blocked'
                )
            ),
            external_channel_id TEXT,
            readback_json TEXT,
            last_outcome TEXT,
            blocked_reason TEXT,
            created_at INTEGER NOT NULL DEFAULT (unixepoch()),
            updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
            UNIQUE (project_id, lifecycle_event_id),
            FOREIGN KEY (project_id)
                REFERENCES projects(id) ON DELETE RESTRICT,
            FOREIGN KEY (project_id, lifecycle_event_id)
                REFERENCES project_events(project_id, event_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (project_id, external_channel_id)
                REFERENCES project_surface_channel_claims(
                    project_id,
                    external_channel_id
                )
                ON DELETE RESTRICT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_project_surface_operations_recovery
        ON project_surface_operations(status, created_at, operation_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS project_surface_operation_leases (
            project_id TEXT PRIMARY KEY,
            operation_id TEXT,
            holder_id TEXT,
            fencing_token INTEGER NOT NULL DEFAULT 0
                CHECK (fencing_token >= 0),
            lease_expires_at INTEGER,
            updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
            FOREIGN KEY (project_id)
                REFERENCES projects(id) ON DELETE RESTRICT,
            FOREIGN KEY (operation_id)
                REFERENCES project_surface_operations(operation_id)
                ON DELETE RESTRICT,
            CHECK (
                (
                    operation_id IS NULL
                    AND holder_id IS NULL
                    AND lease_expires_at IS NULL
                )
                OR (
                    operation_id IS NOT NULL
                    AND holder_id IS NOT NULL
                    AND lease_expires_at IS NOT NULL
                )
            )
        )
        """
    )


@contextlib.contextmanager
def _write_boundary(
    conn: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    if not conn.in_transaction:
        with write_txn(conn):
            yield conn
        return
    savepoint = "project_surface_operation_nested"
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


def _operation_from_row(row: sqlite3.Row | tuple[object, ...]) -> SurfaceOperation:
    if isinstance(row, sqlite3.Row):
        values = tuple(row[field] for field in SurfaceOperation.__dataclass_fields__)
    else:
        values = tuple(row)
    return SurfaceOperation(*values)


def _get(
    conn: sqlite3.Connection,
    operation_id: object,
) -> SurfaceOperation:
    row = conn.execute(
        _SELECT_OPERATION + " WHERE operation_id = ?",
        (_identity(operation_id, "operation_id"),),
    ).fetchone()
    if row is None:
        raise SurfaceOperationConflict("unknown surface operation")
    return _operation_from_row(row)


def operation_for_lifecycle_event(
    conn: sqlite3.Connection,
    *,
    project_id: object,
    lifecycle_event_id: object,
) -> SurfaceOperation | None:
    """Read the immutable operation already prepared for one canonical event."""
    row = conn.execute(
        _SELECT_OPERATION
        + " WHERE project_id = ? AND lifecycle_event_id = ?",
        (
            _identity(project_id, "project_id"),
            _identity(lifecycle_event_id, "lifecycle_event_id"),
        ),
    ).fetchone()
    return _operation_from_row(row) if row is not None else None


def _operation_contract(operation: SurfaceOperation) -> tuple[str, ...]:
    return (
        operation.operation_id,
        operation.project_id,
        operation.lifecycle_event_id,
        operation.kind,
        operation.desired_json,
        operation.prestate_json,
        operation.ownership_marker,
    )


def prepare_or_replay(
    conn: sqlite3.Connection,
    *,
    operation_id: object,
    project_id: object,
    lifecycle_event_id: object,
    kind: object,
    desired_json: object,
    prestate_json: object,
    ownership_marker: object,
) -> SurfaceOperation:
    """Create one immutable operation or replay its exact contract."""
    contract = (
        _identity(operation_id, "operation_id"),
        _identity(project_id, "project_id"),
        _identity(lifecycle_event_id, "lifecycle_event_id"),
        _identity(kind, "kind"),
        _canonical_json_object(desired_json, "desired_json"),
        _canonical_json_object(prestate_json, "prestate_json"),
        _identity(ownership_marker, "ownership_marker"),
    )
    with _write_boundary(conn):
        row = conn.execute(
            _SELECT_OPERATION
            + " WHERE operation_id = ? OR lifecycle_event_id = ?",
            (contract[0], contract[2]),
        ).fetchone()
        if row is not None:
            operation = _operation_from_row(row)
            if _operation_contract(operation) != contract:
                raise SurfaceOperationConflict(
                    "surface operation/event contract drift"
                )
            return operation
        try:
            conn.execute(
                """
                INSERT INTO project_surface_operations (
                    operation_id,
                    project_id,
                    lifecycle_event_id,
                    kind,
                    desired_json,
                    prestate_json,
                    ownership_marker,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'prepared')
                """,
                contract,
            )
        except sqlite3.IntegrityError as exc:
            raise SurfaceOperationConflict(
                "surface operation authority reference is invalid"
            ) from exc
        return _get(conn, contract[0])


def claim_effect(
    conn: sqlite3.Connection,
    operation_id: object,
    *,
    holder_id: object,
    now: object,
    lease_seconds: object,
) -> SurfaceEffectClaim | None:
    """Acquire one exclusive, fenced remote-effect lease per project."""
    holder = _identity(holder_id, "holder_id")
    if (
        type(now) is not int
        or now < 0
        or type(lease_seconds) is not int
        or lease_seconds <= 0
    ):
        raise SurfaceOperationConflict("invalid effect lease")
    expires_at = now + lease_seconds
    with _write_boundary(conn):
        operation = _get(conn, operation_id)
        if operation.status in _TERMINAL_STATUSES:
            return None
        if operation.status not in _PENDING_STATUSES:
            raise SurfaceOperationConflict("surface operation is not claimable")
        earlier = conn.execute(
            """
            SELECT 1
            FROM project_surface_operations AS pending
            JOIN project_events AS event
              ON event.project_id = pending.project_id
             AND event.event_id = pending.lifecycle_event_id
            JOIN project_events AS candidate
              ON candidate.project_id = ?
             AND candidate.event_id = ?
            WHERE pending.project_id = ?
              AND pending.operation_id != ?
              AND pending.status IN (
                  'prepared', 'effect_started', 'sync_pending'
              )
              AND event.sequence < candidate.sequence
            LIMIT 1
            """,
            (
                operation.project_id,
                operation.lifecycle_event_id,
                operation.project_id,
                operation.operation_id,
            ),
        ).fetchone()
        if earlier is not None:
            return None
        lease = conn.execute(
            """
            SELECT operation_id, holder_id, fencing_token, lease_expires_at
            FROM project_surface_operation_leases
            WHERE project_id = ?
            """,
            (operation.project_id,),
        ).fetchone()
        if (
            lease is not None
            and lease["operation_id"] is not None
            and lease["lease_expires_at"] > now
        ):
            return None
        fencing_token = (
            1 if lease is None else lease["fencing_token"] + 1
        )
        conn.execute(
            """
            INSERT INTO project_surface_operation_leases (
                project_id, operation_id, holder_id, fencing_token,
                lease_expires_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
                operation_id = excluded.operation_id,
                holder_id = excluded.holder_id,
                fencing_token = excluded.fencing_token,
                lease_expires_at = excluded.lease_expires_at,
                updated_at = excluded.updated_at
            """,
            (
                operation.project_id,
                operation.operation_id,
                holder,
                fencing_token,
                expires_at,
                now,
            ),
        )
        return SurfaceEffectClaim(
            operation_id=operation.operation_id,
            project_id=operation.project_id,
            holder_id=holder,
            fencing_token=fencing_token,
            lease_expires_at=expires_at,
        )


def _require_effect_claim(
    conn: sqlite3.Connection,
    operation: SurfaceOperation,
    claim: object,
    *,
    now: object,
) -> SurfaceEffectClaim:
    if not isinstance(claim, SurfaceEffectClaim) or type(now) is not int:
        raise SurfaceOperationConflict("surface effect claim is required")
    row = conn.execute(
        """
        SELECT operation_id, holder_id, fencing_token, lease_expires_at
        FROM project_surface_operation_leases
        WHERE project_id = ?
        """,
        (operation.project_id,),
    ).fetchone()
    if (
        row is None
        or claim.operation_id != operation.operation_id
        or claim.project_id != operation.project_id
        or row["operation_id"] != claim.operation_id
        or row["holder_id"] != claim.holder_id
        or row["fencing_token"] != claim.fencing_token
        or row["lease_expires_at"] != claim.lease_expires_at
        or claim.lease_expires_at <= now
    ):
        raise SurfaceOperationConflict("surface effect claim is stale")
    return claim


def _release_effect_claim(
    conn: sqlite3.Connection,
    claim: SurfaceEffectClaim,
    *,
    now: int,
) -> None:
    if conn.execute(
        """
        UPDATE project_surface_operation_leases
        SET operation_id = NULL,
            holder_id = NULL,
            lease_expires_at = NULL,
            updated_at = ?
        WHERE project_id = ?
          AND operation_id = ?
          AND holder_id = ?
          AND fencing_token = ?
          AND lease_expires_at = ?
        """,
        (
            now,
            claim.project_id,
            claim.operation_id,
            claim.holder_id,
            claim.fencing_token,
            claim.lease_expires_at,
        ),
    ).rowcount != 1:
        raise SurfaceOperationConflict("surface effect claim changed")


def mark_effect_started(
    conn: sqlite3.Connection,
    operation_id: object,
    *,
    claim: SurfaceEffectClaim,
    now: int,
) -> SurfaceOperation:
    """Cross the remote-effect boundary once; later replays are read-only."""
    with _write_boundary(conn):
        operation = _get(conn, operation_id)
        if operation.status in _TERMINAL_STATUSES:
            return operation
        _require_effect_claim(conn, operation, claim, now=now)
        if operation.status != "prepared":
            return operation
        conn.execute(
            """
            UPDATE project_surface_operations
            SET status = 'effect_started', updated_at = ?
            WHERE operation_id = ? AND status = 'prepared'
            """,
            (now, operation.operation_id),
        )
        return _get(conn, operation.operation_id)


def renew_effect_claim(
    conn: sqlite3.Connection,
    operation_id: object,
    *,
    claim: SurfaceEffectClaim,
    now: int,
    lease_seconds: int,
) -> SurfaceEffectClaim:
    """Extend exactly one live fenced effect lease without changing its token."""
    if type(now) is not int or now < 0 or type(lease_seconds) is not int or lease_seconds <= 0:
        raise SurfaceOperationConflict("invalid effect lease")
    expires_at = now + lease_seconds
    with _write_boundary(conn):
        operation = _get(conn, operation_id)
        _require_effect_claim(conn, operation, claim, now=now)
        changed = conn.execute(
            """
            UPDATE project_surface_operation_leases
            SET lease_expires_at = ?, updated_at = ?
            WHERE project_id = ? AND operation_id = ? AND holder_id = ?
              AND fencing_token = ? AND lease_expires_at = ?
            """,
            (
                expires_at,
                now,
                claim.project_id,
                claim.operation_id,
                claim.holder_id,
                claim.fencing_token,
                claim.lease_expires_at,
            ),
        )
        if changed.rowcount != 1:
            raise SurfaceOperationConflict("surface effect claim changed")
        return SurfaceEffectClaim(
            operation_id=claim.operation_id,
            project_id=claim.project_id,
            holder_id=claim.holder_id,
            fencing_token=claim.fencing_token,
            lease_expires_at=expires_at,
        )


def _claim_channel(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    external_channel_id: str,
) -> None:
    by_channel = conn.execute(
        """
        SELECT project_id
        FROM project_surface_channel_claims
        WHERE external_channel_id = ?
        """,
        (external_channel_id,),
    ).fetchone()
    if by_channel is not None:
        claimed_project = by_channel[0]
        if claimed_project != project_id:
            raise SurfaceChannelCollision(
                "external channel is claimed by another project"
            )
        return
    by_project = conn.execute(
        """
        SELECT external_channel_id
        FROM project_surface_channel_claims
        WHERE project_id = ?
        """,
        (project_id,),
    ).fetchone()
    if by_project is not None:
        raise SurfaceChannelCollision(
            "project is already bound to another external channel"
        )
    try:
        conn.execute(
            """
            INSERT INTO project_surface_channel_claims (
                external_channel_id,
                project_id
            ) VALUES (?, ?)
            """,
            (external_channel_id, project_id),
        )
    except sqlite3.IntegrityError as exc:
        raise SurfaceChannelCollision(
            "external channel claim conflict"
        ) from exc


def reconcile(
    conn: sqlite3.Connection,
    operation_id: object,
    *,
    claim: SurfaceEffectClaim,
    now: int,
    readback_json: object,
    external_channel_id: object = None,
    blocked_reason: object = None,
    outcome: Literal[
        "exact",
        "pending",
        "partial",
        "ambiguous",
        "foreign",
        "collision",
        "blocked",
    ],
) -> SurfaceOperation:
    """Record exact readback without ever rolling back project lifecycle."""
    if outcome not in _OUTCOMES:
        raise SurfaceOperationConflict("invalid reconciliation outcome")
    readback = _canonical_json_object(readback_json, "readback_json")
    supplied_channel = (
        None
        if external_channel_id is None
        else _identity(external_channel_id, "external_channel_id")
    )
    with _write_boundary(conn):
        operation = _get(conn, operation_id)
        if (
            operation.external_channel_id is not None
            and supplied_channel is not None
            and supplied_channel != operation.external_channel_id
        ):
            raise SurfaceOperationConflict(
                "external channel identity changed"
            )
        channel = operation.external_channel_id or supplied_channel
        if outcome == "exact":
            if channel is None:
                raise SurfaceOperationConflict(
                    "exact reconciliation requires a channel identity"
                )
            next_status = "synchronized"
            resolved_blocked_reason = None
        elif outcome in {"pending", "partial", "ambiguous"}:
            next_status = "sync_pending"
            resolved_blocked_reason = None
            if (
                outcome == "ambiguous"
                and operation.external_channel_id is None
            ):
                channel = None
        else:
            next_status = "blocked"
            resolved_blocked_reason = (
                _identity(blocked_reason, "blocked_reason")
                if blocked_reason is not None
                else outcome
            )
            # A foreign first readback is evidence, not an ownership claim.
            if operation.external_channel_id is None:
                channel = None

        if operation.status in _TERMINAL_STATUSES:
            if (
                operation.status == next_status
                and operation.external_channel_id == channel
                and operation.readback_json == readback
                and operation.last_outcome == outcome
                and operation.blocked_reason == resolved_blocked_reason
            ):
                return operation
            raise SurfaceOperationConflict(
                "terminal surface operation is immutable"
            )
        claim = _require_effect_claim(
            conn, operation, claim, now=now
        )
        if operation.status not in {"effect_started", "sync_pending"}:
            raise SurfaceOperationConflict(
                "surface effect has not started"
            )
        if (
            operation.status == next_status
            and operation.external_channel_id == channel
            and operation.readback_json == readback
            and operation.last_outcome == outcome
        ):
            _release_effect_claim(conn, claim, now=now)
            return operation
        if channel is not None:
            _claim_channel(
                conn,
                project_id=operation.project_id,
                external_channel_id=channel,
            )
        conn.execute(
            """
            UPDATE project_surface_operations
            SET
                status = ?,
                external_channel_id = ?,
                readback_json = ?,
                last_outcome = ?,
                blocked_reason = ?,
                updated_at = ?
            WHERE operation_id = ?
            """,
            (
                next_status,
                channel,
                readback,
                outcome,
                resolved_blocked_reason,
                now,
                operation.operation_id,
            ),
        )
        result = _get(conn, operation.operation_id)
        _release_effect_claim(conn, claim, now=now)
        return result


def pending_for_recovery(
    conn: sqlite3.Connection,
) -> tuple[SurfaceOperation, ...]:
    placeholders = ",".join("?" for _ in _PENDING_STATUSES)
    rows = conn.execute(
        _SELECT_OPERATION
        + f"""
        WHERE status IN ({placeholders})
        ORDER BY created_at, operation_id
        """,
        tuple(sorted(_PENDING_STATUSES)),
    ).fetchall()
    return tuple(_operation_from_row(row) for row in rows)


__all__ = [
    "SurfaceEffectClaim",
    "SurfaceOperation",
    "SurfaceOperationConflict",
    "SurfaceChannelCollision",
    "claim_effect",
    "init_schema",
    "mark_effect_started",
    "operation_for_lifecycle_event",
    "pending_for_recovery",
    "prepare_or_replay",
    "reconcile",
    "renew_effect_claim",
]
