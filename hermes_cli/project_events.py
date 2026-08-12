"""Durable canonical project events, delivery obligations, and artifacts."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid
from typing import Literal

from hermes_cli import project_runtime_db as runtime_db
from hermes_cli.project_runtime import (
    _decode_canonical_object,
    canonical_json_object,
)


JSONScalar = str | int | float | bool | None
JSONValue = JSONScalar | tuple["JSONValue", ...] | Mapping[str, "JSONValue"]
_REDACTED = "[REDACTED]"
_SECRET_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "private_key",
    "privatekey",
    "provider_payload",
    "raw_payload",
    "secret",
    "token",
)
_SQLITE_INT_MAX = (1 << 63) - 1
_SAFE_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


@dataclass(frozen=True)
class ProjectEvent:
    event_id: str
    project_id: str
    sequence: int
    kind: str
    turn_id: str | None
    payload: Mapping[str, JSONValue]
    created_at: str


@dataclass(frozen=True)
class ProjectDeliveryClaim:
    delivery_id: str
    project_id: str
    binding_id: str
    event: ProjectEvent
    attempt: int
    lease_expires_at: int


@dataclass(frozen=True)
class ProjectArtifact:
    artifact_id: str
    project_id: str
    turn_id: str | None
    path: str
    metadata: Mapping[str, JSONValue]
    status: Literal["verified"]
    verified_at: str
    created_at: str


class ProjectEventIntegrityError(RuntimeError):
    """Stored event or delivery state cannot be trusted."""


class ProjectDeliveryConflictError(RuntimeError):
    """A delivery claim no longer owns its durable obligation."""


def _require_text(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _timestamp_text(value: object) -> str:
    if type(value) is not int or value < 0:
        raise ProjectEventIntegrityError("invalid canonical event timestamp")
    try:
        return (
            datetime.fromtimestamp(value, tz=timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
    except (OverflowError, OSError, ValueError) as exc:
        raise ProjectEventIntegrityError(
            "invalid canonical event timestamp"
        ) from exc


def _redact_payload(value: object) -> object:
    if isinstance(value, Mapping):
        redacted: dict[str, object] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("project event payload keys must be strings")
            normalized = key.casefold().replace("-", "_")
            redacted[key] = (
                _REDACTED
                if any(part in normalized for part in _SECRET_KEY_PARTS)
                else _redact_payload(item)
            )
        return redacted
    if type(value) in {list, tuple}:
        return [_redact_payload(item) for item in value]
    return value


def _canonical_redacted_mapping(
    value: Mapping[str, object],
) -> tuple[str, Mapping[str, JSONValue]]:
    if not isinstance(value, Mapping):
        raise ValueError("project event payload must be a mapping")
    redacted = _redact_payload(value)
    if type(redacted) is not dict:
        raise ValueError("project event payload must be an object")
    encoded = canonical_json_object(redacted)
    decoded = _decode_canonical_object(encoded)
    return encoded, decoded


def _event_from_row(row: sqlite3.Row) -> ProjectEvent:
    event_id = _require_text(row["event_id"], "event_id")
    project_id = _require_text(row["project_id"], "project_id")
    kind = _require_text(row["kind"], "kind")
    sequence = row["sequence"]
    turn_id = row["turn_id"]
    if (
        type(sequence) is not int
        or sequence <= 0
        or not (turn_id is None or (type(turn_id) is str and turn_id))
    ):
        raise ProjectEventIntegrityError("malformed canonical project event")
    try:
        payload = _decode_canonical_object(row["payload_json"])
    except RuntimeError as exc:
        raise ProjectEventIntegrityError(
            "malformed canonical project payload"
        ) from exc
    return ProjectEvent(
        event_id=event_id,
        project_id=project_id,
        sequence=sequence,
        kind=kind,
        turn_id=turn_id,
        payload=payload,
        created_at=_timestamp_text(row["created_at"]),
    )


def _artifact_from_row(row: sqlite3.Row) -> ProjectArtifact:
    if row["status"] != "verified" or type(row["verified_at"]) is not int:
        raise ProjectEventIntegrityError("artifact is not verified")
    try:
        metadata = _decode_canonical_object(row["metadata_json"])
    except RuntimeError as exc:
        raise ProjectEventIntegrityError(
            "malformed project artifact metadata"
        ) from exc
    return ProjectArtifact(
        artifact_id=_require_text(row["artifact_id"], "artifact_id"),
        project_id=_require_text(row["project_id"], "project_id"),
        turn_id=row["turn_id"],
        path=_require_text(row["path"], "path"),
        metadata=metadata,
        status="verified",
        verified_at=_timestamp_text(row["verified_at"]),
        created_at=_timestamp_text(row["created_at"]),
    )


class ProjectEventOutbox:
    """The transaction-owning ProjectRuntime event/outbox boundary."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        clock: Callable[[], int] | None = None,
        id_factory: Callable[[str], str] | None = None,
    ) -> None:
        if not isinstance(conn, sqlite3.Connection):
            raise TypeError("conn must be a sqlite3.Connection")
        self._conn = conn
        self._clock = clock or (lambda: int(time.time()))
        self._id_factory = id_factory or (
            lambda kind: f"{kind}-{uuid.uuid4().hex}"
        )

    def _now(self) -> int:
        now = self._clock()
        if type(now) is not int or now < 0:
            raise RuntimeError("project event clock returned invalid time")
        return now

    def _require_managed_project(self, project_id: str) -> None:
        _require_text(project_id, "project_id")
        if runtime_db.runtime_state_for_project(
            self._conn,
            project_id,
        ) is None:
            raise ValueError("project is not managed")

    def append_event(
        self,
        project_id: str,
        kind: str,
        payload: Mapping[str, object],
        *,
        turn_id: str | None = None,
        event_id: str | None = None,
    ) -> ProjectEvent:
        """Append one event and all current binding obligations atomically."""
        self._require_managed_project(project_id)
        _require_text(kind, "kind")
        if turn_id is not None:
            _require_text(turn_id, "turn_id")
        payload_json, _ = _canonical_redacted_mapping(payload)
        event_id = _require_text(
            event_id or self._id_factory("event"),
            "event_id",
        )
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            runtime_db._append_runtime_event(
                self._conn,
                event_id=event_id,
                project_id=project_id,
                kind=kind,
                turn_id=turn_id,
                payload_json=payload_json,
                created_at=now,
            )
            row = self._conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? AND event_id = ?
                """,
                (project_id, event_id),
            ).fetchone()
            if row is None:
                raise ProjectEventIntegrityError(
                    "canonical event disappeared during append"
                )
            return _event_from_row(row)

    def events_after(
        self,
        project_id: str,
        cursor: int,
        limit: int,
    ) -> tuple[ProjectEvent, ...]:
        self._require_managed_project(project_id)
        if type(cursor) is not int or cursor < 0:
            raise ValueError("event cursor must be a non-negative integer")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("event limit must be between 1 and 1000")
        rows = self._conn.execute(
            """
            SELECT * FROM project_events
            WHERE project_id = ? AND sequence > ?
            ORDER BY sequence, event_id
            LIMIT ?
            """,
            (project_id, cursor, limit),
        ).fetchall()
        events = tuple(_event_from_row(row) for row in rows)
        expected = cursor + 1
        for event in events:
            if event.project_id != project_id or event.sequence != expected:
                raise ProjectEventIntegrityError(
                    "canonical project event sequence has a gap"
                )
            expected += 1
        return events

    def claim_delivery(
        self,
        project_id: str,
        binding_id: str,
        *,
        lease_seconds: int,
    ) -> ProjectDeliveryClaim | None:
        self._require_managed_project(project_id)
        _require_text(binding_id, "binding_id")
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("delivery lease must be a positive integer")
        now = self._now()
        lease_expires_at = now + lease_seconds
        if lease_expires_at > _SQLITE_INT_MAX:
            raise ValueError("delivery lease exceeds SQLite range")
        with runtime_db.write_transaction(self._conn):
            row = self._conn.execute(
                """
                SELECT delivery.*, event.*
                FROM project_deliveries AS delivery
                JOIN project_events AS event
                  ON event.project_id = delivery.project_id
                 AND event.event_id = delivery.event_id
                WHERE delivery.project_id = ?
                  AND delivery.binding_id = ?
                  AND delivery.status NOT IN ('delivered', 'suppressed')
                ORDER BY event.sequence, event.event_id
                LIMIT 1
                """,
                (project_id, binding_id),
            ).fetchone()
            if row is None:
                return None
            previous_status = row["status"]
            previous_attempt = row["attempt_count"]
            previous_lease = row["lease_expires_at"]
            previous_due = row["next_attempt_at"]
            if (
                previous_status
                not in {"pending", "in_flight", "blocked"}
                or type(previous_attempt) is not int
                or previous_attempt < 0
                or row["cursor"] is not None
                or row["remote_message_ids_json"] is not None
            ):
                raise ProjectEventIntegrityError(
                    "malformed delivery obligation"
                )
            if previous_status == "blocked":
                if (
                    previous_lease is not None
                    or previous_due is not None
                    or not _SAFE_ERROR_CODE.fullmatch(
                        str(row["last_error_code"])
                    )
                ):
                    raise ProjectEventIntegrityError(
                        "malformed blocked delivery"
                    )
                return None
            if previous_status == "pending":
                if (
                    previous_lease is not None
                    or not (
                        previous_due is None
                        or (
                            type(previous_due) is int
                            and previous_due >= 0
                        )
                    )
                ):
                    raise ProjectEventIntegrityError(
                        "malformed pending delivery"
                    )
                if previous_due is not None and previous_due > now:
                    return None
            if previous_status == "in_flight":
                if (
                    type(previous_lease) is not int
                    or previous_lease < 0
                    or previous_due is not None
                ):
                    raise ProjectEventIntegrityError(
                        "malformed delivery lease"
                    )
                if previous_lease > now:
                    return None
            changed = self._conn.execute(
                """
                UPDATE project_deliveries
                SET status = 'in_flight', cursor = NULL,
                    lease_expires_at = ?,
                    remote_message_ids_json = NULL,
                    next_attempt_at = NULL,
                    last_error_code = NULL,
                    attempt_count = attempt_count + 1, updated_at = ?
                WHERE project_id = ? AND binding_id = ?
                  AND delivery_id = ? AND event_id = ?
                  AND status = ? AND attempt_count = ?
                  AND lease_expires_at IS ?
                  AND next_attempt_at IS ?
                """,
                (
                    lease_expires_at,
                    now,
                    project_id,
                    binding_id,
                    row["delivery_id"],
                    row["event_id"],
                    previous_status,
                    previous_attempt,
                    previous_lease,
                    previous_due,
                ),
            )
            if changed.rowcount != 1:
                raise ProjectDeliveryConflictError(
                    "delivery obligation changed during claim"
                )
            return ProjectDeliveryClaim(
                delivery_id=row["delivery_id"],
                project_id=project_id,
                binding_id=binding_id,
                event=_event_from_row(row),
                attempt=previous_attempt + 1,
                lease_expires_at=lease_expires_at,
            )

    @staticmethod
    def _require_claim(claim: object) -> ProjectDeliveryClaim:
        if type(claim) is not ProjectDeliveryClaim:
            raise TypeError("claim must be a ProjectDeliveryClaim")
        return claim

    @staticmethod
    def _require_error_code(error_code: object) -> str:
        if (
            type(error_code) is not str
            or _SAFE_ERROR_CODE.fullmatch(error_code) is None
        ):
            raise ValueError("delivery error code is unsafe")
        return error_code

    def _delivery_row_for_claim(
        self,
        claim: ProjectDeliveryClaim,
    ) -> sqlite3.Row:
        row = self._conn.execute(
            """
            SELECT delivery.*, event.sequence AS event_sequence
            FROM project_deliveries AS delivery
            JOIN project_events AS event
              ON event.project_id = delivery.project_id
             AND event.event_id = delivery.event_id
            WHERE delivery.project_id = ?
              AND delivery.binding_id = ?
              AND delivery.delivery_id = ?
            """,
            (
                claim.project_id,
                claim.binding_id,
                claim.delivery_id,
            ),
        ).fetchone()
        if row is None or row["event_id"] != claim.event.event_id:
            raise ProjectDeliveryConflictError(
                "delivery obligation is unavailable"
            )
        return row

    @staticmethod
    def _require_live_claim_row(
        row: sqlite3.Row,
        claim: ProjectDeliveryClaim,
        *,
        now: int,
    ) -> None:
        if now >= claim.lease_expires_at:
            raise ProjectDeliveryConflictError(
                "delivery claim expired"
            )
        if not (
            row["status"] == "in_flight"
            and row["attempt_count"] == claim.attempt
            and row["lease_expires_at"] == claim.lease_expires_at
            and row["cursor"] is None
        ):
            raise ProjectDeliveryConflictError(
                "delivery claim is stale"
            )

    def renew_delivery(
        self,
        claim: ProjectDeliveryClaim,
        *,
        lease_seconds: int,
    ) -> ProjectDeliveryClaim:
        claim = self._require_claim(claim)
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("delivery lease must be a positive integer")
        now = self._now()
        lease_expires_at = now + lease_seconds
        if lease_expires_at > _SQLITE_INT_MAX:
            raise ValueError("delivery lease exceeds SQLite range")
        with runtime_db.write_transaction(self._conn):
            row = self._delivery_row_for_claim(claim)
            self._require_live_claim_row(row, claim, now=now)
            if lease_expires_at < claim.lease_expires_at:
                raise ValueError("delivery renewal cannot shorten its lease")
            changed = self._conn.execute(
                """
                UPDATE project_deliveries
                SET lease_expires_at = ?, updated_at = ?
                WHERE project_id = ? AND binding_id = ?
                  AND delivery_id = ? AND event_id = ?
                  AND status = 'in_flight'
                  AND attempt_count = ?
                  AND lease_expires_at = ?
                """,
                (
                    lease_expires_at,
                    now,
                    claim.project_id,
                    claim.binding_id,
                    claim.delivery_id,
                    claim.event.event_id,
                    claim.attempt,
                    claim.lease_expires_at,
                ),
            )
            if changed.rowcount != 1:
                raise ProjectDeliveryConflictError(
                    "delivery claim changed during renewal"
                )
        return ProjectDeliveryClaim(
            delivery_id=claim.delivery_id,
            project_id=claim.project_id,
            binding_id=claim.binding_id,
            event=claim.event,
            attempt=claim.attempt,
            lease_expires_at=lease_expires_at,
        )

    def defer_delivery(
        self,
        claim: ProjectDeliveryClaim,
        *,
        error_code: str,
        delay_seconds: int,
    ) -> int:
        claim = self._require_claim(claim)
        error_code = self._require_error_code(error_code)
        if (
            type(delay_seconds) is not int
            or not 0 <= delay_seconds <= 86_400
        ):
            raise ValueError("delivery delay must be between 0 and 86400")
        now = self._now()
        next_attempt_at = now + delay_seconds
        if next_attempt_at > _SQLITE_INT_MAX:
            raise ValueError("delivery retry exceeds SQLite range")
        with runtime_db.write_transaction(self._conn):
            row = self._delivery_row_for_claim(claim)
            self._require_live_claim_row(row, claim, now=now)
            changed = self._conn.execute(
                """
                UPDATE project_deliveries
                SET status = 'pending', cursor = NULL,
                    lease_expires_at = NULL,
                    remote_message_ids_json = NULL,
                    next_attempt_at = ?, last_error_code = ?,
                    updated_at = ?
                WHERE project_id = ? AND binding_id = ?
                  AND delivery_id = ? AND event_id = ?
                  AND status = 'in_flight'
                  AND attempt_count = ?
                  AND lease_expires_at = ?
                """,
                (
                    next_attempt_at,
                    error_code,
                    now,
                    claim.project_id,
                    claim.binding_id,
                    claim.delivery_id,
                    claim.event.event_id,
                    claim.attempt,
                    claim.lease_expires_at,
                ),
            )
            if changed.rowcount != 1:
                raise ProjectDeliveryConflictError(
                    "delivery claim changed during defer"
                )
        return next_attempt_at

    def block_delivery(
        self,
        claim: ProjectDeliveryClaim,
        *,
        error_code: str,
    ) -> None:
        claim = self._require_claim(claim)
        error_code = self._require_error_code(error_code)
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            row = self._delivery_row_for_claim(claim)
            self._require_live_claim_row(row, claim, now=now)
            changed = self._conn.execute(
                """
                UPDATE project_deliveries
                SET status = 'blocked', cursor = NULL,
                    lease_expires_at = NULL,
                    remote_message_ids_json = NULL,
                    next_attempt_at = NULL, last_error_code = ?,
                    updated_at = ?
                WHERE project_id = ? AND binding_id = ?
                  AND delivery_id = ? AND event_id = ?
                  AND status = 'in_flight'
                  AND attempt_count = ?
                  AND lease_expires_at = ?
                """,
                (
                    error_code,
                    now,
                    claim.project_id,
                    claim.binding_id,
                    claim.delivery_id,
                    claim.event.event_id,
                    claim.attempt,
                    claim.lease_expires_at,
                ),
            )
            if changed.rowcount != 1:
                raise ProjectDeliveryConflictError(
                    "delivery claim changed during block"
                )

    @staticmethod
    def _remote_message_ids_json(
        remote_message_ids: object,
    ) -> str:
        if (
            type(remote_message_ids) is not tuple
            or not remote_message_ids
            or any(
                type(message_id) is not str or not message_id
                for message_id in remote_message_ids
            )
            or len(set(remote_message_ids)) != len(remote_message_ids)
        ):
            raise ValueError(
                "remote message IDs must be a non-empty ordered unique tuple"
            )
        return json.dumps(
            remote_message_ids,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def _complete_terminal_delivery(
        self,
        claim: ProjectDeliveryClaim,
        *,
        status: Literal["delivered", "suppressed"],
        remote_message_ids_json: str,
    ) -> int:
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            row = self._delivery_row_for_claim(claim)
            event_sequence = row["event_sequence"]
            if row["status"] in {"delivered", "suppressed"}:
                if (
                    row["status"] == status
                    and row["cursor"] == event_sequence
                    and row["lease_expires_at"] is None
                    and row["remote_message_ids_json"]
                    == remote_message_ids_json
                ):
                    return event_sequence
                raise ProjectDeliveryConflictError(
                    "terminal remote delivery group conflicts"
                )
            self._require_live_claim_row(row, claim, now=now)
            changed = self._conn.execute(
                """
                UPDATE project_deliveries SET
                    status = ?, cursor = ?,
                    lease_expires_at = NULL,
                    remote_message_ids_json = ?,
                    next_attempt_at = NULL, last_error_code = NULL,
                    updated_at = ?
                WHERE project_id = ? AND binding_id = ?
                  AND delivery_id = ? AND event_id = ?
                  AND status = 'in_flight'
                  AND attempt_count = ? AND lease_expires_at = ?
                """,
                (
                    status,
                    event_sequence,
                    remote_message_ids_json,
                    now,
                    claim.project_id,
                    claim.binding_id,
                    claim.delivery_id,
                    claim.event.event_id,
                    claim.attempt,
                    claim.lease_expires_at,
                ),
            )
            if changed.rowcount != 1:
                raise ProjectDeliveryConflictError(
                    "delivery claim changed during completion"
                )
            return event_sequence

    def complete_delivery(
        self,
        claim: ProjectDeliveryClaim,
        *,
        remote_message_ids: tuple[str, ...],
    ) -> int:
        claim = self._require_claim(claim)
        return self._complete_terminal_delivery(
            claim,
            status="delivered",
            remote_message_ids_json=self._remote_message_ids_json(
                remote_message_ids
            ),
        )

    def suppress_origin_delivery(
        self,
        claim: ProjectDeliveryClaim,
    ) -> int:
        claim = self._require_claim(claim)
        row = self._conn.execute(
            """
            SELECT origin_binding_id
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (claim.project_id, claim.event.turn_id),
        ).fetchone()
        if not (
            claim.event.kind == "turn.queued"
            and row is not None
            and row["origin_binding_id"] == claim.binding_id
        ):
            raise ValueError(
                "only an origin turn.queued delivery may be suppressed"
            )
        return self._complete_terminal_delivery(
            claim,
            status="suppressed",
            remote_message_ids_json="[]",
        )

    def acknowledge_delivery(self, claim: ProjectDeliveryClaim) -> int:
        claim = self._require_claim(claim)
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            row = self._delivery_row_for_claim(claim)
            event_sequence = row["event_sequence"]
            if (
                row["status"] == "delivered"
                and row["cursor"] == event_sequence
                and row["remote_message_ids_json"] is None
            ):
                return event_sequence
            self._require_live_claim_row(row, claim, now=now)
            changed = self._conn.execute(
                """
                UPDATE project_deliveries SET
                    status = 'delivered', cursor = ?,
                    lease_expires_at = NULL,
                    remote_message_ids_json = NULL,
                    next_attempt_at = NULL, last_error_code = NULL,
                    updated_at = ?
                WHERE project_id = ? AND binding_id = ?
                  AND delivery_id = ? AND event_id = ?
                  AND status = 'in_flight'
                  AND attempt_count = ? AND lease_expires_at = ?
                """,
                (
                    event_sequence,
                    now,
                    claim.project_id,
                    claim.binding_id,
                    claim.delivery_id,
                    claim.event.event_id,
                    claim.attempt,
                    claim.lease_expires_at,
                ),
            )
            if changed.rowcount != 1:
                raise ProjectDeliveryConflictError(
                    "delivery claim changed during acknowledgement"
                )
            return event_sequence

    def reject_delivery(self, claim: ProjectDeliveryClaim) -> None:
        claim = self._require_claim(claim)
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            row = self._delivery_row_for_claim(claim)
            if (
                row["status"] == "pending"
                and row["attempt_count"] == claim.attempt
                and row["cursor"] is None
                and row["lease_expires_at"] is None
            ):
                return
            self._require_live_claim_row(row, claim, now=now)
            changed = self._conn.execute(
                """
                UPDATE project_deliveries SET
                    status = 'pending', cursor = NULL,
                    lease_expires_at = NULL,
                    remote_message_ids_json = NULL,
                    next_attempt_at = ?, last_error_code = 'retryable',
                    updated_at = ?
                WHERE project_id = ? AND binding_id = ?
                  AND delivery_id = ? AND event_id = ?
                  AND status = 'in_flight'
                  AND attempt_count = ? AND lease_expires_at = ?
                """,
                (
                    now,
                    now,
                    claim.project_id,
                    claim.binding_id,
                    claim.delivery_id,
                    claim.event.event_id,
                    claim.attempt,
                    claim.lease_expires_at,
                ),
            )
            if changed.rowcount != 1:
                raise ProjectDeliveryConflictError(
                    "delivery claim changed during rejection"
                )

    def _project_roots(self, project_id: str) -> tuple[Path, ...]:
        rows = self._conn.execute(
            """
            SELECT path FROM project_folders
            WHERE project_id = ?
            ORDER BY is_primary DESC, added_at, path
            """,
            (project_id,),
        ).fetchall()
        roots: list[Path] = []
        for row in rows:
            try:
                roots.append(Path(row["path"]).resolve(strict=True))
            except (OSError, RuntimeError, TypeError, ValueError):
                continue
        return tuple(roots)

    @staticmethod
    def _path_is_within(path: Path, root: Path) -> bool:
        try:
            return os.path.commonpath(
                (os.path.normcase(str(path)), os.path.normcase(str(root)))
            ) == os.path.normcase(str(root))
        except ValueError:
            return False

    def register_verified_artifact(
        self,
        project_id: str,
        *,
        artifact_id: str,
        path: str | Path,
        metadata: Mapping[str, object],
        turn_id: str | None = None,
        readback: Callable[[Path], bytes] | None = None,
    ) -> ProjectArtifact:
        self._require_managed_project(project_id)
        _require_text(artifact_id, "artifact_id")
        if turn_id is not None:
            _require_text(turn_id, "turn_id")
        try:
            candidate = Path(path).resolve(strict=True)
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise ValueError("artifact path cannot be read") from exc
        if not candidate.is_file() or not any(
            self._path_is_within(candidate, root)
            for root in self._project_roots(project_id)
        ):
            raise PermissionError("artifact is outside every project root")

        before = candidate.stat()
        expected = candidate.read_bytes()
        observed = (
            readback(candidate)
            if readback is not None
            else expected
        )
        after = candidate.stat()
        if (
            type(observed) is not bytes
            or observed != expected
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
        ):
            raise ValueError("artifact readback did not match")

        enriched = dict(metadata)
        enriched["sha256"] = hashlib.sha256(expected).hexdigest()
        enriched["size"] = len(expected)
        metadata_json, frozen_metadata = _canonical_redacted_mapping(
            enriched
        )
        now = self._now()
        canonical_path = str(candidate)
        with runtime_db.write_transaction(self._conn):
            rows = self._conn.execute(
                """
                SELECT * FROM project_artifacts
                WHERE artifact_id = ?
                   OR (project_id = ? AND path = ?)
                """,
                (artifact_id, project_id, canonical_path),
            ).fetchall()
            if rows:
                if len(rows) != 1:
                    raise ValueError("artifact identity conflicts")
                existing = _artifact_from_row(rows[0])
                if not (
                    existing.artifact_id == artifact_id
                    and existing.project_id == project_id
                    and existing.turn_id == turn_id
                    and existing.path == canonical_path
                    and existing.metadata == frozen_metadata
                ):
                    raise ValueError("artifact identity conflicts")
                return existing
            self._conn.execute(
                """
                INSERT INTO project_artifacts (
                    artifact_id, project_id, turn_id, path,
                    metadata_json, status, verified_at, created_at
                ) VALUES (?, ?, ?, ?, ?, 'verified', ?, ?)
                """,
                (
                    artifact_id,
                    project_id,
                    turn_id,
                    canonical_path,
                    metadata_json,
                    now,
                    now,
                ),
            )
            event_payload_json = canonical_json_object(
                {
                    "artifact_id": artifact_id,
                    "metadata": json.loads(metadata_json),
                    "path": canonical_path,
                    "status": "verified",
                }
            )
            runtime_db._append_runtime_event(
                self._conn,
                event_id=_require_text(
                    self._id_factory("event"),
                    "event_id",
                ),
                project_id=project_id,
                kind="artifact.verified",
                turn_id=turn_id,
                payload_json=event_payload_json,
                created_at=now,
            )
            row = self._conn.execute(
                """
                SELECT * FROM project_artifacts
                WHERE project_id = ? AND artifact_id = ?
                """,
                (project_id, artifact_id),
            ).fetchone()
            if row is None:
                raise ProjectEventIntegrityError(
                    "artifact disappeared during registration"
                )
            return _artifact_from_row(row)

    def artifact_for_id(
        self,
        project_id: str,
        artifact_id: str,
    ) -> ProjectArtifact | None:
        self._require_managed_project(project_id)
        _require_text(artifact_id, "artifact_id")
        row = self._conn.execute(
            """
            SELECT * FROM project_artifacts
            WHERE project_id = ? AND artifact_id = ?
            """,
            (project_id, artifact_id),
        ).fetchone()
        return _artifact_from_row(row) if row is not None else None


__all__ = [
    "ProjectArtifact",
    "ProjectDeliveryClaim",
    "ProjectDeliveryConflictError",
    "ProjectEvent",
    "ProjectEventIntegrityError",
    "ProjectEventOutbox",
]
