"""Durable per-project FIFO, lease, fencing, and recovery authority.

This module does not load history, construct agents, call providers, or
deliver to a surface. Concrete worker/readback wiring remains a later-task
adapter concern.
"""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Callable, Literal, Mapping, Protocol

from hermes_cli import project_runtime_db as runtime_db
from hermes_cli.project_policy import ActorContext


JSONScalar = str | int | float | bool | None
JSONValue = (
    JSONScalar
    | tuple["JSONValue", ...]
    | Mapping[str, "JSONValue"]
)
TurnStatus = Literal[
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
ControlState = Literal[
    "running",
    "stop_requested",
    "stopped",
    "resume_requested",
    "terminal",
]
TerminalTurnStatus = Literal["succeeded", "failed"]
ExecutionState = Literal["not_started", "started"]
RecoverySourceStatus = Literal["claimed", "stop_requested"]
ReadbackOutcome = Literal["succeeded", "failed", "stopped", "unknown"]
ApprovalRequest = runtime_db.ApprovalRequest
SQLITE_INT_MAX = (1 << 63) - 1


class RuntimeErrorCode(str, Enum):
    INVALID_ARGUMENT = "invalid_argument"
    ACTOR_NOT_AUTHORIZED = "actor_not_authorized"
    PROJECT_NOT_MANAGED = "project_not_managed"
    PROJECT_NOT_ACTIVE = "project_not_active"
    PROJECT_LINEAGE_INVALID = "project_lineage_invalid"
    PROJECT_VERSION_CONFLICT = "project_version_conflict"
    CONTROL_VERSION_CONFLICT = "control_version_conflict"
    ORIGIN_BINDING_CONFLICT = "origin_binding_conflict"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    TURN_NOT_FOUND = "turn_not_found"
    TURN_NOT_QUEUED = "turn_not_queued"
    TURN_NOT_CLAIMED = "turn_not_claimed"
    TURN_NOT_STOP_REQUESTED = "turn_not_stop_requested"
    TURN_NOT_STOPPED = "turn_not_stopped"
    TURN_TERMINAL = "turn_terminal"
    TURN_NOT_RESUMABLE = "turn_not_resumable"
    TURN_RECOVERY_BLOCKED = "turn_recovery_blocked"
    APPROVAL_CONFLICT = "approval_conflict"
    STALE_TURN_CLAIM = "stale_turn_claim"
    TURN_EXECUTION_NOT_STARTED = "turn_execution_not_started"
    TURN_OPERATIONS_UNRESOLVED = "turn_operations_unresolved"
    TERMINAL_RESULT_CONFLICT = "terminal_result_conflict"


class ProjectRuntimeError(RuntimeError):
    """A safe expected-domain failure for future adapters to map."""

    def __init__(
        self,
        code: RuntimeErrorCode,
        *,
        project_id: str | None = None,
        turn_id: str | None = None,
        current_version: int | None = None,
        current_control_version: int | None = None,
    ) -> None:
        super().__init__(code.value)
        self.code = code
        self.project_id = project_id
        self.turn_id = turn_id
        self.current_version = current_version
        self.current_control_version = current_control_version


@dataclass(frozen=True)
class ProjectTurn:
    turn_id: str
    project_id: str
    sequence: int
    idempotency_key: str
    payload: Mapping[str, JSONValue]
    origin_binding_id: str
    status: TurnStatus
    attempt_id: str | None
    lease_generation: int
    fencing_token: int
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class RunControl:
    turn_id: str
    project_id: str
    control_state: ControlState
    control_version: int
    last_idempotency_key: str | None
    attempt_id: str | None
    updated_at: int


@dataclass(frozen=True)
class TurnClaim:
    turn_id: str
    project_id: str
    sequence: int
    worker_id: str
    attempt_id: str
    lease_generation: int
    fencing_token: int
    lease_expires_at: int
    canonical_session_id: str


@dataclass(frozen=True)
class CanonicalTurnResult:
    status: TerminalTurnStatus
    result_id: str


@dataclass(frozen=True)
class TurnReadbackRequest:
    project_id: str
    turn_id: str
    sequence: int
    worker_id: str
    attempt_id: str
    lease_generation: int
    fencing_token: int
    lease_expires_at: int
    canonical_session_id: str
    source_status: RecoverySourceStatus
    execution_state: ExecutionState | None


@dataclass(frozen=True)
class TurnReadbackResult:
    outcome: ReadbackOutcome
    result_id: str | None = None


class TurnReadbackPort(Protocol):
    def read_turn(
        self,
        request: TurnReadbackRequest,
    ) -> TurnReadbackResult: ...


@dataclass(frozen=True)
class TurnApproval:
    turn_id: str
    approval: ApprovalRequest


def _is_text(value: object) -> bool:
    return type(value) is str and bool(value)


def _is_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _require_text(value: object) -> str:
    if not _is_text(value):
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    return value


def _require_version(value: object) -> int:
    if not _is_nonnegative_int(value):
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    return value


def _validated_json(value: object, seen: set[int]) -> object:
    """Copy only the finite, exact JSON domain and reject aliases/cycles."""
    value_type = type(value)
    if value is None or value_type in {str, bool, int}:
        return value
    if value_type is float:
        if not math.isfinite(value):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        return value
    if value_type is list:
        object_id = id(value)
        if object_id in seen:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        seen.add(object_id)
        try:
            return [_validated_json(item, seen) for item in value]
        finally:
            seen.remove(object_id)
    if value_type is dict:
        object_id = id(value)
        if object_id in seen:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        seen.add(object_id)
        try:
            copied: dict[str, object] = {}
            for key, item in value.items():
                if type(key) is not str:
                    raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
                copied[key] = _validated_json(item, seen)
            return copied
        finally:
            seen.remove(object_id)
    raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)


def canonical_json_object(value: object) -> str:
    """Return deterministic bytes for exactly one finite JSON object."""
    if type(value) is not dict:
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    copied = _validated_json(value, set())
    assert type(copied) is dict
    return json.dumps(
        copied,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _decode_canonical_object(raw: object) -> Mapping[str, object]:
    if type(raw) is not str:
        raise RuntimeError("corrupt canonical runtime payload")
    try:
        decoded = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("corrupt canonical runtime payload") from exc
    canonical = canonical_json_object(decoded)
    if canonical != raw:
        raise RuntimeError("noncanonical runtime payload stored")
    return _freeze_json(decoded)


def _freeze_json(value: object) -> object:
    if type(value) is dict:
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if type(value) is list:
        return tuple(_freeze_json(item) for item in value)
    return value


class ProjectRuntime:
    """The sole Task-4 service allowed to mutate FIFO runtime tables."""

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
        self._id_factory = id_factory or self._new_id

    @staticmethod
    def _new_id(kind: str) -> str:
        return f"{kind}-{uuid.uuid4().hex}"

    def _now(self) -> int:
        now = self._clock()
        if type(now) is not int or now < 0:
            raise RuntimeError("runtime clock returned an invalid timestamp")
        return now

    def _authorize_owner(self, project_id: str, actor: object) -> ActorContext:
        if not isinstance(actor, ActorContext) or not (
            _is_text(actor.actor_id)
            and _is_text(actor.binding_id)
            and actor.is_owner is True
            and type(actor.surface) is str
            and actor.surface in {"desktop", "discord"}
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.ACTOR_NOT_AUTHORIZED, project_id=project_id
            )
        binding = runtime_db.binding_for_id(
            self._conn, project_id=project_id, binding_id=actor.binding_id
        )
        if binding is None or not (
            binding.actor_id == actor.actor_id and binding.surface == actor.surface
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.ACTOR_NOT_AUTHORIZED, project_id=project_id
            )
        return actor

    def _require_state(self, project_id: str) -> runtime_db.RuntimeState:
        state = runtime_db.runtime_state_for_project(self._conn, project_id)
        if state is None:
            raise ProjectRuntimeError(
                RuntimeErrorCode.PROJECT_NOT_MANAGED, project_id=project_id
            )
        if not (
            _is_text(state.conversation_root_id)
            and _is_text(state.conversation_tip_id)
            and _is_text(state.current_phase)
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.PROJECT_LINEAGE_INVALID, project_id=project_id
            )
        root = self._conn.execute(
            """
            SELECT 1 FROM project_conversations
            WHERE project_id = ? AND conversation_id = ?
              AND parent_conversation_id IS NULL
              AND root_conversation_id = conversation_id
            """,
            (project_id, state.conversation_root_id),
        ).fetchone()
        tip = self._conn.execute(
            """
            SELECT 1 FROM project_conversations
            WHERE project_id = ? AND conversation_id = ?
              AND root_conversation_id = ?
            """,
            (project_id, state.conversation_tip_id, state.conversation_root_id),
        ).fetchone()
        if root is None or tip is None:
            raise ProjectRuntimeError(
                RuntimeErrorCode.PROJECT_LINEAGE_INVALID, project_id=project_id
            )
        return state

    @staticmethod
    def _require_active(state: runtime_db.RuntimeState) -> None:
        if state.lifecycle != "active":
            raise ProjectRuntimeError(
                RuntimeErrorCode.PROJECT_NOT_ACTIVE, project_id=state.project_id
            )

    def _turn_from_record(self, record: runtime_db.RuntimeTurnRecord) -> ProjectTurn:
        if not _is_text(record.origin_binding_id):
            raise RuntimeError("durable turn has no canonical origin binding")
        return ProjectTurn(
            turn_id=record.turn_id,
            project_id=record.project_id,
            sequence=record.sequence,
            idempotency_key=record.idempotency_key,
            payload=_decode_canonical_object(record.payload_json),
            origin_binding_id=record.origin_binding_id,
            status=record.status,
            attempt_id=record.attempt_id,
            lease_generation=record.lease_generation,
            fencing_token=record.fencing_token,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    @staticmethod
    def _control_from_record(record: runtime_db.RuntimeControlRecord) -> RunControl:
        return RunControl(
            turn_id=record.turn_id,
            project_id=record.project_id,
            control_state=record.control_state,
            control_version=record.control_version,
            last_idempotency_key=record.idempotency_key,
            attempt_id=record.attempt_id,
            updated_at=record.updated_at,
        )

    def _event(self, project_id: str, kind: str, turn_id: str | None, payload: dict[str, object], now: int) -> None:
        payload_json = canonical_json_object(payload)
        runtime_db._append_runtime_event(
            self._conn, event_id=self._id_factory("event"), project_id=project_id,
            kind=kind, turn_id=turn_id, payload_json=payload_json, created_at=now,
        )

    def _advance_state(self, state: runtime_db.RuntimeState, now: int) -> runtime_db.RuntimeState:
        updated = runtime_db._advance_runtime_version(
            self._conn, project_id=state.project_id,
            expected_version=state.version, updated_at=now,
        )
        if updated is None:
            current = runtime_db.runtime_state_for_project(self._conn, state.project_id)
            raise ProjectRuntimeError(
                RuntimeErrorCode.PROJECT_VERSION_CONFLICT,
                project_id=state.project_id,
                current_version=current.version if current is not None else None,
            )
        return updated

    def _turn(self, project_id: str, turn_id: str) -> runtime_db.RuntimeTurnRecord:
        record = runtime_db._runtime_turn_for_project(
            self._conn, project_id=project_id, turn_id=turn_id
        )
        if record is None:
            raise ProjectRuntimeError(
                RuntimeErrorCode.TURN_NOT_FOUND, project_id=project_id, turn_id=turn_id
            )
        return record

    def _control(self, project_id: str, turn_id: str) -> runtime_db.RuntimeControlRecord:
        control = runtime_db._runtime_control_for_turn(
            self._conn, project_id=project_id, turn_id=turn_id
        )
        if control is None:
            raise RuntimeError("durable turn has no control row")
        return control

    @staticmethod
    def _stored_claim_matches(
        turn: runtime_db.RuntimeTurnRecord,
        control: runtime_db.RuntimeControlRecord,
        claim: TurnClaim,
    ) -> bool:
        return (
            turn.project_id == claim.project_id
            and turn.turn_id == claim.turn_id
            and turn.sequence == claim.sequence
            and turn.attempt_id == claim.attempt_id
            and turn.lease_generation == claim.lease_generation
            and turn.fencing_token == claim.fencing_token
            and control.project_id == claim.project_id
            and control.turn_id == claim.turn_id
            and control.attempt_id == claim.attempt_id
            and control.claim_worker_id == claim.worker_id
            and type(control.claim_lease_expires_at) is int
            and claim.lease_expires_at <= control.claim_lease_expires_at
            and control.claim_canonical_session_id
            == claim.canonical_session_id
        )

    @staticmethod
    def _current_lease_matches(
        lease: runtime_db.WorkerLeaseRecord | None,
        claim: TurnClaim,
    ) -> bool:
        return (
            lease is not None
            and lease.project_id == claim.project_id
            and lease.turn_id == claim.turn_id
            and lease.lease_id == claim.attempt_id
            and lease.worker_id == claim.worker_id
            and lease.lease_generation == claim.lease_generation
            and lease.fencing_token == claim.fencing_token
            and claim.lease_expires_at <= lease.expires_at
        )

    @staticmethod
    def _require_turn_claim(claim: object) -> TurnClaim:
        if (
            type(claim) is not TurnClaim
            or not all(
                _is_text(value)
                for value in (
                    claim.project_id,
                    claim.turn_id,
                    claim.worker_id,
                    claim.attempt_id,
                    claim.canonical_session_id,
                )
            )
            or not all(
                type(value) is int and value > 0
                for value in (
                    claim.sequence,
                    claim.lease_generation,
                    claim.fencing_token,
                )
            )
            or not _is_nonnegative_int(claim.lease_expires_at)
        ):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        return claim

    @staticmethod
    def _stale_turn_claim(claim: TurnClaim) -> ProjectRuntimeError:
        return ProjectRuntimeError(
            RuntimeErrorCode.STALE_TURN_CLAIM,
            project_id=claim.project_id,
            turn_id=claim.turn_id,
        )

    def _live_claim_records(
        self,
        claim: TurnClaim,
        *,
        now: int,
    ) -> tuple[
        runtime_db.RuntimeState,
        runtime_db.RuntimeTurnRecord,
        runtime_db.RuntimeControlRecord,
        runtime_db.WorkerLeaseRecord,
    ]:
        state = runtime_db.runtime_state_for_project(
            self._conn, claim.project_id
        )
        turn = runtime_db._runtime_turn_for_project(
            self._conn,
            project_id=claim.project_id,
            turn_id=claim.turn_id,
        )
        control = runtime_db._runtime_control_for_turn(
            self._conn,
            project_id=claim.project_id,
            turn_id=claim.turn_id,
        )
        lease = runtime_db._current_worker_lease_for_turn(
            self._conn,
            project_id=claim.project_id,
            turn_id=claim.turn_id,
        )
        if not (
            state is not None
            and state.lifecycle == "active"
            and state.conversation_tip_id == claim.canonical_session_id
            and turn is not None
            and control is not None
            and lease is not None
            and self._stored_claim_matches(turn, control, claim)
            and self._current_lease_matches(lease, claim)
            and control.claim_lease_expires_at == lease.expires_at
            and lease.expires_at > now
        ):
            raise self._stale_turn_claim(claim)
        return state, turn, control, lease

    def _require_live_operation_claim(
        self,
        claim: TurnClaim,
        *,
        now: int,
    ) -> tuple[
        runtime_db.RuntimeState,
        runtime_db.RuntimeTurnRecord,
        runtime_db.RuntimeControlRecord,
        runtime_db.WorkerLeaseRecord,
    ]:
        """Share the full Task-5 fence predicate with operation mutations."""
        claim = self._require_turn_claim(claim)
        if not _is_nonnegative_int(now):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        state, turn, control, lease = self._live_claim_records(
            claim, now=now
        )
        if not (
            turn.status == "claimed"
            and control.control_state == "running"
            and turn.execution_state in {"not_started", "started"}
        ):
            raise self._stale_turn_claim(claim)
        return state, turn, control, lease

    @staticmethod
    def _claim_at_horizon(
        claim: TurnClaim,
        lease_expires_at: int,
    ) -> TurnClaim:
        return TurnClaim(
            turn_id=claim.turn_id,
            project_id=claim.project_id,
            sequence=claim.sequence,
            worker_id=claim.worker_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            lease_expires_at=lease_expires_at,
            canonical_session_id=claim.canonical_session_id,
        )

    def heartbeat_turn(
        self,
        claim: TurnClaim,
        *,
        lease_seconds: int,
    ) -> TurnClaim:
        claim = self._require_turn_claim(claim)
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        now = self._now()
        if now > SQLITE_INT_MAX - lease_seconds:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        requested_expires_at = now + lease_seconds
        with runtime_db.write_transaction(self._conn):
            _, turn, control, lease = self._live_claim_records(
                claim, now=now
            )
            allowed = {
                ("claimed", "running"),
                ("awaiting_approval", "running"),
                ("stop_requested", "stop_requested"),
            }
            if (
                (turn.status, control.control_state) not in allowed
                or turn.execution_state not in {"not_started", "started"}
            ):
                raise self._stale_turn_claim(claim)
            new_expires_at = max(lease.expires_at, requested_expires_at)
            renewed = runtime_db._heartbeat_runtime_turn(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
                sequence=claim.sequence,
                turn_status=turn.status,
                control_state=control.control_state,
                attempt_id=claim.attempt_id,
                worker_id=claim.worker_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                canonical_session_id=claim.canonical_session_id,
                old_expires_at=lease.expires_at,
                new_expires_at=new_expires_at,
                now=now,
            )
            if renewed is None or renewed.expires_at != new_expires_at:
                raise self._stale_turn_claim(claim)
            return self._claim_at_horizon(claim, renewed.expires_at)

    def mark_turn_started(
        self,
        claim: TurnClaim,
    ) -> TurnClaim:
        claim = self._require_turn_claim(claim)
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            _, turn, control, lease = self._live_claim_records(
                claim, now=now
            )
            if not (
                turn.status == "claimed"
                and control.control_state == "running"
                and turn.execution_state in {"not_started", "started"}
            ):
                raise self._stale_turn_claim(claim)
            current_claim = self._claim_at_horizon(claim, lease.expires_at)
            if turn.execution_state == "started":
                return current_claim
            if not runtime_db._mark_runtime_turn_started(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
                sequence=claim.sequence,
                attempt_id=claim.attempt_id,
                worker_id=claim.worker_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                canonical_session_id=claim.canonical_session_id,
                expires_at=lease.expires_at,
                now=now,
            ):
                raise self._stale_turn_claim(claim)
            return current_claim

    def commit_turn(
        self,
        claim: TurnClaim,
        result: CanonicalTurnResult,
    ) -> ProjectTurn:
        claim = self._require_turn_claim(claim)
        if not (
            type(result) is CanonicalTurnResult
            and type(result.status) is str
            and result.status in {"succeeded", "failed"}
            and _is_text(result.result_id)
        ):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        now = self._now()
        parked_operation_block = False
        with runtime_db.write_transaction(self._conn):
            state = runtime_db.runtime_state_for_project(
                self._conn, claim.project_id
            )
            turn = runtime_db._runtime_turn_for_project(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )
            control = runtime_db._runtime_control_for_turn(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )
            lease = runtime_db._current_worker_lease_for_turn(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )
            if (
                turn is not None
                and control is not None
                and turn.status in {"succeeded", "failed"}
            ):
                same_claim = (
                    state is not None
                    and control.control_state == "terminal"
                    and lease is None
                    and turn.execution_state == "started"
                    and self._stored_claim_matches(turn, control, claim)
                )
                if not same_claim or turn.terminal_result_id is None:
                    raise self._stale_turn_claim(claim)
                if (
                    turn.status == result.status
                    and turn.terminal_result_id == result.result_id
                ):
                    return self._turn_from_record(turn)
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TERMINAL_RESULT_CONFLICT,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                )

            state, turn, control, lease = self._live_claim_records(
                claim, now=now
            )
            if not (
                turn.status == "claimed"
                and control.control_state == "running"
            ):
                raise self._stale_turn_claim(claim)
            if turn.execution_state == "not_started":
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                )
            if turn.execution_state != "started":
                raise self._stale_turn_claim(claim)
            operation_disposition = (
                runtime_db._project_operation_disposition_for_turn(
                    self._conn,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                )
            )
            if operation_disposition == "post_effect_blocked":
                candidate = (
                    runtime_db
                    ._park_live_runtime_turn_for_operation_block(
                        self._conn,
                        project_id=claim.project_id,
                        turn_id=claim.turn_id,
                        sequence=claim.sequence,
                        attempt_id=claim.attempt_id,
                        worker_id=claim.worker_id,
                        lease_generation=claim.lease_generation,
                        fencing_token=claim.fencing_token,
                        lease_expires_at=lease.expires_at,
                        canonical_session_id=(
                            claim.canonical_session_id
                        ),
                        control_version=control.control_version,
                        now=now,
                    )
                )
                updated_state = self._advance_state(state, now)
                self._event(
                    claim.project_id,
                    "turn.reconciling",
                    claim.turn_id,
                    {
                        "attempt_id": claim.attempt_id,
                        "fencing_token": claim.fencing_token,
                        "lease_generation": (
                            claim.lease_generation
                        ),
                        "source_status": "claimed",
                        "turn_id": claim.turn_id,
                        "version": updated_state.version,
                    },
                    now,
                )
                self._block_current_recovery(
                    candidate, now=now
                )
                parked_operation_block = True
            else:
                terminal_allowed = operation_disposition in {
                    "clear",
                    "reconciled",
                } or (
                    operation_disposition
                    == "pre_effect_blocked"
                    and result.status == "failed"
                )
                if not terminal_allowed:
                    raise ProjectRuntimeError(
                        RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED,
                        project_id=claim.project_id,
                        turn_id=claim.turn_id,
                    )
                committed = runtime_db._commit_runtime_turn(
                    self._conn,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                    sequence=claim.sequence,
                    terminal_status=result.status,
                    terminal_result_id=result.result_id,
                    attempt_id=claim.attempt_id,
                    worker_id=claim.worker_id,
                    lease_generation=claim.lease_generation,
                    fencing_token=claim.fencing_token,
                    canonical_session_id=(
                        claim.canonical_session_id
                    ),
                    expires_at=lease.expires_at,
                    expected_control_version=(
                        control.control_version
                    ),
                    now=now,
                )
                if committed is None:
                    raise self._stale_turn_claim(claim)
                updated_state = self._advance_state(state, now)
                self._event(
                    claim.project_id,
                    f"turn.{result.status}",
                    claim.turn_id,
                    {
                        "attempt_id": claim.attempt_id,
                        "fencing_token": claim.fencing_token,
                        "lease_generation": (
                            claim.lease_generation
                        ),
                        "turn_id": claim.turn_id,
                        "version": updated_state.version,
                    },
                    now,
                )
                return self._turn_from_record(committed)
        if parked_operation_block:
            raise ProjectRuntimeError(
                RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )
        raise RuntimeError("operation commit reached invalid state")

    def reconcile_inflight_turns(
        self,
        readback: TurnReadbackPort,
        *,
        limit: int,
    ) -> tuple[ProjectTurn, ...]:
        if (
            type(limit) is not int
            or not 1 <= limit <= 100
            or not callable(getattr(readback, "read_turn", None))
            or self._conn.in_transaction
        ):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        now = self._now()
        candidates = runtime_db._recovery_candidates(
            self._conn, now=now, limit=limit
        )
        parked: list[
            ProjectTurn | runtime_db.RecoveryCandidateRecord
        ] = []
        for selected in candidates:
            candidate = self._park_recovery_candidate(selected, now=now)
            if candidate is None:
                current = runtime_db._runtime_turn_for_project(
                    self._conn,
                    project_id=selected.project_id,
                    turn_id=selected.turn_id,
                )
                if current is not None and current.status not in {
                    "claimed",
                    "stop_requested",
                    "awaiting_approval",
                }:
                    parked.append(self._turn_from_record(current))
                continue
            parked.append(candidate)

        recovered: list[ProjectTurn] = []
        for item in parked:
            if type(item) is ProjectTurn:
                recovered.append(item)
                continue
            candidate = item
            operation_disposition = (
                runtime_db._project_operation_disposition_for_turn(
                    self._conn,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
            )
            if operation_disposition in {
                "unresolved",
                "post_effect_blocked",
            }:
                if (
                    operation_disposition == "unresolved"
                    and runtime_db._operation_pending_for_turn(
                        self._conn,
                        project_id=candidate.project_id,
                        turn_id=candidate.turn_id,
                    )
                    is not None
                ):
                    recovered.append(
                        self._turn_from_record(
                            self._turn(
                                candidate.project_id,
                                candidate.turn_id,
                            )
                        )
                    )
                    continue
                recovered.append(
                    self._block_recovery(candidate, now=now)
                )
                continue
            if operation_disposition == "pre_effect_blocked":
                recovered.append(
                    self._finalize_recovery(
                        candidate,
                        outcome="failed",
                        result_id=(
                            "operation-pre-effect-blocked:"
                            f"{candidate.project_id}:"
                            f"{candidate.turn_id}:"
                            f"{candidate.attempt_id}:"
                            f"{candidate.lease_generation}:"
                            f"{candidate.fencing_token}"
                        ),
                        now=now,
                    )
                )
                continue
            result_id = None
            if candidate.execution_state == "not_started":
                if candidate.source_status == "stop_requested":
                    outcome = "stopped"
                elif candidate.lifecycle == "active":
                    outcome = "queued"
                else:
                    recovered.append(
                        self._block_recovery(candidate, now=now)
                    )
                    continue
            else:
                request = TurnReadbackRequest(
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                    sequence=candidate.sequence,
                    worker_id=candidate.worker_id,
                    attempt_id=candidate.attempt_id,
                    lease_generation=candidate.lease_generation,
                    fencing_token=candidate.fencing_token,
                    lease_expires_at=candidate.lease_expires_at,
                    canonical_session_id=candidate.canonical_session_id,
                    source_status=candidate.source_status,
                    execution_state=candidate.execution_state,
                )
                try:
                    result = readback.read_turn(request)
                except Exception:
                    recovered.append(
                        self._block_recovery(candidate, now=now)
                    )
                    continue
                if not self._valid_readback_result(
                    result, source_status=candidate.source_status
                ):
                    recovered.append(
                        self._block_recovery(candidate, now=now)
                    )
                    continue
                if result.outcome == "unknown":
                    recovered.append(
                        self._block_recovery(candidate, now=now)
                    )
                    continue
                outcome = result.outcome
                if outcome in {"succeeded", "failed"}:
                    result_id = result.result_id
            recovered.append(
                self._finalize_recovery(
                    candidate,
                    outcome=outcome,
                    result_id=result_id,
                    now=now,
                )
            )
        return tuple(recovered)

    @staticmethod
    def _valid_readback_result(
        result: object,
        *,
        source_status: str,
    ) -> bool:
        if type(result) is not TurnReadbackResult:
            return False
        if type(result.outcome) is not str:
            return False
        if result.outcome in {"succeeded", "failed"}:
            return _is_text(result.result_id)
        if result.outcome not in {"stopped", "unknown"}:
            return False
        if result.result_id is not None:
            return False
        return result.outcome != "stopped" or source_status == "stop_requested"

    def _park_recovery_candidate(
        self,
        selected: runtime_db.RecoveryCandidateRecord,
        *,
        now: int,
    ) -> runtime_db.RecoveryCandidateRecord | None:
        with runtime_db.write_transaction(self._conn):
            turn = runtime_db._runtime_turn_for_project(
                self._conn,
                project_id=selected.project_id,
                turn_id=selected.turn_id,
            )
            if turn is not None and turn.status in {
                "claimed",
                "stop_requested",
            }:
                parked = runtime_db._park_expired_runtime_turn(
                    self._conn, candidate=selected, now=now
                )
                if parked is not None:
                    state = runtime_db.runtime_state_for_project(
                        self._conn, selected.project_id
                    )
                    if state is None:
                        raise RuntimeError(
                            "recovery candidate has no runtime state"
                        )
                    updated_state = self._advance_state(state, now)
                    self._event(
                        selected.project_id,
                        "turn.reconciling",
                        selected.turn_id,
                        {
                            "attempt_id": selected.attempt_id,
                            "fencing_token": selected.fencing_token,
                            "lease_generation": selected.lease_generation,
                            "source_status": selected.source_status,
                            "turn_id": selected.turn_id,
                            "version": updated_state.version,
                        },
                        now,
                    )
            candidate = runtime_db._recovery_candidate_for_attempt(
                self._conn,
                project_id=selected.project_id,
                turn_id=selected.turn_id,
                attempt_id=selected.attempt_id,
                lease_generation=selected.lease_generation,
                fencing_token=selected.fencing_token,
            )
            if candidate is None or runtime_db._recovery_block_exists(
                self._conn,
                project_id=selected.project_id,
                turn_id=selected.turn_id,
                attempt_id=selected.attempt_id,
                lease_generation=selected.lease_generation,
                fencing_token=selected.fencing_token,
            ):
                return None
            return candidate

    def _finalize_recovery(
        self,
        candidate: runtime_db.RecoveryCandidateRecord,
        *,
        outcome: str,
        result_id: str | None,
        now: int,
    ) -> ProjectTurn:
        with runtime_db.write_transaction(self._conn):
            current_candidate = runtime_db._recovery_candidate_for_attempt(
                self._conn,
                project_id=candidate.project_id,
                turn_id=candidate.turn_id,
                attempt_id=candidate.attempt_id,
                lease_generation=candidate.lease_generation,
                fencing_token=candidate.fencing_token,
            )
            if current_candidate is None:
                current = self._turn(
                    candidate.project_id, candidate.turn_id
                )
                return self._turn_from_record(current)
            if runtime_db._recovery_block_exists(
                self._conn,
                project_id=candidate.project_id,
                turn_id=candidate.turn_id,
                attempt_id=candidate.attempt_id,
                lease_generation=candidate.lease_generation,
                fencing_token=candidate.fencing_token,
            ):
                return self._turn_from_record(
                    self._turn(candidate.project_id, candidate.turn_id)
                )
            operation_disposition = (
                runtime_db._project_operation_disposition_for_turn(
                    self._conn,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
            )
            if operation_disposition in {
                "unresolved",
                "post_effect_blocked",
            } or (
                operation_disposition == "pre_effect_blocked"
                and outcome != "failed"
            ):
                if (
                    operation_disposition == "unresolved"
                    and runtime_db._operation_pending_for_turn(
                        self._conn,
                        project_id=candidate.project_id,
                        turn_id=candidate.turn_id,
                    )
                    is not None
                ):
                    return self._turn_from_record(
                        self._turn(
                            candidate.project_id,
                            candidate.turn_id,
                        )
                    )
                return self._block_current_recovery(
                    current_candidate, now=now
                )
            if outcome == "queued":
                state = runtime_db.runtime_state_for_project(
                    self._conn, candidate.project_id
                )
                if state is None:
                    raise RuntimeError(
                        "recovery candidate has no runtime state"
                    )
                if state.lifecycle != "active":
                    return self._block_current_recovery(
                        current_candidate, now=now
                    )
            updated = runtime_db._apply_recovery_outcome(
                self._conn,
                candidate=current_candidate,
                outcome=outcome,
                terminal_result_id=result_id,
                now=now,
            )
            if updated is None:
                return self._turn_from_record(
                    self._turn(candidate.project_id, candidate.turn_id)
                )
            state = runtime_db.runtime_state_for_project(
                self._conn, candidate.project_id
            )
            if state is None:
                raise RuntimeError("recovery candidate has no runtime state")
            updated_state = self._advance_state(state, now)
            event_kind = {
                "queued": "turn.requeued",
                "stopped": "run.stopped",
                "succeeded": "turn.succeeded",
                "failed": "turn.failed",
            }[outcome]
            self._event(
                candidate.project_id,
                event_kind,
                candidate.turn_id,
                {
                    "attempt_id": candidate.attempt_id,
                    "fencing_token": candidate.fencing_token,
                    "lease_generation": candidate.lease_generation,
                    "source_status": candidate.source_status,
                    "turn_id": candidate.turn_id,
                    "version": updated_state.version,
                },
                now,
            )
            return self._turn_from_record(updated)

    def _block_recovery(
        self,
        candidate: runtime_db.RecoveryCandidateRecord,
        *,
        now: int,
    ) -> ProjectTurn:
        with runtime_db.write_transaction(self._conn):
            current_candidate = runtime_db._recovery_candidate_for_attempt(
                self._conn,
                project_id=candidate.project_id,
                turn_id=candidate.turn_id,
                attempt_id=candidate.attempt_id,
                lease_generation=candidate.lease_generation,
                fencing_token=candidate.fencing_token,
            )
            if current_candidate is None:
                return self._turn_from_record(
                    self._turn(candidate.project_id, candidate.turn_id)
                )
            return self._block_current_recovery(
                current_candidate, now=now
            )

    def _block_current_recovery(
        self,
        candidate: runtime_db.RecoveryCandidateRecord,
        *,
        now: int,
    ) -> ProjectTurn:
        if (
            runtime_db._operation_pending_for_turn(
                self._conn,
                project_id=candidate.project_id,
                turn_id=candidate.turn_id,
            )
            is not None
        ):
            return self._turn_from_record(
                self._turn(candidate.project_id, candidate.turn_id)
            )
        block_key = runtime_db._recovery_block_key(
            project_id=candidate.project_id,
            turn_id=candidate.turn_id,
            attempt_id=candidate.attempt_id,
            lease_generation=candidate.lease_generation,
            fencing_token=candidate.fencing_token,
        )
        if not runtime_db._recovery_block_exists(
            self._conn,
            project_id=candidate.project_id,
            turn_id=candidate.turn_id,
            attempt_id=candidate.attempt_id,
            lease_generation=candidate.lease_generation,
            fencing_token=candidate.fencing_token,
        ):
            state = runtime_db.runtime_state_for_project(
                self._conn, candidate.project_id
            )
            if state is None:
                raise RuntimeError("recovery candidate has no runtime state")
            updated_state = self._advance_state(state, now)
            payload = {
                "attempt_id": candidate.attempt_id,
                "fencing_token": candidate.fencing_token,
                "lease_generation": candidate.lease_generation,
                "source_status": candidate.source_status,
                "turn_id": candidate.turn_id,
                "version": updated_state.version,
            }
            runtime_db._append_runtime_event(
                self._conn,
                event_id=block_key,
                project_id=candidate.project_id,
                kind="turn.recovery_blocked",
                turn_id=candidate.turn_id,
                payload_json=canonical_json_object(payload),
                created_at=now,
            )
        if not runtime_db._set_recovery_block_key(
            self._conn,
            candidate=candidate,
            block_key=block_key,
        ):
            current = self._turn(candidate.project_id, candidate.turn_id)
            if current.recovery_block_key != block_key:
                raise RuntimeError("recovery block projection changed")
        return self._turn_from_record(
            self._turn(candidate.project_id, candidate.turn_id)
        )

    def enqueue_turn(
        self,
        project_id: str,
        payload: Mapping[str, object],
        actor: ActorContext,
        *,
        idempotency_key: str,
        expected_version: int,
    ) -> ProjectTurn:
        project_id = _require_text(project_id)
        idempotency_key = _require_text(idempotency_key)
        expected_version = _require_version(expected_version)
        payload_json = canonical_json_object(payload)
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            actor = self._authorize_owner(project_id, actor)
            state = self._require_state(project_id)
            existing = self._conn.execute(
                """
                SELECT * FROM project_turns
                WHERE project_id = ? AND idempotency_key = ?
                """,
                (project_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                record = runtime_db.runtime_turn_from_row(existing)
                if (
                    record.payload_json == payload_json
                    and record.origin_binding_id == actor.binding_id
                ):
                    return self._turn_from_record(record)
                raise ProjectRuntimeError(
                    RuntimeErrorCode.IDEMPOTENCY_CONFLICT, project_id=project_id
                )
            self._require_active(state)
            if state.version != expected_version:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_VERSION_CONFLICT,
                    project_id=project_id,
                    current_version=state.version,
                )
            sequence = runtime_db._allocate_project_sequence(
                self._conn, table="turns", project_id=project_id
            )
            turn_id = self._id_factory("turn")
            runtime_db._insert_queued_runtime_turn(
                self._conn, turn_id=turn_id, project_id=project_id,
                sequence=sequence, idempotency_key=idempotency_key,
                payload_json=payload_json, origin_binding_id=actor.binding_id,
                now=now,
            )
            updated = self._advance_state(state, now)
            self._event(
                project_id, "turn.queued", turn_id,
                {"turn_id": turn_id, "sequence": sequence, "version": updated.version}, now,
            )
            return self._turn_from_record(self._turn(project_id, turn_id))

    def list_queue(self, project_id: str, actor: ActorContext) -> tuple[ProjectTurn, ...]:
        project_id = _require_text(project_id)
        self._authorize_owner(project_id, actor)
        self._require_state(project_id)
        return tuple(
            self._turn_from_record(record)
            for record in runtime_db._queued_turns_for_project(self._conn, project_id=project_id)
        )

    def claim_next_turn(self, project_id: str, worker_id: str, *, lease_seconds: int) -> TurnClaim | None:
        project_id = _require_text(project_id)
        worker_id = _require_text(worker_id)
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        now = self._now()
        if now > SQLITE_INT_MAX - lease_seconds:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        with runtime_db.write_transaction(self._conn):
            state = self._require_state(project_id)
            self._require_active(state)
            turn = runtime_db._claim_oldest_queued_runtime_turn(
                self._conn, project_id=project_id, worker_id=worker_id,
                attempt_id=self._id_factory("attempt"),
                canonical_session_id=state.conversation_tip_id,
                now=now,
                lease_seconds=lease_seconds,
            )
            if turn is None:
                return None
            lease = runtime_db._current_worker_lease_for_turn(
                self._conn, project_id=project_id, turn_id=turn.turn_id
            )
            if lease is None:
                raise RuntimeError("claimed turn has no current worker lease")
            updated_state = self._advance_state(state, now)
            self._event(
                project_id, "turn.claimed", turn.turn_id,
                {
                    "attempt_id": turn.attempt_id, "fencing_token": turn.fencing_token,
                    "lease_generation": turn.lease_generation, "sequence": turn.sequence,
                    "turn_id": turn.turn_id, "version": updated_state.version,
                }, now,
            )
            return TurnClaim(
                turn_id=turn.turn_id, project_id=project_id, sequence=turn.sequence,
                worker_id=worker_id, attempt_id=turn.attempt_id,
                lease_generation=turn.lease_generation, fencing_token=turn.fencing_token,
                lease_expires_at=lease.expires_at,
                canonical_session_id=state.conversation_tip_id,
            )

    def cancel_queued_turn(
        self,
        project_id: str,
        turn_id: str,
        actor: ActorContext,
        *,
        idempotency_key: str,
        expected_version: int,
        expected_control_version: int,
    ) -> ProjectTurn:
        project_id = _require_text(project_id)
        turn_id = _require_text(turn_id)
        idempotency_key = _require_text(idempotency_key)
        expected_version = _require_version(expected_version)
        expected_control_version = _require_version(expected_control_version)
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            actor = self._authorize_owner(project_id, actor)
            state = self._require_state(project_id)
            fingerprint = self._control_fingerprint(
                "cancel",
                project_id,
                turn_id,
                actor.actor_id,
                expected_version,
                expected_control_version,
            )
            replay = self._control_replay(
                project_id, idempotency_key, fingerprint
            )
            if replay is not None:
                return self._turn_from_record(self._turn(project_id, turn_id))
            if state.version != expected_version:
                raise ProjectRuntimeError(RuntimeErrorCode.PROJECT_VERSION_CONFLICT, project_id=project_id, current_version=state.version)
            turn = self._turn(project_id, turn_id)
            if turn.status in {"succeeded", "failed", "cancelled"}:
                raise ProjectRuntimeError(RuntimeErrorCode.TURN_TERMINAL, project_id=project_id, turn_id=turn_id)
            if turn.status != "queued":
                raise ProjectRuntimeError(RuntimeErrorCode.TURN_NOT_QUEUED, project_id=project_id, turn_id=turn_id)
            control = self._control(project_id, turn_id)
            if control.control_version != expected_control_version:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.CONTROL_VERSION_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                    current_control_version=control.control_version,
                )
            if not (
                control.control_state == "running"
                and turn.attempt_id is None
                and control.attempt_id is None
                and turn.lease_generation == 0
                and turn.fencing_token == 0
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_NOT_QUEUED,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            transitioned = runtime_db._transition_runtime_turn_and_control(
                self._conn, project_id=project_id, turn_id=turn_id,
                expected_turn_status="queued", next_turn_status="cancelled",
                expected_control_state="running",
                expected_attempt_id=None,
                expected_control_version=control.control_version,
                next_control_state="terminal", now=now,
                idempotency_key=idempotency_key,
                command_fingerprint=fingerprint,
            )
            if transitioned is None:
                raise ProjectRuntimeError(RuntimeErrorCode.TURN_NOT_QUEUED, project_id=project_id, turn_id=turn_id)
            updated = self._advance_state(state, now)
            self._event(project_id, "turn.cancelled", turn_id, {"turn_id": turn_id, "version": updated.version}, now)
            return self._turn_from_record(self._turn(project_id, turn_id))

    @staticmethod
    def _control_fingerprint(
        kind: str, project_id: str, turn_id: str, actor_id: str,
        expected_version: int, expected_control_version: int,
    ) -> str:
        return canonical_json_object(
            {
                "actor_id": actor_id, "expected_control_version": expected_control_version,
                "expected_version": expected_version, "kind": kind,
                "project_id": project_id, "turn_id": turn_id,
            }
        )

    def _control_replay(
        self, project_id: str, key: str, fingerprint: str
    ) -> runtime_db.RuntimeControlRecord | None:
        row = self._conn.execute(
            """
            SELECT * FROM project_run_controls
            WHERE project_id = ? AND idempotency_key = ?
            """, (project_id, key),
        ).fetchone()
        if row is None:
            return None
        control = runtime_db.runtime_control_from_row(row)
        if control.command_fingerprint != fingerprint:
            raise ProjectRuntimeError(RuntimeErrorCode.IDEMPOTENCY_CONFLICT, project_id=project_id)
        return control

    def request_stop(
        self, project_id: str, turn_id: str, actor: ActorContext, *,
        idempotency_key: str, expected_version: int, expected_control_version: int,
    ) -> RunControl:
        return self._request_control(
            "stop", project_id, turn_id, actor, idempotency_key,
            expected_version, expected_control_version,
        )

    def request_resume(
        self, project_id: str, turn_id: str, actor: ActorContext, *,
        idempotency_key: str, expected_version: int, expected_control_version: int,
    ) -> RunControl:
        return self._request_control(
            "resume", project_id, turn_id, actor, idempotency_key,
            expected_version, expected_control_version,
        )

    def _request_control(
        self, kind: str, project_id: str, turn_id: str, actor: ActorContext,
        idempotency_key: str, expected_version: int, expected_control_version: int,
    ) -> RunControl:
        project_id = _require_text(project_id)
        turn_id = _require_text(turn_id)
        idempotency_key = _require_text(idempotency_key)
        expected_version = _require_version(expected_version)
        expected_control_version = _require_version(expected_control_version)
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            actor = self._authorize_owner(project_id, actor)
            state = self._require_state(project_id)
            fingerprint = self._control_fingerprint(kind, project_id, turn_id, actor.actor_id, expected_version, expected_control_version)
            replay = self._control_replay(project_id, idempotency_key, fingerprint)
            if replay is not None:
                return self._control_from_record(replay)
            turn = self._turn(project_id, turn_id)
            control = self._control(project_id, turn_id)
            if turn.status in {"succeeded", "failed", "cancelled"}:
                if kind == "stop":
                    return self._control_from_record(control)
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_TERMINAL,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            if state.version != expected_version:
                raise ProjectRuntimeError(RuntimeErrorCode.PROJECT_VERSION_CONFLICT, project_id=project_id, current_version=state.version)
            if control.control_version != expected_control_version:
                raise ProjectRuntimeError(RuntimeErrorCode.CONTROL_VERSION_CONFLICT, project_id=project_id, turn_id=turn_id, current_control_version=control.control_version)
            if kind == "stop":
                if turn.status != "claimed" or control.control_state != "running":
                    raise ProjectRuntimeError(RuntimeErrorCode.TURN_NOT_CLAIMED, project_id=project_id, turn_id=turn_id)
                lease = runtime_db._current_worker_lease_for_turn(
                    self._conn, project_id=project_id, turn_id=turn_id
                )
                if not (
                    _is_text(turn.attempt_id)
                    and type(turn.lease_generation) is int
                    and turn.lease_generation > 0
                    and type(turn.fencing_token) is int
                    and turn.fencing_token > 0
                    and control.attempt_id == turn.attempt_id
                    and _is_text(control.claim_worker_id)
                    and type(control.claim_lease_expires_at) is int
                    and control.claim_lease_expires_at >= 0
                    and control.claim_canonical_session_id
                    == state.conversation_tip_id
                    and lease is not None
                    and lease.lease_id == turn.attempt_id
                    and lease.worker_id == control.claim_worker_id
                    and lease.lease_generation == turn.lease_generation
                    and lease.fencing_token == turn.fencing_token
                    and lease.expires_at == control.claim_lease_expires_at
                ):
                    raise ProjectRuntimeError(
                        RuntimeErrorCode.TURN_NOT_CLAIMED,
                        project_id=project_id,
                        turn_id=turn_id,
                    )
                next_turn, next_control, event_kind = "stop_requested", "stop_requested", "run.stop_requested"
            else:
                self._require_active(state)
                operation_disposition = (
                    runtime_db
                    ._project_operation_disposition_for_turn(
                        self._conn,
                        project_id=project_id,
                        turn_id=turn_id,
                    )
                )
                if operation_disposition == "unresolved":
                    raise ProjectRuntimeError(
                        RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED,
                        project_id=project_id,
                        turn_id=turn_id,
                    )
                if operation_disposition in {
                    "pre_effect_blocked",
                    "post_effect_blocked",
                }:
                    raise ProjectRuntimeError(
                        RuntimeErrorCode.TURN_RECOVERY_BLOCKED,
                        project_id=project_id,
                        turn_id=turn_id,
                    )
                if turn.status != "stopped" or control.control_state != "stopped":
                    raise ProjectRuntimeError(RuntimeErrorCode.TURN_NOT_STOPPED, project_id=project_id, turn_id=turn_id)
                next_turn, next_control, event_kind = "queued", "resume_requested", "run.resume_requested"
            transitioned = runtime_db._transition_runtime_turn_and_control(
                self._conn, project_id=project_id, turn_id=turn_id,
                expected_turn_status=turn.status, next_turn_status=next_turn,
                expected_control_state=control.control_state,
                expected_attempt_id=turn.attempt_id,
                expected_control_version=control.control_version,
                next_control_state=next_control, now=now,
                idempotency_key=idempotency_key, command_fingerprint=fingerprint,
            )
            if transitioned is None:
                raise ProjectRuntimeError(RuntimeErrorCode.TURN_NOT_RESUMABLE, project_id=project_id, turn_id=turn_id)
            updated = self._advance_state(state, now)
            self._event(project_id, event_kind, turn_id, {"control_version": control.control_version + 1, "turn_id": turn_id, "version": updated.version}, now)
            return self._control_from_record(self._control(project_id, turn_id))

    def acknowledge_stopped(self, claim: TurnClaim) -> RunControl:
        if (
            not isinstance(claim, TurnClaim)
            or not all(
                _is_text(value)
                for value in (
                    claim.project_id,
                    claim.turn_id,
                    claim.attempt_id,
                    claim.worker_id,
                    claim.canonical_session_id,
                )
            )
            or not all(
                type(value) is int and value > 0
                for value in (
                    claim.sequence,
                    claim.lease_generation,
                    claim.fencing_token,
                )
            )
            or not _is_nonnegative_int(claim.lease_expires_at)
        ):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            state = self._require_state(claim.project_id)
            turn = self._turn(claim.project_id, claim.turn_id)
            control = self._control(claim.project_id, claim.turn_id)
            lease = runtime_db._current_worker_lease_for_turn(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )
            stored_claim_matches = self._stored_claim_matches(
                turn, control, claim
            )
            if (
                turn.status == "stopped"
                and control.control_state == "stopped"
                and lease is None
            ):
                if stored_claim_matches:
                    return self._control_from_record(control)
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_NOT_STOP_REQUESTED,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                )
            if not (
                turn.status == "stop_requested"
                and control.control_state == "stop_requested"
                and state.conversation_tip_id == claim.canonical_session_id
                and stored_claim_matches
                and self._current_lease_matches(lease, claim)
                and lease.expires_at > now
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_NOT_STOP_REQUESTED,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                )
            transitioned = runtime_db._transition_runtime_turn_and_control(
                self._conn, project_id=claim.project_id, turn_id=claim.turn_id,
                expected_turn_status="stop_requested", next_turn_status="stopped",
                expected_control_state="stop_requested",
                expected_attempt_id=claim.attempt_id,
                expected_control_version=control.control_version,
                next_control_state="stopped", now=now,
            )
            if transitioned is None:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_NOT_STOP_REQUESTED,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                )
            if not runtime_db._delete_current_worker_lease(
                self._conn, project_id=claim.project_id, turn_id=claim.turn_id,
                attempt_id=claim.attempt_id, worker_id=claim.worker_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                lease_expires_at=lease.expires_at,
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_NOT_STOP_REQUESTED,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                )
            updated = self._advance_state(state, now)
            self._event(
                claim.project_id,
                "run.stopped",
                claim.turn_id,
                {
                    "attempt_id": claim.attempt_id,
                    "fencing_token": claim.fencing_token,
                    "lease_generation": claim.lease_generation,
                    "turn_id": claim.turn_id,
                    "version": updated.version,
                },
                now,
            )
            return self._control_from_record(
                self._control(claim.project_id, claim.turn_id)
            )

    def request_turn_approval(
        self, turn_id: str, request: ApprovalRequest,
        actor: ActorContext, *, expected_control_version: int,
    ) -> TurnApproval:
        turn_id = _require_text(turn_id)
        expected_control_version = _require_version(expected_control_version)
        if not isinstance(request, ApprovalRequest):
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT, turn_id=turn_id
            )
        project_id = request.project_id
        now = self._now()
        try:
            (
                _,
                targets_json,
                boundary_json,
            ) = runtime_db._approval_identity_storage_values(request)
        except (TypeError, ValueError) as exc:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT,
                project_id=project_id if _is_text(project_id) else None,
                turn_id=turn_id,
            ) from exc
        project_id = _require_text(project_id)
        effective_runtime_version = request.expected_runtime_version + 1
        with runtime_db.write_transaction(self._conn):
            actor = self._authorize_owner(project_id, actor)
            if (
                request.requester_actor_id != actor.actor_id
                or request.authorization_actor_id != actor.actor_id
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.APPROVAL_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            existing = self._conn.execute(
                """
                SELECT * FROM project_approvals
                WHERE approval_id = ? AND operation_id IS NULL
                """,
                (request.approval_id,),
            ).fetchone()
            if existing is not None:
                stored = runtime_db._approval_from_row(existing)
                if (
                    runtime_db._row_matches_immutable_request(
                        existing,
                        request,
                        targets_json=targets_json,
                        boundary_json=boundary_json,
                    )
                    and existing["effective_runtime_version"]
                    == effective_runtime_version
                    and existing["turn_expected_control_version"]
                    == expected_control_version
                    and existing["turn_id"] == turn_id
                ):
                    return TurnApproval(turn_id=turn_id, approval=stored)
                raise ProjectRuntimeError(
                    RuntimeErrorCode.APPROVAL_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            linked_collision = self._conn.execute(
                """
                SELECT 1 FROM project_approvals
                WHERE approval_id = ? AND operation_id IS NOT NULL
                LIMIT 1
                """,
                (request.approval_id,),
            ).fetchone()
            if linked_collision is not None:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.APPROVAL_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            try:
                runtime_db._approval_storage_values(request, now)
            except (TypeError, ValueError) as exc:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.INVALID_ARGUMENT,
                    project_id=project_id,
                    turn_id=turn_id,
                ) from exc
            state = self._require_state(project_id)
            self._require_active(state)
            turn = self._turn(project_id, turn_id)
            if turn.status != "claimed":
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_NOT_CLAIMED,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            if not (
                request.expected_runtime_version == state.version
                and request.expected_lifecycle == state.lifecycle
                and request.expected_phase == state.current_phase
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.APPROVAL_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            control = self._control(project_id, turn_id)
            if control.control_version != expected_control_version:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.CONTROL_VERSION_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                    current_control_version=control.control_version,
                )
            lease = runtime_db._current_worker_lease_for_turn(
                self._conn, project_id=project_id, turn_id=turn_id
            )
            if not (
                _is_text(turn.attempt_id)
                and type(turn.lease_generation) is int
                and turn.lease_generation > 0
                and type(turn.fencing_token) is int
                and turn.fencing_token > 0
                and control.control_state == "running"
                and control.attempt_id == turn.attempt_id
                and _is_text(control.claim_worker_id)
                and type(control.claim_lease_expires_at) is int
                and control.claim_lease_expires_at >= 0
                and control.claim_canonical_session_id
                == state.conversation_tip_id
                and lease is not None
                and lease.lease_id == turn.attempt_id
                and lease.worker_id == control.claim_worker_id
                and lease.lease_generation == turn.lease_generation
                and lease.fencing_token == turn.fencing_token
                and lease.expires_at == control.claim_lease_expires_at
                and lease.expires_at > now
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_NOT_CLAIMED,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            try:
                created = runtime_db._create_approval_request(
                    self._conn,
                    request,
                    now=now,
                    effective_runtime_version=effective_runtime_version,
                    turn_expected_control_version=expected_control_version,
                )
            except runtime_db.ApprovalConflictError as exc:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.APPROVAL_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                ) from exc
            if not runtime_db._link_approval_to_claimed_turn(
                self._conn, approval_id=request.approval_id, project_id=project_id,
                turn_id=turn_id,
                expected_attempt_id=turn.attempt_id,
                expected_lease_generation=turn.lease_generation,
                expected_fencing_token=turn.fencing_token,
                now=now,
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.APPROVAL_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            updated = self._advance_state(state, now)
            self._event(
                project_id,
                "approval.requested",
                turn_id,
                {
                    "approval_id": created.approval_id,
                    "turn_id": turn_id,
                    "version": updated.version,
                },
                now,
            )
            return TurnApproval(turn_id=turn_id, approval=created)
