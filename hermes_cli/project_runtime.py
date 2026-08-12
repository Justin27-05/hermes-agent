"""Durable per-project FIFO, lease, fencing, and recovery authority.

This module does not load history, construct agents, call providers, or
deliver to a surface. Concrete worker/readback wiring remains a later-task
adapter concern.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Literal, Mapping, Protocol

from hermes_cli import project_runtime_db as runtime_db
from hermes_cli.project_policy import ActorContext

if TYPE_CHECKING:
    from hermes_cli.project_operations import ProjectOperation


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
MAX_PERSISTED_TIMESTAMP = 253_402_300_799


class RuntimeErrorCode(str, Enum):
    INVALID_ARGUMENT = "invalid_argument"
    ACTOR_NOT_AUTHORIZED = "actor_not_authorized"
    PROJECT_NOT_MANAGED = "project_not_managed"
    PROJECT_NOT_ACTIVE = "project_not_active"
    PROJECT_QUEUE_NOT_EMPTY = "project_queue_not_empty"
    PROJECT_LIFECYCLE_CONFLICT = "project_lifecycle_conflict"
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
    OPERATION_APPROVAL_REQUIRED = "operation_approval_required"
    STALE_DISPATCHER_LEASE = "stale_dispatcher_lease"
    STALE_TURN_CLAIM = "stale_turn_claim"
    TURN_EXECUTION_NOT_STARTED = "turn_execution_not_started"
    TURN_OPERATIONS_UNRESOLVED = "turn_operations_unresolved"
    TERMINAL_RESULT_CONFLICT = "terminal_result_conflict"
    PROJECT_AUTHORITY_CONFLICT = "project_authority_conflict"


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
class ProjectRuntimeView:
    """Actor-authorized projection used by command adapters."""

    project_id: str
    lifecycle: str
    current_phase: str
    version: int
    canonical_session_id: str
    queue_depth: int
    active_turn_id: str | None
    active_run_control: str | None
    active_control_version: int | None
    pending_approval_id: str | None
    last_event_sequence: int


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
class ClaimControl:
    state: Literal[
        "running",
        "awaiting_approval",
        "stop_requested",
    ]
    control_version: int
    lease_expires_at: int


@dataclass(frozen=True)
class TurnAttemptIdentity:
    project_id: str
    turn_id: str
    sequence: int
    worker_id: str
    attempt_id: str
    lease_generation: int
    fencing_token: int
    canonical_session_id: str
    lease_expires_at: int


@dataclass(frozen=True)
class TurnOrigin:
    binding_id: str
    surface: Literal["desktop", "discord"]
    external_binding_id: str
    actor_id: str


@dataclass(frozen=True)
class TurnExecutionInput:
    attempt: TurnAttemptIdentity
    payload: Mapping[str, JSONValue]
    origin: TurnOrigin
    contract_revision: int


@dataclass(frozen=True)
class TerminalTurnResult:
    attempt: TurnAttemptIdentity
    status: Literal["succeeded", "failed"]
    result_id: str


@dataclass(frozen=True)
class PreparedTerminalDecision:
    action: Literal["wait", "publish", "discard"]
    terminal: TerminalTurnResult | None
    discard_authority: Literal[
        "stop_requested",
        "cancelled",
        "superseded_attempt",
        "superseded_terminal",
        "recovery_blocked",
    ] | None


@dataclass(frozen=True)
class PreparedApprovalCheckpointDecision:
    action: Literal["wait", "discard"]


@dataclass(frozen=True)
class _RetiredAttemptCertificate:
    event_id: str
    event_sequence: int
    created_at: int
    version: int
    attempt: TurnAttemptIdentity


_RETIRED_ATTEMPT_KEYS = frozenset(
    {
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "canonical_session_id",
        "lease_expires_at",
    }
)
_CLAIMED_ATTEMPT_EVENT_KEYS = frozenset(
    {
        "attempt_id",
        "fencing_token",
        "lease_generation",
        "sequence",
        "turn_id",
        "version",
    }
)
_RECONCILING_ATTEMPT_EVENT_KEYS = frozenset(
    {
        "attempt_id",
        "fencing_token",
        "lease_generation",
        "source_status",
        "turn_id",
        "version",
    }
)
_REQUEUED_ATTEMPT_EVENT_KEYS = frozenset(
    {
        "attempt",
        "attempt_id",
        "fencing_token",
        "lease_generation",
        "source_status",
        "turn_id",
        "version",
    }
)
_LEGACY_REQUEUED_ATTEMPT_EVENT_KEYS = (
    _REQUEUED_ATTEMPT_EVENT_KEYS - {"attempt"}
)
_CANCELLED_ATTEMPT_EVENT_KEYS = frozenset(
    {"retired_attempt_event_id", "turn_id", "version"}
)
_LEGACY_CANCELLED_ATTEMPT_EVENT_KEYS = (
    _CANCELLED_ATTEMPT_EVENT_KEYS - {"retired_attempt_event_id"}
)
_TURN_AUTHORITY_EVENT_KINDS = frozenset(
    {
        "turn.claimed",
        "run.stop_requested",
        "run.stopped",
        "run.resume_requested",
        "turn.cancelled",
        "turn.reconciling",
        "turn.requeued",
        "turn.succeeded",
        "turn.failed",
    }
)


@dataclass(frozen=True)
class TerminalTranscriptAcknowledgement:
    batch_id: str
    attempt: TurnAttemptIdentity
    status: Literal["succeeded", "failed"]
    result_id: str


@dataclass(frozen=True)
class TerminalTranscriptConflict:
    terminal: TerminalTranscriptAcknowledgement
    conflict_key: str
    observed_message_count: int


@dataclass(frozen=True)
class DispatcherLease:
    instance_id: str
    generation: int
    fencing_token: int
    expires_at: int


@dataclass(frozen=True)
class WorkerStart:
    source: Literal["queued_turn", "approved_operation"]
    claim: TurnClaim
    operation: ProjectOperation | None
    dispatcher_lease: DispatcherLease


@dataclass(frozen=True)
class RunnableProjectCursor:
    dispatch_membership_sequence: int
    project_id: str


@dataclass(frozen=True)
class RunnableProject:
    project_id: str
    head_turn_id: str
    sequence: int
    dispatch_membership_sequence: int


@dataclass(frozen=True)
class RunnableProjectScanResult:
    projects: tuple[RunnableProject, ...]
    scanned_through: RunnableProjectCursor | None
    reached_epoch_end: bool


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
class Task7TerminalReadbackEvidence:
    result: TurnReadbackResult
    transcript_batch_id: str | None


class Task7TerminalReadbackPort(Protocol):
    def read_turn_with_evidence(
        self,
        request: TurnReadbackRequest,
    ) -> Task7TerminalReadbackEvidence: ...


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


def _require_task7_batch_id(value: object) -> str:
    if type(value) is not str:
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ProjectRuntimeError(
            RuntimeErrorCode.INVALID_ARGUMENT
        ) from exc
    if not (
        parsed.version == 4
        and parsed.variant == uuid.RFC_4122
        and str(parsed) == value
    ):
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    return value


def _require_version(value: object) -> int:
    if not _is_nonnegative_int(value):
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    return value


def _require_dispatcher_instance_id(value: object) -> str:
    if type(value) is not str:
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ProjectRuntimeError(
            RuntimeErrorCode.INVALID_ARGUMENT
        ) from exc
    if parsed.version != 4 or str(parsed) != value:
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    return value


def _require_dispatcher_lease(value: object) -> DispatcherLease:
    if type(value) is not DispatcherLease:
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    _require_dispatcher_instance_id(value.instance_id)
    if not (
        type(value.generation) is int
        and value.generation > 0
        and type(value.fencing_token) is int
        and value.fencing_token > 0
        and type(value.expires_at) is int
        and 0 <= value.expires_at <= SQLITE_INT_MAX
    ):
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    return value


def _require_membership_sequence(value: object) -> int:
    if not (
        type(value) is int
        and 1 <= value <= SQLITE_INT_MAX
    ):
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    return value


def _require_runnable_cursor(
    value: object,
) -> RunnableProjectCursor:
    if type(value) is not RunnableProjectCursor:
        raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
    _require_membership_sequence(
        value.dispatch_membership_sequence
    )
    _require_text(value.project_id)
    return value


def _dispatcher_lease_from_row(
    row: sqlite3.Row,
) -> DispatcherLease | None:
    try:
        instance_id = row["instance_id"]
        generation = row["generation"]
        fencing_token = row["fencing_token"]
        expires_at = row["expires_at"]
        updated_at = row["updated_at"]
    except (IndexError, KeyError) as exc:
        raise RuntimeError("malformed dispatcher lease") from exc
    if not (
        row["lease_name"] == "core"
        and type(generation) is int
        and 0 <= generation <= SQLITE_INT_MAX
        and type(fencing_token) is int
        and 0 <= fencing_token <= SQLITE_INT_MAX
        and type(expires_at) is int
        and 0 <= expires_at <= SQLITE_INT_MAX
        and type(updated_at) is int
        and 0 <= updated_at <= SQLITE_INT_MAX
    ):
        raise RuntimeError("malformed dispatcher lease")
    if instance_id is None:
        if expires_at != updated_at:
            raise RuntimeError("malformed dispatcher lease")
        return None
    try:
        canonical_instance_id = _require_dispatcher_instance_id(
            instance_id
        )
    except ProjectRuntimeError as exc:
        raise RuntimeError("malformed dispatcher lease") from exc
    if generation == 0 or fencing_token == 0:
        raise RuntimeError("malformed dispatcher lease")
    return DispatcherLease(
        canonical_instance_id,
        generation,
        fencing_token,
        expires_at,
    )


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

    def _event_outbox(self):
        from hermes_cli.project_events import ProjectEventOutbox

        return ProjectEventOutbox(
            self._conn,
            clock=self._clock,
            id_factory=self._id_factory,
        )

    def append_project_event(
        self,
        project_id: str,
        kind: str,
        payload: Mapping[str, object],
        *,
        turn_id: str | None = None,
        event_id: str | None = None,
    ):
        return self._event_outbox().append_event(
            project_id,
            kind,
            payload,
            turn_id=turn_id,
            event_id=event_id,
        )

    def events_after(
        self,
        project_id: str,
        cursor: int,
        limit: int,
    ):
        return self._event_outbox().events_after(
            project_id,
            cursor,
            limit,
        )

    def claim_delivery(
        self,
        project_id: str,
        binding_id: str,
        *,
        lease_seconds: int,
    ):
        return self._event_outbox().claim_delivery(
            project_id,
            binding_id,
            lease_seconds=lease_seconds,
        )

    def ack_delivery(self, claim):
        return self._event_outbox().acknowledge_delivery(claim)

    def nack_delivery(self, claim) -> None:
        self._event_outbox().reject_delivery(claim)

    def renew_delivery(self, claim, *, lease_seconds: int):
        return self._event_outbox().renew_delivery(
            claim,
            lease_seconds=lease_seconds,
        )

    def complete_delivery(
        self,
        claim,
        *,
        remote_message_ids: tuple[str, ...],
    ):
        return self._event_outbox().complete_delivery(
            claim,
            remote_message_ids=remote_message_ids,
        )

    def defer_delivery(
        self,
        claim,
        *,
        error_code: str,
        delay_seconds: int,
    ) -> None:
        self._event_outbox().defer_delivery(
            claim,
            error_code=error_code,
            delay_seconds=delay_seconds,
        )

    def block_delivery(self, claim, *, error_code: str):
        return self._event_outbox().block_delivery(
            claim,
            error_code=error_code,
        )

    def suppress_origin_delivery(self, claim):
        return self._event_outbox().suppress_origin_delivery(claim)

    def register_verified_artifact(
        self,
        project_id: str,
        *,
        artifact_id: str,
        path: str | Path,
        metadata: Mapping[str, object],
        turn_id: str | None = None,
        readback: Callable[[Path], bytes] | None = None,
    ):
        return self._event_outbox().register_verified_artifact(
            project_id,
            artifact_id=artifact_id,
            path=path,
            metadata=metadata,
            turn_id=turn_id,
            readback=readback,
        )

    def artifact_for_id(self, project_id: str, artifact_id: str):
        return self._event_outbox().artifact_for_id(
            project_id,
            artifact_id,
        )

    @staticmethod
    def _command_event_id(project_id: str, idempotency_key: str) -> str:
        digest = hashlib.sha256(
            f"{project_id}\0{idempotency_key}".encode("utf-8")
        ).hexdigest()
        return f"command-{digest}"

    @staticmethod
    def _command_fingerprint(
        kind: str,
        actor: ActorContext,
        expected_version: int,
        payload: Mapping[str, object],
    ) -> str:
        canonical = canonical_json_object(
            {
                "actor": {
                    "actor_id": actor.actor_id,
                    "authority": (
                        "owner" if actor.is_owner else actor.surface
                    ),
                },
                "expected_version": expected_version,
                "kind": kind,
                "payload": payload,
            }
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _command_replay(
        self,
        *,
        project_id: str,
        event_id: str,
        kind: str,
        fingerprint: str,
    ) -> runtime_db.RuntimeState | None:
        row = self._conn.execute(
            """
            SELECT project_id, kind, payload_json
            FROM project_events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = _decode_canonical_object(row["payload_json"])
        except (ProjectRuntimeError, RuntimeError):
            raise ProjectRuntimeError(
                RuntimeErrorCode.IDEMPOTENCY_CONFLICT,
                project_id=project_id,
            ) from None
        stored_fingerprint = payload.get("command_fingerprint")
        if not (
            row["project_id"] == project_id
            and row["kind"] == kind
            and type(stored_fingerprint) is str
            and stored_fingerprint == fingerprint
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.IDEMPOTENCY_CONFLICT,
                project_id=project_id,
            )
        return self._require_state(project_id)

    @staticmethod
    def _require_creating_owner(actor: object) -> ActorContext:
        if not (
            isinstance(actor, ActorContext)
            and type(actor.actor_id) is str
            and bool(actor.actor_id)
            and actor.surface in {"desktop", "discord"}
            and type(actor.binding_id) is str
            and bool(actor.binding_id)
            and actor.is_owner is True
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.ACTOR_NOT_AUTHORIZED
            )
        return actor

    def create_managed_project(
        self,
        actor: ActorContext,
        *,
        name: str,
        idempotency_key: str,
        expected_version: int,
        current_phase: str = "planning",
        slug: str | None = None,
        folders: tuple[str, ...] = (),
        primary_path: str | None = None,
        description: str | None = None,
        icon: str | None = None,
        color: str | None = None,
        board_slug: str | None = None,
    ) -> runtime_db.RuntimeState:
        """Create, adopt, bind, and announce one project atomically."""
        from hermes_cli import projects_db

        actor = self._require_creating_owner(actor)
        name = _require_text(name)
        idempotency_key = _require_text(idempotency_key)
        expected_version = _require_version(expected_version)
        current_phase = _require_text(current_phase)
        if expected_version != 0 or not isinstance(folders, tuple):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        project_id = (
            "p_cmd_"
            + hashlib.sha256(
                f"{actor.actor_id}\0{idempotency_key}".encode("utf-8")
            ).hexdigest()[:24]
        )
        event_id = self._command_event_id(
            project_id, idempotency_key
        )
        command_payload = {
            "board_slug": board_slug,
            "color": color,
            "current_phase": current_phase,
            "description": description,
            "folders": list(folders),
            "icon": icon,
            "name": name,
            "primary_path": primary_path,
            "slug": slug,
        }
        fingerprint = self._command_fingerprint(
            "project.created",
            actor,
            expected_version,
            command_payload,
        )
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            replay = self._command_replay(
                project_id=project_id,
                event_id=event_id,
                kind="project.created",
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay
            if projects_db.get_project(self._conn, project_id) is not None:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.IDEMPOTENCY_CONFLICT,
                    project_id=project_id,
                )
            projects_db.create_project(
                self._conn,
                project_id=project_id,
                name=name,
                slug=slug,
                folders=folders,
                primary_path=primary_path,
                description=description,
                icon=icon,
                color=color,
                board_slug=board_slug,
                caller_owns_transaction=True,
            )
            root_id = (
                "session-"
                + hashlib.sha256(project_id.encode("utf-8")).hexdigest()[:24]
            )
            runtime_db.create_project_conversation(
                self._conn,
                project_id=project_id,
                conversation_id=root_id,
                current_phase=current_phase,
                now=now,
            )
            runtime_db.bind_surface(
                self._conn,
                binding_id=actor.binding_id,
                project_id=project_id,
                surface=actor.surface,
                external_binding_id=actor.binding_id,
                actor_id=actor.actor_id,
                now=now,
            )
            self._event(
                project_id,
                "project.created",
                None,
                {
                    "command_fingerprint": fingerprint,
                    "surface": {"lifecycle": "active", "name": name},
                },
                now,
                event_id=event_id,
            )
            return self._require_state(project_id)

    def rename_project(
        self,
        project_id: str,
        name: str,
        actor: ActorContext,
        *,
        idempotency_key: str,
        expected_version: int,
    ) -> runtime_db.RuntimeState:
        from hermes_cli import projects_db

        project_id = _require_text(project_id)
        name = _require_text(name)
        idempotency_key = _require_text(idempotency_key)
        expected_version = _require_version(expected_version)
        now = self._now()
        event_id = self._command_event_id(project_id, idempotency_key)
        with runtime_db.write_transaction(self._conn):
            actor = self._authorize_owner(project_id, actor)
            fingerprint = self._command_fingerprint(
                "project.renamed",
                actor,
                expected_version,
                {"name": name},
            )
            replay = self._command_replay(
                project_id=project_id,
                event_id=event_id,
                kind="project.renamed",
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay
            state = self._require_state(project_id)
            if state.version != expected_version:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_VERSION_CONFLICT,
                    project_id=project_id,
                    current_version=state.version,
                )
            if not projects_db.update_project(
                self._conn,
                project_id,
                name=name,
                caller_owns_transaction=True,
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_NOT_MANAGED,
                    project_id=project_id,
                )
            updated = self._advance_state(state, now)
            self._event(
                project_id,
                "project.renamed",
                None,
                {
                    "command_fingerprint": fingerprint,
                    "surface": {
                        "lifecycle": updated.lifecycle,
                        "name": name,
                    },
                },
                now,
                event_id=event_id,
            )
            return updated

    def snapshot_for_actor(
        self, project_id: str, actor: ActorContext
    ) -> ProjectRuntimeView:
        """Read one complete adapter projection under durable binding auth."""
        project_id = _require_text(project_id)
        owns_snapshot = not self._conn.in_transaction
        if owns_snapshot:
            self._conn.execute("BEGIN")
        try:
            self._authorize_owner(project_id, actor)
            result = self._snapshot_projection(project_id)
        except Exception:
            if owns_snapshot:
                self._conn.execute("ROLLBACK")
            raise
        if owns_snapshot:
            self._conn.execute("COMMIT")
        return result

    def snapshot_for_core(
        self,
        project_id: str,
        dispatcher_lease: DispatcherLease,
    ) -> ProjectRuntimeView:
        """Read a projection only for the exact live Core dispatcher lease."""
        project_id = _require_text(project_id)
        if type(dispatcher_lease) is not DispatcherLease:
            raise ProjectRuntimeError(
                RuntimeErrorCode.ACTOR_NOT_AUTHORIZED,
                project_id=project_id,
            )
        dispatcher_lease = _require_dispatcher_lease(dispatcher_lease)
        owns_snapshot = not self._conn.in_transaction
        if owns_snapshot:
            self._conn.execute("BEGIN")
        try:
            self._require_dispatcher_start_authority(
                dispatcher_lease, self._now()
            )
            result = self._snapshot_projection(project_id)
        except Exception:
            if owns_snapshot:
                self._conn.execute("ROLLBACK")
            raise
        if owns_snapshot:
            self._conn.execute("COMMIT")
        return result

    def _snapshot_projection(
        self, project_id: str
    ) -> ProjectRuntimeView:
        state = self._require_state(project_id)
        queue_depth = self._conn.execute(
            """
            SELECT COUNT(*) FROM project_turns
            WHERE project_id = ? AND status = 'queued'
            """,
            (project_id,),
        ).fetchone()[0]
        active = self._conn.execute(
            """
            SELECT turn.turn_id, control.control_state,
                   control.control_version
            FROM project_turns AS turn
            JOIN project_run_controls AS control
              ON control.project_id = turn.project_id
             AND control.turn_id = turn.turn_id
            WHERE turn.project_id = ?
              AND turn.status NOT IN (
                  'queued', 'succeeded', 'failed', 'cancelled'
              )
            ORDER BY turn.sequence
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        approval = self._conn.execute(
            """
            SELECT approval_id FROM project_approvals
            WHERE project_id = ? AND status = 'pending'
            ORDER BY created_at, approval_id
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        last_event_sequence = self._conn.execute(
            """
            SELECT COALESCE(MAX(sequence), 0)
            FROM project_events WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()[0]
        assert state.current_phase is not None
        assert state.conversation_tip_id is not None
        return ProjectRuntimeView(
            project_id=project_id,
            lifecycle=state.lifecycle,
            current_phase=state.current_phase,
            version=state.version,
            canonical_session_id=state.conversation_tip_id,
            queue_depth=queue_depth,
            active_turn_id=(
                active["turn_id"] if active is not None else None
            ),
            active_run_control=(
                active["control_state"] if active is not None else None
            ),
            active_control_version=(
                active["control_version"]
                if active is not None
                else None
            ),
            pending_approval_id=(
                approval["approval_id"] if approval is not None else None
            ),
            last_event_sequence=last_event_sequence,
        )

    def artifact_for_actor(
        self,
        project_id: str,
        artifact_id: str,
        actor: ActorContext,
    ) -> Mapping[str, object] | None:
        project_id = _require_text(project_id)
        artifact_id = _require_text(artifact_id)
        owns_snapshot = not self._conn.in_transaction
        if owns_snapshot:
            self._conn.execute("BEGIN")
        try:
            self._authorize_owner(project_id, actor)
            self._require_state(project_id)
            artifact = self.artifact_for_id(
                project_id, artifact_id
            )
            artifact_row = self._conn.execute(
                """
                SELECT created_at
                FROM project_artifacts
                WHERE project_id = ? AND artifact_id = ?
                """,
                (project_id, artifact_id),
            ).fetchone()
        except Exception:
            if owns_snapshot:
                self._conn.execute("ROLLBACK")
            raise
        if owns_snapshot:
            self._conn.execute("COMMIT")
        if artifact is None:
            return None
        if artifact_row is None:
            raise RuntimeError("project artifact disappeared")
        return MappingProxyType(
            {
                "artifact_id": artifact.artifact_id,
                "created_at": artifact_row["created_at"],
                "metadata": artifact.metadata,
                "path": artifact.path,
                "project_id": artifact.project_id,
                "status": artifact.status,
                "turn_id": artifact.turn_id,
                "verified_at": artifact.verified_at,
            }
        )

    def resolve_approval(
        self,
        project_id: str,
        approval_id: str,
        outcome: Literal["approved", "denied"],
        actor: ActorContext,
        *,
        idempotency_key: str,
        expected_version: int,
    ) -> ApprovalRequest:
        project_id = _require_text(project_id)
        approval_id = _require_text(approval_id)
        idempotency_key = _require_text(idempotency_key)
        expected_version = _require_version(expected_version)
        if outcome not in {"approved", "denied"}:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT,
                project_id=project_id,
            )
        now = self._now()
        event_id = self._command_event_id(project_id, idempotency_key)
        with runtime_db.write_transaction(self._conn):
            actor = self._authorize_owner(project_id, actor)
            fingerprint = self._command_fingerprint(
                "approval.resolved",
                actor,
                expected_version,
                {"approval_id": approval_id, "outcome": outcome},
            )
            replay = self._command_replay(
                project_id=project_id,
                event_id=event_id,
                kind="approval.resolved",
                fingerprint=fingerprint,
            )
            if replay is not None:
                row = self._conn.execute(
                    """
                    SELECT * FROM project_approvals
                    WHERE project_id = ? AND approval_id = ?
                      AND operation_id IS NULL
                    """,
                    (project_id, approval_id),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        "approval replay lost its durable approval"
                    )
                return runtime_db._approval_from_row(row)
            approval_row = self._conn.execute(
                """
                SELECT operation_id
                FROM project_approvals
                WHERE project_id = ? AND approval_id = ?
                """,
                (project_id, approval_id),
            ).fetchone()
            if (
                approval_row is not None
                and approval_row["operation_id"] is not None
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.OPERATION_APPROVAL_REQUIRED,
                    project_id=project_id,
                )
            state = self._require_state(project_id)
            if state.version != expected_version:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_VERSION_CONFLICT,
                    project_id=project_id,
                    current_version=state.version,
                )
            approval = runtime_db.resolve_approval(
                self._conn,
                approval_id=approval_id,
                resolver=actor,
                outcome=outcome,
                now=now,
            )
            if approval is None or approval.project_id != project_id:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.APPROVAL_CONFLICT,
                    project_id=project_id,
                )
            self._event(
                project_id,
                "approval.resolved",
                None,
                {"command_fingerprint": fingerprint},
                now,
                event_id=event_id,
            )
            return approval

    def mark_technically_complete(
        self,
        project_id: str,
        dispatcher_lease: DispatcherLease,
        *,
        idempotency_key: str,
        expected_version: int,
    ) -> runtime_db.RuntimeState:
        return self._transition_project_lifecycle(
            project_id,
            dispatcher_lease,
            idempotency_key=idempotency_key,
            expected_version=expected_version,
            event_kind="project.technically_completed",
            target="awaiting_acceptance",
            allowed_sources=("active",),
            hermes_only=True,
            require_empty_queue=True,
        )

    def accept_completion(
        self,
        project_id: str,
        actor: ActorContext,
        *,
        idempotency_key: str,
        expected_version: int,
    ) -> runtime_db.RuntimeState:
        return self._transition_project_lifecycle(
            project_id,
            actor,
            idempotency_key=idempotency_key,
            expected_version=expected_version,
            event_kind="project.completion_accepted",
            target="completed",
            allowed_sources=("awaiting_acceptance",),
        )

    def reopen_project(
        self,
        project_id: str,
        actor: ActorContext,
        *,
        idempotency_key: str,
        expected_version: int,
    ) -> runtime_db.RuntimeState:
        return self._transition_project_lifecycle(
            project_id,
            actor,
            idempotency_key=idempotency_key,
            expected_version=expected_version,
            event_kind="project.reopened",
            target="active",
            allowed_sources=("awaiting_acceptance", "completed"),
        )

    def _transition_project_lifecycle(
        self,
        project_id: str,
        authority: ActorContext | DispatcherLease,
        *,
        idempotency_key: str,
        expected_version: int,
        event_kind: str,
        target: runtime_db.Lifecycle,
        allowed_sources: tuple[runtime_db.Lifecycle, ...],
        hermes_only: bool = False,
        require_empty_queue: bool = False,
    ) -> runtime_db.RuntimeState:
        from hermes_cli import projects_db

        project_id = _require_text(project_id)
        idempotency_key = _require_text(idempotency_key)
        expected_version = _require_version(expected_version)
        now = self._now()
        event_id = self._command_event_id(project_id, idempotency_key)
        with runtime_db.write_transaction(self._conn):
            if hermes_only:
                if type(authority) is not DispatcherLease:
                    raise ProjectRuntimeError(
                        RuntimeErrorCode.ACTOR_NOT_AUTHORIZED,
                        project_id=project_id,
                    )
                dispatcher_lease = _require_dispatcher_lease(authority)
                self._require_dispatcher_start_authority(
                    dispatcher_lease, now
                )
                actor = ActorContext(
                    "hermes", "system", "core", False
                )
            else:
                actor = self._authorize_owner(project_id, authority)
            fingerprint = self._command_fingerprint(
                event_kind,
                actor,
                expected_version,
                {"target": target},
            )
            replay = self._command_replay(
                project_id=project_id,
                event_id=event_id,
                kind=event_kind,
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay
            state = self._require_state(project_id)
            if state.version != expected_version:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_VERSION_CONFLICT,
                    project_id=project_id,
                    current_version=state.version,
                )
            if state.lifecycle not in allowed_sources:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_LIFECYCLE_CONFLICT,
                    project_id=project_id,
                    current_version=state.version,
                )
            if require_empty_queue:
                pending = self._conn.execute(
                    """
                    SELECT 1 FROM project_turns
                    WHERE project_id = ?
                      AND status NOT IN (
                          'succeeded', 'failed', 'cancelled'
                      )
                    LIMIT 1
                    """,
                    (project_id,),
                ).fetchone()
                if pending is not None:
                    raise ProjectRuntimeError(
                        RuntimeErrorCode.PROJECT_QUEUE_NOT_EMPTY,
                        project_id=project_id,
                        current_version=state.version,
                    )
            updated = runtime_db.transition_lifecycle(
                self._conn,
                project_id=project_id,
                expected_version=expected_version,
                lifecycle=target,
                updated_at=now,
            )
            if updated is None:
                current = runtime_db.runtime_state_for_project(
                    self._conn, project_id
                )
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_VERSION_CONFLICT,
                    project_id=project_id,
                    current_version=(
                        current.version if current is not None else None
                    ),
                )
            project = projects_db.get_project(self._conn, project_id)
            if project is None:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_NOT_MANAGED,
                    project_id=project_id,
                )
            self._event(
                project_id,
                event_kind,
                None,
                {
                    "command_fingerprint": fingerprint,
                    "surface": {
                        "lifecycle": updated.lifecycle,
                        "name": project.name,
                    },
                },
                now,
                event_id=event_id,
            )
            return updated

    def _dispatcher_now_and_expiry(
        self, lease_seconds: object
    ) -> tuple[int, int]:
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        now = self._now()
        if now > SQLITE_INT_MAX - lease_seconds:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        return now, now + lease_seconds

    def runnable_project_membership_upper_watermark(
        self,
    ) -> int | None:
        """Read the inclusive upper bound for one runnable scan epoch."""
        member = runtime_db._runnable_membership_upper(self._conn)
        return (
            member.dispatch_membership_sequence
            if member is not None
            else None
        )

    def scan_runnable_projects(
        self,
        *,
        after: RunnableProjectCursor | None,
        through_membership_sequence: int,
        limit: int,
    ) -> RunnableProjectScanResult:
        """Classify one bounded raw membership page without claiming."""
        if after is not None:
            after = _require_runnable_cursor(after)
        through_membership_sequence = _require_membership_sequence(
            through_membership_sequence
        )
        if not (
            type(limit) is int
            and 1 <= limit <= 100
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        if (
            after is not None
            and after.dispatch_membership_sequence
            > through_membership_sequence
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )

        raw_members = runtime_db._runnable_membership_page(
            self._conn,
            after=(
                (
                    after.dispatch_membership_sequence,
                    after.project_id,
                )
                if after is not None
                else None
            ),
            through_membership_sequence=(
                through_membership_sequence
            ),
            limit=limit,
        )
        projects: list[RunnableProject] = []
        for member in raw_members:
            state = runtime_db.runtime_state_for_project(
                self._conn,
                member.project_id,
            )
            if state is None or (
                state.dispatch_membership_sequence
                != member.dispatch_membership_sequence
            ):
                raise RuntimeError(
                    "runnable membership changed during classification"
                )
            if state.lifecycle != "active":
                continue
            if (
                state.transcript_pending_batch_id is not None
                or state.transcript_dispatch_block_key is not None
            ):
                continue
            head = runtime_db._runnable_project_head(
                self._conn,
                project_id=member.project_id,
            )
            if head is None:
                continue
            runtime_db._validate_runtime_turn_pair(
                self._conn,
                turn=head,
            )
            if head.status != "queued":
                continue
            projects.append(
                RunnableProject(
                    member.project_id,
                    head.turn_id,
                    head.sequence,
                    member.dispatch_membership_sequence,
                )
            )

        scanned_through = (
            RunnableProjectCursor(
                raw_members[-1].dispatch_membership_sequence,
                raw_members[-1].project_id,
            )
            if raw_members
            else after
        )
        reached_epoch_end = not raw_members
        if raw_members:
            reached_epoch_end = (
                raw_members[-1].dispatch_membership_sequence
                == through_membership_sequence
                or not runtime_db._runnable_membership_remaining(
                    self._conn,
                    after=(
                        raw_members[
                            -1
                        ].dispatch_membership_sequence,
                        raw_members[-1].project_id,
                    ),
                    through_membership_sequence=(
                        through_membership_sequence
                    ),
                )
            )
        return RunnableProjectScanResult(
            tuple(projects),
            scanned_through,
            reached_epoch_end,
        )

    def acquire_dispatcher_lease(
        self,
        instance_id: str,
        *,
        lease_seconds: int,
    ) -> DispatcherLease | None:
        """Acquire or take over the sticky profile-wide Core lease."""
        instance_id = _require_dispatcher_instance_id(instance_id)
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        with runtime_db.write_transaction(self._conn):
            now, requested_expires_at = (
                self._dispatcher_now_and_expiry(lease_seconds)
            )
            row = self._conn.execute(
                """
                SELECT lease_name, instance_id, generation,
                       fencing_token, expires_at, updated_at
                FROM project_dispatcher_leases
                WHERE lease_name = 'core'
                """
            ).fetchone()
            if row is None:
                self._conn.execute(
                    """
                    INSERT INTO project_dispatcher_leases (
                        lease_name, instance_id, generation,
                        fencing_token, expires_at, updated_at
                    ) VALUES ('core', ?, 1, 1, ?, ?)
                    """,
                    (instance_id, requested_expires_at, now),
                )
                return DispatcherLease(
                    instance_id,
                    1,
                    1,
                    requested_expires_at,
                )

            current = _dispatcher_lease_from_row(row)
            if (
                current is not None
                and current.instance_id == instance_id
                and now < current.expires_at
            ):
                return current
            if current is not None and now < current.expires_at:
                return None

            generation = row["generation"]
            fencing_token = row["fencing_token"]
            if (
                generation >= SQLITE_INT_MAX
                or fencing_token >= SQLITE_INT_MAX
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.INVALID_ARGUMENT
                )
            changed = self._conn.execute(
                """
                UPDATE project_dispatcher_leases
                SET instance_id = ?,
                    generation = ?,
                    fencing_token = ?,
                    expires_at = ?,
                    updated_at = ?
                WHERE lease_name = 'core'
                  AND instance_id IS ?
                  AND generation IS ?
                  AND fencing_token IS ?
                  AND expires_at IS ?
                  AND updated_at IS ?
                """,
                (
                    instance_id,
                    generation + 1,
                    fencing_token + 1,
                    requested_expires_at,
                    now,
                    row["instance_id"],
                    generation,
                    fencing_token,
                    row["expires_at"],
                    row["updated_at"],
                ),
            )
            if changed.rowcount != 1:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.STALE_DISPATCHER_LEASE
                )
            return DispatcherLease(
                instance_id,
                generation + 1,
                fencing_token + 1,
                requested_expires_at,
            )

    def renew_dispatcher_lease(
        self,
        lease: DispatcherLease,
        *,
        lease_seconds: int,
    ) -> DispatcherLease:
        """Extend only the matching live Core epoch."""
        lease = _require_dispatcher_lease(lease)
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        with runtime_db.write_transaction(self._conn):
            now, requested_expires_at = (
                self._dispatcher_now_and_expiry(lease_seconds)
            )
            row = self._conn.execute(
                """
                SELECT lease_name, instance_id, generation,
                       fencing_token, expires_at, updated_at
                FROM project_dispatcher_leases
                WHERE lease_name = 'core'
                """
            ).fetchone()
            current = (
                _dispatcher_lease_from_row(row)
                if row is not None
                else None
            )
            if not (
                current is not None
                and current.instance_id == lease.instance_id
                and current.generation == lease.generation
                and current.fencing_token == lease.fencing_token
                and lease.expires_at <= current.expires_at
                and now < current.expires_at
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.STALE_DISPATCHER_LEASE
                )
            expires_at = max(
                current.expires_at,
                requested_expires_at,
            )
            changed = self._conn.execute(
                """
                UPDATE project_dispatcher_leases
                SET expires_at = ?, updated_at = ?
                WHERE lease_name = 'core'
                  AND instance_id = ?
                  AND generation = ?
                  AND fencing_token = ?
                  AND expires_at = ?
                  AND updated_at = ?
                """,
                (
                    expires_at,
                    now,
                    current.instance_id,
                    current.generation,
                    current.fencing_token,
                    current.expires_at,
                    row["updated_at"],
                ),
            )
            if changed.rowcount != 1:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.STALE_DISPATCHER_LEASE
                )
            return DispatcherLease(
                current.instance_id,
                current.generation,
                current.fencing_token,
                expires_at,
            )

    def release_dispatcher_lease(
        self,
        lease: DispatcherLease,
    ) -> bool:
        """Release only one exact Core lease DTO and retain its counters."""
        lease = _require_dispatcher_lease(lease)
        now = self._now()
        if now > SQLITE_INT_MAX:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        with runtime_db.write_transaction(self._conn):
            row = self._conn.execute(
                """
                SELECT lease_name, instance_id, generation,
                       fencing_token, expires_at, updated_at
                FROM project_dispatcher_leases
                WHERE lease_name = 'core'
                """
            ).fetchone()
            current = (
                _dispatcher_lease_from_row(row)
                if row is not None
                else None
            )
            if current is None or current != lease:
                return False
            changed = self._conn.execute(
                """
                UPDATE project_dispatcher_leases
                SET instance_id = NULL,
                    expires_at = ?,
                    updated_at = ?
                WHERE lease_name = 'core'
                  AND instance_id = ?
                  AND generation = ?
                  AND fencing_token = ?
                  AND expires_at = ?
                  AND updated_at = ?
                """,
                (
                    now,
                    now,
                    current.instance_id,
                    current.generation,
                    current.fencing_token,
                    current.expires_at,
                    row["updated_at"],
                ),
            )
            if changed.rowcount != 1:
                return False
            return True

    def _require_dispatcher_start_authority(
        self,
        dispatcher_lease: DispatcherLease,
        now: int,
    ) -> None:
        """Validate one exact live Core tuple under caller-owned transaction."""
        if not self._conn.in_transaction:
            raise RuntimeError(
                "dispatcher authority requires an active transaction"
            )
        row = self._conn.execute(
            """
            SELECT instance_id, generation, fencing_token, expires_at
            FROM project_dispatcher_leases
            WHERE lease_name = 'core'
            """
        ).fetchone()
        if not (
            row is not None
            and type(row["instance_id"]) is str
            and row["instance_id"] == dispatcher_lease.instance_id
            and type(row["generation"]) is int
            and row["generation"] == dispatcher_lease.generation
            and type(row["fencing_token"]) is int
            and row["fencing_token"]
            == dispatcher_lease.fencing_token
            and type(row["expires_at"]) is int
            and row["expires_at"] == dispatcher_lease.expires_at
            and now < row["expires_at"]
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.STALE_DISPATCHER_LEASE
            )

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

    def _event(
        self,
        project_id: str,
        kind: str,
        turn_id: str | None,
        payload: dict[str, object],
        now: int,
        *,
        event_id: str | None = None,
        deliver: bool = True,
    ) -> str:
        payload_json = canonical_json_object(payload)
        event_id = event_id or self._id_factory("event")
        runtime_db._append_runtime_event(
            self._conn, event_id=event_id, project_id=project_id,
            kind=kind, turn_id=turn_id, payload_json=payload_json, created_at=now,
            deliveries=deliver,
        )
        return event_id

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

    @staticmethod
    def _task7_authority_conflict(
        claim: TurnClaim,
    ) -> ProjectRuntimeError:
        return ProjectRuntimeError(
            RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
            project_id=claim.project_id,
            turn_id=claim.turn_id,
        )

    def _task7_control_transition_count(
        self,
        *,
        project_id: str,
        turn_id: str,
    ) -> int:
        return self._conn.execute(
            """
            SELECT COUNT(*)
            FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind IN (
                  'turn.claimed',
                  'run.stop_requested',
                  'run.stopped',
                  'run.resume_requested',
                  'turn.cancelled',
                  'turn.reconciling',
                  'turn.requeued',
                  'turn.succeeded',
                  'turn.failed'
              )
            """,
            (project_id, turn_id),
        ).fetchone()[0]

    @staticmethod
    def _require_turn_attempt_identity(
        attempt: object,
    ) -> TurnAttemptIdentity:
        if (
            type(attempt) is not TurnAttemptIdentity
            or not all(
                _is_text(value)
                for value in (
                    attempt.project_id,
                    attempt.turn_id,
                    attempt.worker_id,
                    attempt.attempt_id,
                    attempt.canonical_session_id,
                )
            )
            or not all(
                type(value) is int
                and 1 <= value <= SQLITE_INT_MAX
                for value in (
                    attempt.sequence,
                    attempt.lease_generation,
                    attempt.fencing_token,
                )
            )
            or not (
                type(attempt.lease_expires_at) is int
                and 0
                <= attempt.lease_expires_at
                <= MAX_PERSISTED_TIMESTAMP
            )
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        return attempt

    @staticmethod
    def _task7_attempt_authority_conflict(
        attempt: TurnAttemptIdentity,
    ) -> ProjectRuntimeError:
        return ProjectRuntimeError(
            RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
            project_id=attempt.project_id,
            turn_id=attempt.turn_id,
        )

    @staticmethod
    def _task7_attempt_matches_turn(
        attempt: TurnAttemptIdentity,
        turn: runtime_db.RuntimeTurnRecord,
    ) -> bool:
        return (
            turn.project_id == attempt.project_id
            and turn.turn_id == attempt.turn_id
            and turn.sequence == attempt.sequence
            and turn.attempt_id == attempt.attempt_id
            and turn.lease_generation == attempt.lease_generation
            and turn.fencing_token == attempt.fencing_token
        )

    @staticmethod
    def _task7_attempt_matches_control(
        attempt: TurnAttemptIdentity,
        control: runtime_db.RuntimeControlRecord,
    ) -> bool:
        return (
            control.project_id == attempt.project_id
            and control.turn_id == attempt.turn_id
            and control.attempt_id == attempt.attempt_id
            and control.claim_worker_id == attempt.worker_id
            and type(control.claim_lease_expires_at) is int
            and attempt.lease_expires_at
                <= control.claim_lease_expires_at
                <= MAX_PERSISTED_TIMESTAMP
            and control.claim_canonical_session_id
                == attempt.canonical_session_id
        )

    @staticmethod
    def _task7_attempt_matches_lease(
        attempt: TurnAttemptIdentity,
        lease: runtime_db.WorkerLeaseRecord | None,
        control: runtime_db.RuntimeControlRecord,
    ) -> bool:
        return (
            lease is not None
            and lease.project_id == attempt.project_id
            and lease.turn_id == attempt.turn_id
            and lease.lease_id == attempt.attempt_id
            and lease.worker_id == attempt.worker_id
            and lease.lease_generation == attempt.lease_generation
            and lease.fencing_token == attempt.fencing_token
            and type(control.claim_lease_expires_at) is int
            and lease.expires_at == control.claim_lease_expires_at
            and attempt.lease_expires_at <= lease.expires_at
        )

    @staticmethod
    def _task7_event_int(
        value: object,
        *,
        minimum: int = 0,
    ) -> bool:
        return (
            type(value) is int
            and minimum <= value <= SQLITE_INT_MAX
        )

    @classmethod
    def _task7_event_payload(
        cls,
        row: sqlite3.Row,
    ) -> Mapping[str, object]:
        if not (
            _is_text(row["event_id"])
            and _is_text(row["project_id"])
            and cls._task7_event_int(
                row["sequence"],
                minimum=1,
            )
            and _is_text(row["kind"])
            and _is_text(row["turn_id"])
            and cls._task7_event_int(row["created_at"])
        ):
            raise RuntimeError("malformed runtime authority event")
        payload = _decode_canonical_object(row["payload_json"])
        if not isinstance(payload, Mapping):
            raise RuntimeError("malformed runtime authority payload")
        return payload

    def _task7_turn_authority_events(
        self,
        *,
        project_id: str,
        turn_id: str,
    ) -> tuple[sqlite3.Row, ...]:
        rows = tuple(
            self._conn.execute(
                """
                SELECT event_id, project_id, sequence, kind, turn_id,
                       payload_json, created_at
                FROM project_events
                WHERE project_id = ? AND turn_id = ?
                ORDER BY sequence, event_id
                """,
                (project_id, turn_id),
            ).fetchall()
        )
        previous_sequence = 0
        for row in rows:
            if not (
                row["project_id"] == project_id
                and row["turn_id"] == turn_id
                and self._task7_event_int(
                    row["sequence"],
                    minimum=1,
                )
                and row["sequence"] > previous_sequence
            ):
                raise RuntimeError(
                    "malformed runtime authority history"
                )
            previous_sequence = row["sequence"]
        return rows

    @staticmethod
    def _task7_retired_attempt_from_payload(
        value: object,
    ) -> TurnAttemptIdentity:
        if not (
            isinstance(value, Mapping)
            and set(value) == _RETIRED_ATTEMPT_KEYS
        ):
            raise RuntimeError(
                "malformed retired attempt certificate"
            )
        attempt = TurnAttemptIdentity(
            project_id=value["project_id"],
            turn_id=value["turn_id"],
            sequence=value["sequence"],
            worker_id=value["worker_id"],
            attempt_id=value["attempt_id"],
            lease_generation=value["lease_generation"],
            fencing_token=value["fencing_token"],
            canonical_session_id=value["canonical_session_id"],
            lease_expires_at=value["lease_expires_at"],
        )
        try:
            return ProjectRuntime._require_turn_attempt_identity(
                attempt
            )
        except ProjectRuntimeError as exc:
            raise RuntimeError(
                "malformed retired attempt certificate"
            ) from exc

    @classmethod
    def _task7_claimed_event_payload(
        cls,
        row: sqlite3.Row,
    ) -> Mapping[str, object]:
        payload = cls._task7_event_payload(row)
        if not (
            set(payload) == _CLAIMED_ATTEMPT_EVENT_KEYS
            and _is_text(payload["attempt_id"])
            and cls._task7_event_int(
                payload["lease_generation"],
                minimum=1,
            )
            and cls._task7_event_int(
                payload["fencing_token"],
                minimum=1,
            )
            and cls._task7_event_int(
                payload["sequence"],
                minimum=1,
            )
            and payload["turn_id"] == row["turn_id"]
            and cls._task7_event_int(
                payload["version"],
                minimum=1,
            )
        ):
            raise RuntimeError("malformed turn.claimed authority")
        return payload

    @classmethod
    def _task7_reconciling_event_payload(
        cls,
        row: sqlite3.Row,
    ) -> Mapping[str, object]:
        payload = cls._task7_event_payload(row)
        if not (
            set(payload) == _RECONCILING_ATTEMPT_EVENT_KEYS
            and _is_text(payload["attempt_id"])
            and cls._task7_event_int(
                payload["lease_generation"],
                minimum=1,
            )
            and cls._task7_event_int(
                payload["fencing_token"],
                minimum=1,
            )
            and payload["source_status"]
            in {"claimed", "stop_requested"}
            and payload["turn_id"] == row["turn_id"]
            and cls._task7_event_int(
                payload["version"],
                minimum=1,
            )
        ):
            raise RuntimeError(
                "malformed turn.reconciling authority"
            )
        return payload

    @classmethod
    def _task7_requeued_event_payload(
        cls,
        row: sqlite3.Row,
    ) -> tuple[
        Mapping[str, object],
        TurnAttemptIdentity | None,
    ]:
        payload = cls._task7_event_payload(row)
        keys = set(payload)
        if not (
            keys == _REQUEUED_ATTEMPT_EVENT_KEYS
            or keys == _LEGACY_REQUEUED_ATTEMPT_EVENT_KEYS
        ) or not (
            _is_text(payload["attempt_id"])
            and cls._task7_event_int(
                payload["lease_generation"],
                minimum=1,
            )
            and cls._task7_event_int(
                payload["fencing_token"],
                minimum=1,
            )
            and payload["source_status"]
            in {"claimed", "stop_requested"}
            and payload["turn_id"] == row["turn_id"]
            and cls._task7_event_int(
                payload["version"],
                minimum=1,
            )
        ):
            raise RuntimeError("malformed turn.requeued authority")
        if "attempt" not in payload:
            return payload, None
        attempt = cls._task7_retired_attempt_from_payload(
            payload["attempt"]
        )
        if not (
            attempt.project_id == row["project_id"]
            and attempt.turn_id == row["turn_id"]
            and attempt.attempt_id == payload["attempt_id"]
            and attempt.lease_generation
            == payload["lease_generation"]
            and attempt.fencing_token == payload["fencing_token"]
        ):
            raise RuntimeError(
                "contradictory retired attempt certificate"
            )
        return payload, attempt

    def _task7_retired_attempt_certificate(
        self,
        attempt: TurnAttemptIdentity,
    ) -> tuple[
        _RetiredAttemptCertificate,
        tuple[sqlite3.Row, ...],
    ]:
        events = self._task7_turn_authority_events(
            project_id=attempt.project_id,
            turn_id=attempt.turn_id,
        )
        attempt_key = (
            attempt.attempt_id,
            attempt.lease_generation,
            attempt.fencing_token,
        )
        claimed: list[
            tuple[sqlite3.Row, Mapping[str, object]]
        ] = []
        reconciling: list[
            tuple[sqlite3.Row, Mapping[str, object]]
        ] = []
        requeued: list[
            tuple[
                sqlite3.Row,
                Mapping[str, object],
                TurnAttemptIdentity | None,
            ]
        ] = []
        for row in events:
            if row["kind"] == "turn.claimed":
                payload = self._task7_claimed_event_payload(row)
                if (
                    payload["attempt_id"],
                    payload["lease_generation"],
                    payload["fencing_token"],
                ) == attempt_key:
                    claimed.append((row, payload))
            elif row["kind"] == "turn.reconciling":
                payload = self._task7_reconciling_event_payload(
                    row
                )
                if (
                    payload["attempt_id"],
                    payload["lease_generation"],
                    payload["fencing_token"],
                ) == attempt_key:
                    reconciling.append((row, payload))
            elif row["kind"] == "turn.requeued":
                payload, certified_attempt = (
                    self._task7_requeued_event_payload(row)
                )
                top_level_key = (
                    payload["attempt_id"],
                    payload["lease_generation"],
                    payload["fencing_token"],
                )
                certified_key = (
                    (
                        certified_attempt.attempt_id,
                        certified_attempt.lease_generation,
                        certified_attempt.fencing_token,
                    )
                    if certified_attempt is not None
                    else None
                )
                if (
                    top_level_key == attempt_key
                    or certified_key == attempt_key
                ):
                    requeued.append(
                        (row, payload, certified_attempt)
                    )

        if not (
            len(claimed) == 1
            and len(reconciling) == 1
            and len(requeued) == 1
        ):
            raise RuntimeError(
                "retired attempt authority is not unique"
            )
        claimed_row, claimed_payload = claimed[0]
        reconciling_row, reconciling_payload = reconciling[0]
        (
            requeued_row,
            requeued_payload,
            certified_attempt,
        ) = requeued[0]
        if certified_attempt is None:
            raise RuntimeError(
                "legacy requeue cannot certify retired authority"
            )
        same_attempt = (
            certified_attempt.project_id == attempt.project_id
            and certified_attempt.turn_id == attempt.turn_id
            and certified_attempt.sequence == attempt.sequence
            and certified_attempt.worker_id == attempt.worker_id
            and certified_attempt.attempt_id == attempt.attempt_id
            and certified_attempt.lease_generation
            == attempt.lease_generation
            and certified_attempt.fencing_token
            == attempt.fencing_token
            and certified_attempt.canonical_session_id
            == attempt.canonical_session_id
            and attempt.lease_expires_at
            <= certified_attempt.lease_expires_at
        )
        ordered = (
            claimed_row["sequence"]
            < reconciling_row["sequence"]
            < requeued_row["sequence"]
            and claimed_payload["version"]
            < reconciling_payload["version"]
            < requeued_payload["version"]
            and claimed_row["created_at"]
            <= reconciling_row["created_at"]
            <= requeued_row["created_at"]
        )
        exact_chain = (
            claimed_payload["sequence"] == attempt.sequence
            and claimed_payload["turn_id"] == attempt.turn_id
            and reconciling_payload["source_status"] == "claimed"
            and requeued_payload["source_status"] == "claimed"
        )
        if not (same_attempt and ordered and exact_chain):
            raise RuntimeError(
                "retired attempt authority chain mismatch"
            )
        return (
            _RetiredAttemptCertificate(
                event_id=requeued_row["event_id"],
                event_sequence=requeued_row["sequence"],
                created_at=requeued_row["created_at"],
                version=requeued_payload["version"],
                attempt=certified_attempt,
            ),
            events,
        )

    def _task7_cancelled_attempt_certificate(
        self,
        attempt: TurnAttemptIdentity,
        *,
        state: runtime_db.RuntimeState,
        turn: runtime_db.RuntimeTurnRecord,
        control: runtime_db.RuntimeControlRecord,
    ) -> None:
        certificate, events = (
            self._task7_retired_attempt_certificate(attempt)
        )
        cancellations: list[
            tuple[sqlite3.Row, Mapping[str, object]]
        ] = []
        for row in events:
            if (
                row["kind"] != "turn.cancelled"
                or row["sequence"] <= certificate.event_sequence
            ):
                continue
            payload = self._task7_event_payload(row)
            keys = set(payload)
            if not (
                (
                    keys == _CANCELLED_ATTEMPT_EVENT_KEYS
                    or keys
                    == _LEGACY_CANCELLED_ATTEMPT_EVENT_KEYS
                )
                and payload["turn_id"] == attempt.turn_id
                and self._task7_event_int(
                    payload["version"],
                    minimum=1,
                )
            ):
                raise RuntimeError(
                    "malformed turn.cancelled authority"
                )
            cancellations.append((row, payload))
        if len(cancellations) != 1:
            raise RuntimeError(
                "cancelled attempt authority is not unique"
            )
        cancelled_row, cancelled_payload = cancellations[0]
        if not (
            set(cancelled_payload)
            == _CANCELLED_ATTEMPT_EVENT_KEYS
            and cancelled_payload["retired_attempt_event_id"]
            == certificate.event_id
            and certificate.event_sequence
            < cancelled_row["sequence"]
            and certificate.version
            < cancelled_payload["version"]
            <= state.version
            and certificate.created_at
            <= cancelled_row["created_at"]
            <= state.updated_at
            and cancelled_row["created_at"] <= turn.updated_at
            and cancelled_row["created_at"] <= control.updated_at
        ):
            raise RuntimeError(
                "cancelled attempt authority mismatch"
            )
        if any(
            row["kind"] in _TURN_AUTHORITY_EVENT_KINDS
            and (
                certificate.event_sequence
                < row["sequence"]
                < cancelled_row["sequence"]
                or row["sequence"] > cancelled_row["sequence"]
            )
            for row in events
        ):
            raise RuntimeError(
                "cancelled attempt authority was contradicted"
            )

    def _task7_superseding_attempt_certificate(
        self,
        attempt: TurnAttemptIdentity,
        *,
        state: runtime_db.RuntimeState,
        turn: runtime_db.RuntimeTurnRecord,
        control: runtime_db.RuntimeControlRecord,
        lease: runtime_db.WorkerLeaseRecord | None,
    ) -> None:
        certificate, events = (
            self._task7_retired_attempt_certificate(attempt)
        )
        if not (
            state.lifecycle == "active"
            and state.conversation_tip_id
            == attempt.canonical_session_id
            and state.transcript_pending_batch_id is None
            and state.transcript_dispatch_block_key is None
            and turn.project_id == attempt.project_id
            and turn.turn_id == attempt.turn_id
            and turn.sequence == attempt.sequence
            and turn.status in {"claimed", "awaiting_approval"}
            and turn.execution_state
            in {"not_started", "started"}
            and _is_text(turn.attempt_id)
            and turn.attempt_id != attempt.attempt_id
            and turn.lease_generation
            > attempt.lease_generation
            and turn.fencing_token > attempt.fencing_token
            and turn.terminal_result_id is None
            and turn.recovery_block_key is None
            and turn.transcript_applied_batch_id is None
            and control.project_id == attempt.project_id
            and control.turn_id == attempt.turn_id
            and control.control_state == "running"
            and control.attempt_id == turn.attempt_id
            and _is_text(control.claim_worker_id)
            and self._task7_event_int(
                control.claim_lease_expires_at
            )
            and control.claim_canonical_session_id
            == state.conversation_tip_id
            and lease is not None
            and lease.project_id == attempt.project_id
            and lease.turn_id == attempt.turn_id
            and lease.lease_id == turn.attempt_id
            and lease.worker_id == control.claim_worker_id
            and lease.lease_generation
            == turn.lease_generation
            and lease.fencing_token == turn.fencing_token
            and lease.expires_at
            == control.claim_lease_expires_at
        ):
            raise RuntimeError(
                "superseding attempt projection mismatch"
            )
        current_key = (
            turn.attempt_id,
            turn.lease_generation,
            turn.fencing_token,
        )
        claims: list[
            tuple[sqlite3.Row, Mapping[str, object]]
        ] = []
        for row in events:
            if row["kind"] != "turn.claimed":
                continue
            payload = self._task7_claimed_event_payload(row)
            if (
                payload["attempt_id"],
                payload["lease_generation"],
                payload["fencing_token"],
            ) == current_key:
                claims.append((row, payload))
        if len(claims) != 1:
            raise RuntimeError(
                "superseding claim authority is not unique"
            )
        claim_row, claim_payload = claims[0]
        if not (
            certificate.event_sequence < claim_row["sequence"]
            and certificate.version
            < claim_payload["version"]
            <= state.version
            and certificate.created_at
            <= claim_row["created_at"]
            <= state.updated_at
            and claim_row["created_at"] <= turn.updated_at
            and claim_row["created_at"] <= control.updated_at
            and claim_row["created_at"] <= lease.updated_at
            and claim_payload["sequence"] == turn.sequence
            and claim_payload["turn_id"] == turn.turn_id
        ):
            raise RuntimeError(
                "superseding claim authority mismatch"
            )
        if any(
            row["kind"] in _TURN_AUTHORITY_EVENT_KINDS
            and row["sequence"] > claim_row["sequence"]
            for row in events
        ):
            raise RuntimeError(
                "superseding claim authority was contradicted"
            )

    def _task7_queued_retired_attempt_event_id(
        self,
        *,
        state: runtime_db.RuntimeState,
        turn: runtime_db.RuntimeTurnRecord,
    ) -> str | None:
        if (
            turn.lease_generation == 0
            and turn.fencing_token == 0
        ):
            return None
        if not (
            turn.lease_generation > 0
            and turn.fencing_token > 0
        ):
            raise RuntimeError(
                "mixed retired attempt counters"
            )
        events = self._task7_turn_authority_events(
            project_id=turn.project_id,
            turn_id=turn.turn_id,
        )
        requeues = tuple(
            row for row in events if row["kind"] == "turn.requeued"
        )
        if not requeues:
            raise RuntimeError(
                "queued retired attempt has no requeue"
            )
        latest_requeue = requeues[-1]
        payload, retired_attempt = (
            self._task7_requeued_event_payload(latest_requeue)
        )
        if not (
            payload["attempt_id"]
            and payload["lease_generation"]
            == turn.lease_generation
            and payload["fencing_token"] == turn.fencing_token
            and payload["turn_id"] == turn.turn_id
            and payload["source_status"] == "claimed"
            and latest_requeue["created_at"] <= turn.updated_at
            and payload["version"] <= state.version
            and not any(
                row["kind"] in _TURN_AUTHORITY_EVENT_KINDS
                and row["sequence"] > latest_requeue["sequence"]
                for row in events
            )
        ):
            raise RuntimeError(
                "queued retired attempt projection mismatch"
            )
        if retired_attempt is None:
            return None
        if not (
            retired_attempt.sequence == turn.sequence
            and retired_attempt.canonical_session_id
            == state.conversation_tip_id
        ):
            raise RuntimeError(
                "queued retired attempt identity mismatch"
            )
        certificate, _ = self._task7_retired_attempt_certificate(
            retired_attempt
        )
        if certificate.event_id != latest_requeue["event_id"]:
            raise RuntimeError(
                "queued retired attempt certificate mismatch"
            )
        return certificate.event_id

    def resolve_prepared_terminal(
        self,
        attempt: TurnAttemptIdentity,
        *,
        prepared_result_id: str,
        status: Literal["succeeded", "failed"],
    ) -> PreparedTerminalDecision:
        """Resolve one prepared State proof from one Projects snapshot."""
        attempt = self._require_turn_attempt_identity(attempt)
        prepared_result_id = _require_task7_batch_id(
            prepared_result_id
        )
        if type(status) is not str or status not in {
            "succeeded",
            "failed",
        }:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        if self._conn.in_transaction:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )

        self._conn.execute("BEGIN")
        try:
            try:
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    attempt.project_id,
                )
                turn = runtime_db._runtime_turn_for_project(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                control = runtime_db._runtime_control_for_turn(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                lease = runtime_db._current_worker_lease_for_turn(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
            except RuntimeError as exc:
                raise self._task7_attempt_authority_conflict(
                    attempt
                ) from exc

            if not (
                state is not None
                and state.lifecycle == "active"
                and state.conversation_tip_id
                    == attempt.canonical_session_id
                and turn is not None
                and turn.project_id == attempt.project_id
                and turn.turn_id == attempt.turn_id
                and turn.sequence == attempt.sequence
                and control is not None
                and control.project_id == attempt.project_id
                and control.turn_id == attempt.turn_id
            ):
                raise self._task7_attempt_authority_conflict(
                    attempt
                )

            if not self._task7_attempt_matches_turn(
                attempt,
                turn,
            ):
                if (
                    turn.attempt_id != attempt.attempt_id
                    and (
                        turn.lease_generation,
                        turn.fencing_token,
                    )
                    > (
                        attempt.lease_generation,
                        attempt.fencing_token,
                    )
                ):
                    decision = PreparedTerminalDecision(
                        action="discard",
                        terminal=None,
                        discard_authority="superseded_attempt",
                    )
                else:
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
            elif not self._task7_attempt_matches_control(
                attempt,
                control,
            ):
                raise self._task7_attempt_authority_conflict(
                    attempt
                )
            elif turn.status in {"succeeded", "failed"}:
                try:
                    terminal_result_id = _require_task7_batch_id(
                        turn.terminal_result_id
                    )
                except ProjectRuntimeError as exc:
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    ) from exc
                if not (
                    control.control_state == "terminal"
                    and turn.execution_state == "started"
                    and lease is None
                    and turn.transcript_applied_batch_id is None
                    and state.transcript_pending_batch_id
                        == terminal_result_id
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                if (
                    turn.status == status
                    and terminal_result_id == prepared_result_id
                ):
                    decision = PreparedTerminalDecision(
                        action="publish",
                        terminal=TerminalTurnResult(
                            attempt=attempt,
                            status=status,
                            result_id=prepared_result_id,
                        ),
                        discard_authority=None,
                    )
                else:
                    decision = PreparedTerminalDecision(
                        action="discard",
                        terminal=None,
                        discard_authority="superseded_terminal",
                    )
            elif turn.status == "stop_requested":
                if not (
                    control.control_state == "stop_requested"
                    and self._task7_attempt_matches_lease(
                        attempt,
                        lease,
                        control,
                    )
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                decision = PreparedTerminalDecision(
                    action="discard",
                    terminal=None,
                    discard_authority="stop_requested",
                )
            elif turn.status == "cancelled":
                if not (
                    control.control_state == "terminal"
                    and lease is None
                    and turn.terminal_result_id is None
                    and turn.transcript_applied_batch_id is None
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                decision = PreparedTerminalDecision(
                    action="discard",
                    terminal=None,
                    discard_authority="cancelled",
                )
            elif turn.status == "reconciling":
                if not (
                    lease is None
                    and control.control_state
                        in {"running", "stop_requested"}
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                if turn.recovery_block_key is not None:
                    decision = PreparedTerminalDecision(
                        action="discard",
                        terminal=None,
                        discard_authority="recovery_blocked",
                    )
                elif control.control_state == "stop_requested":
                    decision = PreparedTerminalDecision(
                        action="discard",
                        terminal=None,
                        discard_authority="stop_requested",
                    )
                else:
                    decision = PreparedTerminalDecision(
                        action="wait",
                        terminal=None,
                        discard_authority=None,
                    )
            elif turn.status in {"claimed", "awaiting_approval"}:
                if not (
                    control.control_state == "running"
                    and self._task7_attempt_matches_lease(
                        attempt,
                        lease,
                        control,
                    )
                    and turn.terminal_result_id is None
                    and turn.transcript_applied_batch_id is None
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                decision = PreparedTerminalDecision(
                    action="wait",
                    terminal=None,
                    discard_authority=None,
                )
            elif turn.status == "stopped":
                if not (
                    control.control_state == "stopped"
                    and lease is None
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                decision = PreparedTerminalDecision(
                    action="discard",
                    terminal=None,
                    discard_authority="stop_requested",
                )
            else:
                raise self._task7_attempt_authority_conflict(
                    attempt
                )
            self._conn.execute("COMMIT")
            return decision
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise

    def _resolve_prepared_approval_checkpoint_authority(
        self,
        attempt: TurnAttemptIdentity,
        *,
        operation_id: str,
        approval_id: str,
    ) -> tuple[
        PreparedApprovalCheckpointDecision,
        Literal[
            "stop_requested",
            "cancelled",
            "superseded_attempt",
            "superseded_terminal",
            "recovery_blocked",
        ]
        | None,
    ]:
        """Resolve one pre-operation checkpoint from one Projects snapshot."""
        attempt = self._require_turn_attempt_identity(attempt)
        if not (_is_text(operation_id) and _is_text(approval_id)):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        if self._conn.in_transaction:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)

        self._conn.execute("BEGIN")
        try:
            decision = (
                self._resolve_prepared_approval_checkpoint_authority_in_snapshot(
                    attempt,
                    operation_id=operation_id,
                    approval_id=approval_id,
                )
            )
            self._conn.execute("COMMIT")
            return decision
        except BaseException:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise

    def _resolve_prepared_approval_checkpoint_authority_in_snapshot(
        self,
        attempt: TurnAttemptIdentity,
        *,
        operation_id: str,
        approval_id: str,
    ) -> tuple[
        PreparedApprovalCheckpointDecision,
        Literal[
            "stop_requested",
            "cancelled",
            "superseded_attempt",
            "superseded_terminal",
            "recovery_blocked",
        ]
        | None,
    ]:
        """Resolve one checkpoint while the caller owns the read snapshot."""
        attempt = self._require_turn_attempt_identity(attempt)
        if not (_is_text(operation_id) and _is_text(approval_id)):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        if not self._conn.in_transaction:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)

        try:
            try:
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    attempt.project_id,
                )
                turn = runtime_db._runtime_turn_for_project(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                control = runtime_db._runtime_control_for_turn(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                lease = runtime_db._current_worker_lease_for_turn(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
            except RuntimeError as exc:
                raise self._task7_attempt_authority_conflict(
                    attempt
                ) from exc

            if not (
                state is not None
                and state.lifecycle == "active"
                and state.conversation_tip_id
                    == attempt.canonical_session_id
                and turn is not None
                and turn.project_id == attempt.project_id
                and turn.turn_id == attempt.turn_id
                and turn.sequence == attempt.sequence
                and control is not None
                and control.project_id == attempt.project_id
                and control.turn_id == attempt.turn_id
            ):
                raise self._task7_attempt_authority_conflict(attempt)

            cancelled_projection = (
                turn.status == "cancelled"
                and turn.attempt_id is None
                and turn.lease_generation == attempt.lease_generation
                and turn.fencing_token == attempt.fencing_token
            )
            if cancelled_projection:
                if not (
                    control.control_state == "terminal"
                    and control.attempt_id is None
                    and control.claim_worker_id is None
                    and control.claim_lease_expires_at is None
                    and control.claim_canonical_session_id is None
                    and lease is None
                    and turn.execution_state is None
                    and turn.terminal_result_id is None
                    and turn.recovery_block_key is None
                    and turn.transcript_applied_batch_id is None
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(attempt)
                try:
                    self._task7_cancelled_attempt_certificate(
                        attempt,
                        state=state,
                        turn=turn,
                        control=control,
                    )
                except Exception as exc:
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    ) from exc
                decision = PreparedApprovalCheckpointDecision("discard")
                discard_authority = "cancelled"
            elif not self._task7_attempt_matches_turn(attempt, turn):
                if (
                    turn.attempt_id != attempt.attempt_id
                    and turn.lease_generation
                    > attempt.lease_generation
                    and turn.fencing_token > attempt.fencing_token
                ):
                    try:
                        self._task7_superseding_attempt_certificate(
                            attempt,
                            state=state,
                            turn=turn,
                            control=control,
                            lease=lease,
                        )
                    except Exception as exc:
                        raise self._task7_attempt_authority_conflict(
                            attempt
                        ) from exc
                    decision = PreparedApprovalCheckpointDecision(
                        "discard"
                    )
                    discard_authority = "superseded_attempt"
                else:
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
            elif not self._task7_attempt_matches_control(
                attempt,
                control,
            ):
                raise self._task7_attempt_authority_conflict(attempt)
            elif turn.status in {"succeeded", "failed"}:
                try:
                    terminal_result_id = _require_task7_batch_id(
                        turn.terminal_result_id
                    )
                except ProjectRuntimeError as exc:
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    ) from exc
                if not (
                    control.control_state == "terminal"
                    and turn.execution_state == "started"
                    and lease is None
                    and turn.transcript_applied_batch_id is None
                    and state.transcript_pending_batch_id
                        == terminal_result_id
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                decision = PreparedApprovalCheckpointDecision(
                    "discard"
                )
                discard_authority = "superseded_terminal"
            elif turn.status == "stop_requested":
                if not (
                    control.control_state == "stop_requested"
                    and self._task7_attempt_matches_lease(
                        attempt,
                        lease,
                        control,
                    )
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                decision = PreparedApprovalCheckpointDecision(
                    "discard"
                )
                discard_authority = "stop_requested"
            elif turn.status == "reconciling":
                if not (
                    lease is None
                    and control.control_state
                        in {"running", "stop_requested"}
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                if turn.recovery_block_key is not None:
                    decision = PreparedApprovalCheckpointDecision(
                        "discard"
                    )
                    discard_authority = "recovery_blocked"
                elif control.control_state == "stop_requested":
                    decision = PreparedApprovalCheckpointDecision(
                        "discard"
                    )
                    discard_authority = "stop_requested"
                else:
                    decision = PreparedApprovalCheckpointDecision(
                        "wait"
                    )
                    discard_authority = None
            elif turn.status in {"claimed", "awaiting_approval"}:
                if not (
                    control.control_state == "running"
                    and self._task7_attempt_matches_lease(
                        attempt,
                        lease,
                        control,
                    )
                    and turn.terminal_result_id is None
                    and turn.transcript_applied_batch_id is None
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                decision = PreparedApprovalCheckpointDecision("wait")
                discard_authority = None
            elif turn.status == "stopped":
                if not (
                    control.control_state == "stopped"
                    and lease is None
                    and state.transcript_pending_batch_id is None
                    and state.transcript_dispatch_block_key is None
                ):
                    raise self._task7_attempt_authority_conflict(
                        attempt
                    )
                decision = PreparedApprovalCheckpointDecision(
                    "discard"
                )
                discard_authority = "stop_requested"
            else:
                raise self._task7_attempt_authority_conflict(attempt)
            return decision, discard_authority
        except BaseException:
            raise

    def resolve_prepared_approval_checkpoint(
        self,
        attempt: TurnAttemptIdentity,
        *,
        operation_id: str,
        approval_id: str,
    ) -> PreparedApprovalCheckpointDecision:
        decision, _ = (
            self._resolve_prepared_approval_checkpoint_authority(
                attempt,
                operation_id=operation_id,
                approval_id=approval_id,
            )
        )
        return decision

    def ack_terminal_transcript_applied(
        self,
        acknowledgement: TerminalTranscriptAcknowledgement,
    ) -> Literal["acknowledged", "already_acknowledged"]:
        """Atomically mark one terminal transcript applied and clear its gate."""
        if type(acknowledgement) is not TerminalTranscriptAcknowledgement:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        attempt = self._require_turn_attempt_identity(
            acknowledgement.attempt
        )
        batch_id = _require_task7_batch_id(
            acknowledgement.batch_id
        )
        result_id = _require_task7_batch_id(
            acknowledgement.result_id
        )
        if (
            type(acknowledgement.status) is not str
            or acknowledgement.status not in {
                "succeeded",
                "failed",
            }
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        if self._conn.in_transaction:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )

        with runtime_db.write_transaction(self._conn):
            try:
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    attempt.project_id,
                )
                turn = runtime_db._runtime_turn_for_project(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                control = runtime_db._runtime_control_for_turn(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                lease = runtime_db._current_worker_lease_for_turn(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
            except RuntimeError as exc:
                raise self._task7_attempt_authority_conflict(
                    attempt
                ) from exc
            if not (
                batch_id == result_id
                and state is not None
                and turn is not None
                and control is not None
                and self._task7_attempt_matches_turn(
                    attempt,
                    turn,
                )
                and self._task7_attempt_matches_control(
                    attempt,
                    control,
                )
                and turn.status == acknowledgement.status
                and turn.terminal_result_id == result_id
                and turn.execution_state == "started"
                and control.control_state == "terminal"
                and lease is None
            ):
                raise self._task7_attempt_authority_conflict(
                    attempt
                )

            if turn.transcript_applied_batch_id is not None:
                if turn.transcript_applied_batch_id == batch_id:
                    return "already_acknowledged"
                raise self._task7_attempt_authority_conflict(
                    attempt
                )
            if not (
                state.lifecycle == "active"
                and state.conversation_tip_id
                    == attempt.canonical_session_id
                and state.transcript_pending_batch_id == batch_id
                and state.transcript_dispatch_block_key is None
            ):
                raise self._task7_attempt_authority_conflict(
                    attempt
                )

            applied = self._conn.execute(
                """
                UPDATE project_turns
                SET transcript_applied_batch_id = ?
                WHERE project_id = ? AND turn_id = ? AND sequence = ?
                  AND status = ? AND terminal_result_id = ?
                  AND execution_state = 'started'
                  AND attempt_id = ? AND lease_generation = ?
                  AND fencing_token = ?
                  AND transcript_applied_batch_id IS NULL
                """,
                (
                    batch_id,
                    attempt.project_id,
                    attempt.turn_id,
                    attempt.sequence,
                    acknowledgement.status,
                    result_id,
                    attempt.attempt_id,
                    attempt.lease_generation,
                    attempt.fencing_token,
                ),
            )
            if applied.rowcount != 1:
                raise self._task7_attempt_authority_conflict(
                    attempt
                )
            cleared = self._conn.execute(
                """
                UPDATE project_runtime_state
                SET transcript_pending_batch_id = NULL
                WHERE project_id = ?
                  AND lifecycle = 'active'
                  AND conversation_tip_id = ?
                  AND transcript_pending_batch_id = ?
                  AND transcript_dispatch_block_key IS NULL
                """,
                (
                    attempt.project_id,
                    attempt.canonical_session_id,
                    batch_id,
                ),
            )
            if cleared.rowcount != 1:
                raise self._task7_attempt_authority_conflict(
                    attempt
                )
            return "acknowledged"

    def record_terminal_transcript_conflict(
        self,
        conflict: TerminalTranscriptConflict,
    ) -> Literal["recorded", "already_recorded"]:
        """Block dispatch and retain one exact terminal count-drift proof."""
        if (
            type(conflict) is not TerminalTranscriptConflict
            or type(conflict.terminal)
                is not TerminalTranscriptAcknowledgement
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        terminal = conflict.terminal
        attempt = terminal.attempt
        if type(attempt) is not TurnAttemptIdentity:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )

        def authority_conflict() -> ProjectRuntimeError:
            return self._task7_attempt_authority_conflict(attempt)

        try:
            attempt = self._require_turn_attempt_identity(attempt)
            batch_id = _require_task7_batch_id(terminal.batch_id)
            result_id = _require_task7_batch_id(terminal.result_id)
        except ProjectRuntimeError as exc:
            raise authority_conflict() from exc
        observed_message_count = conflict.observed_message_count
        if not (
            type(terminal.status) is str
            and terminal.status in {"succeeded", "failed"}
            and batch_id == result_id
            and type(observed_message_count) is int
            and 0 <= observed_message_count <= SQLITE_INT_MAX
        ):
            raise authority_conflict()
        identity = {
            "attempt_id": attempt.attempt_id,
            "batch_id": batch_id,
            "canonical_session_id": attempt.canonical_session_id,
            "fencing_token": attempt.fencing_token,
            "lease_expires_at": attempt.lease_expires_at,
            "lease_generation": attempt.lease_generation,
            "observed_message_count": observed_message_count,
            "project_id": attempt.project_id,
            "result_id": result_id,
            "sequence": attempt.sequence,
            "status": terminal.status,
            "turn_id": attempt.turn_id,
            "worker_id": attempt.worker_id,
        }
        try:
            identity_json = json.dumps(
                identity,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            expected_key = (
                "transcript-conflict-"
                + hashlib.sha256(
                    identity_json.encode("utf-8")
                ).hexdigest()
            )
        except (TypeError, ValueError, UnicodeError) as exc:
            raise authority_conflict() from exc
        if (
            type(conflict.conflict_key) is not str
            or conflict.conflict_key != expected_key
        ):
            raise authority_conflict()
        if self._conn.in_transaction:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )

        with runtime_db.write_transaction(self._conn):
            try:
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    attempt.project_id,
                )
                turn = runtime_db._runtime_turn_for_project(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                control = runtime_db._runtime_control_for_turn(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                lease = runtime_db._current_worker_lease_for_turn(
                    self._conn,
                    project_id=attempt.project_id,
                    turn_id=attempt.turn_id,
                )
                events = self._conn.execute(
                    """
                    SELECT *
                    FROM project_events
                    WHERE event_id = ?
                       OR (
                            project_id = ?
                            AND turn_id = ?
                            AND kind = 'turn.transcript_conflicted'
                       )
                    """,
                    (
                        expected_key,
                        attempt.project_id,
                        attempt.turn_id,
                    ),
                ).fetchall()
            except RuntimeError as exc:
                raise authority_conflict() from exc
            if not (
                state is not None
                and turn is not None
                and control is not None
                and self._task7_attempt_matches_turn(attempt, turn)
                and turn.status == terminal.status
                and turn.terminal_result_id == result_id
                and turn.execution_state == "started"
                and turn.transcript_applied_batch_id is None
                and self._task7_attempt_matches_control(
                    attempt,
                    control,
                )
                and control.control_state == "terminal"
                and lease is None
            ):
                raise authority_conflict()

            if (
                state.transcript_pending_batch_id is None
                and state.transcript_dispatch_block_key == expected_key
            ):
                if len(events) != 1:
                    raise authority_conflict()
                event = events[0]
                try:
                    payload = json.loads(
                        event["payload_json"],
                        parse_constant=lambda _value: (
                            _ for _ in ()
                        ).throw(ValueError()),
                    )
                except (
                    TypeError,
                    ValueError,
                    json.JSONDecodeError,
                ) as exc:
                    raise authority_conflict() from exc
                event_version = (
                    payload.get("version")
                    if type(payload) is dict
                    else None
                )
                if not (
                    type(event_version) is int
                    and 1 <= event_version <= SQLITE_INT_MAX
                ):
                    raise authority_conflict()
                expected_payload_json = canonical_json_object(
                    {
                        **identity,
                        "conflict_key": expected_key,
                        "version": event_version,
                    }
                )
                if not (
                    event["event_id"] == expected_key
                    and event["project_id"] == attempt.project_id
                    and event["kind"]
                        == "turn.transcript_conflicted"
                    and event["turn_id"] == attempt.turn_id
                    and event["payload_json"]
                        == expected_payload_json
                ):
                    raise authority_conflict()
                return "already_recorded"

            if not (
                state.lifecycle == "active"
                and state.conversation_tip_id
                    == attempt.canonical_session_id
                and state.transcript_pending_batch_id == batch_id
                and state.transcript_dispatch_block_key is None
                and not events
                and type(state.version) is int
                and 0 <= state.version < SQLITE_INT_MAX
            ):
                raise authority_conflict()
            now = self._now()
            if now > MAX_PERSISTED_TIMESTAMP:
                raise authority_conflict()
            resulting_version = state.version + 1
            updated = self._conn.execute(
                """
                UPDATE project_runtime_state
                SET transcript_pending_batch_id = NULL,
                    transcript_dispatch_block_key = ?,
                    version = version + 1,
                    updated_at = ?
                WHERE project_id = ?
                  AND lifecycle = 'active'
                  AND conversation_tip_id = ?
                  AND version = ?
                  AND transcript_pending_batch_id = ?
                  AND transcript_dispatch_block_key IS NULL
                """,
                (
                    expected_key,
                    now,
                    attempt.project_id,
                    attempt.canonical_session_id,
                    state.version,
                    batch_id,
                ),
            )
            if updated.rowcount != 1:
                raise authority_conflict()
            self._event(
                attempt.project_id,
                "turn.transcript_conflicted",
                attempt.turn_id,
                {
                    **identity,
                    "conflict_key": expected_key,
                    "version": resulting_version,
                },
                now,
                event_id=expected_key,
                deliver=False,
            )
            return "recorded"

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

    def control_for_claim(
        self,
        claim: TurnClaim,
    ) -> ClaimControl:
        """Read the exact live control projection in one SQLite snapshot."""
        claim = self._require_turn_claim(claim)
        if self._conn.in_transaction:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        now = self._now()
        self._conn.execute("BEGIN")
        try:
            try:
                _, turn, control, lease = self._live_claim_records(
                    claim,
                    now=now,
                )
            except RuntimeError as exc:
                raise self._stale_turn_claim(claim) from exc
            state = {
                ("claimed", "running"): "running",
                (
                    "awaiting_approval",
                    "running",
                ): "awaiting_approval",
                (
                    "stop_requested",
                    "stop_requested",
                ): "stop_requested",
            }.get((turn.status, control.control_state))
            if (
                state is None
                or turn.execution_state
                not in {"not_started", "started"}
            ):
                raise self._stale_turn_claim(claim)
            result = ClaimControl(
                state,
                control.control_version,
                lease.expires_at,
            )
            self._conn.commit()
            return result
        except BaseException:
            if self._conn.in_transaction:
                self._conn.rollback()
            raise

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

    def execution_input_for_claim(
        self,
        claim: TurnClaim,
    ) -> TurnExecutionInput:
        """Read one exact, started attempt and its immutable origin."""
        claim = self._require_turn_claim(claim)
        if self._conn.in_transaction:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        now = self._now()
        self._conn.execute("BEGIN")
        try:
            try:
                _, turn, control, lease = self._live_claim_records(
                    claim,
                    now=now,
                )
                if not (
                    turn.status == "claimed"
                    and control.control_state == "running"
                ):
                    raise self._stale_turn_claim(claim)
                if turn.execution_state != "started":
                    if turn.execution_state == "not_started":
                        raise ProjectRuntimeError(
                            RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED,
                            project_id=claim.project_id,
                            turn_id=claim.turn_id,
                        )
                    raise self._stale_turn_claim(claim)
                if not _is_text(turn.origin_binding_id):
                    raise self._stale_turn_claim(claim)
                binding = runtime_db.binding_for_id(
                    self._conn,
                    project_id=claim.project_id,
                    binding_id=turn.origin_binding_id,
                )
                if binding is None or not (
                    binding.binding_id == turn.origin_binding_id
                    and binding.project_id == claim.project_id
                    and type(binding.surface) is str
                    and binding.surface in {"desktop", "discord"}
                    and _is_text(binding.external_binding_id)
                    and _is_text(binding.actor_id)
                ):
                    raise self._stale_turn_claim(claim)
                payload = _decode_canonical_object(turn.payload_json)
                revision_rows = self._conn.execute(
                    """
                    SELECT revision, typeof(revision) AS revision_type
                    FROM project_contracts
                    WHERE project_id = ?
                    """,
                    (claim.project_id,),
                ).fetchall()
                revisions: list[int] = []
                for row in revision_rows:
                    revision = row["revision"]
                    if not (
                        row["revision_type"] == "integer"
                        and type(revision) is int
                        and 0 < revision <= SQLITE_INT_MAX
                    ):
                        raise RuntimeError("corrupt project contract revision")
                    revisions.append(revision)
                result = TurnExecutionInput(
                    TurnAttemptIdentity(
                        project_id=claim.project_id,
                        turn_id=claim.turn_id,
                        sequence=claim.sequence,
                        worker_id=claim.worker_id,
                        attempt_id=claim.attempt_id,
                        lease_generation=claim.lease_generation,
                        fencing_token=claim.fencing_token,
                        canonical_session_id=claim.canonical_session_id,
                        lease_expires_at=lease.expires_at,
                    ),
                    payload,
                    TurnOrigin(
                        binding.binding_id,
                        binding.surface,
                        binding.external_binding_id,
                        binding.actor_id,
                    ),
                    max(revisions, default=0),
                )
            except ProjectRuntimeError:
                raise
            except (RuntimeError, TypeError, ValueError, sqlite3.Error) as exc:
                raise self._stale_turn_claim(claim) from exc
            self._conn.commit()
            return result
        except BaseException:
            if self._conn.in_transaction:
                self._conn.rollback()
            raise

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

    def commit_turn_with_task7_batch(
        self,
        claim: TurnClaim,
        result: CanonicalTurnResult,
        *,
        transcript_batch_id: str,
    ) -> ProjectTurn:
        """Atomically terminalize one Task-7 attempt and install its gate."""
        claim = self._require_turn_claim(claim)
        if not (
            type(result) is CanonicalTurnResult
            and type(result.status) is str
            and result.status in {"succeeded", "failed"}
            and _is_text(result.result_id)
        ):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        transcript_batch_id = _require_task7_batch_id(
            transcript_batch_id
        )
        if result.result_id != transcript_batch_id:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        if self._conn.in_transaction:
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)

        now = self._now()
        parked_operation_block = False
        with runtime_db.write_transaction(self._conn):
            state = runtime_db.runtime_state_for_project(
                self._conn,
                claim.project_id,
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
                replay_authority = (
                    state is not None
                    and state.lifecycle == "active"
                    and state.conversation_tip_id
                        == claim.canonical_session_id
                    and state.transcript_pending_batch_id
                        == transcript_batch_id
                    and state.transcript_dispatch_block_key is None
                    and turn.project_id == claim.project_id
                    and turn.turn_id == claim.turn_id
                    and turn.sequence == claim.sequence
                    and turn.attempt_id == claim.attempt_id
                    and turn.lease_generation
                        == claim.lease_generation
                    and turn.fencing_token == claim.fencing_token
                    and turn.execution_state == "started"
                    and turn.transcript_applied_batch_id is None
                    and control.control_state == "terminal"
                    and control.attempt_id == claim.attempt_id
                    and control.claim_worker_id == claim.worker_id
                    and control.claim_lease_expires_at
                        == claim.lease_expires_at
                    and control.claim_canonical_session_id
                        == claim.canonical_session_id
                    and lease is None
                    and control.control_version
                        == self._task7_control_transition_count(
                            project_id=claim.project_id,
                            turn_id=claim.turn_id,
                        )
                )
                if not replay_authority:
                    raise self._task7_authority_conflict(claim)
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
                claim,
                now=now,
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
            if not (
                state.transcript_pending_batch_id is None
                and state.transcript_dispatch_block_key is None
                and turn.transcript_applied_batch_id is None
                and control.claim_lease_expires_at
                    == claim.lease_expires_at
                and lease.expires_at == claim.lease_expires_at
            ):
                raise self._task7_authority_conflict(claim)

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
                self._block_current_recovery(candidate, now=now)
                parked_operation_block = True
            else:
                terminal_allowed = operation_disposition in {
                    "clear",
                    "reconciled",
                } or (
                    operation_disposition == "pre_effect_blocked"
                    and result.status == "failed"
                )
                if not terminal_allowed:
                    raise ProjectRuntimeError(
                        RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED,
                        project_id=claim.project_id,
                        turn_id=claim.turn_id,
                    )
                committed = (
                    runtime_db._commit_runtime_turn_with_task7_batch(
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
                )
                if committed is None:
                    raise self._task7_authority_conflict(claim)
                updated_state = (
                    runtime_db
                    ._advance_runtime_version_with_task7_pending_batch(
                        self._conn,
                        project_id=claim.project_id,
                        expected_version=state.version,
                        transcript_batch_id=transcript_batch_id,
                        updated_at=now,
                    )
                )
                if updated_state is None:
                    raise self._task7_authority_conflict(claim)
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
        raise RuntimeError("Task-7 operation commit reached invalid state")

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
            self._conn,
            now=now,
            limit=limit,
            stop_first=True,
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
            if candidate.source_status == "stop_requested":
                if candidate.execution_state == "not_started":
                    recovered.append(
                        self._finalize_recovery(
                            candidate,
                            outcome="stopped",
                            result_id=None,
                            now=now,
                        )
                    )
                    continue
                request = TurnReadbackRequest(
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                    sequence=candidate.sequence,
                    worker_id=candidate.worker_id,
                    attempt_id=candidate.attempt_id,
                    lease_generation=candidate.lease_generation,
                    fencing_token=candidate.fencing_token,
                    lease_expires_at=candidate.lease_expires_at,
                    canonical_session_id=(
                        candidate.canonical_session_id
                    ),
                    source_status=candidate.source_status,
                    execution_state=candidate.execution_state,
                )
                try:
                    readback.read_turn(request)
                except Exception:
                    pass
                recovered.append(
                    self._force_block_stop_recovery(
                        candidate,
                        now=now,
                    )
                )
                continue
            operation_disposition = (
                runtime_db._project_operation_disposition_for_turn(
                    self._conn,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
            )
            if operation_disposition == "reconciled":
                recovered.append(
                    self._block_recovery(candidate, now=now)
                )
                continue
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

    def reconcile_inflight_turns_with_task7_evidence(
        self,
        readback: Task7TerminalReadbackPort,
        *,
        limit: int = 100,
    ) -> tuple[ProjectTurn, ...]:
        """Recover parked attempts with explicit terminal batch evidence."""
        if (
            type(limit) is not int
            or not 1 <= limit <= 100
            or not callable(
                getattr(readback, "read_turn_with_evidence", None)
            )
            or self._conn.in_transaction
        ):
            raise ProjectRuntimeError(RuntimeErrorCode.INVALID_ARGUMENT)
        now = self._now()
        candidates = runtime_db._recovery_candidates(
            self._conn,
            now=now,
            limit=limit,
            stop_first=True,
        )
        parked: list[
            ProjectTurn | runtime_db.RecoveryCandidateRecord
        ] = []
        for selected in candidates:
            candidate = self._park_recovery_candidate(
                selected,
                now=now,
            )
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
            if candidate.source_status == "stop_requested":
                if candidate.execution_state == "not_started":
                    recovered.append(
                        self._finalize_recovery(
                            candidate,
                            outcome="stopped",
                            result_id=None,
                            now=now,
                        )
                    )
                    continue
                request = TurnReadbackRequest(
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                    sequence=candidate.sequence,
                    worker_id=candidate.worker_id,
                    attempt_id=candidate.attempt_id,
                    lease_generation=candidate.lease_generation,
                    fencing_token=candidate.fencing_token,
                    lease_expires_at=candidate.lease_expires_at,
                    canonical_session_id=(
                        candidate.canonical_session_id
                    ),
                    source_status=candidate.source_status,
                    execution_state=candidate.execution_state,
                )
                evidence = None
                try:
                    evidence = readback.read_turn_with_evidence(
                        request
                    )
                except Exception:
                    pass
                if (
                    self._valid_task7_readback_evidence(
                        evidence,
                        source_status=candidate.source_status,
                    )
                    and evidence.result.outcome == "stopped"
                ):
                    recovered.append(
                        self._finalize_recovery(
                            candidate,
                            outcome="stopped",
                            result_id=None,
                            now=now,
                        )
                    )
                else:
                    recovered.append(
                        self._force_block_stop_recovery(
                            candidate,
                            now=now,
                        )
                    )
                continue
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
                recovered.append(
                    self._finalize_recovery(
                        candidate,
                        outcome=outcome,
                        result_id=None,
                        now=now,
                    )
                )
                continue

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
                evidence = readback.read_turn_with_evidence(request)
            except Exception:
                recovered.append(
                    self._block_recovery(candidate, now=now)
                )
                continue
            if not self._valid_task7_readback_evidence(
                evidence,
                source_status=candidate.source_status,
            ):
                recovered.append(
                    self._block_recovery(candidate, now=now)
                )
                continue
            result = evidence.result
            if result.outcome == "unknown":
                recovered.append(
                    self._block_recovery(candidate, now=now)
                )
                continue
            if result.outcome == "stopped":
                recovered.append(
                    self._finalize_recovery(
                        candidate,
                        outcome="stopped",
                        result_id=None,
                        now=now,
                    )
                )
                continue
            assert result.outcome in {"succeeded", "failed"}
            assert result.result_id is not None
            assert evidence.transcript_batch_id is not None
            recovered.append(
                self._finalize_task7_terminal_recovery(
                    candidate,
                    outcome=result.outcome,
                    result_id=result.result_id,
                    transcript_batch_id=(
                        evidence.transcript_batch_id
                    ),
                    now=now,
                )
            )
        return tuple(recovered)

    @staticmethod
    def _valid_task7_readback_evidence(
        evidence: object,
        *,
        source_status: str,
    ) -> bool:
        if type(evidence) is not Task7TerminalReadbackEvidence:
            return False
        result = evidence.result
        if type(result) is not TurnReadbackResult:
            return False
        if result.outcome in {"succeeded", "failed"}:
            if not (
                type(result.result_id) is str
                and result.result_id
                and evidence.transcript_batch_id == result.result_id
            ):
                return False
            try:
                _require_task7_batch_id(result.result_id)
            except ProjectRuntimeError:
                return False
            return True
        return (
            evidence.transcript_batch_id is None
            and ProjectRuntime._valid_readback_result(
                result,
                source_status=source_status,
            )
        )

    def _finalize_task7_terminal_recovery(
        self,
        candidate: runtime_db.RecoveryCandidateRecord,
        *,
        outcome: Literal["succeeded", "failed"],
        result_id: str,
        transcript_batch_id: str,
        now: int,
    ) -> ProjectTurn:
        with runtime_db.write_transaction(self._conn):
            try:
                current_candidate = (
                    runtime_db._recovery_candidate_for_attempt(
                        self._conn,
                        project_id=candidate.project_id,
                        turn_id=candidate.turn_id,
                        attempt_id=candidate.attempt_id,
                        lease_generation=(
                            candidate.lease_generation
                        ),
                        fencing_token=candidate.fencing_token,
                    )
                )
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    candidate.project_id,
                )
                turn = runtime_db._runtime_turn_for_project(
                    self._conn,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
                lease = (
                    runtime_db._current_worker_lease_for_turn(
                        self._conn,
                        project_id=candidate.project_id,
                        turn_id=candidate.turn_id,
                    )
                )
            except RuntimeError as exc:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                ) from exc
            if not (
                current_candidate == candidate
                and state is not None
                and state.lifecycle == candidate.lifecycle
                and state.conversation_tip_id
                    == candidate.canonical_session_id
                and state.transcript_pending_batch_id is None
                and state.transcript_dispatch_block_key is None
                and turn is not None
                and turn.transcript_applied_batch_id is None
                and lease is None
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
            if (
                runtime_db._project_operation_disposition_for_turn(
                    self._conn,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
                not in {"clear", "reconciled"}
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
            updated = runtime_db._apply_task7_terminal_recovery(
                self._conn,
                candidate=candidate,
                terminal_status=outcome,
                terminal_result_id=result_id,
                now=now,
            )
            if updated is None:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
            updated_state = (
                runtime_db
                ._advance_runtime_version_with_task7_pending_batch(
                    self._conn,
                    project_id=candidate.project_id,
                    expected_version=state.version,
                    transcript_batch_id=transcript_batch_id,
                    updated_at=now,
                )
            )
            if updated_state is None:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                )
            self._event(
                candidate.project_id,
                f"turn.{outcome}",
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
            stop_won = (
                current_candidate.source_status
                == "stop_requested"
                and outcome == "stopped"
            )
            if not stop_won:
                operation_disposition = (
                    runtime_db._project_operation_disposition_for_turn(
                        self._conn,
                        project_id=candidate.project_id,
                        turn_id=candidate.turn_id,
                    )
                )
                if operation_disposition in {
                    "reconciled",
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
                        current_candidate,
                        now=now,
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
            event_payload: dict[str, object] = {
                "attempt_id": current_candidate.attempt_id,
                "fencing_token": current_candidate.fencing_token,
                "lease_generation": current_candidate.lease_generation,
                "source_status": current_candidate.source_status,
                "turn_id": current_candidate.turn_id,
                "version": updated_state.version,
            }
            if outcome == "queued":
                event_payload["attempt"] = {
                    "project_id": current_candidate.project_id,
                    "turn_id": current_candidate.turn_id,
                    "sequence": current_candidate.sequence,
                    "worker_id": current_candidate.worker_id,
                    "attempt_id": current_candidate.attempt_id,
                    "lease_generation": (
                        current_candidate.lease_generation
                    ),
                    "fencing_token": current_candidate.fencing_token,
                    "canonical_session_id": (
                        current_candidate.canonical_session_id
                    ),
                    "lease_expires_at": (
                        current_candidate.lease_expires_at
                    ),
                }
            self._event(
                current_candidate.project_id,
                event_kind,
                current_candidate.turn_id,
                event_payload,
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

    def _force_block_stop_recovery(
        self,
        candidate: runtime_db.RecoveryCandidateRecord,
        *,
        now: int,
    ) -> ProjectTurn:
        if candidate.source_status != "stop_requested":
            raise RuntimeError(
                "forced recovery block requires stop authority"
            )
        with runtime_db.write_transaction(self._conn):
            current_candidate = (
                runtime_db._recovery_candidate_for_attempt(
                    self._conn,
                    project_id=candidate.project_id,
                    turn_id=candidate.turn_id,
                    attempt_id=candidate.attempt_id,
                    lease_generation=candidate.lease_generation,
                    fencing_token=candidate.fencing_token,
                )
            )
            if current_candidate is None:
                return self._turn_from_record(
                    self._turn(
                        candidate.project_id,
                        candidate.turn_id,
                    )
                )
            if current_candidate.source_status != "stop_requested":
                raise RuntimeError(
                    "forced recovery block lost stop authority"
                )
            return self._block_current_recovery(
                current_candidate,
                now=now,
                force_stop=True,
            )

    def _block_current_recovery(
        self,
        candidate: runtime_db.RecoveryCandidateRecord,
        *,
        now: int,
        force_stop: bool = False,
    ) -> ProjectTurn:
        if force_stop and candidate.source_status != "stop_requested":
            raise RuntimeError(
                "forced recovery block requires stop authority"
            )
        if not force_stop and (
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

    def claim_next_turn_for_dispatcher(
        self,
        project_id: str,
        worker_id: str,
        *,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease,
    ) -> WorkerStart | None:
        """Issue one queued start under an exact live Core lease."""
        project_id = _require_text(project_id)
        worker_id = _require_text(worker_id)
        dispatcher_lease = _require_dispatcher_lease(
            dispatcher_lease
        )
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT
            )
        with runtime_db.write_transaction(self._conn):
            now = self._now()
            if now > SQLITE_INT_MAX - lease_seconds:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.INVALID_ARGUMENT
                )
            self._require_dispatcher_start_authority(
                dispatcher_lease,
                now,
            )
            state = self._require_state(project_id)
            self._require_active(state)
            if (
                state.transcript_pending_batch_id is not None
                or state.transcript_dispatch_block_key is not None
            ):
                return None
            head = runtime_db._runnable_project_head(
                self._conn,
                project_id=project_id,
            )
            if head is None:
                return None
            runtime_db._validate_runtime_turn_pair(
                self._conn,
                turn=head,
            )
            if head.status != "queued":
                return None
            attempt_id = self._id_factory("attempt")
            if type(attempt_id) is not str or not attempt_id:
                raise RuntimeError(
                    "turn attempt factory returned invalid identity"
                )
            turn = runtime_db._claim_oldest_queued_runtime_turn(
                self._conn,
                project_id=project_id,
                worker_id=worker_id,
                attempt_id=attempt_id,
                canonical_session_id=state.conversation_tip_id,
                now=now,
                lease_seconds=lease_seconds,
                require_task7_terminal_gate_clear=True,
            )
            if turn is None:
                return None
            lease = runtime_db._current_worker_lease_for_turn(
                self._conn,
                project_id=project_id,
                turn_id=turn.turn_id,
            )
            if lease is None:
                raise RuntimeError(
                    "claimed turn has no current worker lease"
                )
            updated_state = self._advance_state(state, now)
            self._event(
                project_id,
                "turn.claimed",
                turn.turn_id,
                {
                    "attempt_id": turn.attempt_id,
                    "fencing_token": turn.fencing_token,
                    "lease_generation": turn.lease_generation,
                    "sequence": turn.sequence,
                    "turn_id": turn.turn_id,
                    "version": updated_state.version,
                },
                now,
            )
            claim = TurnClaim(
                turn_id=turn.turn_id,
                project_id=project_id,
                sequence=turn.sequence,
                worker_id=worker_id,
                attempt_id=turn.attempt_id,
                lease_generation=turn.lease_generation,
                fencing_token=turn.fencing_token,
                lease_expires_at=lease.expires_at,
                canonical_session_id=state.conversation_tip_id,
            )
            return WorkerStart(
                "queued_turn",
                claim,
                None,
                dispatcher_lease,
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
            ):
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TURN_NOT_QUEUED,
                    project_id=project_id,
                    turn_id=turn_id,
                )
            try:
                retired_attempt_event_id = (
                    self._task7_queued_retired_attempt_event_id(
                        state=state,
                        turn=turn,
                    )
                )
            except Exception as exc:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
                    project_id=project_id,
                    turn_id=turn_id,
                ) from exc
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
            cancelled_payload: dict[str, object] = {
                "turn_id": turn_id,
                "version": updated.version,
            }
            if retired_attempt_event_id is not None:
                cancelled_payload["retired_attempt_event_id"] = (
                    retired_attempt_event_id
                )
            self._event(
                project_id,
                "turn.cancelled",
                turn_id,
                cancelled_payload,
                now,
            )
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

    def _control_event_replay(
        self,
        *,
        project_id: str,
        turn_id: str,
        key: str,
        kind: str,
        fingerprint: str,
        expected_version: int,
        expected_control_version: int,
    ) -> runtime_db.RuntimeControlRecord | None:
        event_id = self._command_event_id(project_id, key)
        row = self._conn.execute(
            """
            SELECT project_id, turn_id, kind, payload_json
            FROM project_events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        expected_payload = canonical_json_object(
            {
                "command_fingerprint": fingerprint,
                "control_version": expected_control_version + 1,
                "turn_id": turn_id,
                "version": expected_version + 1,
            }
        )
        if not (
            row["project_id"] == project_id
            and row["turn_id"] == turn_id
            and row["kind"] == kind
            and row["payload_json"] == expected_payload
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.IDEMPOTENCY_CONFLICT,
                project_id=project_id,
                turn_id=turn_id,
            )
        return self._control(project_id, turn_id)

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
            event_kind = (
                "run.stop_requested"
                if kind == "stop"
                else "run.resume_requested"
            )
            replay = self._control_event_replay(
                project_id=project_id,
                turn_id=turn_id,
                key=idempotency_key,
                kind=event_kind,
                fingerprint=fingerprint,
                expected_version=expected_version,
                expected_control_version=expected_control_version,
            )
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
                lease = runtime_db._current_worker_lease_for_turn(
                    self._conn,
                    project_id=project_id,
                    turn_id=turn_id,
                )
                retained_claim_is_coherent = (
                    _is_text(turn.attempt_id)
                    and turn.execution_state
                    in {"not_started", "started"}
                    and control.attempt_id == turn.attempt_id
                    and _is_text(control.claim_worker_id)
                    and type(control.claim_lease_expires_at) is int
                    and control.claim_lease_expires_at >= 0
                    and control.claim_canonical_session_id
                    == state.conversation_tip_id
                )
                if (
                    retained_claim_is_coherent
                    and turn.status == "stop_requested"
                    and control.control_state == "stop_requested"
                    and lease is not None
                    and lease.project_id == project_id
                    and lease.turn_id == turn_id
                    and lease.lease_id == turn.attempt_id
                    and lease.worker_id == control.claim_worker_id
                    and lease.lease_generation
                    == turn.lease_generation
                    and lease.fencing_token == turn.fencing_token
                    and lease.expires_at
                    == control.claim_lease_expires_at
                ):
                    return self._control_from_record(control)
                if (
                    retained_claim_is_coherent
                    and turn.status == "stopped"
                    and control.control_state == "stopped"
                    and lease is None
                ):
                    return self._control_from_record(control)
                if turn.status != "claimed" or control.control_state != "running":
                    raise ProjectRuntimeError(RuntimeErrorCode.TURN_NOT_CLAIMED, project_id=project_id, turn_id=turn_id)
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
                next_turn, next_control = "stop_requested", "stop_requested"
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
                next_turn, next_control = "queued", "resume_requested"
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
            self._event(
                project_id,
                event_kind,
                turn_id,
                {
                    "command_fingerprint": fingerprint,
                    "control_version": control.control_version + 1,
                    "turn_id": turn_id,
                    "version": updated.version,
                },
                now,
                event_id=self._command_event_id(
                    project_id, idempotency_key
                ),
            )
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
        now = runtime_db._task7_outer_timestamp(self._conn)
        if now is None:
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
                    caller_owns_transaction=True,
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
