"""Durable, provider-neutral authority for one logical external operation."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from enum import Enum
from typing import Literal, Mapping, Protocol

from hermes_cli import project_runtime_db as runtime_db
from hermes_cli.project_policy import (
    ActorContext,
    Decision,
    PolicyDecision,
    approval_class_for_action,
    canonicalize_targets,
)
from hermes_cli.project_runtime import (
    JSONValue,
    ProjectRuntime,
    ProjectRuntimeError,
    RuntimeErrorCode,
    SQLITE_INT_MAX,
    TurnClaim,
    _decode_canonical_object,
    canonical_json_object,
)


OperationStatus = Literal[
    "awaiting_approval",
    "approved",
    "effect_started",
    "receipt_recorded",
    "unknown",
    "reconciled",
    "blocked",
]
ReadbackOutcome = Literal["applied", "not_applied", "unknown"]
OperationDisposition = Literal[
    "clear", "reconciled", "unresolved", "blocked"
]


class OperationErrorCode(str, Enum):
    INVALID_OPERATION_ARGUMENT = "invalid_operation_argument"
    OPERATION_POLICY_DENIED = "operation_policy_denied"
    OPERATION_NOT_FOUND = "operation_not_found"
    OPERATION_IDEMPOTENCY_CONFLICT = "operation_idempotency_conflict"
    OPERATION_STATE_CONFLICT = "operation_state_conflict"
    OPERATION_APPROVAL_CONFLICT = "operation_approval_conflict"
    OPERATION_CAPABILITY_UNSUPPORTED = "operation_capability_unsupported"
    OPERATION_RECEIPT_CONFLICT = "operation_receipt_conflict"
    LEGACY_OPERATION_UNMANAGED = "legacy_operation_unmanaged"


class ProjectOperationError(RuntimeError):
    """A stable, secret-free operation failure for future adapters."""

    def __init__(
        self,
        code: OperationErrorCode,
        *,
        project_id: str | None = None,
        turn_id: str | None = None,
        operation_id: str | None = None,
        current_version: int | None = None,
    ) -> None:
        super().__init__(code.value)
        self.code = code
        self.project_id = project_id
        self.turn_id = turn_id
        self.operation_id = operation_id
        self.current_version = current_version


def _immutable_json_mapping(
    value: Mapping[str, JSONValue],
) -> Mapping[str, JSONValue]:
    """Copy one exact finite JSON mapping into immutable public data."""
    def mutable_json(item: object, seen: set[int]) -> object:
        item_type = type(item)
        if item is None or item_type in {str, bool, int, float}:
            return item
        if item_type in {list, tuple}:
            identity = id(item)
            if identity in seen:
                raise TypeError("cyclic JSON")
            seen.add(identity)
            try:
                return [
                    mutable_json(child, seen) for child in item
                ]
            finally:
                seen.remove(identity)
        if isinstance(item, Mapping):
            identity = id(item)
            if identity in seen:
                raise TypeError("cyclic JSON")
            seen.add(identity)
            try:
                copied: dict[str, object] = {}
                for key, child in item.items():
                    if type(key) is not str:
                        raise TypeError("non-string JSON key")
                    copied[key] = mutable_json(child, seen)
                return copied
            finally:
                seen.remove(identity)
        raise TypeError("unsupported JSON type")

    try:
        copied = mutable_json(value, set())
        encoded = canonical_json_object(copied)
        frozen = _decode_canonical_object(encoded)
    except Exception as exc:
        raise TypeError("value must be an exact finite JSON mapping") from exc
    return frozen


@dataclass(frozen=True)
class OperationApprovalSpec:
    approval_id: str
    approval_class: str
    expires_at: int
    authorization: ActorContext


@dataclass(frozen=True)
class OperationIntent:
    operation_id: str
    project_id: str
    turn_id: str
    idempotency_key: str
    canonical_action: str
    command_revision: int
    targets: tuple[str, ...]
    batch_items: tuple[str, ...]
    payload: Mapping[str, JSONValue]
    readback_kind: str | None
    remote_idempotency_supported: bool

    def __post_init__(self) -> None:
        if type(self.remote_idempotency_supported) is not bool:
            raise TypeError(
                "remote_idempotency_supported must be an exact bool"
            )
        object.__setattr__(
            self, "payload", _immutable_json_mapping(self.payload)
        )


@dataclass(frozen=True)
class OperationReceipt:
    receipt_id: str
    payload: Mapping[str, JSONValue]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "payload", _immutable_json_mapping(self.payload)
        )


@dataclass(frozen=True)
class ProjectOperation:
    operation_id: str
    project_id: str
    turn_id: str
    idempotency_key: str
    canonical_action: str
    command_revision: int
    targets: tuple[str, ...]
    batch_items: tuple[str, ...]
    status: OperationStatus
    approval_id: str | None
    readback_kind: str | None
    receipt_id: str | None
    blocked_reason: str | None
    attempt_id: str
    lease_generation: int
    fencing_token: int
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class OperationReadbackRequest:
    operation_id: str
    project_id: str
    turn_id: str
    canonical_action: str
    targets: tuple[str, ...]
    batch_items: tuple[str, ...]
    idempotency_key: str
    readback_kind: str
    receipt: OperationReceipt | None
    attempt_id: str
    lease_generation: int
    fencing_token: int


@dataclass(frozen=True)
class OperationReadbackResult:
    outcome: ReadbackOutcome
    evidence: Mapping[str, JSONValue] | None
    receipt: OperationReceipt | None

    def __post_init__(self) -> None:
        if self.evidence is not None:
            object.__setattr__(
                self,
                "evidence",
                _immutable_json_mapping(self.evidence),
            )


class OperationReadbackPort(Protocol):
    def read_operation(
        self,
        request: OperationReadbackRequest,
    ) -> OperationReadbackResult: ...


class ProjectOperationGuard:
    """The sole normal mutator for Task-6 operation authority."""

    def __init__(self, runtime: ProjectRuntime) -> None:
        if type(runtime) is not ProjectRuntime:
            raise TypeError("runtime must be a ProjectRuntime")
        self._runtime = runtime
        self._conn: sqlite3.Connection = runtime._conn

    @staticmethod
    def _error(
        code: OperationErrorCode,
        *,
        intent: OperationIntent | None = None,
        current_version: int | None = None,
    ) -> ProjectOperationError:
        return ProjectOperationError(
            code,
            project_id=(
                intent.project_id
                if type(intent) is OperationIntent
                and type(intent.project_id) is str
                else None
            ),
            turn_id=(
                intent.turn_id
                if type(intent) is OperationIntent
                and type(intent.turn_id) is str
                else None
            ),
            operation_id=(
                intent.operation_id
                if type(intent) is OperationIntent
                and type(intent.operation_id) is str
                else None
            ),
            current_version=current_version,
        )

    @staticmethod
    def _canonical_batch_items(values: object) -> tuple[str, ...]:
        if (
            type(values) is not tuple
            or not values
            or any(type(value) is not str or not value for value in values)
            or len(set(values)) != len(values)
        ):
            raise ValueError("invalid operation batch")
        return values

    @staticmethod
    def _intent_storage(
        intent: object,
    ) -> tuple[tuple[str, ...], str, tuple[str, ...], str, str]:
        if type(intent) is not OperationIntent:
            raise ValueError("intent must be exact")
        if not all(
            type(value) is str and bool(value)
            for value in (
                intent.operation_id,
                intent.project_id,
                intent.turn_id,
                intent.idempotency_key,
                intent.canonical_action,
            )
        ):
            raise ValueError("operation identity must be exact")
        if (
            type(intent.command_revision) is not int
            or intent.command_revision <= 0
        ):
            raise ValueError("invalid command revision")
        targets = canonicalize_targets(intent.targets)
        if targets is None or not targets:
            raise ValueError("invalid canonical targets")
        batch_items = ProjectOperationGuard._canonical_batch_items(
            intent.batch_items
        )
        if intent.readback_kind is not None and not (
            type(intent.readback_kind) is str and intent.readback_kind
        ):
            raise ValueError("invalid readback kind")
        mutable_payload = {
            key: ProjectOperationGuard._thaw_json(value)
            for key, value in intent.payload.items()
        }
        payload_json = canonical_json_object(mutable_payload)
        targets_json = json.dumps(
            targets,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        batch_items_json = json.dumps(
            batch_items,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return (
            targets,
            targets_json,
            batch_items,
            batch_items_json,
            payload_json,
        )

    @staticmethod
    def _thaw_json(value: object) -> object:
        if isinstance(value, Mapping):
            return {
                key: ProjectOperationGuard._thaw_json(item)
                for key, item in value.items()
            }
        if type(value) is tuple:
            return [
                ProjectOperationGuard._thaw_json(item)
                for item in value
            ]
        return value

    @staticmethod
    def _validate_policy(
        policy: object,
        approval: object,
        intent: OperationIntent,
    ) -> tuple[Decision, OperationApprovalSpec | None]:
        if (
            type(policy) is not PolicyDecision
            or type(policy.decision) is not Decision
            or type(policy.rule_id) is not str
            or not policy.rule_id
            or type(policy.reason) is not str
            or not policy.reason
        ):
            raise ValueError("invalid policy decision")
        if policy.decision is Decision.DENY:
            raise ProjectOperationGuard._error(
                OperationErrorCode.OPERATION_POLICY_DENIED,
                intent=intent,
            )
        if policy.decision is Decision.ALLOW:
            if approval is not None or policy.approval_class is not None:
                raise ValueError("allow cannot carry approval")
            return policy.decision, None
        if not (
            type(approval) is OperationApprovalSpec
            and type(policy.approval_class) is str
            and policy.approval_class
            and type(approval.approval_id) is str
            and approval.approval_id
            and type(approval.approval_class) is str
            and approval.approval_class == policy.approval_class
            and approval.approval_class
            == approval_class_for_action(intent.canonical_action)
            and type(approval.expires_at) is int
            and approval.expires_at >= 0
            and type(approval.authorization) is ActorContext
            and approval.authorization.is_owner is True
        ):
            raise ValueError("invalid operation approval")
        return policy.decision, approval

    @staticmethod
    def _approval_fingerprint_storage(
        approval: object,
    ) -> str:
        if not (
            type(approval) is OperationApprovalSpec
            and type(approval.approval_id) is str
            and approval.approval_id
            and type(approval.approval_class) is str
            and approval.approval_class
            and type(approval.expires_at) is int
            and approval.expires_at >= 0
            and type(approval.authorization) is ActorContext
            and type(approval.authorization.actor_id) is str
            and approval.authorization.actor_id
            and type(approval.authorization.is_owner) is bool
        ):
            raise ValueError("invalid approval fingerprint")
        return canonical_json_object(
            {
                "approval_class": approval.approval_class,
                "approval_id": approval.approval_id,
                "authorization_actor_id": (
                    approval.authorization.actor_id
                ),
                "expires_at": approval.expires_at,
                "requires_owner": approval.authorization.is_owner,
            }
        )

    def _existing_operation(
        self,
        *,
        claim: TurnClaim,
        intent: OperationIntent,
        targets_json: str,
        batch_items_json: str,
        payload_json: str,
        approval_id: str | None,
        approval_fingerprint_json: str | None,
        remote_idempotency_supported: bool,
    ) -> runtime_db.ProjectOperationRecord | None:
        try:
            by_id = runtime_db._project_operation_for_id(
                self._conn,
                project_id=intent.project_id,
                operation_id=intent.operation_id,
            )
            global_id = self._conn.execute(
                """
                SELECT project_id FROM project_operations
                WHERE operation_id = ?
                """,
                (intent.operation_id,),
            ).fetchone()
            by_key = runtime_db._project_operation_for_idempotency_key(
                self._conn,
                project_id=intent.project_id,
                idempotency_key=intent.idempotency_key,
            )
        except runtime_db.LegacyOperationUnmanagedError as exc:
            raise self._error(
                OperationErrorCode.LEGACY_OPERATION_UNMANAGED,
                intent=intent,
            ) from exc
        conflict = (
            global_id is not None
            and global_id["project_id"] != intent.project_id
        )
        if by_id is not None and by_key is not None and by_id != by_key:
            conflict = True
        existing = by_id or by_key
        approval_matches = True
        if (
            existing is not None
            and approval_fingerprint_json is not None
            and existing.approval_id is not None
        ):
            fingerprint = json.loads(approval_fingerprint_json)
            approval_row = self._conn.execute(
                """
                SELECT approval_class, authorization_actor_id, expires_at
                FROM project_approvals
                WHERE project_id = ? AND approval_id = ?
                  AND operation_id = ?
                """,
                (
                    intent.project_id,
                    existing.approval_id,
                    intent.operation_id,
                ),
            ).fetchone()
            approval_matches = (
                approval_row is not None
                and approval_row["approval_class"]
                == fingerprint["approval_class"]
                and approval_row["authorization_actor_id"]
                == fingerprint["authorization_actor_id"]
                and approval_row["expires_at"]
                == fingerprint["expires_at"]
            )
        if existing is None and not conflict:
            return None
        if (
            conflict
            or existing is None
            or existing.operation_id != intent.operation_id
            or existing.project_id != intent.project_id
            or existing.turn_id != intent.turn_id
            or existing.idempotency_key != intent.idempotency_key
            or existing.canonical_action != intent.canonical_action
            or existing.command_revision != intent.command_revision
            or existing.targets_json != targets_json
            or existing.batch_items_json != batch_items_json
            or existing.payload_json != payload_json
            or existing.approval_id != approval_id
            or existing.readback_kind != intent.readback_kind
            or existing.remote_idempotency_supported
            is not remote_idempotency_supported
            or existing.approval_fingerprint_json
            != approval_fingerprint_json
            or existing.attempt_id != claim.attempt_id
            or existing.lease_generation != claim.lease_generation
            or existing.fencing_token != claim.fencing_token
            or not approval_matches
        ):
            raise self._error(
                OperationErrorCode.OPERATION_IDEMPOTENCY_CONFLICT,
                intent=intent,
            )
        return existing

    @staticmethod
    def _public_operation(
        record: runtime_db.ProjectOperationRecord,
    ) -> ProjectOperation:
        targets = json.loads(record.targets_json)
        batch_items = json.loads(record.batch_items_json)
        return ProjectOperation(
            operation_id=record.operation_id,
            project_id=record.project_id,
            turn_id=record.turn_id,
            idempotency_key=record.idempotency_key,
            canonical_action=record.canonical_action,
            command_revision=record.command_revision,
            targets=tuple(targets),
            batch_items=tuple(batch_items),
            status=record.status,
            approval_id=record.approval_id,
            readback_kind=record.readback_kind,
            receipt_id=record.receipt_id,
            blocked_reason=record.blocked_reason,
            attempt_id=record.attempt_id,
            lease_generation=record.lease_generation,
            fencing_token=record.fencing_token,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def prepare(
        self,
        claim: TurnClaim,
        intent: OperationIntent,
        *,
        policy: PolicyDecision,
        approval: OperationApprovalSpec | None,
    ) -> ProjectOperation:
        try:
            (
                targets,
                targets_json,
                batch_items,
                batch_items_json,
                payload_json,
            ) = self._intent_storage(intent)
        except ProjectOperationError:
            raise
        except Exception as exc:
            raise self._error(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                intent=(
                    intent if type(intent) is OperationIntent else None
                ),
            ) from exc
        capability_supported = (
            intent.remote_idempotency_supported
            and intent.readback_kind is not None
        )
        replay_fingerprint: str | None = None
        if (
            type(policy) is PolicyDecision
            and policy.decision is Decision.REQUIRE_APPROVAL
        ):
            try:
                replay_fingerprint = (
                    self._approval_fingerprint_storage(approval)
                )
            except ValueError:
                replay_fingerprint = None
        replay_approval_id = (
            approval.approval_id
            if (
                capability_supported
                and replay_fingerprint is not None
                and type(approval) is OperationApprovalSpec
            )
            else None
        )
        existing = self._existing_operation(
            claim=claim,
            intent=intent,
            targets_json=targets_json,
            batch_items_json=batch_items_json,
            payload_json=payload_json,
            approval_id=replay_approval_id,
            approval_fingerprint_json=replay_fingerprint,
            remote_idempotency_supported=(
                intent.remote_idempotency_supported
            ),
        )
        if existing is not None:
            return self._public_operation(existing)
        try:
            decision, approval_spec = self._validate_policy(
                policy, approval, intent
            )
            approval_fingerprint_json = (
                self._approval_fingerprint_storage(approval_spec)
                if decision is Decision.REQUIRE_APPROVAL
                else None
            )
        except ProjectOperationError:
            raise
        except Exception as exc:
            raise self._error(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                intent=intent,
            ) from exc
        approval_id = (
            approval_spec.approval_id
            if decision is Decision.REQUIRE_APPROVAL
            and capability_supported
            else None
        )

        now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            state, _, control, _ = (
                self._runtime._require_live_operation_claim(
                    claim, now=now
                )
            )
            if (
                intent.project_id != claim.project_id
                or intent.turn_id != claim.turn_id
            ):
                raise self._error(
                    OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                    intent=intent,
                )
            if (
                capability_supported
                and not runtime_db
                ._turn_allows_new_unresolved_operation(
                    self._conn,
                    project_id=intent.project_id,
                    turn_id=intent.turn_id,
                )
            ):
                raise self._operation_state_conflict(
                    claim, intent.operation_id, state.version
                )
            initial_status = (
                "blocked"
                if not capability_supported
                else "approved"
            )
            inserted = runtime_db._insert_project_operation(
                self._conn,
                operation_id=intent.operation_id,
                project_id=intent.project_id,
                turn_id=intent.turn_id,
                idempotency_key=intent.idempotency_key,
                command_revision=intent.command_revision,
                targets_json=targets_json,
                payload_json=payload_json,
                status=initial_status,
                canonical_action=intent.canonical_action,
                batch_items_json=batch_items_json,
                readback_kind=intent.readback_kind,
                attempt_id=claim.attempt_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                blocked_reason=(
                    "operation_capability_unsupported"
                    if not capability_supported
                    else None
                ),
                remote_idempotency_supported=(
                    intent.remote_idempotency_supported
                ),
                approval_fingerprint_json=(
                    approval_fingerprint_json
                ),
                now=now,
            )
            if not inserted:
                raced = self._existing_operation(
                    claim=claim,
                    intent=intent,
                    targets_json=targets_json,
                    batch_items_json=batch_items_json,
                    payload_json=payload_json,
                    approval_id=approval_id,
                    approval_fingerprint_json=(
                        approval_fingerprint_json
                    ),
                    remote_idempotency_supported=(
                        intent.remote_idempotency_supported
                    ),
                )
                if raced is None:
                    raise self._error(
                        OperationErrorCode.OPERATION_IDEMPOTENCY_CONFLICT,
                        intent=intent,
                    )
                return self._public_operation(raced)

            if (
                decision is Decision.REQUIRE_APPROVAL
                and capability_supported
            ):
                assert approval_spec is not None
                request = runtime_db.ApprovalRequest(
                    approval_id=approval_spec.approval_id,
                    project_id=intent.project_id,
                    requester_actor_id=(
                        approval_spec.authorization.actor_id
                    ),
                    authorization_actor_id=(
                        approval_spec.authorization.actor_id
                    ),
                    canonical_action=intent.canonical_action,
                    approval_class=approval_spec.approval_class,
                    command_revision=intent.command_revision,
                    expected_runtime_version=state.version,
                    expected_lifecycle=state.lifecycle,
                    expected_phase=state.current_phase,
                    targets=targets,
                    batch_id=intent.operation_id,
                    batch_items=batch_items,
                    status="pending",
                    expires_at=approval_spec.expires_at,
                )
                try:
                    self._runtime.request_turn_approval(
                        intent.turn_id,
                        request,
                        approval_spec.authorization,
                        expected_control_version=(
                            control.control_version
                        ),
                    )
                except ProjectRuntimeError as exc:
                    raise self._error(
                        OperationErrorCode.OPERATION_APPROVAL_CONFLICT,
                        intent=intent,
                    ) from exc
                if not runtime_db._link_project_operation_approval(
                    self._conn,
                    project_id=intent.project_id,
                    turn_id=intent.turn_id,
                    operation_id=intent.operation_id,
                    approval_id=approval_spec.approval_id,
                    attempt_id=claim.attempt_id,
                    lease_generation=claim.lease_generation,
                    fencing_token=claim.fencing_token,
                    now=now,
                ):
                    raise self._error(
                        OperationErrorCode.OPERATION_APPROVAL_CONFLICT,
                        intent=intent,
                    )
                updated_state = runtime_db.runtime_state_for_project(
                    self._conn, intent.project_id
                )
                if updated_state is None:
                    raise RuntimeError(
                        "operation approval lost runtime state"
                    )
                event_status = "awaiting_approval"
            else:
                updated_state = self._runtime._advance_state(state, now)
                event_status = initial_status
            self._runtime._event(
                intent.project_id,
                "operation.intent_recorded",
                intent.turn_id,
                {
                    "operation_id": intent.operation_id,
                    "status": event_status,
                    "turn_id": intent.turn_id,
                    "version": updated_state.version,
                },
                now,
            )
            stored = runtime_db._project_operation_for_id(
                self._conn,
                project_id=intent.project_id,
                operation_id=intent.operation_id,
            )
            if stored is None:
                raise RuntimeError("operation insert disappeared")
            return self._public_operation(stored)

    def resolve_operation_approval(
        self,
        approval_id: str,
        resolver: ActorContext,
        *,
        outcome: Literal["approved", "denied"],
    ) -> ProjectOperation:
        if type(approval_id) is not str or not approval_id:
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT
            )
        return self._resolve_operation_approval(
            approval_id,
            resolver=resolver,
            outcome=outcome,
            maintenance=False,
        )

    def _approval_conflict(
        self,
        *,
        row: sqlite3.Row | None,
        operation: runtime_db.ProjectOperationRecord | None = None,
    ) -> ProjectOperationError:
        return ProjectOperationError(
            OperationErrorCode.OPERATION_APPROVAL_CONFLICT,
            project_id=(
                row["project_id"] if row is not None else None
            ),
            turn_id=(
                operation.turn_id
                if operation is not None
                else (
                    row["turn_id"] if row is not None else None
                )
            ),
            operation_id=(
                operation.operation_id
                if operation is not None
                else (
                    row["operation_id"] if row is not None else None
                )
            ),
        )

    def _resolver_matches(
        self,
        row: sqlite3.Row,
        resolver: object,
    ) -> bool:
        if not (
            type(resolver) is ActorContext
            and resolver.actor_id == row["authorization_actor_id"]
        ):
            return False
        try:
            self._runtime._authorize_owner(
                row["project_id"], resolver
            )
        except ProjectRuntimeError:
            return False
        return True

    def _linked_operation(
        self,
        row: sqlite3.Row,
    ) -> runtime_db.ProjectOperationRecord:
        try:
            if not all(
                type(row[field]) is str and row[field]
                for field in (
                    "project_id",
                    "turn_id",
                    "operation_id",
                    "approval_id",
                )
            ):
                raise ValueError("incomplete approval link")
            operation = runtime_db._project_operation_for_id(
                self._conn,
                project_id=row["project_id"],
                operation_id=row["operation_id"],
            )
        except Exception as exc:
            raise self._approval_conflict(
                row=row, operation=None
            ) from exc
        if operation is None or not (
            operation.approval_id == row["approval_id"]
            and operation.turn_id == row["turn_id"]
            and operation.canonical_action == row["canonical_action"]
            and operation.command_revision == row["command_revision"]
            and operation.targets_json == row["targets_json"]
        ):
            raise self._approval_conflict(
                row=row, operation=operation
            )
        try:
            approval = runtime_db._approval_from_row(row)
        except Exception as exc:
            raise self._approval_conflict(
                row=row, operation=operation
            ) from exc
        if not (
            approval.batch_id == operation.operation_id
            and json.dumps(
                approval.batch_items,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            == operation.batch_items_json
            and approval.authorization_actor_id
            == row["authorization_actor_id"]
            and approval.approval_class
            == approval_class_for_action(
                operation.canonical_action
            )
            and operation.approval_fingerprint_json
            == canonical_json_object(
                {
                    "approval_class": approval.approval_class,
                    "approval_id": approval.approval_id,
                    "authorization_actor_id": (
                        approval.authorization_actor_id
                    ),
                    "expires_at": approval.expires_at,
                    "requires_owner": True,
                }
            )
        ):
            raise self._approval_conflict(
                row=row, operation=operation
            )
        return operation

    def _pending_approval_context(
        self,
        row: sqlite3.Row,
        operation: runtime_db.ProjectOperationRecord,
    ) -> tuple[
        runtime_db.RuntimeState,
        runtime_db.RuntimeTurnRecord,
        runtime_db.RuntimeControlRecord,
        runtime_db.WorkerLeaseRecord,
        bool,
    ]:
        state = runtime_db.runtime_state_for_project(
            self._conn, operation.project_id
        )
        turn = runtime_db._runtime_turn_for_project(
            self._conn,
            project_id=operation.project_id,
            turn_id=operation.turn_id,
        )
        control = runtime_db._runtime_control_for_turn(
            self._conn,
            project_id=operation.project_id,
            turn_id=operation.turn_id,
        )
        lease = runtime_db._current_worker_lease_for_turn(
            self._conn,
            project_id=operation.project_id,
            turn_id=operation.turn_id,
        )
        structurally_valid = (
            state is not None
            and turn is not None
            and control is not None
            and lease is not None
            and operation.status == "awaiting_approval"
            and turn.status == "awaiting_approval"
            and control.control_state == "running"
            and type(turn.attempt_id) is str
            and bool(turn.attempt_id)
            and type(turn.lease_generation) is int
            and turn.lease_generation > 0
            and type(turn.fencing_token) is int
            and turn.fencing_token > 0
            and control.attempt_id == turn.attempt_id
            and type(control.claim_worker_id) is str
            and bool(control.claim_worker_id)
            and type(control.claim_lease_expires_at) is int
            and type(control.claim_canonical_session_id) is str
            and bool(control.claim_canonical_session_id)
            and lease.lease_id == turn.attempt_id
            and lease.worker_id == control.claim_worker_id
            and lease.lease_generation == turn.lease_generation
            and lease.fencing_token == turn.fencing_token
            and lease.expires_at
            == control.claim_lease_expires_at
        )
        if not structurally_valid:
            raise self._approval_conflict(
                row=row, operation=operation
            )
        assert state is not None
        assert turn is not None
        assert control is not None
        assert lease is not None
        stale = not (
            type(row["effective_runtime_version"]) is int
            and state.version == row["effective_runtime_version"]
            and state.lifecycle == row["expected_lifecycle"]
            and state.current_phase == row["expected_phase"]
            and type(row["turn_expected_control_version"]) is int
            and control.control_version
            == row["turn_expected_control_version"]
            and operation.attempt_id == turn.attempt_id
            and operation.lease_generation
            == turn.lease_generation
            and operation.fencing_token == turn.fencing_token
            and control.claim_canonical_session_id
            == state.conversation_tip_id
        )
        return state, turn, control, lease, stale

    def _finalize_approval_policy(
        self,
        *,
        row: sqlite3.Row,
        operation: runtime_db.ProjectOperationRecord,
        state: runtime_db.RuntimeState,
        turn: runtime_db.RuntimeTurnRecord,
        control: runtime_db.RuntimeControlRecord,
        lease: runtime_db.WorkerLeaseRecord,
        approval_status: Literal["denied", "expired"],
        resolved_by_actor_id: str | None,
        blocked_reason: str,
        event_reason: str,
        now: int,
    ) -> ProjectOperation:
        terminal_result_id = (
            f"approval-blocked:{row['approval_id']}:"
            f"{operation.operation_id}:{event_reason}"
        )
        changed = runtime_db._finalize_project_operation_approval_policy(
            self._conn,
            approval_id=row["approval_id"],
            approval_status=approval_status,
            resolved_by_actor_id=resolved_by_actor_id,
            project_id=operation.project_id,
            operation_id=operation.operation_id,
            turn_id=operation.turn_id,
            blocked_reason=blocked_reason,
            terminal_result_id=terminal_result_id,
            attempt_id=turn.attempt_id,
            lease_generation=turn.lease_generation,
            fencing_token=turn.fencing_token,
            control_version=control.control_version,
            worker_id=lease.worker_id,
            lease_expires_at=lease.expires_at,
            canonical_session_id=(
                control.claim_canonical_session_id
            ),
            now=now,
        )
        if not changed:
            raise self._approval_conflict(
                row=row, operation=operation
            )
        updated = self._runtime._advance_state(state, now)
        approval_kind = (
            "approval.denied"
            if approval_status == "denied"
            else "approval.expired"
        )
        self._runtime._event(
            operation.project_id,
            approval_kind,
            operation.turn_id,
            {
                "approval_id": row["approval_id"],
                "operation_id": operation.operation_id,
                "reason": event_reason,
                "turn_id": operation.turn_id,
                "version": updated.version,
            },
            now,
        )
        self._runtime._event(
            operation.project_id,
            "operation.blocked",
            operation.turn_id,
            {
                "approval_id": row["approval_id"],
                "operation_id": operation.operation_id,
                "reason": blocked_reason,
                "turn_id": operation.turn_id,
                "version": updated.version,
            },
            now,
        )
        self._runtime._event(
            operation.project_id,
            "turn.failed",
            operation.turn_id,
            {
                "operation_id": operation.operation_id,
                "reason": blocked_reason,
                "terminal_result_id": terminal_result_id,
                "turn_id": operation.turn_id,
                "version": updated.version,
            },
            now,
        )
        stored = runtime_db._project_operation_for_id(
            self._conn,
            project_id=operation.project_id,
            operation_id=operation.operation_id,
        )
        if stored is None:
            raise RuntimeError("finalized operation disappeared")
        return self._public_operation(stored)

    def _resolve_operation_approval(
        self,
        approval_id: str,
        *,
        resolver: object,
        outcome: object,
        maintenance: bool,
    ) -> ProjectOperation:
        now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            row = self._conn.execute(
                """
                SELECT * FROM project_approvals
                WHERE approval_id = ?
                """,
                (approval_id,),
            ).fetchone()
            if row is None:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_NOT_FOUND
                )
            operation = self._linked_operation(row)
            status = row["status"]
            if status != "pending":
                if (
                    status == "approved"
                    and row["consumed_at"] is not None
                    and operation.status == "approved"
                    and type(outcome) is str
                    and outcome == "approved"
                    and self._resolver_matches(row, resolver)
                ):
                    return self._public_operation(operation)
                if (
                    status == "denied"
                    and operation.status == "blocked"
                    and operation.blocked_reason == "approval_denied"
                    and type(outcome) is str
                    and outcome == "denied"
                    and self._resolver_matches(row, resolver)
                ):
                    return self._public_operation(operation)
                raise self._approval_conflict(
                    row=row, operation=operation
                )
            (
                state,
                turn,
                control,
                lease,
                stale,
            ) = self._pending_approval_context(row, operation)
            if now >= row["expires_at"]:
                return self._finalize_approval_policy(
                    row=row,
                    operation=operation,
                    state=state,
                    turn=turn,
                    control=control,
                    lease=lease,
                    approval_status="expired",
                    resolved_by_actor_id=None,
                    blocked_reason="approval_time_expired",
                    event_reason="time_expired",
                    now=now,
                )
            if stale:
                return self._finalize_approval_policy(
                    row=row,
                    operation=operation,
                    state=state,
                    turn=turn,
                    control=control,
                    lease=lease,
                    approval_status="expired",
                    resolved_by_actor_id=None,
                    blocked_reason="approval_stale_boundary",
                    event_reason="stale_boundary",
                    now=now,
                )
            if maintenance or not (
                type(outcome) is str
                and outcome in {"approved", "denied"}
                and self._resolver_matches(row, resolver)
            ):
                raise self._approval_conflict(
                    row=row, operation=operation
                )
            if outcome == "denied":
                return self._finalize_approval_policy(
                    row=row,
                    operation=operation,
                    state=state,
                    turn=turn,
                    control=control,
                    lease=lease,
                    approval_status="denied",
                    resolved_by_actor_id=resolver.actor_id,
                    blocked_reason="approval_denied",
                    event_reason="denied",
                    now=now,
                )
            released = runtime_db._approve_project_operation_approval(
                self._conn,
                approval_id=row["approval_id"],
                project_id=operation.project_id,
                operation_id=operation.operation_id,
                turn_id=operation.turn_id,
                authorization_actor_id=resolver.actor_id,
                attempt_id=turn.attempt_id,
                lease_generation=turn.lease_generation,
                fencing_token=turn.fencing_token,
                expected_control_version=control.control_version,
                release_live_turn=lease.expires_at > now,
                now=now,
            )
            if released is None:
                raise self._approval_conflict(
                    row=row, operation=operation
                )
            updated = self._runtime._advance_state(state, now)
            self._runtime._event(
                operation.project_id,
                "approval.approved",
                operation.turn_id,
                {
                    "approval_id": row["approval_id"],
                    "operation_id": operation.operation_id,
                    "turn_id": operation.turn_id,
                    "version": updated.version,
                },
                now,
            )
            self._runtime._event(
                operation.project_id,
                "operation.approved",
                operation.turn_id,
                {
                    "approval_id": row["approval_id"],
                    "operation_id": operation.operation_id,
                    "turn_id": operation.turn_id,
                    "version": updated.version,
                },
                now,
            )
            if released:
                self._runtime._event(
                    operation.project_id,
                    "turn.approval_released",
                    operation.turn_id,
                    {
                        "approval_id": row["approval_id"],
                        "operation_id": operation.operation_id,
                        "turn_id": operation.turn_id,
                        "version": updated.version,
                    },
                    now,
                )
            stored = runtime_db._project_operation_for_id(
                self._conn,
                project_id=operation.project_id,
                operation_id=operation.operation_id,
            )
            if stored is None:
                raise RuntimeError("approved operation disappeared")
            return self._public_operation(stored)

    def expire_due_operation_approvals(
        self,
        *,
        limit: int,
    ) -> tuple[ProjectOperation, ...]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT
            )
        now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            approval_ids = (
                runtime_db._select_operation_approval_maintenance(
                    self._conn, now=now, limit=limit
                )
            )
        expired: list[ProjectOperation] = []
        for approval_id in approval_ids:
            try:
                expired.append(
                    self._resolve_operation_approval(
                        approval_id,
                        resolver=None,
                        outcome=None,
                        maintenance=True,
                    )
                )
            except ProjectOperationError as exc:
                if (
                    exc.code
                    is not OperationErrorCode.OPERATION_APPROVAL_CONFLICT
                ):
                    raise
        return tuple(expired)

    def _rehydrate_approved_operation(
        self,
        project_id: str,
        operation_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
    ) -> TurnClaim | None:
        if not (
            type(project_id) is str
            and project_id
            and type(operation_id) is str
            and operation_id
            and type(worker_id) is str
            and worker_id
            and type(lease_seconds) is int
            and lease_seconds > 0
        ):
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                project_id=(
                    project_id if type(project_id) is str else None
                ),
                operation_id=(
                    operation_id
                    if type(operation_id) is str
                    else None
                ),
            )
        now = self._runtime._now()
        if now > SQLITE_INT_MAX - lease_seconds:
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                project_id=project_id,
                operation_id=operation_id,
            )
        with runtime_db.write_transaction(self._conn):
            state = runtime_db.runtime_state_for_project(
                self._conn, project_id
            )
            if state is None or state.lifecycle != "active":
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    operation_id=operation_id,
                    current_version=(
                        state.version if state is not None else None
                    ),
                )
            try:
                operation = runtime_db._project_operation_for_id(
                    self._conn,
                    project_id=project_id,
                    operation_id=operation_id,
                )
            except runtime_db.LegacyOperationUnmanagedError as exc:
                raise ProjectOperationError(
                    OperationErrorCode.LEGACY_OPERATION_UNMANAGED,
                    project_id=project_id,
                    operation_id=operation_id,
                ) from exc
            if operation is None:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_NOT_FOUND,
                    project_id=project_id,
                    operation_id=operation_id,
                )
            if operation.status != "approved":
                return None
            turn = runtime_db._runtime_turn_for_project(
                self._conn,
                project_id=project_id,
                turn_id=operation.turn_id,
            )
            control = runtime_db._runtime_control_for_turn(
                self._conn,
                project_id=project_id,
                turn_id=operation.turn_id,
            )
            lease = runtime_db._current_worker_lease_for_turn(
                self._conn,
                project_id=project_id,
                turn_id=operation.turn_id,
            )
            if turn is None or control is None:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation_id,
                    current_version=state.version,
                )
            if turn.status not in {
                "awaiting_approval",
                "reconciling",
            }:
                return None
            oldest = self._conn.execute(
                """
                SELECT turn_id FROM project_turns
                WHERE project_id = ?
                  AND status NOT IN ('succeeded', 'failed', 'cancelled')
                ORDER BY sequence, turn_id
                LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            current_pair = (
                oldest is not None
                and oldest["turn_id"] == operation.turn_id
                and type(turn.attempt_id) is str
                and turn.attempt_id
                and operation.attempt_id == turn.attempt_id
                and operation.lease_generation
                == turn.lease_generation
                and operation.fencing_token == turn.fencing_token
                and control.control_state == "running"
                and control.attempt_id == turn.attempt_id
                and type(control.claim_worker_id) is str
                and control.claim_worker_id
                and type(control.claim_lease_expires_at) is int
                and type(control.claim_canonical_session_id) is str
                and control.claim_canonical_session_id
                == state.conversation_tip_id
                and turn.recovery_block_key is None
            )
            if not current_pair:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation_id,
                    current_version=state.version,
                )
            if turn.status == "awaiting_approval":
                if not (
                    lease is not None
                    and lease.lease_id == turn.attempt_id
                    and lease.worker_id
                    == control.claim_worker_id
                    and lease.lease_generation
                    == turn.lease_generation
                    and lease.fencing_token == turn.fencing_token
                    and lease.expires_at
                    == control.claim_lease_expires_at
                ):
                    raise ProjectOperationError(
                        OperationErrorCode.OPERATION_STATE_CONFLICT,
                        project_id=project_id,
                        turn_id=operation.turn_id,
                        operation_id=operation_id,
                        current_version=state.version,
                    )
                if lease.expires_at > now:
                    return None
            elif lease is not None:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation_id,
                    current_version=state.version,
                )
            if operation.approval_id is not None:
                approval = self._conn.execute(
                    """
                    SELECT status, consumed_at
                    FROM project_approvals
                    WHERE project_id = ? AND approval_id = ?
                      AND operation_id = ? AND turn_id = ?
                    """,
                    (
                        project_id,
                        operation.approval_id,
                        operation_id,
                        operation.turn_id,
                    ),
                ).fetchone()
                if not (
                    approval is not None
                    and approval["status"] == "approved"
                    and type(approval["consumed_at"]) is int
                ):
                    raise ProjectOperationError(
                        OperationErrorCode.OPERATION_APPROVAL_CONFLICT,
                        project_id=project_id,
                        turn_id=operation.turn_id,
                        operation_id=operation_id,
                        current_version=state.version,
                    )
            if (
                turn.lease_generation >= SQLITE_INT_MAX
                or turn.fencing_token >= SQLITE_INT_MAX
            ):
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation_id,
                    current_version=state.version,
                )
            new_attempt_id = self._runtime._id_factory("attempt")
            if type(new_attempt_id) is not str or not new_attempt_id:
                raise RuntimeError(
                    "operation attempt factory returned invalid identity"
                )
            generation = turn.lease_generation + 1
            fence = turn.fencing_token + 1
            expires_at = now + lease_seconds
            stored_turn, stored_lease = (
                runtime_db._rehydrate_project_operation_claim(
                    self._conn,
                    project_id=project_id,
                    operation_id=operation_id,
                    turn_id=operation.turn_id,
                    source_turn_status=turn.status,
                    old_attempt_id=turn.attempt_id,
                    old_lease_generation=turn.lease_generation,
                    old_fencing_token=turn.fencing_token,
                    old_control_version=control.control_version,
                    old_worker_id=control.claim_worker_id,
                    old_lease_expires_at=(
                        control.claim_lease_expires_at
                    ),
                    old_canonical_session_id=(
                        control.claim_canonical_session_id
                    ),
                    old_lease_present=lease is not None,
                    new_attempt_id=new_attempt_id,
                    new_worker_id=worker_id,
                    new_lease_generation=generation,
                    new_fencing_token=fence,
                    new_lease_expires_at=expires_at,
                    canonical_session_id=state.conversation_tip_id,
                    now=now,
                )
            )
            updated = self._runtime._advance_state(state, now)
            self._runtime._event(
                project_id,
                "operation.rehydrated",
                operation.turn_id,
                {
                    "attempt_id": new_attempt_id,
                    "fencing_token": fence,
                    "lease_generation": generation,
                    "operation_id": operation_id,
                    "turn_id": operation.turn_id,
                    "version": updated.version,
                },
                now,
            )
            self._runtime._event(
                project_id,
                "turn.claimed",
                operation.turn_id,
                {
                    "attempt_id": new_attempt_id,
                    "fencing_token": fence,
                    "lease_generation": generation,
                    "sequence": stored_turn.sequence,
                    "turn_id": operation.turn_id,
                    "version": updated.version,
                },
                now,
            )
            return TurnClaim(
                turn_id=operation.turn_id,
                project_id=project_id,
                sequence=stored_turn.sequence,
                worker_id=worker_id,
                attempt_id=new_attempt_id,
                lease_generation=generation,
                fencing_token=fence,
                lease_expires_at=stored_lease.expires_at,
                canonical_session_id=state.conversation_tip_id,
            )

    def _recover_pending_operations(
        self,
        readback: OperationReadbackPort,
        *,
        worker_id: str,
        lease_seconds: int,
        limit: int,
    ) -> tuple[
        tuple[ProjectOperation, TurnClaim | None], ...
    ]:
        """Run one bounded Task-6 pass over the derived recovery lane."""
        if not (
            callable(getattr(readback, "read_operation", None))
            and type(worker_id) is str
            and worker_id
            and type(lease_seconds) is int
            and lease_seconds > 0
            and type(limit) is int
            and 1 <= limit <= 100
            and not self._conn.in_transaction
        ):
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT
            )
        selected = runtime_db._operation_pending_candidates(
            self._conn, limit=limit
        )
        recovered: list[
            tuple[ProjectOperation, TurnClaim | None]
        ] = []
        for candidate in selected:
            current = runtime_db._operation_pending_for_turn(
                self._conn,
                project_id=candidate.project_id,
                turn_id=candidate.turn_id,
            )
            if current != candidate:
                continue
            if candidate.status == "approved":
                try:
                    fresh = self._rehydrate_approved_operation(
                        candidate.project_id,
                        candidate.operation_id,
                        worker_id=worker_id,
                        lease_seconds=lease_seconds,
                    )
                except ProjectOperationError as exc:
                    if (
                        exc.code
                        is OperationErrorCode.OPERATION_STATE_CONFLICT
                    ):
                        continue
                    raise
                if fresh is None:
                    continue
                recovered.append(
                    (
                        self._stored_public_operation(
                            candidate.project_id,
                            candidate.operation_id,
                        ),
                        fresh,
                    )
                )
                continue
            recovery = runtime_db._recovery_candidate_for_attempt(
                self._conn,
                project_id=candidate.project_id,
                turn_id=candidate.turn_id,
                attempt_id=candidate.attempt_id,
                lease_generation=candidate.lease_generation,
                fencing_token=candidate.fencing_token,
            )
            if recovery is None:
                continue
            claim = TurnClaim(
                turn_id=recovery.turn_id,
                project_id=recovery.project_id,
                sequence=recovery.sequence,
                worker_id=recovery.worker_id,
                attempt_id=recovery.attempt_id,
                lease_generation=recovery.lease_generation,
                fencing_token=recovery.fencing_token,
                lease_expires_at=recovery.lease_expires_at,
                canonical_session_id=recovery.canonical_session_id,
            )
            try:
                operation = self.reconcile(
                    claim, candidate.operation_id, readback
                )
            except ProjectOperationError as exc:
                if (
                    exc.code
                    is OperationErrorCode.OPERATION_STATE_CONFLICT
                ):
                    continue
                raise
            except ProjectRuntimeError as exc:
                if exc.code is RuntimeErrorCode.STALE_TURN_CLAIM:
                    continue
                raise
            recovered.append((operation, None))
        return tuple(recovered)

    @staticmethod
    def _validate_operation_id(operation_id: object) -> None:
        if type(operation_id) is not str or not operation_id:
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                operation_id=(
                    operation_id
                    if type(operation_id) is str
                    else None
                ),
            )

    @staticmethod
    def _operation_state_conflict(
        claim: TurnClaim,
        operation_id: str,
        current_version: int,
    ) -> ProjectOperationError:
        return ProjectOperationError(
            OperationErrorCode.OPERATION_STATE_CONFLICT,
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            operation_id=operation_id,
            current_version=current_version,
        )

    @staticmethod
    def _operation_receipt_conflict(
        claim: TurnClaim,
        operation_id: str,
        current_version: int,
    ) -> ProjectOperationError:
        return ProjectOperationError(
            OperationErrorCode.OPERATION_RECEIPT_CONFLICT,
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            operation_id=operation_id,
            current_version=current_version,
        )

    @staticmethod
    def _is_exact_receipt_unique_conflict(
        error: sqlite3.IntegrityError,
    ) -> bool:
        return str(error) == (
            "UNIQUE constraint failed: "
            "project_operations.project_id, "
            "project_operations.receipt_id"
        )

    def _operation_for_claim(
        self,
        claim: TurnClaim,
        operation_id: str,
    ) -> runtime_db.ProjectOperationRecord:
        try:
            operation = runtime_db._project_operation_for_id(
                self._conn,
                project_id=claim.project_id,
                operation_id=operation_id,
            )
        except runtime_db.LegacyOperationUnmanagedError as exc:
            raise ProjectOperationError(
                OperationErrorCode.LEGACY_OPERATION_UNMANAGED,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
                operation_id=operation_id,
            ) from exc
        if operation is None:
            raise ProjectOperationError(
                OperationErrorCode.OPERATION_NOT_FOUND,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
                operation_id=operation_id,
            )
        if not (
            operation.turn_id == claim.turn_id
            and operation.attempt_id == claim.attempt_id
            and operation.lease_generation
            == claim.lease_generation
            and operation.fencing_token == claim.fencing_token
        ):
            raise self._runtime._stale_turn_claim(claim)
        return operation

    def _stored_public_operation(
        self,
        project_id: str,
        operation_id: str,
    ) -> ProjectOperation:
        operation = runtime_db._project_operation_for_id(
            self._conn,
            project_id=project_id,
            operation_id=operation_id,
        )
        if operation is None:
            raise RuntimeError("operation transition disappeared")
        return self._public_operation(operation)

    @staticmethod
    def _transition_event_payload(
        claim: TurnClaim,
        operation_id: str,
        version: int,
    ) -> dict[str, object]:
        return {
            "attempt_id": claim.attempt_id,
            "fencing_token": claim.fencing_token,
            "lease_generation": claim.lease_generation,
            "operation_id": operation_id,
            "turn_id": claim.turn_id,
            "version": version,
        }

    @classmethod
    def _receipt_storage(
        cls,
        receipt: object,
    ) -> tuple[str, str]:
        try:
            if not (
                type(receipt) is OperationReceipt
                and type(receipt.receipt_id) is str
                and receipt.receipt_id
            ):
                raise ValueError("invalid receipt")
            receipt_json = canonical_json_object(
                {
                    key: cls._thaw_json(value)
                    for key, value in receipt.payload.items()
                }
            )
        except Exception as exc:
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT
            ) from exc
        return receipt.receipt_id, receipt_json

    @staticmethod
    def _receipt_from_record(
        operation: runtime_db.ProjectOperationRecord,
    ) -> OperationReceipt:
        if (
            operation.receipt_id is None
            or operation.receipt_json is None
        ):
            raise RuntimeError("recorded receipt is incomplete")
        return OperationReceipt(
            operation.receipt_id,
            json.loads(operation.receipt_json),
        )

    @staticmethod
    def _readback_request(
        operation: runtime_db.ProjectOperationRecord,
        receipt: OperationReceipt | None,
    ) -> OperationReadbackRequest:
        if operation.readback_kind is None:
            raise RuntimeError("managed operation lost readback kind")
        return OperationReadbackRequest(
            operation_id=operation.operation_id,
            project_id=operation.project_id,
            turn_id=operation.turn_id,
            canonical_action=operation.canonical_action,
            targets=tuple(json.loads(operation.targets_json)),
            batch_items=tuple(
                json.loads(operation.batch_items_json)
            ),
            idempotency_key=operation.idempotency_key,
            readback_kind=operation.readback_kind,
            receipt=receipt,
            attempt_id=operation.attempt_id,
            lease_generation=operation.lease_generation,
            fencing_token=operation.fencing_token,
        )

    def _reconciliation_authority(
        self,
        claim: TurnClaim,
        *,
        now: int,
    ) -> tuple[
        runtime_db.RuntimeState,
        bool,
        runtime_db.RuntimeTurnRecord | None,
        runtime_db.RuntimeControlRecord | None,
        runtime_db.WorkerLeaseRecord | None,
    ]:
        claim = self._runtime._require_turn_claim(claim)
        try:
            state, turn, control, lease = (
                self._runtime._require_live_operation_claim(
                    claim, now=now
                )
            )
            return state, False, turn, control, lease
        except ProjectRuntimeError as stale:
            candidate = (
                runtime_db._recovery_candidate_for_attempt(
                    self._conn,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                    attempt_id=claim.attempt_id,
                    lease_generation=claim.lease_generation,
                    fencing_token=claim.fencing_token,
                )
            )
            state = runtime_db.runtime_state_for_project(
                self._conn, claim.project_id
            )
            turn = runtime_db._runtime_turn_for_project(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )
            if not (
                candidate is not None
                and state is not None
                and state.lifecycle == "active"
                and state.conversation_tip_id
                == claim.canonical_session_id
                and turn is not None
                and turn.recovery_block_key is None
                and candidate.sequence == claim.sequence
                and candidate.worker_id == claim.worker_id
                and candidate.lease_expires_at
                == claim.lease_expires_at
                and candidate.canonical_session_id
                == claim.canonical_session_id
            ):
                raise stale
            return state, True, None, None, None

    @classmethod
    def _classify_readback(
        cls,
        result: object,
        parked_receipt: OperationReceipt | None,
    ) -> tuple[
        Literal["approved", "reconciled", "blocked"],
        str | None,
        str | None,
        str | None,
        str | None,
        str,
    ]:
        if parked_receipt is None:
            parked_id = parked_json = None
        else:
            parked_id, parked_json = cls._receipt_storage(
                parked_receipt
            )

        def blocked() -> tuple[
            Literal["approved", "reconciled", "blocked"],
            str | None,
            str | None,
            str | None,
            str | None,
            str,
        ]:
            return (
                "blocked",
                parked_id,
                parked_json,
                None,
                "operation_readback_ambiguous",
                "operation.blocked",
            )

        try:
            if not (
                type(result) is OperationReadbackResult
                and type(result.outcome) is str
                and result.outcome
                in {"applied", "not_applied", "unknown"}
            ):
                return blocked()
            if result.outcome == "unknown":
                return blocked()
            if not (
                isinstance(result.evidence, Mapping)
                and bool(result.evidence)
            ):
                return blocked()
            evidence = {
                key: cls._thaw_json(value)
                for key, value in result.evidence.items()
            }
            if result.outcome == "not_applied":
                if (
                    parked_receipt is not None
                    or result.receipt is not None
                ):
                    return blocked()
                return (
                    "approved",
                    None,
                    None,
                    canonical_json_object(
                        {
                            "evidence": evidence,
                            "outcome": "not_applied",
                        }
                    ),
                    None,
                    "operation.not_applied",
                )
            result_receipt = result.receipt
            if result_receipt is not None:
                result_id, result_json = cls._receipt_storage(
                    result_receipt
                )
            else:
                result_id = result_json = None
            if parked_receipt is not None:
                if (
                    result_receipt is None
                    or result_id != parked_id
                    or result_json != parked_json
                ):
                    return blocked()
                receipt_id, receipt_json = parked_id, parked_json
            else:
                if result_receipt is None:
                    return blocked()
                receipt_id, receipt_json = result_id, result_json
            return (
                "reconciled",
                receipt_id,
                receipt_json,
                canonical_json_object(
                    {
                        "evidence": evidence,
                        "outcome": "applied",
                    }
                ),
                None,
                "operation.reconciled",
            )
        except Exception:
            return blocked()

    def mark_started(
        self,
        claim: TurnClaim,
        operation_id: str,
    ) -> ProjectOperation:
        self._validate_operation_id(operation_id)
        now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            operation = self._operation_for_claim(
                claim, operation_id
            )
            if (
                operation.status == "approved"
                and operation.readback_json is not None
            ):
                state = runtime_db.runtime_state_for_project(
                    self._conn, claim.project_id
                )
                if state is None:
                    raise RuntimeError("operation project disappeared")
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            state, _, _, _ = (
                self._runtime._require_live_operation_claim(
                    claim, now=now
                )
            )
            if operation.status == "effect_started":
                return self._public_operation(operation)
            if operation.status != "approved":
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            if not runtime_db._mark_project_operation_started(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
                operation_id=operation_id,
                attempt_id=claim.attempt_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                now=now,
            ):
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            updated = self._runtime._advance_state(state, now)
            self._runtime._event(
                claim.project_id,
                "operation.effect_started",
                claim.turn_id,
                self._transition_event_payload(
                    claim, operation_id, updated.version
                ),
                now,
            )
            return self._stored_public_operation(
                claim.project_id, operation_id
            )

    def record_receipt(
        self,
        claim: TurnClaim,
        operation_id: str,
        receipt: OperationReceipt,
    ) -> ProjectOperation:
        self._validate_operation_id(operation_id)
        receipt_id, receipt_json = self._receipt_storage(receipt)
        now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            state, _, _, _ = (
                self._runtime._require_live_operation_claim(
                    claim, now=now
                )
            )
            operation = self._operation_for_claim(
                claim, operation_id
            )
            if operation.status in {
                "receipt_recorded",
                "reconciled",
            }:
                if (
                    operation.receipt_id == receipt_id
                    and operation.receipt_json == receipt_json
                ):
                    return self._public_operation(operation)
                raise self._operation_receipt_conflict(
                    claim, operation_id, state.version
                )
            if operation.status != "effect_started":
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            owner = runtime_db._project_operation_receipt_owner(
                self._conn,
                project_id=claim.project_id,
                receipt_id=receipt_id,
            )
            if owner is not None and owner != operation_id:
                raise self._operation_receipt_conflict(
                    claim, operation_id, state.version
                )
            try:
                recorded = runtime_db._record_project_operation_receipt(
                    self._conn,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                    operation_id=operation_id,
                    attempt_id=claim.attempt_id,
                    lease_generation=claim.lease_generation,
                    fencing_token=claim.fencing_token,
                    receipt_id=receipt_id,
                    receipt_json=receipt_json,
                    now=now,
                )
            except sqlite3.IntegrityError as exc:
                if not self._is_exact_receipt_unique_conflict(exc):
                    raise
                raise self._operation_receipt_conflict(
                    claim, operation_id, state.version
                ) from exc
            if not recorded:
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            updated = self._runtime._advance_state(state, now)
            self._runtime._event(
                claim.project_id,
                "operation.receipt_recorded",
                claim.turn_id,
                self._transition_event_payload(
                    claim, operation_id, updated.version
                ),
                now,
            )
            return self._stored_public_operation(
                claim.project_id, operation_id
            )

    def reconcile(
        self,
        claim: TurnClaim,
        operation_id: str,
        readback: OperationReadbackPort,
    ) -> ProjectOperation:
        self._validate_operation_id(operation_id)
        now = self._runtime._now()
        parked_receipt: OperationReceipt | None = None
        with runtime_db.write_transaction(self._conn):
            (
                state,
                recovery_candidate,
                _,
                _,
                _,
            ) = (
                self._reconciliation_authority(
                    claim, now=now
                )
            )
            operation = self._operation_for_claim(
                claim, operation_id
            )
            if operation.status in {
                "reconciled",
                "blocked",
            } or (
                operation.status == "approved"
                and operation.readback_json is not None
            ):
                return self._public_operation(operation)
            if operation.status not in {
                "effect_started",
                "receipt_recorded",
                "unknown",
            }:
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            if operation.receipt_id is not None:
                parked_receipt = self._receipt_from_record(operation)
            if operation.status != "unknown":
                if not runtime_db._park_project_operation_unknown(
                    self._conn,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                    operation_id=operation_id,
                    source_status=operation.status,
                    attempt_id=claim.attempt_id,
                    lease_generation=claim.lease_generation,
                    fencing_token=claim.fencing_token,
                    now=now,
                ):
                    raise self._operation_state_conflict(
                        claim, operation_id, state.version
                    )
                updated = self._runtime._advance_state(state, now)
                self._runtime._event(
                    claim.project_id,
                    "operation.unknown",
                    claim.turn_id,
                    self._transition_event_payload(
                        claim, operation_id, updated.version
                    ),
                    now,
                )
            request = self._readback_request(
                operation, parked_receipt
            )

        try:
            result = readback.read_operation(request)
        except Exception:
            result = None

        phase_c_now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            (
                state,
                phase_c_candidate,
                phase_c_turn,
                phase_c_control,
                phase_c_lease,
            ) = (
                self._reconciliation_authority(
                    claim, now=phase_c_now
                )
            )
            if phase_c_candidate != recovery_candidate:
                raise self._runtime._stale_turn_claim(claim)
            current = self._operation_for_claim(
                claim, operation_id
            )
            if current.status != "unknown":
                if current.status in {
                    "reconciled",
                    "blocked",
                } or (
                    current.status == "approved"
                    and current.readback_json is not None
                ):
                    return self._public_operation(current)
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            (
                target_status,
                final_receipt_id,
                final_receipt_json,
                readback_json,
                blocked_reason,
                event_kind,
            ) = self._classify_readback(result, parked_receipt)
            if final_receipt_id is not None:
                owner = runtime_db._project_operation_receipt_owner(
                    self._conn,
                    project_id=claim.project_id,
                    receipt_id=final_receipt_id,
                )
                if owner is not None and owner != operation_id:
                    target_status = "blocked"
                    final_receipt_id = (
                        current.receipt_id
                        if current.receipt_id is not None
                        else None
                    )
                    final_receipt_json = (
                        current.receipt_json
                        if current.receipt_id is not None
                        else None
                    )
                    readback_json = None
                    blocked_reason = "operation_readback_ambiguous"
                    event_kind = "operation.blocked"
            try:
                finalized = (
                    runtime_db._finalize_project_operation_readback(
                        self._conn,
                        project_id=claim.project_id,
                        turn_id=claim.turn_id,
                        operation_id=operation_id,
                        attempt_id=claim.attempt_id,
                        lease_generation=claim.lease_generation,
                        fencing_token=claim.fencing_token,
                        target_status=target_status,
                        receipt_id=final_receipt_id,
                        receipt_json=final_receipt_json,
                        readback_json=readback_json,
                        blocked_reason=blocked_reason,
                        now=phase_c_now,
                    )
                )
            except sqlite3.IntegrityError as exc:
                if not self._is_exact_receipt_unique_conflict(exc):
                    raise
                target_status = "blocked"
                final_receipt_id = current.receipt_id
                final_receipt_json = current.receipt_json
                readback_json = None
                blocked_reason = "operation_readback_ambiguous"
                event_kind = "operation.blocked"
                finalized = (
                    runtime_db._finalize_project_operation_readback(
                        self._conn,
                        project_id=claim.project_id,
                        turn_id=claim.turn_id,
                        operation_id=operation_id,
                        attempt_id=claim.attempt_id,
                        lease_generation=claim.lease_generation,
                        fencing_token=claim.fencing_token,
                        target_status=target_status,
                        receipt_id=final_receipt_id,
                        receipt_json=final_receipt_json,
                        readback_json=readback_json,
                        blocked_reason=blocked_reason,
                        now=phase_c_now,
                    )
                )
            if not finalized:
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            if (
                target_status == "approved"
                and not phase_c_candidate
            ):
                if (
                    phase_c_turn is None
                    or phase_c_control is None
                    or phase_c_lease is None
                ):
                    raise RuntimeError(
                        "live reconciliation records disappeared"
                    )
                runtime_db._park_live_runtime_turn_for_operation_block(
                    self._conn,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                    sequence=phase_c_turn.sequence,
                    attempt_id=claim.attempt_id,
                    worker_id=claim.worker_id,
                    lease_generation=claim.lease_generation,
                    fencing_token=claim.fencing_token,
                    lease_expires_at=phase_c_lease.expires_at,
                    canonical_session_id=claim.canonical_session_id,
                    control_version=phase_c_control.control_version,
                    now=phase_c_now,
                )
            updated = self._runtime._advance_state(
                state, phase_c_now
            )
            payload = self._transition_event_payload(
                claim, operation_id, updated.version
            )
            if blocked_reason is not None:
                payload["reason"] = blocked_reason
            self._runtime._event(
                claim.project_id,
                event_kind,
                claim.turn_id,
                payload,
                phase_c_now,
            )
            return self._stored_public_operation(
                claim.project_id, operation_id
            )

    def block_unknown(
        self,
        claim: TurnClaim,
        operation_id: str,
    ) -> ProjectOperation:
        self._validate_operation_id(operation_id)
        now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            state, _, _, _ = (
                self._runtime._require_live_operation_claim(
                    claim, now=now
                )
            )
            operation = self._operation_for_claim(
                claim, operation_id
            )
            if operation.status == "blocked":
                return self._public_operation(operation)
            if operation.status != "unknown":
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            reason = "operation_readback_ambiguous"
            receipt_id = operation.receipt_id
            receipt_json = operation.receipt_json
            if not runtime_db._finalize_project_operation_readback(
                self._conn,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
                operation_id=operation_id,
                attempt_id=claim.attempt_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                target_status="blocked",
                receipt_id=receipt_id,
                receipt_json=receipt_json,
                readback_json=None,
                blocked_reason=reason,
                now=now,
            ):
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            updated = self._runtime._advance_state(state, now)
            payload = self._transition_event_payload(
                claim, operation_id, updated.version
            )
            payload["reason"] = reason
            self._runtime._event(
                claim.project_id,
                "operation.blocked",
                claim.turn_id,
                payload,
                now,
            )
            return self._stored_public_operation(
                claim.project_id, operation_id
            )

    def disposition_for_turn(
        self,
        project_id: str,
        turn_id: str,
    ) -> OperationDisposition:
        if not (
            type(project_id) is str
            and project_id
            and type(turn_id) is str
            and turn_id
        ):
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                project_id=(
                    project_id
                    if type(project_id) is str
                    else None
                ),
                turn_id=(
                    turn_id if type(turn_id) is str else None
                ),
            )
        internal = (
            runtime_db._project_operation_disposition_for_turn(
                self._conn,
                project_id=project_id,
                turn_id=turn_id,
            )
        )
        if internal in {
            "pre_effect_blocked",
            "post_effect_blocked",
        }:
            return "blocked"
        return internal
