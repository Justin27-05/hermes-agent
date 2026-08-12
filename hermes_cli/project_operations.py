"""Durable, provider-neutral authority for one logical external operation."""

from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
import uuid
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from typing import Any, Literal, Mapping, Protocol

from hermes_cli import project_runtime_db as runtime_db
from hermes_cli.project_policy import (
    ActorContext,
    ContractPolicyView,
    Decision,
    PolicyDecision,
    ProjectBindingView,
    ProjectCommand,
    ProjectPolicyView,
    approval_class_for_action,
    canonicalize_targets,
    decide as decide_project_policy,
)
from hermes_cli.project_runtime import (
    DispatcherLease,
    JSONValue,
    ProjectRuntime,
    ProjectRuntimeError,
    RuntimeErrorCode,
    SQLITE_INT_MAX,
    TurnAttemptIdentity,
    TurnClaim,
    TurnExecutionInput,
    TurnOrigin,
    WorkerStart,
    _decode_canonical_object,
    _require_dispatcher_lease,
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
    policy_authority_sha256: str | None = None

    @property
    def approval_checkpoint_id(self) -> str | None:
        return getattr(self, "_approval_checkpoint_id", None)


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


@dataclass(frozen=True)
class OperationRecoveryCursor:
    recovery_membership_sequence: int
    project_id: str
    operation_id: str
    turn_id: str


@dataclass(frozen=True)
class OperationRecoveryMember:
    recovery_membership_sequence: int
    project_id: str
    operation_id: str
    turn_id: str
    status: OperationStatus


@dataclass(frozen=True)
class OperationRecoveryMemberScanResult:
    members: tuple[OperationRecoveryMember, ...]
    scanned_through: OperationRecoveryCursor | None
    reached_epoch_end: bool


@dataclass(frozen=True)
class OperationRecoveryScanResult:
    starts: tuple[WorkerStart, ...]
    scanned_through: OperationRecoveryCursor | None
    reached_epoch_end: bool


@dataclass(frozen=True)
class ApprovalCheckpointIdentity:
    checkpoint_id: str
    attempt: TurnAttemptIdentity
    operation_id: str
    approval_id: str


@dataclass(frozen=True)
class ApprovalCheckpointDecision:
    action: Literal["wait", "publish", "discard"]


def _require_operation_membership_sequence(value: object) -> int:
    if not (
        type(value) is int
        and 1 <= value <= SQLITE_INT_MAX
    ):
        raise ProjectOperationError(
            OperationErrorCode.INVALID_OPERATION_ARGUMENT
        )
    return value


def _require_operation_recovery_cursor(
    value: object,
) -> OperationRecoveryCursor:
    if type(value) is not OperationRecoveryCursor:
        raise ProjectOperationError(
            OperationErrorCode.INVALID_OPERATION_ARGUMENT
        )
    _require_operation_membership_sequence(
        value.recovery_membership_sequence
    )
    if not all(
        type(item) is str and bool(item)
        for item in (
            value.project_id,
            value.operation_id,
            value.turn_id,
        )
    ):
        raise ProjectOperationError(
            OperationErrorCode.INVALID_OPERATION_ARGUMENT
        )
    return value


class OperationReadbackPort(Protocol):
    def read_operation(
        self,
        request: OperationReadbackRequest,
    ) -> OperationReadbackResult: ...


class ApprovalCheckpointReadPort(Protocol):
    def publication_state(
        self,
        checkpoint: ApprovalCheckpointIdentity,
    ) -> Literal[
        "published", "waiting", "permanent_conflict"
    ]: ...


class _FixedOperationReadback:
    def __init__(self, result: OperationReadbackResult) -> None:
        self._result = result

    def read_operation(
        self,
        request: OperationReadbackRequest,
    ) -> OperationReadbackResult:
        return self._result


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_policy_value(value: object) -> object:
    value_type = type(value)
    if value is None or value_type in {str, int, float, bool}:
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        if not all(type(key) is str for key in value):
            raise TypeError("policy authority keys must be strings")
        return {
            key: _canonical_policy_value(value[key])
            for key in sorted(value)
        }
    if value_type in {tuple, list}:
        return [_canonical_policy_value(item) for item in value]
    if value_type in {set, frozenset}:
        return sorted(_canonical_policy_value(item) for item in value)
    if is_dataclass(value):
        return {
            field.name: _canonical_policy_value(
                getattr(value, field.name)
            )
            for field in fields(value)
        }
    raise TypeError("unsupported policy authority value")


def _canonical_policy_authority_json(value: object) -> str:
    converted = _canonical_policy_value(value)
    if type(converted) is not dict:
        raise TypeError("policy authority must be a dataclass object")
    return canonical_json_object(converted)


class ProjectOperationGuard:
    """The sole normal mutator for Task-6 operation authority."""

    def __init__(self, runtime: ProjectRuntime) -> None:
        if type(runtime) is not ProjectRuntime:
            raise TypeError("runtime must be a ProjectRuntime")
        self._runtime = runtime
        self._conn: sqlite3.Connection = runtime._conn

    @staticmethod
    def _c14_types() -> tuple[type[Any], type[Any], type[Any]]:
        from gateway.project_runtime_worker import (
            BoundProjectOperationAuthority,
            CertifiedProjectOperationExecutionRequest,
            ProjectPolicyDecisionCarrier,
        )

        return (
            BoundProjectOperationAuthority,
            ProjectPolicyDecisionCarrier,
            CertifiedProjectOperationExecutionRequest,
        )

    @staticmethod
    def _attempt_matches_claim(
        attempt: object,
        claim: TurnClaim,
    ) -> bool:
        return (
            type(attempt) is TurnAttemptIdentity
            and attempt.project_id == claim.project_id
            and attempt.turn_id == claim.turn_id
            and attempt.sequence == claim.sequence
            and attempt.worker_id == claim.worker_id
            and attempt.attempt_id == claim.attempt_id
            and attempt.lease_generation == claim.lease_generation
            and attempt.fencing_token == claim.fencing_token
            and attempt.canonical_session_id
            == claim.canonical_session_id
            and attempt.lease_expires_at <= claim.lease_expires_at
        )

    @staticmethod
    def _authority_storage(
        intent: OperationIntent,
        authority: object,
        policy: PolicyDecision,
        policy_authority: object,
    ) -> tuple[str, str, str, str, str, str]:
        (
            authority_type,
            carrier_type,
            _,
        ) = ProjectOperationGuard._c14_types()
        if not (
            type(authority) is authority_type
            and type(policy_authority) is carrier_type
            and type(policy) is PolicyDecision
            and authority == policy_authority.operation_authority
            and intent == authority.intent
            and policy == policy_authority.decision
        ):
            raise PermissionError("operation policy authority mismatch")

        command = authority.command
        effect_scope = json.loads(authority.effect_scope_json)
        if type(effect_scope) is not dict:
            raise PermissionError("operation effect scope is not an object")
        expected_authority = {
            "command": {
                "name": command.name,
                "project_id": command.project_id,
                "revision": command.revision,
                "action_class": command.action_class,
                "targets": list(command.targets),
                "batch_id": command.batch_id,
                "batch_items": list(command.batch_items),
                "metadata": dict(command.metadata),
            },
            "intent": {
                "operation_id": intent.operation_id,
                "project_id": intent.project_id,
                "turn_id": intent.turn_id,
                "idempotency_key": intent.idempotency_key,
                "canonical_action": intent.canonical_action,
                "command_revision": intent.command_revision,
                "targets": list(intent.targets),
                "batch_items": list(intent.batch_items),
                "payload": dict(intent.payload),
                "readback_kind": intent.readback_kind,
                "remote_idempotency_supported": (
                    intent.remote_idempotency_supported
                ),
            },
            "policy_batch_id": authority.policy_batch_id,
            "capability_fingerprint": [
                intent.canonical_action,
                intent.command_revision,
                intent.readback_kind,
                intent.remote_idempotency_supported,
            ],
            "effect_scope": effect_scope,
        }
        expected_scope = canonical_json_object(effect_scope)
        expected_authority_json = canonical_json_object(
            expected_authority
        )
        if not (
            authority.effect_scope_json == expected_scope
            and authority.effect_scope_sha256
            == _sha256_text(expected_scope)
            and authority.authority_json == expected_authority_json
            and authority.authority_sha256
            == _sha256_text(expected_authority_json)
            and command.project_id == intent.project_id
            and command.name == intent.canonical_action
            and command.targets == intent.targets
            and command.batch_id == authority.policy_batch_id
            and command.batch_items == intent.batch_items
            and tuple(effect_scope.get("targets", ()))
            == intent.targets
            and tuple(effect_scope.get("batch_items", ()))
            == intent.batch_items
            and effect_scope.get(
                "payload_effects",
                effect_scope.get("payload"),
            )
            == dict(intent.payload)
            and not (
                set(effect_scope)
                - {
                    "targets",
                    "batch_items",
                    "payload_effects",
                    "payload",
                }
            )
        ):
            raise PermissionError("operation authority serialization drift")
        policy_json = _canonical_policy_authority_json(
            policy_authority
        )
        return (
            expected_authority_json,
            authority.authority_sha256,
            expected_scope,
            authority.effect_scope_sha256,
            policy_json,
            _sha256_text(policy_json),
        )

    def _current_policy_views(
        self,
        *,
        project_id: str,
        contract_id: str,
        origin: TurnOrigin,
    ) -> tuple[
        ProjectPolicyView,
        str,
        str,
        ContractPolicyView,
        ActorContext,
    ]:
        state = runtime_db.runtime_state_for_project(
            self._conn,
            project_id,
        )
        contract_row = self._conn.execute(
            """
            SELECT contract_id, revision, contract_json, status
            FROM project_contracts
            WHERE project_id = ? AND contract_id = ?
            """,
            (project_id, contract_id),
        ).fetchone()
        binding_rows = self._conn.execute(
            """
            SELECT binding_id, surface, external_binding_id, actor_id
            FROM project_surface_bindings
            WHERE project_id = ?
            ORDER BY binding_id
            """,
            (project_id,),
        ).fetchall()
        folder_rows = self._conn.execute(
            """
            SELECT path FROM project_folders
            WHERE project_id = ?
            ORDER BY is_primary DESC, path
            """,
            (project_id,),
        ).fetchall()
        if state is None or contract_row is None:
            raise PermissionError("project policy authority is unavailable")
        try:
            contract_payload = json.loads(
                contract_row["contract_json"]
            )
            allowed_action_classes = frozenset(
                contract_payload["allowed_action_classes"]
            )
            allowed_phases = frozenset(
                contract_payload["allowed_phases"]
            )
            approved_plan_ref = contract_payload.get(
                "approved_plan_ref"
            )
            contract = ContractPolicyView(
                contract_row["revision"],
                allowed_action_classes,
                allowed_phases,
                approved_plan_ref,
            )
            bindings = tuple(
                ProjectBindingView(
                    row["binding_id"],
                    row["surface"],
                    row["actor_id"],
                    project_id,
                )
                for row in binding_rows
                if row["actor_id"] is not None
            )
            project = ProjectPolicyView(
                project_id,
                state.lifecycle,
                state.current_phase,
                tuple(
                    row["path"].replace("\\", "/")
                    for row in folder_rows
                ),
                approved_plan_ref,
                bindings,
            )
            matching = next(
                row
                for row in binding_rows
                if row["binding_id"] == origin.binding_id
            )
            if not (
                matching["surface"] == origin.surface
                and matching["external_binding_id"]
                == origin.external_binding_id
                and matching["actor_id"] == origin.actor_id
            ):
                raise PermissionError("project owner binding drift")
            actor = ActorContext(
                matching["actor_id"],
                matching["surface"],
                matching["binding_id"],
                True,
            )
        except (
            KeyError,
            StopIteration,
            TypeError,
            ValueError,
        ) as exc:
            raise PermissionError(
                "project policy authority is malformed"
            ) from exc
        return (
            project,
            contract_row["status"],
            _sha256_text(contract_row["contract_json"]),
            contract,
            actor,
        )

    def _require_prepare_policy_authority(
        self,
        *,
        claim: TurnClaim,
        intent: OperationIntent,
        authority: object,
        policy: PolicyDecision,
        policy_authority: object,
        state: object,
        control: object,
    ) -> None:
        _, carrier_type, _ = self._c14_types()
        if type(policy_authority) is not carrier_type:
            raise PermissionError("invalid project policy authority")
        carrier = policy_authority
        if not self._attempt_matches_claim(
            carrier.execution_attempt,
            claim,
        ):
            raise PermissionError("project attempt authority drift")
        (
            project,
            contract_status,
            contract_digest,
            contract,
            actor,
        ) = self._current_policy_views(
            project_id=claim.project_id,
            contract_id=carrier.contract_id,
            origin=carrier.execution_origin,
        )
        command = authority.command
        fresh_decision = decide_project_policy(
            command,
            project,
            contract,
            actor,
        )
        checks = {
            "origin": (
                carrier.execution_origin.actor_id == actor.actor_id
            ),
            "project": carrier.project == project,
            "contract_status": (
                carrier.contract_status == contract_status == "active"
            ),
            "contract_digest": (
                carrier.contract_json_sha256 == contract_digest
            ),
            "contract": carrier.contract == contract,
            "actor": carrier.actor == actor,
            "control": (
                carrier.control_version
                == getattr(control, "control_version", None)
            ),
            "runtime": (
                carrier.runtime_version == getattr(state, "version", None)
            ),
            "decision": (
                carrier.decision == policy == fresh_decision
            ),
            "command_project": command.project_id == claim.project_id,
            "command_revision": command.revision == contract.revision,
            "action_class": (
                command.action_class in contract.allowed_action_classes
            ),
            "phase": (
                command.metadata == {"phase": project.current_phase}
            ),
            "intent": (
                intent.project_id == claim.project_id
                and intent.turn_id == claim.turn_id
            ),
        }
        failed = tuple(
            label for label, accepted in checks.items() if not accepted
        )
        if failed:
            raise PermissionError(
                "project policy authority drift: " + ",".join(failed)
            )

    @staticmethod
    def _claim_from_attempt(
        attempt: TurnAttemptIdentity,
    ) -> TurnClaim:
        if type(attempt) is not TurnAttemptIdentity:
            raise TypeError("execution attempt must be exact")
        return TurnClaim(
            turn_id=attempt.turn_id,
            project_id=attempt.project_id,
            sequence=attempt.sequence,
            worker_id=attempt.worker_id,
            attempt_id=attempt.attempt_id,
            lease_generation=attempt.lease_generation,
            fencing_token=attempt.fencing_token,
            lease_expires_at=attempt.lease_expires_at,
            canonical_session_id=attempt.canonical_session_id,
        )

    def _require_c14_record_authority(
        self,
        record: runtime_db.ProjectOperationRecord,
    ) -> tuple[
        Mapping[str, object],
        Mapping[str, object],
        Mapping[str, object],
    ]:
        certificate = (
            record.operation_authority_json,
            record.operation_authority_sha256,
            record.effect_scope_json,
            record.effect_scope_sha256,
            record.policy_authority_json,
            record.policy_authority_sha256,
        )
        if not all(type(value) is str and value for value in certificate):
            raise PermissionError("operation has no C14 policy authority")
        (
            authority_json,
            authority_sha256,
            effect_scope_json,
            effect_scope_sha256,
            policy_authority_json,
            policy_authority_sha256,
        ) = certificate
        try:
            authority = json.loads(authority_json)
            effect_scope = json.loads(effect_scope_json)
            policy_authority = json.loads(policy_authority_json)
            command = authority["command"]
            intent = authority["intent"]
            stored_targets = json.loads(record.targets_json)
            stored_batch_items = json.loads(record.batch_items_json)
            stored_payload = json.loads(record.payload_json)
            nested_authority = policy_authority[
                "operation_authority"
            ]
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise PermissionError(
                "malformed stored operation authority"
            ) from exc
        if not (
            type(authority) is dict
            and type(effect_scope) is dict
            and type(policy_authority) is dict
            and type(command) is dict
            and type(intent) is dict
            and type(nested_authority) is dict
            and canonical_json_object(authority) == authority_json
            and canonical_json_object(effect_scope) == effect_scope_json
            and canonical_json_object(policy_authority)
            == policy_authority_json
            and _sha256_text(authority_json) == authority_sha256
            and _sha256_text(effect_scope_json) == effect_scope_sha256
            and _sha256_text(policy_authority_json)
            == policy_authority_sha256
            and authority.get("effect_scope") == effect_scope
            and command.get("name") == record.canonical_action
            and command.get("project_id") == record.project_id
            and command.get("targets") == stored_targets
            and command.get("batch_items") == stored_batch_items
            and intent.get("operation_id") == record.operation_id
            and intent.get("project_id") == record.project_id
            and intent.get("turn_id") == record.turn_id
            and intent.get("idempotency_key")
            == record.idempotency_key
            and intent.get("canonical_action")
            == record.canonical_action
            and intent.get("command_revision")
            == record.command_revision
            and intent.get("targets") == stored_targets
            and intent.get("batch_items") == stored_batch_items
            and intent.get("payload") == stored_payload
            and intent.get("readback_kind") == record.readback_kind
            and intent.get("remote_idempotency_supported")
            is record.remote_idempotency_supported
            and effect_scope.get("targets") == stored_targets
            and effect_scope.get("batch_items") == stored_batch_items
            and effect_scope.get(
                "payload_effects",
                effect_scope.get("payload"),
            )
            == stored_payload
            and authority.get("capability_fingerprint")
            == [
                record.canonical_action,
                record.command_revision,
                record.readback_kind,
                record.remote_idempotency_supported,
            ]
            and nested_authority.get("authority_json")
            == authority_json
            and nested_authority.get("authority_sha256")
            == authority_sha256
            and nested_authority.get("effect_scope_json")
            == effect_scope_json
            and nested_authority.get("effect_scope_sha256")
            == effect_scope_sha256
            and nested_authority.get("command") == command
            and nested_authority.get("intent") == intent
        ):
            raise PermissionError("stored operation authority drift")
        return authority, effect_scope, policy_authority

    def _require_current_operation_policy(
        self,
        record: runtime_db.ProjectOperationRecord,
    ) -> None:
        authority, _, stored = self._require_c14_record_authority(
            record
        )
        try:
            origin_value = stored["execution_origin"]
            command_value = authority["command"]
            origin = TurnOrigin(
                origin_value["binding_id"],
                origin_value["surface"],
                origin_value["external_binding_id"],
                origin_value["actor_id"],
            )
            command = ProjectCommand(
                command_value["name"],
                command_value["project_id"],
                command_value["revision"],
                command_value["action_class"],
                tuple(command_value["targets"]),
                command_value["batch_id"],
                tuple(command_value["batch_items"]),
                command_value["metadata"],
            )
            (
                project,
                contract_status,
                contract_digest,
                contract,
                actor,
            ) = self._current_policy_views(
                project_id=record.project_id,
                contract_id=stored["contract_id"],
                origin=origin,
            )
            fresh_decision = decide_project_policy(
                command,
                project,
                contract,
                actor,
            )
        except (
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise PermissionError(
                "current operation policy authority is malformed"
            ) from exc
        if not (
            contract_status == stored.get("contract_status") == "active"
            and contract_digest
            == stored.get("contract_json_sha256")
            and _canonical_policy_value(project)
            == stored.get("project")
            and _canonical_policy_value(contract)
            == stored.get("contract")
            and _canonical_policy_value(actor)
            == stored.get("actor")
            and _canonical_policy_value(fresh_decision)
            == stored.get("decision")
            and command.action_class
            in contract.allowed_action_classes
            and command.metadata
            == {"phase": project.current_phase}
        ):
            raise PermissionError(
                "current project operation policy drift"
            )

    def _record_for_public_operation(
        self,
        operation: ProjectOperation,
    ) -> runtime_db.ProjectOperationRecord:
        if type(operation) is not ProjectOperation:
            raise TypeError("operation must be exact")
        try:
            record = runtime_db._project_operation_for_id(
                self._conn,
                project_id=operation.project_id,
                operation_id=operation.operation_id,
            )
        except (
            RuntimeError,
            runtime_db.LegacyOperationUnmanagedError,
        ) as exc:
            raise PermissionError(
                "operation certificate is unavailable"
            ) from exc
        if record is None or self._public_operation(record) != operation:
            raise PermissionError("public operation authority drift")
        return record

    def certified_execution_request(
        self,
        execution: TurnExecutionInput,
        operation: ProjectOperation,
    ) -> object:
        if type(execution) is not TurnExecutionInput:
            raise TypeError("execution must be exact")
        record = self._record_for_public_operation(operation)
        authority, _, _ = self._require_c14_record_authority(record)
        claim = self._claim_from_attempt(execution.attempt)
        now = self._runtime._now()
        self._runtime._require_live_operation_claim(claim, now=now)
        if not (
            operation.status == "approved"
            and record.status == "approved"
            and record.project_id == execution.attempt.project_id
            and record.turn_id == execution.attempt.turn_id
            and record.attempt_id == execution.attempt.attempt_id
            and record.lease_generation
            == execution.attempt.lease_generation
            and record.fencing_token == execution.attempt.fencing_token
            and type(record.readback_kind) is str
            and bool(record.readback_kind)
            and record.remote_idempotency_supported is True
        ):
            raise PermissionError(
                "operation is not certified for this execution"
            )
        payload = authority["intent"]["payload"]
        if type(payload) is not dict:
            raise PermissionError("operation payload is not canonical")
        _, _, request_type = self._c14_types()
        return request_type(
            operation=operation,
            attempt=execution.attempt,
            payload=_immutable_json_mapping(payload),
            approval_checkpoint_id=record.approval_checkpoint_id,
            operation_authority_json=record.operation_authority_json,
            operation_authority_sha256=(
                record.operation_authority_sha256
            ),
            effect_scope_json=record.effect_scope_json,
            effect_scope_sha256=record.effect_scope_sha256,
            policy_authority_sha256=record.policy_authority_sha256,
            remote_idempotency_supported=(
                record.remote_idempotency_supported
            ),
            capability_fingerprint=(
                record.canonical_action,
                record.command_revision,
                record.readback_kind,
                record.remote_idempotency_supported,
            ),
        )

    def operation_recovery_membership_upper_watermark(
        self,
    ) -> int | None:
        """Read the inclusive upper bound for one recovery scan epoch."""
        member = runtime_db._operation_recovery_membership_upper(
            self._conn
        )
        return (
            member.recovery_membership_sequence
            if member is not None
            else None
        )

    def scan_operation_recovery_members(
        self,
        *,
        after: OperationRecoveryCursor | None,
        through_membership_sequence: int,
        limit: int,
    ) -> OperationRecoveryMemberScanResult:
        """Read one bounded certified recovery page without acting."""
        if after is not None:
            after = _require_operation_recovery_cursor(after)
        through_membership_sequence = (
            _require_operation_membership_sequence(
                through_membership_sequence
            )
        )
        if not (
            type(limit) is int
            and 1 <= limit <= 100
        ):
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT
            )
        if (
            after is not None
            and after.recovery_membership_sequence
            > through_membership_sequence
        ):
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT
            )

        raw_members = (
            runtime_db._operation_recovery_membership_page(
                self._conn,
                after=(
                    (
                        after.recovery_membership_sequence,
                        after.project_id,
                        after.operation_id,
                        after.turn_id,
                    )
                    if after is not None
                    else None
                ),
                through_membership_sequence=(
                    through_membership_sequence
                ),
                limit=limit,
            )
        )
        members = tuple(
            OperationRecoveryMember(
                member.recovery_membership_sequence,
                member.project_id,
                member.operation_id,
                member.turn_id,
                member.status,
            )
            for member in raw_members
        )
        scanned_through = (
            OperationRecoveryCursor(
                raw_members[-1].recovery_membership_sequence,
                raw_members[-1].project_id,
                raw_members[-1].operation_id,
                raw_members[-1].turn_id,
            )
            if raw_members
            else after
        )
        reached_epoch_end = not raw_members
        if raw_members:
            reached_epoch_end = (
                raw_members[-1].recovery_membership_sequence
                == through_membership_sequence
                or not runtime_db._operation_recovery_membership_remaining(
                    self._conn,
                    after=(
                        raw_members[
                            -1
                        ].recovery_membership_sequence,
                        raw_members[-1].project_id,
                        raw_members[-1].operation_id,
                        raw_members[-1].turn_id,
                    ),
                    through_membership_sequence=(
                        through_membership_sequence
                    ),
                )
            )
        return OperationRecoveryMemberScanResult(
            members,
            scanned_through,
            reached_epoch_end,
        )

    def _block_approved_operation_recovery(
        self,
        candidate: runtime_db.ProjectOperationRecord,
        *,
        expected_checkpoint: ApprovalCheckpointIdentity | None,
        dispatcher_lease: DispatcherLease,
        blocked_reason: str,
    ) -> bool:
        """Persist one exact approved-operation recovery block."""
        now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            self._runtime._require_dispatcher_start_authority(
                dispatcher_lease,
                now,
            )
            state = runtime_db.runtime_state_for_project(
                self._conn,
                candidate.project_id,
            )
            if not (
                state is not None
                and state.lifecycle == "active"
                and state.transcript_pending_batch_id is None
                and state.transcript_dispatch_block_key is None
            ):
                return False
            try:
                operation = runtime_db._project_operation_for_id(
                    self._conn,
                    project_id=candidate.project_id,
                    operation_id=candidate.operation_id,
                )
            except runtime_db.LegacyOperationUnmanagedError:
                return False
            if not (
                operation is not None
                and operation == candidate
                and operation.status == "approved"
                and operation.approval_id is not None
                and type(blocked_reason) is str
                and bool(blocked_reason)
            ):
                return False
            try:
                checkpoint = (
                    self._checkpoint_event_identity(operation)
                    if operation.approval_checkpoint_id is not None
                    else None
                )
            except Exception:
                return False
            if checkpoint != expected_checkpoint:
                return False

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
            oldest = self._conn.execute(
                """
                SELECT turn_id FROM project_turns
                WHERE project_id = ?
                  AND status NOT IN ('succeeded', 'failed', 'cancelled')
                ORDER BY sequence, turn_id
                LIMIT 1
                """,
                (operation.project_id,),
            ).fetchone()
            approval = self._conn.execute(
                """
                SELECT status, consumed_at
                FROM project_approvals
                WHERE project_id = ? AND approval_id = ?
                  AND operation_id = ? AND turn_id = ?
                """,
                (
                    operation.project_id,
                    operation.approval_id,
                    operation.operation_id,
                    operation.turn_id,
                ),
            ).fetchone()
            live_claim = (
                turn is not None
                and control is not None
                and lease is not None
                and turn.status == "claimed"
                and turn.execution_state == "started"
                and lease.lease_id == operation.attempt_id
                and lease.worker_id == control.claim_worker_id
                and lease.lease_generation
                    == operation.lease_generation
                and lease.fencing_token == operation.fencing_token
                and lease.expires_at
                    == control.claim_lease_expires_at
                and lease.expires_at <= now
            )
            parked_recovery = (
                runtime_db._recovery_candidate_for_attempt(
                    self._conn,
                    project_id=operation.project_id,
                    turn_id=operation.turn_id,
                    attempt_id=operation.attempt_id,
                    lease_generation=operation.lease_generation,
                    fencing_token=operation.fencing_token,
                )
                if (
                    turn is not None
                    and turn.status == "reconciling"
                    and lease is None
                )
                else None
            )
            if not (
                turn is not None
                and control is not None
                and oldest is not None
                and oldest["turn_id"] == operation.turn_id
                and (live_claim or parked_recovery is not None)
                and turn.recovery_block_key is None
                and turn.attempt_id == operation.attempt_id
                and turn.lease_generation
                    == operation.lease_generation
                and turn.fencing_token == operation.fencing_token
                and control.control_state == "running"
                and control.attempt_id == operation.attempt_id
                and type(control.claim_worker_id) is str
                and bool(control.claim_worker_id)
                and type(control.claim_lease_expires_at) is int
                and control.claim_canonical_session_id
                    == state.conversation_tip_id
                and approval is not None
                and approval["status"] == "approved"
                and type(approval["consumed_at"]) is int
            ):
                return False
            if expected_checkpoint is not None:
                checkpoint_attempt = expected_checkpoint.attempt
                if not runtime_db._checkpoint_current_authority_relation(
                    checkpoint_attempt_id=(
                        checkpoint_attempt.attempt_id
                    ),
                    checkpoint_worker_id=checkpoint_attempt.worker_id,
                    checkpoint_canonical_session_id=(
                        checkpoint_attempt.canonical_session_id
                    ),
                    checkpoint_lease_generation=(
                        checkpoint_attempt.lease_generation
                    ),
                    checkpoint_fencing_token=(
                        checkpoint_attempt.fencing_token
                    ),
                    checkpoint_lease_expires_at=(
                        checkpoint_attempt.lease_expires_at
                    ),
                    current_attempt_id=operation.attempt_id,
                    current_worker_id=control.claim_worker_id,
                    current_canonical_session_id=(
                        control.claim_canonical_session_id
                    ),
                    current_lease_generation=(
                        operation.lease_generation
                    ),
                    current_fencing_token=operation.fencing_token,
                    current_lease_expires_at=(
                        control.claim_lease_expires_at
                    ),
                ):
                    return False

            try:
                runtime_db._decertify_project_operation(
                    self._conn,
                    operation,
                )
                blocked = self._conn.execute(
                    """
                    UPDATE project_operations
                    SET status = 'blocked', blocked_reason = ?,
                        readback_json = NULL, updated_at = ?
                    WHERE project_id = ? AND operation_id = ?
                      AND turn_id = ? AND status = 'approved'
                      AND guard_validated = 0
                      AND attempt_id = ?
                      AND lease_generation = ?
                      AND fencing_token = ?
                      AND approval_id = ?
                      AND approval_checkpoint_id IS ?
                    """,
                    (
                        blocked_reason,
                        now,
                        operation.project_id,
                        operation.operation_id,
                        operation.turn_id,
                        operation.attempt_id,
                        operation.lease_generation,
                        operation.fencing_token,
                        operation.approval_id,
                        operation.approval_checkpoint_id,
                    ),
                )
                if blocked.rowcount != 1:
                    raise RuntimeError(
                        "approved operation changed while blocking"
                    )
                recovery = parked_recovery
                if recovery is None:
                    assert lease is not None
                    recovery = (
                        runtime_db
                        ._park_live_runtime_turn_for_operation_block(
                        self._conn,
                        project_id=operation.project_id,
                        turn_id=operation.turn_id,
                        sequence=turn.sequence,
                        attempt_id=operation.attempt_id,
                        worker_id=lease.worker_id,
                        lease_generation=operation.lease_generation,
                        fencing_token=operation.fencing_token,
                        lease_expires_at=lease.expires_at,
                        canonical_session_id=(
                            control.claim_canonical_session_id
                        ),
                        control_version=control.control_version,
                        now=now,
                    )
                    )
                if recovery is None:
                    raise RuntimeError(
                        "approved operation recovery disappeared"
                    )
                block_key = runtime_db._recovery_block_key(
                    project_id=recovery.project_id,
                    turn_id=recovery.turn_id,
                    attempt_id=recovery.attempt_id,
                    lease_generation=recovery.lease_generation,
                    fencing_token=recovery.fencing_token,
                )
                updated_state = self._runtime._advance_state(
                    state,
                    now,
                )
                runtime_db._append_runtime_event(
                    self._conn,
                    event_id=block_key,
                    project_id=recovery.project_id,
                    kind="turn.recovery_blocked",
                    turn_id=recovery.turn_id,
                    payload_json=canonical_json_object(
                        {
                            "attempt_id": recovery.attempt_id,
                            "fencing_token": recovery.fencing_token,
                            "lease_generation": (
                                recovery.lease_generation
                            ),
                            "source_status": recovery.source_status,
                            "turn_id": recovery.turn_id,
                            "version": updated_state.version,
                        }
                    ),
                    created_at=now,
                )
                if not runtime_db._set_recovery_block_key(
                    self._conn,
                    candidate=recovery,
                    block_key=block_key,
                ):
                    raise RuntimeError(
                        "approved operation recovery block changed"
                    )
                runtime_db._certify_project_operation(
                    self._conn,
                    project_id=operation.project_id,
                    operation_id=operation.operation_id,
                )
            except Exception as exc:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=operation.project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation.operation_id,
                    current_version=state.version,
                ) from exc
            return True

    def recover_pending_operations(
        self,
        readback: OperationReadbackPort | object,
        approval_checkpoints: ApprovalCheckpointReadPort,
        *,
        worker_id: str,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease,
        max_claims: int,
        after: OperationRecoveryCursor | None,
        through_membership_sequence: int,
        limit: int,
    ) -> OperationRecoveryScanResult:
        """Classify one certified raw page and issue bounded starts."""
        direct_readback = (
            readback
            if callable(getattr(readback, "read_operation", None))
            else None
        )
        capability_getter = (
            getattr(readback, "get", None)
            if direct_readback is None
            else None
        )
        if not (
            (
                direct_readback is not None
                or callable(capability_getter)
            )
            and callable(
                getattr(
                    approval_checkpoints,
                    "publication_state",
                    None,
                )
            )
            and type(worker_id) is str
            and worker_id
            and type(lease_seconds) is int
            and lease_seconds > 0
            and type(limit) is int
            and 1 <= limit <= 100
            and type(max_claims) is int
            and 0 <= max_claims <= limit
            and not self._conn.in_transaction
        ):
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT
            )
        dispatcher_lease = _require_dispatcher_lease(
            dispatcher_lease
        )
        if after is not None:
            after = _require_operation_recovery_cursor(after)
        through_membership_sequence = (
            _require_operation_membership_sequence(
                through_membership_sequence
            )
        )
        if (
            after is not None
            and after.recovery_membership_sequence
            > through_membership_sequence
        ):
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT
            )

        page = self.scan_operation_recovery_members(
            after=after,
            through_membership_sequence=(
                through_membership_sequence
            ),
            limit=limit,
        )
        starts: list[WorkerStart] = []
        for member in page.members:
            if member.status == "approved":
                try:
                    candidate = runtime_db._project_operation_for_id(
                        self._conn,
                        project_id=member.project_id,
                        operation_id=member.operation_id,
                    )
                except runtime_db.LegacyOperationUnmanagedError:
                    continue
            else:
                candidate = runtime_db._operation_pending_for_turn(
                    self._conn,
                    project_id=member.project_id,
                    turn_id=member.turn_id,
                )
            if not (
                candidate is not None
                and candidate.project_id == member.project_id
                and candidate.operation_id == member.operation_id
                and candidate.turn_id == member.turn_id
                and candidate.status == member.status
                and candidate.recovery_membership_sequence
                == member.recovery_membership_sequence
            ):
                continue

            if candidate.status != "approved":
                recovery = (
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
                if recovery is None:
                    continue
                parked_receipt = (
                    self._receipt_from_record(candidate)
                    if candidate.receipt_id is not None
                    else None
                )
                request = self._readback_request(
                    candidate,
                    parked_receipt,
                )
                recovery_readback = direct_readback
                if recovery_readback is None:
                    assert callable(capability_getter)
                    fingerprint = (
                        candidate.canonical_action,
                        candidate.command_revision,
                        candidate.readback_kind,
                        candidate.remote_idempotency_supported,
                    )
                    adapter = capability_getter(fingerprint, None)
                    if adapter is None:
                        continue
                    declared = getattr(adapter, "fingerprint", None)
                    if not (
                        (
                            declared is None
                            or (
                                isinstance(declared, tuple)
                                and tuple(declared) == fingerprint
                            )
                        )
                        and callable(
                            getattr(adapter, "read_operation", None)
                        )
                    ):
                        continue
                    recovery_readback = adapter
                try:
                    result = recovery_readback.read_operation(request)
                except Exception:
                    continue
                claim = TurnClaim(
                    turn_id=recovery.turn_id,
                    project_id=recovery.project_id,
                    sequence=recovery.sequence,
                    worker_id=recovery.worker_id,
                    attempt_id=recovery.attempt_id,
                    lease_generation=(
                        recovery.lease_generation
                    ),
                    fencing_token=recovery.fencing_token,
                    lease_expires_at=recovery.lease_expires_at,
                    canonical_session_id=(
                        recovery.canonical_session_id
                    ),
                )
                try:
                    operation = self.reconcile(
                        claim,
                        candidate.operation_id,
                        _FixedOperationReadback(result),
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
                if operation.status != "approved":
                    continue
                candidate = (
                    runtime_db._operation_pending_for_turn(
                        self._conn,
                        project_id=member.project_id,
                        turn_id=member.turn_id,
                    )
                )
                if not (
                    candidate is not None
                    and candidate.operation_id
                    == member.operation_id
                    and candidate.status == "approved"
                ):
                    continue

            expected_checkpoint = None
            checkpoint_outcome = None
            if candidate.approval_checkpoint_id is not None:
                try:
                    expected_checkpoint = (
                        self._checkpoint_event_identity(
                            candidate,
                            certify_current=True,
                        )
                    )
                except Exception:
                    continue
                try:
                    checkpoint_outcome = (
                        approval_checkpoints.publication_state(
                            expected_checkpoint
                        )
                    )
                except Exception:
                    continue
                if checkpoint_outcome == "waiting":
                    continue
                if checkpoint_outcome != "published":
                    try:
                        self._block_approved_operation_recovery(
                            candidate,
                            expected_checkpoint=expected_checkpoint,
                            dispatcher_lease=dispatcher_lease,
                            blocked_reason=(
                                "approval_checkpoint_conflict"
                            ),
                        )
                    except ProjectOperationError as exc:
                        if (
                            exc.code
                            is OperationErrorCode.OPERATION_STATE_CONFLICT
                        ):
                            continue
                        raise
                    except ProjectRuntimeError as exc:
                        if (
                            exc.code
                            is RuntimeErrorCode.STALE_DISPATCHER_LEASE
                        ):
                            break
                        raise
                    continue
            elif candidate.approval_id is not None:
                try:
                    self._block_approved_operation_recovery(
                        candidate,
                        expected_checkpoint=None,
                        dispatcher_lease=dispatcher_lease,
                        blocked_reason=(
                            "approval_checkpoint_missing"
                        ),
                    )
                except ProjectOperationError as exc:
                    if (
                        exc.code
                        is OperationErrorCode.OPERATION_STATE_CONFLICT
                    ):
                        continue
                    raise
                except ProjectRuntimeError as exc:
                    if (
                        exc.code
                        is RuntimeErrorCode.STALE_DISPATCHER_LEASE
                    ):
                        break
                    raise
                continue

            if capability_getter is not None:
                try:
                    self._require_current_operation_policy(candidate)
                except PermissionError:
                    try:
                        self._block_approved_operation_recovery(
                            candidate,
                            expected_checkpoint=expected_checkpoint,
                            dispatcher_lease=dispatcher_lease,
                            blocked_reason="operation_policy_stale",
                        )
                    except ProjectOperationError as exc:
                        if (
                            exc.code
                            is OperationErrorCode.OPERATION_STATE_CONFLICT
                        ):
                            continue
                        raise
                    except ProjectRuntimeError as exc:
                        if (
                            exc.code
                            is RuntimeErrorCode.STALE_DISPATCHER_LEASE
                        ):
                            break
                        raise
                    continue

                fingerprint = (
                    candidate.canonical_action,
                    candidate.command_revision,
                    candidate.readback_kind,
                    candidate.remote_idempotency_supported,
                )
                adapter = capability_getter(fingerprint, None)
                declared = (
                    getattr(adapter, "fingerprint", None)
                    if adapter is not None
                    else None
                )
                capability_available = (
                    adapter is not None
                    and (
                        declared is None
                        or (
                            isinstance(declared, tuple)
                            and tuple(declared) == fingerprint
                        )
                    )
                    and callable(getattr(adapter, "execute", None))
                    and (
                        callable(
                            getattr(adapter, "read_operation", None)
                        )
                        or callable(getattr(adapter, "readback", None))
                    )
                )
                if not capability_available:
                    try:
                        self._block_approved_operation_recovery(
                            candidate,
                            expected_checkpoint=expected_checkpoint,
                            dispatcher_lease=dispatcher_lease,
                            blocked_reason=(
                                "operation_executor_unavailable"
                            ),
                        )
                    except ProjectOperationError as exc:
                        if (
                            exc.code
                            is OperationErrorCode.OPERATION_STATE_CONFLICT
                        ):
                            continue
                        raise
                    except ProjectRuntimeError as exc:
                        if (
                            exc.code
                            is RuntimeErrorCode.STALE_DISPATCHER_LEASE
                        ):
                            break
                        raise
                    continue

            if len(starts) >= max_claims:
                continue
            try:
                if expected_checkpoint is not None:
                    rehydrated = (
                        self._rehydrate_approved_operation_start(
                            candidate.project_id,
                            candidate.operation_id,
                            worker_id=worker_id,
                            lease_seconds=lease_seconds,
                            dispatcher_lease=dispatcher_lease,
                            expected_checkpoint=expected_checkpoint,
                        )
                    )
                    start = (
                        WorkerStart(
                            "approved_operation",
                            rehydrated[0],
                            rehydrated[1],
                            dispatcher_lease,
                        )
                        if rehydrated is not None
                        else None
                    )
                else:
                    start = self.rehydrate_approved_operation_for_dispatcher(
                        candidate.project_id,
                        candidate.operation_id,
                        worker_id=worker_id,
                        lease_seconds=lease_seconds,
                        dispatcher_lease=dispatcher_lease,
                    )
            except ProjectOperationError as exc:
                if (
                    exc.code
                    is OperationErrorCode.OPERATION_STATE_CONFLICT
                ):
                    continue
                raise
            except ProjectRuntimeError as exc:
                if (
                    exc.code
                    is RuntimeErrorCode.STALE_DISPATCHER_LEASE
                ):
                    break
                raise
            if start is not None:
                starts.append(start)

        return OperationRecoveryScanResult(
            tuple(starts),
            page.scanned_through,
            page.reached_epoch_end,
        )

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
        expected_class = approval_class_for_action(
            intent.canonical_action
        )
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
            if expected_class is not None:
                raise ProjectOperationGuard._error(
                    OperationErrorCode.OPERATION_POLICY_DENIED,
                    intent=intent,
                )
            if approval is not None or policy.approval_class is not None:
                raise ValueError("allow cannot carry approval")
            return policy.decision, None
        if not (
            expected_class is not None
            and type(approval) is OperationApprovalSpec
            and type(policy.approval_class) is str
            and policy.approval_class == expected_class
            and type(approval.approval_id) is str
            and approval.approval_id
            and type(approval.approval_class) is str
            and approval.approval_class == policy.approval_class
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
        approval_checkpoint_id: str | None,
        operation_authority_json: str | None = None,
        operation_authority_sha256: str | None = None,
        effect_scope_json: str | None = None,
        effect_scope_sha256: str | None = None,
        policy_authority_json: str | None = None,
        policy_authority_sha256: str | None = None,
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
            or existing.approval_checkpoint_id != approval_checkpoint_id
            or existing.operation_authority_json
            != operation_authority_json
            or existing.operation_authority_sha256
            != operation_authority_sha256
            or existing.effect_scope_json != effect_scope_json
            or existing.effect_scope_sha256 != effect_scope_sha256
            or existing.policy_authority_json != policy_authority_json
            or existing.policy_authority_sha256
            != policy_authority_sha256
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
        operation = ProjectOperation(
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
            policy_authority_sha256=record.policy_authority_sha256,
        )
        object.__setattr__(
            operation,
            "_approval_checkpoint_id",
            record.approval_checkpoint_id,
        )
        return operation

    def _checkpoint_event_identity(
        self,
        operation: runtime_db.ProjectOperationRecord,
        *,
        certify_current: bool = False,
    ) -> ApprovalCheckpointIdentity:
        approval = runtime_db._operation_approval_certification_row(
            self._conn,
            operation,
        )
        authority = runtime_db._checkpoint_intent_authority(
            self._conn,
            operation,
            approval,
        )
        if authority is None:
            raise ValueError("missing checkpoint authority")
        if certify_current:
            runtime_db._certify_checkpoint_current_authority(
                self._conn,
                operation,
                authority,
            )
        return ApprovalCheckpointIdentity(
            authority.checkpoint_id,
            TurnAttemptIdentity(
                authority.project_id,
                authority.turn_id,
                authority.sequence,
                authority.worker_id,
                authority.attempt_id,
                authority.lease_generation,
                authority.fencing_token,
                authority.canonical_session_id,
                authority.lease_expires_at,
            ),
            authority.operation_id,
            authority.approval_id,
        )

    @staticmethod
    def _valid_checkpoint_identity(
        checkpoint: object,
    ) -> bool:
        if not (
            type(checkpoint) is ApprovalCheckpointIdentity
            and type(checkpoint.attempt) is TurnAttemptIdentity
            and all(
                type(value) is str and bool(value)
                for value in (
                    checkpoint.checkpoint_id,
                    checkpoint.operation_id,
                    checkpoint.approval_id,
                    checkpoint.attempt.project_id,
                    checkpoint.attempt.turn_id,
                    checkpoint.attempt.worker_id,
                    checkpoint.attempt.attempt_id,
                    checkpoint.attempt.canonical_session_id,
                )
            )
            and all(
                type(value) is int
                and 1 <= value <= SQLITE_INT_MAX
                for value in (
                    checkpoint.attempt.sequence,
                    checkpoint.attempt.lease_generation,
                    checkpoint.attempt.fencing_token,
                )
            )
            and type(checkpoint.attempt.lease_expires_at) is int
            and 0
            <= checkpoint.attempt.lease_expires_at
            <= SQLITE_INT_MAX
        ):
            return False
        try:
            parsed = uuid.UUID(checkpoint.checkpoint_id)
        except (AttributeError, TypeError, ValueError):
            return False
        return (
            parsed.version == 4
            and parsed.variant == uuid.RFC_4122
            and str(parsed) == checkpoint.checkpoint_id
        )

    def _resolve_approval_checkpoint_authority(
        self,
        checkpoint: ApprovalCheckpointIdentity,
    ) -> tuple[
        ApprovalCheckpointDecision,
        Literal[
            "stop_requested",
            "cancelled",
            "superseded_attempt",
            "superseded_terminal",
            "recovery_blocked",
        ]
        | None,
    ]:
        try:
            if not self._valid_checkpoint_identity(checkpoint):
                raise ValueError("invalid checkpoint identity")
            if self._conn.in_transaction:
                raise ValueError(
                    "checkpoint resolution requires an idle connection"
                )
            self._conn.execute("BEGIN")
            try:
                row = self._conn.execute(
                    """
                    SELECT * FROM project_operations
                    WHERE project_id = ? AND operation_id = ?
                    """,
                    (
                        checkpoint.attempt.project_id,
                        checkpoint.operation_id,
                    ),
                ).fetchone()
                if row is None:
                    bound_checkpoint = self._conn.execute(
                        """
                        SELECT 1 FROM project_operations
                        WHERE project_id = ?
                          AND approval_checkpoint_id = ?
                        """,
                        (
                            checkpoint.attempt.project_id,
                            checkpoint.checkpoint_id,
                        ),
                    ).fetchone()
                    if bound_checkpoint is not None:
                        raise ValueError(
                            "checkpoint operation mismatch"
                        )
                    resolved, discard_authority = (
                        self._runtime
                        ._resolve_prepared_approval_checkpoint_authority_in_snapshot(
                            checkpoint.attempt,
                            operation_id=checkpoint.operation_id,
                            approval_id=checkpoint.approval_id,
                        )
                    )
                    decision = (
                        ApprovalCheckpointDecision(resolved.action),
                        discard_authority,
                    )
                else:
                    operation = runtime_db.project_operation_from_row(
                        row
                    )
                    stored = self._checkpoint_event_identity(operation)
                    if not (
                        stored.checkpoint_id
                        == checkpoint.checkpoint_id
                        and stored.operation_id
                        == checkpoint.operation_id
                        and stored.approval_id
                        == checkpoint.approval_id
                        and stored.attempt.project_id
                        == checkpoint.attempt.project_id
                        and stored.attempt.turn_id
                        == checkpoint.attempt.turn_id
                        and stored.attempt.sequence
                        == checkpoint.attempt.sequence
                        and stored.attempt.worker_id
                        == checkpoint.attempt.worker_id
                        and stored.attempt.attempt_id
                        == checkpoint.attempt.attempt_id
                        and stored.attempt.lease_generation
                        == checkpoint.attempt.lease_generation
                        and stored.attempt.fencing_token
                        == checkpoint.attempt.fencing_token
                        and stored.attempt.canonical_session_id
                        == checkpoint.attempt.canonical_session_id
                        and checkpoint.attempt.lease_expires_at
                        <= stored.attempt.lease_expires_at
                    ):
                        raise ValueError(
                            "checkpoint identity mismatch"
                        )
                    decision = (
                        ApprovalCheckpointDecision("publish"),
                        None,
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
        except Exception as exc:
            raise ProjectRuntimeError(
                RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            ) from exc
        return decision

    def resolve_approval_checkpoint(
        self,
        checkpoint: ApprovalCheckpointIdentity,
    ) -> ApprovalCheckpointDecision:
        decision, _ = self._resolve_approval_checkpoint_authority(
            checkpoint
        )
        return decision

    def prepare(
        self,
        claim: TurnClaim,
        intent: OperationIntent,
        *,
        policy: PolicyDecision,
        approval: OperationApprovalSpec | None,
        approval_checkpoint_id: str | None = None,
        authority: object | None = None,
        policy_authority: object | None = None,
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
        has_policy_authority = (
            authority is not None or policy_authority is not None
        )
        if has_policy_authority:
            try:
                authority_storage = self._authority_storage(
                    intent,
                    authority,
                    policy,
                    policy_authority,
                )
            except (
                ProjectOperationError,
                PermissionError,
                TypeError,
                ValueError,
            ):
                raise
            except Exception as exc:
                raise PermissionError(
                    "invalid project operation authority"
                ) from exc
        else:
            authority_storage = (None,) * 6
        (
            operation_authority_json,
            operation_authority_sha256,
            effect_scope_json,
            effect_scope_sha256,
            policy_authority_json,
            policy_authority_sha256,
        ) = authority_storage
        capability_supported = (
            intent.remote_idempotency_supported
            and intent.readback_kind is not None
        )
        approval_id = (
            approval_spec.approval_id
            if decision is Decision.REQUIRE_APPROVAL
            and capability_supported
            else None
        )
        if approval_checkpoint_id is not None:
            try:
                parsed_checkpoint = uuid.UUID(approval_checkpoint_id)
                valid_checkpoint = (
                    str(parsed_checkpoint) == approval_checkpoint_id
                    and parsed_checkpoint.version == 4
                )
            except (TypeError, ValueError, AttributeError):
                valid_checkpoint = False
            if not (
                valid_checkpoint
                and capability_supported
                and decision is Decision.REQUIRE_APPROVAL
                and approval_id is not None
            ):
                raise self._error(
                    OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                    intent=intent,
                )
            if self._conn.in_transaction:
                raise self._error(
                    OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                    intent=intent,
                )
        existing = self._existing_operation(
            claim=claim,
            intent=intent,
            targets_json=targets_json,
            batch_items_json=batch_items_json,
            payload_json=payload_json,
            approval_id=approval_id,
            approval_fingerprint_json=approval_fingerprint_json,
            remote_idempotency_supported=(
                intent.remote_idempotency_supported
            ),
            approval_checkpoint_id=approval_checkpoint_id,
            operation_authority_json=operation_authority_json,
            operation_authority_sha256=operation_authority_sha256,
            effect_scope_json=effect_scope_json,
            effect_scope_sha256=effect_scope_sha256,
            policy_authority_json=policy_authority_json,
            policy_authority_sha256=policy_authority_sha256,
        )
        if existing is not None:
            return self._public_operation(existing)

        intent_event_id = None
        transaction = (
            runtime_db.task7_outer_write_transaction(self._conn)
            if approval_checkpoint_id is not None
            else runtime_db.write_transaction(self._conn)
        )
        with transaction:
            if approval_checkpoint_id is not None:
                existing = self._existing_operation(
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
                    approval_checkpoint_id=(
                        approval_checkpoint_id
                    ),
                    operation_authority_json=(
                        operation_authority_json
                    ),
                    operation_authority_sha256=(
                        operation_authority_sha256
                    ),
                    effect_scope_json=effect_scope_json,
                    effect_scope_sha256=effect_scope_sha256,
                    policy_authority_json=policy_authority_json,
                    policy_authority_sha256=(
                        policy_authority_sha256
                    ),
                )
                if existing is not None:
                    return self._public_operation(existing)
            now = self._runtime._now()
            if approval_checkpoint_id is not None:
                runtime_db._bind_task7_outer_timestamp(
                    self._conn,
                    now,
                )
            if approval_checkpoint_id is not None:
                intent_event_id = self._runtime._id_factory(
                    "event"
                )
                if not (
                    type(intent_event_id) is str
                    and intent_event_id
                ):
                    raise self._error(
                        OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                        intent=intent,
                    )
            state, _, control, _ = (
                self._runtime._require_live_operation_claim(
                    claim, now=now
                )
            )
            if has_policy_authority:
                self._require_prepare_policy_authority(
                    claim=claim,
                    intent=intent,
                    authority=authority,
                    policy=policy,
                    policy_authority=policy_authority,
                    state=state,
                    control=control,
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
                operation_authority_json=(
                    operation_authority_json
                ),
                operation_authority_sha256=(
                    operation_authority_sha256
                ),
                effect_scope_json=effect_scope_json,
                effect_scope_sha256=effect_scope_sha256,
                policy_authority_json=policy_authority_json,
                policy_authority_sha256=policy_authority_sha256,
                now=now,
                approval_checkpoint_id=approval_checkpoint_id,
                intent_event_id=intent_event_id,
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
                    approval_checkpoint_id=approval_checkpoint_id,
                    operation_authority_json=(
                        operation_authority_json
                    ),
                    operation_authority_sha256=(
                        operation_authority_sha256
                    ),
                    effect_scope_json=effect_scope_json,
                    effect_scope_sha256=effect_scope_sha256,
                    policy_authority_json=policy_authority_json,
                    policy_authority_sha256=(
                        policy_authority_sha256
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
                try:
                    linked = runtime_db._link_project_operation_approval(
                        self._conn,
                        project_id=intent.project_id,
                        turn_id=intent.turn_id,
                        operation_id=intent.operation_id,
                        approval_id=approval_spec.approval_id,
                        attempt_id=claim.attempt_id,
                        lease_generation=claim.lease_generation,
                        fencing_token=claim.fencing_token,
                        now=now,
                    )
                except Exception as exc:
                    raise self._error(
                        OperationErrorCode.OPERATION_APPROVAL_CONFLICT,
                        intent=intent,
                    ) from exc
                if not linked:
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
            event_payload: dict[str, object] = {
                "operation_id": intent.operation_id,
                "status": event_status,
                "turn_id": intent.turn_id,
                "version": updated_state.version,
            }
            if approval_checkpoint_id is not None:
                event_payload.update(
                    {
                        "approval_checkpoint_id": approval_checkpoint_id,
                        "approval_id": approval_id,
                        "attempt": {
                            "project_id": claim.project_id,
                            "turn_id": claim.turn_id,
                            "sequence": claim.sequence,
                            "worker_id": claim.worker_id,
                            "attempt_id": claim.attempt_id,
                            "lease_generation": claim.lease_generation,
                            "fencing_token": claim.fencing_token,
                            "canonical_session_id": claim.canonical_session_id,
                            "lease_expires_at": claim.lease_expires_at,
                        },
                    }
                )
            try:
                self._runtime._event(
                    intent.project_id,
                    "operation.intent_recorded",
                    intent.turn_id,
                    event_payload,
                    now,
                    event_id=intent_event_id,
                )
            except Exception as exc:
                raise self._operation_state_conflict(
                    claim, intent.operation_id, updated_state.version
                ) from exc
            if approval_checkpoint_id is not None:
                try:
                    staging = self._conn.execute(
                        """
                        SELECT * FROM project_operations
                        WHERE project_id = ? AND operation_id = ?
                        """,
                        (intent.project_id, intent.operation_id),
                    ).fetchone()
                    if staging is None:
                        raise RuntimeError("checkpoint operation disappeared")
                    staged_operation = runtime_db._project_operation_from_row(
                        staging, expected_guard_validated=0
                    )
                    staged_identity = self._checkpoint_event_identity(
                        staged_operation
                    )
                    if staged_identity.attempt != TurnAttemptIdentity(
                        claim.project_id, claim.turn_id, claim.sequence,
                        claim.worker_id, claim.attempt_id,
                        claim.lease_generation, claim.fencing_token,
                        claim.canonical_session_id, claim.lease_expires_at,
                    ):
                        raise RuntimeError("checkpoint attempt mismatch")
                except Exception as exc:
                    raise self._operation_state_conflict(
                        claim, intent.operation_id, updated_state.version
                    ) from exc
            try:
                stored = runtime_db._certify_project_operation(
                    self._conn,
                    project_id=intent.project_id,
                    operation_id=intent.operation_id,
                )
            except Exception as exc:
                raise self._operation_state_conflict(
                    claim, intent.operation_id, updated_state.version
                ) from exc
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

    def _decertify_for_approval(
        self,
        *,
        row: sqlite3.Row,
        operation: runtime_db.ProjectOperationRecord,
    ) -> None:
        try:
            runtime_db._decertify_project_operation(
                self._conn, operation
            )
        except Exception as exc:
            raise self._approval_conflict(
                row=row, operation=operation
            ) from exc

    def _certify_for_approval(
        self,
        *,
        row: sqlite3.Row,
        operation: runtime_db.ProjectOperationRecord,
    ) -> runtime_db.ProjectOperationRecord:
        try:
            return runtime_db._certify_project_operation(
                self._conn,
                project_id=operation.project_id,
                operation_id=operation.operation_id,
            )
        except Exception as exc:
            raise self._approval_conflict(
                row=row, operation=operation
            ) from exc

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
        stored = self._certify_for_approval(
            row=row,
            operation=operation,
        )
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
                self._decertify_for_approval(
                    row=row, operation=operation
                )
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
                self._decertify_for_approval(
                    row=row, operation=operation
                )
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
                self._decertify_for_approval(
                    row=row, operation=operation
                )
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
            self._decertify_for_approval(
                row=row, operation=operation
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
            stored = self._certify_for_approval(
                row=row,
                operation=operation,
            )
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
        result = self._rehydrate_approved_operation_start(
            project_id,
            operation_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            dispatcher_lease=None,
        )
        return result[0] if result is not None else None

    def rehydrate_approved_operation_for_dispatcher(
        self,
        project_id: str,
        operation_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease,
    ) -> WorkerStart | None:
        """Issue one approved-operation start under exact Core authority."""
        dispatcher_lease = _require_dispatcher_lease(
            dispatcher_lease
        )
        result = self._rehydrate_approved_operation_start(
            project_id,
            operation_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            dispatcher_lease=dispatcher_lease,
        )
        if result is None:
            return None
        claim, operation = result
        return WorkerStart(
            "approved_operation",
            claim,
            operation,
            dispatcher_lease,
        )

    def _rehydrate_approved_operation_start(
        self,
        project_id: str,
        operation_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease | None,
        expected_checkpoint: ApprovalCheckpointIdentity | None = None,
    ) -> tuple[TurnClaim, ProjectOperation] | None:
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
        now = (
            self._runtime._now()
            if dispatcher_lease is None
            else None
        )
        if (
            now is not None
            and now > SQLITE_INT_MAX - lease_seconds
        ):
            raise ProjectOperationError(
                OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                project_id=project_id,
                operation_id=operation_id,
            )
        with runtime_db.write_transaction(self._conn):
            if dispatcher_lease is not None:
                now = self._runtime._now()
                if now > SQLITE_INT_MAX - lease_seconds:
                    raise ProjectOperationError(
                        OperationErrorCode.INVALID_OPERATION_ARGUMENT,
                        project_id=project_id,
                        operation_id=operation_id,
                    )
                self._runtime._require_dispatcher_start_authority(
                    dispatcher_lease,
                    now,
                )
            assert now is not None
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
            if dispatcher_lease is not None and (
                state.transcript_pending_batch_id is not None
                or state.transcript_dispatch_block_key is not None
            ):
                return None
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
            except RuntimeError as exc:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    operation_id=operation_id,
                    current_version=state.version,
                ) from exc
            if operation is None:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_NOT_FOUND,
                    project_id=project_id,
                    operation_id=operation_id,
                )
            checkpoint_identity = None
            if operation.approval_checkpoint_id is not None:
                try:
                    checkpoint_identity = (
                        self._checkpoint_event_identity(operation)
                    )
                except Exception as exc:
                    raise ProjectOperationError(
                        OperationErrorCode.OPERATION_STATE_CONFLICT,
                        project_id=project_id,
                        turn_id=operation.turn_id,
                        operation_id=operation_id,
                        current_version=state.version,
                    ) from exc
            if expected_checkpoint is not None:
                if (
                    not self._valid_checkpoint_identity(
                        expected_checkpoint
                    )
                    or checkpoint_identity != expected_checkpoint
                ):
                    return None
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
                "awaiting_approval", "claimed", "reconciling"
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
            if checkpoint_identity is not None:
                checkpoint_attempt = checkpoint_identity.attempt
                current_pair = (
                    current_pair
                    and runtime_db._checkpoint_current_authority_relation(
                        checkpoint_attempt_id=(
                            checkpoint_attempt.attempt_id
                        ),
                        checkpoint_worker_id=checkpoint_attempt.worker_id,
                        checkpoint_canonical_session_id=(
                            checkpoint_attempt.canonical_session_id
                        ),
                        checkpoint_lease_generation=(
                            checkpoint_attempt.lease_generation
                        ),
                        checkpoint_fencing_token=(
                            checkpoint_attempt.fencing_token
                        ),
                        checkpoint_lease_expires_at=(
                            checkpoint_attempt.lease_expires_at
                        ),
                        current_attempt_id=operation.attempt_id,
                        current_worker_id=control.claim_worker_id,
                        current_canonical_session_id=(
                            control.claim_canonical_session_id
                        ),
                        current_lease_generation=(
                            operation.lease_generation
                        ),
                        current_fencing_token=operation.fencing_token,
                        current_lease_expires_at=(
                            control.claim_lease_expires_at
                        ),
                    )
                )
            if not current_pair:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation_id,
                    current_version=state.version,
                )
            if (
                dispatcher_lease is not None
                and expected_checkpoint is None
                and checkpoint_identity is not None
            ):
                return None
            if turn.status in {"awaiting_approval", "claimed"}:
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
            if dispatcher_lease is not None:
                current_state = runtime_db.runtime_state_for_project(
                    self._conn,
                    project_id,
                )
                if not (
                    current_state is not None
                    and current_state.lifecycle == "active"
                    and current_state.version == state.version
                    and current_state.conversation_tip_id
                        == state.conversation_tip_id
                    and current_state.transcript_pending_batch_id
                        is None
                    and current_state.transcript_dispatch_block_key
                        is None
                ):
                    return None
            generation = turn.lease_generation + 1
            fence = turn.fencing_token + 1
            expires_at = now + lease_seconds
            try:
                runtime_db._decertify_project_operation(
                    self._conn, operation
                )
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
                        require_task7_terminal_gate_clear=(
                            dispatcher_lease is not None
                        ),
                    )
                )
            except Exception as exc:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation_id,
                    current_version=state.version,
                ) from exc
            updated = self._runtime._advance_state(state, now)
            rehydrated_event_id = self._runtime._id_factory("event")
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
                event_id=rehydrated_event_id,
            )
            claimed_event_id = self._runtime._id_factory("event")
            if claimed_event_id == rehydrated_event_id:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation_id,
                    current_version=updated.version,
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
                event_id=claimed_event_id,
            )
            try:
                certified = runtime_db._certify_project_operation(
                    self._conn,
                    project_id=project_id,
                    operation_id=operation_id,
                )
            except Exception as exc:
                raise ProjectOperationError(
                    OperationErrorCode.OPERATION_STATE_CONFLICT,
                    project_id=project_id,
                    turn_id=operation.turn_id,
                    operation_id=operation_id,
                    current_version=updated.version,
                ) from exc
            claim = TurnClaim(
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
            return claim, self._public_operation(certified)

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

    def _decertify_for_claim(
        self,
        operation: runtime_db.ProjectOperationRecord,
        claim: TurnClaim,
        current_version: int,
    ) -> None:
        try:
            runtime_db._decertify_project_operation(
                self._conn, operation
            )
        except Exception as exc:
            raise self._operation_state_conflict(
                claim, operation.operation_id, current_version
            ) from exc

    def _certify_for_claim(
        self,
        claim: TurnClaim,
        operation_id: str,
        current_version: int,
    ) -> runtime_db.ProjectOperationRecord:
        try:
            return runtime_db._certify_project_operation(
                self._conn,
                project_id=claim.project_id,
                operation_id=operation_id,
            )
        except Exception as exc:
            raise self._operation_state_conflict(
                claim, operation_id, current_version
            ) from exc

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
        *,
        approval_checkpoints: ApprovalCheckpointReadPort | None = None,
    ) -> ProjectOperation:
        self._validate_operation_id(operation_id)
        operation = self._operation_for_claim(claim, operation_id)
        if operation.status == "effect_started":
            now = self._runtime._now()
            with runtime_db.write_transaction(self._conn):
                operation = self._operation_for_claim(
                    claim, operation_id
                )
                state, _, _, _ = (
                    self._runtime._require_live_operation_claim(
                        claim, now=now
                    )
                )
                if operation.status != "effect_started":
                    raise self._operation_state_conflict(
                        claim, operation_id, state.version
                    )
                return self._public_operation(operation)
        expected_checkpoint = None
        if operation.approval_checkpoint_id is not None:
            try:
                expected_checkpoint = self._checkpoint_event_identity(
                    operation,
                    certify_current=True,
                )
            except Exception as exc:
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    claim.project_id,
                )
                raise self._operation_state_conflict(
                    claim,
                    operation_id,
                    state.version if state is not None else 0,
                ) from exc
            publication_state = getattr(
                approval_checkpoints,
                "publication_state",
                None,
            )
            if not callable(publication_state):
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    claim.project_id,
                )
                raise self._operation_state_conflict(
                    claim,
                    operation_id,
                    state.version if state is not None else 0,
                )
            try:
                checkpoint_state = publication_state(
                    expected_checkpoint
                )
            except Exception as exc:
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    claim.project_id,
                )
                raise self._operation_state_conflict(
                    claim,
                    operation_id,
                    state.version if state is not None else 0,
                ) from exc
            if checkpoint_state != "published":
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    claim.project_id,
                )
                raise self._operation_state_conflict(
                    claim,
                    operation_id,
                    state.version if state is not None else 0,
                )

        now = self._runtime._now()
        with runtime_db.write_transaction(self._conn):
            operation = self._operation_for_claim(
                claim, operation_id
            )
            try:
                fresh_checkpoint = (
                    self._checkpoint_event_identity(
                        operation,
                        certify_current=True,
                    )
                    if operation.approval_checkpoint_id is not None
                    else None
                )
            except Exception as exc:
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    claim.project_id,
                )
                raise self._operation_state_conflict(
                    claim,
                    operation_id,
                    state.version if state is not None else 0,
                ) from exc
            if fresh_checkpoint != expected_checkpoint:
                state = runtime_db.runtime_state_for_project(
                    self._conn,
                    claim.project_id,
                )
                raise self._operation_state_conflict(
                    claim,
                    operation_id,
                    state.version if state is not None else 0,
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
            self._require_current_operation_policy(operation)
            if operation.status == "effect_started":
                return self._public_operation(operation)
            if operation.status != "approved":
                raise self._operation_state_conflict(
                    claim, operation_id, state.version
                )
            self._decertify_for_claim(
                operation, claim, state.version
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
            stored = self._certify_for_claim(
                claim, operation_id, updated.version
            )
            return self._public_operation(stored)

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
            self._decertify_for_claim(
                operation, claim, state.version
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
            stored = self._certify_for_claim(
                claim, operation_id, updated.version
            )
            return self._public_operation(stored)

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
                self._decertify_for_claim(
                    operation, claim, state.version
                )
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
                self._certify_for_claim(
                    claim, operation_id, updated.version
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
            self._decertify_for_claim(
                current, claim, state.version
            )
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
            stored = self._certify_for_claim(
                claim, operation_id, updated.version
            )
            return self._public_operation(stored)

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
            self._decertify_for_claim(
                operation, claim, state.version
            )
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
            stored = self._certify_for_claim(
                claim, operation_id, updated.version
            )
            return self._public_operation(stored)

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


ProjectOperationGuard.prepare.__signature__ = inspect.Signature(
    parameters=tuple(
        parameter
        for parameter in inspect.signature(
            ProjectOperationGuard.prepare
        ).parameters.values()
        if parameter.name not in {"authority", "policy_authority"}
    )
)
