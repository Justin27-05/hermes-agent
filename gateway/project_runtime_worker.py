"""Narrow consumer and stop-closure seams for preclaimed project starts."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import os
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import copy_context
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, TYPE_CHECKING, Literal, Protocol, TypeVar, cast

from agent.errors import (
    ProjectExecutionControlSignal,
    ProjectToolExecutionDenied,
)
from hermes_cli.project_operations import (
    OperationApprovalSpec,
    OperationIntent,
    OperationReadbackRequest,
    OperationReadbackResult,
    OperationReceipt,
    ProjectOperation,
    ProjectOperationGuard,
)
from hermes_cli.project_policy import (
    ActorContext,
    ContractPolicyView,
    Decision,
    PolicyDecision,
    ProjectCommand,
    ProjectPolicyView,
    canonicalize_targets,
)

from hermes_cli.project_runtime import (
    CanonicalTurnResult,
    ClaimControl,
    ProjectRuntimeError,
    ProjectTurn,
    RuntimeErrorCode,
    TurnAttemptIdentity,
    RunControl,
    TurnClaim,
    TurnExecutionInput,
    TurnOrigin,
    ProjectRuntime,
    WorkerStart,
)
from gateway.config import GatewayConfig
from gateway.session import (
    ProjectBatchApplyResult,
    ProjectHistorySnapshot,
    SessionContext,
    build_canonical_project_session_context,
)
from hermes_state import PendingProjectBatch

_T = TypeVar("_T")
_BATCH_UNBOUND = object()
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _OnceOutcome:
    value: object | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _OnceCall:
    key: tuple[object, ...]
    future: asyncio.Future[_OnceOutcome]


@dataclass(frozen=True)
class StopRequest:
    project_id: str
    turn_id: str
    attempt_id: str
    worker_id: str
    lease_generation: int
    fencing_token: int
    canonical_session_id: str
    control_version: int


class ProjectRuntimeWorker(Protocol):
    async def run_start(self, start: WorkerStart) -> None: ...

    def request_stop(self, request: StopRequest) -> bool: ...


class QuiescingRunner(Protocol):
    def request_cancel(self) -> bool: ...

    async def wait_quiescent(self) -> None: ...


class ProjectRuntimeCloserPort(Protocol):
    async def control_for_claim(
        self,
        claim: TurnClaim,
    ) -> ClaimControl: ...

    async def commit_turn_with_task7_batch(
        self,
        claim: TurnClaim,
        result: CanonicalTurnResult,
        *,
        transcript_batch_id: str,
    ) -> ProjectTurn: ...

    async def acknowledge_stopped(
        self,
        claim: TurnClaim,
    ) -> RunControl: ...


class ProjectBatchApplyPort(Protocol):
    async def apply_project_batch(
        self,
        batch_id: str,
    ) -> ProjectBatchApplyResult: ...


class ProjectRuntimeLiveHandle:
    """Own one exact live runner and its idempotent cancellation latch."""

    def __init__(
        self,
        start: WorkerStart,
        runner: QuiescingRunner,
    ) -> None:
        if type(start) is not WorkerStart:
            raise TypeError("start must be a WorkerStart")
        if not (
            callable(getattr(runner, "request_cancel", None))
            and callable(getattr(runner, "wait_quiescent", None))
        ):
            raise TypeError("runner must support cancellation and quiescence")
        self._start = start
        self._runner = runner
        self._active = True
        self._terminal_won = False
        self._cancel_requested = False
        self._stop_control_version: int | None = None
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._terminal_call: _OnceCall | None = None
        self._stop_call: _OnceCall | None = None
        self._apply_call: _OnceCall | None = None
        self._batch_id: object = _BATCH_UNBOUND

    def request_cancel(self) -> bool:
        if (
            not self._active
            or self._terminal_won
            or self._cancel_requested
        ):
            return False
        self._cancel_requested = True
        self._runner.request_cancel()
        return True

    def _request_cleanup_cancel(self) -> bool:
        """Quiesce a discarded parent without reopening public C11 stop state."""
        if not self._active or self._cancel_requested:
            return False
        self._cancel_requested = True
        self._runner.request_cancel()
        return True

    def request_stop(self, request: StopRequest) -> bool:
        claim = self._start.claim
        if (
            type(request) is not StopRequest
            or not self._active
            or self._terminal_won
            or request.project_id != claim.project_id
            or request.turn_id != claim.turn_id
            or request.attempt_id != claim.attempt_id
            or request.worker_id != claim.worker_id
            or request.lease_generation != claim.lease_generation
            or request.fencing_token != claim.fencing_token
            or request.canonical_session_id
            != claim.canonical_session_id
            or type(request.control_version) is not int
            or request.control_version < 0
        ):
            return False
        if self._stop_control_version is not None:
            return self._stop_control_version == request.control_version
        self._stop_control_version = request.control_version
        self.request_cancel()
        return True

    def mark_terminal_won(self) -> None:
        if self._active:
            self._terminal_won = True

    def deactivate(self) -> None:
        self._active = False

    async def wait_quiescent(self) -> None:
        await self._runner.wait_quiescent()

    def _can_close_stop(self) -> bool:
        return self._active and not self._terminal_won

    @staticmethod
    def _claim_identity(claim: TurnClaim) -> tuple[object, ...]:
        return (
            claim.project_id,
            claim.turn_id,
            claim.sequence,
            claim.worker_id,
            claim.attempt_id,
            claim.lease_generation,
            claim.fencing_token,
            claim.canonical_session_id,
        )

    def _require_claim(self, claim: TurnClaim) -> None:
        if (
            type(claim) is not TurnClaim
            or not self._active
            or self._claim_identity(claim)
            != self._claim_identity(self._start.claim)
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.STALE_TURN_CLAIM,
                project_id=getattr(claim, "project_id", None),
                turn_id=getattr(claim, "turn_id", None),
            )
        self._event_loop()

    def _event_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError(
                "project runtime live handle belongs to another event loop"
            )
        return loop

    def _bind_batch(self, batch_id: str | None) -> None:
        self._event_loop()
        if batch_id is None:
            return
        if type(batch_id) is not str or not batch_id:
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT,
                project_id=self._start.claim.project_id,
                turn_id=self._start.claim.turn_id,
            )
        if self._batch_id is _BATCH_UNBOUND:
            self._batch_id = batch_id
        elif self._batch_id != batch_id:
            raise ProjectRuntimeError(
                RuntimeErrorCode.TERMINAL_RESULT_CONFLICT,
                project_id=self._start.claim.project_id,
                turn_id=self._start.claim.turn_id,
            )

    def _has_stop_call(self) -> bool:
        return self._stop_call is not None

    async def _run_once(
        self,
        *,
        slot_name: str,
        key: tuple[object, ...],
        operation: Callable[[], Awaitable[_T]],
    ) -> _T:
        loop = self._event_loop()
        existing = cast(_OnceCall | None, getattr(self, slot_name))
        if existing is None:
            future: asyncio.Future[_OnceOutcome] = loop.create_future()
            setattr(self, slot_name, _OnceCall(key, future))
            owner = True
        else:
            if existing.key != key:
                raise ProjectRuntimeError(
                    RuntimeErrorCode.TERMINAL_RESULT_CONFLICT,
                    project_id=self._start.claim.project_id,
                    turn_id=self._start.claim.turn_id,
                )
            future = existing.future
            owner = False

        if not owner:
            outcome = await asyncio.shield(future)
            if outcome.error is not None:
                raise outcome.error
            return cast(_T, outcome.value)

        try:
            value = await operation()
        except BaseException as error:
            future.set_result(_OnceOutcome(error=error))
            raise
        future.set_result(_OnceOutcome(value=value))
        return value

    async def _run_terminal_once(
        self,
        *,
        result: CanonicalTurnResult,
        batch_id: str,
        operation: Callable[
            [],
            Awaitable[ProjectBatchApplyResult],
        ],
    ) -> ProjectBatchApplyResult:
        self._bind_batch(batch_id)
        return await self._run_once(
            slot_name="_terminal_call",
            key=(result.status, batch_id),
            operation=operation,
        )

    async def _run_stop_once(
        self,
        operation: Callable[[], Awaitable[None]],
    ) -> None:
        await self._run_once(
            slot_name="_stop_call",
            key=("stop",),
            operation=operation,
        )

    async def _run_apply_once(
        self,
        *,
        batch_id: str,
        operation: Callable[
            [],
            Awaitable[ProjectBatchApplyResult],
        ],
    ) -> ProjectBatchApplyResult:
        self._bind_batch(batch_id)
        return await self._run_once(
            slot_name="_apply_call",
            key=(batch_id,),
            operation=operation,
        )


class ProjectRuntimeTerminalCloser:
    """Linearize runner quiescence with one Projects terminal boundary."""

    def __init__(
        self,
        runtime: ProjectRuntimeCloserPort,
        batches: ProjectBatchApplyPort,
    ) -> None:
        if not all(
            callable(getattr(runtime, name, None))
            for name in (
                "control_for_claim",
                "commit_turn_with_task7_batch",
                "acknowledge_stopped",
            )
        ):
            raise TypeError("runtime does not implement the closer port")
        if not callable(getattr(batches, "apply_project_batch", None)):
            raise TypeError("batches does not implement the apply port")
        self._runtime = runtime
        self._batches = batches

    @staticmethod
    def _current_claim(
        claim: TurnClaim,
        control: ClaimControl,
    ) -> TurnClaim:
        return replace(
            claim,
            lease_expires_at=control.lease_expires_at,
        )

    async def _perform_observed_stop_ack(
        self,
        *,
        claim: TurnClaim,
        runner: ProjectRuntimeLiveHandle,
    ) -> None:
        if not runner._can_close_stop():
            raise ProjectRuntimeError(
                RuntimeErrorCode.STALE_TURN_CLAIM,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )
        runner.request_cancel()
        await runner.wait_quiescent()
        await self._runtime.acknowledge_stopped(claim)

    async def _apply_once(
        self,
        *,
        runner: ProjectRuntimeLiveHandle,
        batch_id: str,
    ) -> ProjectBatchApplyResult:
        async def apply() -> ProjectBatchApplyResult:
            return await self._batches.apply_project_batch(batch_id)

        return await runner._run_apply_once(
            batch_id=batch_id,
            operation=apply,
        )

    async def _stop_once_then_apply(
        self,
        *,
        runner: ProjectRuntimeLiveHandle,
        batch_id: str | None,
        acknowledge: Callable[[], Awaitable[None]],
    ) -> ProjectBatchApplyResult | None:
        await runner._run_stop_once(acknowledge)
        if batch_id is None:
            return None
        return await self._apply_once(
            runner=runner,
            batch_id=batch_id,
        )

    async def acknowledge_stop(
        self,
        *,
        claim: TurnClaim,
        runner: ProjectRuntimeLiveHandle,
        batch_id: str | None,
    ) -> ProjectBatchApplyResult | None:
        runner._require_claim(claim)
        runner._bind_batch(batch_id)

        async def observe_and_acknowledge() -> None:
            control = await self._runtime.control_for_claim(claim)
            if control.state != "stop_requested":
                raise ProjectRuntimeError(
                    RuntimeErrorCode.STALE_TURN_CLAIM,
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                )
            await self._perform_observed_stop_ack(
                claim=self._current_claim(claim, control),
                runner=runner,
            )

        return await self._stop_once_then_apply(
            runner=runner,
            batch_id=batch_id,
            acknowledge=observe_and_acknowledge,
        )

    async def _resolve_prepared_terminal_once(
        self,
        *,
        claim: TurnClaim,
        result: CanonicalTurnResult,
        batch_id: str,
        runner: ProjectRuntimeLiveHandle,
    ) -> ProjectBatchApplyResult:
        await runner.wait_quiescent()
        try:
            control = await self._runtime.control_for_claim(claim)
        except ProjectRuntimeError as exc:
            if (
                exc.code is not RuntimeErrorCode.STALE_TURN_CLAIM
                or not runner._has_stop_call()
            ):
                raise

            async def acknowledge_again() -> None:
                raise AssertionError(
                    "cached stop acknowledgement must not run again"
                )

            stopped = await self._stop_once_then_apply(
                runner=runner,
                batch_id=batch_id,
                acknowledge=acknowledge_again,
            )
            assert stopped is not None
            return stopped
        current_claim = self._current_claim(claim, control)
        if control.state == "stop_requested":
            stopped = await self._stop_once_then_apply(
                runner=runner,
                batch_id=batch_id,
                acknowledge=lambda: self._perform_observed_stop_ack(
                    claim=current_claim,
                    runner=runner,
                ),
            )
            assert stopped is not None
            return stopped
        if control.state == "awaiting_approval":
            raise ProjectRuntimeError(
                RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )

        try:
            await self._runtime.commit_turn_with_task7_batch(
                current_claim,
                result,
                transcript_batch_id=batch_id,
            )
        except ProjectRuntimeError as exc:
            if exc.code is not RuntimeErrorCode.STALE_TURN_CLAIM:
                raise
            try:
                reread = await self._runtime.control_for_claim(
                    current_claim
                )
            except Exception:
                return await self._apply_once(
                    runner=runner,
                    batch_id=batch_id,
                )
            if reread.state == "stop_requested":
                reread_claim = self._current_claim(
                    current_claim,
                    reread,
                )
                stopped = await self._stop_once_then_apply(
                    runner=runner,
                    batch_id=batch_id,
                    acknowledge=lambda: (
                        self._perform_observed_stop_ack(
                            claim=reread_claim,
                            runner=runner,
                        )
                    ),
                )
                assert stopped is not None
                return stopped
            return await self._apply_once(
                runner=runner,
                batch_id=batch_id,
            )

        runner.mark_terminal_won()
        return await self._apply_once(
            runner=runner,
            batch_id=batch_id,
        )

    async def resolve_prepared_terminal(
        self,
        *,
        claim: TurnClaim,
        result: CanonicalTurnResult,
        batch_id: str,
        runner: ProjectRuntimeLiveHandle,
    ) -> ProjectBatchApplyResult:
        if (
            type(result) is not CanonicalTurnResult
            or result.result_id != batch_id
        ):
            raise ProjectRuntimeError(
                RuntimeErrorCode.INVALID_ARGUMENT,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
            )
        runner._require_claim(claim)

        async def resolve() -> ProjectBatchApplyResult:
            return await self._resolve_prepared_terminal_once(
                claim=claim,
                result=result,
                batch_id=batch_id,
                runner=runner,
            )

        return await runner._run_terminal_once(
            result=result,
            batch_id=batch_id,
            operation=resolve,
        )


@dataclass(frozen=True)
class ProjectAgentRevisions:
    base_signature: str
    tool_revision: str
    model_revision: str


@dataclass(frozen=True)
class ProjectAgentRunResult:
    status: Literal["succeeded", "failed"]
    base_message_count: int
    messages: tuple[Mapping[str, object], ...]


@dataclass(frozen=True)
class ProjectOperationProposal:
    intent: OperationIntent
    policy_batch_id: str | None
    effect_scope_json: str
    effect_scope_sha256: str
    capability_fingerprint: tuple[str, int, str | None, bool]


@dataclass(frozen=True)
class ProjectReadProposal:
    canonical_action: str
    targets: tuple[str, ...]
    policy_batch_id: str | None
    batch_items: tuple[str, ...]


@dataclass(frozen=True)
class BoundProjectOperationAuthority:
    command: ProjectCommand
    intent: OperationIntent
    policy_batch_id: str | None
    effect_scope_json: str
    effect_scope_sha256: str
    authority_json: str
    authority_sha256: str


@dataclass(frozen=True)
class ProjectPolicyDecisionCarrier:
    execution_attempt: TurnAttemptIdentity
    execution_origin: TurnOrigin
    control_version: int
    runtime_version: int
    operation_authority: BoundProjectOperationAuthority
    project: ProjectPolicyView
    contract_id: str
    contract_status: Literal["active"]
    contract_json_sha256: str
    contract: ContractPolicyView
    actor: ActorContext
    decision: PolicyDecision


@dataclass(frozen=True)
class CertifiedProjectOperationExecutionRequest:
    operation: ProjectOperation
    attempt: TurnAttemptIdentity
    payload: Mapping[str, object]
    approval_checkpoint_id: str | None
    operation_authority_json: str
    operation_authority_sha256: str
    effect_scope_json: str
    effect_scope_sha256: str
    policy_authority_sha256: str
    remote_idempotency_supported: bool
    capability_fingerprint: tuple[str, int, str, bool]


@dataclass(frozen=True)
class ProjectExecutionContext:
    execution: TurnExecutionInput
    owner_loop: asyncio.AbstractEventLoop


class ProjectCheckpointFailed(
    RuntimeError,
    ProjectExecutionControlSignal,
):
    pass


class ProjectCheckpointSettlementPending(
    RuntimeError,
    ProjectExecutionControlSignal,
):
    pass


class ProjectApprovalPublished(
    RuntimeError,
    ProjectExecutionControlSignal,
):
    pass


class ProjectOperationUnresolved(
    RuntimeError,
    ProjectExecutionControlSignal,
):
    pass


class ApprovedOperationTurn(QuiescingRunner, Protocol):
    async def result(self) -> ProjectAgentRunResult: ...


class ApprovedOperationExecutionPort(Protocol):
    def create_turn(
        self,
        execution: TurnExecutionInput,
        operation: ProjectOperation,
        *,
        base_message_count: int,
    ) -> ApprovedOperationTurn: ...


def _fresh_projects_call(
    projects_db_factory: Callable[[], Any],
    operation: Callable[[Any], _T],
) -> _T:
    connection = None
    try:
        connection = projects_db_factory()
        return operation(connection)
    finally:
        if connection is not None:
            connection.close()


def _worker_runtime_target(connection: Any) -> Any:
    direct = getattr(connection, "mark_turn_started", None)
    return connection if callable(direct) else ProjectRuntime(connection)


async def _await_retained_io(call: Awaitable[_T]) -> _T:
    task = asyncio.ensure_future(call)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            break
    if cancelled:
        try:
            task.result()
        except BaseException:
            pass
        raise asyncio.CancelledError()
    return task.result()


class ProjectRuntimeWorkerFacade:
    def __init__(
        self,
        projects_db_factory: Callable[[], Any],
        *,
        io_runner: Callable[..., Awaitable[Any]],
    ) -> None:
        if not callable(projects_db_factory) or not callable(io_runner):
            raise TypeError("worker facade dependencies must be callable")
        self._projects_db_factory = projects_db_factory
        self._io_runner = io_runner

    async def _call(self, name: str, *args: object, **kwargs: object) -> Any:
        return await _await_retained_io(
            self._io_runner(
                _fresh_projects_call,
                self._projects_db_factory,
                lambda connection: getattr(
                    _worker_runtime_target(connection),
                    name,
                )(*args, **kwargs),
            )
        )

    async def mark_turn_started(self, claim: TurnClaim) -> TurnClaim:
        return await self._call("mark_turn_started", claim)

    async def execution_input_for_claim(
        self,
        claim: TurnClaim,
    ) -> TurnExecutionInput:
        return await self._call("execution_input_for_claim", claim)

    async def heartbeat_turn(
        self,
        claim: TurnClaim,
        *,
        lease_seconds: int,
    ) -> TurnClaim:
        return await self._call(
            "heartbeat_turn",
            claim,
            lease_seconds=lease_seconds,
        )

    async def control_for_claim(self, claim: TurnClaim) -> ClaimControl:
        return await self._call("control_for_claim", claim)

    async def commit_turn_with_task7_batch(
        self,
        claim: TurnClaim,
        result: CanonicalTurnResult,
        *,
        transcript_batch_id: str,
    ) -> ProjectTurn:
        return await self._call(
            "commit_turn_with_task7_batch",
            claim,
            result,
            transcript_batch_id=transcript_batch_id,
        )

    async def acknowledge_stopped(self, claim: TurnClaim) -> RunControl:
        return await self._call("acknowledge_stopped", claim)


def _canonical_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _canonical_payload_json(value: object) -> str:
    active: set[int] = set()

    def validate(item: object) -> None:
        if item is None or type(item) in {str, int, bool}:
            return
        if type(item) is float:
            if not math.isfinite(item):
                raise ValueError("payload contains a non-finite number")
            return
        if isinstance(item, Mapping):
            identity = id(item)
            if identity in active:
                raise ValueError("payload contains a cycle")
            if not all(type(key) is str for key in item):
                raise TypeError("payload mapping keys must be strings")
            active.add(identity)
            try:
                for nested in item.values():
                    validate(nested)
            finally:
                active.remove(identity)
            return
        if isinstance(item, (list, tuple)):
            identity = id(item)
            if identity in active:
                raise ValueError("payload contains a cycle")
            active.add(identity)
            try:
                for nested in item:
                    validate(nested)
            finally:
                active.remove(identity)
            return
        raise TypeError("payload contains an unsupported value")

    validate(value)

    def detached(item: object) -> object:
        if item is None or type(item) in {str, int, float, bool}:
            return item
        if isinstance(item, Mapping):
            return {
                key: detached(nested)
                for key, nested in item.items()
            }
        if isinstance(item, (list, tuple)):
            return [detached(nested) for nested in item]
        raise TypeError("payload contains an unsupported value")

    return _canonical_json(detached(value))


def _require_uuid4(value: object, *, label: str) -> str:
    if not _is_canonical_uuid4(value):
        raise ValueError(f"{label} must be a canonical UUIDv4")
    return cast(str, value)


def _registry_adapter(registry: object, fingerprint: tuple[object, ...]) -> Any:
    getter = getattr(registry, "get", None)
    adapter = getter(fingerprint, None) if callable(getter) else None
    if adapter is None:
        raise PermissionError("project capability is not registered")
    return adapter


def _require_bound_authority(
    execution: TurnExecutionInput,
    proposal: ProjectOperationProposal,
    authority: object,
    snapshot: object,
    adapter: object,
) -> BoundProjectOperationAuthority:
    if type(authority) is not BoundProjectOperationAuthority:
        raise TypeError("operation binder returned invalid authority")
    intent = authority.intent
    command = authority.command
    canonical_targets = canonicalize_targets(intent.targets)
    if canonical_targets is None or canonical_targets != intent.targets:
        raise PermissionError(
            "bound project operation targets are not canonical"
        )
    expected_fingerprint = (
        intent.canonical_action,
        intent.command_revision,
        intent.readback_kind,
        intent.remote_idempotency_supported,
    )
    declared = getattr(adapter, "fingerprint", None)
    declared_fingerprint = (
        tuple(declared)
        if isinstance(declared, tuple)
        else (
            getattr(adapter, "canonical_action", None),
            getattr(adapter, "command_revision", None),
            getattr(adapter, "readback_kind", None),
            getattr(adapter, "remote_idempotency_supported", None),
        )
    )
    if not (
        authority.intent == proposal.intent
        and authority.policy_batch_id == proposal.policy_batch_id
        and authority.effect_scope_json == proposal.effect_scope_json
        and authority.effect_scope_sha256 == proposal.effect_scope_sha256
        and _canonical_sha256(authority.effect_scope_json)
        == authority.effect_scope_sha256
        and _canonical_sha256(authority.authority_json)
        == authority.authority_sha256
        and proposal.capability_fingerprint == expected_fingerprint
        and proposal.capability_fingerprint == declared_fingerprint
        and command.project_id == execution.attempt.project_id
        and intent.project_id == execution.attempt.project_id
        and intent.turn_id == execution.attempt.turn_id
        and command.revision == execution.contract_revision
        and command.revision == getattr(snapshot, "contract_revision", None)
        and command.name == intent.canonical_action
        and command.name == declared_fingerprint[0]
        and type(command.action_class) is str
        and command.action_class
        in getattr(snapshot, "allowed_action_classes", ())
        and intent.command_revision == declared_fingerprint[1]
        and command.targets == intent.targets
        and command.targets == proposal.intent.targets
        and command.batch_id == authority.policy_batch_id
        and command.batch_items == intent.batch_items
        and command.metadata
        == {"phase": getattr(snapshot, "current_phase", None)}
    ):
        raise PermissionError("bound project operation authority drift")
    decoded_scope = json.loads(authority.effect_scope_json)
    decoded_authority = json.loads(authority.authority_json)
    if not (
        type(decoded_scope) is dict
        and type(decoded_authority) is dict
        and decoded_authority.get("effect_scope") == decoded_scope
        and tuple(decoded_authority.get("capability_fingerprint", ()))
        == expected_fingerprint
        and decoded_authority.get("policy_batch_id")
        == authority.policy_batch_id
    ):
        raise PermissionError("operation authority serialization drift")
    encoded_command = decoded_authority.get("command")
    encoded_intent = decoded_authority.get("intent")
    if not (
        type(encoded_command) is dict
        and type(encoded_intent) is dict
        and encoded_command.get("name") == command.name
        and encoded_command.get("project_id") == command.project_id
        and encoded_command.get("revision") == command.revision
        and encoded_command.get("action_class") == command.action_class
        and tuple(encoded_command.get("targets", ())) == command.targets
        and encoded_command.get("batch_id") == command.batch_id
        and tuple(encoded_command.get("batch_items", ()))
        == command.batch_items
        and encoded_command.get("metadata") == dict(command.metadata)
        and encoded_intent.get("operation_id") == intent.operation_id
        and encoded_intent.get("project_id") == intent.project_id
        and encoded_intent.get("turn_id") == intent.turn_id
        and encoded_intent.get("idempotency_key") == intent.idempotency_key
        and encoded_intent.get("canonical_action")
        == intent.canonical_action
        and encoded_intent.get("command_revision")
        == intent.command_revision
        and tuple(encoded_intent.get("targets", ())) == intent.targets
        and tuple(encoded_intent.get("batch_items", ()))
        == intent.batch_items
        and encoded_intent.get("payload") == dict(intent.payload)
        and encoded_intent.get("readback_kind") == intent.readback_kind
        and encoded_intent.get("remote_idempotency_supported")
        is intent.remote_idempotency_supported
    ):
        raise PermissionError("operation authority fields drift")
    scope_targets = tuple(decoded_scope.get("targets", ()))
    scope_batch_items = tuple(decoded_scope.get("batch_items", ()))
    if scope_targets != intent.targets or scope_batch_items != intent.batch_items:
        raise PermissionError("operation effect scope drift")
    payload_scope = decoded_scope.get(
        "payload_effects",
        decoded_scope.get("payload"),
    )
    if payload_scope is not None and payload_scope != dict(intent.payload):
        raise PermissionError("operation payload scope drift")
    allowed_scope_keys = {"targets", "batch_items", "payload_effects", "payload"}
    if set(decoded_scope) - allowed_scope_keys:
        raise PermissionError("operation effect scope contains unknown fields")
    return authority


class ProjectToolPolicySnapshotFacade:
    def __init__(
        self,
        projects_db_factory: Callable[[], Any],
        *,
        read_binder: Callable[..., ProjectCommand],
        operation_binder: Callable[..., BoundProjectOperationAuthority],
        capability_registry: object,
        policy_decider: Callable[..., PolicyDecision],
        snapshot_materializer: Callable[[object], object],
        authority_clock: Callable[[], int | float],
        approval_id_factory: Callable[[], str],
        io_runner: Callable[..., Awaitable[Any]],
    ) -> None:
        dependencies = (
            projects_db_factory,
            read_binder,
            operation_binder,
            policy_decider,
            snapshot_materializer,
            authority_clock,
            approval_id_factory,
            io_runner,
        )
        if not all(callable(value) for value in dependencies):
            raise TypeError("policy facade dependencies must be callable")
        self._projects_db_factory = projects_db_factory
        self._read_binder = read_binder
        self._operation_binder = operation_binder
        self._capability_registry = capability_registry
        self._policy_decider = policy_decider
        self._snapshot_materializer = snapshot_materializer
        self._authority_clock = authority_clock
        self._approval_id_factory = approval_id_factory
        self._io_runner = io_runner

    def _snapshot(
        self,
        execution: TurnExecutionInput,
        operation: Callable[[object, object], _T],
    ) -> _T:
        def invoke(connection: Any) -> _T:
            snapshot = connection.load_project_policy_snapshot(
                execution.attempt.project_id,
                execution.contract_revision,
                execution.origin,
            )
            return operation(snapshot, self._snapshot_materializer(snapshot))

        return _fresh_projects_call(self._projects_db_factory, invoke)

    @staticmethod
    def _require_materialized(
        execution: TurnExecutionInput,
        snapshot: object,
        materialized: object,
    ) -> None:
        project = getattr(materialized, "project", None)
        contract = getattr(materialized, "contract", None)
        actor = getattr(materialized, "actor", None)
        if not (
            type(execution) is TurnExecutionInput
            and type(execution.attempt) is TurnAttemptIdentity
            and type(execution.origin) is TurnOrigin
            and type(project) is ProjectPolicyView
            and type(contract) is ContractPolicyView
            and type(actor) is ActorContext
            and project.project_id == execution.attempt.project_id
            and project.lifecycle == "active"
            and contract.revision == execution.contract_revision
            and getattr(materialized, "contract_status", None) == "active"
            and actor.actor_id == execution.origin.actor_id
            and actor.surface == execution.origin.surface
            and actor.binding_id == execution.origin.binding_id
            and actor.is_owner is True
            and getattr(snapshot, "binding_id", None)
            == execution.origin.binding_id
        ):
            raise PermissionError("invalid durable project policy snapshot")

    async def authorize_project_read(
        self,
        execution: TurnExecutionInput,
        proposal: ProjectReadProposal,
    ) -> PolicyDecision:
        if type(proposal) is not ProjectReadProposal:
            raise TypeError("proposal must be ProjectReadProposal")

        def authorize(snapshot: object, materialized: object) -> PolicyDecision:
            self._require_materialized(execution, snapshot, materialized)
            command = self._read_binder(snapshot, execution, proposal)
            if not (
                type(command) is ProjectCommand
                and command.project_id == execution.attempt.project_id
                and command.revision == execution.contract_revision
                and command.name == proposal.canonical_action
                and command.targets == proposal.targets
                and command.batch_id == proposal.policy_batch_id
                and command.batch_items == proposal.batch_items
                and command.metadata
                == {"phase": getattr(snapshot, "current_phase", None)}
            ):
                raise PermissionError("read policy binding drift")
            decision = self._policy_decider(
                command,
                materialized.project,
                materialized.contract,
                materialized.actor,
            )
            if type(decision) is not PolicyDecision:
                raise TypeError("policy decider returned invalid decision")
            return decision

        return await _await_retained_io(
            self._io_runner(self._snapshot, execution, authorize)
        )

    async def authorize_project_operation(
        self,
        execution: TurnExecutionInput,
        proposal: ProjectOperationProposal,
    ) -> object:
        if type(proposal) is not ProjectOperationProposal:
            raise TypeError("proposal must be ProjectOperationProposal")

        def authorize(snapshot: object, materialized: object) -> object:
            self._require_materialized(execution, snapshot, materialized)
            adapter = _registry_adapter(
                self._capability_registry,
                proposal.capability_fingerprint,
            )
            authority = _require_bound_authority(
                execution,
                proposal,
                self._operation_binder(snapshot, execution, proposal),
                snapshot,
                adapter,
            )
            if authority.command.action_class == "internal_delivery" or (
                authority.command.name in {"event.deliver", "internal_delivery"}
            ):
                raise PermissionError("project delivery is not a C14 capability")
            decision = self._policy_decider(
                authority.command,
                materialized.project,
                materialized.contract,
                materialized.actor,
            )
            if type(decision) is not PolicyDecision:
                raise TypeError("policy decider returned invalid decision")
            carrier = ProjectPolicyDecisionCarrier(
                execution.attempt,
                execution.origin,
                materialized.control_version,
                materialized.runtime_version,
                authority,
                materialized.project,
                materialized.contract_id,
                materialized.contract_status,
                materialized.contract_json_sha256,
                materialized.contract,
                materialized.actor,
                decision,
            )
            approval = None
            if decision.decision is Decision.REQUIRE_APPROVAL:
                approval = OperationApprovalSpec(
                    _require_uuid4(
                        self._approval_id_factory(),
                        label="approval id",
                    ),
                    cast(str, decision.approval_class),
                    self._authority_clock() + 3600,
                    materialized.actor,
                )
            if approval is None:
                return carrier
            return carrier, approval

        return await _await_retained_io(
            self._io_runner(self._snapshot, execution, authorize)
        )


class ProjectOperationPrepareFacade:
    def __init__(
        self,
        projects_db_factory: Callable[[], Any],
        *,
        io_runner: Callable[..., Awaitable[Any]],
        runtime_factory: Callable[[Any], Any] = ProjectRuntime,
        operation_guard_factory: Callable[[Any], Any] = ProjectOperationGuard,
    ) -> None:
        if not all(
            callable(value)
            for value in (
                projects_db_factory,
                io_runner,
                runtime_factory,
                operation_guard_factory,
            )
        ):
            raise TypeError("prepare facade dependencies must be callable")
        self._projects_db_factory = projects_db_factory
        self._io_runner = io_runner
        self._runtime_factory = runtime_factory
        self._operation_guard_factory = operation_guard_factory

    async def prepare(
        self,
        claim: TurnClaim,
        intent: OperationIntent,
        *,
        authority: BoundProjectOperationAuthority,
        policy: PolicyDecision,
        policy_authority: ProjectPolicyDecisionCarrier,
        approval: OperationApprovalSpec | None = None,
        approval_checkpoint_id: str | None | object = _BATCH_UNBOUND,
    ) -> ProjectOperation:
        def invoke(connection: Any) -> ProjectOperation:
            guard = self._operation_guard_factory(
                self._runtime_factory(connection)
            )
            kwargs: dict[str, object] = {
                "authority": authority,
                "policy": policy,
                "policy_authority": policy_authority,
                "approval": approval,
            }
            if approval_checkpoint_id is not _BATCH_UNBOUND:
                kwargs["approval_checkpoint_id"] = approval_checkpoint_id
            try:
                return guard.prepare(claim, intent, **kwargs)
            except (PermissionError, TypeError, ValueError):
                raise
            except RuntimeError as exc:
                raise PermissionError(
                    "project operation guard rejected request"
                ) from exc

        return await _await_retained_io(
            self._io_runner(
                _fresh_projects_call,
                self._projects_db_factory,
                invoke,
            )
        )


class _ReadbackResultPort:
    def __init__(self, result: OperationReadbackResult) -> None:
        self._result = result

    def read_operation(self, request: OperationReadbackRequest) -> OperationReadbackResult:
        return self._result


class _OwnerLoopCheckpointReadPort:
    def __init__(
        self,
        checkpoint_facade: object,
        owner_loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._checkpoint_facade = checkpoint_facade
        self._owner_loop = owner_loop

    def publication_state(self, checkpoint: object) -> object:
        call = self._checkpoint_facade.publication_state(checkpoint)
        return asyncio.run_coroutine_threadsafe(
            call,
            self._owner_loop,
        ).result()


class ProjectOperationExecutionFacade:
    def __init__(
        self,
        projects_db_factory: Callable[[], Any],
        *,
        approval_checkpoints: object,
        io_runner: Callable[..., Awaitable[Any]],
        runtime_factory: Callable[[Any], Any] = ProjectRuntime,
        operation_guard_factory: Callable[[Any], Any] = ProjectOperationGuard,
    ) -> None:
        if not all(
            callable(value)
            for value in (
                projects_db_factory,
                io_runner,
                runtime_factory,
                operation_guard_factory,
            )
        ):
            raise TypeError("execution facade dependencies must be callable")
        self._projects_db_factory = projects_db_factory
        self._approval_checkpoints = approval_checkpoints
        self._io_runner = io_runner
        self._runtime_factory = runtime_factory
        self._operation_guard_factory = operation_guard_factory

    async def _guard_call(
        self,
        method: str,
        *args: object,
        **kwargs: object,
    ) -> Any:
        def invoke(connection: Any) -> Any:
            guard = self._operation_guard_factory(
                self._runtime_factory(connection)
            )
            try:
                return getattr(guard, method)(*args, **kwargs)
            except (PermissionError, TypeError, ValueError):
                raise
            except RuntimeError as exc:
                raise PermissionError(
                    "project operation guard rejected request"
                ) from exc

        return await _await_retained_io(
            self._io_runner(
                _fresh_projects_call,
                self._projects_db_factory,
                invoke,
            )
        )

    @staticmethod
    def _guard_inputs(
        request: CertifiedProjectOperationExecutionRequest,
    ) -> tuple[TurnClaim, str]:
        if type(request) is not CertifiedProjectOperationExecutionRequest:
            raise TypeError(
                "request must be CertifiedProjectOperationExecutionRequest"
            )
        operation = request.operation
        attempt = request.attempt
        if type(operation) is not ProjectOperation:
            raise TypeError("request.operation must be ProjectOperation")
        if type(attempt) is not TurnAttemptIdentity:
            raise TypeError("request.attempt must be TurnAttemptIdentity")
        operation_id = operation.operation_id
        if not (
            type(operation_id) is str
            and bool(operation_id)
            and operation.project_id == attempt.project_id
            and operation.turn_id == attempt.turn_id
            and operation.attempt_id == attempt.attempt_id
            and operation.lease_generation == attempt.lease_generation
            and operation.fencing_token == attempt.fencing_token
        ):
            raise PermissionError(
                "certified operation request does not match its attempt"
            )
        claim = TurnClaim(
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
        return claim, operation_id

    async def certified_execution_request(
        self,
        execution: TurnExecutionInput,
        operation: ProjectOperation,
    ) -> CertifiedProjectOperationExecutionRequest:
        return await self._guard_call(
            "certified_execution_request",
            execution,
            operation,
        )

    async def mark_started(
        self,
        request: CertifiedProjectOperationExecutionRequest,
    ) -> ProjectOperation:
        claim, operation_id = self._guard_inputs(request)
        return await self._guard_call(
            "mark_started",
            claim,
            operation_id,
            approval_checkpoints=self._approval_checkpoints,
        )

    async def _mark_started_with_checkpoint(
        self,
        request: CertifiedProjectOperationExecutionRequest,
    ) -> ProjectOperation:
        claim, operation_id = self._guard_inputs(request)
        checkpoint_port = _OwnerLoopCheckpointReadPort(
            self._approval_checkpoints,
            asyncio.get_running_loop(),
        )
        return await self._guard_call(
            "mark_started",
            claim,
            operation_id,
            approval_checkpoints=checkpoint_port,
        )

    async def record_receipt(
        self,
        request: CertifiedProjectOperationExecutionRequest,
        receipt: OperationReceipt,
    ) -> ProjectOperation:
        claim, operation_id = self._guard_inputs(request)
        return await self._guard_call(
            "record_receipt",
            claim,
            operation_id,
            receipt,
        )

    async def reconcile(
        self,
        request: CertifiedProjectOperationExecutionRequest,
        readback: object,
    ) -> ProjectOperation:
        claim, operation_id = self._guard_inputs(request)
        port = (
            _ReadbackResultPort(readback)
            if type(readback) is OperationReadbackResult
            else readback
        )
        return await self._guard_call(
            "reconcile",
            claim,
            operation_id,
            port,
        )


def _request_adapter(
    execution: TurnExecutionInput,
    operation: ProjectOperation,
    request: object,
    capabilities: object,
) -> Any:
    if not (
        type(execution) is TurnExecutionInput
        and type(operation) is ProjectOperation
        and type(request) is CertifiedProjectOperationExecutionRequest
        and request.operation is operation
        and request.attempt is execution.attempt
        and operation.project_id == execution.attempt.project_id
        and operation.turn_id == execution.attempt.turn_id
        and operation.attempt_id == execution.attempt.attempt_id
        and operation.lease_generation
        == execution.attempt.lease_generation
        and operation.fencing_token == execution.attempt.fencing_token
        and operation.status == "approved"
        and request.approval_checkpoint_id
        == operation.approval_checkpoint_id
        and (
            request.approval_checkpoint_id is None
            or _is_canonical_uuid4(request.approval_checkpoint_id)
        )
        and request.policy_authority_sha256
        == operation.policy_authority_sha256
        and request.remote_idempotency_supported is True
        and operation.readback_kind is not None
        and request.capability_fingerprint
        == (
            operation.canonical_action,
            operation.command_revision,
            operation.readback_kind,
            True,
        )
    ):
        raise PermissionError("invalid certified operation request")
    try:
        authority = json.loads(request.operation_authority_json)
        effect_scope = json.loads(request.effect_scope_json)
        intent = authority["intent"]
        command = authority["command"]
        payload_json = _canonical_payload_json(request.payload)
        payload = json.loads(payload_json)
    except (
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise PermissionError(
            "malformed certified operation request"
        ) from exc
    lower_hex = frozenset("0123456789abcdef")
    if not (
        type(authority) is dict
        and type(effect_scope) is dict
        and type(intent) is dict
        and type(command) is dict
        and request.operation_authority_json
        == _canonical_json(authority)
        and request.operation_authority_sha256
        == _canonical_sha256(request.operation_authority_json)
        and request.effect_scope_json == _canonical_json(effect_scope)
        and request.effect_scope_sha256
        == _canonical_sha256(request.effect_scope_json)
        and type(request.policy_authority_sha256) is str
        and len(request.policy_authority_sha256) == 64
        and set(request.policy_authority_sha256) <= lower_hex
        and authority.get("effect_scope") == effect_scope
        and authority.get("capability_fingerprint")
        == list(request.capability_fingerprint)
        and intent.get("operation_id") == operation.operation_id
        and intent.get("project_id") == operation.project_id
        and intent.get("turn_id") == operation.turn_id
        and intent.get("idempotency_key") == operation.idempotency_key
        and intent.get("canonical_action")
        == operation.canonical_action
        and intent.get("command_revision")
        == operation.command_revision
        and tuple(intent.get("targets", ())) == operation.targets
        and tuple(intent.get("batch_items", ()))
        == operation.batch_items
        and intent.get("payload") == payload
        and intent.get("readback_kind") == operation.readback_kind
        and intent.get("remote_idempotency_supported") is True
        and command.get("name") == operation.canonical_action
        and command.get("project_id") == operation.project_id
        and tuple(command.get("targets", ())) == operation.targets
        and tuple(command.get("batch_items", ()))
        == operation.batch_items
        and tuple(effect_scope.get("targets", ()))
        == operation.targets
        and tuple(effect_scope.get("batch_items", ()))
        == operation.batch_items
        and (
            (
                "payload_effects" not in effect_scope
                and "payload" not in effect_scope
            )
            or effect_scope.get(
                "payload_effects",
                effect_scope.get("payload"),
            )
            == payload
        )
    ):
        raise PermissionError("certified operation request drift")
    adapter = _registry_adapter(
        capabilities,
        request.capability_fingerprint,
    )
    declared = getattr(adapter, "fingerprint", None)
    if not (
        (
            declared is None
            or (
                isinstance(declared, tuple)
                and tuple(declared)
                == request.capability_fingerprint
            )
        )
        and callable(getattr(adapter, "execute", None))
        and (
            callable(getattr(adapter, "read_operation", None))
            or callable(getattr(adapter, "readback", None))
        )
    ):
        raise PermissionError("registered capability authority drift")
    return adapter


async def _joined_outcome(
    call: Awaitable[_T],
) -> tuple[_T | None, BaseException | None, bool]:
    task = asyncio.ensure_future(call)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            break
    try:
        return task.result(), None, cancelled
    except BaseException as error:
        return None, error, cancelled


class CanonicalProjectLiveOperationCoordinator:
    def __init__(
        self,
        *,
        prepare_facade: object,
        execution_facade: object,
        capability_registry: object,
        effect_runner: Callable[..., Awaitable[Any]],
    ) -> None:
        self._prepare = prepare_facade
        self._execution = execution_facade
        self._capabilities = capability_registry
        self._effect_runner = effect_runner
        self._tasks: dict[
            tuple[object, ...],
            asyncio.Task[str],
        ] = {}
        self._cancelled: set[tuple[object, ...]] = set()

    async def _execute_once(
        self,
        execution: TurnExecutionInput,
        authority: BoundProjectOperationAuthority,
        policy_authority: ProjectPolicyDecisionCarrier,
    ) -> str:
        if policy_authority.decision.decision is not Decision.ALLOW:
            raise ProjectToolExecutionDenied(
                "operation policy did not allow live execution"
            )
        attempt = execution.attempt
        claim = TurnClaim(
            attempt.turn_id,
            attempt.project_id,
            attempt.sequence,
            attempt.worker_id,
            attempt.attempt_id,
            attempt.lease_generation,
            attempt.fencing_token,
            attempt.lease_expires_at,
            attempt.canonical_session_id,
        )
        try:
            operation = await self._prepare.prepare(
                claim,
                authority.intent,
                authority=authority,
                policy=policy_authority.decision,
                policy_authority=policy_authority,
                approval=None,
                approval_checkpoint_id=None,
            )
            if not (
                type(operation) is ProjectOperation
                and operation.status == "approved"
                and operation.operation_id
                == authority.intent.operation_id
            ):
                raise PermissionError(
                    "prepare returned invalid operation authority"
                )
            final = await CanonicalProjectOperationExecutionCoordinator(
                execution_facade=self._execution,
                capability_registry=self._capabilities,
                effect_runner=self._effect_runner,
            )._execute_operation(execution, operation)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise ProjectOperationUnresolved(
                "live project operation did not reconcile"
            ) from exc
        if type(final) is not ProjectOperation or final.status != "reconciled":
            raise ProjectOperationUnresolved(
                "live project operation did not reconcile"
            )
        return _canonical_json(
            {
                "operation_id": final.operation_id,
                "status": "reconciled",
            }
        )

    async def execute(
        self,
        execution: TurnExecutionInput,
        authority: BoundProjectOperationAuthority,
        policy_authority: ProjectPolicyDecisionCarrier,
    ) -> str:
        if not (
            type(execution) is TurnExecutionInput
            and type(authority) is BoundProjectOperationAuthority
            and type(policy_authority)
            is ProjectPolicyDecisionCarrier
            and authority == policy_authority.operation_authority
            and authority.intent.project_id
            == execution.attempt.project_id
            and authority.intent.turn_id == execution.attempt.turn_id
        ):
            raise PermissionError("live operation authority mismatch")
        key = (
            execution.attempt.project_id,
            execution.attempt.turn_id,
            execution.attempt.attempt_id,
            authority.intent.operation_id,
        )
        if key in self._cancelled:
            raise ProjectOperationUnresolved(
                "cancelled live project operation is not replayable"
            )
        task = self._tasks.get(key)
        if task is None:
            task = asyncio.create_task(
                self._execute_once(
                    execution,
                    authority,
                    policy_authority,
                )
            )
            self._tasks[key] = task
        result, error, cancelled = await _joined_outcome(task)
        if cancelled:
            self._cancelled.add(key)
            raise asyncio.CancelledError()
        if error is not None:
            raise error
        assert type(result) is str
        return result


class CanonicalProjectOperationExecutionCoordinator:
    def __init__(
        self,
        *,
        execution_facade: object,
        capability_registry: object,
        effect_runner: Callable[..., Awaitable[Any]],
    ) -> None:
        self._execution = execution_facade
        self._capabilities = capability_registry
        self._effect_runner = effect_runner

    async def _execute_operation(
        self,
        execution: TurnExecutionInput,
        operation: ProjectOperation,
    ) -> ProjectOperation:
        request = await self._execution.certified_execution_request(
            execution,
            operation,
        )
        adapter = _request_adapter(
            execution,
            operation,
            request,
            self._capabilities,
        )
        try:
            mark = getattr(
                self._execution,
                "_mark_started_with_checkpoint",
                self._execution.mark_started,
            )
            started = await mark(request)
        except BaseException as exc:
            raise ProjectOperationUnresolved(
                "operation could not be marked started"
            ) from exc
        if not (
            type(started) is ProjectOperation
            and started.operation_id == operation.operation_id
            and started.status == "effect_started"
        ):
            raise ProjectOperationUnresolved(
                "operation start did not commit"
            )

        effect, effect_error, effect_cancelled = await _joined_outcome(
            self._effect_runner(
                adapter.execute,
                request,
                idempotency_key=operation.idempotency_key,
            )
        )
        tainted = effect_error is not None
        if effect is not None:
            if type(effect) is OperationReceipt:
                try:
                    recorded = await self._execution.record_receipt(
                        request,
                        effect,
                    )
                    if not (
                        type(recorded) is ProjectOperation
                        and recorded.operation_id
                        == operation.operation_id
                        and recorded.status == "receipt_recorded"
                    ):
                        tainted = True
                except BaseException:
                    tainted = True
            else:
                tainted = True
        readback_method = getattr(adapter, "read_operation", None)
        if not callable(readback_method):
            readback_method = getattr(adapter, "readback", None)
        if not callable(readback_method):
            raise ProjectOperationUnresolved(
                "operation readback capability is unavailable"
            )
        readback_request = OperationReadbackRequest(
            operation_id=started.operation_id,
            project_id=started.project_id,
            turn_id=started.turn_id,
            canonical_action=started.canonical_action,
            targets=started.targets,
            batch_items=started.batch_items,
            idempotency_key=started.idempotency_key,
            readback_kind=cast(str, started.readback_kind),
            receipt=effect if type(effect) is OperationReceipt else None,
            attempt_id=started.attempt_id,
            lease_generation=started.lease_generation,
            fencing_token=started.fencing_token,
        )
        readback_result, readback_error, readback_cancelled = (
            await _joined_outcome(
                self._effect_runner(
                    readback_method,
                    readback_request,
                )
            )
        )
        if (
            readback_error is not None
            or type(readback_result) is not OperationReadbackResult
        ):
            tainted = True
            readback_result = OperationReadbackResult(
                "unknown",
                None,
                None,
            )
        try:
            final = await self._execution.reconcile(
                request,
                readback_result,
            )
        except BaseException as exc:
            raise ProjectOperationUnresolved(
                "operation reconciliation failed"
            ) from exc
        if effect_cancelled or readback_cancelled:
            raise asyncio.CancelledError()
        if tainted or not (
            type(final) is ProjectOperation
            and final.operation_id == operation.operation_id
            and final.status == "reconciled"
        ):
            raise ProjectOperationUnresolved(
                "operation did not reconcile exactly"
            )
        return final

    async def execute(
        self,
        execution: TurnExecutionInput,
        operation: ProjectOperation,
    ) -> ProjectAgentRunResult:
        final = await self._execute_operation(execution, operation)
        operation_id = final.operation_id
        return ProjectAgentRunResult(
            "succeeded",
            0,
            (
                {
                    "role": "user",
                    "content": _canonical_json(
                        {
                            "kind": "project_operation_resume",
                            "operation_id": operation_id,
                        }
                    ),
                },
                {
                    "role": "assistant",
                    "content": _canonical_json(
                        {
                            "kind": "project_operation_result",
                            "operation_id": operation_id,
                            "status": "reconciled",
                        }
                    ),
                },
            ),
        )


class CanonicalApprovedOperationTurn:
    def __init__(
        self,
        execution: TurnExecutionInput,
        operation: ProjectOperation,
        *,
        base_message_count: int,
        coordinator: CanonicalProjectOperationExecutionCoordinator,
    ) -> None:
        self._execution = execution
        self._operation = operation
        self._base_message_count = base_message_count
        self._coordinator = coordinator
        self._task: asyncio.Task[ProjectAgentRunResult] | None = None
        self._cancel_requested = False
        self._result: ProjectAgentRunResult | None = None

    def request_cancel(self) -> bool:
        if (
            self._cancel_requested
            or (self._task is not None and self._task.done())
        ):
            return False
        self._cancel_requested = True
        return True

    async def wait_quiescent(self) -> None:
        if self._task is not None:
            await asyncio.gather(
                asyncio.shield(self._task),
                return_exceptions=True,
            )

    async def result(self) -> ProjectAgentRunResult:
        if self._cancel_requested and self._task is None:
            raise asyncio.CancelledError()
        if self._task is None:
            self._task = asyncio.create_task(
                self._coordinator.execute(
                    self._execution,
                    self._operation,
                )
            )
        result, error, externally_cancelled = await _joined_outcome(
            self._task
        )
        if self._cancel_requested or externally_cancelled:
            raise asyncio.CancelledError()
        if error is not None:
            raise error
        if type(result) is not ProjectAgentRunResult:
            raise ProjectOperationUnresolved(
                "approved operation returned an invalid result"
            )
        if self._result is None:
            self._result = ProjectAgentRunResult(
                result.status,
                self._base_message_count,
                result.messages,
            )
        return self._result


class CanonicalProjectOperationCheckpointCoordinator:
    def __init__(
        self,
        *,
        batches: object,
        operations: object,
        batch_id_factory: Callable[[], str],
        on_published: Callable[
            [Sequence[Mapping[str, object]]],
            object,
        ],
    ) -> None:
        self._batches = batches
        self._operations = operations
        self._batch_id_factory = batch_id_factory
        self._on_published = on_published
        self.operation_prepared = False
        self._active = False
        self._cancel_requested = False

    @property
    def cancel_requested(self) -> bool:
        return self._cancel_requested

    def request_cancel(self) -> bool:
        if not self._active or self._cancel_requested:
            return False
        self._cancel_requested = True
        return True

    async def checkpoint_operation_intent(
        self,
        execution: TurnExecutionInput,
        authority: BoundProjectOperationAuthority,
        policy_authority: ProjectPolicyDecisionCarrier,
        approval: OperationApprovalSpec,
        *,
        base_message_count: int,
        messages: Sequence[Mapping[str, object]],
    ) -> None:
        if not (
            type(execution) is TurnExecutionInput
            and type(authority) is BoundProjectOperationAuthority
            and type(policy_authority)
            is ProjectPolicyDecisionCarrier
            and type(approval) is OperationApprovalSpec
            and authority == policy_authority.operation_authority
            and policy_authority.decision.decision
            is Decision.REQUIRE_APPROVAL
            and authority.intent.project_id
            == execution.attempt.project_id
            and authority.intent.turn_id == execution.attempt.turn_id
            and type(base_message_count) is int
            and base_message_count >= 0
            and isinstance(messages, Sequence)
            and not isinstance(messages, (str, bytes))
            and all(
                isinstance(message, Mapping)
                and "tool_calls" not in message
                for message in messages
            )
        ):
            raise ProjectCheckpointFailed(
                "invalid approval checkpoint authority"
            )
        if self._active:
            raise ProjectCheckpointSettlementPending(
                "approval checkpoint is already active"
            )
        self._active = True
        self._cancel_requested = False
        self.operation_prepared = False
        attempt = execution.attempt
        claim = TurnClaim(
            attempt.turn_id,
            attempt.project_id,
            attempt.sequence,
            attempt.worker_id,
            attempt.attempt_id,
            attempt.lease_generation,
            attempt.fencing_token,
            attempt.lease_expires_at,
            attempt.canonical_session_id,
        )
        try:
            batch_id = self._batch_id_factory()
            if not _is_canonical_uuid4(batch_id):
                raise ProjectCheckpointFailed(
                    "checkpoint batch id is invalid"
                )

            pending, pending_error, pending_cancelled = (
                await _joined_outcome(
                    self._batches.prepare_approval_checkpoint(
                        claim,
                        batch_id=batch_id,
                        operation_id=authority.intent.operation_id,
                        approval_id=approval.approval_id,
                        base_message_count=base_message_count,
                        messages=tuple(messages),
                    )
                )
            )
            if pending_cancelled:
                self._cancel_requested = True
            if pending_error is not None or not (
                type(pending) is PendingProjectBatch
                and pending.batch_id == batch_id
                and pending.kind == "approval_checkpoint"
                and pending.state == "prepared"
                and pending.attempt == execution.attempt
                and pending.operation_id
                == authority.intent.operation_id
                and pending.approval_id == approval.approval_id
                and pending.base_message_count == base_message_count
            ):
                raise ProjectCheckpointFailed(
                    "approval checkpoint could not be prepared"
                ) from pending_error

            operation, operation_error, operation_cancelled = (
                await _joined_outcome(
                    self._operations.prepare(
                        claim,
                        authority.intent,
                        authority=authority,
                        policy=policy_authority.decision,
                        policy_authority=policy_authority,
                        approval=approval,
                        approval_checkpoint_id=batch_id,
                    )
                )
            )
            if operation_cancelled:
                self._cancel_requested = True
            if operation_error is not None or not (
                type(operation) is ProjectOperation
                and operation.operation_id
                == authority.intent.operation_id
                and operation.project_id == attempt.project_id
                and operation.turn_id == attempt.turn_id
                and operation.status == "awaiting_approval"
                and operation.approval_id == approval.approval_id
            ):
                raise ProjectCheckpointFailed(
                    "approval operation could not be prepared"
                ) from operation_error
            self.operation_prepared = True

            applied, apply_error, apply_cancelled = (
                await _joined_outcome(
                    self._batches.apply_project_batch(batch_id)
                )
            )
            if apply_cancelled:
                self._cancel_requested = True
            if apply_error is not None or type(
                applied
            ) is not ProjectBatchApplyResult:
                raise ProjectCheckpointSettlementPending(
                    "approval checkpoint settlement is pending"
                ) from apply_error
            if self._cancel_requested or applied.outcome != "published":
                raise ProjectCheckpointSettlementPending(
                    "approval checkpoint settlement is pending"
                )

            published = self._on_published(tuple(messages))
            if inspect.isawaitable(published):
                publish_result, publish_error, publish_cancelled = (
                    await _joined_outcome(published)
                )
                del publish_result
                if publish_cancelled:
                    self._cancel_requested = True
                if publish_error is not None or self._cancel_requested:
                    raise ProjectCheckpointSettlementPending(
                        "approval publication baseline is pending"
                    ) from publish_error
            raise ProjectApprovalPublished(
                "approval checkpoint published"
            )
        except (
            ProjectApprovalPublished,
            ProjectCheckpointFailed,
            ProjectCheckpointSettlementPending,
        ):
            raise
        except BaseException as exc:
            signal = (
                ProjectCheckpointSettlementPending
                if self.operation_prepared
                else ProjectCheckpointFailed
            )
            raise signal("approval checkpoint failed") from exc
        finally:
            self._active = False


class ProjectRuntimeExecutionPort(ProjectRuntimeCloserPort, Protocol):
    async def mark_turn_started(self, claim: TurnClaim) -> TurnClaim: ...

    async def execution_input_for_claim(
        self,
        claim: TurnClaim,
    ) -> TurnExecutionInput: ...

    async def heartbeat_turn(
        self,
        claim: TurnClaim,
        *,
        lease_seconds: int,
    ) -> TurnClaim: ...


class ProjectBatchWorkerPort(ProjectBatchApplyPort, Protocol):
    async def load_project_history(
        self,
        session_id: str,
    ) -> ProjectHistorySnapshot: ...

    async def prepare_terminal_result(
        self,
        claim: TurnClaim,
        *,
        batch_id: str,
        status: Literal["succeeded", "failed"],
        base_message_count: int,
        messages: Sequence[Mapping[str, object]],
    ) -> PendingProjectBatch: ...

    async def prepare_approval_checkpoint(
        self,
        claim: TurnClaim,
        *,
        batch_id: str,
        operation_id: str,
        approval_id: str,
        base_message_count: int,
        messages: Sequence[Mapping[str, object]],
    ) -> PendingProjectBatch: ...

    async def apply_project_batch(
        self,
        batch_id: str,
    ) -> ProjectBatchApplyResult: ...


class ProjectAgentTurn(QuiescingRunner, Protocol):
    async def result(self) -> ProjectAgentRunResult: ...


class ProjectAgent(Protocol):
    def create_turn(
        self,
        execution: TurnExecutionInput,
        operation: ProjectOperation | None,
    ) -> ProjectAgentTurn: ...


class ProjectAgentBuild(Protocol):
    @property
    def revisions(self) -> ProjectAgentRevisions: ...

    async def create_project_agent(
        self,
        *,
        history: ProjectHistorySnapshot,
    ) -> ProjectAgent: ...


class ProjectAgentFactory(Protocol):
    async def resolve_project_agent(
        self,
        *,
        context: SessionContext,
        contract_revision: int,
    ) -> ProjectAgentBuild: ...

    async def release_project_agent(
        self,
        agent: ProjectAgent,
    ) -> None: ...


class _GatewayProjectTurn:
    def __init__(
        self,
        agent: "_GatewayProjectAgent",
        execution: TurnExecutionInput,
        canonical_payload: str,
    ) -> None:
        if type(canonical_payload) is not str or not canonical_payload:
            raise ValueError("project turn payload is not canonical JSON")
        self._agent = agent
        self._execution = execution
        self._canonical_payload = canonical_payload
        self._task: asyncio.Task[ProjectAgentRunResult] | None = None
        self._cancel_requested = False
        self._interrupt_sent = False

    def request_cancel(self) -> bool:
        if self._cancel_requested:
            return False
        self._cancel_requested = True
        if self._task is not None and not self._task.done():
            self._interrupt_sent = True
            self._agent._raw.interrupt()
        return True

    async def result(self) -> ProjectAgentRunResult:
        if self._task is None:
            self._task = asyncio.create_task(self._run())
        return await asyncio.shield(self._task)

    async def wait_quiescent(self) -> None:
        task = self._task
        if task is None:
            return
        await asyncio.gather(asyncio.shield(task), return_exceptions=True)

    async def _run(self) -> ProjectAgentRunResult:
        if self._cancel_requested:
            raise asyncio.CancelledError()
        return await self._agent._run_turn(self)


_PROJECT_RESULT_HARNESS_KEYS = frozenset(
    {
        "completed",
        "failed",
        "interrupted",
        "partial",
        "error",
        "final_response",
        "messages",
        "session_id",
        "agent_persisted",
    }
)
_PROJECT_RESULT_RICH_BASE_KEYS = frozenset(
    {
        "final_response",
        "last_reasoning",
        "messages",
        "api_calls",
        "completed",
        "turn_exit_reason",
        "failed",
        "partial",
        "interrupted",
        "response_transformed",
        "response_previewed",
        "model",
        "provider",
        "base_url",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "reasoning_tokens",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "last_prompt_tokens",
        "estimated_cost_usd",
        "cost_status",
        "cost_source",
        "service_tier",
        "session_id",
    }
)
_PROJECT_RESULT_RICH_CONDITIONAL_KEYS = frozenset(
    {
        "guardrail",
        "cleanup_errors",
        "pending_steer",
        "interrupt_message",
    }
)


def _is_project_iteration_exit_reason(value: object) -> bool:
    if value == "budget_exhausted":
        return True
    if type(value) is not str:
        return False
    prefix = "max_iterations_reached("
    if not value.startswith(prefix) or not value.endswith(")"):
        return False
    used, separator, maximum = value[len(prefix) : -1].partition("/")
    if (
        separator != "/"
        or not used.isdecimal()
        or not maximum.isdecimal()
    ):
        return False
    return (
        str(int(used)) == used
        and str(int(maximum)) == maximum
        and int(maximum) > 0
    )


class _GatewayProjectAgent:
    def __init__(
        self,
        *,
        raw: object,
        history: ProjectHistorySnapshot,
        off_loop_runner: Callable[..., Awaitable[Any]],
        turn_context_binder: Callable[[TurnExecutionInput], object],
        execution_gate: object | None = None,
    ) -> None:
        if not (
            type(history) is ProjectHistorySnapshot
            and type(history.messages) is tuple
            and type(history.message_count) is int
            and history.message_count >= 0
        ):
            raise ValueError("invalid project history snapshot")
        self._raw = raw
        self._off_loop_runner = off_loop_runner
        self._turn_context_binder = turn_context_binder
        self._execution_gate = execution_gate
        self._session_id = history.session_id
        self._base_message_count = history.message_count
        self._messages: tuple[Mapping[str, object], ...] = tuple(
            json.loads(
                _canonical_json(tuple(history.messages))
            )
        )
        self._turns: set[_GatewayProjectTurn] = set()
        self._run_lock = asyncio.Lock()
        self._closed = False

    def create_turn(
        self,
        execution: TurnExecutionInput,
        operation: ProjectOperation | None,
    ) -> ProjectAgentTurn:
        if self._closed:
            raise RuntimeError("project agent is closed")
        if type(execution) is not TurnExecutionInput:
            raise ProjectToolExecutionDenied(
                "execution must be TurnExecutionInput"
            )
        if operation is not None:
            raise PermissionError("approved operations use the dedicated port")
        canonical_payload = _canonical_payload_json(execution.payload)
        turn = _GatewayProjectTurn(
            self,
            execution,
            canonical_payload,
        )
        self._turns.add(turn)
        return turn

    @staticmethod
    def _invoke_raw(
        raw: object,
        binder: Callable[[TurnExecutionInput], object],
        execution: TurnExecutionInput,
        payload: str,
        history: tuple[Mapping[str, object], ...],
        execution_gate: object | None,
        base_message_count: int,
    ) -> object:
        def run_raw() -> object:
            return raw.run_conversation(
                user_message=payload,
                conversation_history=history,
            )

        with binder(execution):
            if execution_gate is None:
                return copy_context().run(run_raw)
            bind_execution = getattr(
                execution_gate,
                "bind_execution",
                None,
            )
            if not callable(bind_execution):
                raise RuntimeError(
                    "project execution gate cannot bind a turn"
                )
            with bind_execution(
                execution,
                base_message_count=base_message_count,
            ):
                return copy_context().run(run_raw)

    @staticmethod
    def _normalize_result(
        value: object,
        *,
        session_id: str,
        history: tuple[Mapping[str, object], ...],
        payload: str,
        base_message_count: int,
    ) -> ProjectAgentRunResult:
        if type(value) is not dict:
            raise TypeError("project agent returned a non-mapping result")
        result_keys = frozenset(value)
        if result_keys == _PROJECT_RESULT_HARNESS_KEYS:
            result_family = "harness"
            error = value["error"]
            turn_exit_reason = None
        elif (
            _PROJECT_RESULT_RICH_BASE_KEYS.issubset(result_keys)
            and result_keys.issubset(
                _PROJECT_RESULT_RICH_BASE_KEYS
                | _PROJECT_RESULT_RICH_CONDITIONAL_KEYS
            )
        ):
            result_family = "rich"
            error = None
            turn_exit_reason = value["turn_exit_reason"]
        else:
            raise ValueError(
                "project agent returned unknown result fields: "
                f"{sorted(result_keys)}"
            )
        completed = value["completed"]
        failed = value["failed"]
        interrupted = value["interrupted"]
        partial = value["partial"]
        response = value["final_response"]
        if not (
            type(completed) is bool
            and type(failed) is bool
            and type(interrupted) is bool
            and type(partial) is bool
            and (error is None or type(error) is str)
            and value["session_id"] == session_id
            and (
                result_family != "harness"
                or type(value["agent_persisted"]) is bool
            )
            and (
                result_family != "rich"
                or type(turn_exit_reason) is str
            )
            and isinstance(value["messages"], Sequence)
            and not isinstance(value["messages"], (str, bytes))
        ):
            raise ValueError("project agent returned malformed evidence")
        rows = tuple(value["messages"])
        if (
            len(rows) < len(history)
            or rows[: len(history)] != history
            or not all(isinstance(row, Mapping) for row in rows)
        ):
            raise ValueError("project agent rewrote canonical history")
        if completed:
            valid_outcome = (
                not failed
                and not interrupted
                and not partial
                and error is None
                and type(response) is str
            )
            status: Literal["succeeded", "failed"] = "succeeded"
        else:
            if result_family == "rich":
                valid_outcome = (
                    not (failed and interrupted)
                    and (
                        failed
                        or interrupted
                        or partial
                        or _is_project_iteration_exit_reason(
                            turn_exit_reason
                        )
                    )
                    and (response is None or type(response) is str)
                )
            else:
                valid_outcome = (
                    (failed or interrupted)
                    and not (failed and interrupted)
                    and (response is None or type(response) is str)
                    and type(error) is str
                )
            status = "failed"
        if not valid_outcome:
            raise ValueError("project agent returned contradictory evidence")
        detached = (
            {"role": "user", "content": payload},
            {
                "role": "assistant",
                "content": response if type(response) is str else "",
            },
        )
        return ProjectAgentRunResult(
            status,
            base_message_count,
            detached,
        )

    async def _run_turn(
        self,
        turn: _GatewayProjectTurn,
    ) -> ProjectAgentRunResult:
        payload = turn._canonical_payload
        async with self._run_lock:
            if turn._cancel_requested:
                raise asyncio.CancelledError()
            history = self._messages
            value = await self._off_loop_runner(
                self._invoke_raw,
                self._raw,
                self._turn_context_binder,
                turn._execution,
                payload,
                history,
                self._execution_gate,
                self._base_message_count,
            )
            if turn._cancel_requested:
                raise asyncio.CancelledError()
            result = self._normalize_result(
                value,
                session_id=self._session_id,
                history=history,
                payload=payload,
                base_message_count=self._base_message_count,
            )
            self._messages = history + result.messages
            self._base_message_count += len(result.messages)
            return result

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for turn in tuple(self._turns):
            if turn._task is not None and not turn._task.done():
                turn.request_cancel()
        for turn in tuple(self._turns):
            await turn.wait_quiescent()
        await self._off_loop_runner(self._raw.close)


class _GatewayProjectBuild:
    def __init__(
        self,
        factory: "GatewayProjectAgentFactory",
        snapshot: object,
        revisions: ProjectAgentRevisions,
    ) -> None:
        self._factory = factory
        self._snapshot = snapshot
        self._revisions = revisions

    @property
    def revisions(self) -> ProjectAgentRevisions:
        return self._revisions

    async def create_project_agent(
        self,
        *,
        history: ProjectHistorySnapshot,
    ) -> ProjectAgent:
        return await self._factory._create_agent(self._snapshot, history)


class _GatewayProjectExecutionGate:
    def __init__(self, authorizer: object, checkpoint: object) -> None:
        self._authorizer = authorizer
        self._checkpoint = checkpoint
        self._owner_loop = asyncio.get_running_loop()
        self._execution: TurnExecutionInput | None = None
        self._base_message_count: int | None = None

    @property
    def owner_loop(self) -> asyncio.AbstractEventLoop:
        return self._owner_loop

    @property
    def execution(self) -> TurnExecutionInput | None:
        return self._execution

    @property
    def base_message_count(self) -> int | None:
        return self._base_message_count

    def bind_execution(
        self,
        execution: TurnExecutionInput,
        *,
        base_message_count: int,
    ) -> object:
        gate = self

        class BoundExecution:
            def __enter__(self) -> TurnExecutionInput:
                if gate._execution is not None:
                    raise RuntimeError(
                        "project execution gate is already bound"
                    )
                gate._execution = execution
                gate._base_message_count = base_message_count
                return execution

            def __exit__(
                self,
                exc_type: object,
                exc: object,
                traceback: object,
            ) -> bool:
                gate._execution = None
                gate._base_message_count = None
                return False

        return BoundExecution()

    def request_cancel(self) -> bool:
        cancel = getattr(self._checkpoint, "request_cancel", None)
        return bool(cancel()) if callable(cancel) else False

    def __getattr__(self, name: str) -> object:
        try:
            return getattr(self._authorizer, name)
        except AttributeError:
            return getattr(self._checkpoint, name)


class GatewayProjectAgentFactory:
    def __init__(
        self,
        *,
        snapshot_resolver: Callable[[SessionContext, int], object],
        agent_builder: Callable[..., object],
        off_loop_runner: Callable[..., Awaitable[Any]],
        turn_context_binder: Callable[[TurnExecutionInput], object],
        tool_authorizer: object,
        checkpoint_coordinator: object,
    ) -> None:
        if not all(
            callable(value)
            for value in (
                snapshot_resolver,
                agent_builder,
                off_loop_runner,
                turn_context_binder,
            )
        ):
            raise TypeError("project agent factory dependencies must be callable")
        self._snapshot_resolver = snapshot_resolver
        self._agent_builder = agent_builder
        self._off_loop_runner = off_loop_runner
        self._turn_context_binder = turn_context_binder
        self._tool_authorizer = tool_authorizer
        self._checkpoint_coordinator = checkpoint_coordinator

    @staticmethod
    def _validate_snapshot(snapshot: object) -> ProjectAgentRevisions:
        revisions = getattr(snapshot, "revisions", None)
        if not (
            type(revisions) is ProjectAgentRevisions
            and getattr(snapshot, "runtime_kind", None) == "hermes"
            and getattr(snapshot, "registry_generation", None)
            == getattr(snapshot, "declared_registry_generation", None)
            and revisions.base_signature
            == getattr(snapshot, "base_signature", None)
            == getattr(snapshot, "declared_base_signature", None)
            and revisions.tool_revision
            == getattr(snapshot, "tool_revision", None)
            == getattr(snapshot, "declared_tool_revision", None)
            and revisions.model_revision
            == getattr(snapshot, "model_revision", None)
            == getattr(snapshot, "declared_model_revision", None)
            and isinstance(
                getattr(snapshot, "constructor_kwargs", None),
                Mapping,
            )
            and type(getattr(snapshot, "tool_descriptors", None)) is tuple
        ):
            raise ValueError("invalid frozen project agent snapshot")
        return revisions

    @staticmethod
    def _bind_canonical_session(raw: object, session_id: str) -> None:
        missing = object()
        marker = getattr(
            raw,
            "_project_canonical_session_id",
            missing,
        )
        if marker is missing:
            setattr(raw, "session_id", session_id)
            session_readback = getattr(raw, "session_id", None)
            if (
                type(session_readback) is not str
                or session_readback != session_id
            ):
                raise ValueError(
                    "project agent rejected its canonical session"
                )
            setattr(raw, "_project_canonical_session_id", session_id)
        elif type(marker) is not str or marker != session_id:
            raise ValueError(
                "project agent cannot be reused across sessions"
            )
        marker_readback = getattr(
            raw,
            "_project_canonical_session_id",
            None,
        )
        session_readback = getattr(raw, "session_id", None)
        if (
            type(marker_readback) is not str
            or marker_readback != session_id
            or type(session_readback) is not str
            or session_readback != session_id
        ):
            raise ValueError(
                "project agent canonical session readback failed"
            )

    async def _close_failed_raw(self, raw: object) -> None:
        try:
            close = getattr(raw, "close", None)
            if callable(close):
                await _await_retained_io(
                    self._off_loop_runner(close)
                )
        except BaseException as error:
            logger.warning(
                "project agent cleanup after construction failure failed",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def resolve_project_agent(
        self,
        *,
        context: SessionContext,
        contract_revision: int,
    ) -> ProjectAgentBuild:
        snapshot = await self._off_loop_runner(
            self._snapshot_resolver,
            context,
            contract_revision,
        )
        revisions = self._validate_snapshot(snapshot)
        return _GatewayProjectBuild(self, snapshot, revisions)

    async def _create_agent(
        self,
        snapshot: object,
        history: ProjectHistorySnapshot,
    ) -> ProjectAgent:
        canonical_session_id = getattr(history, "session_id", None)
        if (
            type(canonical_session_id) is not str
            or not canonical_session_id
        ):
            raise ValueError(
                "project history has no canonical session"
            )
        execution_gate = _GatewayProjectExecutionGate(
            self._tool_authorizer,
            self._checkpoint_coordinator,
        )
        constructor = dict(snapshot.constructor_kwargs)
        constructor.update(
            {
                "session_db": None,
                "save_trajectories": False,
                "quiet_mode": True,
                "skip_context_files": True,
                "skip_memory": True,
                "streaming_callback": None,
                "delivery_callback": None,
                "approval_notifier": None,
                "provider_metadata_prewarm": False,
                "external_memory_sync": False,
                "memory_review": False,
                "skill_review": False,
                "plugin_lifecycle": False,
                "project_execution_gate": execution_gate,
                "session_id": canonical_session_id,
            }
        )
        raw = await self._off_loop_runner(
            self._agent_builder,
            snapshot,
            **constructor,
        )
        if getattr(raw, "project_execution_gate", None) is not execution_gate:
            await self._close_failed_raw(raw)
            raise ValueError("project agent execution gate mismatch")
        try:
            self._bind_canonical_session(
                raw,
                canonical_session_id,
            )
        except BaseException:
            await self._close_failed_raw(raw)
            raise
        for name, value in (
            ("_persist_disabled", True),
            ("_session_db", None),
            ("_session_json_enabled", False),
            ("_end_session_on_close", False),
            ("compression_enabled", False),
            ("_memory_nudge_interval", 0),
            ("_skill_nudge_interval", 0),
            ("background_review_callback", None),
        ):
            setattr(raw, name, value)
        if not all(
            callable(getattr(raw, name, None))
            for name in ("run_conversation", "interrupt", "close")
        ):
            raise TypeError("agent builder returned an invalid agent")
        return _GatewayProjectAgent(
            raw=raw,
            history=history,
            off_loop_runner=self._off_loop_runner,
            turn_context_binder=self._turn_context_binder,
            execution_gate=execution_gate,
        )

    async def release_project_agent(
        self,
        agent: ProjectAgent,
    ) -> None:
        if type(agent) is not _GatewayProjectAgent:
            raise TypeError("agent does not belong to this factory")
        await agent.close()


def canonical_uuid4() -> str:
    return str(uuid.uuid4())


def project_agent_cache_key(
    profile_home: Path,
    project_id: str,
    canonical_session_id: str,
) -> str:
    if not isinstance(profile_home, Path) or not all(
        type(value) is str and bool(value)
        for value in (project_id, canonical_session_id)
    ):
        raise ValueError("invalid project agent cache identity")
    normalized = os.path.normcase(os.path.realpath(profile_home))
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return (
        f"project:v1:{digest}:{project_id}:{canonical_session_id}"
    )


def project_agent_cache_signature(
    revisions: ProjectAgentRevisions,
    contract_revision: int,
) -> str:
    if not (
        type(revisions) is ProjectAgentRevisions
        and all(
            type(value) is str and bool(value)
            for value in (
                revisions.base_signature,
                revisions.tool_revision,
                revisions.model_revision,
            )
        )
        and type(contract_revision) is int
        and contract_revision >= 0
    ):
        raise ValueError("invalid project agent revisions")
    encoded = json.dumps(
        [
            revisions.base_signature,
            contract_revision,
            revisions.tool_revision,
            revisions.model_revision,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass
class _IdleAgent:
    signature: str
    message_count: int
    agent: ProjectAgent


@dataclass
class _LiveEntry:
    handle: ProjectRuntimeLiveHandle
    durable_stop_event: asyncio.Event
    accepted_stop_control_version: int | None
    heartbeat_stop_event: asyncio.Event
    heartbeat_task: asyncio.Task[None] | None
    current_claim: TurnClaim
    watch_error: BaseException | None = None


def _live_key_from_claim(claim: TurnClaim) -> tuple[object, ...]:
    return (
        claim.project_id,
        claim.turn_id,
        claim.attempt_id,
        claim.worker_id,
        claim.lease_generation,
        claim.fencing_token,
        claim.canonical_session_id,
    )


def _live_key_from_stop(request: StopRequest) -> tuple[object, ...]:
    return (
        request.project_id,
        request.turn_id,
        request.attempt_id,
        request.worker_id,
        request.lease_generation,
        request.fencing_token,
        request.canonical_session_id,
    )


def _canonical_result_messages(
    result: ProjectAgentRunResult,
    expected_base: int,
) -> tuple[Mapping[str, object], ...]:
    if not (
        type(result) is ProjectAgentRunResult
        and type(result.status) is str
        and result.status in {"succeeded", "failed"}
        and type(result.base_message_count) is int
        and result.base_message_count >= 0
        and result.base_message_count == expected_base
        and type(result.messages) is tuple
        and all(isinstance(message, Mapping) for message in result.messages)
    ):
        raise ValueError("invalid project agent run result")
    try:
        from hermes_state import _canonical_terminal_transcript

        raw = _canonical_terminal_transcript(result.messages)
        detached = json.loads(raw)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("invalid project agent transcript") from exc
    if type(detached) is not list or not all(
        type(message) is dict for message in detached
    ):
        raise ValueError("invalid project agent transcript")
    return tuple(detached)


def _require_worker_start(start: object) -> WorkerStart:
    from hermes_cli.project_operations import ProjectOperation
    from hermes_cli.project_runtime import DispatcherLease

    if type(start) is not WorkerStart:
        raise TypeError("start must be a WorkerStart")
    claim = start.claim
    lease = start.dispatcher_lease
    if not (
        type(claim) is TurnClaim
        and all(
            type(value) is str and bool(value)
            for value in (
                claim.project_id,
                claim.turn_id,
                claim.worker_id,
                claim.attempt_id,
                claim.canonical_session_id,
            )
        )
        and all(
            type(value) is int and value > 0
            for value in (
                claim.sequence,
                claim.lease_generation,
                claim.fencing_token,
            )
        )
        and type(claim.lease_expires_at) is int
        and claim.lease_expires_at >= 0
        and type(lease) is DispatcherLease
        and type(lease.instance_id) is str
        and bool(lease.instance_id)
        and all(
            type(value) is int and value > 0
            for value in (
                lease.generation,
                lease.fencing_token,
                lease.expires_at,
            )
        )
        and (
            (
                start.source == "queued_turn"
                and type(start.source) is str
                and start.operation is None
            )
            or (
                start.source == "approved_operation"
                and type(start.source) is str
                and type(start.operation) is ProjectOperation
            )
        )
    ):
        raise ValueError("invalid dispatcher worker start")
    return start


def _is_canonical_uuid4(value: object) -> bool:
    if type(value) is not str:
        return False
    try:
        parsed = uuid.UUID(value)
    except (TypeError, ValueError, AttributeError):
        return False
    return (
        parsed.version == 4
        and parsed.variant == uuid.RFC_4122
        and str(parsed) == value
    )


_SUPPORTED_BATCH_OUTCOMES = frozenset(
    {
        "wait",
        "published",
        "discarded",
        "conflicted",
        "already_published",
        "already_discarded",
        "already_conflicted",
        "settlement_pending",
        "remediation_pending",
        "state_conflict",
        "authority_conflict",
    }
)
_MAX_PERSISTED_TIMESTAMP = 253402300799.0


def _claim_attempt(claim: TurnClaim) -> TurnAttemptIdentity:
    return TurnAttemptIdentity(
        claim.project_id,
        claim.turn_id,
        claim.sequence,
        claim.worker_id,
        claim.attempt_id,
        claim.lease_generation,
        claim.fencing_token,
        claim.canonical_session_id,
        claim.lease_expires_at,
    )


def _require_marked_claim(
    start_claim: TurnClaim,
    current_claim: object,
) -> TurnClaim:
    if type(current_claim) is not TurnClaim:
        raise ValueError("mark_turn_started returned an invalid claim")
    if (
        (
            current_claim.project_id,
            current_claim.turn_id,
            current_claim.sequence,
            current_claim.worker_id,
            current_claim.attempt_id,
            current_claim.lease_generation,
            current_claim.fencing_token,
            current_claim.canonical_session_id,
        )
        != (
            start_claim.project_id,
            start_claim.turn_id,
            start_claim.sequence,
            start_claim.worker_id,
            start_claim.attempt_id,
            start_claim.lease_generation,
            start_claim.fencing_token,
            start_claim.canonical_session_id,
        )
        or type(current_claim.lease_expires_at) is not int
        or current_claim.lease_expires_at < start_claim.lease_expires_at
    ):
        raise ValueError("mark_turn_started changed claim identity")
    return current_claim


def _require_prepared_terminal(
    prepared: object,
    *,
    claim: TurnClaim,
    batch_id: str,
    status: str,
    base_message_count: int,
) -> PendingProjectBatch:
    expected_attempt = _claim_attempt(claim)
    if not (
        type(prepared) is PendingProjectBatch
        and type(prepared.batch_id) is str
        and prepared.batch_id == batch_id
        and type(prepared.batch_creation_sequence) is int
        and prepared.batch_creation_sequence > 0
        and type(prepared.kind) is str
        and prepared.kind == "terminal_result"
        and type(prepared.state) is str
        and prepared.state == "prepared"
        and type(prepared.attempt) is TurnAttemptIdentity
        and prepared.attempt == expected_attempt
        and type(prepared.terminal_status) is str
        and prepared.terminal_status == status
        and prepared.operation_id is None
        and prepared.approval_id is None
        and type(prepared.base_message_count) is int
        and prepared.base_message_count == base_message_count
        and type(prepared.created_at) in {int, float}
        and math.isfinite(prepared.created_at)
        and 0 <= prepared.created_at <= _MAX_PERSISTED_TIMESTAMP
    ):
        raise ValueError("invalid prepared project terminal batch")
    return prepared


def _require_apply_result(
    result: object,
) -> ProjectBatchApplyResult:
    if not (
        type(result) is ProjectBatchApplyResult
        and type(result.outcome) is str
        and result.outcome in _SUPPORTED_BATCH_OUTCOMES
    ):
        raise ValueError("invalid project batch apply result")
    return result


class CanonicalProjectRuntimeWorker:
    """Execute only dispatcher-issued starts through the C11 closer."""

    def __init__(
        self,
        runtime: ProjectRuntimeExecutionPort,
        batches: ProjectBatchWorkerPort,
        agents: ProjectAgentFactory,
        config: GatewayConfig,
        *,
        profile_home: Path,
        lease_seconds: int,
        heartbeat_interval_seconds: float,
        batch_id_factory: Callable[[], str] = canonical_uuid4,
        cache_capacity: int = 128,
        approved_operations: ApprovedOperationExecutionPort | None = None,
    ) -> None:
        if not (
            isinstance(config, GatewayConfig)
            and isinstance(profile_home, Path)
            and type(lease_seconds) is int
            and lease_seconds > 0
            and type(heartbeat_interval_seconds) in {int, float}
            and not isinstance(heartbeat_interval_seconds, bool)
            and math.isfinite(heartbeat_interval_seconds)
            and heartbeat_interval_seconds > 0
            and callable(batch_id_factory)
            and type(cache_capacity) is int
            and cache_capacity > 0
        ):
            raise ValueError("invalid canonical project worker configuration")
        if not all(
            callable(getattr(runtime, name, None))
            for name in (
                "mark_turn_started",
                "execution_input_for_claim",
                "heartbeat_turn",
                "control_for_claim",
                "commit_turn_with_task7_batch",
                "acknowledge_stopped",
            )
        ):
            raise TypeError("runtime does not implement the worker port")
        if not all(
            callable(getattr(batches, name, None))
            for name in (
                "load_project_history",
                "prepare_terminal_result",
                "prepare_approval_checkpoint",
                "apply_project_batch",
            )
        ):
            raise TypeError("batches does not implement the worker port")
        if not all(
            callable(getattr(agents, name, None))
            for name in (
                "resolve_project_agent",
                "release_project_agent",
            )
        ):
            raise TypeError("agents does not implement the worker port")
        if (
            approved_operations is not None
            and not callable(
                getattr(approved_operations, "create_turn", None)
            )
        ):
            raise TypeError(
                "approved operations does not implement the execution port"
            )
        self._runtime = runtime
        self._batches = batches
        self._agents = agents
        self._config = config
        self._profile_home = profile_home
        self._lease_seconds = lease_seconds
        self._heartbeat_interval_seconds = float(
            heartbeat_interval_seconds
        )
        self._batch_id_factory = batch_id_factory
        self._cache_capacity = cache_capacity
        self._approved_operations = approved_operations
        self._cache: OrderedDict[str, _IdleAgent] = OrderedDict()
        self._live: dict[tuple[object, ...], _LiveEntry] = {}
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._closer = ProjectRuntimeTerminalCloser(runtime, batches)
        self._closing = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def _require_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError("project worker belongs to another event loop")

    async def _reap_task(
        self,
        task: asyncio.Task[object],
    ) -> tuple[bool, BaseException | None]:
        """Await a cleanup task without letting caller cancellation orphan it."""
        current = asyncio.current_task()
        cancellations = current.cancelling() if current is not None else 0
        externally_cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not task.done():
                    externally_cancelled = True
            except BaseException:
                break
        if (
            current is not None
            and current.cancelling() > cancellations
        ):
            externally_cancelled = True
        try:
            task.result()
        except BaseException as error:
            return externally_cancelled, error
        return externally_cancelled, None

    async def _release(self, agent: ProjectAgent) -> tuple[bool, bool]:
        release_task = asyncio.create_task(
            self._agents.release_project_agent(agent)
        )
        externally_cancelled, error = await self._reap_task(release_task)
        if error is not None:
            logger.warning(
                "project agent release failed",
                exc_info=(type(error), error, error.__traceback__),
            )
            return False, externally_cancelled
        return True, externally_cancelled

    async def _watch(self, entry: _LiveEntry, start: WorkerStart) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    entry.heartbeat_stop_event.wait(),
                    timeout=self._heartbeat_interval_seconds,
                )
                return
            except asyncio.TimeoutError:
                pass
            try:
                control = await self._runtime.control_for_claim(
                    entry.current_claim
                )
                if control.state == "stop_requested":
                    request = StopRequest(
                        start.claim.project_id,
                        start.claim.turn_id,
                        start.claim.attempt_id,
                        start.claim.worker_id,
                        start.claim.lease_generation,
                        start.claim.fencing_token,
                        start.claim.canonical_session_id,
                        control.control_version,
                    )
                    self.request_stop(request)
                    return
                if control.state not in {"running", "awaiting_approval"}:
                    raise ProjectRuntimeError(
                        RuntimeErrorCode.STALE_TURN_CLAIM,
                        project_id=start.claim.project_id,
                        turn_id=start.claim.turn_id,
                    )
                entry.current_claim = replace(
                    entry.current_claim,
                    lease_expires_at=control.lease_expires_at,
                )
                entry.current_claim = await self._runtime.heartbeat_turn(
                    entry.current_claim,
                    lease_seconds=self._lease_seconds,
                )
            except BaseException as exc:
                if isinstance(exc, asyncio.CancelledError):
                    raise
                entry.watch_error = exc
                try:
                    entry.handle.request_cancel()
                except BaseException:
                    # The watch failure remains the primary outcome; the
                    # cancellation latch prevents cleanup from retrying it.
                    pass
                return

    async def _stop_watch(
        self,
        entry: _LiveEntry,
    ) -> tuple[bool, BaseException | None]:
        entry.heartbeat_stop_event.set()
        task = entry.heartbeat_task
        if task is None:
            return False, None
        if not task.done():
            task.cancel()
        externally_cancelled, error = await self._reap_task(task)
        if isinstance(error, asyncio.CancelledError):
            error = None
        return externally_cancelled, error

    async def _wait_for_quiescence(
        self,
        handle: ProjectRuntimeLiveHandle,
    ) -> tuple[bool, BaseException | None]:
        return await self._reap_task(
            asyncio.create_task(handle.wait_quiescent())
        )

    async def _check_in(
        self,
        cache_key: str,
        idle: _IdleAgent,
        entry: _LiveEntry,
    ) -> tuple[bool, bool]:
        """Release all displaced owners before exposing the new idle owner."""
        while True:
            victims: list[ProjectAgent] = []
            displaced = self._cache.pop(cache_key, None)
            if displaced is not None and displaced.agent is not idle.agent:
                victims.append(displaced.agent)
            while len(self._cache) >= self._cache_capacity:
                _, evicted = self._cache.popitem(last=False)
                if evicted.agent is not idle.agent:
                    victims.append(evicted.agent)
            if not victims:
                if (
                    entry.durable_stop_event.is_set()
                    or entry.handle._has_stop_call()
                ):
                    return False, False
                self._cache[cache_key] = idle
                return True, False
            release_failed = False
            externally_cancelled = False
            for victim in victims:
                released, victim_cancelled = await self._release(victim)
                externally_cancelled = (
                    externally_cancelled or victim_cancelled
                )
                if not released:
                    release_failed = True
            if externally_cancelled:
                return False, True
            if release_failed:
                return False, False

    def request_stop(self, request: StopRequest) -> bool:
        if type(request) is not StopRequest:
            return False
        entry = self._live.get(_live_key_from_stop(request))
        if entry is None:
            return False
        try:
            accepted = entry.handle.request_stop(request)
        except BaseException as error:
            try:
                accepted = entry.handle.request_stop(request)
            except BaseException as confirmation_error:
                raise error from confirmation_error
            if not accepted:
                raise
        if not accepted:
            return False
        if entry.accepted_stop_control_version is None:
            entry.accepted_stop_control_version = request.control_version
            entry.durable_stop_event.set()
        return entry.accepted_stop_control_version == request.control_version

    async def run_start(self, start: WorkerStart) -> None:
        if self._closing or self._closed:
            raise RuntimeError("project worker is closing or closed")
        self._require_loop()
        start = _require_worker_start(start)
        agent: ProjectAgent | None = None
        promoted = False
        live_entry: _LiveEntry | None = None
        live_key: tuple[object, ...] | None = None
        primary_error: BaseException | None = None
        cleanup_error: BaseException | None = None
        cleanup_cancelled = False
        cache_key: str | None = None
        signature: str | None = None
        try:
            current_claim = _require_marked_claim(
                start.claim,
                await self._runtime.mark_turn_started(start.claim),
            )
            execution = await self._runtime.execution_input_for_claim(
                current_claim
            )
            expected_attempt = _claim_attempt(current_claim)
            if (
                type(execution) is not TurnExecutionInput
                or type(execution.attempt) is not TurnAttemptIdentity
                or execution.attempt != expected_attempt
            ):
                raise ValueError("execution input does not match worker start")
            history = await self._batches.load_project_history(
                current_claim.canonical_session_id
            )
            if (
                type(history) is not ProjectHistorySnapshot
                or history.session_id != current_claim.canonical_session_id
                or type(history.message_count) is not int
                or history.message_count < 0
            ):
                raise ValueError("invalid project history snapshot")

            if start.source == "approved_operation":
                if self._approved_operations is None:
                    raise RuntimeError(
                        "approved operation execution is unavailable"
                    )
                assert type(start.operation) is ProjectOperation
                turn = self._approved_operations.create_turn(
                    execution,
                    start.operation,
                    base_message_count=history.message_count,
                )
            else:
                cache_key = project_agent_cache_key(
                    self._profile_home,
                    current_claim.project_id,
                    current_claim.canonical_session_id,
                )
                context = build_canonical_project_session_context(
                    current_claim.project_id,
                    current_claim.canonical_session_id,
                    self._config,
                    session_key=cache_key,
                )
                build = await self._agents.resolve_project_agent(
                    context=context,
                    contract_revision=execution.contract_revision,
                )
                signature = project_agent_cache_signature(
                    build.revisions,
                    execution.contract_revision,
                )

                idle = self._cache.pop(cache_key, None)
                if idle is not None:
                    if (
                        idle.signature == signature
                        and idle.message_count == history.message_count
                    ):
                        agent = idle.agent
                    else:
                        _, externally_cancelled = await self._release(
                            idle.agent
                        )
                        if externally_cancelled:
                            raise asyncio.CancelledError()
                if agent is None:
                    agent = await build.create_project_agent(
                        history=history
                    )
                turn = agent.create_turn(execution, start.operation)

            live_key = _live_key_from_claim(current_claim)
            handle = ProjectRuntimeLiveHandle(start, turn)
            durable_stop = asyncio.Event()
            heartbeat_stop = asyncio.Event()
            live_entry = _LiveEntry(
                handle,
                durable_stop,
                None,
                heartbeat_stop,
                None,
                current_claim,
            )
            if live_key in self._live:
                raise RuntimeError("duplicate live project attempt")
            self._live[live_key] = live_entry
            live_entry.heartbeat_task = asyncio.create_task(
                self._watch(live_entry, start)
            )

            run_result: ProjectAgentRunResult | None = None
            run_error: BaseException | None = None
            try:
                run_result = await turn.result()
            except BaseException as exc:
                run_error = exc

            external_cancel = (
                isinstance(run_error, asyncio.CancelledError)
                and asyncio.current_task() is not None
                and asyncio.current_task().cancelling() > 0
            )
            watch_cancelled, watch_stop_error = await self._stop_watch(
                live_entry
            )
            cleanup_cancelled = cleanup_cancelled or watch_cancelled
            if watch_cancelled:
                raise asyncio.CancelledError()
            if watch_stop_error is not None:
                raise watch_stop_error
            if external_cancel:
                assert run_error is not None
                raise run_error
            if live_entry.watch_error is not None:
                raise live_entry.watch_error
            if run_error is not None:
                if durable_stop.is_set():
                    await self._closer.acknowledge_stop(
                        claim=live_entry.current_claim,
                        runner=handle,
                        batch_id=None,
                    )
                    return
                raise run_error

            assert run_result is not None
            detached = _canonical_result_messages(
                run_result,
                history.message_count,
            )
            batch_id = self._batch_id_factory()
            if not _is_canonical_uuid4(batch_id):
                raise ValueError("invalid project terminal batch id")
            prepared = await self._batches.prepare_terminal_result(
                live_entry.current_claim,
                batch_id=batch_id,
                status=run_result.status,
                base_message_count=history.message_count,
                messages=detached,
            )
            _require_prepared_terminal(
                prepared,
                claim=live_entry.current_claim,
                batch_id=batch_id,
                status=run_result.status,
                base_message_count=history.message_count,
            )
            outcome = _require_apply_result(
                await self._closer.resolve_prepared_terminal(
                claim=live_entry.current_claim,
                result=CanonicalTurnResult(run_result.status, batch_id),
                batch_id=batch_id,
                runner=handle,
                )
            )
            if (
                outcome.outcome in {"published", "already_published"}
                and not durable_stop.is_set()
                and not handle._has_stop_call()
                and agent is not None
            ):
                assert cache_key is not None
                assert signature is not None
                promoted, externally_cancelled = await self._check_in(
                    cache_key,
                    _IdleAgent(
                        signature,
                        history.message_count + len(detached),
                        agent,
                    ),
                    live_entry,
                )
                if externally_cancelled:
                    raise asyncio.CancelledError()
        except BaseException as error:
            primary_error = error
            raise
        finally:
            if live_entry is not None:
                watch_cancelled, watch_stop_error = await self._stop_watch(
                    live_entry
                )
                cleanup_cancelled = cleanup_cancelled or watch_cancelled
                if cleanup_error is None and watch_stop_error is not None:
                    cleanup_error = watch_stop_error
                if not promoted:
                    try:
                        live_entry.handle._request_cleanup_cancel()
                    except BaseException as cancel_error:
                        if cleanup_error is None:
                            cleanup_error = cancel_error
                    quiescence_cancelled, quiescence_error = (
                        await self._wait_for_quiescence(live_entry.handle)
                    )
                    cleanup_cancelled = (
                        cleanup_cancelled or quiescence_cancelled
                    )
                    if cleanup_error is None and quiescence_error is not None:
                        cleanup_error = quiescence_error
                live_entry.handle.deactivate()
                if (
                    live_key is not None
                    and self._live.get(live_key) is live_entry
                ):
                    self._live.pop(live_key)
            if agent is not None and not promoted:
                _, release_cancelled = await self._release(agent)
                cleanup_cancelled = cleanup_cancelled or release_cancelled
            if primary_error is None:
                if cleanup_cancelled:
                    raise asyncio.CancelledError()
                if cleanup_error is not None:
                    raise cleanup_error

    async def close(self) -> None:
        self._require_loop()
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._close_once())
        await asyncio.shield(self._close_task)

    async def _close_once(self) -> None:
        first_error: BaseException | None = None
        idle_agents = tuple(entry.agent for entry in self._cache.values())
        self._cache.clear()
        for agent in idle_agents:
            task = asyncio.create_task(
                self._agents.release_project_agent(agent)
            )
            _, error = await self._reap_task(task)
            if error is not None:
                logger.warning(
                    "project agent release failed during close",
                    exc_info=(
                        type(error),
                        error,
                        error.__traceback__,
                    ),
                )
                if first_error is None:
                    first_error = error
        self._closed = True
        if first_error is not None:
            raise first_error
