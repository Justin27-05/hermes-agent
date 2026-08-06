"""Core-fenced issuance seam for durable project worker starts."""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from gateway.project_runtime_worker import (
    ProjectRuntimeWorker,
    StopRequest,
)
from hermes_cli.project_operations import (
    ApprovalCheckpointReadPort,
    OperationReadbackPort,
    OperationRecoveryCursor,
    ProjectOperation,
    ProjectOperationGuard,
)
from hermes_cli.project_runtime import (
    DispatcherLease,
    ProjectRuntime,
    RunnableProjectCursor,
    WorkerStart,
)

logger = logging.getLogger(__name__)

_SCAN_LIMIT = 100
_CORE_LEASE_SECONDS = 30
_CORE_RENEW_SECONDS = 10
_TURN_LEASE_SECONDS = 90
_POLL_SECONDS = 1


def _fresh_connection_call(
    factory: Callable[[], Any],
    operation: Callable[[Any], Any],
) -> Any:
    connection = None
    try:
        connection = factory()
        return operation(connection)
    finally:
        if connection is not None:
            connection.close()


def _runtime_target(connection: Any) -> Any:
    direct = getattr(connection, "acquire_dispatcher_lease", None)
    return connection if callable(direct) else ProjectRuntime(connection)


class ProjectDispatcherRuntimeFacade:
    def __init__(
        self,
        projects_db_factory: Callable[[], Any],
        *,
        terminal_readback: object,
        io_runner: Callable[..., Awaitable[Any]],
    ) -> None:
        if not callable(projects_db_factory) or not callable(io_runner):
            raise TypeError("dispatcher runtime dependencies must be callable")
        self._projects_db_factory = projects_db_factory
        self._terminal_readback = terminal_readback
        self._io_runner = io_runner

    async def _call(self, name: str, *args: object, **kwargs: object) -> Any:
        return await self._io_runner(
            _fresh_connection_call,
            self._projects_db_factory,
            lambda connection: getattr(
                _runtime_target(connection),
                name,
            )(*args, **kwargs),
        )

    async def acquire_dispatcher_lease(
        self,
        instance_id: str,
        *,
        lease_seconds: int,
    ) -> DispatcherLease | None:
        return await self._call(
            "acquire_dispatcher_lease",
            instance_id,
            lease_seconds=lease_seconds,
        )

    async def renew_dispatcher_lease(
        self,
        lease: DispatcherLease,
        *,
        lease_seconds: int,
    ) -> DispatcherLease | None:
        return await self._call(
            "renew_dispatcher_lease",
            lease,
            lease_seconds=lease_seconds,
        )

    async def release_dispatcher_lease(
        self,
        lease: DispatcherLease,
    ) -> bool:
        return await self._call("release_dispatcher_lease", lease)

    async def controls_for_live_starts(
        self,
        starts: tuple[WorkerStart, ...],
    ) -> tuple[Any, ...]:
        def controls(connection: Any) -> tuple[Any, ...]:
            target = _runtime_target(connection)
            direct = getattr(target, "controls_for_live_starts", None)
            if callable(direct):
                return tuple(direct(starts))
            return tuple(
                target.control_for_claim(start.claim) for start in starts
            )

        return await self._io_runner(
            _fresh_connection_call,
            self._projects_db_factory,
            controls,
        )

    async def reconcile_inflight_turns_with_task7_evidence(
        self,
        *,
        limit: int,
    ) -> tuple[Any, ...]:
        return await self._call(
            "reconcile_inflight_turns_with_task7_evidence",
            self._terminal_readback,
            limit=limit,
        )

    async def runnable_project_membership_upper_watermark(self) -> int | None:
        return await self._call(
            "runnable_project_membership_upper_watermark"
        )

    async def scan_runnable_projects(
        self,
        *,
        after: RunnableProjectCursor | None,
        through_membership_sequence: int,
        limit: int,
    ) -> Any:
        return await self._call(
            "scan_runnable_projects",
            after=after,
            through_membership_sequence=through_membership_sequence,
            limit=limit,
        )

    async def claim_next_turn_for_dispatcher(
        self,
        project_id: str,
        worker_id: str,
        *,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease,
    ) -> WorkerStart | None:
        return await self._call(
            "claim_next_turn_for_dispatcher",
            project_id,
            worker_id,
            lease_seconds=lease_seconds,
            dispatcher_lease=dispatcher_lease,
        )


class ProjectDispatcherOperationFacade:
    def __init__(
        self,
        projects_db_factory: Callable[[], Any],
        *,
        approval_checkpoints: object,
        executor_capabilities: object,
        io_runner: Callable[..., Awaitable[Any]],
    ) -> None:
        if not callable(projects_db_factory) or not callable(io_runner):
            raise TypeError("dispatcher operation dependencies must be callable")
        self._projects_db_factory = projects_db_factory
        self._approval_checkpoints = approval_checkpoints
        self._executor_capabilities = executor_capabilities
        self._io_runner = io_runner

    async def _call(self, name: str, *args: object, **kwargs: object) -> Any:
        def invoke(connection: Any) -> Any:
            target = connection
            if not callable(getattr(target, name, None)):
                target = ProjectOperationGuard(ProjectRuntime(connection))
            return getattr(target, name)(*args, **kwargs)

        return await self._io_runner(
            _fresh_connection_call,
            self._projects_db_factory,
            invoke,
        )

    async def expire_due_operation_approvals(
        self,
        *,
        limit: int,
    ) -> tuple[ProjectOperation, ...]:
        return await self._call(
            "expire_due_operation_approvals",
            limit=limit,
        )

    async def operation_recovery_membership_upper_watermark(
        self,
    ) -> int | None:
        return await self._call(
            "operation_recovery_membership_upper_watermark"
        )

    async def recover_pending_operations(
        self,
        executor_capabilities: object | None = None,
        *,
        worker_id: str,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease,
        max_claims: int,
        after: OperationRecoveryCursor | None,
        through_membership_sequence: int,
        limit: int,
    ) -> Any:
        capabilities = (
            self._executor_capabilities
            if executor_capabilities is None
            else executor_capabilities
        )
        return await self._call(
            "recover_pending_operations",
            capabilities,
            self._approval_checkpoints,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            dispatcher_lease=dispatcher_lease,
            max_claims=max_claims,
            after=after,
            through_membership_sequence=through_membership_sequence,
            limit=limit,
        )


class ProjectRuntimeDispatcher:
    """Issue and register one finite, slot-bounded dispatch tick."""

    def __init__(
        self,
        runtime: ProjectRuntime | ProjectDispatcherRuntimeFacade,
        operation_guard: ProjectOperationGuard | ProjectDispatcherOperationFacade,
        worker: ProjectRuntimeWorker,
        *,
        settlement: object | None = None,
        io_runner: Callable[..., Awaitable[Any]] | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
        wait_for_wake: Callable[[asyncio.Event, float], Awaitable[object]]
        | None = None,
        uuid_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
        worker_cap: int = 1,
    ) -> None:
        if type(worker_cap) is not int:
            raise TypeError("worker_cap must be an exact int")
        if worker_cap <= 0:
            raise ValueError("worker_cap must be positive")
        if not callable(getattr(worker, "run_start", None)):
            raise TypeError("worker must provide run_start")
        self._runtime = runtime
        self._operation_guard = operation_guard
        self._settlement = settlement
        self._io_runner = io_runner
        self._monotonic_clock = monotonic_clock
        self._wait_for_wake = wait_for_wake or self._default_wait_for_wake
        self._worker = worker
        self._worker_cap = worker_cap
        generated_id = uuid_factory()
        try:
            parsed_id = uuid.UUID(generated_id)
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("dispatcher id must be a canonical UUIDv4") from exc
        if not (
            type(generated_id) is str
            and parsed_id.version == 4
            and parsed_id.variant == uuid.RFC_4122
            and str(parsed_id) == generated_id
        ):
            raise ValueError("dispatcher id must be a canonical UUIDv4")
        self._instance_id = generated_id
        self._live_worker_tasks: set[asyncio.Task[None]] = set()
        self._live_project_tasks: dict[
            str,
            asyncio.Task[None],
        ] = {}
        self._live_start_tasks: dict[
            tuple[str, str, str, int, int],
            tuple[WorkerStart, asyncio.Task[None]],
        ] = {}
        self._operation_recovery_upper: int | None = None
        self._operation_recovery_cursor: (
            OperationRecoveryCursor | None
        ) = None
        self._runnable_upper: int | None = None
        self._runnable_cursor: RunnableProjectCursor | None = None
        self._settlement_upper: Any | None = None
        self._settlement_cursor: Any | None = None
        self._settlement_completed_this_tick = False
        self._lease: DispatcherLease | None = None
        self._retained_lease: DispatcherLease | None = None
        self._renew_deadline: float | None = None
        self._lane_backoffs: dict[str, float] = {}
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._run_task: asyncio.Task[None] | None = None
        self._running = False
        self._closing = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._wake_event: asyncio.Event | None = None
        self._wake_lock = threading.Lock()
        self._wake_pending = False

    @staticmethod
    async def _default_wait_for_wake(
        wake_event: asyncio.Event,
        timeout_seconds: float,
    ) -> str:
        try:
            await asyncio.wait_for(
                wake_event.wait(),
                timeout=timeout_seconds,
            )
            return "wake"
        except asyncio.TimeoutError:
            return "timeout"

    async def _port_call(
        self,
        port: object,
        name: str,
        *args: object,
        **kwargs: object,
    ) -> Any:
        method = getattr(port, name)
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        if self._io_runner is None:
            return method(*args, **kwargs)
        return await self._io_runner(method, *args, **kwargs)

    def wake(self) -> None:
        with self._wake_lock:
            if self._closing or self._closed:
                return
            self._wake_pending = True
            loop = self._owner_loop
            event = self._wake_event
        if loop is not None and event is not None:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                return

    async def _wait(self) -> None:
        event = self._wake_event
        if event is None:
            raise RuntimeError("dispatcher wake event is not bound")
        with self._wake_lock:
            pending = self._wake_pending
            self._wake_pending = False
            if pending:
                event.set()
            else:
                event.clear()
        await self._wait_for_wake(
            event,
            _POLL_SECONDS,
        )
        with self._wake_lock:
            if not pending:
                # A wake which released an already-blocked wait is consumed by
                # that wait.  When this wait began from the pre-bind latch,
                # however, preserve any second wake arriving during the
                # latch-to-wait handoff for the following tick.
                self._wake_pending = False
            if not self._wake_pending:
                event.clear()

    async def run(self) -> None:
        if self._closed or self._closing:
            return
        loop = asyncio.get_running_loop()
        if self._owner_loop is not None and self._owner_loop is not loop:
            raise RuntimeError("dispatcher belongs to another event loop")
        if self._running:
            raise RuntimeError("dispatcher is already running concurrently")
        self._owner_loop = loop
        self._wake_event = asyncio.Event()
        self._running = True
        self._run_task = asyncio.current_task()
        try:
            while not self._closing:
                await self._tick()
                if self._closing:
                    break
                await self._wait()
        finally:
            self._running = False

    def _lane_due(self, lane: str, now: float) -> bool:
        return now >= self._lane_backoffs.get(lane, float("-inf"))

    async def _lane(
        self,
        lane: str,
        operation: Callable[[], Awaitable[Any]],
        *,
        now: float,
    ) -> tuple[bool, Any]:
        if not self._lane_due(lane, now):
            return False, None
        try:
            result = await operation()
        except Exception as error:
            self._lane_backoffs[lane] = now + _POLL_SECONDS
            logger.error(
                "project dispatcher lane failed: %s: %s",
                lane,
                error,
                extra={"lane": lane, "exception": error},
                exc_info=(type(error), error, error.__traceback__),
            )
            return False, None
        self._lane_backoffs.pop(lane, None)
        return True, result

    async def _tick(self) -> None:
        if self._closing:
            return
        now = self._monotonic_clock()
        if self._lease is None:
            try:
                acquired = await self._port_call(
                    self._runtime,
                    "acquire_dispatcher_lease",
                    self._instance_id,
                    lease_seconds=_CORE_LEASE_SECONDS,
                )
            except Exception:
                return
            if type(acquired) is DispatcherLease:
                self._lease = acquired
                self._retained_lease = acquired
                self._renew_deadline = now + _CORE_RENEW_SECONDS
            return

        if (
            self._renew_deadline is not None
            and now >= self._renew_deadline
        ):
            try:
                renewed = await self._port_call(
                    self._runtime,
                    "renew_dispatcher_lease",
                    self._lease,
                    lease_seconds=_CORE_LEASE_SECONDS,
                )
            except Exception:
                renewed = None
            if type(renewed) is not DispatcherLease:
                self._lease = None
                self._renew_deadline = None
                return
            self._lease = renewed
            self._retained_lease = renewed
            self._renew_deadline = now + _CORE_RENEW_SECONDS

        lease = self._lease
        if lease is None:
            return

        self._settlement_completed_this_tick = False
        await self._stop_lane(now)
        await self._settlement_lane("settlement_first", now)
        await self._lane(
            "reconcile",
            lambda: self._port_call(
                self._runtime,
                "reconcile_inflight_turns_with_task7_evidence",
                limit=_SCAN_LIMIT,
            ),
            now=now,
        )
        await self._lane(
            "expiry",
            lambda: self._port_call(
                self._operation_guard,
                "expire_due_operation_approvals",
                limit=_SCAN_LIMIT,
            ),
            now=now,
        )
        await self._recovery_lane(now, lease)
        await self._settlement_lane("settlement_second", now)
        if self.available_slots > 0:
            await self._runnable_lane(now, lease)

    async def _stop_lane(self, now: float) -> None:
        starts = tuple(
            start
            for start, task in self._live_start_tasks.values()
            if not task.done()
        )
        ok, controls = await self._lane(
            "stops",
            lambda: self._port_call(
                self._runtime,
                "controls_for_live_starts",
                starts,
            ),
            now=now,
        )
        if not ok or not controls:
            return
        for item in controls:
            if type(item) is StopRequest:
                try:
                    self._worker.request_stop(item)
                except Exception:
                    continue

    async def _settlement_lane(self, lane: str, now: float) -> None:
        if self._settlement is None:
            return

        async def settle() -> None:
            if self._settlement_upper is None:
                if self._settlement_completed_this_tick:
                    return
                self._settlement_upper = await self._port_call(
                    self._settlement,
                    "pending_project_batch_upper_watermark",
                )
                self._settlement_cursor = None
            upper = self._settlement_upper
            if upper is None:
                return
            sequence = getattr(
                upper,
                "batch_creation_sequence",
                upper,
            )
            page = await self._port_call(
                self._settlement,
                "scan_pending_project_batches",
                after=self._settlement_cursor,
                through_batch_sequence=sequence,
                limit=_SCAN_LIMIT,
            )
            members = getattr(page, "batches", None)
            if members is None:
                members = getattr(page, "members", ())
            for member in tuple(members):
                batch_id = (
                    member
                    if type(member) is str
                    else member.batch_id
                )
                await self._port_call(
                    self._settlement,
                    "apply_project_batch",
                    batch_id,
                )
            self._settlement_cursor = page.scanned_through
            if page.reached_epoch_end:
                self._settlement_upper = None
                self._settlement_cursor = None
                self._settlement_completed_this_tick = True

        was_due = self._lane_due(lane, now)
        ok, _ = await self._lane(lane, settle, now=now)
        if was_due and not ok:
            deadline = now + _POLL_SECONDS
            self._lane_backoffs["settlement_first"] = deadline
            self._lane_backoffs["settlement_second"] = deadline

    async def _recovery_lane(
        self,
        now: float,
        lease: DispatcherLease,
    ) -> None:
        async def recover() -> None:
            if self._operation_recovery_upper is None:
                self._operation_recovery_upper = await self._port_call(
                    self._operation_guard,
                    "operation_recovery_membership_upper_watermark",
                )
                self._operation_recovery_cursor = None
            upper = self._operation_recovery_upper
            if upper is None:
                return
            page = await self._port_call(
                self._operation_guard,
                "recover_pending_operations",
                worker_id=self._instance_id,
                lease_seconds=_TURN_LEASE_SECONDS,
                dispatcher_lease=lease,
                max_claims=min(self.available_slots, _SCAN_LIMIT),
                after=self._operation_recovery_cursor,
                through_membership_sequence=upper,
                limit=_SCAN_LIMIT,
            )
            self._operation_recovery_cursor = page.scanned_through
            if page.reached_epoch_end:
                self._operation_recovery_upper = None
                self._operation_recovery_cursor = None
            for start in page.starts:
                if self._closing or self.available_slots <= 0:
                    break
                self._reserve_and_start(start)

        await self._lane("recovery", recover, now=now)

    async def _runnable_lane(
        self,
        now: float,
        lease: DispatcherLease,
    ) -> None:
        async def runnable() -> None:
            if self._runnable_upper is None:
                self._runnable_upper = await self._port_call(
                    self._runtime,
                    "runnable_project_membership_upper_watermark",
                )
                self._runnable_cursor = None
            upper = self._runnable_upper
            if upper is None:
                return
            page = await self._port_call(
                self._runtime,
                "scan_runnable_projects",
                after=self._runnable_cursor,
                through_membership_sequence=upper,
                limit=_SCAN_LIMIT,
            )
            self._runnable_cursor = page.scanned_through
            if page.reached_epoch_end:
                self._runnable_upper = None
                self._runnable_cursor = None
            for project in page.projects:
                if self._closing or self.available_slots <= 0:
                    break
                start = await self._port_call(
                    self._runtime,
                    "claim_next_turn_for_dispatcher",
                    project.project_id,
                    self._instance_id,
                    lease_seconds=_TURN_LEASE_SECONDS,
                    dispatcher_lease=lease,
                )
                if start is not None and not self._closing:
                    self._reserve_and_start(start)

        await self._lane("runnable", runnable, now=now)

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        if self._owner_loop is not None and self._owner_loop is not loop:
            raise RuntimeError("dispatcher belongs to another event loop")
        caller_is_run = asyncio.current_task() is self._run_task
        if self._close_task is None:
            self._closing = True
            event = self._wake_event
            owner_loop = self._owner_loop
            if event is not None:
                if owner_loop is loop:
                    event.set()
                elif owner_loop is not None:
                    owner_loop.call_soon_threadsafe(event.set)
            self._close_task = asyncio.create_task(
                self._close_once(caller_is_run=caller_is_run)
            )
        await asyncio.shield(self._close_task)

    async def _close_once(self, *, caller_is_run: bool) -> None:
        first_error: BaseException | None = None
        run_task = self._run_task
        if (
            not caller_is_run
            and run_task is not None
            and run_task is not asyncio.current_task()
        ):
            try:
                await asyncio.shield(run_task)
            except BaseException as error:
                if not isinstance(error, asyncio.CancelledError):
                    first_error = error
        tasks = tuple(self._live_worker_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            outcomes = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )
            for outcome in outcomes:
                if (
                    isinstance(outcome, BaseException)
                    and not isinstance(outcome, asyncio.CancelledError)
                    and first_error is None
                ):
                    first_error = outcome
        await asyncio.sleep(0)
        try:
            await self._worker.close()
        except BaseException as error:
            if first_error is None:
                first_error = error
        lease = self._retained_lease
        if lease is not None:
            try:
                await self._port_call(
                    self._runtime,
                    "release_dispatcher_lease",
                    lease,
                )
            except BaseException as error:
                if first_error is None:
                    first_error = error
        self._lease = None
        self._retained_lease = None
        self._closed = True
        self._closing = True
        self._owner_loop = None
        if first_error is not None:
            raise first_error

    @property
    def available_slots(self) -> int:
        slots = self._worker_cap - len(self._live_worker_tasks)
        if slots < 0:
            raise RuntimeError("worker capacity invariant violated")
        return slots

    def issue_queued_start(
        self,
        project_id: str,
        worker_id: str,
        *,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease,
    ) -> WorkerStart | None:
        return self._runtime.claim_next_turn_for_dispatcher(
            project_id,
            worker_id,
            lease_seconds=lease_seconds,
            dispatcher_lease=dispatcher_lease,
        )

    def issue_approved_operation_start(
        self,
        project_id: str,
        operation_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease,
    ) -> WorkerStart | None:
        return (
            self._operation_guard
            .rehydrate_approved_operation_for_dispatcher(
                project_id,
                operation_id,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                dispatcher_lease=dispatcher_lease,
            )
        )

    def dispatch_once(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        dispatcher_lease: DispatcherLease,
        readback: OperationReadbackPort,
        approval_checkpoints: ApprovalCheckpointReadPort,
    ) -> None:
        """Run one event-loop-affine recovery-first dispatch tick."""
        asyncio.get_running_loop()
        self._poll_live_stops()
        available_slots = self.available_slots

        if self._operation_recovery_upper is None:
            self._operation_recovery_upper = (
                self._operation_guard
                .operation_recovery_membership_upper_watermark()
            )
            self._operation_recovery_cursor = None
        recovery_upper = self._operation_recovery_upper
        if recovery_upper is not None:
            recovery = (
                self._operation_guard.recover_pending_operations(
                    readback,
                    approval_checkpoints,
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    dispatcher_lease=dispatcher_lease,
                    max_claims=min(
                        available_slots,
                        _SCAN_LIMIT,
                    ),
                    after=self._operation_recovery_cursor,
                    through_membership_sequence=recovery_upper,
                    limit=_SCAN_LIMIT,
                )
            )
            self._operation_recovery_cursor = (
                recovery.scanned_through
            )
            if recovery.reached_epoch_end:
                self._operation_recovery_upper = None
                self._operation_recovery_cursor = None
            for start in recovery.starts:
                self._reserve_and_start(start)

        if self.available_slots <= 0:
            return
        if self._runnable_upper is None:
            self._runnable_upper = (
                self._runtime
                .runnable_project_membership_upper_watermark()
            )
            self._runnable_cursor = None
        runnable_upper = self._runnable_upper
        if runnable_upper is None:
            return
        runnable = self._runtime.scan_runnable_projects(
            after=self._runnable_cursor,
            through_membership_sequence=runnable_upper,
            limit=_SCAN_LIMIT,
        )
        self._runnable_cursor = runnable.scanned_through
        if runnable.reached_epoch_end:
            self._runnable_upper = None
            self._runnable_cursor = None
        for project in runnable.projects:
            if self.available_slots <= 0:
                break
            start = self.issue_queued_start(
                project.project_id,
                worker_id,
                lease_seconds=lease_seconds,
                dispatcher_lease=dispatcher_lease,
            )
            if start is not None:
                self._reserve_and_start(start)

    def _reserve_and_start(self, start: WorkerStart) -> None:
        if self.available_slots <= 0:
            raise RuntimeError("worker capacity exhausted")
        project_id = start.claim.project_id
        if project_id in self._live_project_tasks:
            raise RuntimeError("project already has a live worker")
        loop = asyncio.get_running_loop()
        registration_gate: asyncio.Future[None] = (
            loop.create_future()
        )
        task = loop.create_task(
            self._run_after_registration(
                start,
                registration_gate,
            )
        )
        self._live_worker_tasks.add(task)
        self._live_project_tasks[project_id] = task
        self._live_start_tasks[self._start_key(start)] = (
            start,
            task,
        )
        task.add_done_callback(self._worker_done)
        registration_gate.set_result(None)

    async def _run_after_registration(
        self,
        start: WorkerStart,
        registration_gate: asyncio.Future[None],
    ) -> None:
        await registration_gate
        await self._worker.run_start(start)

    def _worker_done(self, task: asyncio.Task[None]) -> None:
        self._live_worker_tasks.discard(task)
        for project_id, registered_task in tuple(
            self._live_project_tasks.items()
        ):
            if registered_task is task:
                del self._live_project_tasks[project_id]
        for key, (_, registered_task) in tuple(
            self._live_start_tasks.items()
        ):
            if registered_task is task:
                del self._live_start_tasks[key]
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "project runtime worker failed",
                exc_info=(
                    type(error),
                    error,
                    error.__traceback__,
                ),
            )

    @staticmethod
    def _start_key(
        start: WorkerStart,
    ) -> tuple[str, str, str, int, int]:
        claim = start.claim
        return (
            claim.project_id,
            claim.turn_id,
            claim.attempt_id,
            claim.lease_generation,
            claim.fencing_token,
        )

    @staticmethod
    def _stop_request(
        start: WorkerStart,
        *,
        control_version: int,
    ) -> StopRequest:
        claim = start.claim
        return StopRequest(
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            attempt_id=claim.attempt_id,
            worker_id=claim.worker_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            canonical_session_id=claim.canonical_session_id,
            control_version=control_version,
        )

    def _registered_start(
        self,
        project_id: str,
        turn_id: str,
    ) -> WorkerStart | None:
        task = self._live_project_tasks.get(project_id)
        if task is None or task.done():
            return None
        for start, registered_task in (
            self._live_start_tasks.values()
        ):
            if (
                registered_task is task
                and start.claim.project_id == project_id
                and start.claim.turn_id == turn_id
            ):
                return start
        return None

    def notify_stop(
        self,
        project_id: str,
        turn_id: str,
    ) -> bool:
        start = self._registered_start(project_id, turn_id)
        if start is None:
            return False
        try:
            control = self._runtime.control_for_claim(start.claim)
        except Exception:
            return False
        if control.state != "stop_requested":
            return False
        try:
            request_stop = object.__getattribute__(
                self._worker,
                "request_stop",
            )
        except AttributeError:
            return False
        try:
            return bool(
                request_stop(
                    self._stop_request(
                        start,
                        control_version=control.control_version,
                    )
                )
            )
        except Exception:
            return False

    def _poll_live_stops(self) -> None:
        live_entries = tuple(
            self._live_start_tasks.values()
        )
        if not live_entries:
            return
        try:
            request_stop = object.__getattribute__(
                self._worker,
                "request_stop",
            )
        except AttributeError:
            return
        for start, task in live_entries:
            if task.done():
                continue
            try:
                control = self._runtime.control_for_claim(
                    start.claim
                )
            except Exception:
                continue
            if (
                control.state != "stop_requested"
            ):
                continue
            try:
                request_stop(
                    self._stop_request(
                        start,
                        control_version=control.control_version,
                    )
                )
            except Exception:
                continue
