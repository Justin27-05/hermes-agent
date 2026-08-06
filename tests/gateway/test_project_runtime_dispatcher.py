"""Deterministic Task-7 dispatcher authority tests."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import shutil
import sqlite3
import threading
import tempfile
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import get_type_hints

import pytest

from hermes_cli import projects_db
from tests.gateway.project_runtime_test_helpers import (
    DeadlineWakeWaitProbe,
    ManualMonotonicClock,
    ProbeSet,
    RetainedThreadRunner,
    WakeWaitProbe,
    release_probes,
    run_probe,
)


_DISPATCHER_PROBE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "project_runtime_dispatcher_probe.py"
)
_INSTANCE_A = "11111111-1111-4111-8111-111111111111"
_INSTANCE_B = "22222222-2222-4222-8222-222222222222"


def _task7_c5_worker_start(
    source,
    suffix,
    dispatcher_lease,
    *,
    worker_id="task7-c5-worker",
):
    from hermes_cli.project_operations import ProjectOperation
    from hermes_cli.project_runtime import TurnClaim, WorkerStart

    claim = TurnClaim(
        turn_id=f"turn-{suffix}",
        project_id=f"project-{suffix}",
        sequence=1,
        worker_id=worker_id,
        attempt_id=f"attempt-{suffix}",
        lease_generation=1,
        fencing_token=1,
        lease_expires_at=100,
        canonical_session_id=f"session-{suffix}",
    )
    operation = (
        ProjectOperation(
            operation_id=f"operation-{suffix}",
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            idempotency_key=f"operation-key-{suffix}",
            canonical_action="local_code_edit",
            command_revision=1,
            targets=("C:/work/file.py",),
            batch_items=("write-file",),
            status="approved",
            approval_id=None,
            readback_kind="remote-ledger",
            receipt_id=None,
            blocked_reason=None,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            created_at=1,
            updated_at=1,
        )
        if source == "approved_operation"
        else None
    )
    return WorkerStart(
        source,
        claim,
        operation,
        dispatcher_lease,
    )


def _prepare(
    probe_id: str,
    action: str,
    db_path: Path,
    *,
    instance_id: str,
    now: int,
    lease_seconds: int | None = None,
    lease: dict[str, object] | None = None,
) -> dict[str, object]:
    frame: dict[str, object] = {
        "version": 1,
        "event": "prepare",
        "probe_id": probe_id,
        "action": action,
        "db_path": str(db_path),
        "instance_id": instance_id,
        "now": now,
    }
    if lease_seconds is not None:
        frame["lease_seconds"] = lease_seconds
    if lease is not None:
        frame["lease"] = lease
    return frame


def _stored_lease(db_path: Path) -> tuple[object, ...]:
    conn = projects_db.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT lease_name, instance_id, generation, fencing_token,
                   expires_at, updated_at
            FROM project_dispatcher_leases
            WHERE lease_name = 'core'
            """
        ).fetchone()
        assert row is not None
        return tuple(row)
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_task7_c9_count_drift_post_terminal_cas_is_rediscovered_and_blocks_starts_without_agent_run(
    tmp_path,
) -> None:
    """Bounded settlement creates a real block that fences every start path.

    Mutations caught: requiring the later C14 scheduler, leaving the blocked
    project runnable, minting a queued/approved start, or running a worker while
    rediscovering/finalizing the terminal conflict.
    """
    import gateway.session as session_module
    from gateway.session import AsyncSessionStore
    from gateway.project_runtime_dispatcher import ProjectRuntimeDispatcher
    from hermes_cli import project_operations
    from hermes_cli import project_runtime_db as prdb
    from hermes_cli.project_policy import (
        ActorContext,
        Decision,
        PolicyDecision,
    )
    from hermes_cli.project_runtime import CanonicalTurnResult, ProjectRuntime
    from hermes_state import SessionDB

    projects_path = tmp_path / "c9-dispatch-projects.db"
    state = SessionDB(db_path=tmp_path / "c9-dispatch-state.db")
    conn = projects_db.connect(projects_path)
    worker_runs = []

    class ZeroRunWorker:
        async def run_start(self, start):
            worker_runs.append(start)
            raise AssertionError(
                "C9 settlement/block may not run a worker or agent"
            )

    class NoExternalReadback:
        def read_operation(self, request):
            raise AssertionError(
                "blocked dispatcher consulted operation readback"
            )

        def publication_state(self, checkpoint):
            raise AssertionError(
                "blocked dispatcher consulted approval publication"
            )

    def state_snapshot(session_id, batch_id):
        return {
            "batch": dict(
                state._conn.execute(
                    """
                    SELECT * FROM project_turn_transcript_batches
                    WHERE batch_id = ?
                    """,
                    (batch_id,),
                ).fetchone()
            ),
            "session": dict(
                state._conn.execute(
                    "SELECT * FROM sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()
            ),
            "messages": tuple(
                tuple(row)
                for row in state._conn.execute(
                    "SELECT * FROM messages WHERE session_id = ? ORDER BY id",
                    (session_id,),
                )
            ),
            "delegations": tuple(
                tuple(row)
                for row in state._conn.execute(
                    "SELECT * FROM async_delegations ORDER BY delegation_id"
                )
            ),
        }

    def projects_snapshot(project_ids):
        placeholders = ", ".join("?" for _ in project_ids)
        return {
            "runtime": tuple(
                tuple(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM project_runtime_state
                    WHERE project_id IN ({placeholders})
                    ORDER BY project_id
                    """,
                    project_ids,
                )
            ),
            "turns": tuple(
                tuple(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM project_turns
                    WHERE project_id IN ({placeholders})
                    ORDER BY project_id, sequence
                    """,
                    project_ids,
                )
            ),
            "controls": tuple(
                tuple(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM project_run_controls
                    WHERE project_id IN ({placeholders})
                    ORDER BY project_id, turn_id
                    """,
                    project_ids,
                )
            ),
            "leases": tuple(
                tuple(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM project_worker_leases
                    WHERE project_id IN ({placeholders})
                    ORDER BY project_id, turn_id
                    """,
                    project_ids,
                )
            ),
            "events": tuple(
                tuple(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM project_events
                    WHERE project_id IN ({placeholders})
                    ORDER BY project_id, sequence
                    """,
                    project_ids,
                )
            ),
            "operations": tuple(
                tuple(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM project_operations
                    WHERE project_id IN ({placeholders})
                    ORDER BY project_id, operation_id
                    """,
                    project_ids,
                )
            ),
            "approvals": tuple(
                tuple(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM project_approvals
                    WHERE project_id IN ({placeholders})
                    ORDER BY project_id, approval_id
                    """,
                    project_ids,
                )
            ),
            "deliveries": tuple(
                tuple(row)
                for row in conn.execute(
                    f"""
                    SELECT * FROM project_deliveries
                    WHERE project_id IN ({placeholders})
                    ORDER BY project_id, delivery_id
                    """,
                    project_ids,
                )
            ),
            "membership": tuple(
                tuple(row)
                for row in conn.execute(
                    """
                    SELECT * FROM project_runtime_membership_counters
                    ORDER BY lane
                    """
                )
            ),
            "dispatcher_leases": tuple(
                tuple(row)
                for row in conn.execute(
                    """
                    SELECT * FROM project_dispatcher_leases
                    ORDER BY lease_name
                    """
                )
            ),
        }

    try:
        project_id = projects_db.create_project(conn, name="C9 rediscovery")
        session_id = "c9-dispatch-session"
        prdb.create_project_conversation(
            conn,
            project_id=project_id,
            conversation_id=session_id,
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="c9-owner",
            project_id=project_id,
            surface="desktop",
            external_binding_id="c9-window",
            actor_id="owner",
            now=1,
        )
        state.create_session(session_id, source="cli")
        runtime = ProjectRuntime(conn, clock=lambda: 100)
        actor = ActorContext("owner", "desktop", "c9-owner", True)
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "recover"},
            actor,
            idempotency_key="c9-dispatch",
            expected_version=0,
        )
        queued_turn = runtime.enqueue_turn(
            project_id,
            {"message": "must remain blocked"},
            actor,
            idempotency_key="c9-blocked-queue",
            expected_version=1,
        )
        claim = runtime.claim_next_turn(project_id, "c9-worker", lease_seconds=30)
        assert claim is not None
        claim = runtime.mark_turn_started(claim)
        batch_id = "223e4567-e89b-42d3-a456-426614174009"
        prepared = state.prepare_terminal_result(
            claim,
            batch_id=batch_id,
            status="succeeded",
            base_message_count=0,
            messages=(
                {"role": "user", "content": "u", "timestamp": 1.0},
                {"role": "assistant", "content": "a", "timestamp": 2.0},
            ),
        )
        runtime.commit_turn_with_task7_batch(
            claim,
            CanonicalTurnResult("succeeded", batch_id),
            transcript_batch_id=batch_id,
        )
        state.append_message(session_id, "user", "drift")

        # Directly drive the existing bounded settlement epoch. No C14
        # recurring scheduler/lifecycle surface is involved.
        upper = state.pending_project_batch_upper_watermark()
        assert upper is not None
        page = state.list_pending_project_batches(
            after=None, through=upper, limit=100
        )
        assert page == (prepared,)
        before_state = state_snapshot(session_id, batch_id)
        before_projects = projects_snapshot((project_id,))
        adapter = AsyncSessionStore(
            state,
            projects_db_factory=lambda: projects_db.connect(projects_path),
        )
        outcomes = []
        for discovered in page:
            outcomes.append(
                await adapter.apply_project_batch(discovered.batch_id)
            )
        assert outcomes == [
            session_module.ProjectBatchApplyResult(outcome="conflicted")
        ]
        after_state = state_snapshot(session_id, batch_id)
        after_projects = projects_snapshot((project_id,))
        assert after_state["batch"]["state"] == "conflicted"
        block_key = after_state["batch"]["transcript_conflict_key"]
        assert type(block_key) is str
        assert block_key.startswith("transcript-conflict-")
        runtime_row = conn.execute(
            """
            SELECT transcript_pending_batch_id,
                   transcript_dispatch_block_key
            FROM project_runtime_state
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()
        assert tuple(runtime_row) == (None, block_key)
        assert after_state["session"] == before_state["session"]
        assert after_state["messages"] == before_state["messages"]
        assert after_state["delegations"] == before_state["delegations"]
        assert after_projects["deliveries"] == before_projects["deliveries"]
        assert conn.execute(
            """
            SELECT transcript_applied_batch_id FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0] is None
        assert state.list_pending_project_batches(
            after=None, through=upper, limit=100
        ) == ()

        # A genuine approved operation uses the other Core-fenced ingress.
        # Terminal and rehydratable FIFO histories cannot be the same project,
        # so install the real C9 block value on this separate valid fixture.
        operation_project = projects_db.create_project(
            conn,
            name="C9 approved blocked",
            folders=("C:/work/c9",),
        )
        prdb.create_project_conversation(
            conn,
            project_id=operation_project,
            conversation_id="c9-operation-session",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="c9-operation-owner",
            project_id=operation_project,
            surface="desktop",
            external_binding_id="c9-operation-window",
            actor_id="owner",
            now=1,
        )
        operation_actor = ActorContext(
            "owner", "desktop", "c9-operation-owner", True
        )
        operation_turn = runtime.enqueue_turn(
            operation_project,
            {"message": "approved blocked"},
            operation_actor,
            idempotency_key="c9-operation-turn",
            expected_version=0,
        )
        operation_claim = runtime.claim_next_turn(
            operation_project,
            "c9-operation-worker",
            lease_seconds=30,
        )
        assert operation_claim is not None
        operation_claim = runtime.mark_turn_started(operation_claim)
        operation_guard = project_operations.ProjectOperationGuard(runtime)
        operation_id = "c9-approved-operation"
        approval_id = "c9-approved-approval"
        operation_guard.prepare(
            operation_claim,
            project_operations.OperationIntent(
                operation_id=operation_id,
                project_id=operation_project,
                turn_id=operation_turn.turn_id,
                idempotency_key="c9-approved-key",
                canonical_action="publish",
                command_revision=1,
                targets=("C:/work/c9/result.txt",),
                batch_items=("publish-c9",),
                payload={"content_digest": "sha256:c9"},
                readback_kind="remote-ledger",
                remote_idempotency_supported=True,
            ),
            policy=PolicyDecision(
                Decision.REQUIRE_APPROVAL,
                "policy.approval.publish",
                "publish requires approval",
                "publish",
            ),
            approval=project_operations.OperationApprovalSpec(
                approval_id,
                "publish",
                1_000,
                operation_actor,
            ),
        )
        approved = operation_guard.resolve_operation_approval(
            approval_id,
            operation_actor,
            outcome="approved",
        )
        assert approved.status == "approved"
        conn.execute(
            """
            UPDATE project_runtime_state
            SET transcript_dispatch_block_key = ?
            WHERE project_id = ?
            """,
            (block_key, operation_project),
        )
        conn.commit()

        dispatcher_lease = runtime.acquire_dispatcher_lease(
            _INSTANCE_A, lease_seconds=30
        )
        assert dispatcher_lease is not None
        worker = ZeroRunWorker()
        dispatcher = ProjectRuntimeDispatcher(
            runtime,
            operation_guard,
            worker,
            worker_cap=2,
        )
        selected_projects = (project_id, operation_project)
        blocked_before = projects_snapshot(selected_projects)

        # Raw runnable discovery excludes both blocked projects.
        runnable_upper = runtime.runnable_project_membership_upper_watermark()
        assert runnable_upper is not None
        runnable = runtime.scan_runnable_projects(
            after=None,
            through_membership_sequence=runnable_upper,
            limit=100,
        )
        assert project_id not in {
            candidate.project_id for candidate in runnable.projects
        }
        assert operation_project not in {
            candidate.project_id for candidate in runnable.projects
        }

        # Both final write paths repeat the gate predicate transactionally.
        queued_trace = []
        conn.set_trace_callback(queued_trace.append)
        try:
            assert dispatcher.issue_queued_start(
                project_id,
                "c9-blocked-worker",
                lease_seconds=30,
                dispatcher_lease=dispatcher_lease,
            ) is None
        finally:
            conn.set_trace_callback(None)
        approved_trace = []
        conn.set_trace_callback(approved_trace.append)
        try:
            assert dispatcher.issue_approved_operation_start(
                operation_project,
                operation_id,
                worker_id="c9-blocked-operation-worker",
                lease_seconds=30,
                dispatcher_lease=dispatcher_lease,
            ) is None
        finally:
            conn.set_trace_callback(None)
        for trace in (queued_trace, approved_trace):
            normalized = [
                " ".join(statement.upper().split())
                for statement in trace
            ]
            begin = [
                index
                for index, statement in enumerate(normalized)
                if statement.startswith("BEGIN")
            ]
            ending = [
                index
                for index, statement in enumerate(normalized)
                if statement in {"COMMIT", "ROLLBACK"}
            ]
            assert len(begin) == len(ending) == 1
            assert normalized[begin[0]] == "BEGIN IMMEDIATE"
            assert begin[0] < ending[0]
            runtime_authority_reads = [
                index
                for index, statement in enumerate(normalized)
                if statement.startswith("SELECT")
                and "FROM PROJECT_RUNTIME_STATE" in statement
                and (
                    "TRANSCRIPT_DISPATCH_BLOCK_KEY" in statement
                    or statement.startswith(
                        "SELECT * FROM PROJECT_RUNTIME_STATE"
                    )
                )
            ]
            assert runtime_authority_reads
            assert all(
                begin[0] < index < ending[0]
                for index in runtime_authority_reads
            )
            assert not [
                statement
                for statement in normalized
                if statement.startswith(
                    ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
            ]
            assert conn.in_transaction is False
        assert projects_snapshot(selected_projects) == blocked_before

        # Run one real bounded dispatcher tick. It may scan but cannot issue,
        # reserve, schedule or run either source.
        dispatcher.dispatch_once(
            worker_id="c9-dispatch-worker",
            lease_seconds=30,
            dispatcher_lease=dispatcher_lease,
            readback=NoExternalReadback(),
            approval_checkpoints=NoExternalReadback(),
        )
        await asyncio.sleep(0)
        assert worker_runs == []
        assert dispatcher.available_slots == 2
        assert projects_snapshot(selected_projects) == blocked_before
        assert state_snapshot(session_id, batch_id) == after_state

        replay_before_state = state_snapshot(session_id, batch_id)
        replay_before_projects = projects_snapshot(selected_projects)
        replay = await adapter.apply_project_batch(batch_id)
        assert replay == session_module.ProjectBatchApplyResult(
            outcome="already_conflicted"
        )
        assert state_snapshot(session_id, batch_id) == replay_before_state
        assert projects_snapshot(selected_projects) == replay_before_projects
        assert worker_runs == []
    finally:
        conn.close()
        state.close()


class _C14DispatcherRuntime:
    def __init__(
        self,
        trace,
        lease,
        *,
        acquire_outcomes=(),
        renew_outcomes=(),
        runnable_pages=(),
        queued_starts=(),
    ):
        self.trace = trace
        self.lease = lease
        self.acquire_outcomes = list(acquire_outcomes)
        self.renew_outcomes = list(renew_outcomes)
        self.runnable_pages = list(runnable_pages)
        self.queued_starts = list(queued_starts)
        self.failures = {}
        self.runnable_upper = 41

    def _fail(self, lane):
        failure = self.failures.pop(lane, None)
        if failure is not None:
            raise failure

    @staticmethod
    def _outcome(outcomes, fallback):
        value = outcomes.pop(0) if outcomes else fallback
        if isinstance(value, BaseException):
            raise value
        return value

    def acquire_dispatcher_lease(self, instance_id, *, lease_seconds):
        self.trace.append(("acquire", instance_id, lease_seconds))
        self._fail("acquire")
        return self._outcome(self.acquire_outcomes, self.lease)

    def renew_dispatcher_lease(self, lease, *, lease_seconds):
        self.trace.append(("renew", lease, lease_seconds))
        self._fail("renew")
        return self._outcome(self.renew_outcomes, self.lease)

    def release_dispatcher_lease(self, lease):
        self.trace.append(("release", lease))
        return True

    def controls_for_live_starts(self, starts):
        self.trace.append(("stops", tuple(starts)))
        self._fail("stops")
        return ()

    def reconcile_inflight_turns_with_task7_evidence(
        self,
        *,
        limit,
    ):
        self.trace.append(("reconcile", limit))
        self._fail("reconcile")
        return ()

    def runnable_project_membership_upper_watermark(self):
        self.trace.append(("runnable_upper", self.runnable_upper))
        self._fail("runnable")
        return self.runnable_upper

    def scan_runnable_projects(self, *, after, through_membership_sequence, limit):
        self.trace.append(
            (
                "runnable_scan",
                after,
                through_membership_sequence,
                limit,
            )
        )
        self._fail("runnable_scan")
        if self.runnable_pages:
            return self.runnable_pages.pop(0)
        return SimpleNamespace(
            projects=(),
            scanned_through=after,
            reached_epoch_end=True,
        )

    def claim_next_turn_for_dispatcher(
        self,
        project_id,
        worker_id,
        *,
        lease_seconds,
        dispatcher_lease,
    ):
        self.trace.append(
            (
                "claim",
                project_id,
                worker_id,
                lease_seconds,
                dispatcher_lease,
            )
        )
        self._fail("claim")
        return self.queued_starts.pop(0) if self.queued_starts else None


class _C14DispatcherOperations:
    def __init__(self, trace, *, recovery_pages=()):
        self.trace = trace
        self.recovery_pages = list(recovery_pages)
        self.failures = {}
        self.recovery_upper = 31

    def _fail(self, lane):
        failure = self.failures.pop(lane, None)
        if failure is not None:
            raise failure

    def expire_due_operation_approvals(self, *, limit):
        self.trace.append(("expiry", limit))
        self._fail("expiry")
        return ()

    def operation_recovery_membership_upper_watermark(self):
        self.trace.append(("recovery_upper", self.recovery_upper))
        self._fail("recovery")
        return self.recovery_upper

    def recover_pending_operations(
        self,
        *,
        worker_id,
        lease_seconds,
        dispatcher_lease,
        max_claims,
        after,
        through_membership_sequence,
        limit,
    ):
        self.trace.append(
            (
                "recovery_scan",
                worker_id,
                lease_seconds,
                dispatcher_lease,
                max_claims,
                after,
                through_membership_sequence,
                limit,
            )
        )
        self._fail("recovery_scan")
        if self.recovery_pages:
            return self.recovery_pages.pop(0)
        return SimpleNamespace(
            starts=(),
            scanned_through=after,
            reached_epoch_end=True,
        )


class _C14NoConfigRead:
    """A default-path sentinel: C14 timing is internal, never configuration."""

    def __getattr__(self, name):
        raise AssertionError(f"dispatcher read live configuration: {name}")


class _C14AtomicHandoffWaiter:
    """Inject a cross-thread wake in the latch-consume-to-wait handoff."""

    def __init__(self):
        self.calls = []
        self.entered = asyncio.Queue()
        self.race_wake = None
        self._raced = False

    async def __call__(self, wake_event, timeout_seconds):
        self.calls.append((wake_event, timeout_seconds))
        self.entered.put_nowait(len(self.calls) - 1)
        if self.race_wake is not None and not self._raced:
            self._raced = True
            thread = threading.Thread(target=self.race_wake)
            thread.start()
            thread.join(timeout=5)
            assert not thread.is_alive()
        await wake_event.wait()
        return "wake"

    async def next_wait(self):
        return await asyncio.wait_for(self.entered.get(), timeout=5)


class _C14RealTask6RecoveryConnection:
    """One facade-owned SQLite connection delegating to the real Task-6 guard."""

    def __init__(self, db_path):
        from hermes_cli.project_operations import ProjectOperationGuard
        from hermes_cli.project_runtime import ProjectRuntime

        self._connection = projects_db.connect(db_path)
        self._guard = ProjectOperationGuard(
            ProjectRuntime(self._connection, clock=lambda: 101)
        )

    def __getattr__(self, name):
        return getattr(self._guard, name)

    def close(self):
        self._connection.close()


class _C14RealTask6RecoveryFactory:
    """Creates a fresh real guard/connection for every facade invocation."""

    def __init__(self, db_path):
        self.db_path = db_path
        self.connections = []

    def __call__(self):
        connection = _C14RealTask6RecoveryConnection(self.db_path)
        self.connections.append(connection)
        return connection


class _C14DispatcherSettlement:
    def __init__(self, trace, *, pages=()):
        self.trace = trace
        self.pages = list(pages)
        self.fail_on_scan_call = None
        self.scan_calls = 0
        self.upper = 21

    def pending_project_batch_upper_watermark(self):
        self.trace.append(("settlement_upper", self.upper))
        return self.upper

    def scan_pending_project_batches(
        self,
        *,
        after,
        through_batch_sequence,
        limit,
    ):
        self.scan_calls += 1
        self.trace.append(
            (
                "settlement_scan",
                self.scan_calls,
                after,
                through_batch_sequence,
                limit,
            )
        )
        if self.fail_on_scan_call == self.scan_calls:
            self.fail_on_scan_call = None
            raise RuntimeError("settlement lane failure")
        if self.pages:
            return self.pages.pop(0)
        return SimpleNamespace(
            batches=(),
            scanned_through=after,
            reached_epoch_end=True,
        )

    def apply_project_batch(self, batch_id):
        self.trace.append(("settlement_apply", batch_id))
        return SimpleNamespace(outcome="already_published")


class _C14DispatcherWorker:
    def __init__(self, trace):
        self.trace = trace
        self.starts = []
        self.start_entered = asyncio.Event()
        self.start_release = asyncio.Event()
        self.raise_on_start = None
        self.stop_requests = []
        self.close_entered = asyncio.Event()
        self.close_release = asyncio.Event()
        self.block_close = False

    async def run_start(self, start):
        self.trace.append(("worker_start", start))
        self.starts.append(start)
        self.start_entered.set()
        if self.raise_on_start is not None:
            raise self.raise_on_start
        await self.start_release.wait()

    def request_stop(self, request):
        self.trace.append(("worker_stop", request))
        self.stop_requests.append(request)
        self.start_release.set()
        return True

    async def close(self):
        self.trace.append("worker_close")
        self.close_entered.set()
        if self.block_close:
            await self.close_release.wait()


def _c14_dispatcher(
    runtime,
    operations,
    settlement,
    worker,
    *,
    clock,
    waiter,
    io_runner,
    uuid_factory,
    worker_cap=None,
):
    from gateway.project_runtime_dispatcher import ProjectRuntimeDispatcher

    kwargs = {
        "settlement": settlement,
        "io_runner": io_runner,
        "monotonic_clock": clock,
        "wait_for_wake": waiter,
        "uuid_factory": uuid_factory,
    }
    if worker_cap is not None:
        kwargs["worker_cap"] = worker_cap
    return ProjectRuntimeDispatcher(
        runtime,
        operations,
        worker,
        **kwargs,
    )


async def _c14_begin(dispatcher, waiter):
    task = asyncio.create_task(dispatcher.run())
    await waiter.next_wait()
    return task


async def _c14_next_tick(dispatcher, waiter):
    dispatcher.wake()
    return await waiter.next_wait()


async def _c14_close(dispatcher, run_task, io_runner):
    try:
        await dispatcher.close()
        await run_task
    finally:
        io_runner.close()


@pytest.mark.asyncio
async def test_task7_c14_composition_core_loop_exact_order_and_standby():
    """Core authority alone opens the seven ordered, bounded leader lanes."""
    from gateway.project_runtime_dispatcher import ProjectRuntimeDispatcher
    from hermes_cli.project_runtime import DispatcherLease

    assert inspect.iscoroutinefunction(
        getattr(ProjectRuntimeDispatcher, "run", None)
    ), "C14 requires the recurring Core leader/standby run loop"

    lease = DispatcherLease(_INSTANCE_A, 7, 11, 300)

    # A missing or failed acquisition is an acquire-only standby tick.
    for acquire_outcome in (
        None,
        RuntimeError("core acquire failed"),
    ):
        trace = []
        clock = ManualMonotonicClock()
        waiter = WakeWaitProbe()
        runner = RetainedThreadRunner("c14-standby-io")
        runtime = _C14DispatcherRuntime(
            trace,
            lease,
            acquire_outcomes=(acquire_outcome,),
        )
        operations = _C14DispatcherOperations(trace)
        settlement = _C14DispatcherSettlement(trace)
        worker = _C14DispatcherWorker(trace)
        dispatcher = _c14_dispatcher(
            runtime,
            operations,
            settlement,
            worker,
            clock=clock,
            waiter=waiter,
            io_runner=runner,
            uuid_factory=lambda: _INSTANCE_A,
        )
        run_task = await _c14_begin(dispatcher, waiter)
        assert trace == [("acquire", _INSTANCE_A, 30)]
        await _c14_close(dispatcher, run_task, runner)
        assert worker.starts == []

    # The first acquired tick is still acquire-only. The next tick performs
    # the exact leader ordering, keeps one settlement epoch across both
    # positions, and passes fixed defaults only through narrow ports.
    trace = []
    clock = ManualMonotonicClock()
    waiter = WakeWaitProbe()
    runner = RetainedThreadRunner("c14-leader-io")
    runtime = _C14DispatcherRuntime(trace, lease)
    runtime.config = _C14NoConfigRead()
    operations = _C14DispatcherOperations(trace)
    settlement = _C14DispatcherSettlement(
        trace,
        pages=(
            SimpleNamespace(
                batches=("batch-a",),
                scanned_through=SimpleNamespace(
                    batch_creation_sequence=1,
                    batch_id="batch-a",
                ),
                reached_epoch_end=False,
            ),
            SimpleNamespace(
                batches=("batch-b",),
                scanned_through=SimpleNamespace(
                    batch_creation_sequence=2,
                    batch_id="batch-b",
                ),
                reached_epoch_end=True,
            ),
        ),
    )
    worker = _C14DispatcherWorker(trace)
    dispatcher = _c14_dispatcher(
        runtime,
        operations,
        settlement,
        worker,
        clock=clock,
        waiter=waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    run_task = await _c14_begin(dispatcher, waiter)
    assert trace == [("acquire", _INSTANCE_A, 30)]
    await _c14_next_tick(dispatcher, waiter)
    lane_names = [
        entry[0] if isinstance(entry, tuple) else entry
        for entry in trace[1:]
    ]
    assert lane_names == [
        "stops",
        "settlement_upper",
        "settlement_scan",
        "settlement_apply",
        "reconcile",
        "expiry",
        "recovery_upper",
        "recovery_scan",
        "settlement_scan",
        "settlement_apply",
        "runnable_upper",
        "runnable_scan",
    ]
    assert all(
        entry[-1] == 100
        for entry in trace
        if isinstance(entry, tuple)
        and entry[0]
        in {
            "reconcile",
            "expiry",
            "settlement_scan",
            "recovery_scan",
            "runnable_scan",
        }
    )
    recovery_call = next(
        entry for entry in trace if entry[0] == "recovery_scan"
    )
    assert recovery_call[1:5] == (
        _INSTANCE_A,
        90,
        lease,
        1,
    )
    assert waiter.calls[0][1] == 1
    assert worker.starts == []
    await _c14_close(dispatcher, run_task, runner)
    assert trace[-2:] == ["worker_close", ("release", lease)]

    # The same real one-second loop must advance from its poll deadline even
    # when no producer calls wake(). The fake clock wait is event-gated, so
    # this proves timeout routing without wall-clock sleep or retry.
    trace = []
    clock = ManualMonotonicClock()
    poll_waiter = DeadlineWakeWaitProbe()
    runner = RetainedThreadRunner("c14-poll-io")
    runtime = _C14DispatcherRuntime(trace, lease)
    operations = _C14DispatcherOperations(trace)
    settlement = _C14DispatcherSettlement(trace)
    worker = _C14DispatcherWorker(trace)
    dispatcher = _c14_dispatcher(
        runtime,
        operations,
        settlement,
        worker,
        clock=clock,
        waiter=poll_waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    run_task = await _c14_begin(dispatcher, poll_waiter)
    assert trace == [("acquire", _INSTANCE_A, 30)]
    assert poll_waiter.calls[0][1] == 1
    poll_waiter.expire(0)
    assert await poll_waiter.next_wait() == 1
    assert poll_waiter.release_reasons == [(0, "timeout")]
    assert any(
        isinstance(entry, tuple) and entry[0] == "runnable_scan"
        for entry in trace
    )
    assert not poll_waiter.calls[1][0].is_set()
    await _c14_close(dispatcher, run_task, runner)

    # A recovery start consumes capacity before runnable discovery.
    recovery_start = _task7_c5_worker_start(
        "approved_operation",
        "c14-recovery-capacity",
        lease,
        worker_id=_INSTANCE_A,
    )
    trace = []
    clock = ManualMonotonicClock()
    waiter = WakeWaitProbe()
    runner = RetainedThreadRunner("c14-capacity-io")
    runtime = _C14DispatcherRuntime(trace, lease)
    operations = _C14DispatcherOperations(
        trace,
        recovery_pages=(
            SimpleNamespace(
                starts=(recovery_start,),
                scanned_through=object(),
                reached_epoch_end=True,
            ),
        ),
    )
    settlement = _C14DispatcherSettlement(trace)
    worker = _C14DispatcherWorker(trace)
    dispatcher = _c14_dispatcher(
        runtime,
        operations,
        settlement,
        worker,
        clock=clock,
        waiter=waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    run_task = await _c14_begin(dispatcher, waiter)
    await _c14_next_tick(dispatcher, waiter)
    await asyncio.wait_for(worker.start_entered.wait(), timeout=5)
    assert worker.starts == [recovery_start]
    assert worker.starts[0].claim.worker_id == _INSTANCE_A
    assert not any(
        isinstance(entry, tuple) and entry[0] == "runnable_upper"
        for entry in trace
    )
    worker.start_release.set()
    await _c14_close(dispatcher, run_task, runner)

    # Due renew loss/error demotes immediately, keeps the old lease only for
    # close, leaves live work untouched, and retains all epoch cursors.
    for renew_outcome in (
        None,
        RuntimeError("core renew failed"),
    ):
        trace = []
        clock = ManualMonotonicClock()
        waiter = WakeWaitProbe()
        runner = RetainedThreadRunner("c14-renew-io")
        runnable_cursor = "renew-runnable-cursor"
        recovery_cursor = "renew-recovery-cursor"
        settlement_cursor = "renew-settlement-cursor"
        renew_live_start = _task7_c5_worker_start(
            "approved_operation",
            f"renew-live-{type(renew_outcome).__name__}",
            lease,
            worker_id=_INSTANCE_A,
        )
        runtime = _C14DispatcherRuntime(
            trace,
            lease,
            renew_outcomes=(renew_outcome,),
            runnable_pages=(
                SimpleNamespace(
                    projects=(),
                    scanned_through=runnable_cursor,
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    projects=(),
                    scanned_through=runnable_cursor,
                    reached_epoch_end=False,
                ),
            ),
        )
        operations = _C14DispatcherOperations(
            trace,
            recovery_pages=(
                SimpleNamespace(
                    starts=(renew_live_start,),
                    scanned_through=recovery_cursor,
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    starts=(),
                    scanned_through=recovery_cursor,
                    reached_epoch_end=False,
                ),
            ),
        )
        settlement = _C14DispatcherSettlement(
            trace,
            pages=(
                SimpleNamespace(
                    batches=(),
                    scanned_through="renew-settlement-first",
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    batches=(),
                    scanned_through=settlement_cursor,
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    batches=(),
                    scanned_through=settlement_cursor,
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    batches=(),
                    scanned_through=settlement_cursor,
                    reached_epoch_end=False,
                ),
            ),
        )
        worker = _C14DispatcherWorker(trace)
        dispatcher = _c14_dispatcher(
            runtime,
            operations,
            settlement,
            worker,
            clock=clock,
            waiter=waiter,
            io_runner=runner,
            uuid_factory=lambda: _INSTANCE_A,
            worker_cap=2,
        )
        run_task = await _c14_begin(dispatcher, waiter)
        await _c14_next_tick(dispatcher, waiter)
        await asyncio.wait_for(worker.start_entered.wait(), timeout=5)
        assert worker.starts == [renew_live_start]
        assert dispatcher.available_slots == 1
        before_renew = tuple(trace)
        clock.advance_to(10)
        await _c14_next_tick(dispatcher, waiter)
        assert trace[len(before_renew):] == [("renew", lease, 30)]
        assert worker.stop_requests == []
        assert worker.starts == [renew_live_start]
        assert not worker.start_release.is_set()
        assert dispatcher.available_slots == 1
        upper_counts = {
            lane: sum(
                isinstance(entry, tuple) and entry[0] == lane
                for entry in trace
            )
            for lane in (
                "settlement_upper",
                "recovery_upper",
                "runnable_upper",
            )
        }
        await _c14_next_tick(dispatcher, waiter)
        assert trace[-1] == ("acquire", _INSTANCE_A, 30)
        await _c14_next_tick(dispatcher, waiter)
        assert all(
            sum(
                isinstance(entry, tuple) and entry[0] == lane
                for entry in trace
            )
            == count
            for lane, count in upper_counts.items()
        )
        resumed_settlement = [
            entry
            for entry in trace
            if isinstance(entry, tuple)
            and entry[0] == "settlement_scan"
        ][-1]
        resumed_recovery = [
            entry
            for entry in trace
            if isinstance(entry, tuple)
            and entry[0] == "recovery_scan"
        ][-1]
        resumed_runnable = [
            entry
            for entry in trace
            if isinstance(entry, tuple)
            and entry[0] == "runnable_scan"
        ][-1]
        assert resumed_settlement[2:4] == (
            settlement_cursor,
            21,
        )
        assert resumed_recovery[5:7] == (
            recovery_cursor,
            31,
        )
        assert resumed_runnable[1:3] == (
            runnable_cursor,
            41,
        )
        worker.start_release.set()
        await _c14_close(dispatcher, run_task, runner)
        assert trace[-2:] == ["worker_close", ("release", lease)]

    # This is deliberately a concrete authority path.  The facade owns a
    # fresh SQLite connection and delegates to the
    # real Task-6 guard; the durable operation row is the oracle for an
    # unavailable registered capability.  Repeating the scan cannot append a
    # second recovery transition or issue a WorkerStart.
    from gateway.project_runtime_dispatcher import ProjectDispatcherOperationFacade
    from hermes_cli.project_operations import (
        OperationApprovalSpec,
        OperationIntent,
        ProjectOperationGuard,
    )
    from hermes_cli.project_policy import (
        ActorContext,
        ContractPolicyView,
        Decision,
        PolicyDecision,
        ProjectBindingView,
        ProjectCommand,
        ProjectPolicyView,
        decide as decide_project_policy,
    )
    from hermes_cli.project_runtime import (
        ProjectRuntime,
        TurnAttemptIdentity,
        TurnOrigin,
    )
    from hermes_cli import project_runtime_db as prdb
    import gateway.project_runtime_worker as worker_module

    with tempfile.TemporaryDirectory(prefix="c14-recovery-") as directory:
        database = Path(directory) / "projects.db"
        connection = projects_db.connect(database)
        try:
            project_id = projects_db.create_project(
                connection,
                name="C14 durable recovery block",
                folders=("c:/work/c14",),
            )
            prdb.create_project_conversation(
                connection,
                project_id=project_id,
                conversation_id="c14-recovery-session",
                current_phase="implementation",
                now=1,
            )
            prdb.bind_surface(
                connection,
                binding_id="c14-recovery-owner",
                project_id=project_id,
                surface="desktop",
                external_binding_id="c14-recovery-window",
                actor_id="owner",
                now=1,
            )
            contract_json = json.dumps(
                {
                    "allowed_action_classes": ["publish"],
                    "allowed_phases": ["implementation"],
                    "approved_plan_ref": "plan-7",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            connection.execute(
                """
                INSERT INTO project_contracts (
                    contract_id, project_id, revision, contract_json,
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "c14-contract-7",
                    project_id,
                    7,
                    contract_json,
                    "active",
                    1,
                    1,
                ),
            )
            connection.commit()
            runtime = ProjectRuntime(connection, clock=lambda: 100)
            actor = ActorContext(
                "owner", "desktop", "c14-recovery-owner", True
            )
            turn = runtime.enqueue_turn(
                project_id,
                {"message": "recover durable operation"},
                actor,
                idempotency_key="c14-recovery-turn",
                expected_version=0,
            )
            claim = runtime.claim_next_turn(
                project_id,
                "c14-worker",
                lease_seconds=1,
            )
            assert claim is not None
            claim = runtime.mark_turn_started(claim)
            guard = ProjectOperationGuard(runtime)
            operation_id = "c14-durable-operation"
            checkpoint_id = "44444444-4444-4444-8444-444444444444"
            intent = OperationIntent(
                operation_id,
                project_id,
                turn.turn_id,
                "c14-durable-idempotency",
                "publish",
                1,
                ("c:/work/c14/file.py",),
                ("write",),
                {"path": "c:/work/c14/file.py"},
                "remote-ledger",
                True,
            )
            command = ProjectCommand(
                "publish",
                project_id,
                7,
                "publish",
                ("c:/work/c14/file.py",),
                None,
                ("write",),
                {"phase": "implementation"},
            )
            effect_scope_json = json.dumps(
                {
                    "batch_items": ["write"],
                    "payload": {"path": "c:/work/c14/file.py"},
                    "targets": ["c:/work/c14/file.py"],
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            authority_json = json.dumps(
                {
                    "capability_fingerprint": [
                        "publish",
                        1,
                        "remote-ledger",
                        True,
                    ],
                    "command": {
                        "action_class": "publish",
                        "batch_id": None,
                        "batch_items": ["write"],
                        "metadata": {"phase": "implementation"},
                        "name": "publish",
                        "project_id": project_id,
                        "revision": 7,
                        "targets": ["c:/work/c14/file.py"],
                    },
                    "effect_scope": json.loads(effect_scope_json),
                    "intent": {
                        "batch_items": ["write"],
                        "canonical_action": "publish",
                        "command_revision": 1,
                        "idempotency_key": "c14-durable-idempotency",
                        "operation_id": operation_id,
                        "payload": {"path": "c:/work/c14/file.py"},
                        "project_id": project_id,
                        "readback_kind": "remote-ledger",
                        "remote_idempotency_supported": True,
                        "targets": ["c:/work/c14/file.py"],
                        "turn_id": turn.turn_id,
                    },
                    "policy_batch_id": None,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            authority = worker_module.BoundProjectOperationAuthority(
                command=command,
                intent=intent,
                policy_batch_id=None,
                effect_scope_json=effect_scope_json,
                effect_scope_sha256=hashlib.sha256(
                    effect_scope_json.encode("utf-8")
                ).hexdigest(),
                authority_json=authority_json,
                authority_sha256=hashlib.sha256(
                    authority_json.encode("utf-8")
                ).hexdigest(),
            )
            project_view = ProjectPolicyView(
                project_id,
                "active",
                "implementation",
                ("c:/work/c14",),
                "plan-7",
                (
                    ProjectBindingView(
                        "c14-recovery-owner",
                        "desktop",
                        "owner",
                        project_id,
                    ),
                ),
            )
            contract_view = ContractPolicyView(
                7,
                frozenset({"publish"}),
                frozenset({"implementation"}),
                "plan-7",
            )
            decision = decide_project_policy(
                command,
                project_view,
                contract_view,
                actor,
            )
            assert decision.decision is Decision.REQUIRE_APPROVAL
            control = runtime.control_for_claim(claim)
            carrier = worker_module.ProjectPolicyDecisionCarrier(
                execution_attempt=TurnAttemptIdentity(
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                    sequence=claim.sequence,
                    worker_id=claim.worker_id,
                    attempt_id=claim.attempt_id,
                    lease_generation=claim.lease_generation,
                    fencing_token=claim.fencing_token,
                    canonical_session_id=claim.canonical_session_id,
                    lease_expires_at=claim.lease_expires_at,
                ),
                execution_origin=TurnOrigin(
                    "c14-recovery-owner",
                    "desktop",
                    "c14-recovery-window",
                    "owner",
                ),
                control_version=control.control_version,
                runtime_version=connection.execute(
                    "SELECT version FROM project_runtime_state "
                    "WHERE project_id = ?",
                    (project_id,),
                ).fetchone()[0],
                operation_authority=authority,
                project=project_view,
                contract_id="c14-contract-7",
                contract_status="active",
                contract_json_sha256=hashlib.sha256(
                    contract_json.encode("utf-8")
                ).hexdigest(),
                contract=contract_view,
                actor=actor,
                decision=decision,
            )
            guard.prepare(
                claim,
                intent,
                authority=authority,
                policy_authority=carrier,
                policy=decision,
                approval=OperationApprovalSpec(
                    "33333333-3333-4333-8333-333333333333",
                    "publish",
                    3_700,
                    actor,
                ),
                approval_checkpoint_id=checkpoint_id,
            )
            runtime_lease = runtime.acquire_dispatcher_lease(
                _INSTANCE_A, lease_seconds=30
            )
            assert runtime_lease is not None
            guard.resolve_operation_approval(
                "33333333-3333-4333-8333-333333333333",
                actor,
                outcome="approved",
            )
            seed_certificate = connection.execute(
                """
                SELECT policy_authority_json,
                       policy_authority_sha256,
                       operation_authority_json,
                       operation_authority_sha256,
                       effect_scope_json,
                       effect_scope_sha256
                FROM project_operations
                WHERE operation_id = ?
                """,
                (operation_id,),
            ).fetchone()
            assert seed_certificate is not None
            assert all(
                type(value) is str and value
                for value in seed_certificate
            )
            assert seed_certificate[1] == hashlib.sha256(
                seed_certificate[0].encode("utf-8")
            ).hexdigest()
            assert tuple(seed_certificate[2:]) == (
                authority.authority_json,
                authority.authority_sha256,
                authority.effect_scope_json,
                authority.effect_scope_sha256,
            )
            upper = guard.operation_recovery_membership_upper_watermark()
            assert upper is not None
        finally:
            connection.close()

        recovery_databases = {
            "operation_executor_unavailable": Path(directory) / "missing.db",
            "operation_policy_stale": Path(directory) / "stale.db",
        }
        for recovery_database in recovery_databases.values():
            shutil.copyfile(database, recovery_database)

        class PublishedCheckpoint:
            def publication_state(self, checkpoint):
                return "published"

        for expected_reason, recovery_database in recovery_databases.items():
            durable_runner = RetainedThreadRunner(
                f"c14-{expected_reason}-io"
            )
            durable_runner_closed = False
            try:
                if expected_reason == "operation_policy_stale":
                    stale = projects_db.connect(recovery_database)
                    try:
                        stale.execute(
                            "UPDATE project_runtime_state "
                            "SET current_phase = 'review' WHERE project_id = ?",
                            (project_id,),
                        )
                        stale.commit()
                    finally:
                        stale.close()
                real_factory = _C14RealTask6RecoveryFactory(
                    recovery_database
                )
                facade = ProjectDispatcherOperationFacade(
                    real_factory,
                    approval_checkpoints=PublishedCheckpoint(),
                    executor_capabilities={},
                    io_runner=durable_runner,
                )
                dispatcher_trace = []
                dispatcher_runtime = _C14DispatcherRuntime(
                    dispatcher_trace,
                    runtime_lease,
                )
                dispatcher_settlement = _C14DispatcherSettlement(
                    dispatcher_trace
                )
                dispatcher_worker = _C14DispatcherWorker(
                    dispatcher_trace
                )
                dispatcher_waiter = WakeWaitProbe()
                dispatcher = _c14_dispatcher(
                    dispatcher_runtime,
                    facade,
                    dispatcher_settlement,
                    dispatcher_worker,
                    clock=ManualMonotonicClock(),
                    waiter=dispatcher_waiter,
                    io_runner=durable_runner,
                    uuid_factory=lambda: _INSTANCE_A,
                )

                def durable_recovery_snapshot():
                    observed = projects_db.connect(recovery_database)
                    try:
                        row = observed.execute(
                            """
                            SELECT status, blocked_reason,
                                   canonical_action, command_revision,
                                   readback_kind,
                                   remote_idempotency_supported,
                                   policy_authority_json,
                                   policy_authority_sha256,
                                   operation_authority_json,
                                   operation_authority_sha256,
                                   effect_scope_json,
                                   effect_scope_sha256
                            FROM project_operations
                            WHERE operation_id = ?
                            """,
                            (operation_id,),
                        ).fetchone()
                        events = observed.execute(
                            "SELECT COUNT(*) FROM project_events "
                            "WHERE project_id = ? AND kind = ?",
                            (project_id, "turn.recovery_blocked"),
                        ).fetchone()[0]
                        blocks = observed.execute(
                            "SELECT COUNT(*) FROM project_operations "
                            "WHERE project_id = ? AND status = 'blocked' "
                            "AND blocked_reason = ?",
                            (project_id, expected_reason),
                        ).fetchone()[0]
                    finally:
                        observed.close()
                    return tuple(row), events, blocks

                run_task = await _c14_begin(
                    dispatcher,
                    dispatcher_waiter,
                )
                (
                    acquired_row,
                    acquired_events,
                    acquired_blocks,
                ) = durable_recovery_snapshot()
                assert tuple(acquired_row[:6]) == (
                    "approved",
                    None,
                    "publish",
                    1,
                    "remote-ledger",
                    1,
                )
                assert tuple(acquired_row[6:]) == tuple(
                    seed_certificate
                )
                assert acquired_events == acquired_blocks == 0
                assert dispatcher_trace == [
                    ("acquire", _INSTANCE_A, 30)
                ]
                assert dispatcher_worker.starts == []

                await _c14_next_tick(
                    dispatcher,
                    dispatcher_waiter,
                )
                (
                    first_row,
                    first_events,
                    first_blocks,
                ) = durable_recovery_snapshot()
                assert tuple(first_row[:6]) == (
                    "blocked",
                    expected_reason,
                    "publish",
                    1,
                    "remote-ledger",
                    1,
                )
                assert tuple(first_row[6:]) == tuple(seed_certificate)
                assert first_events == first_blocks == 1
                assert dispatcher_worker.starts == []

                await _c14_next_tick(dispatcher, dispatcher_waiter)
                (
                    second_row,
                    second_events,
                    second_blocks,
                ) = durable_recovery_snapshot()
                assert second_row == first_row
                assert second_events == first_events == 1
                assert second_blocks == first_blocks == 1
                assert dispatcher_worker.starts == []
                assert not any(
                    isinstance(entry, tuple)
                    and entry[0] == "worker_start"
                    for entry in dispatcher_trace
                )
                try:
                    await dispatcher.close()
                    await run_task
                finally:
                    durable_runner_closed = True
                    durable_runner.close()
            finally:
                if not durable_runner_closed:
                    durable_runner.close()


@pytest.mark.asyncio
async def test_task7_c14_composition_lane_failure_backoff_is_independent(
    caplog,
):
    """Each failed non-Core lane retains only its own epoch and deadline."""
    from gateway.project_runtime_dispatcher import ProjectRuntimeDispatcher
    from hermes_cli.project_runtime import DispatcherLease

    assert inspect.iscoroutinefunction(
        getattr(ProjectRuntimeDispatcher, "run", None)
    ), "C14 requires independently backed-off recurring lanes"

    caplog.set_level(logging.DEBUG)

    def assert_lane_failure_log(lane, error_message):
        matching_records = []
        for record in caplog.records:
            message = record.getMessage()
            lane_evidence = (
                getattr(record, "lane", None) == lane
                or lane in message
            )
            exception_candidates = [
                message,
                getattr(record, "exception", None),
            ]
            captured_exception = getattr(record, "exc_info", None)
            if captured_exception is not None:
                exception_candidates.append(captured_exception[1])
            exception_evidence = any(
                candidate is not None
                and error_message in str(candidate)
                for candidate in exception_candidates
            )
            if lane_evidence and exception_evidence:
                matching_records.append(record)
        assert matching_records, (lane, error_message, caplog.records)
        caplog.clear()

    lease = DispatcherLease(_INSTANCE_A, 7, 11, 300)
    failure_lanes = (
        "stops",
        "settlement_first",
        "reconcile",
        "expiry",
        "recovery",
        "settlement_second",
        "runnable",
    )
    for failure_lane in failure_lanes:
        caplog.clear()
        trace = []
        clock = ManualMonotonicClock()
        waiter = WakeWaitProbe()
        runner = RetainedThreadRunner(f"c14-backoff-{failure_lane}")
        runtime = _C14DispatcherRuntime(
            trace,
            lease,
            runnable_pages=(
                SimpleNamespace(
                    projects=(),
                    scanned_through="runnable-cursor",
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    projects=(),
                    scanned_through="runnable-cursor",
                    reached_epoch_end=False,
                ),
            ),
        )
        operations = _C14DispatcherOperations(
            trace,
            recovery_pages=(
                SimpleNamespace(
                    starts=(),
                    scanned_through="recovery-cursor",
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    starts=(),
                    scanned_through="recovery-cursor",
                    reached_epoch_end=False,
                ),
            ),
        )
        settlement = _C14DispatcherSettlement(
            trace,
            pages=(
                SimpleNamespace(
                    batches=(),
                    scanned_through="settlement-cursor",
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    batches=(),
                    scanned_through="settlement-cursor",
                    reached_epoch_end=False,
                ),
                SimpleNamespace(
                    batches=(),
                    scanned_through="settlement-cursor",
                    reached_epoch_end=False,
                ),
            ),
        )
        if failure_lane in {
            "stops",
            "reconcile",
            "runnable",
        }:
            runtime.failures[
                "runnable_scan"
                if failure_lane == "runnable"
                else failure_lane
            ] = RuntimeError(f"{failure_lane} lane failure")
        elif failure_lane in {"expiry", "recovery"}:
            operations.failures[
                "recovery_scan"
                if failure_lane == "recovery"
                else failure_lane
            ] = RuntimeError(f"{failure_lane} lane failure")
        elif failure_lane == "settlement_first":
            settlement.fail_on_scan_call = 1
        else:
            settlement.fail_on_scan_call = 2
        worker = _C14DispatcherWorker(trace)
        dispatcher = _c14_dispatcher(
            runtime,
            operations,
            settlement,
            worker,
            clock=clock,
            waiter=waiter,
            io_runner=runner,
            uuid_factory=lambda: _INSTANCE_A,
        )
        run_task = await _c14_begin(dispatcher, waiter)
        await _c14_next_tick(dispatcher, waiter)
        expected_error_message = (
            "settlement lane failure"
            if failure_lane.startswith("settlement_")
            else f"{failure_lane} lane failure"
        )
        assert_lane_failure_log(
            failure_lane,
            expected_error_message,
        )
        first_tick = tuple(trace)
        assert sum(entry[0] == "acquire" for entry in first_tick) == 1
        later_lanes = {
            "stops": (
                "settlement_scan",
                "reconcile",
                "expiry",
                "recovery_scan",
                "runnable_scan",
            ),
            "settlement_first": (
                "reconcile",
                "expiry",
                "recovery_scan",
                "runnable_scan",
            ),
            "reconcile": ("expiry", "recovery_scan", "runnable_scan"),
            "expiry": ("recovery_scan", "runnable_scan"),
            "recovery": ("runnable_scan",),
            "settlement_second": ("runnable_scan",),
            "runnable": (),
        }[failure_lane]
        assert all(
            any(
                isinstance(entry, tuple) and entry[0] == lane
                for entry in first_tick
            )
            for lane in later_lanes
        ), (failure_lane, first_tick)

        failed_name = {
            "settlement_first": "settlement_scan",
            "settlement_second": "settlement_scan",
            "recovery": "recovery_scan",
            "runnable": "runnable_scan",
        }.get(failure_lane, failure_lane)
        failed_count = sum(
            isinstance(entry, tuple) and entry[0] == failed_name
            for entry in trace
        )
        failed_epoch_pair = {
            "settlement_scan": next(
                (entry[2], entry[3])
                for entry in reversed(trace)
                if isinstance(entry, tuple)
                and entry[0] == "settlement_scan"
            ),
            "recovery_scan": next(
                (entry[5], entry[6])
                for entry in reversed(trace)
                if isinstance(entry, tuple)
                and entry[0] == "recovery_scan"
            ),
            "runnable_scan": next(
                (entry[1], entry[2])
                for entry in reversed(trace)
                if isinstance(entry, tuple)
                and entry[0] == "runnable_scan"
            ),
        }.get(failed_name)
        await _c14_next_tick(dispatcher, waiter)
        assert sum(
            isinstance(entry, tuple) and entry[0] == failed_name
            for entry in trace
        ) == failed_count
        assert sum(
            isinstance(entry, tuple) and entry[0] == "acquire"
            for entry in trace
        ) == 1

        clock.advance_to(1)
        await _c14_next_tick(dispatcher, waiter)
        assert sum(
            isinstance(entry, tuple) and entry[0] == failed_name
            for entry in trace
        ) > failed_count
        scan_pair_extractors = {
            "settlement_scan": lambda entry: (entry[2], entry[3]),
            "recovery_scan": lambda entry: (entry[5], entry[6]),
            "runnable_scan": lambda entry: (entry[1], entry[2]),
        }
        if failed_name in scan_pair_extractors:
            retry_epoch_pairs = [
                scan_pair_extractors[failed_name](entry)
                for entry in trace
                if isinstance(entry, tuple) and entry[0] == failed_name
            ]
            assert retry_epoch_pairs.count(failed_epoch_pair) >= 2
        if failure_lane == "settlement_first":
            assert failed_epoch_pair == (None, 21)
        if failure_lane == "settlement_second":
            settlement_scans = [
                entry
                for entry in first_tick
                if isinstance(entry, tuple)
                and entry[0] == "settlement_scan"
            ]
            assert settlement_scans[0][2:4] == (None, 21)
            assert settlement_scans[1][2:4] == (
                "settlement-cursor",
                21,
            )
        # A successful retry clears only its own one-poll backoff.  The next
        # ordinary wake may enter that lane immediately; no unrelated lane
        # progress or clock advancement is allowed to clear it instead.
        cleared_count = sum(
            isinstance(entry, tuple) and entry[0] == failed_name
            for entry in trace
        )
        await _c14_next_tick(dispatcher, waiter)
        assert sum(
            isinstance(entry, tuple) and entry[0] == failed_name
            for entry in trace
        ) > cleared_count
        assert all(call[1] == 1 for call in waiter.calls)
        settlement_calls = [
            entry for entry in trace if entry[0] == "settlement_scan"
        ]
        recovery_calls = [
            entry for entry in trace if entry[0] == "recovery_scan"
        ]
        runnable_calls = [
            entry for entry in trace if entry[0] == "runnable_scan"
        ]
        assert all(entry[3] == 21 for entry in settlement_calls)
        assert all(entry[6] == 31 for entry in recovery_calls)
        assert all(entry[2] == 41 for entry in runnable_calls)
        await _c14_close(dispatcher, run_task, runner)

    # Two lanes fail at different monotonic instants.  Their one-poll
    # deadlines must remain independent: settlement is due at 1.0 while
    # expiry, failed on a later tick, remains backed off until 1.5.  The
    # recovery and runnable lanes continue on every intervening tick.
    trace = []
    caplog.clear()
    clock = ManualMonotonicClock()
    waiter = WakeWaitProbe()
    runner = RetainedThreadRunner("c14-staggered-backoff")
    runtime = _C14DispatcherRuntime(trace, lease)
    operations = _C14DispatcherOperations(trace)
    settlement = _C14DispatcherSettlement(trace)
    settlement.fail_on_scan_call = 1
    dispatcher = _c14_dispatcher(
        runtime,
        operations,
        settlement,
        _C14DispatcherWorker(trace),
        clock=clock,
        waiter=waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    run_task = await _c14_begin(dispatcher, waiter)
    await _c14_next_tick(dispatcher, waiter)
    assert_lane_failure_log(
        "settlement_first",
        "settlement lane failure",
    )
    assert settlement.scan_calls == 1
    operations.failures["expiry"] = RuntimeError(
        "later expiry lane failure"
    )
    clock.advance_to(0.5)
    await _c14_next_tick(dispatcher, waiter)
    assert_lane_failure_log(
        "expiry",
        "later expiry lane failure",
    )
    assert settlement.scan_calls == 1
    expiry_calls = sum(
        isinstance(entry, tuple) and entry[0] == "expiry"
        for entry in trace
    )
    assert expiry_calls == 2
    later_progress = {
        lane: sum(
            isinstance(entry, tuple) and entry[0] == lane
            for entry in trace
        )
        for lane in ("recovery_scan", "runnable_scan")
    }
    clock.advance_to(1)
    await _c14_next_tick(dispatcher, waiter)
    assert settlement.scan_calls > 1
    assert sum(
        isinstance(entry, tuple) and entry[0] == "expiry"
        for entry in trace
    ) == expiry_calls
    assert all(
        sum(
            isinstance(entry, tuple) and entry[0] == lane
            for entry in trace
        )
        > count
        for lane, count in later_progress.items()
    )
    clock.advance_to(1.5)
    await _c14_next_tick(dispatcher, waiter)
    assert sum(
        isinstance(entry, tuple) and entry[0] == "expiry"
        for entry in trace
    ) > expiry_calls
    assert sum(
        isinstance(entry, tuple) and entry[0] == "acquire"
        for entry in trace
    ) == 1
    await _c14_close(dispatcher, run_task, runner)
    assert trace[-1] == ("release", lease)


@pytest.mark.asyncio
async def test_task7_c14_composition_wake_coalesces_without_resetting_epochs():
    """Thread-safe wake is a coalesced hint, never scheduling authority."""
    from gateway.project_runtime_dispatcher import ProjectRuntimeDispatcher
    from hermes_cli.project_runtime import DispatcherLease

    assert callable(
        getattr(ProjectRuntimeDispatcher, "wake", None)
    ), "C14 requires one thread-safe coalescing wake hook"

    lease = DispatcherLease(_INSTANCE_A, 7, 11, 300)

    # This is the only dangerous wake race: a cross-thread wake exactly while
    # the first pre-bind latch is consumed and the loop transitions to wait.
    # The injected waiter supplies no timeout/retry escape hatch.
    handoff_trace = []
    handoff_waiter = _C14AtomicHandoffWaiter()
    handoff_runner = RetainedThreadRunner("c14-atomic-handoff-io")
    handoff_dispatcher = _c14_dispatcher(
        _C14DispatcherRuntime(handoff_trace, lease),
        _C14DispatcherOperations(handoff_trace),
        _C14DispatcherSettlement(handoff_trace),
        _C14DispatcherWorker(handoff_trace),
        clock=ManualMonotonicClock(),
        waiter=handoff_waiter,
        io_runner=handoff_runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    handoff_dispatcher.wake()
    handoff_waiter.race_wake = handoff_dispatcher.wake
    handoff_run = asyncio.create_task(handoff_dispatcher.run())
    assert await handoff_waiter.next_wait() == 0
    assert await handoff_waiter.next_wait() == 1
    await _c14_close(handoff_dispatcher, handoff_run, handoff_runner)

    class TickGatedWakeWaitProbe(WakeWaitProbe):
        def __init__(self):
            super().__init__()
            self.blocked = {}

        async def __call__(self, wake_event, timeout_seconds):
            call_index = len(self.calls)
            blocked = asyncio.Event()
            self.blocked[call_index] = blocked
            self.calls.append((wake_event, timeout_seconds))
            self.entered.put_nowait(call_index)
            if not wake_event.is_set():
                blocked.set()
            await wake_event.wait()
            return "wake"

        async def wait_until_blocked(self, call_index):
            await asyncio.wait_for(
                self.blocked[call_index].wait(),
                timeout=5,
            )

    trace = []
    clock = ManualMonotonicClock()
    waiter = TickGatedWakeWaitProbe()
    runner = RetainedThreadRunner("c14-wake-io")
    runtime = _C14DispatcherRuntime(
        trace,
        lease,
        runnable_pages=tuple(
            SimpleNamespace(
                projects=(),
                scanned_through="fixed-runnable-cursor",
                reached_epoch_end=False,
            )
            for _ in range(6)
        ),
    )
    operations = _C14DispatcherOperations(
        trace,
        recovery_pages=tuple(
            SimpleNamespace(
                starts=(),
                scanned_through="fixed-recovery-cursor",
                reached_epoch_end=False,
            )
            for _ in range(6)
        ),
    )
    settlement = _C14DispatcherSettlement(
        trace,
        pages=tuple(
            SimpleNamespace(
                batches=(),
                scanned_through="fixed-settlement-cursor",
                reached_epoch_end=False,
            )
            for _ in range(12)
        ),
    )
    settlement.fail_on_scan_call = 1
    worker = _C14DispatcherWorker(trace)
    dispatcher = _c14_dispatcher(
        runtime,
        operations,
        settlement,
        worker,
        clock=clock,
        waiter=waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )

    # Calls made before owner-loop binding collapse to one consumed latch.
    for _ in range(20):
        dispatcher.wake()
    run_task = asyncio.create_task(dispatcher.run())
    first_wait = await waiter.next_wait()
    second_wait = await waiter.next_wait()
    assert (first_wait, second_wait) == (0, 1)
    await waiter.wait_until_blocked(second_wait)
    assert waiter.entered.empty()
    assert sum(
        isinstance(entry, tuple) and entry[0] == "acquire"
        for entry in trace
    ) == 1

    # Many real cross-thread calls while a wait is pending produce one tick.
    threads = [
        threading.Thread(target=dispatcher.wake)
        for _ in range(20)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    third_wait = await waiter.next_wait()
    await waiter.wait_until_blocked(third_wait)
    assert waiter.entered.empty()
    before_idle = len(trace)
    assert all(call[1] == 1 for call in waiter.calls)
    assert sum(
        entry[0] == "settlement_upper"
        for entry in trace
        if isinstance(entry, tuple)
    ) == 1
    assert sum(
        entry[0] == "recovery_upper"
        for entry in trace
        if isinstance(entry, tuple)
    ) == 1
    assert sum(
        entry[0] == "runnable_upper"
        for entry in trace
        if isinstance(entry, tuple)
    ) == 1

    # A wake never rewrites the original renew/backoff deadlines. At their
    # original exact times each due action runs once.
    dispatcher.wake()
    await waiter.next_wait()
    assert len(trace) > before_idle
    assert not any(
        entry[0] == "renew"
        for entry in trace
        if isinstance(entry, tuple)
    )
    clock.advance_to(1)
    dispatcher.wake()
    await waiter.next_wait()
    assert settlement.scan_calls >= 2
    assert sum(
        isinstance(entry, tuple)
        and entry[0] == "settlement_scan"
        and entry[2] is None
        for entry in trace
    ) == 2
    clock.advance_to(10)
    dispatcher.wake()
    await waiter.next_wait()
    assert sum(
        entry[0] == "renew"
        for entry in trace
        if isinstance(entry, tuple)
    ) == 1
    assert all(
        entry[3] == 21
        for entry in trace
        if isinstance(entry, tuple)
        and entry[0] == "settlement_scan"
    )
    assert all(
        entry[6] == 31
        for entry in trace
        if isinstance(entry, tuple)
        and entry[0] == "recovery_scan"
    )
    assert all(
        entry[2] == 41
        for entry in trace
        if isinstance(entry, tuple)
        and entry[0] == "runnable_scan"
    )

    # Wait-blocked close consumes the wake and makes later wakes no-ops.
    await _c14_close(dispatcher, run_task, runner)
    after_close = tuple(trace)
    for _ in range(10):
        dispatcher.wake()
    assert tuple(trace) == after_close

    # Wakes are also inert with respect to a live exact registration.  A
    # coalesced cross-thread burst cannot duplicate, replace or free the
    # running start while the worker still owns it.
    live_trace = []
    live_clock = ManualMonotonicClock()
    live_waiter = WakeWaitProbe()
    live_runner = RetainedThreadRunner("c14-live-wake-io")
    live_start = _task7_c5_worker_start(
        "approved_operation",
        "wake-live",
        lease,
        worker_id=_INSTANCE_A,
    )
    live_worker = _C14DispatcherWorker(live_trace)
    live_dispatcher = _c14_dispatcher(
        _C14DispatcherRuntime(live_trace, lease),
        _C14DispatcherOperations(
            live_trace,
            recovery_pages=(
                SimpleNamespace(
                    starts=(live_start,),
                    scanned_through="wake-live-end",
                    reached_epoch_end=True,
                ),
            ),
        ),
        _C14DispatcherSettlement(live_trace),
        live_worker,
        clock=live_clock,
        waiter=live_waiter,
        io_runner=live_runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    live_run = await _c14_begin(live_dispatcher, live_waiter)
    await _c14_next_tick(live_dispatcher, live_waiter)
    await asyncio.wait_for(live_worker.start_entered.wait(), timeout=5)
    assert live_dispatcher.available_slots == 0
    live_threads = [
        threading.Thread(target=live_dispatcher.wake)
        for _ in range(20)
    ]
    for thread in live_threads:
        thread.start()
    for thread in live_threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    await live_waiter.next_wait()
    assert live_worker.starts == [live_start]
    assert live_dispatcher.available_slots == 0
    live_worker.start_release.set()
    await _c14_close(live_dispatcher, live_run, live_runner)
    assert live_dispatcher.available_slots == 1

    # An epoch advances only after its own explicit end.  In particular,
    # runnable discovery may take the new upper on the following tick, while
    # a wake, a different lane, and a failed settlement scan retain theirs.
    epoch_trace = []
    epoch_clock = ManualMonotonicClock()
    epoch_waiter = WakeWaitProbe()
    epoch_runner = RetainedThreadRunner("c14-epoch-reset-io")
    epoch_runtime = _C14DispatcherRuntime(
        epoch_trace,
        lease,
        runnable_pages=(
            SimpleNamespace(
                projects=(), scanned_through="runnable-end", reached_epoch_end=True
            ),
            SimpleNamespace(
                projects=(), scanned_through="runnable-next", reached_epoch_end=False
            ),
        ),
    )
    epoch_operations = _C14DispatcherOperations(
        epoch_trace,
        recovery_pages=(
            SimpleNamespace(starts=(), scanned_through="recovery-end", reached_epoch_end=True),
            SimpleNamespace(starts=(), scanned_through="recovery-next", reached_epoch_end=False),
        ),
    )
    epoch_settlement = _C14DispatcherSettlement(
        epoch_trace,
        pages=(
            SimpleNamespace(batches=(), scanned_through="settlement-end", reached_epoch_end=True),
            SimpleNamespace(batches=(), scanned_through="settlement-next", reached_epoch_end=False),
        ),
    )
    epoch_dispatcher = _c14_dispatcher(
        epoch_runtime,
        epoch_operations,
        epoch_settlement,
        _C14DispatcherWorker(epoch_trace),
        clock=epoch_clock,
        waiter=epoch_waiter,
        io_runner=epoch_runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    epoch_run = await _c14_begin(epoch_dispatcher, epoch_waiter)
    await _c14_next_tick(epoch_dispatcher, epoch_waiter)
    epoch_runtime.runnable_upper = 42
    epoch_operations.recovery_upper = 32
    epoch_settlement.upper = 22
    await _c14_next_tick(epoch_dispatcher, epoch_waiter)
    assert [entry[1] for entry in epoch_trace if entry[0] == "runnable_upper"] == [41, 42]
    assert [entry[1] for entry in epoch_trace if entry[0] == "recovery_upper"] == [31, 32]
    assert [entry[1] for entry in epoch_trace if entry[0] == "settlement_upper"][:2] == [21, 22]
    await _c14_close(epoch_dispatcher, epoch_run, epoch_runner)

    # Close also joins an exact off-thread scan and discards a late start.
    trace = []
    clock = ManualMonotonicClock()
    waiter = WakeWaitProbe()
    runner = RetainedThreadRunner("c14-close-scan-io")
    scan_entered = threading.Event()
    scan_release = threading.Event()
    late_start = _task7_c5_worker_start(
        "queued_turn",
        "late-after-close",
        lease,
        worker_id=_INSTANCE_A,
    )

    class BlockedScanRuntime(_C14DispatcherRuntime):
        def scan_runnable_projects(self, **kwargs):
            self.trace.append(("blocked_scan_enter", kwargs))
            scan_entered.set()
            assert scan_release.wait(timeout=5)
            self.trace.append("blocked_scan_exit")
            return SimpleNamespace(
                projects=(SimpleNamespace(project_id="late-project"),),
                scanned_through=object(),
                reached_epoch_end=True,
            )

        def claim_next_turn_for_dispatcher(self, *args, **kwargs):
            self.trace.append(("late_claim", args, kwargs))
            return late_start

    runtime = BlockedScanRuntime(trace, lease)
    operations = _C14DispatcherOperations(trace)
    settlement = _C14DispatcherSettlement(trace)
    worker = _C14DispatcherWorker(trace)
    dispatcher = _c14_dispatcher(
        runtime,
        operations,
        settlement,
        worker,
        clock=clock,
        waiter=waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    run_task = await _c14_begin(dispatcher, waiter)
    dispatcher.wake()
    await asyncio.wait_for(
        asyncio.to_thread(scan_entered.wait),
        timeout=5,
    )
    close_task = asyncio.create_task(dispatcher.close())
    dispatcher.wake()
    scan_release.set()
    await close_task
    await run_task
    runner.close()
    assert worker.starts == []
    assert trace.index("blocked_scan_exit") < trace.index("worker_close")
    assert trace[-1] == ("release", lease)

    # The connection-owning invocation itself is retained too, even when close
    # begins during the first Core acquisition rather than during a scan.
    db_trace = []
    db_waiter = WakeWaitProbe()
    db_runner = RetainedThreadRunner("c14-close-db-io")
    db_entered = threading.Event()
    db_release = threading.Event()

    class BlockedDatabaseRuntime(_C14DispatcherRuntime):
        def acquire_dispatcher_lease(self, instance_id, *, lease_seconds):
            self.trace.append(("blocked_db_enter", instance_id, lease_seconds))
            db_entered.set()
            assert db_release.wait(timeout=5)
            self.trace.append("blocked_db_exit")
            return self.lease

    db_worker = _C14DispatcherWorker(db_trace)
    db_dispatcher = _c14_dispatcher(
        BlockedDatabaseRuntime(db_trace, lease),
        _C14DispatcherOperations(db_trace),
        _C14DispatcherSettlement(db_trace),
        db_worker,
        clock=ManualMonotonicClock(),
        waiter=db_waiter,
        io_runner=db_runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    db_run = asyncio.create_task(db_dispatcher.run())
    await asyncio.wait_for(
        asyncio.to_thread(db_entered.wait),
        timeout=5,
    )
    db_close = asyncio.create_task(db_dispatcher.close())
    for _ in range(10):
        db_dispatcher.wake()
    db_release.set()
    await db_close
    await db_run
    db_runner.close()
    assert db_trace.index("blocked_db_exit") < db_trace.index("worker_close")
    assert db_trace[-1] == ("release", lease)
    db_after_close = tuple(db_trace)
    db_dispatcher.wake()
    assert tuple(db_trace) == db_after_close


@pytest.mark.asyncio
async def test_task7_c14_composition_supervision_fresh_uuid_and_controlled_close():
    """Fresh-instance supervision and close retain every owned future."""
    import gateway.project_runtime_worker as worker_module
    import gateway.run as run_module
    import gateway.session as session_module
    from gateway.project_runtime_dispatcher import ProjectRuntimeDispatcher
    from hermes_cli.project_runtime import DispatcherLease

    assert inspect.iscoroutinefunction(
        getattr(ProjectRuntimeDispatcher, "close", None)
    ), "C14 requires cancellation-safe dispatcher close"
    assert inspect.iscoroutinefunction(
        getattr(worker_module.CanonicalProjectRuntimeWorker, "close", None)
    ), "C14 requires an idempotent cache-draining worker close"
    gateway_runner_type = getattr(run_module, "GatewayRunner", None)
    assert inspect.isclass(
        gateway_runner_type
    ), "C14 supervision belongs to the public GatewayRunner composition root"
    assert inspect.iscoroutinefunction(
        getattr(gateway_runner_type, "start", None)
    ), "GatewayRunner.start must compose and supervise the recurring runtime"
    assert inspect.iscoroutinefunction(
        getattr(gateway_runner_type, "stop", None)
    ), "GatewayRunner.stop must close C14 before generic teardown"

    lease_a = DispatcherLease(_INSTANCE_A, 1, 1, 300)
    uuid_calls = []
    trace = []

    def invalid_uuid():
        uuid_calls.append("invalid")
        return "not-a-v4"

    runtime = _C14DispatcherRuntime(trace, lease_a)
    operations = _C14DispatcherOperations(trace)
    settlement = _C14DispatcherSettlement(trace)
    worker = _C14DispatcherWorker(trace)
    runner = RetainedThreadRunner("c14-invalid-id-io")
    with pytest.raises(ValueError, match="UUIDv4|uuid"):
        _c14_dispatcher(
            runtime,
            operations,
            settlement,
            worker,
            clock=ManualMonotonicClock(),
            waiter=WakeWaitProbe(),
            io_runner=runner,
            uuid_factory=invalid_uuid,
        )
    assert uuid_calls == ["invalid"]
    assert trace == []
    assert runner.calls == []
    runner.close()

    # Default cap is one and the UUID factory is consumed once per instance.
    generated = iter((_INSTANCE_A, _INSTANCE_B))
    uuid_calls.clear()

    def next_uuid():
        value = next(generated)
        uuid_calls.append(value)
        return value

    dispatchers = []
    for expected, lease in (
        (_INSTANCE_A, lease_a),
        (
            _INSTANCE_B,
            DispatcherLease(_INSTANCE_B, 1, 1, 300),
        ),
    ):
        local_trace = []
        local_waiter = WakeWaitProbe()
        local_runner = RetainedThreadRunner(f"c14-id-{expected[:4]}-io")
        local_runtime = _C14DispatcherRuntime(local_trace, lease)
        local_runtime.config = _C14NoConfigRead()
        dispatcher = _c14_dispatcher(
            local_runtime,
            _C14DispatcherOperations(local_trace),
            _C14DispatcherSettlement(local_trace),
            _C14DispatcherWorker(local_trace),
            clock=ManualMonotonicClock(),
            waiter=local_waiter,
            io_runner=local_runner,
            uuid_factory=next_uuid,
        )
        dispatchers.append(
            (
                dispatcher,
                local_trace,
                lease,
                local_runner,
                local_waiter,
            )
        )
    assert uuid_calls == [_INSTANCE_A, _INSTANCE_B]
    for (
        dispatcher,
        local_trace,
        lease,
        local_runner,
        waiter,
    ) in dispatchers:
        run_task = await _c14_begin(dispatcher, waiter)
        await _c14_close(dispatcher, run_task, local_runner)
        assert local_trace[0] == (
            "acquire",
            lease.instance_id,
            30,
        )
        assert all(
            not (
                isinstance(entry, tuple)
                and entry[0] in {"renew", "claim"}
                and entry[-1] != 30
            )
            for entry in local_trace
        )
        assert local_trace[-1] == ("release", lease)

    # One fresh UUID is threaded unchanged through acquire, operation
    # recovery, every runnable claim, both returned starts and exact release.
    # Two slots are injected only so both start sources are observable in one
    # bounded tick; the default one-slot behavior is proved independently.
    identity_trace = []
    identity_waiter = WakeWaitProbe()
    identity_runner = RetainedThreadRunner("c14-identity-io")
    recovery_start = _task7_c5_worker_start(
        "approved_operation",
        "identity-recovery",
        lease_a,
        worker_id=_INSTANCE_A,
    )
    queued_start = _task7_c5_worker_start(
        "queued_turn",
        "identity-queued",
        lease_a,
        worker_id=_INSTANCE_A,
    )
    identity_runtime = _C14DispatcherRuntime(
        identity_trace,
        lease_a,
        runnable_pages=(
            SimpleNamespace(
                projects=(
                    SimpleNamespace(
                        project_id=queued_start.claim.project_id
                    ),
                ),
                scanned_through="identity-runnable-end",
                reached_epoch_end=True,
            ),
        ),
        queued_starts=(queued_start,),
    )
    identity_operations = _C14DispatcherOperations(
        identity_trace,
        recovery_pages=(
            SimpleNamespace(
                starts=(recovery_start,),
                scanned_through="identity-recovery-end",
                reached_epoch_end=True,
            ),
        ),
    )

    class IdentityWorker(_C14DispatcherWorker):
        def __init__(self, trace):
            super().__init__(trace)
            self.all_started = asyncio.Event()

        async def run_start(self, start):
            self.trace.append(("worker_start", start))
            self.starts.append(start)
            if len(self.starts) == 2:
                self.all_started.set()
            await self.start_release.wait()

    identity_worker = IdentityWorker(identity_trace)
    identity_dispatcher = _c14_dispatcher(
        identity_runtime,
        identity_operations,
        _C14DispatcherSettlement(identity_trace),
        identity_worker,
        clock=ManualMonotonicClock(),
        waiter=identity_waiter,
        io_runner=identity_runner,
        uuid_factory=lambda: _INSTANCE_A,
        worker_cap=2,
    )
    identity_run = await _c14_begin(
        identity_dispatcher,
        identity_waiter,
    )
    await _c14_next_tick(identity_dispatcher, identity_waiter)
    await asyncio.wait_for(identity_worker.all_started.wait(), timeout=5)
    assert identity_trace[0] == ("acquire", _INSTANCE_A, 30)
    identity_recovery = next(
        entry
        for entry in identity_trace
        if isinstance(entry, tuple) and entry[0] == "recovery_scan"
    )
    assert identity_recovery[1:5] == (
        _INSTANCE_A,
        90,
        lease_a,
        2,
    )
    identity_claim = next(
        entry
        for entry in identity_trace
        if isinstance(entry, tuple) and entry[0] == "claim"
    )
    assert identity_claim[2:] == (
        _INSTANCE_A,
        90,
        lease_a,
    )
    assert identity_worker.starts == [
        recovery_start,
        queued_start,
    ]
    assert all(
        start.claim.worker_id == _INSTANCE_A
        and start.dispatcher_lease == lease_a
        for start in identity_worker.starts
    )
    identity_worker.start_release.set()
    await _c14_close(
        identity_dispatcher,
        identity_run,
        identity_runner,
    )
    assert identity_trace[-1] == ("release", lease_a)

    # With no timing/limit/cap overrides, the observable defaults remain
    # internal and never consult live configuration.
    default_trace = []
    default_clock = ManualMonotonicClock()
    default_waiter = WakeWaitProbe()
    default_runner = RetainedThreadRunner("c14-defaults-io")
    default_recovery = _task7_c5_worker_start(
        "approved_operation",
        "default-cap",
        lease_a,
        worker_id=_INSTANCE_A,
    )
    default_queued = _task7_c5_worker_start(
        "queued_turn",
        "default-cap-queued",
        lease_a,
        worker_id=_INSTANCE_A,
    )
    default_runtime = _C14DispatcherRuntime(
        default_trace,
        lease_a,
        runnable_pages=(
            SimpleNamespace(
                projects=(
                    SimpleNamespace(
                        project_id=default_queued.claim.project_id
                    ),
                ),
                scanned_through="default-runnable-end",
                reached_epoch_end=True,
            ),
        ),
        queued_starts=(default_queued,),
    )
    default_runtime.config = _C14NoConfigRead()
    default_worker = _C14DispatcherWorker(default_trace)
    default_dispatcher = _c14_dispatcher(
        default_runtime,
        _C14DispatcherOperations(
            default_trace,
            recovery_pages=(
                SimpleNamespace(
                    starts=(default_recovery,),
                    scanned_through="default-recovery-end",
                    reached_epoch_end=True,
                ),
            ),
        ),
        _C14DispatcherSettlement(default_trace),
        default_worker,
        clock=default_clock,
        waiter=default_waiter,
        io_runner=default_runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    default_run = await _c14_begin(
        default_dispatcher,
        default_waiter,
    )
    await _c14_next_tick(default_dispatcher, default_waiter)
    await asyncio.wait_for(default_worker.start_entered.wait(), timeout=5)
    default_recovery_call = next(
        entry
        for entry in default_trace
        if isinstance(entry, tuple) and entry[0] == "recovery_scan"
    )
    assert default_recovery_call[1:5] == (
        _INSTANCE_A,
        90,
        lease_a,
        1,
    )
    assert default_worker.starts == [default_recovery]
    assert default_dispatcher.available_slots == 0
    assert not any(
        isinstance(entry, tuple) and entry[0] == "claim"
        for entry in default_trace
    )
    assert all(call[1] == 1 for call in default_waiter.calls)
    assert all(
        entry[-1] == 100
        for entry in default_trace
        if isinstance(entry, tuple)
        and entry[0]
        in {
            "settlement_scan",
            "reconcile",
            "expiry",
            "recovery_scan",
            "runnable_scan",
        }
    )
    default_clock.advance_to(10)
    await _c14_next_tick(default_dispatcher, default_waiter)
    assert [
        entry
        for entry in default_trace
        if isinstance(entry, tuple) and entry[0] == "renew"
    ] == [("renew", lease_a, 30)]
    default_worker.start_release.set()
    await _c14_close(
        default_dispatcher,
        default_run,
        default_runner,
    )

    # A second concurrent run is rejected before another facade call.
    trace = []
    waiter = WakeWaitProbe()
    runner = RetainedThreadRunner("c14-double-run-io")
    runtime = _C14DispatcherRuntime(trace, lease_a)
    dispatcher = _c14_dispatcher(
        runtime,
        _C14DispatcherOperations(trace),
        _C14DispatcherSettlement(trace),
        _C14DispatcherWorker(trace),
        clock=ManualMonotonicClock(),
        waiter=waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    first_run = await _c14_begin(dispatcher, waiter)
    with pytest.raises(RuntimeError, match="already running|concurrent"):
        await dispatcher.run()
    assert sum(entry[0] == "acquire" for entry in trace) == 1
    await _c14_close(dispatcher, first_run, runner)

    # Close may be entered from the run task itself (for example from its
    # finalization path).  It must skip awaiting that exact task while still
    # performing worker close and exact retained-lease release once.
    trace = []
    runner = RetainedThreadRunner("c14-self-close-io")

    class SelfClosingWaiter:
        def __init__(self):
            self.dispatcher = None
            self.calls = []

        async def __call__(self, wake_event, timeout_seconds):
            self.calls.append((wake_event, timeout_seconds))
            assert self.dispatcher is not None
            await self.dispatcher.close()
            return "closed"

    self_waiter = SelfClosingWaiter()
    dispatcher = _c14_dispatcher(
        _C14DispatcherRuntime(trace, lease_a),
        _C14DispatcherOperations(trace),
        _C14DispatcherSettlement(trace),
        _C14DispatcherWorker(trace),
        clock=ManualMonotonicClock(),
        waiter=self_waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    self_waiter.dispatcher = dispatcher
    await asyncio.wait_for(dispatcher.run(), timeout=5)
    await dispatcher.close()
    runner.close()
    assert [entry[0] for entry in trace if isinstance(entry, tuple)] == [
        "acquire",
        "release",
    ]
    assert trace.count("worker_close") == 1
    assert trace[-1] == ("release", lease_a)

    # Close-before-run closes the worker, performs no lease work, and makes a
    # later sequential run a no-op.
    trace = []
    waiter = WakeWaitProbe()
    runner = RetainedThreadRunner("c14-close-before-run-io")
    runtime = _C14DispatcherRuntime(trace, lease_a)
    dispatcher = _c14_dispatcher(
        runtime,
        _C14DispatcherOperations(trace),
        _C14DispatcherSettlement(trace),
        _C14DispatcherWorker(trace),
        clock=ManualMonotonicClock(),
        waiter=waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    dispatcher.wake()
    await dispatcher.close()
    await dispatcher.close()
    await dispatcher.run()
    assert trace == ["worker_close"]
    runner.close()

    # Cancelling one close waiter cannot cancel the shared cleanup. The other
    # waiter observes one stop/join, one worker close, and release last.
    trace = []
    waiter = WakeWaitProbe()
    runner = RetainedThreadRunner("c14-shared-close-io")
    start = _task7_c5_worker_start(
        "approved_operation",
        "close-shared",
        lease_a,
        worker_id=_INSTANCE_A,
    )
    runtime = _C14DispatcherRuntime(trace, lease_a)
    operations = _C14DispatcherOperations(
        trace,
        recovery_pages=(
            SimpleNamespace(
                starts=(start,),
                scanned_through=object(),
                reached_epoch_end=True,
            ),
        ),
    )
    worker = _C14DispatcherWorker(trace)
    dispatcher = _c14_dispatcher(
        runtime,
        operations,
        _C14DispatcherSettlement(trace),
        worker,
        clock=ManualMonotonicClock(),
        waiter=waiter,
        io_runner=runner,
        uuid_factory=lambda: _INSTANCE_A,
    )
    run_task = await _c14_begin(dispatcher, waiter)
    await _c14_next_tick(dispatcher, waiter)
    await asyncio.wait_for(worker.start_entered.wait(), timeout=5)
    worker.block_close = True
    close_one = asyncio.create_task(dispatcher.close())
    close_two = asyncio.create_task(dispatcher.close())
    worker.start_release.set()
    await asyncio.wait_for(worker.close_entered.wait(), timeout=5)
    close_one.cancel()
    first_outcome = (
        await asyncio.gather(close_one, return_exceptions=True)
    )[0]
    assert isinstance(first_outcome, asyncio.CancelledError)
    assert not close_two.done()
    worker.close_release.set()
    await close_two
    await run_task
    runner.close()
    assert trace.count("worker_close") == 1
    assert trace[-1] == ("release", lease_a)
    assert worker.starts == [start]
    assert worker.starts[0].claim.worker_id == _INSTANCE_A
    assert dispatcher.available_slots == 1

    # Both the durable-settlement signal and an ordinary worker exception
    # release their exact reservation.  Neither is an excuse to drain
    # settlement during close: only the next fresh dispatcher may do that.
    for failure in (
        getattr(worker_module, "ProjectCheckpointSettlementPending", None),
        RuntimeError("c14 worker failure"),
    ):
        assert failure is not None, "C14 exposes settlement-pending distinctly"
        slot_trace = []
        slot_waiter = WakeWaitProbe()
        slot_runner = RetainedThreadRunner("c14-slot-release-io")
        slot_start = _task7_c5_worker_start(
            "approved_operation",
            f"slot-{type(failure).__name__}",
            lease_a,
            worker_id=_INSTANCE_A,
        )
        slot_runtime = _C14DispatcherRuntime(slot_trace, lease_a)
        slot_worker = _C14DispatcherWorker(slot_trace)
        slot_worker.raise_on_start = failure
        slot_settlement = _C14DispatcherSettlement(slot_trace)
        slot_dispatcher = _c14_dispatcher(
            slot_runtime,
            _C14DispatcherOperations(
                slot_trace,
                recovery_pages=(
                    SimpleNamespace(
                        starts=(slot_start,),
                        scanned_through=object(),
                        reached_epoch_end=True,
                    ),
                ),
            ),
            slot_settlement,
            slot_worker,
            clock=ManualMonotonicClock(),
            waiter=slot_waiter,
            io_runner=slot_runner,
            uuid_factory=lambda: _INSTANCE_A,
        )
        slot_run = await _c14_begin(slot_dispatcher, slot_waiter)
        await _c14_next_tick(slot_dispatcher, slot_waiter)
        await asyncio.wait_for(slot_worker.start_entered.wait(), timeout=5)
        await _c14_next_tick(slot_dispatcher, slot_waiter)
        assert slot_dispatcher.available_slots == 1
        settlement_before_close = slot_settlement.scan_calls
        await _c14_close(slot_dispatcher, slot_run, slot_runner)
        assert slot_settlement.scan_calls == settlement_before_close

    # Exercise controlled close through the real canonical worker, not a
    # presence-only worker double.  Two completed turns create two idle cache
    # owners; a third raw turn remains event-gated while dispatcher close
    # cancels and joins it.  The first cache release then fails, but the sibling
    # is still drained and the exact Core lease is released last.  Because the
    # cache is detached atomically before release awaits, a fresh run_start at
    # that gate must reach zero dependency calls.
    from gateway.config import GatewayConfig
    from gateway.session import (
        ProjectBatchApplyResult,
        ProjectHistorySnapshot,
    )
    from hermes_cli.project_runtime import (
        ClaimControl,
        TurnAttemptIdentity,
        TurnExecutionInput,
        TurnOrigin,
    )
    from hermes_state import PendingProjectBatch

    close_trace = []
    raw_entered = asyncio.Event()
    raw_cancelled = asyncio.Event()
    raw_release = asyncio.Event()
    cache_release_entered = asyncio.Event()
    cache_release = asyncio.Event()
    cache_release_error = RuntimeError("c14 cache release failure")

    class ClosingRuntime(_C14DispatcherRuntime):
        def __init__(self):
            live_start = _task7_c5_worker_start(
                "queued_turn",
                "raw-close",
                lease_a,
                worker_id=_INSTANCE_A,
            )
            super().__init__(
                close_trace,
                lease_a,
                runnable_pages=(
                    SimpleNamespace(
                        projects=(
                            SimpleNamespace(
                                project_id=live_start.claim.project_id
                            ),
                        ),
                        scanned_through="raw-close-end",
                        reached_epoch_end=True,
                    ),
                ),
                queued_starts=(live_start,),
            )
            self.live_start = live_start
            self.worker_calls = []

        async def mark_turn_started(self, claim):
            self.worker_calls.append(("mark", claim.turn_id))
            return claim

        async def execution_input_for_claim(self, claim):
            self.worker_calls.append(("input", claim.turn_id))
            return TurnExecutionInput(
                TurnAttemptIdentity(
                    project_id=claim.project_id,
                    turn_id=claim.turn_id,
                    sequence=claim.sequence,
                    worker_id=claim.worker_id,
                    attempt_id=claim.attempt_id,
                    lease_generation=claim.lease_generation,
                    fencing_token=claim.fencing_token,
                    canonical_session_id=claim.canonical_session_id,
                    lease_expires_at=claim.lease_expires_at,
                ),
                {"message": claim.turn_id},
                TurnOrigin(
                    "c14-close-owner",
                    "desktop",
                    "c14-close-window",
                    "owner",
                ),
                7,
            )

        async def heartbeat_turn(self, claim, *, lease_seconds):
            self.worker_calls.append(
                ("heartbeat", claim.turn_id, lease_seconds)
            )
            return claim

        async def control_for_claim(self, claim):
            self.worker_calls.append(("control", claim.turn_id))
            return ClaimControl("running", 3, claim.lease_expires_at)

        async def commit_turn_with_task7_batch(
            self,
            claim,
            result,
            *,
            transcript_batch_id,
        ):
            self.worker_calls.append(
                ("commit", claim.turn_id, transcript_batch_id)
            )
            return result

        async def acknowledge_stopped(self, claim):
            self.worker_calls.append(("stopped", claim.turn_id))

    class ClosingBatches:
        def __init__(self):
            self.calls = []

        async def load_project_history(self, session_id):
            self.calls.append(("history", session_id))
            return ProjectHistorySnapshot(session_id, (), 0)

        async def prepare_terminal_result(
            self,
            claim,
            *,
            batch_id,
            status,
            base_message_count,
            messages,
        ):
            self.calls.append(("prepare", claim.turn_id, batch_id))
            return PendingProjectBatch(
                batch_id=batch_id,
                batch_creation_sequence=1,
                kind="terminal_result",
                state="prepared",
                attempt=TurnAttemptIdentity(
                    claim.project_id,
                    claim.turn_id,
                    claim.sequence,
                    claim.worker_id,
                    claim.attempt_id,
                    claim.lease_generation,
                    claim.fencing_token,
                    claim.canonical_session_id,
                    claim.lease_expires_at,
                ),
                terminal_status=status,
                operation_id=None,
                approval_id=None,
                base_message_count=base_message_count,
                created_at=1,
            )

        async def prepare_approval_checkpoint(self, *args, **kwargs):
            raise AssertionError(
                "controlled-close probe cannot prepare an approval"
            )

        async def apply_project_batch(self, batch_id):
            self.calls.append(("apply", batch_id))
            return ProjectBatchApplyResult("published")

    class ClosingTurn:
        def __init__(self, label, *, blocked):
            self.label = label
            self.blocked = blocked
            self.cancel_count = 0
            self.quiescent_count = 0

        def request_cancel(self):
            self.cancel_count += 1
            close_trace.append(("turn_interrupt", self.label))
            return True

        async def wait_quiescent(self):
            self.quiescent_count += 1
            close_trace.append(("turn_quiescent", self.label))

        async def result(self):
            close_trace.append(("turn_result", self.label))
            if self.blocked:
                raw_entered.set()
                try:
                    await raw_release.wait()
                except asyncio.CancelledError:
                    raw_cancelled.set()
                    await raw_release.wait()
                    raise
            return worker_module.ProjectAgentRunResult(
                "succeeded",
                0,
                (
                    {"role": "user", "content": self.label},
                    {"role": "assistant", "content": "done"},
                ),
            )

    class ClosingAgent:
        def __init__(self, label, *, blocked=False):
            self.label = label
            self.blocked = blocked
            self.turns = []

        def create_turn(self, execution, operation):
            assert operation is None
            turn = ClosingTurn(self.label, blocked=self.blocked)
            self.turns.append(turn)
            return turn

    class ClosingBuild:
        def __init__(self, factory):
            self.factory = factory
            self.revisions = worker_module.ProjectAgentRevisions(
                "c14-close-base",
                "c14-close-tools",
                "c14-close-model",
            )

        async def create_project_agent(self, *, history):
            agent = self.factory.available.pop(0)
            self.factory.created.append(agent)
            return agent

    class ClosingFactory:
        def __init__(self):
            self.idle_a = ClosingAgent("idle-a")
            self.idle_b = ClosingAgent("idle-b")
            self.raw = ClosingAgent("raw", blocked=True)
            self.available = [self.idle_a, self.idle_b, self.raw]
            self.created = []
            self.resolve_calls = []
            self.released = []

        async def resolve_project_agent(
            self,
            *,
            context,
            contract_revision,
        ):
            self.resolve_calls.append((context, contract_revision))
            return ClosingBuild(self)

        async def release_project_agent(self, agent):
            self.released.append(agent)
            close_trace.append(("agent_release_enter", agent.label))
            if agent is self.idle_a:
                cache_release_entered.set()
                await cache_release.wait()
                close_trace.append(("agent_release_error", agent.label))
                raise cache_release_error
            close_trace.append(("agent_release_exit", agent.label))

    class ZeroApprovedOperations:
        def __init__(self):
            self.calls = []

        def create_turn(
            self,
            execution,
            operation,
            *,
            base_message_count,
        ):
            self.calls.append(
                (execution, operation, base_message_count)
            )
            raise AssertionError(
                "queued controlled-close turn used approved operation port"
            )

    closing_runtime = ClosingRuntime()
    closing_batches = ClosingBatches()
    closing_factory = ClosingFactory()
    zero_approved = ZeroApprovedOperations()
    batch_ids = iter(
        (
            "123e4567-e89b-42d3-a456-426614174000",
            "223e4567-e89b-42d3-a456-426614174000",
            "323e4567-e89b-42d3-a456-426614174000",
        )
    )
    with tempfile.TemporaryDirectory(
        prefix="c14-cache-detach-"
    ) as directory:
        closing_worker = worker_module.CanonicalProjectRuntimeWorker(
            closing_runtime,
            closing_batches,
            closing_factory,
            GatewayConfig(),
            profile_home=Path(directory),
            lease_seconds=90,
            heartbeat_interval_seconds=30,
            batch_id_factory=batch_ids.__next__,
            approved_operations=zero_approved,
        )
        for suffix in ("idle-a", "idle-b"):
            await closing_worker.run_start(
                _task7_c5_worker_start(
                    "queued_turn",
                    suffix,
                    lease_a,
                    worker_id=_INSTANCE_A,
                )
            )
        assert closing_factory.released == []

        closing_waiter = WakeWaitProbe()
        closing_runner = RetainedThreadRunner("c14-real-close-io")
        closing_settlement = _C14DispatcherSettlement(close_trace)
        closing_dispatcher = _c14_dispatcher(
            closing_runtime,
            _C14DispatcherOperations(close_trace),
            closing_settlement,
            closing_worker,
            clock=ManualMonotonicClock(),
            waiter=closing_waiter,
            io_runner=closing_runner,
            uuid_factory=lambda: _INSTANCE_A,
        )
        closing_run = await _c14_begin(
            closing_dispatcher,
            closing_waiter,
        )
        await _c14_next_tick(closing_dispatcher, closing_waiter)
        await asyncio.wait_for(raw_entered.wait(), timeout=5)
        settlement_before_close = closing_settlement.scan_calls
        close_one = asyncio.create_task(closing_dispatcher.close())
        close_two = asyncio.create_task(closing_dispatcher.close())
        await asyncio.wait_for(raw_cancelled.wait(), timeout=5)
        assert not close_one.done()
        assert not close_two.done()
        raw_release.set()
        await asyncio.wait_for(cache_release_entered.wait(), timeout=5)

        dependency_counts = (
            len(closing_runtime.worker_calls),
            len(closing_batches.calls),
            len(closing_factory.resolve_calls),
            len(zero_approved.calls),
        )
        with pytest.raises(RuntimeError, match="closed|closing"):
            await closing_worker.run_start(
                _task7_c5_worker_start(
                    "queued_turn",
                    "post-detach",
                    lease_a,
                    worker_id=_INSTANCE_A,
                )
            )
        assert (
            len(closing_runtime.worker_calls),
            len(closing_batches.calls),
            len(closing_factory.resolve_calls),
            len(zero_approved.calls),
        ) == dependency_counts

        cache_release.set()
        close_outcomes = await asyncio.gather(
            close_one,
            close_two,
            return_exceptions=True,
        )
        run_outcome = (
            await asyncio.gather(
                closing_run,
                return_exceptions=True,
            )
        )[0]
        closing_runner.close()

    assert close_outcomes == [
        cache_release_error,
        cache_release_error,
    ]
    assert (
        run_outcome is None
        or run_outcome is cache_release_error
    )
    assert closing_factory.released == [
        closing_factory.raw,
        closing_factory.idle_a,
        closing_factory.idle_b,
    ]
    assert closing_factory.raw.turns[0].cancel_count == 1
    assert closing_factory.raw.turns[0].quiescent_count == 1
    assert closing_settlement.scan_calls == settlement_before_close
    assert close_trace[-1] == ("release", lease_a)
    assert closing_dispatcher.available_slots == 1

    # Exercise the real GatewayRunner supervisor through only public
    # start/stop. Generic watcher bodies are replaced before construction by
    # immediate hermetic coroutines; the real `_spawn_supervised` implementation
    # remains untouched and its one-second crash backoff is event-gated.
    from contextlib import contextmanager
    from contextvars import copy_context
    from types import MappingProxyType
    from unittest.mock import patch
    from uuid import UUID

    import agent.auxiliary_client as auxiliary_client_module
    import agent.agent_init as agent_init_module
    import agent.model_metadata as model_metadata_module
    import agent.shell_hooks as shell_hooks_module
    import cron.scheduler as cron_scheduler_module
    import gateway.channel_directory as channel_directory_module
    import gateway.hooks as hooks_module
    import gateway.pairing as pairing_module
    import gateway.project_runtime_dispatcher as dispatcher_module
    import gateway.relay as relay_module
    import gateway.shutdown_forensics as shutdown_forensics_module
    import gateway.status as status_module
    import hermes_cli.config as hermes_config_module
    import hermes_cli.plugins as plugins_module
    import hermes_cli.runtime_provider as runtime_provider_module
    import hermes_cli.security_advisories as security_advisories_module
    import hermes_state as state_module
    import run_agent as run_agent_module
    import tools.async_delegation as async_delegation_module
    import tools.browser_tool as browser_tool_module
    import tools.registry as tool_registry_module
    import tools.terminal_tool as terminal_tool_module
    import tools.tirith_security as tirith_security_module
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from hermes_cli.project_operations import (
        OperationApprovalSpec,
        OperationIntent,
    )
    from hermes_cli.project_policy import (
        ActorContext,
        ContractPolicyView,
        Decision,
        ProjectBindingView,
        ProjectCommand,
        ProjectPolicyView,
        decide as decide_project_policy,
    )
    from hermes_cli.project_runtime import (
        TurnAttemptIdentity,
        TurnExecutionInput,
        TurnOrigin,
    )
    from tools.process_registry import process_registry

    real_policy_facade_type = getattr(
        worker_module,
        "ProjectToolPolicySnapshotFacade",
        None,
    )
    assert inspect.isclass(real_policy_facade_type)
    real_agent_factory_type = getattr(
        worker_module,
        "GatewayProjectAgentFactory",
        None,
    )
    assert inspect.isclass(real_agent_factory_type)

    composition_trace = []
    first_close_entered = asyncio.Event()
    first_close_release = asyncio.Event()
    supervisor_backoff_entered = asyncio.Event()
    supervisor_backoff_release = asyncio.Event()
    successor_started = asyncio.Event()
    generic_watcher_started = asyncio.Event()
    generic_watcher_cancelled = asyncio.Event()
    generic_watcher_release = asyncio.Event()
    constructed = 0
    created_executors = []
    executor_shutdown_trace = []
    runner_join_trace = []
    composed_workers = []
    composed_dispatchers = []
    composed_io_runners = []
    composed_policy_facades = []
    runtime_facades = []
    operation_facades = []
    settlement_facades = []
    terminal_readback_facades = []
    checkpoint_read_facades = []
    batch_worker_facades = []
    operation_prepare_facades = []
    operation_execution_facades = []
    checkpoint_coordinators = []
    approved_coordinators = []
    approved_turns = []
    agent_factories = []
    executor_owner = threading.local()
    original_executor_type = (
        run_module.concurrent.futures.ThreadPoolExecutor
    )
    generic_watcher_noop_calls = []
    direct_scheduler_noop_calls = []
    lifecycle_noop_calls = []
    unexpected_scheduler_calls = []
    approval_id_calls = []
    authority_clock_calls = []
    c14_config_reads = []
    session_databases = []
    service_noop_calls = []
    dispatcher_uuid_factory_calls = []
    agent_dependency_probe_active = [False]
    agent_fallback_calls = []
    agent_snapshot_probes = []
    built_agent_probes = []
    bound_turn_probes = []
    raw_agent_context_observations = []
    agent_authorizer_calls = []
    seeded_model = "c14-frozen-route-model"
    seeded_provider = "openai"
    seeded_enabled_toolsets = ("file", "terminal")
    seeded_memory_settings = {
        "memory_enabled": False,
        "user_profile_enabled": False,
        "write_approval": True,
        "memory_char_limit": 1703,
        "user_char_limit": 907,
        "provider": "",
    }
    seeded_skill_settings = {
        "external_dirs": [],
        "template_vars": False,
        "inline_shell": False,
        "inline_shell_timeout": 17,
        "guard_agent_created": True,
        "write_approval": True,
    }
    seeded_compression_settings = {
        "enabled": False,
        "threshold": 0.731,
        "target_ratio": 0.417,
        "protect_last_n": 13,
        "proactive_prune_tokens": 47003,
        "proactive_prune_min_result_chars": 8111,
        "proactive_prune_min_reclaim_tokens": 4201,
        "min_tail_user_messages": 7,
    }
    seeded_runtime_settings = {
        "api_key": "c14-hermetic-key",
        "base_url": "https://c14.invalid/v1",
        "provider": seeded_provider,
        "requested_provider": seeded_provider,
        "api_mode": "chat_completions",
        "command": None,
        "args": [],
        "credential_pool": None,
        "max_tokens": 4097,
    }
    seeded_agent_source_config = {
        "model": {
            "default": seeded_model,
            "context_length": 131071,
            "max_tokens": seeded_runtime_settings["max_tokens"],
        },
        "memory": seeded_memory_settings,
        "skills": seeded_skill_settings,
        "compression": seeded_compression_settings,
        "platform_toolsets": {
            "local": list(seeded_enabled_toolsets),
        },
    }
    seeded_tool_schemas = (
        {
            "type": "function",
            "function": {
                "name": "project_status",
                "description": "C14 frozen project status descriptor",
                "parameters": {
                    "type": "object",
                    "properties": {},
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "local_code_edit",
                "description": "C14 frozen local edit descriptor",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "patch": {"type": "string"},
                    },
                    "required": ["patch"],
                },
            },
        },
    )
    seeded_tool_descriptors = tuple(
        schema["function"]["name"] for schema in seeded_tool_schemas
    )
    seeded_tool_definition_calls = []

    generic_supervised_watcher_names = (
        "_session_expiry_watcher",
        "_kanban_notifier_watcher",
        "_kanban_dispatcher_watcher",
        "_handoff_watcher",
        "_async_delegation_watcher",
        "_scale_to_zero_watcher",
        "_drain_control_watcher",
        "_run_process_watcher",
    )

    def inert_supervised_watcher(name):
        async def run_inert_watcher(*args, **kwargs):
            generic_watcher_noop_calls.append(name)

        return run_inert_watcher

    async def gated_generic_watcher(*args, **kwargs):
        generic_watcher_noop_calls.append(
            "_session_expiry_watcher"
        )
        generic_watcher_started.set()
        try:
            await generic_watcher_release.wait()
        except asyncio.CancelledError:
            composition_trace.append(
                ("generic_cancel", "_session_expiry_watcher")
            )
            generic_watcher_cancelled.set()
            raise

    async def inert_loop_heartbeat(*args, **kwargs):
        direct_scheduler_noop_calls.append("loop_heartbeat_forever")

    async def inert_platform_reconnect(*args, **kwargs):
        direct_scheduler_noop_calls.append(
            "_platform_reconnect_watcher"
        )

    def inert_resume_schedule(*args, **kwargs):
        direct_scheduler_noop_calls.append(
            "_schedule_resume_pending_sessions"
        )

    def forbidden_update_schedule(*args, **kwargs):
        unexpected_scheduler_calls.append(
            "_schedule_update_notification_watch"
        )

    async def inert_update_notification(*args, **kwargs):
        lifecycle_noop_calls.append("_send_update_notification")
        return True

    def inert_lifecycle(name, result=None):
        async def run_inert_lifecycle(*args, **kwargs):
            lifecycle_noop_calls.append(name)
            return result

        return run_inert_lifecycle

    def inert_sync_lifecycle(name, result=None):
        def run_inert_lifecycle(*args, **kwargs):
            lifecycle_noop_calls.append(name)
            return result

        return run_inert_lifecycle

    def inert_service(name, result=None):
        def run_inert_service(*args, **kwargs):
            service_noop_calls.append(name)
            return result

        return run_inert_service

    def inert_async_service(name, result=None):
        async def run_inert_service(*args, **kwargs):
            service_noop_calls.append(name)
            return result

        return run_inert_service

    class InertHookRegistry:
        def __init__(self):
            self.loaded_hooks = ()

        def discover_and_load(self):
            lifecycle_noop_calls.append("hooks.discover_and_load")

        async def emit(self, *args, **kwargs):
            lifecycle_noop_calls.append("hooks.emit")

    class InertPairingStore:
        def __init__(self, *args, **kwargs):
            service_noop_calls.append("PairingStore")

    class C14ConfigSentinel:
        timing_names = frozenset(
            {
                "project_runtime",
                "project_runtime_worker_cap",
                "project_runtime_dispatcher_lease_seconds",
                "project_runtime_dispatcher_renew_seconds",
                "project_runtime_turn_lease_seconds",
                "project_runtime_turn_heartbeat_seconds",
                "project_runtime_approval_lifetime_seconds",
                "project_runtime_poll_seconds",
                "project_runtime_scan_limit",
                "worker_cap",
                "dispatcher_lease_seconds",
                "dispatcher_renew_interval_seconds",
                "turn_lease_seconds",
                "turn_heartbeat_interval_seconds",
                "approval_lifetime_seconds",
                "poll_interval_seconds",
                "scan_limit",
            }
        )

        def __init__(self, delegate):
            self.delegate = delegate

        def __getattr__(self, name):
            if agent_dependency_probe_active[0]:
                agent_fallback_calls.append(
                    ("gateway_config_attribute", name)
                )
                raise AssertionError(
                    "agent dependency read live GatewayConfig: "
                    f"{name}"
                )
            if (
                name in self.timing_names
                or name.startswith("project_runtime_")
            ):
                c14_config_reads.append(("attribute", name))
                raise AssertionError(
                    f"C14 timing read live configuration: {name}"
                )
            return getattr(self.delegate, name)

    class C14ToolRegistrySentinel:
        def __init__(self, delegate):
            self.delegate = delegate

        def __getattr__(self, name):
            if agent_dependency_probe_active[0]:
                agent_fallback_calls.append(
                    ("tool_registry_attribute", name)
                )
                raise AssertionError(
                    "agent dependency read the live tool registry: "
                    f"{name}"
                )
            return getattr(self.delegate, name)

    hermetic_primary_instances = []
    hermetic_request_instances = []
    hermetic_provider_calls = []
    hermetic_provider_turns = []
    hermetic_provider_poison = []
    hermetic_provider_lifecycle_trace = []
    hermetic_provider_lock = threading.Lock()
    active_hermetic_provider_script = [None]

    class HermeticProjectCompletions:
        def __init__(self, owner):
            self.owner = owner

        def create(self, **kwargs):
            owner = self.owner
            context = copy_context()
            with hermetic_provider_lock:
                script = owner.script
                if (
                    owner.kind != "request"
                    or script is None
                    or active_hermetic_provider_script[0] is not script
                    or owner.create_count != 0
                    or owner.close_count != 0
                ):
                    hermetic_provider_poison.append(
                        ("unexpected_create", owner, dict(kwargs))
                    )
                    raise AssertionError(
                        "C14 graph used an unowned provider create"
                    )
                owner.create_count += 1
                hermetic_provider_lifecycle_trace.append(
                    (
                        len(hermetic_provider_lifecycle_trace),
                        "create",
                        owner,
                    )
                )
                call = (owner, self, dict(kwargs))
                script["calls"].append(call)
                hermetic_provider_calls.append(call)
                response = script["responses"][owner.request_index]
            raw_agent_context_observations.append(
                (
                    context,
                    threading.get_ident(),
                    getattr(executor_owner, "value", None),
                )
            )
            return response

    class HermeticProjectOpenAI:
        def __init__(self, *args, **kwargs):
            assert agent_dependency_probe_active[0]
            self.args = args
            self.kwargs = dict(kwargs)
            self.create_count = 0
            self.close_count = 0
            self.chat = SimpleNamespace(
                completions=HermeticProjectCompletions(self)
            )
            with hermetic_provider_lock:
                script = active_hermetic_provider_script[0]
                if script is None:
                    self.kind = "primary"
                    self.script = None
                    self.request_index = None
                    hermetic_primary_instances.append(self)
                    hermetic_provider_lifecycle_trace.append(
                        (
                            len(hermetic_provider_lifecycle_trace),
                            "constructor",
                            self,
                        )
                    )
                    return
                request_index = len(script["clients"])
                if request_index >= len(script["responses"]):
                    hermetic_provider_poison.append(
                        (
                            "unexpected_request_constructor",
                            request_index,
                            args,
                            kwargs,
                        )
                    )
                    raise AssertionError(
                        "C14 deny constructed an extra request client"
                    )
                self.kind = "request"
                self.script = script
                self.request_index = request_index
                script["clients"].append(self)
                hermetic_request_instances.append(self)
                hermetic_provider_lifecycle_trace.append(
                    (
                        len(hermetic_provider_lifecycle_trace),
                        "constructor",
                        self,
                    )
                )

        @property
        def closed(self):
            return self.close_count > 0

        @property
        def is_closed(self):
            return self.closed

        def close(self):
            with hermetic_provider_lock:
                valid_close = (
                    self.kind == "request"
                    and self.create_count == 1
                    and self.close_count == 0
                ) or (
                    self.kind == "primary"
                    and self.create_count == 0
                    and self.close_count == 0
                )
                if not valid_close:
                    hermetic_provider_poison.append(
                        (
                            "unexpected_close",
                            self,
                            self.create_count,
                            self.close_count,
                        )
                    )
                    raise AssertionError(
                        "C14 graph closed a provider out of order"
                    )
                self.close_count += 1
                hermetic_provider_lifecycle_trace.append(
                    (
                        len(hermetic_provider_lifecycle_trace),
                        "close",
                        self,
                    )
                )

    def hermetic_provider_events_for(client):
        with hermetic_provider_lock:
            return tuple(
                event
                for event in hermetic_provider_lifecycle_trace
                if event[2] is client
            )

    def project_status_tool_response(call_id):
        tool_call = SimpleNamespace(
            id=call_id,
            type="function",
            function=SimpleNamespace(
                name="project_status",
                arguments="{}",
            ),
        )
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content="",
                        tool_calls=[tool_call],
                    ),
                    finish_reason="tool_calls",
                )
            ],
            model="frozen-model",
            usage=None,
        )

    def arm_hermetic_provider_script(responses):
        script = {
            "responses": tuple(responses),
            "clients": [],
            "calls": [],
        }
        with hermetic_provider_lock:
            assert active_hermetic_provider_script[0] is None
            active_hermetic_provider_script[0] = script
        return script

    def finish_hermetic_provider_script(script, primary):
        with hermetic_provider_lock:
            assert active_hermetic_provider_script[0] is script
            active_hermetic_provider_script[0] = None
            clients = tuple(script["clients"])
            calls = tuple(script["calls"])
        assert len(clients) == len(script["responses"])
        assert len(calls) == len(script["responses"])
        assert [owner for owner, _, _ in calls] == list(clients)
        assert all(type(client) is HermeticProjectOpenAI for client in clients)
        assert all(client.kind == "request" for client in clients)
        assert all(client is not primary for client in clients)
        assert all(client.create_count == 1 for client in clients)
        assert all(client.close_count == 1 for client in clients)
        for client in clients:
            client_events = hermetic_provider_events_for(client)
            constructor_events = [
                event
                for event in client_events
                if event[1] == "constructor"
            ]
            create_events = [
                event for event in client_events
                if event[1] == "create"
            ]
            close_events = [
                event for event in client_events
                if event[1] == "close"
            ]
            assert len(constructor_events) == 1
            assert len(create_events) == 1
            assert len(close_events) == 1
            assert (
                constructor_events[0][0]
                < create_events[0][0]
                < close_events[0][0]
            )
        assert all(
            client.kwargs.get("max_retries") == 0
            for client in clients
        )
        assert primary.create_count == 0
        assert primary.close_count == 0
        record = SimpleNamespace(clients=clients, calls=calls)
        hermetic_provider_turns.append(record)
        return record

    class RecordingExecutor(original_executor_type):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created_executors.append(self)

        def submit(self, function, /, *args, **kwargs):
            def invoke():
                executor_owner.value = self
                return function(*args, **kwargs)

            return super().submit(invoke)

        def shutdown(self, wait=True, *, cancel_futures=False):
            executor_shutdown_trace.append(self)
            composition_trace.append(("executor_shutdown", self))
            return super().shutdown(
                wait=wait,
                cancel_futures=cancel_futures,
            )

    session_database_root = None

    class TracedSessionDatabase(state_module.SessionDB):
        def __init__(self, *args, **kwargs):
            label = f"session-{len(session_databases) + 1}"
            super().__init__(session_database_root / f"{label}.db")
            self.c14_label = label
            self.c14_closed = False
            session_databases.append(self)

        def close(self):
            if self.c14_closed:
                return
            self.c14_closed = True
            composition_trace.append(
                ("session_db_close", self.c14_label)
            )
            return super().close()

    class StrictDispatcherRuntimeFacade:
        def __init__(self, *args, **kwargs):
            self.io_runner = kwargs.get("io_runner")
            self.terminal_readback = kwargs.get("terminal_readback")
            assert type(self.terminal_readback) is (
                StrictTerminalReadbackFacade
            )
            runtime_facades.append(self)

    class StrictDispatcherOperationFacade:
        def __init__(self, *args, **kwargs):
            self.io_runner = kwargs.get("io_runner")
            self.executor_capabilities = kwargs.get(
                "executor_capabilities"
            )
            self.approval_checkpoints = kwargs.get(
                "approval_checkpoints"
            )
            assert type(self.approval_checkpoints) is (
                StrictCheckpointReadFacade
            )
            assert type(self.executor_capabilities) is type(
                ttl_capabilities
            )
            operation_facades.append(self)

    class StrictTerminalReadbackFacade:
        def __init__(self, *args, **kwargs):
            self.io_runner = kwargs.get("io_runner")
            terminal_readback_facades.append(self)

    class StrictCheckpointReadFacade:
        def __init__(self, *args, **kwargs):
            self.io_runner = kwargs.get("io_runner")
            checkpoint_read_facades.append(self)

    class StrictBatchWorkerFacade:
        def __init__(self, *args, **kwargs):
            self.io_runner = kwargs.get("io_runner")
            batch_worker_facades.append(self)

    class StrictOperationPrepareFacade:
        def __init__(self, *args, **kwargs):
            self.io_runner = kwargs.get("io_runner")
            operation_prepare_facades.append(self)

    class StrictOperationExecutionFacade:
        def __init__(self, *args, **kwargs):
            self.io_runner = kwargs.get("io_runner")
            self.approval_checkpoints = kwargs.get(
                "approval_checkpoints"
            )
            assert type(self.approval_checkpoints) is (
                StrictCheckpointReadFacade
            )
            operation_execution_facades.append(self)

    class StrictCheckpointCoordinator:
        def __init__(
            self,
            *,
            batches,
            operations,
            batch_id_factory,
            on_published,
        ):
            self.batches = batches
            self.operations = operations
            self.batch_id_factory = batch_id_factory
            self.on_published = on_published
            assert type(self.batches) is StrictBatchWorkerFacade
            assert type(self.operations) is StrictOperationPrepareFacade
            assert callable(self.batch_id_factory)
            assert callable(self.on_published)
            checkpoint_coordinators.append(self)

    class StrictApprovedCoordinator:
        def __init__(
            self,
            *,
            execution_facade,
            capability_registry,
            effect_runner,
        ):
            self.execution_facade = execution_facade
            self.capability_registry = capability_registry
            self.effect_runner = effect_runner
            assert type(self.execution_facade) is (
                StrictOperationExecutionFacade
            )
            assert type(self.capability_registry) is type(
                ttl_capabilities
            )
            assert callable(self.effect_runner)
            approved_coordinators.append(self)

    class StrictApprovedTurn:
        def __init__(
            self,
            execution,
            operation,
            *,
            base_message_count,
            coordinator,
        ):
            self.execution = execution
            self.operation = operation
            self.base_message_count = base_message_count
            self.coordinator = coordinator
            assert type(self.coordinator) is StrictApprovedCoordinator
            approved_turns.append(self)

    class ZeroShutdownSettlementFacade:
        def __init__(self, *args, **kwargs):
            self.io_runner = kwargs.get("io_runner")
            self.calls = []
            settlement_facades.append(self)

        def pending_project_batch_upper_watermark(self):
            self.calls.append("upper")
            return None

        def scan_pending_project_batches(self, **kwargs):
            self.calls.append(("scan", kwargs))
            return SimpleNamespace(
                batches=(),
                scanned_through=None,
                reached_epoch_end=True,
            )

        def apply_project_batch(self, batch_id):
            self.calls.append(("apply", batch_id))
            return SimpleNamespace(outcome="already_published")

    class ComposerCapability:
        canonical_action = "publish"
        command_revision = 1
        readback_kind = "remote-ledger"
        remote_idempotency_supported = True

        @property
        def fingerprint(self):
            return (
                self.canonical_action,
                self.command_revision,
                self.readback_kind,
                self.remote_idempotency_supported,
            )

    ttl_capability = ComposerCapability()
    ttl_capabilities = MappingProxyType(
        {ttl_capability.fingerprint: ttl_capability}
    )
    ttl_policy_snapshot = SimpleNamespace(
        project_id="c14-project",
        lifecycle="active",
        current_phase="implementation",
        roots=("c:/work",),
        approved_plan_ref="plan-7",
        contract_id="contract-c14",
        contract_status="active",
        contract_revision=7,
        contract_json_sha256="contract-json-sha256",
        allowed_action_classes=frozenset({"publish"}),
        allowed_phases=frozenset({"implementation"}),
        actor_id="owner-1",
        actor_surface="desktop",
        binding_id="desktop-binding",
        actor_is_owner=True,
        control_version=3,
        runtime_version=5,
    )

    class TtlPolicyConnection:
        def __init__(self):
            self.owner = threading.get_ident()
            self.closed = False

        def load_project_policy_snapshot(self, *args, **kwargs):
            assert not self.closed
            assert threading.get_ident() == self.owner
            return ttl_policy_snapshot

        def close(self):
            assert not self.closed
            assert threading.get_ident() == self.owner
            self.closed = True

    def ttl_policy_connection_factory():
        return TtlPolicyConnection()

    def materialize_ttl_policy(snapshot):
        return SimpleNamespace(
            project=ProjectPolicyView(
                snapshot.project_id,
                snapshot.lifecycle,
                snapshot.current_phase,
                snapshot.roots,
                snapshot.approved_plan_ref,
                (
                    ProjectBindingView(
                        snapshot.binding_id,
                        snapshot.actor_surface,
                        snapshot.actor_id,
                        snapshot.project_id,
                    ),
                ),
            ),
            contract_id=snapshot.contract_id,
            contract_status=snapshot.contract_status,
            contract_json_sha256=snapshot.contract_json_sha256,
            contract=ContractPolicyView(
                snapshot.contract_revision,
                snapshot.allowed_action_classes,
                snapshot.allowed_phases,
                snapshot.approved_plan_ref,
            ),
            actor=ActorContext(
                snapshot.actor_id,
                snapshot.actor_surface,
                snapshot.binding_id,
                snapshot.actor_is_owner,
            ),
            control_version=snapshot.control_version,
            runtime_version=snapshot.runtime_version,
        )

    def bind_ttl_read(*args, **kwargs):
        raise AssertionError("TTL probe is an operation proposal")

    def bind_ttl_operation(snapshot, execution, proposal):
        intent = proposal.intent
        command = ProjectCommand(
            intent.canonical_action,
            execution.attempt.project_id,
            snapshot.contract_revision,
            "publish",
            intent.targets,
            proposal.policy_batch_id,
            intent.batch_items,
            {"phase": snapshot.current_phase},
        )
        effect_scope = json.loads(proposal.effect_scope_json)
        authority_payload = {
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
            "policy_batch_id": proposal.policy_batch_id,
            "capability_fingerprint": list(
                proposal.capability_fingerprint
            ),
            "effect_scope": effect_scope,
        }
        authority_json = json.dumps(
            authority_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        return worker_module.BoundProjectOperationAuthority(
            command,
            intent,
            proposal.policy_batch_id,
            proposal.effect_scope_json,
            proposal.effect_scope_sha256,
            authority_json,
            hashlib.sha256(
                authority_json.encode("utf-8")
            ).hexdigest(),
        )

    def ttl_authority_clock():
        authority_clock_calls.append(100)
        return 100

    def ttl_approval_id():
        approval_id_calls.append(
            "623e4567-e89b-42d3-a456-426614174000"
        )
        return "623e4567-e89b-42d3-a456-426614174000"

    class ComposedPolicyFacade(real_policy_facade_type):
        def __init__(self, *args, **kwargs):
            assert "approval_lifetime_seconds" not in kwargs
            self.composer_capabilities = kwargs.get(
                "capability_registry"
            )
            self.composer_io_runner = kwargs.get("io_runner")
            super().__init__(
                ttl_policy_connection_factory,
                read_binder=bind_ttl_read,
                operation_binder=bind_ttl_operation,
                capability_registry=ttl_capabilities,
                policy_decider=decide_project_policy,
                snapshot_materializer=materialize_ttl_policy,
                authority_clock=ttl_authority_clock,
                approval_id_factory=ttl_approval_id,
                io_runner=self.composer_io_runner,
            )
            composed_policy_facades.append(self)

        async def authorize(
            self,
            execution,
            invocation,
            transcript,
        ):
            agent_authorizer_calls.append(
                (
                    self,
                    execution,
                    execution.origin,
                    invocation,
                    tuple(transcript),
                    threading.get_ident(),
                )
            )
            return SimpleNamespace(action="deny")

    class StrictAgentFactory(real_agent_factory_type):
        def __init__(
            self,
            *,
            snapshot_resolver,
            agent_builder,
            off_loop_runner,
            turn_context_binder,
            tool_authorizer,
            checkpoint_coordinator,
        ):
            self.c14_snapshot_resolver = snapshot_resolver
            self.c14_agent_builder = agent_builder
            self.c14_off_loop_runner = off_loop_runner
            self.c14_turn_context_binder = turn_context_binder
            self.c14_tool_authorizer = tool_authorizer
            self.c14_checkpoint_coordinator = checkpoint_coordinator
            self.c14_resolver_calls = []
            self.c14_builder_calls = []
            self.c14_binder_calls = []

            def observed_snapshot_resolver(context, contract_revision):
                previous_probe = agent_dependency_probe_active[0]
                agent_dependency_probe_active[0] = True
                try:
                    snapshot = snapshot_resolver(
                        context,
                        contract_revision,
                    )
                finally:
                    agent_dependency_probe_active[0] = previous_probe
                self.c14_resolver_calls.append(
                    (
                        context,
                        contract_revision,
                        snapshot,
                        threading.get_ident(),
                        getattr(executor_owner, "value", None),
                    )
                )
                return snapshot

            def observed_agent_builder(snapshot, **received_options):
                project_execution_gate = received_options[
                    "project_execution_gate"
                ]
                options = {
                    "project_execution_gate": (
                        project_execution_gate
                    ),
                    "session_db": received_options["session_db"],
                    "save_trajectories": received_options[
                        "save_trajectories"
                    ],
                    "quiet_mode": received_options["quiet_mode"],
                    "skip_memory": received_options["skip_memory"],
                    "streaming_callback": received_options[
                        "streaming_callback"
                    ],
                    "delivery_callback": received_options[
                        "delivery_callback"
                    ],
                    "approval_notifier": received_options[
                        "approval_notifier"
                    ],
                    "provider_metadata_prewarm": (
                        received_options["provider_metadata_prewarm"]
                    ),
                    "external_memory_sync": received_options[
                        "external_memory_sync"
                    ],
                    "memory_review": received_options["memory_review"],
                    "skill_review": received_options["skill_review"],
                    "plugin_lifecycle": received_options[
                        "plugin_lifecycle"
                    ],
                }
                assert options == {
                    "project_execution_gate": (
                        project_execution_gate
                    ),
                    "session_db": None,
                    "save_trajectories": False,
                    "quiet_mode": True,
                    "skip_memory": True,
                    "streaming_callback": None,
                    "delivery_callback": None,
                    "approval_notifier": None,
                    "provider_metadata_prewarm": False,
                    "external_memory_sync": False,
                    "memory_review": False,
                    "skill_review": False,
                    "plugin_lifecycle": False,
                }
                assert received_options["skip_context_files"] is True
                assert received_options["session_id"] == "c14-session"
                constructor_values = dict(snapshot.constructor_kwargs)
                assert all(
                    received_options[name] == value
                    for name, value in constructor_values.items()
                )
                assert set(received_options) == (
                    set(constructor_values)
                    | set(options)
                    | {"skip_context_files", "session_id"}
                )
                provider_count_before = len(
                    hermetic_primary_instances
                )
                previous_probe = agent_dependency_probe_active[0]
                agent_dependency_probe_active[0] = True
                try:
                    built = agent_builder(snapshot, **received_options)
                finally:
                    agent_dependency_probe_active[0] = previous_probe
                assert len(hermetic_primary_instances) == (
                    provider_count_before + 1
                )
                assert built.client is hermetic_primary_instances[-1]
                built._disable_streaming = True
                built_agent_probes.append(built)
                assert (
                    getattr(
                        built,
                        "project_execution_gate",
                        None,
                    )
                    is project_execution_gate
                )
                self.c14_builder_calls.append(
                    (
                        snapshot,
                        options,
                        built,
                        threading.get_ident(),
                        getattr(executor_owner, "value", None),
                    )
                )
                return built

            @contextmanager
            def observed_turn_context(execution):
                before = copy_context()
                inside = None
                try:
                    with turn_context_binder(execution) as entered:
                        inside = copy_context()
                        yield entered
                finally:
                    after = copy_context()
                    self.c14_binder_calls.append(
                        (
                            execution,
                            before,
                            inside,
                            after,
                            threading.get_ident(),
                            getattr(executor_owner, "value", None),
                        )
                    )

            super().__init__(
                snapshot_resolver=observed_snapshot_resolver,
                agent_builder=observed_agent_builder,
                off_loop_runner=off_loop_runner,
                turn_context_binder=observed_turn_context,
                tool_authorizer=tool_authorizer,
                checkpoint_coordinator=checkpoint_coordinator,
            )
            self.snapshot_resolver = observed_snapshot_resolver
            self.agent_builder = observed_agent_builder
            self.off_loop_runner = off_loop_runner
            self.turn_context_binder = observed_turn_context
            self.tool_authorizer = tool_authorizer
            self.checkpoint_coordinator = checkpoint_coordinator
            assert callable(self.snapshot_resolver)
            assert callable(self.agent_builder)
            assert callable(self.off_loop_runner)
            assert callable(self.turn_context_binder)
            assert type(self.tool_authorizer) is ComposedPolicyFacade
            assert type(self.checkpoint_coordinator) is (
                StrictCheckpointCoordinator
            )
            self.c14_dependencies = (
                self.c14_snapshot_resolver,
                self.c14_agent_builder,
                self.c14_off_loop_runner,
                self.c14_turn_context_binder,
                self.c14_tool_authorizer,
                self.c14_checkpoint_coordinator,
            )
            agent_factories.append(self)

    class GatewayComposedWorker:
        def __init__(self, *args, **kwargs):
            assert len(args) >= 4
            self.runtime = args[0]
            self.batches = args[1]
            self.agent_factory = args[2]
            self.config = args[3]
            assert type(self.agent_factory) is StrictAgentFactory
            self.approved_operations = kwargs.get(
                "approved_operations"
            )
            self.lease_seconds = kwargs.get("lease_seconds")
            self.heartbeat_seconds = kwargs.get(
                "heartbeat_interval_seconds"
            )
            self.keyword_names = frozenset(kwargs)
            self.label = None
            composed_workers.append(self)

        async def run_start(self, start):
            raise AssertionError(
                "supervision probe dispatcher cannot start a worker"
            )

        def request_stop(self, request):
            return False

        async def close(self):
            composition_trace.append(
                ("worker_close", self.label)
            )

    class GatewayComposedDispatcher:
        def __init__(self, label, instance_id, worker):
            self.label = label
            self.instance_id = instance_id
            self.worker = worker
            self.run_release = asyncio.Event()
            self.close_complete = asyncio.Event()
            self.close_started = False

        async def run(self):
            composition_trace.append(
                ("run", self.label, self.instance_id)
            )
            if self.label == "first":
                raise RuntimeError("C14 supervised crash")
            successor_started.set()
            await self.run_release.wait()

        async def close(self):
            if self.close_started:
                await self.close_complete.wait()
                return
            self.close_started = True
            composition_trace.append(
                ("close", self.label, self.instance_id)
            )
            if self.label == "first":
                first_close_entered.set()
                await first_close_release.wait()
            await self.worker.close()
            self.run_release.set()
            composition_trace.append(
                ("release", self.label, self.instance_id)
            )
            self.close_complete.set()

    def build_dispatcher(runtime, operations, worker, **kwargs):
        nonlocal constructed
        constructed += 1
        label = "first" if constructed == 1 else "second"
        settlement = kwargs.get("settlement")
        io_runner = kwargs.get("io_runner")
        uuid_factory = kwargs.get("uuid_factory")
        assert {
            "terminal_readback",
            "approval_checkpoints",
            "executor_capabilities",
        }.isdisjoint(kwargs)
        assert type(runtime) is StrictDispatcherRuntimeFacade
        assert type(operations) is StrictDispatcherOperationFacade
        assert type(settlement) is ZeroShutdownSettlementFacade
        assert type(worker) is GatewayComposedWorker
        capabilities = operations.executor_capabilities
        agent_factory = worker.agent_factory
        checkpoint = agent_factory.checkpoint_coordinator
        assert (
            agent_factory.tool_authorizer.composer_capabilities
            is capabilities
        )
        assert (
            agent_factory.tool_authorizer.composer_io_runner
            is io_runner
        )
        assert runtime.io_runner is io_runner
        assert operations.io_runner is io_runner
        assert settlement.io_runner is io_runner
        assert checkpoint.batches.io_runner is io_runner
        assert checkpoint.operations.io_runner is io_runner
        assert callable(uuid_factory)
        instance_id = str(uuid_factory())
        dispatcher_uuid_factory_calls.append(
            (label, uuid_factory, instance_id)
        )
        parsed = UUID(instance_id)
        assert parsed.version == 4
        assert str(parsed) == instance_id
        worker.label = label
        composition_trace.append(
            ("construct", label, instance_id)
        )
        dispatcher = GatewayComposedDispatcher(
            label,
            instance_id,
            worker,
        )
        composed_dispatchers.append(
            SimpleNamespace(
                label=label,
                instance_id=instance_id,
                runtime=runtime,
                operations=operations,
                settlement=settlement,
                worker=worker,
                agent_factory=agent_factory,
                batch_worker=checkpoint.batches,
                operation_prepare=checkpoint.operations,
                checkpoint_coordinator=checkpoint,
                capabilities=capabilities,
                terminal_readback=runtime.terminal_readback,
                approval_checkpoints=(
                    operations.approval_checkpoints
                ),
                io_runner=io_runner,
                dispatcher=dispatcher,
            )
        )
        composed_io_runners.append(io_runner)
        return dispatcher

    original_cfg_get = run_module.cfg_get
    config_sentinel = None

    def guarded_cfg_get(config_value, *path, **kwargs):
        normalized = tuple(str(part).lower() for part in path)
        if (
            config_value is config_sentinel
            and agent_dependency_probe_active[0]
        ):
            agent_fallback_calls.append(
                ("gateway_cfg_get", normalized)
            )
            raise AssertionError(
                "agent dependency read live GatewayConfig via cfg_get: "
                f"{normalized}"
            )
        if config_value is config_sentinel and (
            "project_runtime" in normalized
            or any(
                part in C14ConfigSentinel.timing_names
                for part in normalized
            )
        ):
            c14_config_reads.append(("cfg_get", normalized))
            raise AssertionError(
                f"C14 timing read live configuration: {normalized}"
            )
        return original_cfg_get(config_value, *path, **kwargs)

    def guarded_agent_config_service(name, result):
        def run_guarded_service(*args, **kwargs):
            if agent_dependency_probe_active[0]:
                agent_fallback_calls.append(
                    ("config_provider_service", name)
                )
                raise AssertionError(
                    "agent dependency used a live config/provider "
                    f"service: {name}"
                )
            service_noop_calls.append(name)
            if isinstance(result, dict):
                return dict(result)
            if isinstance(result, list):
                return list(result)
            return result

        return run_guarded_service

    def guarded_live_agent_fallback(name, implementation):
        def run_guarded_fallback(*args, **kwargs):
            if agent_dependency_probe_active[0]:
                agent_fallback_calls.append(
                    ("legacy_agent_fallback", name)
                )
                raise AssertionError(
                    "project agent dependency used a live legacy "
                    f"fallback: {name}"
                )
            return implementation(*args, **kwargs)

        return run_guarded_fallback

    def guarded_seeded_tool_definitions(
        enabled_toolsets=None,
        disabled_toolsets=None,
        *args,
        **kwargs,
    ):
        if agent_dependency_probe_active[0]:
            agent_fallback_calls.append(
                ("legacy_agent_fallback", "run_agent.get_tool_definitions")
            )
            raise AssertionError(
                "project agent dependency used live tool definitions"
            )
        resolved_toolsets = tuple(sorted(enabled_toolsets or ()))
        assert resolved_toolsets == seeded_enabled_toolsets
        assert not disabled_toolsets
        seeded_tool_definition_calls.append(
            (
                resolved_toolsets,
                tuple(args),
                dict(kwargs),
                threading.get_ident(),
            )
        )
        import copy

        return copy.deepcopy(list(seeded_tool_schemas))

    original_thread_type = threading.Thread

    def guarded_project_thread(*args, **kwargs):
        if agent_dependency_probe_active[0]:
            agent_fallback_calls.append(
                (
                    "unretained_agent_thread",
                    kwargs.get("name"),
                )
            )
            raise AssertionError(
                "project agent spawned an unretained thread"
            )
        return original_thread_type(*args, **kwargs)

    def exercise_exact_turn_binding(binder, execution):
        missing = object()
        before = copy_context()
        manager = binder(execution)
        enter = getattr(manager, "__enter__", None)
        exit_context = getattr(manager, "__exit__", None)
        assert callable(enter)
        assert callable(exit_context)
        entered = enter()
        try:
            inside = copy_context()
            variables = set(before) | set(inside)
            changed = tuple(
                variable
                for variable in variables
                if inside.get(variable, missing)
                is not before.get(variable, missing)
            )
            bound_values = tuple(inside.get(variable) for variable in changed)

            def binds_exact_turn(value):
                return (
                    value is execution
                    or getattr(value, "execution", None) is execution
                    or getattr(value, "execution_input", None) is execution
                    or (
                        getattr(value, "execution_attempt", None)
                        is execution.attempt
                        and getattr(value, "execution_origin", None)
                        is execution.origin
                    )
                )

            assert changed
            assert any(
                binds_exact_turn(value) for value in bound_values
            )
            if entered is not None:
                assert binds_exact_turn(entered)
        finally:
            exit_result = exit_context(None, None, None)
        after = copy_context()
        assert exit_result in {None, False}
        assert all(
            after.get(variable, missing)
            is before.get(variable, missing)
            for variable in set(before) | set(after)
        )
        bound_turn_probes.append(
            (
                execution,
                entered,
                changed,
                threading.get_ident(),
                getattr(executor_owner, "value", None),
            )
        )
        return entered

    async def event_gated_supervisor_wait(delay, *args, **kwargs):
        if delay == 1:
            supervisor_backoff_entered.set()
            await supervisor_backoff_release.wait()
            return
        unexpected_scheduler_calls.append(("asyncio.sleep", delay))
        raise AssertionError(f"unexpected scheduler sleep: {delay}")

    with tempfile.TemporaryDirectory(prefix="c14-gateway-") as directory:
        root = Path(directory)
        session_database_root = root / "state"
        session_database_root.mkdir()
        base_config = GatewayConfig(
            platforms={
                Platform.DISCORD: PlatformConfig(enabled=False, token="")
            },
            sessions_dir=root / "sessions",
        )
        config_sentinel = C14ConfigSentinel(base_config)
        frozen_registry_generation = (
            tool_registry_module.registry._generation
        )
        seeded_prompt_cache_settings = (
            gateway_runner_type._extract_cache_busting_config(
                seeded_agent_source_config
            )
        )
        assert seeded_prompt_cache_settings[
            "tools.registry_generation"
        ] == frozen_registry_generation
        tool_registry_sentinel = C14ToolRegistrySentinel(
            tool_registry_module.registry
        )
        with ExitStack() as patches:
            for module, name, replacement in (
                (
                    dispatcher_module,
                    "ProjectDispatcherRuntimeFacade",
                    StrictDispatcherRuntimeFacade,
                ),
                (
                    run_module,
                    "ProjectDispatcherRuntimeFacade",
                    StrictDispatcherRuntimeFacade,
                ),
                (
                    dispatcher_module,
                    "ProjectDispatcherOperationFacade",
                    StrictDispatcherOperationFacade,
                ),
                (
                    run_module,
                    "ProjectDispatcherOperationFacade",
                    StrictDispatcherOperationFacade,
                ),
                (
                    session_module,
                    "ProjectBatchSettlementFacade",
                    ZeroShutdownSettlementFacade,
                ),
                (
                    run_module,
                    "ProjectBatchSettlementFacade",
                    ZeroShutdownSettlementFacade,
                ),
                (
                    session_module,
                    "ProjectTask7TerminalReadbackFacade",
                    StrictTerminalReadbackFacade,
                ),
                (
                    run_module,
                    "ProjectTask7TerminalReadbackFacade",
                    StrictTerminalReadbackFacade,
                ),
                (
                    session_module,
                    "ProjectApprovalCheckpointReadFacade",
                    StrictCheckpointReadFacade,
                ),
                (
                    run_module,
                    "ProjectApprovalCheckpointReadFacade",
                    StrictCheckpointReadFacade,
                ),
                (
                    session_module,
                    "ProjectBatchWorkerFacade",
                    StrictBatchWorkerFacade,
                ),
                (
                    run_module,
                    "ProjectBatchWorkerFacade",
                    StrictBatchWorkerFacade,
                ),
                (
                    worker_module,
                    "ProjectToolPolicySnapshotFacade",
                    ComposedPolicyFacade,
                ),
                (
                    run_module,
                    "ProjectToolPolicySnapshotFacade",
                    ComposedPolicyFacade,
                ),
                (
                    worker_module,
                    "ProjectOperationPrepareFacade",
                    StrictOperationPrepareFacade,
                ),
                (
                    run_module,
                    "ProjectOperationPrepareFacade",
                    StrictOperationPrepareFacade,
                ),
                (
                    worker_module,
                    "ProjectOperationExecutionFacade",
                    StrictOperationExecutionFacade,
                ),
                (
                    run_module,
                    "ProjectOperationExecutionFacade",
                    StrictOperationExecutionFacade,
                ),
                (
                    worker_module,
                    "CanonicalProjectOperationCheckpointCoordinator",
                    StrictCheckpointCoordinator,
                ),
                (
                    run_module,
                    "CanonicalProjectOperationCheckpointCoordinator",
                    StrictCheckpointCoordinator,
                ),
                (
                    worker_module,
                    "CanonicalProjectOperationExecutionCoordinator",
                    StrictApprovedCoordinator,
                ),
                (
                    run_module,
                    "CanonicalProjectOperationExecutionCoordinator",
                    StrictApprovedCoordinator,
                ),
                (
                    worker_module,
                    "CanonicalApprovedOperationTurn",
                    StrictApprovedTurn,
                ),
                (
                    run_module,
                    "CanonicalApprovedOperationTurn",
                    StrictApprovedTurn,
                ),
                (
                    worker_module,
                    "GatewayProjectAgentFactory",
                    StrictAgentFactory,
                ),
                (
                    run_module,
                    "GatewayProjectAgentFactory",
                    StrictAgentFactory,
                ),
                (
                    run_agent_module,
                    "OpenAI",
                    HermeticProjectOpenAI,
                ),
                (
                    run_module,
                    "OpenAI",
                    HermeticProjectOpenAI,
                ),
                (
                    worker_module,
                    "CanonicalProjectRuntimeWorker",
                    GatewayComposedWorker,
                ),
                (
                    run_module,
                    "CanonicalProjectRuntimeWorker",
                    GatewayComposedWorker,
                ),
                (
                    run_module,
                    "ProjectRuntimeDispatcher",
                    build_dispatcher,
                ),
                (
                    dispatcher_module,
                    "ProjectRuntimeDispatcher",
                    build_dispatcher,
                ),
            ):
                patches.enter_context(
                    patch.object(
                        module,
                        name,
                        replacement,
                        create=True,
                    )
                )
            patches.enter_context(
                patch.object(
                    run_module.concurrent.futures,
                    "ThreadPoolExecutor",
                    RecordingExecutor,
                )
            )
            patches.enter_context(
                patch.object(
                    state_module,
                    "SessionDB",
                    TracedSessionDatabase,
                )
            )
            patches.enter_context(
                patch.object(
                    tool_registry_module,
                    "registry",
                    tool_registry_sentinel,
                )
            )
            patches.enter_context(
                patch.object(
                    hooks_module,
                    "HookRegistry",
                    InertHookRegistry,
                )
            )
            patches.enter_context(
                patch.object(
                    pairing_module,
                    "PairingStore",
                    InertPairingStore,
                )
            )
            for module, name, replacement in (
                (
                    tirith_security_module,
                    "ensure_installed",
                    inert_service("ensure_installed"),
                ),
                (
                    plugins_module,
                    "discover_plugins",
                    inert_service("discover_plugins"),
                ),
                (
                    relay_module,
                    "self_provision_relay",
                    inert_service("self_provision_relay"),
                ),
                (
                    relay_module,
                    "register_relay_adapter",
                    inert_service("register_relay_adapter", False),
                ),
                (
                    relay_module,
                    "send_relay_policy",
                    inert_service("send_relay_policy"),
                ),
                (
                    shell_hooks_module,
                    "register_from_config",
                    inert_service("register_from_config"),
                ),
                (
                    status_module,
                    "write_runtime_status",
                    inert_service("write_runtime_status"),
                ),
                (
                    status_module,
                    "remove_pid_file",
                    inert_service("remove_pid_file"),
                ),
                (
                    status_module,
                    "release_gateway_runtime_lock",
                    inert_service("release_gateway_runtime_lock"),
                ),
                (
                    channel_directory_module,
                    "build_channel_directory",
                    inert_async_service(
                        "build_channel_directory",
                        {"platforms": {}},
                    ),
                ),
                (
                    shutdown_forensics_module,
                    "check_systemd_timing_alignment",
                    inert_service(
                        "check_systemd_timing_alignment",
                        None,
                    ),
                ),
                (
                    security_advisories_module,
                    "detect_compromised",
                    inert_service("detect_compromised", []),
                ),
                (
                    security_advisories_module,
                    "gateway_log_message",
                    inert_service("gateway_log_message", None),
                ),
                (
                    auxiliary_client_module,
                    "shutdown_cached_clients",
                    inert_service("shutdown_cached_clients"),
                ),
                (
                    cron_scheduler_module,
                    "mark_running_jobs_interrupted",
                    inert_service(
                        "mark_running_jobs_interrupted",
                        [],
                    ),
                ),
                (
                    async_delegation_module,
                    "interrupt_all",
                    inert_service("interrupt_all", 0),
                ),
                (
                    terminal_tool_module,
                    "cleanup_all_environments",
                    inert_service("cleanup_all_environments"),
                ),
                (
                    browser_tool_module,
                    "cleanup_all_browsers",
                    inert_service("cleanup_all_browsers"),
                ),
                (
                    hermes_config_module,
                    "load_config",
                    guarded_agent_config_service(
                        "load_config",
                        seeded_agent_source_config,
                    ),
                ),
                (
                    hermes_config_module,
                    "load_config_readonly",
                    guarded_agent_config_service(
                        "load_config_readonly",
                        seeded_agent_source_config,
                    ),
                ),
                (
                    hermes_config_module,
                    "read_raw_config",
                    guarded_agent_config_service(
                        "read_raw_config",
                        seeded_agent_source_config,
                    ),
                ),
                (
                    hermes_config_module,
                    "get_compatible_custom_providers",
                    guarded_agent_config_service(
                        "get_compatible_custom_providers",
                        [],
                    ),
                ),
                (
                    hermes_config_module,
                    "get_custom_provider_context_length",
                    guarded_agent_config_service(
                        "get_custom_provider_context_length",
                        None,
                    ),
                ),
                (
                    runtime_provider_module,
                    "load_config",
                    guarded_agent_config_service(
                        "runtime_provider.load_config",
                        seeded_agent_source_config,
                    ),
                ),
                (
                    runtime_provider_module,
                    "get_compatible_custom_providers",
                    guarded_agent_config_service(
                        "runtime_provider.custom_providers",
                        [],
                    ),
                ),
            ):
                patches.enter_context(
                    patch.object(module, name, replacement)
                )
            patches.enter_context(
                patch.object(
                    process_registry,
                    "kill_all",
                    inert_service("process_registry.kill_all", 0),
                )
            )
            patches.enter_context(
                patch.object(
                    run_module,
                    "_hermes_home",
                    root / "hermes-home",
                )
            )
            patches.enter_context(
                patch.object(
                    run_module,
                    "loop_heartbeat_forever",
                    inert_loop_heartbeat,
                )
            )
            patches.enter_context(
                patch.object(
                    run_module,
                    "cfg_get",
                    guarded_cfg_get,
                )
            )
            for fallback_name in (
                "_resolve_turn_agent_config",
                "_read_user_config",
                "_load_provider_routing",
                "_load_fallback_model",
                "_refresh_fallback_model",
                "_load_reasoning_config",
                "_resolve_session_reasoning_config",
                "_extract_cache_busting_config",
            ):
                raw_fallback = inspect.getattr_static(
                    gateway_runner_type,
                    fallback_name,
                )
                if fallback_name == "_read_user_config":
                    guarded_fallback = guarded_agent_config_service(
                        fallback_name,
                        seeded_agent_source_config,
                    )
                elif isinstance(raw_fallback, staticmethod):
                    guarded_fallback = staticmethod(
                        guarded_live_agent_fallback(
                            fallback_name,
                            raw_fallback.__func__,
                        )
                    )
                elif isinstance(raw_fallback, classmethod):
                    guarded_fallback = classmethod(
                        guarded_live_agent_fallback(
                            fallback_name,
                            raw_fallback.__func__,
                        )
                    )
                else:
                    guarded_fallback = guarded_live_agent_fallback(
                        fallback_name,
                        raw_fallback,
                    )
                patches.enter_context(
                    patch.object(
                        gateway_runner_type,
                        fallback_name,
                        guarded_fallback,
                    )
                )
            for fallback_name in (
                "_load_gateway_config",
                "_load_gateway_runtime_config",
                "_resolve_gateway_model",
                "_resolve_runtime_agent_kwargs_for_provider",
                "_credential_pool_for_provider",
                "_try_resolve_fallback_provider",
                "_checkpoint_agent_kwargs",
            ):
                fallback = getattr(run_module, fallback_name)
                seeded_result = {
                    "_load_gateway_config": seeded_agent_source_config,
                    "_load_gateway_runtime_config": (
                        seeded_agent_source_config
                    ),
                    "_resolve_gateway_model": seeded_model,
                    "_resolve_runtime_agent_kwargs_for_provider": (
                        seeded_runtime_settings
                    ),
                }.get(fallback_name)
                replacement = (
                    guarded_agent_config_service(
                        fallback_name,
                        seeded_result,
                    )
                    if seeded_result is not None
                    else guarded_live_agent_fallback(
                        fallback_name,
                        fallback,
                    )
                )
                patches.enter_context(
                    patch.object(
                        run_module,
                        fallback_name,
                        replacement,
                    )
                )
            for fallback_name in (
                "resolve_requested_provider",
                "resolve_runtime_provider",
                "has_named_custom_provider",
                "find_custom_provider_identity",
                "find_custom_provider_identity_by_model",
            ):
                fallback = getattr(
                    runtime_provider_module,
                    fallback_name,
                )
                patches.enter_context(
                    patch.object(
                        runtime_provider_module,
                        fallback_name,
                        guarded_live_agent_fallback(
                            f"runtime_provider.{fallback_name}",
                            fallback,
                        ),
                    )
                )
            for dependency_name in (
                "get_tool_definitions",
                "check_toolset_requirements",
            ):
                dependency = getattr(
                    run_agent_module,
                    dependency_name,
                )
                patches.enter_context(
                    patch.object(
                        run_agent_module,
                        dependency_name,
                        (
                            guarded_seeded_tool_definitions
                            if dependency_name == "get_tool_definitions"
                            else guarded_live_agent_fallback(
                                f"run_agent.{dependency_name}",
                                dependency,
                            )
                        ),
                    )
                )
            patches.enter_context(
                patch.object(
                    agent_init_module,
                    "fetch_model_metadata",
                    guarded_live_agent_fallback(
                        "agent_init.fetch_model_metadata",
                        agent_init_module.fetch_model_metadata,
                    ),
                )
            )
            patches.enter_context(
                patch.object(
                    model_metadata_module.requests,
                    "get",
                    guarded_live_agent_fallback(
                        "model_metadata.requests.get",
                        model_metadata_module.requests.get,
                    ),
                )
            )
            patches.enter_context(
                patch.object(
                    agent_init_module.threading,
                    "Thread",
                    guarded_project_thread,
                )
            )
            patches.enter_context(
                patch.object(
                    run_module.asyncio,
                    "sleep",
                    event_gated_supervisor_wait,
                )
            )
            patches.enter_context(
                patch.object(
                    process_registry,
                    "recover_from_checkpoint",
                    return_value=0,
                )
            )
            patches.enter_context(
                patch.object(
                    process_registry,
                    "pending_watchers",
                    [],
                )
            )
            for watcher_name in generic_supervised_watcher_names:
                watcher = (
                    gated_generic_watcher
                    if watcher_name == "_session_expiry_watcher"
                    else inert_supervised_watcher(watcher_name)
                )
                patches.enter_context(
                    patch.object(
                        gateway_runner_type,
                        watcher_name,
                        watcher,
                    )
                )
            patches.enter_context(
                patch.object(
                    gateway_runner_type,
                    "_platform_reconnect_watcher",
                    inert_platform_reconnect,
                )
            )
            patches.enter_context(
                patch.object(
                    gateway_runner_type,
                    "_schedule_resume_pending_sessions",
                    inert_resume_schedule,
                )
            )
            patches.enter_context(
                patch.object(
                    gateway_runner_type,
                    "_schedule_update_notification_watch",
                    forbidden_update_schedule,
                )
            )
            patches.enter_context(
                patch.object(
                    gateway_runner_type,
                    "_send_update_notification",
                    inert_update_notification,
                )
            )
            for lifecycle_name in (
                "_send_restart_notification",
                "_send_home_channel_startup_notifications",
                "_redeliver_pending_obligations",
                "_finish_startup_restore",
                "_stop_systemd_watchdog",
                "_cancel_secondary_profile_reconnect_tasks",
            ):
                patches.enter_context(
                    patch.object(
                        gateway_runner_type,
                        lifecycle_name,
                        inert_lifecycle(lifecycle_name),
                    )
                )
            for lifecycle_name, result in (
                ("_start_loop_liveness_guards", None),
                ("_stop_loop_liveness_guards", None),
                ("_scale_to_zero_should_arm", False),
                ("_log_scale_to_zero_not_armed_reason", None),
            ):
                patches.enter_context(
                    patch.object(
                        gateway_runner_type,
                        lifecycle_name,
                        inert_sync_lifecycle(
                            lifecycle_name,
                            result,
                        ),
                    )
                )

            gateway = gateway_runner_type(config_sentinel)
            assert await gateway.start() is True
            await asyncio.wait_for(
                generic_watcher_started.wait(),
                timeout=5,
            )
            await asyncio.wait_for(first_close_entered.wait(), timeout=5)
            first_graph = composed_dispatchers[0]
            assert composition_trace[:3] == [
                (
                    "construct",
                    "first",
                    first_graph.instance_id,
                ),
                ("run", "first", first_graph.instance_id),
                ("close", "first", first_graph.instance_id),
            ]
            assert len(composed_workers) == 1
            first_close_release.set()
            await asyncio.wait_for(
                supervisor_backoff_entered.wait(),
                timeout=5,
            )
            assert composition_trace[3:5] == [
                ("worker_close", "first"),
                ("release", "first", first_graph.instance_id),
            ]
            assert len(composed_dispatchers) == 1
            supervisor_backoff_release.set()
            await asyncio.wait_for(successor_started.wait(), timeout=5)
            assert len(composed_dispatchers) == 2
            second_graph = composed_dispatchers[1]
            assert second_graph.instance_id != first_graph.instance_id
            assert [
                (label, instance_id)
                for label, _, instance_id
                in dispatcher_uuid_factory_calls
            ] == [
                ("first", first_graph.instance_id),
                ("second", second_graph.instance_id),
            ]
            assert composition_trace[5:7] == [
                (
                    "construct",
                    "second",
                    second_graph.instance_id,
                ),
                ("run", "second", second_graph.instance_id),
            ]

            for first_dependency, second_dependency in (
                (first_graph.runtime, second_graph.runtime),
                (first_graph.operations, second_graph.operations),
                (first_graph.settlement, second_graph.settlement),
                (first_graph.worker, second_graph.worker),
                (
                    first_graph.batch_worker,
                    second_graph.batch_worker,
                ),
                (
                    first_graph.operation_prepare,
                    second_graph.operation_prepare,
                ),
                (
                    first_graph.checkpoint_coordinator,
                    second_graph.checkpoint_coordinator,
                ),
            ):
                assert first_dependency is not second_dependency
            agent_dependency_names = (
                "c14_snapshot_resolver",
                "c14_agent_builder",
                "c14_off_loop_runner",
                "c14_turn_context_binder",
                "c14_tool_authorizer",
                "c14_checkpoint_coordinator",
            )
            for graph in composed_dispatchers:
                factory = graph.agent_factory
                assert factory.c14_resolver_calls == []
                assert factory.c14_builder_calls == []
                dependencies = tuple(
                    getattr(factory, name)
                    for name in agent_dependency_names
                )
                assert len({id(value) for value in dependencies}) == 6
                assert all(
                    value is not config_sentinel
                    for value in dependencies
                )
                assert type(factory.tool_authorizer) is (
                    ComposedPolicyFacade
                )
                assert factory.tool_authorizer in composed_policy_facades
                assert factory.checkpoint_coordinator is (
                    graph.checkpoint_coordinator
                )
                assert graph.batch_worker in batch_worker_facades
                assert graph.operation_prepare in (
                    operation_prepare_facades
                )
            assert seeded_tool_definition_calls
            assert all(
                toolsets == seeded_enabled_toolsets
                for toolsets, _, _, _ in seeded_tool_definition_calls
            )
            assert {
                "_read_user_config",
                "_load_gateway_config",
                "_load_gateway_runtime_config",
                "load_config",
                "load_config_readonly",
                "read_raw_config",
                "runtime_provider.load_config",
            }.intersection(service_noop_calls)
            for dependency_name in agent_dependency_names:
                first_dependency = getattr(
                    first_graph.agent_factory,
                    dependency_name,
                )
                second_dependency = getattr(
                    second_graph.agent_factory,
                    dependency_name,
                )
                assert first_dependency is not second_dependency
                assert getattr(
                    first_dependency,
                    "__self__",
                    first_dependency,
                ) is not getattr(
                    second_dependency,
                    "__self__",
                    second_dependency,
                )
            assert all(
                graph.worker.config is config_sentinel
                for graph in composed_dispatchers
            )
            assert all(
                graph.worker.lease_seconds == 90
                and graph.worker.heartbeat_seconds == 30
                and "approval_lifetime_seconds"
                not in graph.worker.keyword_names
                for graph in composed_dispatchers
            )

            owned_executors = [
                executor
                for executor in created_executors
                if str(
                    getattr(executor, "_thread_name_prefix", "")
                ).startswith("hermes-")
            ]
            assert len(owned_executors) == 3
            assert len(composed_io_runners) == 2
            io_runner_owners = [
                await io_runner(
                    lambda: getattr(executor_owner, "value", None)
                )
                for io_runner in composed_io_runners
            ]
            assert io_runner_owners[0] is io_runner_owners[1]
            project_io_executor = io_runner_owners[0]
            assert project_io_executor in owned_executors
            agent_executor_owners = [
                await factory.off_loop_runner(
                    lambda: getattr(executor_owner, "value", None)
                )
                for factory in agent_factories
            ]
            assert agent_executor_owners[0] is (
                agent_executor_owners[1]
            )
            agent_executor = agent_executor_owners[0]
            assert agent_executor in owned_executors
            assert agent_executor is not project_io_executor
            ttl_execution = TurnExecutionInput(
                TurnAttemptIdentity(
                    "c14-project",
                    "turn-composer-ttl",
                    1,
                    second_graph.instance_id,
                    "attempt-composer-ttl",
                    1,
                    1,
                    "c14-session",
                    190,
                ),
                {"message": "critical publish"},
                TurnOrigin(
                    "desktop-binding",
                    "desktop",
                    "desktop-window",
                    "owner-1",
                ),
                7,
            )
            probe_context = session_module.SessionContext(
                source=session_module.SessionSource(
                    platform=Platform.LOCAL,
                    chat_id="project:c14-project",
                ),
                connected_platforms=[],
                home_channels={},
                session_key="project:c14-project",
                session_id="c14-session",
            )
            probe_history = session_module.ProjectHistorySnapshot(
                "c14-session",
                (),
                0,
            )
            gateway_owner_thread = threading.get_ident()

            def assert_deeply_immutable(
                value,
                path=("snapshot",),
                *,
                approved_dataclass_types,
                approved_enum_types,
            ):
                from dataclasses import fields, is_dataclass
                from enum import Enum
                from pathlib import (
                    Path,
                    PosixPath,
                    PurePath,
                    PurePosixPath,
                    PureWindowsPath,
                    WindowsPath,
                )
                from types import MappingProxyType

                value_type = type(value)
                if value_type in {
                    type(None),
                    bool,
                    int,
                    float,
                    str,
                    bytes,
                }:
                    return ((path, value),)
                if value_type in {
                    Path,
                    PosixPath,
                    PurePath,
                    PurePosixPath,
                    PureWindowsPath,
                    WindowsPath,
                }:
                    return ((path, value),)
                if value_type in approved_enum_types:
                    assert isinstance(value, Enum)
                    return (
                        (path, value),
                        *assert_deeply_immutable(
                            value.value,
                            (*path, "value"),
                            approved_dataclass_types=(
                                approved_dataclass_types
                            ),
                            approved_enum_types=approved_enum_types,
                        ),
                    )
                if isinstance(value, Enum):
                    raise AssertionError(
                        "project snapshot contains an unapproved Enum "
                        f"at {'.'.join(path)}: {value_type.__name__}"
                    )
                if value_type is MappingProxyType:
                    if value:
                        key = next(iter(value))
                        with pytest.raises(
                            (AttributeError, TypeError)
                        ):
                            value[key] = value[key]
                    entries = [(path, value)]
                    for key, nested_value in value.items():
                        assert type(key) is str
                        entries.extend(
                            assert_deeply_immutable(
                                nested_value,
                                (*path, key),
                                approved_dataclass_types=(
                                    approved_dataclass_types
                                ),
                                approved_enum_types=approved_enum_types,
                            )
                        )
                    return tuple(entries)
                if value_type in {tuple, frozenset}:
                    entries = [(path, value)]
                    for nested_value in value:
                        entries.extend(
                            assert_deeply_immutable(
                                nested_value,
                                (*path, "item"),
                                approved_dataclass_types=(
                                    approved_dataclass_types
                                ),
                                approved_enum_types=approved_enum_types,
                            )
                        )
                    return tuple(entries)
                if value_type in approved_dataclass_types:
                    assert is_dataclass(value)
                    assert not isinstance(value, type)
                    field_definitions = fields(value)
                    entries = [(path, value)]
                    for field in field_definitions:
                        nested_value = getattr(value, field.name)
                        with pytest.raises((AttributeError, TypeError)):
                            setattr(value, field.name, nested_value)
                        entries.extend(
                            assert_deeply_immutable(
                                nested_value,
                                (*path, field.name),
                                approved_dataclass_types=(
                                    approved_dataclass_types
                                ),
                                approved_enum_types=approved_enum_types,
                            )
                        )
                    return tuple(entries)
                if is_dataclass(value):
                    raise AssertionError(
                        "project snapshot contains an unapproved DTO "
                        f"at {'.'.join(path)}: {value_type.__name__}"
                    )
                raise AssertionError(
                    "project snapshot contains an unapproved mutable "
                    f"or opaque value at {'.'.join(path)}: "
                    f"{value_type.__name__}"
                )

            def assert_closed_project_snapshot(snapshot):
                from dataclasses import fields, is_dataclass
                from enum import Enum
                from pathlib import PurePath
                from types import MappingProxyType

                assert is_dataclass(snapshot)
                approved_dataclass_types = frozenset(
                    {
                        type(snapshot),
                        worker_module.ProjectAgentRevisions,
                    }
                )
                approved_enum_types = frozenset()
                immutable_snapshot_entries = (
                    assert_deeply_immutable(
                        snapshot,
                        approved_dataclass_types=(
                            approved_dataclass_types
                        ),
                        approved_enum_types=approved_enum_types,
                    )
                )

                def semantic_fingerprint(value):
                    value_type = type(value)
                    if value_type in {
                        type(None),
                        bool,
                        int,
                        float,
                        str,
                        bytes,
                    }:
                        return ("atom", value_type.__name__, value)
                    if isinstance(value, PurePath):
                        return ("path", str(value))
                    if value_type in approved_enum_types:
                        assert isinstance(value, Enum)
                        return (
                            "enum",
                            value_type.__qualname__,
                            semantic_fingerprint(value.value),
                        )
                    if isinstance(value, Enum):
                        raise AssertionError(
                            "unsupported semantic Enum value: "
                            f"{value_type.__name__}"
                        )
                    if isinstance(value, (dict, MappingProxyType)):
                        return (
                            "record",
                            tuple(
                                (
                                    key,
                                    semantic_fingerprint(nested_value),
                                )
                                for key, nested_value in sorted(
                                    value.items()
                                )
                            ),
                        )
                    if isinstance(value, (list, tuple)):
                        return (
                            "sequence",
                            tuple(
                                semantic_fingerprint(nested_value)
                                for nested_value in value
                            ),
                        )
                    if isinstance(value, frozenset):
                        return (
                            "set",
                            tuple(
                                sorted(
                                    (
                                        semantic_fingerprint(nested_value)
                                        for nested_value in value
                                    ),
                                    key=repr,
                                )
                            ),
                        )
                    if value_type in approved_dataclass_types:
                        assert is_dataclass(value)
                        return (
                            "record",
                            tuple(
                                (
                                    field.name,
                                    semantic_fingerprint(
                                        getattr(value, field.name)
                                    ),
                                )
                                for field in sorted(
                                    fields(value),
                                    key=lambda field: field.name,
                                )
                            ),
                        )
                    if is_dataclass(value):
                        raise AssertionError(
                            "unsupported semantic DTO value: "
                            f"{value_type.__name__}"
                        )
                    raise AssertionError(
                        "unsupported semantic snapshot value: "
                        f"{value_type.__name__}"
                    )

                snapshot_fingerprints = frozenset(
                    semantic_fingerprint(value)
                    for _, value in immutable_snapshot_entries
                )
                seeded_semantics = (
                    (
                        "prompt-cache settings",
                        seeded_prompt_cache_settings,
                    ),
                    ("memory settings", seeded_memory_settings),
                    ("skill settings", seeded_skill_settings),
                    (
                        "compression settings",
                        seeded_compression_settings,
                    ),
                    ("runtime settings", seeded_runtime_settings),
                    ("tool descriptors", seeded_tool_descriptors),
                    ("tool schemas", seeded_tool_schemas),
                    ("enabled toolsets", seeded_enabled_toolsets),
                )
                assert all(
                    semantic_fingerprint(expected_value)
                    in snapshot_fingerprints
                    for _, expected_value in seeded_semantics
                ), (
                    "the one consumed snapshot must contain every exact "
                    "independently seeded frozen semantic input: "
                    + ", ".join(
                        label for label, _ in seeded_semantics
                    )
                )

                assert snapshot.registry_generation == (
                    frozen_registry_generation
                )
                assert snapshot.declared_registry_generation == (
                    snapshot.registry_generation
                )
                assert snapshot.declared_base_signature == (
                    snapshot.base_signature
                )
                assert snapshot.declared_tool_revision == (
                    snapshot.tool_revision
                )
                assert snapshot.declared_model_revision == (
                    snapshot.model_revision
                )
                assert snapshot.revisions == (
                    worker_module.ProjectAgentRevisions(
                        snapshot.base_signature,
                        snapshot.tool_revision,
                        snapshot.model_revision,
                    )
                )
                assert all(
                    type(value) is str and bool(value)
                    for value in (
                        snapshot.base_signature,
                        snapshot.tool_revision,
                        snapshot.model_revision,
                        snapshot.resolved_agent,
                        snapshot.resolved_provider,
                    )
                )
                assert snapshot.runtime_kind == "hermes"
                constructor_values = dict(
                    snapshot.constructor_kwargs
                )
                assert constructor_values["model"] == seeded_model
                assert (
                    constructor_values["provider"]
                    == seeded_provider
                )
                assert constructor_values["base_url"] == (
                    seeded_runtime_settings["base_url"]
                )
                assert constructor_values["api_mode"] == (
                    seeded_runtime_settings["api_mode"]
                )
                assert constructor_values["max_tokens"] == (
                    seeded_runtime_settings["max_tokens"]
                )
                assert tuple(
                    constructor_values["enabled_toolsets"]
                ) == seeded_enabled_toolsets
                normalized_tool_revision = (
                    snapshot.tool_revision.lower()
                )
                assert str(snapshot.registry_generation) in (
                    normalized_tool_revision
                )
                assert all(
                    toolset.lower() in normalized_tool_revision
                    for toolset in seeded_enabled_toolsets
                ), (
                    "tool revision must contain every enabled toolset "
                    "and the current registry generation"
                )

                assert snapshot.resolved_provider == seeded_provider
                normalized_model_revision = (
                    snapshot.model_revision.lower()
                )
                assert all(
                    value.lower() in normalized_model_revision
                    for value in (
                        seeded_model,
                        seeded_provider,
                        seeded_runtime_settings["base_url"],
                        seeded_runtime_settings["api_mode"],
                        snapshot.runtime_kind,
                    )
                ), (
                    "model revision must contain the resolved model, "
                    "provider, and runtime route signature"
                )
                return constructor_values, tuple(
                    field.name for field in fields(snapshot)
                )

            from dataclasses import dataclass
            from enum import Enum
            from pathlib import PurePath

            class OpaqueTuple(tuple):
                pass

            class OpaqueFrozenset(frozenset):
                pass

            class OpaquePath(type(PurePath("c14"))):
                pass

            class OpaqueEnum(Enum):
                VALUE = "value"

            @dataclass(frozen=True)
            class OpaqueFrozenDataclass:
                value: str

            opaque_tuple = OpaqueTuple(("value",))
            opaque_frozenset = OpaqueFrozenset({"value"})
            opaque_path = OpaquePath("c14")
            opaque_dataclass = OpaqueFrozenDataclass("value")
            for opaque_value in (
                opaque_tuple,
                opaque_frozenset,
                opaque_path,
                OpaqueEnum.VALUE,
                opaque_dataclass,
            ):
                with pytest.raises(AssertionError):
                    assert_deeply_immutable(
                        opaque_value,
                        approved_dataclass_types=frozenset(
                            {worker_module.ProjectAgentRevisions}
                        ),
                        approved_enum_types=frozenset(),
                    )

            agent_dependency_probe_active[0] = True
            try:
                for graph in composed_dispatchers:
                    factory = graph.agent_factory
                    resolver_before = len(
                        factory.c14_resolver_calls
                    )
                    builder_before = len(
                        factory.c14_builder_calls
                    )
                    assert resolver_before == 0
                    assert builder_before == 0
                    build = await factory.resolve_project_agent(
                        context=probe_context,
                        contract_revision=7,
                    )
                    assert len(factory.c14_resolver_calls) == 1
                    assert factory.c14_builder_calls == []
                    (
                        resolved_context,
                        resolved_revision,
                        resolved_snapshot,
                        resolver_thread,
                        resolver_executor,
                    ) = factory.c14_resolver_calls[-1]
                    assert resolved_context is probe_context
                    assert resolved_revision == 7
                    snapshot = resolved_snapshot
                    agent_snapshot_probes.append(snapshot)
                    (
                        constructor_values,
                        snapshot_field_names,
                    ) = assert_closed_project_snapshot(snapshot)
                    assert snapshot_field_names
                    assert build.revisions == snapshot.revisions
                    built_before = len(built_agent_probes)
                    project_agent = await build.create_project_agent(
                        history=probe_history,
                    )
                    assert len(factory.c14_resolver_calls) == 1
                    assert len(factory.c14_builder_calls) == 1
                    (
                        builder_snapshot,
                        builder_options,
                        builder_product,
                        builder_thread,
                        builder_executor,
                    ) = factory.c14_builder_calls[-1]
                    assert builder_snapshot is resolved_snapshot
                    assert builder_product is built_agent_probes[-1]
                    assert resolver_executor is agent_executor
                    assert builder_executor is agent_executor
                    assert resolver_thread != gateway_owner_thread
                    assert builder_thread != gateway_owner_thread
                    project_gate = builder_options[
                        "project_execution_gate"
                    ]
                    assert builder_options == {
                        "project_execution_gate": project_gate,
                        "session_db": None,
                        "save_trajectories": False,
                        "quiet_mode": True,
                        "skip_memory": True,
                        "streaming_callback": None,
                        "delivery_callback": None,
                        "approval_notifier": None,
                        "provider_metadata_prewarm": False,
                        "external_memory_sync": False,
                        "memory_review": False,
                        "skill_review": False,
                        "plugin_lifecycle": False,
                    }
                    assert len(built_agent_probes) == built_before + 1
                    built_agent = built_agent_probes[-1]
                    assert type(built_agent) is (
                        run_agent_module.AIAgent
                    )
                    assert (
                        built_agent.run_conversation.__func__
                        is run_agent_module.AIAgent.run_conversation
                    )
                    for name in (
                        "model",
                        "base_url",
                        "provider",
                        "requested_provider",
                        "api_mode",
                        "max_tokens",
                        "enabled_toolsets",
                    ):
                        assert getattr(built_agent, name) == (
                            constructor_values[name]
                        )
                    assert built_agent.acp_command == (
                        constructor_values["command"]
                    )
                    assert built_agent.acp_args == list(
                        constructor_values["args"]
                    )
                    assert built_agent._credential_pool == (
                        constructor_values["credential_pool"]
                    )
                    assert built_agent._tool_snapshot_generation == (
                        constructor_values[
                            "project_registry_generation"
                        ]
                    )
                    assert tuple(built_agent.tools) == (
                        seeded_tool_schemas
                    )
                    assert all(
                        getattr(built_agent, name) is value
                        for name, value in builder_options.items()
                        if hasattr(built_agent, name)
                    )
                    assert (
                        built_agent.project_execution_gate
                        is project_gate
                    )
                    assert callable(
                        getattr(project_gate, "request_cancel", None)
                    )
                    assert built_agent._persist_disabled is True
                    assert built_agent._session_db is None
                    assert (
                        built_agent._session_json_enabled is False
                    )
                    assert (
                        built_agent._end_session_on_close is False
                    )
                    assert built_agent.compression_enabled is False
                    assert built_agent._memory_nudge_interval == 0
                    assert built_agent._skill_nudge_interval == 0
                    assert (
                        built_agent.background_review_callback
                        is None
                    )
                    provider_client = built_agent.client
                    assert type(provider_client) is (
                        HermeticProjectOpenAI
                    )
                    assert provider_client is (
                        hermetic_primary_instances[-1]
                    )
                    assert len(hermetic_primary_instances) == len(
                        built_agent_probes
                    )
                    assert provider_client.kind == "primary"
                    assert provider_client.create_count == 0
                    assert provider_client.close_count == 0
                    provider_calls_before = len(
                        hermetic_provider_calls
                    )
                    request_clients_before = len(
                        hermetic_request_instances
                    )
                    provider_poison_before = len(
                        hermetic_provider_poison
                    )
                    raw_observation_before = len(
                        raw_agent_context_observations
                    )
                    authorizer_before = len(
                        agent_authorizer_calls
                    )
                    binder_before = len(
                        factory.c14_binder_calls
                    )
                    provider_script = arm_hermetic_provider_script(
                        (
                            project_status_tool_response(
                                "c14-project-status-"
                                f"{len(hermetic_provider_turns) + 1}"
                            ),
                        )
                    )
                    original_public_run_conversation = (
                        run_agent_module.AIAgent.run_conversation
                    )
                    observed_public_run_receivers = []

                    def observe_public_run_conversation(
                        receiver,
                        *args,
                        **kwargs,
                    ):
                        observed_public_run_receivers.append(receiver)
                        return original_public_run_conversation(
                            receiver,
                            *args,
                            **kwargs,
                        )

                    setattr(
                        run_agent_module.AIAgent,
                        "run_conversation",
                        observe_public_run_conversation,
                    )
                    try:
                        project_turn = project_agent.create_turn(
                            ttl_execution,
                            None,
                        )
                        with pytest.raises(PermissionError):
                            await project_turn.result()
                    finally:
                        try:
                            await project_turn.wait_quiescent()
                        finally:
                            setattr(
                                run_agent_module.AIAgent,
                                "run_conversation",
                                original_public_run_conversation,
                            )
                    provider_record = (
                        finish_hermetic_provider_script(
                            provider_script,
                            provider_client,
                        )
                    )
                    assert observed_public_run_receivers == [built_agent]
                    assert len(provider_record.clients) == 1
                    assert len(provider_record.calls) == 1
                    assert len(hermetic_request_instances) == (
                        request_clients_before + 1
                    )
                    assert len(hermetic_provider_calls) == (
                        provider_calls_before + 1
                    )
                    (
                        call_owner,
                        call_completions,
                        call_kwargs,
                    ) = provider_record.calls[0]
                    assert call_owner is provider_record.clients[0]
                    assert call_owner is not provider_client
                    assert call_completions is (
                        call_owner.chat.completions
                    )
                    assert call_kwargs
                    assert call_owner.close_count == 1
                    assert provider_client.close_count == 0
                    assert all(
                        primary.create_count == 0
                        for primary in hermetic_primary_instances
                    )
                    assert len(hermetic_provider_poison) == (
                        provider_poison_before
                    )
                    assert len(raw_agent_context_observations) == (
                        raw_observation_before + 1
                    )
                    (
                        raw_context,
                        raw_thread,
                        raw_executor,
                    ) = raw_agent_context_observations[-1]
                    assert raw_thread != gateway_owner_thread
                    assert raw_executor is agent_executor
                    raw_context_values = tuple(
                        value for _, value in raw_context.items()
                    )
                    assert any(
                        value is ttl_execution
                        or getattr(value, "execution", None)
                        is ttl_execution
                        or getattr(
                            value,
                            "execution_input",
                            None,
                        )
                        is ttl_execution
                        for value in raw_context_values
                    )
                    assert len(agent_authorizer_calls) == (
                        authorizer_before + 1
                    )
                    (
                        observed_authorizer,
                        authorized_execution,
                        authorized_origin,
                        authorized_invocation,
                        authorized_transcript,
                        authorizer_thread,
                    ) = agent_authorizer_calls[-1]
                    assert observed_authorizer is (
                        factory.tool_authorizer
                    )
                    assert authorized_execution is ttl_execution
                    assert authorized_origin is ttl_execution.origin
                    assert (
                        authorized_invocation.canonical_action
                        == "read.project_status"
                    )
                    assert authorized_invocation.route == "sequential"
                    assert (
                        authorized_invocation.effect_capable
                        is False
                    )
                    assert authorized_transcript == ()
                    assert authorizer_thread == gateway_owner_thread
                    assert len(factory.c14_binder_calls) == (
                        binder_before + 1
                    )
                    (
                        bound_execution,
                        before_context,
                        inside_context,
                        after_context,
                        binder_thread,
                        binder_executor,
                    ) = factory.c14_binder_calls[-1]
                    assert bound_execution is ttl_execution
                    assert binder_thread == raw_thread
                    assert binder_executor is agent_executor
                    missing_context_value = object()
                    context_drift = [
                        (
                            getattr(variable, "name", repr(variable)),
                            before_context.get(
                                variable,
                                missing_context_value,
                            ),
                            after_context.get(
                                variable,
                                missing_context_value,
                            ),
                        )
                        for variable in (
                            set(before_context) | set(after_context)
                        )
                        if after_context.get(
                            variable,
                            missing_context_value,
                        )
                        is not before_context.get(
                            variable,
                            missing_context_value,
                        )
                    ]
                    assert context_drift == []
                    inside_values = tuple(
                        value
                        for _, value in inside_context.items()
                    )
                    assert any(
                        value is ttl_execution
                        or getattr(value, "execution", None)
                        is ttl_execution
                        or getattr(
                            value,
                            "execution_input",
                            None,
                        )
                        is ttl_execution
                        for value in inside_values
                    )
                    assert any(
                        value is ttl_execution.origin
                        or getattr(value, "origin", None)
                        is ttl_execution.origin
                        or getattr(
                            value,
                            "execution_origin",
                            None,
                        )
                        is ttl_execution.origin
                        or getattr(
                            getattr(value, "execution", None),
                            "origin",
                            None,
                        )
                        is ttl_execution.origin
                        for value in inside_values
                    )
                    after_values = tuple(
                        value
                        for _, value in after_context.items()
                    )
                    assert all(
                        value is not ttl_execution
                        and value is not ttl_execution.origin
                        and getattr(value, "execution", None)
                        is not ttl_execution
                        and getattr(
                            value,
                            "execution_input",
                            None,
                        )
                        is not ttl_execution
                        and getattr(value, "origin", None)
                        is not ttl_execution.origin
                        and getattr(
                            value,
                            "execution_origin",
                            None,
                        )
                        is not ttl_execution.origin
                        for value in after_values
                    )
                    await factory.off_loop_runner(
                        exercise_exact_turn_binding,
                        factory.turn_context_binder,
                        ttl_execution,
                    )
                    await factory.release_project_agent(project_agent)
                    assert built_agent.client is None
                    assert provider_client.closed is True
                    assert provider_client.close_count == 1
                    primary_events = hermetic_provider_events_for(
                        provider_client
                    )
                    primary_constructor_events = [
                        event
                        for event in primary_events
                        if event[1] == "constructor"
                    ]
                    primary_create_events = [
                        event
                        for event in primary_events
                        if event[1] == "create"
                    ]
                    primary_close_events = [
                        event
                        for event in primary_events
                        if event[1] == "close"
                    ]
                    assert len(primary_constructor_events) == 1
                    assert primary_create_events == []
                    assert len(primary_close_events) == 1
                    assert (
                        primary_constructor_events[0][0]
                        < primary_close_events[0][0]
                    )
            finally:
                agent_dependency_probe_active[0] = False
            assert agent_fallback_calls == []
            assert all(
                len(graph.agent_factory.c14_resolver_calls) == 1
                and len(graph.agent_factory.c14_builder_calls) == 1
                for graph in composed_dispatchers
            )
            assert len(agent_snapshot_probes) == 2
            assert agent_snapshot_probes[0] is not (
                agent_snapshot_probes[1]
            )
            assert type(agent_snapshot_probes[0]) is type(
                agent_snapshot_probes[1]
            )
            from dataclasses import fields

            snapshot_field_names = tuple(
                field.name
                for field in fields(agent_snapshot_probes[0])
            )
            assert snapshot_field_names
            assert all(
                getattr(agent_snapshot_probes[0], field_name)
                == getattr(agent_snapshot_probes[1], field_name)
                for field_name in snapshot_field_names
            )
            assert len(built_agent_probes) == 2
            assert len(hermetic_primary_instances) == 2
            assert len(hermetic_request_instances) == 2
            assert len(hermetic_provider_turns) == 2
            assert len(hermetic_provider_calls) == 2
            assert hermetic_provider_poison == []
            assert all(
                primary.close_count == 1
                for primary in hermetic_primary_instances
            )
            assert all(
                request.close_count == 1
                for request in hermetic_request_instances
            )
            assert built_agent_probes[0] is not built_agent_probes[1]
            assert len(bound_turn_probes) == 2
            assert all(
                execution is ttl_execution
                and executor is agent_executor
                and thread_id != gateway_owner_thread
                for (
                    execution,
                    _,
                    _,
                    thread_id,
                    executor,
                ), _built in zip(
                    bound_turn_probes,
                    built_agent_probes,
                    strict=True,
                )
            )
            approved_probe_start = _task7_c5_worker_start(
                "approved_operation",
                "composer-approved-port",
                lease_a,
                worker_id=second_graph.instance_id,
            )
            for graph in composed_dispatchers:
                approved_port = graph.worker.approved_operations
                assert callable(
                    getattr(approved_port, "create_turn", None)
                )
                approved_turn = approved_port.create_turn(
                    ttl_execution,
                    approved_probe_start.operation,
                    base_message_count=0,
                )
                assert type(approved_turn) is StrictApprovedTurn
            assert len(approved_coordinators) == 2
            assert len(approved_turns) == 2
            for graph, coordinator in zip(
                composed_dispatchers,
                approved_coordinators,
                strict=True,
            ):
                assert (
                    coordinator.execution_facade
                    in operation_execution_facades
                )
                assert (
                    coordinator.execution_facade.approval_checkpoints
                    is graph.approval_checkpoints
                )
                assert (
                    coordinator.execution_facade.io_runner
                    is graph.io_runner
                )
                assert (
                    coordinator.capability_registry
                    is graph.capabilities
                )
            capability_executor_owners = [
                await coordinator.effect_runner(
                    lambda: getattr(executor_owner, "value", None)
                )
                for coordinator in approved_coordinators
            ]
            assert capability_executor_owners[0] is (
                capability_executor_owners[1]
            )
            capability_executor = capability_executor_owners[0]
            assert capability_executor in owned_executors
            assert capability_executor is not project_io_executor
            agent_executor_owners = [
                await factory.off_loop_runner(
                    lambda: getattr(executor_owner, "value", None)
                )
                for factory in agent_factories
            ]
            assert agent_executor_owners[0] is agent_executor_owners[1]
            agent_executor = agent_executor_owners[0]
            assert agent_executor in owned_executors
            assert agent_executor not in {
                project_io_executor,
                capability_executor,
            }
            generic_executor_candidates = [
                executor
                for executor in owned_executors
                if executor
                not in {project_io_executor, capability_executor}
            ]
            assert generic_executor_candidates == [agent_executor]
            generic_executor = agent_executor

            async def assert_composed_runner_cancel_joins(
                label,
                runner,
                expected_executor,
            ):
                owner_loop = asyncio.get_running_loop()
                entered = asyncio.Event()
                release = threading.Event()
                call_trace = []

                def blocked_call():
                    assert getattr(
                        executor_owner,
                        "value",
                        None,
                    ) is expected_executor
                    call_trace.append(("entered", label))
                    owner_loop.call_soon_threadsafe(entered.set)
                    assert release.wait(timeout=5)
                    call_trace.append(("returned", label))
                    return label

                task = asyncio.create_task(runner(blocked_call))
                assert await asyncio.wait_for(
                    entered.wait(),
                    timeout=5,
                )
                task.cancel()
                assert task.cancelling() == 1
                assert not task.done()
                assert expected_executor not in executor_shutdown_trace
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert call_trace == [
                    ("entered", label),
                    ("returned", label),
                ]
                assert expected_executor not in executor_shutdown_trace
                runner_join_trace.append(label)

            await assert_composed_runner_cancel_joins(
                "project-io",
                second_graph.io_runner,
                project_io_executor,
            )
            await assert_composed_runner_cancel_joins(
                "capability-effect",
                approved_coordinators[-1].effect_runner,
                capability_executor,
            )
            await assert_composed_runner_cancel_joins(
                "project-agent",
                second_graph.agent_factory.off_loop_runner,
                agent_executor,
            )

            ttl_intent = OperationIntent(
                "operation-composer-ttl",
                "c14-project",
                "turn-composer-ttl",
                "idempotency-composer-ttl",
                "publish",
                1,
                ("c:/work/file.py",),
                ("write",),
                {
                    "path": "c:/work/file.py",
                    "content": "exact",
                },
                "remote-ledger",
                True,
            )
            ttl_effect_scope_json = json.dumps(
                {
                    "targets": ["c:/work/file.py"],
                    "batch_items": ["write"],
                    "payload_effects": dict(ttl_intent.payload),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            ttl_authorization = await composed_policy_facades[
                -1
            ].authorize_project_operation(
                ttl_execution,
                worker_module.ProjectOperationProposal(
                    ttl_intent,
                    None,
                    ttl_effect_scope_json,
                    hashlib.sha256(
                        ttl_effect_scope_json.encode("utf-8")
                    ).hexdigest(),
                    ttl_capability.fingerprint,
                ),
            )
            if (
                type(ttl_authorization) is tuple
                and len(ttl_authorization) == 2
            ):
                ttl_carrier, ttl_approval = ttl_authorization
            else:
                ttl_carrier = getattr(
                    ttl_authorization,
                    "policy_authority",
                    getattr(ttl_authorization, "carrier", None),
                )
                ttl_approval = getattr(
                    ttl_authorization,
                    "approval",
                    None,
                )
            assert ttl_carrier.decision.decision is (
                Decision.REQUIRE_APPROVAL
            )
            assert ttl_approval == OperationApprovalSpec(
                "623e4567-e89b-42d3-a456-426614174000",
                "publish",
                3_700,
                ttl_carrier.actor,
            )
            assert authority_clock_calls == [100]
            assert (
                ttl_approval.expires_at
                == authority_clock_calls[0] + 3_600
            )
            assert approval_id_calls == [
                "623e4567-e89b-42d3-a456-426614174000"
            ]
            assert c14_config_reads == []

            await gateway.stop()
            await asyncio.wait_for(
                generic_watcher_cancelled.wait(),
                timeout=5,
            )

    assert len(runtime_facades) == 2
    assert len(operation_facades) == 2
    assert len(settlement_facades) == 2
    assert len(terminal_readback_facades) == 2
    assert len(checkpoint_read_facades) == 2
    assert len(batch_worker_facades) == 2
    assert len(operation_prepare_facades) == 2
    assert len(operation_execution_facades) == 2
    assert len(checkpoint_coordinators) == 2
    assert len(approved_coordinators) == 2
    assert len(approved_turns) == 2
    assert len(agent_factories) == 2
    assert len(composed_policy_facades) == 2
    assert runner_join_trace == [
        "project-io",
        "capability-effect",
        "project-agent",
    ]
    assert composition_trace[:10] == [
        ("construct", "first", first_graph.instance_id),
        ("run", "first", first_graph.instance_id),
        ("close", "first", first_graph.instance_id),
        ("worker_close", "first"),
        ("release", "first", first_graph.instance_id),
        ("construct", "second", second_graph.instance_id),
        ("run", "second", second_graph.instance_id),
        ("close", "second", second_graph.instance_id),
        ("worker_close", "second"),
        ("release", "second", second_graph.instance_id),
    ]
    second_release_index = composition_trace.index(
        ("release", "second", second_graph.instance_id)
    )
    generic_cancel_index = composition_trace.index(
        ("generic_cancel", "_session_expiry_watcher")
    )
    session_close_indices = [
        index
        for index, entry in enumerate(composition_trace)
        if isinstance(entry, tuple)
        and entry[0] == "session_db_close"
    ]
    assert len(session_close_indices) == 2
    assert executor_shutdown_trace == [
        capability_executor,
        project_io_executor,
        generic_executor,
    ]
    capability_shutdown_index = composition_trace.index(
        ("executor_shutdown", capability_executor)
    )
    project_io_shutdown_index = composition_trace.index(
        ("executor_shutdown", project_io_executor)
    )
    generic_shutdown_index = composition_trace.index(
        ("executor_shutdown", generic_executor)
    )
    assert (
        second_release_index
        < generic_cancel_index
        < min(session_close_indices)
    )
    assert (
        max(session_close_indices)
        < capability_shutdown_index
        < project_io_shutdown_index
        < generic_shutdown_index
    )
    # A failed predecessor's cleanup must not erase the live successor:
    # public stop closes and fully drains that successor exactly once.
    assert [
        entry
        for entry in composition_trace
        if entry
        in {
            ("close", "second", second_graph.instance_id),
            ("worker_close", "second"),
            ("release", "second", second_graph.instance_id),
        }
    ] == [
        ("close", "second", second_graph.instance_id),
        ("worker_close", "second"),
        (
            "release",
            "second",
            second_graph.instance_id,
        ),
    ]
    assert composition_trace.count(
        ("close", "first", first_graph.instance_id)
    ) == 1
    assert composition_trace.count(
        ("close", "second", second_graph.instance_id)
    ) == 1
    assert sorted(generic_watcher_noop_calls) == sorted(
        [
            "_session_expiry_watcher",
            "_kanban_notifier_watcher",
            "_kanban_dispatcher_watcher",
            "_handoff_watcher",
            "_async_delegation_watcher",
            "_drain_control_watcher",
        ]
    )
    assert sorted(direct_scheduler_noop_calls) == sorted(
        [
            "loop_heartbeat_forever",
            "_schedule_resume_pending_sessions",
            "_platform_reconnect_watcher",
        ]
    )
    assert unexpected_scheduler_calls == []
    assert {
        "_start_loop_liveness_guards",
        "hooks.discover_and_load",
        "hooks.emit",
        "_send_update_notification",
        "_send_restart_notification",
        "_redeliver_pending_obligations",
        "_finish_startup_restore",
        "_log_scale_to_zero_not_armed_reason",
        "_stop_systemd_watchdog",
        "_cancel_secondary_profile_reconnect_tasks",
        "_stop_loop_liveness_guards",
    }.issubset(lifecycle_noop_calls)
    assert {
        "PairingStore",
        "ensure_installed",
        "load_config",
        "check_systemd_timing_alignment",
        "detect_compromised",
        "gateway_log_message",
        "discover_plugins",
        "self_provision_relay",
        "register_relay_adapter",
        "register_from_config",
        "build_channel_directory",
        "write_runtime_status",
        "process_registry.kill_all",
        "mark_running_jobs_interrupted",
        "interrupt_all",
        "cleanup_all_environments",
        "cleanup_all_browsers",
        "shutdown_cached_clients",
        "remove_pid_file",
        "release_gateway_runtime_lock",
    }.issubset(service_noop_calls)
    assert all(database.c14_closed for database in session_databases)
    assert all(facade.calls == [] for facade in settlement_facades)
    assert c14_config_reads == []


def test_fresh_process_core_lease_race_takeover_and_stale_writers(
    tmp_path,
):
    db_path = tmp_path / "projects.db"
    projects_db.connect(db_path).close()

    with ProbeSet(_DISPATCHER_PROBE) as probes:
        contenders = [
            probes.spawn(
                _prepare(
                    "acquire-a",
                    "acquire",
                    db_path,
                    instance_id=_INSTANCE_A,
                    now=100,
                    lease_seconds=30,
                )
            ),
            probes.spawn(
                _prepare(
                    "acquire-b",
                    "acquire",
                    db_path,
                    instance_id=_INSTANCE_B,
                    now=100,
                    lease_seconds=30,
                )
            ),
        ]
        release_probes(contenders)
        results = [handle.expect("result") for handle in contenders]
        for handle in contenders:
            handle.complete()

        leaders = [
            result
            for result in results
            if result["ok"] is True
            and result["lease"] is not None
        ]
        standbys = [
            result
            for result in results
            if result["ok"] is True
            and result["lease"] is None
        ]
        assert len(leaders) == len(standbys) == 1
        assert leaders[0]["write_count"] == 1
        assert len(leaders[0]["mutations"]) == 1
        assert leaders[0]["mutations"][0].lstrip().upper().startswith(
            "INSERT"
        )
        assert standbys[0]["write_count"] == 0
        assert standbys[0]["mutations"] == []
        old_lease = leaders[0]["lease"]
        assert type(old_lease) is dict
        assert old_lease == {
            "instance_id": (
                _INSTANCE_A
                if results[0]["lease"] is not None
                else _INSTANCE_B
            ),
            "generation": 1,
            "fencing_token": 1,
            "expires_at": 130,
        }
        old_instance = old_lease["instance_id"]
        standby_instance = (
            _INSTANCE_B
            if old_instance == _INSTANCE_A
            else _INSTANCE_A
        )
        assert _stored_lease(db_path) == (
            "core",
            old_instance,
            1,
            1,
            130,
            100,
        )

        before_boundary = _stored_lease(db_path)
        expiry_minus_one = run_probe(
            probes,
            _prepare(
                "expiry-minus-one",
                "acquire",
                db_path,
                instance_id=standby_instance,
                now=129,
                lease_seconds=30,
            ),
        )
        assert expiry_minus_one == {
            "version": 1,
            "probe_id": "expiry-minus-one",
            "event": "result",
            "action": "acquire",
            "ok": True,
            "lease": None,
            "write_count": 0,
            "mutations": [],
        }
        assert _stored_lease(db_path) == before_boundary

        boundary_renew = probes.spawn(
            _prepare(
                "expiry-renew",
                "renew",
                db_path,
                instance_id=old_instance,
                now=130,
                lease_seconds=30,
                lease=old_lease,
            )
        )
        boundary_takeover = probes.spawn(
            _prepare(
                "takeover",
                "acquire",
                db_path,
                instance_id=standby_instance,
                now=130,
                lease_seconds=30,
            )
        )
        release_probes([boundary_renew, boundary_takeover])
        stale_renew = boundary_renew.expect("result")
        takeover = boundary_takeover.expect("result")
        boundary_renew.complete()
        boundary_takeover.complete()

        assert stale_renew == {
            "version": 1,
            "probe_id": "expiry-renew",
            "event": "result",
            "action": "renew",
            "ok": False,
            "error": {"code": "stale_dispatcher_lease"},
            "write_count": 0,
            "mutations": [],
        }
        assert takeover["ok"] is True
        assert takeover["lease"] == {
            "instance_id": standby_instance,
            "generation": 2,
            "fencing_token": 2,
            "expires_at": 160,
        }
        assert takeover["write_count"] == 1
        assert len(takeover["mutations"]) == 1
        assert takeover["mutations"][0].lstrip().upper().startswith(
            "UPDATE"
        )
        after_takeover = _stored_lease(db_path)

        stale_release = run_probe(
            probes,
            _prepare(
                "stale-release",
                "release",
                db_path,
                instance_id=old_instance,
                now=131,
                lease=old_lease,
            ),
        )
        assert stale_release == {
            "version": 1,
            "probe_id": "stale-release",
            "event": "result",
            "action": "release",
            "ok": True,
            "released": False,
            "write_count": 0,
            "mutations": [],
        }
        assert _stored_lease(db_path) == after_takeover


def test_task7_c4_startauthority_dispatcher_delegates_only_fenced_issuance():
    from gateway.project_runtime_dispatcher import (
        ProjectRuntimeDispatcher,
    )
    from hermes_cli.project_runtime import DispatcherLease

    lease = DispatcherLease(
        "11111111-1111-4111-8111-111111111111",
        7,
        11,
        500,
    )
    queued_start = object()
    approved_start = object()

    class FenceOnlyRuntime:
        def __init__(self):
            self.calls = []

        def claim_next_turn_for_dispatcher(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return queued_start

        def __getattr__(self, name):
            raise AssertionError(
                f"dispatcher touched forbidden runtime member {name}"
            )

    class FenceOnlyOperationGuard:
        def __init__(self):
            self.calls = []

        def rehydrate_approved_operation_for_dispatcher(
            self,
            *args,
            **kwargs,
        ):
            self.calls.append((args, kwargs))
            return approved_start

        def __getattr__(self, name):
            raise AssertionError(
                "dispatcher touched forbidden operation-guard member "
                f"{name}"
            )

    class NoSchedulingWorker:
        async def run_start(self, start):
            raise AssertionError(
                "C4 issuance methods scheduled a worker"
            )

    runtime = FenceOnlyRuntime()
    guard = FenceOnlyOperationGuard()
    dispatcher = ProjectRuntimeDispatcher(
        runtime,
        guard,
        NoSchedulingWorker(),
        worker_cap=1,
    )

    assert dispatcher.issue_queued_start(
        "project-1",
        "worker-1",
        lease_seconds=30,
        dispatcher_lease=lease,
    ) is queued_start
    assert dispatcher.issue_approved_operation_start(
        "project-1",
        "operation-1",
        worker_id="worker-2",
        lease_seconds=45,
        dispatcher_lease=lease,
    ) is approved_start
    assert runtime.calls == [
        (
            ("project-1", "worker-1"),
            {
                "lease_seconds": 30,
                "dispatcher_lease": lease,
            },
        )
    ]
    assert guard.calls == [
        (
            ("project-1", "operation-1"),
            {
                "worker_id": "worker-2",
                "lease_seconds": 45,
                "dispatcher_lease": lease,
            },
        )
    ]


@pytest.mark.asyncio
async def test_task7_c5_slotbudget_recovery_precedes_queue_and_done_frees_slot(
    caplog,
):
    from gateway.project_runtime_dispatcher import (
        ProjectRuntimeDispatcher,
    )
    from hermes_cli.project_runtime import DispatcherLease

    calls = []
    dispatcher_lease = DispatcherLease(
        _INSTANCE_A,
        1,
        1,
        100,
    )
    recovery_start = _task7_c5_worker_start(
        "approved_operation",
        "recovery",
        dispatcher_lease,
    )
    queued_start = _task7_c5_worker_start(
        "queued_turn",
        "queue",
        dispatcher_lease,
    )
    recovery_cursor = object()
    final_recovery_cursor = object()
    worker_failure = RuntimeError("task7-c5 worker failure")

    class Runtime:
        def __init__(self):
            self.queue_upper_reads = 0

        def runnable_project_membership_upper_watermark(self):
            calls.append("queue_upper")
            self.queue_upper_reads += 1
            return 1

        def scan_runnable_projects(self, **kwargs):
            calls.append(("queue_scan", kwargs))
            return SimpleNamespace(
                projects=(
                    SimpleNamespace(project_id="queued-project"),
                ),
                scanned_through=object(),
                reached_epoch_end=True,
            )

        def claim_next_turn_for_dispatcher(
            self,
            project_id,
            worker_id,
            **kwargs,
        ):
            calls.append(
                ("queue_claim", project_id, worker_id, kwargs)
            )
            return queued_start

        def claim_next_turn(self, *args, **kwargs):
            raise AssertionError("unfenced queue claim was used")

        def run_project(self, *args, **kwargs):
            raise AssertionError("worker-side reclaim was used")

        def __getattr__(self, name):
            raise AssertionError(
                f"dispatcher touched forbidden runtime member {name}"
            )

    class Guard:
        def __init__(self):
            self.upper_reads = 0
            self.recovery_calls = 0

        def operation_recovery_membership_upper_watermark(self):
            calls.append("recovery_upper")
            self.upper_reads += 1
            return 1 if self.upper_reads == 1 else None

        def recover_pending_operations(self, *args, **kwargs):
            calls.append(("recovery", kwargs))
            self.recovery_calls += 1
            if self.recovery_calls == 1:
                return SimpleNamespace(
                    starts=(recovery_start,),
                    scanned_through=recovery_cursor,
                    reached_epoch_end=False,
                )
            assert self.recovery_calls == 2
            return SimpleNamespace(
                starts=(),
                scanned_through=final_recovery_cursor,
                reached_epoch_end=True,
            )

        def __getattr__(self, name):
            raise AssertionError(
                "dispatcher touched forbidden operation-guard member "
                f"{name}"
            )

    class Worker:
        def __init__(self):
            self.starts = []
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def run_start(self, start):
            self.starts.append(start)
            if start is queued_start:
                raise worker_failure
            self.entered.set()
            await self.release.wait()

    runtime = Runtime()
    guard = Guard()
    worker = Worker()
    dispatcher = ProjectRuntimeDispatcher(
        runtime,
        guard,
        worker,
        worker_cap=1,
    )
    for invalid_cap in (True, 0, -1):
        with pytest.raises((TypeError, ValueError)):
            ProjectRuntimeDispatcher(
                runtime,
                guard,
                worker,
                worker_cap=invalid_cap,
            )

    with pytest.raises(RuntimeError, match="no running event loop"):
        await asyncio.to_thread(
            dispatcher.dispatch_once,
            worker_id="task7-c5-worker",
            lease_seconds=30,
            dispatcher_lease=dispatcher_lease,
            readback=object(),
            approval_checkpoints=object(),
        )
    assert calls == []
    assert dispatcher.available_slots == 1

    dispatcher.dispatch_once(
        worker_id="task7-c5-worker",
        lease_seconds=30,
        dispatcher_lease=dispatcher_lease,
        readback=object(),
        approval_checkpoints=object(),
    )
    assert dispatcher.available_slots == 0
    assert calls[0] == "recovery_upper"
    assert calls[1][0] == "recovery"
    assert calls[1][1]["max_claims"] == 1
    assert calls[1][1]["after"] is None
    assert calls[1][1]["through_membership_sequence"] == 1
    assert calls[1][1]["limit"] == 100
    assert "queue_upper" not in calls

    await worker.entered.wait()
    assert worker.starts == [recovery_start]
    dispatcher.dispatch_once(
        worker_id="task7-c5-worker",
        lease_seconds=30,
        dispatcher_lease=dispatcher_lease,
        readback=object(),
        approval_checkpoints=object(),
    )
    assert calls[2][0] == "recovery"
    assert calls[2][1]["max_claims"] == 0
    assert calls[2][1]["after"] is recovery_cursor
    assert calls[2][1]["through_membership_sequence"] == 1
    assert calls[2][1]["limit"] == 100
    assert "queue_upper" not in calls
    assert dispatcher.available_slots == 0

    recovery_task = next(iter(dispatcher._live_worker_tasks))
    worker.release.set()
    await recovery_task
    assert dispatcher.available_slots == 1

    with caplog.at_level(
        logging.ERROR,
        logger="gateway.project_runtime_dispatcher",
    ):
        dispatcher.dispatch_once(
            worker_id="task7-c5-worker",
            lease_seconds=30,
            dispatcher_lease=dispatcher_lease,
            readback=object(),
            approval_checkpoints=object(),
        )
        assert calls[-2][0] == "queue_scan"
        assert calls[-1][0] == "queue_claim"
        assert dispatcher.available_slots == 0
        queued_task = next(iter(dispatcher._live_worker_tasks))
        with pytest.raises(
            RuntimeError,
            match="task7-c5 worker failure",
        ) as failed:
            await queued_task
    assert failed.value is worker_failure
    assert worker.starts == [recovery_start, queued_start]
    assert dispatcher.available_slots == 1
    assert sum(
        type(call) is tuple and call[0] == "queue_claim"
        for call in calls
    ) == 1
    failures = [
        record
        for record in caplog.records
        if record.name == "gateway.project_runtime_dispatcher"
        and record.message == "project runtime worker failed"
    ]
    assert len(failures) == 1
    assert failures[0].levelno == logging.ERROR
    assert failures[0].exc_info is not None
    assert failures[0].exc_info[1] is worker_failure


@pytest.mark.asyncio
async def test_task7_c5_slotbudget_both_sources_keep_exact_start_without_reclaim():
    from gateway.project_runtime_dispatcher import (
        ProjectRuntimeDispatcher,
    )
    from gateway.project_runtime_worker import ProjectRuntimeWorker
    from hermes_cli.project_runtime import DispatcherLease, WorkerStart

    assert ProjectRuntimeWorker._is_protocol is True
    assert inspect.iscoroutinefunction(ProjectRuntimeWorker.run_start)
    assert tuple(
        inspect.signature(
            ProjectRuntimeWorker.run_start
        ).parameters
    ) == ("self", "start")
    assert get_type_hints(ProjectRuntimeWorker.run_start) == {
        "start": WorkerStart,
        "return": type(None),
    }

    calls = []
    dispatcher_lease = DispatcherLease(
        _INSTANCE_A,
        1,
        1,
        100,
    )
    recovery_start = _task7_c5_worker_start(
        "approved_operation",
        "recovery",
        dispatcher_lease,
    )
    queued_start = _task7_c5_worker_start(
        "queued_turn",
        "queue",
        dispatcher_lease,
    )

    class PoisonRuntime:
        def runnable_project_membership_upper_watermark(self):
            calls.append("queue_upper")
            return 1

        def scan_runnable_projects(self, **kwargs):
            calls.append(("queue_scan", kwargs))
            return SimpleNamespace(
                projects=(
                    SimpleNamespace(project_id="queued-project"),
                ),
                scanned_through=object(),
                reached_epoch_end=True,
            )

        def claim_next_turn_for_dispatcher(
            self,
            project_id,
            worker_id,
            **kwargs,
        ):
            calls.append(
                ("fenced_queue_claim", project_id, worker_id, kwargs)
            )
            return queued_start

        def claim_next_turn(self, *args, **kwargs):
            raise AssertionError("unfenced queue claim was used")

        def run_project(self, *args, **kwargs):
            raise AssertionError("worker-side reclaim was used")

        def __getattr__(self, name):
            raise AssertionError(
                f"dispatcher touched forbidden runtime member {name}"
            )

    class PoisonGuard:
        def operation_recovery_membership_upper_watermark(self):
            calls.append("recovery_upper")
            return 1

        def recover_pending_operations(self, *args, **kwargs):
            calls.append(("bounded_recovery", kwargs))
            return SimpleNamespace(
                starts=(recovery_start,),
                scanned_through=object(),
                reached_epoch_end=True,
            )

        def _recover_pending_operations(self, *args, **kwargs):
            raise AssertionError("legacy recovery was used")

        def _rehydrate_approved_operation(self, *args, **kwargs):
            raise AssertionError("unfenced rehydration was used")

        def __getattr__(self, name):
            raise AssertionError(
                "dispatcher touched forbidden operation-guard member "
                f"{name}"
            )

    class ExactStartWorker:
        def __init__(self):
            self.starts = []
            self.ready = asyncio.Event()
            self.release = asyncio.Event()

        async def run_start(self, start):
            self.starts.append(start)
            if len(self.starts) == 2:
                self.ready.set()
            await self.release.wait()

        def __getattr__(self, name):
            raise AssertionError(
                f"dispatcher touched forbidden worker member {name}"
            )

    worker = ExactStartWorker()
    dispatcher = ProjectRuntimeDispatcher(
        PoisonRuntime(),
        PoisonGuard(),
        worker,
        worker_cap=2,
    )
    dispatcher.dispatch_once(
        worker_id="task7-c5-worker",
        lease_seconds=30,
        dispatcher_lease=dispatcher_lease,
        readback=object(),
        approval_checkpoints=object(),
    )

    assert dispatcher.available_slots == 0
    assert calls[0] == "recovery_upper"
    assert calls[1][0] == "bounded_recovery"
    assert calls[2] == "queue_upper"
    assert calls[3][0] == "queue_scan"
    assert calls[4][0] == "fenced_queue_claim"
    tasks = tuple(dispatcher._live_worker_tasks)
    assert len(tasks) == 2
    await worker.ready.wait()
    assert worker.starts[0] is recovery_start
    assert worker.starts[1] is queued_start

    worker.release.set()
    await asyncio.gather(*tasks)
    assert dispatcher.available_slots == 2


@pytest.mark.asyncio
async def test_task7_c8_publish_ack_post_terminal_cas_is_rediscovered_without_agent_run(
    tmp_path,
    monkeypatch,
) -> None:
    """The bounded settlement lane closes a C7 gate without a worker start."""
    import gateway.session as session_module
    from gateway.session import AsyncSessionStore
    from hermes_cli import project_runtime_db as prdb
    from hermes_cli import projects_db
    from hermes_cli.project_policy import ActorContext
    from hermes_cli.project_runtime import CanonicalTurnResult, ProjectRuntime
    from hermes_state import SessionDB

    projects_path = tmp_path / "projects.db"
    state = SessionDB(db_path=tmp_path / "state.db")
    conn = projects_db.connect(projects_path)
    worker_starts = []
    agent_runs = []
    try:
        project_id = projects_db.create_project(conn, name="C8 rediscovery")
        prdb.create_project_conversation(
            conn, project_id=project_id, conversation_id="c8-session",
            current_phase="implementation", now=1,
        )
        prdb.bind_surface(
            conn, binding_id="c8-owner", project_id=project_id,
            surface="desktop", external_binding_id="c8-window", actor_id="owner", now=1,
        )
        state.create_session("c8-session", source="cli")
        runtime = ProjectRuntime(conn, clock=lambda: 100)
        actor = ActorContext("owner", "desktop", "c8-owner", True)
        turn = runtime.enqueue_turn(
            project_id, {"message": "recover terminal"}, actor,
            idempotency_key="c8-rediscover", expected_version=0,
        )
        claim = runtime.claim_next_turn(project_id, "c8-worker", lease_seconds=30)
        assert claim is not None
        claim = runtime.mark_turn_started(claim)
        batch_id = "123e4567-e89b-42d3-a456-426614174000"
        prepared = state.prepare_terminal_result(
            claim, batch_id=batch_id, status="succeeded", base_message_count=0,
            messages=(
                {"role": "user", "content": "recover", "timestamp": 10.0},
                {"role": "assistant", "content": "settled", "timestamp": 11.0},
            ),
        )
        runtime.commit_turn_with_task7_batch(
            claim, CanonicalTurnResult("succeeded", batch_id),
            transcript_batch_id=batch_id,
        )

        upper = state.pending_project_batch_upper_watermark()
        assert upper is not None
        assert state.list_pending_project_batches(
            after=None, through=upper, limit=1
        ) == (prepared,)
        project_connections = []
        project_traces = []
        closed_connection_ids: set[int] = set()
        resolver_instances = []

        def projects_factory():
            assert not state._conn.in_transaction
            project_connection = projects_db.connect(projects_path)
            trace = []
            project_connection.set_trace_callback(trace.append)
            project_connections.append(project_connection)
            project_traces.append(trace)
            return project_connection

        original_authority_resolver = (
            session_module.ProjectBatchAuthorityResolver
        )

        def record_closed_connection_on_owner_thread(connection):
            try:
                connection.execute("SELECT 1")
            except sqlite3.ProgrammingError as exc:
                assert "closed" in str(exc).lower()
                closed_connection_ids.add(id(connection))
                return
            raise AssertionError("Projects connection remained open")

        class RecordingResolver:
            def __init__(self, projects_db_factory):
                resolver_instances.append(self)
                self.delegate = original_authority_resolver(
                    projects_db_factory
                )

            def resolve_prepared_terminal(
                self,
                attempt,
                *,
                prepared_result_id,
                status,
            ):
                factory_count = len(project_connections)
                try:
                    return self.delegate.resolve_prepared_terminal(
                        attempt,
                        prepared_result_id=prepared_result_id,
                        status=status,
                    )
                finally:
                    assert len(project_connections) == factory_count + 1
                    project_connection = project_connections[-1]
                    record_closed_connection_on_owner_thread(
                        project_connection
                    )

            def ack_terminal_transcript_applied(
                self,
                acknowledgement,
            ):
                factory_count = len(project_connections)
                try:
                    return (
                        self.delegate
                        .ack_terminal_transcript_applied(
                            acknowledgement
                        )
                    )
                finally:
                    assert len(project_connections) == factory_count + 1
                    project_connection = project_connections[-1]
                    record_closed_connection_on_owner_thread(
                        project_connection
                    )

        monkeypatch.setattr(
            session_module,
            "ProjectBatchAuthorityResolver",
            RecordingResolver,
        )
        adapter = AsyncSessionStore(
            state,
            projects_db_factory=projects_factory,
        )
        assert len(resolver_instances) == 1
        from gateway.project_runtime_worker import ProjectRuntimeWorker

        def forbidden_claim(*args, **kwargs):
            worker_starts.append((args, kwargs))
            raise AssertionError("C8 rediscovery must not claim or start a worker")

        async def forbidden_agent_start(*args, **kwargs):
            agent_runs.append((args, kwargs))
            raise AssertionError("C8 rediscovery must not run an agent")

        monkeypatch.setattr(ProjectRuntime, "claim_next_turn", forbidden_claim)
        monkeypatch.setattr(
            ProjectRuntime, "claim_next_turn_for_dispatcher", forbidden_claim
        )
        monkeypatch.setattr(ProjectRuntimeWorker, "run_start", forbidden_agent_start)
        monkeypatch.setattr(
            state,
            "append_message",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("rediscovery may not enter the public agent append path")
            ),
        )

        def project_authority_snapshot():
            return {
                "runtime": dict(
                    conn.execute(
                        """
                        SELECT * FROM project_runtime_state
                        WHERE project_id = ?
                        """,
                        (project_id,),
                    ).fetchone()
                ),
                "turn": dict(
                    conn.execute(
                        "SELECT * FROM project_turns WHERE turn_id = ?",
                        (turn.turn_id,),
                    ).fetchone()
                ),
                "control": tuple(
                    conn.execute(
                        """
                        SELECT * FROM project_run_controls
                        WHERE turn_id = ?
                        """,
                        (turn.turn_id,),
                    ).fetchone()
                ),
                "leases": tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_worker_leases
                        WHERE project_id = ? ORDER BY lease_id
                        """,
                        (project_id,),
                    )
                ),
                "events": tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_events
                        WHERE project_id = ? ORDER BY sequence
                        """,
                        (project_id,),
                    )
                ),
                "deliveries": tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_deliveries
                        WHERE project_id = ? ORDER BY delivery_id
                        """,
                        (project_id,),
                    )
                ),
                "membership": tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_runtime_membership_counters
                        ORDER BY lane
                        """
                    )
                ),
            }

        def state_authority_snapshot():
            return {
                "batch": dict(
                    state._conn.execute(
                        """
                        SELECT * FROM project_turn_transcript_batches
                        WHERE batch_id = ?
                        """,
                        (batch_id,),
                    ).fetchone()
                ),
                "messages": tuple(
                    tuple(row)
                    for row in state._conn.execute(
                        """
                        SELECT * FROM messages
                        WHERE session_id = 'c8-session'
                        ORDER BY id
                        """
                    )
                ),
                "session": dict(
                    state._conn.execute(
                        """
                        SELECT * FROM sessions
                        WHERE id = 'c8-session'
                        """
                    ).fetchone()
                ),
                "counter": tuple(
                    state._conn.execute(
                        """
                        SELECT singleton, last_sequence
                        FROM project_batch_sequence_counter
                        """
                    ).fetchone()
                ),
            }

        project_before = project_authority_snapshot()
        state_before = state_authority_snapshot()
        assert project_before["leases"] == ()
        assert project_before["turn"]["attempt_id"] == claim.attempt_id
        assert state_before["messages"] == ()
        state_trace = []
        state._conn.set_trace_callback(state_trace.append)
        try:
            result = await adapter.apply_project_batch(prepared.batch_id)
        finally:
            state._conn.set_trace_callback(None)
        assert result.outcome == "published"
        project_after = project_authority_snapshot()
        state_after = state_authority_snapshot()

        assert conn.execute(
            "SELECT transcript_pending_batch_id FROM project_runtime_state "
            "WHERE project_id = ?",
            (project_id,),
        ).fetchone()[0] is None
        assert conn.execute(
            "SELECT transcript_applied_batch_id FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == batch_id
        assert len(state.get_messages("c8-session")) == 2

        runtime_before = dict(project_before["runtime"])
        runtime_after = dict(project_after["runtime"])
        assert runtime_before.pop("transcript_pending_batch_id") == batch_id
        assert runtime_after.pop("transcript_pending_batch_id") is None
        assert runtime_after == runtime_before
        turn_before = dict(project_before["turn"])
        turn_after = dict(project_after["turn"])
        assert turn_before.pop("transcript_applied_batch_id") is None
        assert turn_after.pop("transcript_applied_batch_id") == batch_id
        assert turn_after == turn_before
        assert project_after["control"] == project_before["control"]
        assert project_after["leases"] == project_before["leases"] == ()
        assert project_after["events"] == project_before["events"]
        assert project_after["deliveries"] == project_before["deliveries"]
        assert project_after["membership"] == project_before["membership"]

        batch_before = dict(state_before["batch"])
        batch_after = dict(state_after["batch"])
        assert batch_before.pop("state") == "prepared"
        assert batch_after.pop("state") == "published"
        assert batch_before.pop("published_at") is None
        published_at = batch_after.pop("published_at")
        assert type(published_at) in {int, float}
        assert 0 <= published_at <= 253_402_300_799.0
        assert batch_before.pop("projects_acknowledged_at") is None
        acknowledged_at = batch_after.pop("projects_acknowledged_at")
        assert type(acknowledged_at) in {int, float}
        assert 0 <= acknowledged_at <= 253_402_300_799.0
        assert batch_after == batch_before
        assert state_after["counter"] == state_before["counter"]
        session_before = dict(state_before["session"])
        session_after = dict(state_after["session"])
        assert session_before.pop("message_count") == 0
        assert session_after.pop("message_count") == 2
        assert session_before.pop("tool_call_count") == 0
        assert session_after.pop("tool_call_count") == 0
        assert session_after == session_before
        assert [
            row[3] for row in state_after["messages"]
        ] == ["recover", "settled"]

        project_mutations = [
            statement
            for trace in project_traces
            for statement in trace
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]
        normalized_project_mutations = [
            " ".join(statement.upper().split())
            for statement in project_mutations
        ]
        assert normalized_project_mutations
        assert all(
            (
                statement.startswith("UPDATE PROJECT_TURNS")
                and "TRANSCRIPT_APPLIED_BATCH_ID" in statement
            )
            or (
                statement.startswith("UPDATE PROJECT_RUNTIME_STATE")
                and "TRANSCRIPT_PENDING_BATCH_ID" in statement
            )
            for statement in normalized_project_mutations
        )

        def assigned_columns(statement):
            set_clause = statement.split(" SET ", 1)[1].split(
                " WHERE ",
                1,
            )[0]
            return {
                assignment.split("=", 1)[0].strip().split(".")[-1]
                for assignment in set_clause.split(",")
            }

        for statement in normalized_project_mutations:
            if statement.startswith("UPDATE PROJECT_TURNS"):
                assert assigned_columns(statement) == {
                    "TRANSCRIPT_APPLIED_BATCH_ID"
                }
            else:
                assert statement.startswith(
                    "UPDATE PROJECT_RUNTIME_STATE"
                )
                assert assigned_columns(statement) == {
                    "TRANSCRIPT_PENDING_BATCH_ID"
                }
        normalized_state_mutations = [
            " ".join(statement.upper().split())
            for statement in state_trace
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]
        assert any(
            "INSERT INTO MESSAGES" in statement
            for statement in normalized_state_mutations
        )
        assert any(
            "PROJECT_TURN_TRANSCRIPT_BATCHES" in statement
            and "STATE" in statement
            for statement in normalized_state_mutations
        )
        assert any(
            "PROJECT_TURN_TRANSCRIPT_BATCHES" in statement
            and "PROJECTS_ACKNOWLEDGED_AT" in statement
            for statement in normalized_state_mutations
        )
        assert not any(
            "PROJECT_TURNS" in statement
            or "PROJECT_WORKER_LEASES" in statement
            or "PROJECT_EVENTS" in statement
            for statement in normalized_state_mutations
        )

        replay_before = (
            project_authority_snapshot(),
            state_authority_snapshot(),
        )
        replay_factory_count = len(project_connections)
        replay_state_changes = state._conn.total_changes
        replay = await adapter.apply_project_batch(prepared.batch_id)
        assert replay.outcome == "already_published"
        assert (
            project_authority_snapshot(),
            state_authority_snapshot(),
        ) == replay_before
        assert len(project_connections) == replay_factory_count
        assert state._conn.total_changes == replay_state_changes
        assert {
            id(project_connection)
            for project_connection in project_connections
        } == closed_connection_ids
        assert worker_starts == []
        assert agent_runs == []
    finally:
        conn.close()
        state.close()
