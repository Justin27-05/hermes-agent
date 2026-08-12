"""C11 stop-closure contract for a preclaimed project worker."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import sqlite3
import threading
from dataclasses import fields, replace
from types import SimpleNamespace

import pytest

from tests.gateway.project_runtime_test_helpers import RetainedThreadRunner


@pytest.mark.asyncio
async def test_task7_c11_stop_closure_linearizes_stop_and_terminal_cas_without_retry(
    tmp_path,
    caplog,
):
    """A durable stop must quiesce before its single ack/apply boundary.

    Catches a closer that acknowledges a still-running agent, retries a stale
    terminal CAS, or applies a prepared terminal batch before stop authority.
    """
    import gateway.project_runtime_worker as worker_module
    from hermes_cli import project_runtime as runtime_module

    assert hasattr(worker_module, "StopRequest"), (
        "C11 requires the exact-key StopRequest contract"
    )
    assert hasattr(worker_module, "ProjectRuntimeTerminalCloser"), (
        "C11 requires one reusable cancel/quiesce/ack/apply closer"
    )
    assert hasattr(worker_module, "ProjectRuntimeLiveHandle"), (
        "C11 requires one exact-key live cancellation handle"
    )
    assert callable(
        getattr(worker_module.ProjectRuntimeWorker, "request_stop", None)
    ), "C11 extends the worker protocol with exact stop forwarding"
    assert tuple(field.name for field in fields(worker_module.StopRequest)) == (
        "project_id",
        "turn_id",
        "attempt_id",
        "worker_id",
        "lease_generation",
        "fencing_token",
        "canonical_session_id",
        "control_version",
    )
    assert hasattr(runtime_module, "ClaimControl"), (
        "C11 requires the read-only exact-claim control DTO"
    )
    assert tuple(field.name for field in fields(runtime_module.ClaimControl)) == (
        "state",
        "control_version",
        "lease_expires_at",
    )

    # The durable authority read is exact, accepts an older local heartbeat
    # horizon, and is read-only even for stale/caller-owned-transaction paths.
    from hermes_cli import project_runtime_db as prdb
    from hermes_cli import projects_db
    from hermes_cli.project_policy import ActorContext
    runtime_conn = projects_db.connect(tmp_path / "c11-control.db")
    try:
        def live_case(label: str, *, started: bool = True):
            project_id = projects_db.create_project(
                runtime_conn,
                name=f"C11 {label}",
            )
            binding_id = f"{label}-binding"
            prdb.create_project_conversation(
                runtime_conn,
                project_id=project_id,
                conversation_id=f"{label}-session",
                current_phase="implementation",
                now=1,
            )
            prdb.bind_surface(
                runtime_conn,
                binding_id=binding_id,
                project_id=project_id,
                surface="desktop",
                external_binding_id=f"{label}-window",
                actor_id="owner",
                now=1,
            )
            runtime = runtime_module.ProjectRuntime(
                runtime_conn,
                clock=lambda: 100,
            )
            actor = ActorContext("owner", "desktop", binding_id, True)
            turn = runtime.enqueue_turn(
                project_id,
                {"message": label},
                actor,
                idempotency_key=f"{label}-enqueue",
                expected_version=0,
            )
            claim = runtime.claim_next_turn(
                project_id,
                f"{label}-worker",
                lease_seconds=30,
            )
            assert claim is not None
            if started:
                claim = runtime.mark_turn_started(claim)
            return (
                project_id,
                runtime,
                actor,
                turn,
                claim,
            )

        (
            _,
            running_runtime,
            _,
            _,
            running_claim,
        ) = live_case("c11-running")
        changes = runtime_conn.total_changes
        running = running_runtime.control_for_claim(running_claim)
        assert (running.state, running.lease_expires_at) == (
            "running",
            running_claim.lease_expires_at,
        )
        assert runtime_conn.total_changes == changes
        renewed = running_runtime.heartbeat_turn(
            running_claim,
            lease_seconds=60,
        )
        changes = runtime_conn.total_changes
        renewed_control = running_runtime.control_for_claim(running_claim)
        assert renewed_control.lease_expires_at == renewed.lease_expires_at
        assert renewed_control.lease_expires_at > running_claim.lease_expires_at
        assert runtime_conn.total_changes == changes

        # A valid not-started attempt is live authority too.  The read itself
        # remains side-effect free.
        (
            not_started_project,
            not_started_runtime,
            _,
            not_started_turn,
            not_started_claim,
        ) = live_case("c11-not-started", started=False)
        assert prdb._runtime_turn_for_project(
            runtime_conn,
            project_id=not_started_project,
            turn_id=not_started_turn.turn_id,
        ).execution_state == "not_started"
        not_started_changes = runtime_conn.total_changes
        assert not_started_runtime.control_for_claim(not_started_claim).state == "running"
        assert runtime_conn.total_changes == not_started_changes

        # A successful authority read samples time exactly once and owns one
        # explicit read snapshot.
        clock_samples: list[str] = []

        def sampled_clock():
            clock_samples.append("clock")
            return 100

        snapshot_runtime = runtime_module.ProjectRuntime(
            runtime_conn,
            clock=sampled_clock,
        )
        snapshot_trace: list[str] = []
        runtime_conn.set_trace_callback(snapshot_trace.append)
        try:
            assert snapshot_runtime.control_for_claim(running_claim).state == "running"
        finally:
            runtime_conn.set_trace_callback(None)
        transaction_trace = [
            statement.strip().upper()
            for statement in snapshot_trace
            if statement.lstrip().upper().startswith(
                ("BEGIN", "COMMIT", "ROLLBACK")
            )
        ]
        assert clock_samples == ["clock"]
        assert len(transaction_trace) == 2
        assert transaction_trace[0].startswith("BEGIN")
        assert transaction_trace[1] == "COMMIT"

        # Exact-claim validation happens before the first SQL statement.
        malformed_trace: list[str] = []
        malformed_changes = runtime_conn.total_changes
        runtime_conn.set_trace_callback(malformed_trace.append)
        try:
            with pytest.raises(runtime_module.ProjectRuntimeError) as malformed:
                running_runtime.control_for_claim(
                    replace(running_claim, sequence=True)
                )
        finally:
            runtime_conn.set_trace_callback(None)
        assert malformed.value.code is runtime_module.RuntimeErrorCode.INVALID_ARGUMENT
        assert malformed_trace == []
        assert runtime_conn.total_changes == malformed_changes
        for field, wrong_value in (
            ("project_id", "wrong-project"), ("turn_id", "wrong-turn"),
            ("sequence", 2), ("worker_id", "wrong-worker"),
            ("attempt_id", "wrong-attempt"), ("lease_generation", 99),
            ("fencing_token", 99), ("canonical_session_id", "wrong-session"),
            ("lease_expires_at", renewed.lease_expires_at + 1),
        ):
            stale_changes = runtime_conn.total_changes
            with pytest.raises(runtime_module.ProjectRuntimeError) as stale:
                running_runtime.control_for_claim(
                    replace(running_claim, **{field: wrong_value})
                )
            assert stale.value.code is runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
            assert runtime_conn.total_changes == stale_changes
        now = [100]
        expiring_runtime = runtime_module.ProjectRuntime(runtime_conn, clock=lambda: now[0])
        now[0] = renewed.lease_expires_at
        expired_changes = runtime_conn.total_changes
        with pytest.raises(runtime_module.ProjectRuntimeError) as expired:
            expiring_runtime.control_for_claim(running_claim)
        assert expired.value.code is runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert runtime_conn.total_changes == expired_changes

        # Each legal mapping is exercised from its own coherent durable pair;
        # no case borrows a stop control while pretending to await approval.
        (
            awaiting_project,
            awaiting_runtime,
            _,
            awaiting_turn,
            awaiting_claim,
        ) = live_case("c11-awaiting")
        runtime_conn.execute(
            "UPDATE project_turns SET status = 'awaiting_approval' "
            "WHERE project_id = ? AND turn_id = ?",
            (awaiting_project, awaiting_turn.turn_id),
        )
        runtime_conn.commit()
        assert awaiting_runtime.control_for_claim(awaiting_claim).state == "awaiting_approval"

        (
            incoherent_project,
            incoherent_runtime,
            _,
            incoherent_turn,
            incoherent_claim,
        ) = live_case("c11-incoherent")
        runtime_conn.execute(
            "UPDATE project_turns SET status = 'stop_requested' "
            "WHERE project_id = ? AND turn_id = ?",
            (incoherent_project, incoherent_turn.turn_id),
        )
        runtime_conn.commit()
        incoherent_changes = runtime_conn.total_changes
        with pytest.raises(runtime_module.ProjectRuntimeError) as incoherent:
            incoherent_runtime.control_for_claim(incoherent_claim)
        assert incoherent.value.code is runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert runtime_conn.total_changes == incoherent_changes

        def assert_stale_read_only(runtime, observed_claim):
            trace: list[str] = []
            before_changes = runtime_conn.total_changes
            runtime_conn.set_trace_callback(trace.append)
            try:
                with pytest.raises(runtime_module.ProjectRuntimeError) as stale:
                    runtime.control_for_claim(observed_claim)
            finally:
                runtime_conn.set_trace_callback(None)
            assert stale.value.code is runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
            assert runtime_conn.total_changes == before_changes
            assert not [
                statement
                for statement in trace
                if statement.lstrip().upper().startswith(
                    ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
            ]

        # Each missing authority component fails closed independently.
        (
            missing_control_project,
            missing_control_runtime,
            _,
            missing_control_turn,
            missing_control_claim,
        ) = live_case("c11-missing-control")
        runtime_conn.execute(
            "DELETE FROM project_run_controls "
            "WHERE project_id = ? AND turn_id = ?",
            (missing_control_project, missing_control_turn.turn_id),
        )
        runtime_conn.commit()
        assert_stale_read_only(missing_control_runtime, missing_control_claim)

        (
            missing_lease_project,
            missing_lease_runtime,
            _,
            missing_lease_turn,
            missing_lease_claim,
        ) = live_case("c11-missing-lease")
        runtime_conn.execute(
            "DELETE FROM project_worker_leases "
            "WHERE project_id = ? AND turn_id = ?",
            (missing_lease_project, missing_lease_turn.turn_id),
        )
        runtime_conn.commit()
        assert_stale_read_only(missing_lease_runtime, missing_lease_claim)

        (
            missing_turn_project,
            missing_turn_runtime,
            _,
            missing_turn,
            missing_turn_claim,
        ) = live_case("c11-missing-turn")
        runtime_conn.execute("PRAGMA foreign_keys = OFF")
        try:
            runtime_conn.execute(
                "DELETE FROM project_turns "
                "WHERE project_id = ? AND turn_id = ?",
                (missing_turn_project, missing_turn.turn_id),
            )
            runtime_conn.commit()
        finally:
            runtime_conn.execute("PRAGMA foreign_keys = ON")
        assert_stale_read_only(missing_turn_runtime, missing_turn_claim)

        # Individually valid stored statuses outside the three allowed pairs
        # are never projected as live control.
        for label, turn_status, control_state in (
            ("c11-pair-claimed-stop", "claimed", "stop_requested"),
            ("c11-pair-awaiting-stop", "awaiting_approval", "stop_requested"),
            ("c11-pair-stop-running", "stop_requested", "running"),
            ("c11-pair-stopped", "stopped", "stopped"),
        ):
            (
                pair_project,
                pair_runtime,
                _,
                pair_turn,
                pair_claim,
            ) = live_case(label)
            runtime_conn.execute(
                "UPDATE project_turns SET status = ? "
                "WHERE project_id = ? AND turn_id = ?",
                (turn_status, pair_project, pair_turn.turn_id),
            )
            runtime_conn.execute(
                "UPDATE project_run_controls SET control_state = ? "
                "WHERE project_id = ? AND turn_id = ?",
                (control_state, pair_project, pair_turn.turn_id),
            )
            runtime_conn.commit()
            assert_stale_read_only(pair_runtime, pair_claim)

        (
            lifecycle_project,
            lifecycle_runtime,
            _,
            _,
            lifecycle_claim,
        ) = live_case("c11-lifecycle")
        lifecycle_state = lifecycle_runtime._require_state(lifecycle_project)
        with prdb.write_transaction(runtime_conn):
            assert prdb.transition_lifecycle(
                runtime_conn,
                project_id=lifecycle_project,
                expected_version=lifecycle_state.version,
                lifecycle="awaiting_acceptance",
                updated_at=101,
            ) is not None
        lifecycle_changes = runtime_conn.total_changes
        with pytest.raises(runtime_module.ProjectRuntimeError) as lifecycle_stale:
            lifecycle_runtime.control_for_claim(lifecycle_claim)
        assert lifecycle_stale.value.code is runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert runtime_conn.total_changes == lifecycle_changes

        (
            tip_project,
            tip_runtime,
            _,
            _,
            tip_claim,
        ) = live_case("c11-tip")
        assert prdb.advance_conversation_tip(
            runtime_conn,
            project_id=tip_project,
            expected_tip_id=tip_claim.canonical_session_id,
            child_conversation_id="c11-tip-child",
            now=101,
        ) is not None
        tip_changes = runtime_conn.total_changes
        with pytest.raises(runtime_module.ProjectRuntimeError) as tip_stale:
            tip_runtime.control_for_claim(tip_claim)
        assert tip_stale.value.code is runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert runtime_conn.total_changes == tip_changes

        runtime_conn.execute("BEGIN")
        caller_transaction_trace: list[str] = []
        caller_transaction_changes = runtime_conn.total_changes
        runtime_conn.set_trace_callback(caller_transaction_trace.append)
        try:
            with pytest.raises(runtime_module.ProjectRuntimeError) as owned:
                running_runtime.control_for_claim(running_claim)
            assert owned.value.code is runtime_module.RuntimeErrorCode.INVALID_ARGUMENT
        finally:
            runtime_conn.set_trace_callback(None)
            runtime_conn.rollback()
        assert caller_transaction_trace == []
        assert runtime_conn.total_changes == caller_transaction_changes

        # Exact and fresh durable duplicates are both no-ops once stop has
        # won, first while live and again after the stopped/stopped close.
        (
            stop_project,
            stop_runtime,
            stop_actor,
            stop_turn,
            stop_claim,
        ) = live_case("c11-stop")

        def stop_snapshot():
            state = stop_runtime._require_state(stop_project)
            control = stop_runtime._control(stop_project, stop_turn.turn_id)
            control_row = runtime_conn.execute(
                "SELECT control_state, control_version, idempotency_key, "
                "command_fingerprint, updated_at FROM project_run_controls "
                "WHERE project_id = ? AND turn_id = ?",
                (stop_project, stop_turn.turn_id),
            ).fetchone()
            event_count = runtime_conn.execute(
                "SELECT COUNT(*) FROM project_events "
                "WHERE project_id = ? AND turn_id = ?",
                (stop_project, stop_turn.turn_id),
            ).fetchone()[0]
            return (
                runtime_conn.total_changes,
                state.version,
                control.control_version,
                tuple(control_row),
                event_count,
            )

        original_state = stop_runtime._require_state(stop_project)
        original_control = stop_runtime._control(stop_project, stop_turn.turn_id)
        first_stop = stop_runtime.request_stop(
            stop_project,
            stop_turn.turn_id,
            stop_actor,
            idempotency_key="c11-stop-first",
            expected_version=original_state.version,
            expected_control_version=original_control.control_version,
        )
        assert stop_runtime.control_for_claim(stop_claim).state == "stop_requested"
        stopped_requested_snapshot = stop_snapshot()
        assert stop_runtime.request_stop(
            stop_project,
            stop_turn.turn_id,
            stop_actor,
            idempotency_key="c11-stop-first",
            expected_version=original_state.version,
            expected_control_version=original_control.control_version,
        ) == first_stop
        assert stop_snapshot() == stopped_requested_snapshot
        current_state = stop_runtime._require_state(stop_project)
        current_control = stop_runtime._control(stop_project, stop_turn.turn_id)
        assert stop_runtime.request_stop(
            stop_project,
            stop_turn.turn_id,
            stop_actor,
            idempotency_key="c11-stop-fresh",
            expected_version=current_state.version,
            expected_control_version=current_control.control_version,
        ) == first_stop
        assert stop_snapshot() == stopped_requested_snapshot

        stopped = stop_runtime.acknowledge_stopped(stop_claim)
        assert stopped.control_state == "stopped"
        stopped_snapshot = stop_snapshot()
        assert stop_runtime.request_stop(
            stop_project,
            stop_turn.turn_id,
            stop_actor,
            idempotency_key="c11-stop-first",
            expected_version=original_state.version,
            expected_control_version=original_control.control_version,
        ) == stopped
        assert stop_snapshot() == stopped_snapshot

        # An already-terminal successful turn keeps the pre-existing Task-4
        # write-free stop no-op.
        (
            terminal_project,
            terminal_runtime,
            terminal_actor,
            terminal_turn,
            terminal_claim,
        ) = live_case("c11-terminal")
        terminal_runtime.commit_turn(
            terminal_claim,
            runtime_module.CanonicalTurnResult(
                "succeeded",
                "c11-terminal-result",
            ),
        )
        terminal_state = terminal_runtime._require_state(terminal_project)
        terminal_control = terminal_runtime._control(
            terminal_project,
            terminal_turn.turn_id,
        )
        terminal_snapshot = (
            runtime_conn.total_changes,
            terminal_state.version,
            terminal_control.control_version,
            tuple(
                runtime_conn.execute(
                    "SELECT * FROM project_run_controls "
                    "WHERE project_id = ? AND turn_id = ?",
                    (terminal_project, terminal_turn.turn_id),
                ).fetchone()
            ),
            runtime_conn.execute(
                "SELECT COUNT(*) FROM project_events "
                "WHERE project_id = ? AND turn_id = ?",
                (terminal_project, terminal_turn.turn_id),
            ).fetchone()[0],
        )
        terminal_stop = terminal_runtime.request_stop(
            terminal_project,
            terminal_turn.turn_id,
            terminal_actor,
            idempotency_key="c11-terminal-stop-noop",
            expected_version=terminal_state.version,
            expected_control_version=terminal_control.control_version,
        )
        assert terminal_stop.control_state == "terminal"
        assert (
            runtime_conn.total_changes,
            terminal_runtime._require_state(terminal_project).version,
            terminal_runtime._control(
                terminal_project,
                terminal_turn.turn_id,
            ).control_version,
            tuple(
                runtime_conn.execute(
                    "SELECT * FROM project_run_controls "
                    "WHERE project_id = ? AND turn_id = ?",
                    (terminal_project, terminal_turn.turn_id),
                ).fetchone()
            ),
            runtime_conn.execute(
                "SELECT COUNT(*) FROM project_events "
                "WHERE project_id = ? AND turn_id = ?",
                (terminal_project, terminal_turn.turn_id),
            ).fetchone()[0],
        ) == terminal_snapshot
        stopped_snapshot_after_other_project = stop_snapshot()
        current_state = stop_runtime._require_state(stop_project)
        current_control = stop_runtime._control(stop_project, stop_turn.turn_id)
        assert stop_runtime.request_stop(
            stop_project,
            stop_turn.turn_id,
            stop_actor,
            idempotency_key="c11-stop-after-stopped",
            expected_version=current_state.version,
            expected_control_version=current_control.control_version,
        ) == stopped
        assert stop_snapshot() == stopped_snapshot_after_other_project
    finally:
        runtime_conn.close()

    claim = runtime_module.TurnClaim(
        project_id="c11-project",
        turn_id="c11-turn",
        sequence=1,
        worker_id="c11-worker",
        attempt_id="c11-attempt",
        lease_generation=3,
        fencing_token=5,
        lease_expires_at=30,
        canonical_session_id="c11-session",
    )
    start = runtime_module.WorkerStart(
        source="queued_turn",
        claim=claim,
        operation=None,
        dispatcher_lease=runtime_module.DispatcherLease(
            instance_id="c11-dispatcher",
            generation=1,
            fencing_token=1,
            expires_at=60,
        ),
    )
    request = worker_module.StopRequest(
        project_id=claim.project_id,
        turn_id=claim.turn_id,
        attempt_id=claim.attempt_id,
        worker_id=claim.worker_id,
        lease_generation=claim.lease_generation,
        fencing_token=claim.fencing_token,
        canonical_session_id=claim.canonical_session_id,
        control_version=7,
    )
    order: list[str] = []
    waiting = asyncio.Event()
    quiescent = asyncio.Event()

    class Runner:
        def request_cancel(self):
            order.append("cancel")
            return True

        async def wait_quiescent(self):
            waiting.set()
            await quiescent.wait()
            order.append("quiescent")

    # The worker's request-side registry is exact and contains no durable
    # authority: replays share one cancellation hook; wrong authority and a
    # terminal/deactivated handle are rejected without fallback discovery.
    shared_handle = worker_module.ProjectRuntimeLiveHandle(start, Runner())
    assert shared_handle.request_cancel() is True
    assert shared_handle.request_cancel() is False
    # An earlier direct cancel does not prevent the exact durable-stop version
    # from latching; neither the first acceptance nor its replay reinvokes the
    # underlying hook.
    assert shared_handle.request_stop(request) is True
    assert shared_handle.request_stop(request) is True
    assert order == ["cancel"]
    for field, wrong_value in (
        ("project_id", "wrong-project"), ("turn_id", "wrong-turn"),
        ("attempt_id", "wrong-attempt"), ("worker_id", "wrong-worker"),
        ("lease_generation", 4), ("fencing_token", 6),
        ("canonical_session_id", "wrong-session"), ("control_version", 8),
    ):
        hook_snapshot = tuple(order)
        assert shared_handle.request_stop(
            replace(request, **{field: wrong_value})
        ) is False
        assert tuple(order) == hook_snapshot

    terminal_handle = worker_module.ProjectRuntimeLiveHandle(start, Runner())
    terminal_hook_snapshot = tuple(order)
    terminal_handle.mark_terminal_won()
    assert terminal_handle.request_stop(request) is False
    assert terminal_handle.request_cancel() is False
    assert tuple(order) == terminal_hook_snapshot

    inactive_handle = worker_module.ProjectRuntimeLiveHandle(start, Runner())
    inactive_hook_snapshot = tuple(order)
    inactive_handle.deactivate()
    assert inactive_handle.request_stop(request) is False
    assert inactive_handle.request_cancel() is False
    assert tuple(order) == inactive_hook_snapshot
    order.clear()

    # Cancellation and the accepted durable stop version are side-effect
    # boundaries.  Both must latch before the hook: a hook may re-enter every
    # request route after performing its effect and then raise.
    cancel_after_effect = RuntimeError("cancel hook failed after side effect")

    def capture_cancel_result(call):
        try:
            return call()
        except Exception as exc:
            return exc

    class ReentrantCancelThenRaiseRunner:
        def __init__(self):
            self.cancel_calls = 0
            self.handle = None
            self.reentrant_results: list[object] = []

        def request_cancel(self):
            self.cancel_calls += 1
            if self.cancel_calls == 1:
                self.reentrant_results = [
                    capture_cancel_result(self.handle.request_cancel),
                    capture_cancel_result(
                        lambda: self.handle.request_stop(
                            replace(request, control_version=8)
                        )
                    ),
                    capture_cancel_result(
                        lambda: self.handle.request_stop(request)
                    ),
                ]
            raise cancel_after_effect

        async def wait_quiescent(self):
            raise AssertionError("cancel latch probe must not await quiescence")

    raising_runner = ReentrantCancelThenRaiseRunner()
    raising_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        raising_runner,
    )
    raising_runner.handle = raising_handle
    with pytest.raises(RuntimeError) as raised_cancel_after_effect:
        raising_handle.request_stop(request)
    assert type(raised_cancel_after_effect.value) is RuntimeError
    assert str(raised_cancel_after_effect.value) == str(cancel_after_effect)
    raising_same_version_after_error = capture_cancel_result(
        lambda: raising_handle.request_stop(request)
    )
    raising_different_version_after_error = capture_cancel_result(
        lambda: raising_handle.request_stop(
            replace(request, control_version=8)
        )
    )

    class Runtime:
        async def control_for_claim(self, observed_claim):
            assert observed_claim == claim
            order.append("control")
            return runtime_module.ClaimControl("stop_requested", 7, 41)

        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            raise AssertionError("stop winner must not terminal-CAS")

        async def acknowledge_stopped(self, observed_claim):
            assert observed_claim.lease_expires_at == 41
            order.append("ack")
            return object()

    class Batches:
        async def apply_project_batch(self, batch_id):
            from gateway.session import ProjectBatchApplyResult

            assert batch_id == "123e4567-e89b-42d3-a456-426614174000"
            order.append("apply")
            return ProjectBatchApplyResult(outcome="discarded")

    closer = worker_module.ProjectRuntimeTerminalCloser(Runtime(), Batches())
    closing_handle = worker_module.ProjectRuntimeLiveHandle(start, Runner())
    closing = asyncio.create_task(
        closer.acknowledge_stop(
            claim=claim,
            runner=closing_handle,
            batch_id="123e4567-e89b-42d3-a456-426614174000",
        )
    )
    await waiting.wait()
    assert order == ["control", "cancel"]
    quiescent.set()
    result = await closing
    assert result.outcome == "discarded"
    assert order == ["control", "cancel", "quiescent", "ack", "apply"]
    # The closer does not own registry teardown: a quiesced, acknowledged
    # handle remains live and accepts its exact idempotent stop replay.
    assert closing_handle.request_stop(request) is True
    assert order == ["control", "cancel", "quiescent", "ack", "apply"]

    # A dispatcher hint may win the in-process race before the closer starts.
    # The closer's direct cancel then returns False, but quiescence, ack and
    # apply are still mandatory exactly once.
    hinted_order: list[str] = []

    class HintedRunner:
        def request_cancel(self):
            hinted_order.append("cancel")
            return True

        async def wait_quiescent(self):
            hinted_order.append("quiescent")

    class HintedRuntime:
        def __init__(self):
            self.reads = 0

        async def control_for_claim(self, observed_claim):
            self.reads += 1
            hinted_order.append("control")
            return runtime_module.ClaimControl("stop_requested", 7, 43)

        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            raise AssertionError("hinted stop must not terminal-CAS")

        async def acknowledge_stopped(self, observed_claim):
            assert observed_claim.lease_expires_at == 43
            hinted_order.append("ack")
            return object()

    class HintedBatches:
        async def apply_project_batch(self, batch_id):
            from gateway.session import ProjectBatchApplyResult

            hinted_order.append("apply")
            return ProjectBatchApplyResult("discarded")

    hinted_handle = worker_module.ProjectRuntimeLiveHandle(start, HintedRunner())
    assert hinted_handle.request_stop(request) is True
    assert hinted_order == ["cancel"]
    hinted_runtime = HintedRuntime()
    hinted_result = await worker_module.ProjectRuntimeTerminalCloser(
        hinted_runtime,
        HintedBatches(),
    ).resolve_prepared_terminal(
        claim=claim,
        result=runtime_module.CanonicalTurnResult(
            "succeeded",
            "193e4567-e89b-42d3-a456-426614174000",
        ),
        runner=hinted_handle,
        batch_id="193e4567-e89b-42d3-a456-426614174000",
    )
    assert hinted_result.outcome == "discarded"
    assert hinted_runtime.reads == 1
    assert hinted_order == [
        "cancel",
        "quiescent",
        "control",
        "quiescent",
        "ack",
        "apply",
    ]
    assert hinted_handle.request_stop(request) is True

    # Natural completion takes one terminal CAS, marks the exact handle won
    # before apply, and never turns a later stop into an acknowledgement.
    natural_order: list[str] = []
    class NaturalRunner:
        def request_cancel(self):
            natural_order.append("cancel")
            return True
        async def wait_quiescent(self):
            natural_order.append("quiescent")
    class NaturalRuntime:
        async def control_for_claim(self, observed_claim):
            natural_order.append("control")
            return runtime_module.ClaimControl("running", 8, 42)
        async def commit_turn_with_task7_batch(self, observed_claim, result, *, transcript_batch_id):
            assert observed_claim.lease_expires_at == 42
            assert result.result_id == transcript_batch_id
            natural_order.append("cas")
            return object()
        async def acknowledge_stopped(self, observed_claim):
            raise AssertionError("terminal winner must not acknowledge stop")
    class NaturalBatches:
        async def apply_project_batch(self, batch_id):
            # This runs inside apply, not after it: terminal ownership must be
            # visible before any State settlement can start.
            assert natural_handle.request_stop(request) is False
            assert natural_handle.request_cancel() is False
            natural_order.append("apply")
            from gateway.session import ProjectBatchApplyResult
            return ProjectBatchApplyResult("published")
    natural_handle = worker_module.ProjectRuntimeLiveHandle(start, NaturalRunner())
    natural_closer = worker_module.ProjectRuntimeTerminalCloser(NaturalRuntime(), NaturalBatches())
    natural_result = await natural_closer.resolve_prepared_terminal(
        claim=claim,
        result=runtime_module.CanonicalTurnResult("succeeded", "223e4567-e89b-42d3-a456-426614174000"),
        batch_id="223e4567-e89b-42d3-a456-426614174000",
        runner=natural_handle,
    )
    assert natural_result.outcome == "published"
    assert natural_order == ["quiescent", "control", "cas", "apply"]
    assert natural_handle.request_stop(request) is False

    none_order: list[str] = []

    class NoneRunner:
        def request_cancel(self):
            none_order.append("cancel")
            return True

        async def wait_quiescent(self):
            none_order.append("quiescent")

    class NoneRuntime:
        async def control_for_claim(self, observed_claim):
            none_order.append("control")
            return runtime_module.ClaimControl("stop_requested", 7, 44)

        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            raise AssertionError("batch-less stop must not terminal-CAS")

        async def acknowledge_stopped(self, observed_claim):
            assert observed_claim.lease_expires_at == 44
            none_order.append("ack")
            return object()

    class NoneBatches:
        async def apply_project_batch(self, batch_id):
            raise AssertionError("batch_id=None must not apply State")

    none_handle = worker_module.ProjectRuntimeLiveHandle(start, NoneRunner())
    assert await worker_module.ProjectRuntimeTerminalCloser(
        NoneRuntime(),
        NoneBatches(),
    ).acknowledge_stop(
        claim=claim,
        runner=none_handle,
        batch_id=None,
    ) is None
    assert none_order == ["control", "cancel", "quiescent", "ack"]
    assert none_handle.request_stop(request) is True

    # resolve_prepared_terminal owns the initial natural-quiescence barrier.
    # If its sole control observation is already stop_requested, it enters the
    # shared stop closer without any terminal CAS or second authority read.
    initial_stop_order: list[str] = []

    class InitialStopRunner:
        def request_cancel(self):
            initial_stop_order.append("cancel")
            return True

        async def wait_quiescent(self):
            initial_stop_order.append("quiescent")

    class InitialStopRuntime:
        def __init__(self):
            self.reads = 0

        async def control_for_claim(self, observed_claim):
            self.reads += 1
            initial_stop_order.append("control")
            return runtime_module.ClaimControl("stop_requested", 7, 45)

        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            raise AssertionError("initial stop must not terminal-CAS")

        async def acknowledge_stopped(self, observed_claim):
            assert observed_claim.lease_expires_at == 45
            initial_stop_order.append("ack")
            return object()

    class InitialStopBatches:
        async def apply_project_batch(self, batch_id):
            from gateway.session import ProjectBatchApplyResult

            initial_stop_order.append("apply")
            return ProjectBatchApplyResult("discarded")

    initial_stop_runtime = InitialStopRuntime()
    initial_stop_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        InitialStopRunner(),
    )
    initial_stop_result = await worker_module.ProjectRuntimeTerminalCloser(
        initial_stop_runtime,
        InitialStopBatches(),
    ).resolve_prepared_terminal(
        claim=claim,
        result=runtime_module.CanonicalTurnResult(
            "succeeded",
            "293e4567-e89b-42d3-a456-426614174000",
        ),
        batch_id="293e4567-e89b-42d3-a456-426614174000",
        runner=initial_stop_handle,
    )
    assert initial_stop_result.outcome == "discarded"
    assert initial_stop_runtime.reads == 1
    assert initial_stop_order == [
        "quiescent",
        "control",
        "cancel",
        "quiescent",
        "ack",
        "apply",
    ]
    assert initial_stop_handle.request_stop(request) is True

    # Initial authority failures and non-STALE terminal-CAS failures
    # propagate unchanged.  Neither class of failure is a retry or apply
    # permission.
    initial_error_order: list[str] = []
    initial_error = RuntimeError("initial control failed")

    class InitialErrorRunner:
        def request_cancel(self):
            initial_error_order.append("cancel")
            return True

        async def wait_quiescent(self):
            initial_error_order.append("quiescent")

    class InitialErrorRuntime:
        async def control_for_claim(self, observed_claim):
            initial_error_order.append("control")
            raise initial_error

        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            initial_error_order.append("cas")
            raise AssertionError("initial read failure must not terminal-CAS")

        async def acknowledge_stopped(self, observed_claim):
            initial_error_order.append("ack")
            raise AssertionError("initial read failure must not acknowledge")

    class InitialErrorBatches:
        async def apply_project_batch(self, batch_id):
            initial_error_order.append("apply")
            raise AssertionError("initial read failure must not apply")

    initial_error_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        InitialErrorRunner(),
    )
    with pytest.raises(RuntimeError) as raised_initial_error:
        await worker_module.ProjectRuntimeTerminalCloser(
            InitialErrorRuntime(),
            InitialErrorBatches(),
        ).resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "succeeded",
                "303e4567-e89b-42d3-a456-426614174000",
            ),
            batch_id="303e4567-e89b-42d3-a456-426614174000",
            runner=initial_error_handle,
        )
    assert raised_initial_error.value is initial_error
    assert initial_error_order == ["quiescent", "control"]

    cas_error_order: list[str] = []
    cas_error = runtime_module.ProjectRuntimeError(
        runtime_module.RuntimeErrorCode.INVALID_ARGUMENT
    )

    class CasErrorRunner:
        def request_cancel(self):
            cas_error_order.append("cancel")
            return True

        async def wait_quiescent(self):
            cas_error_order.append("quiescent")

    class CasErrorRuntime:
        def __init__(self):
            self.reads = 0
            self.cas_calls = 0

        async def control_for_claim(self, observed_claim):
            self.reads += 1
            cas_error_order.append("control")
            return runtime_module.ClaimControl("running", 1, 46)

        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            self.cas_calls += 1
            cas_error_order.append("cas")
            raise cas_error

        async def acknowledge_stopped(self, observed_claim):
            cas_error_order.append("ack")
            raise AssertionError("non-STALE CAS failure must not acknowledge")

    class CasErrorBatches:
        async def apply_project_batch(self, batch_id):
            cas_error_order.append("apply")
            raise AssertionError("non-STALE CAS failure must not apply")

    cas_error_runtime = CasErrorRuntime()
    cas_error_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        CasErrorRunner(),
    )
    with pytest.raises(runtime_module.ProjectRuntimeError) as raised_cas_error:
        await worker_module.ProjectRuntimeTerminalCloser(
            cas_error_runtime,
            CasErrorBatches(),
        ).resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "failed",
                "313e4567-e89b-42d3-a456-426614174000",
            ),
            batch_id="313e4567-e89b-42d3-a456-426614174000",
            runner=cas_error_handle,
        )
    assert raised_cas_error.value is cas_error
    assert (cas_error_runtime.reads, cas_error_runtime.cas_calls) == (1, 1)
    assert cas_error_order == ["quiescent", "control", "cas"]

    # The shared stopped closer forwards every durable settlement result and
    # propagates ack/apply failures without repeating either boundary.
    from gateway.session import ProjectBatchApplyResult as BatchApplyResult

    # Terminal success has the same once-only ownership requirement as stop
    # closure.  The second caller enters while the first is blocked in its
    # control read; it must join that owner task rather than rely on the
    # runtime CAS or State apply being idempotent after a duplicate call.
    async def run_concurrent_terminal_owners(*, fail_apply: bool):
        terminal_order: list[str] = []
        terminal_control_entered = asyncio.Event()
        release_terminal_control = asyncio.Event()
        terminal_joiner_invoked = asyncio.Event()

        class TerminalOwnerRunner:
            def request_cancel(self):
                raise AssertionError("terminal owner must not cancel")

            async def wait_quiescent(self):
                terminal_order.append("quiescent")

        class TerminalOwnerRuntime:
            def __init__(self):
                self.reads = 0
                self.cas_calls = 0

            async def control_for_claim(self, observed_claim):
                assert observed_claim == claim
                self.reads += 1
                if self.reads == 1:
                    terminal_control_entered.set()
                await release_terminal_control.wait()
                return runtime_module.ClaimControl("running", 21, 101)

            async def commit_turn_with_task7_batch(
                self,
                observed_claim,
                observed_result,
                *,
                transcript_batch_id,
            ):
                assert observed_claim.lease_expires_at == 101
                assert observed_result.result_id == transcript_batch_id
                self.cas_calls += 1
                # This deliberately models the concrete runtime's idempotent
                # terminal-CAS replay; the closer must still call it once.
                return object()

            async def acknowledge_stopped(self, observed_claim):
                raise AssertionError("terminal owner must not acknowledge stop")

        class TerminalOwnerBatches:
            def __init__(self):
                self.applies = 0

            async def apply_project_batch(self, batch_id):
                assert batch_id == "383e4567-e89b-42d3-a456-426614174000"
                self.applies += 1
                terminal_order.append("apply")
                if fail_apply:
                    raise RuntimeError(
                        f"terminal apply failed {self.applies}"
                    )
                return BatchApplyResult("published")

        owner_runner = TerminalOwnerRunner()
        owner_runtime = TerminalOwnerRuntime()
        owner_batches = TerminalOwnerBatches()
        owner_handle = worker_module.ProjectRuntimeLiveHandle(
            start,
            owner_runner,
        )
        owner_closer = worker_module.ProjectRuntimeTerminalCloser(
            owner_runtime,
            owner_batches,
        )
        terminal_result = runtime_module.CanonicalTurnResult(
            "succeeded",
            "383e4567-e89b-42d3-a456-426614174000",
        )
        first_owner = asyncio.create_task(
            owner_closer.resolve_prepared_terminal(
                claim=claim,
                result=terminal_result,
                batch_id="383e4567-e89b-42d3-a456-426614174000",
                runner=owner_handle,
            )
        )
        await terminal_control_entered.wait()

        async def invoke_terminal_joiner():
            terminal_joiner_invoked.set()
            return await owner_closer.resolve_prepared_terminal(
                claim=claim,
                result=terminal_result,
                batch_id="383e4567-e89b-42d3-a456-426614174000",
                runner=owner_handle,
            )

        second_owner = asyncio.create_task(invoke_terminal_joiner())
        await terminal_joiner_invoked.wait()
        assert owner_batches.applies == 0
        release_terminal_control.set()
        observed = await asyncio.gather(
            first_owner,
            second_owner,
            return_exceptions=True,
        )
        return (
            observed,
            owner_runtime,
            owner_batches,
            terminal_order,
        )

    (
        terminal_owner_results,
        terminal_owner_runtime,
        terminal_owner_batches,
        terminal_owner_order,
    ) = await run_concurrent_terminal_owners(fail_apply=False)
    (
        terminal_owner_errors,
        terminal_error_runtime,
        terminal_error_batches,
        terminal_error_order,
    ) = await run_concurrent_terminal_owners(fail_apply=True)

    # Python 3.11 does not expose eager_task_factory, so this test substitutes
    # its scheduling boundary with the equivalent deterministic first step.
    # A corrected dispatcher can gate the worker body until registration; the
    # assertion remains solely on public notify_stop behavior from that body.
    import gateway.project_runtime_dispatcher as dispatcher_module

    eager_stop_results: list[bool] = []
    eager_worker_entered = asyncio.Event()

    class EagerRuntime:
        def __init__(self):
            self.control_calls: list[object] = []

        def control_for_claim(self, observed_claim):
            self.control_calls.append(observed_claim)
            return runtime_module.ClaimControl("stop_requested", 31, 111)

    class EagerWorker:
        def __init__(self):
            self.requests: list[object] = []

        async def run_start(self, observed_start):
            eager_stop_results.append(
                eager_dispatcher.notify_stop(
                    observed_start.claim.project_id,
                    observed_start.claim.turn_id,
                )
            )
            eager_worker_entered.set()

        def request_stop(self, stop_request):
            self.requests.append(stop_request)
            return True

    eager_runtime = EagerRuntime()
    eager_worker = EagerWorker()
    eager_dispatcher = dispatcher_module.ProjectRuntimeDispatcher(
        eager_runtime,
        object(),
        eager_worker,
        worker_cap=1,
    )
    eager_loop = asyncio.get_running_loop()
    previous_task_factory = eager_loop.get_task_factory()

    def create_task_eagerly(loop, coro, *, context=None):
        assert loop is eager_loop
        try:
            yielded = coro.send(None)
        except StopIteration as completed:
            task = loop.create_future()
            task.set_result(completed.value)
            return task
        if isinstance(yielded, asyncio.Future):
            yielded._asyncio_future_blocking = False
        if context is None:
            return asyncio.Task(coro, loop=loop)
        return asyncio.Task(
            coro,
            loop=loop,
            context=context,
        )

    try:
        eager_loop.set_task_factory(create_task_eagerly)
        eager_dispatcher._reserve_and_start(start)
    finally:
        eager_loop.set_task_factory(previous_task_factory)
    eager_tasks = tuple(eager_dispatcher._live_worker_tasks)
    try:
        await eager_worker_entered.wait()
        await asyncio.gather(*eager_tasks, return_exceptions=False)
    finally:
        await asyncio.gather(*eager_tasks, return_exceptions=True)

    # All three probes run before the first assertion so each gap is exercised
    # against the current implementation in this single unparameterized node.
    assert raising_runner.cancel_calls == 1, (
        "cancel and stop version must latch before invoking the hook"
    )
    assert raising_runner.reentrant_results == [False, False, True]
    assert raising_same_version_after_error is True
    assert raising_different_version_after_error is False
    assert (
        terminal_owner_runtime.reads,
        terminal_owner_runtime.cas_calls,
        terminal_owner_batches.applies,
    ) == (1, 1, 1)
    assert (
        terminal_error_runtime.reads,
        terminal_error_runtime.cas_calls,
        terminal_error_batches.applies,
    ) == (1, 1, 1)
    assert [type(observed) for observed in terminal_owner_results] == [
        BatchApplyResult,
        BatchApplyResult,
    ]
    assert [observed.outcome for observed in terminal_owner_results] == [
        "published",
        "published",
    ]
    assert [type(observed) for observed in terminal_owner_errors] == [
        RuntimeError,
        RuntimeError,
    ]
    assert [str(observed) for observed in terminal_owner_errors] == [
        "terminal apply failed 1",
        "terminal apply failed 1",
    ]
    assert (
        terminal_owner_order.index("quiescent")
        < terminal_owner_order.index("apply")
    )
    assert (
        terminal_error_order.index("quiescent")
        < terminal_error_order.index("apply")
    )
    assert eager_stop_results == [True]
    assert eager_runtime.control_calls == [claim]
    assert eager_worker.requests == [
        worker_module.StopRequest(
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            attempt_id=claim.attempt_id,
            worker_id=claim.worker_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            canonical_session_id=claim.canonical_session_id,
            control_version=31,
        )
    ]

    def stop_probe(
        *,
        apply_result=None,
        ack_error=None,
        apply_error=None,
    ):
        probe_order: list[str] = []

        class ProbeRunner:
            def request_cancel(self):
                probe_order.append("cancel")
                return True

            async def wait_quiescent(self):
                probe_order.append("quiescent")

        class ProbeRuntime:
            def __init__(self):
                self.reads = 0
                self.acks = 0

            async def control_for_claim(self, observed_claim):
                self.reads += 1
                probe_order.append("control")
                return runtime_module.ClaimControl("stop_requested", 1, 47)

            async def commit_turn_with_task7_batch(self, *args, **kwargs):
                raise AssertionError("stop winner must not terminal-CAS")

            async def acknowledge_stopped(self, observed_claim):
                self.acks += 1
                probe_order.append("ack")
                if ack_error is not None:
                    raise ack_error
                return object()

        class ProbeBatches:
            def __init__(self):
                self.applies = 0

            async def apply_project_batch(self, batch_id):
                self.applies += 1
                probe_order.append("apply")
                if apply_error is not None:
                    raise apply_error
                return apply_result

        probe_runtime = ProbeRuntime()
        probe_batches = ProbeBatches()
        probe_handle = worker_module.ProjectRuntimeLiveHandle(
            start,
            ProbeRunner(),
        )
        probe_closer = worker_module.ProjectRuntimeTerminalCloser(
            probe_runtime,
            probe_batches,
        )
        return (
            probe_closer,
            probe_handle,
            probe_order,
            probe_runtime,
            probe_batches,
        )

    ack_error = RuntimeError("stop acknowledgement failed")
    (
        ack_error_closer,
        ack_error_handle,
        ack_error_order,
        ack_error_runtime,
        ack_error_batches,
    ) = stop_probe(ack_error=ack_error)
    with pytest.raises(RuntimeError) as raised_ack_error:
        await ack_error_closer.resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "succeeded",
                "413e4567-e89b-42d3-a456-426614174000",
            ),
            batch_id="413e4567-e89b-42d3-a456-426614174000",
            runner=ack_error_handle,
        )
    assert raised_ack_error.value is ack_error
    assert (ack_error_runtime.reads, ack_error_runtime.acks) == (1, 1)
    assert ack_error_batches.applies == 0
    assert ack_error_order == [
        "quiescent",
        "control",
        "cancel",
        "quiescent",
        "ack",
    ]

    apply_error = RuntimeError("State apply failed")
    (
        apply_error_closer,
        apply_error_handle,
        apply_error_order,
        apply_error_runtime,
        apply_error_batches,
    ) = stop_probe(apply_error=apply_error)
    with pytest.raises(RuntimeError) as raised_apply_error:
        await apply_error_closer.resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "failed",
                "513e4567-e89b-42d3-a456-426614174000",
            ),
            batch_id="513e4567-e89b-42d3-a456-426614174000",
            runner=apply_error_handle,
        )
    assert raised_apply_error.value is apply_error
    assert (apply_error_runtime.reads, apply_error_runtime.acks) == (1, 1)
    assert apply_error_batches.applies == 1
    assert apply_error_order == [
        "quiescent",
        "control",
        "cancel",
        "quiescent",
        "ack",
        "apply",
    ]

    for outcome in (
        "wait",
        "state_conflict",
        "settlement_pending",
        "published",
    ):
        expected_apply = BatchApplyResult(outcome)
        (
            outcome_closer,
            outcome_handle,
            outcome_order,
            outcome_runtime,
            outcome_batches,
        ) = stop_probe(apply_result=expected_apply)
        observed_apply = await outcome_closer.resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "succeeded",
                "613e4567-e89b-42d3-a456-426614174000",
            ),
            batch_id="613e4567-e89b-42d3-a456-426614174000",
            runner=outcome_handle,
        )
        assert observed_apply is expected_apply
        assert (outcome_runtime.reads, outcome_runtime.acks) == (1, 1)
        assert outcome_batches.applies == 1
        assert outcome_order == [
            "quiescent",
            "control",
            "cancel",
            "quiescent",
            "ack",
            "apply",
        ]

    # A stale terminal CAS has exactly one recovery read.  A durable stop on
    # that reread rebases the horizon once, acknowledges once, then applies;
    # it never retries terminal CAS.
    stale_order: list[str] = []
    class StaleRuntime:
        def __init__(self):
            self.reads = 0
            self.cas_calls = 0

        async def control_for_claim(self, observed_claim):
            self.reads += 1
            if self.reads == 1:
                assert observed_claim == claim
            else:
                assert observed_claim.lease_expires_at == 51
            return runtime_module.ClaimControl(
                "running" if self.reads == 1 else "stop_requested",
                self.reads, 50 + self.reads,
            )

        async def commit_turn_with_task7_batch(
            self,
            observed_claim,
            *args,
            **kwargs,
        ):
            assert observed_claim.lease_expires_at == 51
            self.cas_calls += 1
            raise runtime_module.ProjectRuntimeError(
                runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
            )

        async def acknowledge_stopped(self, observed_claim):
            assert observed_claim.lease_expires_at == 52
            stale_order.append("ack")
            return object()
    class StaleBatches:
        async def apply_project_batch(self, batch_id):
            from gateway.session import ProjectBatchApplyResult
            stale_order.append("apply")
            return ProjectBatchApplyResult("discarded")
    stale_runtime = StaleRuntime()
    stale_handle = worker_module.ProjectRuntimeLiveHandle(start, NaturalRunner())
    stale_result = await worker_module.ProjectRuntimeTerminalCloser(stale_runtime, StaleBatches()).resolve_prepared_terminal(
        claim=claim,
        result=runtime_module.CanonicalTurnResult("succeeded", "323e4567-e89b-42d3-a456-426614174000"),
        batch_id="323e4567-e89b-42d3-a456-426614174000",
        runner=stale_handle,
    )
    assert stale_result.outcome == "discarded"
    assert stale_runtime.cas_calls == 1 and stale_runtime.reads == 2
    assert stale_order == ["ack", "apply"]
    assert stale_handle.request_stop(request) is True

    # A direct durable-stop close can race the sole terminal CAS after it has
    # observed running but before its stale-CAS reread observes stop.  Both
    # callers must join one in-flight cancel/quiesce/ack/apply task, including
    # its exact result or exception object.
    async def run_concurrent_stop_closers(*, fail_apply: bool):
        race_order: list[str] = []
        cas_entered = asyncio.Event()
        release_stale_cas = asyncio.Event()
        stop_waiting = asyncio.Event()
        reread_finished = asyncio.Event()
        release_quiescence = asyncio.Event()

        class RaceRunner:
            def __init__(self):
                self.waits = 0

            def request_cancel(self):
                race_order.append("cancel")
                return True

            async def wait_quiescent(self):
                self.waits += 1
                if self.waits == 1:
                    race_order.append("natural-quiescent")
                    return
                race_order.append("stop-waiting")
                stop_waiting.set()
                await release_quiescence.wait()
                race_order.append("stop-quiescent")

        class RaceRuntime:
            def __init__(self):
                self.reads = 0
                self.cas_calls = 0
                self.acks = 0
                self.ack_horizons: list[int] = []

            async def control_for_claim(self, observed_claim):
                self.reads += 1
                if self.reads == 1:
                    assert observed_claim == claim
                    race_order.append("control-running")
                    return runtime_module.ClaimControl("running", 1, 91)
                if self.reads == 2:
                    assert observed_claim == claim
                    race_order.append("control-direct-stop")
                    return runtime_module.ClaimControl(
                        "stop_requested",
                        2,
                        92,
                    )
                if self.reads == 3:
                    assert observed_claim.lease_expires_at == 91
                    race_order.append("control-reread-stop")
                    reread_finished.set()
                    return runtime_module.ClaimControl(
                        "stop_requested",
                        3,
                        93,
                    )
                raise AssertionError("concurrent close must not reread again")

            async def commit_turn_with_task7_batch(
                self,
                observed_claim,
                result,
                *,
                transcript_batch_id,
            ):
                assert observed_claim.lease_expires_at == 91
                assert result.result_id == transcript_batch_id
                self.cas_calls += 1
                race_order.append("cas")
                cas_entered.set()
                await release_stale_cas.wait()
                raise runtime_module.ProjectRuntimeError(
                    runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
                )

            async def acknowledge_stopped(self, observed_claim):
                self.acks += 1
                self.ack_horizons.append(observed_claim.lease_expires_at)
                race_order.append("ack")
                return object()

        class RaceBatches:
            def __init__(self):
                self.applies = 0

            async def apply_project_batch(self, batch_id):
                assert batch_id == "373e4567-e89b-42d3-a456-426614174000"
                self.applies += 1
                race_order.append("apply")
                if fail_apply:
                    raise RuntimeError(f"race apply failed {self.applies}")
                return BatchApplyResult("discarded")

        race_runner = RaceRunner()
        race_runtime = RaceRuntime()
        race_batches = RaceBatches()
        race_handle = worker_module.ProjectRuntimeLiveHandle(
            start,
            race_runner,
        )
        race_closer = worker_module.ProjectRuntimeTerminalCloser(
            race_runtime,
            race_batches,
        )
        terminal_close = asyncio.create_task(
            race_closer.resolve_prepared_terminal(
                claim=claim,
                result=runtime_module.CanonicalTurnResult(
                    "succeeded",
                    "373e4567-e89b-42d3-a456-426614174000",
                ),
                batch_id="373e4567-e89b-42d3-a456-426614174000",
                runner=race_handle,
            )
        )
        await cas_entered.wait()
        direct_close = asyncio.create_task(
            race_closer.acknowledge_stop(
                claim=claim,
                runner=race_handle,
                batch_id="373e4567-e89b-42d3-a456-426614174000",
            )
        )
        await stop_waiting.wait()
        assert (race_runtime.acks, race_batches.applies) == (0, 0)
        release_stale_cas.set()
        await reread_finished.wait()
        release_quiescence.set()
        observed = await asyncio.gather(
            direct_close,
            terminal_close,
            return_exceptions=True,
        )
        return (
            observed,
            race_order,
            race_runtime,
            race_batches,
        )

    (
        concurrent_results,
        concurrent_order,
        concurrent_runtime,
        concurrent_batches,
    ) = await run_concurrent_stop_closers(fail_apply=False)
    (
        concurrent_errors,
        error_order,
        error_runtime,
        error_batches,
    ) = await run_concurrent_stop_closers(fail_apply=True)

    assert (concurrent_runtime.acks, concurrent_batches.applies) == (1, 1), (
        "concurrent direct and stale-CAS stop paths must share one "
        "acknowledgement/apply sequence"
    )
    assert (error_runtime.acks, error_batches.applies) == (1, 1)
    assert [type(observed) for observed in concurrent_results] == [
        BatchApplyResult,
        BatchApplyResult,
    ]
    assert [observed.outcome for observed in concurrent_results] == [
        "discarded",
        "discarded",
    ]
    assert [type(observed) for observed in concurrent_errors] == [
        RuntimeError,
        RuntimeError,
    ]
    assert [str(observed) for observed in concurrent_errors] == [
        "race apply failed 1",
        "race apply failed 1",
    ]
    assert len(concurrent_runtime.ack_horizons) == 1
    assert concurrent_runtime.ack_horizons[0] in {92, 93}
    assert len(error_runtime.ack_horizons) == 1
    assert error_runtime.ack_horizons[0] in {92, 93}
    assert concurrent_order.count("cancel") == error_order.count("cancel") == 1
    assert (
        concurrent_order.index("stop-quiescent")
        < concurrent_order.index("ack")
        < concurrent_order.index("apply")
    )
    assert (
        error_order.index("stop-quiescent")
        < error_order.index("ack")
        < error_order.index("apply")
    )

    # Exact initial stale without a same-handle stop owner is not generic
    # permission to apply the prepared batch.
    ownerless_order: list[str] = []
    ownerless_batch_id = "383e4567-e89b-42d3-a456-426614174001"

    class OwnerlessStaleRunner:
        def __init__(self):
            self.cancels = 0

        def request_cancel(self):
            self.cancels += 1
            ownerless_order.append("cancel")
            return True

        async def wait_quiescent(self):
            ownerless_order.append("natural-proof")

    class OwnerlessStaleRuntime:
        def __init__(self):
            self.reads = 0
            self.cas_calls = 0
            self.acks = 0
            self.initial_stale = runtime_module.ProjectRuntimeError(
                runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM,
                project_id=claim.project_id,
                turn_id=claim.turn_id,
                current_control_version=481,
            )

        async def control_for_claim(self, observed_claim):
            assert observed_claim == claim
            self.reads += 1
            ownerless_order.append("initial-stale")
            raise self.initial_stale

        async def commit_turn_with_task7_batch(
            self,
            observed_claim,
            *args,
            **kwargs,
        ):
            self.cas_calls += 1
            raise AssertionError("ownerless initial stale must not attempt CAS")

        async def acknowledge_stopped(self, observed_claim):
            self.acks += 1
            raise AssertionError(
                "ownerless initial stale must not acknowledge stop"
            )

    class OwnerlessStaleBatches:
        def __init__(self):
            self.applies = 0

        async def apply_project_batch(self, batch_id):
            assert batch_id == ownerless_batch_id
            self.applies += 1
            return BatchApplyResult("discarded")

    ownerless_runner = OwnerlessStaleRunner()
    ownerless_runtime = OwnerlessStaleRuntime()
    ownerless_batches = OwnerlessStaleBatches()
    ownerless_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        ownerless_runner,
    )
    ownerless_closer = worker_module.ProjectRuntimeTerminalCloser(
        ownerless_runtime,
        ownerless_batches,
    )
    ownerless_observed = (
        await asyncio.gather(
            ownerless_closer.resolve_prepared_terminal(
                claim=claim,
                result=runtime_module.CanonicalTurnResult(
                    "succeeded",
                    ownerless_batch_id,
                ),
                batch_id=ownerless_batch_id,
                runner=ownerless_handle,
            ),
            return_exceptions=True,
        )
    )[0]
    ownerless_calls = (
        ownerless_runtime.reads,
        ownerless_runtime.cas_calls,
        ownerless_runtime.acks,
        ownerless_runner.cancels,
        ownerless_batches.applies,
    )
    ownerless_replay = (
        await asyncio.gather(
            ownerless_closer.resolve_prepared_terminal(
                claim=claim,
                result=runtime_module.CanonicalTurnResult(
                    "succeeded",
                    ownerless_batch_id,
                ),
                batch_id=ownerless_batch_id,
                runner=ownerless_handle,
            ),
            return_exceptions=True,
        )
    )[0]
    ownerless_replay_calls = (
        ownerless_runtime.reads,
        ownerless_runtime.cas_calls,
        ownerless_runtime.acks,
        ownerless_runner.cancels,
        ownerless_batches.applies,
    )

    assert [type(ownerless_observed), type(ownerless_replay)] == [
        runtime_module.ProjectRuntimeError,
        runtime_module.ProjectRuntimeError,
    ]
    assert [
        ownerless_observed.code,
        ownerless_replay.code,
    ] == [
        runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM,
        runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM,
    ]
    assert [
        (
            observed.project_id,
            observed.turn_id,
            observed.current_control_version,
            str(observed),
        )
        for observed in (ownerless_observed, ownerless_replay)
    ] == [
        (
            claim.project_id,
            claim.turn_id,
            481,
            "stale_turn_claim",
        ),
        (
            claim.project_id,
            claim.turn_id,
            481,
            "stale_turn_claim",
        ),
    ]
    assert ownerless_calls == (1, 0, 0, 0, 0)
    assert ownerless_replay_calls == ownerless_calls
    assert (
        ownerless_order.index("natural-proof")
        < ownerless_order.index("initial-stale")
    )

    # A same-handle stop owner alone is insufficient for handoff.  A
    # non-domain failure from the resolver's mandatory initial read remains
    # the terminal outcome and never opens the bound batch apply slot.
    non_stale_order: list[str] = []
    non_stale_natural_waiting = asyncio.Event()
    release_non_stale_natural = asyncio.Event()
    non_stale_batch_id = "383e4567-e89b-42d3-a456-426614174003"

    class NonStaleHandoffRunner:
        def __init__(self):
            self.cancels = 0

        def request_cancel(self):
            self.cancels += 1
            non_stale_order.append("cancel")
            return True

        async def wait_quiescent(self):
            if not non_stale_natural_waiting.is_set():
                non_stale_order.append("terminal-natural-wait")
                non_stale_natural_waiting.set()
                await release_non_stale_natural.wait()
                non_stale_order.append("terminal-natural-proof")
                return
            non_stale_order.append("stop-quiescent")

    class NonStaleHandoffRuntime:
        def __init__(self):
            self.reads = 0
            self.cas_calls = 0
            self.acks = 0
            self.stop_acknowledged = False

        async def control_for_claim(self, observed_claim):
            assert observed_claim == claim
            self.reads += 1
            if not release_non_stale_natural.is_set():
                non_stale_order.append("direct-stop-control")
                return runtime_module.ClaimControl(
                    "stop_requested",
                    6,
                    96,
                )
            assert self.stop_acknowledged
            non_stale_order.append("terminal-initial-error")
            raise RuntimeError("terminal initial control read failed")

        async def commit_turn_with_task7_batch(
            self,
            observed_claim,
            *args,
            **kwargs,
        ):
            self.cas_calls += 1
            raise AssertionError("non-stale initial error must not attempt CAS")

        async def acknowledge_stopped(self, observed_claim):
            self.acks += 1
            self.stop_acknowledged = True
            non_stale_order.append("ack")
            return object()

    class NonStaleHandoffBatches:
        def __init__(self):
            self.applies = 0

        async def apply_project_batch(self, batch_id):
            assert batch_id == non_stale_batch_id
            self.applies += 1
            return BatchApplyResult("discarded")

    non_stale_runner = NonStaleHandoffRunner()
    non_stale_runtime = NonStaleHandoffRuntime()
    non_stale_batches = NonStaleHandoffBatches()
    non_stale_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        non_stale_runner,
    )
    non_stale_closer = worker_module.ProjectRuntimeTerminalCloser(
        non_stale_runtime,
        non_stale_batches,
    )
    non_stale_terminal = asyncio.create_task(
        non_stale_closer.resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "succeeded",
                non_stale_batch_id,
            ),
            batch_id=non_stale_batch_id,
            runner=non_stale_handle,
        )
    )
    await non_stale_natural_waiting.wait()
    assert (
        await non_stale_closer.acknowledge_stop(
            claim=claim,
            runner=non_stale_handle,
            batch_id=None,
        )
        is None
    )
    assert (
        non_stale_runtime.reads,
        non_stale_runtime.acks,
        non_stale_runner.cancels,
        non_stale_batches.applies,
    ) == (1, 1, 1, 0)

    non_stale_order.append("terminal-natural-release")
    release_non_stale_natural.set()
    non_stale_observed = (
        await asyncio.gather(
            non_stale_terminal,
            return_exceptions=True,
        )
    )[0]
    non_stale_calls = (
        non_stale_runtime.reads,
        non_stale_runtime.cas_calls,
        non_stale_runtime.acks,
        non_stale_runner.cancels,
        non_stale_batches.applies,
    )
    non_stale_replay = (
        await asyncio.gather(
            non_stale_closer.resolve_prepared_terminal(
                claim=claim,
                result=runtime_module.CanonicalTurnResult(
                    "succeeded",
                    non_stale_batch_id,
                ),
                batch_id=non_stale_batch_id,
                runner=non_stale_handle,
            ),
            return_exceptions=True,
        )
    )[0]
    non_stale_replay_calls = (
        non_stale_runtime.reads,
        non_stale_runtime.cas_calls,
        non_stale_runtime.acks,
        non_stale_runner.cancels,
        non_stale_batches.applies,
    )

    assert [type(non_stale_observed), type(non_stale_replay)] == [
        RuntimeError,
        RuntimeError,
    ]
    assert [str(non_stale_observed), str(non_stale_replay)] == [
        "terminal initial control read failed",
        "terminal initial control read failed",
    ]
    assert non_stale_calls == (2, 0, 1, 1, 0)
    assert non_stale_replay_calls == non_stale_calls
    assert (
        non_stale_order.index("terminal-natural-proof")
        < non_stale_order.index("terminal-initial-error")
    )
    assert (
        non_stale_order.index("direct-stop-control")
        < non_stale_order.index("cancel")
        < non_stale_order.index("stop-quiescent")
        < non_stale_order.index("ack")
        < non_stale_order.index("terminal-natural-release")
    )

    # The narrow handoff also joins a cached stop-ack failure.  That failure
    # denies the already-bound batch apply instead of turning initial stale
    # into generic apply authority.
    failed_handoff_order: list[str] = []
    failed_natural_waiting = asyncio.Event()
    release_failed_natural = asyncio.Event()
    failed_handoff_batch_id = "383e4567-e89b-42d3-a456-426614174002"

    class FailedHandoffRunner:
        def __init__(self):
            self.cancels = 0

        def request_cancel(self):
            self.cancels += 1
            failed_handoff_order.append("cancel")
            return True

        async def wait_quiescent(self):
            if not failed_natural_waiting.is_set():
                failed_handoff_order.append("terminal-natural-wait")
                failed_natural_waiting.set()
                await release_failed_natural.wait()
                failed_handoff_order.append("terminal-natural-proof")
                return
            failed_handoff_order.append("stop-quiescent")

    class FailedHandoffRuntime:
        def __init__(self):
            self.reads = 0
            self.cas_calls = 0
            self.acks = 0
            self.stop_acknowledged = False

        async def control_for_claim(self, observed_claim):
            assert observed_claim == claim
            self.reads += 1
            if not release_failed_natural.is_set():
                failed_handoff_order.append("direct-stop-control")
                return runtime_module.ClaimControl(
                    "stop_requested",
                    5,
                    95,
                )
            assert not self.stop_acknowledged
            failed_handoff_order.append("terminal-initial-stale")
            raise runtime_module.ProjectRuntimeError(
                runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
            )

        async def commit_turn_with_task7_batch(
            self,
            observed_claim,
            *args,
            **kwargs,
        ):
            self.cas_calls += 1
            raise AssertionError("failed stop handoff must not attempt CAS")

        async def acknowledge_stopped(self, observed_claim):
            self.acks += 1
            failed_handoff_order.append("ack-failed")
            raise RuntimeError("handoff stop acknowledgement failed")

    class FailedHandoffBatches:
        def __init__(self):
            self.applies = 0

        async def apply_project_batch(self, batch_id):
            assert batch_id == failed_handoff_batch_id
            self.applies += 1
            return BatchApplyResult("discarded")

    failed_handoff_runner = FailedHandoffRunner()
    failed_handoff_runtime = FailedHandoffRuntime()
    failed_handoff_batches = FailedHandoffBatches()
    failed_handoff_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        failed_handoff_runner,
    )
    failed_handoff_closer = worker_module.ProjectRuntimeTerminalCloser(
        failed_handoff_runtime,
        failed_handoff_batches,
    )
    failed_handoff_terminal = asyncio.create_task(
        failed_handoff_closer.resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "succeeded",
                failed_handoff_batch_id,
            ),
            batch_id=failed_handoff_batch_id,
            runner=failed_handoff_handle,
        )
    )
    await failed_natural_waiting.wait()
    failed_direct = (
        await asyncio.gather(
            failed_handoff_closer.acknowledge_stop(
                claim=claim,
                runner=failed_handoff_handle,
                batch_id=None,
            ),
            return_exceptions=True,
        )
    )[0]
    assert type(failed_direct) is RuntimeError
    assert str(failed_direct) == "handoff stop acknowledgement failed"
    assert (
        failed_handoff_runtime.reads,
        failed_handoff_runtime.acks,
        failed_handoff_runner.cancels,
        failed_handoff_batches.applies,
    ) == (1, 1, 1, 0)

    failed_handoff_order.append("terminal-natural-release")
    release_failed_natural.set()
    failed_terminal_observed = (
        await asyncio.gather(
            failed_handoff_terminal,
            return_exceptions=True,
        )
    )[0]
    failed_calls_after_terminal = (
        failed_handoff_runtime.reads,
        failed_handoff_runtime.cas_calls,
        failed_handoff_runtime.acks,
        failed_handoff_runner.cancels,
        failed_handoff_batches.applies,
    )
    failed_replay_observed = (
        await asyncio.gather(
            failed_handoff_closer.resolve_prepared_terminal(
                claim=claim,
                result=runtime_module.CanonicalTurnResult(
                    "succeeded",
                    failed_handoff_batch_id,
                ),
                batch_id=failed_handoff_batch_id,
                runner=failed_handoff_handle,
            ),
            return_exceptions=True,
        )
    )[0]
    failed_calls_after_replay = (
        failed_handoff_runtime.reads,
        failed_handoff_runtime.cas_calls,
        failed_handoff_runtime.acks,
        failed_handoff_runner.cancels,
        failed_handoff_batches.applies,
    )

    assert [
        type(failed_direct),
        type(failed_terminal_observed),
        type(failed_replay_observed),
    ] == [RuntimeError, RuntimeError, RuntimeError]
    assert [
        str(failed_direct),
        str(failed_terminal_observed),
        str(failed_replay_observed),
    ] == [
        "handoff stop acknowledgement failed",
        "handoff stop acknowledgement failed",
        "handoff stop acknowledgement failed",
    ]
    assert failed_calls_after_terminal == (2, 0, 1, 1, 0)
    assert failed_calls_after_replay == failed_calls_after_terminal
    assert (
        failed_handoff_order.index("terminal-natural-proof")
        < failed_handoff_order.index("terminal-initial-stale")
    )
    assert (
        failed_handoff_order.index("direct-stop-control")
        < failed_handoff_order.index("cancel")
        < failed_handoff_order.index("stop-quiescent")
        < failed_handoff_order.index("ack-failed")
        < failed_handoff_order.index("terminal-natural-release")
    )

    # A batch-bound terminal resolver can still be in its mandatory natural
    # wait when a batchless direct stop owns and completes the handle's sole
    # stop acknowledgement.  Its later exact initial stale read must hand off
    # to that cached success and apply the already-bound batch once.
    handoff_order: list[str] = []
    terminal_natural_waiting = asyncio.Event()
    release_terminal_natural = asyncio.Event()
    handoff_batch_id = "383e4567-e89b-42d3-a456-426614174000"

    class HandoffRunner:
        def __init__(self):
            self.cancels = 0

        def request_cancel(self):
            self.cancels += 1
            handoff_order.append("cancel")
            return True

        async def wait_quiescent(self):
            if not terminal_natural_waiting.is_set():
                handoff_order.append("terminal-natural-wait")
                terminal_natural_waiting.set()
                await release_terminal_natural.wait()
                handoff_order.append("terminal-natural-proof")
                return
            handoff_order.append("stop-quiescent")

    class HandoffRuntime:
        def __init__(self):
            self.reads = 0
            self.cas_calls = 0
            self.acks = 0
            self.stop_acknowledged = False

        async def control_for_claim(self, observed_claim):
            assert observed_claim == claim
            self.reads += 1
            if not release_terminal_natural.is_set():
                handoff_order.append("direct-stop-control")
                return runtime_module.ClaimControl(
                    "stop_requested",
                    4,
                    94,
                )
            assert self.stop_acknowledged
            handoff_order.append("terminal-initial-stale")
            raise runtime_module.ProjectRuntimeError(
                runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
            )

        async def commit_turn_with_task7_batch(
            self,
            observed_claim,
            *args,
            **kwargs,
        ):
            self.cas_calls += 1
            raise AssertionError("initial-stale handoff must not attempt CAS")

        async def acknowledge_stopped(self, observed_claim):
            self.acks += 1
            self.stop_acknowledged = True
            handoff_order.append("ack")
            return object()

    class HandoffBatches:
        def __init__(self):
            self.applies = 0

        async def apply_project_batch(self, batch_id):
            assert batch_id == handoff_batch_id
            assert release_terminal_natural.is_set()
            assert "terminal-natural-proof" in handoff_order
            self.applies += 1
            handoff_order.append("apply")
            return BatchApplyResult("discarded")

    handoff_runner = HandoffRunner()
    handoff_runtime = HandoffRuntime()
    handoff_batches = HandoffBatches()
    handoff_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        handoff_runner,
    )
    handoff_closer = worker_module.ProjectRuntimeTerminalCloser(
        handoff_runtime,
        handoff_batches,
    )
    handoff_terminal = asyncio.create_task(
        handoff_closer.resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "succeeded",
                handoff_batch_id,
            ),
            batch_id=handoff_batch_id,
            runner=handoff_handle,
        )
    )
    await terminal_natural_waiting.wait()
    assert (handoff_runtime.reads, handoff_batches.applies) == (0, 0)

    handoff_stop = asyncio.create_task(
        handoff_closer.acknowledge_stop(
            claim=claim,
            runner=handoff_handle,
            batch_id=None,
        )
    )
    assert await handoff_stop is None
    assert (
        handoff_runtime.reads,
        handoff_runtime.acks,
        handoff_runner.cancels,
        handoff_batches.applies,
    ) == (1, 1, 1, 0)

    handoff_order.append("terminal-natural-release")
    release_terminal_natural.set()
    terminal_observed = (
        await asyncio.gather(handoff_terminal, return_exceptions=True)
    )[0]
    calls_after_terminal = (
        handoff_runtime.reads,
        handoff_runtime.cas_calls,
        handoff_runtime.acks,
        handoff_runner.cancels,
        handoff_batches.applies,
    )
    replay_observed = (
        await asyncio.gather(
            handoff_closer.resolve_prepared_terminal(
                claim=claim,
                result=runtime_module.CanonicalTurnResult(
                    "succeeded",
                    handoff_batch_id,
                ),
                batch_id=handoff_batch_id,
                runner=handoff_handle,
            ),
            return_exceptions=True,
        )
    )[0]
    calls_after_replay = (
        handoff_runtime.reads,
        handoff_runtime.cas_calls,
        handoff_runtime.acks,
        handoff_runner.cancels,
        handoff_batches.applies,
    )

    assert type(terminal_observed) is BatchApplyResult, (
        "the resolver's exact initial stale read must join the cached "
        f"stop success, got {type(terminal_observed).__name__}: "
        f"{terminal_observed}"
    )
    assert type(replay_observed) is BatchApplyResult
    assert terminal_observed.outcome == replay_observed.outcome == "discarded"
    assert calls_after_terminal == (2, 0, 1, 1, 1)
    assert calls_after_replay == calls_after_terminal
    assert (
        handoff_order.index("terminal-natural-proof")
        < handoff_order.index("terminal-initial-stale")
    )
    assert (
        handoff_order.index("direct-stop-control")
        < handoff_order.index("cancel")
        < handoff_order.index("stop-quiescent")
        < handoff_order.index("ack")
    )
    assert handoff_order.index("ack") < handoff_order.index("apply")
    assert (
        handoff_order.index("terminal-natural-release")
        < handoff_order.index("apply")
    )

    # Every other stale-CAS reread (including a coherent running result) is
    # apply-only: no second CAS and never a speculative stopped acknowledgement.
    fallback_order: list[str] = []
    class FallbackRuntime(StaleRuntime):
        async def control_for_claim(self, observed_claim):
            self.reads += 1
            return runtime_module.ClaimControl("running", self.reads, 70 + self.reads)

        async def commit_turn_with_task7_batch(
            self,
            observed_claim,
            *args,
            **kwargs,
        ):
            assert observed_claim.lease_expires_at == 71
            self.cas_calls += 1
            raise runtime_module.ProjectRuntimeError(
                runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
            )

        async def acknowledge_stopped(self, observed_claim):
            raise AssertionError("non-stop reread must not acknowledge")
    class FallbackBatches:
        async def apply_project_batch(self, batch_id):
            from gateway.session import ProjectBatchApplyResult
            fallback_order.append("apply")
            return ProjectBatchApplyResult("discarded")
    fallback_runtime = FallbackRuntime()
    fallback_handle = worker_module.ProjectRuntimeLiveHandle(
        start,
        NaturalRunner(),
    )
    fallback = await worker_module.ProjectRuntimeTerminalCloser(fallback_runtime, FallbackBatches()).resolve_prepared_terminal(
        claim=claim, result=runtime_module.CanonicalTurnResult("succeeded", "333e4567-e89b-42d3-a456-426614174000"),
        batch_id="333e4567-e89b-42d3-a456-426614174000",
        runner=fallback_handle,
    )
    assert fallback.outcome == "discarded"
    assert fallback_runtime.cas_calls == 1 and fallback_runtime.reads == 2
    assert fallback_order == ["apply"]
    assert fallback_handle.request_stop(request) is True

    # A stale or failed *reread* is also apply-only.  It may not retry the
    # terminal CAS, acknowledge a speculative stop, or tear down the handle.
    async def assert_apply_only_after_reread_failure(read_failure):
        apply_only_order: list[str] = []

        class RereadFailureRuntime:
            def __init__(self):
                self.reads = 0
                self.cas_calls = 0

            async def control_for_claim(self, observed_claim):
                self.reads += 1
                if self.reads == 1:
                    assert observed_claim == claim
                    return runtime_module.ClaimControl("running", 1, 81)
                assert observed_claim.lease_expires_at == 81
                raise read_failure

            async def commit_turn_with_task7_batch(self, observed_claim, *args, **kwargs):
                assert observed_claim.lease_expires_at == 81
                self.cas_calls += 1
                raise runtime_module.ProjectRuntimeError(
                    runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
                )

            async def acknowledge_stopped(self, observed_claim):
                raise AssertionError("failed reread must not acknowledge")

        class RereadFailureBatches:
            async def apply_project_batch(self, batch_id):
                apply_only_order.append("apply")
                from gateway.session import ProjectBatchApplyResult

                return ProjectBatchApplyResult("discarded")

        failed_runtime = RereadFailureRuntime()
        failed_handle = worker_module.ProjectRuntimeLiveHandle(
            start,
            NaturalRunner(),
        )
        resolved = await worker_module.ProjectRuntimeTerminalCloser(
            failed_runtime,
            RereadFailureBatches(),
        ).resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult(
                "succeeded",
                "363e4567-e89b-42d3-a456-426614174000",
            ),
            batch_id="363e4567-e89b-42d3-a456-426614174000",
            runner=failed_handle,
        )
        assert resolved.outcome == "discarded"
        assert (failed_runtime.reads, failed_runtime.cas_calls) == (2, 1)
        assert apply_only_order == ["apply"]
        assert failed_handle.request_stop(request) is True

    await assert_apply_only_after_reread_failure(
        runtime_module.ProjectRuntimeError(
            runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
        )
    )
    await assert_apply_only_after_reread_failure(RuntimeError("reread failed"))

    class MismatchRuntime:
        async def control_for_claim(self, observed_claim):
            raise AssertionError("result identity must reject before control read")

        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            raise AssertionError("result identity must reject before terminal CAS")

        async def acknowledge_stopped(self, observed_claim):
            raise AssertionError("result identity must reject before stop ack")

    class MismatchBatches:
        async def apply_project_batch(self, batch_id):
            raise AssertionError("result identity must reject before State apply")

    with pytest.raises(runtime_module.ProjectRuntimeError) as mismatch:
        await worker_module.ProjectRuntimeTerminalCloser(
            MismatchRuntime(),
            MismatchBatches(),
        ).resolve_prepared_terminal(
            claim=claim,
            result=runtime_module.CanonicalTurnResult("succeeded", "different-result-id"),
            batch_id="343e4567-e89b-42d3-a456-426614174000",
            runner=worker_module.ProjectRuntimeLiveHandle(start, NaturalRunner()),
        )
    assert mismatch.value.code is runtime_module.RuntimeErrorCode.INVALID_ARGUMENT

    awaiting_calls: list[str] = []

    class AwaitingRuntime:
        async def control_for_claim(self, observed_claim):
            awaiting_calls.append("control")
            return runtime_module.ClaimControl("awaiting_approval", 1, 41)

        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            awaiting_calls.append("cas")
            raise AssertionError("approval boundary must not terminal-CAS")

        async def acknowledge_stopped(self, observed_claim):
            awaiting_calls.append("ack")
            raise AssertionError("approval boundary must not acknowledge")

    class AwaitingBatches:
        async def apply_project_batch(self, batch_id):
            awaiting_calls.append("apply")
            raise AssertionError("approval boundary must not settle State")

    with pytest.raises(runtime_module.ProjectRuntimeError) as awaiting:
        await worker_module.ProjectRuntimeTerminalCloser(
            AwaitingRuntime(),
            AwaitingBatches(),
        ).resolve_prepared_terminal(
            claim=claim, result=runtime_module.CanonicalTurnResult("succeeded", "353e4567-e89b-42d3-a456-426614174000"),
            batch_id="353e4567-e89b-42d3-a456-426614174000",
            runner=worker_module.ProjectRuntimeLiveHandle(start, NaturalRunner()),
        )
    assert awaiting.value.code is runtime_module.RuntimeErrorCode.TURN_OPERATIONS_UNRESOLVED
    assert awaiting_calls == ["control"]

    # If runner quiescence cannot be proved, neither durable acknowledgement
    # nor State settlement is allowed on the live path.
    failure_order: list[str] = []
    class FailingRunner:
        def request_cancel(self):
            failure_order.append("cancel")
            return True
        async def wait_quiescent(self):
            raise RuntimeError("runner did not quiesce")
    class FailureRuntime:
        async def control_for_claim(self, observed_claim):
            return runtime_module.ClaimControl("stop_requested", 1, 41)
        async def commit_turn_with_task7_batch(self, *args, **kwargs):
            raise AssertionError("stop path must not CAS")
        async def acknowledge_stopped(self, observed_claim):
            failure_order.append("ack")
            return object()
    class FailureBatches:
        async def apply_project_batch(self, batch_id):
            failure_order.append("apply")
            raise AssertionError("non-quiescent runner must not settle")
    failure_handle = worker_module.ProjectRuntimeLiveHandle(start, FailingRunner())
    with pytest.raises(RuntimeError, match="did not quiesce"):
        await worker_module.ProjectRuntimeTerminalCloser(FailureRuntime(), FailureBatches()).acknowledge_stop(
            claim=claim,
            runner=failure_handle,
            batch_id="423e4567-e89b-42d3-a456-426614174000",
        )
    assert failure_order == ["cancel"]
    assert failure_handle.request_stop(request) is True

    # Dispatcher forwarding is a bounded in-process hint.  Its exact registry
    # exists before run_start executes and derives authority from one fresh
    # control read only; the caller never supplies a claim or control version.
    from gateway.project_runtime_dispatcher import ProjectRuntimeDispatcher

    assert hasattr(ProjectRuntimeDispatcher, "notify_stop")
    expected_request = worker_module.StopRequest(
        project_id=claim.project_id,
        turn_id=claim.turn_id,
        attempt_id=claim.attempt_id,
        worker_id=claim.worker_id,
        lease_generation=claim.lease_generation,
        fencing_token=claim.fencing_token,
        canonical_session_id=claim.canonical_session_id,
        control_version=9,
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    registration_results: list[bool] = []

    class DispatchRuntime:
        def __init__(self):
            self.control_calls: list[object] = []
            self.state = "stop_requested"
            self.failure: Exception | None = None

        def control_for_claim(self, observed_claim):
            self.control_calls.append(observed_claim)
            if self.failure is not None:
                raise self.failure
            return runtime_module.ClaimControl(self.state, 9, 61)

    class DispatchWorker:
        def __init__(self):
            self.requests: list[object] = []
            self.accept = True

        async def run_start(self, observed_start):
            registration_results.append(
                dispatcher.notify_stop(
                    observed_start.claim.project_id,
                    observed_start.claim.turn_id,
                )
            )
            entered.set()
            await release.wait()

        def request_stop(self, stop_request):
            self.requests.append(stop_request)
            return self.accept

    dispatch_runtime = DispatchRuntime()
    dispatch_worker = DispatchWorker()
    dispatcher = ProjectRuntimeDispatcher(
        dispatch_runtime,
        object(),
        dispatch_worker,
        worker_cap=1,
    )
    dispatcher._reserve_and_start(start)
    dispatch_tasks = tuple(dispatcher._live_worker_tasks)
    try:
        await entered.wait()
        assert registration_results == [True]
        assert dispatch_runtime.control_calls == [claim]
        assert dispatch_worker.requests == [expected_request]

        reads_before = len(dispatch_runtime.control_calls)
        assert dispatcher.notify_stop("missing-project", claim.turn_id) is False
        assert dispatcher.notify_stop(claim.project_id, "missing-turn") is False
        assert len(dispatch_runtime.control_calls) == reads_before

        dispatch_runtime.state = "running"
        reads_before = len(dispatch_runtime.control_calls)
        requests_before = len(dispatch_worker.requests)
        assert dispatcher.notify_stop(claim.project_id, claim.turn_id) is False
        assert len(dispatch_runtime.control_calls) == reads_before + 1
        assert len(dispatch_worker.requests) == requests_before

        dispatch_runtime.state = "stop_requested"
        dispatch_runtime.failure = runtime_module.ProjectRuntimeError(
            runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
        )
        reads_before = len(dispatch_runtime.control_calls)
        requests_before = len(dispatch_worker.requests)
        assert dispatcher.notify_stop(claim.project_id, claim.turn_id) is False
        assert len(dispatch_runtime.control_calls) == reads_before + 1
        assert len(dispatch_worker.requests) == requests_before
        dispatch_runtime.failure = None

        dispatch_worker.accept = False
        reads_before = len(dispatch_runtime.control_calls)
        requests_before = len(dispatch_worker.requests)
        assert dispatcher.notify_stop(claim.project_id, claim.turn_id) is False
        assert len(dispatch_runtime.control_calls) == reads_before + 1
        assert dispatch_worker.requests[requests_before:] == [expected_request]
        dispatch_worker.accept = True

        release.set()
        await asyncio.gather(*dispatch_tasks, return_exceptions=False)
        for task in dispatch_tasks:
            dispatcher._worker_done(task)
        reads_before = len(dispatch_runtime.control_calls)
        assert dispatcher.notify_stop(claim.project_id, claim.turn_id) is False
        assert len(dispatch_runtime.control_calls) == reads_before
    finally:
        release.set()
        await asyncio.gather(*dispatch_tasks, return_exceptions=True)

    # One project has one live task even when capacity remains, and the total
    # live registry cannot exceed the configured worker cap.
    same_project_start = replace(
        start,
        claim=replace(
            claim,
            turn_id="c11-second-turn",
            sequence=2,
            attempt_id="c11-second-attempt",
            lease_generation=4,
            fencing_token=6,
        ),
    )
    other_start = replace(
        start,
        claim=replace(
            claim,
            project_id="c11-other-project",
            turn_id="c11-other-turn",
            sequence=1,
            attempt_id="c11-other-attempt",
            worker_id="c11-other-worker",
            lease_generation=1,
            fencing_token=1,
            canonical_session_id="c11-other-session",
        ),
    )
    third_start = replace(
        start,
        claim=replace(
            claim,
            project_id="c11-third-project",
            turn_id="c11-third-turn",
            sequence=1,
            attempt_id="c11-third-attempt",
            worker_id="c11-third-worker",
            lease_generation=1,
            fencing_token=1,
            canonical_session_id="c11-third-session",
        ),
    )
    bound_release = asyncio.Event()
    bound_entered = asyncio.Event()

    class BoundWorker:
        def __init__(self):
            self.starts: list[object] = []

        async def run_start(self, observed_start):
            self.starts.append(observed_start)
            if len(self.starts) == 2:
                bound_entered.set()
            await bound_release.wait()

        def request_stop(self, stop_request):
            return True

    bound_worker = BoundWorker()
    bound_dispatcher = ProjectRuntimeDispatcher(
        DispatchRuntime(),
        object(),
        bound_worker,
        worker_cap=2,
    )
    bound_dispatcher._reserve_and_start(start)
    bound_tasks = tuple(bound_dispatcher._live_worker_tasks)
    try:
        with pytest.raises(RuntimeError, match="project"):
            bound_dispatcher._reserve_and_start(same_project_start)
        bound_dispatcher._reserve_and_start(other_start)
        bound_tasks = tuple(bound_dispatcher._live_worker_tasks)
        assert len(bound_tasks) == 2
        with pytest.raises(RuntimeError, match="capacity"):
            bound_dispatcher._reserve_and_start(third_start)
        await bound_entered.wait()
        assert {entry.claim.project_id for entry in bound_worker.starts} == {
            claim.project_id,
            other_start.claim.project_id,
        }
    finally:
        bound_release.set()
        await asyncio.gather(*bound_tasks, return_exceptions=True)

    # A finite dispatch tick polls every live exact claim once before it asks
    # the recovery side for work, and forwards only a fresh durable stop.
    poll_release = asyncio.Event()
    poll_entered = asyncio.Event()
    poll_trace: list[str] = []

    class PollRuntime:
        def __init__(self):
            self.control_calls: list[object] = []
            self.failures: dict[str, Exception] = {}

        def control_for_claim(self, observed_claim):
            self.control_calls.append(observed_claim)
            poll_trace.append(f"control:{observed_claim.turn_id}")
            failure = self.failures.get(observed_claim.turn_id)
            if failure is not None:
                raise failure
            assert observed_claim == other_start.claim
            return runtime_module.ClaimControl("stop_requested", 14, 61)

    class PollGuard:
        def operation_recovery_membership_upper_watermark(self):
            poll_trace.append("recovery-upper")
            return 1

        def recover_pending_operations(self, *args, **kwargs):
            from types import SimpleNamespace

            poll_trace.append("recovery")
            assert kwargs["max_claims"] == 0
            return SimpleNamespace(
                starts=(),
                scanned_through=object(),
                reached_epoch_end=True,
            )

    class PollWorker:
        def __init__(self):
            self.starts: list[object] = []
            self.requests: list[object] = []

        async def run_start(self, observed_start):
            self.starts.append(observed_start)
            if len(self.starts) == 2:
                poll_entered.set()
            await poll_release.wait()

        def request_stop(self, stop_request):
            self.requests.append(stop_request)
            return True

    poll_runtime = PollRuntime()
    poll_worker = PollWorker()
    poll_dispatcher = ProjectRuntimeDispatcher(
        poll_runtime,
        PollGuard(),
        poll_worker,
        worker_cap=2,
    )
    poll_dispatcher._reserve_and_start(start)
    poll_dispatcher._reserve_and_start(other_start)
    poll_tasks = tuple(poll_dispatcher._live_worker_tasks)
    try:
        await poll_entered.wait()
        expected_polled_stop = worker_module.StopRequest(
            project_id=other_start.claim.project_id,
            turn_id=other_start.claim.turn_id,
            attempt_id=other_start.claim.attempt_id,
            worker_id=other_start.claim.worker_id,
            lease_generation=other_start.claim.lease_generation,
            fencing_token=other_start.claim.fencing_token,
            canonical_session_id=(
                other_start.claim.canonical_session_id
            ),
            control_version=14,
        )
        for poll_failure in (
            runtime_module.ProjectRuntimeError(
                runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM
            ),
            RuntimeError("live control read failed"),
        ):
            poll_runtime.control_calls.clear()
            poll_runtime.failures = {claim.turn_id: poll_failure}
            poll_worker.requests.clear()
            poll_trace.clear()

            poll_dispatcher.dispatch_once(
                worker_id="c11-poll-worker",
                lease_seconds=30,
                dispatcher_lease=start.dispatcher_lease,
                readback=object(),
                approval_checkpoints=object(),
            )

            assert len(poll_runtime.control_calls) == 2
            assert set(poll_runtime.control_calls) == {
                claim,
                other_start.claim,
            }
            assert {
                entry: poll_runtime.control_calls.count(entry)
                for entry in (claim, other_start.claim)
            } == {
                claim: 1,
                other_start.claim: 1,
            }
            assert set(poll_trace[:2]) == {
                f"control:{claim.turn_id}",
                f"control:{other_start.claim.turn_id}",
            }
            assert poll_trace[2:] == ["recovery-upper", "recovery"]
            assert poll_worker.requests == [expected_polled_stop]
            assert all(not task.done() for task in poll_tasks)
            assert poll_dispatcher.available_slots == 0
    finally:
        poll_release.set()
        await asyncio.gather(*poll_tasks, return_exceptions=True)

    # The real task callback, rather than test-owned cleanup, releases both
    # registry entries and the slot.  It also observes and logs a worker
    # exception before notifying this deterministic callback barrier.
    import logging

    callback_normal_entered = asyncio.Event()
    callback_normal_release = asyncio.Event()
    callback_failure_entered = asyncio.Event()
    callback_failure_release = asyncio.Event()
    callback_failure = RuntimeError("callback worker failed")

    class CallbackRuntime:
        def __init__(self):
            self.control_calls: list[object] = []

        def control_for_claim(self, observed_claim):
            self.control_calls.append(observed_claim)
            return runtime_module.ClaimControl("stop_requested", 16, 61)

    class CallbackWorker:
        async def run_start(self, observed_start):
            if observed_start is start:
                callback_normal_entered.set()
                await callback_normal_release.wait()
                return
            assert observed_start is same_project_start
            callback_failure_entered.set()
            await callback_failure_release.wait()
            raise callback_failure

        def request_stop(self, stop_request):
            raise AssertionError("completed task must not receive stop")

    class CallbackProbeDispatcher(ProjectRuntimeDispatcher):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.callback_count = 0
            self.callback_event = asyncio.Event()

        def arm_callback(self):
            self.callback_event = asyncio.Event()

        def _worker_done(self, task):
            super()._worker_done(task)
            self.callback_count += 1
            self.callback_event.set()

    callback_runtime = CallbackRuntime()
    callback_dispatcher = CallbackProbeDispatcher(
        callback_runtime,
        object(),
        CallbackWorker(),
        worker_cap=1,
    )
    callback_dispatcher._reserve_and_start(start)
    normal_task = next(iter(callback_dispatcher._live_worker_tasks))
    await callback_normal_entered.wait()
    callback_normal_release.set()
    await callback_dispatcher.callback_event.wait()
    assert normal_task.done()
    assert normal_task.exception() is None
    assert callback_dispatcher.callback_count == 1
    assert callback_dispatcher.available_slots == 1
    assert callback_dispatcher._live_worker_tasks == set()
    reads_before = len(callback_runtime.control_calls)
    assert callback_dispatcher.notify_stop(
        claim.project_id,
        claim.turn_id,
    ) is False
    assert len(callback_runtime.control_calls) == reads_before

    # Reusing the same project proves the project-key entry was also removed.
    callback_dispatcher.arm_callback()
    callback_dispatcher._reserve_and_start(same_project_start)
    failed_task = next(iter(callback_dispatcher._live_worker_tasks))
    await callback_failure_entered.wait()
    with caplog.at_level(
        logging.ERROR,
        logger="gateway.project_runtime_dispatcher",
    ):
        callback_failure_release.set()
        await callback_dispatcher.callback_event.wait()
    assert failed_task.done()
    assert callback_dispatcher.callback_count == 2
    assert callback_dispatcher.available_slots == 1
    assert callback_dispatcher._live_worker_tasks == set()
    reads_before = len(callback_runtime.control_calls)
    assert callback_dispatcher.notify_stop(
        same_project_start.claim.project_id,
        same_project_start.claim.turn_id,
    ) is False
    assert len(callback_runtime.control_calls) == reads_before
    callback_failures = [
        record
        for record in caplog.records
        if record.name == "gateway.project_runtime_dispatcher"
        and record.message == "project runtime worker failed"
        and record.exc_info is not None
        and record.exc_info[1] is callback_failure
    ]
    assert len(callback_failures) == 1
    assert callback_failures[0].levelno == logging.ERROR

    # A late callback for an old task must never evict the exact replacement
    # registered for the same project.
    replacement_start = replace(
        start,
        claim=replace(
            claim,
            turn_id="c11-replacement-turn",
            sequence=2,
            attempt_id="c11-replacement-attempt",
            lease_generation=4,
            fencing_token=7,
        ),
    )
    old_entered = asyncio.Event()
    old_release = asyncio.Event()
    replacement_entered = asyncio.Event()
    replacement_release = asyncio.Event()

    class ReplacementRuntime:
        def __init__(self):
            self.control_calls: list[object] = []

        def control_for_claim(self, observed_claim):
            self.control_calls.append(observed_claim)
            return runtime_module.ClaimControl("stop_requested", 15, 61)

    class ReplacementWorker:
        def __init__(self):
            self.requests: list[object] = []

        async def run_start(self, observed_start):
            if observed_start.claim.turn_id == claim.turn_id:
                old_entered.set()
                await old_release.wait()
            else:
                replacement_entered.set()
                await replacement_release.wait()

        def request_stop(self, stop_request):
            self.requests.append(stop_request)
            return True

    replacement_runtime = ReplacementRuntime()
    replacement_worker = ReplacementWorker()
    replacement_dispatcher = ProjectRuntimeDispatcher(
        replacement_runtime,
        object(),
        replacement_worker,
        worker_cap=1,
    )
    replacement_dispatcher._reserve_and_start(start)
    old_task = next(iter(replacement_dispatcher._live_worker_tasks))
    replacement_task: asyncio.Task[None] | None = None
    try:
        await old_entered.wait()
        old_release.set()
        await old_task
        replacement_dispatcher._worker_done(old_task)

        replacement_dispatcher._reserve_and_start(replacement_start)
        replacement_task = next(iter(replacement_dispatcher._live_worker_tasks))
        await replacement_entered.wait()
        replacement_dispatcher._worker_done(old_task)

        assert replacement_dispatcher.notify_stop(
            claim.project_id,
            replacement_start.claim.turn_id,
        ) is True
        assert replacement_runtime.control_calls == [replacement_start.claim]
        assert replacement_worker.requests == [
            worker_module.StopRequest(
                project_id=replacement_start.claim.project_id,
                turn_id=replacement_start.claim.turn_id,
                attempt_id=replacement_start.claim.attempt_id,
                worker_id=replacement_start.claim.worker_id,
                lease_generation=replacement_start.claim.lease_generation,
                fencing_token=replacement_start.claim.fencing_token,
                canonical_session_id=(
                    replacement_start.claim.canonical_session_id
                ),
                control_version=15,
            )
        ]
    finally:
        old_release.set()
        replacement_release.set()
        tasks = tuple(replacement_dispatcher._live_worker_tasks)
        if replacement_task is not None and replacement_task not in tasks:
            tasks += (replacement_task,)
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_task7_c13_worker_context_run_start_orders_closer_and_failure_eviction_without_reclaim(
    tmp_path,
) -> None:
    """One preclaimed worker run caches only published work and never reclaims.

    This catches a C13 worker that discovers work itself, promotes an agent
    before C11 publication, skips durable-stop acknowledgement, or keeps a
    mutated parent agent after a stopped/failed run.
    """
    from gateway.config import GatewayConfig, Platform
    from gateway.session import ProjectBatchApplyResult, ProjectHistorySnapshot
    from hermes_cli import project_runtime as runtime_module
    from hermes_state import PendingProjectBatch
    import gateway.project_runtime_worker as worker_module

    required = (
        "ProjectAgentRevisions",
        "ProjectAgentRunResult",
        "CanonicalProjectRuntimeWorker",
        "ProjectAgentFactory",
        "ProjectAgentBuild",
        "ProjectAgent",
        "ProjectAgentTurn",
        "ProjectRuntimeExecutionPort",
        "ProjectBatchWorkerPort",
        "project_agent_cache_key",
        "project_agent_cache_signature",
    )
    assert all(hasattr(worker_module, name) for name in required)

    def claim(turn_id: str, attempt_id: str) -> runtime_module.TurnClaim:
        return runtime_module.TurnClaim(
            turn_id=turn_id,
            project_id="c13-project",
            sequence={
                "turn-success": 1,
                "turn-discord": 2,
                "turn-revision": 3,
                "turn-mismatch": 4,
                "turn-stop": 5,
                "turn-failure": 6,
                "turn-watch": 7,
            }[turn_id],
            worker_id="c13-worker",
            attempt_id=attempt_id,
            lease_generation=1,
            fencing_token=1,
            lease_expires_at=130,
            canonical_session_id="c13-session",
        )

    lease = runtime_module.DispatcherLease(
        "11111111-1111-4111-8111-111111111111", 1, 1, 130
    )
    success_start = runtime_module.WorkerStart(
        "queued_turn", claim("turn-success", "attempt-success"), None, lease
    )
    stop_start = runtime_module.WorkerStart(
        "queued_turn", claim("turn-stop", "attempt-stop"), None, lease
    )
    discord_start = runtime_module.WorkerStart(
        "queued_turn", claim("turn-discord", "attempt-discord"), None, lease
    )
    revision_start = runtime_module.WorkerStart(
        "queued_turn", claim("turn-revision", "attempt-revision"), None, lease
    )
    mismatch_start = runtime_module.WorkerStart(
        "queued_turn", claim("turn-mismatch", "attempt-mismatch"), None, lease
    )
    failure_start = runtime_module.WorkerStart(
        "queued_turn", claim("turn-failure", "attempt-failure"), None, lease
    )
    watch_start = runtime_module.WorkerStart(
        "queued_turn", claim("turn-watch", "attempt-watch"), None, lease
    )

    class Runtime:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []
            self.trace: list[str] = []
            self.stop_requested = False
            self.watch_error_turn: str | None = None
            self.watch_tick = asyncio.Event()

        async def mark_turn_started(self, value):
            self.calls.append(("start", value.turn_id))
            self.trace.append("runtime.start")
            return value

        async def execution_input_for_claim(self, value):
            self.calls.append(("input", value.turn_id))
            self.trace.append("runtime.input")
            surface = "discord" if value.turn_id == "turn-discord" else "desktop"
            return runtime_module.TurnExecutionInput(
                runtime_module.TurnAttemptIdentity(
                    project_id=value.project_id,
                    turn_id=value.turn_id,
                    sequence=value.sequence,
                    worker_id=value.worker_id,
                    attempt_id=value.attempt_id,
                    lease_generation=value.lease_generation,
                    fencing_token=value.fencing_token,
                    canonical_session_id=value.canonical_session_id,
                    lease_expires_at=value.lease_expires_at,
                ),
                {"opaque": value.turn_id},
                runtime_module.TurnOrigin(
                    f"{surface}-binding", surface, f"{surface}-window", "owner-1"
                ),
                1 if value.turn_id in {"turn-revision", "turn-mismatch", "turn-stop", "turn-failure", "turn-watch"} else 0,
            )

        async def heartbeat_turn(self, value, *, lease_seconds):
            self.calls.append(("heartbeat", value.turn_id))
            self.trace.append("runtime.heartbeat")
            return value

        async def control_for_claim(self, value):
            self.calls.append(("control", value.turn_id))
            self.trace.append("runtime.control")
            if self.watch_error_turn == value.turn_id:
                self.watch_tick.set()
                raise RuntimeError("watch failure")
            return runtime_module.ClaimControl(
                "stop_requested" if self.stop_requested else "running",
                3,
                value.lease_expires_at,
            )

        async def commit_turn_with_task7_batch(
            self, value, result, *, transcript_batch_id
        ):
            self.calls.append(("commit", value.turn_id))
            self.trace.append("runtime.commit")
            return object()

        async def acknowledge_stopped(self, value):
            self.calls.append(("stopped", value.turn_id))
            self.trace.append("runtime.stopped")
            return object()

    class Batches:
        def __init__(self, trace) -> None:
            self.calls: list[tuple[str, str]] = []
            self.trace = trace
            self.history_counts = [1, 3, 5, 9, 11, 11, 11]
            self.prepared = []

        async def load_project_history(self, session_id):
            self.calls.append(("history", session_id))
            self.trace.append("batches.history")
            count = self.history_counts.pop(0)
            return ProjectHistorySnapshot(
                session_id, ({"role": "user", "content": "prior"},), count
            )

        async def prepare_terminal_result(
            self, value, *, batch_id, status, base_message_count, messages
        ):
            self.calls.append(("prepare", value.turn_id))
            self.trace.append("batches.prepare")
            self.prepared.append((value.turn_id, status, base_message_count, messages))
            return PendingProjectBatch(
                batch_id=batch_id,
                batch_creation_sequence=1,
                kind="terminal_result",
                state="prepared",
                attempt=runtime_module.TurnAttemptIdentity(
                    value.project_id, value.turn_id, value.sequence,
                    value.worker_id, value.attempt_id, value.lease_generation,
                    value.fencing_token, value.canonical_session_id,
                    value.lease_expires_at,
                ),
                terminal_status=status,
                operation_id=None,
                approval_id=None,
                base_message_count=base_message_count,
                created_at=1.0,
            )

        async def prepare_approval_checkpoint(self, *args, **kwargs):
            raise AssertionError("C13 must not prepare approval checkpoints")

        async def apply_project_batch(self, batch_id):
            self.calls.append(("apply", batch_id))
            self.trace.append("batches.apply")
            return ProjectBatchApplyResult("published")

    class Turn:
        def __init__(self, outcome: str, base_count: int | None, trace) -> None:
            self.outcome = outcome
            self.base_count = base_count
            self.trace = trace
            self.entered = asyncio.Event()
            self.release = asyncio.Event()
            self.cancelled = False
            self.cancel_count = 0
            self.quiescent_count = 0
            self.cancelled_event = asyncio.Event()

        def request_cancel(self):
            self.cancelled = True
            self.cancel_count += 1
            self.cancelled_event.set()
            self.trace.append("turn.cancel")
            return True

        async def wait_quiescent(self):
            self.quiescent_count += 1
            self.trace.append("turn.quiescent")
            return None

        async def result(self):
            self.entered.set()
            self.trace.append("turn.result")
            if self.outcome == "stop":
                await self.release.wait()
                raise asyncio.CancelledError()
            if self.outcome == "failure":
                raise RuntimeError("agent failure")
            if self.outcome == "watch":
                await self.cancelled_event.wait()
            return worker_module.ProjectAgentRunResult(
                "succeeded", self.base_count,
                (
                    {"role": "user", "content": "follow-up"},
                    {"role": "assistant", "content": "completed"},
                ),
            )

    class Agent:
        def __init__(self, outcomes, trace):
            self.outcomes = list(outcomes)
            self.turns: list[Turn] = []
            self.executions = []
            self.trace = trace
            self.turn_created = asyncio.Event()

        def create_turn(self, execution, operation):
            assert operation is None
            self.trace.append("agent.create_turn")
            self.executions.append(execution)
            outcome, base_count = self.outcomes.pop(0)
            turn = Turn(outcome, base_count, self.trace)
            self.turns.append(turn)
            self.turn_created.set()
            return turn

    class Build:
        def __init__(self, factory, contract_revision):
            self.factory = factory
            self.revisions = worker_module.ProjectAgentRevisions(
                "base-config", "tools-v1", "model-v1"
            )

        async def create_project_agent(self, *, history):
            self.factory.trace.append("build.create_parent")
            agent = self.factory.available_agents.pop(0)
            self.factory.created.append(agent)
            return agent

    class Factory:
        def __init__(self) -> None:
            self.trace: list[str] = []
            self.first = Agent((("success", 1), ("success", 3)), self.trace)
            self.second = Agent((("success", 5),), self.trace)
            self.third = Agent((("success", 9), ("stop", None)), self.trace)
            self.fourth = Agent((("failure", None),), self.trace)
            self.fifth = Agent((("watch", 11),), self.trace)
            self.available_agents = [self.first, self.second, self.third, self.fourth, self.fifth]
            self.created: list[Agent] = []
            self.released: list[Agent] = []
            self.contexts = []

        async def resolve_project_agent(self, *, context, contract_revision):
            self.trace.append("factory.resolve")
            self.contexts.append(context)
            return Build(self, contract_revision)

        async def release_project_agent(self, agent):
            self.trace.append("factory.release")
            self.released.append(agent)

    runtime = Runtime()
    factory = Factory()
    batches = Batches(factory.trace)
    runtime.trace = factory.trace
    config = GatewayConfig()
    worker = worker_module.CanonicalProjectRuntimeWorker(
        runtime,
        batches,
        factory,
        config,
        profile_home=tmp_path / "profile",
        lease_seconds=30,
        heartbeat_interval_seconds=60.0,
        batch_id_factory=iter((
            "123e4567-e89b-42d3-a456-426614174000",
            "223e4567-e89b-42d3-a456-426614174000",
            "323e4567-e89b-42d3-a456-426614174000",
            "423e4567-e89b-42d3-a456-426614174000",
        )).__next__,
    )

    await worker.run_start(success_start)
    assert factory.created == [factory.first]
    assert factory.released == []
    assert factory.trace == [
        "runtime.start", "runtime.input", "batches.history",
        "factory.resolve", "build.create_parent", "agent.create_turn",
        "turn.result", "batches.prepare", "turn.quiescent",
        "runtime.control", "runtime.commit", "batches.apply",
    ]
    assert batches.prepared == [(
        "turn-success", "succeeded", 1,
        (
            {"role": "user", "content": "follow-up"},
            {"role": "assistant", "content": "completed"},
        ),
    )]

    factory.trace.clear()
    await worker.run_start(discord_start)
    assert factory.created == [factory.first]
    assert len(factory.contexts) == 2
    assert all(context.source.platform is Platform.LOCAL for context in factory.contexts)
    assert all(context.source.chat_id == "project:c13-project" for context in factory.contexts)
    from gateway.session import build_session_context_prompt
    assert build_session_context_prompt(factory.contexts[0]).encode("utf-8") == build_session_context_prompt(factory.contexts[1]).encode("utf-8")
    assert factory.first.executions == [
        runtime_module.TurnExecutionInput(
            runtime_module.TurnAttemptIdentity(
                "c13-project", "turn-success", 1, "c13-worker",
                "attempt-success", 1, 1, "c13-session", 130,
            ),
            {"opaque": "turn-success"},
            runtime_module.TurnOrigin(
                "desktop-binding", "desktop", "desktop-window", "owner-1"
            ),
            0,
        ),
        runtime_module.TurnExecutionInput(
            runtime_module.TurnAttemptIdentity(
                "c13-project", "turn-discord", 2, "c13-worker",
                "attempt-discord", 1, 1, "c13-session", 130,
            ),
            {"opaque": "turn-discord"},
            runtime_module.TurnOrigin(
                "discord-binding", "discord", "discord-window", "owner-1"
            ),
            0,
        ),
    ]

    await worker.run_start(revision_start)
    assert factory.created == [factory.first, factory.second]
    assert factory.released == [factory.first]

    await worker.run_start(mismatch_start)
    assert factory.created == [factory.first, factory.second, factory.third]
    assert factory.released == [factory.first, factory.second]

    factory.third.turn_created = asyncio.Event()
    stop_runtime_before = tuple(runtime.calls)
    stop_batches_before = tuple(batches.calls)
    stop_trace_before = len(factory.trace)
    stopping = asyncio.create_task(worker.run_start(stop_start))
    stop_turn = None
    try:
        await asyncio.wait_for(factory.third.turn_created.wait(), timeout=2)
        stop_turn = factory.third.turns[-1]
        await asyncio.wait_for(stop_turn.entered.wait(), timeout=2)
        runtime.stop_requested = True
        assert worker.request_stop(
            worker_module.StopRequest(
                "c13-project", "turn-stop", "attempt-stop", "c13-worker",
                1, 1, "c13-session", 3,
            )
        ) is True
        stop_turn.release.set()
        await asyncio.wait_for(asyncio.shield(stopping), timeout=2)
    finally:
        runtime.stop_requested = True
        if stop_turn is None and factory.third.turns:
            stop_turn = factory.third.turns[-1]
        if stop_turn is not None:
            stop_turn.release.set()
        if not stopping.done():
            stopping.cancel()
        await asyncio.wait_for(
            asyncio.gather(stopping, return_exceptions=True),
            timeout=2,
        )
    assert stop_turn is not None
    stop_runtime_after = runtime.calls[len(stop_runtime_before):]
    stop_batches_after = batches.calls[len(stop_batches_before):]
    stop_trace = factory.trace[stop_trace_before:]
    assert stop_turn.cancelled is True
    assert stop_turn.quiescent_count >= 1
    assert stop_trace.index("turn.cancel") < stop_trace.index(
        "turn.quiescent"
    ) < stop_trace.index("factory.release")
    assert stop_runtime_after.count(("stopped", "turn-stop")) == 1
    assert ("commit", "turn-stop") not in stop_runtime_after
    assert stop_batches_after == [("history", "c13-session")]
    assert factory.released == [factory.first, factory.second, factory.third]
    assert ("prepare", "turn-stop") not in batches.calls

    failure_runtime_before = tuple(runtime.calls)
    failure_batches_before = tuple(batches.calls)
    failure_trace_before = len(factory.trace)
    with pytest.raises(RuntimeError, match="agent failure"):
        await worker.run_start(failure_start)
    failure_runtime_after = runtime.calls[len(failure_runtime_before):]
    failure_batches_after = batches.calls[len(failure_batches_before):]
    assert ("commit", "turn-failure") not in failure_runtime_after
    assert ("stopped", "turn-failure") not in failure_runtime_after
    assert failure_batches_after == [("history", "c13-session")]
    failure_trace = factory.trace[failure_trace_before:]
    failure_turn = factory.fourth.turns[-1]
    assert failure_turn.cancel_count == 1
    assert failure_turn.quiescent_count == 1
    assert failure_trace.index("turn.cancel") < failure_trace.index(
        "turn.quiescent"
    ) < failure_trace.index("factory.release")
    assert factory.created == [factory.first, factory.second, factory.third, factory.fourth]
    assert factory.released == [factory.first, factory.second, factory.third, factory.fourth]

    runtime.watch_error_turn = "turn-watch"
    factory.fifth.turn_created = asyncio.Event()
    watch_worker = worker_module.CanonicalProjectRuntimeWorker(
        runtime, batches, factory, config,
        profile_home=tmp_path / "profile",
        lease_seconds=30,
        heartbeat_interval_seconds=0.001,
        batch_id_factory=lambda: "523e4567-e89b-42d3-a456-426614174000",
    )
    watch_runtime_before = tuple(runtime.calls)
    watch_batches_before = tuple(batches.calls)
    watch_trace_before = len(factory.trace)
    watching = asyncio.create_task(watch_worker.run_start(watch_start))
    try:
        await asyncio.wait_for(factory.fifth.turn_created.wait(), timeout=2)
        await asyncio.wait_for(runtime.watch_tick.wait(), timeout=2)
        with pytest.raises(RuntimeError, match="watch failure"):
            await asyncio.wait_for(watching, timeout=2)
    finally:
        if not watching.done():
            watching.cancel()
        await asyncio.gather(watching, return_exceptions=True)
    watch_runtime_after = runtime.calls[len(watch_runtime_before):]
    watch_batches_after = batches.calls[len(watch_batches_before):]
    assert factory.fifth.turns[-1].cancelled is True
    assert ("heartbeat", "turn-watch") not in watch_runtime_after
    assert ("commit", "turn-watch") not in watch_runtime_after
    assert ("stopped", "turn-watch") not in watch_runtime_after
    assert watch_batches_after == [("history", "c13-session")]
    watch_trace = factory.trace[watch_trace_before:]
    assert factory.fifth.turns[-1].quiescent_count == 1
    assert watch_trace.index("turn.cancel") < watch_trace.index(
        "turn.quiescent"
    ) < watch_trace.index("factory.release")
    assert factory.released == [
        factory.first, factory.second, factory.third, factory.fourth, factory.fifth,
    ]
    assert all(factory.released.count(agent) == 1 for agent in factory.released)
    assert all(name != "claim" for name, _ in runtime.calls)

    # Reviewfix harnesses below deliberately expose only the public worker
    # ports. Normal dispatcher exclusion prevents these collisions in C14,
    # but C13 must still keep cache/runner ownership exact under races.
    from types import SimpleNamespace

    review_sequence = 100

    def review_start(
        label: str,
        *,
        project_id: str = "review-project",
        session_id: str = "review-session",
    ):
        nonlocal review_sequence
        review_sequence += 1
        value = runtime_module.TurnClaim(
            f"turn-{label}", project_id, review_sequence, "review-worker",
            f"attempt-{label}", 1, 1, 130, session_id,
        )
        return runtime_module.WorkerStart("queued_turn", value, None, lease)

    def review_attempt(value):
        return runtime_module.TurnAttemptIdentity(
            value.project_id, value.turn_id, value.sequence,
            value.worker_id, value.attempt_id, value.lease_generation,
            value.fencing_token, value.canonical_session_id,
            value.lease_expires_at,
        )

    def review_execution(value, contract_revision=0):
        return runtime_module.TurnExecutionInput(
            review_attempt(value), {"opaque": value.turn_id},
            runtime_module.TurnOrigin(
                "review-binding", "desktop", "review-window", "owner-1"
            ),
            contract_revision,
        )

    class ReviewRuntime:
        def __init__(self, controls=(), mark_result=None):
            self.calls = []
            self.controls = list(controls)
            self.mark_result = mark_result

        async def mark_turn_started(self, value):
            self.calls.append(("mark", value.turn_id))
            if callable(self.mark_result):
                return self.mark_result(value)
            return value

        async def execution_input_for_claim(self, value):
            self.calls.append(("input", value.turn_id))
            return review_execution(value)

        async def heartbeat_turn(self, value, *, lease_seconds):
            self.calls.append(("heartbeat", value.turn_id))
            return value

        async def control_for_claim(self, value):
            self.calls.append(("control", value.turn_id))
            state = self.controls.pop(0) if self.controls else "running"
            if isinstance(state, BaseException):
                raise state
            return runtime_module.ClaimControl(
                state, 7, value.lease_expires_at
            )

        async def commit_turn_with_task7_batch(
            self, value, result, *, transcript_batch_id
        ):
            self.calls.append(("commit", value.turn_id))
            return object()

        async def acknowledge_stopped(self, value):
            self.calls.append(("ack", value.turn_id))
            return object()

    class ReviewBatches:
        def __init__(
            self,
            history_counts,
            apply_results=(),
            *,
            forged_prepare=False,
            prepared_transforms=(),
            prepare_entered=None,
            prepare_release=None,
        ):
            self.calls = []
            self.history_counts = list(history_counts)
            self.apply_results = list(apply_results)
            self.forged_prepare = forged_prepare
            self.prepared_transforms = list(prepared_transforms)
            self.prepare_entered = prepare_entered
            self.prepare_release = prepare_release

        async def load_project_history(self, session_id):
            self.calls.append(("history", session_id))
            return ProjectHistorySnapshot(
                session_id,
                ({"role": "user", "content": "prior"},),
                self.history_counts.pop(0),
            )

        async def prepare_terminal_result(
            self, value, *, batch_id, status, base_message_count, messages
        ):
            self.calls.append(("prepare", value.turn_id))
            if self.prepare_entered is not None:
                self.prepare_entered.set()
            if self.prepare_release is not None:
                await self.prepare_release.wait()
            if self.forged_prepare:
                return SimpleNamespace(batch_id=batch_id)
            prepared = PendingProjectBatch(
                batch_id, 1, "terminal_result", "prepared",
                review_attempt(value), status, None, None,
                base_message_count, 1.0,
            )
            if self.prepared_transforms:
                transform = self.prepared_transforms.pop(0)
                if transform is not None:
                    prepared = transform(prepared)
            return prepared

        async def prepare_approval_checkpoint(self, *args, **kwargs):
            raise AssertionError("reviewfix must not prepare approval")

        async def apply_project_batch(self, batch_id):
            self.calls.append(("apply", batch_id))
            result = (
                self.apply_results.pop(0)
                if self.apply_results
                else ProjectBatchApplyResult("published")
            )
            if isinstance(result, BaseException):
                raise result
            return result

    class ReviewTurn:
        def __init__(self, behavior="result", base_count=1, trace=None):
            self.behavior = behavior
            self.base_count = base_count
            self.trace = trace if trace is not None else []
            self.entered = asyncio.Event()
            self.cancel_event = asyncio.Event()
            self.result_gate = asyncio.Event()
            self.background = None
            self.cancel_count = 0
            self.quiescent_count = 0

        def request_cancel(self):
            self.cancel_count += 1
            self.trace.append("cancel")
            self.cancel_event.set()
            return self.cancel_count == 1

        async def wait_quiescent(self):
            self.quiescent_count += 1
            self.trace.append("quiescent")
            if self.background is not None:
                await self.background

        async def _background(self):
            await self.cancel_event.wait()

        async def result(self):
            self.entered.set()
            self.trace.append("result")
            if self.behavior == "error":
                raise RuntimeError("review runner primary")
            if self.behavior == "stop_block":
                await self.cancel_event.wait()
                raise asyncio.CancelledError()
            if self.behavior == "shielded":
                self.background = asyncio.create_task(self._background())
                await asyncio.shield(self.background)
            if self.behavior == "gated_result":
                await self.result_gate.wait()
            return worker_module.ProjectAgentRunResult(
                "succeeded", self.base_count,
                (
                    {"role": "user", "content": "review"},
                    {"role": "assistant", "content": "done"},
                ),
            )

    class ReviewAgent:
        def __init__(self, turns, trace=None):
            self.turns = list(turns)
            self.created_turns = []
            self.created = asyncio.Event()
            self.trace = trace if trace is not None else []

        def create_turn(self, execution, operation):
            assert operation is None
            turn = self.turns.pop(0)
            self.created_turns.append(turn)
            self.trace.append("create-turn")
            self.created.set()
            return turn

    class ReviewBuild:
        revisions = worker_module.ProjectAgentRevisions(
            "review-base", "review-tools", "review-model"
        )

        def __init__(self, owner):
            self.owner = owner

        async def create_project_agent(self, *, history):
            self.owner.trace.append("create-parent")
            agent = self.owner.available.pop(0)
            self.owner.created.append(agent)
            return agent

    class ReviewFactory:
        def __init__(self, agents, release_hook=None, trace=None):
            self.available = list(agents)
            self.release_hook = release_hook
            self.trace = trace if trace is not None else []
            self.created = []
            self.releases = []
            self.resolve_calls = 0

        async def resolve_project_agent(self, *, context, contract_revision):
            self.resolve_calls += 1
            self.trace.append("resolve")
            return ReviewBuild(self)

        async def release_project_agent(self, agent):
            self.releases.append(agent)
            self.trace.append("release")
            if self.release_hook is not None:
                await self.release_hook(agent)

    review_uuid_value = 0x60000000

    def next_review_uuid():
        nonlocal review_uuid_value
        review_uuid_value += 1
        return f"{review_uuid_value:08x}-0000-4000-8000-000000000000"

    def review_worker(
        runtime_value,
        batches_value,
        factory_value,
        *,
        capacity=128,
        interval=60.0,
    ):
        return worker_module.CanonicalProjectRuntimeWorker(
            runtime_value, batches_value, factory_value, config,
            profile_home=tmp_path / "review-profile",
            lease_seconds=30,
            heartbeat_interval_seconds=interval,
            batch_id_factory=next_review_uuid,
            cache_capacity=capacity,
        )

    def review_stop_request(start_value, version=7):
        value = start_value.claim
        return worker_module.StopRequest(
            value.project_id, value.turn_id, value.attempt_id,
            value.worker_id, value.lease_generation, value.fencing_token,
            value.canonical_session_id, version,
        )

    # A closer-only remote stop must evict even when State reports a
    # publication-like apply outcome. The subsequent compatible run proves
    # eviction through construction calls rather than private cache state.
    for remote_outcome in ("published", "already_published"):
        remote_trace = []
        remote_first_turn = ReviewTurn("result", 1, remote_trace)
        remote_first = ReviewAgent(
            [
                remote_first_turn,
                ReviewTurn("result", 3, remote_trace),
            ],
            remote_trace,
        )
        remote_second = ReviewAgent(
            [ReviewTurn("result", 3, remote_trace)],
            remote_trace,
        )
        remote_factory = ReviewFactory(
            [remote_first, remote_second],
            trace=remote_trace,
        )
        remote_runtime = ReviewRuntime(["stop_requested", "running"])
        remote_batches = ReviewBatches(
            [1, 3],
            [
                ProjectBatchApplyResult(remote_outcome),
                ProjectBatchApplyResult("published"),
            ],
        )
        remote_worker = review_worker(
            remote_runtime, remote_batches, remote_factory
        )
        remote_one = review_start(f"remote-{remote_outcome}-one")
        remote_two = review_start(f"remote-{remote_outcome}-two")
        await asyncio.wait_for(
            remote_worker.run_start(remote_one), timeout=2
        )
        assert remote_first_turn.cancel_count == 1
        remote_cancel_index = remote_trace.index("cancel")
        remote_release_index = remote_trace.index("release")
        remote_quiescent_after_cancel = remote_trace.index(
            "quiescent", remote_cancel_index + 1
        )
        assert (
            remote_cancel_index
            < remote_quiescent_after_cancel
            < remote_release_index
        )
        assert remote_runtime.calls.count(("ack", remote_one.claim.turn_id)) == 1
        assert remote_factory.releases == [remote_first]
        assert remote_worker.request_stop(review_stop_request(remote_one)) is False
        await asyncio.wait_for(
            remote_worker.run_start(remote_two), timeout=2
        )
        assert remote_factory.created == [remote_first, remote_second]
        assert remote_factory.releases.count(remote_first) == 1

    # Durable stop after an explicit result but while preparation is gated
    # exercises the prepared-terminal stop path (not the batchless path).
    durable_trace = []
    durable_prepare_entered = asyncio.Event()
    durable_prepare_release = asyncio.Event()
    durable_first_turn = ReviewTurn("result", 1, durable_trace)
    durable_first = ReviewAgent(
        [durable_first_turn, ReviewTurn("result", 3, durable_trace)],
        durable_trace,
    )
    durable_second = ReviewAgent(
        [ReviewTurn("result", 3, durable_trace)], durable_trace
    )
    durable_factory = ReviewFactory(
        [durable_first, durable_second], trace=durable_trace
    )
    durable_runtime = ReviewRuntime(["stop_requested", "running"])
    durable_batches = ReviewBatches(
        [1, 3],
        [
            ProjectBatchApplyResult("published"),
            ProjectBatchApplyResult("published"),
        ],
        prepare_entered=durable_prepare_entered,
        prepare_release=durable_prepare_release,
    )
    durable_worker = review_worker(
        durable_runtime, durable_batches, durable_factory
    )
    durable_one = review_start("durable-prepared-one")
    durable_two = review_start("durable-prepared-two")
    durable_task = asyncio.create_task(durable_worker.run_start(durable_one))
    try:
        await asyncio.wait_for(durable_prepare_entered.wait(), timeout=2)
        assert durable_worker.request_stop(
            review_stop_request(durable_one)
        ) is True
        durable_prepare_release.set()
        await asyncio.wait_for(durable_task, timeout=2)
    finally:
        durable_prepare_release.set()
        if not durable_task.done():
            durable_task.cancel()
        await asyncio.gather(durable_task, return_exceptions=True)
    assert durable_first_turn.cancel_count == 1
    assert durable_first_turn.quiescent_count >= 1
    assert [name for name, _ in durable_batches.calls] == [
        "history", "prepare", "apply",
    ]
    assert durable_runtime.calls.count(
        ("ack", durable_one.claim.turn_id)
    ) == 1
    assert ("commit", durable_one.claim.turn_id) not in durable_runtime.calls
    assert durable_factory.releases == [durable_first]
    await durable_worker.run_start(durable_two)
    assert durable_factory.created == [durable_first, durable_second]
    assert durable_factory.releases.count(durable_first) == 1

    # External cancellation owns the outcome even when result shields a
    # background task. Cleanup must cancel once, reap/quiesce, release, and
    # remove the public stop lookup in that exact order.
    class BlockingControlRuntime(ReviewRuntime):
        def __init__(self):
            super().__init__()
            self.control_entered = asyncio.Event()
            self.control_release = asyncio.Event()
            self.control_cancelled = asyncio.Event()
            self.control_reaped = asyncio.Event()

        async def control_for_claim(self, value):
            self.calls.append(("control", value.turn_id))
            self.control_entered.set()
            try:
                await self.control_release.wait()
                return runtime_module.ClaimControl(
                    "running", 7, value.lease_expires_at
                )
            except asyncio.CancelledError:
                self.control_cancelled.set()
                raise
            finally:
                self.control_reaped.set()

    cancel_trace = []
    cancel_turn = ReviewTurn("shielded", trace=cancel_trace)
    cancel_agent = ReviewAgent([cancel_turn], trace=cancel_trace)
    cancel_factory = ReviewFactory([cancel_agent], trace=cancel_trace)
    cancel_runtime = BlockingControlRuntime()
    cancel_batches = ReviewBatches([1])
    cancel_worker = review_worker(
        cancel_runtime, cancel_batches, cancel_factory, interval=0.001
    )
    cancel_start = review_start("outer-cancel")
    cancelling = asyncio.create_task(cancel_worker.run_start(cancel_start))
    try:
        await asyncio.wait_for(cancel_turn.entered.wait(), timeout=2)
        await asyncio.wait_for(cancel_runtime.control_entered.wait(), timeout=2)
        cancelling.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(cancelling), timeout=2)
    finally:
        cancel_runtime.control_release.set()
        cancel_turn.cancel_event.set()
        if cancel_turn.background is not None:
            await asyncio.wait_for(cancel_turn.background, timeout=2)
        if not cancelling.done():
            cancelling.cancel()
        await asyncio.gather(cancelling, return_exceptions=True)
    assert cancel_turn.cancel_count == 1
    assert cancel_turn.quiescent_count == 1
    assert cancel_runtime.control_cancelled.is_set()
    assert cancel_runtime.control_reaped.is_set()
    assert cancel_trace.index("cancel") < cancel_trace.index(
        "quiescent"
    ) < cancel_trace.index("release")
    assert cancel_worker.request_stop(review_stop_request(cancel_start)) is False
    assert cancel_batches.calls == [("history", "review-session")]
    assert not any(name in {"commit", "ack"} for name, _ in cancel_runtime.calls)

    # Cancellation can arrive while the first post-result watch reap is
    # itself waiting for cancelled control cleanup. It remains authoritative,
    # skips every terminal side effect, and discards the mutated parent.
    class StopWatchCancellationRuntime(ReviewRuntime):
        def __init__(self):
            super().__init__()
            self.control_entered = asyncio.Event()
            self.control_hold = asyncio.Event()
            self.control_cancelled = asyncio.Event()
            self.control_cleanup_gate = asyncio.Event()
            self.control_cleanup_finished = asyncio.Event()
            self.blocked_once = False

        async def control_for_claim(self, value):
            self.calls.append(("control", value.turn_id))
            if not self.blocked_once:
                self.blocked_once = True
                self.control_entered.set()
                try:
                    await self.control_hold.wait()
                except asyncio.CancelledError:
                    self.control_cancelled.set()
                    await self.control_cleanup_gate.wait()
                    self.control_cleanup_finished.set()
                    raise
            return runtime_module.ClaimControl(
                "running", 7, value.lease_expires_at
            )

    stop_watch_trace = []
    stop_watch_turn = ReviewTurn(
        "gated_result", 1, stop_watch_trace
    )
    stop_watch_first = ReviewAgent(
        [
            stop_watch_turn,
            ReviewTurn("result", 1, stop_watch_trace),
        ],
        stop_watch_trace,
    )
    stop_watch_second = ReviewAgent(
        [ReviewTurn("result", 1, stop_watch_trace)],
        stop_watch_trace,
    )
    stop_watch_factory = ReviewFactory(
        [stop_watch_first, stop_watch_second],
        trace=stop_watch_trace,
    )
    stop_watch_runtime = StopWatchCancellationRuntime()
    stop_watch_batches = ReviewBatches([1, 1])
    stop_watch_worker = review_worker(
        stop_watch_runtime,
        stop_watch_batches,
        stop_watch_factory,
        interval=0.001,
    )
    stop_watch_one = review_start("stop-watch-cancel-one")
    stop_watch_two = review_start("stop-watch-cancel-two")
    stop_watch_task = asyncio.create_task(
        stop_watch_worker.run_start(stop_watch_one)
    )
    try:
        await asyncio.wait_for(stop_watch_turn.entered.wait(), timeout=2)
        await asyncio.wait_for(
            stop_watch_runtime.control_entered.wait(), timeout=2
        )
        stop_watch_turn.result_gate.set()
        await asyncio.wait_for(
            stop_watch_runtime.control_cancelled.wait(), timeout=2
        )
        stop_watch_task.cancel()
        cancellation_delivery = asyncio.Event()
        asyncio.get_running_loop().call_soon(cancellation_delivery.set)
        await asyncio.wait_for(cancellation_delivery.wait(), timeout=2)
        assert stop_watch_task.cancelling() > 0
        assert stop_watch_task.done() is False
        stop_watch_runtime.control_cleanup_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(
                asyncio.shield(stop_watch_task), timeout=2
            )
    finally:
        stop_watch_turn.result_gate.set()
        stop_watch_turn.cancel_event.set()
        stop_watch_runtime.control_hold.set()
        stop_watch_runtime.control_cleanup_gate.set()
        if not stop_watch_task.done():
            stop_watch_task.cancel()
        await asyncio.wait_for(
            asyncio.gather(stop_watch_task, return_exceptions=True),
            timeout=2,
        )
    assert stop_watch_runtime.control_cleanup_finished.is_set()
    assert [name for name, _ in stop_watch_batches.calls] == ["history"]
    assert not any(
        name in {"commit", "ack"} for name, _ in stop_watch_runtime.calls
    )
    assert stop_watch_turn.cancel_count == 1
    assert stop_watch_turn.quiescent_count == 1
    stop_watch_cleanup = stop_watch_trace[
        stop_watch_trace.index("cancel"):
    ]
    assert stop_watch_cleanup == ["cancel", "quiescent", "release"]
    assert stop_watch_factory.releases == [stop_watch_first]
    assert stop_watch_worker.request_stop(
        review_stop_request(stop_watch_one)
    ) is False
    await asyncio.wait_for(
        stop_watch_worker.run_start(stop_watch_two), timeout=2
    )
    assert stop_watch_factory.created == [
        stop_watch_first, stop_watch_second,
    ]
    assert stop_watch_factory.releases.count(stop_watch_first) == 1

    # A watch failure is primary even when its cancel hook raises after
    # latching cancellation and releasing result(). Cleanup never retries the
    # hook and the unchanged-history follow-up reconstructs a fresh parent.
    watch_primary_error = RuntimeError("review watch primary")
    cancel_cleanup_error = RuntimeError("review cancel cleanup")

    class RaisingCancelTurn(ReviewTurn):
        def request_cancel(self):
            self.cancel_count += 1
            self.trace.append("cancel")
            self.cancel_event.set()
            raise cancel_cleanup_error

        async def result(self):
            self.entered.set()
            self.trace.append("result")
            await self.cancel_event.wait()
            return worker_module.ProjectAgentRunResult(
                "succeeded",
                self.base_count,
                (
                    {"role": "user", "content": "review"},
                    {"role": "assistant", "content": "done"},
                ),
            )

    class WatchPrimaryRuntime(ReviewRuntime):
        def __init__(self):
            super().__init__()
            self.watch_raised = asyncio.Event()
            self.raise_once = True

        async def control_for_claim(self, value):
            self.calls.append(("control", value.turn_id))
            if self.raise_once:
                self.raise_once = False
                self.watch_raised.set()
                raise watch_primary_error
            return runtime_module.ClaimControl(
                "running", 7, value.lease_expires_at
            )

    watch_primary_trace = []
    watch_primary_turn = RaisingCancelTurn(
        "result", 1, watch_primary_trace
    )
    watch_primary_first = ReviewAgent(
        [
            watch_primary_turn,
            ReviewTurn("result", 1, watch_primary_trace),
        ],
        watch_primary_trace,
    )
    watch_primary_second = ReviewAgent(
        [ReviewTurn("result", 1, watch_primary_trace)],
        watch_primary_trace,
    )
    watch_primary_factory = ReviewFactory(
        [watch_primary_first, watch_primary_second],
        trace=watch_primary_trace,
    )
    watch_primary_runtime = WatchPrimaryRuntime()
    watch_primary_batches = ReviewBatches([1, 1])
    watch_primary_worker = review_worker(
        watch_primary_runtime,
        watch_primary_batches,
        watch_primary_factory,
        interval=0.001,
    )
    watch_primary_one = review_start("watch-primary-one")
    watch_primary_two = review_start("watch-primary-two")
    watch_primary_task = asyncio.create_task(
        watch_primary_worker.run_start(watch_primary_one)
    )
    try:
        await asyncio.wait_for(watch_primary_turn.entered.wait(), timeout=2)
        await asyncio.wait_for(
            watch_primary_runtime.watch_raised.wait(), timeout=2
        )
        with pytest.raises(RuntimeError) as watch_primary_info:
            await asyncio.wait_for(
                asyncio.shield(watch_primary_task), timeout=2
            )
        assert watch_primary_info.value is watch_primary_error
        assert watch_primary_info.value is not cancel_cleanup_error
    finally:
        watch_primary_turn.cancel_event.set()
        if not watch_primary_task.done():
            watch_primary_task.cancel()
        await asyncio.wait_for(
            asyncio.gather(
                watch_primary_task, return_exceptions=True
            ),
            timeout=2,
        )
    assert watch_primary_turn.cancel_count == 1
    assert watch_primary_turn.quiescent_count == 1
    watch_primary_cleanup = watch_primary_trace[
        watch_primary_trace.index("cancel"):
    ]
    assert watch_primary_cleanup == ["cancel", "quiescent", "release"]
    assert [name for name, _ in watch_primary_batches.calls] == ["history"]
    assert not any(
        name in {"commit", "ack"} for name, _ in watch_primary_runtime.calls
    )
    assert watch_primary_factory.releases == [watch_primary_first]
    assert watch_primary_worker.request_stop(
        review_stop_request(watch_primary_one)
    ) is False
    await asyncio.wait_for(
        watch_primary_worker.run_start(watch_primary_two), timeout=2
    )
    assert watch_primary_factory.created == [
        watch_primary_first, watch_primary_second,
    ]
    assert watch_primary_factory.releases.count(watch_primary_first) == 1

    # Duplicate cleanup must never pop/deactivate the incumbent entry.
    duplicate_trace = []
    incumbent_turn = ReviewTurn("stop_block", trace=duplicate_trace)
    duplicate_turn = ReviewTurn("result", trace=duplicate_trace)
    incumbent_agent = ReviewAgent([incumbent_turn], trace=duplicate_trace)
    duplicate_agent = ReviewAgent([duplicate_turn], trace=duplicate_trace)
    duplicate_factory = ReviewFactory(
        [incumbent_agent, duplicate_agent],
        trace=duplicate_trace,
    )
    duplicate_runtime = ReviewRuntime(["stop_requested"])
    duplicate_batches = ReviewBatches([1, 1])
    duplicate_worker = review_worker(
        duplicate_runtime, duplicate_batches, duplicate_factory
    )
    duplicate_start = review_start("duplicate-live")
    incumbent = asyncio.create_task(
        duplicate_worker.run_start(duplicate_start)
    )
    duplicate = None
    try:
        await asyncio.wait_for(incumbent_turn.entered.wait(), timeout=2)
        duplicate = asyncio.create_task(
            duplicate_worker.run_start(duplicate_start)
        )
        with pytest.raises(Exception):
            await asyncio.wait_for(
                asyncio.shield(duplicate), timeout=2
            )
        assert duplicate_turn.cancel_count == 1
        assert duplicate_turn.quiescent_count == 1
        duplicate_cancel_index = duplicate_trace.index("cancel")
        duplicate_quiescent_index = duplicate_trace.index(
            "quiescent", duplicate_cancel_index + 1
        )
        duplicate_release_index = duplicate_trace.index(
            "release", duplicate_quiescent_index + 1
        )
        assert (
            duplicate_cancel_index
            < duplicate_quiescent_index
            < duplicate_release_index
        )
        assert duplicate_factory.releases == [duplicate_agent]
        assert duplicate_worker.request_stop(
            review_stop_request(duplicate_start)
        ) is True
        await asyncio.wait_for(asyncio.shield(incumbent), timeout=2)
    finally:
        incumbent_turn.cancel_event.set()
        duplicate_turn.cancel_event.set()
        for task in (duplicate, incumbent):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.wait_for(
            asyncio.gather(
                *(
                    task
                    for task in (duplicate, incumbent)
                    if task is not None
                ),
                return_exceptions=True,
            ),
            timeout=2,
        )
    assert incumbent_turn.cancel_count == 1
    assert duplicate_factory.releases == [
        duplicate_agent, incumbent_agent,
    ]
    assert duplicate_factory.releases.count(duplicate_agent) == 1
    assert duplicate_factory.releases.count(incumbent_agent) == 1
    assert duplicate_worker.request_stop(
        review_stop_request(duplicate_start)
    ) is False

    # A release-originated cancellation is cleanup noise, not permission to
    # replace the runner's primary error.
    async def cancelled_release(_agent):
        raise asyncio.CancelledError()

    primary_turn = ReviewTurn("error")
    primary_agent = ReviewAgent([primary_turn])
    primary_factory = ReviewFactory(
        [primary_agent], release_hook=cancelled_release
    )
    primary_runtime = ReviewRuntime()
    primary_batches = ReviewBatches([1])
    primary_worker = review_worker(
        primary_runtime, primary_batches, primary_factory
    )
    primary_start = review_start("release-cancel")
    with pytest.raises(RuntimeError, match="review runner primary"):
        await primary_worker.run_start(primary_start)
    assert primary_factory.releases == [primary_agent]
    assert primary_turn.cancel_count == 1
    assert primary_turn.quiescent_count == 1
    assert primary_worker.request_stop(review_stop_request(primary_start)) is False

    # All declared ports are constructor contracts, before mark/start can
    # mutate Projects authority.
    port_methods = {
        "runtime": (
            "mark_turn_started", "execution_input_for_claim",
            "heartbeat_turn", "control_for_claim",
            "commit_turn_with_task7_batch", "acknowledge_stopped",
        ),
        "batches": (
            "load_project_history", "prepare_terminal_result",
            "prepare_approval_checkpoint", "apply_project_batch",
        ),
        "factory": (
            "resolve_project_agent", "release_project_agent",
        ),
    }
    for port_name, method_names in port_methods.items():
        for method_name in method_names:
            matrix_runtime = ReviewRuntime()
            matrix_batches = ReviewBatches([1])
            matrix_factory = ReviewFactory(
                [ReviewAgent([ReviewTurn()])]
            )
            matrix_port = {
                "runtime": matrix_runtime,
                "batches": matrix_batches,
                "factory": matrix_factory,
            }[port_name]
            setattr(matrix_port, method_name, None)
            with pytest.raises(Exception):
                review_worker(
                    matrix_runtime, matrix_batches, matrix_factory
                )
            assert matrix_runtime.calls == []
            assert matrix_batches.calls == []
            assert matrix_factory.resolve_calls == 0
            assert matrix_factory.created == []
            assert matrix_factory.releases == []

    # mark_turn_started may extend only the lease horizon. Rotating immutable
    # attempt/session identity is rejected before all downstream consumers.
    rotated_values = {
        "project_id": "rotated-project",
        "turn_id": "rotated-turn",
        "sequence": 999,
        "worker_id": "rotated-worker",
        "attempt_id": "rotated-attempt",
        "lease_generation": 9,
        "fencing_token": 9,
        "canonical_session_id": "rotated-session",
    }
    for field_name, field_value in rotated_values.items():
        rotated_start = review_start(f"rotated-{field_name}")
        rotated_runtime = ReviewRuntime(
            mark_result=lambda value, name=field_name, new=field_value: (
                replace(value, **{name: new})
            )
        )
        rotated_batches = ReviewBatches([1])
        rotated_factory = ReviewFactory(
            [ReviewAgent([ReviewTurn()])]
        )
        rotated_worker = review_worker(
            rotated_runtime, rotated_batches, rotated_factory
        )
        with pytest.raises(Exception):
            await rotated_worker.run_start(rotated_start)
        assert rotated_runtime.calls == [
            ("mark", rotated_start.claim.turn_id)
        ]
        assert rotated_batches.calls == []
        assert rotated_factory.resolve_calls == 0
        assert rotated_factory.created == []

    class TurnClaimSubclass(runtime_module.TurnClaim):
        pass

    def claim_subclass(value):
        return TurnClaimSubclass(
            value.turn_id,
            value.project_id,
            value.sequence,
            value.worker_id,
            value.attempt_id,
            value.lease_generation,
            value.fencing_token,
            value.lease_expires_at,
            value.canonical_session_id,
        )

    def claim_duck(value):
        return SimpleNamespace(
            turn_id=value.turn_id,
            project_id=value.project_id,
            sequence=value.sequence,
            worker_id=value.worker_id,
            attempt_id=value.attempt_id,
            lease_generation=value.lease_generation,
            fencing_token=value.fencing_token,
            lease_expires_at=value.lease_expires_at,
            canonical_session_id=value.canonical_session_id,
        )

    for carrier_label, carrier in (
        ("duck", claim_duck),
        ("subclass", claim_subclass),
    ):
        carrier_start = review_start(f"mark-carrier-{carrier_label}")
        carrier_runtime = ReviewRuntime(mark_result=carrier)
        carrier_batches = ReviewBatches([1])
        carrier_factory = ReviewFactory(
            [ReviewAgent([ReviewTurn()])]
        )
        carrier_worker = review_worker(
            carrier_runtime, carrier_batches, carrier_factory
        )
        with pytest.raises(Exception):
            await carrier_worker.run_start(carrier_start)
        assert carrier_runtime.calls == [
            ("mark", carrier_start.claim.turn_id)
        ]
        assert carrier_batches.calls == []
        assert carrier_factory.resolve_calls == 0
        assert carrier_factory.created == []

    for invalid_horizon in (0, -1, True, 129, 131.0):
        invalid_horizon_start = review_start(
            f"invalid-horizon-{invalid_horizon!r}"
        )
        invalid_horizon_runtime = ReviewRuntime(
            mark_result=lambda value, horizon=invalid_horizon: replace(
                value, lease_expires_at=horizon
            )
        )
        invalid_horizon_batches = ReviewBatches([1])
        invalid_horizon_factory = ReviewFactory(
            [ReviewAgent([ReviewTurn()])]
        )
        invalid_horizon_worker = review_worker(
            invalid_horizon_runtime,
            invalid_horizon_batches,
            invalid_horizon_factory,
        )
        with pytest.raises(Exception):
            await invalid_horizon_worker.run_start(
                invalid_horizon_start
            )
        assert invalid_horizon_runtime.calls == [
            ("mark", invalid_horizon_start.claim.turn_id)
        ]
        assert invalid_horizon_batches.calls == []
        assert invalid_horizon_factory.resolve_calls == 0
        assert invalid_horizon_factory.created == []

    class HorizonRuntime(ReviewRuntime):
        def __init__(self):
            super().__init__(
                ["running"],
                mark_result=lambda value: replace(
                    value, lease_expires_at=131
                ),
            )
            self.horizons = []

        async def execution_input_for_claim(self, value):
            self.horizons.append(("input", value.lease_expires_at))
            return await super().execution_input_for_claim(value)

        async def control_for_claim(self, value):
            self.horizons.append(("control", value.lease_expires_at))
            return await super().control_for_claim(value)

        async def commit_turn_with_task7_batch(
            self, value, result, *, transcript_batch_id
        ):
            self.horizons.append(("commit", value.lease_expires_at))
            return await super().commit_turn_with_task7_batch(
                value,
                result,
                transcript_batch_id=transcript_batch_id,
            )

    class HorizonBatches(ReviewBatches):
        def __init__(self):
            super().__init__([1])
            self.prepare_horizons = []

        async def prepare_terminal_result(
            self,
            value,
            *,
            batch_id,
            status,
            base_message_count,
            messages,
        ):
            self.prepare_horizons.append(value.lease_expires_at)
            return await super().prepare_terminal_result(
                value,
                batch_id=batch_id,
                status=status,
                base_message_count=base_message_count,
                messages=messages,
            )

    horizon_start = review_start("advanced-horizon")
    horizon_runtime = HorizonRuntime()
    horizon_batches = HorizonBatches()
    horizon_agent = ReviewAgent([ReviewTurn("result", 1)])
    horizon_factory = ReviewFactory([horizon_agent])
    horizon_worker = review_worker(
        horizon_runtime, horizon_batches, horizon_factory
    )
    await asyncio.wait_for(
        horizon_worker.run_start(horizon_start), timeout=2
    )
    assert horizon_runtime.horizons == [
        ("input", 131),
        ("control", 131),
        ("commit", 131),
    ]
    assert horizon_batches.prepare_horizons == [131]
    assert ("commit", horizon_start.claim.turn_id) in horizon_runtime.calls
    assert horizon_batches.calls[-1][0] == "apply"

    # Keep the legal extension assertion local: callers downstream of
    # mark-start must receive the current claim, never the stale start claim.
    assert horizon_runtime.calls[:2] == [
        ("mark", horizon_start.claim.turn_id),
        ("input", horizon_start.claim.turn_id),
    ]

    # Duck carriers and every frozen PendingProjectBatch identity field must
    # fail before the closer. Each follow-up has a matching cache baseline so
    # reconstruction, rather than an incidental count miss, proves no reuse.
    class PendingProjectBatchSubclass(PendingProjectBatch):
        pass

    class TurnAttemptIdentitySubclass(
        runtime_module.TurnAttemptIdentity
    ):
        pass

    def pending_subclass(pending):
        return PendingProjectBatchSubclass(
            pending.batch_id,
            pending.batch_creation_sequence,
            pending.kind,
            pending.state,
            pending.attempt,
            pending.terminal_status,
            pending.operation_id,
            pending.approval_id,
            pending.base_message_count,
            pending.created_at,
        )

    def attempt_duck(pending):
        attempt = pending.attempt
        return replace(
            pending,
            attempt=SimpleNamespace(
                project_id=attempt.project_id,
                turn_id=attempt.turn_id,
                sequence=attempt.sequence,
                worker_id=attempt.worker_id,
                attempt_id=attempt.attempt_id,
                lease_generation=attempt.lease_generation,
                fencing_token=attempt.fencing_token,
                canonical_session_id=attempt.canonical_session_id,
                lease_expires_at=attempt.lease_expires_at,
            ),
        )

    def attempt_subclass(pending):
        attempt = pending.attempt
        return replace(
            pending,
            attempt=TurnAttemptIdentitySubclass(
                attempt.project_id,
                attempt.turn_id,
                attempt.sequence,
                attempt.worker_id,
                attempt.attempt_id,
                attempt.lease_generation,
                attempt.fencing_token,
                attempt.canonical_session_id,
                attempt.lease_expires_at,
            ),
        )

    prepared_mutations = [
        ("duck", lambda pending: SimpleNamespace(batch_id=pending.batch_id)),
        ("subclass", pending_subclass),
        ("attempt-duck", attempt_duck),
        ("attempt-subclass", attempt_subclass),
        (
            "batch-id",
            lambda pending: replace(
                pending,
                batch_id="ffffffff-0000-4000-8000-000000000000",
            ),
        ),
        (
            "batch-creation-sequence",
            lambda pending: replace(
                pending, batch_creation_sequence=0
            ),
        ),
        ("kind", lambda pending: replace(pending, kind="approval_checkpoint")),
        ("state", lambda pending: replace(pending, state="published")),
    ]
    attempt_mutations = {
        "project_id": "forged-project",
        "turn_id": "forged-turn",
        "sequence": 999,
        "worker_id": "forged-worker",
        "attempt_id": "forged-attempt",
        "lease_generation": 9,
        "fencing_token": 9,
        "canonical_session_id": "forged-session",
        "lease_expires_at": 999,
    }
    for attempt_field, attempt_value in attempt_mutations.items():
        prepared_mutations.append(
            (
                f"attempt-{attempt_field}",
                lambda pending, field=attempt_field, value=attempt_value: (
                    replace(
                        pending,
                        attempt=replace(
                            pending.attempt, **{field: value}
                        ),
                    )
                ),
            )
        )
    prepared_mutations.extend(
        (
            (
                "terminal-status",
                lambda pending: replace(pending, terminal_status="failed"),
            ),
            (
                "operation-id",
                lambda pending: replace(pending, operation_id="forged-op"),
            ),
            (
                "approval-id",
                lambda pending: replace(
                    pending, approval_id="forged-approval"
                ),
            ),
            (
                "base-count",
                lambda pending: replace(
                    pending, base_message_count=pending.base_message_count + 1
                ),
            ),
            (
                "created-at",
                lambda pending: replace(
                    pending, created_at=float("nan")
                ),
            ),
        )
    )
    for mutation_label, mutation in prepared_mutations:
        prepare_trace = []
        invalid_turn = ReviewTurn("result", 1, prepare_trace)
        invalid_agent = ReviewAgent(
            [invalid_turn, ReviewTurn("result", 3, prepare_trace)],
            prepare_trace,
        )
        replacement_agent = ReviewAgent(
            [ReviewTurn("result", 3, prepare_trace)], prepare_trace
        )
        prepare_factory = ReviewFactory(
            [invalid_agent, replacement_agent], trace=prepare_trace
        )
        prepare_runtime = ReviewRuntime(["running"])
        prepare_batches = ReviewBatches(
            [1, 3], prepared_transforms=[mutation, None]
        )
        prepare_worker = review_worker(
            prepare_runtime, prepare_batches, prepare_factory
        )
        invalid_start = review_start(f"prepared-{mutation_label}-bad")
        replacement_start = review_start(
            f"prepared-{mutation_label}-next"
        )
        with pytest.raises(Exception):
            await prepare_worker.run_start(invalid_start)
        assert not any(
            name in {"control", "commit", "ack"}
            for name, _ in prepare_runtime.calls
        )
        assert not any(name == "apply" for name, _ in prepare_batches.calls)
        assert invalid_turn.cancel_count == 1
        assert invalid_turn.quiescent_count == 1
        assert prepare_trace.index("cancel") < prepare_trace.index(
            "quiescent"
        ) < prepare_trace.index("release")
        assert prepare_factory.releases == [invalid_agent]
        await prepare_worker.run_start(replacement_start)
        assert prepare_factory.created == [invalid_agent, replacement_agent]
        assert prepare_factory.releases.count(invalid_agent) == 1

    # Authority failures are distinct from invalid returned carriers. The
    # original exception object remains primary while the checked-out runner
    # is cancelled, quiesced, released, and never reused.
    prepare_primary_error = RuntimeError("review prepare primary")

    def raise_prepare_primary(_pending):
        raise prepare_primary_error

    prepare_error_trace = []
    prepare_error_turn = ReviewTurn("result", 1, prepare_error_trace)
    prepare_error_first = ReviewAgent(
        [
            prepare_error_turn,
            ReviewTurn("result", 3, prepare_error_trace),
        ],
        prepare_error_trace,
    )
    prepare_error_second = ReviewAgent(
        [ReviewTurn("result", 3, prepare_error_trace)],
        prepare_error_trace,
    )
    prepare_error_factory = ReviewFactory(
        [prepare_error_first, prepare_error_second],
        trace=prepare_error_trace,
    )
    prepare_error_runtime = ReviewRuntime(["running"])
    prepare_error_batches = ReviewBatches(
        [1, 3],
        prepared_transforms=[raise_prepare_primary, None],
    )
    prepare_error_worker = review_worker(
        prepare_error_runtime,
        prepare_error_batches,
        prepare_error_factory,
    )
    prepare_error_one = review_start("prepare-primary-one")
    prepare_error_two = review_start("prepare-primary-two")
    with pytest.raises(RuntimeError) as prepare_primary_info:
        await asyncio.wait_for(
            prepare_error_worker.run_start(prepare_error_one), timeout=2
        )
    assert prepare_primary_info.value is prepare_primary_error
    assert not any(
        name in {"control", "commit", "ack"}
        for name, _ in prepare_error_runtime.calls
    )
    assert [name for name, _ in prepare_error_batches.calls] == [
        "history", "prepare",
    ]
    assert prepare_error_turn.cancel_count == 1
    assert prepare_error_turn.quiescent_count == 1
    prepare_error_cleanup = prepare_error_trace[
        prepare_error_trace.index("cancel"):
    ]
    assert prepare_error_cleanup == ["cancel", "quiescent", "release"]
    assert prepare_error_factory.releases == [prepare_error_first]
    await asyncio.wait_for(
        prepare_error_worker.run_start(prepare_error_two), timeout=2
    )
    assert prepare_error_factory.created == [
        prepare_error_first, prepare_error_second,
    ]
    assert prepare_error_factory.releases.count(prepare_error_first) == 1

    apply_primary_error = RuntimeError("review apply primary")
    apply_error_trace = []
    apply_error_turn = ReviewTurn("result", 1, apply_error_trace)
    apply_error_first = ReviewAgent(
        [
            apply_error_turn,
            ReviewTurn("result", 3, apply_error_trace),
        ],
        apply_error_trace,
    )
    apply_error_second = ReviewAgent(
        [ReviewTurn("result", 3, apply_error_trace)],
        apply_error_trace,
    )
    apply_error_factory = ReviewFactory(
        [apply_error_first, apply_error_second],
        trace=apply_error_trace,
    )
    apply_error_runtime = ReviewRuntime(["running", "running"])
    apply_error_batches = ReviewBatches(
        [1, 3],
        [
            apply_primary_error,
            ProjectBatchApplyResult("published"),
        ],
    )
    apply_error_worker = review_worker(
        apply_error_runtime,
        apply_error_batches,
        apply_error_factory,
    )
    apply_error_one = review_start("apply-primary-one")
    apply_error_two = review_start("apply-primary-two")
    with pytest.raises(RuntimeError) as apply_primary_info:
        await asyncio.wait_for(
            apply_error_worker.run_start(apply_error_one), timeout=2
        )
    assert apply_primary_info.value is apply_primary_error
    assert apply_error_runtime.calls.count(
        ("control", apply_error_one.claim.turn_id)
    ) == 1
    assert apply_error_runtime.calls.count(
        ("commit", apply_error_one.claim.turn_id)
    ) == 1
    assert ("ack", apply_error_one.claim.turn_id) not in apply_error_runtime.calls
    assert [name for name, _ in apply_error_batches.calls] == [
        "history", "prepare", "apply",
    ]
    assert apply_error_turn.cancel_count == 1
    assert apply_error_turn.quiescent_count == 2
    apply_error_cleanup = apply_error_trace[
        apply_error_trace.index("cancel"):
    ]
    assert apply_error_cleanup == ["cancel", "quiescent", "release"]
    assert apply_error_factory.releases == [apply_error_first]
    await asyncio.wait_for(
        apply_error_worker.run_start(apply_error_two), timeout=2
    )
    assert apply_error_factory.created == [
        apply_error_first, apply_error_second,
    ]
    assert apply_error_factory.releases.count(apply_error_first) == 1

    # A lookalike closer/apply result may not promote. A subsequent exact
    # compatible run therefore constructs a different parent.
    class ProjectBatchApplyResultSubclass(ProjectBatchApplyResult):
        pass

    forged_apply_values = (
        ("duck", SimpleNamespace(outcome="published")),
        ("subclass", ProjectBatchApplyResultSubclass("published")),
        ("unsupported", ProjectBatchApplyResult("unsupported")),
    )
    for forged_label, forged_value in forged_apply_values:
        apply_trace = []
        forged_apply_turn = ReviewTurn("result", 1, apply_trace)
        forged_apply_first = ReviewAgent(
            [
                forged_apply_turn,
                ReviewTurn("result", 3, apply_trace),
            ],
            apply_trace,
        )
        forged_apply_second = ReviewAgent(
            [ReviewTurn("result", 3, apply_trace)], apply_trace
        )
        forged_apply_factory = ReviewFactory(
            [forged_apply_first, forged_apply_second], trace=apply_trace
        )
        forged_apply_runtime = ReviewRuntime(["running", "running"])
        forged_apply_batches = ReviewBatches(
            [1, 3],
            [forged_value, ProjectBatchApplyResult("published")],
        )
        forged_apply_worker = review_worker(
            forged_apply_runtime,
            forged_apply_batches,
            forged_apply_factory,
        )
        forged_apply_one = review_start(f"forged-{forged_label}-one")
        forged_apply_two = review_start(f"forged-{forged_label}-two")
        with pytest.raises(Exception):
            await forged_apply_worker.run_start(forged_apply_one)
        assert forged_apply_turn.cancel_count == 1
        assert forged_apply_turn.quiescent_count == 2
        apply_cleanup = apply_trace[apply_trace.index("cancel"):]
        assert apply_cleanup == ["cancel", "quiescent", "release"]
        assert forged_apply_factory.releases == [forged_apply_first]
        await forged_apply_worker.run_start(forged_apply_two)
        assert forged_apply_factory.created == [
            forged_apply_first, forged_apply_second,
        ]
        assert forged_apply_factory.releases.count(forged_apply_first) == 1

    # Exact supported outcomes that do not publish are normal non-promoting
    # exits. Every one still owns full cancellation/quiescence/release, and
    # a compatible follow-up must reconstruct rather than reuse mutation.
    nonpublishing_outcomes = (
        "wait",
        "discarded",
        "conflicted",
        "already_discarded",
        "already_conflicted",
        "settlement_pending",
        "remediation_pending",
        "state_conflict",
        "authority_conflict",
    )
    for nonpublishing_outcome in nonpublishing_outcomes:
        nonpublishing_trace = []
        nonpublishing_turn = ReviewTurn(
            "result", 1, nonpublishing_trace
        )
        nonpublishing_first = ReviewAgent(
            [
                nonpublishing_turn,
                ReviewTurn("result", 3, nonpublishing_trace),
            ],
            nonpublishing_trace,
        )
        nonpublishing_second = ReviewAgent(
            [ReviewTurn("result", 3, nonpublishing_trace)],
            nonpublishing_trace,
        )
        nonpublishing_factory = ReviewFactory(
            [nonpublishing_first, nonpublishing_second],
            trace=nonpublishing_trace,
        )
        nonpublishing_runtime = ReviewRuntime(["running", "running"])
        nonpublishing_batches = ReviewBatches(
            [1, 3],
            [
                ProjectBatchApplyResult(nonpublishing_outcome),
                ProjectBatchApplyResult("published"),
            ],
        )
        nonpublishing_worker = review_worker(
            nonpublishing_runtime,
            nonpublishing_batches,
            nonpublishing_factory,
        )
        nonpublishing_one = review_start(
            f"nonpublishing-{nonpublishing_outcome}-one"
        )
        nonpublishing_two = review_start(
            f"nonpublishing-{nonpublishing_outcome}-two"
        )
        await asyncio.wait_for(
            nonpublishing_worker.run_start(nonpublishing_one), timeout=2
        )
        assert nonpublishing_turn.cancel_count == 1
        assert nonpublishing_turn.quiescent_count == 2
        nonpublishing_cleanup = nonpublishing_trace[
            nonpublishing_trace.index("cancel"):
        ]
        assert nonpublishing_cleanup == [
            "cancel", "quiescent", "release",
        ]
        assert nonpublishing_factory.releases == [nonpublishing_first]
        assert nonpublishing_worker.request_stop(
            review_stop_request(nonpublishing_one)
        ) is False
        await asyncio.wait_for(
            nonpublishing_worker.run_start(nonpublishing_two), timeout=2
        )
        assert nonpublishing_factory.created == [
            nonpublishing_first, nonpublishing_second,
        ]
        assert (
            nonpublishing_factory.releases.count(nonpublishing_first)
            == 1
        )

    # Concurrent starts for one cache key may both construct while checked
    # out. B cannot finish or become reusable while release of displaced A is
    # gated. C starts during that gate with its own parent, later displaces B,
    # and only C is reused by the compatible follow-up.
    same_key_trace = []
    same_key_release_entered = asyncio.Event()
    same_key_release_finish = asyncio.Event()
    same_key_a_turn = ReviewTurn("gated_result", 1, same_key_trace)
    same_key_a_trap = ReviewTurn("gated_result", 3, same_key_trace)
    same_key_b_turn = ReviewTurn("gated_result", 1, same_key_trace)
    same_key_b_trap = ReviewTurn("gated_result", 3, same_key_trace)
    same_key_c_turn = ReviewTurn("gated_result", 3, same_key_trace)
    same_key_a = ReviewAgent(
        [same_key_a_turn, same_key_a_trap], same_key_trace
    )
    same_key_b = ReviewAgent(
        [same_key_b_turn, same_key_b_trap],
        same_key_trace,
    )
    same_key_c = ReviewAgent(
        [same_key_c_turn, ReviewTurn("result", 5, same_key_trace)],
        same_key_trace,
    )

    async def gate_same_key_displacement(agent):
        if agent is same_key_a:
            same_key_release_entered.set()
            await same_key_release_finish.wait()

    same_key_factory = ReviewFactory(
        [same_key_a, same_key_b, same_key_c],
        release_hook=gate_same_key_displacement,
        trace=same_key_trace,
    )
    same_key_runtime = ReviewRuntime(
        ["running", "running", "running", "running"]
    )
    same_key_batches = ReviewBatches([1, 1, 3, 5])
    same_key_worker = review_worker(
        same_key_runtime, same_key_batches, same_key_factory
    )
    same_key_a_start = review_start("same-key-a")
    same_key_b_start = review_start("same-key-b")
    same_key_c_start = review_start("same-key-c")
    same_key_followup = review_start("same-key-followup")
    same_key_a_task = asyncio.create_task(
        same_key_worker.run_start(same_key_a_start)
    )
    same_key_b_task = None
    same_key_c_task = None
    same_key_followup_task = None
    try:
        await asyncio.wait_for(same_key_a_turn.entered.wait(), timeout=2)
        same_key_b_task = asyncio.create_task(
            same_key_worker.run_start(same_key_b_start)
        )
        await asyncio.wait_for(same_key_b_turn.entered.wait(), timeout=2)
        assert same_key_factory.created == [same_key_a, same_key_b]

        same_key_a_turn.result_gate.set()
        await asyncio.wait_for(
            asyncio.shield(same_key_a_task), timeout=2
        )
        same_key_b_turn.result_gate.set()
        await asyncio.wait_for(
            same_key_release_entered.wait(), timeout=2
        )
        assert same_key_b_task.done() is False
        assert same_key_factory.releases == [same_key_a]

        same_key_c_task = asyncio.create_task(
            same_key_worker.run_start(same_key_c_start)
        )
        await asyncio.wait_for(same_key_c.created.wait(), timeout=2)
        await asyncio.wait_for(same_key_c_turn.entered.wait(), timeout=2)
        assert same_key_factory.created == [
            same_key_a, same_key_b, same_key_c,
        ]
        assert same_key_b_task.done() is False
        assert same_key_c_task.done() is False

        same_key_release_finish.set()
        await asyncio.wait_for(
            asyncio.shield(same_key_b_task), timeout=2
        )
        same_key_c_turn.result_gate.set()
        await asyncio.wait_for(
            asyncio.shield(same_key_c_task), timeout=2
        )
        assert same_key_factory.releases == [same_key_a, same_key_b]

        same_key_followup_task = asyncio.create_task(
            same_key_worker.run_start(same_key_followup)
        )
        await asyncio.wait_for(
            asyncio.shield(same_key_followup_task), timeout=2
        )
        assert same_key_factory.created == [
            same_key_a, same_key_b, same_key_c,
        ]
        assert same_key_factory.releases == [same_key_a, same_key_b]
    finally:
        same_key_release_finish.set()
        same_key_a_turn.result_gate.set()
        same_key_a_trap.result_gate.set()
        same_key_b_turn.result_gate.set()
        same_key_b_trap.result_gate.set()
        same_key_c_turn.result_gate.set()
        same_key_tasks = (
            same_key_a_task,
            same_key_b_task,
            same_key_c_task,
            same_key_followup_task,
        )
        for task in same_key_tasks:
            if task is not None and not task.done():
                task.cancel()
        await asyncio.wait_for(
            asyncio.gather(
                *(task for task in same_key_tasks if task is not None),
                return_exceptions=True,
            ),
            timeout=2,
        )

    # Capacity cancellation: the old idle release is the handoff gate. The
    # new parent is unpromoted and released before outer cancellation escapes;
    # an old-key probe cannot reuse the formally released old parent, and the
    # subsequent compatible new-key retry must also construct cleanly.
    eviction_trace = []
    eviction_entered = asyncio.Event()
    eviction_release = asyncio.Event()
    old_release_completed = asyncio.Event()
    old_eviction_turn = ReviewTurn("result", 1, eviction_trace)
    new_eviction_turn = ReviewTurn("result", 1, eviction_trace)
    old_eviction_agent = ReviewAgent(
        [
            old_eviction_turn,
            ReviewTurn("result", 3, eviction_trace),
        ],
        eviction_trace,
    )
    new_eviction_agent = ReviewAgent(
        [
            new_eviction_turn,
            ReviewTurn("result", 3, eviction_trace),
        ],
        eviction_trace,
    )
    old_probe_agent = ReviewAgent(
        [ReviewTurn("result", 3, eviction_trace)],
        eviction_trace,
    )
    retry_eviction_agent = ReviewAgent(
        [ReviewTurn("result", 3, eviction_trace)],
        eviction_trace,
    )

    async def block_old_eviction(agent):
        if agent is old_eviction_agent:
            eviction_entered.set()
            await eviction_release.wait()
            old_release_completed.set()

    eviction_factory = ReviewFactory(
        [
            old_eviction_agent,
            new_eviction_agent,
            old_probe_agent,
            retry_eviction_agent,
        ],
        release_hook=block_old_eviction,
        trace=eviction_trace,
    )
    eviction_runtime = ReviewRuntime(
        ["running", "running", "running", "running"]
    )
    eviction_batches = ReviewBatches([1, 1, 3, 3])
    eviction_worker = review_worker(
        eviction_runtime, eviction_batches, eviction_factory, capacity=1
    )
    old_start = review_start(
        "capacity-old", project_id="capacity-old", session_id="old-session"
    )
    new_start = review_start(
        "capacity-new", project_id="capacity-new", session_id="new-session"
    )
    old_probe_start = review_start(
        "capacity-old-probe",
        project_id="capacity-old",
        session_id="old-session",
    )
    retry_start = review_start(
        "capacity-retry", project_id="capacity-new", session_id="new-session"
    )
    await asyncio.wait_for(
        eviction_worker.run_start(old_start), timeout=2
    )
    eviction_task = asyncio.create_task(eviction_worker.run_start(new_start))
    try:
        await asyncio.wait_for(eviction_entered.wait(), timeout=2)
        eviction_cleanup_start = len(eviction_trace)
        eviction_quiescent_before = new_eviction_turn.quiescent_count
        eviction_task.cancel()
        eviction_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(eviction_task), timeout=2)
        assert old_release_completed.is_set()
    finally:
        eviction_release.set()
        if not eviction_task.done():
            eviction_task.cancel()
        await asyncio.wait_for(
            asyncio.gather(eviction_task, return_exceptions=True),
            timeout=2,
        )
    assert old_release_completed.is_set()
    eviction_cleanup = eviction_trace[eviction_cleanup_start:]
    assert new_eviction_turn.cancel_count == 1
    assert (
        new_eviction_turn.quiescent_count
        - eviction_quiescent_before
        == 1
    )
    assert eviction_cleanup == ["cancel", "quiescent", "release"]
    assert eviction_factory.releases == [
        old_eviction_agent, new_eviction_agent,
    ]
    await asyncio.wait_for(
        eviction_worker.run_start(old_probe_start), timeout=2
    )
    assert eviction_factory.created == [
        old_eviction_agent, new_eviction_agent, old_probe_agent,
    ]
    assert eviction_factory.releases == [
        old_eviction_agent, new_eviction_agent,
    ]
    await asyncio.wait_for(
        eviction_worker.run_start(retry_start), timeout=2
    )
    assert eviction_factory.created == [
        old_eviction_agent,
        new_eviction_agent,
        old_probe_agent,
        retry_eviction_agent,
    ]
    assert eviction_factory.releases == [
        old_eviction_agent, new_eviction_agent, old_probe_agent,
    ]
    assert eviction_factory.releases.count(old_eviction_agent) == 1
    assert eviction_factory.releases.count(new_eviction_agent) == 1
    assert eviction_factory.releases.count(old_probe_agent) == 1

    # A stale-CAS apply-only terminal does not mark the handle terminal-won.
    # An exact stop accepted while capacity handoff awaits old-owner release
    # therefore vetoes candidate promotion for both publishing outcomes.
    for handoff_outcome in ("published", "already_published"):
        handoff_trace = []
        handoff_release_entered = asyncio.Event()
        handoff_release_gate = asyncio.Event()
        handoff_release_completed = asyncio.Event()
        handoff_old_turn = ReviewTurn("result", 1, handoff_trace)
        handoff_candidate_turn = ReviewTurn(
            "result", 1, handoff_trace
        )
        handoff_old_agent = ReviewAgent(
            [handoff_old_turn], handoff_trace
        )
        handoff_candidate_agent = ReviewAgent(
            [
                handoff_candidate_turn,
                ReviewTurn("result", 3, handoff_trace),
            ],
            handoff_trace,
        )
        handoff_replacement_agent = ReviewAgent(
            [ReviewTurn("result", 3, handoff_trace)],
            handoff_trace,
        )

        async def gate_handoff_old_release(
            agent,
            *,
            old=handoff_old_agent,
            entered=handoff_release_entered,
            gate=handoff_release_gate,
            completed=handoff_release_completed,
        ):
            if agent is old:
                entered.set()
                await gate.wait()
                completed.set()

        handoff_factory = ReviewFactory(
            [
                handoff_old_agent,
                handoff_candidate_agent,
                handoff_replacement_agent,
            ],
            release_hook=gate_handoff_old_release,
            trace=handoff_trace,
        )
        handoff_old_start = review_start(
            f"handoff-{handoff_outcome}-old",
            project_id=f"handoff-{handoff_outcome}-old-project",
            session_id=f"handoff-{handoff_outcome}-old-session",
        )
        handoff_candidate_start = review_start(
            f"handoff-{handoff_outcome}-candidate",
            project_id=f"handoff-{handoff_outcome}-new-project",
            session_id=f"handoff-{handoff_outcome}-new-session",
        )
        handoff_followup_start = review_start(
            f"handoff-{handoff_outcome}-followup",
            project_id=f"handoff-{handoff_outcome}-new-project",
            session_id=f"handoff-{handoff_outcome}-new-session",
        )

        class HandoffStaleRuntime(ReviewRuntime):
            def __init__(self):
                super().__init__(
                    ["running", "running", "running", "running"]
                )
                self.stale_calls = 0

            async def commit_turn_with_task7_batch(
                self, value, result, *, transcript_batch_id
            ):
                if value.turn_id == handoff_candidate_start.claim.turn_id:
                    self.calls.append(("commit", value.turn_id))
                    self.stale_calls += 1
                    raise runtime_module.ProjectRuntimeError(
                        runtime_module.RuntimeErrorCode.STALE_TURN_CLAIM,
                        project_id=value.project_id,
                        turn_id=value.turn_id,
                        current_control_version=7,
                    )
                return await super().commit_turn_with_task7_batch(
                    value,
                    result,
                    transcript_batch_id=transcript_batch_id,
                )

        handoff_runtime = HandoffStaleRuntime()
        handoff_batches = ReviewBatches(
            [1, 1, 3],
            [
                ProjectBatchApplyResult("published"),
                ProjectBatchApplyResult(handoff_outcome),
                ProjectBatchApplyResult("published"),
            ],
        )
        handoff_worker = review_worker(
            handoff_runtime,
            handoff_batches,
            handoff_factory,
            capacity=1,
        )
        await asyncio.wait_for(
            handoff_worker.run_start(handoff_old_start), timeout=2
        )
        handoff_candidate_task = asyncio.create_task(
            handoff_worker.run_start(handoff_candidate_start)
        )
        try:
            await asyncio.wait_for(
                handoff_release_entered.wait(), timeout=2
            )
            assert handoff_worker.request_stop(
                review_stop_request(handoff_candidate_start)
            ) is True
            assert handoff_candidate_turn.cancel_count == 1
            handoff_release_gate.set()
            await asyncio.wait_for(
                asyncio.shield(handoff_candidate_task), timeout=2
            )
        finally:
            handoff_release_gate.set()
            handoff_candidate_turn.cancel_event.set()
            if not handoff_candidate_task.done():
                handoff_candidate_task.cancel()
            await asyncio.wait_for(
                asyncio.gather(
                    handoff_candidate_task,
                    return_exceptions=True,
                ),
                timeout=2,
            )
        assert handoff_release_completed.is_set()
        assert handoff_candidate_turn.cancel_count == 1
        handoff_cancel_index = handoff_trace.index("cancel")
        handoff_quiescent_index = handoff_trace.index(
            "quiescent", handoff_cancel_index + 1
        )
        handoff_candidate_release_index = handoff_trace.index(
            "release", handoff_quiescent_index + 1
        )
        assert (
            handoff_cancel_index
            < handoff_quiescent_index
            < handoff_candidate_release_index
        )
        assert handoff_factory.releases == [
            handoff_old_agent, handoff_candidate_agent,
        ]

        assert handoff_factory.releases.count(handoff_old_agent) == 1
        assert (
            handoff_factory.releases.count(handoff_candidate_agent)
            == 1
        )
        assert handoff_runtime.stale_calls == 1
        assert handoff_runtime.calls.count(
            ("control", handoff_candidate_start.claim.turn_id)
        ) == 2
        assert handoff_runtime.calls.count(
            ("commit", handoff_candidate_start.claim.turn_id)
        ) == 1
        assert (
            "ack", handoff_candidate_start.claim.turn_id
        ) not in handoff_runtime.calls
        assert handoff_worker.request_stop(
            review_stop_request(handoff_candidate_start)
        ) is False
        await asyncio.wait_for(
            handoff_worker.run_start(handoff_followup_start), timeout=2
        )
        assert handoff_factory.created == [
            handoff_old_agent,
            handoff_candidate_agent,
            handoff_replacement_agent,
        ]
        assert handoff_factory.releases == [
            handoff_old_agent, handoff_candidate_agent,
        ]

    # A control-watch stop whose cancel hook has already effected cancellation
    # before raising is cleanup noise. It must latch once, acknowledge the
    # durable stop batchlessly, and evict rather than reuse the parent.
    class StopHookNoiseTurn(ReviewTurn):
        def __init__(self, trace):
            super().__init__("stop_block", trace=trace)
            self.cancel_effected = asyncio.Event()

        def request_cancel(self):
            self.cancel_count += 1
            self.trace.append("hook.cancel")
            self.cancel_event.set()
            self.cancel_effected.set()
            raise RuntimeError("cancel hook cleanup noise")

        async def wait_quiescent(self):
            self.quiescent_count += 1
            self.trace.append("hook.quiescent")

        async def result(self):
            self.entered.set()
            self.trace.append("hook.result")
            await self.cancel_event.wait()
            raise asyncio.CancelledError()

    class StopHookNoiseRuntime(ReviewRuntime):
        def __init__(self, trace, stop_turn_id):
            super().__init__()
            self.trace = trace
            self.stop_turn_id = stop_turn_id
            self.watch_control_seen = asyncio.Event()

        async def control_for_claim(self, value):
            self.calls.append(("control", value.turn_id))
            if value.turn_id == self.stop_turn_id:
                self.watch_control_seen.set()
                return runtime_module.ClaimControl(
                    "stop_requested", 29, value.lease_expires_at
                )
            return runtime_module.ClaimControl(
                "running", 29, value.lease_expires_at
            )

        async def acknowledge_stopped(self, value):
            self.calls.append(("ack", value.turn_id))
            self.trace.append("runtime.ack")
            return object()

    hook_trace = []
    hook_turn = StopHookNoiseTurn(hook_trace)
    hook_first = ReviewAgent([hook_turn], hook_trace)
    hook_second = ReviewAgent(
        [ReviewTurn("result", 1, hook_trace)], hook_trace
    )
    hook_factory = ReviewFactory(
        [hook_first, hook_second], trace=hook_trace
    )
    hook_start = review_start("stop-hook-noise")
    hook_followup = review_start("stop-hook-followup")
    hook_runtime = StopHookNoiseRuntime(
        hook_trace, hook_start.claim.turn_id
    )
    hook_batches = ReviewBatches([1, 1])
    hook_worker = review_worker(
        hook_runtime, hook_batches, hook_factory, interval=0.001
    )
    hook_task = asyncio.create_task(hook_worker.run_start(hook_start))
    try:
        await asyncio.wait_for(hook_turn.entered.wait(), timeout=2)
        await asyncio.wait_for(
            hook_runtime.watch_control_seen.wait(), timeout=2
        )
        hook_outcome = (
            await asyncio.wait_for(
                asyncio.gather(hook_task, return_exceptions=True), timeout=2
            )
        )[0]
    finally:
        if not hook_task.done():
            hook_task.cancel()
        await asyncio.wait_for(
            asyncio.gather(hook_task, return_exceptions=True), timeout=2
        )
    assert hook_outcome is None
    assert hook_turn.cancel_count == 1
    assert hook_turn.cancel_effected.is_set()
    assert hook_turn.quiescent_count >= 1
    hook_cancel = hook_trace.index("hook.cancel")
    hook_quiescent = hook_trace.index("hook.quiescent", hook_cancel + 1)
    hook_ack = hook_trace.index("runtime.ack")
    hook_release = hook_trace.index("release")
    assert hook_cancel < hook_quiescent < hook_ack < hook_release
    assert hook_runtime.calls.count(("ack", hook_start.claim.turn_id)) == 1
    assert ("commit", hook_start.claim.turn_id) not in hook_runtime.calls
    assert hook_batches.calls == [("history", "review-session")]
    assert hook_factory.releases == [hook_first]
    assert hook_worker.request_stop(review_stop_request(hook_start, 29)) is False
    await asyncio.wait_for(hook_worker.run_start(hook_followup), timeout=2)
    assert hook_factory.created == [hook_first, hook_second]
    assert hook_factory.releases == [hook_first]


@pytest.mark.asyncio
async def test_task7_c14_composition_agent_factory_runs_sync_agent_off_loop_without_persistence_or_delivery(
    tmp_path,
    monkeypatch,
) -> None:
    """The concrete bridge is isolated, fail-closed, and exactly quiescent."""
    import gateway.session as session_module
    import gateway.project_runtime_worker as worker_module
    from gateway.config import GatewayConfig, Platform
    from gateway.session import (
        ProjectBatchApplyResult,
        ProjectHistorySnapshot,
        SessionContext,
        SessionSource,
    )
    from hermes_cli.project_operations import (
        OperationApprovalSpec,
        OperationIntent,
        OperationReadbackRequest,
        OperationReadbackResult,
        OperationReceipt,
        ProjectOperation,
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
    from hermes_cli import project_runtime as runtime_module
    from hermes_state import PendingProjectBatch

    required = (
        "GatewayProjectAgentFactory",
        "ProjectExecutionContext",
        "ProjectOperationProposal",
        "ProjectReadProposal",
        "BoundProjectOperationAuthority",
        "ProjectPolicyDecisionCarrier",
        "CertifiedProjectOperationExecutionRequest",
        "ProjectCheckpointFailed",
        "ProjectCheckpointSettlementPending",
        "ProjectApprovalPublished",
        "ProjectOperationUnresolved",
        "ProjectToolPolicySnapshotFacade",
        "ProjectOperationPrepareFacade",
        "ProjectOperationExecutionFacade",
        "CanonicalProjectOperationCheckpointCoordinator",
        "CanonicalProjectLiveOperationCoordinator",
        "CanonicalProjectOperationExecutionCoordinator",
        "CanonicalApprovedOperationTurn",
        "ApprovedOperationExecutionPort",
    )
    assert all(hasattr(worker_module, name) for name in required), (
        "C14 requires the concrete project-only agent/operation bridge"
    )
    factory_parameters = tuple(
        inspect.signature(
            worker_module.GatewayProjectAgentFactory
        ).parameters.values()
    )
    assert tuple(
        parameter.name for parameter in factory_parameters
    ) == (
        "snapshot_resolver",
        "agent_builder",
        "off_loop_runner",
        "turn_context_binder",
        "tool_authorizer",
        "checkpoint_coordinator",
    )
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        and parameter.default is inspect.Parameter.empty
        for parameter in factory_parameters
    )
    assert hasattr(
        session_module,
        "ProjectBatchWorkerFacade",
    ), "C14 requires the raw State worker facade"
    assert tuple(
        field.name
        for field in fields(worker_module.ProjectOperationProposal)
    ) == (
        "intent",
        "policy_batch_id",
        "effect_scope_json",
        "effect_scope_sha256",
        "capability_fingerprint",
    )
    assert tuple(
        field.name for field in fields(worker_module.ProjectReadProposal)
    ) == (
        "canonical_action",
        "targets",
        "policy_batch_id",
        "batch_items",
    )
    assert tuple(
        field.name
        for field in fields(
            worker_module.BoundProjectOperationAuthority
        )
    ) == (
        "command",
        "intent",
        "policy_batch_id",
        "effect_scope_json",
        "effect_scope_sha256",
        "authority_json",
        "authority_sha256",
    )
    assert tuple(
        field.name
        for field in fields(worker_module.ProjectPolicyDecisionCarrier)
    ) == (
        "execution_attempt",
        "execution_origin",
        "control_version",
        "runtime_version",
        "operation_authority",
        "project",
        "contract_id",
        "contract_status",
        "contract_json_sha256",
        "contract",
        "actor",
        "decision",
    )
    assert tuple(
        field.name
        for field in fields(
            worker_module.CertifiedProjectOperationExecutionRequest
        )
    ) == (
        "operation",
        "attempt",
        "payload",
        "approval_checkpoint_id",
        "operation_authority_json",
        "operation_authority_sha256",
        "effect_scope_json",
        "effect_scope_sha256",
        "policy_authority_sha256",
        "remote_idempotency_supported",
        "capability_fingerprint",
    )

    context = SessionContext(
        source=SessionSource(
            platform=Platform.LOCAL,
            chat_id="project:c14-project",
        ),
        connected_platforms=[],
        home_channels={},
        session_key="project:c14-project",
        session_id="c14-session",
    )
    history_messages = (
        {"role": "user", "content": "historical request"},
        {"role": "assistant", "content": "historical answer"},
    )
    history = ProjectHistorySnapshot(
        "c14-session",
        history_messages,
        9,
    )
    canonical_payload = (
        '{"message":"do not select this field",'
        '"nested":{"a":1,"z":2}}'
    )

    def attempt(
        suffix,
        *,
        horizon=190,
        attempt_id=None,
        worker_id="c14-worker",
        generation=3,
        fence=5,
        sequence=11,
    ):
        return runtime_module.TurnAttemptIdentity(
            "c14-project",
            f"turn-{suffix}",
            sequence,
            worker_id,
            attempt_id or f"attempt-{suffix}",
            generation,
            fence,
            "c14-session",
            horizon,
        )

    def execution(
        suffix,
        *,
        surface="desktop",
        horizon=190,
        attempt_id=None,
        worker_id="c14-worker",
        generation=3,
        fence=5,
        sequence=11,
        payload=None,
    ):
        return runtime_module.TurnExecutionInput(
            attempt(
                suffix,
                horizon=horizon,
                attempt_id=attempt_id,
                worker_id=worker_id,
                generation=generation,
                fence=fence,
                sequence=sequence,
            ),
            payload
            if payload is not None
            else {
                "message": "do not select this field",
                "nested": {"z": 2, "a": 1},
            },
            runtime_module.TurnOrigin(
                f"{surface}-binding",
                surface,
                f"{surface}-window",
                "owner-1",
            ),
            7,
        )

    def authority_fixture(
        execution_value,
        *,
        contract_revision=7,
        capability_revision=1,
        action="local_code_edit",
        action_class="routine_effect",
        target="c:/work/file.py",
        payload=None,
        policy_batch_id=None,
        batch_items=("write",),
        decision=Decision.ALLOW,
        intent_override=None,
        phase="implementation",
        approval_id=(
            "523e4567-e89b-42d3-a456-426614174000"
        ),
        approval_expires_at=3700,
    ):
        payload_value = (
            payload
            if payload is not None
            else {
                "path": "C:/work/file.py",
                "content": "exact",
            }
        )
        intent = (
            intent_override
            if intent_override is not None
            else OperationIntent(
                f"operation-{execution_value.attempt.attempt_id}",
                execution_value.attempt.project_id,
                execution_value.attempt.turn_id,
                f"idempotency-{execution_value.attempt.attempt_id}",
                action,
                capability_revision,
                (target,),
                batch_items,
                payload_value,
                "remote-ledger",
                True,
            )
        )
        action = intent.canonical_action
        capability_revision = intent.command_revision
        target_values = intent.targets
        batch_items = intent.batch_items
        payload_value = intent.payload
        command = ProjectCommand(
            action,
            execution_value.attempt.project_id,
            contract_revision,
            action_class,
            target_values,
            policy_batch_id,
            batch_items,
            {"phase": phase},
        )
        effect_scope = {
            "targets": list(target_values),
            "batch_items": list(batch_items),
            "payload_effects": dict(payload_value),
        }
        effect_scope_json = json.dumps(
            effect_scope,
            sort_keys=True,
            separators=(",", ":"),
        )
        effect_scope_sha256 = hashlib.sha256(
            effect_scope_json.encode("utf-8")
        ).hexdigest()
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
            "policy_batch_id": policy_batch_id,
            "capability_fingerprint": [
                action,
                capability_revision,
                "remote-ledger",
                True,
            ],
            "effect_scope": effect_scope,
        }
        authority_json = json.dumps(
            authority_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        authority = worker_module.BoundProjectOperationAuthority(
            command,
            intent,
            policy_batch_id,
            effect_scope_json,
            effect_scope_sha256,
            authority_json,
            hashlib.sha256(authority_json.encode("utf-8")).hexdigest(),
        )
        actor = ActorContext(
            "owner-1",
            execution_value.origin.surface,
            execution_value.origin.binding_id,
            True,
        )
        policy = PolicyDecision(
            decision,
            "c14-policy",
            "exact fresh snapshot",
            (
                "critical"
                if decision is Decision.REQUIRE_APPROVAL
                else None
            ),
        )
        carrier = worker_module.ProjectPolicyDecisionCarrier(
            execution_value.attempt,
            execution_value.origin,
            3,
            5,
            authority,
            ProjectPolicyView(
                execution_value.attempt.project_id,
                "active",
                phase,
                ("C:/work",),
                "plan-7",
                (
                    ProjectBindingView(
                        execution_value.origin.binding_id,
                        execution_value.origin.surface,
                        "owner-1",
                        execution_value.attempt.project_id,
                    ),
                ),
            ),
            "contract-c14",
            "active",
            "contract-json-sha256",
            ContractPolicyView(
                contract_revision,
                frozenset({"routine_effect", "critical_effect"}),
                frozenset({"implementation"}),
                "plan-7",
            ),
            actor,
            policy,
        )
        approval = (
            OperationApprovalSpec(
                approval_id,
                "critical",
                approval_expires_at,
                actor,
            )
            if decision is Decision.REQUIRE_APPROVAL
            else None
        )
        return authority, carrier, approval

    class FrozenSnapshot(SimpleNamespace):
        pass

    revisions = worker_module.ProjectAgentRevisions(
        "base-signature",
        "tools:core@23",
        "model:provider/model@route",
    )

    def snapshot(**changes):
        values = {
            "constructor_kwargs": {"model": "frozen-model"},
            "resolved_agent": "normal-hermes",
            "resolved_provider": "frozen-provider",
            "registry_generation": 23,
            "declared_registry_generation": 23,
            "base_signature": revisions.base_signature,
            "declared_base_signature": revisions.base_signature,
            "tool_revision": revisions.tool_revision,
            "declared_tool_revision": revisions.tool_revision,
            "model_revision": revisions.model_revision,
            "declared_model_revision": revisions.model_revision,
            "revisions": revisions,
            "runtime_kind": "hermes",
            "tool_descriptors": ("read.project_status", "local_code_edit"),
        }
        values.update(changes)
        return FrozenSnapshot(**values)

    class TurnBinder:
        def __init__(self, trace):
            self.trace = trace

        def __call__(self, value):
            owner = self

            class Bound:
                def __enter__(self):
                    owner.trace.append(("bind", value))
                    return value

                def __exit__(self, exc_type, exc, traceback):
                    owner.trace.append(("unbind", value))
                    return False

            return Bound()

    class Authorizer:
        def __init__(self, trace):
            self.trace = trace
            self.decisions = []
            self.calls = []

        async def authorize(self, execution_value, invocation, transcript):
            self.trace.append(("authorize", invocation.route))
            self.calls.append(
                (
                    execution_value,
                    invocation,
                    tuple(transcript),
                    threading.get_ident(),
                )
            )
            if invocation.canonical_action in {
                "event.deliver",
                "internal_delivery",
            }:
                return SimpleNamespace(action="deny")
            if self.decisions:
                return self.decisions.pop(0)
            return SimpleNamespace(action="allow_read_only")

    class UnusedCheckpointSentinel:
        def request_cancel(self):
            return False

        async def checkpoint_operation_intent(self, *args, **kwargs):
            raise AssertionError(
                "normalization-only turns cannot enter a checkpoint"
            )

    class SyncAgentProbe:
        def __init__(
            self,
            trace,
            outcomes,
            constructor_options,
            project_execution_gate,
        ):
            self.trace = trace
            self.outcomes = list(outcomes)
            self.constructor_options = constructor_options
            self.project_execution_gate = project_execution_gate
            self.calls = []
            self.interrupt_count = 0
            self.close_count = 0
            self.entered = threading.Event()
            self.release = threading.Event()
            self.gated = False
            self._persist_disabled = False
            self._session_db = object()
            self._session_json_enabled = True
            self._end_session_on_close = True
            self.compression_enabled = True
            self._memory_nudge_interval = 9
            self._skill_nudge_interval = 9
            self.background_review_callback = object()

        def run_conversation(
            self,
            *,
            user_message,
            conversation_history,
        ):
            self.trace.append(("raw.run", threading.get_ident()))
            self.calls.append(
                (
                    user_message,
                    tuple(conversation_history),
                    threading.get_ident(),
                )
            )
            self.entered.set()
            if self.gated:
                assert self.release.wait(timeout=5)
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        def interrupt(self):
            self.interrupt_count += 1
            self.trace.append("raw.interrupt")
            self.project_execution_gate.request_cancel()

        def close(self):
            self.close_count += 1
            self.trace.append(("raw.close", threading.get_ident()))

    class Fixture:
        def __init__(self, snapshots, raw_outcomes):
            self.trace = []
            self.snapshots = list(snapshots)
            self.raw_outcomes = list(raw_outcomes)
            self.resolver_calls = []
            self.builder_calls = []
            self.raw_agents = []
            self.agent_runner = RetainedThreadRunner(
                "c14-agent",
                max_workers=1,
            )
            self.authorizer = Authorizer(self.trace)
            self.checkpoint = UnusedCheckpointSentinel()
            self.binder = TurnBinder(self.trace)

        def resolve_snapshot(self, context_value, contract_revision):
            self.trace.append(("resolve", threading.get_ident()))
            self.resolver_calls.append(
                (context_value, contract_revision, threading.get_ident())
            )
            return self.snapshots.pop(0)

        def build_agent(self, snapshot_value, **kwargs):
            self.trace.append(("build", threading.get_ident()))
            self.builder_calls.append(
                (snapshot_value, kwargs, threading.get_ident())
            )
            raw = SyncAgentProbe(
                self.trace,
                self.raw_outcomes.pop(0),
                kwargs,
                kwargs["project_execution_gate"],
            )
            self.raw_agents.append(raw)
            return raw

        def factory(self):
            return worker_module.GatewayProjectAgentFactory(
                snapshot_resolver=self.resolve_snapshot,
                agent_builder=self.build_agent,
                off_loop_runner=self.agent_runner,
                turn_context_binder=self.binder,
                tool_authorizer=self.authorizer,
                checkpoint_coordinator=self.checkpoint,
            )

        def close(self):
            self.agent_runner.close()

    def completed_raw(
        response,
        *,
        prefix=history_messages,
        session_id="c14-session",
        **overrides,
    ):
        value = {
            "completed": True,
            "failed": False,
            "interrupted": False,
            "partial": False,
            "error": None,
            "final_response": response,
            "messages": list(prefix)
            + [
                {"role": "user", "content": "provider request"},
                {"role": "assistant", "content": response},
            ],
            "session_id": session_id,
            "agent_persisted": True,
        }
        value.update(overrides)
        return value

    # One immutable resolution/build supplies revisions and all constructor
    # inputs. The raw synchronous call and close run off the owner loop.
    fixture = Fixture(
        (snapshot(),),
        (
            (
                completed_raw("exact response"),
                completed_raw(
                    "discord response",
                    prefix=history_messages
                    + (
                        {
                            "role": "user",
                            "content": canonical_payload,
                        },
                        {
                            "role": "assistant",
                            "content": "exact response",
                        },
                    ),
                ),
            ),
        ),
    )
    owner_thread = threading.get_ident()
    factory = fixture.factory()
    build = await factory.resolve_project_agent(
        context=context,
        contract_revision=7,
    )
    assert build.revisions == revisions
    parent = await build.create_project_agent(history=history)
    raw = fixture.raw_agents[0]
    constructor = fixture.builder_calls[0][1]
    assert constructor["session_db"] is None
    assert constructor["save_trajectories"] is False
    assert constructor["quiet_mode"] is True
    assert constructor["skip_memory"] is True
    assert constructor["streaming_callback"] is None
    assert constructor["delivery_callback"] is None
    assert constructor["approval_notifier"] is None
    assert constructor["provider_metadata_prewarm"] is False
    assert constructor["external_memory_sync"] is False
    assert constructor["memory_review"] is False
    assert constructor["skill_review"] is False
    assert constructor["plugin_lifecycle"] is False
    assert raw._persist_disabled is True
    assert raw._session_db is None
    assert raw._session_json_enabled is False
    assert raw._end_session_on_close is False
    assert raw.compression_enabled is False
    assert raw._memory_nudge_interval == 0
    assert raw._skill_nudge_interval == 0
    assert raw.background_review_callback is None

    turn = parent.create_turn(execution("success"), None)
    result_one = asyncio.create_task(turn.result())
    result_two = asyncio.create_task(turn.result())
    first, second = await asyncio.gather(result_one, result_two)
    assert first is second
    assert first == worker_module.ProjectAgentRunResult(
        "succeeded",
        9,
        (
            {
                "role": "user",
                "content": canonical_payload,
            },
            {"role": "assistant", "content": "exact response"},
        ),
    )
    assert raw.calls == [
        (
            canonical_payload,
            history_messages,
            raw.calls[0][2],
        )
    ]
    assert raw.calls[0][2] != owner_thread
    assert fixture.resolver_calls[0][2] != owner_thread
    assert fixture.builder_calls[0][2] != owner_thread
    assert [entry[0] for entry in fixture.trace].count("raw.run") == 1
    assert fixture.trace.index(("bind", execution("success"))) < next(
        index
        for index, entry in enumerate(fixture.trace)
        if isinstance(entry, tuple) and entry[0] == "raw.run"
    )
    discord_result = await parent.create_turn(
        execution("discord", surface="discord"),
        None,
    ).result()
    assert discord_result.messages == (
        {"role": "user", "content": canonical_payload},
        {"role": "assistant", "content": "discord response"},
    )
    assert len(fixture.resolver_calls) == 1
    assert len(fixture.builder_calls) == 1
    assert fixture.resolver_calls[0][0] is context
    assert context.source.platform is Platform.LOCAL
    assert context.source.chat_id == "project:c14-project"
    assert context.source.user_id is None
    assert raw.calls[1][1] == history_messages + first.messages
    bound_origins = [
        entry[1].origin.surface
        for entry in fixture.trace
        if isinstance(entry, tuple) and entry[0] == "bind"
    ]
    assert bound_origins == ["desktop", "discord"]
    await turn.wait_quiescent()
    await factory.release_project_agent(parent)
    assert raw.close_count == 1
    assert next(
        entry[1]
        for entry in fixture.trace
        if isinstance(entry, tuple) and entry[0] == "raw.close"
    ) != owner_thread
    fixture.close()

    # Every supported explicit noncancelled failure is canonicalized to the
    # same State pair. Internal tool rows never become cache-only history.
    valid_failures = (
        {
            "completed": False,
            "failed": True,
            "interrupted": False,
            "partial": False,
            "error": "content policy",
            "final_response": None,
        },
        {
            "completed": False,
            "failed": True,
            "interrupted": False,
            "partial": True,
            "error": "compression",
            "final_response": "",
        },
        {
            "completed": False,
            "failed": False,
            "interrupted": True,
            "partial": True,
            "error": "retry interrupted",
            "final_response": "partial text",
        },
        {
            "completed": False,
            "failed": True,
            "interrupted": False,
            "partial": True,
            "error": "iteration exhausted",
            "final_response": "last response",
        },
    )
    for index, evidence in enumerate(valid_failures):
        raw_value = completed_raw(
            "ignored",
            **evidence,
        )
        fixture = Fixture(
            (snapshot(),),
            ((raw_value,),),
        )
        factory = fixture.factory()
        build = await factory.resolve_project_agent(
            context=context,
            contract_revision=7,
        )
        parent = await build.create_project_agent(history=history)
        outcome = await parent.create_turn(
            execution(f"failed-{index}"),
            None,
        ).result()
        assert outcome.status == "failed"
        assert outcome.base_message_count == 9
        assert outcome.messages[0]["role"] == "user"
        assert outcome.messages[1] == {
            "role": "assistant",
            "content": evidence["final_response"] or "",
        }
        assert len(outcome.messages) == 2
        await factory.release_project_agent(parent)
        fixture.close()

    # Every malformed/unknown normal-Hermes exit fails closed without
    # advancing the in-memory baseline. The valid follow-up sees the original
    # byte-exact history again.
    invalid_results = (
        completed_raw(
            "valid evidence plus unknown outcome",
            unknown_outcome=True,
        ),
        completed_raw(
            "contradiction",
            completed=True,
            failed=True,
        ),
        completed_raw(
            "contradiction",
            completed=True,
            interrupted=True,
        ),
        ("not", "a", "mapping"),
        completed_raw("bad messages", messages="not-a-sequence"),
        completed_raw(
            "bad row",
            messages=list(history_messages) + ["not-a-mapping"],
        ),
        completed_raw(None),
        completed_raw(42),
        completed_raw(
            "shrink",
            prefix=history_messages[:1],
        ),
        completed_raw(
            "rewrite",
            prefix=(
                {"role": "user", "content": "rewritten"},
                history_messages[1],
            ),
        ),
        completed_raw(
            "reorder",
            prefix=tuple(reversed(history_messages)),
        ),
        completed_raw(
            "compression",
            prefix=(
                {
                    "role": "user",
                    "content": "compressed historical prefix",
                },
            ),
        ),
        completed_raw(
            "rotated",
            session_id="rotated-session",
        ),
    )
    for index, invalid in enumerate(invalid_results):
        follow_up = completed_raw(f"valid-after-invalid-{index}")
        fixture = Fixture(
            (snapshot(),),
            ((invalid, follow_up),),
        )
        factory = fixture.factory()
        build = await factory.resolve_project_agent(
            context=context,
            contract_revision=7,
        )
        parent = await build.create_project_agent(history=history)
        with pytest.raises((TypeError, ValueError, RuntimeError)):
            await parent.create_turn(
                execution(f"invalid-{index}"),
                None,
            ).result()
        recovered = await parent.create_turn(
            execution(f"valid-{index}"),
            None,
        ).result()
        assert recovered.status == "succeeded"
        assert fixture.raw_agents[0].calls[-1][1] == history_messages
        await factory.release_project_agent(parent)
        fixture.close()

    # A mismatch in each frozen signature/generation and native runtimes
    # rejects before builder, agent ownership, cache, or release.
    mismatch_snapshots = (
        snapshot(declared_base_signature="wrong-base"),
        snapshot(declared_tool_revision="wrong-tools"),
        snapshot(declared_model_revision="wrong-model"),
        snapshot(declared_registry_generation=24),
        snapshot(runtime_kind="codex_app_server"),
        snapshot(runtime_kind="copilot_acp"),
    )
    for bad_snapshot in mismatch_snapshots:
        fixture = Fixture((bad_snapshot,), ())
        factory = fixture.factory()
        with pytest.raises((TypeError, ValueError, RuntimeError)):
            build = await factory.resolve_project_agent(
                context=context,
                contract_revision=7,
            )
            await build.create_project_agent(history=history)
        assert fixture.builder_calls == []
        assert fixture.raw_agents == []
        fixture.close()

    invalid_payloads = (
        {"not_finite": float("nan")},
        {1: "non-string-key"},
    )
    cyclic_payload = {}
    cyclic_payload["self"] = cyclic_payload
    for index, invalid_payload in enumerate(
        invalid_payloads + (cyclic_payload,)
    ):
        fixture = Fixture(
            (snapshot(),),
            ((completed_raw("must not run"),),),
        )
        factory = fixture.factory()
        build = await factory.resolve_project_agent(
            context=context,
            contract_revision=7,
        )
        parent = await build.create_project_agent(history=history)
        with pytest.raises((TypeError, ValueError)):
            invalid_turn = parent.create_turn(
                execution(
                    f"invalid-payload-{index}",
                    payload=invalid_payload,
                ),
                None,
            )
            await invalid_turn.result()
        assert fixture.raw_agents[0].calls == []
        await factory.release_project_agent(parent)
        fixture.close()

    detached_payload = {
        "message": "whole payload",
        "nested": {"items": [1, 2]},
    }
    fixture = Fixture(
        (snapshot(),),
        ((completed_raw("detached"),),),
    )
    factory = fixture.factory()
    build = await factory.resolve_project_agent(
        context=context,
        contract_revision=7,
    )
    parent = await build.create_project_agent(history=history)
    detached_turn = parent.create_turn(
        execution("detached", payload=detached_payload),
        None,
    )
    detached_payload["nested"]["items"].append(3)
    detached_result = await detached_turn.result()
    assert detached_result.messages[0] == {
        "role": "user",
        "content": (
            '{"message":"whole payload","nested":{"items":[1,2]}}'
        ),
    }
    await factory.release_project_agent(parent)
    fixture.close()

    # Pre-start cancellation invokes no raw call; post-submit cancellation
    # interrupts once, shares one future, and beats the late valid result.
    fixture = Fixture(
        (snapshot(),),
        ((completed_raw("must not run"), completed_raw("late result")),),
    )
    factory = fixture.factory()
    build = await factory.resolve_project_agent(
        context=context,
        contract_revision=7,
    )
    parent = await build.create_project_agent(history=history)
    raw = fixture.raw_agents[0]
    prestart = parent.create_turn(execution("prestart-cancel"), None)
    assert prestart.request_cancel() is True
    assert prestart.request_cancel() is False
    with pytest.raises(asyncio.CancelledError):
        await prestart.result()
    assert raw.calls == []
    assert raw.interrupt_count == 0

    raw.gated = True
    late = parent.create_turn(execution("late-cancel"), None)
    late_one = asyncio.create_task(late.result())
    late_two = asyncio.create_task(late.result())
    await asyncio.wait_for(
        asyncio.to_thread(raw.entered.wait),
        timeout=5,
    )
    assert late.request_cancel() is True
    assert late.request_cancel() is False
    assert raw.interrupt_count == 1
    quiescent = asyncio.create_task(late.wait_quiescent())
    assert not quiescent.done()
    close_waiter = asyncio.create_task(
        factory.release_project_agent(parent)
    )
    assert not close_waiter.done()
    raw.release.set()
    outcomes = await asyncio.gather(
        late_one,
        late_two,
        return_exceptions=True,
    )
    assert all(
        isinstance(outcome, asyncio.CancelledError)
        for outcome in outcomes
    )
    await quiescent
    await close_waiter
    assert raw.close_count == 1
    fixture.close()

    # The real policy facade owns the proposal -> fresh snapshot -> binder
    # -> immutable authority transition.  No model-controlled phase, metadata,
    # action class, contract revision, or owner bit crosses this boundary.
    policy_runner = RetainedThreadRunner("c14-policy", max_workers=4)
    effect_runner = RetainedThreadRunner("c14-effect", max_workers=2)
    policy_trace = []
    binder_trace = []
    uuid_calls = []
    binder_mutation = [None]
    policy_barrier = [None]

    class CapabilityAdapter:
        canonical_action = "local_code_edit"
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

    class CapabilityRegistry:
        def __init__(self):
            self.adapters = {
                (
                    "local_code_edit",
                    1,
                    "remote-ledger",
                    True,
                ): CapabilityAdapter(),
                ("publish", 1, "remote-ledger", True): (
                    CapabilityAdapter()
                ),
            }

        def get(self, fingerprint, default=None):
            return self.adapters.get(tuple(fingerprint), default)

        def __contains__(self, fingerprint):
            return tuple(fingerprint) in self.adapters

    capabilities = CapabilityRegistry()
    capabilities.adapters[
        ("publish", 1, "remote-ledger", True)
    ].canonical_action = "publish"

    policy_snapshot = [
        SimpleNamespace(
            project_id="c14-project",
            lifecycle="active",
            current_phase="implementation",
            roots=("C:/work",),
            approved_plan_ref="plan-7",
            contract_id="contract-c14",
            contract_status="active",
            contract_revision=7,
            contract_json_sha256="contract-json-sha256",
            allowed_action_classes=frozenset(
                {"status", "local_code_edit", "publish"}
            ),
            allowed_phases=frozenset({"implementation"}),
            actor_id="owner-1",
            actor_surface="desktop",
            binding_id="desktop-binding",
            actor_is_owner=True,
            control_version=3,
            runtime_version=5,
        )
    ]
    policy_snapshot_field_names = (
        "project_id",
        "lifecycle",
        "current_phase",
        "roots",
        "approved_plan_ref",
        "contract_id",
        "contract_status",
        "contract_revision",
        "contract_json_sha256",
        "allowed_action_classes",
        "allowed_phases",
        "actor_id",
        "actor_surface",
        "binding_id",
        "actor_is_owner",
        "control_version",
        "runtime_version",
    )

    def public_policy_snapshot_values(value):
        return {
            name: getattr(value, name)
            for name in policy_snapshot_field_names
        }

    class PolicyConnection:
        def __init__(self):
            self.thread_id = threading.get_ident()
            self.closed = False

        def load_project_policy_snapshot(self, *args, **kwargs):
            assert not self.closed
            assert threading.get_ident() == self.thread_id
            barrier = policy_barrier[0]
            if barrier is not None:
                barrier.wait(timeout=5)
            policy_trace.append(
                ("snapshot", args, kwargs, self.thread_id)
            )
            return policy_snapshot[0]

        def close(self):
            assert not self.closed
            assert threading.get_ident() == self.thread_id
            self.closed = True
            policy_trace.append(("close", self.thread_id))

    class PolicyConnectionFactory:
        def __init__(self):
            self.connections = []

        def __call__(self):
            connection = PolicyConnection()
            self.connections.append(connection)
            return connection

    policy_connections = PolicyConnectionFactory()

    def materialize_policy_snapshot(snapshot_value):
        return SimpleNamespace(
            project=ProjectPolicyView(
                snapshot_value.project_id,
                snapshot_value.lifecycle,
                snapshot_value.current_phase,
                snapshot_value.roots,
                snapshot_value.approved_plan_ref,
                (
                    ProjectBindingView(
                        snapshot_value.binding_id,
                        snapshot_value.actor_surface,
                        snapshot_value.actor_id,
                        snapshot_value.project_id,
                    ),
                ),
            ),
            contract_id=snapshot_value.contract_id,
            contract_status=snapshot_value.contract_status,
            contract_json_sha256=(
                snapshot_value.contract_json_sha256
            ),
            contract=ContractPolicyView(
                snapshot_value.contract_revision,
                snapshot_value.allowed_action_classes,
                snapshot_value.allowed_phases,
                snapshot_value.approved_plan_ref,
            ),
            actor=ActorContext(
                snapshot_value.actor_id,
                snapshot_value.actor_surface,
                snapshot_value.binding_id,
                snapshot_value.actor_is_owner,
            ),
            control_version=snapshot_value.control_version,
            runtime_version=snapshot_value.runtime_version,
        )

    def rebuild_authority(
        value,
        *,
        command=None,
        intent=None,
        policy_batch_id=None,
        effect_scope=None,
        fingerprint=None,
    ):
        command_value = command or value.command
        intent_value = intent or value.intent
        batch_value = (
            value.policy_batch_id
            if policy_batch_id is None
            else policy_batch_id
        )
        scope_value = (
            json.loads(value.effect_scope_json)
            if effect_scope is None
            else effect_scope
        )
        fingerprint_value = (
            (
                intent_value.canonical_action,
                intent_value.command_revision,
                intent_value.readback_kind,
                intent_value.remote_idempotency_supported,
            )
            if fingerprint is None
            else fingerprint
        )
        scope_json = json.dumps(
            scope_value,
            sort_keys=True,
            separators=(",", ":"),
        )
        authority_payload = {
            "command": {
                "name": command_value.name,
                "project_id": command_value.project_id,
                "revision": command_value.revision,
                "action_class": command_value.action_class,
                "targets": list(command_value.targets),
                "batch_id": command_value.batch_id,
                "batch_items": list(command_value.batch_items),
                "metadata": dict(command_value.metadata),
            },
            "intent": {
                "operation_id": intent_value.operation_id,
                "project_id": intent_value.project_id,
                "turn_id": intent_value.turn_id,
                "idempotency_key": intent_value.idempotency_key,
                "canonical_action": intent_value.canonical_action,
                "command_revision": intent_value.command_revision,
                "targets": list(intent_value.targets),
                "batch_items": list(intent_value.batch_items),
                "payload": dict(intent_value.payload),
                "readback_kind": intent_value.readback_kind,
                "remote_idempotency_supported": (
                    intent_value.remote_idempotency_supported
                ),
            },
            "policy_batch_id": batch_value,
            "capability_fingerprint": list(fingerprint_value),
            "effect_scope": scope_value,
        }
        authority_json = json.dumps(
            authority_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        return worker_module.BoundProjectOperationAuthority(
            command_value,
            intent_value,
            batch_value,
            scope_json,
            hashlib.sha256(scope_json.encode("utf-8")).hexdigest(),
            authority_json,
            hashlib.sha256(
                authority_json.encode("utf-8")
            ).hexdigest(),
        )

    def mutate_bound_authority(value, label):
        if label == "contract_revision":
            return rebuild_authority(
                value,
                command=replace(
                    value.command,
                    revision=value.command.revision + 1,
                ),
            )
        if label == "capability_revision":
            changed = replace(
                value.intent,
                command_revision=value.intent.command_revision + 1,
            )
            return rebuild_authority(value, intent=changed)
        if label == "canonical_action":
            return rebuild_authority(
                value,
                command=replace(value.command, name="local_test"),
            )
        if label == "action_class":
            return rebuild_authority(
                value,
                command=replace(
                    value.command,
                    action_class="local_test",
                ),
            )
        if label == "target":
            target = "C:/work/other.py"
            changed_intent = replace(value.intent, targets=(target,))
            changed_command = replace(
                value.command, targets=(target,)
            )
            scope = json.loads(value.effect_scope_json)
            scope["targets"] = [target]
            return rebuild_authority(
                value,
                command=changed_command,
                intent=changed_intent,
                effect_scope=scope,
            )
        if label == "policy_batch_id":
            return rebuild_authority(
                value,
                command=replace(
                    value.command,
                    batch_id="different-policy-batch",
                ),
                policy_batch_id="different-policy-batch",
            )
        if label == "batch_items":
            changed_intent = replace(
                value.intent,
                batch_items=("different-item",),
            )
            changed_command = replace(
                value.command,
                batch_items=("different-item",),
            )
            scope = json.loads(value.effect_scope_json)
            scope["batch_items"] = ["different-item"]
            return rebuild_authority(
                value,
                command=changed_command,
                intent=changed_intent,
                effect_scope=scope,
            )
        if label == "capability_fingerprint":
            return rebuild_authority(
                value,
                fingerprint=(
                    value.intent.canonical_action,
                    99,
                    value.intent.readback_kind,
                    True,
                ),
            )
        if label == "complete_payload":
            payload = dict(value.intent.payload)
            payload["content"] = "drifted"
            changed_intent = replace(value.intent, payload=payload)
            scope = json.loads(value.effect_scope_json)
            scope["payload_effects"] = payload
            return rebuild_authority(
                value,
                intent=changed_intent,
                effect_scope=scope,
            )
        if label == "payload_only_effect_scope":
            scope = json.loads(value.effect_scope_json)
            scope["unrepresented_effect"] = "C:/work/secret.py"
            return rebuild_authority(
                value,
                effect_scope=scope,
            )
        raise AssertionError(label)

    def bind_project_read(snapshot_value, execution_value, proposal):
        binder_trace.append(
            ("read", snapshot_value, execution_value, proposal)
        )
        return ProjectCommand(
            proposal.canonical_action,
            execution_value.attempt.project_id,
            snapshot_value.contract_revision,
            "status",
            proposal.targets,
            proposal.policy_batch_id,
            proposal.batch_items,
            {"phase": snapshot_value.current_phase},
        )

    def bind_project_operation(
        snapshot_value,
        execution_value,
        proposal,
    ):
        binder_trace.append(
            ("operation", snapshot_value, execution_value, proposal)
        )
        action_class = (
            "publish"
            if proposal.intent.canonical_action == "publish"
            else "local_code_edit"
        )
        authority, _, _ = authority_fixture(
            execution_value,
            contract_revision=snapshot_value.contract_revision,
            action=proposal.intent.canonical_action,
            action_class=action_class,
            target=proposal.intent.targets[0],
            payload=proposal.intent.payload,
            policy_batch_id=proposal.policy_batch_id,
            batch_items=proposal.intent.batch_items,
            intent_override=proposal.intent,
            phase=snapshot_value.current_phase,
        )
        mutation = binder_mutation[0]
        return (
            authority
            if mutation is None
            else mutate_bound_authority(authority, mutation)
        )

    def allocate_approval_id():
        uuid_calls.append("approval")
        return "623e4567-e89b-42d3-a456-426614174000"

    policy_facade = worker_module.ProjectToolPolicySnapshotFacade(
        policy_connections,
        read_binder=bind_project_read,
        operation_binder=bind_project_operation,
        capability_registry=capabilities,
        policy_decider=decide_project_policy,
        snapshot_materializer=materialize_policy_snapshot,
        authority_clock=lambda: 100,
        approval_id_factory=allocate_approval_id,
        io_runner=policy_runner,
    )

    def authorization_parts(value):
        if type(value) is worker_module.ProjectPolicyDecisionCarrier:
            return value, None
        if type(value) is tuple and len(value) == 2:
            carrier_value, approval_value = value
        else:
            carrier_value = getattr(
                value,
                "policy_authority",
                getattr(value, "carrier", None),
            )
            approval_value = getattr(value, "approval", None)
        assert type(carrier_value) is (
            worker_module.ProjectPolicyDecisionCarrier
        )
        assert (
            approval_value is None
            or type(approval_value) is OperationApprovalSpec
        )
        return carrier_value, approval_value

    read_proposal = worker_module.ProjectReadProposal(
        "status",
        (),
        None,
        (),
    )
    assert not hasattr(read_proposal, "phase")
    assert not hasattr(read_proposal, "metadata")
    read_decision = await policy_facade.authorize_project_read(
        execution("policy-read"),
        read_proposal,
    )
    assert read_decision.decision is Decision.ALLOW
    assert uuid_calls == []

    base_execution = execution("policy-effect")
    base_intent = OperationIntent(
        "operation-policy-effect",
        "c14-project",
        base_execution.attempt.turn_id,
        "idempotency-policy-effect",
        "local_code_edit",
        1,
        ("c:/work/file.py",),
        ("write",),
        {
            "path": "C:/work/file.py",
            "content": "exact",
        },
        "remote-ledger",
        True,
    )
    base_scope = {
        "targets": ["c:/work/file.py"],
        "batch_items": ["write"],
        "payload_effects": dict(base_intent.payload),
    }
    base_scope_json = json.dumps(
        base_scope,
        sort_keys=True,
        separators=(",", ":"),
    )
    base_proposal = worker_module.ProjectOperationProposal(
        base_intent,
        None,
        base_scope_json,
        hashlib.sha256(
            base_scope_json.encode("utf-8")
        ).hexdigest(),
        ("local_code_edit", 1, "remote-ledger", True),
    )
    assert not hasattr(base_proposal, "phase")
    assert not hasattr(base_proposal, "metadata")
    allowed_value = await policy_facade.authorize_project_operation(
        base_execution,
        base_proposal,
    )
    allowed_carrier, allowed_approval = authorization_parts(
        allowed_value
    )
    assert allowed_approval is None
    assert allowed_carrier.decision.decision is Decision.ALLOW
    assert allowed_carrier.contract.revision == 7
    assert (
        allowed_carrier.operation_authority.intent.command_revision
        == 1
    )
    assert (
        allowed_carrier.operation_authority.command.metadata
        == {"phase": "implementation"}
    )
    assert uuid_calls == []

    # A fresh snapshot is consulted on every authorization.  The binder, not
    # the proposal, supplies the durable phase, and a phase change is observed
    # without reconstructing the cached agent or reusing authority.
    policy_snapshot[0] = SimpleNamespace(
        **{
            **public_policy_snapshot_values(policy_snapshot[0]),
            "current_phase": "verification",
            "allowed_phases": frozenset({"verification"}),
        }
    )
    verification_value = await policy_facade.authorize_project_operation(
        execution("policy-verification"),
        replace(
            base_proposal,
            intent=replace(
                base_intent,
                operation_id="operation-policy-verification",
                turn_id="turn-policy-verification",
                idempotency_key="idempotency-policy-verification",
            ),
        ),
    )
    verification_carrier, _ = authorization_parts(verification_value)
    assert (
        verification_carrier.operation_authority.command.metadata
        == {"phase": "verification"}
    )
    assert binder_trace[-1][1].current_phase == "verification"
    policy_snapshot[0] = SimpleNamespace(
        **{
            **public_policy_snapshot_values(policy_snapshot[0]),
            "current_phase": "implementation",
            "allowed_phases": frozenset({"implementation"}),
        }
    )

    # Only REQUIRE_APPROVAL allocates one canonical UUID and binds the exact
    # injected authority time + 3600 seconds and the durable owner.
    critical_execution = execution("policy-critical")
    critical_intent = replace(
        base_intent,
        operation_id="operation-policy-critical",
        turn_id=critical_execution.attempt.turn_id,
        idempotency_key="idempotency-policy-critical",
        canonical_action="publish",
    )
    critical_scope = {
        "targets": ["c:/work/file.py"],
        "batch_items": ["write"],
        "payload_effects": dict(critical_intent.payload),
    }
    critical_scope_json = json.dumps(
        critical_scope,
        sort_keys=True,
        separators=(",", ":"),
    )
    critical_proposal = worker_module.ProjectOperationProposal(
        critical_intent,
        None,
        critical_scope_json,
        hashlib.sha256(
            critical_scope_json.encode("utf-8")
        ).hexdigest(),
        ("publish", 1, "remote-ledger", True),
    )
    critical_value = await policy_facade.authorize_project_operation(
        critical_execution,
        critical_proposal,
    )
    critical_carrier, critical_approval = authorization_parts(
        critical_value
    )
    assert critical_carrier.decision.decision is (
        Decision.REQUIRE_APPROVAL
    )
    assert critical_approval == OperationApprovalSpec(
        "623e4567-e89b-42d3-a456-426614174000",
        "publish",
        3700,
        critical_carrier.actor,
    )
    assert uuid_calls == ["approval"]

    # Otherwise-valid routine and critical proposals cannot reach the
    # operation binder, policy decision/approval allocation, or any
    # capability when their exact fingerprints are absent from this facade's
    # construction-bound registry.  This is the normal authorization
    # boundary, independently of recovery.
    allow_registry_miss_execution = execution(
        "policy-registry-miss-allow"
    )
    allow_registry_miss_intent = replace(
        base_intent,
        operation_id="operation-policy-registry-miss-allow",
        turn_id=allow_registry_miss_execution.attempt.turn_id,
        idempotency_key="idempotency-policy-registry-miss-allow",
    )
    allow_registry_miss_proposal = replace(
        base_proposal,
        intent=allow_registry_miss_intent,
    )
    critical_registry_miss_execution = execution(
        "policy-registry-miss-critical"
    )
    critical_registry_miss_intent = replace(
        critical_intent,
        operation_id="operation-policy-registry-miss-critical",
        turn_id=critical_registry_miss_execution.attempt.turn_id,
        idempotency_key=(
            "idempotency-policy-registry-miss-critical"
        ),
    )
    critical_registry_miss_proposal = replace(
        critical_proposal,
        intent=critical_registry_miss_intent,
    )
    registry_miss_connections = PolicyConnectionFactory()
    registry_miss_lookups = []
    registry_miss_binder_calls = []
    registry_miss_policy_calls = []
    registry_miss_uuid_calls = []

    class RegistryMissPoisonAdapter:
        canonical_action = "local_test"
        command_revision = 99
        readback_kind = "poison-readback"
        remote_idempotency_supported = False

        def __init__(self):
            self.trace = []

        @property
        def fingerprint(self):
            return (
                self.canonical_action,
                self.command_revision,
                self.readback_kind,
                self.remote_idempotency_supported,
            )

        def execute(self, *args, **kwargs):
            self.trace.append(("effect", args, kwargs))
            raise AssertionError(
                "registry miss reached a capability effect"
            )

        def readback(self, *args, **kwargs):
            self.trace.append(("readback", args, kwargs))
            raise AssertionError(
                "registry miss reached capability readback"
            )

    class MissingCapabilityRegistry:
        def __init__(self):
            self.poison_adapter = RegistryMissPoisonAdapter()
            self.adapters = {
                self.poison_adapter.fingerprint: (
                    self.poison_adapter
                )
            }

        def get(self, fingerprint, default=None):
            registry_miss_lookups.append(
                (tuple(fingerprint), default)
            )
            return self.adapters.get(
                tuple(fingerprint),
                default,
            )

    def registry_miss_operation_binder(*args, **kwargs):
        registry_miss_binder_calls.append((args, kwargs))
        raise AssertionError(
            "registry miss reached operation authority binder"
        )

    def registry_miss_policy_decider(*args, **kwargs):
        registry_miss_policy_calls.append((args, kwargs))
        raise AssertionError(
            "registry miss reached policy decision"
        )

    def registry_miss_approval_id():
        registry_miss_uuid_calls.append("approval")
        raise AssertionError(
            "registry miss allocated an approval"
        )

    missing_capabilities = MissingCapabilityRegistry()
    registry_miss_facade = (
        worker_module.ProjectToolPolicySnapshotFacade(
            registry_miss_connections,
            read_binder=bind_project_read,
            operation_binder=registry_miss_operation_binder,
            capability_registry=missing_capabilities,
            policy_decider=registry_miss_policy_decider,
            snapshot_materializer=materialize_policy_snapshot,
            authority_clock=lambda: 100,
            approval_id_factory=registry_miss_approval_id,
            io_runner=policy_runner,
        )
    )
    for (
        registry_miss_execution,
        registry_miss_proposal,
    ) in (
        (
            allow_registry_miss_execution,
            allow_registry_miss_proposal,
        ),
        (
            critical_registry_miss_execution,
            critical_registry_miss_proposal,
        ),
    ):
        registry_lookups_before = len(registry_miss_lookups)
        registry_connections_before = len(
            registry_miss_connections.connections
        )
        effect_runner_before = len(effect_runner.calls)
        with pytest.raises(
            PermissionError,
            match=r"(?i)capability",
        ):
            await registry_miss_facade.authorize_project_operation(
                registry_miss_execution,
                registry_miss_proposal,
            )
        assert registry_miss_lookups[
            registry_lookups_before:
        ] == [
            (
                registry_miss_proposal.capability_fingerprint,
                None,
            )
        ]
        assert len(
            registry_miss_connections.connections
        ) == registry_connections_before + 1
        registry_miss_connection = (
            registry_miss_connections.connections[-1]
        )
        assert (
            registry_miss_connection.thread_id
            != owner_thread
        )
        assert registry_miss_connection.closed is True
        assert registry_miss_binder_calls == []
        assert registry_miss_policy_calls == []
        assert registry_miss_uuid_calls == []
        assert missing_capabilities.poison_adapter.trace == []
        assert len(effect_runner.calls) == effect_runner_before

    invalid_snapshot_changes = (
        {"project_id": "other-project"},
        {"contract_revision": 8},
        {"actor_id": "unregistered-owner"},
        {"actor_surface": "discord"},
        {"binding_id": "unknown-binding"},
        {"contract_status": "draft"},
        {"lifecycle": "stopped"},
    )
    for index, changes in enumerate(invalid_snapshot_changes):
        original_snapshot = policy_snapshot[0]
        policy_snapshot[0] = SimpleNamespace(
            **{
                **public_policy_snapshot_values(original_snapshot),
                **changes,
            }
        )
        before_uuid = list(uuid_calls)
        before_binds = len(binder_trace)
        with pytest.raises((TypeError, ValueError, PermissionError)):
            await policy_facade.authorize_project_operation(
                execution(f"invalid-snapshot-{index}"),
                replace(
                    base_proposal,
                    intent=replace(
                        base_intent,
                        operation_id=f"operation-invalid-{index}",
                        turn_id=f"turn-invalid-snapshot-{index}",
                        idempotency_key=f"idempotency-invalid-{index}",
                    ),
                ),
            )
        assert uuid_calls == before_uuid
        assert len(binder_trace) in {before_binds, before_binds + 1}
        policy_snapshot[0] = original_snapshot

    # Each internally self-consistent but policy-inconsistent binder result
    # fails before UUID allocation or operation/effect authority can escape.
    drift_labels = (
        "contract_revision",
        "capability_revision",
        "canonical_action",
        "action_class",
        "target",
        "policy_batch_id",
        "batch_items",
        "capability_fingerprint",
        "complete_payload",
        "payload_only_effect_scope",
    )
    for index, label in enumerate(drift_labels):
        drift_execution = execution(f"authority-drift-{index}")
        drift_intent = replace(
            base_intent,
            operation_id=f"operation-drift-{index}",
            turn_id=drift_execution.attempt.turn_id,
            idempotency_key=f"idempotency-drift-{index}",
        )
        drift_proposal = replace(
            base_proposal,
            intent=drift_intent,
        )
        binder_mutation[0] = label
        before_uuid = list(uuid_calls)
        with pytest.raises((TypeError, ValueError, PermissionError)):
            await policy_facade.authorize_project_operation(
                drift_execution,
                drift_proposal,
            )
        assert uuid_calls == before_uuid
    binder_mutation[0] = None

    # Two concurrent calls retain their own operation, payload and scope.  A
    # ContextVar or hidden-map reconstruction would cross these assertions.
    concurrent_inputs = []
    for index in range(2):
        concurrent_execution = execution(
            f"concurrent-authority-{index}"
        )
        target = f"c:/work/concurrent-{index}.py"
        payload_path = f"C:/work/concurrent-{index}.py"
        concurrent_intent = replace(
            base_intent,
            operation_id=f"operation-concurrent-{index}",
            turn_id=concurrent_execution.attempt.turn_id,
            idempotency_key=f"idempotency-concurrent-{index}",
            targets=(target,),
            payload={"path": payload_path, "content": str(index)},
        )
        concurrent_scope = {
            "targets": [target],
            "batch_items": ["write"],
            "payload_effects": dict(concurrent_intent.payload),
        }
        concurrent_json = json.dumps(
            concurrent_scope,
            sort_keys=True,
            separators=(",", ":"),
        )
        concurrent_inputs.append(
            (
                concurrent_execution,
                worker_module.ProjectOperationProposal(
                    concurrent_intent,
                    None,
                    concurrent_json,
                    hashlib.sha256(
                        concurrent_json.encode("utf-8")
                    ).hexdigest(),
                    (
                        "local_code_edit",
                        1,
                        "remote-ledger",
                        True,
                    ),
                ),
            )
        )
    policy_barrier[0] = threading.Barrier(2)
    try:
        concurrent_values = await asyncio.gather(
            *(
                policy_facade.authorize_project_operation(
                    execution_value,
                    proposal_value,
                )
                for execution_value, proposal_value in concurrent_inputs
            )
        )
    finally:
        policy_barrier[0] = None
    concurrent_carriers = [
        authorization_parts(value)[0]
        for value in concurrent_values
    ]
    assert [
        value.operation_authority.intent.operation_id
        for value in concurrent_carriers
    ] == [
        "operation-concurrent-0",
        "operation-concurrent-1",
    ]
    assert [
        json.loads(value.operation_authority.effect_scope_json)[
            "targets"
        ]
        for value in concurrent_carriers
    ] == [
        ["c:/work/concurrent-0.py"],
        ["c:/work/concurrent-1.py"],
    ]
    assert all(
        connection.closed
        for connection in policy_connections.connections
    )

    # Exercise the binding-named prepare facade over real Task-6 authority
    # and temporary SQLite state.  Its keyword DI is intentional: C14 owns
    # construction of a fresh ProjectRuntime/ProjectOperationGuard for every
    # factory connection rather than accepting a prebuilt fake guard.
    from hermes_cli import project_runtime_db as prdb
    from hermes_cli import projects_db
    from hermes_cli.project_operations import (
        ProjectOperationError,
        ProjectOperationGuard,
    )

    class RecordingPreparedProjectsConnection(sqlite3.Connection):
        def close(self):
            close_thread = threading.get_ident()
            super().close()
            with pytest.raises(
                sqlite3.ProgrammingError,
                match="closed",
            ):
                sqlite3.Connection.execute(self, "SELECT 1")
            self.close_thread = close_thread
            self.closed_verified = True

    def seed_real_facade_case(label, *, critical=False):
        projects_path = tmp_path / f"c14-real-{label}.db"
        state_path = tmp_path / f"c14-real-{label}-state.db"
        connection = projects_db.connect(projects_path)
        project_id = projects_db.create_project(
            connection,
            name=f"C14 real {label}",
            folders=("C:/work",),
        )
        session_id = f"c14-real-{label}-session"
        binding_id = f"c14-real-{label}-binding"
        external_binding_id = f"c14-real-{label}-window"
        prdb.create_project_conversation(
            connection,
            project_id=project_id,
            conversation_id=session_id,
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            connection,
            binding_id=binding_id,
            project_id=project_id,
            surface="desktop",
            external_binding_id=external_binding_id,
            actor_id="owner-1",
            now=1,
        )
        contract_json = json.dumps(
            {
                "allowed_action_classes": [
                    "local_code_edit",
                    "publish",
                ],
                "allowed_phases": ["implementation"],
                "approved_plan_ref": "plan-7",
                "revision": 7,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        contract_digest = hashlib.sha256(
            contract_json.encode("utf-8")
        ).hexdigest()
        connection.execute(
            """
            INSERT INTO project_contracts (
                contract_id, project_id, revision, contract_json,
                status, created_at, updated_at
            ) VALUES (?, ?, 7, ?, 'active', 1, 1)
            """,
            ("contract-c14", project_id, contract_json),
        )
        connection.commit()
        clock = [100]
        runtime = runtime_module.ProjectRuntime(
            connection,
            clock=lambda: clock[0],
        )
        actor = ActorContext(
            "owner-1",
            "desktop",
            binding_id,
            True,
        )
        turn = runtime.enqueue_turn(
            project_id,
            {"message": f"real {label}"},
            actor,
            idempotency_key=f"turn-real-{label}",
            expected_version=0,
        )
        claim = runtime.claim_next_turn(
            project_id,
            f"worker-real-{label}",
            lease_seconds=90,
        )
        assert claim is not None
        claim = runtime.mark_turn_started(claim)
        state = prdb.runtime_state_for_project(
            connection,
            project_id,
        )
        assert state is not None
        control = runtime.control_for_claim(claim)
        attempt_value = runtime_module.TurnAttemptIdentity(
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
        execution_value = runtime_module.TurnExecutionInput(
            attempt_value,
            {
                "path": "C:/work/file.py",
                "content": "exact",
            },
            runtime_module.TurnOrigin(
                binding_id,
                "desktop",
                external_binding_id,
                "owner-1",
            ),
            7,
        )
        authority, carrier, _ = authority_fixture(
            execution_value,
            action="publish" if critical else "local_code_edit",
            action_class=(
                "publish"
                if critical
                else "local_code_edit"
            ),
            target="c:/work/file.py",
            payload=execution_value.payload,
            decision=(
                Decision.REQUIRE_APPROVAL
                if critical
                else Decision.ALLOW
            ),
            approval_id=(
                "c23e4567-e89b-42d3-a456-426614174000"
            ),
        )
        decision = (
            PolicyDecision(
                Decision.REQUIRE_APPROVAL,
                "c14-real-approval",
                "publish is critical",
                "publish",
            )
            if critical
            else carrier.decision
        )
        carrier = replace(
            carrier,
            control_version=control.control_version,
            runtime_version=state.version,
            project=ProjectPolicyView(
                project_id,
                state.lifecycle,
                state.current_phase,
                ("C:/work",),
                "plan-7",
                (
                    ProjectBindingView(
                        binding_id,
                        "desktop",
                        "owner-1",
                        project_id,
                    ),
                ),
            ),
            contract_json_sha256=contract_digest,
            contract=ContractPolicyView(
                7,
                frozenset(
                    {
                        "local_code_edit",
                        "publish",
                    }
                ),
                frozenset({"implementation"}),
                "plan-7",
            ),
            actor=actor,
            decision=decision,
        )
        approval = (
            OperationApprovalSpec(
                "c23e4567-e89b-42d3-a456-426614174000",
                "publish",
                3700,
                actor,
            )
            if critical
            else None
        )
        connection.close()

        def projects_db_factory():
            projects_connection = sqlite3.connect(
                projects_path,
                factory=RecordingPreparedProjectsConnection,
            )
            projects_connection.row_factory = sqlite3.Row
            projects_connection.execute("PRAGMA foreign_keys = ON")
            projects_connection.created_thread = threading.get_ident()
            projects_connection.close_thread = None
            projects_connection.closed_verified = False
            return projects_connection

        def project_runtime_factory(projects_connection):
            return runtime_module.ProjectRuntime(
                projects_connection,
                clock=lambda: clock[0],
            )

        prepare_connections = []

        def prepare_projects_db_factory():
            prepared_connection = projects_db_factory()
            prepare_connections.append(prepared_connection)
            return prepared_connection

        prepare_facade = (
            worker_module.ProjectOperationPrepareFacade(
                prepare_projects_db_factory,
                io_runner=policy_runner,
                runtime_factory=project_runtime_factory,
                operation_guard_factory=ProjectOperationGuard,
            )
        )
        return SimpleNamespace(
            label=label,
            projects_path=projects_path,
            state_path=state_path,
            project_id=project_id,
            session_id=session_id,
            binding_id=binding_id,
            turn=turn,
            claim=claim,
            execution=execution_value,
            authority=authority,
            carrier=carrier,
            approval=approval,
            actor=actor,
            clock=clock,
            contract_digest=contract_digest,
            projects_db_factory=projects_db_factory,
            project_runtime_factory=project_runtime_factory,
            prepare_connections=prepare_connections,
            prepare_facade=prepare_facade,
        )

    async def authorize_real_facade_case(
        value,
        *,
        allowed_action_classes=None,
        allowed_phases=None,
    ):
        if allowed_action_classes is None:
            allowed_action_classes = frozenset(
                {"local_code_edit", "publish"}
            )
        if allowed_phases is None:
            allowed_phases = frozenset({"implementation"})
        original_snapshot = policy_snapshot[0]
        policy_snapshot[0] = SimpleNamespace(
            project_id=value.project_id,
            lifecycle=value.carrier.project.lifecycle,
            current_phase=value.carrier.project.current_phase,
            roots=value.carrier.project.roots,
            approved_plan_ref=(
                value.carrier.project.approved_plan_ref
            ),
            contract_id=value.carrier.contract_id,
            contract_status=value.carrier.contract_status,
            contract_revision=value.carrier.contract.revision,
            contract_json_sha256=value.contract_digest,
            allowed_action_classes=allowed_action_classes,
            allowed_phases=allowed_phases,
            actor_id=value.actor.actor_id,
            actor_surface=value.actor.surface,
            binding_id=value.actor.binding_id,
            actor_is_owner=value.actor.is_owner,
            control_version=value.carrier.control_version,
            runtime_version=value.carrier.runtime_version,
        )
        intent_value = value.authority.intent
        try:
            authorized = (
                await policy_facade.authorize_project_operation(
                    value.execution,
                    worker_module.ProjectOperationProposal(
                        intent_value,
                        value.authority.policy_batch_id,
                        value.authority.effect_scope_json,
                        value.authority.effect_scope_sha256,
                        (
                            intent_value.canonical_action,
                            intent_value.command_revision,
                            intent_value.readback_kind,
                            intent_value.remote_idempotency_supported,
                        ),
                    ),
                )
            )
        finally:
            policy_snapshot[0] = original_snapshot
        carrier_value, approval_value = authorization_parts(
            authorized
        )
        value.authority = carrier_value.operation_authority
        value.carrier = carrier_value
        value.approval = approval_value
        return carrier_value, approval_value

    async def real_prepare(value, *, checkpoint_id=None):
        return await value.prepare_facade.prepare(
            value.claim,
            value.authority.intent,
            authority=value.authority,
            policy=value.carrier.decision,
            policy_authority=value.carrier,
            approval=value.approval,
            approval_checkpoint_id=checkpoint_id,
        )

    def assert_facade_connections_closed(
        connection_records,
        *,
        expected_count=None,
    ):
        if expected_count is not None:
            assert len(connection_records) == expected_count
        for connection_record in connection_records:
            recorded_factory_thread = None
            if type(connection_record) is (
                RecordingPreparedProjectsConnection
            ):
                connection_value = connection_record
            else:
                assert type(connection_record) is tuple
                assert len(connection_record) == 2
                connection_value, recorded_factory_thread = (
                    connection_record
                )
                assert type(connection_value) is (
                    RecordingPreparedProjectsConnection
                )
                assert type(recorded_factory_thread) is int
                assert (
                    recorded_factory_thread
                    == connection_value.created_thread
                )
            assert connection_value.created_thread != owner_thread
            assert (
                connection_value.close_thread
                == connection_value.created_thread
            )
            assert connection_value.closed_verified

    real_valid = seed_real_facade_case("valid")
    (
        real_valid_carrier,
        real_valid_approval,
    ) = await authorize_real_facade_case(real_valid)
    assert real_valid_carrier is real_valid.carrier
    assert real_valid_carrier.operation_authority is (
        real_valid.authority
    )
    assert real_valid_approval is None
    real_prepared = await real_prepare(real_valid)
    assert type(real_prepared) is ProjectOperation
    assert real_prepared.status == "approved"
    assert_facade_connections_closed(
        real_valid.prepare_connections,
        expected_count=1,
    )
    real_check = real_valid.projects_db_factory()
    try:
        real_row = real_check.execute(
            """
            SELECT operation_authority_json,
                   operation_authority_sha256,
                   effect_scope_json,
                   effect_scope_sha256,
                   policy_authority_json,
                   policy_authority_sha256,
                   approval_checkpoint_id
            FROM project_operations
            WHERE project_id = ? AND operation_id = ?
            """,
            (
                real_valid.project_id,
                real_valid.authority.intent.operation_id,
            ),
        ).fetchone()
        assert real_row is not None
        assert real_row["operation_authority_json"] == (
            real_valid.authority.authority_json
        )
        assert real_row["operation_authority_sha256"] == (
            real_valid.authority.authority_sha256
        )
        assert real_row["effect_scope_json"] == (
            real_valid.authority.effect_scope_json
        )
        assert real_row["effect_scope_sha256"] == (
            real_valid.authority.effect_scope_sha256
        )
        assert real_row["policy_authority_json"]
        assert len(real_row["policy_authority_sha256"]) == 64
        assert real_row["approval_checkpoint_id"] is None
    finally:
        real_check.close()

    # Authorities and carriers are immutable values crossing serialization
    # boundaries: equal reconstructions are accepted, while a genuinely
    # different authority beside the carrier denies before any write.
    for pair_kind in (
        "equal_but_distinct_authority",
        "equal_but_distinct_carrier",
        "swapped_authority",
    ):
        pair_case = seed_real_facade_case(
            f"pair-{pair_kind}"
        )
        await authorize_real_facade_case(pair_case)
        pair_carrier = pair_case.carrier
        if pair_kind == "equal_but_distinct_authority":
            pair_authority = replace(pair_case.authority)
            assert pair_authority == pair_case.authority
            assert pair_authority is not pair_case.authority
        elif pair_kind == "equal_but_distinct_carrier":
            reconstructed_authority = replace(
                pair_case.authority
            )
            pair_authority = pair_case.authority
            pair_carrier = replace(
                pair_case.carrier,
                operation_authority=reconstructed_authority,
            )
            assert pair_carrier == pair_case.carrier
            assert pair_carrier is not pair_case.carrier
        else:
            swap_source = seed_real_facade_case(
                "pair-swap-source"
            )
            await authorize_real_facade_case(swap_source)
            pair_authority = swap_source.authority
        should_prepare = pair_kind != "swapped_authority"
        if should_prepare:
            pair_prepared = await pair_case.prepare_facade.prepare(
                pair_case.claim,
                pair_case.authority.intent,
                authority=pair_authority,
                policy=pair_carrier.decision,
                policy_authority=pair_carrier,
                approval=pair_case.approval,
                approval_checkpoint_id=None,
            )
            assert type(pair_prepared) is ProjectOperation
            assert (
                pair_prepared.operation_id
                == pair_case.authority.intent.operation_id
            )
        else:
            with pytest.raises(
                (
                    PermissionError,
                    ValueError,
                    ProjectOperationError,
                )
            ):
                await pair_case.prepare_facade.prepare(
                    pair_case.claim,
                    pair_case.authority.intent,
                    authority=pair_authority,
                    policy=pair_carrier.decision,
                    policy_authority=pair_carrier,
                    approval=pair_case.approval,
                    approval_checkpoint_id=None,
                )
        assert_facade_connections_closed(
            pair_case.prepare_connections,
            expected_count=1,
        )
        pair_check = pair_case.projects_db_factory()
        try:
            assert pair_check.execute(
                """
                SELECT COUNT(*) FROM project_operations
                WHERE project_id = ? AND operation_id = ?
                """,
                (
                    pair_case.project_id,
                    pair_case.authority.intent.operation_id,
                ),
            ).fetchone()[0] == int(should_prepare)
        finally:
            pair_check.close()

    for race in (
        "phase",
        "contract_digest",
        "contract_status",
        "binding",
        "stop",
    ):
        race_case = seed_real_facade_case(f"race-{race}")
        await authorize_real_facade_case(race_case)
        mutation = race_case.projects_db_factory()
        try:
            if race == "phase":
                mutation.execute(
                    """
                    UPDATE project_runtime_state
                    SET current_phase = 'verification'
                    WHERE project_id = ?
                    """,
                    (race_case.project_id,),
                )
                mutation.commit()
            elif race == "contract_digest":
                mutation.execute(
                    """
                    UPDATE project_contracts
                    SET contract_json = '{"revision":8}'
                    WHERE project_id = ?
                    """,
                    (race_case.project_id,),
                )
                mutation.commit()
            elif race == "contract_status":
                mutation.execute(
                    """
                    UPDATE project_contracts
                    SET status = 'draft'
                    WHERE project_id = ?
                    """,
                    (race_case.project_id,),
                )
                mutation.commit()
            elif race == "binding":
                mutation.execute(
                    """
                    UPDATE project_surface_bindings
                    SET actor_id = 'other-owner'
                    WHERE project_id = ? AND binding_id = ?
                    """,
                    (
                        race_case.project_id,
                        race_case.binding_id,
                    ),
                )
                mutation.commit()
            else:
                race_runtime = (
                    race_case.project_runtime_factory(mutation)
                )
                race_state = prdb.runtime_state_for_project(
                    mutation,
                    race_case.project_id,
                )
                assert race_state is not None
                race_control = race_runtime.control_for_claim(
                    race_case.claim
                )
                race_runtime.request_stop(
                    race_case.project_id,
                    race_case.claim.turn_id,
                    race_case.actor,
                    idempotency_key=f"stop-{race}",
                    expected_version=race_state.version,
                    expected_control_version=(
                        race_control.control_version
                    ),
                )
        finally:
            mutation.close()
        with pytest.raises(
            (
                PermissionError,
                ValueError,
                ProjectOperationError,
                runtime_module.ProjectRuntimeError,
            )
        ):
            await real_prepare(race_case)
        assert_facade_connections_closed(
            race_case.prepare_connections,
            expected_count=1,
        )
        rejection_check = race_case.projects_db_factory()
        try:
            assert rejection_check.execute(
                """
                SELECT COUNT(*) FROM project_operations
                WHERE project_id = ? AND operation_id = ?
                """,
                (
                    race_case.project_id,
                    race_case.authority.intent.operation_id,
                ),
            ).fetchone()[0] == 0
        finally:
            rejection_check.close()

    # Cancelling an awaiting caller cannot abandon a facade-owned connection
    # or its real guard transition.  The facade joins the retained I/O job,
    # closes on that same worker thread, and only then delivers cancellation.
    prepare_cancel_case = seed_real_facade_case(
        "prepare-cancel"
    )
    await authorize_real_facade_case(prepare_cancel_case)
    prepare_cancel_entered = threading.Event()
    prepare_cancel_release = threading.Event()
    prepare_cancel_connections = []

    def prepare_cancel_projects_db_factory():
        cancel_connection = (
            prepare_cancel_case.projects_db_factory()
        )
        prepare_cancel_connections.append(
            (cancel_connection, threading.get_ident())
        )
        prepare_cancel_entered.set()
        assert prepare_cancel_release.wait(timeout=5)
        return cancel_connection

    prepare_cancel_facade = (
        worker_module.ProjectOperationPrepareFacade(
            prepare_cancel_projects_db_factory,
            io_runner=policy_runner,
            runtime_factory=(
                prepare_cancel_case.project_runtime_factory
            ),
            operation_guard_factory=ProjectOperationGuard,
        )
    )
    prepare_cancel_task = asyncio.create_task(
        prepare_cancel_facade.prepare(
            prepare_cancel_case.claim,
            prepare_cancel_case.authority.intent,
            authority=prepare_cancel_case.authority,
            policy=prepare_cancel_case.carrier.decision,
            policy_authority=prepare_cancel_case.carrier,
            approval=prepare_cancel_case.approval,
            approval_checkpoint_id=None,
        )
    )
    assert await asyncio.wait_for(
        asyncio.to_thread(prepare_cancel_entered.wait),
        timeout=5,
    )
    prepare_cancel_task.cancel()
    assert prepare_cancel_task.cancelling() == 1
    try:
        assert not prepare_cancel_task.done()
    finally:
        prepare_cancel_release.set()
    with pytest.raises(asyncio.CancelledError):
        await prepare_cancel_task
    assert_facade_connections_closed(
        prepare_cancel_connections,
        expected_count=1,
    )
    prepare_cancel_check = (
        prepare_cancel_case.projects_db_factory()
    )
    try:
        assert prepare_cancel_check.execute(
            """
            SELECT COUNT(*) FROM project_operations
            WHERE project_id = ? AND operation_id = ?
            """,
            (
                prepare_cancel_case.project_id,
                (
                    prepare_cancel_case.authority.intent
                    .operation_id
                ),
            ),
        ).fetchone()[0] == 1
    finally:
        prepare_cancel_check.close()

    # The public AIAgent conversation loop, reached only through a
    # ProjectAgentTurn, drives every scheduling route through one turn-bound
    # firewall. Provider responses are finite in-memory values and every
    # constructor side path that could perform live work is disabled.
    import run_agent
    from unittest.mock import MagicMock

    frozen_tool_definitions = [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": name,
                "parameters": {
                    "type": "object",
                    "properties": {},
                },
            },
        }
        for name in (
            "web_search",
            "read_file",
            "terminal",
            "todo",
            "c14_effect",
            "publish",
            "tool_call",
            "event.deliver",
            "internal_delivery",
        )
    ]
    monkeypatch.setattr(
        run_agent,
        "get_tool_definitions",
        lambda *args, **kwargs: frozen_tool_definitions,
    )
    monkeypatch.setattr(
        run_agent,
        "check_toolset_requirements",
        lambda *args, **kwargs: {},
    )
    provider_lifecycle_lock = threading.Lock()
    provider_primary_clients = []
    provider_request_clients = []
    provider_turn_records = []
    provider_lifecycle_trace = []
    active_provider_script = [None]
    poison_calls = {
        "thread_submit": [],
        "subprocess": [],
        "provider_model": [],
    }

    class HermeticSchedulingCompletions:
        def __init__(self, owner):
            self.owner = owner

        def create(self, **kwargs):
            owner = self.owner
            with provider_lifecycle_lock:
                script = owner.script
                if (
                    owner.kind != "request"
                    or script is None
                    or active_provider_script[0] is not script
                    or owner.create_count != 0
                    or owner.close_count != 0
                ):
                    poison_calls["provider_model"].append(
                        ("unexpected_create", owner, dict(kwargs))
                    )
                    raise AssertionError(
                        "project scheduling used an unowned provider create"
                    )
                owner.create_count += 1
                provider_lifecycle_trace.append(
                    (
                        len(provider_lifecycle_trace),
                        "create",
                        owner,
                    )
                )
                script["calls"].append((owner, dict(kwargs)))
                response = script["responses"][owner.request_index]
            return response

    class HermeticSchedulingOpenAI:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = dict(kwargs)
            self.close_count = 0
            self.create_count = 0
            self.chat = SimpleNamespace(
                completions=HermeticSchedulingCompletions(self)
            )
            with provider_lifecycle_lock:
                script = active_provider_script[0]
                if script is None:
                    if provider_primary_clients:
                        poison_calls["provider_model"].append(
                            ("unexpected_primary_constructor", args, kwargs)
                        )
                        raise AssertionError(
                            "project scheduling constructed another primary"
                        )
                    self.kind = "primary"
                    self.script = None
                    self.request_index = None
                    provider_primary_clients.append(self)
                    provider_lifecycle_trace.append(
                        (
                            len(provider_lifecycle_trace),
                            "constructor",
                            self,
                        )
                    )
                    return
                request_index = len(script["clients"])
                if request_index >= len(script["responses"]):
                    poison_calls["provider_model"].append(
                        (
                            "unexpected_request_constructor",
                            request_index,
                            args,
                            kwargs,
                        )
                    )
                    raise AssertionError(
                        "denied project tool constructed an extra "
                        "request-local provider"
                    )
                self.kind = "request"
                self.script = script
                self.request_index = request_index
                script["clients"].append(self)
                provider_request_clients.append(self)
                provider_lifecycle_trace.append(
                    (
                        len(provider_lifecycle_trace),
                        "constructor",
                        self,
                    )
                )

        @property
        def is_closed(self):
            return self.close_count > 0

        def close(self):
            with provider_lifecycle_lock:
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
                    poison_calls["provider_model"].append(
                        (
                            "unexpected_close",
                            self,
                            self.create_count,
                            self.close_count,
                        )
                    )
                    raise AssertionError(
                        "project scheduling closed a provider out of order"
                    )
                self.close_count += 1
                provider_lifecycle_trace.append(
                    (
                        len(provider_lifecycle_trace),
                        "close",
                        self,
                    )
                )

    def provider_events_for(client):
        with provider_lifecycle_lock:
            return tuple(
                event
                for event in provider_lifecycle_trace
                if event[2] is client
            )

    def arm_provider_script(responses):
        script = {
            "responses": tuple(responses),
            "clients": [],
            "calls": [],
        }
        with provider_lifecycle_lock:
            assert active_provider_script[0] is None
            active_provider_script[0] = script
        return script

    def finish_provider_script(script):
        with provider_lifecycle_lock:
            assert active_provider_script[0] is script
            active_provider_script[0] = None
            clients = tuple(script["clients"])
            calls = tuple(script["calls"])
        assert len(clients) == len(script["responses"])
        assert len(calls) == len(script["responses"])
        assert [owner for owner, _ in calls] == list(clients)
        assert all(client.kind == "request" for client in clients)
        assert all(client.create_count == 1 for client in clients)
        assert all(client.close_count == 1 for client in clients)
        for client in clients:
            client_events = provider_events_for(client)
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
        assert len(provider_primary_clients) == 1
        primary = provider_primary_clients[0]
        assert all(client is not primary for client in clients)
        assert primary.create_count == 0
        assert primary.close_count == 0
        record = SimpleNamespace(
            clients=clients,
            calls=calls,
        )
        provider_turn_records.append(record)
        return record

    monkeypatch.setattr(
        run_agent,
        "OpenAI",
        HermeticSchedulingOpenAI,
    )
    from agent import agent_init as agent_init_module
    from agent import model_metadata as model_metadata_module
    from hermes_cli import config as agent_config_module

    live_agent_dependency_calls = []

    def forbid_live_agent_dependency(name):
        def forbidden(*args, **kwargs):
            live_agent_dependency_calls.append((name, args, kwargs))
            raise AssertionError(
                f"project agent reached live dependency: {name}"
            )

        return forbidden

    for dependency_name in (
        "load_config",
        "load_config_readonly",
        "read_raw_config",
        "get_compatible_custom_providers",
        "get_custom_provider_context_length",
    ):
        monkeypatch.setattr(
            agent_config_module,
            dependency_name,
            forbid_live_agent_dependency(dependency_name),
        )
    monkeypatch.setattr(
        agent_init_module,
        "fetch_model_metadata",
        forbid_live_agent_dependency("fetch_model_metadata"),
    )
    monkeypatch.setattr(
        model_metadata_module.requests,
        "get",
        forbid_live_agent_dependency("provider_http_get"),
    )

    scheduler_trace = []
    legacy_trace = []
    schedule_decisions = []
    scheduling_event_lock = threading.Lock()
    scheduling_event_trace = []

    def append_scheduling_event(
        kind,
        tool_call_id,
        canonical_action,
        arguments,
    ):
        canonical_arguments = json.dumps(
            dict(arguments),
            sort_keys=True,
            separators=(",", ":"),
        )
        with scheduling_event_lock:
            event = (
                len(scheduling_event_trace),
                kind,
                tool_call_id,
                canonical_action,
                canonical_arguments,
            )
            scheduling_event_trace.append(event)
        return event

    def scheduling_events_snapshot():
        with scheduling_event_lock:
            return tuple(scheduling_event_trace)

    from types import MappingProxyType
    from hermes_state import SessionDB

    scheduling_capability_case = seed_real_facade_case(
        "scheduling-capability"
    )
    scheduling_carrier, scheduling_approval = (
        await authorize_real_facade_case(
            scheduling_capability_case
        )
    )
    assert scheduling_carrier is (
        scheduling_capability_case.carrier
    )
    assert scheduling_approval is None
    scheduling_state = SessionDB(
        db_path=scheduling_capability_case.state_path
    )
    scheduling_state.close()

    def scheduling_state_factory():
        return SessionDB(
            db_path=scheduling_capability_case.state_path
        )

    scheduling_checkpoint_read = (
        session_module.ProjectApprovalCheckpointReadFacade(
            scheduling_state_factory,
            io_runner=policy_runner,
        )
    )
    scheduling_execution_connections = []

    def scheduling_execution_db_factory():
        connection = (
            scheduling_capability_case.projects_db_factory()
        )
        scheduling_execution_connections.append(
            (connection, threading.get_ident())
        )
        return connection

    scheduling_execution_facade = (
        worker_module.ProjectOperationExecutionFacade(
            scheduling_execution_db_factory,
            approval_checkpoints=scheduling_checkpoint_read,
            io_runner=policy_runner,
            runtime_factory=(
                scheduling_capability_case
                .project_runtime_factory
            ),
            operation_guard_factory=ProjectOperationGuard,
        )
    )
    scheduling_registry_lookups = []

    class ObservedCapabilityFingerprint(tuple):
        def __eq__(self, other):
            scheduling_registry_lookups.append(tuple(other))
            return super().__eq__(other)

        __hash__ = tuple.__hash__

    class SchedulingCapabilityAdapter:
        canonical_action = "local_code_edit"
        command_revision = 1
        readback_kind = "remote-ledger"
        remote_idempotency_supported = True

        def __init__(self):
            self.trace = []
            self.poisoned = False

        @property
        def fingerprint(self):
            return (
                self.canonical_action,
                self.command_revision,
                self.readback_kind,
                self.remote_idempotency_supported,
            )

        def execute(self, request, idempotency_key=None):
            self.trace.append(
                (
                    "execute",
                    request,
                    idempotency_key,
                    threading.get_ident(),
                )
            )
            if self.poisoned:
                raise AssertionError(
                    "denied tool reached registered capability"
                )
            assert idempotency_key == (
                request.operation.idempotency_key
            )
            return OperationReceipt(
                "receipt-scheduling-capability",
                {"remote_id": "scheduling-capability"},
            )

        def readback(self, request):
            self.trace.append(
                (
                    "readback",
                    request,
                    threading.get_ident(),
                )
            )
            if self.poisoned:
                raise AssertionError(
                    "denied tool reached capability readback"
                )
            receipt = OperationReceipt(
                "receipt-scheduling-capability",
                {"remote_id": "scheduling-capability"},
            )
            return OperationReadbackResult(
                "applied",
                {"remote_id": "scheduling-capability"},
                receipt,
            )

        def read_operation(self, request):
            return self.readback(request)

    scheduling_capability_adapter = (
        SchedulingCapabilityAdapter()
    )
    scheduling_capability_registry = MappingProxyType(
        {
            ObservedCapabilityFingerprint(
                scheduling_capability_adapter.fingerprint
            ): scheduling_capability_adapter
        }
    )
    scheduling_live_coordinator = (
        worker_module.CanonicalProjectLiveOperationCoordinator(
            prepare_facade=(
                scheduling_capability_case.prepare_facade
            ),
            execution_facade=scheduling_execution_facade,
            capability_registry=scheduling_capability_registry,
            effect_runner=effect_runner,
        )
    )

    class SchedulingFirewall:
        def __init__(self, live_operation_coordinator):
            self.execution = None
            self.batch_calls = []
            self.calls = []
            self.cancel_count = 0
            self.live_operation_coordinator = (
                live_operation_coordinator
            )
            self.typed_operation_calls = []

        def bind_execution(self, execution_value):
            self.execution = execution_value
            return self

        def __enter__(self):
            return self.execution

        def __exit__(self, exc_type, exc, traceback):
            self.execution = None
            return False

        def classify_batch(self, execution_value, invocations, transcript):
            invocations = tuple(invocations)
            self.batch_calls.append(
                (execution_value, invocations, tuple(transcript))
            )
            scheduler_trace.append(
                (
                    "classify",
                    tuple(
                        getattr(item, "canonical_action", None)
                        for item in invocations
                    ),
                )
            )
            critical = [
                item
                for item in invocations
                if getattr(item, "critical", False)
            ]
            effectful = [
                item
                for item in invocations
                if getattr(item, "effect_capable", False)
            ]
            if len(critical) > 1 or (
                critical and len(effectful) > 1
            ):
                raise PermissionError(
                    "critical project batch denied"
                )
            return invocations

        async def authorize(
            self,
            execution_value,
            invocation,
            transcript,
        ):
            self.calls.append(
                (execution_value, invocation, tuple(transcript))
            )
            scheduler_trace.append(
                (
                    "authorize",
                    invocation.route,
                    invocation.canonical_action,
                )
            )
            append_scheduling_event(
                "authorize",
                invocation.tool_call_id,
                invocation.canonical_action,
                invocation.arguments,
            )
            if (
                type(execution_value)
                is not runtime_module.TurnExecutionInput
                or execution_value.contract_revision != 7
                or execution_value.origin.actor_id != "owner-1"
                or execution_value.origin.surface
                not in {"desktop", "discord"}
                or invocation.canonical_action
                in {"event.deliver", "internal_delivery"}
                or (
                    execution_value
                    is not scheduling_capability_case.execution
                    and (
                        execution_value.attempt.project_id
                        != "c14-project"
                        or execution_value.origin.binding_id
                        != f"{execution_value.origin.surface}-binding"
                        or execution_value.origin.external_binding_id
                        != f"{execution_value.origin.surface}-window"
                    )
                )
            ):
                return SimpleNamespace(action="deny")
            if schedule_decisions:
                return schedule_decisions.pop(0)
            if invocation.effect_capable:
                return SimpleNamespace(action="deny")
            return SimpleNamespace(action="allow_read_only")

        async def typed_project_operation(
            self,
            execution_value,
            authority,
            policy_authority,
        ):
            self.typed_operation_calls.append(
                (
                    execution_value,
                    authority,
                    policy_authority,
                )
            )
            return await self.live_operation_coordinator.execute(
                execution_value,
                authority,
                policy_authority,
            )

        def request_cancel(self):
            self.cancel_count += 1
            return self.cancel_count == 1

    firewall = SchedulingFirewall(scheduling_live_coordinator)

    class SharedSchedulingAgent(run_agent.AIAgent):
        def __init__(self, gate):
            super().__init__(
                api_key="test-key-1234567890",
                base_url="https://hermetic-provider.invalid/v1",
                model="frozen-model",
                quiet_mode=True,
                skip_context_files=True,
                skip_memory=True,
                project_execution_gate=gate,
                project_tool_schemas=tuple(frozen_tool_definitions),
                project_registry_generation=23,
                project_request_timeout=30.0,
                provider_metadata_prewarm=False,
                external_memory_sync=False,
                memory_review=False,
                skill_review=False,
                plugin_lifecycle=False,
            )

    scheduling_agent = SharedSchedulingAgent(firewall)
    scheduling_agent._disable_streaming = True
    assert (
        scheduling_agent.run_conversation.__func__
        is run_agent.AIAgent.run_conversation
    )
    assert scheduling_agent.client is provider_primary_clients[0]
    assert type(scheduling_agent.client) is HermeticSchedulingOpenAI
    assert scheduling_agent.client.close_count == 0

    def tool_call(name, arguments="{}", call_id=None):
        return SimpleNamespace(
            id=call_id or f"call-{name}",
            type="function",
            function=SimpleNamespace(
                name=name,
                arguments=arguments,
            ),
        )

    def raw_dispatch(name, args, task_id, **kwargs):
        append_scheduling_event(
            "dispatch",
            kwargs.get("tool_call_id"),
            name,
            args,
        )
        legacy_trace.append(
            (
                "dispatch",
                name,
                dict(args),
                threading.get_ident(),
            )
        )
        return json.dumps(
            {"ok": name},
            sort_keys=True,
            separators=(",", ":"),
        )

    monkeypatch.setattr(
        run_agent,
        "handle_function_call",
        raw_dispatch,
    )

    middleware_module = __import__(
        "hermes_cli.middleware",
        fromlist=["apply_tool_request_middleware"],
    )
    plugins_module = __import__(
        "hermes_cli.plugins",
        fromlist=["resolve_pre_tool_block"],
    )
    original_middleware = middleware_module.apply_tool_request_middleware
    original_pre_tool = plugins_module.resolve_pre_tool_block

    def traced_middleware(*args, **kwargs):
        legacy_trace.append(("middleware", args[0]))
        return original_middleware(*args, **kwargs)

    def traced_pre_tool(*args, **kwargs):
        legacy_trace.append(("legacy_pre_tool", args[0]))
        return original_pre_tool(*args, **kwargs)

    monkeypatch.setattr(
        middleware_module,
        "apply_tool_request_middleware",
        traced_middleware,
    )
    monkeypatch.setattr(
        plugins_module,
        "resolve_pre_tool_block",
        traced_pre_tool,
    )

    scheduling_binder_trace = []

    class SchedulingTurnBinder:
        def __call__(self, execution_value):
            bound = firewall.bind_execution(execution_value)

            class Binding:
                def __enter__(self):
                    scheduling_binder_trace.append(
                        ("bind", execution_value)
                    )
                    return bound.__enter__()

                def __exit__(self, exc_type, exc, traceback):
                    try:
                        return bound.__exit__(
                            exc_type,
                            exc,
                            traceback,
                        )
                    finally:
                        scheduling_binder_trace.append(
                            ("unbind", execution_value)
                        )

            return Binding()

    scheduling_agent_runner = RetainedThreadRunner(
        "c14-public-scheduling-agent",
        max_workers=1,
    )

    def build_scheduling_agent(snapshot_value, **kwargs):
        scheduling_agent.project_execution_gate = kwargs[
            "project_execution_gate"
        ]
        return scheduling_agent

    scheduling_factory = worker_module.GatewayProjectAgentFactory(
        snapshot_resolver=lambda context_value, revision: snapshot(),
        agent_builder=build_scheduling_agent,
        off_loop_runner=scheduling_agent_runner,
        turn_context_binder=SchedulingTurnBinder(),
        tool_authorizer=firewall,
        checkpoint_coordinator=UnusedCheckpointSentinel(),
    )
    scheduling_build = await scheduling_factory.resolve_project_agent(
        context=context,
        contract_revision=7,
    )
    scheduling_parent = await scheduling_build.create_project_agent(
        history=history,
    )

    def model_tool_response(calls):
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content="",
                        tool_calls=list(calls),
                    ),
                    finish_reason="tool_calls",
                )
            ],
            model="frozen-model",
            usage=None,
        )

    def model_final_response(label):
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=f"completed {label}",
                        tool_calls=None,
                    ),
                    finish_reason="stop",
                )
            ],
            model="frozen-model",
            usage=None,
        )

    async def run_public_scheduling_turn(
        execution_value,
        calls,
        label,
        *,
        expect_follow_up_model,
    ):
        responses = [model_tool_response(calls)]
        if expect_follow_up_model:
            responses.append(model_final_response(label))
        provider_script = arm_provider_script(responses)
        turn = scheduling_parent.create_turn(execution_value, None)
        try:
            return await turn.result()
        finally:
            await turn.wait_quiescent()
            provider_record = finish_provider_script(
                provider_script
            )
            assert len(provider_record.clients) == (
                2 if expect_follow_up_model else 1
            )

    # A noncritical typed decision must travel through the real AIAgent
    # scheduler.  A manual gate callback would not prove that direct registry
    # routing actually selects the construction-bound live coordinator.
    typed_decision = SimpleNamespace(
        action="typed_project_operation",
        authority=scheduling_capability_case.authority,
        policy=scheduling_capability_case.carrier,
        approval=None,
    )
    schedule_decisions.append(typed_decision)
    typed_call = tool_call(
        "c14_effect",
        json.dumps(
            dict(scheduling_capability_case.authority.intent.payload),
            sort_keys=True,
            separators=(",", ":"),
        ),
        "typed-direct",
    )
    typed_legacy_before = tuple(legacy_trace)
    typed_batches_before = len(firewall.batch_calls)
    typed_authorizations_before = len(firewall.calls)
    typed_registry_before = len(scheduling_registry_lookups)
    typed_effects_before = len(effect_runner.calls)
    typed_scheduler_result = await run_public_scheduling_turn(
        scheduling_capability_case.execution,
        [typed_call],
        "c14-typed-direct",
        expect_follow_up_model=True,
    )
    typed_provider_record = provider_turn_records[-1]
    assert len(typed_provider_record.clients) == 2
    assert len(typed_provider_record.calls) == 2
    typed_follow_up_messages = (
        typed_provider_record.calls[1][1]["messages"]
    )
    assert [message["role"] for message in typed_follow_up_messages[-2:]] == [
        "assistant",
        "tool",
    ]
    typed_assistant_tool_calls = typed_follow_up_messages[-2]["tool_calls"]
    assert len(typed_assistant_tool_calls) == 1
    assert typed_assistant_tool_calls[0]["id"] == typed_call.id
    assert typed_assistant_tool_calls[0]["type"] == typed_call.type
    assert typed_assistant_tool_calls[0]["function"] == {
        "name": typed_call.function.name,
        "arguments": typed_call.function.arguments,
    }
    typed_tool_rows = [
        message
        for message in typed_follow_up_messages
        if message.get("role") == "tool"
    ]
    assert typed_tool_rows == [
        {
            "role": "tool",
            "name": "c14_effect",
            "tool_call_id": typed_call.id,
            "content": json.dumps(
                {
                    "operation_id": (
                        scheduling_capability_case.authority.intent
                        .operation_id
                    ),
                    "status": "reconciled",
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
    ]
    assert typed_scheduler_result == worker_module.ProjectAgentRunResult(
        "succeeded",
        9,
        (
            {
                "role": "user",
                "content": json.dumps(
                    dict(scheduling_capability_case.execution.payload),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ),
            },
            {
                "role": "assistant",
                "content": "completed c14-typed-direct",
            },
        ),
    )
    assert schedule_decisions == []
    assert len(firewall.batch_calls) == typed_batches_before + 1
    assert firewall.batch_calls[-1][0] is (
        scheduling_capability_case.execution
    )
    assert firewall.batch_calls[-1][2] == ()
    assert len(firewall.batch_calls[-1][1]) == 1
    assert len(firewall.calls) == typed_authorizations_before + 1
    typed_authorization = firewall.calls[-1]
    assert typed_authorization[0] is (
        scheduling_capability_case.execution
    )
    assert typed_authorization[1] is (
        firewall.batch_calls[-1][1][0]
    )
    assert typed_authorization[1].route == "registry"
    assert typed_authorization[1].canonical_action == "c14_effect"
    assert typed_authorization[1].effect_capable is True
    assert typed_authorization[2] == ()
    assert firewall.typed_operation_calls == [
        (
            scheduling_capability_case.execution,
            scheduling_capability_case.authority,
            scheduling_capability_case.carrier,
        )
    ]
    assert tuple(legacy_trace) == typed_legacy_before
    assert (
        len(scheduling_registry_lookups)
        > typed_registry_before
    )
    assert [
        entry[0]
        for entry in scheduling_capability_adapter.trace
    ] == ["execute", "readback"]
    assert len(effect_runner.calls) == typed_effects_before + 2
    assert all(
        entry[-1] != owner_thread
        for entry in scheduling_capability_adapter.trace
    )
    assert_facade_connections_closed(
        scheduling_capability_case.prepare_connections,
        expected_count=1,
    )
    assert_facade_connections_closed(
        scheduling_execution_connections,
        expected_count=4,
    )

    # Establish the poison baseline only after the real scheduler has
    # completed its one allowed effect, readback, and reconciliation.
    scheduling_capability_adapter.poisoned = True
    scheduling_capability_baseline = {
        "registry_lookup": len(scheduling_registry_lookups),
        "coordinator": len(firewall.typed_operation_calls),
        "capability_execute": len(
            [
                entry
                for entry in scheduling_capability_adapter.trace
                if entry[0] == "execute"
            ]
        ),
        "capability_readback": len(
            [
                entry
                for entry in scheduling_capability_adapter.trace
                if entry[0] == "readback"
            ]
        ),
        "effect_runner": len(effect_runner.calls),
    }
    route_batches = (
        (
            "sequential",
            [tool_call("web_search", '{"query":"one"}', "seq")],
        ),
        (
            "concurrent",
            [
                tool_call("web_search", '{"query":"a"}', "con-a"),
                tool_call("read_file", '{"path":"a.py"}', "con-b"),
            ],
        ),
        (
            "segmented",
            [
                tool_call("web_search", '{"query":"a"}', "seg-a"),
                tool_call("read_file", '{"path":"a.py"}', "seg-b"),
                tool_call(
                    "terminal",
                    '{"command":"echo denied"}',
                    "seg-effect",
                ),
                tool_call("web_search", '{"query":"b"}', "seg-c"),
                tool_call("read_file", '{"path":"b.py"}', "seg-d"),
            ],
        ),
        (
            "agent_loop",
            [
                tool_call(
                    "todo",
                    '{"todos":[{"content":"must deny"}]}',
                    "loop",
                )
            ],
        ),
        (
            "registry",
            [
                tool_call(
                    "c14_effect",
                    '{"path":"C:/work/file.py"}',
                    "registry",
                )
            ],
        ),
    )
    for route, calls in route_batches:
        batches_before_route = len(firewall.batch_calls)
        calls_before_route = len(firewall.calls)
        events_before_route = len(scheduling_events_snapshot())
        dispatches_before_route = [
            item for item in legacy_trace if item[0] == "dispatch"
        ]
        if route in {"sequential", "concurrent"}:
            route_outcome = await run_public_scheduling_turn(
                execution(f"scheduling-{route}"),
                calls,
                f"c14-{route}",
                expect_follow_up_model=True,
            )
            assert route_outcome.status == "succeeded"
        else:
            with pytest.raises(PermissionError):
                await run_public_scheduling_turn(
                    execution(f"scheduling-{route}"),
                    calls,
                    f"c14-{route}",
                    expect_follow_up_model=False,
                )
        route_batch_delta = firewall.batch_calls[
            batches_before_route:
        ]
        assert len(route_batch_delta) == 1
        assert firewall.batch_calls[-1][1]
        assert route in {
            call[1].route
            for call in firewall.calls[calls_before_route:]
        }
        assert all(
            call[0].attempt.project_id == "c14-project"
            for call in firewall.calls[calls_before_route:]
        )
        if route == "concurrent":
            from collections import Counter

            expected_concurrent_identities = [
                (
                    call.id,
                    f"read.{call.function.name}",
                    json.dumps(
                        json.loads(call.function.arguments),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    route,
                )
                for call in calls
            ]
            concurrent_batch_invocations = (
                route_batch_delta[0][1]
            )
            assert len(concurrent_batch_invocations) == 2
            batch_identities = [
                (
                    invocation.tool_call_id,
                    invocation.canonical_action,
                    json.dumps(
                        dict(invocation.arguments),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    invocation.route,
                )
                for invocation in concurrent_batch_invocations
            ]
            assert Counter(batch_identities) == Counter(
                expected_concurrent_identities
            )
            concurrent_authorizations = firewall.calls[
                calls_before_route:
            ]
            assert len(concurrent_authorizations) == 2
            authorization_identities = [
                (
                    authorization[1].tool_call_id,
                    authorization[1].canonical_action,
                    json.dumps(
                        dict(authorization[1].arguments),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    authorization[1].route,
                )
                for authorization in concurrent_authorizations
            ]
            assert Counter(authorization_identities) == Counter(
                expected_concurrent_identities
            )
            concurrent_dispatch_delta = [
                item
                for item in legacy_trace
                if item[0] == "dispatch"
            ][len(dispatches_before_route):]
            assert Counter(
                (
                    entry[1],
                    json.dumps(
                        entry[2],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
                for entry in concurrent_dispatch_delta
            ) == Counter(
                (
                    call.function.name,
                    json.dumps(
                        json.loads(call.function.arguments),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
                for call in calls
            )
            concurrent_events = scheduling_events_snapshot()[
                events_before_route:
            ]
            raw_action_by_call_id = {
                call.id: call.function.name for call in calls
            }
            for (
                call_id,
                action,
                canonical_arguments,
                _,
            ) in expected_concurrent_identities:
                authorize_events = [
                    event
                    for event in concurrent_events
                    if event[1:] == (
                        "authorize",
                        call_id,
                        action,
                        canonical_arguments,
                    )
                ]
                dispatch_events = [
                    event
                    for event in concurrent_events
                    if event[1:] == (
                        "dispatch",
                        call_id,
                        raw_action_by_call_id[call_id],
                        canonical_arguments,
                    )
                ]
                assert len(authorize_events) == 1
                assert len(dispatch_events) == 1
                assert authorize_events[0][0] < dispatch_events[0][0]

            concurrent_provider_record = provider_turn_records[-1]
            assert len(concurrent_provider_record.calls) == 2
            concurrent_follow_up_messages = (
                concurrent_provider_record.calls[1][1]["messages"]
            )
            concurrent_assistant = concurrent_follow_up_messages[-3]
            concurrent_tool_rows = concurrent_follow_up_messages[-2:]
            assert concurrent_assistant["role"] == "assistant"
            assert [
                (
                    call["id"],
                    call["function"]["name"],
                    call["function"]["arguments"],
                )
                for call in concurrent_assistant["tool_calls"]
            ] == [
                (
                    call.id,
                    call.function.name,
                    call.function.arguments,
                )
                for call in calls
            ]
            assert concurrent_tool_rows == [
                {
                    "role": "tool",
                    "name": call.function.name,
                    "tool_call_id": call.id,
                    "content": json.dumps(
                        {"ok": call.function.name},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
                for call in calls
            ]

    # The same constructed agent performs a fresh authorization for each
    # turn-local execution context.  ALLOW reaches the legacy executor once;
    # a later DENY with a different exact origin reaches no second executor.
    allow_execution = execution(
        "cached-policy-allow",
        surface="desktop",
    )
    deny_execution = execution(
        "cached-policy-deny",
        surface="discord",
    )
    schedule_decisions.extend(
        (
            SimpleNamespace(action="allow_read_only"),
            SimpleNamespace(action="deny"),
        )
    )
    dispatch_before_policy_switch = len(
        [item for item in legacy_trace if item[0] == "dispatch"]
    )
    calls_before_policy_switch = len(firewall.calls)
    await run_public_scheduling_turn(
        allow_execution,
        [
            tool_call(
                "c14_effect",
                '{"path":"C:/work/file.py"}',
                "cached-policy-allow",
            )
        ],
        "c14-policy-allow",
        expect_follow_up_model=True,
    )
    with pytest.raises(PermissionError):
        await run_public_scheduling_turn(
            deny_execution,
            [
                tool_call(
                    "c14_effect",
                    '{"path":"C:/work/file.py"}',
                    "cached-policy-deny",
                )
            ],
            "c14-policy-deny",
            expect_follow_up_model=False,
        )
    assert [
        call[0]
        for call in firewall.calls[
            calls_before_policy_switch:
        ]
    ] == [allow_execution, deny_execution]
    assert [
        call[0].origin.surface
        for call in firewall.calls[
            calls_before_policy_switch:
        ]
    ] == ["desktop", "discord"]
    assert len(
        [item for item in legacy_trace if item[0] == "dispatch"]
    ) == dispatch_before_policy_switch + 1

    invalid_type_snapshot = (
        len(firewall.calls),
        len(firewall.batch_calls),
        len(
            [
                item
                for item in legacy_trace
                if item[0] == "dispatch"
            ]
        ),
        len(provider_request_clients),
        len(provider_turn_records),
    )
    assert active_provider_script[0] is None
    with pytest.raises(
        PermissionError,
        match="execution must be TurnExecutionInput",
    ):
        scheduling_parent.create_turn(object(), None)
    assert (
        len(firewall.calls),
        len(firewall.batch_calls),
        len(
            [
                item
                for item in legacy_trace
                if item[0] == "dispatch"
            ]
        ),
        len(provider_request_clients),
        len(provider_turn_records),
    ) == invalid_type_snapshot
    assert active_provider_script[0] is None

    invalid_scheduling_executions = {
        "real_seed_identity_neighbor": replace(
            scheduling_capability_case.execution
        ),
        "project": replace(
            allow_execution,
            attempt=replace(
                allow_execution.attempt,
                project_id="other-project",
            ),
        ),
        "contract_revision": replace(
            allow_execution,
            contract_revision=8,
        ),
        "origin_binding": replace(
            allow_execution,
            origin=replace(
                allow_execution.origin,
                binding_id="other-binding",
            ),
        ),
        "origin_surface": replace(
            allow_execution,
            origin=replace(
                allow_execution.origin,
                surface="discord",
            ),
        ),
        "origin_external_binding": replace(
            allow_execution,
            origin=replace(
                allow_execution.origin,
                external_binding_id="other-window",
            ),
        ),
        "actor": replace(
            allow_execution,
            origin=replace(
                allow_execution.origin,
                actor_id="other-actor",
            ),
        ),
    }
    for label, invalid_execution in (
        invalid_scheduling_executions.items()
    ):
        dispatch_before_invalid = len(
            [
                item
                for item in legacy_trace
                if item[0] == "dispatch"
            ]
        )
        calls_before_invalid = len(firewall.calls)
        with pytest.raises(PermissionError):
            await run_public_scheduling_turn(
                invalid_execution,
                [
                    tool_call(
                        "web_search",
                        '{"query":"must not execute"}',
                        f"invalid-authority-{label}",
                    )
                ],
                f"c14-invalid-authority-{label}",
                expect_follow_up_model=False,
            )
        assert len(firewall.calls) == calls_before_invalid + 1
        assert firewall.calls[-1][0] is invalid_execution
        assert len(
            [
                item
                for item in legacy_trace
                if item[0] == "dispatch"
            ]
        ) == dispatch_before_invalid
    # Tool Search uses the real unwrap route.  The resolver harness controls
    # only the external registry answer and preserves the exact nested args.
    from tools import tool_search as tool_search_module
    from agent import tool_executor as tool_executor_module

    original_resolve_underlying = (
        tool_search_module.resolve_underlying_call
    )

    def resolve_underlying(value):
        nested = value.get("nested")
        if nested:
            return (
                "tool_call",
                {
                    "name": "c14_effect",
                    "arguments": {
                        "path": "C:/work/file.py",
                    },
                },
                None,
            )
        return (
            value.get("name", "c14_effect"),
            value.get(
                "arguments",
                {"path": "C:/work/file.py"},
            ),
            None,
        )

    monkeypatch.setattr(
        tool_search_module,
        "resolve_underlying_call",
        resolve_underlying,
    )
    monkeypatch.setattr(
        tool_executor_module,
        "_tool_search_scoped_names",
        lambda agent: frozenset(
            {
                "c14_effect",
                "event.deliver",
                "internal_delivery",
                "tool_call",
            }
        ),
    )

    # Once authorization denies, no alternate executor family may even be
    # consulted.  Install concrete poison sentinels around all direct and
    # actually-unwrapped deny cases, including thread/process/provider/model
    # and the construction-bound typed capability path.
    import subprocess as subprocess_module
    from tools import daemon_pool as daemon_pool_module

    original_daemon_submit = (
        daemon_pool_module.DaemonThreadPoolExecutor.submit
    )
    original_subprocess_popen = subprocess_module.Popen
    original_subprocess_run = subprocess_module.run

    def poison_thread_submit(*args, **kwargs):
        poison_calls["thread_submit"].append(
            (args, kwargs)
        )
        raise AssertionError(
            "denied tool submitted a worker thread"
        )

    def poison_subprocess(*args, **kwargs):
        poison_calls["subprocess"].append((args, kwargs))
        raise AssertionError("denied tool spawned a subprocess")

    monkeypatch.setattr(
        daemon_pool_module.DaemonThreadPoolExecutor,
        "submit",
        poison_thread_submit,
    )
    monkeypatch.setattr(
        subprocess_module,
        "Popen",
        poison_subprocess,
    )
    monkeypatch.setattr(
        subprocess_module,
        "run",
        poison_subprocess,
    )

    def poison_snapshot():
        snapshot_value = {
            key: len(calls)
            for key, calls in poison_calls.items()
        }
        snapshot_value.update(
            {
                "registry_lookup": (
                    len(scheduling_registry_lookups)
                    - scheduling_capability_baseline[
                        "registry_lookup"
                    ]
                ),
                "coordinator": (
                    len(firewall.typed_operation_calls)
                    - scheduling_capability_baseline[
                        "coordinator"
                    ]
                ),
                "capability_execute": (
                    len(
                        [
                            entry
                            for entry in (
                                scheduling_capability_adapter
                                .trace
                            )
                            if entry[0] == "execute"
                        ]
                    )
                    - scheduling_capability_baseline[
                        "capability_execute"
                    ]
                ),
                "capability_readback": (
                    len(
                        [
                            entry
                            for entry in (
                                scheduling_capability_adapter
                                .trace
                            )
                            if entry[0] == "readback"
                        ]
                    )
                    - scheduling_capability_baseline[
                        "capability_readback"
                    ]
                ),
                "effect_runner": (
                    len(effect_runner.calls)
                    - scheduling_capability_baseline[
                        "effect_runner"
                    ]
                ),
            }
        )
        return snapshot_value

    for delivery_name in (
        "event.deliver",
        "internal_delivery",
    ):
        for route in ("direct", "mcp_single_unwrapped"):
            delivery_arguments = {
                "event_id": "event-c14",
                "route": route,
            }
            delivery_call = tool_call(
                (
                    delivery_name
                    if route == "direct"
                    else "tool_call"
                ),
                json.dumps(
                    (
                        delivery_arguments
                        if route == "direct"
                        else {
                            "name": delivery_name,
                            "arguments": delivery_arguments,
                        }
                    ),
                    separators=(",", ":"),
                ),
                f"{route}-{delivery_name}",
            )
            poison_before_delivery = poison_snapshot()
            calls_before_delivery = len(firewall.calls)
            dispatch_before_delivery = len(
                [
                    item
                    for item in legacy_trace
                    if item[0] == "dispatch"
                ]
            )
            with pytest.raises(PermissionError):
                await run_public_scheduling_turn(
                    execution(f"delivery-{route}-{delivery_name}"),
                    [delivery_call],
                    f"c14-{route}-{delivery_name}",
                    expect_follow_up_model=False,
                )
            assert len(firewall.calls) == (
                calls_before_delivery + 1
            )
            assert firewall.calls[-1][1].canonical_action == (
                delivery_name
            )
            assert firewall.calls[-1][1].route == route
            assert poison_snapshot() == poison_before_delivery
            assert len(
                [
                    item
                    for item in legacy_trace
                    if item[0] == "dispatch"
                ]
            ) == dispatch_before_delivery

    # Raw model arguments cannot promote an otherwise read-only adapter by
    # asserting project phase, policy metadata, or an unknown effect-bearing
    # field. Exercise both the direct and actually unwrapped public routes.
    raw_authority_injections = (
        {"query": "status", "phase": "verification"},
        {
            "query": "status",
            "metadata": {"policy_decision": "allow"},
        },
        {
            "query": "status",
            "unknown_effect": {
                "delete": "C:/work/file.py",
            },
        },
    )
    for route in ("direct", "mcp_single_unwrapped"):
        for injection_index, injected_arguments in enumerate(
            raw_authority_injections
        ):
            before_legacy = len(legacy_trace)
            poison_before_injection = poison_snapshot()
            call_arguments = (
                injected_arguments
                if route == "direct"
                else {
                    "name": "web_search",
                    "arguments": injected_arguments,
                }
            )
            with pytest.raises(PermissionError):
                await run_public_scheduling_turn(
                    execution(
                        f"raw-injection-{route}-{injection_index}"
                    ),
                    [
                        tool_call(
                            (
                                "web_search"
                                if route == "direct"
                                else "tool_call"
                            ),
                            json.dumps(
                                call_arguments,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                            (
                                "raw-injection-"
                                f"{route}-{injection_index}"
                            ),
                        )
                    ],
                    f"c14-raw-injection-{route}-{injection_index}",
                    expect_follow_up_model=False,
                )
            assert len(legacy_trace) == before_legacy
            assert poison_snapshot() == poison_before_injection

    # Every legacy bypass label is untrusted data.  Both direct and actually
    # unwrapped invocations deny before middleware, approval fallback, or raw
    # dispatch.
    bypasses = (
        "force",
        "allowlist",
        "yolo",
        "mode_off",
        "session_cache",
        "permanent_cache",
        "missing_notifier",
        "no_human_executor",
    )
    for route in ("direct", "mcp_single_unwrapped"):
        for bypass in bypasses:
            before_legacy = len(legacy_trace)
            before_calls = len(firewall.calls)
            poison_before_bypass = poison_snapshot()
            bypass_arguments = {
                "path": "C:/work/file.py",
                "bypass": bypass,
                "route": route,
            }
            call_name = (
                "c14_effect"
                if route == "direct"
                else "tool_call"
            )
            call_arguments = (
                bypass_arguments
                if route == "direct"
                else {
                    "name": "c14_effect",
                    "arguments": bypass_arguments,
                }
            )
            call = tool_call(
                call_name,
                json.dumps(
                    call_arguments,
                    separators=(",", ":"),
                ),
                f"{route}-{bypass}",
            )
            with pytest.raises(PermissionError):
                await run_public_scheduling_turn(
                    execution(f"bypass-{route}-{bypass}"),
                    [call],
                    f"c14-{route}-{bypass}",
                    expect_follow_up_model=False,
                )
            assert len(firewall.calls) == before_calls + 1
            assert len(legacy_trace) == before_legacy
            assert poison_snapshot() == poison_before_bypass

    # Tool Search is unwrapped by the real executor, then classified again.
    before_legacy = len(legacy_trace)
    poison_before_unwrapped = poison_snapshot()
    with pytest.raises(PermissionError):
        await run_public_scheduling_turn(
            execution("single-unwrapped"),
            [
                tool_call(
                    "tool_call",
                    '{"name":"c14_effect","arguments":{"path":'
                    '"C:/work/file.py"}}',
                    "unwrap-one",
                )
            ],
            "c14-single-unwrapped",
            expect_follow_up_model=False,
        )
    assert len(legacy_trace) == before_legacy
    assert poison_snapshot() == poison_before_unwrapped

    poison_before_nested = poison_snapshot()
    with pytest.raises(PermissionError):
        await run_public_scheduling_turn(
            execution("multi-unwrapped"),
            [
                tool_call(
                    "tool_call",
                    '{"nested":true}',
                    "unwrap-many",
                )
            ],
            "c14-multi-unwrapped",
            expect_follow_up_model=False,
        )
    assert poison_snapshot() == poison_before_nested
    monkeypatch.setattr(
        tool_search_module,
        "resolve_underlying_call",
        original_resolve_underlying,
    )

    # Full-batch preclassification sees both mixed-critical orders before a
    # member can enter per-call authorization or legacy execution.
    for critical_first in (True, False):
        critical_call = tool_call(
            "publish",
            '{"path":"C:/work/file.py"}',
            f"critical-{critical_first}",
        )
        routine_call = tool_call(
            "c14_effect",
            '{"path":"C:/work/other.py"}',
            f"routine-{critical_first}",
        )
        calls = (
            [critical_call, routine_call]
            if critical_first
            else [routine_call, critical_call]
        )
        before_authorize = len(firewall.calls)
        before_legacy = len(legacy_trace)
        poison_before_mixed = poison_snapshot()
        with pytest.raises(
            PermissionError,
            match="critical project batch denied",
        ):
            await run_public_scheduling_turn(
                execution(f"mixed-{critical_first}"),
                calls,
                f"c14-mixed-{critical_first}",
                expect_follow_up_model=False,
            )
        assert len(firewall.calls) == before_authorize
        assert len(legacy_trace) == before_legacy
        assert poison_snapshot() == poison_before_mixed
        assert len(firewall.batch_calls[-1][1]) == 2

    assert poison_snapshot() == {
        "thread_submit": 0,
        "subprocess": 0,
        "provider_model": 0,
        "registry_lookup": 0,
        "coordinator": 0,
        "capability_execute": 0,
        "capability_readback": 0,
        "effect_runner": 0,
    }
    assert scheduling_binder_trace
    assert len(scheduling_binder_trace) % 2 == 0
    for bind_entry, unbind_entry in zip(
        scheduling_binder_trace[::2],
        scheduling_binder_trace[1::2],
        strict=True,
    ):
        assert bind_entry[0] == "bind"
        assert unbind_entry[0] == "unbind"
        assert bind_entry[1] is unbind_entry[1]
    assert firewall.execution is None
    monkeypatch.setattr(
        daemon_pool_module.DaemonThreadPoolExecutor,
        "submit",
        original_daemon_submit,
    )
    monkeypatch.setattr(
        subprocess_module,
        "Popen",
        original_subprocess_popen,
    )
    monkeypatch.setattr(
        subprocess_module,
        "run",
        original_subprocess_run,
    )
    await scheduling_factory.release_project_agent(
        scheduling_parent
    )
    assert scheduling_agent.client is None
    assert len(provider_primary_clients) == 1
    primary_provider = provider_primary_clients[0]
    assert primary_provider.close_count == 1
    primary_provider_events = provider_events_for(primary_provider)
    primary_constructor_events = [
        event
        for event in primary_provider_events
        if event[1] == "constructor"
    ]
    primary_create_events = [
        event
        for event in primary_provider_events
        if event[1] == "create"
    ]
    primary_close_events = [
        event
        for event in primary_provider_events
        if event[1] == "close"
    ]
    assert len(primary_constructor_events) == 1
    assert primary_create_events == []
    assert len(primary_close_events) == 1
    assert (
        primary_constructor_events[0][0]
        < primary_close_events[0][0]
    )
    assert all(
        client.close_count == 1
        for client in provider_request_clients
    )
    monkeypatch.setattr(run_agent, "OpenAI", MagicMock)
    scheduling_agent_runner.close()
    assert live_agent_dependency_calls == []

    # Build exact public operation carriers without depending on private DB
    # records.  Added C14 public fields are supplied by name; an unexpected
    # public shape is a contract failure rather than silently defaulted.
    def operation_value(
        authority,
        *,
        status,
        attempt_value,
        receipt_id=None,
        approval_id=None,
        blocked_reason=None,
        checkpoint_id=None,
    ):
        values = {
            "operation_id": authority.intent.operation_id,
            "project_id": authority.intent.project_id,
            "turn_id": authority.intent.turn_id,
            "idempotency_key": authority.intent.idempotency_key,
            "canonical_action": authority.intent.canonical_action,
            "command_revision": authority.intent.command_revision,
            "targets": authority.intent.targets,
            "batch_items": authority.intent.batch_items,
            "status": status,
            "approval_id": approval_id,
            "readback_kind": authority.intent.readback_kind,
            "receipt_id": receipt_id,
            "blocked_reason": blocked_reason,
            "attempt_id": attempt_value.attempt_id,
            "lease_generation": attempt_value.lease_generation,
            "fencing_token": attempt_value.fencing_token,
            "created_at": 100,
            "updated_at": 101,
            "operation_authority_json": authority.authority_json,
            "operation_authority_sha256": authority.authority_sha256,
            "effect_scope_json": authority.effect_scope_json,
            "effect_scope_sha256": authority.effect_scope_sha256,
            "policy_authority_json": "stored-policy-authority-json",
            "policy_authority_sha256": (
                hashlib.sha256(
                    b"stored-policy-authority-json"
                ).hexdigest()
            ),
            "approval_checkpoint_id": checkpoint_id,
            "remote_idempotency_supported": True,
        }
        public_names = {
            field.name for field in fields(ProjectOperation)
        }
        unknown = public_names - values.keys()
        assert not unknown, (
            f"unbound ProjectOperation fields: {sorted(unknown)}"
        )
        operation = ProjectOperation(
            **{
                name: value
                for name, value in values.items()
                if name in public_names
            }
        )
        object.__setattr__(
            operation,
            "_approval_checkpoint_id",
            checkpoint_id,
        )
        return operation

    def certified_request(
        operation,
        authority,
        attempt_value,
        *,
        checkpoint_id=None,
        payload=None,
        authority_json=None,
        authority_sha256=None,
        effect_scope_json=None,
        effect_scope_sha256=None,
        policy_authority_sha256=None,
        fingerprint=None,
    ):
        return (
            worker_module.CertifiedProjectOperationExecutionRequest(
                operation=operation,
                attempt=attempt_value,
                payload=(
                    dict(authority.intent.payload)
                    if payload is None
                    else payload
                ),
                approval_checkpoint_id=checkpoint_id,
                operation_authority_json=(
                    authority.authority_json
                    if authority_json is None
                    else authority_json
                ),
                operation_authority_sha256=(
                    authority.authority_sha256
                    if authority_sha256 is None
                    else authority_sha256
                ),
                effect_scope_json=(
                    authority.effect_scope_json
                    if effect_scope_json is None
                    else effect_scope_json
                ),
                effect_scope_sha256=(
                    authority.effect_scope_sha256
                    if effect_scope_sha256 is None
                    else effect_scope_sha256
                ),
                policy_authority_sha256=(
                    hashlib.sha256(
                        b"stored-policy-authority-json"
                    ).hexdigest()
                    if policy_authority_sha256 is None
                    else policy_authority_sha256
                ),
                remote_idempotency_supported=True,
                capability_fingerprint=(
                    (
                        authority.intent.canonical_action,
                        authority.intent.command_revision,
                        authority.intent.readback_kind,
                        True,
                    )
                    if fingerprint is None
                    else fingerprint
                ),
            )
        )

    def claim_for_execution(execution_value):
        attempt_value = execution_value.attempt
        return runtime_module.TurnClaim(
            attempt_value.turn_id,
            attempt_value.project_id,
            attempt_value.sequence,
            attempt_value.worker_id,
            attempt_value.attempt_id,
            attempt_value.lease_generation,
            attempt_value.fencing_token,
            attempt_value.lease_expires_at,
            attempt_value.canonical_session_id,
        )

    class PrepareFacade:
        def __init__(
            self,
            *,
            operation,
            execution_value,
            authority,
            policy_authority,
        ):
            self.operation = operation
            self.execution = execution_value
            self.authority = authority
            self.policy_authority = policy_authority
            self.trace = []

        async def prepare(
            self,
            claim,
            intent,
            *,
            authority,
            policy,
            policy_authority,
            approval=None,
            approval_checkpoint_id=None,
        ):
            self.trace.append(
                (
                    "prepare",
                    claim,
                    intent,
                    authority,
                    policy,
                    policy_authority,
                    approval,
                    approval_checkpoint_id,
                )
            )
            assert claim == claim_for_execution(self.execution)
            assert intent is self.authority.intent
            assert authority is self.authority
            assert policy is self.policy_authority.decision
            assert policy_authority is self.policy_authority
            assert approval is None
            assert approval_checkpoint_id is None
            return self.operation

    def fake_prepare_facade(
        execution_facade,
        execution_value,
        authority,
        policy_authority,
    ):
        return PrepareFacade(
            operation=execution_facade.prepared,
            execution_value=execution_value,
            authority=authority,
            policy_authority=policy_authority,
        )

    class ExecutionFacade:
        def __init__(
            self,
            *,
            authority,
            execution_value,
            final_status="reconciled",
            effect_result=None,
            readback_result=None,
        ):
            self.authority = authority
            self.execution = execution_value
            self.trace = []
            self.prepared = operation_value(
                authority,
                status="approved",
                attempt_value=execution_value.attempt,
            )
            self.started = operation_value(
                authority,
                status="effect_started",
                attempt_value=execution_value.attempt,
            )
            self.recorded = operation_value(
                authority,
                status="receipt_recorded",
                attempt_value=execution_value.attempt,
                receipt_id="receipt-c14",
            )
            self.final = operation_value(
                authority,
                status=final_status,
                attempt_value=execution_value.attempt,
                receipt_id=(
                    "receipt-c14"
                    if final_status == "reconciled"
                    else None
                ),
                blocked_reason=(
                    "blocked"
                    if final_status == "blocked"
                    else None
                ),
            )
            self.request = certified_request(
                self.prepared,
                authority,
                execution_value.attempt,
            )
            self.fail_at = None
            self.effect_result = effect_result
            self.readback_result = readback_result

        async def certified_execution_request(
            self,
            execution_value,
            operation,
        ):
            self.trace.append(
                (
                    "certified_execution_request",
                    execution_value,
                    operation,
                )
            )
            if self.fail_at == "certified_execution_request":
                raise PermissionError("stored authority drift")
            return self.request

        async def mark_started(self, request):
            self.trace.append(("mark_started", request))
            if self.fail_at == "mark_started":
                raise PermissionError("mark policy race")
            return self.started

        async def record_receipt(self, request, receipt):
            self.trace.append(("record_receipt", request, receipt))
            if self.fail_at == "record_receipt":
                raise RuntimeError("receipt write failed")
            return self.recorded

        async def reconcile(self, request, readback):
            self.trace.append(("reconcile", request, readback))
            if self.fail_at == "reconcile":
                raise RuntimeError("reconcile failed")
            return self.final

    class EffectAdapter:
        def __init__(self, effect_result, readback_result):
            self.effect_result = effect_result
            self.readback_result = readback_result
            self.trace = []
            self.entered = threading.Event()
            self.release = threading.Event()
            self.gated = False

        def execute(self, request, idempotency_key=None):
            if idempotency_key is not None:
                assert idempotency_key == (
                    request.operation.idempotency_key
                )
            self.trace.append(
                ("effect", request, threading.get_ident())
            )
            self.entered.set()
            if self.gated:
                assert self.release.wait(timeout=5)
            if isinstance(self.effect_result, BaseException):
                raise self.effect_result
            return self.effect_result

        def readback(self, request):
            assert type(request) is OperationReadbackRequest
            self.trace.append(
                ("readback", request, threading.get_ident())
            )
            if isinstance(self.readback_result, BaseException):
                raise self.readback_result
            return self.readback_result

        def read_operation(self, request):
            return self.readback(request)

    class ExecutionRegistry(CapabilityRegistry):
        def __init__(self, adapter):
            super().__init__()
            self.adapter = adapter

        def get(self, fingerprint, default=None):
            if tuple(fingerprint) in self:
                return self.adapter
            return default

    live_execution = execution("live-operation", horizon=220)
    live_authority, live_carrier, _ = authority_fixture(
        live_execution,
    )
    live_receipt = OperationReceipt(
        "receipt-c14",
        {"remote_id": "remote-c14"},
    )
    live_readback = OperationReadbackResult(
        "applied",
        {"remote_id": "remote-c14"},
        live_receipt,
    )

    from hermes_state import SessionDB

    def build_real_execution_facade(value):
        state_store = SessionDB(db_path=value.state_path)
        state_store.close()

        def state_db_factory():
            return SessionDB(db_path=value.state_path)

        checkpoint_read = (
            session_module.ProjectApprovalCheckpointReadFacade(
                state_db_factory,
                io_runner=policy_runner,
            )
        )

        execution_connections = []

        def execution_projects_db_factory():
            execution_connection = (
                value.projects_db_factory()
            )
            execution_connections.append(
                (
                    execution_connection,
                    threading.get_ident(),
                )
            )
            return execution_connection

        facade = worker_module.ProjectOperationExecutionFacade(
            execution_projects_db_factory,
            approval_checkpoints=checkpoint_read,
            io_runner=policy_runner,
            runtime_factory=value.project_runtime_factory,
            operation_guard_factory=ProjectOperationGuard,
        )
        return facade, execution_connections

    continuity_execution_facade, continuity_connections = (
        build_real_execution_facade(real_valid)
    )
    continuity_request = await (
        continuity_execution_facade.certified_execution_request(
            real_valid.execution,
            real_prepared,
        )
    )
    assert continuity_request.operation is real_prepared
    assert continuity_request.attempt is (
        real_valid.execution.attempt
    )
    assert continuity_request.payload == (
        real_valid.authority.intent.payload
    )
    assert continuity_request.operation_authority_json == (
        real_valid.authority.authority_json
    )
    assert continuity_request.operation_authority_sha256 == (
        real_valid.authority.authority_sha256
    )
    assert continuity_request.effect_scope_json == (
        real_valid.authority.effect_scope_json
    )
    assert continuity_request.effect_scope_sha256 == (
        real_valid.authority.effect_scope_sha256
    )
    assert continuity_request.policy_authority_sha256 == (
        real_row["policy_authority_sha256"]
    )
    assert_facade_connections_closed(
        continuity_connections,
        expected_count=1,
    )

    real_execution_facade, real_execution_connections = (
        build_real_execution_facade(real_valid)
    )
    real_live_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    real_live_coordinator = (
        worker_module.CanonicalProjectLiveOperationCoordinator(
            prepare_facade=real_valid.prepare_facade,
            execution_facade=real_execution_facade,
            capability_registry=ExecutionRegistry(
                real_live_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    real_live_result = await real_live_coordinator.execute(
        real_valid.execution,
        real_valid.authority,
        real_valid.carrier,
    )
    assert json.loads(real_live_result) == {
        "operation_id": real_valid.authority.intent.operation_id,
        "status": "reconciled",
    }
    assert len(
        [
            entry
            for entry in real_live_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1
    assert_facade_connections_closed(
        real_execution_connections,
        expected_count=4,
    )

    # Simulate the exact crash after the real operation reconciles but before
    # the worker can prepare/commit a terminal batch.  C12 recovery treats the
    # durable reconciled disposition as a one-way recovery block: it never
    # consults legacy readback, starts an agent, or repeats the effect.
    real_valid.clock[0] = (
        real_valid.execution.attempt.lease_expires_at + 1
    )

    class PoisonLegacyTurnReadback:
        def __init__(self):
            self.calls = []

        def read_turn(self, request):
            self.calls.append(request)
            raise AssertionError(
                "reconciled operation cannot use legacy readback"
            )

    crash_readback = PoisonLegacyTurnReadback()
    crash_connection = real_valid.projects_db_factory()
    crash_runtime = real_valid.project_runtime_factory(
        crash_connection
    )
    crash_recovered = crash_runtime.reconcile_inflight_turns(
        crash_readback,
        limit=100,
    )
    assert [turn.status for turn in crash_recovered] == [
        "reconciling"
    ]
    assert crash_readback.calls == []
    crash_turn_row = crash_connection.execute(
        """
        SELECT status, terminal_result_id, recovery_block_key
        FROM project_turns
        WHERE project_id = ? AND turn_id = ?
        """,
        (
            real_valid.project_id,
            real_valid.claim.turn_id,
        ),
    ).fetchone()
    assert crash_turn_row is not None
    assert crash_turn_row["status"] == "reconciling"
    assert crash_turn_row["terminal_result_id"] is None
    assert crash_turn_row["recovery_block_key"]
    assert crash_connection.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND turn_id = ?
          AND kind = 'turn.recovery_blocked'
        """,
        (
            real_valid.project_id,
            real_valid.claim.turn_id,
        ),
    ).fetchone()[0] == 1
    assert crash_connection.execute(
        """
        SELECT status FROM project_operations
        WHERE project_id = ? AND operation_id = ?
        """,
        (
            real_valid.project_id,
            real_valid.authority.intent.operation_id,
        ),
    ).fetchone()[0] == "reconciled"
    assert crash_runtime.claim_next_turn(
        real_valid.project_id,
        "crash-retry-worker",
        lease_seconds=90,
    ) is None
    crash_dispatcher_lease = (
        crash_runtime.acquire_dispatcher_lease(
            "d23e4567-e89b-42d3-a456-426614174099",
            lease_seconds=30,
        )
    )
    assert crash_dispatcher_lease is not None
    assert crash_runtime.claim_next_turn_for_dispatcher(
        real_valid.project_id,
        "crash-retry-dispatcher-worker",
        lease_seconds=90,
        dispatcher_lease=crash_dispatcher_lease,
    ) is None
    crash_changes = crash_connection.total_changes
    assert crash_runtime.reconcile_inflight_turns(
        crash_readback,
        limit=100,
    ) == ()
    assert crash_connection.total_changes == crash_changes
    assert crash_readback.calls == []
    assert crash_connection.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND turn_id = ?
          AND kind = 'turn.recovery_blocked'
        """,
        (
            real_valid.project_id,
            real_valid.claim.turn_id,
        ),
    ).fetchone()[0] == 1
    assert len(
        [
            entry
            for entry in real_live_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1
    crash_connection.close()

    # An explicit policy DENY never allocates approval authority and is
    # rejected by the real live coordinator before either raw facade opens,
    # so no operation or receipt can become durable.
    deny_case = seed_real_facade_case("explicit-deny")
    deny_uuid_count = len(uuid_calls)
    deny_carrier, deny_approval = (
        await authorize_real_facade_case(
            deny_case,
            allowed_action_classes=frozenset({"local_code_edit"}),
            allowed_phases=frozenset(),
        )
    )
    assert deny_carrier.decision.decision is Decision.DENY
    assert deny_approval is None
    assert len(uuid_calls) == deny_uuid_count
    deny_execution_facade, deny_execution_connections = (
        build_real_execution_facade(deny_case)
    )
    deny_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    deny_coordinator = (
        worker_module.CanonicalProjectLiveOperationCoordinator(
            prepare_facade=deny_case.prepare_facade,
            execution_facade=deny_execution_facade,
            capability_registry=ExecutionRegistry(
                deny_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    with pytest.raises(
        (PermissionError, TypeError, ValueError)
    ):
        await deny_coordinator.execute(
            deny_case.execution,
            deny_case.authority,
            deny_case.carrier,
        )
    assert deny_case.prepare_connections == []
    assert deny_execution_connections == []
    assert deny_adapter.trace == []
    deny_check = deny_case.projects_db_factory()
    try:
        assert tuple(
            deny_check.execute(
                """
                SELECT COUNT(*),
                       COUNT(*) FILTER (
                           WHERE receipt_id IS NOT NULL
                       )
                FROM project_operations
                WHERE project_id = ?
                """,
                (deny_case.project_id,),
            ).fetchone()
        ) == (0, 0)
        assert deny_check.execute(
            """
            SELECT COUNT(*) FROM project_approvals
            WHERE project_id = ?
            """,
            (deny_case.project_id,),
        ).fetchone()[0] == 0
    finally:
        deny_check.close()

    # Certified-request and mark each reconstruct the stored bytes from a
    # fresh SQLite-backed guard.  Independent post-prepare tampering fails
    # before mark/effect.
    for drift_boundary, column in (
        (
            "certified_execution_request",
            "operation_authority_sha256",
        ),
        ("mark_started", "policy_authority_sha256"),
    ):
        drift_case = seed_real_facade_case(
            f"stored-{drift_boundary}"
        )
        await authorize_real_facade_case(drift_case)
        drift_operation = await real_prepare(drift_case)
        drift_execution_facade, drift_execution_connections = (
            build_real_execution_facade(drift_case)
        )
        drift_request = None
        if drift_boundary == "mark_started":
            drift_request = await (
                drift_execution_facade
                .certified_execution_request(
                    drift_case.execution,
                    drift_operation,
                )
            )
        tamper = drift_case.projects_db_factory()
        try:
            tamper.execute(
                f"""
                UPDATE project_operations
                SET {column} = ?
                WHERE project_id = ? AND operation_id = ?
                """,
                (
                    "0" * 64,
                    drift_case.project_id,
                    drift_operation.operation_id,
                ),
            )
            tamper.commit()
        finally:
            tamper.close()
        with pytest.raises((PermissionError, ValueError)):
            if drift_boundary == "mark_started":
                assert drift_request is not None
                await drift_execution_facade.mark_started(
                    drift_request
                )
            else:
                await (
                    drift_execution_facade
                    .certified_execution_request(
                        drift_case.execution,
                        drift_operation,
                    )
                )
        assert_facade_connections_closed(
            drift_execution_connections,
            expected_count=(
                2
                if drift_boundary == "mark_started"
                else 1
            ),
        )

    # The execution facade has the same retained I/O ownership rule as
    # prepare.  Cancellation while its raw factory is blocked joins the real
    # certified-request read and closes the exact connection before it wins.
    execution_cancel_case = seed_real_facade_case(
        "execution-cancel"
    )
    await authorize_real_facade_case(execution_cancel_case)
    execution_cancel_operation = await real_prepare(
        execution_cancel_case
    )
    execution_cancel_state = SessionDB(
        db_path=execution_cancel_case.state_path
    )
    execution_cancel_state.close()

    def execution_cancel_state_factory():
        return SessionDB(
            db_path=execution_cancel_case.state_path
        )

    execution_cancel_checkpoints = (
        session_module.ProjectApprovalCheckpointReadFacade(
            execution_cancel_state_factory,
            io_runner=policy_runner,
        )
    )
    execution_cancel_entered = threading.Event()
    execution_cancel_release = threading.Event()
    execution_cancel_connections = []

    def execution_cancel_projects_db_factory():
        cancel_connection = (
            execution_cancel_case.projects_db_factory()
        )
        execution_cancel_connections.append(
            (cancel_connection, threading.get_ident())
        )
        execution_cancel_entered.set()
        assert execution_cancel_release.wait(timeout=5)
        return cancel_connection

    execution_cancel_facade = (
        worker_module.ProjectOperationExecutionFacade(
            execution_cancel_projects_db_factory,
            approval_checkpoints=(
                execution_cancel_checkpoints
            ),
            io_runner=policy_runner,
            runtime_factory=(
                execution_cancel_case
                .project_runtime_factory
            ),
            operation_guard_factory=ProjectOperationGuard,
        )
    )
    execution_cancel_task = asyncio.create_task(
        execution_cancel_facade.certified_execution_request(
            execution_cancel_case.execution,
            execution_cancel_operation,
        )
    )
    assert await asyncio.wait_for(
        asyncio.to_thread(execution_cancel_entered.wait),
        timeout=5,
    )
    execution_cancel_task.cancel()
    assert execution_cancel_task.cancelling() == 1
    try:
        assert not execution_cancel_task.done()
    finally:
        execution_cancel_release.set()
    with pytest.raises(asyncio.CancelledError):
        await execution_cancel_task
    assert_facade_connections_closed(
        execution_cancel_connections,
        expected_count=1,
    )

    policy_chain_authority = (
        allowed_carrier.operation_authority
    )
    policy_chain_facade = ExecutionFacade(
        authority=policy_chain_authority,
        execution_value=base_execution,
    )
    policy_chain_prepare = fake_prepare_facade(
        policy_chain_facade,
        base_execution,
        policy_chain_authority,
        allowed_carrier,
    )
    policy_chain_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    policy_chain_coordinator = (
        worker_module.CanonicalProjectLiveOperationCoordinator(
            prepare_facade=policy_chain_prepare,
            execution_facade=policy_chain_facade,
            capability_registry=ExecutionRegistry(
                policy_chain_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    assert json.loads(
        await policy_chain_coordinator.execute(
            base_execution,
            policy_chain_authority,
            allowed_carrier,
        )
    ) == {
        "operation_id": base_intent.operation_id,
        "status": "reconciled",
    }
    assert policy_chain_prepare.trace[0][3] is (
        policy_chain_authority
    )
    assert policy_chain_prepare.trace[0][5] is allowed_carrier
    assert (
        policy_chain_facade.request.operation_authority_json
        is policy_chain_authority.authority_json
    )
    assert (
        policy_chain_facade.request.operation_authority_sha256
        is policy_chain_authority.authority_sha256
    )
    assert (
        policy_chain_facade.request.effect_scope_json
        is policy_chain_authority.effect_scope_json
    )
    assert (
        policy_chain_facade.request.effect_scope_sha256
        is policy_chain_authority.effect_scope_sha256
    )

    live_facade = ExecutionFacade(
        authority=live_authority,
        execution_value=live_execution,
    )
    live_prepare = fake_prepare_facade(
        live_facade,
        live_execution,
        live_authority,
        live_carrier,
    )
    live_adapter = EffectAdapter(live_receipt, live_readback)
    live_coordinator = (
        worker_module.CanonicalProjectLiveOperationCoordinator(
            prepare_facade=live_prepare,
            execution_facade=live_facade,
            capability_registry=ExecutionRegistry(live_adapter),
            effect_runner=effect_runner,
        )
    )
    live_result = await live_coordinator.execute(
        live_execution,
        live_authority,
        live_carrier,
    )
    assert json.loads(live_result) == {
        "operation_id": live_authority.intent.operation_id,
        "status": "reconciled",
    }
    assert [entry[0] for entry in live_prepare.trace] == [
        "prepare"
    ]
    assert [entry[0] for entry in live_facade.trace] == [
        "certified_execution_request",
        "mark_started",
        "record_receipt",
        "reconcile",
    ]
    assert [entry[0] for entry in live_adapter.trace] == [
        "effect",
        "readback",
    ]
    assert live_prepare.trace[0][3] is live_authority
    assert live_prepare.trace[0][5] is live_carrier
    assert (
        live_facade.request.operation_authority_json
        == live_authority.authority_json
    )
    assert (
        live_facade.request.operation_authority_sha256
        == live_authority.authority_sha256
    )
    assert (
        live_facade.request.effect_scope_sha256
        == live_authority.effect_scope_sha256
    )
    live_repeat_one, live_repeat_two = await asyncio.gather(
        live_coordinator.execute(
            live_execution,
            live_authority,
            live_carrier,
        ),
        live_coordinator.execute(
            live_execution,
            live_authority,
            live_carrier,
        ),
    )
    assert live_repeat_one == live_result
    assert live_repeat_two == live_result
    assert len(
        [
            entry
            for entry in live_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1

    # A carrier cannot be paired with another valid authority.
    other_authority, _, _ = authority_fixture(
        execution("other-authority"),
    )
    mismatch_facade = ExecutionFacade(
        authority=live_authority,
        execution_value=live_execution,
    )
    mismatch_prepare = fake_prepare_facade(
        mismatch_facade,
        live_execution,
        live_authority,
        live_carrier,
    )
    mismatch_adapter = EffectAdapter(live_receipt, live_readback)
    mismatch_coordinator = (
        worker_module.CanonicalProjectLiveOperationCoordinator(
            prepare_facade=mismatch_prepare,
            execution_facade=mismatch_facade,
            capability_registry=ExecutionRegistry(
                mismatch_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    with pytest.raises(
        (TypeError, ValueError, PermissionError)
    ):
        await mismatch_coordinator.execute(
            live_execution,
            other_authority,
            live_carrier,
        )
    assert mismatch_prepare.trace == []
    assert mismatch_facade.trace == []
    assert mismatch_adapter.trace == []

    # Mark-time policy is reconstructed from Projects after a real prepare.
    # Each race is introduced only when the real execution facade opens its
    # second connection: certified request has returned, but the actual guard
    # has not yet entered mark_started.  The live coordinator therefore binds
    # the public prepare/execution/registry seams while permitting no effect.
    def mark_time_snapshot(connection, case):
        operation_row = connection.execute(
            """
            SELECT status, attempt_id, lease_generation, fencing_token,
                   receipt_id, readback_json, guard_validated
            FROM project_operations
            WHERE project_id = ? AND operation_id = ?
            """,
            (
                case.project_id,
                case.authority.intent.operation_id,
            ),
        ).fetchone()
        turn_row = connection.execute(
            """
            SELECT status, attempt_id, lease_generation, fencing_token,
                   execution_state, terminal_result_id
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (case.project_id, case.claim.turn_id),
        ).fetchone()
        control_row = connection.execute(
            """
            SELECT control_state, control_version, attempt_id,
                   claim_worker_id, claim_lease_expires_at
            FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (case.project_id, case.claim.turn_id),
        ).fetchone()
        lease_row = connection.execute(
            """
            SELECT lease_id, worker_id, lease_generation,
                   fencing_token, expires_at
            FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ?
            """,
            (case.project_id, case.claim.turn_id),
        ).fetchone()
        assert operation_row is not None
        assert turn_row is not None
        assert control_row is not None
        assert lease_row is not None
        return (
            tuple(operation_row),
            tuple(turn_row),
            tuple(control_row),
            tuple(lease_row),
            connection.execute(
                """
                SELECT COUNT(*) FROM project_events
                WHERE project_id = ?
                """,
                (case.project_id,),
            ).fetchone()[0],
        )

    for mark_race in (
        "phase",
        "contract",
        "binding",
        "stop",
    ):
        mark_case = seed_real_facade_case(
            f"mark-time-{mark_race}"
        )
        mark_carrier, mark_approval = (
            await authorize_real_facade_case(mark_case)
        )
        assert mark_carrier is mark_case.carrier
        assert mark_approval is None
        mark_operation = await real_prepare(mark_case)
        assert mark_operation.status == "approved"

        mark_state = SessionDB(db_path=mark_case.state_path)
        mark_state.close()

        def mark_state_factory():
            return SessionDB(db_path=mark_case.state_path)

        mark_checkpoints = (
            session_module.ProjectApprovalCheckpointReadFacade(
                mark_state_factory,
                io_runner=policy_runner,
            )
        )
        mark_connections = []
        mark_open_count = [0]
        before_mark = []

        def mark_projects_db_factory():
            connection = mark_case.projects_db_factory()
            mark_open_count[0] += 1
            if mark_open_count[0] == 2:
                if mark_race == "phase":
                    state = prdb.runtime_state_for_project(
                        connection,
                        mark_case.project_id,
                    )
                    assert state is not None
                    changed = prdb.transition_current_phase(
                        connection,
                        project_id=mark_case.project_id,
                        expected_version=state.version,
                        current_phase="verification",
                        updated_at=101,
                    )
                    assert changed is not None
                    assert changed.current_phase == "verification"
                elif mark_race == "contract":
                    changed = connection.execute(
                        """
                        UPDATE project_contracts
                        SET status = 'draft', updated_at = 101
                        WHERE project_id = ? AND contract_id = ?
                          AND status = 'active'
                        """,
                        (mark_case.project_id, "contract-c14"),
                    )
                    assert changed.rowcount == 1
                    connection.commit()
                elif mark_race == "binding":
                    changed = connection.execute(
                        """
                        UPDATE project_surface_bindings
                        SET actor_id = 'other-owner'
                        WHERE project_id = ? AND binding_id = ?
                          AND actor_id = 'owner-1'
                        """,
                        (
                            mark_case.project_id,
                            mark_case.binding_id,
                        ),
                    )
                    assert changed.rowcount == 1
                    connection.commit()
                else:
                    runtime = mark_case.project_runtime_factory(
                        connection
                    )
                    state = prdb.runtime_state_for_project(
                        connection,
                        mark_case.project_id,
                    )
                    assert state is not None
                    control = runtime.control_for_claim(
                        mark_case.claim
                    )
                    stopped = runtime.request_stop(
                        mark_case.project_id,
                        mark_case.claim.turn_id,
                        mark_case.actor,
                        idempotency_key=(
                            f"mark-time-stop-{mark_case.label}"
                        ),
                        expected_version=state.version,
                        expected_control_version=(
                            control.control_version
                        ),
                    )
                    assert stopped.control_state == "stop_requested"
                before_mark.append(
                    mark_time_snapshot(connection, mark_case)
                )
            mark_connections.append(
                (connection, threading.get_ident())
            )
            return connection

        mark_execution_facade = (
            worker_module.ProjectOperationExecutionFacade(
                mark_projects_db_factory,
                approval_checkpoints=mark_checkpoints,
                io_runner=policy_runner,
                runtime_factory=mark_case.project_runtime_factory,
                operation_guard_factory=ProjectOperationGuard,
            )
        )
        mark_adapter = EffectAdapter(
            live_receipt,
            live_readback,
        )
        mark_coordinator = (
            worker_module.CanonicalProjectLiveOperationCoordinator(
                prepare_facade=mark_case.prepare_facade,
                execution_facade=mark_execution_facade,
                capability_registry=ExecutionRegistry(
                    mark_adapter
                ),
                effect_runner=effect_runner,
            )
        )
        with pytest.raises(
            (
                worker_module.ProjectOperationUnresolved,
                ProjectOperationError,
                PermissionError,
                ValueError,
            )
        ):
            await mark_coordinator.execute(
                mark_case.execution,
                mark_case.authority,
                mark_case.carrier,
            )
        assert mark_open_count == [2], mark_race
        assert len(before_mark) == 1
        assert mark_adapter.trace == []
        assert_facade_connections_closed(
            mark_connections,
            expected_count=2,
        )
        assert_facade_connections_closed(
            mark_case.prepare_connections,
            expected_count=2,
        )
        mark_check = mark_case.projects_db_factory()
        try:
            after_mark = mark_time_snapshot(
                mark_check,
                mark_case,
            )
        finally:
            mark_check.close()
        assert after_mark == before_mark[0], mark_race
        assert after_mark[0][:4] == (
            "approved",
            mark_operation.attempt_id,
            mark_operation.lease_generation,
            mark_operation.fencing_token,
        )

    # A monotonically newer horizon with the complete same identity remains
    # valid; the immutable carrier retains the older authorization horizon.
    renewed_execution = replace(
        live_execution,
        attempt=replace(
            live_execution.attempt,
            lease_expires_at=260,
        ),
    )
    renewed_facade = ExecutionFacade(
        authority=live_authority,
        execution_value=renewed_execution,
    )
    renewed_prepare = fake_prepare_facade(
        renewed_facade,
        renewed_execution,
        live_authority,
        live_carrier,
    )
    renewed_adapter = EffectAdapter(live_receipt, live_readback)
    renewed_coordinator = (
        worker_module.CanonicalProjectLiveOperationCoordinator(
            prepare_facade=renewed_prepare,
            execution_facade=renewed_facade,
            capability_registry=ExecutionRegistry(
                renewed_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    renewed_result = await renewed_coordinator.execute(
        renewed_execution,
        live_authority,
        live_carrier,
    )
    assert json.loads(renewed_result)["status"] == "reconciled"
    assert (
        live_carrier.execution_attempt.lease_expires_at
        == 220
    )
    assert (
        renewed_facade.request.attempt.lease_expires_at
        == 260
    )

    class ReceiptSubclass(OperationReceipt):
        pass

    live_failure_cases = (
        ("mark_started", live_receipt, live_readback, "reconciled"),
        ("effect_exception", RuntimeError("effect failed"), live_readback, "unknown"),
        ("effect_none", None, live_readback, "unknown"),
        ("malformed_receipt", {"receipt_id": "fake"}, live_readback, "reconciled"),
        (
            "subclass_receipt",
            ReceiptSubclass("receipt-subclass", {"ok": True}),
            live_readback,
            "reconciled",
        ),
        ("malformed_readback", live_receipt, object(), "blocked"),
    )
    for label, effect_value, readback_value, final_status in (
        live_failure_cases
    ):
        failure_facade = ExecutionFacade(
            authority=live_authority,
            execution_value=live_execution,
            final_status=final_status,
        )
        failure_prepare = fake_prepare_facade(
            failure_facade,
            live_execution,
            live_authority,
            live_carrier,
        )
        if label == "mark_started":
            failure_facade.fail_at = "mark_started"
        failure_adapter = EffectAdapter(
            effect_value,
            readback_value,
        )
        failure_coordinator = (
            worker_module.CanonicalProjectLiveOperationCoordinator(
                prepare_facade=failure_prepare,
                execution_facade=failure_facade,
                capability_registry=ExecutionRegistry(
                    failure_adapter
                ),
                effect_runner=effect_runner,
            )
        )
        with pytest.raises(
            worker_module.ProjectOperationUnresolved
        ):
            await failure_coordinator.execute(
                live_execution,
                live_authority,
                live_carrier,
            )
        effect_calls = [
            entry
            for entry in failure_adapter.trace
            if entry[0] == "effect"
        ]
        assert len(effect_calls) == (
            0 if label == "mark_started" else 1
        )
        assert len(
            [
                entry
                for entry in failure_facade.trace
                if entry[0] == "reconcile"
            ]
        ) == (0 if label == "mark_started" else 1)
        assert not any(
            entry[0] == "record_receipt"
            for entry in failure_facade.trace
            if label
            in {
                "effect_exception",
                "effect_none",
                "malformed_receipt",
                "subclass_receipt",
            }
        )

    # Caller cancellation during the already-started live effect retains and
    # joins that effect, performs one late reconcile, and still beats the
    # reconciled result.  A repeat cannot issue a second effect.
    live_cancel_facade = ExecutionFacade(
        authority=live_authority,
        execution_value=live_execution,
    )
    live_cancel_prepare = fake_prepare_facade(
        live_cancel_facade,
        live_execution,
        live_authority,
        live_carrier,
    )
    live_cancel_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    live_cancel_adapter.gated = True
    live_cancel_coordinator = (
        worker_module.CanonicalProjectLiveOperationCoordinator(
            prepare_facade=live_cancel_prepare,
            execution_facade=live_cancel_facade,
            capability_registry=ExecutionRegistry(
                live_cancel_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    live_cancel_task = asyncio.create_task(
        live_cancel_coordinator.execute(
            live_execution,
            live_authority,
            live_carrier,
        )
    )
    await asyncio.wait_for(
        asyncio.to_thread(live_cancel_adapter.entered.wait),
        timeout=5,
    )
    live_cancel_task.cancel()
    assert not live_cancel_task.done()
    live_cancel_adapter.release.set()
    with pytest.raises(asyncio.CancelledError):
        await live_cancel_task
    assert len(
        [
            entry
            for entry in live_cancel_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1
    assert len(
        [
            entry
            for entry in live_cancel_facade.trace
            if entry[0] == "reconcile"
        ]
    ) == 1
    with pytest.raises(
        (
            asyncio.CancelledError,
            worker_module.ProjectOperationUnresolved,
        )
    ):
        await live_cancel_coordinator.execute(
            live_execution,
            live_authority,
            live_carrier,
        )
    assert len(
        [
            entry
            for entry in live_cancel_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1

    # Stored authority, payload, policy digest and registry fingerprint are
    # revalidated from the exact certified request before mark/effect.
    request_drifts = (
        {
            "authority_json": (
                live_authority.authority_json + " "
            )
        },
        {"authority_sha256": "0" * 64},
        {
            "effect_scope_json": (
                live_authority.effect_scope_json + " "
            )
        },
        {"effect_scope_sha256": "1" * 64},
        {"policy_authority_sha256": "2" * 64},
        {"payload": {"path": "C:/work/other.py"}},
        {
            "fingerprint": (
                "local_code_edit",
                2,
                "remote-ledger",
                True,
            )
        },
    )
    for changes in request_drifts:
        drift_facade = ExecutionFacade(
            authority=live_authority,
            execution_value=live_execution,
        )
        drift_prepare = fake_prepare_facade(
            drift_facade,
            live_execution,
            live_authority,
            live_carrier,
        )
        drift_facade.request = certified_request(
            drift_facade.prepared,
            live_authority,
            live_execution.attempt,
            **changes,
        )
        drift_adapter = EffectAdapter(live_receipt, live_readback)
        drift_coordinator = (
            worker_module.CanonicalProjectLiveOperationCoordinator(
                prepare_facade=drift_prepare,
                execution_facade=drift_facade,
                capability_registry=ExecutionRegistry(
                    drift_adapter
                ),
                effect_runner=effect_runner,
            )
        )
        with pytest.raises(
            (
                worker_module.ProjectOperationUnresolved,
                TypeError,
                ValueError,
                PermissionError,
            )
        ):
            await drift_coordinator.execute(
                live_execution,
                live_authority,
                live_carrier,
            )
        assert drift_adapter.trace == []
        assert not any(
            entry[0] == "mark_started"
            for entry in drift_facade.trace
        )

    # The real checkpoint coordinator crosses State prepare, Projects guard,
    # the durable operation-prepared latch and State apply as distinct stages.
    # Exact DTOs, not object() carriers, flow through every boundary.
    checkpoint_trace = []
    checkpoint_batch_id = (
        "723e4567-e89b-42d3-a456-426614174000"
    )
    checkpoint_execution = execution("checkpoint")
    checkpoint_authority, checkpoint_carrier, checkpoint_approval = (
        authority_fixture(
            checkpoint_execution,
            action="publish",
            action_class="publish",
            decision=Decision.REQUIRE_APPROVAL,
            approval_id=(
                "823e4567-e89b-42d3-a456-426614174000"
            ),
            approval_expires_at=3700,
        )
    )
    checkpoint_messages = (
        {
            "role": "user",
            "content": canonical_payload,
        },
        {
            "role": "assistant",
            "content": (
                '{"approval_id":'
                '"823e4567-e89b-42d3-a456-426614174000",'
                '"kind":"project_operation_awaiting_approval",'
                '"operation_id":"operation-attempt-checkpoint"}'
            ),
        },
    )

    class CheckpointState:
        def __init__(self, outcome="published"):
            self.outcome = outcome
            self.fail_prepare = False
            self.fail_apply = False
            self.guard_entered = asyncio.Event()
            self.guard_release = asyncio.Event()
            self.before_apply_entered = asyncio.Event()
            self.before_apply_release = asyncio.Event()
            self.apply_entered = asyncio.Event()
            self.apply_release = asyncio.Event()
            self.gate_guard = False
            self.gate_before_apply = False
            self.gate_apply = False

        async def prepare_approval_checkpoint(
            self,
            claim,
            *,
            batch_id,
            operation_id,
            approval_id,
            base_message_count,
            messages,
        ):
            checkpoint_trace.append(
                (
                    "state_prepare",
                    claim,
                    batch_id,
                    operation_id,
                    approval_id,
                    base_message_count,
                    tuple(messages),
                )
            )
            if self.fail_prepare:
                raise RuntimeError("State prepare failed")
            return PendingProjectBatch(
                batch_id,
                1,
                "approval_checkpoint",
                "prepared",
                checkpoint_execution.attempt,
                None,
                operation_id,
                approval_id,
                base_message_count,
                100.0,
            )

        async def apply_project_batch(self, batch_id):
            if self.gate_before_apply:
                self.before_apply_entered.set()
                await self.before_apply_release.wait()
            checkpoint_trace.append(("state_apply", batch_id))
            self.apply_entered.set()
            if self.gate_apply:
                await self.apply_release.wait()
            if self.fail_apply:
                raise RuntimeError("State apply failed")
            return ProjectBatchApplyResult(self.outcome)

    class CheckpointPrepare:
        def __init__(
            self,
            state,
            *,
            expected_policy_authority=None,
        ):
            self.state = state
            self.fail = False
            self.expected_policy_authority = (
                expected_policy_authority
            )

        async def prepare(
            self,
            claim,
            intent,
            *,
            authority,
            policy,
            policy_authority,
            approval,
            approval_checkpoint_id,
        ):
            checkpoint_trace.append(
                (
                    "projects_prepare",
                    claim,
                    intent,
                    policy,
                    authority,
                    policy_authority,
                    approval,
                    approval_checkpoint_id,
                )
            )
            if (
                self.expected_policy_authority is not None
                and (
                    type(policy_authority)
                    is not type(self.expected_policy_authority)
                    or policy_authority
                    != self.expected_policy_authority
                )
            ):
                raise PermissionError(
                    "Projects guard rejected forged policy authority"
                )
            if self.fail:
                raise PermissionError("Projects guard failed")
            prepared_operation = operation_value(
                authority,
                status="awaiting_approval",
                attempt_value=checkpoint_execution.attempt,
                approval_id=approval.approval_id,
            )
            checkpoint_trace.append(
                ("projects_durable_commit", prepared_operation)
            )
            self.state.guard_entered.set()
            if self.state.gate_guard:
                await self.state.guard_release.wait()
            return prepared_operation

    baseline_advances = []

    def checkpoint_coordinator(state, prepare):
        return (
            worker_module.CanonicalProjectOperationCheckpointCoordinator(
                batches=state,
                operations=prepare,
                batch_id_factory=lambda: checkpoint_batch_id,
                on_published=lambda messages: (
                    baseline_advances.append(tuple(messages))
                ),
            )
        )

    state = CheckpointState()
    prepare = CheckpointPrepare(state)
    coordinator = checkpoint_coordinator(state, prepare)
    with pytest.raises(worker_module.ProjectApprovalPublished):
        await coordinator.checkpoint_operation_intent(
            checkpoint_execution,
            checkpoint_authority,
            checkpoint_carrier,
            checkpoint_approval,
            base_message_count=9,
            messages=checkpoint_messages,
        )
    assert [entry[0] for entry in checkpoint_trace] == [
        "state_prepare",
        "projects_prepare",
        "projects_durable_commit",
        "state_apply",
    ]
    assert baseline_advances == [checkpoint_messages]
    assert coordinator.operation_prepared is True
    assert all(
        "tool_calls" not in message
        for message in checkpoint_messages
    )

    for outcome in (
        "wait",
        "discarded",
        "conflicted",
        "settlement_pending",
        "remediation_pending",
        "state_conflict",
        "authority_conflict",
    ):
        checkpoint_trace.clear()
        baseline_advances.clear()
        state = CheckpointState(outcome)
        prepare = CheckpointPrepare(state)
        coordinator = checkpoint_coordinator(state, prepare)
        with pytest.raises(
            worker_module.ProjectCheckpointSettlementPending
        ):
            await coordinator.checkpoint_operation_intent(
                checkpoint_execution,
                checkpoint_authority,
                checkpoint_carrier,
                checkpoint_approval,
                base_message_count=9,
                messages=checkpoint_messages,
            )
        assert [entry[0] for entry in checkpoint_trace] == [
            "state_prepare",
            "projects_prepare",
            "projects_durable_commit",
            "state_apply",
        ]
        assert coordinator.operation_prepared is True
        assert baseline_advances == []

    state = CheckpointState()
    state.fail_prepare = True
    coordinator = checkpoint_coordinator(
        state,
        CheckpointPrepare(state),
    )
    with pytest.raises(worker_module.ProjectCheckpointFailed):
        await coordinator.checkpoint_operation_intent(
            checkpoint_execution,
            checkpoint_authority,
            checkpoint_carrier,
            checkpoint_approval,
            base_message_count=9,
            messages=checkpoint_messages,
        )
    assert coordinator.operation_prepared is False

    state = CheckpointState()
    prepare = CheckpointPrepare(state)
    prepare.fail = True
    coordinator = checkpoint_coordinator(state, prepare)
    with pytest.raises(worker_module.ProjectCheckpointFailed):
        await coordinator.checkpoint_operation_intent(
            checkpoint_execution,
            checkpoint_authority,
            checkpoint_carrier,
            checkpoint_approval,
            base_message_count=9,
            messages=checkpoint_messages,
        )
    assert coordinator.operation_prepared is False

    state = CheckpointState()
    state.fail_apply = True
    coordinator = checkpoint_coordinator(
        state,
        CheckpointPrepare(state),
    )
    with pytest.raises(
        worker_module.ProjectCheckpointSettlementPending
    ):
        await coordinator.checkpoint_operation_intent(
            checkpoint_execution,
            checkpoint_authority,
            checkpoint_carrier,
            checkpoint_approval,
            base_message_count=9,
            messages=checkpoint_messages,
        )
    assert coordinator.operation_prepared is True
    assert baseline_advances == []

    # Cancellation before guard-result delivery, at the returned-guard /
    # State-apply boundary, and during State apply is recorded on the dedicated
    # latch.  Neither child task nor the coordinator is cancelled; each joins
    # and produces SettlementPending.
    for stage in (
        "guard_result",
        "after_guard_return",
        "state_apply",
    ):
        checkpoint_trace.clear()
        state = CheckpointState()
        state.gate_guard = stage == "guard_result"
        state.gate_before_apply = (
            stage == "after_guard_return"
        )
        state.gate_apply = stage == "state_apply"
        coordinator = checkpoint_coordinator(
            state,
            CheckpointPrepare(state),
        )
        task = asyncio.create_task(
            coordinator.checkpoint_operation_intent(
                checkpoint_execution,
                checkpoint_authority,
                checkpoint_carrier,
                checkpoint_approval,
                base_message_count=9,
                messages=checkpoint_messages,
            )
        )
        if stage == "guard_result":
            await state.guard_entered.wait()
        elif stage == "after_guard_return":
            await state.before_apply_entered.wait()
        else:
            await state.apply_entered.wait()
        assert coordinator.request_cancel() is True
        assert coordinator.request_cancel() is False
        assert not task.cancelled()
        assert not task.done()
        state.guard_release.set()
        state.before_apply_release.set()
        state.apply_release.set()
        with pytest.raises(
            worker_module.ProjectCheckpointSettlementPending
        ):
            await task
        assert task.done()
        assert not task.cancelled()
        assert coordinator.operation_prepared is True
        assert baseline_advances == []

    # Repeat all three cancellation boundaries through the actual synchronous
    # AIAgent callback bridge.  The test wraps (but does not replace)
    # run_coroutine_threadsafe so it can prove the exact concurrent Future is
    # never cancelled and the agent callback remains blocked until the
    # owner-loop coordinator has joined its child stage.
    real_run_coroutine_threadsafe = (
        asyncio.run_coroutine_threadsafe
    )
    submitted_bridge_futures = []

    def observed_run_coroutine_threadsafe(coro, loop):
        future = real_run_coroutine_threadsafe(coro, loop)
        submitted_bridge_futures.append(future)
        return future

    monkeypatch.setattr(
        asyncio,
        "run_coroutine_threadsafe",
        observed_run_coroutine_threadsafe,
    )

    class BridgeAIAgent(run_agent.AIAgent):
        def __init__(self, project_gate):
            super().__init__(
                api_key="test-key-1234567890",
                base_url="https://hermetic-provider.invalid/v1",
                model="frozen-model",
                quiet_mode=True,
                skip_context_files=True,
                skip_memory=True,
                project_execution_gate=project_gate,
                project_tool_schemas=tuple(frozen_tool_definitions),
                project_registry_generation=23,
                project_request_timeout=30.0,
                provider_metadata_prewarm=False,
                external_memory_sync=False,
                memory_review=False,
                skill_review=False,
                plugin_lifecycle=False,
            )
            self.client = MagicMock()
            self._disable_streaming = True
            self.project_execution_gate = project_gate
            self.callback_entered = threading.Event()
            self.callback_exited = threading.Event()
            self.interrupt_count = 0

        def run_conversation(
            self,
            *,
            user_message,
            conversation_history,
        ):
            self.callback_entered.set()
            try:
                call = tool_call(
                    "publish",
                    (
                        '{"content":"exact",'
                        '"path":"C:/work/file.py"}'
                    ),
                    "checkpoint-publish",
                )
                self.client.chat.completions.create.side_effect = (
                    model_tool_response([call]),
                    model_final_response("checkpoint-publish"),
                )
                return super().run_conversation(
                    user_message=user_message,
                    conversation_history=conversation_history,
                )
            finally:
                self.callback_exited.set()

        def interrupt(self):
            self.interrupt_count += 1
            return super().interrupt()

    class BridgeFactoryHarness:
        def __init__(
            self,
            coordinator_value,
            *,
            context_value,
            execution_value,
            authority_value,
            carrier_value,
            approval_value,
        ):
            self.coordinator = coordinator_value
            self.context = context_value
            self.execution = execution_value
            self.runner = RetainedThreadRunner(
                "c14-checkpoint-agent",
                max_workers=1,
            )
            self.agent = None
            self.resolver_calls = 0
            self.builder_calls = 0
            self.authorizer = Authorizer([])
            self.authorizer.decisions.append(
                SimpleNamespace(
                    action="typed_project_operation",
                    authority=authority_value,
                    policy=carrier_value,
                    approval=approval_value,
                )
            )

        def resolve_snapshot(
            self,
            context_value,
            contract_revision,
        ):
            self.resolver_calls += 1
            assert context_value is self.context
            assert contract_revision == 7
            return snapshot()

        def build_agent(self, snapshot_value, **kwargs):
            self.builder_calls += 1
            self.agent = BridgeAIAgent(
                kwargs["project_execution_gate"]
            )
            return self.agent

        def build(self):
            return worker_module.GatewayProjectAgentFactory(
                snapshot_resolver=self.resolve_snapshot,
                agent_builder=self.build_agent,
                off_loop_runner=self.runner,
                turn_context_binder=TurnBinder([]),
                tool_authorizer=self.authorizer,
                checkpoint_coordinator=self.coordinator,
            )

        def close(self):
            self.runner.close()

    original_guard_prepare = ProjectOperationGuard.prepare
    active_bridge_guard_gate = [None]

    def gated_real_guard_prepare(guard, *args, **kwargs):
        operation = original_guard_prepare(
            guard,
            *args,
            **kwargs,
        )
        gate = active_bridge_guard_gate[0]
        if gate is not None:
            gate.guard_committed.set()
            if gate.stage == "guard_result":
                gate.entered.set()
                assert gate.release.wait(timeout=5)
        return operation

    monkeypatch.setattr(
        ProjectOperationGuard,
        "prepare",
        gated_real_guard_prepare,
    )

    async def real_checkpoint_bridge_case(label, stage):
        case = seed_real_facade_case(label, critical=True)
        returned_carrier, returned_approval = (
            await authorize_real_facade_case(case)
        )
        assert returned_carrier is case.carrier
        assert returned_carrier.operation_authority is (
            case.authority
        )
        assert returned_approval is case.approval
        assert returned_approval is not None
        state_connection = SessionDB(
            db_path=case.state_path
        )
        state_connection.create_session(
            case.session_id,
            source="cli",
        )
        state_connection.close()

        gate = SimpleNamespace(
            stage=stage,
            entered=threading.Event(),
            release=threading.Event(),
            guard_committed=threading.Event(),
        )
        state_connections = []

        def state_db_factory():
            if (
                stage == "after_guard_return"
                and gate.guard_committed.is_set()
            ):
                gate.entered.set()
                assert gate.release.wait(timeout=5)
            state_value = SessionDB(db_path=case.state_path)
            state_connections.append(state_value)
            return state_value

        def state_authority_projects_factory():
            if stage == "state_apply":
                gate.entered.set()
                assert gate.release.wait(timeout=5)
            return case.projects_db_factory()

        batches = session_module.ProjectBatchWorkerFacade(
            state_db_factory,
            projects_db_factory=(
                state_authority_projects_factory
            ),
            io_runner=policy_runner,
        )
        published_messages = []
        stage_number = {
            "guard_result": 1,
            "after_guard_return": 2,
            "state_apply": 3,
            "published": 4,
        }[stage]
        batch_id = (
            f"b23e4567-e89b-42d3-a456-42661417400"
            f"{stage_number}"
        )
        coordinator_value = (
            worker_module
            .CanonicalProjectOperationCheckpointCoordinator(
                batches=batches,
                operations=case.prepare_facade,
                batch_id_factory=lambda: batch_id,
                on_published=lambda messages: (
                    published_messages.append(tuple(messages))
                ),
            )
        )
        context_value = SessionContext(
            source=SessionSource(
                platform=Platform.LOCAL,
                chat_id=f"project:{case.project_id}",
            ),
            connected_platforms=[],
            home_channels={},
            session_key=f"project:{case.project_id}",
            session_id=case.session_id,
        )
        history_value = ProjectHistorySnapshot(
            case.session_id,
            (),
            0,
        )
        canonical_user_message = json.dumps(
            case.execution.payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        expected_messages = (
            {
                "role": "user",
                "content": canonical_user_message,
            },
            {
                "role": "assistant",
                "content": json.dumps(
                    {
                        "approval_id": (
                            case.approval.approval_id
                        ),
                        "kind": (
                            "project_operation_awaiting_approval"
                        ),
                        "operation_id": (
                            case.authority.intent.operation_id
                        ),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
        harness_value = BridgeFactoryHarness(
            coordinator_value,
            context_value=context_value,
            execution_value=case.execution,
            authority_value=case.authority,
            carrier_value=case.carrier,
            approval_value=case.approval,
        )
        return SimpleNamespace(
            case=case,
            gate=gate,
            batches=batches,
            state_connections=state_connections,
            coordinator=coordinator_value,
            context=context_value,
            history=history_value,
            messages=expected_messages,
            published_messages=published_messages,
            harness=harness_value,
            batch_id=batch_id,
        )

    for stage in (
        "guard_result",
        "after_guard_return",
        "state_apply",
    ):
        real_bridge = await real_checkpoint_bridge_case(
            f"real-bridge-{stage}",
            stage,
        )
        active_bridge_guard_gate[0] = real_bridge.gate
        harness = real_bridge.harness
        bridge_factory = harness.build()
        bridge_build = await bridge_factory.resolve_project_agent(
            context=real_bridge.context,
            contract_revision=7,
        )
        bridge_parent = await bridge_build.create_project_agent(
            history=real_bridge.history
        )
        bridge_turn = bridge_parent.create_turn(
            real_bridge.case.execution,
            None,
        )
        futures_before = len(submitted_bridge_futures)
        tasks_before = set(asyncio.all_tasks())
        bridge_result = asyncio.create_task(
            bridge_turn.result()
        )
        assert await asyncio.wait_for(
            asyncio.to_thread(
                real_bridge.gate.entered.wait
            ),
            timeout=5,
        )
        gated_projects = (
            real_bridge.case.projects_db_factory()
        )
        try:
            gated_operation = gated_projects.execute(
                """
                SELECT status, approval_checkpoint_id
                FROM project_operations
                WHERE project_id = ? AND operation_id = ?
                """,
                (
                    real_bridge.case.project_id,
                    (
                        real_bridge.case.authority.intent
                        .operation_id
                    ),
                ),
            ).fetchone()
            assert gated_operation is not None
            assert gated_operation["status"] == (
                "awaiting_approval"
            )
            assert gated_operation[
                "approval_checkpoint_id"
            ] == real_bridge.batch_id
        finally:
            gated_projects.close()
        gated_state = SessionDB(
            db_path=real_bridge.case.state_path
        )
        try:
            assert gated_state._conn.execute(
                """
                SELECT state
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (real_bridge.batch_id,),
            ).fetchone()[0] == "prepared"
        finally:
            gated_state.close()
        assert harness.agent is not None
        await asyncio.wait_for(
            asyncio.to_thread(
                harness.agent.callback_entered.wait
            ),
            timeout=5,
        )
        assert len(submitted_bridge_futures) == (
            futures_before + 2
        )
        classifier_future = submitted_bridge_futures[
            futures_before
        ]
        assert classifier_future.done()
        assert not classifier_future.cancelled()
        assert classifier_future.exception() is None
        bridge_future = submitted_bridge_futures[-1]
        assert not bridge_future.done()
        assert not bridge_future.cancelled()
        assert not harness.agent.callback_exited.is_set()
        assert bridge_turn.request_cancel() is True
        assert bridge_turn.request_cancel() is False
        assert harness.agent.interrupt_count == 1
        assert coordinator.cancel_requested is True
        assert not bridge_future.done()
        assert not bridge_future.cancelled()
        assert not bridge_result.done()
        assert not harness.agent.callback_exited.is_set()

        real_bridge.gate.release.set()
        with pytest.raises(
            worker_module.ProjectCheckpointSettlementPending
        ):
            await bridge_result
        active_bridge_guard_gate[0] = None
        await bridge_turn.wait_quiescent()
        assert bridge_future.done()
        assert not bridge_future.cancelled()
        assert type(bridge_future.exception()) is (
            worker_module.ProjectCheckpointSettlementPending
        )
        assert harness.agent.callback_exited.is_set()
        assert len(harness.authorizer.calls) == 1
        assert harness.authorizer.calls[0][0] == (
            real_bridge.case.execution
        )
        assert (
            harness.authorizer.calls[0][1].canonical_action
            == "publish"
        )
        assert harness.authorizer.calls[0][1].route == (
            "sequential"
        )
        assert harness.authorizer.calls[0][3] == owner_thread
        assert harness.authorizer.decisions == []
        assert real_bridge.coordinator.operation_prepared is True
        assert real_bridge.published_messages == []
        assert_facade_connections_closed(
            real_bridge.case.prepare_connections,
            expected_count=1,
        )
        assert real_bridge.state_connections
        for closed_state in real_bridge.state_connections:
            with pytest.raises(
                sqlite3.ProgrammingError,
                match="closed database",
            ):
                closed_state._conn.execute("SELECT 1")
        projects_check = (
            real_bridge.case.projects_db_factory()
        )
        try:
            operation_row = projects_check.execute(
                """
                SELECT status, approval_id,
                       approval_checkpoint_id
                FROM project_operations
                WHERE project_id = ? AND operation_id = ?
                """,
                (
                    real_bridge.case.project_id,
                    (
                        real_bridge.case.authority.intent
                        .operation_id
                    ),
                ),
            ).fetchone()
            assert operation_row is not None
            assert operation_row["approval_id"] == (
                real_bridge.case.approval.approval_id
            )
            assert operation_row[
                "approval_checkpoint_id"
            ] == real_bridge.batch_id
        finally:
            projects_check.close()
        state_check = SessionDB(
            db_path=real_bridge.case.state_path
        )
        try:
            batch_row = state_check._conn.execute(
                """
                SELECT state, operation_id, approval_id
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (real_bridge.batch_id,),
            ).fetchone()
            assert batch_row is not None
            assert batch_row["state"] in {
                "prepared",
                "published",
            }
            assert batch_row["operation_id"] == (
                real_bridge.case.authority.intent.operation_id
            )
            assert batch_row["approval_id"] == (
                real_bridge.case.approval.approval_id
            )
        finally:
            state_check.close()
        assert not {
            task
            for task in asyncio.all_tasks()
            if (
                task not in tasks_before
                and task is not asyncio.current_task()
                and not task.done()
            )
        }
        await bridge_factory.release_project_agent(
            bridge_parent
        )
        harness.close()

    # The same real AIAgent REQUIRE_APPROVAL scheduling path also proves the
    # non-cancelled published outcome.  The exact bridge Future carries the
    # dedicated publication signal, the canonical suffix becomes baseline,
    # and the critical capability/raw executor remains untouched.
    published_bridge = await real_checkpoint_bridge_case(
        "real-bridge-published",
        "published",
    )
    published_harness = published_bridge.harness
    published_factory = published_harness.build()
    published_build = await published_factory.resolve_project_agent(
        context=published_bridge.context,
        contract_revision=7,
    )
    published_parent = await published_build.create_project_agent(
        history=published_bridge.history
    )
    published_turn = published_parent.create_turn(
        published_bridge.case.execution,
        None,
    )
    futures_before = len(submitted_bridge_futures)
    dispatch_before = len(
        [item for item in legacy_trace if item[0] == "dispatch"]
    )
    with pytest.raises(worker_module.ProjectApprovalPublished):
        await published_turn.result()
    await published_turn.wait_quiescent()
    assert len(submitted_bridge_futures) == futures_before + 2
    published_classifier_future = submitted_bridge_futures[
        futures_before
    ]
    assert published_classifier_future.done()
    assert not published_classifier_future.cancelled()
    assert published_classifier_future.exception() is None
    published_bridge_future = submitted_bridge_futures[-1]
    assert published_bridge_future.done()
    assert not published_bridge_future.cancelled()
    assert type(published_bridge_future.exception()) is (
        worker_module.ProjectApprovalPublished
    )
    assert published_harness.agent is not None
    assert published_harness.agent.callback_exited.is_set()
    assert len(published_harness.authorizer.calls) == 1
    assert published_harness.authorizer.calls[0][0] == (
        published_bridge.case.execution
    )
    assert (
        published_harness.authorizer.calls[0][1]
        .canonical_action
        == "publish"
    )
    assert published_harness.authorizer.calls[0][1].route == (
        "sequential"
    )
    assert (
        published_harness.authorizer.calls[0][3]
        == owner_thread
    )
    assert published_harness.authorizer.decisions == []
    assert published_bridge.published_messages == [
        published_bridge.messages
    ]
    assert (
        published_bridge.coordinator.operation_prepared
        is True
    )
    assert_facade_connections_closed(
        published_bridge.case.prepare_connections,
        expected_count=1,
    )
    assert published_bridge.state_connections
    for closed_state in published_bridge.state_connections:
        with pytest.raises(
            sqlite3.ProgrammingError,
            match="closed database",
        ):
            closed_state._conn.execute("SELECT 1")
    assert len(
        [item for item in legacy_trace if item[0] == "dispatch"]
    ) == dispatch_before
    await published_factory.release_project_agent(
        published_parent
    )
    published_harness.close()

    # Persisted authority/certificate bytes are revalidated again at the
    # actual dispatcher rehydration boundary.  Negative-only SQL tampering
    # cannot produce a WorkerStart, successor lease, or capability effect.
    for tamper_index, tamper_column in enumerate(
        (
            "operation_authority_json",
            "operation_authority_sha256",
            "effect_scope_json",
            "effect_scope_sha256",
            "policy_authority_json",
            "policy_authority_sha256",
        ),
        start=5,
    ):
        tamper_bridge = await real_checkpoint_bridge_case(
            f"rehydrate-tamper-{tamper_column}",
            "published",
        )
        with pytest.raises(worker_module.ProjectApprovalPublished):
            await (
                tamper_bridge.coordinator
                .checkpoint_operation_intent(
                    tamper_bridge.case.execution,
                    tamper_bridge.case.authority,
                    tamper_bridge.case.carrier,
                    tamper_bridge.case.approval,
                    base_message_count=0,
                    messages=tamper_bridge.messages,
                )
            )
        tamper_bridge.harness.close()
        assert_facade_connections_closed(
            tamper_bridge.case.prepare_connections,
            expected_count=1,
        )
        assert tamper_bridge.state_connections
        for closed_state in tamper_bridge.state_connections:
            with pytest.raises(
                sqlite3.ProgrammingError,
                match="closed database",
            ):
                closed_state._conn.execute("SELECT 1")
        tamper_connection = (
            tamper_bridge.case.projects_db_factory()
        )
        tamper_runtime = (
            tamper_bridge.case.project_runtime_factory(
                tamper_connection
            )
        )
        tamper_guard = ProjectOperationGuard(tamper_runtime)
        tamper_approved = (
            tamper_guard.resolve_operation_approval(
                tamper_bridge.case.approval.approval_id,
                tamper_bridge.case.actor,
                outcome="approved",
            )
        )
        assert tamper_approved.status == "approved"
        tamper_lease = tamper_runtime.acquire_dispatcher_lease(
            (
                "c23e4567-e89b-42d3-a456-"
                f"42661417400{tamper_index:x}"
            ),
            lease_seconds=30,
        )
        assert tamper_lease is not None
        original_turn = tamper_runtime.execution_input_for_claim(
            tamper_bridge.case.claim
        )
        tamper_connection.execute(
            f"""
            UPDATE project_operations
            SET {tamper_column} = ?
            WHERE project_id = ? AND operation_id = ?
            """,
            (
                (
                    '{"forged":"authority"}'
                    if tamper_column.endswith("_json")
                    else "0" * 64
                ),
                tamper_bridge.case.project_id,
                tamper_bridge.case.authority.intent.operation_id,
            ),
        )
        tamper_connection.commit()
        try:
            tampered_start = (
                tamper_guard
                .rehydrate_approved_operation_for_dispatcher(
                    tamper_bridge.case.project_id,
                    (
                        tamper_bridge.case.authority.intent
                        .operation_id
                    ),
                    worker_id="tamper-must-not-start",
                    lease_seconds=90,
                    dispatcher_lease=tamper_lease,
                )
            )
        except ProjectOperationError:
            tampered_start = None
        assert tampered_start is None
        unchanged_turn = (
            tamper_runtime.execution_input_for_claim(
                tamper_bridge.case.claim
            )
        )
        assert unchanged_turn == original_turn
        assert tamper_runtime.control_for_claim(
            tamper_bridge.case.claim
        ).lease_expires_at == (
            tamper_bridge.case.claim.lease_expires_at
        )
        unchanged_operation = tamper_connection.execute(
            """
            SELECT status, attempt_id, lease_generation,
                   fencing_token
            FROM project_operations
            WHERE project_id = ? AND operation_id = ?
            """,
            (
                tamper_bridge.case.project_id,
                tamper_bridge.case.authority.intent.operation_id,
            ),
        ).fetchone()
        assert unchanged_operation is not None
        assert tuple(unchanged_operation) == (
            "approved",
            tamper_approved.attempt_id,
            tamper_approved.lease_generation,
            tamper_approved.fencing_token,
        )
        tamper_connection.close()

    # Carrier forgery is rejected either by the coordinator's local pair/
    # decision checks or by the Projects guard before its durable commit.  The
    # latter owns current policy truth, so State may already be prepared but is
    # never applied and no model baseline advances.
    forged_carriers = {
        "execution_attempt": replace(
            checkpoint_carrier,
            execution_attempt=replace(
                checkpoint_carrier.execution_attempt,
                lease_expires_at=(
                    checkpoint_carrier.execution_attempt.lease_expires_at
                    + 1
                ),
            ),
        ),
        "operation_authority": replace(
            checkpoint_carrier,
            operation_authority=other_authority,
        ),
        "execution_origin": replace(
            checkpoint_carrier,
            execution_origin=replace(
                checkpoint_carrier.execution_origin,
                actor_id="other-owner",
            ),
        ),
        "control_version": replace(
            checkpoint_carrier,
            control_version=(
                checkpoint_carrier.control_version + 1
            ),
        ),
        "runtime_version": replace(
            checkpoint_carrier,
            runtime_version=(
                checkpoint_carrier.runtime_version + 1
            ),
        ),
        "project": replace(
            checkpoint_carrier,
            project=replace(
                checkpoint_carrier.project,
                lifecycle="stopped",
            ),
        ),
        "contract_id": replace(
            checkpoint_carrier,
            contract_id="other-contract",
        ),
        "contract_status": replace(
            checkpoint_carrier,
            contract_status="draft",
        ),
        "contract_json_sha256": replace(
            checkpoint_carrier,
            contract_json_sha256="other-contract-digest",
        ),
        "contract": replace(
            checkpoint_carrier,
            contract=replace(
                checkpoint_carrier.contract,
                revision=(
                    checkpoint_carrier.contract.revision + 1
                ),
            ),
        ),
        "actor": replace(
            checkpoint_carrier,
            actor=replace(
                checkpoint_carrier.actor,
                binding_id="other-binding",
            ),
        ),
        "decision": replace(
            checkpoint_carrier,
            decision=PolicyDecision(
                Decision.DENY,
                "c14-forged-policy",
                "forged decision",
            ),
        ),
    }
    assert set(forged_carriers) == {
        field.name for field in fields(checkpoint_carrier)
    }
    for field_name, forged in forged_carriers.items():
        checkpoint_trace.clear()
        baseline_advances.clear()
        state = CheckpointState()
        coordinator = checkpoint_coordinator(
            state,
            CheckpointPrepare(
                state,
                expected_policy_authority=checkpoint_carrier,
            ),
        )
        with pytest.raises(worker_module.ProjectCheckpointFailed):
            await coordinator.checkpoint_operation_intent(
                checkpoint_execution,
                checkpoint_authority,
                forged,
                checkpoint_approval,
                base_message_count=9,
                messages=checkpoint_messages,
            )
        if field_name in {"operation_authority", "decision"}:
            assert checkpoint_trace == [], field_name
        else:
            assert [entry[0] for entry in checkpoint_trace] == [
                "state_prepare",
                "projects_prepare",
            ], field_name
        assert coordinator.operation_prepared is False
        assert baseline_advances == []

    # Continue the exact checkpoint published by the actual bridge
    # coordinator into approval -> Core-fenced rehydration -> certified
    # execution.  No checkpoint or operation is manually prepared here.
    durable_case = published_bridge.case
    durable_checkpoint_id = published_bridge.batch_id
    durable_state = SessionDB(
        db_path=durable_case.state_path
    )
    durable_checkpoint_row = durable_state._conn.execute(
        """
        SELECT state, operation_id, approval_id
        FROM project_turn_transcript_batches
        WHERE batch_id = ?
        """,
        (durable_checkpoint_id,),
    ).fetchone()
    assert durable_checkpoint_row is not None
    assert tuple(durable_checkpoint_row) == (
        "published",
        durable_case.authority.intent.operation_id,
        durable_case.approval.approval_id,
    )
    durable_state.close()

    durable_projects = (
        durable_case.projects_db_factory()
    )
    durable_runtime = durable_case.project_runtime_factory(
        durable_projects
    )
    durable_guard = ProjectOperationGuard(durable_runtime)
    durable_approved = (
        durable_guard.resolve_operation_approval(
            durable_case.approval.approval_id,
            durable_case.actor,
            outcome="approved",
        )
    )
    assert durable_approved.status == "approved"
    durable_case.clock[0] = (
        durable_case.claim.lease_expires_at + 1
    )
    durable_dispatcher_lease = (
        durable_runtime.acquire_dispatcher_lease(
            "e23e4567-e89b-42d3-a456-426614174000",
            lease_seconds=30,
        )
    )
    assert durable_dispatcher_lease is not None
    assert (
        durable_guard.rehydrate_approved_operation_for_dispatcher(
            durable_case.project_id,
            durable_case.authority.intent.operation_id,
            worker_id="checkpoint-bypass-must-not-start",
            lease_seconds=90,
            dispatcher_lease=durable_dispatcher_lease,
        )
        is None
    )
    durable_projects.close()

    class NoApprovedOperationReadback:
        def read_operation(self, request):
            raise AssertionError(
                "approved checkpoint recovery needs no effect readback"
            )

    durable_checkpoint_facade = (
        session_module.ProjectApprovalCheckpointReadFacade(
            lambda: SessionDB(db_path=durable_case.state_path),
            io_runner=policy_runner,
        )
    )
    durable_checkpoint_port = (
        worker_module._OwnerLoopCheckpointReadPort(
            durable_checkpoint_facade,
            asyncio.get_running_loop(),
        )
    )

    def recover_durable_checkpoint():
        connection = durable_case.projects_db_factory()
        try:
            runtime = durable_case.project_runtime_factory(connection)
            guard = ProjectOperationGuard(runtime)
            upper = (
                guard.operation_recovery_membership_upper_watermark()
            )
            assert upper is not None
            recovery = guard.recover_pending_operations(
                NoApprovedOperationReadback(),
                durable_checkpoint_port,
                worker_id="durable-rehydrated-worker",
                lease_seconds=90,
                dispatcher_lease=durable_dispatcher_lease,
                max_claims=1,
                after=None,
                through_membership_sequence=upper,
                limit=1,
            )
            assert len(recovery.starts) == 1
            start = recovery.starts[0]
            started_claim = runtime.mark_turn_started(start.claim)
            start = replace(start, claim=started_claim)
            return (
                replace(recovery, starts=(start,)),
                runtime.execution_input_for_claim(start.claim),
            )
        finally:
            connection.close()

    durable_recovery, durable_execution = await policy_runner(
        recover_durable_checkpoint
    )
    assert len(durable_recovery.starts) == 1
    durable_start = durable_recovery.starts[0]
    assert durable_start is not None
    assert durable_start.source == "approved_operation"
    assert durable_start.operation is not None
    assert durable_start.operation.operation_id == (
        durable_case.authority.intent.operation_id
    )
    assert durable_start.operation.status == "approved"
    assert durable_start.operation.approval_id == (
        durable_case.approval.approval_id
    )
    assert durable_start.operation.approval_checkpoint_id == (
        durable_checkpoint_id
    )
    assert (
        durable_case.carrier.execution_attempt.attempt_id
        != durable_start.claim.attempt_id
    )
    assert (
        durable_case.carrier.execution_attempt.worker_id
        != durable_start.claim.worker_id
    )
    assert (
        durable_case.carrier.execution_attempt.lease_generation
        < durable_start.claim.lease_generation
    )
    assert (
        durable_case.carrier.execution_attempt.fencing_token
        < durable_start.claim.fencing_token
    )

    (
        durable_execution_facade,
        durable_execution_connections,
    ) = (
        build_real_execution_facade(durable_case)
    )
    durable_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    durable_execution_coordinator = (
        worker_module.CanonicalProjectOperationExecutionCoordinator(
            execution_facade=durable_execution_facade,
            capability_registry=ExecutionRegistry(
                durable_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    durable_approved_turn = (
        worker_module.CanonicalApprovedOperationTurn(
            durable_execution,
            durable_start.operation,
            base_message_count=2,
            coordinator=durable_execution_coordinator,
        )
    )
    durable_result = await durable_approved_turn.result()
    assert durable_result.status == "succeeded"
    assert len(
        [
            entry
            for entry in durable_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1
    assert_facade_connections_closed(
        durable_execution_connections,
        expected_count=4,
    )
    assert all(
        message["role"] == expected_role
        and "tool_calls" not in message
        for message, expected_role in zip(
            durable_result.messages,
            ("user", "assistant"),
            strict=True,
        )
    )

    # Changed current identity without the stored successor, a stale current
    # horizon, and an execution/WorkerStart mismatch are all rejected before
    # the capability adapter.
    durable_drift_cases = (
        (
            "stale_horizon",
            replace(
                durable_execution,
                attempt=replace(
                    durable_execution.attempt,
                    lease_expires_at=(
                        durable_execution.attempt.lease_expires_at
                        - 1
                    ),
                ),
            ),
            durable_start.operation,
        ),
        (
            "unlinked_generation",
            replace(
                durable_execution,
                attempt=replace(
                    durable_execution.attempt,
                    attempt_id="unlinked-durable-attempt",
                    lease_generation=(
                        durable_execution.attempt.lease_generation
                        + 1
                    ),
                    fencing_token=(
                        durable_execution.attempt.fencing_token
                        + 1
                    ),
                ),
            ),
            replace(
                durable_start.operation,
                attempt_id="unlinked-durable-attempt",
                lease_generation=(
                    durable_execution.attempt.lease_generation
                    + 1
                ),
                fencing_token=(
                    durable_execution.attempt.fencing_token + 1
                ),
            ),
        ),
    )
    for label, drift_execution, drift_operation in (
        durable_drift_cases
    ):
        drift_adapter = EffectAdapter(
            live_receipt,
            live_readback,
        )
        (
            drift_facade,
            durable_drift_connections,
        ) = build_real_execution_facade(durable_case)
        drift_coordinator = (
            worker_module
            .CanonicalProjectOperationExecutionCoordinator(
                execution_facade=drift_facade,
                capability_registry=ExecutionRegistry(
                    drift_adapter
                ),
                effect_runner=effect_runner,
            )
        )
        drift_turn = worker_module.CanonicalApprovedOperationTurn(
            drift_execution,
            drift_operation,
            base_message_count=2,
            coordinator=drift_coordinator,
        )
        with pytest.raises(
            (
                worker_module.ProjectOperationUnresolved,
                PermissionError,
                ValueError,
            )
        ):
            await drift_turn.result()
        assert drift_adapter.trace == [], label
        assert_facade_connections_closed(
            durable_drift_connections,
            expected_count=1,
        )

    # Publish the approved result, restart State, and feed the exact four-row
    # alternating history to the next project-agent turn.
    durable_terminal_batch_id = (
        "f23e4567-e89b-42d3-a456-426614174000"
    )
    durable_state = SessionDB(
        db_path=durable_case.state_path
    )
    durable_state.prepare_terminal_result(
        durable_start.claim,
        batch_id=durable_terminal_batch_id,
        status=durable_result.status,
        base_message_count=2,
        messages=durable_result.messages,
    )
    durable_projects = (
        durable_case.projects_db_factory()
    )
    durable_runtime = durable_case.project_runtime_factory(
        durable_projects
    )
    durable_runtime.commit_turn_with_task7_batch(
        durable_start.claim,
        runtime_module.CanonicalTurnResult(
            durable_result.status,
            durable_terminal_batch_id,
        ),
        transcript_batch_id=durable_terminal_batch_id,
    )
    durable_projects.close()
    durable_state_adapter = session_module.AsyncSessionStore(
        durable_state,
        projects_db_factory=(
            durable_case.projects_db_factory
        ),
    )
    assert (
        await durable_state_adapter.apply_project_batch(
            durable_terminal_batch_id
        )
    ).outcome == "published"
    durable_state.close()
    restarted_state = SessionDB(
        db_path=durable_case.state_path
    )
    restarted_store = session_module.AsyncSessionStore(
        restarted_state,
        projects_db_factory=(
            durable_case.projects_db_factory
        ),
    )
    restarted_history = await (
        restarted_store.load_project_history(
            durable_case.session_id
        )
    )
    assert restarted_history.message_count == 4
    assert tuple(
        message["role"]
        for message in restarted_history.messages
    ) == ("user", "assistant", "user", "assistant")
    assert all(
        "tool_calls" not in message
        for message in restarted_history.messages
    )
    restarted_state.close()

    replay_context = SessionContext(
        source=SessionSource(
            platform=Platform.LOCAL,
            chat_id=f"project:{durable_case.project_id}",
        ),
        connected_platforms=[],
        home_channels={},
        session_key=f"project:{durable_case.project_id}",
        session_id=durable_case.session_id,
    )
    replay_fixture = Fixture(
        (snapshot(),),
        (
            (
                completed_raw(
                    "next replay",
                    prefix=restarted_history.messages,
                    session_id=durable_case.session_id,
                ),
            ),
        ),
    )
    replay_factory = replay_fixture.factory()
    replay_build = await replay_factory.resolve_project_agent(
        context=replay_context,
        contract_revision=7,
    )
    replay_parent = await replay_build.create_project_agent(
        history=restarted_history
    )
    replay_execution = replace(
        durable_execution,
        payload={"message": "next replay"},
    )
    replay_result = await replay_parent.create_turn(
        replay_execution,
        None,
    ).result()
    assert replay_fixture.raw_agents[0].calls[0][1] == (
        restarted_history.messages
    )
    assert replay_result.messages == (
        {"role": "user", "content": '{"message":"next replay"}'},
        {"role": "assistant", "content": "next replay"},
    )
    await replay_factory.release_project_agent(replay_parent)
    replay_fixture.close()

    # Drive one malformed normal-Hermes outcome through the real canonical
    # worker.  It prepares no terminal and is not cached: the next valid start
    # for the same project/session must construct a new parent.
    invalid_worker_claim = runtime_module.TurnClaim(
        "turn-invalid-worker",
        "c14-project",
        21,
        "c14-worker",
        "attempt-invalid-worker",
        1,
        1,
        190,
        "c14-session",
    )
    valid_worker_claim = runtime_module.TurnClaim(
        "turn-valid-worker",
        "c14-project",
        22,
        "c14-worker",
        "attempt-valid-worker",
        1,
        1,
        190,
        "c14-session",
    )
    invalid_worker_execution = runtime_module.TurnExecutionInput(
        runtime_module.TurnAttemptIdentity(
            invalid_worker_claim.project_id,
            invalid_worker_claim.turn_id,
            invalid_worker_claim.sequence,
            invalid_worker_claim.worker_id,
            invalid_worker_claim.attempt_id,
            invalid_worker_claim.lease_generation,
            invalid_worker_claim.fencing_token,
            invalid_worker_claim.canonical_session_id,
            invalid_worker_claim.lease_expires_at,
        ),
        {"message": "invalid worker"},
        runtime_module.TurnOrigin(
            "desktop-binding",
            "desktop",
            "desktop-window",
            "owner-1",
        ),
        7,
    )
    valid_worker_execution = runtime_module.TurnExecutionInput(
        runtime_module.TurnAttemptIdentity(
            valid_worker_claim.project_id,
            valid_worker_claim.turn_id,
            valid_worker_claim.sequence,
            valid_worker_claim.worker_id,
            valid_worker_claim.attempt_id,
            valid_worker_claim.lease_generation,
            valid_worker_claim.fencing_token,
            valid_worker_claim.canonical_session_id,
            valid_worker_claim.lease_expires_at,
        ),
        {"message": "valid worker"},
        runtime_module.TurnOrigin(
            "desktop-binding",
            "desktop",
            "desktop-window",
            "owner-1",
        ),
        7,
    )
    invalid_worker_fixture = Fixture(
        (snapshot(), snapshot()),
        (
            (
                completed_raw(
                    "contradictory",
                    completed=True,
                    failed=True,
                ),
            ),
            (
                completed_raw(
                    "valid after malformed",
                ),
            ),
        ),
    )
    invalid_worker_factory = invalid_worker_fixture.factory()
    invalid_worker_trace = []

    class NormalizerWorkerRuntime:
        def __init__(self):
            self.executions = {
                invalid_worker_claim.turn_id: (
                    invalid_worker_execution
                ),
                valid_worker_claim.turn_id: (
                    valid_worker_execution
                ),
            }

        async def mark_turn_started(self, claim):
            invalid_worker_trace.append(
                ("runtime_start", claim.turn_id)
            )
            return claim

        async def execution_input_for_claim(self, claim):
            return self.executions[claim.turn_id]

        async def heartbeat_turn(self, claim, *, lease_seconds):
            return claim

        async def control_for_claim(self, claim):
            return runtime_module.ClaimControl(
                "running",
                1,
                claim.lease_expires_at,
            )

        async def commit_turn_with_task7_batch(
            self,
            claim,
            result,
            *,
            transcript_batch_id,
        ):
            invalid_worker_trace.append(
                ("commit", claim.turn_id, transcript_batch_id)
            )
            return object()

        async def acknowledge_stopped(self, claim):
            raise AssertionError(
                "normalizer case cannot acknowledge stop"
            )

    class NormalizerWorkerBatches:
        def __init__(self):
            self.prepared = []

        async def load_project_history(self, session_id):
            return history

        async def prepare_terminal_result(
            self,
            claim,
            *,
            batch_id,
            status,
            base_message_count,
            messages,
        ):
            self.prepared.append(
                (
                    claim.turn_id,
                    batch_id,
                    status,
                    tuple(messages),
                )
            )
            return PendingProjectBatch(
                batch_id,
                1,
                "terminal_result",
                "prepared",
                (
                    invalid_worker_execution.attempt
                    if claim is invalid_worker_claim
                    else valid_worker_execution.attempt
                ),
                status,
                None,
                None,
                base_message_count,
                100.0,
            )

        async def prepare_approval_checkpoint(self, *args, **kwargs):
            raise AssertionError(
                "normalizer case cannot checkpoint"
            )

        async def apply_project_batch(self, batch_id):
            return ProjectBatchApplyResult("published")

    class UnusedApprovedExecutionPort:
        def create_turn(self, *args, **kwargs):
            raise AssertionError(
                "queued normalizer case cannot use approved port"
            )

    normalizer_batches = NormalizerWorkerBatches()
    normalizer_worker = (
        worker_module.CanonicalProjectRuntimeWorker(
            NormalizerWorkerRuntime(),
            normalizer_batches,
            invalid_worker_factory,
            GatewayConfig(),
            profile_home=tmp_path / "normalizer-worker",
            lease_seconds=90,
            heartbeat_interval_seconds=30,
            batch_id_factory=lambda: (
                "123e4567-e89b-42d3-a456-426614174111"
            ),
            approved_operations=UnusedApprovedExecutionPort(),
        )
    )
    dispatcher_lease = runtime_module.DispatcherLease(
        "223e4567-e89b-42d3-a456-426614174111",
        1,
        1,
        300,
    )
    with pytest.raises((TypeError, ValueError, RuntimeError)):
        await normalizer_worker.run_start(
            runtime_module.WorkerStart(
                "queued_turn",
                invalid_worker_claim,
                None,
                dispatcher_lease,
            )
        )
    assert normalizer_batches.prepared == []
    assert len(invalid_worker_fixture.builder_calls) == 1
    assert invalid_worker_fixture.raw_agents[0].close_count == 1
    await normalizer_worker.run_start(
        runtime_module.WorkerStart(
            "queued_turn",
            valid_worker_claim,
            None,
            dispatcher_lease,
        )
    )
    assert len(invalid_worker_fixture.builder_calls) == 2
    assert len(invalid_worker_fixture.raw_agents) == 2
    assert len(normalizer_batches.prepared) == 1
    assert normalizer_batches.prepared[0][0] == (
        valid_worker_claim.turn_id
    )
    assert normalizer_batches.prepared[0][2] == "succeeded"
    assert invalid_worker_trace == [
        ("runtime_start", invalid_worker_claim.turn_id),
        ("runtime_start", valid_worker_claim.turn_id),
        (
            "commit",
            valid_worker_claim.turn_id,
            "123e4567-e89b-42d3-a456-426614174111",
        ),
    ]
    await normalizer_worker.close()
    assert invalid_worker_fixture.raw_agents[1].close_count == 1
    invalid_worker_fixture.close()

    # Approved-operation turns execute the real certified coordinator and no
    # agent factory.  Exact rehydrated authority may differ from the immutable
    # checkpoint attempt only through the certified request.
    rehydrated_execution = execution(
        "approved",
        horizon=390,
        attempt_id="rehydrated-attempt",
        worker_id="rehydrated-worker",
        generation=8,
        fence=13,
        sequence=12,
    )
    original_execution = execution(
        "approved",
        horizon=190,
        attempt_id="original-attempt",
        worker_id="original-worker",
        generation=3,
        fence=5,
        sequence=12,
    )
    approved_authority, approved_carrier, approved_spec = (
        authority_fixture(
            original_execution,
            action="publish",
            action_class="publish",
            decision=Decision.REQUIRE_APPROVAL,
            approval_id=(
                "823e4567-e89b-42d3-a456-426614174000"
            ),
        )
    )
    approved_facade = ExecutionFacade(
        authority=approved_authority,
        execution_value=rehydrated_execution,
    )
    approved_facade.prepared = operation_value(
        approved_authority,
        status="approved",
        attempt_value=rehydrated_execution.attempt,
        approval_id=approved_spec.approval_id,
        checkpoint_id=checkpoint_batch_id,
    )
    approved_facade.request = certified_request(
        approved_facade.prepared,
        approved_authority,
        rehydrated_execution.attempt,
        checkpoint_id=checkpoint_batch_id,
    )
    approved_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    approved_coordinator = (
        worker_module.CanonicalProjectOperationExecutionCoordinator(
            execution_facade=approved_facade,
            capability_registry=ExecutionRegistry(
                approved_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    approved_turn = worker_module.CanonicalApprovedOperationTurn(
        rehydrated_execution,
        approved_facade.prepared,
        base_message_count=9,
        coordinator=approved_coordinator,
    )
    approved_one, approved_two = await asyncio.gather(
        approved_turn.result(),
        approved_turn.result(),
    )
    assert approved_one is approved_two
    assert approved_one == worker_module.ProjectAgentRunResult(
        "succeeded",
        9,
        (
            {
                "role": "user",
                "content": (
                    '{"kind":"project_operation_resume",'
                    '"operation_id":"operation-original-attempt"}'
                ),
            },
            {
                "role": "assistant",
                "content": (
                    '{"kind":"project_operation_result",'
                    '"operation_id":"operation-original-attempt",'
                    '"status":"reconciled"}'
                ),
            },
        ),
    )
    assert [entry[0] for entry in approved_facade.trace] == [
        "certified_execution_request",
        "mark_started",
        "record_receipt",
        "reconcile",
    ]
    assert len(
        [
            entry
            for entry in approved_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1
    assert rehydrated_execution.attempt != (
        approved_carrier.execution_attempt
    )
    assert (
        rehydrated_execution.attempt.lease_generation
        > approved_carrier.execution_attempt.lease_generation
    )
    assert (
        rehydrated_execution.attempt.fencing_token
        > approved_carrier.execution_attempt.fencing_token
    )

    heartbeat_execution = replace(
        rehydrated_execution,
        attempt=replace(
            rehydrated_execution.attempt,
            lease_expires_at=420,
        ),
    )
    heartbeat_facade = ExecutionFacade(
        authority=approved_authority,
        execution_value=heartbeat_execution,
    )
    heartbeat_facade.prepared = operation_value(
        approved_authority,
        status="approved",
        attempt_value=rehydrated_execution.attempt,
        approval_id=approved_spec.approval_id,
        checkpoint_id=checkpoint_batch_id,
    )
    heartbeat_facade.request = certified_request(
        heartbeat_facade.prepared,
        approved_authority,
        heartbeat_execution.attempt,
        checkpoint_id=checkpoint_batch_id,
    )
    heartbeat_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    heartbeat_coordinator = (
        worker_module.CanonicalProjectOperationExecutionCoordinator(
            execution_facade=heartbeat_facade,
            capability_registry=ExecutionRegistry(
                heartbeat_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    heartbeat_result = await (
        worker_module.CanonicalApprovedOperationTurn(
            heartbeat_execution,
            heartbeat_facade.prepared,
            base_message_count=9,
            coordinator=heartbeat_coordinator,
        ).result()
    )
    assert heartbeat_result.status == "succeeded"
    assert (
        heartbeat_facade.prepared.attempt_id
        == rehydrated_execution.attempt.attempt_id
    )
    assert (
        heartbeat_facade.prepared.lease_generation
        == rehydrated_execution.attempt.lease_generation
    )
    assert (
        heartbeat_facade.prepared.fencing_token
        == rehydrated_execution.attempt.fencing_token
    )
    assert (
        heartbeat_facade.request.attempt.lease_expires_at
        == 420
    )
    assert len(
        [
            entry
            for entry in heartbeat_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1

    # Every request/current-attempt mismatch is rejected by the real
    # approved coordinator before mark/effect.  These are concrete identity
    # mutations, not labels interpreted by the test facade.
    approved_attempt_drifts = (
        (
            "stale_current_horizon",
            heartbeat_execution,
            replace(
                heartbeat_execution.attempt,
                lease_expires_at=389,
            ),
        ),
        (
            "same_epoch_identity",
            rehydrated_execution,
            replace(
                rehydrated_execution.attempt,
                worker_id="changed-same-epoch-worker",
            ),
        ),
        (
            "worker_start_mismatch",
            replace(
                rehydrated_execution,
                attempt=replace(
                    rehydrated_execution.attempt,
                    lease_expires_at=410,
                ),
            ),
            rehydrated_execution.attempt,
        ),
        (
            "unlinked_attempt_generation",
            rehydrated_execution,
            replace(
                rehydrated_execution.attempt,
                attempt_id="unlinked-attempt",
                lease_generation=(
                    rehydrated_execution.attempt.lease_generation
                    + 1
                ),
            ),
        ),
        (
            "unlinked_fence",
            rehydrated_execution,
            replace(
                rehydrated_execution.attempt,
                fencing_token=(
                    rehydrated_execution.attempt.fencing_token
                    + 1
                ),
            ),
        ),
    )
    for label, turn_execution, request_attempt in (
        approved_attempt_drifts
    ):
        drift_facade = ExecutionFacade(
            authority=approved_authority,
            execution_value=turn_execution,
        )
        drift_facade.prepared = operation_value(
            approved_authority,
            status="approved",
            attempt_value=rehydrated_execution.attempt,
            approval_id=approved_spec.approval_id,
            checkpoint_id=checkpoint_batch_id,
        )
        drift_facade.request = certified_request(
            drift_facade.prepared,
            approved_authority,
            request_attempt,
            checkpoint_id=checkpoint_batch_id,
        )
        drift_adapter = EffectAdapter(
            live_receipt,
            live_readback,
        )
        drift_coordinator = (
            worker_module.CanonicalProjectOperationExecutionCoordinator(
                execution_facade=drift_facade,
                capability_registry=ExecutionRegistry(
                    drift_adapter
                ),
                effect_runner=effect_runner,
            )
        )
        drift_turn = worker_module.CanonicalApprovedOperationTurn(
            turn_execution,
            drift_facade.prepared,
            base_message_count=9,
            coordinator=drift_coordinator,
        )
        with pytest.raises(
            (
                worker_module.ProjectOperationUnresolved,
                TypeError,
                ValueError,
                PermissionError,
            )
        ):
            await drift_turn.result()
        assert drift_adapter.trace == [], label
        assert not any(
            entry[0] == "mark_started"
            for entry in drift_facade.trace
        ), label

    approved_failure_cases = (
        ("mark_started", "reconciled"),
        ("effect_exception", "unknown"),
        ("effect_none", "unknown"),
        ("malformed_receipt", "reconciled"),
        ("subclass_receipt", "reconciled"),
        ("malformed_readback", "blocked"),
        ("unknown", "unknown"),
        ("effect_started", "effect_started"),
        ("receipt_recorded", "receipt_recorded"),
        ("approved", "approved"),
        ("blocked", "blocked"),
    )
    for label, final_status in approved_failure_cases:
        case_facade = ExecutionFacade(
            authority=approved_authority,
            execution_value=rehydrated_execution,
            final_status=final_status,
        )
        case_facade.prepared = operation_value(
            approved_authority,
            status="approved",
            attempt_value=rehydrated_execution.attempt,
            approval_id=approved_spec.approval_id,
            checkpoint_id=checkpoint_batch_id,
        )
        case_facade.request = certified_request(
            case_facade.prepared,
            approved_authority,
            rehydrated_execution.attempt,
            checkpoint_id=checkpoint_batch_id,
        )
        if label == "mark_started":
            case_facade.fail_at = "mark_started"
        effect_value = live_receipt
        if label == "effect_exception":
            effect_value = RuntimeError("effect failed")
        elif label == "effect_none":
            effect_value = None
        elif label == "malformed_receipt":
            effect_value = {"receipt_id": "fake"}
        elif label == "subclass_receipt":
            effect_value = ReceiptSubclass(
                "subclass-receipt",
                {"ok": True},
            )
        readback_value = (
            object()
            if label == "malformed_readback"
            else live_readback
        )
        case_adapter = EffectAdapter(
            effect_value,
            readback_value,
        )
        case_coordinator = (
            worker_module.CanonicalProjectOperationExecutionCoordinator(
                execution_facade=case_facade,
                capability_registry=ExecutionRegistry(
                    case_adapter
                ),
                effect_runner=effect_runner,
            )
        )
        case_turn = worker_module.CanonicalApprovedOperationTurn(
            rehydrated_execution,
            case_facade.prepared,
            base_message_count=9,
            coordinator=case_coordinator,
        )
        with pytest.raises(
            worker_module.ProjectOperationUnresolved
        ):
            await case_turn.result()
        assert len(
            [
                entry
                for entry in case_adapter.trace
                if entry[0] == "effect"
            ]
        ) == (0 if label == "mark_started" else 1)
        assert len(
            [
                entry
                for entry in case_facade.trace
                if entry[0] == "reconcile"
            ]
        ) == (0 if label == "mark_started" else 1)
        with pytest.raises(
            worker_module.ProjectOperationUnresolved
        ):
            await case_turn.result()
        assert len(
            [
                entry
                for entry in case_adapter.trace
                if entry[0] == "effect"
            ]
        ) == (0 if label == "mark_started" else 1)

    # Cancellation during an already-started effect is cooperative: result()
    # waits for the exact effect and reconcile, then cancellation wins over a
    # late reconciled DTO.  No repeat can invoke the effect again.
    cancel_facade = ExecutionFacade(
        authority=approved_authority,
        execution_value=rehydrated_execution,
    )
    cancel_facade.prepared = operation_value(
        approved_authority,
        status="approved",
        attempt_value=rehydrated_execution.attempt,
        approval_id=approved_spec.approval_id,
        checkpoint_id=checkpoint_batch_id,
    )
    cancel_facade.request = certified_request(
        cancel_facade.prepared,
        approved_authority,
        rehydrated_execution.attempt,
        checkpoint_id=checkpoint_batch_id,
    )
    cancel_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    cancel_adapter.gated = True
    cancel_coordinator = (
        worker_module.CanonicalProjectOperationExecutionCoordinator(
            execution_facade=cancel_facade,
            capability_registry=ExecutionRegistry(
                cancel_adapter
            ),
            effect_runner=effect_runner,
        )
    )
    cancel_turn = worker_module.CanonicalApprovedOperationTurn(
        rehydrated_execution,
        cancel_facade.prepared,
        base_message_count=9,
        coordinator=cancel_coordinator,
    )
    cancel_task = asyncio.create_task(cancel_turn.result())
    await asyncio.wait_for(
        asyncio.to_thread(cancel_adapter.entered.wait),
        timeout=5,
    )
    assert cancel_turn.request_cancel() is True
    assert cancel_turn.request_cancel() is False
    quiescent_task = asyncio.create_task(
        cancel_turn.wait_quiescent()
    )
    assert not quiescent_task.done()
    cancel_adapter.release.set()
    with pytest.raises(asyncio.CancelledError):
        await cancel_task
    await quiescent_task
    assert len(
        [
            entry
            for entry in cancel_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1
    assert len(
        [
            entry
            for entry in cancel_facade.trace
            if entry[0] == "reconcile"
        ]
    ) == 1

    # The worker receives only an ApprovedOperationExecutionPort.  Its exact
    # approved WorkerStart uses a fresh real coordinator/turn and bypasses
    # resolution, construction, cache ownership, and the model path entirely.
    worker_facade = ExecutionFacade(
        authority=approved_authority,
        execution_value=rehydrated_execution,
    )
    worker_facade.prepared = operation_value(
        approved_authority,
        status="approved",
        attempt_value=rehydrated_execution.attempt,
        approval_id=approved_spec.approval_id,
        checkpoint_id=checkpoint_batch_id,
    )
    worker_facade.request = certified_request(
        worker_facade.prepared,
        approved_authority,
        rehydrated_execution.attempt,
        checkpoint_id=checkpoint_batch_id,
    )
    worker_adapter = EffectAdapter(
        live_receipt,
        live_readback,
    )
    worker_coordinator = (
        worker_module.CanonicalProjectOperationExecutionCoordinator(
            execution_facade=worker_facade,
            capability_registry=ExecutionRegistry(
                worker_adapter
            ),
            effect_runner=effect_runner,
        )
    )

    class BoundApprovedPort:
        def __init__(self, coordinator_value):
            self.coordinator = coordinator_value
            self.calls = []

        def create_turn(
            self,
            execution_value,
            operation,
            *,
            base_message_count,
        ):
            self.calls.append(
                (execution_value, operation, base_message_count)
            )
            return worker_module.CanonicalApprovedOperationTurn(
                execution_value,
                operation,
                base_message_count=base_message_count,
                coordinator=self.coordinator,
            )

    approved_port: worker_module.ApprovedOperationExecutionPort = (
        BoundApprovedPort(worker_coordinator)
    )
    assert callable(approved_port.create_turn)
    assert not callable(
        getattr(approved_port, "execute_effect", None)
    )
    assert not callable(
        getattr(approved_port, "resolve_adapter", None)
    )

    worker_claim = runtime_module.TurnClaim(
        rehydrated_execution.attempt.turn_id,
        rehydrated_execution.attempt.project_id,
        rehydrated_execution.attempt.sequence,
        rehydrated_execution.attempt.worker_id,
        rehydrated_execution.attempt.attempt_id,
        rehydrated_execution.attempt.lease_generation,
        rehydrated_execution.attempt.fencing_token,
        rehydrated_execution.attempt.lease_expires_at,
        rehydrated_execution.attempt.canonical_session_id,
    )
    approved_start = runtime_module.WorkerStart(
        "approved_operation",
        worker_claim,
        worker_facade.prepared,
        runtime_module.DispatcherLease(
            "923e4567-e89b-42d3-a456-426614174000",
            9,
            17,
            400,
        ),
    )
    worker_trace = []

    class ApprovedRuntime:
        def __init__(self):
            self.commit_results = []

        async def mark_turn_started(self, claim):
            worker_trace.append(("runtime_start", claim))
            assert claim is worker_claim
            return claim

        async def execution_input_for_claim(self, claim):
            worker_trace.append(("runtime_input", claim))
            assert claim is worker_claim
            return rehydrated_execution

        async def heartbeat_turn(self, claim, *, lease_seconds):
            raise AssertionError(
                "approved result must finish before a heartbeat"
            )

        async def control_for_claim(self, claim):
            worker_trace.append(("runtime_control", claim))
            assert claim == worker_claim
            return runtime_module.ClaimControl(
                "running",
                4,
                claim.lease_expires_at,
            )

        async def commit_turn_with_task7_batch(
            self,
            claim,
            result,
            *,
            transcript_batch_id,
        ):
            worker_trace.append(
                (
                    "runtime_commit",
                    claim,
                    result,
                    transcript_batch_id,
                )
            )
            self.commit_results.append(
                (claim, result, transcript_batch_id)
            )
            return object()

        async def acknowledge_stopped(self, claim):
            raise AssertionError(
                "approved happy path must not acknowledge stop"
            )

    class ApprovedBatches:
        def __init__(self):
            self.prepared = []
            self.applied = []

        async def load_project_history(self, session_id):
            worker_trace.append(("history", session_id))
            assert session_id == "c14-session"
            return history

        async def prepare_terminal_result(
            self,
            claim,
            *,
            batch_id,
            status,
            base_message_count,
            messages,
        ):
            worker_trace.append(
                (
                    "terminal_prepare",
                    claim,
                    batch_id,
                    status,
                    base_message_count,
                    tuple(messages),
                )
            )
            self.prepared.append(
                (
                    claim,
                    batch_id,
                    status,
                    base_message_count,
                    tuple(messages),
                )
            )
            return PendingProjectBatch(
                batch_id,
                1,
                "terminal_result",
                "prepared",
                rehydrated_execution.attempt,
                status,
                None,
                None,
                base_message_count,
                100.0,
            )

        async def prepare_approval_checkpoint(self, *args, **kwargs):
            raise AssertionError(
                "approved resume must not prepare another checkpoint"
            )

        async def apply_project_batch(self, batch_id):
            worker_trace.append(("terminal_apply", batch_id))
            self.applied.append(batch_id)
            return ProjectBatchApplyResult("published")

    class PoisonAgentFactory:
        def __init__(self):
            self.resolve_calls = []
            self.release_calls = []

        async def resolve_project_agent(self, **kwargs):
            self.resolve_calls.append(kwargs)
            raise AssertionError(
                "approved start must not resolve an agent"
            )

        async def release_project_agent(self, agent):
            self.release_calls.append(agent)
            raise AssertionError(
                "approved start must not own a cached agent"
            )

    approved_runtime = ApprovedRuntime()
    approved_batches = ApprovedBatches()
    poison_agents = PoisonAgentFactory()
    worker_batch_id = (
        "a23e4567-e89b-42d3-a456-426614174000"
    )
    approved_worker = worker_module.CanonicalProjectRuntimeWorker(
        approved_runtime,
        approved_batches,
        poison_agents,
        GatewayConfig(),
        profile_home=tmp_path / "approved-profile",
        lease_seconds=90,
        heartbeat_interval_seconds=30,
        batch_id_factory=lambda: worker_batch_id,
        approved_operations=approved_port,
    )
    await approved_worker.run_start(approved_start)

    expected_approved_messages = (
        {
            "role": "user",
            "content": (
                '{"kind":"project_operation_resume",'
                '"operation_id":"operation-original-attempt"}'
            ),
        },
        {
            "role": "assistant",
            "content": (
                '{"kind":"project_operation_result",'
                '"operation_id":"operation-original-attempt",'
                '"status":"reconciled"}'
            ),
        },
    )
    assert approved_port.calls == [
        (
            rehydrated_execution,
            worker_facade.prepared,
            9,
        )
    ]
    assert poison_agents.resolve_calls == []
    assert poison_agents.release_calls == []
    assert [entry[0] for entry in worker_facade.trace] == [
        "certified_execution_request",
        "mark_started",
        "record_receipt",
        "reconcile",
    ]
    assert [entry[0] for entry in worker_adapter.trace] == [
        "effect",
        "readback",
    ]
    assert approved_batches.prepared == [
        (
            worker_claim,
            worker_batch_id,
            "succeeded",
            9,
            expected_approved_messages,
        )
    ]
    assert approved_batches.applied == [worker_batch_id]
    assert approved_runtime.commit_results == [
        (
            worker_claim,
            runtime_module.CanonicalTurnResult(
                "succeeded",
                worker_batch_id,
            ),
            worker_batch_id,
        )
    ]
    assert [entry[0] for entry in worker_trace] == [
        "runtime_start",
        "runtime_input",
        "history",
        "terminal_prepare",
        "runtime_control",
        "runtime_commit",
        "terminal_apply",
    ]
    assert len(
        [
            entry
            for entry in worker_adapter.trace
            if entry[0] == "effect"
        ]
    ) == 1

    class MismatchedApprovedRuntime(ApprovedRuntime):
        async def execution_input_for_claim(self, claim):
            assert claim is worker_claim
            return replace(
                rehydrated_execution,
                attempt=replace(
                    rehydrated_execution.attempt,
                    lease_expires_at=(
                        rehydrated_execution.attempt.lease_expires_at
                        - 1
                    ),
                ),
            )

    class PoisonApprovedPort:
        def __init__(self):
            self.calls = []

        def create_turn(
            self,
            execution_value,
            operation,
            *,
            base_message_count,
        ):
            self.calls.append(
                (
                    execution_value,
                    operation,
                    base_message_count,
                )
            )
            raise AssertionError(
                "WorkerStart mismatch must fail before approved port"
            )

    mismatch_runtime = MismatchedApprovedRuntime()
    mismatch_batches = ApprovedBatches()
    mismatch_agents = PoisonAgentFactory()
    mismatch_port: worker_module.ApprovedOperationExecutionPort = (
        PoisonApprovedPort()
    )
    mismatch_worker = worker_module.CanonicalProjectRuntimeWorker(
        mismatch_runtime,
        mismatch_batches,
        mismatch_agents,
        GatewayConfig(),
        profile_home=tmp_path / "approved-mismatch-profile",
        lease_seconds=90,
        heartbeat_interval_seconds=30,
        batch_id_factory=lambda: (
            "b23e4567-e89b-42d3-a456-426614174000"
        ),
        approved_operations=mismatch_port,
    )
    with pytest.raises(ValueError):
        await mismatch_worker.run_start(approved_start)
    assert mismatch_port.calls == []
    assert mismatch_agents.resolve_calls == []
    assert mismatch_agents.release_calls == []
    assert mismatch_batches.prepared == []
    assert mismatch_batches.applied == []

    await approved_worker.close()
    await mismatch_worker.close()
    assert live_agent_dependency_calls == []
    policy_runner.close()
    effect_runner.close()


def test_task7_c14_frozen_agent_snapshot_accepts_immutable_constructor_mapping():
    """The bridge must accept the deeply immutable C14 construction snapshot."""
    from types import MappingProxyType

    import gateway.project_runtime_worker as worker_module

    revisions = worker_module.ProjectAgentRevisions(
        "base-signature",
        "tools:file@7",
        "model:openai/frozen",
    )
    snapshot = SimpleNamespace(
        constructor_kwargs=MappingProxyType({"model": "frozen-model"}),
        registry_generation=7,
        declared_registry_generation=7,
        base_signature=revisions.base_signature,
        declared_base_signature=revisions.base_signature,
        tool_revision=revisions.tool_revision,
        declared_tool_revision=revisions.tool_revision,
        model_revision=revisions.model_revision,
        declared_model_revision=revisions.model_revision,
        revisions=revisions,
        runtime_kind="hermes",
        tool_descriptors=("project_status",),
    )

    assert (
        worker_module.GatewayProjectAgentFactory._validate_snapshot(snapshot)
        == revisions
    )


@pytest.mark.asyncio
async def test_task7_c14_agent_factory_rejects_and_closes_a_builder_gate_mismatch():
    """A raw agent cannot substitute the factory's turn-local execution gate."""
    import gateway.project_runtime_worker as worker_module
    from gateway.session import (
        ProjectHistorySnapshot,
        SessionContext,
        SessionSource,
    )
    from gateway.config import Platform

    revisions = worker_module.ProjectAgentRevisions(
        "base-signature",
        "tools:file@7",
        "model:openai/frozen",
    )
    snapshot = SimpleNamespace(
        constructor_kwargs={"model": "frozen-model"},
        registry_generation=7,
        declared_registry_generation=7,
        base_signature=revisions.base_signature,
        declared_base_signature=revisions.base_signature,
        tool_revision=revisions.tool_revision,
        declared_tool_revision=revisions.tool_revision,
        model_revision=revisions.model_revision,
        declared_model_revision=revisions.model_revision,
        revisions=revisions,
        runtime_kind="hermes",
        tool_descriptors=("project_status",),
    )

    class WrongGateAgent:
        def __init__(self):
            self.project_execution_gate = object()
            self.close_calls = 0

        def run_conversation(self, **kwargs):
            raise AssertionError("mismatched gate agent must not run")

        def interrupt(self):
            raise AssertionError("mismatched gate agent must not interrupt")

        def close(self):
            self.close_calls += 1

    raw = WrongGateAgent()

    async def off_loop_runner(callback, *args, **kwargs):
        return callback(*args, **kwargs)

    factory = worker_module.GatewayProjectAgentFactory(
        snapshot_resolver=lambda context, revision: snapshot,
        agent_builder=lambda built_snapshot, **kwargs: raw,
        off_loop_runner=off_loop_runner,
        turn_context_binder=lambda execution: None,
        tool_authorizer=object(),
        checkpoint_coordinator=object(),
    )
    build = await factory.resolve_project_agent(
        context=SessionContext(
            source=SessionSource(
                platform=Platform.LOCAL,
                chat_id="project:gate-check",
            ),
            connected_platforms=[],
            home_channels={},
            session_key="project:gate-check",
            session_id="gate-check-session",
        ),
        contract_revision=7,
    )

    with pytest.raises(ValueError, match="execution gate"):
        await build.create_project_agent(
            history=ProjectHistorySnapshot(
                "gate-check-session",
                (),
                0,
            )
        )
    assert raw.close_calls == 1


@pytest.mark.asyncio
async def test_task7_c14_bridge_rejects_mismatched_owner_loop_and_closes_on_schedule_failure(
    monkeypatch,
):
    """The tool bridge only schedules on its exact live owner loop."""
    import inspect

    from agent import tool_executor

    class CloseableAwaitable:
        def __init__(self):
            self.close_calls = 0

        def __await__(self):
            async def wait_forever():
                await asyncio.Future()

            return wait_forever().__await__()

        def close(self):
            self.close_calls += 1

    live_loop = asyncio.get_running_loop()
    mismatched_loop = asyncio.new_event_loop()
    try:
        mismatched_source = CloseableAwaitable()
        with pytest.raises(RuntimeError, match="owner loop"):
            tool_executor._project_bridge_call(
                SimpleNamespace(_project_execution_owner_loop=live_loop),
                SimpleNamespace(owner_loop=mismatched_loop),
                mismatched_source,
            )
        assert mismatched_source.close_calls == 1
    finally:
        mismatched_loop.close()

    scheduled_wrappers = []

    def fail_scheduling(wrapper, loop):
        scheduled_wrappers.append(wrapper)
        raise RuntimeError("scheduler rejected bridge")

    monkeypatch.setattr(
        tool_executor.asyncio,
        "run_coroutine_threadsafe",
        fail_scheduling,
    )
    scheduled_source = CloseableAwaitable()
    with pytest.raises(RuntimeError, match="scheduler rejected bridge"):
        tool_executor._project_bridge_call(
            SimpleNamespace(_project_execution_owner_loop=live_loop),
            SimpleNamespace(owner_loop=live_loop),
            scheduled_source,
        )
    assert scheduled_source.close_calls == 1
    assert len(scheduled_wrappers) == 1
    assert inspect.getcoroutinestate(scheduled_wrappers[0]) == "CORO_CLOSED"
