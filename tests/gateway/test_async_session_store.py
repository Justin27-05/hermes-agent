"""Async SessionStore boundary for gateway event-loop safety."""

import ast
import asyncio
import inspect
import json
import math
import sqlite3
import threading
from dataclasses import FrozenInstanceError, dataclass, fields
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Mapping, Sequence, get_type_hints

import pytest

from gateway.session import AsyncSessionStore


class _SpyStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []
        self.label = "store"

    def read(self, value: str) -> str:
        self.calls.append((value, threading.get_ident()))
        return value


@pytest.mark.asyncio
async def test_async_session_store_offloads_calls() -> None:
    store = _SpyStore()
    facade = AsyncSessionStore(store)  # type: ignore[arg-type]
    loop_thread = threading.get_ident()

    assert await facade.read("ok") == "ok"
    assert store.calls == [("ok", store.calls[0][1])]
    assert store.calls[0][1] != loop_thread
    assert facade.label == "store"
    assert facade._store is store


def _nearest_function(node: ast.AST, parents: dict[ast.AST, ast.AST]):
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current
    return None


def _is_awaited(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, ast.Await):
            return True
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return False
    return False


def test_gateway_async_code_uses_one_awaited_session_store_boundary() -> None:
    """Loop-side store calls must use the facade; raw store remains sync-only."""
    root = Path(__file__).resolve().parents[2]
    violations: list[str] = []
    for rel in ("gateway/run.py", "gateway/slash_commands.py"):
        tree = ast.parse((root / rel).read_text(encoding="utf-8"))
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        for owner in (node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)):
            raw_aliases = {
                target.id
                for node in ast.walk(owner)
                if isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Attribute)
                and isinstance(node.value.value, ast.Name)
                and node.value.value.id in {"self", "_self"}
                and node.value.attr == "session_store"
                for target in node.targets
                if isinstance(target, ast.Name)
            }
            for node in ast.walk(owner):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                if _nearest_function(node, parents) is not owner:
                    # A nested sync helper (for example run_sync) executes off-loop.
                    continue
                receiver = node.func.value
                if isinstance(receiver, ast.Name) and receiver.id in raw_aliases:
                    violations.append(
                        f"{rel}:{node.lineno} raw alias {receiver.id}.{node.func.attr}() in async {owner.name}"
                    )
                    continue
                if not (
                    isinstance(receiver, ast.Attribute)
                    and isinstance(receiver.value, ast.Name)
                    and receiver.value.id in {"self", "_self"}
                ):
                    continue
                if receiver.attr == "session_store":
                    violations.append(
                        f"{rel}:{node.lineno} raw session_store.{node.func.attr}() in async {owner.name}"
                    )
                elif receiver.attr == "async_session_store" and not _is_awaited(
                    node, parents
                ):
                    violations.append(
                        f"{rel}:{node.lineno} unawaited async_session_store.{node.func.attr}()"
                    )
    assert not violations, "\n".join(violations)


def test_every_async_compression_check_is_awaited() -> None:
    root = Path(__file__).resolve().parents[2]
    tree = ast.parse((root / "gateway/run.py").read_text(encoding="utf-8"))
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    violations = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_session_has_compression_in_flight"
            and not _is_awaited(node, parents)
        ):
            violations.append(node.lineno)
    assert not violations, f"compression check must be awaited at lines {violations}"


def test_gateway_initializes_async_session_store_facade() -> None:
    source = (Path(__file__).resolve().parents[2] / "gateway/run.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id == "self"
            and target.attr == "_async_session_store"
            for target in node.targets
        )
    ]
    assert assignments, "GatewayRunner must initialize one AsyncSessionStore facade"


def test_no_repository_local_claude_permissions_file() -> None:
    root = Path(__file__).resolve().parents[2]
    assert not (root / ".claude" / "settings.json").exists()


@pytest.mark.asyncio
async def test_task7_c9_count_drift_cross_db_fault_boundaries_converge_projects_then_state_once(
    tmp_path,
    monkeypatch,
) -> None:
    """All C9 crash windows converge in State -> Projects -> State order.

    Mutations caught: overlapping transactions, an unbound/same-thread
    resolver, incomplete crash recovery, duplicate concurrent records, or any
    transcript/counter/delivery/cache/applied write during remediation.
    """
    import hashlib

    import gateway.session as session_module
    from hermes_cli import project_runtime_db as prdb
    from hermes_cli import projects_db
    from hermes_cli.project_policy import ActorContext
    from hermes_cli.project_runtime import CanonicalTurnResult, ProjectRuntime
    from hermes_state import SessionDB

    state = SessionDB(db_path=tmp_path / "c9-state.db")
    projects_path = tmp_path / "c9-projects.db"
    conn = projects_db.connect(projects_path)
    original_resolver = session_module.ProjectBatchAuthorityResolver
    loop_thread = threading.get_ident()
    resolver_instances = []
    factory_connections = []
    factory_connections_by_thread = {}
    factory_threads = []
    record_calls = []
    record_connection_traces = []
    order = []
    transaction_lock = threading.Lock()
    transaction_owner = {}
    active_transaction_counts = {"state": 0, "projects": 0}
    active_state_transactions = {}
    completed_state_transactions = []
    modes = {}
    concurrency_barriers = {}
    final_fingerprint_originals = {}

    def normalized(statement):
        return " ".join(statement.upper().split())

    def state_trace(statement):
        compact = normalized(statement)
        thread_id = threading.get_ident()
        with transaction_lock:
            owner = transaction_owner.setdefault(
                thread_id, {"state": 0, "projects": 0}
            )
            if compact.startswith("BEGIN"):
                assert owner["projects"] == 0
                assert active_transaction_counts["projects"] == 0
                owner["state"] += 1
                active_transaction_counts["state"] += 1
                active_state_transactions[thread_id] = set()
            elif compact in {"COMMIT", "ROLLBACK"}:
                owner["state"] -= 1
                assert owner["state"] >= 0
                active_transaction_counts["state"] -= 1
                assert active_transaction_counts["state"] >= 0
                completed_state_transactions.append(
                    (
                        compact,
                        frozenset(
                            active_state_transactions.pop(thread_id, ())
                        ),
                    )
                )
        if (
            "UPDATE PROJECT_TURN_TRANSCRIPT_BATCHES" in compact
            and "CONFLICT_PENDING" in compact
        ):
            order.append("state-reserve")
            with transaction_lock:
                active_state_transactions[thread_id].add("state-reserve")
        elif (
            "UPDATE PROJECT_TURN_TRANSCRIPT_BATCHES" in compact
            and "CONFLICTED" in compact
        ):
            order.append("state-finalize")
            with transaction_lock:
                active_state_transactions[thread_id].add("state-finalize")

    state._conn.set_trace_callback(state_trace)

    def connection_is_closed(connection):
        try:
            connection.execute("SELECT 1")
        except sqlite3.ProgrammingError as exc:
            assert "closed" in str(exc).lower()
            return True
        return False

    def projects_factory():
        assert not state._conn.in_transaction
        thread_id = threading.get_ident()
        connection = projects_db.connect(projects_path)
        trace = []

        def project_trace(statement):
            compact = normalized(statement)
            trace.append(statement)
            callback_thread = threading.get_ident()
            with transaction_lock:
                owner = transaction_owner.setdefault(
                    callback_thread, {"state": 0, "projects": 0}
                )
                if compact.startswith("BEGIN"):
                    assert owner["state"] == 0
                    assert active_transaction_counts["state"] == 0
                    owner["projects"] += 1
                    active_transaction_counts["projects"] += 1
                elif compact in {"COMMIT", "ROLLBACK"}:
                    owner["projects"] -= 1
                    assert owner["projects"] >= 0
                    active_transaction_counts["projects"] -= 1
                    assert active_transaction_counts["projects"] >= 0
            if (
                "UPDATE PROJECT_RUNTIME_STATE" in compact
                and "TRANSCRIPT_DISPATCH_BLOCK_KEY" in compact
            ) or (
                "INSERT INTO PROJECT_EVENTS" in compact
                and "TURN.TRANSCRIPT_CONFLICTED" in compact
            ):
                order.append("projects-record")

        connection.set_trace_callback(project_trace)
        if any(mode == "projects-event-fault" for mode in modes.values()):
            connection.executescript(
                """
                CREATE TEMP TRIGGER c9_projects_event_fault
                BEFORE INSERT ON project_events
                WHEN NEW.kind = 'turn.transcript_conflicted'
                BEGIN
                    SELECT RAISE(ABORT, 'injected C9 Projects fault');
                END
                """
            )
        factory_connections.append(connection)
        factory_connections_by_thread.setdefault(thread_id, []).append(
            connection
        )
        factory_threads.append(thread_id)
        record_connection_traces.append(trace)
        return connection

    class InstrumentedResolver:
        def __init__(self, factory):
            self.delegate = original_resolver(factory)
            resolver_instances.append(self)

        def resolve_prepared_terminal(self, *args, **kwargs):
            resolver_thread = threading.get_ident()
            owned_connections = factory_connections_by_thread.get(
                resolver_thread, ()
            )
            before_factory = len(owned_connections)
            try:
                return self.delegate.resolve_prepared_terminal(
                    *args, **kwargs
                )
            finally:
                owned_connections = factory_connections_by_thread[
                    resolver_thread
                ]
                assert len(owned_connections) == before_factory + 1
                connection = owned_connections[before_factory]
                assert connection_is_closed(connection)

        def record_terminal_transcript_conflict(self, conflict):
            batch_id = conflict.terminal.batch_id
            mode = modes.get(batch_id)
            assert not state._conn.in_transaction
            record_thread = threading.get_ident()
            assert record_thread != loop_thread
            record_calls.append(
                (batch_id, conflict, record_thread, mode)
            )
            row = state._conn.execute(
                """
                SELECT state, transcript_conflict_key,
                       observed_message_count, remediated_at
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
            assert tuple(row) == (
                "conflict_pending",
                conflict.conflict_key,
                conflict.observed_message_count,
                None,
            )
            order.append("resolver-record")
            if mode == "after-reserve-fault":
                raise RuntimeError("injected after State reservation")
            barrier = concurrency_barriers.get(batch_id)
            if barrier is not None:
                barrier.wait(timeout=10)
            owned_connections = factory_connections_by_thread.get(
                record_thread, ()
            )
            before_factory = len(owned_connections)
            try:
                result = self.delegate.record_terminal_transcript_conflict(
                    conflict
                )
            finally:
                owned_connections = factory_connections_by_thread[
                    record_thread
                ]
                assert len(owned_connections) == before_factory + 1
                connection = owned_connections[before_factory]
                assert connection_is_closed(connection)
            if mode == "final-fingerprint-drift":
                original_sha = state._conn.execute(
                    """
                    SELECT transcript_sha256
                    FROM project_turn_transcript_batches
                    WHERE batch_id = ?
                    """,
                    (batch_id,),
                ).fetchone()[0]
                external_state = sqlite3.connect(state.db_path)
                try:
                    external_state.execute(
                        """
                        UPDATE project_turn_transcript_batches
                        SET transcript_sha256 = ?
                        WHERE batch_id = ?
                        """,
                        ("e" * 64, batch_id),
                    )
                    external_state.commit()
                finally:
                    external_state.close()
                final_fingerprint_originals[batch_id] = original_sha
            return result

        def ack_terminal_transcript_applied(self, acknowledgement):
            raise AssertionError(
                "count drift may not acknowledge an applied transcript"
            )

    monkeypatch.setattr(
        session_module,
        "ProjectBatchAuthorityResolver",
        InstrumentedResolver,
    )

    def setup_case(label, batch_id):
        project_id = projects_db.create_project(conn, name=f"C9 {label}")
        session_id = f"c9-{label}-session"
        binding_id = f"c9-{label}-owner"
        prdb.create_project_conversation(
            conn,
            project_id=project_id,
            conversation_id=session_id,
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id=binding_id,
            project_id=project_id,
            surface="desktop",
            external_binding_id=f"c9-{label}-window",
            actor_id="owner",
            now=1,
        )
        state.create_session(session_id, source="cli")
        runtime = ProjectRuntime(conn, clock=lambda: 100)
        actor = ActorContext("owner", "desktop", binding_id, True)
        turn = runtime.enqueue_turn(
            project_id,
            {"message": label},
            actor,
            idempotency_key=f"c9-{label}",
            expected_version=0,
        )
        claim = runtime.claim_next_turn(
            project_id, f"c9-{label}-worker", lease_seconds=30
        )
        assert claim is not None
        claim = runtime.mark_turn_started(claim)
        prepared = state.prepare_terminal_result(
            claim,
            batch_id=batch_id,
            status="succeeded",
            base_message_count=0,
            messages=(
                {
                    "role": "user",
                    "content": f"{label} user",
                    "timestamp": 1.0,
                },
                {
                    "role": "assistant",
                    "content": f"{label} result",
                    "timestamp": 2.0,
                },
            ),
        )
        runtime.commit_turn_with_task7_batch(
            claim,
            CanonicalTurnResult("succeeded", batch_id),
            transcript_batch_id=batch_id,
        )
        state.append_message(session_id, "user", f"{label} persisted")
        adapter = AsyncSessionStore(
            state,
            projects_db_factory=projects_factory,
        )
        return (
            project_id,
            session_id,
            turn,
            claim,
            prepared,
            runtime,
            adapter,
        )

    def snapshot(project_id, session_id, turn_id, batch_id):
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
            "runtime": dict(
                conn.execute(
                    "SELECT * FROM project_runtime_state WHERE project_id = ?",
                    (project_id,),
                ).fetchone()
            ),
            "turn": dict(
                conn.execute(
                    "SELECT * FROM project_turns WHERE turn_id = ?",
                    (turn_id,),
                ).fetchone()
            ),
            "control": dict(
                conn.execute(
                    "SELECT * FROM project_run_controls WHERE turn_id = ?",
                    (turn_id,),
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
        }

    def assert_no_transcript_delivery_cache_or_applied(before, after):
        assert after["session"] == before["session"]
        assert after["messages"] == before["messages"]
        assert after["delegations"] == before["delegations"]
        assert after["deliveries"] == before["deliveries"]
        assert (
            after["turn"]["transcript_applied_batch_id"]
            == before["turn"]["transcript_applied_batch_id"]
            is None
        )
        for column in (
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "tool_call_count",
            "message_count",
        ):
            assert after["session"].get(column) == before["session"].get(
                column
            )

    def expected_key(claim, batch_id, observed_count):
        identity = {
            "attempt_id": claim.attempt_id,
            "batch_id": batch_id,
            "canonical_session_id": claim.canonical_session_id,
            "fencing_token": claim.fencing_token,
            "lease_expires_at": claim.lease_expires_at,
            "lease_generation": claim.lease_generation,
            "observed_message_count": observed_count,
            "project_id": claim.project_id,
            "result_id": batch_id,
            "sequence": claim.sequence,
            "status": "succeeded",
            "turn_id": claim.turn_id,
            "worker_id": claim.worker_id,
        }
        canonical = json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return "transcript-conflict-" + hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest()

    try:
        # Exact public resolver surface is construction-bound and offloaded.
        assert hasattr(
            original_resolver,
            "record_terminal_transcript_conflict",
        )
        assert tuple(
            (
                name,
                parameter.kind,
            )
            for name, parameter in inspect.signature(
                original_resolver.record_terminal_transcript_conflict
            ).parameters.items()
        ) == (
            ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ("conflict", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        )
        from hermes_cli.project_runtime import TerminalTranscriptConflict

        assert get_type_hints(
            original_resolver.record_terminal_transcript_conflict
        ) == {
            "conflict": TerminalTranscriptConflict,
            "return": Literal["recorded", "already_recorded"],
        }

        (
            project_id,
            session_id,
            turn,
            claim,
            prepared,
            _,
            adapter,
        ) = setup_case(
            "success",
            "123e4567-e89b-42d3-a456-426614174029",
        )
        before = snapshot(
            project_id, session_id, turn.turn_id, prepared.batch_id
        )
        order.clear()
        result = await adapter.apply_project_batch(prepared.batch_id)
        # Intentional current RED: C8 returns state_conflict for real drift.
        assert result == session_module.ProjectBatchApplyResult(
            outcome="conflicted"
        )
        after = snapshot(
            project_id, session_id, turn.turn_id, prepared.batch_id
        )
        assert_no_transcript_delivery_cache_or_applied(before, after)
        assert after["batch"]["state"] == "conflicted"
        assert after["batch"]["observed_message_count"] == 1
        assert after["batch"]["transcript_conflict_key"] == expected_key(
            claim, prepared.batch_id, 1
        )
        assert after["batch"]["published_at"] is None
        assert after["batch"]["projects_acknowledged_at"] is None
        assert after["runtime"]["transcript_pending_batch_id"] is None
        assert after["runtime"]["transcript_dispatch_block_key"] == (
            after["batch"]["transcript_conflict_key"]
        )
        assert after["runtime"]["version"] == before["runtime"]["version"] + 1
        assert len(after["events"]) == len(before["events"]) + 1
        assert after["control"] == before["control"]
        assert after["leases"] == before["leases"] == ()
        assert order.index("state-reserve") < order.index(
            "resolver-record"
        )
        assert order.index("resolver-record") < order.index(
            "projects-record"
        )
        assert order.index("projects-record") < order.index(
            "state-finalize"
        )
        assert transaction_owner
        assert all(
            owner == {"state": 0, "projects": 0}
            for owner in transaction_owner.values()
        )
        assert active_transaction_counts == {"state": 0, "projects": 0}
        assert len(resolver_instances) == 1
        assert len(record_calls) == 1
        assert record_calls[0][2] != loop_thread
        assert all(thread != loop_thread for thread in factory_threads)

        replay_before = snapshot(
            project_id, session_id, turn.turn_id, prepared.batch_id
        )
        replay_project_factory_count = len(factory_connections)
        replay = await adapter.apply_project_batch(prepared.batch_id)
        assert replay == session_module.ProjectBatchApplyResult(
            outcome="already_conflicted"
        )
        assert snapshot(
            project_id, session_id, turn.turn_id, prepared.batch_id
        ) == replay_before
        assert len(factory_connections) == replay_project_factory_count
        assert len(resolver_instances) == 1

        # Fault 1: after reservation, before Projects.
        (
            reserve_project,
            reserve_session,
            reserve_turn,
            _,
            reserve_batch,
            _,
            reserve_adapter,
        ) = setup_case(
            "reserve-fault",
            "223e4567-e89b-42d3-a456-426614174029",
        )
        modes[reserve_batch.batch_id] = "after-reserve-fault"
        reserve_before = snapshot(
            reserve_project,
            reserve_session,
            reserve_turn.turn_id,
            reserve_batch.batch_id,
        )
        reserve_factory_count = len(factory_connections)
        reserve_result = await reserve_adapter.apply_project_batch(
            reserve_batch.batch_id
        )
        assert reserve_result == session_module.ProjectBatchApplyResult(
            outcome="remediation_pending"
        )
        reserve_partial = snapshot(
            reserve_project,
            reserve_session,
            reserve_turn.turn_id,
            reserve_batch.batch_id,
        )
        assert reserve_partial["batch"]["state"] == "conflict_pending"
        assert reserve_partial["runtime"] == reserve_before["runtime"]
        assert reserve_partial["events"] == reserve_before["events"]
        new_connections = factory_connections[reserve_factory_count:]
        assert len(new_connections) in {0, 1}
        assert_no_transcript_delivery_cache_or_applied(
            reserve_before, reserve_partial
        )

        # A durable pending replay classifies a State settlement exception as
        # remediation still pending. It neither revisits Projects nor changes
        # either durable snapshot.
        reserve_exception_before = snapshot(
            reserve_project,
            reserve_session,
            reserve_turn.turn_id,
            reserve_batch.batch_id,
        )
        reserve_state_changes = state._conn.total_changes
        reserve_project_changes = conn.total_changes
        reserve_factory_calls = len(factory_connections)
        reserve_record_calls = len(record_calls)
        pending_publish_calls = []

        def raise_pending_publish(fingerprint):
            assert fingerprint[0] == reserve_batch.batch_id
            pending_publish_calls.append(fingerprint)
            raise RuntimeError("injected pending State replay fault")

        with monkeypatch.context() as pending_publish_fault:
            pending_publish_fault.setattr(
                state,
                "_publish_project_batch",
                raise_pending_publish,
            )
            assert await reserve_adapter.apply_project_batch(
                reserve_batch.batch_id
            ) == session_module.ProjectBatchApplyResult(
                outcome="remediation_pending"
            )
        assert len(pending_publish_calls) == 1
        assert snapshot(
            reserve_project,
            reserve_session,
            reserve_turn.turn_id,
            reserve_batch.batch_id,
        ) == reserve_exception_before
        assert state._conn.total_changes == reserve_state_changes
        assert conn.total_changes == reserve_project_changes
        assert len(factory_connections) == reserve_factory_calls
        assert len(record_calls) == reserve_record_calls

        modes.pop(reserve_batch.batch_id)
        assert await reserve_adapter.apply_project_batch(
            reserve_batch.batch_id
        ) == session_module.ProjectBatchApplyResult(outcome="conflicted")
        reserve_retried = snapshot(
            reserve_project,
            reserve_session,
            reserve_turn.turn_id,
            reserve_batch.batch_id,
        )
        assert_no_transcript_delivery_cache_or_applied(
            reserve_partial, reserve_retried
        )

        # Fault 2: Projects event insertion aborts its whole transaction.
        (
            projects_fault_project,
            projects_fault_session,
            projects_fault_turn,
            _,
            projects_fault_batch,
            _,
            projects_fault_adapter,
        ) = setup_case(
            "projects-fault",
            "323e4567-e89b-42d3-a456-426614174029",
        )
        modes[projects_fault_batch.batch_id] = "projects-event-fault"
        projects_fault_before = snapshot(
            projects_fault_project,
            projects_fault_session,
            projects_fault_turn.turn_id,
            projects_fault_batch.batch_id,
        )
        assert await projects_fault_adapter.apply_project_batch(
            projects_fault_batch.batch_id
        ) == session_module.ProjectBatchApplyResult(
            outcome="remediation_pending"
        )
        projects_fault_partial = snapshot(
            projects_fault_project,
            projects_fault_session,
            projects_fault_turn.turn_id,
            projects_fault_batch.batch_id,
        )
        assert projects_fault_partial["batch"]["state"] == (
            "conflict_pending"
        )
        assert projects_fault_partial["runtime"] == (
            projects_fault_before["runtime"]
        )
        assert projects_fault_partial["events"] == (
            projects_fault_before["events"]
        )
        assert_no_transcript_delivery_cache_or_applied(
            projects_fault_before, projects_fault_partial
        )
        modes.pop(projects_fault_batch.batch_id)
        assert await projects_fault_adapter.apply_project_batch(
            projects_fault_batch.batch_id
        ) == session_module.ProjectBatchApplyResult(outcome="conflicted")
        projects_fault_retried = snapshot(
            projects_fault_project,
            projects_fault_session,
            projects_fault_turn.turn_id,
            projects_fault_batch.batch_id,
        )
        assert_no_transcript_delivery_cache_or_applied(
            projects_fault_partial, projects_fault_retried
        )

        # Fault 3: Projects is durable, final State CAS rolls back. Replay
        # records Projects write-free, then retries only finalization.
        (
            final_project,
            final_session,
            final_turn,
            _,
            final_batch,
            _,
            final_adapter,
        ) = setup_case(
            "final-fault",
            "423e4567-e89b-42d3-a456-426614174029",
        )
        final_before = snapshot(
            final_project,
            final_session,
            final_turn.turn_id,
            final_batch.batch_id,
        )
        state._conn.executescript(
            f"""
            CREATE TEMP TRIGGER c9_final_state_fault
            BEFORE UPDATE ON project_turn_transcript_batches
            WHEN OLD.batch_id = '{final_batch.batch_id}'
             AND OLD.state = 'conflict_pending'
             AND NEW.state = 'conflicted'
            BEGIN
                SELECT RAISE(ABORT, 'injected C9 final State fault');
            END
            """
        )
        assert await final_adapter.apply_project_batch(
            final_batch.batch_id
        ) == session_module.ProjectBatchApplyResult(
            outcome="remediation_pending"
        )
        final_partial = snapshot(
            final_project,
            final_session,
            final_turn.turn_id,
            final_batch.batch_id,
        )
        assert final_partial["batch"]["state"] == "conflict_pending"
        assert final_partial["runtime"]["transcript_pending_batch_id"] is None
        assert final_partial["runtime"][
            "transcript_dispatch_block_key"
        ] == final_partial["batch"]["transcript_conflict_key"]
        assert len(final_partial["events"]) == len(final_before["events"]) + 1
        assert_no_transcript_delivery_cache_or_applied(
            final_before, final_partial
        )
        state._conn.execute("DROP TRIGGER c9_final_state_fault")
        record_trace_count = len(record_connection_traces)
        assert await final_adapter.apply_project_batch(
            final_batch.batch_id
        ) == session_module.ProjectBatchApplyResult(outcome="conflicted")
        final_retried = snapshot(
            final_project,
            final_session,
            final_turn.turn_id,
            final_batch.batch_id,
        )
        assert_no_transcript_delivery_cache_or_applied(
            final_partial, final_retried
        )
        replay_record_traces = record_connection_traces[
            record_trace_count:
        ]
        assert replay_record_traces
        assert not [
            statement
            for trace in replay_record_traces
            for statement in trace
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]

        # A full-fingerprint mismatch after the durable Projects record also
        # fails the final CAS without rewriting either database.
        (
            fingerprint_project,
            fingerprint_session,
            fingerprint_turn,
            _,
            fingerprint_batch,
            _,
            fingerprint_adapter,
        ) = setup_case(
            "final-fingerprint",
            "473e4567-e89b-42d3-a456-426614174029",
        )
        modes[fingerprint_batch.batch_id] = "final-fingerprint-drift"
        assert await fingerprint_adapter.apply_project_batch(
            fingerprint_batch.batch_id
        ) == session_module.ProjectBatchApplyResult(
            outcome="remediation_pending"
        )
        fingerprint_partial = snapshot(
            fingerprint_project,
            fingerprint_session,
            fingerprint_turn.turn_id,
            fingerprint_batch.batch_id,
        )
        assert fingerprint_partial["batch"]["state"] == "conflict_pending"
        assert fingerprint_partial["batch"]["transcript_sha256"] == "e" * 64
        assert fingerprint_partial["runtime"][
            "transcript_dispatch_block_key"
        ] == fingerprint_partial["batch"]["transcript_conflict_key"]
        state._conn.execute(
            """
            UPDATE project_turn_transcript_batches
            SET transcript_sha256 = ?
            WHERE batch_id = ?
            """,
            (
                final_fingerprint_originals[fingerprint_batch.batch_id],
                fingerprint_batch.batch_id,
            ),
        )
        state._conn.commit()
        modes.pop(fingerprint_batch.batch_id)
        assert await fingerprint_adapter.apply_project_batch(
            fingerprint_batch.batch_id
        ) == session_module.ProjectBatchApplyResult(outcome="conflicted")
        fingerprint_retried = snapshot(
            fingerprint_project,
            fingerprint_session,
            fingerprint_turn.turn_id,
            fingerprint_batch.batch_id,
        )
        assert_no_transcript_delivery_cache_or_applied(
            fingerprint_partial, fingerprint_retried
        )

        # Two calls synchronize after State reservation. They allocate one
        # block/event/version and one final remediation timestamp.
        (
            concurrent_project,
            concurrent_session,
            concurrent_turn,
            _,
            concurrent_batch,
            _,
            concurrent_adapter,
        ) = setup_case(
            "concurrent",
            "523e4567-e89b-42d3-a456-426614174029",
        )
        concurrent_before = snapshot(
            concurrent_project,
            concurrent_session,
            concurrent_turn.turn_id,
            concurrent_batch.batch_id,
        )
        concurrency_barriers[concurrent_batch.batch_id] = (
            threading.Barrier(2)
        )
        order.clear()
        concurrent_state_transaction_start = len(
            completed_state_transactions
        )
        concurrent_results = await asyncio.gather(
            concurrent_adapter.apply_project_batch(
                concurrent_batch.batch_id
            ),
            concurrent_adapter.apply_project_batch(
                concurrent_batch.batch_id
            ),
        )
        assert sorted(
            result.outcome for result in concurrent_results
        ) == ["already_conflicted", "conflicted"]
        concurrent_state_transactions = completed_state_transactions[
            concurrent_state_transaction_start:
        ]
        assert sum(
            ending == "COMMIT" and "state-reserve" in mutations
            for ending, mutations in concurrent_state_transactions
        ) == 1
        assert sum(
            ending == "COMMIT" and "state-finalize" in mutations
            for ending, mutations in concurrent_state_transactions
        ) == 1
        concurrent_after = snapshot(
            concurrent_project,
            concurrent_session,
            concurrent_turn.turn_id,
            concurrent_batch.batch_id,
        )
        assert concurrent_after["batch"]["state"] == "conflicted"
        assert concurrent_after["runtime"]["version"] == (
            concurrent_before["runtime"]["version"] + 1
        )
        assert len(concurrent_after["events"]) == (
            len(concurrent_before["events"]) + 1
        )
        assert_no_transcript_delivery_cache_or_applied(
            concurrent_before, concurrent_after
        )
        concurrent_key = concurrent_after["batch"][
            "transcript_conflict_key"
        ]
        assert sum(
            row[0] == concurrent_key
            for row in concurrent_after["events"]
        ) == 1
    finally:
        state._conn.set_trace_callback(None)
        conn.close()
        state.close()


@pytest.mark.asyncio
async def test_task7_c8_publish_ack_cross_db_crash_boundaries_converge_projects_then_state_once(
    tmp_path,
    monkeypatch,
) -> None:
    """Session publication is durable before, but never concurrent with, Projects ack."""
    import gateway.session as session_module
    from gateway.session import AsyncSessionStore
    from hermes_cli import project_operations
    from hermes_cli import project_runtime_db as prdb
    from hermes_cli import projects_db
    from hermes_cli.project_policy import (
        ActorContext,
        Decision,
        PolicyDecision,
    )
    from hermes_cli.project_runtime import CanonicalTurnResult, ProjectRuntime
    from hermes_state import SessionDB

    projects_path = tmp_path / "projects.db"
    state = SessionDB(db_path=tmp_path / "state.db")
    conn = projects_db.connect(projects_path)
    try:
        project_id = projects_db.create_project(conn, name="C8 publish")
        prdb.create_project_conversation(
            conn,
            project_id=project_id,
            conversation_id="c8-session",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="c8-owner",
            project_id=project_id,
            surface="desktop",
            external_binding_id="c8-window",
            actor_id="owner",
            now=1,
        )
        state.create_session("c8-session", source="cli")
        runtime = ProjectRuntime(conn, clock=lambda: 100)
        actor = ActorContext("owner", "desktop", "c8-owner", True)
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "publish transcript"},
            actor,
            idempotency_key="c8-publish",
            expected_version=0,
        )
        queued_turn = runtime.enqueue_turn(
            project_id,
            {"message": "must remain gated"},
            actor,
            idempotency_key="c8-gated-queue",
            expected_version=1,
        )
        claim = runtime.claim_next_turn(project_id, "c8-worker", lease_seconds=30)
        assert claim is not None
        claim = runtime.mark_turn_started(claim)
        batch_id = "123e4567-e89b-42d3-a456-426614174000"
        prepared_contract = state.prepare_terminal_result(
            claim,
            batch_id=batch_id,
            status="succeeded",
            base_message_count=0,
            messages=(
                {
                    "role": "user",
                    "content": "publish",
                    "timestamp": 10.0,
                    "api_content": "publish [wire]",
                    "display_kind": "project_turn",
                    "display_metadata": {"ordinal": 1},
                    "platform_message_id": "c8-platform-user",
                    "observed": True,
                },
                {
                    "role": "assistant",
                    "content": "done",
                    "timestamp": 11.0,
                    "tool_name": "write",
                    "tool_calls": [
                        {
                            "id": "c8-tool",
                            "type": "function",
                            "function": {
                                "name": "write",
                                "arguments": "{}",
                            },
                        }
                    ],
                    "tool_call_id": "c8-tool",
                    "token_count": 17,
                    "finish_reason": "tool_calls",
                    "reasoning": "private summary",
                    "reasoning_content": "public summary",
                    "reasoning_details": [
                        {"type": "summary", "text": "short"}
                    ],
                    "codex_reasoning_items": [
                        {
                            "type": "reasoning",
                            "encrypted_content": "ciphertext",
                        }
                    ],
                    "codex_message_items": [
                        {"type": "message", "id": "codex-1"}
                    ],
                    "platform_message_id": "c8-platform-assistant",
                    "observed": False,
                    "effect_disposition": "unknown",
                    "api_content": "done [wire]",
                    "display_kind": "project_terminal",
                    "display_metadata": {"ordinal": 2},
                },
            ),
        )

        # Keep today's RED first-trip at the missing State prepare method.
        # Once that API exists, bind every public C8 carrier and callable
        # exactly before exercising cross-database settlement.
        from hermes_cli.project_runtime import (
            PreparedTerminalDecision,
            TerminalTranscriptAcknowledgement,
            TurnAttemptIdentity,
            TurnClaim,
        )
        from hermes_state import PendingProjectBatch

        assert tuple(
            field.name for field in fields(PendingProjectBatch)
        ) == (
            "batch_id",
            "batch_creation_sequence",
            "kind",
            "state",
            "attempt",
            "terminal_status",
            "operation_id",
            "approval_id",
            "base_message_count",
            "created_at",
        )
        assert PendingProjectBatch.__module__ == "hermes_state"
        assert get_type_hints(PendingProjectBatch) == {
            "batch_id": str,
            "batch_creation_sequence": int,
            "kind": Literal[
                "terminal_result",
                "approval_checkpoint",
            ],
            "state": Literal[
                "prepared",
                "published",
                "conflict_pending",
            ],
            "attempt": TurnAttemptIdentity,
            "terminal_status": (
                Literal["succeeded", "failed"] | None
            ),
            "operation_id": str | None,
            "approval_id": str | None,
            "base_message_count": int,
            "created_at": float,
        }
        assert type(prepared_contract) is PendingProjectBatch
        with pytest.raises(FrozenInstanceError):
            prepared_contract.state = "published"

        apply_outcomes = Literal[
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
        ]
        assert tuple(
            field.name
            for field in fields(
                session_module.ProjectBatchApplyResult
            )
        ) == ("outcome",)
        assert (
            session_module.ProjectBatchApplyResult.__module__
            == "gateway.session"
        )
        assert get_type_hints(
            session_module.ProjectBatchApplyResult
        ) == {
            "outcome": apply_outcomes,
        }
        frozen_apply_result = (
            session_module.ProjectBatchApplyResult(outcome="wait")
        )
        with pytest.raises(FrozenInstanceError):
            frozen_apply_result.outcome = "published"

        assert tuple(
            field.name
            for field in fields(
                session_module.ProjectBatchAuthorityDecision
            )
        ) == ("action", "discard_authority")
        assert (
            session_module.ProjectBatchAuthorityDecision.__module__
            == "gateway.session"
        )
        assert get_type_hints(
            session_module.ProjectBatchAuthorityDecision
        ) == {
            "action": Literal["wait", "publish", "discard"],
            "discard_authority": Literal[
                "stop_requested",
                "cancelled",
                "superseded_attempt",
                "superseded_terminal",
                "recovery_blocked",
            ] | None,
        }
        frozen_authority = (
            session_module.ProjectBatchAuthorityDecision(
                action="wait",
                discard_authority=None,
            )
        )
        with pytest.raises(FrozenInstanceError):
            frozen_authority.action = "publish"

        prepare_signature = inspect.signature(
            SessionDB.prepare_terminal_result
        )
        assert tuple(
            (name, parameter.kind)
            for name, parameter
            in prepare_signature.parameters.items()
        ) == (
            ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ("claim", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ("batch_id", inspect.Parameter.KEYWORD_ONLY),
            ("status", inspect.Parameter.KEYWORD_ONLY),
            ("base_message_count", inspect.Parameter.KEYWORD_ONLY),
            ("messages", inspect.Parameter.KEYWORD_ONLY),
        )
        assert get_type_hints(
            SessionDB.prepare_terminal_result
        ) == {
            "claim": TurnClaim,
            "batch_id": str,
            "status": Literal["succeeded", "failed"],
            "base_message_count": int,
            "messages": Sequence[Mapping[str, object]],
            "return": PendingProjectBatch,
        }

        resolve_signature = inspect.signature(
            ProjectRuntime.resolve_prepared_terminal
        )
        assert tuple(
            (name, parameter.kind)
            for name, parameter
            in resolve_signature.parameters.items()
        ) == (
            ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ("attempt", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ("prepared_result_id", inspect.Parameter.KEYWORD_ONLY),
            ("status", inspect.Parameter.KEYWORD_ONLY),
        )
        assert get_type_hints(
            ProjectRuntime.resolve_prepared_terminal
        ) == {
            "attempt": TurnAttemptIdentity,
            "prepared_result_id": str,
            "status": Literal["succeeded", "failed"],
            "return": PreparedTerminalDecision,
        }

        projects_ack_signature = inspect.signature(
            ProjectRuntime.ack_terminal_transcript_applied
        )
        assert tuple(
            (name, parameter.kind)
            for name, parameter
            in projects_ack_signature.parameters.items()
        ) == (
            ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            (
                "acknowledgement",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ),
        )
        assert get_type_hints(
            ProjectRuntime.ack_terminal_transcript_applied
        ) == {
            "acknowledgement": TerminalTranscriptAcknowledgement,
            "return": Literal[
                "acknowledged",
                "already_acknowledged",
            ],
        }

        apply_signature = inspect.signature(
            AsyncSessionStore.apply_project_batch
        )
        assert tuple(
            (name, parameter.kind)
            for name, parameter
            in apply_signature.parameters.items()
        ) == (
            ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ("batch_id", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        )
        assert get_type_hints(
            AsyncSessionStore.apply_project_batch
        ) == {
            "batch_id": str,
            "return": session_module.ProjectBatchApplyResult,
        }
        assert inspect.iscoroutinefunction(
            AsyncSessionStore.apply_project_batch
        )

        runtime.commit_turn_with_task7_batch(
            claim,
            CanonicalTurnResult("succeeded", batch_id),
            transcript_batch_id=batch_id,
        )

        # C8 must batch-insert internally; the public one-message path may not
        # be used from either side of the cross-database authority boundary.
        monkeypatch.setattr(
            state,
            "append_message",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("public one-message append escaped C8 batch transaction")
            ),
        )

        factory_calls = []
        factory_traces = {}
        factory_thread_ids = []
        resolver_instances = []
        resolver_calls = []
        resolver_decisions = []
        resolver_connections = []
        projects_ack_calls = []
        projects_ack_connections = []
        closed_connection_ids: set[int] = set()
        settlement_threads = []
        inject_projects_ack_fault = [False]
        fingerprint_race_batch = [None]
        fingerprint_race_after_mutation = [None]
        fingerprint_race_decisions = []
        discard_fingerprint_race_batch = [None]
        discard_fingerprint_after_mutation = [None]
        discard_carrier_race = [None]
        discard_carrier_projects_after_mutation = [None]
        discard_factory_read_budget = [None]
        expected_discard_resolver_connection = [None]
        post_ack_fingerprint_batch = [None]
        post_ack_fingerprint_original = [None]
        post_ack_fingerprint_after_mutation = [None]
        loop = asyncio.get_running_loop()
        loop_thread_id = threading.get_ident()
        factory_entered = threading.Event()
        loop_callback_ran = threading.Event()
        factory_release = threading.Event()
        offload_probe_pending = [False]
        factory_release_observations = []
        loop_callback_threads = []
        callback_before_release = []
        watchdog_errors = []

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
                settlement_threads.append(
                    ("resolve", threading.get_ident())
                )
                resolver_calls.append(prepared_result_id)
                factory_count = len(factory_calls)
                try:
                    decision = self.delegate.resolve_prepared_terminal(
                        attempt,
                        prepared_result_id=prepared_result_id,
                        status=status,
                    )
                finally:
                    assert len(factory_calls) == factory_count + 1
                    resolver_connection = factory_calls[-1]
                    record_closed_connection_on_owner_thread(
                        resolver_connection
                    )
                    assert resolver_connection not in resolver_connections
                    resolver_connections.append(resolver_connection)

                resolver_decisions.append(
                    (prepared_result_id, decision)
                )
                if prepared_result_id == fingerprint_race_batch[0]:
                    fingerprint_race_decisions.append(decision)
                    race_conn = sqlite3.connect(
                        str(tmp_path / "state.db")
                    )
                    try:
                        race_conn.execute(
                            """
                            UPDATE project_turn_transcript_batches
                            SET state = 'published',
                                published_at = ?,
                                transcript_sha256 = ?
                            WHERE batch_id = ?
                            """,
                            (20.0, "f" * 64, prepared_result_id),
                        )
                        race_conn.commit()
                        fingerprint_race_after_mutation[0] = tuple(
                            race_conn.execute(
                                """
                                SELECT state, transcript_json,
                                       transcript_sha256,
                                       discard_authority,
                                       projects_acknowledged_at
                                FROM project_turn_transcript_batches
                                WHERE batch_id = ?
                                """,
                                (prepared_result_id,),
                            ).fetchone()
                        )
                    finally:
                        race_conn.close()
                if (
                    prepared_result_id
                    == discard_fingerprint_race_batch[0]
                ):
                    assert decision.action == "discard"
                    race_conn = sqlite3.connect(
                        str(tmp_path / "state.db")
                    )
                    try:
                        race_conn.execute(
                            """
                            UPDATE project_turn_transcript_batches
                            SET state = 'discarded',
                                discard_authority = ?,
                                transcript_sha256 = ?
                            WHERE batch_id = ?
                            """,
                            (
                                "superseded_terminal",
                                "d" * 64,
                                prepared_result_id,
                            ),
                        )
                        race_conn.commit()
                        discard_fingerprint_after_mutation[0] = tuple(
                            race_conn.execute(
                                """
                                SELECT state, transcript_sha256,
                                       discard_authority,
                                       projects_acknowledged_at
                                FROM project_turn_transcript_batches
                                WHERE batch_id = ?
                                """,
                                (prepared_result_id,),
                            ).fetchone()
                        )
                    finally:
                        race_conn.close()
                    discard_fingerprint_race_batch[0] = None
                if (
                    discard_carrier_race[0] is not None
                    and prepared_result_id
                    == discard_carrier_race[0]["batch_id"]
                ):
                    assert decision.action == "discard"
                    race_conn = projects_db.connect(projects_path)
                    try:
                        race_conn.execute("BEGIN IMMEDIATE")
                        race_conn.execute(
                            """
                            UPDATE project_turns
                            SET terminal_result_id = ?,
                                transcript_applied_batch_id = NULL
                            WHERE project_id = ? AND turn_id = ?
                            """,
                            (
                                prepared_result_id,
                                discard_carrier_race[0]["project_id"],
                                discard_carrier_race[0]["turn_id"],
                            ),
                        )
                        race_conn.execute(
                            """
                            UPDATE project_runtime_state
                            SET transcript_pending_batch_id = ?,
                                transcript_dispatch_block_key = NULL
                            WHERE project_id = ?
                            """,
                            (
                                prepared_result_id,
                                discard_carrier_race[0]["project_id"],
                            ),
                        )
                        race_conn.commit()
                        discard_carrier_projects_after_mutation[0] = (
                            tuple(
                                race_conn.execute(
                                    """
                                    SELECT terminal_result_id,
                                           transcript_applied_batch_id
                                    FROM project_turns
                                    WHERE project_id = ? AND turn_id = ?
                                    """,
                                    (
                                        discard_carrier_race[0][
                                            "project_id"
                                        ],
                                        discard_carrier_race[0]["turn_id"],
                                    ),
                                ).fetchone()
                            ),
                            tuple(
                                race_conn.execute(
                                    """
                                    SELECT transcript_pending_batch_id,
                                           transcript_dispatch_block_key
                                    FROM project_runtime_state
                                    WHERE project_id = ?
                                    """,
                                    (
                                        discard_carrier_race[0][
                                            "project_id"
                                        ],
                                    ),
                                ).fetchone()
                            ),
                        )
                    except BaseException:
                        race_conn.rollback()
                        raise
                    finally:
                        race_conn.close()
                    discard_carrier_race[0] = None
                return decision

            def ack_terminal_transcript_applied(
                self,
                acknowledgement,
            ):
                settlement_threads.append(
                    ("ack", threading.get_ident())
                )
                projects_ack_calls.append(acknowledgement.batch_id)
                factory_count = len(factory_calls)
                try:
                    outcome = (
                        self.delegate
                        .ack_terminal_transcript_applied(
                            acknowledgement
                        )
                    )
                finally:
                    assert len(factory_calls) == factory_count + 1
                    projects_connection = factory_calls[-1]
                    record_closed_connection_on_owner_thread(
                        projects_connection
                    )
                    assert (
                        projects_connection
                        not in projects_ack_connections
                    )
                    projects_ack_connections.append(
                        projects_connection
                    )

                if (
                    acknowledgement.batch_id
                    == post_ack_fingerprint_batch[0]
                ):
                    race_conn = sqlite3.connect(
                        str(tmp_path / "state.db")
                    )
                    try:
                        post_ack_fingerprint_original[0] = (
                            race_conn.execute(
                                """
                                SELECT transcript_sha256
                                FROM project_turn_transcript_batches
                                WHERE batch_id = ?
                                """,
                                (acknowledgement.batch_id,),
                            ).fetchone()[0]
                        )
                        race_conn.execute(
                            """
                            UPDATE project_turn_transcript_batches
                            SET transcript_sha256 = ?
                            WHERE batch_id = ?
                            """,
                            ("e" * 64, acknowledgement.batch_id),
                        )
                        race_conn.commit()
                        post_ack_fingerprint_after_mutation[0] = tuple(
                            race_conn.execute(
                                """
                                SELECT state, transcript_sha256,
                                       projects_acknowledged_at
                                FROM project_turn_transcript_batches
                                WHERE batch_id = ?
                                """,
                                (acknowledgement.batch_id,),
                            ).fetchone()
                        )
                    finally:
                        race_conn.close()
                    post_ack_fingerprint_batch[0] = None
                return outcome

        monkeypatch.setattr(
            session_module,
            "ProjectBatchAuthorityResolver",
            RecordingResolver,
        )

        def projects_factory():
            assert not state._conn.in_transaction
            factory_thread_ids.append(threading.get_ident())
            if offload_probe_pending[0]:
                offload_probe_pending[0] = False
                factory_entered.set()
                factory_release_observations.append(
                    factory_release.wait(timeout=5.0)
                )
            if discard_factory_read_budget[0] is not None:
                if discard_factory_read_budget[0] == 0:
                    raise AssertionError(
                        "discard path reopened Projects authority"
                    )
                discard_factory_read_budget[0] -= 1
            projects_connection = projects_db.connect(projects_path)
            if discard_factory_read_budget[0] is not None:
                expected_discard_resolver_connection[0] = (
                    projects_connection
                )
            trace = []
            projects_connection.set_trace_callback(trace.append)
            factory_calls.append(projects_connection)
            factory_traces[id(projects_connection)] = trace
            if inject_projects_ack_fault[0]:
                projects_connection.executescript(
                    """
                    CREATE TEMP TRIGGER c8_crash_before_projects_ack
                    BEFORE UPDATE OF transcript_applied_batch_id
                    ON project_turns
                    BEGIN
                        SELECT RAISE(
                            ABORT,
                            'c8 injected projects-ack crash'
                        );
                    END;
                    """
                )
            return projects_connection

        def loop_probe_callback():
            loop_callback_threads.append(threading.get_ident())
            loop_callback_ran.set()

        def offload_watchdog():
            if not factory_entered.wait(timeout=10.0):
                watchdog_errors.append(
                    "projects factory did not enter before deadlock guard"
                )
                return
            loop.call_soon_threadsafe(loop_probe_callback)
            if not loop_callback_ran.wait(timeout=10.0):
                watchdog_errors.append(
                    "event loop callback did not run before deadlock guard"
                )
                return
            callback_before_release.append(
                not factory_release.is_set()
            )
            factory_release.set()

        def connection_is_closed(connection):
            return id(connection) in closed_connection_ids

        def assert_no_projects_dml(connection):
            assert not [
                statement
                for statement in factory_traces[id(connection)]
                if statement.lstrip().upper().startswith(
                    ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
            ]

        message_insert_attempts = []
        inject_message_insert_fault = [True]

        def after_message_insert():
            message_insert_attempts.append(len(message_insert_attempts) + 1)
            assert factory_calls
            assert all(
                connection_is_closed(connection)
                for connection in factory_calls
            )
            if (
                inject_message_insert_fault[0]
                and len(message_insert_attempts) == 2
            ):
                raise sqlite3.IntegrityError(
                    "c8 injected after second batch message insert"
                )
            return 1

        state._conn.create_function(
            "c8_after_message_insert",
            0,
            after_message_insert,
        )
        state._conn.executescript(
            """
            CREATE TEMP TRIGGER c8_after_message_insert
            AFTER INSERT ON messages
            WHEN NEW.session_id = 'c8-session'
            BEGIN
                SELECT c8_after_message_insert();
            END;
            """
        )

        state_ack_attempts = []
        inject_state_ack_fault = [False]

        def before_state_ack():
            assert state._conn.in_transaction is True
            assert projects_ack_connections
            assert connection_is_closed(
                projects_ack_connections[-1]
            )
            state_ack_attempts.append(len(state_ack_attempts) + 1)
            if inject_state_ack_fault[0]:
                raise sqlite3.IntegrityError(
                    "c8 injected state acknowledgement crash"
                )
            return 1

        state._conn.create_function(
            "c8_before_state_ack",
            0,
            before_state_ack,
        )
        state._conn.executescript(
            """
            CREATE TEMP TRIGGER c8_before_state_ack
            BEFORE UPDATE OF projects_acknowledged_at
            ON project_turn_transcript_batches
            WHEN OLD.projects_acknowledged_at IS NULL
             AND NEW.projects_acknowledged_at IS NOT NULL
            BEGIN
                SELECT c8_before_state_ack();
            END;
            """
        )

        state_discard_attempts = []

        def before_state_discard():
            assert state._conn.in_transaction is True
            assert expected_discard_resolver_connection[0] is not None
            assert connection_is_closed(
                expected_discard_resolver_connection[0]
            )
            state_discard_attempts.append(
                len(state_discard_attempts) + 1
            )
            return 1

        state._conn.create_function(
            "c8_before_state_discard",
            0,
            before_state_discard,
        )
        state._conn.executescript(
            """
            CREATE TEMP TRIGGER c8_before_state_discard
            BEFORE UPDATE OF state
            ON project_turn_transcript_batches
            WHEN OLD.state = 'prepared'
             AND NEW.state = 'discarded'
            BEGIN
                SELECT c8_before_state_discard();
            END;
            """
        )

        adapter = AsyncSessionStore(
            state,
            projects_db_factory=projects_factory,
        )
        assert len(resolver_instances) == 1
        bound_resolver = resolver_instances[0]

        class PoisonResolver:
            def __init__(self, *args, **kwargs):
                raise AssertionError(
                    "AsyncSessionStore must retain its construction-bound "
                    "ProjectBatchAuthorityResolver"
                )

        monkeypatch.setattr(
            session_module,
            "ProjectBatchAuthorityResolver",
            PoisonResolver,
        )
        offload_probe_pending[0] = True
        watchdog = threading.Thread(
            target=offload_watchdog,
            daemon=True,
        )
        watchdog.start()

        # Crash after the second of multiple inserts.  Both message rows, both
        # session counters, and prepared->published share one rollback.
        with pytest.raises(
            sqlite3.OperationalError,
            match="user-defined function raised exception",
        ):
            await adapter.apply_project_batch(batch_id)
        await asyncio.to_thread(watchdog.join, 10.0)
        assert not watchdog.is_alive()
        assert watchdog_errors == []
        assert loop_callback_ran.is_set()
        assert loop_callback_threads == [loop_thread_id]
        assert callback_before_release == [True]
        assert factory_release_observations == [True]
        assert factory_thread_ids == [settlement_threads[0][1]]
        assert settlement_threads[0][0] == "resolve"
        assert settlement_threads[0][1] != loop_thread_id
        assert resolver_instances == [bound_resolver]
        assert resolver_calls == [batch_id]
        assert projects_ack_calls == []
        assert len(factory_calls) == 1
        assert connection_is_closed(factory_calls[0])
        assert message_insert_attempts == [1, 2]
        assert state.get_messages("c8-session") == []
        assert tuple(
            state._conn.execute(
                """
                SELECT message_count, tool_call_count
                FROM sessions WHERE id = 'c8-session'
                """
            ).fetchone()
        ) == (0, 0)
        assert tuple(
            state._conn.execute(
                """
                SELECT state, published_at, projects_acknowledged_at
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
        ) == ("prepared", None, None)
        assert conn.execute(
            """
            SELECT transcript_pending_batch_id
            FROM project_runtime_state
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()[0] == batch_id

        inject_message_insert_fault[0] = False
        inject_projects_ack_fault[0] = True
        first = await adapter.apply_project_batch(batch_id)
        assert first.outcome == "settlement_pending"
        assert resolver_calls == [batch_id, batch_id]
        assert len(projects_ack_calls) == 1
        assert len(factory_calls) == 3
        assert resolver_connections == [
            factory_calls[0],
            factory_calls[1],
        ]
        assert projects_ack_connections == [factory_calls[2]]
        assert all(
            connection_is_closed(connection)
            for connection in factory_calls
        )
        for resolver_connection in resolver_connections:
            assert_no_projects_dml(resolver_connection)
        assert [
            message["content"]
            for message in state.get_messages("c8-session")
        ] == ["publish", "done"]
        assert message_insert_attempts == [1, 2, 3, 4]
        assert tuple(
            state._conn.execute(
                """
                SELECT message_count, tool_call_count
                FROM sessions WHERE id = 'c8-session'
                """
            ).fetchone()
        ) == (2, 1)

        persisted_messages = [
            dict(row)
            for row in state._conn.execute(
                """
                SELECT role, content, tool_call_id, tool_calls, tool_name,
                       effect_disposition, timestamp, token_count,
                       finish_reason, reasoning, reasoning_content,
                       reasoning_details, codex_reasoning_items,
                       codex_message_items, platform_message_id, observed,
                       api_content, display_kind, display_metadata
                FROM messages
                WHERE session_id = 'c8-session'
                ORDER BY id
                """
            )
        ]
        user_message = dict(persisted_messages[0])
        user_display_metadata = user_message.pop(
            "display_metadata"
        )
        assert user_message == {
            "role": "user",
            "content": "publish",
            "tool_call_id": None,
            "tool_calls": None,
            "tool_name": None,
            "effect_disposition": None,
            "timestamp": 10.0,
            "token_count": None,
            "finish_reason": None,
            "reasoning": None,
            "reasoning_content": None,
            "reasoning_details": None,
            "codex_reasoning_items": None,
            "codex_message_items": None,
            "platform_message_id": "c8-platform-user",
            "observed": 1,
            "api_content": "publish [wire]",
            "display_kind": "project_turn",
        }
        assert json.loads(user_display_metadata) == {"ordinal": 1}
        assistant_message = persisted_messages[1]
        assert {
            "role": assistant_message["role"],
            "content": assistant_message["content"],
            "tool_call_id": assistant_message["tool_call_id"],
            "tool_name": assistant_message["tool_name"],
            "effect_disposition": assistant_message[
                "effect_disposition"
            ],
            "timestamp": assistant_message["timestamp"],
            "token_count": assistant_message["token_count"],
            "finish_reason": assistant_message["finish_reason"],
            "reasoning": assistant_message["reasoning"],
            "reasoning_content": assistant_message["reasoning_content"],
            "platform_message_id": assistant_message[
                "platform_message_id"
            ],
            "observed": assistant_message["observed"],
            "api_content": assistant_message["api_content"],
            "display_kind": assistant_message["display_kind"],
        } == {
            "role": "assistant",
            "content": "done",
            "tool_call_id": "c8-tool",
            "tool_name": "write",
            "effect_disposition": "unknown",
            "timestamp": 11.0,
            "token_count": 17,
            "finish_reason": "tool_calls",
            "reasoning": "private summary",
            "reasoning_content": "public summary",
            "platform_message_id": "c8-platform-assistant",
            "observed": 0,
            "api_content": "done [wire]",
            "display_kind": "project_terminal",
        }
        assert json.loads(assistant_message["tool_calls"]) == [
            {
                "id": "c8-tool",
                "type": "function",
                "function": {"name": "write", "arguments": "{}"},
            }
        ]
        assert json.loads(assistant_message["reasoning_details"]) == [
            {"type": "summary", "text": "short"}
        ]
        assert json.loads(
            assistant_message["codex_reasoning_items"]
        ) == [
            {
                "type": "reasoning",
                "encrypted_content": "ciphertext",
            }
        ]
        assert json.loads(assistant_message["codex_message_items"]) == [
            {"type": "message", "id": "codex-1"}
        ]
        assert json.loads(assistant_message["display_metadata"]) == {
            "ordinal": 2
        }
        assert conn.execute(
            """
            SELECT transcript_pending_batch_id
            FROM project_runtime_state
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()[0] == batch_id
        assert conn.execute(
            """
            SELECT transcript_applied_batch_id
            FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0] is None

        published_upper = state.pending_project_batch_upper_watermark()
        assert published_upper is not None
        published_page = state.list_pending_project_batches(
            after=None,
            through=published_upper,
            limit=1,
        )
        assert len(published_page) == 1
        assert published_page[0].batch_id == batch_id
        assert published_page[0].state == "published"

        dispatcher_lease = runtime.acquire_dispatcher_lease(
            "11111111-1111-4111-8111-111111111111", lease_seconds=30
        )
        assert dispatcher_lease is not None
        assert runtime.claim_next_turn_for_dispatcher(
            project_id,
            "c8-dispatcher",
            lease_seconds=30,
            dispatcher_lease=dispatcher_lease,
        ) is None
        assert conn.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (queued_turn.turn_id,),
        ).fetchone()[0] == "queued"

        # Build a real approved operation through the public guard.  A genuine
        # terminal history cannot coexist with this rehydratable FIFO shape,
        # so only the project gate itself is installed test-side.
        operation_project = projects_db.create_project(
            conn,
            name="C8 approved gate",
            folders=("C:/work/operations",),
        )
        prdb.create_project_conversation(
            conn,
            project_id=operation_project,
            conversation_id="c8-operation-session",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="c8-operation-owner",
            project_id=operation_project,
            surface="desktop",
            external_binding_id="c8-operation-window",
            actor_id="owner",
            now=1,
        )
        operation_actor = ActorContext(
            "owner",
            "desktop",
            "c8-operation-owner",
            True,
        )
        operation_turn = runtime.enqueue_turn(
            operation_project,
            {"message": "approved operation must remain gated"},
            operation_actor,
            idempotency_key="c8-operation-turn",
            expected_version=0,
        )
        operation_claim = runtime.claim_next_turn(
            operation_project,
            "c8-operation-worker",
            lease_seconds=30,
        )
        assert operation_claim is not None
        operation_claim = runtime.mark_turn_started(operation_claim)
        operation_guard = project_operations.ProjectOperationGuard(runtime)
        operation_id = "c8-approved-operation"
        approval_id = "c8-approved-operation-approval"
        operation_guard.prepare(
            operation_claim,
            project_operations.OperationIntent(
                operation_id=operation_id,
                project_id=operation_project,
                turn_id=operation_turn.turn_id,
                idempotency_key="c8-approved-operation-key",
                canonical_action="publish",
                command_revision=1,
                targets=("C:/work/operations/c8.txt",),
                batch_items=("publish-c8",),
                payload={"content_digest": "sha256:c8"},
                readback_kind="remote-ledger",
                remote_idempotency_supported=True,
            ),
            policy=PolicyDecision(
                Decision.REQUIRE_APPROVAL,
                "policy.approval.publish",
                "publish is critical",
                "publish",
            ),
            approval=project_operations.OperationApprovalSpec(
                approval_id,
                "publish",
                1_000,
                operation_actor,
            ),
        )
        approved_operation = operation_guard.resolve_operation_approval(
            approval_id,
            operation_actor,
            outcome="approved",
        )
        assert approved_operation.status == "approved"
        conn.execute(
            """
            UPDATE project_runtime_state
            SET transcript_pending_batch_id = ?
            WHERE project_id = ?
            """,
            (batch_id, operation_project),
        )
        conn.commit()

        def gated_projects_snapshot():
            selected_projects = (project_id, operation_project)
            return (
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_runtime_state
                        WHERE project_id IN (?, ?)
                        ORDER BY project_id
                        """,
                        selected_projects,
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_turns
                        WHERE project_id IN (?, ?)
                        ORDER BY project_id, sequence
                        """,
                        selected_projects,
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_run_controls
                        WHERE project_id IN (?, ?)
                        ORDER BY project_id, turn_id
                        """,
                        selected_projects,
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_worker_leases
                        WHERE project_id IN (?, ?)
                        ORDER BY project_id, turn_id
                        """,
                        selected_projects,
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_events
                        WHERE project_id IN (?, ?)
                        ORDER BY project_id, sequence
                        """,
                        selected_projects,
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_operations
                        WHERE project_id IN (?, ?)
                        ORDER BY project_id, operation_id
                        """,
                        selected_projects,
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_approvals
                        WHERE project_id IN (?, ?)
                        ORDER BY project_id, approval_id
                        """,
                        selected_projects,
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_runtime_membership_counters
                        ORDER BY lane
                        """
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_dispatcher_leases
                        ORDER BY lease_name
                        """
                    )
                ),
            )

        identity_allocations = []
        original_id_factory = runtime._id_factory

        def forbidden_identity(kind):
            identity_allocations.append(kind)
            raise AssertionError(
                "pending transcript gate must precede identity allocation"
            )

        monkeypatch.setattr(runtime, "_id_factory", forbidden_identity)
        gated_before = gated_projects_snapshot()
        assert runtime.claim_next_turn_for_dispatcher(
            project_id,
            "c8-dispatcher",
            lease_seconds=30,
            dispatcher_lease=dispatcher_lease,
        ) is None
        assert operation_guard.rehydrate_approved_operation_for_dispatcher(
            operation_project,
            operation_id,
            worker_id="c8-operation-rehydrate",
            lease_seconds=30,
            dispatcher_lease=dispatcher_lease,
        ) is None
        assert identity_allocations == []
        assert gated_projects_snapshot() == gated_before
        monkeypatch.setattr(runtime, "_id_factory", original_id_factory)

        # Session publication is now durable.  After Projects commits its ack,
        # mutate the immutable State fingerprint before State's exact CAS.
        # The adapter may not stamp acknowledgement authority onto the changed
        # batch and reports settlement still pending.
        inject_projects_ack_fault[0] = False
        post_ack_fingerprint_batch[0] = batch_id
        fingerprint_settlement = await adapter.apply_project_batch(
            batch_id
        )
        assert fingerprint_settlement.outcome == "settlement_pending"
        assert resolver_calls == [batch_id, batch_id]
        assert len(projects_ack_calls) == 2
        assert len(factory_calls) == 4
        assert projects_ack_connections[-1] is factory_calls[-1]
        assert connection_is_closed(projects_ack_connections[-1])
        assert state_ack_attempts == []
        assert post_ack_fingerprint_after_mutation[0] == (
            "published",
            "e" * 64,
            None,
        )
        assert tuple(
            state._conn.execute(
                """
                SELECT state, transcript_sha256,
                       projects_acknowledged_at
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
        ) == post_ack_fingerprint_after_mutation[0]
        assert tuple(
            conn.execute(
                """
                SELECT s.transcript_pending_batch_id,
                       t.transcript_applied_batch_id
                FROM project_runtime_state AS s
                JOIN project_turns AS t
                  ON t.project_id = s.project_id
                WHERE s.project_id = ? AND t.turn_id = ?
                """,
                (project_id, turn.turn_id),
            ).fetchone()
        ) == (None, batch_id)
        restore_state = sqlite3.connect(str(tmp_path / "state.db"))
        try:
            restore_state.execute(
                """
                UPDATE project_turn_transcript_batches
                SET transcript_sha256 = ?
                WHERE batch_id = ?
                """,
                (post_ack_fingerprint_original[0], batch_id),
            )
            restore_state.commit()
        finally:
            restore_state.close()

        # Replay the already-applied Projects proof, then crash the separate
        # State acknowledgement while its own transaction is active.
        inject_state_ack_fault[0] = True
        second = await adapter.apply_project_batch(batch_id)
        assert second.outcome == "settlement_pending"
        assert resolver_calls == [batch_id, batch_id]
        assert len(projects_ack_calls) == 3
        assert len(factory_calls) == 5
        assert projects_ack_connections[-1] is factory_calls[-1]
        assert connection_is_closed(projects_ack_connections[-1])
        assert state_ack_attempts == [1]
        assert conn.execute(
            """
            SELECT transcript_pending_batch_id
            FROM project_runtime_state
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()[0] is None
        assert conn.execute(
            """
            SELECT transcript_applied_batch_id
            FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0] == batch_id
        assert state._conn.execute(
            """
            SELECT projects_acknowledged_at
            FROM project_turn_transcript_batches
            WHERE batch_id = ?
            """,
            (batch_id,),
        ).fetchone()[0] is None
        ack_pending_upper = state.pending_project_batch_upper_watermark()
        assert ack_pending_upper is not None
        ack_pending_page = state.list_pending_project_batches(
            after=None,
            through=ack_pending_upper,
            limit=1,
        )
        assert (
            len(ack_pending_page) == 1
            and ack_pending_page[0].batch_id == batch_id
            and ack_pending_page[0].state == "published"
        )

        # Replay the already-applied Projects proof, then durably acknowledge
        # it in state.  The finite timestamp is immutable storage authority.
        inject_state_ack_fault[0] = False
        replay = await adapter.apply_project_batch(batch_id)
        assert replay.outcome == "already_published"
        assert resolver_calls == [batch_id, batch_id]
        assert len(projects_ack_calls) == 4
        assert len(factory_calls) == 6
        assert projects_ack_connections[-1] is factory_calls[-1]
        assert connection_is_closed(projects_ack_connections[-1])
        assert state_ack_attempts == [1, 2]
        acknowledged_at = state._conn.execute(
            """
            SELECT projects_acknowledged_at
            FROM project_turn_transcript_batches
            WHERE batch_id = ?
            """,
            (batch_id,),
        ).fetchone()[0]
        assert type(acknowledged_at) in {int, float}
        assert math.isfinite(acknowledged_at)
        assert 0 <= acknowledged_at <= 253_402_300_799.0
        assert len(state.get_messages("c8-session")) == 2
        assert state.pending_project_batch_upper_watermark() is None
        post_ack_page = state.list_pending_project_batches(
            after=None,
            through=published_upper,
            limit=100,
        )
        assert post_ack_page == ()
        before_published_upper = type(published_upper)(
            published_upper.batch_creation_sequence,
            "00000000-0000-4000-8000-000000000000",
        )
        assert (
            before_published_upper.batch_creation_sequence,
            before_published_upper.batch_id,
        ) < (
            published_upper.batch_creation_sequence,
            published_upper.batch_id,
        )
        assert not state._pending_project_batches_remaining(
            after=before_published_upper,
            through=published_upper,
        )
        assert state._conn.execute(
            """
            SELECT projects_acknowledged_at
            FROM project_turn_transcript_batches
            WHERE batch_id = ?
            """,
            (batch_id,),
        ).fetchone()[0] == acknowledged_at

        def durable_cross_db_snapshot():
            return (
                tuple(
                    tuple(row)
                    for row in state._conn.execute(
                        """
                        SELECT * FROM project_turn_transcript_batches
                        ORDER BY batch_creation_sequence, batch_id
                        """
                    )
                ),
                tuple(
                    tuple(row)
                    for row in state._conn.execute(
                        """
                        SELECT * FROM messages
                        WHERE session_id = 'c8-session'
                        ORDER BY id
                        """
                    )
                ),
                tuple(
                    state._conn.execute(
                        """
                        SELECT message_count, tool_call_count
                        FROM sessions WHERE id = 'c8-session'
                        """
                    ).fetchone()
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_runtime_state
                        WHERE project_id = ?
                        """,
                        (project_id,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_turns
                        WHERE project_id = ? ORDER BY sequence
                        """,
                        (project_id,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_run_controls
                        WHERE project_id = ? ORDER BY turn_id
                        """,
                        (project_id,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_worker_leases
                        WHERE project_id = ? ORDER BY lease_id
                        """,
                        (project_id,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_events
                        WHERE project_id = ? ORDER BY sequence
                        """,
                        (project_id,),
                    )
                ),
            )

        before_write_free_replay = durable_cross_db_snapshot()
        before_state_changes = state._conn.total_changes
        before_factory_count = len(factory_calls)
        before_resolver_count = len(resolver_calls)
        before_projects_ack_count = len(projects_ack_calls)
        before_state_ack_count = len(state_ack_attempts)
        final_replay = await adapter.apply_project_batch(
            batch_id=batch_id
        )
        assert final_replay.outcome == "already_published"
        assert durable_cross_db_snapshot() == before_write_free_replay
        assert state._conn.total_changes == before_state_changes
        assert len(factory_calls) == before_factory_count
        assert len(resolver_calls) == before_resolver_count
        assert len(projects_ack_calls) == before_projects_ack_count
        assert len(state_ack_attempts) == before_state_ack_count

        def create_mapping_case(name, mapping_batch_id):
            mapping_project = projects_db.create_project(
                conn,
                name=f"C8 {name} mapping",
            )
            mapping_session = f"c8-{name}-session"
            mapping_binding = f"c8-{name}-owner"
            prdb.create_project_conversation(
                conn,
                project_id=mapping_project,
                conversation_id=mapping_session,
                current_phase="implementation",
                now=1,
            )
            prdb.bind_surface(
                conn,
                binding_id=mapping_binding,
                project_id=mapping_project,
                surface="desktop",
                external_binding_id=f"c8-{name}-window",
                actor_id="owner",
                now=1,
            )
            state.create_session(mapping_session, source="cli")
            mapping_actor = ActorContext(
                "owner",
                "desktop",
                mapping_binding,
                True,
            )
            mapping_turn = runtime.enqueue_turn(
                mapping_project,
                {"message": f"{name} mapping"},
                mapping_actor,
                idempotency_key=f"c8-{name}-mapping",
                expected_version=0,
            )
            mapping_claim = runtime.claim_next_turn(
                mapping_project,
                f"c8-{name}-worker",
                lease_seconds=30,
            )
            assert mapping_claim is not None
            mapping_claim = runtime.mark_turn_started(mapping_claim)
            state.prepare_terminal_result(
                mapping_claim,
                batch_id=mapping_batch_id,
                status="succeeded",
                base_message_count=0,
                messages=(
                    {
                        "role": "user",
                        "content": f"{name} request",
                        "timestamp": 30.0,
                    },
                    {
                        "role": "assistant",
                        "content": f"{name} result",
                        "timestamp": 31.0,
                    },
                ),
            )
            return (
                mapping_project,
                mapping_session,
                mapping_turn,
                mapping_claim,
            )

        def mapping_case_snapshot(
            mapping_project,
            mapping_session,
            mapping_turn,
            mapping_batch_id,
        ):
            return (
                tuple(
                    state._conn.execute(
                        """
                        SELECT *
                        FROM project_turn_transcript_batches
                        WHERE batch_id = ?
                        """,
                        (mapping_batch_id,),
                    ).fetchone()
                ),
                tuple(
                    tuple(row)
                    for row in state._conn.execute(
                        """
                        SELECT * FROM messages
                        WHERE session_id = ? ORDER BY id
                        """,
                        (mapping_session,),
                    )
                ),
                tuple(
                    state._conn.execute(
                        "SELECT * FROM sessions WHERE id = ?",
                        (mapping_session,),
                    ).fetchone()
                ),
                tuple(
                    conn.execute(
                        """
                        SELECT * FROM project_runtime_state
                        WHERE project_id = ?
                        """,
                        (mapping_project,),
                    ).fetchone()
                ),
                tuple(
                    conn.execute(
                        "SELECT * FROM project_turns WHERE turn_id = ?",
                        (mapping_turn.turn_id,),
                    ).fetchone()
                ),
                tuple(
                    conn.execute(
                        """
                        SELECT * FROM project_run_controls
                        WHERE turn_id = ?
                        """,
                        (mapping_turn.turn_id,),
                    ).fetchone()
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_worker_leases
                        WHERE project_id = ? ORDER BY lease_id
                        """,
                        (mapping_project,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_events
                        WHERE project_id = ? ORDER BY sequence
                        """,
                        (mapping_project,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_deliveries
                        WHERE project_id = ? ORDER BY delivery_id
                        """,
                        (mapping_project,),
                    )
                ),
            )

        wait_batch = "623e4567-e89b-42d3-a456-426614174000"
        (
            wait_project,
            wait_session,
            wait_turn,
            _,
        ) = create_mapping_case("wait", wait_batch)
        wait_before = mapping_case_snapshot(
            wait_project,
            wait_session,
            wait_turn,
            wait_batch,
        )
        wait_state_changes = state._conn.total_changes
        wait_projects_changes = conn.total_changes
        wait_factory_count = len(factory_calls)
        wait_resolver_count = len(resolver_calls)
        wait_resolver_connection_count = len(
            resolver_connections
        )
        wait_ack_count = len(projects_ack_calls)
        wait_result = await adapter.apply_project_batch(wait_batch)
        assert wait_result.outcome == "wait"
        assert len(factory_calls) == wait_factory_count + 1
        assert len(resolver_calls) == wait_resolver_count + 1
        assert (
            len(resolver_connections)
            == wait_resolver_connection_count + 1
        )
        assert resolver_calls[-1] == wait_batch
        assert resolver_connections[-1] is factory_calls[-1]
        assert len(projects_ack_calls) == wait_ack_count
        assert connection_is_closed(factory_calls[-1])
        assert_no_projects_dml(factory_calls[-1])
        assert resolver_decisions[-1][0] == wait_batch
        assert resolver_decisions[-1][1].action == "wait"
        assert state._conn.total_changes == wait_state_changes
        assert conn.total_changes == wait_projects_changes
        assert mapping_case_snapshot(
            wait_project,
            wait_session,
            wait_turn,
            wait_batch,
        ) == wait_before

        authority_batch = (
            "723e4567-e89b-42d3-a456-426614174000"
        )
        (
            authority_project,
            authority_session,
            authority_turn,
            authority_claim,
        ) = create_mapping_case(
            "authority-conflict",
            authority_batch,
        )
        runtime.commit_turn_with_task7_batch(
            authority_claim,
            CanonicalTurnResult("succeeded", authority_batch),
            transcript_batch_id=authority_batch,
        )
        conn.execute(
            """
            UPDATE project_runtime_state
            SET transcript_pending_batch_id = ?
            WHERE project_id = ?
            """,
            (
                "823e4567-e89b-42d3-a456-426614174000",
                authority_project,
            ),
        )
        conn.commit()
        authority_before = mapping_case_snapshot(
            authority_project,
            authority_session,
            authority_turn,
            authority_batch,
        )
        authority_state_changes = state._conn.total_changes
        authority_projects_changes = conn.total_changes
        authority_factory_count = len(factory_calls)
        authority_resolver_count = len(resolver_calls)
        authority_resolver_connection_count = len(
            resolver_connections
        )
        authority_ack_count = len(projects_ack_calls)
        authority_result = await adapter.apply_project_batch(
            authority_batch
        )
        assert authority_result.outcome == "authority_conflict"
        assert len(factory_calls) == authority_factory_count + 1
        assert len(resolver_calls) == authority_resolver_count + 1
        assert (
            len(resolver_connections)
            == authority_resolver_connection_count + 1
        )
        assert resolver_calls[-1] == authority_batch
        assert resolver_connections[-1] is factory_calls[-1]
        assert len(projects_ack_calls) == authority_ack_count
        assert connection_is_closed(factory_calls[-1])
        assert_no_projects_dml(factory_calls[-1])
        assert state._conn.total_changes == authority_state_changes
        assert conn.total_changes == authority_projects_changes
        assert mapping_case_snapshot(
            authority_project,
            authority_session,
            authority_turn,
            authority_batch,
        ) == authority_before

        discard_batch = "323e4567-e89b-42d3-a456-426614174000"
        different_terminal_batch = "423e4567-e89b-42d3-a456-426614174000"
        discarded_claim = runtime.claim_next_turn(project_id, "c8-worker", lease_seconds=30)
        assert discarded_claim is not None
        discarded_claim = runtime.mark_turn_started(discarded_claim)
        state.prepare_terminal_result(
            discarded_claim,
            batch_id=discard_batch,
            status="succeeded",
            base_message_count=2,
            messages=(
                {"role": "user", "content": "obsolete", "timestamp": 12.0},
                {"role": "assistant", "content": "discard", "timestamp": 13.0},
            ),
        )
        runtime.commit_turn_with_task7_batch(
            discarded_claim,
            CanonicalTurnResult("succeeded", different_terminal_batch),
            transcript_batch_id=different_terminal_batch,
        )
        before_discard_messages = tuple(
            state.get_messages("c8-session")
        )
        before_discard_ack_calls = len(projects_ack_calls)

        # A discard uses the same immutable fingerprint CAS as publication.
        # Atomically make the resolver snapshot stale and State discarded
        # after the sole read-only resolver returns.  The stale terminal row
        # must conflict before any Projects acknowledgement.
        discard_factory_before = len(factory_calls)
        discard_resolver_before = len(resolver_calls)
        discard_state_attempts_before = len(state_discard_attempts)
        discard_state_changes_before = state._conn.total_changes
        discard_factory_read_budget[0] = 1
        expected_discard_resolver_connection[0] = None
        discard_fingerprint_race_batch[0] = discard_batch
        discard_state_conflict = await adapter.apply_project_batch(
            discard_batch
        )
        discard_factory_read_budget[0] = None
        assert discard_state_conflict.outcome == "state_conflict"
        assert len(factory_calls) == discard_factory_before + 1
        assert len(resolver_calls) == discard_resolver_before + 1
        assert resolver_calls[-1] == discard_batch
        assert resolver_connections[-1] is factory_calls[-1]
        assert expected_discard_resolver_connection[0] is factory_calls[-1]
        assert connection_is_closed(factory_calls[-1])
        assert_no_projects_dml(factory_calls[-1])
        assert len(projects_ack_calls) == before_discard_ack_calls
        assert discard_batch not in projects_ack_calls
        assert len(state_discard_attempts) == discard_state_attempts_before
        assert state._conn.total_changes == discard_state_changes_before
        assert tuple(
            state._conn.execute(
                """
                SELECT state, transcript_sha256, discard_authority,
                       projects_acknowledged_at
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (discard_batch,),
            ).fetchone()
        ) == discard_fingerprint_after_mutation[0]
        assert discard_fingerprint_after_mutation[0] == (
            "discarded",
            "d" * 64,
            "superseded_terminal",
            None,
        )
        assert tuple(state.get_messages("c8-session")) == (
            before_discard_messages
        )

        restore_discard_state = sqlite3.connect(
            str(tmp_path / "state.db")
        )
        try:
            restore_discard_state.execute(
                """
                DELETE FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (discard_batch,),
            )
            restore_discard_state.commit()
        finally:
            restore_discard_state.close()
        state.prepare_terminal_result(
            discarded_claim,
            batch_id=discard_batch,
            status="succeeded",
            base_message_count=2,
            messages=(
                {
                    "role": "user",
                    "content": "obsolete",
                    "timestamp": 12.0,
                },
                {
                    "role": "assistant",
                    "content": "discard",
                    "timestamp": 13.0,
                },
            ),
        )

        # Mutate Projects from discard authority to publish authority after
        # that same sole resolver returns.  The immutable decision carrier,
        # not a second read or inference, owns the State discard reason.
        discard_factory_before = len(factory_calls)
        discard_resolver_before = len(resolver_calls)
        discard_state_attempts_before = len(state_discard_attempts)
        discard_factory_read_budget[0] = 1
        expected_discard_resolver_connection[0] = None
        discard_carrier_race[0] = {
            "batch_id": discard_batch,
            "project_id": project_id,
            "turn_id": discarded_claim.turn_id,
        }
        discarded = await adapter.apply_project_batch(discard_batch)
        discard_factory_read_budget[0] = None
        assert discarded.outcome == "discarded"
        assert len(factory_calls) == discard_factory_before + 1
        assert len(resolver_calls) == discard_resolver_before + 1
        assert resolver_calls[-1] == discard_batch
        assert resolver_connections[-1] is factory_calls[-1]
        assert expected_discard_resolver_connection[0] is factory_calls[-1]
        assert connection_is_closed(factory_calls[-1])
        assert_no_projects_dml(factory_calls[-1])
        assert (
            len(state_discard_attempts)
            == discard_state_attempts_before + 1
        )
        assert discard_carrier_projects_after_mutation[0] == (
            (discard_batch, None),
            (discard_batch, None),
        )
        discard_decision = next(
            decision
            for selected_batch, decision in reversed(resolver_decisions)
            if selected_batch == discard_batch
        )
        assert discard_decision.action == "discard"
        assert discard_decision.terminal is None
        assert (
            discard_decision.discard_authority
            == "superseded_terminal"
        )
        assert len(projects_ack_calls) == before_discard_ack_calls
        assert tuple(
            state._conn.execute(
                """
                SELECT state, discard_authority
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (discard_batch,),
            ).fetchone()
        ) == ("discarded", "superseded_terminal")
        assert [message["content"] for message in state.get_messages("c8-session")] == [
            "publish", "done"
        ]
        assert tuple(state.get_messages("c8-session")) == (
            before_discard_messages
        )
        assert tuple(
            state._conn.execute(
                """
                SELECT message_count, tool_call_count
                FROM sessions WHERE id = 'c8-session'
                """
            ).fetchone()
        ) == (2, 1)
        before_discard_replay = (
            tuple(state.get_messages("c8-session")),
            tuple(
                state._conn.execute(
                    """
                    SELECT *
                    FROM project_turn_transcript_batches
                    WHERE batch_id = ?
                    """,
                    (discard_batch,),
                ).fetchone()
            ),
            conn.execute(
                """
                SELECT transcript_pending_batch_id
                FROM project_runtime_state
                WHERE project_id = ?
                """,
                (project_id,),
            ).fetchone()[0],
        )
        before_discard_replay_changes = state._conn.total_changes
        before_discard_replay_factory = len(factory_calls)
        before_discard_replay_resolver = len(resolver_calls)
        before_discard_replay_ack = len(projects_ack_calls)
        before_discard_replay_state_attempts = len(
            state_discard_attempts
        )
        assert await adapter.apply_project_batch(discard_batch) == type(discarded)(
            outcome="already_discarded"
        )
        assert (
            tuple(state.get_messages("c8-session")),
            tuple(
                state._conn.execute(
                    """
                    SELECT *
                    FROM project_turn_transcript_batches
                    WHERE batch_id = ?
                    """,
                    (discard_batch,),
                ).fetchone()
            ),
            conn.execute(
                """
                SELECT transcript_pending_batch_id
                FROM project_runtime_state
                WHERE project_id = ?
                """,
                (project_id,),
            ).fetchone()[0],
        ) == before_discard_replay
        assert state._conn.total_changes == before_discard_replay_changes
        assert len(factory_calls) == before_discard_replay_factory
        assert len(resolver_calls) == before_discard_replay_resolver
        assert len(projects_ack_calls) == before_discard_replay_ack
        assert (
            len(state_discard_attempts)
            == before_discard_replay_state_attempts
        )
        assert tuple(
            state._conn.execute(
                """
                SELECT state, discard_authority
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (discard_batch,),
            ).fetchone()
        ) == ("discarded", "superseded_terminal")

        # Atomically make State published with a stale fingerprint after one
        # resolver call but before SessionDB's publish CAS.  Only the injected
        # race writes; the adapter returns state_conflict, does not append,
        # does not ack Projects, and never infers a different action from
        # terminal state alone.
        race_project = projects_db.create_project(
            conn,
            name="C8 fingerprint race",
        )
        prdb.create_project_conversation(
            conn,
            project_id=race_project,
            conversation_id="c8-race-session",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="c8-race-owner",
            project_id=race_project,
            surface="desktop",
            external_binding_id="c8-race-window",
            actor_id="owner",
            now=1,
        )
        state.create_session("c8-race-session", source="cli")
        race_actor = ActorContext(
            "owner",
            "desktop",
            "c8-race-owner",
            True,
        )
        race_turn = runtime.enqueue_turn(
            race_project,
            {"message": "fingerprint race"},
            race_actor,
            idempotency_key="c8-fingerprint-race",
            expected_version=0,
        )
        race_claim = runtime.claim_next_turn(
            race_project,
            "c8-race-worker",
            lease_seconds=30,
        )
        assert race_claim is not None
        race_claim = runtime.mark_turn_started(race_claim)
        race_batch = "523e4567-e89b-42d3-a456-426614174000"
        state.prepare_terminal_result(
            race_claim,
            batch_id=race_batch,
            status="succeeded",
            base_message_count=0,
            messages=(
                {
                    "role": "user",
                    "content": "race",
                    "timestamp": 20.0,
                },
                {
                    "role": "assistant",
                    "content": "must not publish",
                    "timestamp": 21.0,
                },
            ),
        )
        runtime.commit_turn_with_task7_batch(
            race_claim,
            CanonicalTurnResult("succeeded", race_batch),
            transcript_batch_id=race_batch,
        )

        def race_projects_snapshot():
            return (
                tuple(
                    conn.execute(
                        """
                        SELECT * FROM project_runtime_state
                        WHERE project_id = ?
                        """,
                        (race_project,),
                    ).fetchone()
                ),
                tuple(
                    conn.execute(
                        """
                        SELECT * FROM project_turns
                        WHERE turn_id = ?
                        """,
                        (race_turn.turn_id,),
                    ).fetchone()
                ),
                tuple(
                    conn.execute(
                        """
                        SELECT * FROM project_run_controls
                        WHERE turn_id = ?
                        """,
                        (race_turn.turn_id,),
                    ).fetchone()
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_worker_leases
                        WHERE project_id = ?
                        """,
                        (race_project,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_events
                        WHERE project_id = ? ORDER BY sequence
                        """,
                        (race_project,),
                    )
                ),
            )

        race_projects_before = race_projects_snapshot()
        race_state_changes = state._conn.total_changes
        race_resolver_count = len(resolver_calls)
        race_projects_ack_count = len(projects_ack_calls)
        race_factory_count = len(factory_calls)
        fingerprint_race_batch[0] = race_batch
        state_conflict = await adapter.apply_project_batch(race_batch)
        assert state_conflict.outcome == "state_conflict"
        assert len(resolver_calls) == race_resolver_count + 1
        assert resolver_calls[-1] == race_batch
        assert len(projects_ack_calls) == race_projects_ack_count
        assert race_batch not in projects_ack_calls
        assert len(factory_calls) == race_factory_count + 1
        assert connection_is_closed(factory_calls[-1])
        assert race_projects_snapshot() == race_projects_before
        assert state._conn.total_changes == race_state_changes
        assert state.get_messages("c8-race-session") == []
        assert tuple(
            state._conn.execute(
                """
                SELECT message_count, tool_call_count
                FROM sessions WHERE id = 'c8-race-session'
                """
            ).fetchone()
        ) == (0, 0)
        assert fingerprint_race_decisions[-1].action == "publish"
        assert fingerprint_race_decisions[-1].discard_authority is None
        assert tuple(
            state._conn.execute(
                """
                SELECT state, transcript_json, transcript_sha256,
                       discard_authority, projects_acknowledged_at
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (race_batch,),
            ).fetchone()
        ) == fingerprint_race_after_mutation[0]
        assert fingerprint_race_after_mutation[0][0] == "published"
        assert resolver_instances == [bound_resolver]
        assert len(settlement_threads) == (
            len(resolver_calls) + len(projects_ack_calls)
        )
        assert len(factory_thread_ids) == len(settlement_threads)
        assert all(
            thread_id != loop_thread_id
            for _, thread_id in settlement_threads
        )
        assert all(
            thread_id != loop_thread_id
            for thread_id in factory_thread_ids
        )
    finally:
        conn.close()
        state.close()


@dataclass(frozen=True)
class _C14PolicySnapshot:
    project_id: str
    lifecycle: str
    current_phase: str
    roots: tuple[str, ...]
    approved_plan_ref: str
    contract_id: str
    contract_status: Literal["active"]
    contract_revision: int
    contract_json_sha256: str
    allowed_action_classes: frozenset[str]
    allowed_phases: frozenset[str]
    actor_id: str
    actor_surface: Literal["desktop", "discord"]
    binding_id: str
    actor_is_owner: bool
    control_version: int
    runtime_version: int


class _C14FacadeConnection:
    def __init__(self, factory):
        self.factory = factory
        self.connection_id = len(factory.connections)
        self.created_thread = threading.get_ident()
        self.sqlite = sqlite3.connect(":memory:")
        self.used_threads = []
        self.close_thread = None
        self.closed = False
        self.retained_transaction = None
        self.factory.protocol_trace.append(
            (
                "open",
                self.factory.label,
                self.connection_id,
                self.created_thread,
            )
        )

    def _transaction(
        self,
        name,
        args,
        kwargs,
        *,
        record_call,
        observe_gate,
    ):
        thread_id = threading.get_ident()
        transaction = (
            self.factory.label,
            self.connection_id,
            name,
            thread_id,
        )
        self.sqlite.execute("BEGIN")
        assert self.sqlite.in_transaction
        self.factory.active_transactions[self.factory.label].add(
            transaction
        )
        self.factory.protocol_trace.append(
            (
                "begin",
                self.factory.label,
                self.connection_id,
                thread_id,
                name,
            )
        )
        try:
            self.sqlite.execute("SELECT 1").fetchone()
            self.factory.protocol_trace.append(
                (
                    "use",
                    self.factory.label,
                    self.connection_id,
                    thread_id,
                    name,
                )
            )
            if record_call:
                self.factory.trace.append(
                    (
                        name,
                        self.connection_id,
                        thread_id,
                        args,
                        kwargs,
                    )
                )
            if observe_gate:
                entered = self.factory.entered.get(name)
                if entered is not None:
                    entered.set()
                release = self.factory.release.get(name)
                if release is not None:
                    assert release.wait(timeout=5)
                failure = self.factory.failures.pop(name, None)
                if failure is not None:
                    raise failure
            result = self.factory.results.get(
                name,
                (name, args, kwargs),
            )
            value = (
                result(*args, **kwargs)
                if callable(result)
                else result
            )
        except BaseException:
            self.sqlite.rollback()
            assert not self.sqlite.in_transaction
            self.factory.protocol_trace.append(
                (
                    "rollback",
                    self.factory.label,
                    self.connection_id,
                    thread_id,
                    name,
                )
            )
            raise
        else:
            self.sqlite.commit()
            assert not self.sqlite.in_transaction
            self.factory.protocol_trace.append(
                (
                    "commit",
                    self.factory.label,
                    self.connection_id,
                    thread_id,
                    name,
                )
            )
            return value
        finally:
            self.factory.active_transactions[self.factory.label].remove(
                transaction
            )

    def _invoke(self, name, *args, **kwargs):
        assert not self.closed
        thread_id = threading.get_ident()
        self.used_threads.append(thread_id)
        if (
            name == "apply_project_batch"
        ):
            authority_resolver = next(
                (
                    value
                    for value in (*args, *kwargs.values())
                    if callable(
                        getattr(
                            value,
                            "resolve_approval_checkpoint",
                            None,
                        )
                    )
                ),
                None,
            )
            assert authority_resolver is not None
            assert self.factory.authority_checkpoint is not None
            self._transaction(
                "apply_project_batch.state_prepare",
                args,
                kwargs,
                record_call=False,
                observe_gate=False,
            )
            assert not any(
                self.factory.active_transactions.values()
            )
            decision = authority_resolver.resolve_approval_checkpoint(
                self.factory.authority_checkpoint
            )
            assert decision.action == "publish"
            assert decision.discard_authority is None
            assert not any(
                self.factory.active_transactions.values()
            )
        return self._transaction(
            name,
            args,
            kwargs,
            record_call=True,
            observe_gate=True,
        )

    def _retain_policy_snapshot_transaction(
        self,
        name,
        args,
        kwargs,
    ):
        assert not self.closed
        assert self.retained_transaction is None
        thread_id = threading.get_ident()
        self.used_threads.append(thread_id)
        transaction = (
            self.factory.label,
            self.connection_id,
            name,
            thread_id,
        )
        self.sqlite.execute("BEGIN")
        assert self.sqlite.in_transaction
        self.factory.active_transactions[self.factory.label].add(
            transaction
        )
        self.factory.protocol_trace.append(
            (
                "begin",
                self.factory.label,
                self.connection_id,
                thread_id,
                name,
            )
        )
        try:
            self.sqlite.execute("SELECT 1").fetchone()
            self.factory.protocol_trace.append(
                (
                    "use",
                    self.factory.label,
                    self.connection_id,
                    thread_id,
                    name,
                )
            )
            self.factory.trace.append(
                (
                    name,
                    self.connection_id,
                    thread_id,
                    args,
                    kwargs,
                )
            )
            entered = self.factory.entered.get(name)
            if entered is not None:
                entered.set()
            release = self.factory.release.get(name)
            if release is not None:
                assert release.wait(timeout=5)
            failure = self.factory.failures.pop(name, None)
            if failure is not None:
                raise failure
        except BaseException:
            self.sqlite.rollback()
            assert not self.sqlite.in_transaction
            self.factory.active_transactions[
                self.factory.label
            ].remove(transaction)
            self.factory.protocol_trace.append(
                (
                    "rollback",
                    self.factory.label,
                    self.connection_id,
                    thread_id,
                    name,
                )
            )
            raise
        self.retained_transaction = transaction
        return self.factory.policy_snapshot

    def close(self):
        assert not self.closed
        if self.retained_transaction is not None:
            transaction = self.retained_transaction
            assert self.sqlite.in_transaction
            self.sqlite.commit()
            assert not self.sqlite.in_transaction
            self.factory.active_transactions[
                self.factory.label
            ].remove(transaction)
            self.factory.protocol_trace.append(
                (
                    "commit",
                    self.factory.label,
                    self.connection_id,
                    threading.get_ident(),
                    transaction[2],
                )
            )
            self.retained_transaction = None
        assert not self.sqlite.in_transaction
        self.close_thread = threading.get_ident()
        self.sqlite.close()
        self.closed = True
        self.factory.trace.append(
            ("close", self.connection_id, self.close_thread)
        )
        self.factory.protocol_trace.append(
            (
                "close",
                self.factory.label,
                self.connection_id,
                self.close_thread,
            )
        )

    def load_project_history(self, *args, **kwargs):
        return self._invoke("load_project_history", *args, **kwargs)

    def prepare_terminal_result(self, *args, **kwargs):
        return self._invoke("prepare_terminal_result", *args, **kwargs)

    def prepare_approval_checkpoint(self, *args, **kwargs):
        return self._invoke(
            "prepare_approval_checkpoint",
            *args,
            **kwargs,
        )

    def apply_project_batch(self, *args, **kwargs):
        return self._invoke("apply_project_batch", *args, **kwargs)

    def mark_turn_started(self, *args, **kwargs):
        return self._invoke("mark_turn_started", *args, **kwargs)

    def execution_input_for_claim(self, *args, **kwargs):
        return self._invoke(
            "execution_input_for_claim",
            *args,
            **kwargs,
        )

    def heartbeat_turn(self, *args, **kwargs):
        return self._invoke("heartbeat_turn", *args, **kwargs)

    def control_for_claim(self, *args, **kwargs):
        return self._invoke("control_for_claim", *args, **kwargs)

    def commit_turn_with_task7_batch(self, *args, **kwargs):
        return self._invoke(
            "commit_turn_with_task7_batch",
            *args,
            **kwargs,
        )

    def acknowledge_stopped(self, *args, **kwargs):
        return self._invoke("acknowledge_stopped", *args, **kwargs)

    def authorize_project_read(self, *args, **kwargs):
        return self._invoke("authorize_project_read", *args, **kwargs)

    def authorize_project_operation(self, *args, **kwargs):
        return self._invoke(
            "authorize_project_operation",
            *args,
            **kwargs,
        )

    def acquire_dispatcher_lease(self, *args, **kwargs):
        return self._invoke(
            "acquire_dispatcher_lease",
            *args,
            **kwargs,
        )

    def renew_dispatcher_lease(self, *args, **kwargs):
        return self._invoke(
            "renew_dispatcher_lease",
            *args,
            **kwargs,
        )

    def release_dispatcher_lease(self, *args, **kwargs):
        return self._invoke(
            "release_dispatcher_lease",
            *args,
            **kwargs,
        )

    def controls_for_live_starts(self, *args, **kwargs):
        return self._invoke(
            "controls_for_live_starts",
            *args,
            **kwargs,
        )

    def reconcile_inflight_turns_with_task7_evidence(
        self,
        *args,
        **kwargs,
    ):
        return self._invoke(
            "reconcile_inflight_turns_with_task7_evidence",
            *args,
            **kwargs,
        )

    def runnable_project_membership_upper_watermark(
        self,
        *args,
        **kwargs,
    ):
        return self._invoke(
            "runnable_project_membership_upper_watermark",
            *args,
            **kwargs,
        )

    def scan_runnable_projects(self, *args, **kwargs):
        return self._invoke(
            "scan_runnable_projects",
            *args,
            **kwargs,
        )

    def claim_next_turn_for_dispatcher(self, *args, **kwargs):
        return self._invoke(
            "claim_next_turn_for_dispatcher",
            *args,
            **kwargs,
        )

    def expire_due_operation_approvals(self, *args, **kwargs):
        return self._invoke(
            "expire_due_operation_approvals",
            *args,
            **kwargs,
        )

    def operation_recovery_membership_upper_watermark(
        self,
        *args,
        **kwargs,
    ):
        return self._invoke(
            "operation_recovery_membership_upper_watermark",
            *args,
            **kwargs,
        )

    def recover_pending_operations(self, *args, **kwargs):
        return self._invoke(
            "recover_pending_operations",
            *args,
            **kwargs,
        )

    def pending_project_batch_upper_watermark(
        self,
        *args,
        **kwargs,
    ):
        return self._invoke(
            "pending_project_batch_upper_watermark",
            *args,
            **kwargs,
        )

    def scan_pending_project_batches(self, *args, **kwargs):
        return self._invoke(
            "scan_pending_project_batches",
            *args,
            **kwargs,
        )

    def read_turn_with_evidence(self, *args, **kwargs):
        return self._invoke(
            "read_turn_with_evidence",
            *args,
            **kwargs,
        )

    def publication_state(self, *args, **kwargs):
        return self._invoke("publication_state", *args, **kwargs)

    def load_project_policy_snapshot(self, *args, **kwargs):
        return self._retain_policy_snapshot_transaction(
            "load_project_policy_snapshot",
            args,
            kwargs,
        )

    def resolve_project_batch_authority(self, *args, **kwargs):
        return self._invoke(
            "resolve_project_batch_authority",
            *args,
            **kwargs,
        )


class _C14FacadeFactory:
    def __init__(
        self,
        label,
        active_transactions,
        protocol_trace,
    ):
        self.label = label
        self.active_transactions = active_transactions
        self.protocol_trace = protocol_trace
        self.connections = []
        self.trace = []
        self.results = {}
        self.failures = {}
        self.entered = {}
        self.release = {}
        self.checkpoint_identity = None
        self.checkpoint_states = []
        self.readback_request = None
        self.readback_results = []
        self.authority_checkpoint = None
        self.policy_snapshot = _C14PolicySnapshot(
            "c14-project",
            "active",
            "implementation",
            ("c:/work",),
            "plan-7",
            "contract-c14",
            "active",
            7,
            "contract-sha256",
            frozenset({"read_only", "routine_effect"}),
            frozenset({"implementation"}),
            "owner-1",
            "desktop",
            "desktop-binding",
            True,
            3,
            5,
        )

    def __call__(self):
        connection = _C14FacadeConnection(self)
        self.connections.append(connection)
        return connection


class _C14RawProjectsConnection(sqlite3.Connection):
    def bind_factory(self, factory):
        self.factory = factory
        self.connection_id = len(factory.connections)
        self.created_thread = threading.get_ident()
        self.used_threads = []
        self.close_thread = None
        self.closed = False
        self.factory.protocol_trace.append(
            (
                "open",
                self.factory.label,
                self.connection_id,
                self.created_thread,
            )
        )

    def close(self):
        assert not self.closed
        assert not self.in_transaction
        self.close_thread = threading.get_ident()
        super().close()
        self.closed = True
        self.factory.trace.append(
            ("close", self.connection_id, self.close_thread)
        )
        self.factory.protocol_trace.append(
            (
                "close",
                self.factory.label,
                self.connection_id,
                self.close_thread,
            )
        )


class _C14RawFacadeFactory:
    def __init__(
        self,
        label,
        active_transactions,
        protocol_trace,
    ):
        self.label = label
        self.active_transactions = active_transactions
        self.protocol_trace = protocol_trace
        self.connections = []
        self.runtimes = []
        self.guards = []
        self.trace = []
        self.results = {}
        self.failures = {}
        self.entered = {}
        self.release = {}
        self.checkpoint_identity = None
        self.checkpoint_states = []
        self.readback_request = None
        self.readback_results = []

    def __call__(self):
        connection = sqlite3.connect(
            ":memory:",
            factory=_C14RawProjectsConnection,
        )
        connection.bind_factory(self)
        self.connections.append(connection)
        return connection

    def runtime_factory(self, connection):
        assert type(connection) is _C14RawProjectsConnection
        assert connection.factory is self
        assert not connection.closed
        thread_id = threading.get_ident()
        assert thread_id == connection.created_thread
        runtime = _C14RawFacadeRuntime(connection)
        self.runtimes.append(runtime)
        self.protocol_trace.append(
            (
                "runtime",
                self.label,
                connection.connection_id,
                thread_id,
            )
        )
        return runtime

    def operation_guard_factory(self, runtime):
        assert type(runtime) is _C14RawFacadeRuntime
        assert runtime.connection.factory is self
        thread_id = threading.get_ident()
        assert thread_id == runtime.connection.created_thread
        guard = _C14RawFacadeOperationGuard(runtime)
        self.guards.append(guard)
        self.protocol_trace.append(
            (
                "guard",
                self.label,
                runtime.connection.connection_id,
                thread_id,
            )
        )
        return guard


class _C14RawFacadeRuntime:
    def __init__(self, connection):
        self.connection = connection


class _C14RawFacadeOperationGuard:
    def __init__(self, runtime):
        self.runtime = runtime
        self.connection = runtime.connection
        self.factory = self.connection.factory

    def _transition(self, name, args, kwargs):
        connection = self.connection
        assert not connection.closed
        thread_id = threading.get_ident()
        assert thread_id == connection.created_thread
        connection.used_threads.append(thread_id)
        transaction = (
            self.factory.label,
            connection.connection_id,
            name,
            thread_id,
        )
        connection.execute("BEGIN")
        assert connection.in_transaction
        self.factory.active_transactions[self.factory.label].add(
            transaction
        )
        self.factory.protocol_trace.append(
            (
                "begin",
                self.factory.label,
                connection.connection_id,
                thread_id,
                name,
            )
        )
        try:
            connection.execute("SELECT 1").fetchone()
            self.factory.protocol_trace.append(
                (
                    "use",
                    self.factory.label,
                    connection.connection_id,
                    thread_id,
                    name,
                )
            )
            self.factory.trace.append(
                (
                    name,
                    connection.connection_id,
                    thread_id,
                    args,
                    kwargs,
                )
            )
            entered = self.factory.entered.get(name)
            if entered is not None:
                entered.set()
            release = self.factory.release.get(name)
            if release is not None:
                assert release.wait(timeout=5)
            failure = self.factory.failures.pop(name, None)
            if failure is not None:
                raise failure
            result = self.factory.results.get(
                name,
                (name, args, kwargs),
            )
            value = (
                result(*args, **kwargs)
                if callable(result)
                else result
            )
        except BaseException:
            connection.rollback()
            assert not connection.in_transaction
            self.factory.protocol_trace.append(
                (
                    "rollback",
                    self.factory.label,
                    connection.connection_id,
                    thread_id,
                    name,
                )
            )
            raise
        else:
            connection.commit()
            assert not connection.in_transaction
            self.factory.protocol_trace.append(
                (
                    "commit",
                    self.factory.label,
                    connection.connection_id,
                    thread_id,
                    name,
                )
            )
            return value
        finally:
            self.factory.active_transactions[
                self.factory.label
            ].remove(transaction)

    def prepare(self, *args, **kwargs):
        return self._transition("prepare", args, kwargs)

    def certified_execution_request(self, *args, **kwargs):
        return self._transition(
            "certified_execution_request",
            args,
            kwargs,
        )

    def mark_started(self, *args, **kwargs):
        checkpoint = next(
            (
                value
                for value in (*args, *kwargs.values())
                if callable(getattr(value, "publication_state", None))
            ),
            None,
        )
        if (
            checkpoint is not None
            and self.factory.checkpoint_identity is not None
        ):
            self.factory.checkpoint_states.append(
                checkpoint.publication_state(
                    self.factory.checkpoint_identity
                )
            )
        return self._transition("mark_started", args, kwargs)

    def record_receipt(self, *args, **kwargs):
        return self._transition("record_receipt", args, kwargs)

    def reconcile(self, *args, **kwargs):
        readback = next(
            (
                value
                for value in (*args, *kwargs.values())
                if callable(getattr(value, "read_operation", None))
            ),
            None,
        )
        if readback is not None and self.factory.readback_request is not None:
            self.factory.readback_results.append(
                readback.read_operation(self.factory.readback_request)
            )
        return self._transition("reconcile", args, kwargs)


@pytest.mark.asyncio
async def test_task7_c14_composition_facades_are_narrow_and_connections_are_executor_owned(
    tmp_path,
    monkeypatch,
):
    """Every C14 facade exposes one typed capability and owns one connection."""
    import hashlib

    from gateway import project_runtime_dispatcher as dispatcher_module
    from gateway import project_runtime_worker as worker_module
    from gateway import session as session_module
    from hermes_cli import project_runtime_db as prdb
    from hermes_cli import projects_db
    from hermes_cli.project_operations import (
        ApprovalCheckpointIdentity,
        OperationIntent,
        OperationReadbackRequest,
        OperationReadbackResult,
        OperationReceipt,
        OperationRecoveryScanResult,
        ProjectOperation,
        ProjectOperationError,
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
    )
    from hermes_cli.project_runtime import (
        CanonicalTurnResult,
        ClaimControl,
        DispatcherLease,
        ProjectTurn,
        ProjectRuntime,
        RunControl,
        RunnableProjectScanResult,
        Task7TerminalReadbackEvidence,
        TurnAttemptIdentity,
        TurnClaim,
        TurnExecutionInput,
        TurnOrigin,
        TurnReadbackRequest,
        TurnReadbackResult,
        WorkerStart,
    )
    from hermes_state import PendingProjectBatch, ProjectBatchCursor
    from tests.gateway.project_runtime_test_helpers import (
        RetainedThreadRunner,
    )

    @dataclass(frozen=True)
    class FixedUpperPendingProjectBatchPage:
        members: tuple[PendingProjectBatch, ...]
        scanned_through: ProjectBatchCursor | None
        reached_epoch_end: bool

    required = {
        session_module: (
            "ProjectBatchWorkerFacade",
            "ProjectBatchSettlementFacade",
            "ProjectTask7TerminalReadbackFacade",
            "ProjectApprovalCheckpointReadFacade",
        ),
        worker_module: (
            "ProjectRuntimeWorkerFacade",
            "ProjectToolPolicySnapshotFacade",
            "ProjectOperationPrepareFacade",
            "ProjectOperationExecutionFacade",
            "CanonicalProjectOperationExecutionCoordinator",
            "CanonicalApprovedOperationTurn",
            "ApprovedOperationExecutionPort",
        ),
        dispatcher_module: (
            "ProjectDispatcherRuntimeFacade",
            "ProjectDispatcherOperationFacade",
        ),
    }
    assert all(
        hasattr(module, name)
        for module, names in required.items()
        for name in names
    ), "C14 requires strict factory-backed State/Projects facades"
    prepare_constructor = inspect.signature(
        worker_module.ProjectOperationPrepareFacade
    )
    execution_constructor = inspect.signature(
        worker_module.ProjectOperationExecutionFacade
    )
    assert tuple(prepare_constructor.parameters) == (
        "projects_db_factory",
        "io_runner",
        "runtime_factory",
        "operation_guard_factory",
    )
    assert tuple(execution_constructor.parameters) == (
        "projects_db_factory",
        "approval_checkpoints",
        "io_runner",
        "runtime_factory",
        "operation_guard_factory",
    )
    for constructor in (
        prepare_constructor,
        execution_constructor,
    ):
        parameters = tuple(constructor.parameters.values())
        assert parameters[0].kind is (
            inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        assert all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            for parameter in parameters[1:]
        )
        assert constructor.parameters[
            "runtime_factory"
        ].default is ProjectRuntime
        assert constructor.parameters[
            "operation_guard_factory"
        ].default is ProjectOperationGuard
    operation_transitions = (
        "prepare",
        "certified_execution_request",
        "mark_started",
        "record_receipt",
        "reconcile",
    )
    assert issubclass(
        _C14RawProjectsConnection,
        sqlite3.Connection,
    )
    assert all(
        not hasattr(seam, transition)
        for seam in (
            _C14FacadeConnection,
            _C14RawProjectsConnection,
            _C14RawFacadeFactory,
            _C14RawFacadeRuntime,
        )
        for transition in operation_transitions
    )
    assert all(
        callable(
            getattr(_C14RawFacadeOperationGuard, transition, None)
        )
        for transition in operation_transitions
    )

    loop_thread = threading.get_ident()
    io_runner = RetainedThreadRunner(
        "c14-project-io",
        max_workers=2,
    )
    effect_runner = RetainedThreadRunner(
        "c14-project-effect",
        max_workers=1,
    )
    agent_runner = RetainedThreadRunner(
        "c14-project-agent",
        max_workers=1,
    )
    protocol_trace = []
    active_transactions = {
        label: set()
        for label in (
            "state",
            "projects-authority",
            "runtime",
            "policy",
            "prepare",
            "execution",
            "dispatcher-operation",
        )
    }
    projects_factory = _C14FacadeFactory(
        "projects-authority",
        active_transactions,
        protocol_trace,
    )
    state_factory = _C14FacadeFactory(
        "state",
        active_transactions,
        protocol_trace,
    )
    runtime_factory = _C14FacadeFactory(
        "runtime",
        active_transactions,
        protocol_trace,
    )
    policy_factory = _C14FacadeFactory(
        "policy",
        active_transactions,
        protocol_trace,
    )
    prepare_factory = _C14RawFacadeFactory(
        "prepare",
        active_transactions,
        protocol_trace,
    )
    execution_factory = _C14RawFacadeFactory(
        "execution",
        active_transactions,
        protocol_trace,
    )
    operation_factory = _C14FacadeFactory(
        "dispatcher-operation",
        active_transactions,
        protocol_trace,
    )
    resolver_constructions = []
    resolver_calls = []

    class InstrumentedProjectBatchAuthorityResolver:
        def __init__(self, projects_db_factory):
            assert projects_db_factory is projects_factory
            self.projects_db_factory = projects_db_factory
            resolver_constructions.append(
                (self, projects_db_factory, threading.get_ident())
            )

        def resolve_approval_checkpoint(self, checkpoint):
            assert active_transactions["state"] == set()
            connection = self.projects_db_factory()
            try:
                connection.resolve_project_batch_authority(
                    checkpoint
                )
                resolver_calls.append(
                    (
                        self,
                        checkpoint,
                        threading.get_ident(),
                        connection,
                    )
                )
                return session_module.ProjectBatchAuthorityDecision(
                    "publish",
                    None,
                )
            finally:
                connection.close()
                assert active_transactions["projects-authority"] == set()

    monkeypatch.setattr(
        session_module,
        "ProjectBatchAuthorityResolver",
        InstrumentedProjectBatchAuthorityResolver,
    )
    binder_trace = []

    def observe_policy_transaction(stage):
        thread_id = threading.get_ident()
        assert thread_id != loop_thread
        transactions = active_transactions["policy"]
        assert len(transactions) == 1
        transaction = next(iter(transactions))
        assert transaction == (
            "policy",
            transaction[1],
            "load_project_policy_snapshot",
            thread_id,
        )
        protocol_trace.append(
            (
                stage,
                "policy",
                transaction[1],
                thread_id,
                "load_project_policy_snapshot",
            )
        )

    def bind_read(snapshot, execution, proposal):
        observe_policy_transaction("bind_read")
        binder_trace.append(("read", snapshot, execution, proposal))
        return ProjectCommand(
            proposal.canonical_action,
            execution.attempt.project_id,
            execution.contract_revision,
            "read_only",
            proposal.targets,
            proposal.policy_batch_id,
            proposal.batch_items,
            {"phase": snapshot.current_phase},
        )

    def bind_operation(snapshot, execution, proposal):
        observe_policy_transaction("bind_operation")
        binder_trace.append(("operation", snapshot, execution, proposal))
        command = ProjectCommand(
            proposal.intent.canonical_action,
            execution.attempt.project_id,
            execution.contract_revision,
            "routine_effect",
            proposal.intent.targets,
            proposal.policy_batch_id,
            proposal.intent.batch_items,
            {"phase": snapshot.current_phase},
        )
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
                "operation_id": proposal.intent.operation_id,
                "project_id": proposal.intent.project_id,
                "turn_id": proposal.intent.turn_id,
                "idempotency_key": proposal.intent.idempotency_key,
                "canonical_action": proposal.intent.canonical_action,
                "command_revision": proposal.intent.command_revision,
                "targets": list(proposal.intent.targets),
                "batch_items": list(proposal.intent.batch_items),
                "payload": dict(proposal.intent.payload),
                "readback_kind": proposal.intent.readback_kind,
                "remote_idempotency_supported": (
                    proposal.intent.remote_idempotency_supported
                ),
            },
            "policy_batch_id": proposal.policy_batch_id,
            "capability_fingerprint": list(
                proposal.capability_fingerprint
            ),
            "effect_scope": json.loads(proposal.effect_scope_json),
        }
        authority_json = json.dumps(
            authority_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        return worker_module.BoundProjectOperationAuthority(
            command=command,
            intent=proposal.intent,
            policy_batch_id=proposal.policy_batch_id,
            effect_scope_json=proposal.effect_scope_json,
            effect_scope_sha256=proposal.effect_scope_sha256,
            authority_json=authority_json,
            authority_sha256=hashlib.sha256(
                authority_json.encode("utf-8")
            ).hexdigest(),
        )

    def decide(command, project, contract, actor):
        observe_policy_transaction("decide")
        assert command.project_id == project.project_id
        assert command.revision == contract.revision
        assert actor.actor_id == "owner-1"
        return PolicyDecision(
            Decision.ALLOW,
            "c14-allow",
            "bound project operation",
        )

    @dataclass(frozen=True)
    class MaterializedPolicySnapshot:
        project: ProjectPolicyView
        contract_id: str
        contract_status: Literal["active"]
        contract_json_sha256: str
        contract: ContractPolicyView
        actor: ActorContext
        control_version: int
        runtime_version: int

    def materialize_snapshot(snapshot):
        return MaterializedPolicySnapshot(
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

    effect_entered = threading.Event()
    effect_release = threading.Event()
    effect_release.set()
    projects_transaction_labels = tuple(
        label
        for label in active_transactions
        if label != "state"
    )

    class ExactCapability:
        fingerprint = ("local_code_edit", 1, "remote-ledger", True)

        def __init__(self):
            self.effect_calls = []
            self.readback_calls = []
            self.effect_failure = None

        def execute(self, request, idempotency_key):
            assert type(request) is (
                worker_module.CertifiedProjectOperationExecutionRequest
            )
            assert idempotency_key == "c14-idempotency"
            transaction_snapshot = {
                label: tuple(transactions)
                for label, transactions in active_transactions.items()
            }
            assert transaction_snapshot["state"] == ()
            assert all(
                transaction_snapshot[label] == ()
                for label in projects_transaction_labels
            )
            effect_entered.set()
            assert effect_release.wait(timeout=5)
            self.effect_calls.append(
                (
                    request,
                    idempotency_key,
                    threading.get_ident(),
                    transaction_snapshot,
                )
            )
            if self.effect_failure is not None:
                failure = self.effect_failure
                self.effect_failure = None
                raise failure
            return OperationReceipt(
                "c14-receipt",
                {"remote_id": "c14-effect"},
            )

        def read_operation(self, request):
            assert type(request) is OperationReadbackRequest
            transaction_snapshot = {
                label: tuple(transactions)
                for label, transactions in active_transactions.items()
            }
            assert transaction_snapshot["state"] == ()
            assert all(
                transaction_snapshot[label] == ()
                for label in projects_transaction_labels
            )
            self.readback_calls.append(
                (
                    request,
                    threading.get_ident(),
                    transaction_snapshot,
                )
            )
            return OperationReadbackResult(
                "applied",
                {"remote_id": "c14-effect"},
                OperationReceipt(
                    "c14-receipt",
                    {"remote_id": "c14-effect"},
                ),
            )

    capability = ExactCapability()
    capabilities = MappingProxyType(
        {capability.fingerprint: capability}
    )
    terminal_readback = (
        session_module.ProjectTask7TerminalReadbackFacade(
            state_factory,
            io_runner=io_runner,
        )
    )
    checkpoints = session_module.ProjectApprovalCheckpointReadFacade(
        state_factory,
        io_runner=io_runner,
    )

    facades = {
        "state": session_module.ProjectBatchWorkerFacade(
            state_factory,
            projects_db_factory=projects_factory,
            io_runner=io_runner,
        ),
        "worker": worker_module.ProjectRuntimeWorkerFacade(
            runtime_factory,
            io_runner=io_runner,
        ),
        "policy": worker_module.ProjectToolPolicySnapshotFacade(
            policy_factory,
            read_binder=bind_read,
            operation_binder=bind_operation,
            capability_registry=capabilities,
            policy_decider=decide,
            snapshot_materializer=materialize_snapshot,
            authority_clock=lambda: 100,
            approval_id_factory=lambda: (
                "123e4567-e89b-42d3-a456-426614174000"
            ),
            io_runner=io_runner,
        ),
        "prepare": worker_module.ProjectOperationPrepareFacade(
            prepare_factory,
            io_runner=io_runner,
            runtime_factory=prepare_factory.runtime_factory,
            operation_guard_factory=(
                prepare_factory.operation_guard_factory
            ),
        ),
        "execution": worker_module.ProjectOperationExecutionFacade(
            execution_factory,
            approval_checkpoints=checkpoints,
            io_runner=io_runner,
            runtime_factory=execution_factory.runtime_factory,
            operation_guard_factory=(
                execution_factory.operation_guard_factory
            ),
        ),
        "dispatcher_runtime": (
            dispatcher_module.ProjectDispatcherRuntimeFacade(
                runtime_factory,
                terminal_readback=terminal_readback,
                io_runner=io_runner,
            )
        ),
        "dispatcher_operation": (
            dispatcher_module.ProjectDispatcherOperationFacade(
                operation_factory,
                approval_checkpoints=checkpoints,
                executor_capabilities=capabilities,
                io_runner=io_runner,
            )
        ),
        "settlement": session_module.ProjectBatchSettlementFacade(
            state_factory,
            projects_db_factory=projects_factory,
            io_runner=io_runner,
        ),
        "terminal_readback": (
            terminal_readback
        ),
        "checkpoint_read": (
            checkpoints
        ),
    }
    class StrictApprovedOperationExecutionPort:
        def __init__(self):
            self.calls = []
            self.call_threads = []

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
            self.call_threads.append(threading.get_ident())
            coordinator = (
                worker_module.CanonicalProjectOperationExecutionCoordinator(
                    execution_facade=facades["execution"],
                    capability_registry=capabilities,
                    effect_runner=effect_runner,
                )
            )
            return worker_module.CanonicalApprovedOperationTurn(
                execution=execution_value,
                operation=operation,
                base_message_count=base_message_count,
                coordinator=coordinator,
            )

    approved_port = StrictApprovedOperationExecutionPort()
    facades["approved"] = approved_port

    positives = {
        "state": (
            "load_project_history",
            "prepare_terminal_result",
            "prepare_approval_checkpoint",
            "apply_project_batch",
        ),
        "worker": (
            "mark_turn_started",
            "execution_input_for_claim",
            "heartbeat_turn",
            "control_for_claim",
            "commit_turn_with_task7_batch",
            "acknowledge_stopped",
        ),
        "policy": (
            "authorize_project_read",
            "authorize_project_operation",
        ),
        "prepare": ("prepare",),
        "execution": (
            "certified_execution_request",
            "mark_started",
            "record_receipt",
            "reconcile",
        ),
        "dispatcher_runtime": (
            "acquire_dispatcher_lease",
            "renew_dispatcher_lease",
            "release_dispatcher_lease",
            "controls_for_live_starts",
            "reconcile_inflight_turns_with_task7_evidence",
            "runnable_project_membership_upper_watermark",
            "scan_runnable_projects",
            "claim_next_turn_for_dispatcher",
        ),
        "dispatcher_operation": (
            "expire_due_operation_approvals",
            "operation_recovery_membership_upper_watermark",
            "recover_pending_operations",
        ),
        "settlement": (
            "pending_project_batch_upper_watermark",
            "scan_pending_project_batches",
            "apply_project_batch",
        ),
        "terminal_readback": ("read_turn_with_evidence",),
        "checkpoint_read": ("publication_state",),
        "approved": ("create_turn",),
    }
    forbidden = {
        "state": (
            "connection",
            "session_db",
            "resolver",
            "factory",
            "publish",
            "discard",
            "execute",
            "append_message",
            "create_session",
            "update_session",
            "delete_session",
            "resolve_prepared_terminal",
            "record_terminal_transcript_conflict",
        ),
        "worker": (
            "connection",
            "runtime",
            "factory",
            "acquire_dispatcher_lease",
            "scan_runnable_projects",
            "claim_next_turn_for_dispatcher",
            "recover_pending_operations",
            "enqueue_turn",
            "claim_next_turn",
            "dispatch_once",
            "reconcile_inflight_turns_with_task7_evidence",
            "raw_runtime",
        ),
        "policy": (
            "connection",
            "contract_json",
            "prepare",
            "approve",
            "mutate",
            "query",
            "authorize_project_command",
            "approval_mutator",
            "runtime",
            "load_contract_json",
        ),
        "prepare": (
            "connection",
            "guard",
            "mark_started",
            "recover_pending_operations",
            "approve",
            "runtime",
            "raw_operation",
            "record_receipt",
            "reconcile",
            "dispatcher_lease",
            "effect_adapter",
        ),
        "execution": (
            "connection",
            "guard",
            "prepare",
            "recover_pending_operations",
            "approval_mutator",
            "dispatcher_lease",
            "effect_adapter",
            "runtime",
            "raw_operation",
            "approve",
            "scan_operation_recovery_members",
        ),
        "dispatcher_runtime": (
            "connection",
            "runtime",
            "mark_turn_started",
            "commit_turn_with_task7_batch",
            "run_project",
            "prepare",
            "mark_started",
            "record_receipt",
            "reconcile",
            "apply_project_batch",
            "session_db",
        ),
        "dispatcher_operation": (
            "connection",
            "guard",
            "mark_started",
            "record_receipt",
            "reconcile",
            "runtime",
            "claim_next_turn_for_dispatcher",
            "apply_project_batch",
            "session_db",
        ),
        "settlement": (
            "connection",
            "session_db",
            "publish",
            "discard",
            "run_start",
            "resolve_project_agent",
            "create_turn",
            "worker",
            "reserve_worker",
        ),
        "terminal_readback": (
            "connection",
            "append_message",
            "publish",
            "apply_project_batch",
            "prepare_terminal_result",
            "session_db",
        ),
        "checkpoint_read": (
            "connection",
            "apply_project_batch",
            "publish",
            "prepare_approval_checkpoint",
            "append_message",
            "session_db",
        ),
        "approved": (
            "resolve_adapter",
            "execute_effect",
            "invoke_effect",
            "connection",
            "guard",
            "factory",
            "runtime",
            "session_db",
            "run_conversation",
            "resolve_project_agent",
        ),
    }
    for label, facade in facades.items():
        assert all(
            callable(getattr(facade, name, None))
            for name in positives[label]
        )
        assert all(
            not hasattr(facade, name)
            for name in forbidden[label]
        )

    attempt = TurnAttemptIdentity(
        "c14-project",
        "c14-turn",
        1,
        "c14-worker",
        "c14-attempt",
        2,
        3,
        "c14-session",
        190,
    )
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
    execution = TurnExecutionInput(
        attempt,
        {"path": "c:/work/file.py", "content": "exact"},
        TurnOrigin(
            "desktop-binding",
            "desktop",
            "desktop-window",
            "owner-1",
        ),
        7,
    )
    lease = DispatcherLease(
        "11111111-1111-4111-8111-111111111111",
        1,
        1,
        200,
    )
    intent = OperationIntent(
        "operation-c14",
        "c14-project",
        "c14-turn",
        "c14-idempotency",
        "local_code_edit",
        1,
        ("c:/work/file.py",),
        ("write",),
        {"path": "c:/work/file.py", "content": "exact"},
        "remote-ledger",
        True,
    )
    read_proposal = worker_module.ProjectReadProposal(
        "read.project_status",
        ("c:/work",),
        None,
        (),
    )
    effect_scope_json = (
        '{"batch_items":["write"],'
        '"targets":["c:/work/file.py"]}'
    )
    operation_proposal = worker_module.ProjectOperationProposal(
        intent,
        None,
        effect_scope_json,
        hashlib.sha256(effect_scope_json.encode("utf-8")).hexdigest(),
        ("local_code_edit", 1, "remote-ledger", True),
    )
    actor = ActorContext(
        "owner-1",
        "desktop",
        "desktop-binding",
        True,
    )
    allow = PolicyDecision(
        Decision.ALLOW,
        "c14-allow",
        "bound project operation",
    )
    authority_flow = {}
    prepare_observations = []
    certified_observations = []

    def operation_with_status(status, *, receipt_id=None):
        return ProjectOperation(
            "operation-c14",
            "c14-project",
            "c14-turn",
            "c14-idempotency",
            "local_code_edit",
            1,
            ("c:/work/file.py",),
            ("write",),
            status,
            None,
            "remote-ledger",
            receipt_id,
            None,
            attempt.attempt_id,
            attempt.lease_generation,
            attempt.fencing_token,
            100,
            101,
        )

    approved_operation = operation_with_status("approved")
    effect_started_operation = operation_with_status("effect_started")
    receipt_recorded_operation = operation_with_status(
        "receipt_recorded",
        receipt_id="c14-receipt",
    )
    reconciled_operation = operation_with_status(
        "reconciled",
        receipt_id="c14-receipt",
    )
    checkpoint_identity = ApprovalCheckpointIdentity(
        "323e4567-e89b-42d3-a456-426614174000",
        attempt,
        intent.operation_id,
        "423e4567-e89b-42d3-a456-426614174000",
    )
    readback_request = OperationReadbackRequest(
        intent.operation_id,
        intent.project_id,
        intent.turn_id,
        intent.canonical_action,
        intent.targets,
        intent.batch_items,
        intent.idempotency_key,
        "remote-ledger",
        OperationReceipt(
            "c14-receipt",
            {"remote_id": "c14-effect"},
        ),
        attempt.attempt_id,
        attempt.lease_generation,
        attempt.fencing_token,
    )
    settlement_after = ProjectBatchCursor(
        1,
        "223e4567-e89b-42d3-a456-426614174000",
    )
    settlement_upper = ProjectBatchCursor(
        2,
        "723e4567-e89b-42d3-a456-426614174000",
    )
    settlement_member = PendingProjectBatch(
        settlement_upper.batch_id,
        settlement_upper.batch_creation_sequence,
        "terminal_result",
        "prepared",
        attempt,
        "succeeded",
        None,
        None,
        0,
        101.0,
    )
    settlement_page = FixedUpperPendingProjectBatchPage(
        (settlement_member,),
        settlement_upper,
        True,
    )

    def canonical_policy_value(value):
        value_type = type(value)
        if value is None or value_type in {str, int, float, bool}:
            return value
        if value_type is Decision:
            return value.value
        if isinstance(value, Mapping):
            return {
                key: canonical_policy_value(value[key])
                for key in sorted(value)
            }
        if value_type in {tuple, list}:
            return [canonical_policy_value(item) for item in value]
        if value_type in {set, frozenset}:
            return sorted(
                canonical_policy_value(item) for item in value
            )
        return {
            field.name: canonical_policy_value(
                getattr(value, field.name)
            )
            for field in fields(value)
        }

    def prepare_from_authorized(
        claim_value,
        intent_value,
        *,
        authority,
        policy,
        policy_authority,
        approval,
    ):
        carrier = authority_flow["authorized_carrier"]
        assert type(carrier) is (
            worker_module.ProjectPolicyDecisionCarrier
        )
        assert claim_value is claim
        assert policy_authority == carrier
        assert authority == carrier.operation_authority
        assert intent_value == authority.intent
        assert policy == carrier.decision
        assert approval is None
        assert hashlib.sha256(
            authority.authority_json.encode("utf-8")
        ).hexdigest() == authority.authority_sha256
        assert hashlib.sha256(
            authority.effect_scope_json.encode("utf-8")
        ).hexdigest() == authority.effect_scope_sha256
        policy_authority_json = json.dumps(
            canonical_policy_value(carrier),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        policy_authority_sha256 = hashlib.sha256(
            policy_authority_json.encode("utf-8")
        ).hexdigest()
        authority_flow.update(
            {
                "prepared_carrier": carrier,
                "prepared_authority": authority,
                "operation_authority_json": authority.authority_json,
                "operation_authority_sha256": (
                    authority.authority_sha256
                ),
                "effect_scope_json": authority.effect_scope_json,
                "effect_scope_sha256": authority.effect_scope_sha256,
                "policy_authority_json": policy_authority_json,
                "policy_authority_sha256": policy_authority_sha256,
            }
        )
        for operation in (
            approved_operation,
            effect_started_operation,
            receipt_recorded_operation,
            reconciled_operation,
        ):
            object.__setattr__(
                operation,
                "policy_authority_sha256",
                policy_authority_sha256,
            )
        prepare_observations.append(
            (
                carrier,
                authority,
                authority.authority_json,
                authority.authority_sha256,
                authority.effect_scope_json,
                authority.effect_scope_sha256,
                policy_authority_json,
                policy_authority_sha256,
            )
        )
        return approved_operation

    def certified_from_authorized(execution_value, operation_value):
        carrier = authority_flow["prepared_carrier"]
        authority = authority_flow["prepared_authority"]
        assert carrier is authority_flow["authorized_carrier"]
        assert authority is carrier.operation_authority
        assert execution_value is execution
        assert operation_value is authority_flow["prepared_operation"]
        request = (
            worker_module.CertifiedProjectOperationExecutionRequest(
                operation=operation_value,
                attempt=execution_value.attempt,
                payload=authority.intent.payload,
                approval_checkpoint_id=(
                    operation_value.approval_checkpoint_id
                ),
                operation_authority_json=authority_flow[
                    "operation_authority_json"
                ],
                operation_authority_sha256=authority_flow[
                    "operation_authority_sha256"
                ],
                effect_scope_json=authority_flow[
                    "effect_scope_json"
                ],
                effect_scope_sha256=authority_flow[
                    "effect_scope_sha256"
                ],
                policy_authority_sha256=authority_flow[
                    "policy_authority_sha256"
                ],
                remote_idempotency_supported=(
                    authority.intent.remote_idempotency_supported
                ),
                capability_fingerprint=capability.fingerprint,
            )
        )
        certified_observations.append((carrier, authority, request))
        return request

    class ExactReadbackPort:
        def read_operation(self, request):
            assert type(request) is OperationReadbackRequest
            assert not any(active_transactions.values())
            return OperationReadbackResult(
                "applied",
                {"remote_id": "c14-surface-read"},
                request.receipt,
            )

    surface_readback = ExactReadbackPort()
    execution_factory.checkpoint_identity = checkpoint_identity
    execution_factory.readback_request = readback_request
    state_factory.authority_checkpoint = checkpoint_identity

    def prepared_batch(*_args, **kwargs):
        approval_id = kwargs.get("approval_id")
        return PendingProjectBatch(
            kwargs["batch_id"],
            1,
            (
                "approval_checkpoint"
                if approval_id is not None
                else "terminal_result"
            ),
            "prepared",
            attempt,
            None if approval_id is not None else kwargs["status"],
            kwargs.get("operation_id"),
            approval_id,
            kwargs["base_message_count"],
            100.0,
        )

    state_factory.results.update(
        {
            "load_project_history": session_module.ProjectHistorySnapshot(
                "c14-session",
                (),
                0,
            ),
            "prepare_terminal_result": prepared_batch,
            "prepare_approval_checkpoint": prepared_batch,
            "apply_project_batch": session_module.ProjectBatchApplyResult(
                "published"
            ),
            "read_turn_with_evidence": Task7TerminalReadbackEvidence(
                TurnReadbackResult("succeeded", "c14-result"),
                "223e4567-e89b-42d3-a456-426614174000",
            ),
            "publication_state": "published",
            "pending_project_batch_upper_watermark": settlement_upper,
            "scan_pending_project_batches": settlement_page,
        }
    )
    runtime_factory.results.update(
        {
            "mark_turn_started": claim,
            "execution_input_for_claim": execution,
            "heartbeat_turn": claim,
            "control_for_claim": ClaimControl("running", 3, 190),
            "commit_turn_with_task7_batch": ProjectTurn(
                "c14-turn",
                "c14-project",
                1,
                "c14-idempotency",
                {"path": "c:/work/file.py", "content": "exact"},
                "desktop-binding",
                "succeeded",
                attempt.attempt_id,
                attempt.lease_generation,
                attempt.fencing_token,
                100,
                101,
            ),
            "acknowledge_stopped": RunControl(
                "c14-turn",
                "c14-project",
                "stopped",
                4,
                "c14-idempotency",
                attempt.attempt_id,
                101,
            ),
            "acquire_dispatcher_lease": lease,
            "renew_dispatcher_lease": lease,
            "release_dispatcher_lease": True,
            "controls_for_live_starts": (),
            "reconcile_inflight_turns_with_task7_evidence": (),
            "runnable_project_membership_upper_watermark": 1,
            "scan_runnable_projects": RunnableProjectScanResult(
                (),
                None,
                True,
            ),
            "claim_next_turn_for_dispatcher": None,
        }
    )
    prepare_factory.results["prepare"] = prepare_from_authorized
    execution_factory.results.update(
        {
            "certified_execution_request": certified_from_authorized,
            "mark_started": effect_started_operation,
            "record_receipt": receipt_recorded_operation,
            "reconcile": reconciled_operation,
        }
    )
    operation_factory.results.update(
        {
            "expire_due_operation_approvals": (),
            "operation_recovery_membership_upper_watermark": 1,
            "recover_pending_operations": OperationRecoveryScanResult(
                (),
                None,
                True,
            ),
        }
    )

    # Call every positive surface once. Each facade must create/use/close one
    # distinct connection wholly on one non-owner project-I/O thread.
    calls = (
        ("state", "load_project_history", ("c14-session",), {}),
        (
            "state",
            "prepare_terminal_result",
            (claim,),
            {
                "batch_id": "223e4567-e89b-42d3-a456-426614174000",
                "status": "succeeded",
                "base_message_count": 0,
                "messages": (),
            },
        ),
        (
            "state",
            "prepare_approval_checkpoint",
            (claim,),
            {
                "batch_id": "323e4567-e89b-42d3-a456-426614174000",
                "operation_id": "operation-c14",
                "approval_id": "423e4567-e89b-42d3-a456-426614174000",
                "base_message_count": 0,
                "messages": (),
            },
        ),
        (
            "state",
            "apply_project_batch",
            ("223e4567-e89b-42d3-a456-426614174000",),
            {},
        ),
        ("worker", "mark_turn_started", (claim,), {}),
        ("worker", "execution_input_for_claim", (claim,), {}),
        (
            "worker",
            "heartbeat_turn",
            (claim,),
            {"lease_seconds": 90},
        ),
        ("worker", "control_for_claim", (claim,), {}),
        (
            "worker",
            "commit_turn_with_task7_batch",
            (claim, CanonicalTurnResult("succeeded", "c14-result")),
            {
                "transcript_batch_id": (
                    "223e4567-e89b-42d3-a456-426614174000"
                )
            },
        ),
        ("worker", "acknowledge_stopped", (claim,), {}),
        (
            "policy",
            "authorize_project_read",
            (execution, read_proposal),
            {},
        ),
        (
            "policy",
            "authorize_project_operation",
            (execution, operation_proposal),
            {},
        ),
        (
            "prepare",
            "prepare",
            (claim, intent),
            {},
        ),
        (
            "execution",
            "certified_execution_request",
            (execution, approved_operation),
            {},
        ),
        (
            "execution",
            "mark_started",
            (),
            {},
        ),
        (
            "execution",
            "record_receipt",
            (),
            {},
        ),
        (
            "execution",
            "reconcile",
            (),
            {},
        ),
        (
            "dispatcher_runtime",
            "acquire_dispatcher_lease",
            (lease.instance_id,),
            {"lease_seconds": 30},
        ),
        (
            "dispatcher_runtime",
            "renew_dispatcher_lease",
            (lease,),
            {"lease_seconds": 30},
        ),
        (
            "dispatcher_runtime",
            "release_dispatcher_lease",
            (lease,),
            {},
        ),
        (
            "dispatcher_runtime",
            "controls_for_live_starts",
            ((),),
            {},
        ),
        (
            "dispatcher_runtime",
            "reconcile_inflight_turns_with_task7_evidence",
            (),
            {"limit": 100},
        ),
        (
            "dispatcher_runtime",
            "runnable_project_membership_upper_watermark",
            (),
            {},
        ),
        (
            "dispatcher_runtime",
            "scan_runnable_projects",
            (),
            {
                "after": None,
                "through_membership_sequence": 1,
                "limit": 100,
            },
        ),
        (
            "dispatcher_runtime",
            "claim_next_turn_for_dispatcher",
            ("c14-project", lease.instance_id),
            {
                "lease_seconds": 90,
                "dispatcher_lease": lease,
            },
        ),
        (
            "dispatcher_operation",
            "expire_due_operation_approvals",
            (),
            {"limit": 100},
        ),
        (
            "dispatcher_operation",
            "operation_recovery_membership_upper_watermark",
            (),
            {},
        ),
        (
            "dispatcher_operation",
            "recover_pending_operations",
            (capabilities,),
            {
                "worker_id": lease.instance_id,
                "lease_seconds": 90,
                "dispatcher_lease": lease,
                "max_claims": 1,
                "after": None,
                "through_membership_sequence": 1,
                "limit": 100,
            },
        ),
        (
            "settlement",
            "pending_project_batch_upper_watermark",
            (),
            {},
        ),
        (
            "settlement",
            "scan_pending_project_batches",
            (),
            {
                "after": settlement_after,
                "through_batch_sequence": (
                    settlement_upper.batch_creation_sequence
                ),
                "limit": 100,
            },
        ),
        (
            "settlement",
            "apply_project_batch",
            ("223e4567-e89b-42d3-a456-426614174000",),
            {},
        ),
        (
            "terminal_readback",
            "read_turn_with_evidence",
            (
                TurnReadbackRequest(
                    "c14-project",
                    "c14-turn",
                    1,
                    "c14-worker",
                    "c14-attempt",
                    2,
                    3,
                    190,
                    "c14-session",
                    "running",
                    None,
                ),
            ),
            {},
        ),
        (
            "checkpoint_read",
            "publication_state",
            (checkpoint_identity,),
            {},
        ),
    )
    expected_async_callables = {
        (label, method_name)
        for label, method_names in positives.items()
        if label != "approved"
        for method_name in method_names
    }
    observed_async_callables = {
        (label, method_name)
        for label, method_name, _args, _kwargs in calls
    }
    assert observed_async_callables == expected_async_callables
    assert len(calls) == len(observed_async_callables)
    all_factories = (
        state_factory,
        projects_factory,
        runtime_factory,
        policy_factory,
        prepare_factory,
        execution_factory,
        operation_factory,
    )
    connection_ids_before = {
        id(connection)
        for factory in all_factories
        for connection in factory.connections
    }
    observed_results = {}
    resolved_calls = []
    for label, method_name, args, kwargs in calls:
        call_key = (label, method_name)
        if call_key == ("prepare", "prepare"):
            carrier = observed_results[
                ("policy", "authorize_project_operation")
            ]
            authority_flow["authorized_carrier"] = carrier
            args = (claim, carrier.operation_authority.intent)
            kwargs = {
                "authority": carrier.operation_authority,
                "policy": carrier.decision,
                "policy_authority": carrier,
                "approval": None,
            }
        elif call_key == (
            "execution",
            "certified_execution_request",
        ):
            args = (
                execution,
                observed_results[("prepare", "prepare")],
            )
        elif call_key == ("execution", "mark_started"):
            args = (
                observed_results[
                    ("execution", "certified_execution_request")
                ],
            )
        elif call_key == ("execution", "record_receipt"):
            args = (
                observed_results[
                    ("execution", "certified_execution_request")
                ],
                OperationReceipt(
                    "c14-receipt",
                    {"remote_id": "c14-effect"},
                ),
            )
        elif call_key == ("execution", "reconcile"):
            args = (
                observed_results[
                    ("execution", "certified_execution_request")
                ],
                surface_readback,
            )
        method = getattr(facades[label], method_name)
        result = await method(
            *args,
            **kwargs,
        )
        observed_results[call_key] = result
        if call_key == ("prepare", "prepare"):
            authority_flow["prepared_operation"] = result
        resolved_calls.append((label, method_name, args, kwargs))
    all_connections = [
        connection
        for factory in all_factories
        for connection in factory.connections
    ]
    new_connections = [
        connection
        for connection in all_connections
        if id(connection) not in connection_ids_before
    ]
    nested_apply_count = sum(
        label in {"state", "settlement"}
        and method_name == "apply_project_batch"
        for label, method_name, _args, _kwargs in calls
    )
    assert len(new_connections) == len(calls) + nested_apply_count
    assert len({id(connection) for connection in new_connections}) == len(
        new_connections
    )
    assert all(
        connection.closed
        and connection.created_thread != loop_thread
        and connection.used_threads == [connection.created_thread]
        and connection.close_thread == connection.created_thread
        for connection in new_connections
    )
    nested_connections = [
        connection
        for connection in projects_factory.connections
        if id(connection) not in connection_ids_before
    ]
    assert len(nested_connections) == nested_apply_count
    assert [
        entry[0]
        for entry in projects_factory.trace
        if entry[0] != "close"
    ].count("resolve_project_batch_authority") >= nested_apply_count
    assert resolver_constructions
    assert all(
        factory is projects_factory
        for _resolver, factory, _thread in resolver_constructions
    )
    assert len(resolver_calls) == nested_apply_count
    assert all(
        checkpoint is checkpoint_identity
        and thread_id == connection.created_thread
        and connection.closed
        for _resolver, checkpoint, thread_id, connection in resolver_calls
    )
    assert not any(active_transactions.values())

    def protocol_slice(connection):
        start = next(
            index
            for index, entry in enumerate(protocol_trace)
            if entry[:3]
            == (
                "open",
                connection.factory.label,
                connection.connection_id,
            )
        )
        end = next(
            index
            for index in range(start + 1, len(protocol_trace))
            if protocol_trace[index][:3]
            == (
                "close",
                connection.factory.label,
                connection.connection_id,
            )
        )
        return protocol_trace[start : end + 1]

    def apply_protocol_shape(connection):
        return [
            (
                entry[0],
                entry[1],
                entry[4] if len(entry) == 5 else None,
            )
            for entry in protocol_slice(connection)
        ]

    def expected_apply_protocol(final_event):
        return [
            ("open", "state", None),
            (
                "begin",
                "state",
                "apply_project_batch.state_prepare",
            ),
            (
                "use",
                "state",
                "apply_project_batch.state_prepare",
            ),
            (
                "commit",
                "state",
                "apply_project_batch.state_prepare",
            ),
            ("open", "projects-authority", None),
            (
                "begin",
                "projects-authority",
                "resolve_project_batch_authority",
            ),
            (
                "use",
                "projects-authority",
                "resolve_project_batch_authority",
            ),
            (
                "commit",
                "projects-authority",
                "resolve_project_batch_authority",
            ),
            ("close", "projects-authority", None),
            ("begin", "state", "apply_project_batch"),
            ("use", "state", "apply_project_batch"),
            (final_event, "state", "apply_project_batch"),
            ("close", "state", None),
        ]

    successful_state_apply_connections = [
        connection
        for connection in new_connections
        if connection.factory is state_factory
        and any(
            entry[0] == "apply_project_batch"
            and entry[1] == connection.connection_id
            for entry in state_factory.trace
        )
    ]
    assert len(successful_state_apply_connections) == nested_apply_count
    for connection in successful_state_apply_connections:
        protocol = protocol_slice(connection)
        assert apply_protocol_shape(connection) == (
            expected_apply_protocol("commit")
        )
        assert {
            entry[3]
            for entry in protocol
        } == {connection.created_thread}

    successful_policy_connections = [
        connection
        for connection in new_connections
        if connection.factory is policy_factory
    ]
    assert len(successful_policy_connections) == 2
    policy_protocol_shapes = {
        tuple(entry[0] for entry in protocol_slice(connection))
        for connection in successful_policy_connections
    }
    assert policy_protocol_shapes == {
        (
            "open",
            "begin",
            "use",
            "bind_read",
            "decide",
            "commit",
            "close",
        ),
        (
            "open",
            "begin",
            "use",
            "bind_operation",
            "decide",
            "commit",
            "close",
        ),
    }
    for connection in successful_policy_connections:
        policy_protocol = protocol_slice(connection)
        assert {
            entry[3] for entry in policy_protocol
        } == {connection.created_thread}
        decision_index = next(
            index
            for index, entry in enumerate(policy_protocol)
            if entry[0] == "decide"
        )
        commit_index = next(
            index
            for index, entry in enumerate(policy_protocol)
            if entry[0] == "commit"
        )
        close_index = next(
            index
            for index, entry in enumerate(policy_protocol)
            if entry[0] == "close"
        )
        assert decision_index < commit_index < close_index

    # The policy proposal carries no phase/metadata. Its final command is
    # bound only inside the fresh snapshot call using that snapshot's current
    # implementation phase, with contract revision 7 and capability revision
    # 1 deliberately unequal.
    assert not hasattr(read_proposal, "phase")
    assert not hasattr(read_proposal, "metadata")
    assert not hasattr(operation_proposal, "phase")
    assert not hasattr(operation_proposal, "metadata")
    assert intent.command_revision == 1
    assert execution.contract_revision == 7
    assert [entry[0] for entry in binder_trace] == [
        "read",
        "operation",
    ]
    assert all(
        entry[1].current_phase == "implementation"
        for entry in binder_trace
    )
    read_decision = observed_results[
        ("policy", "authorize_project_read")
    ]
    operation_carrier = observed_results[
        ("policy", "authorize_project_operation")
    ]
    assert read_decision == PolicyDecision(
        Decision.ALLOW,
        "c14-allow",
        "bound project operation",
    )
    assert type(operation_carrier) is (
        worker_module.ProjectPolicyDecisionCarrier
    )
    assert operation_carrier.execution_attempt == execution.attempt
    assert operation_carrier.execution_origin == execution.origin
    assert operation_carrier.actor == actor
    assert operation_carrier.decision == allow
    assert operation_carrier.contract.revision == 7
    assert operation_carrier.operation_authority.intent.command_revision == 1
    assert operation_carrier.operation_authority.command.metadata == {
        "phase": "implementation"
    }
    assert operation_carrier is authority_flow["authorized_carrier"]
    assert authority_flow["prepared_carrier"] is operation_carrier
    assert authority_flow["prepared_authority"] is (
        operation_carrier.operation_authority
    )
    assert authority_flow["prepared_operation"] is approved_operation
    assert len(prepare_observations) == 1
    (
        prepared_carrier,
        prepared_authority,
        prepared_authority_json,
        prepared_authority_sha256,
        prepared_scope_json,
        prepared_scope_sha256,
        prepared_policy_json,
        prepared_policy_sha256,
    ) = prepare_observations[0]
    assert prepared_carrier is operation_carrier
    assert prepared_authority is operation_carrier.operation_authority
    assert prepared_authority_json == prepared_authority.authority_json
    assert prepared_authority_sha256 == prepared_authority.authority_sha256
    assert prepared_scope_json == prepared_authority.effect_scope_json
    assert prepared_scope_sha256 == prepared_authority.effect_scope_sha256
    assert hashlib.sha256(
        prepared_policy_json.encode("utf-8")
    ).hexdigest() == prepared_policy_sha256
    certified_request = observed_results[
        ("execution", "certified_execution_request")
    ]
    assert type(certified_request) is (
        worker_module.CertifiedProjectOperationExecutionRequest
    )
    assert len(certified_observations) == 1
    assert certified_observations[0] == (
        operation_carrier,
        prepared_authority,
        certified_request,
    )
    assert certified_observations[0][0] is operation_carrier
    assert certified_observations[0][1] is prepared_authority
    assert certified_observations[0][2] is certified_request
    assert certified_request.operation is approved_operation
    assert certified_request.attempt is execution.attempt
    assert certified_request.payload == prepared_authority.intent.payload
    assert (
        certified_request.operation_authority_json
        == prepared_authority_json
    )
    assert (
        certified_request.operation_authority_sha256
        == prepared_authority_sha256
    )
    assert certified_request.effect_scope_json == prepared_scope_json
    assert certified_request.effect_scope_sha256 == prepared_scope_sha256
    assert (
        certified_request.policy_authority_sha256
        == prepared_policy_sha256
    )
    assert certified_request.capability_fingerprint == (
        capability.fingerprint
    )
    assert tuple(
        field.name
        for field in fields(type(certified_request))
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
    policy_connection_calls = [
        entry[0] for entry in policy_factory.trace
        if entry[0] != "close"
    ]
    assert policy_connection_calls == [
        "load_project_policy_snapshot",
        "load_project_policy_snapshot",
    ]

    settlement_upper_result = observed_results[
        ("settlement", "pending_project_batch_upper_watermark")
    ]
    settlement_page_result = observed_results[
        ("settlement", "scan_pending_project_batches")
    ]
    assert type(settlement_upper_result) is ProjectBatchCursor
    assert settlement_upper_result == settlement_upper
    assert type(settlement_page_result) is (
        FixedUpperPendingProjectBatchPage
    )
    assert tuple(
        field.name
        for field in fields(type(settlement_page_result))
    ) == (
        "members",
        "scanned_through",
        "reached_epoch_end",
    )
    assert type(settlement_page_result.members) is tuple
    assert settlement_page_result.members == (settlement_member,)
    assert all(
        type(member) is PendingProjectBatch
        for member in settlement_page_result.members
    )
    assert type(settlement_page_result.scanned_through) is (
        ProjectBatchCursor
    )
    assert settlement_page_result.scanned_through == settlement_upper
    assert type(settlement_page_result.reached_epoch_end) is bool
    assert settlement_page_result.reached_epoch_end is True
    with pytest.raises(FrozenInstanceError):
        settlement_page_result.reached_epoch_end = False
    upper_signature = inspect.signature(
        facades["settlement"].pending_project_batch_upper_watermark
    )
    scan_signature = inspect.signature(
        facades["settlement"].scan_pending_project_batches
    )
    assert tuple(upper_signature.parameters) == ()
    assert tuple(scan_signature.parameters) == (
        "after",
        "through_batch_sequence",
        "limit",
    )
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in scan_signature.parameters.values()
    )

    reconstructed_authority = (
        worker_module.BoundProjectOperationAuthority(
            command=prepared_authority.command,
            intent=prepared_authority.intent,
            policy_batch_id=prepared_authority.policy_batch_id,
            effect_scope_json=prepared_authority.effect_scope_json,
            effect_scope_sha256=prepared_authority.effect_scope_sha256,
            authority_json=prepared_authority.authority_json,
            authority_sha256=prepared_authority.authority_sha256,
        )
    )
    assert reconstructed_authority == prepared_authority
    assert reconstructed_authority is not prepared_authority
    before_reconstruction_rejections = {
        id(connection)
        for factory in all_factories
        for connection in factory.connections
    }
    reconstructed_result = await facades["prepare"].prepare(
        claim,
        reconstructed_authority.intent,
        authority=reconstructed_authority,
        policy=operation_carrier.decision,
        policy_authority=operation_carrier,
        approval=None,
    )
    assert reconstructed_result is approved_operation
    swapped_carrier = worker_module.ProjectPolicyDecisionCarrier(
        execution_attempt=operation_carrier.execution_attempt,
        execution_origin=operation_carrier.execution_origin,
        control_version=operation_carrier.control_version,
        runtime_version=operation_carrier.runtime_version,
        operation_authority=reconstructed_authority,
        project=operation_carrier.project,
        contract_id=operation_carrier.contract_id,
        contract_status=operation_carrier.contract_status,
        contract_json_sha256=(
            operation_carrier.contract_json_sha256
        ),
        contract=operation_carrier.contract,
        actor=operation_carrier.actor,
        decision=operation_carrier.decision,
    )
    assert swapped_carrier == operation_carrier
    assert swapped_carrier is not operation_carrier
    reconstructed_carrier_result = await facades["prepare"].prepare(
        claim,
        prepared_authority.intent,
        authority=prepared_authority,
        policy=operation_carrier.decision,
        policy_authority=swapped_carrier,
        approval=None,
    )
    assert reconstructed_carrier_result is approved_operation
    mismatched_authority = worker_module.BoundProjectOperationAuthority(
        command=prepared_authority.command,
        intent=prepared_authority.intent,
        policy_batch_id="different-policy-batch",
        effect_scope_json=prepared_authority.effect_scope_json,
        effect_scope_sha256=prepared_authority.effect_scope_sha256,
        authority_json=prepared_authority.authority_json,
        authority_sha256=prepared_authority.authority_sha256,
    )
    assert mismatched_authority != prepared_authority
    with pytest.raises(
        (
            AssertionError,
            PermissionError,
            ValueError,
            ProjectOperationError,
        )
    ):
        await facades["prepare"].prepare(
            claim,
            mismatched_authority.intent,
            authority=mismatched_authority,
            policy=operation_carrier.decision,
            policy_authority=operation_carrier,
            approval=None,
        )
    value_comparison_connections = [
        connection
        for factory in all_factories
        for connection in factory.connections
        if id(connection) not in before_reconstruction_rejections
    ]
    assert len(value_comparison_connections) == 3
    assert all(
        connection.factory is prepare_factory
        and connection.closed
        and connection.created_thread != loop_thread
        and connection.used_threads == [connection.created_thread]
        and connection.close_thread == connection.created_thread
        for connection in value_comparison_connections
    )
    assert not any(active_transactions.values())

    # This slice tests only facade ownership and delegation. The exact real
    # runtime and guard are constructed, while a thin guard-call recorder
    # proves that typed but internally inconsistent inputs reach that seam
    # unchanged. Any SQL here would therefore belong to the facade and fail.
    raw_seam_trace = []
    raw_seam_connections = []
    raw_seam_runtimes = []
    raw_seam_runtime_connections = {}
    raw_seam_guards = []
    raw_seam_failures = {}
    raw_seam_entered = {}
    raw_seam_release = {}
    raw_inconsistent_authority = (
        worker_module.BoundProjectOperationAuthority(
            command=prepared_authority.command,
            intent=prepared_authority.intent,
            policy_batch_id=prepared_authority.policy_batch_id,
            effect_scope_json='{"targets":["C:/drift"]}',
            effect_scope_sha256="1" * 64,
            authority_json='{"authority":"drift"}',
            authority_sha256="2" * 64,
        )
    )
    raw_inconsistent_operation = ProjectOperation(
        "operation-raw-inconsistent",
        execution.attempt.project_id,
        execution.attempt.turn_id,
        "raw-inconsistent-idempotency",
        "different-action",
        99,
        ("C:/different.py",),
        ("different-item",),
        "approved",
        None,
        "different-readback",
        None,
        None,
        execution.attempt.attempt_id,
        execution.attempt.lease_generation,
        execution.attempt.fencing_token,
        100,
        101,
    )
    raw_inconsistent_request = (
        worker_module.CertifiedProjectOperationExecutionRequest(
            operation=raw_inconsistent_operation,
            attempt=execution.attempt,
            payload={"path": "C:/drift", "content": "drift"},
            approval_checkpoint_id="different-checkpoint",
            operation_authority_json='{"authority":"drift"}',
            operation_authority_sha256="3" * 64,
            effect_scope_json='{"targets":["C:/drift"]}',
            effect_scope_sha256="4" * 64,
            policy_authority_sha256="5" * 64,
            remote_idempotency_supported=True,
            capability_fingerprint=(
                "different-action",
                99,
                "different-readback",
                True,
            ),
        )
    )
    assert type(raw_inconsistent_authority) is (
        worker_module.BoundProjectOperationAuthority
    )
    assert raw_inconsistent_authority != (
        operation_carrier.operation_authority
    )
    assert type(raw_inconsistent_operation) is ProjectOperation
    assert (
        raw_inconsistent_operation.project_id
        == execution.attempt.project_id
    )
    assert type(raw_inconsistent_request) is (
        worker_module.CertifiedProjectOperationExecutionRequest
    )
    assert (
        raw_inconsistent_request.operation_authority_sha256
        != prepared_authority.authority_sha256
    )
    raw_seam_results = {
        "prepare": approved_operation,
        "certified_execution_request": certified_request,
        "mark_started": effect_started_operation,
        "record_receipt": receipt_recorded_operation,
        "reconcile": reconciled_operation,
    }
    raw_receipt = OperationReceipt(
        "c14-receipt",
        {"remote_id": "c14-effect"},
    )
    raw_expected_calls = {
        "prepare": (
            (claim, raw_inconsistent_authority.intent),
            {
                "authority": raw_inconsistent_authority,
                "policy": operation_carrier.decision,
                "policy_authority": operation_carrier,
                "approval": None,
                "approval_checkpoint_id": None,
            },
        ),
        "certified_execution_request": (
            (execution, raw_inconsistent_operation),
            {},
        ),
        "mark_started": (
            (
                claim,
                raw_inconsistent_operation.operation_id,
            ),
            {"approval_checkpoints": checkpoints},
        ),
        "record_receipt": (
            (
                claim,
                raw_inconsistent_operation.operation_id,
                raw_receipt,
            ),
            {},
        ),
        "reconcile": (
            (
                claim,
                raw_inconsistent_operation.operation_id,
                surface_readback,
            ),
            {},
        ),
    }

    class RecordingRawSQLiteConnection(sqlite3.Connection):
        def bind_recording(self):
            self.connection_id = len(raw_seam_connections)
            self.created_thread = threading.get_ident()
            self.close_thread = None
            self.closed = False

        def close(self):
            assert not self.closed
            assert not self.in_transaction
            self.close_thread = threading.get_ident()
            raw_seam_trace.append(
                ("close", self, self.close_thread, None)
            )
            self.set_trace_callback(None)
            super().close()
            with pytest.raises(
                sqlite3.ProgrammingError,
                match="closed",
            ):
                sqlite3.Connection.execute(self, "SELECT 1")
            self.closed = True

    def raw_projects_db_factory():
        connection = sqlite3.connect(
            ":memory:",
            factory=RecordingRawSQLiteConnection,
        )
        connection.bind_recording()

        def record_sql(statement):
            thread_id = threading.get_ident()
            raw_seam_trace.append(
                ("sql", connection, thread_id, statement)
            )

        connection.set_trace_callback(record_sql)
        raw_seam_connections.append(connection)
        raw_seam_trace.append(
            (
                "factory",
                connection,
                connection.created_thread,
                None,
            )
        )
        return connection

    def exact_project_runtime_factory(connection):
        assert type(connection) is RecordingRawSQLiteConnection
        assert not connection.closed
        thread_id = threading.get_ident()
        assert thread_id == connection.created_thread
        runtime = ProjectRuntime(connection)
        assert type(runtime) is ProjectRuntime
        raw_seam_runtime_connections[id(runtime)] = connection
        raw_seam_runtimes.append(
            (connection, runtime, thread_id)
        )
        raw_seam_trace.append(
            ("runtime", connection, thread_id, runtime)
        )
        return runtime

    class ThinGuardCallRecorder:
        def __init__(self, guard, connection):
            assert type(guard) is ProjectOperationGuard
            self.guard = guard
            self.connection = connection

        def _delegate(self, method_name, args, kwargs):
            connection = self.connection
            thread_id = threading.get_ident()
            assert thread_id == connection.created_thread
            expected_args, expected_kwargs = raw_expected_calls[
                method_name
            ]
            assert len(args) == len(expected_args)
            assert all(
                actual is expected
                or (
                    type(actual) is TurnClaim
                    and type(expected) is TurnClaim
                    and actual == expected
                )
                for actual, expected in zip(args, expected_args)
            )
            assert tuple(kwargs) == tuple(expected_kwargs)
            assert all(
                kwargs[key] is expected_kwargs[key]
                for key in expected_kwargs
            )
            raw_seam_trace.append(
                (
                    "delegate",
                    connection,
                    thread_id,
                    (method_name, args, kwargs, self),
                )
            )
            try:
                entered = raw_seam_entered.get(method_name)
                if entered is not None:
                    entered.set()
                release = raw_seam_release.get(method_name)
                if release is not None:
                    assert release.wait(timeout=5)
                failure = raw_seam_failures.pop(
                    method_name,
                    None,
                )
                if failure is not None:
                    raise failure
            except BaseException:
                raw_seam_trace.append(
                    (
                        "raise",
                        connection,
                        thread_id,
                        method_name,
                    )
                )
                raise
            raw_seam_trace.append(
                (
                    "return",
                    connection,
                    thread_id,
                    method_name,
                )
            )
            return raw_seam_results[method_name]

        def prepare(self, *args, **kwargs):
            return self._delegate("prepare", args, kwargs)

        def certified_execution_request(self, *args, **kwargs):
            return self._delegate(
                "certified_execution_request",
                args,
                kwargs,
            )

        def mark_started(self, *args, **kwargs):
            return self._delegate("mark_started", args, kwargs)

        def record_receipt(self, *args, **kwargs):
            return self._delegate("record_receipt", args, kwargs)

        def reconcile(self, *args, **kwargs):
            return self._delegate("reconcile", args, kwargs)

    def recording_guard_protocol_factory(runtime):
        assert type(runtime) is ProjectRuntime
        thread_id = threading.get_ident()
        connection = raw_seam_runtime_connections[id(runtime)]
        assert type(connection) is RecordingRawSQLiteConnection
        assert thread_id == connection.created_thread
        guard = ProjectOperationGuard(runtime)
        assert type(guard) is ProjectOperationGuard
        recorder = ThinGuardCallRecorder(guard, connection)
        raw_seam_guards.append(
            (runtime, guard, recorder, thread_id)
        )
        raw_seam_trace.append(
            (
                "guard",
                connection,
                thread_id,
                (guard, recorder),
            )
        )
        return recorder

    raw_prepare_facade = worker_module.ProjectOperationPrepareFacade(
        raw_projects_db_factory,
        io_runner=io_runner,
        runtime_factory=exact_project_runtime_factory,
        operation_guard_factory=recording_guard_protocol_factory,
    )
    raw_execution_facade = (
        worker_module.ProjectOperationExecutionFacade(
            raw_projects_db_factory,
            approval_checkpoints=checkpoints,
            io_runner=io_runner,
            runtime_factory=exact_project_runtime_factory,
            operation_guard_factory=recording_guard_protocol_factory,
        )
    )
    raw_facade_calls = {
        "prepare": (
            raw_prepare_facade.prepare,
            raw_expected_calls["prepare"][0],
            raw_expected_calls["prepare"][1],
        ),
        "certified_execution_request": (
            raw_execution_facade.certified_execution_request,
            raw_expected_calls[
                "certified_execution_request"
            ][0],
            {},
        ),
        "mark_started": (
            raw_execution_facade.mark_started,
            (raw_inconsistent_request,),
            {},
        ),
        "record_receipt": (
            raw_execution_facade.record_receipt,
            (raw_inconsistent_request, raw_receipt),
            {},
        ),
        "reconcile": (
            raw_execution_facade.reconcile,
            (raw_inconsistent_request, surface_readback),
            {},
        ),
    }

    def assert_raw_seam_invocation(
        trace_start,
        connection_start,
        method_name,
        outcome,
    ):
        invocation_connections = raw_seam_connections[
            connection_start:
        ]
        assert len(invocation_connections) == 1
        connection = invocation_connections[0]
        invocation_trace = raw_seam_trace[trace_start:]
        core_events = [
            entry[0]
            for entry in invocation_trace
            if entry[0]
            in {
                "factory",
                "runtime",
                "guard",
                "delegate",
                "close",
            }
        ]
        assert core_events == [
            "factory",
            "runtime",
            "guard",
            "delegate",
            "close",
        ]
        assert all(
            entry[1] is connection
            for entry in invocation_trace
        )
        assert {
            entry[2] for entry in invocation_trace
        } == {connection.created_thread}
        assert connection.created_thread != loop_thread
        assert connection.close_thread == connection.created_thread
        assert connection.closed
        runtime_entries = [
            entry
            for entry in invocation_trace
            if entry[0] == "runtime"
        ]
        guard_entries = [
            entry
            for entry in invocation_trace
            if entry[0] == "guard"
        ]
        delegate_entries = [
            entry
            for entry in invocation_trace
            if entry[0] == "delegate"
        ]
        assert len(runtime_entries) == 1
        assert len(guard_entries) == 1
        assert len(delegate_entries) == 1
        runtime = runtime_entries[0][3]
        guard, recorder = guard_entries[0][3]
        assert type(runtime) is ProjectRuntime
        assert type(guard) is ProjectOperationGuard
        assert type(recorder) is ThinGuardCallRecorder
        assert recorder.guard is guard
        assert recorder.connection is connection
        assert delegate_entries[0][3][0] == method_name
        assert delegate_entries[0][3][3] is recorder
        assert [
            entry[0]
            for entry in invocation_trace
            if entry[0] in {"return", "raise"}
        ] == [outcome]
        assert not [
            entry
            for entry in invocation_trace
            if entry[0] == "sql"
        ]

    for (
        raw_method_name,
        (raw_method, raw_args, raw_kwargs),
    ) in raw_facade_calls.items():
        trace_start = len(raw_seam_trace)
        connection_start = len(raw_seam_connections)
        raw_result = await raw_method(*raw_args, **raw_kwargs)
        assert raw_result is raw_seam_results[raw_method_name]
        assert_raw_seam_invocation(
            trace_start,
            connection_start,
            raw_method_name,
            "return",
        )

    for (
        raw_method_name,
        (raw_method, raw_args, raw_kwargs),
    ) in raw_facade_calls.items():
        trace_start = len(raw_seam_trace)
        connection_start = len(raw_seam_connections)
        failure_message = (
            f"c14 raw {raw_method_name} failure"
        )
        raw_seam_failures[raw_method_name] = RuntimeError(
            failure_message
        )
        with pytest.raises(
            PermissionError,
            match="project operation guard rejected request",
        ) as rejected:
            await raw_method(*raw_args, **raw_kwargs)
        assert type(rejected.value.__cause__) is RuntimeError
        assert str(rejected.value.__cause__) == failure_message
        assert_raw_seam_invocation(
            trace_start,
            connection_start,
            raw_method_name,
            "raise",
        )

    for (
        raw_method_name,
        (raw_method, raw_args, raw_kwargs),
    ) in raw_facade_calls.items():
        entered = threading.Event()
        release = threading.Event()
        raw_seam_entered[raw_method_name] = entered
        raw_seam_release[raw_method_name] = release
        trace_start = len(raw_seam_trace)
        connection_start = len(raw_seam_connections)
        raw_cancelled_call = asyncio.create_task(
            raw_method(*raw_args, **raw_kwargs)
        )
        await asyncio.wait_for(
            asyncio.to_thread(entered.wait),
            timeout=5,
        )
        active_connection = raw_seam_connections[-1]
        assert not active_connection.in_transaction
        assert not active_connection.closed
        raw_cancelled_call.cancel()
        assert raw_cancelled_call.cancelling() == 1
        assert not raw_cancelled_call.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await raw_cancelled_call
        assert_raw_seam_invocation(
            trace_start,
            connection_start,
            raw_method_name,
            "return",
        )
        del raw_seam_entered[raw_method_name]
        del raw_seam_release[raw_method_name]

    assert len(raw_seam_connections) == (
        len(raw_facade_calls) * 3
    )
    assert len(
        {id(connection) for connection in raw_seam_connections}
    ) == len(raw_seam_connections)
    assert len(raw_seam_runtimes) == len(raw_seam_connections)
    assert len(raw_seam_guards) == len(raw_seam_connections)

    # Separately prove the exact production factory contract with one
    # non-vacuous, seeded prepare. This path returns the real guard itself;
    # its durable row/event and SQL trace prove that the facade invoked the
    # real transition before closing its raw connection.
    real_projects_path = tmp_path / "c14-real-raw-prepare.db"
    real_seed = projects_db.connect(real_projects_path)
    real_project_id = projects_db.create_project(
        real_seed,
        name="C14 raw facade",
        folders=("c:/work",),
    )
    real_session_id = "c14-real-raw-session"
    real_binding_id = "c14-real-raw-binding"
    real_external_binding_id = "c14-real-raw-window"
    prdb.create_project_conversation(
        real_seed,
        project_id=real_project_id,
        conversation_id=real_session_id,
        current_phase="implementation",
        now=1,
    )
    prdb.bind_surface(
        real_seed,
        binding_id=real_binding_id,
        project_id=real_project_id,
        surface="desktop",
        external_binding_id=real_external_binding_id,
        actor_id="owner-1",
        now=1,
    )
    real_contract_json = json.dumps(
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
    real_contract_sha256 = hashlib.sha256(
        real_contract_json.encode("utf-8")
    ).hexdigest()
    real_seed.execute(
        """
        INSERT INTO project_contracts (
            contract_id, project_id, revision, contract_json,
            status, created_at, updated_at
        ) VALUES (?, ?, 7, ?, 'active', 1, 1)
        """,
        (
            "contract-c14-real-raw",
            real_project_id,
            real_contract_json,
        ),
    )
    real_seed.commit()
    real_seed_runtime = ProjectRuntime(
        real_seed,
        clock=lambda: 100,
    )
    real_actor = ActorContext(
        "owner-1",
        "desktop",
        real_binding_id,
        True,
    )
    real_turn = real_seed_runtime.enqueue_turn(
        real_project_id,
        {"path": "c:/work/file.py", "content": "exact"},
        real_actor,
        idempotency_key="c14-real-raw-turn",
        expected_version=0,
    )
    real_claim = real_seed_runtime.claim_next_turn(
        real_project_id,
        "c14-real-raw-worker",
        lease_seconds=90,
    )
    assert real_claim is not None
    assert real_claim.turn_id == real_turn.turn_id
    real_claim = real_seed_runtime.mark_turn_started(real_claim)
    real_state = prdb.runtime_state_for_project(
        real_seed,
        real_project_id,
    )
    assert real_state is not None
    real_control = real_seed_runtime.control_for_claim(real_claim)
    real_attempt = TurnAttemptIdentity(
        real_claim.project_id,
        real_claim.turn_id,
        real_claim.sequence,
        real_claim.worker_id,
        real_claim.attempt_id,
        real_claim.lease_generation,
        real_claim.fencing_token,
        real_claim.canonical_session_id,
        real_claim.lease_expires_at,
    )
    real_execution = TurnExecutionInput(
        real_attempt,
        {"path": "c:/work/file.py", "content": "exact"},
        TurnOrigin(
            real_binding_id,
            "desktop",
            real_external_binding_id,
            "owner-1",
        ),
        7,
    )
    real_intent = OperationIntent(
        "operation-c14-real-raw",
        real_project_id,
        real_claim.turn_id,
        "c14-real-raw-operation",
        "local_code_edit",
        1,
        ("c:/work/file.py",),
        ("write",),
        {"path": "c:/work/file.py", "content": "exact"},
        "remote-ledger",
        True,
    )
    real_command = ProjectCommand(
        real_intent.canonical_action,
        real_project_id,
        7,
        "local_code_edit",
        real_intent.targets,
        None,
        real_intent.batch_items,
        {"phase": "implementation"},
    )
    real_effect_scope = {
        "targets": list(real_intent.targets),
        "batch_items": list(real_intent.batch_items),
        "payload_effects": dict(real_intent.payload),
    }
    real_effect_scope_json = json.dumps(
        real_effect_scope,
        sort_keys=True,
        separators=(",", ":"),
    )
    real_authority_payload = {
        "command": {
            "name": real_command.name,
            "project_id": real_command.project_id,
            "revision": real_command.revision,
            "action_class": real_command.action_class,
            "targets": list(real_command.targets),
            "batch_id": real_command.batch_id,
            "batch_items": list(real_command.batch_items),
            "metadata": dict(real_command.metadata),
        },
        "intent": {
            "operation_id": real_intent.operation_id,
            "project_id": real_intent.project_id,
            "turn_id": real_intent.turn_id,
            "idempotency_key": real_intent.idempotency_key,
            "canonical_action": real_intent.canonical_action,
            "command_revision": real_intent.command_revision,
            "targets": list(real_intent.targets),
            "batch_items": list(real_intent.batch_items),
            "payload": dict(real_intent.payload),
            "readback_kind": real_intent.readback_kind,
            "remote_idempotency_supported": (
                real_intent.remote_idempotency_supported
            ),
        },
        "policy_batch_id": None,
        "capability_fingerprint": [
            "local_code_edit",
            1,
            "remote-ledger",
            True,
        ],
        "effect_scope": real_effect_scope,
    }
    real_authority_json = json.dumps(
        real_authority_payload,
        sort_keys=True,
        separators=(",", ":"),
    )
    real_authority = (
        worker_module.BoundProjectOperationAuthority(
            real_command,
            real_intent,
            None,
            real_effect_scope_json,
            hashlib.sha256(
                real_effect_scope_json.encode("utf-8")
            ).hexdigest(),
            real_authority_json,
            hashlib.sha256(
                real_authority_json.encode("utf-8")
            ).hexdigest(),
        )
    )
    real_decision = PolicyDecision(
        Decision.ALLOW,
        "policy.allow.routine_in_plan",
        "routine owner work is within the approved plan and scope",
    )
    real_carrier = worker_module.ProjectPolicyDecisionCarrier(
        real_attempt,
        real_execution.origin,
        real_control.control_version,
        real_state.version,
        real_authority,
        ProjectPolicyView(
            real_project_id,
            real_state.lifecycle,
            real_state.current_phase,
            ("c:/work",),
            "plan-7",
            (
                ProjectBindingView(
                    real_binding_id,
                    "desktop",
                    "owner-1",
                    real_project_id,
                ),
            ),
        ),
        "contract-c14-real-raw",
        "active",
        real_contract_sha256,
        ContractPolicyView(
            7,
            frozenset({"local_code_edit", "publish"}),
            frozenset({"implementation"}),
            "plan-7",
        ),
        real_actor,
        real_decision,
    )
    real_seed.close()

    real_prepare_lifecycle = []
    real_prepare_connections = []
    real_prepare_runtimes = []
    real_prepare_runtime_connections = {}
    real_prepare_guards = []

    class SeededRecordingProjectsConnection(sqlite3.Connection):
        def bind_recording(self):
            self.created_thread = threading.get_ident()
            self.close_thread = None
            self.closed = False
            self.statements = []

        def close(self):
            assert not self.closed
            self.close_thread = threading.get_ident()
            real_prepare_lifecycle.append(
                ("close", self, self.close_thread)
            )
            self.set_trace_callback(None)
            super().close()
            with pytest.raises(
                sqlite3.ProgrammingError,
                match="closed",
            ):
                sqlite3.Connection.execute(self, "SELECT 1")
            self.closed = True

    def exact_real_projects_factory():
        connection = sqlite3.connect(
            str(real_projects_path),
            factory=SeededRecordingProjectsConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.bind_recording()
        connection.set_trace_callback(
            lambda statement: connection.statements.append(
                (statement, threading.get_ident())
            )
        )
        real_prepare_connections.append(connection)
        real_prepare_lifecycle.append(
            ("factory", connection, connection.created_thread)
        )
        return connection

    def exact_real_runtime_factory(connection):
        runtime = ProjectRuntime(
            connection,
            clock=lambda: 100,
        )
        assert type(runtime) is ProjectRuntime
        real_prepare_runtime_connections[id(runtime)] = connection
        real_prepare_runtimes.append(runtime)
        real_prepare_lifecycle.append(
            ("runtime", connection, threading.get_ident())
        )
        return runtime

    def exact_real_guard_factory(runtime):
        guard = ProjectOperationGuard(runtime)
        assert type(guard) is ProjectOperationGuard
        real_prepare_guards.append(guard)
        real_prepare_lifecycle.append(
            (
                "guard",
                real_prepare_runtime_connections[id(runtime)],
                threading.get_ident(),
            )
        )
        return guard

    exact_real_prepare_facade = (
        worker_module.ProjectOperationPrepareFacade(
            exact_real_projects_factory,
            io_runner=io_runner,
            runtime_factory=exact_real_runtime_factory,
            operation_guard_factory=exact_real_guard_factory,
        )
    )
    exact_real_prepared = await exact_real_prepare_facade.prepare(
        real_claim,
        real_intent,
        authority=real_authority,
        policy=real_decision,
        policy_authority=real_carrier,
        approval=None,
        approval_checkpoint_id=None,
    )
    assert type(exact_real_prepared) is ProjectOperation
    assert exact_real_prepared.operation_id == real_intent.operation_id
    assert exact_real_prepared.status == "approved"
    assert len(real_prepare_connections) == 1
    assert len(real_prepare_runtimes) == 1
    assert len(real_prepare_guards) == 1
    real_prepare_connection = real_prepare_connections[0]
    real_prepare_runtime = real_prepare_runtimes[0]
    real_prepare_guard = real_prepare_guards[0]
    assert type(real_prepare_connection) is (
        SeededRecordingProjectsConnection
    )
    assert type(real_prepare_runtime) is ProjectRuntime
    assert type(real_prepare_guard) is ProjectOperationGuard
    assert [entry[0] for entry in real_prepare_lifecycle] == [
        "factory",
        "runtime",
        "guard",
        "close",
    ]
    assert {
        entry[2] for entry in real_prepare_lifecycle
    } == {real_prepare_connection.created_thread}
    assert real_prepare_connection.created_thread != loop_thread
    assert (
        real_prepare_connection.close_thread
        == real_prepare_connection.created_thread
    )
    assert real_prepare_connection.closed
    assert real_prepare_connection.statements
    assert {
        thread_id
        for _statement, thread_id
        in real_prepare_connection.statements
    } == {real_prepare_connection.created_thread}
    normalized_real_prepare_sql = [
        " ".join(statement.upper().split())
        for statement, _thread_id
        in real_prepare_connection.statements
    ]
    # Trace callbacks may repeat parent SQL for BEFORE triggers; the exact
    # durable single-write proof is provided by the two COUNT(*) checks below.
    assert sum(
        statement.startswith("INSERT INTO PROJECT_OPERATIONS")
        for statement in normalized_real_prepare_sql
    ) >= 1
    real_prepare_check = projects_db.connect(real_projects_path)
    try:
        assert real_prepare_check.execute(
            """
            SELECT COUNT(*) FROM project_operations
            WHERE project_id = ? AND operation_id = ?
              AND status = 'approved'
            """,
            (real_project_id, real_intent.operation_id),
        ).fetchone()[0] == 1
        assert real_prepare_check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'operation.intent_recorded'
            """,
            (real_project_id, real_claim.turn_id),
        ).fetchone()[0] == 1
    finally:
        real_prepare_check.close()

    # Every bound worker/coordinator/dispatcher lane gets its own exception
    # and cancellation proof. The cancellation assertion is made while the
    # fake transaction is genuinely active, then the exact executor future is
    # released and joined before CancelledError escapes.
    factories_by_label = {
        "state": state_factory,
        "worker": runtime_factory,
        "policy": policy_factory,
        "prepare": prepare_factory,
        "execution": execution_factory,
        "dispatcher_runtime": runtime_factory,
        "dispatcher_operation": operation_factory,
        "settlement": state_factory,
        "terminal_readback": state_factory,
        "checkpoint_read": state_factory,
    }

    def connections_since(before):
        return [
            connection
            for factory in all_factories
            for connection in factory.connections
            if id(connection) not in before
        ]

    for label, method_name, args, kwargs in resolved_calls:
        factory = factories_by_label[label]
        gate_name = (
            "load_project_policy_snapshot"
            if label == "policy"
            else method_name
        )
        before = {
            id(connection)
            for owned_factory in all_factories
            for connection in owned_factory.connections
        }
        failure_message = f"c14 {label}.{method_name} failure"
        factory.failures[gate_name] = RuntimeError(failure_message)
        if label in {"prepare", "execution"}:
            with pytest.raises(
                PermissionError,
                match="project operation guard rejected request",
            ) as rejected:
                await getattr(facades[label], method_name)(
                    *args,
                    **kwargs,
                )
            assert type(rejected.value.__cause__) is RuntimeError
            assert str(rejected.value.__cause__) == failure_message
        else:
            with pytest.raises(RuntimeError, match=failure_message):
                await getattr(facades[label], method_name)(
                    *args,
                    **kwargs,
                )
        failed_connections = connections_since(before)
        expected_connections = 1 + int(
            label in {"state", "settlement"}
            and method_name == "apply_project_batch"
        )
        assert len(failed_connections) == expected_connections
        assert all(
            connection.closed
            and connection.created_thread != loop_thread
            and connection.used_threads == [connection.created_thread]
            and connection.close_thread == connection.created_thread
            for connection in failed_connections
        )
        if (
            label in {"state", "settlement"}
            and method_name == "apply_project_batch"
        ):
            failed_state_connection = next(
                connection
                for connection in failed_connections
                if connection.factory is state_factory
            )
            assert apply_protocol_shape(
                failed_state_connection
            ) == expected_apply_protocol("rollback")
        assert not any(active_transactions.values())

    for label, method_name, args, kwargs in resolved_calls:
        factory = factories_by_label[label]
        gate_name = (
            "load_project_policy_snapshot"
            if label == "policy"
            else method_name
        )
        entered = threading.Event()
        release = threading.Event()
        factory.entered[gate_name] = entered
        factory.release[gate_name] = release
        before = {
            id(connection)
            for owned_factory in all_factories
            for connection in owned_factory.connections
        }
        cancelled_call = asyncio.create_task(
            getattr(facades[label], method_name)(*args, **kwargs)
        )
        await asyncio.wait_for(
            asyncio.to_thread(entered.wait),
            timeout=5,
        )
        main_connection = factory.connections[-1]
        assert not main_connection.closed
        assert any(active_transactions.values())
        cancelled_call.cancel()
        assert not cancelled_call.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_call
        cancelled_connections = connections_since(before)
        expected_connections = 1 + int(
            label in {"state", "settlement"}
            and method_name == "apply_project_batch"
        )
        assert len(cancelled_connections) == expected_connections
        assert all(
            connection.closed
            and connection.created_thread != loop_thread
            and connection.used_threads == [connection.created_thread]
            and connection.close_thread == connection.created_thread
            for connection in cancelled_connections
        )
        if (
            label in {"state", "settlement"}
            and method_name == "apply_project_batch"
        ):
            cancelled_state_connection = next(
                connection
                for connection in cancelled_connections
                if connection.factory is state_factory
            )
            assert apply_protocol_shape(
                cancelled_state_connection
            ) == expected_apply_protocol("commit")
        assert not any(active_transactions.values())
        del factory.entered[gate_name]
        del factory.release[gate_name]

    # The real worker must select the approved-operation port before any
    # project-agent resolution. The port's sole callable constructs the real
    # canonical coordinator and turn from the strict execution facade.
    from gateway.config import GatewayConfig

    class PoisonAgentFactory:
        async def resolve_project_agent(self, **_kwargs):
            raise AssertionError(
                "approved operation resolved an AIAgent"
            )

        async def release_project_agent(self, _agent):
            raise AssertionError(
                "approved operation released an AIAgent"
            )

    worker_port = StrictApprovedOperationExecutionPort()
    composed_worker = worker_module.CanonicalProjectRuntimeWorker(
        facades["worker"],
        facades["state"],
        PoisonAgentFactory(),
        GatewayConfig(),
        profile_home=tmp_path / "c14-approved-worker",
        lease_seconds=90,
        heartbeat_interval_seconds=30,
        batch_id_factory=lambda: (
            "623e4567-e89b-42d3-a456-426614174000"
        ),
        approved_operations=worker_port,
    )
    approved_start = WorkerStart(
        "approved_operation",
        claim,
        approved_operation,
        lease,
    )
    worker_effect_start = len(capability.effect_calls)
    worker_readback_start = len(capability.readback_calls)
    effect_release.set()
    await composed_worker.run_start(approved_start)
    assert worker_port.calls == [
        (execution, approved_operation, 0)
    ]
    assert worker_port.call_threads == [loop_thread]
    assert len(capability.effect_calls) == worker_effect_start + 1
    assert len(capability.readback_calls) == worker_readback_start + 1
    await composed_worker.close()

    # Saturate the one-worker AIAgent pool with a synchronous
    # run_coroutine_threadsafe wait on another real canonical turn. The owner
    # loop must still drive its real coordinator through the independent
    # project-I/O and capability effect/readback pools.
    owner_loop = asyncio.get_running_loop()
    bridge_futures = []
    agent_threads = []

    def agent_waits_for_owner(turn):
        agent_threads.append(threading.get_ident())
        future = asyncio.run_coroutine_threadsafe(
            turn.result(),
            owner_loop,
        )
        bridge_futures.append(future)
        return future.result(timeout=5)

    full_path_connection_start = {
        id(connection)
        for factory in all_factories
        for connection in factory.connections
    }
    full_path_trace_start = len(execution_factory.trace)
    checkpoint_trace_start = len(state_factory.trace)
    full_path_protocol_start = len(protocol_trace)
    effect_call_start = len(capability.effect_calls)
    readback_call_start = len(capability.readback_calls)
    effect_entered.clear()
    effect_release.clear()
    turn = approved_port.create_turn(
        execution,
        approved_operation,
        base_message_count=9,
    )
    agent_task = asyncio.create_task(
        agent_runner(agent_waits_for_owner, turn)
    )
    await asyncio.wait_for(
        asyncio.to_thread(effect_entered.wait),
        timeout=5,
    )
    assert not agent_task.done()
    assert not bridge_futures[-1].done()
    assert not any(active_transactions.values())
    effect_release.set()
    live_result = await agent_task
    await turn.wait_quiescent()
    assert type(live_result) is worker_module.ProjectAgentRunResult
    assert live_result.status == "succeeded"
    assert live_result.base_message_count == 9
    assert len(live_result.messages) == 2
    assert approved_port.calls == [
        (execution, approved_operation, 9)
    ]
    full_path_calls = [
        entry[0]
        for entry in execution_factory.trace[full_path_trace_start:]
        if entry[0] != "close"
    ]
    assert full_path_calls == [
        "certified_execution_request",
        "mark_started",
        "record_receipt",
        "reconcile",
    ]
    checkpoint_calls = [
        entry[0]
        for entry in state_factory.trace[checkpoint_trace_start:]
        if entry[0] != "close"
    ]
    assert checkpoint_calls == ["publication_state"]
    full_path_protocol = protocol_trace[full_path_protocol_start:]
    checkpoint_close = next(
        index
        for index, entry in enumerate(full_path_protocol)
        if entry[0] == "close"
        and entry[1] == "state"
    )
    mark_begin = next(
        index
        for index, entry in enumerate(full_path_protocol)
        if entry[0] == "begin"
        and entry[1] == "execution"
        and entry[4] == "mark_started"
    )
    assert checkpoint_close < mark_begin
    assert len(capability.effect_calls) == effect_call_start + 1
    assert len(capability.readback_calls) == readback_call_start + 1
    assert all(
        transactions["state"] == ()
        and all(
            transactions[label] == ()
            for label in projects_transaction_labels
        )
        for *_call, transactions in (
            capability.effect_calls[effect_call_start:]
            + capability.readback_calls[readback_call_start:]
        )
    )

    full_path_connections = [
        connection
        for factory in all_factories
        for connection in factory.connections
        if id(connection) not in full_path_connection_start
    ]
    assert full_path_connections
    assert all(
        connection.closed
        and connection.created_thread == connection.close_thread
        and connection.used_threads == [connection.created_thread]
        for connection in full_path_connections
    )
    io_threads = {
        connection.created_thread
        for connection in full_path_connections
    }
    effect_threads = {
        thread_id
        for _request, _key, thread_id, _transactions in capability.effect_calls[
            effect_call_start:
        ]
    } | {
        thread_id
        for _request, thread_id, _transactions in capability.readback_calls[
            readback_call_start:
        ]
    }
    assert len(set(agent_threads)) == 1
    assert io_threads
    assert effect_threads
    assert set(agent_threads).isdisjoint(io_threads)
    assert set(agent_threads).isdisjoint(effect_threads)
    assert io_threads.isdisjoint(effect_threads)
    assert loop_thread not in (
        set(agent_threads) | io_threads | effect_threads
    )
    assert not any(active_transactions.values())

    # A real coordinator effect exception is joined, reconciled once, and
    # converted to the dedicated unresolved signal rather than a terminal
    # result or an effect retry.
    exceptional_connection_start = {
        id(connection)
        for factory in all_factories
        for connection in factory.connections
    }
    exceptional_effect_start = len(capability.effect_calls)
    exceptional_readback_start = len(capability.readback_calls)
    capability.effect_failure = RuntimeError(
        "c14 injected capability failure"
    )
    effect_entered.clear()
    effect_release.set()
    exceptional_turn = approved_port.create_turn(
        execution,
        approved_operation,
        base_message_count=9,
    )
    with pytest.raises(worker_module.ProjectOperationUnresolved):
        await exceptional_turn.result()
    await exceptional_turn.wait_quiescent()
    assert len(capability.effect_calls) == exceptional_effect_start + 1
    assert len(capability.readback_calls) == exceptional_readback_start + 1
    exceptional_connections = [
        connection
        for factory in all_factories
        for connection in factory.connections
        if id(connection) not in exceptional_connection_start
    ]
    assert exceptional_connections
    assert all(
        connection.closed
        and connection.created_thread == connection.close_thread
        and connection.used_threads == [connection.created_thread]
        for connection in exceptional_connections
    )
    assert not any(active_transactions.values())

    # Cancellation during the separately gated effect cannot release the
    # synchronously waiting agent thread early. The late effect is joined,
    # receipt/readback reconciliation completes, and every exact connection
    # and future is done before pool close.
    cancelled_connection_start = {
        id(connection)
        for factory in all_factories
        for connection in factory.connections
    }
    effect_entered.clear()
    effect_release.clear()
    cancelled_turn = approved_port.create_turn(
        execution,
        approved_operation,
        base_message_count=9,
    )
    cancelled_agent_task = asyncio.create_task(
        agent_runner(agent_waits_for_owner, cancelled_turn)
    )
    await asyncio.wait_for(
        asyncio.to_thread(effect_entered.wait),
        timeout=5,
    )
    assert cancelled_turn.request_cancel() is True
    assert cancelled_turn.request_cancel() is False
    assert not cancelled_agent_task.done()
    assert not bridge_futures[-1].done()
    effect_release.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_agent_task
    await cancelled_turn.wait_quiescent()
    cancelled_connections = [
        connection
        for factory in all_factories
        for connection in factory.connections
        if id(connection) not in cancelled_connection_start
    ]
    assert cancelled_connections
    assert all(
        connection.closed
        and connection.created_thread == connection.close_thread
        and connection.used_threads == [connection.created_thread]
        for connection in cancelled_connections
    )
    assert approved_port.calls == [
        (execution, approved_operation, 9),
        (execution, approved_operation, 9),
        (execution, approved_operation, 9),
    ]
    assert approved_port.call_threads == [
        loop_thread,
        loop_thread,
        loop_thread,
    ]
    assert not any(active_transactions.values())
    assert all(future.done() for future in bridge_futures)
    assert all(future.done() for future in io_runner.futures)
    assert all(future.done() for future in effect_runner.futures)
    assert all(future.done() for future in agent_runner.futures)

    io_runner.close()
    effect_runner.close()
    agent_runner.close()


@pytest.mark.asyncio
async def test_task7_c13_worker_context_history_loads_one_current_lineage_snapshot_after_start(
    tmp_path,
    monkeypatch,
) -> None:
    """The project-only history bridge preserves lineage and the tip counter.

    This catches routing through ``load_transcript`` (which may silently
    replace a storage failure with an empty list), a tip-only replay, or an
    AsyncSessionStore fallback through its broad ``__getattr__`` shim.
    """
    from gateway import session as session_module
    from hermes_state import SessionDB

    assert "load_project_history" in session_module.SessionStore.__dict__
    assert "load_project_history" in session_module.AsyncSessionStore.__dict__
    assert hasattr(session_module, "ProjectHistorySnapshot")
    assert hasattr(SessionDB, "_project_history_snapshot")

    state = SessionDB(db_path=tmp_path / "c13-history-state.db")
    try:
        state.create_session("c13-root", source="cli")
        state.create_session(
            "c13-tip", source="cli", parent_session_id="c13-root"
        )
        state.append_message("c13-root", "user", "root request")
        state.append_message("c13-tip", "assistant", "tip answer")

        original_snapshot = state._project_history_snapshot
        snapshot_calls: list[tuple[str, int]] = []

        def observe_snapshot(session_id: str):
            snapshot_calls.append((session_id, threading.get_ident()))
            return original_snapshot(session_id)

        monkeypatch.setattr(state, "_project_history_snapshot", observe_snapshot)

        # A real SessionStore normally builds this field in its constructor;
        # direct construction avoids a process-global State database while
        # retaining the concrete synchronous bridge under test.
        store = object.__new__(session_module.SessionStore)
        store._db = state
        facade = session_module.AsyncSessionStore(store)
        loop_thread = threading.get_ident()
        statements: list[tuple[str, int]] = []
        state._conn.set_trace_callback(
            lambda statement: statements.append((statement, threading.get_ident()))
        )
        try:
            snapshot = await facade.load_project_history("c13-tip")
        finally:
            state._conn.set_trace_callback(None)

        assert type(snapshot) is session_module.ProjectHistorySnapshot
        assert snapshot.session_id == "c13-tip"
        assert snapshot.messages == (
            {"role": "user", "content": "root request"},
            {"role": "assistant", "content": "tip answer"},
        )
        # The exact tip counter is deliberately different from root-to-tip
        # replay length in legal compression/lineage histories.
        assert snapshot.message_count == 1
        assert [session_id for session_id, _ in snapshot_calls] == ["c13-tip"]
        assert snapshot_calls[0][1] != loop_thread
        transaction_statements = [
            statement.lstrip().upper() for statement, _ in statements
            if statement.lstrip().upper().startswith(("BEGIN", "COMMIT", "ROLLBACK"))
        ]
        assert transaction_statements in (
            ["BEGIN", "COMMIT"],
            ["BEGIN", "ROLLBACK"],
        )
        assert all(thread_id != loop_thread for _, thread_id in statements)
        assert not any(
            statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
            for statement, _ in statements
        )

        def raise_storage_error(session_id: str):
            raise sqlite3.DatabaseError(f"c13 history snapshot failed: {session_id}")

        monkeypatch.setattr(state, "_project_history_snapshot", raise_storage_error)
        with pytest.raises(sqlite3.DatabaseError, match="c13 history snapshot failed"):
            await facade.load_project_history("c13-tip")
    finally:
        state.close()


@pytest.mark.asyncio
async def test_task7_c11_stop_closure_authority_discard_crash_boundaries_replay_once(
    tmp_path,
):
    """Both stop-authoritative crash boundaries discard exactly once.

    Catches resolver connection reuse, publication on either stop path, a
    crash replay which writes, and a final discard which forgets its reason.
    """
    from hermes_cli import project_runtime_db as prdb
    from hermes_cli import projects_db
    from hermes_cli.project_policy import ActorContext
    from hermes_cli.project_runtime import ProjectRuntime
    from hermes_state import SessionDB, _project_batch_fingerprint

    state_path = tmp_path / "c11-stop-state.db"
    projects_path = tmp_path / "c11-stop-projects.db"
    state = SessionDB(state_path)
    projects = projects_db.connect(projects_path)

    def projects_snapshot(connection, project_id):
        return tuple(
            (
                table,
                tuple(
                    tuple(row)
                    for row in connection.execute(
                        f"SELECT * FROM {table} "
                        f"WHERE project_id = ? ORDER BY rowid",
                        (project_id,),
                    )
                ),
            )
            for table in (
                "project_runtime_state",
                "project_turns",
                "project_run_controls",
                "project_worker_leases",
                "project_events",
            )
        )

    def assert_closed(connection):
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")

    def assert_no_dml(trace):
        assert not [
            statement
            for statement in trace
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]

    def recorded_projects_factory(records):
        def factory():
            connection = projects_db.connect(projects_path)
            trace: list[str] = []
            connection.set_trace_callback(trace.append)
            records.append((connection, trace))
            return connection

        return factory

    def assert_one_discard_write(
        *,
        before_changes,
        trace,
    ):
        dml = [
            statement
            for statement in trace
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]
        assert state._conn.total_changes == before_changes + 1
        assert len({
            " ".join(statement.upper().split())
            for statement in dml
            if "PROJECT_TURN_TRANSCRIPT_BATCHES" in statement.upper()
            and "DISCARD_AUTHORITY" in statement.upper()
        }) == 1
        assert not [
            statement
            for statement in dml
            if "MESSAGES" in statement.upper()
            or "SESSIONS" in statement.upper()
        ]

    def setup_stopped_attempt(
        *,
        label,
        session_id,
        binding_id,
        batch_id,
        worker_id,
        timestamp,
    ):
        project_id = projects_db.create_project(
            projects, name=f"C11 {label}"
        )
        prdb.create_project_conversation(
            projects,
            project_id=project_id,
            conversation_id=session_id,
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            projects,
            binding_id=binding_id,
            project_id=project_id,
            surface="desktop",
            external_binding_id=f"window-{label}",
            actor_id="owner",
            now=1,
        )
        state.create_session(session_id, source="cli")
        runtime = ProjectRuntime(projects, clock=lambda: 100)
        actor = ActorContext("owner", "desktop", binding_id, True)
        turn = runtime.enqueue_turn(
            project_id,
            {"message": label},
            actor,
            idempotency_key=f"turn-{label}",
            expected_version=0,
        )
        claim = runtime.claim_next_turn(
            project_id, worker_id, lease_seconds=30
        )
        assert claim is not None
        claim = runtime.mark_turn_started(claim)
        state.prepare_terminal_result(
            claim,
            batch_id=batch_id,
            status="succeeded",
            base_message_count=0,
            messages=(
                {
                    "role": "user",
                    "content": "stop",
                    "timestamp": timestamp,
                },
                {
                    "role": "assistant",
                    "content": "never append",
                    "timestamp": timestamp + 1.0,
                },
            ),
        )
        assert runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key=f"stop-{label}",
            expected_version=runtime._require_state(
                project_id
            ).version,
            expected_control_version=runtime._control(
                project_id, turn.turn_id
            ).control_version,
        ).control_state == "stop_requested"
        return {
            "project_id": project_id,
            "runtime": runtime,
            "turn": turn,
            "claim": claim,
            "batch_id": batch_id,
            "session_id": session_id,
        }

    def batch_snapshot(batch_id):
        row = state._conn.execute(
            "SELECT * FROM project_turn_transcript_batches "
            "WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        assert row is not None
        return tuple(row)

    def assert_never_published(case):
        row = state._conn.execute(
            "SELECT state, discard_authority, published_at, "
            "projects_acknowledged_at "
            "FROM project_turn_transcript_batches "
            "WHERE batch_id = ?",
            (case["batch_id"],),
        ).fetchone()
        assert tuple(row) == (
            "discarded",
            "stop_requested",
            None,
            None,
        )
        assert state.get_messages(case["session_id"]) == []
        assert projects.execute(
            "SELECT transcript_applied_batch_id FROM project_turns "
            "WHERE turn_id = ?",
            (case["turn"].turn_id,),
        ).fetchone()[0] is None

    try:
        # Boundary A: an external settlement loop may observe durable
        # stop_requested before the live closer acknowledges the runner.  That
        # authority discards State immediately; Projects stays read-only.
        before_ack = setup_stopped_attempt(
            label="discard-before-ack",
            session_id="c11-before-ack-session",
            binding_id="c11-before-ack-owner",
            batch_id="123e4567-e89b-42d3-a456-426614174000",
            worker_id="c11-before-ack-worker",
            timestamp=1.0,
        )
        before_ack_projects = projects_snapshot(
            projects, before_ack["project_id"]
        )
        before_ack_messages = tuple(
            state.get_messages(before_ack["session_id"])
        )
        before_ack_changes = state._conn.total_changes
        before_ack_factory_records: list[
            tuple[sqlite3.Connection, list[str]]
        ] = []
        before_ack_adapter = AsyncSessionStore(
            state,
            projects_db_factory=recorded_projects_factory(
                before_ack_factory_records
            ),
        )
        before_ack_trace: list[str] = []
        state._conn.set_trace_callback(before_ack_trace.append)
        try:
            assert (
                await before_ack_adapter.apply_project_batch(
                    before_ack["batch_id"]
                )
            ).outcome == "discarded"
        finally:
            state._conn.set_trace_callback(None)
        assert len(before_ack_factory_records) == 1
        assert_closed(before_ack_factory_records[0][0])
        assert_no_dml(before_ack_factory_records[0][1])
        assert_one_discard_write(
            before_changes=before_ack_changes,
            trace=before_ack_trace,
        )
        assert projects_snapshot(
            projects, before_ack["project_id"]
        ) == before_ack_projects
        assert tuple(
            state.get_messages(before_ack["session_id"])
        ) == before_ack_messages
        assert_never_published(before_ack)

        # Crash after the stop-authoritative discard but before ack.  A fresh
        # Projects runtime acknowledges the same claim; final State replay is
        # then entirely local and opens no resolver connection.
        state.close()
        projects.close()
        state = SessionDB(state_path)
        projects = projects_db.connect(projects_path)
        assert ProjectRuntime(
            projects, clock=lambda: 100
        ).acknowledge_stopped(
            before_ack["claim"]
        ).control_state == "stopped"
        before_ack_replay_projects = projects_snapshot(
            projects, before_ack["project_id"]
        )
        before_ack_replay_state = batch_snapshot(
            before_ack["batch_id"]
        )
        before_ack_replay_messages = tuple(
            state.get_messages(before_ack["session_id"])
        )
        before_ack_replay_changes = state._conn.total_changes
        before_ack_replay_records: list[
            tuple[sqlite3.Connection, list[str]]
        ] = []
        before_ack_replay_adapter = AsyncSessionStore(
            state,
            projects_db_factory=recorded_projects_factory(
                before_ack_replay_records
            ),
        )
        before_ack_replay_trace: list[str] = []
        state._conn.set_trace_callback(
            before_ack_replay_trace.append
        )
        try:
            assert (
                await before_ack_replay_adapter.apply_project_batch(
                    before_ack["batch_id"]
                )
            ).outcome == "already_discarded"
        finally:
            state._conn.set_trace_callback(None)
        assert before_ack_replay_records == []
        assert_no_dml(before_ack_replay_trace)
        assert state._conn.total_changes == before_ack_replay_changes
        assert batch_snapshot(
            before_ack["batch_id"]
        ) == before_ack_replay_state
        assert projects_snapshot(
            projects, before_ack["project_id"]
        ) == before_ack_replay_projects
        assert tuple(
            state.get_messages(before_ack["session_id"])
        ) == before_ack_replay_messages
        assert_never_published(before_ack)

        # The retained reason is part of final idempotency, independently of
        # the public adapter's resolver-free final replay.
        before_ack_row = state._conn.execute(
            "SELECT * FROM project_turn_transcript_batches "
            "WHERE batch_id = ?",
            (before_ack["batch_id"],),
        ).fetchone()
        assert before_ack_row is not None
        fingerprint = _project_batch_fingerprint(before_ack_row)
        reason_changes = state._conn.total_changes
        assert state._discard_project_batch(
            fingerprint, "stop_requested"
        ) == "already_discarded"
        assert state._discard_project_batch(
            fingerprint, "cancelled"
        ) == "state_conflict"
        with pytest.raises(ValueError):
            state._discard_project_batch(
                fingerprint, "not-an-authority"
            )
        assert state._conn.total_changes == reason_changes

        # Boundary B: the live closer has already acknowledged stopped, but
        # the process dies before applying its prepared batch.
        after_ack = setup_stopped_attempt(
            label="ack-before-discard",
            session_id="c11-after-ack-session",
            binding_id="c11-after-ack-owner",
            batch_id="223e4567-e89b-42d3-a456-426614174000",
            worker_id="c11-after-ack-worker",
            timestamp=3.0,
        )
        assert after_ack["runtime"].acknowledge_stopped(
            after_ack["claim"]
        ).control_state == "stopped"
        state.close()
        projects.close()
        state = SessionDB(state_path)
        projects = projects_db.connect(projects_path)
        after_ack_projects = projects_snapshot(
            projects, after_ack["project_id"]
        )
        after_ack_messages = tuple(
            state.get_messages(after_ack["session_id"])
        )
        after_ack_changes = state._conn.total_changes
        after_ack_factory_records: list[
            tuple[sqlite3.Connection, list[str]]
        ] = []
        after_ack_adapter = AsyncSessionStore(
            state,
            projects_db_factory=recorded_projects_factory(
                after_ack_factory_records
            ),
        )
        after_ack_trace: list[str] = []
        state._conn.set_trace_callback(after_ack_trace.append)
        try:
            assert (
                await after_ack_adapter.apply_project_batch(
                    after_ack["batch_id"]
                )
            ).outcome == "discarded"
        finally:
            state._conn.set_trace_callback(None)
        assert len(after_ack_factory_records) == 1
        assert_closed(after_ack_factory_records[0][0])
        assert_no_dml(after_ack_factory_records[0][1])
        assert_one_discard_write(
            before_changes=after_ack_changes,
            trace=after_ack_trace,
        )
        assert projects_snapshot(
            projects, after_ack["project_id"]
        ) == after_ack_projects
        assert tuple(
            state.get_messages(after_ack["session_id"])
        ) == after_ack_messages
        assert_never_published(after_ack)

        after_ack_replay_projects = projects_snapshot(
            projects, after_ack["project_id"]
        )
        after_ack_replay_state = batch_snapshot(
            after_ack["batch_id"]
        )
        after_ack_replay_changes = state._conn.total_changes
        after_ack_replay_trace: list[str] = []
        after_ack_factory_count = len(after_ack_factory_records)
        state._conn.set_trace_callback(
            after_ack_replay_trace.append
        )
        try:
            assert (
                await after_ack_adapter.apply_project_batch(
                    after_ack["batch_id"]
                )
            ).outcome == "already_discarded"
        finally:
            state._conn.set_trace_callback(None)
        assert len(after_ack_factory_records) == after_ack_factory_count
        assert_no_dml(after_ack_replay_trace)
        assert state._conn.total_changes == after_ack_replay_changes
        assert batch_snapshot(
            after_ack["batch_id"]
        ) == after_ack_replay_state
        assert projects_snapshot(
            projects, after_ack["project_id"]
        ) == after_ack_replay_projects
        assert_never_published(after_ack)
    finally:
        try:
            projects.close()
        finally:
            state.close()


@pytest.mark.asyncio
async def test_task7_c10_exact_checkpoint_publishes_context_before_fresh_fenced_rehydration(
    tmp_path,
    monkeypatch,
) -> None:
    """Approval context is durable once, before any critical recovery start."""
    from dataclasses import replace

    from hermes_cli import project_runtime_db as prdb
    from hermes_cli import projects_db
    from hermes_cli.project_operations import (
        ApprovalCheckpointIdentity,
        OperationApprovalSpec,
        OperationIntent,
        ProjectOperationGuard,
    )
    from hermes_cli.project_policy import ActorContext, Decision, PolicyDecision
    from hermes_cli.project_runtime import (
        CanonicalTurnResult,
        ProjectRuntime,
        ProjectRuntimeError,
        RuntimeErrorCode,
        TurnAttemptIdentity,
        TurnClaim,
        TurnReadbackResult,
    )
    from hermes_state import SessionDB

    state = SessionDB(db_path=tmp_path / "c10-state.db")
    projects_path = tmp_path / "c10-projects.db"
    conn = projects_db.connect(projects_path)
    loop_thread = threading.get_ident()
    ledger_lock = threading.Lock()
    ledger_violations = []
    active_transactions = {"state": set(), "projects": set()}
    state_statements = []
    main_project_statements = []
    protocol_order = []
    factory_records = []
    order_sequence = [0]

    def next_order(kind, detail, thread_id):
        with ledger_lock:
            order_sequence[0] += 1
            item = (order_sequence[0], kind, detail, thread_id)
            protocol_order.append(item)
            return item[0]

    def normalized(statement):
        return " ".join(statement.upper().split())

    def dml_categories(statements):
        categories = []
        for statement in statements:
            words = statement.strip().split()
            if not words:
                continue
            verb = words[0].upper()
            if verb == "UPDATE" and len(words) > 1:
                categories.append((verb, words[1].strip('"`[]').lower()))
            elif verb in {"INSERT", "REPLACE"} and len(words) > 2:
                categories.append(
                    (verb, words[2].strip('"`[]').lower())
                )
            elif verb == "DELETE" and len(words) > 2:
                categories.append(
                    (verb, words[2].strip('"`[]').lower())
                )
        return tuple(categories)

    def observe_transaction(domain, connection, statement, sink):
        sink.append(statement)
        compact = normalized(statement)
        thread_id = threading.get_ident()
        connection_id = id(connection)
        other = "projects" if domain == "state" else "state"
        with ledger_lock:
            if compact.startswith("BEGIN"):
                if active_transactions[other]:
                    ledger_violations.append(
                        (
                            "overlap-at-begin",
                            domain,
                            thread_id,
                            tuple(active_transactions[other]),
                        )
                    )
                active_transactions[domain].add(connection_id)
            elif compact in {"COMMIT", "ROLLBACK"}:
                active_transactions[domain].discard(connection_id)
            elif compact.startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            ):
                if active_transactions[other]:
                    ledger_violations.append(
                        (
                            "overlap-at-dml",
                            domain,
                            thread_id,
                            tuple(active_transactions[other]),
                        )
                    )
        if compact.startswith(
            ("INSERT", "UPDATE", "DELETE", "REPLACE")
        ):
            next_order(f"{domain}-dml", compact.split()[0], thread_id)

    def state_trace(statement):
        observe_transaction(
            "state",
            state._conn,
            statement,
            state_statements,
        )

    def main_project_trace(statement):
        observe_transaction(
            "projects",
            conn,
            statement,
            main_project_statements,
        )

    state._conn.set_trace_callback(state_trace)
    conn.set_trace_callback(main_project_trace)

    class RecordingProjectsConnection(sqlite3.Connection):
        def close(self):
            record = getattr(self, "c10_record", None)
            if record is not None and not record["closed"]:
                thread_id = threading.get_ident()
                if thread_id != record["owner_thread"]:
                    ledger_violations.append(
                        (
                            "close-thread",
                            thread_id,
                            record["owner_thread"],
                        )
                    )
                if self.in_transaction:
                    ledger_violations.append(
                        ("close-in-transaction", thread_id)
                    )
                record["close_order"] = next_order(
                    "projects-close",
                    record["ordinal"],
                    thread_id,
                )
                record["closed"] = True
                record["close_thread"] = thread_id
            super().close()

    def projects_factory():
        thread_id = threading.get_ident()
        with ledger_lock:
            if active_transactions["state"] or state._conn.in_transaction:
                ledger_violations.append(
                    (
                        "projects-open-during-state",
                        thread_id,
                        tuple(active_transactions["state"]),
                    )
                )
        connection = sqlite3.connect(
            str(projects_path),
            factory=RecordingProjectsConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        record = {
            "ordinal": len(factory_records) + 1,
            "connection": connection,
            "connection_id": id(connection),
            "owner_thread": thread_id,
            "open_order": next_order(
                "projects-open",
                len(factory_records) + 1,
                thread_id,
            ),
            "close_order": None,
            "closed": False,
            "close_thread": None,
            "statements": [],
        }
        connection.c10_record = record

        def trace(statement):
            observe_transaction(
                "projects",
                connection,
                statement,
                record["statements"],
            )

        connection.set_trace_callback(trace)
        factory_records.append(record)
        return connection

    def state_snapshot(session_id, batch_id):
        def rows(sql, parameters=()):
            return tuple(
                tuple(row)
                for row in state._conn.execute(sql, parameters)
            )

        return {
            "session": rows(
                """
                SELECT id, project_id, source, started_at, ended_at,
                       end_reason, message_count, tool_call_count,
                       input_tokens, output_tokens, cache_read_tokens,
                       cache_write_tokens, reasoning_tokens, api_call_count,
                       archived, pinned
                FROM sessions
                WHERE id = ?
                """,
                (session_id,),
            ),
            "messages": rows(
                """
                SELECT id, session_id, role, content, tool_call_id,
                       tool_calls, tool_name, effect_disposition, timestamp,
                       token_count, finish_reason, reasoning,
                       reasoning_content, reasoning_details,
                       codex_reasoning_items, codex_message_items,
                       platform_message_id, observed, active, compacted,
                       api_content, display_kind, display_metadata
                FROM messages
                WHERE session_id = ?
                ORDER BY id
                """,
                (session_id,),
            ),
            "batch": rows(
                """
                SELECT batch_id, batch_creation_sequence, kind, session_id,
                       project_id, turn_id, sequence, worker_id, attempt_id,
                       lease_generation, fencing_token, lease_expires_at,
                       terminal_status, operation_id, approval_id,
                       base_message_count, transcript_json,
                       transcript_sha256, state, created_at, published_at,
                       projects_acknowledged_at, transcript_conflict_key,
                       observed_message_count, remediated_at,
                       discard_authority
                FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ),
            "batch_counter": rows(
                """
                SELECT singleton, last_sequence
                FROM project_batch_sequence_counter
                ORDER BY singleton
                """
            ),
            "total_changes": state._conn.total_changes,
        }

    def projects_snapshot(project_id, turn_id):
        def rows(sql, parameters=()):
            return tuple(tuple(row) for row in conn.execute(sql, parameters))

        return {
            "state": rows(
                """
                SELECT project_id, lifecycle, current_phase, version,
                       conversation_root_id, conversation_tip_id,
                       dispatch_membership_sequence,
                       transcript_pending_batch_id,
                       transcript_dispatch_block_key, updated_at
                FROM project_runtime_state
                WHERE project_id = ?
                """,
                (project_id,),
            ),
            "turn": rows(
                """
                SELECT turn_id, project_id, sequence, status, attempt_id,
                       lease_generation, fencing_token, execution_state,
                       terminal_result_id, recovery_block_key,
                       transcript_applied_batch_id, updated_at
                FROM project_turns
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ),
            "control": rows(
                """
                SELECT turn_id, project_id, control_state, control_version,
                       attempt_id, claim_worker_id, claim_lease_expires_at,
                       claim_canonical_session_id, updated_at
                FROM project_run_controls
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ),
            "leases": rows(
                """
                SELECT lease_id, project_id, turn_id, worker_id,
                       lease_generation, fencing_token, expires_at, updated_at
                FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                ORDER BY lease_id
                """,
                (project_id, turn_id),
            ),
            "operations": rows(
                """
                SELECT operation_id, project_id, turn_id, approval_id,
                       status, guard_revision, guard_validated,
                       canonical_action, attempt_id, lease_generation,
                       fencing_token, receipt_id, blocked_reason,
                       approval_checkpoint_id, intent_event_id,
                       recovery_membership_sequence, updated_at
                FROM project_operations
                WHERE project_id = ?
                ORDER BY operation_id
                """,
                (project_id,),
            ),
            "approvals": rows(
                """
                SELECT approval_id, project_id, turn_id, operation_id,
                       operation_maintenance_seq, status, resolved_at,
                       resolved_by_actor_id, consumed_at
                FROM project_approvals
                WHERE project_id = ?
                ORDER BY approval_id
                """,
                (project_id,),
            ),
            "events": rows(
                """
                SELECT event_id, project_id, sequence, kind, turn_id,
                       payload_json, created_at
                FROM project_events
                WHERE project_id = ?
                ORDER BY sequence, event_id
                """,
                (project_id,),
            ),
            "deliveries": rows(
                """
                SELECT delivery_id, project_id, binding_id, event_id,
                       status, cursor, attempt_count, updated_at
                FROM project_deliveries
                WHERE project_id = ?
                ORDER BY delivery_id
                """,
                (project_id,),
            ),
            "membership": rows(
                """
                SELECT lane, last_sequence
                FROM project_runtime_membership_counters
                ORDER BY lane
                """
            ),
            "total_changes": conn.total_changes,
        }

    def setup(label, batch_id, *, with_operation=True, started=True):
        project_id = projects_db.create_project(
            conn, name=f"C10 {label}", folders=(f"C:/work/{label}",)
        )
        session_id = f"c10-{label}-session"
        binding_id = f"c10-{label}-owner"
        prdb.create_project_conversation(
            conn, project_id=project_id, conversation_id=session_id,
            current_phase="implementation", now=1,
        )
        prdb.bind_surface(
            conn, binding_id=binding_id, project_id=project_id,
            surface="desktop", external_binding_id=f"c10-{label}-window",
            actor_id="owner", now=1,
        )
        state.create_session(session_id, source="cli")
        now = [100]
        runtime = ProjectRuntime(conn, clock=lambda: now[0])
        actor = ActorContext("owner", "desktop", binding_id, True)
        turn = runtime.enqueue_turn(
            project_id, {"message": label}, actor,
            idempotency_key=f"c10-{label}-turn", expected_version=0,
        )
        claim = runtime.claim_next_turn(project_id, f"c10-{label}-worker", lease_seconds=30)
        assert claim is not None
        if started:
            claim = runtime.mark_turn_started(claim)
        operation_id = f"c10-{label}-operation"
        approval_id = f"c10-{label}-approval"
        transcript = (
            {
                "role": "user",
                "content": f"{label} approval request",
                "timestamp": 1.0,
            },
            {
                "role": "assistant",
                "content": f"{label} awaiting approval",
                "timestamp": 2.0,
            },
        )
        counter_before_prepare = state._conn.execute(
            """
            SELECT last_sequence
            FROM project_batch_sequence_counter
            WHERE singleton = 1
            """
        ).fetchone()[0]
        prepared = state.prepare_approval_checkpoint(
            claim, batch_id=batch_id, operation_id=operation_id,
            approval_id=approval_id, base_message_count=0,
            messages=transcript,
        )
        prepared_storage = state._conn.execute(
            """
            SELECT batch_creation_sequence, kind, state, operation_id,
                   approval_id
            FROM project_turn_transcript_batches
            WHERE batch_id = ?
            """,
            (batch_id,),
        ).fetchone()
        assert tuple(prepared_storage) == (
            counter_before_prepare + 1,
            "approval_checkpoint",
            "prepared",
            operation_id,
            approval_id,
        )
        assert state._conn.execute(
            """
            SELECT last_sequence
            FROM project_batch_sequence_counter
            WHERE singleton = 1
            """
        ).fetchone()[0] == counter_before_prepare + 1
        before_replay = state_snapshot(session_id, batch_id)
        replay_trace_start = len(state_statements)
        assert state.prepare_approval_checkpoint(
            claim, batch_id=batch_id, operation_id=operation_id,
            approval_id=approval_id, base_message_count=0,
            messages=transcript,
        ) == prepared
        assert state_snapshot(session_id, batch_id) == before_replay
        assert dml_categories(
            state_statements[replay_trace_start:]
        ) == ()
        guard = ProjectOperationGuard(runtime)
        operation = None
        if with_operation:
            # The durable State batch and sequence allocation precede the
            # first Projects operation transaction.
            assert state._conn.execute(
                """
                SELECT state FROM project_turn_transcript_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()[0] == "prepared"
            assert conn.execute(
                """
                SELECT COUNT(*) FROM project_operations
                WHERE project_id = ? AND operation_id = ?
                """,
                (project_id, operation_id),
            ).fetchone()[0] == 0
            operation = guard.prepare(
                claim,
                OperationIntent(
                    operation_id=operation_id, project_id=project_id, turn_id=turn.turn_id,
                    idempotency_key=f"c10-{label}-operation-key", canonical_action="publish",
                    command_revision=1, targets=(f"C:/work/{label}/release",),
                    batch_items=("publish",), payload={"content_digest": f"sha256:{label}"},
                    readback_kind="remote-ledger", remote_idempotency_supported=True,
                ),
                policy=PolicyDecision(
                    Decision.REQUIRE_APPROVAL, "policy.approval.publish",
                    "critical checkpoint", "publish",
                ),
                approval=OperationApprovalSpec(approval_id, "publish", 1_000, actor),
                approval_checkpoint_id=batch_id,
            )
        attempt = TurnAttemptIdentity(
            claim.project_id, claim.turn_id, claim.sequence, claim.worker_id,
            claim.attempt_id, claim.lease_generation, claim.fencing_token,
            claim.canonical_session_id, claim.lease_expires_at,
        )
        identity = ApprovalCheckpointIdentity(
            batch_id, attempt, operation_id, approval_id
        )
        assert identity.attempt == attempt
        assert state.publication_state(identity) == "waiting"
        missing_batch_id = (
            batch_id[:-1] + ("0" if batch_id[-1] != "0" else "1")
        )
        for mismatch in (
            ApprovalCheckpointIdentity(
                missing_batch_id,
                attempt,
                operation_id,
                approval_id,
            ),
            ApprovalCheckpointIdentity(
                batch_id,
                TurnAttemptIdentity(
                    attempt.project_id,
                    attempt.turn_id,
                    attempt.sequence,
                    attempt.worker_id,
                    f"{attempt.attempt_id}-wrong",
                    attempt.lease_generation,
                    attempt.fencing_token,
                    attempt.canonical_session_id,
                    attempt.lease_expires_at,
                ),
                operation_id,
                approval_id,
            ),
            ApprovalCheckpointIdentity(
                batch_id,
                TurnAttemptIdentity(
                    attempt.project_id,
                    attempt.turn_id,
                    attempt.sequence,
                    attempt.worker_id,
                    attempt.attempt_id,
                    attempt.lease_generation,
                    attempt.fencing_token,
                    attempt.canonical_session_id,
                    attempt.lease_expires_at + 1,
                ),
                operation_id,
                approval_id,
            ),
            ApprovalCheckpointIdentity(
                batch_id,
                attempt,
                f"{operation_id}-wrong",
                approval_id,
            ),
            ApprovalCheckpointIdentity(
                batch_id,
                attempt,
                operation_id,
                f"{approval_id}-wrong",
            ),
        ):
            assert state.publication_state(mismatch) == "permanent_conflict"
        return project_id, turn, claim, now, runtime, guard, actor, operation, prepared, identity

    try:
        adapter = AsyncSessionStore(
            state,
            projects_db_factory=projects_factory,
        )
        pre_operation = setup(
            "before-operation", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            with_operation=False,
        )
        pre_project, pre_turn, pre_claim, _, pre_runtime, _, pre_actor, _, pre_batch, pre_identity = pre_operation
        pre_session = pre_identity.attempt.canonical_session_id
        before_wait_state = state_snapshot(
            pre_session,
            pre_batch.batch_id,
        )
        before_wait_projects = projects_snapshot(
            pre_project,
            pre_turn.turn_id,
        )
        wait_factory_start = len(factory_records)
        wait_state_trace_start = len(state_statements)
        wait_project_trace_start = len(main_project_statements)
        assert (await adapter.apply_project_batch(pre_batch.batch_id)).outcome == "wait"
        assert state_snapshot(
            pre_session,
            pre_batch.batch_id,
        ) == before_wait_state
        assert projects_snapshot(
            pre_project,
            pre_turn.turn_id,
        ) == before_wait_projects
        assert dml_categories(
            state_statements[wait_state_trace_start:]
        ) == ()
        assert dml_categories(
            main_project_statements[wait_project_trace_start:]
        ) == ()
        wait_records = factory_records[wait_factory_start:]
        assert len(wait_records) == 1
        assert wait_records[0]["closed"]
        assert (
            wait_records[0]["owner_thread"]
            == wait_records[0]["close_thread"]
            != loop_thread
        )
        assert dml_categories(wait_records[0]["statements"]) == ()
        assert state.get_messages("c10-before-operation-session") == []
        pre_claim = pre_runtime.heartbeat_turn(
            pre_claim,
            lease_seconds=10_000,
        )
        pre_state = conn.execute(
            """
            SELECT version FROM project_runtime_state
            WHERE project_id = ?
            """,
            (pre_project,),
        ).fetchone()[0]
        pre_control = conn.execute(
            """
            SELECT control_version FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (pre_project, pre_turn.turn_id),
        ).fetchone()[0]
        pre_runtime.request_stop(
            pre_project, pre_turn.turn_id, pre_actor,
            idempotency_key="c10-before-operation-stop",
            expected_version=pre_state,
            expected_control_version=pre_control,
        )
        before_discard_state = state_snapshot(
            pre_session,
            pre_batch.batch_id,
        )
        before_discard_projects = projects_snapshot(
            pre_project,
            pre_turn.turn_id,
        )
        discard_factory_start = len(factory_records)
        discard_state_trace_start = len(state_statements)
        discard_project_trace_start = len(main_project_statements)
        discard_order_start = len(protocol_order)
        assert (await adapter.apply_project_batch(pre_batch.batch_id)).outcome == "discarded"
        discard_records = factory_records[discard_factory_start:]
        assert len(discard_records) == 1
        assert discard_records[0]["closed"]
        assert dml_categories(discard_records[0]["statements"]) == ()
        discard_after_state = state_snapshot(
            pre_session,
            pre_batch.batch_id,
        )
        assert discard_after_state["session"] == (
            before_discard_state["session"]
        )
        assert discard_after_state["messages"] == ()
        assert discard_after_state["batch_counter"] == (
            before_discard_state["batch_counter"]
        )
        discard_row = discard_after_state["batch"][0]
        assert discard_row[18:] == (
            "discarded",
            discard_row[19],
            None,
            None,
            None,
            None,
            None,
            "stop_requested",
        )
        assert projects_snapshot(
            pre_project,
            pre_turn.turn_id,
        ) == before_discard_projects
        assert dml_categories(
            main_project_statements[discard_project_trace_start:]
        ) == ()
        discard_state_dml = dml_categories(
            state_statements[discard_state_trace_start:]
        )
        assert discard_state_dml
        discard_order = protocol_order[discard_order_start:]
        first_state_dml_order = min(
            item[0]
            for item in discard_order
            if item[1] == "state-dml"
        )
        assert (
            discard_records[0]["close_order"]
            < first_state_dml_order
        )

        replay_discard_state = state_snapshot(
            pre_session,
            pre_batch.batch_id,
        )
        replay_discard_projects = projects_snapshot(
            pre_project,
            pre_turn.turn_id,
        )
        replay_discard_factory = len(factory_records)
        replay_discard_state_trace = len(state_statements)
        replay_discard_project_trace = len(main_project_statements)
        assert (
            await adapter.apply_project_batch(pre_batch.batch_id)
        ).outcome == "already_discarded"
        assert len(factory_records) == replay_discard_factory
        assert state_snapshot(
            pre_session,
            pre_batch.batch_id,
        ) == replay_discard_state
        assert projects_snapshot(
            pre_project,
            pre_turn.turn_id,
        ) == replay_discard_projects
        assert dml_categories(
            state_statements[replay_discard_state_trace:]
        ) == ()
        assert dml_categories(
            main_project_statements[replay_discard_project_trace:]
        ) == ()
        assert state.publication_state(pre_identity) == "permanent_conflict"

        async def assert_discard_carrier(case, carrier):
            (
                carrier_project,
                carrier_turn,
                _,
                _,
                _,
                _,
                _,
                _,
                carrier_batch,
                carrier_identity,
            ) = case
            carrier_session = (
                carrier_identity.attempt.canonical_session_id
            )
            before_state = state_snapshot(
                carrier_session,
                carrier_batch.batch_id,
            )
            before_projects = projects_snapshot(
                carrier_project,
                carrier_turn.turn_id,
            )
            assert before_state["messages"] == ()
            factory_start = len(factory_records)
            state_trace_start = len(state_statements)
            project_trace_start = len(main_project_statements)
            order_start = len(protocol_order)

            first = await adapter.apply_project_batch(
                carrier_batch.batch_id
            )

            assert first.outcome == "discarded"
            records = factory_records[factory_start:]
            assert len(records) == 1
            assert records[0]["closed"]
            assert (
                records[0]["owner_thread"]
                == records[0]["close_thread"]
                != loop_thread
            )
            assert dml_categories(records[0]["statements"]) == ()
            after_state = state_snapshot(
                carrier_session,
                carrier_batch.batch_id,
            )
            assert after_state["session"] == before_state["session"]
            assert after_state["messages"] == ()
            assert (
                after_state["batch_counter"]
                == before_state["batch_counter"]
            )
            assert tuple(
                state._conn.execute(
                    """
                    SELECT state, published_at,
                           projects_acknowledged_at,
                           transcript_conflict_key,
                           observed_message_count, remediated_at,
                           discard_authority
                    FROM project_turn_transcript_batches
                    WHERE batch_id = ?
                    """,
                    (carrier_batch.batch_id,),
                ).fetchone()
            ) == (
                "discarded",
                None,
                None,
                None,
                None,
                None,
                carrier,
            )
            assert projects_snapshot(
                carrier_project,
                carrier_turn.turn_id,
            ) == before_projects
            assert dml_categories(
                main_project_statements[project_trace_start:]
            ) == ()
            state_dml = dml_categories(
                state_statements[state_trace_start:]
            )
            assert state_dml
            first_state_dml_order = min(
                item[0]
                for item in protocol_order[order_start:]
                if item[1] == "state-dml"
            )
            assert records[0]["close_order"] < first_state_dml_order

            replay_state = state_snapshot(
                carrier_session,
                carrier_batch.batch_id,
            )
            replay_projects = projects_snapshot(
                carrier_project,
                carrier_turn.turn_id,
            )
            replay_factory = len(factory_records)
            replay_state_trace = len(state_statements)
            replay_project_trace = len(main_project_statements)

            replay = await adapter.apply_project_batch(
                carrier_batch.batch_id
            )

            assert replay.outcome == "already_discarded"
            assert len(factory_records) == replay_factory
            assert state_snapshot(
                carrier_session,
                carrier_batch.batch_id,
            ) == replay_state
            assert projects_snapshot(
                carrier_project,
                carrier_turn.turn_id,
            ) == replay_projects
            assert dml_categories(
                state_statements[replay_state_trace:]
            ) == ()
            assert dml_categories(
                main_project_statements[replay_project_trace:]
            ) == ()
            assert (
                state.publication_state(carrier_identity)
                == "permanent_conflict"
            )

        def retired_attempt_payload(attempt):
            return {
                "project_id": attempt.project_id,
                "turn_id": attempt.turn_id,
                "sequence": attempt.sequence,
                "worker_id": attempt.worker_id,
                "attempt_id": attempt.attempt_id,
                "lease_generation": attempt.lease_generation,
                "fencing_token": attempt.fencing_token,
                "canonical_session_id": attempt.canonical_session_id,
                "lease_expires_at": attempt.lease_expires_at,
            }

        def requeued_attempt_certificate(
            project_id,
            turn_id,
            identity,
            certified_claim,
        ):
            snapshot = projects_snapshot(project_id, turn_id)
            requeued_events = tuple(
                event
                for event in snapshot["events"]
                if event[3] == "turn.requeued"
                and event[4] == turn_id
            )
            assert len(requeued_events) == 1
            requeued_event = requeued_events[0]
            payload = json.loads(requeued_event[5])
            nested_attempt = payload.get("attempt")
            assert type(nested_attempt) is dict
            expected_attempt = retired_attempt_payload(identity.attempt)
            expected_attempt["lease_expires_at"] = (
                certified_claim.lease_expires_at
            )
            assert nested_attempt == expected_attempt
            assert set(nested_attempt) == set(retired_attempt_payload(
                identity.attempt
            ))
            return snapshot, requeued_event, nested_attempt

        def assert_public_pre_operation_discard(
            project_id,
            turn_id,
            guard,
            identity,
            *,
            certified_horizon,
        ):
            before = projects_snapshot(project_id, turn_id)
            before_changes = conn.total_changes
            trace_start = len(main_project_statements)

            assert (
                guard.resolve_approval_checkpoint(identity).action
                == "discard"
            )
            certified_identity = replace(
                identity,
                attempt=replace(
                    identity.attempt,
                    lease_expires_at=certified_horizon,
                ),
            )
            assert (
                guard.resolve_approval_checkpoint(
                    certified_identity
                ).action
                == "discard"
            )
            for field_name, forged_value in (
                (
                    "attempt_id",
                    f"{identity.attempt.attempt_id}-forged",
                ),
                (
                    "worker_id",
                    f"{identity.attempt.worker_id}-forged",
                ),
                (
                    "sequence",
                    identity.attempt.sequence + 1,
                ),
                (
                    "canonical_session_id",
                    f"{identity.attempt.canonical_session_id}-forged",
                ),
                (
                    "lease_generation",
                    identity.attempt.lease_generation + 1,
                ),
                (
                    "fencing_token",
                    identity.attempt.fencing_token + 1,
                ),
                (
                    "lease_expires_at",
                    certified_horizon + 1,
                ),
            ):
                forged_identity = replace(
                    identity,
                    attempt=replace(
                        identity.attempt,
                        **{field_name: forged_value},
                    ),
                )
                with pytest.raises(
                    ProjectRuntimeError
                ) as authority_conflict:
                    guard.resolve_approval_checkpoint(
                        forged_identity
                    )
                assert (
                    authority_conflict.value.code
                    is RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
                )

            assert projects_snapshot(project_id, turn_id) == before
            assert conn.total_changes == before_changes
            assert dml_categories(
                main_project_statements[trace_start:]
            ) == ()
            assert conn.in_transaction is False

        def assert_projects_call_write_free_conflict(
            project_id,
            turn_id,
            call,
        ):
            before = projects_snapshot(project_id, turn_id)
            before_changes = conn.total_changes
            before_event_count = len(before["events"])
            trace_start = len(main_project_statements)
            with pytest.raises(ProjectRuntimeError) as conflict:
                call()
            assert (
                conflict.value.code
                is RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            after = projects_snapshot(project_id, turn_id)
            assert after == before
            assert len(after["events"]) == before_event_count
            assert conn.total_changes == before_changes
            assert dml_categories(
                main_project_statements[trace_start:]
            ) == ()
            assert conn.in_transaction is False

        def assert_resolver_write_free_conflict(
            project_id,
            turn_id,
            guard,
            identity,
        ):
            assert_projects_call_write_free_conflict(
                project_id,
                turn_id,
                lambda: guard.resolve_approval_checkpoint(identity),
            )

        def assert_committed_fixture_conflict(
            project_id,
            turn_id,
            guard,
            identity,
            install,
            restore,
        ):
            original = projects_snapshot(project_id, turn_id)
            with prdb.write_transaction(conn):
                install()
            try:
                assert_resolver_write_free_conflict(
                    project_id,
                    turn_id,
                    guard,
                    identity,
                )
            finally:
                with prdb.write_transaction(conn):
                    restore()
            restored = projects_snapshot(project_id, turn_id)
            assert {
                key: value
                for key, value in restored.items()
                if key != "total_changes"
            } == {
                key: value
                for key, value in original.items()
                if key != "total_changes"
            }
            assert conn.in_transaction is False

        def update_event_payload(event_id, payload):
            conn.execute(
                """
                UPDATE project_events
                SET payload_json = ?
                WHERE event_id = ?
                """,
                (
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    event_id,
                ),
            )

        def update_event_sequence(event_id, sequence):
            conn.execute(
                """
                UPDATE project_events
                SET sequence = ?
                WHERE event_id = ?
                """,
                (sequence, event_id),
            )

        def restore_event(event):
            conn.execute(
                """
                INSERT INTO project_events (
                    event_id, project_id, sequence, kind, turn_id,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                event,
            )

        class NoNotStartedReadback:
            def read_turn(self, request):
                raise AssertionError(
                    "not-started recovery must not call readback"
                )

        cancelled_case = setup(
            "discard-cancelled",
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            with_operation=False,
            started=False,
        )
        (
            cancelled_project,
            cancelled_turn,
            cancelled_claim,
            cancelled_now,
            cancelled_runtime,
            cancelled_guard,
            cancelled_actor,
            _,
            cancelled_batch,
            cancelled_identity,
        ) = cancelled_case
        cancelled_prepared_horizon = (
            cancelled_identity.attempt.lease_expires_at
        )
        cancelled_claim = cancelled_runtime.heartbeat_turn(
            cancelled_claim,
            lease_seconds=60,
        )
        assert (
            cancelled_prepared_horizon
            < cancelled_claim.lease_expires_at
        )
        cancelled_now[0] = cancelled_claim.lease_expires_at
        cancelled_requeued = cancelled_runtime.reconcile_inflight_turns(
            NoNotStartedReadback(),
            limit=10,
        )
        assert len(cancelled_requeued) == 1
        assert cancelled_requeued[0].status == "queued"
        (
            _,
            cancelled_requeued_event,
            cancelled_certified_attempt,
        ) = requeued_attempt_certificate(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_identity,
            cancelled_claim,
        )
        assert (
            cancelled_prepared_horizon
            < cancelled_certified_attempt["lease_expires_at"]
        )
        cancelled_version = conn.execute(
            """
            SELECT version FROM project_runtime_state
            WHERE project_id = ?
            """,
            (cancelled_project,),
        ).fetchone()[0]
        cancelled_control_version = conn.execute(
            """
            SELECT control_version FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (cancelled_project, cancelled_turn.turn_id),
        ).fetchone()[0]
        cancelled_requeued_payload = cancelled_requeued_event[5]
        malformed_cancel_payload = json.loads(
            cancelled_requeued_payload
        )
        malformed_cancel_payload["attempt"].pop("worker_id")
        with prdb.write_transaction(conn):
            update_event_payload(
                cancelled_requeued_event[0],
                malformed_cancel_payload,
            )
        try:
            assert_projects_call_write_free_conflict(
                cancelled_project,
                cancelled_turn.turn_id,
                lambda: cancelled_runtime.cancel_queued_turn(
                    cancelled_project,
                    cancelled_turn.turn_id,
                    cancelled_actor,
                    idempotency_key="c10-discard-cancelled",
                    expected_version=cancelled_version,
                    expected_control_version=(
                        cancelled_control_version
                    ),
                ),
            )
        finally:
            with prdb.write_transaction(conn):
                conn.execute(
                    """
                    UPDATE project_events
                    SET payload_json = ?
                    WHERE event_id = ?
                    """,
                    (
                        cancelled_requeued_payload,
                        cancelled_requeued_event[0],
                    ),
                )
        cancelled_result = cancelled_runtime.cancel_queued_turn(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_actor,
            idempotency_key="c10-discard-cancelled",
            expected_version=cancelled_version,
            expected_control_version=cancelled_control_version,
        )
        assert cancelled_result.status == "cancelled"
        cancelled_session = cancelled_identity.attempt.canonical_session_id
        incomplete_requeued_payload = json.loads(
            cancelled_requeued_payload
        )
        incomplete_requeued_payload["attempt"].pop("lease_expires_at")
        with prdb.write_transaction(conn):
            conn.execute(
                """
                UPDATE project_events
                SET payload_json = ?
                WHERE project_id = ? AND event_id = ?
                """,
                (
                    json.dumps(
                        incomplete_requeued_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    cancelled_project,
                    cancelled_requeued_event[0],
                ),
            )
        cancelled_replay_state = state_snapshot(
            cancelled_session,
            cancelled_batch.batch_id,
        )
        cancelled_replay_projects = projects_snapshot(
            cancelled_project,
            cancelled_turn.turn_id,
        )
        cancelled_replay_state_changes = state._conn.total_changes
        cancelled_replay_project_changes = conn.total_changes
        cancelled_replay_event_count = len(
            cancelled_replay_projects["events"]
        )
        cancelled_replay_state_trace = len(state_statements)
        cancelled_replay_project_trace = len(main_project_statements)
        cancelled_replay_event_reads = []

        def cancelled_replay_authorizer(
            action,
            arg1,
            arg2,
            database,
            trigger,
        ):
            if action == sqlite3.SQLITE_READ and arg1 == "project_events":
                cancelled_replay_event_reads.append(
                    (arg1, arg2, database, trigger)
                )
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(cancelled_replay_authorizer)
        try:
            assert cancelled_runtime.cancel_queued_turn(
                cancelled_project,
                cancelled_turn.turn_id,
                cancelled_actor,
                idempotency_key="c10-discard-cancelled",
                expected_version=cancelled_version,
                expected_control_version=cancelled_control_version,
            ) == cancelled_result
        finally:
            conn.set_authorizer(None)
        assert cancelled_replay_event_reads == []
        assert state_snapshot(
            cancelled_session,
            cancelled_batch.batch_id,
        ) == cancelled_replay_state
        replayed_cancelled_projects = projects_snapshot(
            cancelled_project,
            cancelled_turn.turn_id,
        )
        assert replayed_cancelled_projects == cancelled_replay_projects
        assert len(replayed_cancelled_projects["events"]) == (
            cancelled_replay_event_count
        )
        assert state._conn.total_changes == cancelled_replay_state_changes
        assert conn.total_changes == cancelled_replay_project_changes
        assert dml_categories(
            state_statements[cancelled_replay_state_trace:]
        ) == ()
        assert dml_categories(
            main_project_statements[cancelled_replay_project_trace:]
        ) == ()
        with prdb.write_transaction(conn):
            conn.execute(
                """
                UPDATE project_events
                SET payload_json = ?
                WHERE project_id = ? AND event_id = ?
                """,
                (
                    cancelled_requeued_payload,
                    cancelled_project,
                    cancelled_requeued_event[0],
                ),
            )
        (
            cancelled_certificate_snapshot,
            cancelled_requeued_event,
            cancelled_certified_attempt,
        ) = requeued_attempt_certificate(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_identity,
            cancelled_claim,
        )
        cancelled_events = cancelled_certificate_snapshot["events"]
        cancelled_attempt_a_events = tuple(
            event
            for event in cancelled_events
            if event[4] == cancelled_turn.turn_id
            and event[3] in {
                "turn.claimed",
                "turn.reconciling",
                "turn.requeued",
            }
        )
        assert tuple(event[3] for event in cancelled_attempt_a_events) == (
            "turn.claimed",
            "turn.reconciling",
            "turn.requeued",
        )
        assert [event[2] for event in cancelled_attempt_a_events] == sorted(
            event[2] for event in cancelled_attempt_a_events
        )
        assert len({event[2] for event in cancelled_attempt_a_events}) == 3
        cancelled_attempt_a_payloads = tuple(
            json.loads(event[5]) for event in cancelled_attempt_a_events
        )
        assert [payload["version"] for payload in cancelled_attempt_a_payloads] == sorted(
            payload["version"] for payload in cancelled_attempt_a_payloads
        )
        assert len(
            {
                payload["version"]
                for payload in cancelled_attempt_a_payloads
            }
        ) == 3
        assert [event[6] for event in cancelled_attempt_a_events] == [
            cancelled_prepared_horizon - 30,
            cancelled_claim.lease_expires_at,
            cancelled_claim.lease_expires_at,
        ]
        assert cancelled_attempt_a_payloads[0] == {
            "attempt_id": cancelled_identity.attempt.attempt_id,
            "fencing_token": cancelled_identity.attempt.fencing_token,
            "lease_generation": cancelled_identity.attempt.lease_generation,
            "sequence": cancelled_identity.attempt.sequence,
            "turn_id": cancelled_turn.turn_id,
            "version": cancelled_attempt_a_payloads[0]["version"],
        }
        assert cancelled_attempt_a_payloads[1]["source_status"] == "claimed"
        assert cancelled_attempt_a_payloads[2]["source_status"] == "claimed"
        for payload in cancelled_attempt_a_payloads[1:]:
            assert {
                key: payload[key]
                for key in (
                    "attempt_id",
                    "fencing_token",
                    "lease_generation",
                    "turn_id",
                )
            } == {
                "attempt_id": cancelled_identity.attempt.attempt_id,
                "fencing_token": cancelled_identity.attempt.fencing_token,
                "lease_generation": cancelled_identity.attempt.lease_generation,
                "turn_id": cancelled_turn.turn_id,
            }
        cancelled_events_for_turn = tuple(
            event
            for event in cancelled_events
            if event[4] == cancelled_turn.turn_id
            and event[3] == "turn.cancelled"
        )
        assert len(cancelled_events_for_turn) == 1
        cancelled_event = cancelled_events_for_turn[0]
        assert (
            cancelled_requeued_event[2]
            < cancelled_event[2]
        )
        cancelled_payload = json.loads(cancelled_event[5])
        assert (
            cancelled_payload.get("retired_attempt_event_id")
            == cancelled_requeued_event[0]
        )
        assert not any(
            event[4] == cancelled_turn.turn_id
            and event[3] in {"turn.claimed", "turn.requeued"}
            and cancelled_requeued_event[2]
            < event[2]
            < cancelled_event[2]
            for event in cancelled_events
        )
        malformed_nested_payload = json.loads(
            cancelled_requeued_event[5]
        )
        malformed_nested_payload["attempt"]["unexpected"] = True
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: update_event_payload(
                cancelled_requeued_event[0],
                malformed_nested_payload,
            ),
            lambda: conn.execute(
                """
                UPDATE project_events SET payload_json = ?
                WHERE event_id = ?
                """,
                (
                    cancelled_requeued_event[5],
                    cancelled_requeued_event[0],
                ),
            ),
        )
        for missing_event in cancelled_attempt_a_events:
            assert_committed_fixture_conflict(
                cancelled_project,
                cancelled_turn.turn_id,
                cancelled_guard,
                cancelled_identity,
                lambda event=missing_event: conn.execute(
                    "DELETE FROM project_events WHERE event_id = ?",
                    (event[0],),
                ),
                lambda event=missing_event: restore_event(event),
            )

        duplicate_requeue_event = (
            f"{cancelled_requeued_event[0]}-duplicate",
            cancelled_project,
            max(event[2] for event in cancelled_events) + 1,
            cancelled_requeued_event[3],
            cancelled_requeued_event[4],
            cancelled_requeued_event[5],
            cancelled_requeued_event[6],
        )
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: restore_event(duplicate_requeue_event),
            lambda: conn.execute(
                "DELETE FROM project_events WHERE event_id = ?",
                (duplicate_requeue_event[0],),
            ),
        )
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: conn.execute(
                "DELETE FROM project_events WHERE event_id = ?",
                (cancelled_event[0],),
            ),
            lambda: restore_event(cancelled_event),
        )
        duplicate_cancelled_event = (
            f"{cancelled_event[0]}-duplicate",
            cancelled_project,
            max(event[2] for event in cancelled_events) + 1,
            "turn.cancelled",
            cancelled_turn.turn_id,
            cancelled_event[5],
            cancelled_event[6],
        )
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: restore_event(duplicate_cancelled_event),
            lambda: conn.execute(
                "DELETE FROM project_events WHERE event_id = ?",
                (duplicate_cancelled_event[0],),
            ),
        )

        claimed_a_event, reconciling_a_event, _ = (
            cancelled_attempt_a_events
        )

        def mismatched_payload_value(value):
            if type(value) is int:
                return value + 1
            return f"{value}-wrong"

        claimed_a_payload = json.loads(claimed_a_event[5])
        for claimed_field in (
            "attempt_id",
            "sequence",
            "lease_generation",
            "fencing_token",
            "turn_id",
        ):
            malformed_claimed_payload = dict(claimed_a_payload)
            malformed_claimed_payload[claimed_field] = (
                mismatched_payload_value(
                    claimed_a_payload[claimed_field]
                )
            )
            assert_committed_fixture_conflict(
                cancelled_project,
                cancelled_turn.turn_id,
                cancelled_guard,
                cancelled_identity,
                lambda payload=malformed_claimed_payload: (
                    update_event_payload(
                        claimed_a_event[0],
                        payload,
                    )
                ),
                lambda: conn.execute(
                    """
                    UPDATE project_events SET payload_json = ?
                    WHERE event_id = ?
                    """,
                    (claimed_a_event[5], claimed_a_event[0]),
                ),
            )

        reconciling_a_payload = json.loads(
            reconciling_a_event[5]
        )
        reconciling_identity_fields = tuple(
            field
            for field in (
                "project_id",
                "turn_id",
                "sequence",
                "worker_id",
                "attempt_id",
                "lease_generation",
                "fencing_token",
                "canonical_session_id",
                "lease_expires_at",
            )
            if field in reconciling_a_payload
        )
        assert {
            "attempt_id",
            "lease_generation",
            "fencing_token",
            "turn_id",
        }.issubset(reconciling_identity_fields)
        for reconciling_field in reconciling_identity_fields:
            malformed_reconciling_payload = dict(
                reconciling_a_payload
            )
            malformed_reconciling_payload[reconciling_field] = (
                mismatched_payload_value(
                    reconciling_a_payload[reconciling_field]
                )
            )
            assert_committed_fixture_conflict(
                cancelled_project,
                cancelled_turn.turn_id,
                cancelled_guard,
                cancelled_identity,
                lambda payload=malformed_reconciling_payload: (
                    update_event_payload(
                        reconciling_a_event[0],
                        payload,
                    )
                ),
                lambda: conn.execute(
                    """
                    UPDATE project_events SET payload_json = ?
                    WHERE event_id = ?
                    """,
                    (
                        reconciling_a_event[5],
                        reconciling_a_event[0],
                    ),
                ),
            )

        requeued_a_payload = json.loads(
            cancelled_requeued_event[5]
        )
        requeued_identity_fields = tuple(
            field
            for field in (
                "project_id",
                "turn_id",
                "sequence",
                "worker_id",
                "attempt_id",
                "lease_generation",
                "fencing_token",
                "canonical_session_id",
                "lease_expires_at",
                "source_status",
            )
            if field in requeued_a_payload
        )
        assert {
            "attempt_id",
            "lease_generation",
            "fencing_token",
            "turn_id",
            "source_status",
        }.issubset(requeued_identity_fields)
        for requeued_field in requeued_identity_fields:
            malformed_requeued_payload = dict(requeued_a_payload)
            malformed_requeued_payload[requeued_field] = (
                mismatched_payload_value(
                    requeued_a_payload[requeued_field]
                )
            )
            assert_committed_fixture_conflict(
                cancelled_project,
                cancelled_turn.turn_id,
                cancelled_guard,
                cancelled_identity,
                lambda payload=malformed_requeued_payload: (
                    update_event_payload(
                        cancelled_requeued_event[0],
                        payload,
                    )
                ),
                lambda: conn.execute(
                    """
                    UPDATE project_events SET payload_json = ?
                    WHERE event_id = ?
                    """,
                    (
                        cancelled_requeued_event[5],
                        cancelled_requeued_event[0],
                    ),
                ),
            )

        event_sequence_scratch = (
            max(event[2] for event in cancelled_events) + 100
        )

        def install_out_of_order_a():
            update_event_sequence(
                claimed_a_event[0],
                event_sequence_scratch,
            )
            update_event_sequence(
                reconciling_a_event[0],
                claimed_a_event[2],
            )
            update_event_sequence(
                claimed_a_event[0],
                reconciling_a_event[2],
            )

        def restore_ordered_a():
            update_event_sequence(
                claimed_a_event[0],
                event_sequence_scratch,
            )
            update_event_sequence(
                reconciling_a_event[0],
                reconciling_a_event[2],
            )
            update_event_sequence(
                claimed_a_event[0],
                claimed_a_event[2],
            )

        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            install_out_of_order_a,
            restore_ordered_a,
        )

        def install_requeue_before_reconciling():
            update_event_sequence(
                reconciling_a_event[0],
                event_sequence_scratch,
            )
            update_event_sequence(
                cancelled_requeued_event[0],
                reconciling_a_event[2],
            )
            update_event_sequence(
                reconciling_a_event[0],
                cancelled_requeued_event[2],
            )

        def restore_reconciling_before_requeue():
            update_event_sequence(
                reconciling_a_event[0],
                event_sequence_scratch,
            )
            update_event_sequence(
                cancelled_requeued_event[0],
                cancelled_requeued_event[2],
            )
            update_event_sequence(
                reconciling_a_event[0],
                reconciling_a_event[2],
            )

        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            install_requeue_before_reconciling,
            restore_reconciling_before_requeue,
        )
        wrong_source_payload = json.loads(reconciling_a_event[5])
        wrong_source_payload["source_status"] = "stop_requested"
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: update_event_payload(
                reconciling_a_event[0],
                wrong_source_payload,
            ),
            lambda: conn.execute(
                """
                UPDATE project_events SET payload_json = ?
                WHERE event_id = ?
                """,
                (reconciling_a_event[5], reconciling_a_event[0]),
            ),
        )
        nonmonotone_version_payload = json.loads(
            reconciling_a_event[5]
        )
        nonmonotone_version_payload["version"] = (
            cancelled_attempt_a_payloads[0]["version"]
        )
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: update_event_payload(
                reconciling_a_event[0],
                nonmonotone_version_payload,
            ),
            lambda: conn.execute(
                """
                UPDATE project_events SET payload_json = ?
                WHERE event_id = ?
                """,
                (reconciling_a_event[5], reconciling_a_event[0]),
            ),
        )
        for invalid_requeued_version in (
            reconciling_a_payload["version"],
            reconciling_a_payload["version"] - 1,
        ):
            nonmonotone_requeued_payload = dict(
                requeued_a_payload
            )
            nonmonotone_requeued_payload["version"] = (
                invalid_requeued_version
            )
            assert_committed_fixture_conflict(
                cancelled_project,
                cancelled_turn.turn_id,
                cancelled_guard,
                cancelled_identity,
                lambda payload=nonmonotone_requeued_payload: (
                    update_event_payload(
                        cancelled_requeued_event[0],
                        payload,
                    )
                ),
                lambda: conn.execute(
                    """
                    UPDATE project_events SET payload_json = ?
                    WHERE event_id = ?
                    """,
                    (
                        cancelled_requeued_event[5],
                        cancelled_requeued_event[0],
                    ),
                ),
            )
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: conn.execute(
                """
                UPDATE project_events SET created_at = ?
                WHERE event_id = ?
                """,
                (claimed_a_event[6] - 1, reconciling_a_event[0]),
            ),
            lambda: conn.execute(
                """
                UPDATE project_events SET created_at = ?
                WHERE event_id = ?
                """,
                (reconciling_a_event[6], reconciling_a_event[0]),
            ),
        )
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: conn.execute(
                """
                UPDATE project_events SET created_at = ?
                WHERE event_id = ?
                """,
                (
                    reconciling_a_event[6] - 1,
                    cancelled_requeued_event[0],
                ),
            ),
            lambda: conn.execute(
                """
                UPDATE project_events SET created_at = ?
                WHERE event_id = ?
                """,
                (
                    cancelled_requeued_event[6],
                    cancelled_requeued_event[0],
                ),
            ),
        )
        wrong_reference_payload = dict(cancelled_payload)
        wrong_reference_payload["retired_attempt_event_id"] = (
            f"{cancelled_requeued_event[0]}-wrong"
        )
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: update_event_payload(
                cancelled_event[0],
                wrong_reference_payload,
            ),
            lambda: conn.execute(
                """
                UPDATE project_events SET payload_json = ?
                WHERE event_id = ?
                """,
                (cancelled_event[5], cancelled_event[0]),
            ),
        )

        authority_event_id = f"{cancelled_event[0]}-intervening"
        authority_event_sequence = cancelled_event[2]
        authority_sequence_scratch = (
            max(event[2] for event in cancelled_events) + 200
        )

        def install_intervening_authority_event():
            update_event_sequence(
                cancelled_event[0],
                authority_sequence_scratch,
            )
            restore_event(
                (
                    authority_event_id,
                    cancelled_project,
                    authority_event_sequence,
                    "run.stop_requested",
                    cancelled_turn.turn_id,
                    json.dumps(
                        {
                            "turn_id": cancelled_turn.turn_id,
                            "version": cancelled_payload["version"],
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    cancelled_event[6],
                )
            )
            update_event_sequence(
                cancelled_event[0],
                authority_event_sequence + 1,
            )

        def restore_uninterrupted_authority():
            conn.execute(
                "DELETE FROM project_events WHERE event_id = ?",
                (authority_event_id,),
            )
            update_event_sequence(
                cancelled_event[0],
                authority_event_sequence,
            )

        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            install_intervening_authority_event,
            restore_uninterrupted_authority,
        )

        # A recovered cancellation is the terminal authority tip.  A later
        # well-formed same-turn claim is not an historical detail: it makes
        # the public checkpoint proof unverifiable while the projection says
        # terminal.
        later_runtime_state = conn.execute(
            """
            SELECT version, updated_at FROM project_runtime_state
            WHERE project_id = ?
            """,
            (cancelled_project,),
        ).fetchone()
        assert later_runtime_state is not None
        later_claim_created_at = max(
            cancelled_event[6] + 1,
            later_runtime_state[1] + 1,
        )
        later_claim_payload = json.loads(claimed_a_event[5])
        later_claim_payload.update(
            {
                "attempt_id": (
                    f"{cancelled_identity.attempt.attempt_id}-later"
                ),
                "lease_generation": (
                    cancelled_identity.attempt.lease_generation + 1
                ),
                "fencing_token": (
                    cancelled_identity.attempt.fencing_token + 1
                ),
                "version": later_runtime_state[0] + 1,
            }
        )
        later_claim_event = (
            f"{cancelled_event[0]}-later-claim",
            cancelled_project,
            max(event[2] for event in cancelled_events) + 1,
            "turn.claimed",
            cancelled_turn.turn_id,
            json.dumps(
                later_claim_payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
            later_claim_created_at,
        )
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: (
                restore_event(later_claim_event),
                conn.execute(
                    """
                    UPDATE project_runtime_state
                    SET version = ?, updated_at = ?
                    WHERE project_id = ?
                    """,
                    (
                        later_claim_payload["version"],
                        later_claim_created_at,
                        cancelled_project,
                    ),
                ),
            ),
            lambda: (
                conn.execute(
                    "DELETE FROM project_events WHERE event_id = ?",
                    (later_claim_event[0],),
                ),
                conn.execute(
                    """
                    UPDATE project_runtime_state
                    SET version = ?, updated_at = ?
                    WHERE project_id = ?
                    """,
                    (
                        later_runtime_state[0],
                        later_runtime_state[1],
                        cancelled_project,
                    ),
                ),
            ),
        )

        # Event-to-projection order is part of the same proof.  Check each
        # terminal projection independently so one current timestamp cannot
        # mask another stale projection row.
        cancelled_created_at = cancelled_event[6]
        terminal_turn_updated_at = conn.execute(
            """
            SELECT updated_at FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (cancelled_project, cancelled_turn.turn_id),
        ).fetchone()[0]
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: conn.execute(
                """
                UPDATE project_turns SET updated_at = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    cancelled_created_at - 1,
                    cancelled_project,
                    cancelled_turn.turn_id,
                ),
            ),
            lambda: conn.execute(
                """
                UPDATE project_turns SET updated_at = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    terminal_turn_updated_at,
                    cancelled_project,
                    cancelled_turn.turn_id,
                ),
            ),
        )
        terminal_control_updated_at = conn.execute(
            """
            SELECT updated_at FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (cancelled_project, cancelled_turn.turn_id),
        ).fetchone()[0]
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: conn.execute(
                """
                UPDATE project_run_controls SET updated_at = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    cancelled_created_at - 1,
                    cancelled_project,
                    cancelled_turn.turn_id,
                ),
            ),
            lambda: conn.execute(
                """
                UPDATE project_run_controls SET updated_at = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    terminal_control_updated_at,
                    cancelled_project,
                    cancelled_turn.turn_id,
                ),
            ),
        )
        terminal_runtime_updated_at = conn.execute(
            """
            SELECT updated_at FROM project_runtime_state
            WHERE project_id = ?
            """,
            (cancelled_project,),
        ).fetchone()[0]
        assert_committed_fixture_conflict(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            lambda: conn.execute(
                """
                UPDATE project_runtime_state SET updated_at = ?
                WHERE project_id = ?
                """,
                (cancelled_created_at - 1, cancelled_project),
            ),
            lambda: conn.execute(
                """
                UPDATE project_runtime_state SET updated_at = ?
                WHERE project_id = ?
                """,
                (terminal_runtime_updated_at, cancelled_project),
            ),
        )
        assert_public_pre_operation_discard(
            cancelled_project,
            cancelled_turn.turn_id,
            cancelled_guard,
            cancelled_identity,
            certified_horizon=(
                cancelled_certified_attempt["lease_expires_at"]
            ),
        )
        await assert_discard_carrier(cancelled_case, "cancelled")

        legacy_cancel_case = setup(
            "discard-legacy-cancelled",
            "bcbcbcbc-bcbc-4bcb-8bcb-bcbcbcbcbcbc",
            with_operation=False,
            started=False,
        )
        (
            legacy_project,
            legacy_turn,
            legacy_claim,
            legacy_now,
            legacy_runtime,
            legacy_guard,
            legacy_actor,
            _,
            _,
            legacy_identity,
        ) = legacy_cancel_case
        legacy_now[0] = legacy_claim.lease_expires_at
        assert legacy_runtime.reconcile_inflight_turns(
            NoNotStartedReadback(),
            limit=10,
        )[0].status == "queued"
        (
            _,
            legacy_requeued_event,
            _,
        ) = requeued_attempt_certificate(
            legacy_project,
            legacy_turn.turn_id,
            legacy_identity,
            legacy_claim,
        )
        legacy_requeued_payload = json.loads(
            legacy_requeued_event[5]
        )
        legacy_requeued_payload.pop("attempt")
        with prdb.write_transaction(conn):
            update_event_payload(
                legacy_requeued_event[0],
                legacy_requeued_payload,
            )
        legacy_version = conn.execute(
            """
            SELECT version FROM project_runtime_state
            WHERE project_id = ?
            """,
            (legacy_project,),
        ).fetchone()[0]
        legacy_control_version = conn.execute(
            """
            SELECT control_version FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (legacy_project, legacy_turn.turn_id),
        ).fetchone()[0]
        legacy_cancelled = legacy_runtime.cancel_queued_turn(
            legacy_project,
            legacy_turn.turn_id,
            legacy_actor,
            idempotency_key="c10-discard-legacy-cancelled",
            expected_version=legacy_version,
            expected_control_version=legacy_control_version,
        )
        assert legacy_cancelled.status == "cancelled"
        legacy_cancel_events = tuple(
            event
            for event in projects_snapshot(
                legacy_project,
                legacy_turn.turn_id,
            )["events"]
            if event[3] == "turn.cancelled"
            and event[4] == legacy_turn.turn_id
        )
        assert len(legacy_cancel_events) == 1
        assert "retired_attempt_event_id" not in json.loads(
            legacy_cancel_events[0][5]
        )
        assert_resolver_write_free_conflict(
            legacy_project,
            legacy_turn.turn_id,
            legacy_guard,
            legacy_identity,
        )
        with prdb.write_transaction(conn):
            conn.execute(
                """
                UPDATE project_events SET payload_json = ?
                WHERE event_id = ?
                """,
                (
                    legacy_requeued_event[5],
                    legacy_requeued_event[0],
                ),
            )
        assert_resolver_write_free_conflict(
            legacy_project,
            legacy_turn.turn_id,
            legacy_guard,
            legacy_identity,
        )

        superseded_attempt_case = setup(
            "discard-superseded-attempt",
            "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            with_operation=False,
            started=False,
        )
        (
            superseded_project,
            superseded_turn,
            superseded_claim,
            superseded_now,
            superseded_runtime,
            superseded_guard,
            _,
            _,
            _,
            superseded_identity,
        ) = superseded_attempt_case
        superseded_prepared_horizon = (
            superseded_identity.attempt.lease_expires_at
        )
        superseded_claim = superseded_runtime.heartbeat_turn(
            superseded_claim,
            lease_seconds=60,
        )
        assert (
            superseded_prepared_horizon
            < superseded_claim.lease_expires_at
        )
        superseded_now[0] = superseded_claim.lease_expires_at
        superseded_requeued = (
            superseded_runtime.reconcile_inflight_turns(
                NoNotStartedReadback(),
                limit=10,
            )
        )
        assert len(superseded_requeued) == 1
        assert superseded_requeued[0].status == "queued"
        replacement_claim = superseded_runtime.claim_next_turn(
            superseded_project,
            "c10-discard-superseded-worker-2",
            lease_seconds=90,
        )
        assert replacement_claim is not None
        assert replacement_claim.attempt_id != superseded_claim.attempt_id
        assert replacement_claim.worker_id != superseded_claim.worker_id
        assert (
            replacement_claim.lease_generation
            == superseded_claim.lease_generation + 1
        )
        assert (
            replacement_claim.fencing_token
            == superseded_claim.fencing_token + 1
        )
        (
            superseded_certificate_snapshot,
            superseded_requeued_event,
            superseded_certified_attempt,
        ) = requeued_attempt_certificate(
            superseded_project,
            superseded_turn.turn_id,
            superseded_identity,
            superseded_claim,
        )
        assert (
            superseded_prepared_horizon
            < superseded_certified_attempt["lease_expires_at"]
            < replacement_claim.lease_expires_at
        )
        assert (
            superseded_certified_attempt["lease_expires_at"] + 1
            < replacement_claim.lease_expires_at
        )
        superseded_turn_rows = (
            superseded_certificate_snapshot["turn"]
        )
        assert len(superseded_turn_rows) == 1
        assert superseded_turn_rows[0][0:8] == (
            replacement_claim.turn_id,
            replacement_claim.project_id,
            replacement_claim.sequence,
            "claimed",
            replacement_claim.attempt_id,
            replacement_claim.lease_generation,
            replacement_claim.fencing_token,
            "not_started",
        )
        assert superseded_turn_rows[0][8:11] == (None, None, None)
        assert superseded_turn_rows[0][11] == superseded_now[0]
        superseded_control_rows = (
            superseded_certificate_snapshot["control"]
        )
        assert len(superseded_control_rows) == 1
        assert superseded_control_rows[0][0:3] == (
            replacement_claim.turn_id,
            replacement_claim.project_id,
            "running",
        )
        assert superseded_control_rows[0][4:8] == (
            replacement_claim.attempt_id,
            replacement_claim.worker_id,
            replacement_claim.lease_expires_at,
            replacement_claim.canonical_session_id,
        )
        assert superseded_control_rows[0][3] > 0
        assert superseded_control_rows[0][8] == superseded_now[0]
        superseded_runtime_rows = superseded_certificate_snapshot["state"]
        assert len(superseded_runtime_rows) == 1
        assert superseded_runtime_rows[0][0:3] == (
            replacement_claim.project_id,
            "active",
            "implementation",
        )
        assert superseded_runtime_rows[0][4:6] == (
            replacement_claim.canonical_session_id,
            replacement_claim.canonical_session_id,
        )
        assert superseded_runtime_rows[0][7:9] == (None, None)
        assert superseded_runtime_rows[0][9] == superseded_now[0]
        superseded_lease_rows = superseded_certificate_snapshot["leases"]
        assert len(superseded_lease_rows) == 1
        assert superseded_lease_rows[0] == (
            replacement_claim.attempt_id,
            replacement_claim.project_id,
            replacement_claim.turn_id,
            replacement_claim.worker_id,
            replacement_claim.lease_generation,
            replacement_claim.fencing_token,
            replacement_claim.lease_expires_at,
            superseded_now[0],
        )
        assert replacement_claim.canonical_session_id == (
            superseded_runtime_rows[0][4]
        )
        later_claim_events = tuple(
            event
            for event in superseded_certificate_snapshot["events"]
            if event[4] == superseded_turn.turn_id
            and event[3] == "turn.claimed"
            and event[2] > superseded_requeued_event[2]
        )
        assert len(later_claim_events) == 1
        replacement_claim_payload = json.loads(
            later_claim_events[0][5]
        )
        assert replacement_claim_payload == {
            "attempt_id": replacement_claim.attempt_id,
            "fencing_token": replacement_claim.fencing_token,
            "lease_generation": replacement_claim.lease_generation,
            "sequence": replacement_claim.sequence,
            "turn_id": replacement_claim.turn_id,
            "version": superseded_runtime_rows[0][3],
        }
        assert replacement_claim.lease_generation > (
            superseded_identity.attempt.lease_generation
        )
        assert replacement_claim.fencing_token > (
            superseded_identity.attempt.fencing_token
        )

        replacement_claim_event = later_claim_events[0]

        def set_b_attempt_projection(
            attempt_id,
            lease_generation,
            fencing_token,
        ):
            conn.execute(
                """
                UPDATE project_turns
                SET attempt_id = ?, lease_generation = ?,
                    fencing_token = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    attempt_id,
                    lease_generation,
                    fencing_token,
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            )
            conn.execute(
                """
                UPDATE project_run_controls
                SET attempt_id = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    attempt_id,
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            )
            conn.execute(
                """
                UPDATE project_worker_leases
                SET lease_id = ?, lease_generation = ?,
                    fencing_token = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    attempt_id,
                    lease_generation,
                    fencing_token,
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            )
            payload = dict(replacement_claim_payload)
            payload.update(
                {
                    "attempt_id": attempt_id,
                    "lease_generation": lease_generation,
                    "fencing_token": fencing_token,
                }
            )
            update_event_payload(
                replacement_claim_event[0],
                payload,
            )

        def restore_b_attempt_projection():
            set_b_attempt_projection(
                replacement_claim.attempt_id,
                replacement_claim.lease_generation,
                replacement_claim.fencing_token,
            )
            conn.execute(
                """
                UPDATE project_events SET payload_json = ?
                WHERE event_id = ?
                """,
                (
                    replacement_claim_event[5],
                    replacement_claim_event[0],
                ),
            )

        for b_attempt_projection in (
            (
                superseded_identity.attempt.attempt_id,
                replacement_claim.lease_generation,
                replacement_claim.fencing_token,
            ),
            (
                replacement_claim.attempt_id,
                replacement_claim.lease_generation,
                superseded_identity.attempt.fencing_token,
            ),
            (
                replacement_claim.attempt_id,
                superseded_identity.attempt.lease_generation,
                replacement_claim.fencing_token,
            ),
            (
                replacement_claim.attempt_id,
                superseded_identity.attempt.lease_generation,
                superseded_identity.attempt.fencing_token,
            ),
        ):
            assert_committed_fixture_conflict(
                superseded_project,
                superseded_turn.turn_id,
                superseded_guard,
                superseded_identity,
                lambda projection=b_attempt_projection: (
                    set_b_attempt_projection(*projection)
                ),
                restore_b_attempt_projection,
            )

        assert_committed_fixture_conflict(
            superseded_project,
            superseded_turn.turn_id,
            superseded_guard,
            superseded_identity,
            lambda: conn.execute(
                """
                UPDATE project_turns SET sequence = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    replacement_claim.sequence + 1,
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            ),
            lambda: conn.execute(
                """
                UPDATE project_turns SET sequence = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    replacement_claim.sequence,
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            ),
        )
        assert_committed_fixture_conflict(
            superseded_project,
            superseded_turn.turn_id,
            superseded_guard,
            superseded_identity,
            lambda: conn.execute(
                """
                UPDATE project_run_controls SET claim_worker_id = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    f"{replacement_claim.worker_id}-wrong",
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            ),
            lambda: conn.execute(
                """
                UPDATE project_run_controls SET claim_worker_id = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    replacement_claim.worker_id,
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            ),
        )
        assert_committed_fixture_conflict(
            superseded_project,
            superseded_turn.turn_id,
            superseded_guard,
            superseded_identity,
            lambda: conn.execute(
                """
                UPDATE project_worker_leases SET worker_id = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    f"{replacement_claim.worker_id}-wrong",
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            ),
            lambda: conn.execute(
                """
                UPDATE project_worker_leases SET worker_id = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    replacement_claim.worker_id,
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            ),
        )
        assert_committed_fixture_conflict(
            superseded_project,
            superseded_turn.turn_id,
            superseded_guard,
            superseded_identity,
            lambda: conn.execute(
                """
                UPDATE project_run_controls
                SET claim_canonical_session_id = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    f"{replacement_claim.canonical_session_id}-wrong",
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            ),
            lambda: conn.execute(
                """
                UPDATE project_run_controls
                SET claim_canonical_session_id = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    replacement_claim.canonical_session_id,
                    superseded_project,
                    superseded_turn.turn_id,
                ),
            ),
        )
        assert_committed_fixture_conflict(
            superseded_project,
            superseded_turn.turn_id,
            superseded_guard,
            superseded_identity,
            lambda: conn.execute(
                "DELETE FROM project_events WHERE event_id = ?",
                (replacement_claim_event[0],),
            ),
            lambda: restore_event(replacement_claim_event),
        )
        wrong_b_event_payload = dict(replacement_claim_payload)
        wrong_b_event_payload["attempt_id"] = (
            f"{replacement_claim.attempt_id}-wrong"
        )
        assert_committed_fixture_conflict(
            superseded_project,
            superseded_turn.turn_id,
            superseded_guard,
            superseded_identity,
            lambda: update_event_payload(
                replacement_claim_event[0],
                wrong_b_event_payload,
            ),
            lambda: conn.execute(
                """
                UPDATE project_events SET payload_json = ?
                WHERE event_id = ?
                """,
                (
                    replacement_claim_event[5],
                    replacement_claim_event[0],
                ),
            ),
        )
        b_sequence_scratch = (
            max(
                event[2]
                for event in superseded_certificate_snapshot["events"]
            )
            + 100
        )

        def install_b_claim_not_after_a():
            update_event_sequence(
                superseded_requeued_event[0],
                b_sequence_scratch,
            )
            update_event_sequence(
                replacement_claim_event[0],
                superseded_requeued_event[2],
            )
            update_event_sequence(
                superseded_requeued_event[0],
                replacement_claim_event[2],
            )

        def restore_b_claim_after_a():
            update_event_sequence(
                superseded_requeued_event[0],
                b_sequence_scratch,
            )
            update_event_sequence(
                replacement_claim_event[0],
                replacement_claim_event[2],
            )
            update_event_sequence(
                superseded_requeued_event[0],
                superseded_requeued_event[2],
            )

        assert_committed_fixture_conflict(
            superseded_project,
            superseded_turn.turn_id,
            superseded_guard,
            superseded_identity,
            install_b_claim_not_after_a,
            restore_b_claim_after_a,
        )
        assert_public_pre_operation_discard(
            superseded_project,
            superseded_turn.turn_id,
            superseded_guard,
            superseded_identity,
            certified_horizon=(
                superseded_certified_attempt["lease_expires_at"]
            ),
        )
        await assert_discard_carrier(
            superseded_attempt_case,
            "superseded_attempt",
        )

        recovery_blocked_case = setup(
            "discard-recovery-blocked",
            "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            with_operation=False,
        )
        (
            recovery_project,
            recovery_turn,
            recovery_claim,
            recovery_now,
            recovery_runtime,
            _,
            _,
            _,
            _,
            _,
        ) = recovery_blocked_case
        recovery_now[0] = recovery_claim.lease_expires_at

        class UnknownCarrierReadback:
            calls = 0

            def read_turn(self, request):
                self.calls += 1
                return TurnReadbackResult("unknown")

        recovery_port = UnknownCarrierReadback()
        recovery_result = recovery_runtime.reconcile_inflight_turns(
            recovery_port,
            limit=10,
        )
        assert recovery_port.calls == 1
        assert len(recovery_result) == 1
        assert recovery_result[0].status == "reconciling"
        assert conn.execute(
            """
            SELECT recovery_block_key
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (recovery_project, recovery_turn.turn_id),
        ).fetchone()[0]
        await assert_discard_carrier(
            recovery_blocked_case,
            "recovery_blocked",
        )

        superseded_terminal_case = setup(
            "discard-superseded-terminal",
            "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            with_operation=False,
        )
        (
            _,
            _,
            terminal_claim,
            _,
            terminal_runtime,
            _,
            _,
            _,
            _,
            _,
        ) = superseded_terminal_case
        superseding_terminal_batch = (
            "ffffffff-ffff-4fff-8fff-ffffffffffff"
        )
        state.prepare_terminal_result(
            terminal_claim,
            batch_id=superseding_terminal_batch,
            status="succeeded",
            base_message_count=0,
            messages=(
                {
                    "role": "user",
                    "content": "superseding terminal",
                    "timestamp": 1.0,
                },
                {
                    "role": "assistant",
                    "content": "terminal kept",
                    "timestamp": 2.0,
                },
            ),
        )
        terminal_runtime.commit_turn_with_task7_batch(
            terminal_claim,
            CanonicalTurnResult(
                "succeeded",
                superseding_terminal_batch,
            ),
            transcript_batch_id=superseding_terminal_batch,
        )
        await assert_discard_carrier(
            superseded_terminal_case,
            "superseded_terminal",
        )

        happy = setup("happy", "11111111-1111-4111-8111-111111111111")
        (
            project_id, turn, claim, now, runtime, guard, actor, operation,
            prepared, identity,
        ) = happy
        assert prepared.kind == "approval_checkpoint"
        assert prepared.state == "prepared"
        assert operation.status == "awaiting_approval"

        happy_session = identity.attempt.canonical_session_id
        immutable_prepare = state_snapshot(
            happy_session,
            prepared.batch_id,
        )
        byte_mismatch_trace = len(state_statements)
        with pytest.raises(ValueError):
            state.prepare_approval_checkpoint(
                claim,
                batch_id=prepared.batch_id,
                operation_id=identity.operation_id,
                approval_id=identity.approval_id,
                base_message_count=0,
                messages=(
                    {
                        "role": "user",
                        "content": "happy approval request changed",
                        "timestamp": 1.0,
                    },
                    {
                        "role": "assistant",
                        "content": "happy awaiting approval",
                        "timestamp": 2.0,
                    },
                ),
            )
        assert state_snapshot(
            happy_session,
            prepared.batch_id,
        ) == immutable_prepare
        assert dml_categories(
            state_statements[byte_mismatch_trace:]
        ) == ()

        def batch_sequence_snapshot():
            return (
                tuple(
                    tuple(row)
                    for row in state._conn.execute(
                        """
                        SELECT batch_id, batch_creation_sequence, kind,
                               session_id, project_id, turn_id,
                               operation_id, approval_id, state
                        FROM project_turn_transcript_batches
                        ORDER BY batch_creation_sequence, batch_id
                        """
                    )
                ),
                tuple(
                    tuple(row)
                    for row in state._conn.execute(
                        """
                        SELECT singleton, last_sequence
                        FROM project_batch_sequence_counter
                        ORDER BY singleton
                        """
                    )
                ),
            )

        different_batch_before = batch_sequence_snapshot()
        different_batch_trace = len(state_statements)
        with pytest.raises(ValueError):
            state.prepare_approval_checkpoint(
                claim,
                batch_id="12111111-1111-4111-8111-111111111111",
                operation_id=identity.operation_id,
                approval_id=identity.approval_id,
                base_message_count=0,
                messages=(
                    {
                        "role": "user",
                        "content": "happy approval request",
                        "timestamp": 1.0,
                    },
                    {
                        "role": "assistant",
                        "content": "happy awaiting approval",
                        "timestamp": 2.0,
                    },
                ),
            )
        assert batch_sequence_snapshot() == different_batch_before
        different_batch_statements = state_statements[
            different_batch_trace:
        ]
        assert any(
            normalized(statement) == "ROLLBACK"
            for statement in different_batch_statements
        )

        approved = guard.resolve_operation_approval(
            identity.approval_id, actor, outcome="approved"
        )
        assert approved.status == "approved"
        now[0] = 131
        dispatcher_lease = runtime.acquire_dispatcher_lease(
            "22222222-2222-4222-8222-222222222222", lease_seconds=30
        )
        assert dispatcher_lease is not None
        upper = guard.operation_recovery_membership_upper_watermark()
        assert upper is not None

        class NoReadback:
            def read_operation(self, request):
                raise AssertionError("approved operation must not read back")

        class RecordingStatePort:
            def __init__(self):
                self.calls = []

            def publication_state(self, observed):
                assert conn.in_transaction is False
                assert state._conn.in_transaction is False
                with ledger_lock:
                    if active_transactions["projects"]:
                        ledger_violations.append(
                            (
                                "state-port-during-projects",
                                threading.get_ident(),
                            )
                        )
                self.calls.append(
                    (
                        observed,
                        threading.get_ident(),
                        conn.in_transaction,
                    )
                )
                return state.publication_state(observed)

        state_port = RecordingStatePort()
        waiting_state_before = state_snapshot(
            happy_session,
            prepared.batch_id,
        )
        waiting_before_publish = projects_snapshot(
            project_id,
            turn.turn_id,
        )
        waiting_recovery_trace = len(main_project_statements)
        waiting_recovery = guard.recover_pending_operations(
            NoReadback(), state_port, worker_id="c10-rehydrator", lease_seconds=30,
            dispatcher_lease=dispatcher_lease, max_claims=1, after=None,
            through_membership_sequence=upper, limit=1,
        )
        assert waiting_recovery.starts == ()
        assert [call[0] for call in state_port.calls] == [identity]
        assert all(call[2] is False for call in state_port.calls)
        assert projects_snapshot(
            project_id,
            turn.turn_id,
        ) == waiting_before_publish
        assert state_snapshot(
            happy_session,
            prepared.batch_id,
        ) == waiting_state_before
        assert dml_categories(
            main_project_statements[waiting_recovery_trace:]
        ) == ()

        publish_state_before = state_snapshot(
            happy_session,
            prepared.batch_id,
        )
        publish_projects_before = projects_snapshot(
            project_id,
            turn.turn_id,
        )
        publish_factory_start = len(factory_records)
        publish_state_trace = len(state_statements)
        publish_project_trace = len(main_project_statements)
        publish_order_start = len(protocol_order)
        assert (await adapter.apply_project_batch(prepared.batch_id)).outcome == "published"
        publish_records = factory_records[publish_factory_start:]
        assert len(publish_records) == 1
        assert publish_records[0]["closed"]
        assert (
            publish_records[0]["owner_thread"]
            == publish_records[0]["close_thread"]
            != loop_thread
        )
        assert dml_categories(publish_records[0]["statements"]) == ()
        published_state = state_snapshot(
            happy_session,
            prepared.batch_id,
        )
        assert (
            published_state["session"][0][6]
            == publish_state_before["session"][0][6] + 2
        )
        assert (
            published_state["session"][0][7]
            == publish_state_before["session"][0][7]
        )
        assert len(published_state["messages"]) == 2
        assert published_state["batch_counter"] == (
            publish_state_before["batch_counter"]
        )
        assert projects_snapshot(
            project_id,
            turn.turn_id,
        ) == publish_projects_before
        assert dml_categories(
            main_project_statements[publish_project_trace:]
        ) == ()
        assert dml_categories(
            state_statements[publish_state_trace:]
        )
        publish_order = protocol_order[publish_order_start:]
        first_publish_state_dml = min(
            item[0]
            for item in publish_order
            if item[1] == "state-dml"
        )
        assert publish_records[0]["close_order"] < first_publish_state_dml

        replay_publish_state = state_snapshot(
            happy_session,
            prepared.batch_id,
        )
        replay_publish_projects = projects_snapshot(
            project_id,
            turn.turn_id,
        )
        replay_publish_factory = len(factory_records)
        replay_publish_state_trace = len(state_statements)
        replay_publish_project_trace = len(main_project_statements)
        assert (
            await adapter.apply_project_batch(prepared.batch_id)
        ).outcome == "already_published"
        assert len(factory_records) == replay_publish_factory
        assert state_snapshot(
            happy_session,
            prepared.batch_id,
        ) == replay_publish_state
        assert projects_snapshot(
            project_id,
            turn.turn_id,
        ) == replay_publish_projects
        assert dml_categories(
            state_statements[replay_publish_state_trace:]
        ) == ()
        assert dml_categories(
            main_project_statements[replay_publish_project_trace:]
        ) == ()
        # The first call is the only mutation; replay is immutable and write-free.
        assert state.publication_state(identity) == "published"
        assert [message["content"] for message in state.get_messages("c10-happy-session")] == [
            "happy approval request", "happy awaiting approval"
        ]
        batch = state._conn.execute(
            "SELECT kind, state, projects_acknowledged_at FROM project_turn_transcript_batches WHERE batch_id = ?",
            (prepared.batch_id,),
        ).fetchone()
        assert tuple(batch) == ("approval_checkpoint", "published", None)
        gate = conn.execute(
            "SELECT transcript_pending_batch_id, transcript_dispatch_block_key FROM project_runtime_state WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        assert tuple(gate) == (None, None)

        starts = guard.recover_pending_operations(
            NoReadback(), state_port, worker_id="c10-rehydrator", lease_seconds=30,
            dispatcher_lease=dispatcher_lease, max_claims=1, after=None,
            through_membership_sequence=upper, limit=1,
        )
        assert len(starts.starts) == 1
        assert starts.starts[0].claim.attempt_id != claim.attempt_id
        assert (
            starts.starts[0].claim.lease_generation
            == claim.lease_generation + 1
        )
        assert (
            starts.starts[0].claim.fencing_token
            == claim.fencing_token + 1
        )
        assert [call[0] for call in state_port.calls] == [
            identity,
            identity,
        ]

        second_recovery_state = state_snapshot(
            happy_session,
            prepared.batch_id,
        )
        second_recovery_projects = projects_snapshot(
            project_id,
            turn.turn_id,
        )
        second_recovery_trace = len(main_project_statements)
        second_recovery = guard.recover_pending_operations(
            NoReadback(),
            state_port,
            worker_id="c10-rehydrator",
            lease_seconds=30,
            dispatcher_lease=dispatcher_lease,
            max_claims=1,
            after=None,
            through_membership_sequence=upper,
            limit=1,
        )
        assert second_recovery.starts == ()
        assert [call[0] for call in state_port.calls] == [
            identity,
            identity,
            identity,
        ]
        assert state_snapshot(
            happy_session,
            prepared.batch_id,
        ) == second_recovery_state
        assert projects_snapshot(
            project_id,
            turn.turn_id,
        ) == second_recovery_projects
        assert dml_categories(
            main_project_statements[second_recovery_trace:]
        ) == ()

        drift = setup("drift", "33333333-3333-4333-8333-333333333333")
        drift_prepared = drift[8]
        drift_identity = drift[9]
        drift_guard = drift[5]
        drift_actor = drift[6]
        drift_guard.resolve_operation_approval(
            drift_identity.approval_id,
            drift_actor,
            outcome="approved",
        )
        drift_session = drift_identity.attempt.canonical_session_id
        state.append_message("c10-drift-session", "user", "outside writer")
        drift_state_before = state_snapshot(
            drift_session,
            drift_prepared.batch_id,
        )
        drift_projects_before = projects_snapshot(
            drift[0],
            drift[1].turn_id,
        )
        drift_factory_start = len(factory_records)
        drift_state_trace = len(state_statements)
        drift_project_trace = len(main_project_statements)
        drift_order_start = len(protocol_order)
        assert (await adapter.apply_project_batch(drift_prepared.batch_id)).outcome == "conflicted"
        drift_records = factory_records[drift_factory_start:]
        assert len(drift_records) == 1
        assert drift_records[0]["closed"]
        assert dml_categories(drift_records[0]["statements"]) == ()
        drift_state_after = state_snapshot(
            drift_session,
            drift_prepared.batch_id,
        )
        assert drift_state_after["session"] == drift_state_before["session"]
        assert drift_state_after["messages"] == drift_state_before["messages"]
        assert (
            drift_state_after["batch_counter"]
            == drift_state_before["batch_counter"]
        )
        drift_final = drift_state_after["batch"][0]
        assert drift_final[2] == "approval_checkpoint"
        assert drift_final[12] is None
        assert drift_final[13:15] == (
            drift_identity.operation_id,
            drift_identity.approval_id,
        )
        assert drift_final[18] == "conflicted"
        assert drift_final[20:] == (
            None,
            None,
            None,
            None,
            None,
            None,
        )
        assert projects_snapshot(
            drift[0],
            drift[1].turn_id,
        ) == drift_projects_before
        assert dml_categories(
            main_project_statements[drift_project_trace:]
        ) == ()
        assert dml_categories(
            state_statements[drift_state_trace:]
        )
        drift_order = protocol_order[drift_order_start:]
        first_drift_state_dml = min(
            item[0]
            for item in drift_order
            if item[1] == "state-dml"
        )
        assert drift_records[0]["close_order"] < first_drift_state_dml

        drift_replay_state = state_snapshot(
            drift_session,
            drift_prepared.batch_id,
        )
        drift_replay_projects = projects_snapshot(
            drift[0],
            drift[1].turn_id,
        )
        drift_replay_factory = len(factory_records)
        drift_replay_state_trace = len(state_statements)
        drift_replay_project_trace = len(main_project_statements)
        assert (await adapter.apply_project_batch(drift_prepared.batch_id)).outcome == "already_conflicted"
        assert len(factory_records) == drift_replay_factory
        assert state_snapshot(
            drift_session,
            drift_prepared.batch_id,
        ) == drift_replay_state
        assert projects_snapshot(
            drift[0],
            drift[1].turn_id,
        ) == drift_replay_projects
        assert dml_categories(
            state_statements[drift_replay_state_trace:]
        ) == ()
        assert dml_categories(
            main_project_statements[drift_replay_project_trace:]
        ) == ()
        assert [message["content"] for message in state.get_messages("c10-drift-session")] == [
            "outside writer"
        ]
        assert state.publication_state(drift_identity) == "permanent_conflict"
        for invalid_final_identity in (
            ApprovalCheckpointIdentity(
                "not-a-canonical-batch",
                drift_identity.attempt,
                drift_identity.operation_id,
                drift_identity.approval_id,
            ),
            ApprovalCheckpointIdentity(
                drift_identity.checkpoint_id,
                drift_identity.attempt,
                f"{drift_identity.operation_id}-wrong",
                drift_identity.approval_id,
            ),
            ApprovalCheckpointIdentity(
                drift_identity.checkpoint_id,
                drift_identity.attempt,
                drift_identity.operation_id,
                f"{drift_identity.approval_id}-wrong",
            ),
        ):
            assert (
                state.publication_state(invalid_final_identity)
                == "permanent_conflict"
            )
        drift_batch = state._conn.execute(
            """
            SELECT state, transcript_conflict_key, observed_message_count,
                   remediated_at, published_at, projects_acknowledged_at,
                   discard_authority
            FROM project_turn_transcript_batches
            WHERE batch_id = ?
            """,
            (drift_prepared.batch_id,),
        ).fetchone()
        assert tuple(drift_batch) == (
            "conflicted",
            None,
            None,
            None,
            None,
            None,
            None,
        )

        # Build the target as a wholly raw, test-local C9 database.  Its first
        # SessionDB open below is therefore the migration under test.
        import hashlib
        import hermes_state as state_module

        schema_path = tmp_path / "c10-upgrade-state.db"
        terminal_batch_id = (
            "55555555-5555-4555-8555-555555555555"
        )
        legacy_approval_batch_id = (
            "56555555-5555-4555-8555-555555555555"
        )
        terminal_transcript = json.dumps(
            [
                {
                    "role": "user",
                    "content": "terminal c9",
                    "timestamp": 1.0,
                },
                {
                    "role": "assistant",
                    "content": "kept",
                    "timestamp": 2.0,
                },
            ],
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        approval_transcript = json.dumps(
            [
                {
                    "role": "user",
                    "content": "approval c9",
                    "timestamp": 1.0,
                },
                {
                    "role": "assistant",
                    "content": "kept",
                    "timestamp": 2.0,
                },
            ],
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        legacy_sessions_table = """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            parent_session_id TEXT,
            started_at REAL NOT NULL,
            FOREIGN KEY (parent_session_id) REFERENCES sessions(id)
        )
        """
        legacy_counter_table = """
        CREATE TABLE project_batch_sequence_counter (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            last_sequence INTEGER NOT NULL
                CHECK (
                    typeof(last_sequence) = 'integer'
                    AND last_sequence >= 0
                    AND last_sequence <= 9223372036854775807
                )
        )
        """
        batch_columns = (
            "batch_id",
            "batch_creation_sequence",
            "kind",
            "session_id",
            "project_id",
            "turn_id",
            "sequence",
            "worker_id",
            "attempt_id",
            "lease_generation",
            "fencing_token",
            "lease_expires_at",
            "terminal_status",
            "operation_id",
            "approval_id",
            "base_message_count",
            "transcript_json",
            "transcript_sha256",
            "state",
            "created_at",
            "published_at",
            "projects_acknowledged_at",
            "transcript_conflict_key",
            "observed_message_count",
            "remediated_at",
            "discard_authority",
        )
        legacy_c9_table = """
        CREATE TABLE project_turn_transcript_batches (
            batch_id TEXT PRIMARY KEY,
            batch_creation_sequence INTEGER NOT NULL
                CHECK (
                    typeof(batch_creation_sequence) = 'integer'
                    AND batch_creation_sequence > 0
                ),
            kind TEXT NOT NULL
                CHECK (
                    kind IN ('terminal_result', 'approval_checkpoint')
                ),
            session_id TEXT NOT NULL
                REFERENCES sessions(id) ON DELETE RESTRICT,
            project_id TEXT NOT NULL CHECK (length(project_id) > 0),
            turn_id TEXT NOT NULL CHECK (length(turn_id) > 0),
            sequence INTEGER NOT NULL
                CHECK (typeof(sequence) = 'integer' AND sequence > 0),
            worker_id TEXT NOT NULL CHECK (length(worker_id) > 0),
            attempt_id TEXT NOT NULL CHECK (length(attempt_id) > 0),
            lease_generation INTEGER NOT NULL
                CHECK (
                    typeof(lease_generation) = 'integer'
                    AND lease_generation > 0
                ),
            fencing_token INTEGER NOT NULL
                CHECK (
                    typeof(fencing_token) = 'integer'
                    AND fencing_token > 0
                ),
            lease_expires_at INTEGER NOT NULL
                CHECK (
                    typeof(lease_expires_at) = 'integer'
                    AND lease_expires_at >= 0
                ),
            terminal_status TEXT
                CHECK (
                    terminal_status IS NULL
                    OR terminal_status IN ('succeeded', 'failed')
                ),
            operation_id TEXT,
            approval_id TEXT,
            base_message_count INTEGER NOT NULL
                CHECK (
                    typeof(base_message_count) = 'integer'
                    AND base_message_count >= 0
                ),
            transcript_json TEXT NOT NULL,
            transcript_sha256 TEXT NOT NULL
                CHECK (length(transcript_sha256) = 64),
            state TEXT NOT NULL
                CHECK (
                    state IN (
                        'prepared',
                        'published',
                        'discarded',
                        'conflict_pending',
                        'conflicted'
                    )
                ),
            created_at REAL NOT NULL
                CHECK (
                    created_at >= 0
                    AND created_at <= 253402300799.0
                ),
            published_at REAL,
            projects_acknowledged_at REAL,
            transcript_conflict_key TEXT,
            observed_message_count INTEGER,
            remediated_at REAL,
            discard_authority TEXT,
            CHECK (
                (
                    kind = 'terminal_result'
                    AND terminal_status IS NOT NULL
                    AND operation_id IS NULL
                    AND approval_id IS NULL
                )
                OR (
                    kind = 'approval_checkpoint'
                    AND terminal_status IS NULL
                    AND typeof(operation_id) = 'text'
                    AND length(operation_id) > 0
                    AND typeof(approval_id) = 'text'
                    AND length(approval_id) > 0
                )
            ),
            CHECK (
                (
                    state = 'published'
                    AND published_at IS NOT NULL
                )
                OR (
                    state != 'published'
                    AND published_at IS NULL
                )
            ),
            CHECK (
                published_at IS NULL
                OR (
                    published_at >= 0
                    AND published_at <= 253402300799.0
                )
            ),
            CHECK (
                projects_acknowledged_at IS NULL
                OR (
                    kind = 'terminal_result'
                    AND state = 'published'
                    AND projects_acknowledged_at >= 0
                    AND projects_acknowledged_at <= 253402300799.0
                )
            ),
            CHECK (
                kind != 'approval_checkpoint'
                OR projects_acknowledged_at IS NULL
            ),
            CHECK (
                (
                    state = 'discarded'
                    AND discard_authority IN (
                        'stop_requested',
                        'cancelled',
                        'superseded_attempt',
                        'superseded_terminal',
                        'recovery_blocked'
                    )
                )
                OR (
                    state != 'discarded'
                    AND discard_authority IS NULL
                )
            ),
            CHECK (
                (
                    state IN ('prepared', 'published', 'discarded')
                    AND transcript_conflict_key IS NULL
                    AND observed_message_count IS NULL
                    AND remediated_at IS NULL
                )
                OR (
                    state = 'conflict_pending'
                    AND kind = 'terminal_result'
                    AND typeof(transcript_conflict_key) = 'text'
                    AND length(transcript_conflict_key) > 0
                    AND typeof(observed_message_count) = 'integer'
                    AND observed_message_count >= 0
                    AND published_at IS NULL
                    AND projects_acknowledged_at IS NULL
                    AND remediated_at IS NULL
                )
                OR (
                    state = 'conflicted'
                    AND kind = 'terminal_result'
                    AND typeof(transcript_conflict_key) = 'text'
                    AND length(transcript_conflict_key) > 0
                    AND typeof(observed_message_count) = 'integer'
                    AND observed_message_count >= 0
                    AND published_at IS NULL
                    AND projects_acknowledged_at IS NULL
                    AND remediated_at IS NOT NULL
                    AND remediated_at >= 0
                    AND remediated_at <= 253402300799.0
                )
            )
        )
        """
        legacy_index_sql = (
            """
            CREATE UNIQUE INDEX
            idx_project_batches_one_terminal_attempt
            ON project_turn_transcript_batches(
                project_id,
                turn_id,
                attempt_id,
                lease_generation,
                fencing_token
            )
            WHERE kind = 'terminal_result'
            """,
            """
            CREATE UNIQUE INDEX idx_project_batches_one_approval
            ON project_turn_transcript_batches(
                project_id,
                operation_id,
                approval_id
            )
            WHERE kind = 'approval_checkpoint'
            """,
            """
            CREATE UNIQUE INDEX idx_project_batches_creation
            ON project_turn_transcript_batches(batch_creation_sequence)
            """,
            """
            CREATE INDEX idx_project_batches_actionable_settlement
            ON project_turn_transcript_batches(
                batch_creation_sequence, batch_id
            )
            WHERE state = 'prepared'
               OR state = 'conflict_pending'
               OR (
                   kind = 'terminal_result'
                   AND state = 'published'
                   AND projects_acknowledged_at IS NULL
               )
            """,
        )
        legacy_canonical_guard_sql = (
            """
            CREATE TRIGGER
            trg_project_batches_c8_discard_insert
            BEFORE INSERT ON project_turn_transcript_batches
            WHEN NOT (
                (
                    NEW.state = 'discarded'
                    AND typeof(NEW.discard_authority) = 'text'
                    AND NEW.discard_authority IN (
                        'stop_requested',
                        'cancelled',
                        'superseded_attempt',
                        'superseded_terminal',
                        'recovery_blocked'
                    )
                )
                OR (
                    NEW.state != 'discarded'
                    AND NEW.discard_authority IS NULL
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid project batch discard authority');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c8_discard_update
            BEFORE UPDATE ON project_turn_transcript_batches
            WHEN NOT (
                (
                    NEW.state = 'discarded'
                    AND typeof(NEW.discard_authority) = 'text'
                    AND NEW.discard_authority IN (
                        'stop_requested',
                        'cancelled',
                        'superseded_attempt',
                        'superseded_terminal',
                        'recovery_blocked'
                    )
                )
                OR (
                    NEW.state != 'discarded'
                    AND NEW.discard_authority IS NULL
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid project batch discard authority');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c8_discard_immutable
            BEFORE UPDATE OF discard_authority
            ON project_turn_transcript_batches
            WHEN OLD.discard_authority IS NOT NULL
             AND NEW.discard_authority IS NOT OLD.discard_authority
            BEGIN
                SELECT RAISE(ABORT, 'project batch discard authority is immutable');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c8_projects_ack_insert
            BEFORE INSERT ON project_turn_transcript_batches
            WHEN NEW.projects_acknowledged_at IS NOT NULL
             AND NOT (
                NEW.kind = 'terminal_result'
                AND NEW.state = 'published'
                AND typeof(NEW.projects_acknowledged_at) IN ('integer', 'real')
                AND NEW.projects_acknowledged_at >= 0
                AND NEW.projects_acknowledged_at <= 253402300799.0
             )
            BEGIN
                SELECT RAISE(ABORT, 'invalid project batch projects acknowledgement');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c8_projects_ack_update
            BEFORE UPDATE ON project_turn_transcript_batches
            WHEN NEW.projects_acknowledged_at IS NOT NULL
             AND NOT (
                NEW.kind = 'terminal_result'
                AND NEW.state = 'published'
                AND typeof(NEW.projects_acknowledged_at) IN ('integer', 'real')
                AND NEW.projects_acknowledged_at >= 0
                AND NEW.projects_acknowledged_at <= 253402300799.0
             )
            BEGIN
                SELECT RAISE(ABORT, 'invalid project batch projects acknowledgement');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c8_projects_ack_immutable
            BEFORE UPDATE OF projects_acknowledged_at
            ON project_turn_transcript_batches
            WHEN OLD.projects_acknowledged_at IS NOT NULL
             AND NEW.projects_acknowledged_at IS NOT OLD.projects_acknowledged_at
            BEGIN
                SELECT RAISE(ABORT, 'project batch projects acknowledgement is immutable');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c8_lease_horizon_insert
            BEFORE INSERT ON project_turn_transcript_batches
            WHEN NOT (
                typeof(NEW.lease_expires_at) = 'integer'
                AND NEW.lease_expires_at >= 0
                AND NEW.lease_expires_at <= 253402300799
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid project batch lease horizon');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c8_lease_horizon_update
            BEFORE UPDATE OF lease_expires_at
            ON project_turn_transcript_batches
            WHEN NOT (
                typeof(NEW.lease_expires_at) = 'integer'
                AND NEW.lease_expires_at >= 0
                AND NEW.lease_expires_at <= 253402300799
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid project batch lease horizon');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c9_conflict_insert
            BEFORE INSERT ON project_turn_transcript_batches
            WHEN NEW.state IN ('conflict_pending', 'conflicted')
              OR NEW.transcript_conflict_key IS NOT NULL
              OR NEW.observed_message_count IS NOT NULL
              OR NEW.remediated_at IS NOT NULL
            BEGIN
                SELECT RAISE(ABORT, 'project batch conflict must be reserved');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c9_conflict_shape
            BEFORE UPDATE ON project_turn_transcript_batches
            WHEN NOT (
                (
                    NEW.state NOT IN ('conflict_pending', 'conflicted')
                    AND NEW.transcript_conflict_key IS NULL
                    AND NEW.observed_message_count IS NULL
                    AND NEW.remediated_at IS NULL
                )
                OR (
                    NEW.state = 'conflict_pending'
                    AND NEW.kind = 'terminal_result'
                    AND NEW.published_at IS NULL
                    AND NEW.projects_acknowledged_at IS NULL
                    AND NEW.discard_authority IS NULL
                    AND typeof(NEW.transcript_conflict_key) = 'text'
                    AND length(NEW.transcript_conflict_key) = 84
                    AND substr(NEW.transcript_conflict_key, 1, 20)
                        = 'transcript-conflict-'
                    AND substr(NEW.transcript_conflict_key, 21)
                        NOT GLOB '*[^0-9a-f]*'
                    AND typeof(NEW.observed_message_count) = 'integer'
                    AND NEW.observed_message_count >= 0
                    AND NEW.observed_message_count <= 9223372036854775807
                    AND NEW.remediated_at IS NULL
                )
                OR (
                    NEW.state = 'conflicted'
                    AND NEW.kind = 'terminal_result'
                    AND NEW.published_at IS NULL
                    AND NEW.projects_acknowledged_at IS NULL
                    AND NEW.discard_authority IS NULL
                    AND typeof(NEW.transcript_conflict_key) = 'text'
                    AND length(NEW.transcript_conflict_key) = 84
                    AND substr(NEW.transcript_conflict_key, 1, 20)
                        = 'transcript-conflict-'
                    AND substr(NEW.transcript_conflict_key, 21)
                        NOT GLOB '*[^0-9a-f]*'
                    AND typeof(NEW.observed_message_count) = 'integer'
                    AND NEW.observed_message_count >= 0
                    AND NEW.observed_message_count <= 9223372036854775807
                    AND typeof(NEW.remediated_at) IN ('integer', 'real')
                    AND NEW.remediated_at >= 0
                    AND NEW.remediated_at <= 253402300799.0
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid project batch conflict proof');
            END
            """,
            """
            CREATE TRIGGER
            trg_project_batches_c9_conflict_immutable
            BEFORE UPDATE OF
                transcript_conflict_key,
                observed_message_count,
                remediated_at
            ON project_turn_transcript_batches
            WHEN (
                OLD.state IN ('conflict_pending', 'conflicted')
                AND (
                    NEW.transcript_conflict_key
                        IS NOT OLD.transcript_conflict_key
                    OR NEW.observed_message_count
                        IS NOT OLD.observed_message_count
                )
            )
            OR (
                OLD.state = 'conflicted'
                AND NEW.remediated_at IS NOT OLD.remediated_at
            )
            BEGIN
                SELECT RAISE(ABORT, 'project batch conflict proof is immutable');
            END
            """,
        )
        stale_guard_name = (
            "trg_project_batches_c9_conflict_transition"
        )
        stale_guard_sql = f"""
        CREATE TRIGGER {stale_guard_name}
        BEFORE UPDATE OF state
        ON project_turn_transcript_batches
        WHEN OLD.state = 'prepared' AND NEW.state = 'conflicted'
        BEGIN
            SELECT RAISE(
                ABORT,
                'stale guard rejects approval direct conflict'
            );
        END
        """
        quoted_columns = ", ".join(
            f'"{column}"' for column in batch_columns
        )
        durable_batch_indexes = {
            "idx_project_batches_actionable_settlement",
            "idx_project_batches_creation",
            "idx_project_batches_one_approval",
            "idx_project_batches_one_terminal_attempt",
        }
        required_batch_guards = {
            "trg_project_batches_c8_discard_insert",
            "trg_project_batches_c8_discard_update",
            "trg_project_batches_c8_discard_immutable",
            "trg_project_batches_c8_projects_ack_insert",
            "trg_project_batches_c8_projects_ack_update",
            "trg_project_batches_c8_projects_ack_immutable",
            "trg_project_batches_c8_lease_horizon_insert",
            "trg_project_batches_c8_lease_horizon_update",
            "trg_project_batches_c9_conflict_insert",
            "trg_project_batches_c9_conflict_shape",
            "trg_project_batches_c9_conflict_transition",
            "trg_project_batches_c9_conflict_immutable",
        }
        raw = sqlite3.connect(schema_path)
        try:
            raw.execute("PRAGMA foreign_keys=ON")
            raw.execute("BEGIN IMMEDIATE")
            raw.execute(legacy_sessions_table)
            raw.execute(legacy_counter_table)
            raw.execute(legacy_c9_table)
            for index_sql in legacy_index_sql:
                raw.execute(index_sql)
            raw.execute(
                """
                INSERT INTO sessions (
                    id, source, parent_session_id, started_at
                ) VALUES (?, ?, ?, ?)
                """,
                ("c10-schema-session", "cli", None, 1.0),
            )
            raw.execute(
                """
                INSERT INTO project_batch_sequence_counter (
                    singleton, last_sequence
                ) VALUES (1, 2)
                """
            )
            raw.executemany(
                f"""
                INSERT INTO project_turn_transcript_batches (
                    {quoted_columns}
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    (
                        terminal_batch_id,
                        1,
                        "terminal_result",
                        "c10-schema-session",
                        "c10-terminal-project",
                        "c10-terminal-turn",
                        1,
                        "c10-terminal-worker",
                        "c10-terminal-attempt",
                        1,
                        1,
                        100,
                        "succeeded",
                        None,
                        None,
                        0,
                        terminal_transcript,
                        hashlib.sha256(
                            terminal_transcript.encode("utf-8")
                        ).hexdigest(),
                        "prepared",
                        1.0,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                    (
                        legacy_approval_batch_id,
                        2,
                        "approval_checkpoint",
                        "c10-schema-session",
                        "c10-approval-project",
                        "c10-approval-turn",
                        1,
                        "c10-approval-worker",
                        "c10-approval-attempt",
                        1,
                        1,
                        100,
                        None,
                        "c10-legacy-operation",
                        "c10-legacy-approval",
                        0,
                        approval_transcript,
                        hashlib.sha256(
                            approval_transcript.encode("utf-8")
                        ).hexdigest(),
                        "prepared",
                        2.0,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                ),
            )

            # Before any guard exists, the frozen C9 table CHECK itself
            # rejects approval prepared -> conflicted.
            raw.execute("SAVEPOINT c10_raw_c9_check")
            with pytest.raises(sqlite3.IntegrityError):
                raw.execute(
                    """
                    UPDATE project_turn_transcript_batches
                    SET state = 'conflicted'
                    WHERE batch_id = ?
                    """,
                    (legacy_approval_batch_id,),
                )
            raw.execute("ROLLBACK TO SAVEPOINT c10_raw_c9_check")
            raw.execute("RELEASE SAVEPOINT c10_raw_c9_check")
            for guard_sql in legacy_canonical_guard_sql:
                raw.execute(guard_sql)
            raw.execute(stale_guard_sql)
            raw.commit()
            assert tuple(raw.execute("PRAGMA foreign_key_check")) == ()
            assert {
                row[0]
                for row in raw.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'table'
                      AND name NOT LIKE 'sqlite_%'
                    """
                )
            } == {
                "sessions",
                "project_batch_sequence_counter",
                "project_turn_transcript_batches",
            }
            assert {
                row[0]
                for row in raw.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'index'
                      AND tbl_name =
                          'project_turn_transcript_batches'
                      AND name NOT LIKE 'sqlite_autoindex_%'
                    """
                )
            } == durable_batch_indexes
            assert {
                row[0]
                for row in raw.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'trigger'
                      AND tbl_name =
                          'project_turn_transcript_batches'
                    """
                )
            } == required_batch_guards
        finally:
            raw.close()

        def raw_legacy_snapshot():
            connection = sqlite3.connect(schema_path)
            try:
                return {
                    "catalogue": tuple(
                        tuple(row)
                        for row in connection.execute(
                            """
                            SELECT type, name, tbl_name, sql
                            FROM sqlite_master
                            WHERE (
                                name = 'project_turn_transcript_batches'
                                OR tbl_name =
                                    'project_turn_transcript_batches'
                            )
                              AND type IN (
                                  'table', 'index', 'trigger'
                              )
                            ORDER BY type, name
                            """
                        )
                    ),
                    "columns": tuple(
                        tuple(row)
                        for row in connection.execute(
                            """
                            PRAGMA table_info(
                                project_turn_transcript_batches
                            )
                            """
                        )
                    ),
                    "rows": tuple(
                        tuple(row)
                        for row in connection.execute(
                            f"""
                            SELECT {quoted_columns}
                            FROM project_turn_transcript_batches
                            ORDER BY batch_creation_sequence, batch_id
                            """
                        )
                    ),
                    "counter": tuple(
                        tuple(row)
                        for row in connection.execute(
                            """
                            SELECT singleton, last_sequence
                            FROM project_batch_sequence_counter
                            ORDER BY singleton
                            """
                        )
                    ),
                    "session": tuple(
                        tuple(row)
                        for row in connection.execute(
                            """
                            SELECT id, source, parent_session_id, started_at
                            FROM sessions
                            WHERE id = 'c10-schema-session'
                            """
                        )
                    ),
                    "foreign_keys": tuple(
                        tuple(row)
                        for row in connection.execute(
                            "PRAGMA foreign_key_check"
                        )
                    ),
                }
            finally:
                connection.close()

        before_fault = raw_legacy_snapshot()
        legacy_rows_before = before_fault["rows"]
        legacy_counter_before = before_fault["counter"]
        legacy_session_before = before_fault["session"]
        original_connect = state_module.sqlite3.connect

        class AuthorizerFaultConnection(sqlite3.Connection):
            denials = 0
            observations = []

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.saw_batch_rebuild = False

                def authorize(action, arg1, arg2, database, source):
                    names = {str(arg1 or ""), str(arg2 or "")}
                    if (
                        action
                        in {
                            sqlite3.SQLITE_ALTER_TABLE,
                            sqlite3.SQLITE_DROP_TABLE,
                        }
                        and "project_turn_transcript_batches" in names
                    ):
                        self.saw_batch_rebuild = True
                    if (
                        action == sqlite3.SQLITE_CREATE_TRIGGER
                        and arg1 == stale_guard_name
                        and self.saw_batch_rebuild
                    ):
                        AuthorizerFaultConnection.denials += 1
                        AuthorizerFaultConnection.observations.append(
                            (
                                self.in_transaction,
                                database,
                                source,
                            )
                        )
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                self.set_authorizer(authorize)

        def failing_connect(database, *args, **kwargs):
            if str(database) == str(schema_path):
                kwargs["factory"] = AuthorizerFaultConnection
            return original_connect(database, *args, **kwargs)

        with monkeypatch.context() as fault_patch:
            fault_patch.setattr(
                state_module.sqlite3,
                "connect",
                failing_connect,
            )
            with pytest.raises(sqlite3.DatabaseError):
                SessionDB(db_path=schema_path)
        assert AuthorizerFaultConnection.denials == 1
        assert AuthorizerFaultConnection.observations
        assert AuthorizerFaultConnection.observations[0][0] is True
        # Catalogue SQL is compared byte-for-byte only across the failed
        # transaction, where rollback must restore the literal C9 database.
        assert raw_legacy_snapshot() == before_fault

        upgraded = SessionDB(db_path=schema_path)

        def named_batch_rows(database):
            return tuple(
                tuple(row)
                for row in database._conn.execute(
                    f"""
                    SELECT {quoted_columns}
                    FROM project_turn_transcript_batches
                    WHERE batch_id IN (?, ?)
                    ORDER BY batch_creation_sequence, batch_id
                    """,
                    (terminal_batch_id, legacy_approval_batch_id),
                )
            )

        transition_guard = (
            "trg_project_batches_c9_conflict_transition"
        )

        def assert_named_batch_objects(database):
            indexes = {
                row[0]
                for row in database._conn.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'index'
                      AND tbl_name =
                          'project_turn_transcript_batches'
                      AND name NOT LIKE 'sqlite_autoindex_%'
                    """
                )
            }
            triggers = tuple(
                row[0]
                for row in database._conn.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'trigger'
                      AND tbl_name =
                          'project_turn_transcript_batches'
                    ORDER BY name
                    """
                )
            )
            assert indexes == durable_batch_indexes
            assert set(triggers) == required_batch_guards
            assert triggers.count(transition_guard) == 1

        try:
            assert named_batch_rows(upgraded) == legacy_rows_before
            assert tuple(
                tuple(row)
                for row in upgraded._conn.execute(
                    """
                    SELECT singleton, last_sequence
                    FROM project_batch_sequence_counter
                    ORDER BY singleton
                    """
                )
            ) == legacy_counter_before
            assert tuple(
                tuple(row)
                for row in upgraded._conn.execute(
                    """
                    SELECT id, source, parent_session_id, started_at
                    FROM sessions
                    WHERE id = 'c10-schema-session'
                    """
                )
            ) == legacy_session_before
            assert tuple(
                upgraded._conn.execute("PRAGMA foreign_key_check")
            ) == ()

            def matrix_batch_id(base, offset):
                return (
                    f"{base:08x}-0000-4000-8000-"
                    f"{offset:012x}"
                )

            def matrix_claim(label, session_id):
                return TurnClaim(
                    turn_id=f"{label}-turn",
                    project_id=f"{label}-project",
                    sequence=1,
                    worker_id=f"{label}-worker",
                    attempt_id=f"{label}-attempt",
                    lease_generation=1,
                    fencing_token=1,
                    lease_expires_at=100,
                    canonical_session_id=session_id,
                )

            def batch_shape(database, batch_id):
                row = database._conn.execute(
                    """
                    SELECT batch_id, kind, terminal_status, operation_id,
                           approval_id, state, published_at,
                           projects_acknowledged_at,
                           transcript_conflict_key,
                           observed_message_count, remediated_at,
                           discard_authority
                    FROM project_turn_transcript_batches
                    WHERE batch_id = ?
                    """,
                    (batch_id,),
                ).fetchone()
                return tuple(row)

            def rejected_crossed_shape(
                database,
                batch_id,
                statement,
                parameters,
            ):
                before = batch_shape(database, batch_id)
                savepoint = (
                    "c10_negative_"
                    + batch_id.replace("-", "_")
                )
                database._conn.execute(f"SAVEPOINT {savepoint}")
                try:
                    with pytest.raises(sqlite3.IntegrityError):
                        database._conn.execute(
                            statement,
                            (*parameters, batch_id),
                        )
                finally:
                    database._conn.execute(
                        f"ROLLBACK TO SAVEPOINT {savepoint}"
                    )
                    database._conn.execute(
                        f"RELEASE SAVEPOINT {savepoint}"
                    )
                assert batch_shape(database, batch_id) == before

            def exercise_conflict_matrix(
                database,
                label,
                base,
            ):
                session_id = f"c10-{label}-matrix-session"
                database.create_session(session_id, source="cli")
                transcript = (
                    {
                        "role": "user",
                        "content": f"{label} user",
                        "timestamp": 1.0,
                    },
                    {
                        "role": "assistant",
                        "content": f"{label} assistant",
                        "timestamp": 2.0,
                    },
                )
                approval_valid_id = matrix_batch_id(base, 1)
                terminal_valid_id = matrix_batch_id(base, 2)
                approval_cross_id = matrix_batch_id(base, 3)
                terminal_cross_id = matrix_batch_id(base, 4)
                terminal_transition_id = matrix_batch_id(base, 5)
                database.prepare_approval_checkpoint(
                    matrix_claim(
                        f"{label}-approval-valid",
                        session_id,
                    ),
                    batch_id=approval_valid_id,
                    operation_id=f"{label}-operation-valid",
                    approval_id=f"{label}-approval-valid",
                    base_message_count=0,
                    messages=transcript,
                )
                database.prepare_terminal_result(
                    matrix_claim(
                        f"{label}-terminal-valid",
                        session_id,
                    ),
                    batch_id=terminal_valid_id,
                    status="succeeded",
                    base_message_count=0,
                    messages=transcript,
                )
                database.prepare_approval_checkpoint(
                    matrix_claim(
                        f"{label}-approval-cross",
                        session_id,
                    ),
                    batch_id=approval_cross_id,
                    operation_id=f"{label}-operation-cross",
                    approval_id=f"{label}-approval-cross",
                    base_message_count=0,
                    messages=transcript,
                )
                database.prepare_terminal_result(
                    matrix_claim(
                        f"{label}-terminal-cross",
                        session_id,
                    ),
                    batch_id=terminal_cross_id,
                    status="failed",
                    base_message_count=0,
                    messages=transcript,
                )
                database.prepare_terminal_result(
                    matrix_claim(
                        f"{label}-terminal-transition",
                        session_id,
                    ),
                    batch_id=terminal_transition_id,
                    status="failed",
                    base_message_count=0,
                    messages=transcript,
                )

                database._conn.execute(
                    """
                    UPDATE project_turn_transcript_batches
                    SET state = 'conflicted'
                    WHERE batch_id = ?
                    """,
                    (approval_valid_id,),
                )
                assert batch_shape(
                    database,
                    approval_valid_id,
                )[1:] == (
                    "approval_checkpoint",
                    None,
                    f"{label}-operation-valid",
                    f"{label}-approval-valid",
                    "conflicted",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                database._conn.commit()
                rejected_crossed_shape(
                    database,
                    approval_valid_id,
                    """
                    UPDATE project_turn_transcript_batches
                    SET state = 'prepared'
                    WHERE batch_id = ?
                    """,
                    (),
                )

                valid_key = (
                    "transcript-conflict-"
                    + "0123456789abcdef" * 4
                )
                database._conn.execute(
                    """
                    UPDATE project_turn_transcript_batches
                    SET state = 'conflict_pending',
                        transcript_conflict_key = ?,
                        observed_message_count = 0
                    WHERE batch_id = ?
                    """,
                    (valid_key, terminal_valid_id),
                )
                database._conn.execute(
                    """
                    UPDATE project_turn_transcript_batches
                    SET state = 'conflicted', remediated_at = 1
                    WHERE batch_id = ?
                    """,
                    (terminal_valid_id,),
                )
                assert batch_shape(
                    database,
                    terminal_valid_id,
                )[1:] == (
                    "terminal_result",
                    "succeeded",
                    None,
                    None,
                    "conflicted",
                    None,
                    None,
                    valid_key,
                    0,
                    1,
                    None,
                )
                database._conn.commit()

                rejected_crossed_shape(
                    database,
                    approval_cross_id,
                    """
                    UPDATE project_turn_transcript_batches
                    SET state = 'conflict_pending',
                        transcript_conflict_key = ?,
                        observed_message_count = 0
                    WHERE batch_id = ?
                    """,
                    (valid_key,),
                )
                rejected_crossed_shape(
                    database,
                    terminal_cross_id,
                    """
                    UPDATE project_turn_transcript_batches
                    SET state = 'conflicted',
                        transcript_conflict_key = NULL,
                        observed_message_count = NULL,
                        remediated_at = NULL
                    WHERE batch_id = ?
                    """,
                    (),
                )
                rejected_crossed_shape(
                    database,
                    terminal_transition_id,
                    """
                    UPDATE project_turn_transcript_batches
                    SET state = 'conflicted',
                        transcript_conflict_key = ?,
                        observed_message_count = 0,
                        remediated_at = 1
                    WHERE batch_id = ?
                    """,
                    (valid_key,),
                )

            fresh_schema = SessionDB(
                db_path=tmp_path / "c10-fresh-schema.db"
            )
            try:
                assert_named_batch_objects(fresh_schema)
                exercise_conflict_matrix(
                    fresh_schema,
                    "fresh",
                    0x70000000,
                )
            finally:
                fresh_schema.close()
            assert_named_batch_objects(upgraded)
            exercise_conflict_matrix(
                upgraded,
                "upgraded",
                0x71000000,
            )
        finally:
            upgraded.close()

        assert not ledger_violations
        assert active_transactions == {
            "state": set(),
            "projects": set(),
        }
        assert factory_records
        assert len(
            {record["connection_id"] for record in factory_records}
        ) == len(factory_records)
        assert all(
            record["closed"]
            and record["owner_thread"] == record["close_thread"]
            and record["owner_thread"] != loop_thread
            for record in factory_records
        )
    finally:
        conn.close()
        state.close()
