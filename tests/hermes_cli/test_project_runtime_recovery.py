"""Task 5 contract and recovery tests for the durable project runtime."""

from __future__ import annotations

import importlib
import inspect
import json
import os
import queue
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import fields, replace
from enum import Enum
from pathlib import Path
from typing import Literal, Protocol, get_type_hints

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import ActorContext


def _make_runtime(path, *, now=100, clock=None):
    conn = projects_db.connect(path)
    project_id = projects_db.create_project(conn, name="Recovery")
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="session-root",
        current_phase="implementation",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="owner-binding",
        project_id=project_id,
        surface="desktop",
        external_binding_id=f"window-{project_id}",
        actor_id="owner",
        now=1,
    )
    module = importlib.import_module("hermes_cli.project_runtime")
    runtime = module.ProjectRuntime(conn, clock=clock or (lambda: now))
    actor = ActorContext("owner", "desktop", "owner-binding", True)
    return module, conn, runtime, project_id, actor


def _claim_snapshot(conn, project_id, turn_id):
    return (
        tuple(
            conn.execute(
                """
                SELECT * FROM project_turns
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        ),
        tuple(
            conn.execute(
                """
                SELECT * FROM project_run_controls
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        ),
        tuple(
            conn.execute(
                """
                SELECT * FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        )
        if conn.execute(
            """
            SELECT 1 FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn_id),
        ).fetchone()
        is not None
        else None,
        prdb.runtime_state_for_project(conn, project_id),
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


def _enqueue_and_claim(runtime, project_id, actor, *, key="turn"):
    turn = runtime.enqueue_turn(
        project_id,
        {"message": key},
        actor,
        idempotency_key=key,
        expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    assert claim is not None
    return turn, claim


_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKER_PROBE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "project_runtime_worker_probe.py"
)


class _ProbeHandle:
    def __init__(self, process, probe_id):
        self.process = process
        self.probe_id = probe_id
        self.stdout = queue.Queue()
        self.stderr = []
        self.threads = [
            threading.Thread(
                target=self._pump_stdout,
                name=f"probe-stdout-{probe_id}",
                daemon=True,
            ),
            threading.Thread(
                target=self._pump_stderr,
                name=f"probe-stderr-{probe_id}",
                daemon=True,
            ),
        ]
        for thread in self.threads:
            thread.start()

    def _pump_stdout(self):
        for line in self.process.stdout:
            self.stdout.put(line)
        self.stdout.put(None)

    def _pump_stderr(self):
        self.stderr.extend(self.process.stderr)

    def send(self, event):
        self.process.stdin.write(
            json.dumps(event, sort_keys=True, separators=(",", ":"))
            + "\n"
        )
        self.process.stdin.flush()

    def expect(self, event, *, timeout=15):
        try:
            line = self.stdout.get(timeout=timeout)
        except queue.Empty as exc:
            raise AssertionError(
                f"probe {self.probe_id} timed out; "
                f"stderr={''.join(self.stderr)!r}"
            ) from exc
        if line is None:
            raise AssertionError(
                f"probe {self.probe_id} exited before {event}; "
                f"returncode={self.process.poll()}; "
                f"stderr={''.join(self.stderr)!r}"
            )
        payload = json.loads(line)
        assert payload["version"] == 1
        assert payload["probe_id"] == self.probe_id
        assert payload["event"] == event
        return payload

    def complete(self, *, returncode=0, timeout=15):
        actual = self.process.wait(timeout=timeout)
        for thread in self.threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
        extras = []
        while True:
            try:
                line = self.stdout.get_nowait()
            except queue.Empty:
                break
            if line is not None:
                extras.append(line)
        assert actual == returncode, (
            self.probe_id,
            actual,
            "".join(self.stderr),
        )
        assert extras == []


class _ProbeSet:
    def __init__(self):
        self.processes = []
        self.handles = []

    def __enter__(self):
        return self

    def spawn(self, prepare):
        environment = os.environ.copy()
        environment["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (str(_REPO_ROOT), environment.get("PYTHONPATH")),
            )
        )
        environment["PYTHONUTF8"] = "1"
        environment["PYTHONIOENCODING"] = "utf-8"
        process = subprocess.Popen(
            [sys.executable, str(_WORKER_PROBE)],
            cwd=_REPO_ROOT,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        self.processes.append(process)
        handle = _ProbeHandle(process, prepare["probe_id"])
        self.handles.append(handle)
        handle.send(prepare)
        return handle

    def __exit__(self, exc_type, exc, traceback):
        errors = []

        def is_alive(process, label):
            try:
                return process.poll() is None
            except BaseException as cleanup_error:
                errors.append(f"{label} poll: {cleanup_error!r}")
                return True

        def attempt(label, action):
            try:
                action()
            except BaseException as cleanup_error:
                errors.append(f"{label}: {cleanup_error!r}")

        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                attempt(
                    f"process {index} terminate", process.terminate
                )
        deadline = time.monotonic() + 5
        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                try:
                    process.wait(
                        timeout=max(0.01, deadline - time.monotonic())
                    )
                except subprocess.TimeoutExpired:
                    pass
                except BaseException as cleanup_error:
                    errors.append(
                        f"process {index} terminate wait: "
                        f"{cleanup_error!r}"
                    )
        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                attempt(f"process {index} kill", process.kill)
        deadline = time.monotonic() + 5
        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                try:
                    process.wait(
                        timeout=max(0.01, deadline - time.monotonic())
                    )
                except subprocess.TimeoutExpired as cleanup_error:
                    errors.append(
                        f"process {index} kill wait: "
                        f"{cleanup_error!r}"
                    )
                except BaseException as cleanup_error:
                    errors.append(
                        f"process {index} kill wait: "
                        f"{cleanup_error!r}"
                    )
        for process_index, process in enumerate(self.processes):
            for stream in (
                process.stdin,
                process.stdout,
                process.stderr,
            ):
                if stream is not None:
                    attempt(
                        f"process {process_index} stream close",
                        stream.close,
                    )
        for handle_index, handle in enumerate(self.handles):
            for thread_index, thread in enumerate(handle.threads):
                attempt(
                    f"handle {handle_index} thread {thread_index} join",
                    lambda thread=thread: thread.join(timeout=5),
                )
        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                errors.append(f"process {index} is still alive")
        for handle_index, handle in enumerate(self.handles):
            for thread_index, thread in enumerate(handle.threads):
                try:
                    alive = thread.is_alive()
                except BaseException as cleanup_error:
                    errors.append(
                        f"handle {handle_index} thread {thread_index} "
                        f"status: {cleanup_error!r}"
                    )
                    continue
                if alive:
                    errors.append(
                        f"handle {handle_index} thread {thread_index} "
                        "is still alive"
                    )
        if errors:
            message = "probe cleanup failed: " + "; ".join(errors)
            if exc is not None:
                attempt("primary exception note", lambda: exc.add_note(message))
            else:
                raise AssertionError(message)
        return False


def _probe_prepare(
    probe_id,
    action,
    path,
    project_id,
    worker_id,
    now,
    **extra,
):
    return {
        "version": 1,
        "event": "prepare",
        "probe_id": probe_id,
        "action": action,
        "db_path": str(path),
        "project_id": project_id,
        "worker_id": worker_id,
        "now": now,
        **extra,
    }


def _release_probes(handles):
    for handle in handles:
        ready = handle.expect("ready")
        assert ready["stage"] == "before_begin_immediate"
    barrier = threading.Barrier(len(handles) + 1)
    errors = []

    def release(handle):
        try:
            barrier.wait(timeout=5)
            handle.send(
                {
                    "version": 1,
                    "event": "go",
                    "probe_id": handle.probe_id,
                }
            )
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=release, args=(handle,), daemon=True)
        for handle in handles
    ]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    assert errors == []


def _run_probe(
    probes,
    prepare,
    *,
    expected_event="result",
    returncode=0,
):
    handle = probes.spawn(prepare)
    _release_probes([handle])
    payload = handle.expect(expected_event)
    handle.complete(returncode=returncode)
    return payload


def test_probe_cleanup_visits_all_children_after_one_cleanup_error():
    class _Stream:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    class _Process:
        def __init__(self, *, fail_terminate=False):
            self.fail_terminate = fail_terminate
            self.returncode = None
            self.calls = []
            self.stdin = _Stream()
            self.stdout = _Stream()
            self.stderr = _Stream()

        def poll(self):
            return self.returncode

        def terminate(self):
            self.calls.append("terminate")
            if self.fail_terminate:
                raise OSError("terminate failed")
            self.returncode = -1

        def wait(self, timeout):
            self.calls.append("wait")
            if self.returncode is None:
                raise subprocess.TimeoutExpired("probe", timeout)
            return self.returncode

        def kill(self):
            self.calls.append("kill")
            self.returncode = -9

    class _Thread:
        def __init__(self):
            self.joined = False

        def join(self, timeout):
            self.joined = True

        def is_alive(self):
            return not self.joined

    class _Handle:
        def __init__(self):
            self.threads = [_Thread(), _Thread()]

    first = _Process(fail_terminate=True)
    second = _Process()
    probes = _ProbeSet()
    probes.processes = [first, second]
    probes.handles = [_Handle(), _Handle()]

    with pytest.raises(AssertionError, match="cleanup"):
        probes.__exit__(None, None, None)

    assert "kill" in first.calls
    assert "terminate" in second.calls
    assert all(
        stream.closed
        for process in (first, second)
        for stream in (process.stdin, process.stdout, process.stderr)
    )
    assert all(
        thread.joined
        for handle in probes.handles
        for thread in handle.threads
    )
    assert all(process.poll() is not None for process in (first, second))


class _RecordingReadback:
    def __init__(self, conn, result=None, *, error=None, barrier=None):
        self.conn = conn
        self.result = result
        self.error = error
        self.barrier = barrier
        self.calls = []

    def read_turn(self, request):
        assert self.conn.in_transaction is False
        self.calls.append(request)
        if self.barrier is not None:
            self.barrier.wait(timeout=5)
        if self.error is not None:
            raise self.error
        return self.result


def _legacy_task4_turn_database(path):
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO projects VALUES ('legacy', 'legacy', 'Legacy', 1, 0);
        CREATE TABLE project_turns (
            turn_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL
                REFERENCES projects(id) ON DELETE RESTRICT,
            sequence INTEGER NOT NULL,
            idempotency_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            origin_binding_id TEXT,
            status TEXT NOT NULL,
            attempt_id TEXT,
            lease_generation INTEGER NOT NULL DEFAULT 0,
            fencing_token INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE (project_id, turn_id),
            UNIQUE (project_id, sequence),
            UNIQUE (project_id, idempotency_key)
        );
        INSERT INTO project_turns VALUES
            ('queued', 'legacy', 1, 'q', '{}', NULL, 'queued',
             NULL, 0, 0, 1, 1),
            ('claimed', 'legacy', 2, 'c', '{}', NULL, 'claimed',
             'attempt-c', 1, 1, 2, 2),
            ('stopped', 'legacy', 3, 's', '{}', NULL, 'stopped',
             'attempt-s', 2, 2, 3, 3),
            ('terminal', 'legacy', 4, 't', '{}', NULL, 'succeeded',
             'attempt-t', 3, 3, 4, 4);
        """
    )
    conn.commit()
    return conn


def test_task4_public_values_remain_exact_and_task9_surface_stays_absent():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert tuple(field.name for field in fields(module.ProjectTurn)) == (
        "turn_id",
        "project_id",
        "sequence",
        "idempotency_key",
        "payload",
        "origin_binding_id",
        "status",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "created_at",
        "updated_at",
    )
    assert tuple(field.name for field in fields(module.RunControl)) == (
        "turn_id",
        "project_id",
        "control_state",
        "control_version",
        "last_idempotency_key",
        "attempt_id",
        "updated_at",
    )
    assert tuple(field.name for field in fields(module.TurnClaim)) == (
        "turn_id",
        "project_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    )
    assert not hasattr(module, "ProjectSnapshot")
    assert not hasattr(module.ProjectRuntime, "execute_command")


def test_task5_aliases_and_frozen_dtos_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert module.TerminalTurnStatus == Literal["succeeded", "failed"]
    assert module.ExecutionState == Literal["not_started", "started"]
    assert module.RecoverySourceStatus == Literal["claimed", "stop_requested"]
    assert module.ReadbackOutcome == Literal[
        "succeeded", "failed", "stopped", "unknown"
    ]
    assert tuple(field.name for field in fields(module.CanonicalTurnResult)) == (
        "status",
        "result_id",
    )
    assert tuple(field.name for field in fields(module.TurnReadbackRequest)) == (
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
        "source_status",
        "execution_state",
    )
    assert tuple(field.name for field in fields(module.TurnReadbackResult)) == (
        "outcome",
        "result_id",
    )
    assert module.CanonicalTurnResult.__dataclass_params__.frozen is True
    assert module.TurnReadbackRequest.__dataclass_params__.frozen is True
    assert module.TurnReadbackResult.__dataclass_params__.frozen is True
    assert get_type_hints(module.CanonicalTurnResult) == {
        "status": module.TerminalTurnStatus,
        "result_id": str,
    }
    assert get_type_hints(module.TurnReadbackResult) == {
        "outcome": module.ReadbackOutcome,
        "result_id": str | None,
    }


def test_task5_readback_protocol_has_one_exact_method():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert issubclass(module.TurnReadbackPort, Protocol)
    assert {
        name
        for name in module.TurnReadbackPort.__dict__
        if not name.startswith("_")
    } == {"read_turn"}
    signature = inspect.signature(module.TurnReadbackPort.read_turn)
    assert tuple(signature.parameters) == ("self", "request")
    assert get_type_hints(module.TurnReadbackPort.read_turn) == {
        "request": module.TurnReadbackRequest,
        "return": module.TurnReadbackResult,
    }


def test_task5_service_signatures_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")
    runtime = module.ProjectRuntime

    expected_parameters = {
        "heartbeat_turn": ("self", "claim", "lease_seconds"),
        "mark_turn_started": ("self", "claim"),
        "commit_turn": ("self", "claim", "result"),
        "reconcile_inflight_turns": ("self", "readback", "limit"),
    }
    expected_hints = {
        "heartbeat_turn": {
            "claim": module.TurnClaim,
            "lease_seconds": int,
            "return": module.TurnClaim,
        },
        "mark_turn_started": {
            "claim": module.TurnClaim,
            "return": module.TurnClaim,
        },
        "commit_turn": {
            "claim": module.TurnClaim,
            "result": module.CanonicalTurnResult,
            "return": module.ProjectTurn,
        },
        "reconcile_inflight_turns": {
            "readback": module.TurnReadbackPort,
            "limit": int,
            "return": tuple[module.ProjectTurn, ...],
        },
    }

    for name, parameters in expected_parameters.items():
        method = getattr(runtime, name)
        signature = inspect.signature(method)
        assert tuple(signature.parameters) == parameters
        assert get_type_hints(method) == expected_hints[name]
    assert (
        inspect.signature(runtime.heartbeat_turn).parameters[
            "lease_seconds"
        ].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    assert (
        inspect.signature(runtime.reconcile_inflight_turns).parameters[
            "limit"
        ].kind
        is inspect.Parameter.KEYWORD_ONLY
    )


def test_task5_stable_error_codes_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert module.RuntimeErrorCode.STALE_TURN_CLAIM.value == "stale_turn_claim"
    assert (
        module.RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED.value
        == "turn_execution_not_started"
    )
    assert (
        module.RuntimeErrorCode.TERMINAL_RESULT_CONFLICT.value
        == "terminal_result_conflict"
    )


def test_fresh_task5_turn_schema_enforces_metadata_and_indexes(tmp_path):
    conn = projects_db.connect(tmp_path / "fresh.db")
    try:
        columns = {
            row["name"]: row
            for row in conn.execute("PRAGMA table_info(project_turns)")
        }
        indexes = {
            row["name"]: row
            for row in conn.execute("PRAGMA index_list(project_turns)")
        }
        lease_indexes = {
            row["name"]
            for row in conn.execute("PRAGMA index_list(project_worker_leases)")
        }
        event_indexes = {
            row["name"]: row
            for row in conn.execute("PRAGMA index_list(project_events)")
        }
        assert columns["execution_state"]["notnull"] == 0
        assert columns["terminal_result_id"]["notnull"] == 0
        assert columns["recovery_block_key"]["notnull"] == 0
        assert indexes["idx_project_turns_terminal_result"]["unique"] == 1
        assert "idx_project_turns_project_sequence" in indexes
        assert "idx_project_turns_actionable_recovery" in indexes
        assert "idx_project_worker_leases_expiry" in lease_indexes
        assert (
            event_indexes[
                "idx_project_events_recovery_block_attempt"
            ]["unique"]
            == 1
        )

        project_id = projects_db.create_project(conn, name="Schema checks")
        common = (
            project_id,
            "{}",
            "claimed",
            "attempt",
            1,
            1,
            1,
            1,
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, created_at, updated_at, execution_state
                ) VALUES ('bad-execution', ?, 1, 'bad-execution', ?, ?, ?, ?,
                          ?, ?, ?, 'unknown')
                """,
                common,
            )
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, created_at, updated_at, terminal_result_id
                ) VALUES ('bad-result', ?, 1, 'bad-result', ?, ?, ?, ?, ?, ?,
                          ?, '')
                """,
                common,
            )
    finally:
        conn.close()


def test_task4_turn_rows_migrate_additively_without_backfill_or_events(tmp_path):
    conn = _legacy_task4_turn_database(tmp_path / "legacy.db")
    old_columns = (
        "turn_id",
        "project_id",
        "sequence",
        "idempotency_key",
        "payload_json",
        "origin_binding_id",
        "status",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "created_at",
        "updated_at",
    )
    before = tuple(
        tuple(row)
        for row in conn.execute(
            f"SELECT {', '.join(old_columns)} FROM project_turns ORDER BY sequence"
        )
    )

    prdb.ensure_schema(conn)
    prdb.ensure_schema(conn)

    after = tuple(
        tuple(row)
        for row in conn.execute(
            f"SELECT {', '.join(old_columns)} FROM project_turns ORDER BY sequence"
        )
    )
    task5_values = tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT execution_state, terminal_result_id, recovery_block_key
            FROM project_turns ORDER BY sequence
            """
        )
    )
    assert after == before
    assert task5_values == ((None, None, None),) * 4
    assert conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE kind = 'turn.recovery_blocked'"
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.parametrize(
    ("execution_state", "terminal_result_id"),
    [
        pytest.param("unknown", None, id="execution-enum"),
        pytest.param(None, "", id="empty-terminal-result"),
    ],
)
def test_task5_turn_mapper_fails_closed_on_malformed_metadata(
    execution_state, terminal_result_id
):
    row = {
        "turn_id": "turn",
        "project_id": "project",
        "sequence": 1,
        "idempotency_key": "key",
        "payload_json": "{}",
        "origin_binding_id": "binding",
        "status": "queued",
        "attempt_id": None,
        "lease_generation": 0,
        "fencing_token": 0,
        "created_at": 1,
        "updated_at": 1,
        "execution_state": execution_state,
        "terminal_result_id": terminal_result_id,
        "recovery_block_key": None,
    }

    with pytest.raises(RuntimeError):
        prdb.runtime_turn_from_row(row)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {"execution_state": "started"},
            id="pristine-queued-execution-state",
        ),
        pytest.param(
            {"terminal_result_id": "queued-result"},
            id="nonterminal-result",
        ),
        pytest.param(
            {
                "status": "succeeded",
                "terminal_result_id": "orphan-result",
            },
            id="terminal-result-without-attempt",
        ),
        pytest.param(
            {
                "status": "succeeded",
                "attempt_id": "attempt",
                "lease_generation": 1,
                "fencing_token": 1,
                "execution_state": "not_started",
                "terminal_result_id": "impossible-result",
            },
            id="terminal-result-before-start",
        ),
        pytest.param(
            {
                "status": "cancelled",
                "attempt_id": "attempt",
                "lease_generation": 1,
                "fencing_token": 1,
                "execution_state": "started",
                "terminal_result_id": "cancel-result",
            },
            id="result-on-non-result-terminal",
        ),
        pytest.param(
            {"recovery_block_key": "unexpected-block-key"},
            id="block-key-on-pristine-queued",
        ),
    ],
)
def test_task5_turn_mapper_rejects_impossible_metadata_combinations(
    overrides,
):
    row = {
        "turn_id": "turn",
        "project_id": "project",
        "sequence": 1,
        "idempotency_key": "key",
        "payload_json": "{}",
        "origin_binding_id": "binding",
        "status": "queued",
        "attempt_id": None,
        "lease_generation": 0,
        "fencing_token": 0,
        "execution_state": None,
        "terminal_result_id": None,
        "recovery_block_key": None,
        "created_at": 1,
        "updated_at": 1,
    }
    row.update(overrides)

    with pytest.raises(RuntimeError):
        prdb.runtime_turn_from_row(row)


def test_pair_validator_rejects_impossible_task5_metadata(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "pair-metadata.db"
    )
    try:
        turn, _ = _enqueue_and_claim(runtime, project_id, actor)
        persisted = prdb._runtime_turn_for_project(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        assert persisted is not None

        with pytest.raises(RuntimeError):
            prdb._validate_runtime_turn_pair(
                conn,
                turn=replace(
                    persisted, terminal_result_id="nonterminal-result"
                ),
            )
    finally:
        conn.close()


def test_claim_scan_rejects_historical_orphan_terminal_result(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "historical-orphan-result.db"
    )
    try:
        with prdb.write_transaction(conn):
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, origin_binding_id, status, attempt_id,
                    lease_generation, fencing_token, execution_state,
                    terminal_result_id, created_at, updated_at
                ) VALUES (
                    'historical', ?, 1, 'historical', '{}',
                    'owner-binding', 'succeeded', NULL, 0, 0, NULL,
                    'orphan-result', 1, 1
                )
                """,
                (project_id,),
            )
            conn.execute(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    idempotency_key, command_fingerprint, attempt_id,
                    claim_worker_id, claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (
                    'historical', ?, 'terminal', 0, NULL, NULL, NULL,
                    NULL, NULL, NULL, 1
                )
                """,
                (project_id,),
            )
        target = runtime.enqueue_turn(
            project_id,
            {"message": "must remain queued"},
            actor,
            idempotency_key="target",
            expected_version=0,
        )
        before = _claim_snapshot(conn, project_id, target.turn_id)

        with pytest.raises(RuntimeError):
            runtime.claim_next_turn(
                project_id, "worker", lease_seconds=30
            )

        assert _claim_snapshot(conn, project_id, target.turn_id) == before
    finally:
        conn.close()


def test_valid_resumed_queued_attempt_metadata_remains_accepted(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "valid-resumed-metadata.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
        runtime.acknowledge_stopped(claim)
        runtime.request_resume(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="resume",
            expected_version=4,
            expected_control_version=3,
        )

        resumed = prdb._runtime_turn_for_project(
            conn, project_id=project_id, turn_id=turn.turn_id
        )

        assert resumed is not None
        assert resumed.status == "queued"
        assert resumed.attempt_id == claim.attempt_id
        assert resumed.execution_state == "started"
        assert resumed.terminal_result_id is None
        prdb._validate_runtime_turn_pair(conn, turn=resumed)
    finally:
        conn.close()


def test_new_claim_persists_not_started_in_the_atomic_claim(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(tmp_path / "claim.db")
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "claim"},
            actor,
            idempotency_key="claim",
            expected_version=0,
        )
        claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)

        assert claim is not None
        row = conn.execute(
            """
            SELECT execution_state, terminal_result_id, recovery_block_key
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()
        assert tuple(row) == ("not_started", None, None)
    finally:
        conn.close()


def test_two_connections_racing_task5_migration_converge(tmp_path):
    path = tmp_path / "migration-race.db"
    _legacy_task4_turn_database(path).close()
    barrier = threading.Barrier(2)

    def migrate():
        conn = sqlite3.connect(path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            barrier.wait(timeout=5)
            prdb.ensure_schema(conn)
            conn.commit()
            return {
                row["name"]
                for row in conn.execute("PRAGMA table_info(project_turns)")
            }
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(migrate) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]

    assert all(
        {
            "execution_state",
            "terminal_result_id",
            "recovery_block_key",
        }
        <= columns
        for columns in results
    )


def test_preprojection_recovery_block_migrates_to_indexed_key(tmp_path):
    path = tmp_path / "legacy-recovery-block.db"
    project_id = "legacy-project"
    turn_id = "legacy-turn"
    attempt_id = "legacy-attempt"
    block_key = prdb._recovery_block_key(
        project_id=project_id,
        turn_id=turn_id,
        attempt_id=attempt_id,
        lease_generation=1,
        fencing_token=1,
    )
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO projects VALUES (
            'legacy-project', 'legacy-project', 'Legacy', 1, 0
        );
        CREATE TABLE project_conversations (
            conversation_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES projects(id),
            parent_conversation_id TEXT,
            root_conversation_id TEXT,
            created_at INTEGER NOT NULL,
            UNIQUE(project_id, conversation_id)
        );
        INSERT INTO project_conversations VALUES (
            'legacy-root', 'legacy-project', NULL, 'legacy-root', 1
        );
        CREATE TABLE project_runtime_state (
            project_id TEXT PRIMARY KEY,
            lifecycle TEXT NOT NULL,
            current_phase TEXT,
            version INTEGER NOT NULL,
            conversation_root_id TEXT,
            conversation_tip_id TEXT,
            updated_at INTEGER NOT NULL
        );
        INSERT INTO project_runtime_state VALUES (
            'legacy-project', 'active', 'implementation', 2,
            'legacy-root', 'legacy-root', 1
        );
        CREATE TABLE project_turns (
            turn_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES projects(id),
            sequence INTEGER NOT NULL,
            idempotency_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            origin_binding_id TEXT,
            status TEXT NOT NULL,
            attempt_id TEXT,
            lease_generation INTEGER NOT NULL,
            fencing_token INTEGER NOT NULL,
            execution_state TEXT,
            terminal_result_id TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(project_id, turn_id),
            UNIQUE(project_id, sequence),
            UNIQUE(project_id, idempotency_key)
        );
        INSERT INTO project_turns VALUES (
            'legacy-turn', 'legacy-project', 1, 'legacy', '{}', NULL,
            'reconciling', 'legacy-attempt', 1, 1, 'started', NULL, 1, 1
        );
        CREATE TABLE project_run_controls (
            turn_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            control_state TEXT NOT NULL,
            control_version INTEGER NOT NULL,
            idempotency_key TEXT,
            command_fingerprint TEXT,
            attempt_id TEXT,
            claim_worker_id TEXT,
            claim_lease_expires_at INTEGER,
            claim_canonical_session_id TEXT,
            updated_at INTEGER NOT NULL,
            UNIQUE(project_id, turn_id),
            UNIQUE(project_id, idempotency_key)
        );
        INSERT INTO project_run_controls VALUES (
            'legacy-turn', 'legacy-project', 'running', 1, NULL, NULL,
            'legacy-attempt', 'worker', 100, 'session', 1
        );
        CREATE TABLE project_events (
            event_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            kind TEXT NOT NULL,
            turn_id TEXT,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(project_id, event_id),
            UNIQUE(project_id, sequence)
        );
        """
    )
    conn.execute(
        """
        INSERT INTO project_events VALUES (
            ?, ?, 1, 'turn.recovery_blocked', ?, ?, 1
        )
        """,
        (
            block_key,
            project_id,
            turn_id,
            json.dumps(
                {
                    "attempt_id": attempt_id,
                    "fencing_token": 1,
                    "lease_generation": 1,
                    "source_status": "claimed",
                    "turn_id": turn_id,
                    "version": 2,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        ),
    )
    conn.commit()
    conn.close()

    migrated = projects_db.connect(path)
    try:
        assert migrated.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn_id,),
        ).fetchone()[0] == block_key
        turn = prdb._runtime_turn_for_project(
            migrated, project_id=project_id, turn_id=turn_id
        )
        prdb._validate_runtime_turn_pair(migrated, turn=turn)
    finally:
        migrated.close()


@pytest.mark.parametrize("boolean_field", ["lease_generation", "fencing_token"])
def test_preprojection_migration_rejects_boolean_block_identity(
    tmp_path, boolean_field
):
    path = tmp_path / f"legacy-boolean-{boolean_field}.db"
    block_key = prdb._recovery_block_key(
        project_id="legacy",
        turn_id="claimed",
        attempt_id="attempt-c",
        lease_generation=1,
        fencing_token=1,
    )
    conn = _legacy_task4_turn_database(path)
    conn.executescript(
        """
        ALTER TABLE project_turns ADD COLUMN execution_state TEXT;
        ALTER TABLE project_turns ADD COLUMN terminal_result_id TEXT;
        UPDATE project_turns
        SET status = 'reconciling', execution_state = 'started'
        WHERE turn_id = 'claimed';
        CREATE TABLE project_events (
            event_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            kind TEXT NOT NULL,
            turn_id TEXT,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(project_id, event_id),
            UNIQUE(project_id, sequence)
        );
        """
    )
    payload = {
        "attempt_id": "attempt-c",
        "fencing_token": 1,
        "lease_generation": 1,
        "source_status": "claimed",
        "turn_id": "claimed",
        "version": 1,
    }
    payload[boolean_field] = True
    conn.execute(
        """
        INSERT INTO project_events VALUES (
            ?, 'legacy', 1, 'turn.recovery_blocked',
            'claimed', ?, 1
        )
        """,
        (
            block_key,
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
        ),
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="block event payload"):
        projects_db.connect(path)

    raw = sqlite3.connect(path)
    try:
        columns = {
            row[1] for row in raw.execute("PRAGMA table_info(project_turns)")
        }
        assert "recovery_block_key" not in columns
    finally:
        raw.close()


def test_heartbeat_extends_both_horizons_without_versions_or_events(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 110

        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)

        assert renewed == replace(claim, lease_expires_at=160)
        lease = prdb._current_worker_lease_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        control = prdb._runtime_control_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        assert lease is not None and lease.expires_at == 160
        assert control is not None and control.claim_lease_expires_at == 160
        assert control.control_version == 1
        assert prdb.runtime_state_for_project(conn, project_id).version == 2
        assert len(
            conn.execute(
                "SELECT 1 FROM project_events WHERE project_id = ?",
                (project_id,),
            ).fetchall()
        ) == 2
        assert before[0][:-1] == _claim_snapshot(
            conn, project_id, turn.turn_id
        )[0][:-1]
        assert module.RuntimeErrorCode.STALE_TURN_CLAIM.value == "stale_turn_claim"
    finally:
        conn.close()


def test_heartbeat_lost_response_retry_uses_observed_horizon_and_never_shortens(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat-retry.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = 110
        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)
        first_snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 120

        replay = runtime.heartbeat_turn(claim, lease_seconds=10)

        assert replay == renewed
        assert _claim_snapshot(conn, project_id, turn.turn_id) == first_snapshot
        with pytest.raises(module.ProjectRuntimeError) as greater:
            runtime.heartbeat_turn(
                replace(renewed, lease_expires_at=161),
                lease_seconds=50,
            )
        assert greater.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == first_snapshot
    finally:
        conn.close()


@pytest.mark.parametrize(
    "field_name",
    [
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    ],
)
def test_heartbeat_rejects_each_forged_authority_field_without_writes(
    tmp_path, field_name
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-forged-{field_name}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        value = getattr(claim, field_name)
        forged_value = f"{value}-forged" if type(value) is str else value + 1
        forged = replace(claim, **{field_name: forged_value})
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(forged, lease_seconds=60)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize("lease_seconds", [True, 0, -1, 1.5])
def test_heartbeat_rejects_invalid_ttl_before_writing(tmp_path, lease_seconds):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-invalid-{lease_seconds}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=lease_seconds)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_heartbeat_rejects_sqlite_integer_overflow_without_writes(tmp_path):
    now = (1 << 63) - 2
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat-overflow.db", clock=lambda: now
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "overflow"},
            actor,
            idempotency_key="overflow",
            expected_version=0,
        )
        conn.execute(
            """
            UPDATE project_turns
            SET status = 'claimed', attempt_id = 'attempt', lease_generation = 1,
                fencing_token = 1, execution_state = 'not_started'
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        )
        conn.execute(
            """
            UPDATE project_run_controls
            SET control_state = 'running', control_version = 1,
                attempt_id = 'attempt', claim_worker_id = 'worker',
                claim_lease_expires_at = ?,
                claim_canonical_session_id = 'session-root'
            WHERE project_id = ? AND turn_id = ?
            """,
            ((1 << 63) - 1, project_id, turn.turn_id),
        )
        conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id, lease_generation,
                fencing_token, expires_at, updated_at
            ) VALUES ('attempt', ?, ?, 'worker', 1, 1, ?, 1)
            """,
            (project_id, turn.turn_id, (1 << 63) - 1),
        )
        conn.commit()
        claim = module.TurnClaim(
            turn.turn_id,
            project_id,
            turn.sequence,
            "worker",
            "attempt",
            1,
            1,
            (1 << 63) - 1,
            "session-root",
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=2)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("turn_status", "control_state"),
    [
        pytest.param("stop_requested", "stop_requested", id="stop-requested"),
        pytest.param("awaiting_approval", "running", id="awaiting-approval"),
    ],
)
def test_heartbeat_is_allowed_for_the_exact_live_task5_status_set(
    tmp_path, turn_status, control_state
):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-{turn_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        conn.execute(
            "UPDATE project_turns SET status = ? WHERE turn_id = ?",
            (turn_status, turn.turn_id),
        )
        conn.execute(
            """
            UPDATE project_run_controls SET control_state = ?
            WHERE turn_id = ?
            """,
            (control_state, turn.turn_id),
        )
        conn.commit()

        renewed = runtime.heartbeat_turn(claim, lease_seconds=60)

        assert renewed.lease_expires_at == 160
    finally:
        conn.close()


@pytest.mark.parametrize(
    "condition",
    ["expired", "reconciling", "legacy", "inactive"],
)
def test_heartbeat_rejects_expired_or_nonlive_claim_state_without_writes(
    tmp_path, condition
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-{condition}.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if condition == "expired":
            now[0] = claim.lease_expires_at
        elif condition == "reconciling":
            conn.execute(
                "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "legacy":
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
        else:
            state = prdb.runtime_state_for_project(conn, project_id)
            prdb.transition_lifecycle(
                conn,
                project_id=project_id,
                expected_version=state.version,
                lifecycle="awaiting_acceptance",
                updated_at=101,
            )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=60)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_mark_started_is_exact_idempotent_and_metadata_only(tmp_path):
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "started.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 101

        first = runtime.mark_turn_started(claim)
        after = _claim_snapshot(conn, project_id, turn.turn_id)
        replay = runtime.mark_turn_started(claim)

        assert first == replay == claim
        assert conn.execute(
            "SELECT execution_state FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "started"
        assert after == _claim_snapshot(conn, project_id, turn.turn_id)
        assert after[1][3] == before[1][3]
        assert after[3].version == before[3].version
        assert after[4] == before[4]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "condition",
    ["expired", "awaiting_approval", "stopped", "reconciling", "legacy"],
)
def test_mark_started_rejects_every_nonlive_execution_state_without_writes(
    tmp_path, condition
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"started-{condition}.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if condition == "expired":
            now[0] = claim.lease_expires_at
        elif condition == "awaiting_approval":
            conn.execute(
                "UPDATE project_turns SET status = 'awaiting_approval' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "stopped":
            conn.execute(
                "UPDATE project_turns SET status = 'stopped' WHERE turn_id = ?",
                (turn.turn_id,),
            )
            conn.execute(
                "UPDATE project_run_controls SET control_state = 'stopped' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "reconciling":
            conn.execute(
                "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        else:
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.mark_turn_started(claim)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_expired_worker_cannot_request_approval_or_acknowledge_stop(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "existing-expiry-gaps.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        request = prdb.ApprovalRequest(
            approval_id="expired-approval",
            project_id=project_id,
            requester_actor_id="owner",
            authorization_actor_id="owner",
            canonical_action="publish",
            approval_class="publish",
            command_revision=1,
            expected_runtime_version=2,
            expected_lifecycle="active",
            expected_phase="implementation",
            targets=("C:/work/release",),
            batch_id="batch",
            batch_items=("release",),
            status="pending",
            expires_at=1000,
        )
        before_approval = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError):
            runtime.request_turn_approval(
                turn.turn_id,
                request,
                actor,
                expected_control_version=1,
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before_approval
        assert conn.execute(
            "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
            (project_id,),
        ).fetchone()[0] == 0

        stopped = runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop-at-expiry",
            expected_version=2,
            expected_control_version=1,
        )
        assert stopped.control_state == "stop_requested"
        before_ack = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError):
            runtime.acknowledge_stopped(claim)

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before_ack
    finally:
        conn.close()


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
def test_commit_started_turn_is_one_atomic_terminal_transition(
    tmp_path, terminal_status
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-{terminal_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        result = module.CanonicalTurnResult(
            status=terminal_status,
            result_id=f"result-{terminal_status}",
        )

        committed = runtime.commit_turn(claim, result)

        assert committed.status == terminal_status
        stored = conn.execute(
            """
            SELECT status, execution_state, terminal_result_id
            FROM project_turns WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()
        control = prdb._runtime_control_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        event = conn.execute(
            """
            SELECT kind, payload_json FROM project_events
            WHERE project_id = ? ORDER BY sequence DESC LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        assert tuple(stored) == (
            terminal_status,
            "started",
            f"result-{terminal_status}",
        )
        assert control.control_state == "terminal"
        assert control.control_version == before[1][3] + 1
        assert prdb.runtime_state_for_project(conn, project_id).version == (
            before[3].version + 1
        )
        assert conn.execute(
            """
            SELECT 1 FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone() is None
        assert event["kind"] == f"turn.{terminal_status}"
        assert f"result-{terminal_status}" not in event["payload_json"]
    finally:
        conn.close()


def test_commit_exact_replay_is_write_free_and_conflicts_on_changed_result(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-replay.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        result = module.CanonicalTurnResult("succeeded", "result-1")
        first = runtime.commit_turn(claim, result)
        snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 10_000

        replay = runtime.commit_turn(claim, result)

        assert replay == first
        assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
        for changed in (
            module.CanonicalTurnResult("failed", "result-1"),
            module.CanonicalTurnResult("succeeded", "result-2"),
        ):
            with pytest.raises(module.ProjectRuntimeError) as conflict:
                runtime.commit_turn(claim, changed)
            assert (
                conflict.value.code
                is module.RuntimeErrorCode.TERMINAL_RESULT_CONFLICT
            )
            assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
    finally:
        conn.close()


def test_commit_before_start_has_a_distinct_write_free_error(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-before-start.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                claim, module.CanonicalTurnResult("succeeded", "result")
            )

        assert (
            rejected.value.code
            is module.RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED
        )
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "result_factory",
    [
        pytest.param(
            lambda module: module.CanonicalTurnResult("unknown", "result"),
            id="status",
        ),
        pytest.param(
            lambda module: module.CanonicalTurnResult("succeeded", ""),
            id="empty-result",
        ),
        pytest.param(lambda module: {"status": "succeeded"}, id="mapping"),
    ],
)
def test_commit_rejects_malformed_results_without_writes(tmp_path, result_factory):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-invalid-result.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(claim, result_factory(module))

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "field_name",
    [
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    ],
)
def test_commit_rejects_each_forged_claim_field_without_writes(
    tmp_path, field_name
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-forged-{field_name}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        value = getattr(claim, field_name)
        forged_value = f"{value}-forged" if type(value) is str else value + 1
        forged = replace(claim, **{field_name: forged_value})
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                forged, module.CanonicalTurnResult("succeeded", "result")
            )

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("turn_status", "control_state"),
    [
        pytest.param("stop_requested", "stop_requested", id="stop-requested"),
        pytest.param("awaiting_approval", "running", id="approval"),
        pytest.param("reconciling", "running", id="reconciling"),
    ],
)
def test_commit_rejects_nonclaimed_statuses_without_writes(
    tmp_path, turn_status, control_state
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-{turn_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        conn.execute(
            "UPDATE project_turns SET status = ? WHERE turn_id = ?",
            (turn_status, turn.turn_id),
        )
        conn.execute(
            "UPDATE project_run_controls SET control_state = ? WHERE turn_id = ?",
            (control_state, turn.turn_id),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                claim, module.CanonicalTurnResult("succeeded", "result")
            )

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_commit_at_expiry_is_stale_but_old_observed_heartbeat_horizon_is_valid(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-expiry.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = 110
        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)
        runtime.mark_turn_started(claim)
        committed = runtime.commit_turn(
            claim, module.CanonicalTurnResult("succeeded", "old-horizon-result")
        )
        assert committed.status == "succeeded"

        second = runtime.enqueue_turn(
            project_id,
            {"message": "expired"},
            actor,
            idempotency_key="expired",
            expected_version=3,
        )
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        now[0] = second_claim.lease_expires_at
        before = _claim_snapshot(conn, project_id, second.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as stale:
            runtime.commit_turn(
                second_claim,
                module.CanonicalTurnResult("failed", "expired-result"),
            )

        assert stale.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, second.turn_id) == before
        assert renewed.lease_expires_at == 160
    finally:
        conn.close()


def test_duplicate_terminal_result_id_rolls_back_the_second_commit(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "duplicate-result.db"
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, project_id, actor, key="first"
        )
        runtime.mark_turn_started(first_claim)
        runtime.commit_turn(
            first_claim,
            module.CanonicalTurnResult("succeeded", "shared-result"),
        )
        second = runtime.enqueue_turn(
            project_id,
            {"message": "second"},
            actor,
            idempotency_key="second",
            expected_version=3,
        )
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        before = _claim_snapshot(conn, project_id, second.turn_id)

        with pytest.raises(sqlite3.IntegrityError):
            runtime.commit_turn(
                second_claim,
                module.CanonicalTurnResult("succeeded", "shared-result"),
            )

        assert _claim_snapshot(conn, project_id, second.turn_id) == before
        assert first.turn_id != second.turn_id
    finally:
        conn.close()


def test_terminal_event_conflict_rolls_back_result_control_lease_and_version(
    tmp_path,
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-event-conflict.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        event_id = conn.execute(
            """
            SELECT event_id FROM project_events
            WHERE project_id = ? ORDER BY sequence LIMIT 1
            """,
            (project_id,),
        ).fetchone()[0]
        conflict_runtime = module.ProjectRuntime(
            conn,
            clock=lambda: 100,
            id_factory=lambda kind: event_id if kind == "event" else "unused",
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(sqlite3.IntegrityError):
            conflict_runtime.commit_turn(
                claim, module.CanonicalTurnResult("failed", "result")
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_requeues_expired_not_started_claim_without_readback(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-not-started.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1
        assert recovered[0].turn_id == turn.turn_id
        assert recovered[0].status == "queued"
        assert port.calls == []
        row = conn.execute(
            """
            SELECT status, attempt_id, lease_generation, fencing_token,
                   execution_state
            FROM project_turns WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()
        control = conn.execute(
            """
            SELECT control_state, control_version, attempt_id,
                   claim_worker_id, claim_lease_expires_at,
                   claim_canonical_session_id
            FROM project_run_controls WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()
        assert tuple(row) == ("queued", None, 1, 1, None)
        assert tuple(control) == ("running", before[1][3] + 2, None, None, None, None)
        assert prdb.runtime_state_for_project(conn, project_id).version == (
            before[3].version + 2
        )
        assert [
            row[0]
            for row in conn.execute(
                """
                SELECT kind FROM project_events
                WHERE project_id = ? ORDER BY sequence DESC LIMIT 2
                """,
                (project_id,),
            ).fetchall()[::-1]
        ] == ["turn.reconciling", "turn.requeued"]
        replacement = runtime.claim_next_turn(
            project_id, "worker-b", lease_seconds=30
        )
        assert replacement.turn_id == claim.turn_id
        assert replacement.attempt_id != claim.attempt_id
        assert replacement.lease_generation == claim.lease_generation + 1
        assert replacement.fencing_token == claim.fencing_token + 1
    finally:
        conn.close()


def test_recovery_stops_expired_not_started_stop_request_without_readback(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-stop.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == "stopped"
        assert port.calls == []
        assert conn.execute(
            "SELECT control_state FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "stopped"
        assert [
            row[0]
            for row in conn.execute(
                """
                SELECT kind FROM project_events
                WHERE project_id = ? ORDER BY sequence DESC LIMIT 2
                """,
                (project_id,),
            ).fetchall()[::-1]
        ] == ["turn.reconciling", "run.stopped"]
    finally:
        conn.close()


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("execution_state", ["started", None])
def test_recovery_readback_terminalizes_started_and_legacy_attempts(
    tmp_path, execution_state, outcome
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"recover-{execution_state}-{outcome}.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if execution_state == "started":
            runtime.mark_turn_started(claim)
        else:
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
            conn.commit()
        port = _RecordingReadback(
            conn,
            module.TurnReadbackResult(outcome, f"result-{outcome}"),
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == outcome
        assert len(port.calls) == 1
        request = port.calls[0]
        assert request == module.TurnReadbackRequest(
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            sequence=claim.sequence,
            worker_id=claim.worker_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            lease_expires_at=claim.lease_expires_at,
            canonical_session_id=claim.canonical_session_id,
            source_status="claimed",
            execution_state=execution_state,
        )
        assert conn.execute(
            "SELECT terminal_result_id FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == f"result-{outcome}"
    finally:
        conn.close()


def test_recovery_accepts_stopped_readback_only_for_stop_source(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-stopped-proof.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("stopped")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == "stopped"
        assert port.calls[0].source_status == "stop_requested"
    finally:
        conn.close()


@pytest.mark.parametrize(
    "port_factory",
    [
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            id="unknown",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("succeeded")
            ),
            id="missing-result",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("unknown", "extra")
            ),
            id="extra-result",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(conn, object()),
            id="wrong-type",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, error=RuntimeError("private readback detail")
            ),
            id="exception",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("stopped")
            ),
            id="source-outcome-mismatch",
        ),
    ],
)
def test_unknown_malformed_and_illegal_readback_blocks_once_per_attempt(
    tmp_path, port_factory
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-block.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        port = port_factory(module, conn)
        now[0] = claim.lease_expires_at

        first = runtime.reconcile_inflight_turns(port, limit=10)
        snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        second = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(first) == 1 and first[0].status == "reconciling"
        assert second == ()
        assert len(port.calls) == 1
        assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
        events = conn.execute(
            """
            SELECT event_id, payload_json FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert len(events) == 1
        assert "private readback detail" not in events[0]["payload_json"]
        assert conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0] == events[0]["event_id"]
    finally:
        conn.close()


def test_recovery_block_event_identity_allows_a_later_attempt_to_block(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-block-attempt-scope.db", clock=lambda: now[0]
    )
    try:
        turn, first_claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(first_claim)
        first_port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = first_claim.lease_expires_at
        runtime.reconcile_inflight_turns(first_port, limit=10)
        first_key = conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0]

        conn.execute(
            """
            UPDATE project_turns
            SET status = 'queued', attempt_id = NULL,
                execution_state = NULL, recovery_block_key = NULL
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        )
        conn.execute(
            """
            UPDATE project_run_controls
            SET control_state = 'running', attempt_id = NULL,
                claim_worker_id = NULL, claim_lease_expires_at = NULL,
                claim_canonical_session_id = NULL
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        )
        conn.commit()
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        second_port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = second_claim.lease_expires_at

        runtime.reconcile_inflight_turns(second_port, limit=10)

        events = conn.execute(
            """
            SELECT event_id FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            ORDER BY sequence
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert len(events) == 2
        assert events[0]["event_id"] != events[1]["event_id"]
        second_key = conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0]
        assert first_key == events[0]["event_id"]
        assert second_key == events[1]["event_id"]
        assert second_key != first_key
    finally:
        conn.close()


def test_recovery_block_key_without_event_fails_closed(tmp_path):
    path = tmp_path / "recover-key-without-event.db"
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    now[0] = claim.lease_expires_at
    selected = prdb._recovery_candidates(conn, now=now[0], limit=1)[0]
    candidate = runtime._park_recovery_candidate(
        selected, now=now[0]
    )
    assert candidate is not None
    block_key = prdb._recovery_block_key(
        project_id=project_id,
        turn_id=turn.turn_id,
        attempt_id=claim.attempt_id,
        lease_generation=claim.lease_generation,
        fencing_token=claim.fencing_token,
    )
    conn.close()
    corrupt = sqlite3.connect(path)
    try:
        corrupt.execute(
            """
            UPDATE project_turns SET recovery_block_key = ?
            WHERE turn_id = ?
            """,
            (block_key, turn.turn_id),
        )
        corrupt.commit()
    finally:
        corrupt.close()

    check = projects_db.connect(path)
    try:
        persisted = prdb._runtime_turn_for_project(
            check, project_id=project_id, turn_id=turn.turn_id
        )
        with pytest.raises(RuntimeError, match="block"):
            prdb._validate_runtime_turn_pair(check, turn=persisted)
    finally:
        check.close()


def test_recovery_block_event_without_key_fails_closed(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-event-without-key.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        now[0] = claim.lease_expires_at
        selected = prdb._recovery_candidates(
            conn, now=now[0], limit=1
        )[0]
        candidate = runtime._park_recovery_candidate(
            selected, now=now[0]
        )
        assert candidate is not None
        block_key = prdb._recovery_block_key(
            project_id=project_id,
            turn_id=turn.turn_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
        )
        with prdb.write_transaction(conn):
            prdb._append_runtime_event(
                conn,
                event_id=block_key,
                project_id=project_id,
                kind="turn.recovery_blocked",
                turn_id=turn.turn_id,
                payload_json=module.canonical_json_object(
                    {
                        "attempt_id": claim.attempt_id,
                        "fencing_token": claim.fencing_token,
                        "lease_generation": claim.lease_generation,
                        "source_status": "claimed",
                        "turn_id": turn.turn_id,
                        "version": 3,
                    }
                ),
                created_at=now[0],
            )
        persisted = prdb._runtime_turn_for_project(
            conn, project_id=project_id, turn_id=turn.turn_id
        )

        with pytest.raises(RuntimeError, match="block"):
            prdb._validate_runtime_turn_pair(conn, turn=persisted)
    finally:
        conn.close()


@pytest.mark.parametrize("boolean_field", ["lease_generation", "fencing_token"])
def test_recovery_block_rejects_boolean_event_identity_without_writes(
    tmp_path, boolean_field
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"recover-boolean-{boolean_field}.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        now[0] = claim.lease_expires_at
        selected = prdb._recovery_candidates(
            conn, now=now[0], limit=1
        )[0]
        candidate = runtime._park_recovery_candidate(
            selected, now=now[0]
        )
        assert candidate is not None
        block_key = prdb._recovery_block_key(
            project_id=project_id,
            turn_id=turn.turn_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
        )
        payload = {
            "attempt_id": claim.attempt_id,
            "fencing_token": claim.fencing_token,
            "lease_generation": claim.lease_generation,
            "source_status": "claimed",
            "turn_id": turn.turn_id,
            "version": 3,
        }
        payload[boolean_field] = True

        with pytest.raises(RuntimeError, match="block event payload"):
            with prdb.write_transaction(conn):
                prdb._append_runtime_event(
                    conn,
                    event_id=block_key,
                    project_id=project_id,
                    kind="turn.recovery_blocked",
                    turn_id=turn.turn_id,
                    payload_json=module.canonical_json_object(payload),
                    created_at=now[0],
                )
                prdb._set_recovery_block_key(
                    conn,
                    candidate=candidate,
                    block_key=block_key,
                )

        assert conn.execute(
            "SELECT COUNT(*) FROM project_events WHERE event_id = ?",
            (block_key,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] is None
    finally:
        conn.close()


def test_awaiting_approval_expiry_is_inert_for_task5_recovery(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-approval-inert.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        conn.execute(
            "UPDATE project_turns SET status = 'awaiting_approval' WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert recovered == ()
        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_inactive_not_started_claim_blocks_but_terminal_proof_can_close(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-inactive.db", clock=lambda: now[0]
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, project_id, actor, key="inactive-not-started"
        )
        state = prdb.runtime_state_for_project(conn, project_id)
        with prdb.write_transaction(conn):
            state = prdb.transition_lifecycle(
                conn,
                project_id=project_id,
                expected_version=state.version,
                lifecycle="awaiting_acceptance",
                updated_at=101,
            )
        now[0] = first_claim.lease_expires_at
        no_call = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "unused")
        )

        blocked = runtime.reconcile_inflight_turns(no_call, limit=10)

        assert blocked[0].status == "reconciling"
        assert no_call.calls == []

        second_project = projects_db.create_project(conn, name="Inactive proof")
        prdb.create_project_conversation(
            conn,
            project_id=second_project,
            conversation_id="second-root",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="second-owner",
            project_id=second_project,
            surface="desktop",
            external_binding_id="second-window",
            actor_id="owner",
            now=1,
        )
        second_actor = ActorContext("owner", "desktop", "second-owner", True)
        second_runtime = module.ProjectRuntime(conn, clock=lambda: now[0])
        now[0] = 200
        second = second_runtime.enqueue_turn(
            second_project,
            {"message": "terminal proof"},
            second_actor,
            idempotency_key="terminal-proof",
            expected_version=0,
        )
        second_claim = second_runtime.claim_next_turn(
            second_project, "worker-two", lease_seconds=30
        )
        second_runtime.mark_turn_started(second_claim)
        second_state = prdb.runtime_state_for_project(conn, second_project)
        with prdb.write_transaction(conn):
            prdb.transition_lifecycle(
                conn,
                project_id=second_project,
                expected_version=second_state.version,
                lifecycle="awaiting_acceptance",
                updated_at=201,
            )
        proof = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "inactive-result")
        )
        now[0] = second_claim.lease_expires_at

        closed = second_runtime.reconcile_inflight_turns(proof, limit=10)

        assert closed[0].turn_id == second.turn_id
        assert closed[0].status == "succeeded"
    finally:
        conn.close()


def test_preexisting_reconciling_attempt_resumes_without_a_lease(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-preexisting.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        conn.execute(
            "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.execute(
            "DELETE FROM project_worker_leases WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.commit()
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("failed", "resumed-result")
        )

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == "failed"
        assert len(port.calls) == 1
    finally:
        conn.close()


def test_claimed_attempt_without_lease_is_not_inferred_as_recoverable(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-orphan-claim.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        conn.execute(
            "DELETE FROM project_worker_leases WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "must-not-read")
        )

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert recovered == ()
        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_rejects_selected_lease_pair_mismatch_without_writes(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-malformed-pair.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        conn.execute(
            """
            UPDATE project_run_controls SET claim_worker_id = 'forged-worker'
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "must-not-read")
        )

        with pytest.raises(
            RuntimeError,
            match="turn/control/lease pair is inconsistent",
        ):
            runtime.reconcile_inflight_turns(port, limit=10)

        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize("limit", [True, 0, 101, 1.5])
def test_recovery_rejects_invalid_limit_before_port_or_write(tmp_path, limit):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"recover-limit-{limit}.db"
    )
    try:
        turn, _ = _enqueue_and_claim(runtime, project_id, actor)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.reconcile_inflight_turns(port, limit=limit)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_rejects_outer_transaction_before_port_or_write(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-outer-transaction.db"
    )
    try:
        turn, _ = _enqueue_and_claim(runtime, project_id, actor)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with prdb.write_transaction(conn):
            with pytest.raises(module.ProjectRuntimeError) as rejected:
                runtime.reconcile_inflight_turns(port, limit=10)
            assert (
                rejected.value.code
                is module.RuntimeErrorCode.INVALID_ARGUMENT
            )

        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_two_terminal_reconcilers_commit_one_canonical_event(tmp_path):
    path = tmp_path / "recover-race.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    now[0] = claim.lease_expires_at
    bootstrap.close()
    barrier = threading.Barrier(2)

    def reconcile(readback_result):
        conn = projects_db.connect(path)
        try:
            port = _RecordingReadback(
                conn,
                module.TurnReadbackResult(*readback_result),
                barrier=barrier,
            )
            result = module.ProjectRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(port, limit=10)
            return result, len(port.calls)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(reconcile, ("succeeded", "race-success")),
            pool.submit(reconcile, ("failed", "race-failure")),
        ]
        results = [future.result(timeout=15) for future in futures]

    check = projects_db.connect(path)
    try:
        assert sum(calls for _, calls in results) == 2
        returned_statuses = {
            result[0].status for result, _ in results
        }
        assert len(returned_statuses) == 1
        phase_b = check.execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind IN ('turn.succeeded', 'turn.failed')
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert len(phase_b) == 1
        expected_event = {
            "succeeded": "turn.succeeded",
            "failed": "turn.failed",
        }[returned_statuses.pop()]
        assert phase_b[0]["kind"] == expected_event
    finally:
        check.close()


@pytest.mark.parametrize(
    ("source_status", "late_result"),
    [
        pytest.param(
            "claimed",
            ("succeeded", "late-success"),
            id="late-succeeded",
        ),
        pytest.param(
            "claimed",
            ("failed", "late-failure"),
            id="late-failed",
        ),
        pytest.param(
            "stop_requested",
            ("stopped", None),
            id="late-stopped",
        ),
    ],
)
def test_recovery_block_fences_late_mixed_readback_outcome(
    tmp_path, source_status, late_result
):
    path = tmp_path / f"recover-block-wins-{late_result[0]}.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    if source_status == "stop_requested":
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
    version_before = prdb.runtime_state_for_project(
        bootstrap, project_id
    ).version
    now[0] = claim.lease_expires_at
    bootstrap.close()
    both_reading = threading.Barrier(2)
    release_late = threading.Event()

    def reconcile(result, *, wait_for_block):
        conn = projects_db.connect(path)
        try:
            class _OrderedReadback:
                def read_turn(self, request):
                    assert conn.in_transaction is False
                    both_reading.wait(timeout=5)
                    if wait_for_block:
                        assert release_late.wait(timeout=10)
                    return module.TurnReadbackResult(*result)

            return module.ProjectRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(_OrderedReadback(), limit=10)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        blocked_future = pool.submit(
            reconcile, ("unknown", None), wait_for_block=False
        )
        late_future = pool.submit(
            reconcile, late_result, wait_for_block=True
        )
        blocked = blocked_future.result(timeout=15)
        release_late.set()
        late = late_future.result(timeout=15)

    check = projects_db.connect(path)
    try:
        assert blocked[0].status == "reconciling"
        assert late[0].status == "reconciling"
        assert check.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "reconciling"
        phase_b_events = check.execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind IN (
                  'turn.recovery_blocked', 'turn.requeued',
                  'turn.succeeded', 'turn.failed', 'run.stopped'
              )
            ORDER BY sequence
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert [row["kind"] for row in phase_b_events] == [
            "turn.recovery_blocked"
        ]
        assert (
            prdb.runtime_state_for_project(check, project_id).version
            == version_before + 2
        )
    finally:
        check.close()


def test_terminal_recovery_winner_makes_late_block_write_free(tmp_path):
    path = tmp_path / "recover-terminal-wins-block.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    version_before = prdb.runtime_state_for_project(
        bootstrap, project_id
    ).version
    now[0] = claim.lease_expires_at
    bootstrap.close()
    both_reading = threading.Barrier(2)
    release_unknown = threading.Event()

    def reconcile(result, *, wait):
        conn = projects_db.connect(path)
        try:
            class _OrderedReadback:
                def read_turn(self, request):
                    assert conn.in_transaction is False
                    both_reading.wait(timeout=5)
                    if wait:
                        assert release_unknown.wait(timeout=10)
                    return result

            return module.ProjectRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(_OrderedReadback(), limit=10)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        terminal_future = pool.submit(
            reconcile,
            module.TurnReadbackResult("succeeded", "winner"),
            wait=False,
        )
        unknown_future = pool.submit(
            reconcile,
            module.TurnReadbackResult("unknown"),
            wait=True,
        )
        terminal = terminal_future.result(timeout=15)
        release_unknown.set()
        late = unknown_future.result(timeout=15)

    check = projects_db.connect(path)
    try:
        assert terminal[0].status == "succeeded"
        assert late[0].status == "succeeded"
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 0
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.succeeded'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 1
        assert (
            prdb.runtime_state_for_project(check, project_id).version
            == version_before + 2
        )
    finally:
        check.close()


def test_recovery_outcome_sql_cas_rejects_blocked_attempt(tmp_path):
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recovery-outcome-block-cas.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        now[0] = claim.lease_expires_at
        selected = prdb._recovery_candidates(
            conn, now=now[0], limit=1
        )[0]
        candidate = runtime._park_recovery_candidate(
            selected, now=now[0]
        )
        assert candidate is not None
        runtime._block_recovery(candidate, now=now[0])
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with prdb.write_transaction(conn):
            updated = prdb._apply_recovery_outcome(
                conn,
                candidate=candidate,
                outcome="succeeded",
                terminal_result_id="must-not-commit",
                now=now[0],
            )

        assert updated is None
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_requeue_sql_cas_requires_current_active_lifecycle(tmp_path):
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "requeue-lifecycle-cas.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        selected = prdb._recovery_candidates(
            conn, now=now[0], limit=1
        )[0]
        candidate = runtime._park_recovery_candidate(
            selected, now=now[0]
        )
        assert candidate is not None
        state = prdb.runtime_state_for_project(conn, project_id)
        with prdb.write_transaction(conn):
            prdb.transition_lifecycle(
                conn,
                project_id=project_id,
                expected_version=state.version,
                lifecycle="awaiting_acceptance",
                updated_at=now[0],
            )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with prdb.write_transaction(conn):
            updated = prdb._apply_recovery_outcome(
                conn,
                candidate=candidate,
                outcome="queued",
                terminal_result_id=None,
                now=now[0],
            )

        assert updated is None
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "next_lifecycle", ["awaiting_acceptance", "completed"]
)
def test_phase_b_revalidates_current_lifecycle_before_requeue(
    tmp_path, next_lifecycle
):
    path = tmp_path / f"recover-current-{next_lifecycle}.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    now[0] = claim.lease_expires_at
    bootstrap.close()
    finalize_entered = threading.Event()
    release_finalize = threading.Event()

    class _DelayedFinalizeRuntime(module.ProjectRuntime):
        def _finalize_recovery(self, *args, **kwargs):
            finalize_entered.set()
            assert release_finalize.wait(timeout=10)
            return super()._finalize_recovery(*args, **kwargs)

    def reconcile():
        conn = projects_db.connect(path)
        try:
            return _DelayedFinalizeRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(
                _RecordingReadback(
                    conn, module.TurnReadbackResult("unknown")
                ),
                limit=10,
            )
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(reconcile)
        assert finalize_entered.wait(timeout=10)
        lifecycle_conn = projects_db.connect(path)
        try:
            state = prdb.runtime_state_for_project(
                lifecycle_conn, project_id
            )
            with prdb.write_transaction(lifecycle_conn):
                state = prdb.transition_lifecycle(
                    lifecycle_conn,
                    project_id=project_id,
                    expected_version=state.version,
                    lifecycle="awaiting_acceptance",
                    updated_at=now[0],
                )
            if next_lifecycle == "completed":
                with prdb.write_transaction(lifecycle_conn):
                    state = prdb.transition_lifecycle(
                        lifecycle_conn,
                        project_id=project_id,
                        expected_version=state.version,
                        lifecycle="completed",
                        updated_at=now[0],
                    )
        finally:
            lifecycle_conn.close()
        release_finalize.set()
        recovered = future.result(timeout=15)

    check = projects_db.connect(path)
    try:
        assert recovered[0].status == "reconciling"
        assert check.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "reconciling"
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 1
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.requeued'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 0
    finally:
        check.close()


def test_lifecycle_block_fences_late_requeue_after_phase_a(tmp_path):
    path = tmp_path / "recover-lifecycle-block-wins-requeue.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    now[0] = claim.lease_expires_at
    bootstrap.close()
    finalize_entered = threading.Event()
    release_finalize = threading.Event()

    class _DelayedFinalizeRuntime(module.ProjectRuntime):
        def _finalize_recovery(self, *args, **kwargs):
            finalize_entered.set()
            assert release_finalize.wait(timeout=10)
            return super()._finalize_recovery(*args, **kwargs)

    def late_requeue():
        conn = projects_db.connect(path)
        try:
            return _DelayedFinalizeRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(
                _RecordingReadback(
                    conn, module.TurnReadbackResult("unknown")
                ),
                limit=10,
            )
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(late_requeue)
        assert finalize_entered.wait(timeout=10)
        blocker_conn = projects_db.connect(path)
        try:
            state = prdb.runtime_state_for_project(
                blocker_conn, project_id
            )
            with prdb.write_transaction(blocker_conn):
                prdb.transition_lifecycle(
                    blocker_conn,
                    project_id=project_id,
                    expected_version=state.version,
                    lifecycle="awaiting_acceptance",
                    updated_at=now[0],
                )
            blocker = module.ProjectRuntime(
                blocker_conn, clock=lambda: now[0]
            )
            blocked = blocker.reconcile_inflight_turns(
                _RecordingReadback(
                    blocker_conn,
                    module.TurnReadbackResult("unknown"),
                ),
                limit=10,
            )
        finally:
            blocker_conn.close()
        release_finalize.set()
        late = future.result(timeout=15)

    check = projects_db.connect(path)
    try:
        assert blocked[0].status == "reconciling"
        assert late[0].status == "reconciling"
        assert check.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "reconciling"
        phase_b_events = check.execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind IN ('turn.recovery_blocked', 'turn.requeued')
            ORDER BY sequence
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert [row["kind"] for row in phase_b_events] == [
            "turn.recovery_blocked"
        ]
    finally:
        check.close()


@pytest.mark.parametrize(
    ("source_status", "readback_result", "expected_status"),
    [
        pytest.param(
            "claimed",
            ("succeeded", "success-after-lifecycle"),
            "succeeded",
            id="succeeded",
        ),
        pytest.param(
            "claimed",
            ("failed", "failure-after-lifecycle"),
            "failed",
            id="failed",
        ),
        pytest.param(
            "stop_requested",
            ("stopped", None),
            "stopped",
            id="stopped",
        ),
    ],
)
def test_proven_terminal_recovery_can_close_after_phase_b_lifecycle_change(
    tmp_path, source_status, readback_result, expected_status
):
    path = tmp_path / f"recover-terminal-inactive-{expected_status}.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    _, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    if source_status == "stop_requested":
        runtime.request_stop(
            project_id,
            claim.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
    now[0] = claim.lease_expires_at
    bootstrap.close()
    finalize_entered = threading.Event()
    release_finalize = threading.Event()

    class _DelayedFinalizeRuntime(module.ProjectRuntime):
        def _finalize_recovery(self, *args, **kwargs):
            finalize_entered.set()
            assert release_finalize.wait(timeout=10)
            return super()._finalize_recovery(*args, **kwargs)

    def reconcile():
        conn = projects_db.connect(path)
        try:
            return _DelayedFinalizeRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(
                _RecordingReadback(
                    conn, module.TurnReadbackResult(*readback_result)
                ),
                limit=10,
            )
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(reconcile)
        assert finalize_entered.wait(timeout=10)
        lifecycle_conn = projects_db.connect(path)
        try:
            state = prdb.runtime_state_for_project(
                lifecycle_conn, project_id
            )
            with prdb.write_transaction(lifecycle_conn):
                prdb.transition_lifecycle(
                    lifecycle_conn,
                    project_id=project_id,
                    expected_version=state.version,
                    lifecycle="awaiting_acceptance",
                    updated_at=now[0],
                )
        finally:
            lifecycle_conn.close()
        release_finalize.set()
        recovered = future.result(timeout=15)

    assert recovered[0].status == expected_status


class _OutcomeEnum(str, Enum):
    SUCCEEDED = "succeeded"


class _ExplosiveOutcome(str):
    def __hash__(self):
        raise AssertionError("outcome hash executed")

    def __eq__(self, other):
        raise AssertionError("outcome equality executed")


@pytest.mark.parametrize(
    "impostor_factory",
    [
        pytest.param(lambda: _OutcomeEnum.SUCCEEDED, id="str-enum"),
        pytest.param(
            lambda: _ExplosiveOutcome("succeeded"),
            id="side-effect-str-subclass",
        ),
    ],
)
def test_readback_outcome_requires_exact_string_and_blocks_impostors(
    tmp_path, impostor_factory
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-outcome-impostor.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        now[0] = claim.lease_expires_at
        port = _RecordingReadback(
            conn,
            module.TurnReadbackResult(
                impostor_factory(), "must-not-terminalize"
            ),
        )

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert recovered[0].status == "reconciling"
        assert conn.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_recovery_takeover_rotates_fence_and_stale_worker_cannot_write(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-takeover-fence.db", clock=lambda: now[0]
    )
    try:
        turn, stale_claim = _enqueue_and_claim(
            runtime, project_id, actor
        )
        now[0] = stale_claim.lease_expires_at
        runtime.reconcile_inflight_turns(
            _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            limit=10,
        )
        current_claim = runtime.claim_next_turn(
            project_id, "worker-current", lease_seconds=30
        )
        assert current_claim is not None
        assert current_claim.turn_id == turn.turn_id
        assert current_claim.attempt_id != stale_claim.attempt_id
        assert (
            current_claim.lease_generation
            == stale_claim.lease_generation + 1
        )
        assert current_claim.fencing_token == stale_claim.fencing_token + 1
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        stale_calls = (
            lambda: runtime.heartbeat_turn(
                stale_claim, lease_seconds=30
            ),
            lambda: runtime.mark_turn_started(stale_claim),
            lambda: runtime.commit_turn(
                stale_claim,
                module.CanonicalTurnResult("succeeded", "stale-result"),
            ),
            lambda: runtime.acknowledge_stopped(stale_claim),
        )
        for call in stale_calls:
            with pytest.raises(module.ProjectRuntimeError):
                call()
            assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_fresh_process_claim_crash_requeues_and_fences_live_stale_writer(
    tmp_path,
):
    path = tmp_path / "recover-process-takeover.db"
    _, conn, runtime, project_id, actor = _make_runtime(path)
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "cross-process"},
            actor,
            idempotency_key="cross-process",
            expected_version=0,
        )
    finally:
        conn.close()

    with _ProbeSet() as probes:
        crashed = _run_probe(
            probes,
            _probe_prepare(
                "claim-crash-a",
                "claim",
                path,
                project_id,
                "process-a",
                100,
                lease_seconds=30,
                crash_after="claim_commit",
            ),
            expected_event="boundary",
            returncode=91,
        )
        assert crashed["boundary"] == "claim_committed"
        stale_claim = crashed["claim"]
        assert stale_claim["turn_id"] == turn.turn_id

        check = projects_db.connect(path)
        try:
            claimed = check.execute(
                """
                SELECT status, execution_state
                FROM project_turns WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()
            assert tuple(claimed) == ("claimed", "not_started")
            assert check.execute(
                """
                SELECT COUNT(*) FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn.turn_id),
            ).fetchone()[0] == 1

            stale_writer = probes.spawn(
                _probe_prepare(
                    "stale-commit-a",
                    "commit",
                    path,
                    project_id,
                    stale_claim["worker_id"],
                    stale_claim["lease_expires_at"],
                    claim=stale_claim,
                    outcome="succeeded",
                    result_id="stale-result",
                )
            )
            stale_ready = stale_writer.expect("ready")
            assert stale_ready["stage"] == "before_begin_immediate"

            recovered = _run_probe(
                probes,
                _probe_prepare(
                    "recover-after-claim-crash",
                    "recover",
                    path,
                    project_id,
                    "recovery",
                    stale_claim["lease_expires_at"],
                    limit=10,
                ),
            )
            assert recovered["readback_requests"] == []
            assert recovered["turns"] == [
                {"status": "queued", "turn_id": turn.turn_id}
            ]

            takeover = _run_probe(
                probes,
                _probe_prepare(
                    "takeover-b",
                    "claim",
                    path,
                    project_id,
                    "process-b",
                    stale_claim["lease_expires_at"],
                    lease_seconds=30,
                ),
            )
            current_claim = takeover["claim"]
            assert current_claim["attempt_id"] != stale_claim["attempt_id"]
            assert (
                current_claim["lease_generation"]
                == stale_claim["lease_generation"] + 1
            )
            assert (
                current_claim["fencing_token"]
                == stale_claim["fencing_token"] + 1
            )
            before_stale = _claim_snapshot(
                check, project_id, turn.turn_id
            )

            stale_writer.send(
                {
                    "version": 1,
                    "event": "go",
                    "probe_id": stale_writer.probe_id,
                }
            )
            stale_result = stale_writer.expect("result")
            stale_writer.complete()
            assert stale_result["ok"] is False
            assert stale_result["error"] == {
                "code": "stale_turn_claim"
            }
            assert (
                _claim_snapshot(check, project_id, turn.turn_id)
                == before_stale
            )

            started = _run_probe(
                probes,
                _probe_prepare(
                    "start-b",
                    "start",
                    path,
                    project_id,
                    current_claim["worker_id"],
                    stale_claim["lease_expires_at"],
                    claim=current_claim,
                ),
            )
            assert started["claim"] == current_claim
            committed = _run_probe(
                probes,
                _probe_prepare(
                    "commit-b",
                    "commit",
                    path,
                    project_id,
                    current_claim["worker_id"],
                    stale_claim["lease_expires_at"],
                    claim=current_claim,
                    outcome="succeeded",
                    result_id="current-result",
                ),
            )
            assert committed["turn"] == {
                "status": "succeeded",
                "turn_id": turn.turn_id,
            }
            assert check.execute(
                """
                SELECT terminal_result_id FROM project_turns
                WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()[0] == "current-result"
            assert check.execute(
                """
                SELECT COUNT(*) FROM project_events
                WHERE turn_id = ? AND kind = 'turn.succeeded'
                  AND payload_json LIKE '%stale-result%'
                """,
                (turn.turn_id,),
            ).fetchone()[0] == 0
        finally:
            check.close()


def test_fresh_process_start_and_phase_a_crashes_recover_terminal(
    tmp_path,
):
    path = tmp_path / "recover-process-crash-boundaries.db"
    _, conn, runtime, project_id, actor = _make_runtime(path)
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "crash boundaries"},
            actor,
            idempotency_key="crash-boundaries",
            expected_version=0,
        )
    finally:
        conn.close()

    with _ProbeSet() as probes:
        claimed = _run_probe(
            probes,
            _probe_prepare(
                "claim-before-start-crash",
                "claim",
                path,
                project_id,
                "process-a",
                100,
                lease_seconds=30,
            ),
        )
        claim = claimed["claim"]
        started = _run_probe(
            probes,
            _probe_prepare(
                "start-crash",
                "start",
                path,
                project_id,
                claim["worker_id"],
                100,
                claim=claim,
                crash_after="start_commit",
            ),
            expected_event="boundary",
            returncode=92,
        )
        assert started["boundary"] == "start_committed"
        assert started["claim"] == claim

        check = projects_db.connect(path)
        try:
            assert check.execute(
                """
                SELECT execution_state FROM project_turns
                WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()[0] == "started"
        finally:
            check.close()

        parked = _run_probe(
            probes,
            _probe_prepare(
                "phase-a-crash",
                "recover",
                path,
                project_id,
                "recovery-a",
                claim["lease_expires_at"],
                limit=10,
                crash_after="phase_a_reconciling_commit",
            ),
            expected_event="boundary",
            returncode=93,
        )
        assert parked["boundary"] == "reconciling_committed"
        request = parked["request"]
        assert request["attempt_id"] == claim["attempt_id"]
        assert request["lease_generation"] == claim["lease_generation"]
        assert request["fencing_token"] == claim["fencing_token"]
        assert request["source_status"] == "claimed"
        assert request["execution_state"] == "started"

        check = projects_db.connect(path)
        try:
            assert check.execute(
                "SELECT status FROM project_turns WHERE turn_id = ?",
                (turn.turn_id,),
            ).fetchone()[0] == "reconciling"
            assert check.execute(
                """
                SELECT COUNT(*) FROM project_worker_leases
                WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()[0] == 0
            kinds = [
                row["kind"]
                for row in check.execute(
                    """
                    SELECT kind FROM project_events
                    WHERE turn_id = ?
                      AND kind IN (
                          'turn.reconciling',
                          'turn.recovery_blocked',
                          'turn.succeeded',
                          'turn.failed',
                          'run.stopped',
                          'turn.requeued'
                      )
                    ORDER BY sequence
                    """,
                    (turn.turn_id,),
                )
            ]
            assert kinds == ["turn.reconciling"]
        finally:
            check.close()

        recovered = _run_probe(
            probes,
            _probe_prepare(
                "recover-after-phase-a-crash",
                "recover",
                path,
                project_id,
                "recovery-b",
                claim["lease_expires_at"],
                limit=10,
                readback={
                    "outcome": "succeeded",
                    "result_id": "recovered-result",
                },
            ),
        )
        assert recovered["readback_requests"] == [request]
        assert recovered["turns"] == [
            {"status": "succeeded", "turn_id": turn.turn_id}
        ]

        check = projects_db.connect(path)
        try:
            terminal = check.execute(
                """
                SELECT status, terminal_result_id
                FROM project_turns WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()
            assert tuple(terminal) == ("succeeded", "recovered-result")
            assert [
                row["kind"]
                for row in check.execute(
                    """
                    SELECT kind FROM project_events
                    WHERE turn_id = ?
                      AND kind IN ('turn.reconciling', 'turn.succeeded')
                    ORDER BY sequence
                    """,
                    (turn.turn_id,),
                )
            ] == ["turn.reconciling", "turn.succeeded"]
        finally:
            check.close()


def test_fresh_process_claim_race_repeats_25_times_and_winner_commits(
    tmp_path,
):
    path = tmp_path / "recover-process-race-25.db"
    module, conn, runtime, first_project, first_actor = _make_runtime(path)
    try:
        with _ProbeSet() as probes:
            for iteration in range(25):
                if iteration == 0:
                    project_id = first_project
                    actor = first_actor
                    project_runtime = runtime
                else:
                    project_id = projects_db.create_project(
                        conn, name=f"Race {iteration}"
                    )
                    session_id = f"race-root-{iteration}"
                    binding_id = f"race-owner-{iteration}"
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
                        external_binding_id=f"window-{iteration}",
                        actor_id="owner",
                        now=1,
                    )
                    actor = ActorContext(
                        "owner", "desktop", binding_id, True
                    )
                    project_runtime = module.ProjectRuntime(
                        conn, clock=lambda: 100
                    )
                turn = project_runtime.enqueue_turn(
                    project_id,
                    {"iteration": iteration},
                    actor,
                    idempotency_key=f"race-{iteration}",
                    expected_version=0,
                )
                workers = [
                    probes.spawn(
                        _probe_prepare(
                            f"race-{iteration}-{side}",
                            "claim",
                            path,
                            project_id,
                            f"worker-{side}-{iteration}",
                            100,
                            lease_seconds=30,
                        )
                    )
                    for side in ("a", "b")
                ]
                _release_probes(workers)
                results = [
                    worker.expect("result") for worker in workers
                ]
                for worker in workers:
                    worker.complete()
                claims = [
                    payload["claim"]
                    for payload in results
                    if payload["claim"] is not None
                ]
                assert len(claims) == 1
                claim = claims[0]

                started = _run_probe(
                    probes,
                    _probe_prepare(
                        f"race-{iteration}-start",
                        "start",
                        path,
                        project_id,
                        claim["worker_id"],
                        100,
                        claim=claim,
                    ),
                )
                assert started["claim"] == claim
                committed = _run_probe(
                    probes,
                    _probe_prepare(
                        f"race-{iteration}-commit",
                        "commit",
                        path,
                        project_id,
                        claim["worker_id"],
                        100,
                        claim=claim,
                        outcome="succeeded",
                        result_id=f"result-{iteration}",
                    ),
                )
                assert committed["turn"] == {
                    "status": "succeeded",
                    "turn_id": turn.turn_id,
                }
                terminal = conn.execute(
                    """
                    SELECT status, terminal_result_id
                    FROM project_turns
                    WHERE project_id = ? AND turn_id = ?
                    """,
                    (project_id, turn.turn_id),
                ).fetchone()
                assert tuple(terminal) == (
                    "succeeded",
                    f"result-{iteration}",
                )
                assert conn.execute(
                    """
                    SELECT COUNT(*) FROM project_worker_leases
                    WHERE project_id = ?
                    """,
                    (project_id,),
                ).fetchone()[0] == 0
                counts = {
                    row["kind"]: row["count"]
                    for row in conn.execute(
                        """
                        SELECT kind, COUNT(*) AS count
                        FROM project_events
                        WHERE project_id = ? AND turn_id = ?
                          AND kind IN ('turn.claimed', 'turn.succeeded')
                        GROUP BY kind
                        """,
                        (project_id, turn.turn_id),
                    )
                }
                assert counts == {
                    "turn.claimed": 1,
                    "turn.succeeded": 1,
                }
    finally:
        conn.close()


def test_claim_scan_is_set_based_and_bounded_by_the_fifo_head(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "bounded-claim-scan.db"
    )
    try:
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, origin_binding_id, status, attempt_id,
                    lease_generation, fencing_token, execution_state,
                    terminal_result_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, '{}', 'owner-binding', 'cancelled',
                          NULL, 0, 0, NULL, NULL, 1, 1)
                """,
                [
                    (
                        f"historical-{sequence}",
                        project_id,
                        sequence,
                        f"historical-{sequence}",
                    )
                    for sequence in range(1, 251)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    idempotency_key, command_fingerprint, attempt_id,
                    claim_worker_id, claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (?, ?, 'terminal', 0, NULL, NULL, NULL,
                          NULL, NULL, NULL, 1)
                """,
                [
                    (f"historical-{sequence}", project_id)
                    for sequence in range(1, 251)
                ],
            )
        target = runtime.enqueue_turn(
            project_id,
            {"message": "bounded"},
            actor,
            idempotency_key="bounded",
            expected_version=0,
        )
        statements = []
        conn.set_trace_callback(statements.append)
        try:
            claim = runtime.claim_next_turn(
                project_id, "bounded-worker", lease_seconds=30
            )
        finally:
            conn.set_trace_callback(None)

        assert claim is not None and claim.turn_id == target.turn_id
        normalized = [
            " ".join(statement.lower().split())
            for statement in statements
        ]
        turn_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_turns" in statement
        ]
        control_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_run_controls" in statement
        ]
        lease_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_worker_leases" in statement
        ]
        assert len(turn_selects) <= 4
        assert len(control_selects) <= 3
        assert len(lease_selects) <= 3
        assert all(
            "order by sequence, turn_id" not in statement
            or "limit 1" in statement
            for statement in turn_selects
        )
    finally:
        conn.close()


def test_recovery_parks_the_whole_batch_before_first_readback(tmp_path):
    now = [100]
    module, conn, runtime, first_project, first_actor = _make_runtime(
        tmp_path / "recover-whole-batch.db", clock=lambda: now[0]
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, first_project, first_actor, key="first"
        )
        runtime.mark_turn_started(first_claim)
        second_project = projects_db.create_project(conn, name="Second")
        prdb.create_project_conversation(
            conn,
            project_id=second_project,
            conversation_id="second-root",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="second-owner",
            project_id=second_project,
            surface="desktop",
            external_binding_id="second-window",
            actor_id="owner",
            now=1,
        )
        second_actor = ActorContext(
            "owner", "desktop", "second-owner", True
        )
        second_runtime = module.ProjectRuntime(
            conn, clock=lambda: now[0]
        )
        second = second_runtime.enqueue_turn(
            second_project,
            {"message": "second"},
            second_actor,
            idempotency_key="second",
            expected_version=0,
        )
        second_claim = second_runtime.claim_next_turn(
            second_project, "second-worker", lease_seconds=30
        )
        assert second_claim is not None
        second_runtime.mark_turn_started(second_claim)
        now[0] = first_claim.lease_expires_at

        class _BatchReadback:
            def __init__(self):
                self.calls = []

            def read_turn(self, request):
                assert conn.in_transaction is False
                if not self.calls:
                    assert {
                        row["status"]
                        for row in conn.execute(
                            """
                            SELECT status FROM project_turns
                            WHERE turn_id IN (?, ?)
                            """,
                            (first.turn_id, second.turn_id),
                        )
                    } == {"reconciling"}
                    assert conn.execute(
                        """
                        SELECT COUNT(*) FROM project_worker_leases
                        WHERE turn_id IN (?, ?)
                        """,
                        (first.turn_id, second.turn_id),
                    ).fetchone()[0] == 0
                self.calls.append(request)
                return module.TurnReadbackResult(
                    "succeeded", f"result-{request.turn_id}"
                )

        port = _BatchReadback()
        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(port.calls) == 2
        assert {turn.status for turn in recovered} == {"succeeded"}
    finally:
        conn.close()


def test_claim_counter_overflow_is_rejected_without_writes(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "claim-counter-overflow.db"
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "overflow"},
            actor,
            idempotency_key="overflow",
            expected_version=0,
        )
        conn.execute(
            """
            UPDATE project_turns
            SET lease_generation = ?, fencing_token = ?
            WHERE turn_id = ?
            """,
            (module.SQLITE_INT_MAX, module.SQLITE_INT_MAX, turn.turn_id),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(RuntimeError, match="counter"):
            runtime.claim_next_turn(
                project_id, "overflow-worker", lease_seconds=30
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_claim_expiry_overflow_is_invalid_without_writes(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "claim-expiry-overflow.db",
        clock=lambda: (1 << 63) - 1,
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "expiry overflow"},
            actor,
            idempotency_key="expiry-overflow",
            expected_version=0,
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.claim_next_turn(
                project_id, "overflow-worker", lease_seconds=1
            )

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_candidate_queries_are_bounded_and_indexed(tmp_path):
    _, conn, _, _, _ = _make_runtime(
        tmp_path / "recover-expiry-plan.db"
    )
    try:
        query_cases = (
            (
                prdb._RECOVERY_EXPIRED_LEASES_SQL,
                (100, 10),
                {"idx_project_worker_leases_expiry"},
            ),
            (
                prdb._RECOVERY_RECONCILING_SQL,
                (10,),
                {
                    "idx_project_turns_actionable_recovery",
                },
            ),
            (
                prdb._RECOVERY_BLOCK_LOOKUP_SQL,
                ("project", "turn", "attempt", 1, 1),
                {"idx_project_events_recovery_block_attempt"},
            ),
        )
        for sql, parameters, expected_indexes in query_cases:
            details = [
                row["detail"]
                for row in conn.execute(
                    f"EXPLAIN QUERY PLAN {sql}", parameters
                )
            ]

            assert all(
                expected in " ".join(details)
                for expected in expected_indexes
            )
            assert not any(
                "USE TEMP B-TREE" in detail for detail in details
            )
            assert not any(
                detail in {"SCAN turn", "SCAN event"}
                for detail in details
            )
    finally:
        conn.close()


def test_recovery_scan_work_does_not_grow_with_terminal_history(tmp_path):
    _, conn, _, project_id, _ = _make_runtime(
        tmp_path / "recover-history-plan.db"
    )
    try:
        def instruction_count():
            instructions = [0]

            def progress():
                instructions[0] += 1
                return 0

            conn.set_progress_handler(progress, 1)
            try:
                assert prdb._recovery_candidates(
                    conn, now=100, limit=10
                ) == ()
            finally:
                conn.set_progress_handler(None, 0)
            return instructions[0]

        conn.execute("ANALYZE")
        baseline = instruction_count()
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, origin_binding_id, status, attempt_id,
                    lease_generation, fencing_token, execution_state,
                    terminal_result_id, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, '{}', 'owner-binding', 'cancelled',
                    NULL, 0, 0, NULL, NULL, 1, 1
                )
                """,
                [
                    (
                        f"history-{sequence}",
                        project_id,
                        sequence,
                        f"history-{sequence}",
                    )
                    for sequence in range(1, 2001)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    idempotency_key, command_fingerprint, attempt_id,
                    claim_worker_id, claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (
                    ?, ?, 'terminal', 0, NULL, NULL, NULL,
                    NULL, NULL, NULL, 1
                )
                """,
                [
                    (f"history-{sequence}", project_id)
                    for sequence in range(1, 2001)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_events (
                    event_id, project_id, sequence, kind, turn_id,
                    payload_json, created_at
                ) VALUES (?, ?, ?, 'turn.history', ?, '{}', 1)
                """,
                [
                    (
                        f"history-event-{sequence}",
                        project_id,
                        sequence,
                        f"history-{sequence}",
                    )
                    for sequence in range(1, 2001)
                ],
            )
        conn.execute("ANALYZE")

        assert instruction_count() <= baseline + 200
    finally:
        conn.close()


def test_recovery_scan_work_does_not_grow_with_unexpired_claims(tmp_path):
    _, conn, _, _, _ = _make_runtime(
        tmp_path / "recover-unexpired-plan.db"
    )
    try:
        def instruction_count():
            instructions = [0]

            def progress():
                instructions[0] += 1
                return 0

            conn.set_progress_handler(progress, 1)
            try:
                assert prdb._recovery_candidates(
                    conn, now=100, limit=10
                ) == ()
            finally:
                conn.set_progress_handler(None, 0)
            return instructions[0]

        conn.execute("ANALYZE")
        baseline = instruction_count()
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO projects (
                    id, slug, name, created_at, archived
                ) VALUES (?, ?, ?, 1, 0)
                """,
                [
                    (
                        f"unexpired-project-{item}",
                        f"unexpired-project-{item}",
                        f"Unexpired {item}",
                    )
                    for item in range(500)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, execution_state, terminal_result_id,
                    created_at, updated_at
                ) VALUES (?, ?, 1, 'claim', '{}', 'claimed', ?, 1, 1,
                          'not_started', NULL, 1, 1)
                """,
                [
                    (
                        f"unexpired-turn-{item}",
                        f"unexpired-project-{item}",
                        f"unexpired-attempt-{item}",
                    )
                    for item in range(500)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    attempt_id, claim_worker_id,
                    claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (?, ?, 'running', 1, ?, 'worker', 1000,
                          'session', 1)
                """,
                [
                    (
                        f"unexpired-turn-{item}",
                        f"unexpired-project-{item}",
                        f"unexpired-attempt-{item}",
                    )
                    for item in range(500)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_worker_leases (
                    lease_id, project_id, turn_id, worker_id,
                    lease_generation, fencing_token, expires_at,
                    updated_at
                ) VALUES (?, ?, ?, 'worker', 1, 1, 1000, 1)
                """,
                [
                    (
                        f"unexpired-attempt-{item}",
                        f"unexpired-project-{item}",
                        f"unexpired-turn-{item}",
                    )
                    for item in range(500)
                ],
            )
        conn.execute("ANALYZE")

        assert instruction_count() <= baseline + 200
    finally:
        conn.close()


def test_blocked_history_scan_is_bounded_and_does_not_starve_actionable(
    tmp_path,
):
    module, conn, _, project_id, _ = _make_runtime(
        tmp_path / "recover-blocked-history.db"
    )
    try:
        blocked = []
        for item in range(2000):
            turn_id = f"blocked-turn-{item}"
            attempt_id = f"blocked-attempt-{item}"
            block_key = prdb._recovery_block_key(
                project_id=project_id,
                turn_id=turn_id,
                attempt_id=attempt_id,
                lease_generation=1,
                fencing_token=1,
            )
            blocked.append((turn_id, attempt_id, block_key))
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, execution_state, terminal_result_id,
                    recovery_block_key, created_at, updated_at
                ) VALUES (?, ?, ?, ?, '{}', 'reconciling', ?, 1, 1,
                          'started', NULL, NULL, 1, 1)
                """,
                [
                    (
                        turn_id,
                        project_id,
                        item + 1,
                        f"blocked-{item}",
                        attempt_id,
                    )
                    for item, (turn_id, attempt_id, _) in enumerate(
                        blocked
                    )
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    attempt_id, claim_worker_id,
                    claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (?, ?, 'running', 1, ?, 'worker', 100,
                          'session', 1)
                """,
                [
                    (turn_id, project_id, attempt_id)
                    for turn_id, attempt_id, _ in blocked
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_events (
                    event_id, project_id, sequence, kind, turn_id,
                    payload_json, created_at
                ) VALUES (?, ?, ?, 'turn.recovery_blocked', ?, ?, 1)
                """,
                [
                    (
                        block_key,
                        project_id,
                        item + 1,
                        turn_id,
                        module.canonical_json_object(
                            {
                                "attempt_id": attempt_id,
                                "fencing_token": 1,
                                "lease_generation": 1,
                                "source_status": "claimed",
                                "turn_id": turn_id,
                                "version": item + 1,
                            }
                        ),
                    )
                    for item, (
                        turn_id,
                        attempt_id,
                        block_key,
                    ) in enumerate(blocked)
                ],
            )
            conn.executemany(
                """
                UPDATE project_turns SET recovery_block_key = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                [
                    (block_key, project_id, turn_id)
                    for turn_id, _, block_key in blocked
                ],
            )
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, execution_state, terminal_result_id,
                    recovery_block_key, created_at, updated_at
                ) VALUES (
                    'actionable-turn', ?, 2001, 'actionable', '{}',
                    'reconciling', 'actionable-attempt', 1, 1,
                    'started', NULL, NULL, 1, 1
                )
                """,
                (project_id,),
            )
            conn.execute(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    attempt_id, claim_worker_id,
                    claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (
                    'actionable-turn', ?, 'running', 1,
                    'actionable-attempt', 'worker', 100, 'session', 1
                )
                """,
                (project_id,),
            )
        conn.execute("ANALYZE")
        instructions = [0]

        def progress():
            instructions[0] += 1
            return 0

        conn.set_progress_handler(progress, 1)
        try:
            candidates = prdb._recovery_candidates(
                conn, now=100, limit=1
            )
        finally:
            conn.set_progress_handler(None, 0)

        assert [candidate.turn_id for candidate in candidates] == [
            "actionable-turn"
        ]
        assert instructions[0] < 1000
    finally:
        conn.close()


def test_heartbeat_after_recovery_discovery_invalidates_candidate_cleanly(
    tmp_path, monkeypatch
):
    path = tmp_path / "recover-heartbeat-wins.db"
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    heartbeat_conn = None
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        heartbeat_conn = projects_db.connect(path)
        heartbeat_runtime = module.ProjectRuntime(
            heartbeat_conn, clock=lambda: claim.lease_expires_at - 1
        )
        original_candidates = prdb._recovery_candidates
        renewed = []

        def discover_then_heartbeat(*args, **kwargs):
            candidates = original_candidates(*args, **kwargs)
            renewed.append(
                heartbeat_runtime.heartbeat_turn(
                    claim, lease_seconds=30
                )
            )
            return candidates

        monkeypatch.setattr(
            prdb, "_recovery_candidates", discover_then_heartbeat
        )
        before_events = tuple(
            conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? ORDER BY sequence
                """,
                (project_id,),
            )
        )

        recovered = runtime.reconcile_inflight_turns(
            _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            limit=10,
        )

        assert recovered == ()
        assert len(renewed) == 1
        assert conn.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "claimed"
        assert tuple(
            conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? ORDER BY sequence
                """,
                (project_id,),
            )
        ) == before_events
    finally:
        if heartbeat_conn is not None:
            heartbeat_conn.close()
        conn.close()
