"""Synchronized fresh-process probe for ProjectRuntime crash/race tests."""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from hermes_cli import project_runtime_db as runtime_db  # noqa: E402
from hermes_cli import projects_db  # noqa: E402
from hermes_cli.project_runtime import (  # noqa: E402
    CanonicalTurnResult,
    ProjectRuntime,
    ProjectRuntimeError,
    TurnClaim,
    TurnReadbackResult,
)


def _read_frame():
    line = sys.stdin.readline()
    if not line:
        raise ValueError("protocol input closed")
    value = json.loads(line)
    if type(value) is not dict:
        raise ValueError("protocol frame must be an object")
    return value


def _required_text(frame, key):
    value = frame.get(key)
    if type(value) is not str or not value:
        raise ValueError(f"{key} must be non-empty text")
    return value


def _emit(prepare, event, **payload):
    print(
        json.dumps(
            {
                "version": 1,
                "probe_id": prepare["probe_id"],
                "event": event,
                **payload,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )


def _claim_from_payload(value, *, project_id, worker_id):
    if type(value) is not dict:
        raise ValueError("claim must be an object")
    claim = TurnClaim(**value)
    if claim.project_id != project_id or claim.worker_id != worker_id:
        raise ValueError("claim identity does not match probe")
    return claim


class _StaticReadback:
    def __init__(self, conn, prepare):
        self._conn = conn
        self._prepare = prepare
        self.requests = []

    def read_turn(self, request):
        if self._conn.in_transaction:
            raise AssertionError("readback ran inside a SQLite transaction")
        self.requests.append(asdict(request))
        value = self._prepare.get("readback")
        if type(value) is not dict:
            raise AssertionError("unexpected canonical readback")
        return TurnReadbackResult(
            value.get("outcome"), value.get("result_id")
        )


class _CrashAfterPhaseA:
    def __init__(self, conn, prepare):
        self._conn = conn
        self._prepare = prepare

    def read_turn(self, request):
        if self._conn.in_transaction:
            raise AssertionError("phase-A boundary still owns a transaction")
        _emit(
            self._prepare,
            "boundary",
            action="recover",
            boundary="reconciling_committed",
            request=asdict(request),
        )
        os._exit(93)


def _prepare_frame():
    frame = _read_frame()
    if frame.get("version") != 1 or frame.get("event") != "prepare":
        raise ValueError("expected protocol prepare frame")
    _required_text(frame, "probe_id")
    action = _required_text(frame, "action")
    if action not in {"claim", "start", "commit", "recover"}:
        raise ValueError("unsupported probe action")
    _required_text(frame, "db_path")
    _required_text(frame, "project_id")
    _required_text(frame, "worker_id")
    if type(frame.get("now")) is not int:
        raise ValueError("now must be an exact integer")
    allowed_crashes = {
        "claim": {None, "claim_commit"},
        "start": {None, "start_commit"},
        "commit": {None},
        "recover": {None, "phase_a_reconciling_commit"},
    }
    if frame.get("crash_after") not in allowed_crashes[action]:
        raise ValueError("unsupported crash boundary")
    return frame


def _await_go(prepare):
    frame = _read_frame()
    if not (
        frame.get("version") == 1
        and frame.get("event") == "go"
        and frame.get("probe_id") == prepare["probe_id"]
    ):
        raise ValueError("expected matching go frame")


def _execute(runtime, conn, prepare):
    action = prepare["action"]
    project_id = prepare["project_id"]
    worker_id = prepare["worker_id"]
    if action == "claim":
        lease_seconds = prepare.get("lease_seconds")
        if type(lease_seconds) is not int:
            raise ValueError("lease_seconds must be an exact integer")
        claim = runtime.claim_next_turn(
            project_id, worker_id, lease_seconds=lease_seconds
        )
        if prepare.get("crash_after") == "claim_commit":
            if claim is None:
                raise AssertionError("claim crash boundary has no claim")
            _emit(
                prepare,
                "boundary",
                action=action,
                boundary="claim_committed",
                claim=asdict(claim),
            )
            os._exit(91)
        return {"claim": asdict(claim) if claim is not None else None}
    if action == "start":
        claim = _claim_from_payload(
            prepare.get("claim"),
            project_id=project_id,
            worker_id=worker_id,
        )
        started = runtime.mark_turn_started(claim)
        if prepare.get("crash_after") == "start_commit":
            _emit(
                prepare,
                "boundary",
                action=action,
                boundary="start_committed",
                claim=asdict(started),
            )
            os._exit(92)
        return {"claim": asdict(started)}
    if action == "commit":
        claim = _claim_from_payload(
            prepare.get("claim"),
            project_id=project_id,
            worker_id=worker_id,
        )
        turn = runtime.commit_turn(
            claim,
            CanonicalTurnResult(
                prepare.get("outcome"), prepare.get("result_id")
            ),
        )
        return {
            "turn": {"status": turn.status, "turn_id": turn.turn_id}
        }
    limit = prepare.get("limit")
    if type(limit) is not int:
        raise ValueError("limit must be an exact integer")
    if prepare.get("crash_after") == "phase_a_reconciling_commit":
        readback = _CrashAfterPhaseA(conn, prepare)
        requests = []
    else:
        readback = _StaticReadback(conn, prepare)
        requests = readback.requests
    turns = runtime.reconcile_inflight_turns(readback, limit=limit)
    return {
        "readback_requests": requests,
        "turns": [
            {"status": turn.status, "turn_id": turn.turn_id}
            for turn in turns
        ],
    }


def main():
    try:
        prepare = _prepare_frame()
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return 2
    conn = projects_db.connect(Path(prepare["db_path"]))
    original_write_transaction = runtime_db.write_transaction
    released = False

    @contextmanager
    def synchronized_write_transaction(connection):
        nonlocal released
        if not released:
            if connection is not conn or connection.in_transaction:
                raise AssertionError("invalid synchronized write boundary")
            released = True
            _emit(
                prepare,
                "ready",
                action=prepare["action"],
                stage="before_begin_immediate",
            )
            _await_go(prepare)
        with original_write_transaction(connection):
            yield

    runtime_db.write_transaction = synchronized_write_transaction
    try:
        runtime = ProjectRuntime(conn, clock=lambda: prepare["now"])
        try:
            result = _execute(runtime, conn, prepare)
        except ProjectRuntimeError as exc:
            _emit(
                prepare,
                "result",
                action=prepare["action"],
                ok=False,
                error={"code": exc.code.value},
            )
        else:
            _emit(
                prepare,
                "result",
                action=prepare["action"],
                ok=True,
                **result,
            )
        return 0
    finally:
        runtime_db.write_transaction = original_write_transaction
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
