"""Fresh-process probe for the profile-wide Task-7 Core lease."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from hermes_cli import project_runtime_db as runtime_db  # noqa: E402
from hermes_cli.project_runtime import (  # noqa: E402
    DispatcherLease,
    ProjectRuntime,
    ProjectRuntimeError,
)


def _read_frame() -> dict[str, object]:
    line = sys.stdin.readline()
    if not line:
        raise ValueError("protocol input closed")
    value = json.loads(line)
    if type(value) is not dict:
        raise ValueError("protocol frame must be an object")
    return value


def _required_text(
    frame: dict[str, object], key: str
) -> str:
    value = frame.get(key)
    if type(value) is not str or not value:
        raise ValueError(f"{key} must be non-empty text")
    return value


def _emit(
    prepare: dict[str, object],
    event: str,
    **payload: object,
) -> None:
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


def _prepare_frame() -> dict[str, object]:
    frame = _read_frame()
    if frame.get("version") != 1 or frame.get("event") != "prepare":
        raise ValueError("expected protocol prepare frame")
    _required_text(frame, "probe_id")
    _required_text(frame, "db_path")
    _required_text(frame, "instance_id")
    action = _required_text(frame, "action")
    if action not in {"acquire", "renew", "release"}:
        raise ValueError("unsupported probe action")
    if type(frame.get("now")) is not int:
        raise ValueError("now must be an exact integer")
    if action in {"acquire", "renew"} and type(
        frame.get("lease_seconds")
    ) is not int:
        raise ValueError("lease_seconds must be an exact integer")
    if action in {"renew", "release"} and type(
        frame.get("lease")
    ) is not dict:
        raise ValueError("lease must be an object")
    return frame


def _await_go(prepare: dict[str, object]) -> None:
    frame = _read_frame()
    if not (
        frame.get("version") == 1
        and frame.get("event") == "go"
        and frame.get("probe_id") == prepare["probe_id"]
    ):
        raise ValueError("expected matching go frame")


def _lease_from_frame(
    prepare: dict[str, object],
) -> DispatcherLease:
    value = prepare["lease"]
    assert type(value) is dict
    return DispatcherLease(**value)


def main() -> int:
    try:
        prepare = _prepare_frame()
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return 2
    conn = sqlite3.connect(str(prepare["db_path"]), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    original_write_transaction = runtime_db.write_transaction
    released = False

    @contextmanager
    def synchronized_write_transaction(connection):
        nonlocal released
        if not released:
            if connection is not conn or connection.in_transaction:
                raise AssertionError(
                    "invalid synchronized write boundary"
                )
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
        runtime = ProjectRuntime(
            conn,
            clock=lambda: prepare["now"],
        )
        before_changes = conn.total_changes
        statements: list[str] = []
        conn.set_trace_callback(statements.append)
        error: ProjectRuntimeError | None = None
        result: dict[str, object] | None = None
        try:
            action = prepare["action"]
            if action == "acquire":
                lease = runtime.acquire_dispatcher_lease(
                    prepare["instance_id"],
                    lease_seconds=prepare["lease_seconds"],
                )
                result = {
                    "lease": (
                        asdict(lease)
                        if lease is not None
                        else None
                    )
                }
            elif action == "renew":
                lease = runtime.renew_dispatcher_lease(
                    _lease_from_frame(prepare),
                    lease_seconds=prepare["lease_seconds"],
                )
                result = {"lease": asdict(lease)}
            else:
                result = {
                    "released": runtime.release_dispatcher_lease(
                        _lease_from_frame(prepare)
                    )
                }
        except ProjectRuntimeError as exc:
            error = exc
        finally:
            conn.set_trace_callback(None)
        mutations = [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]
        telemetry = {
            "write_count": conn.total_changes - before_changes,
            "mutations": mutations,
        }
        if error is not None:
            _emit(
                prepare,
                "result",
                action=prepare["action"],
                ok=False,
                error={"code": error.code.value},
                **telemetry,
            )
        else:
            assert result is not None
            _emit(
                prepare,
                "result",
                action=prepare["action"],
                ok=True,
                **result,
                **telemetry,
            )
        return 0
    finally:
        runtime_db.write_transaction = original_write_transaction
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
