"""Fresh-process probe for durable ProjectRuntime claim and takeover tests."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from hermes_cli import projects_db
from hermes_cli.project_runtime import (  # noqa: E402
    CanonicalTurnResult,
    ProjectRuntime,
    ProjectRuntimeError,
    TurnClaim,
)


class _NoReadback:
    def read_turn(self, request):
        raise AssertionError("not-started recovery must not call readback")


def _claim_payload(claim):
    return asdict(claim) if claim is not None else None


def _stored_claim(raw_claim, *, project_id, worker_id):
    value = json.loads(raw_claim)
    if type(value) is not dict:
        raise ValueError("claim must be an object")
    claim = TurnClaim(**value)
    if claim.project_id != project_id or claim.worker_id != worker_id:
        raise ValueError("claim identity does not match probe arguments")
    return claim


def main() -> int:
    if len(sys.argv) < 6:
        return 2
    _, action, database, project_id, worker_id, raw_now, *arguments = sys.argv
    expected_arguments = {
        "claim": 0,
        "recover-claim": 0,
        "start": 1,
        "commit": 3,
    }
    if expected_arguments.get(action) != len(arguments):
        return 2
    try:
        now = int(raw_now)
    except ValueError:
        return 2
    conn = projects_db.connect(Path(database))
    try:
        runtime = ProjectRuntime(conn, clock=lambda: now)
        recovered = ()
        if action == "recover-claim":
            recovered = runtime.reconcile_inflight_turns(
                _NoReadback(), limit=10
            )
        if action in {"claim", "recover-claim"}:
            claim = runtime.claim_next_turn(
                project_id, worker_id, lease_seconds=30
            )
            result = {
                "claim": _claim_payload(claim),
                "recovered": [
                    {"status": turn.status, "turn_id": turn.turn_id}
                    for turn in recovered
                ],
            }
        elif action == "start":
            claim = runtime.mark_turn_started(
                _stored_claim(
                    arguments[0],
                    project_id=project_id,
                    worker_id=worker_id,
                )
            )
            result = {"claim": _claim_payload(claim), "recovered": []}
        else:
            claim = _stored_claim(
                arguments[0],
                project_id=project_id,
                worker_id=worker_id,
            )
            turn = runtime.commit_turn(
                claim,
                CanonicalTurnResult(arguments[1], arguments[2]),
            )
            result = {
                "claim": None,
                "recovered": [],
                "status": turn.status,
                "turn_id": turn.turn_id,
            }
        print(
            json.dumps(result, sort_keys=True),
            flush=True,
        )
        return 0
    except ProjectRuntimeError as exc:
        print(
            json.dumps(
                {
                    "error": {"code": exc.code.value},
                    "ok": False,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
