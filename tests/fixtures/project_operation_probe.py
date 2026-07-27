"""Fresh-process ready/go probe for ProjectOperation approval races."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hermes_cli import project_runtime_db as runtime_db  # noqa: E402
from hermes_cli import projects_db  # noqa: E402
from hermes_cli.project_operations import (  # noqa: E402
    OperationReadbackResult,
    OperationReceipt,
    ProjectOperationError,
    ProjectOperationGuard,
)
from hermes_cli.project_policy import ActorContext  # noqa: E402
from hermes_cli.project_runtime import (  # noqa: E402
    ProjectRuntime,
    ProjectRuntimeError,
    TurnClaim,
)


def _emit(payload: dict[str, object]) -> None:
    print(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )


def _config(raw: str) -> dict[str, object]:
    value = json.loads(raw)
    if type(value) is not dict:
        raise ValueError("probe config must be an object")
    return value


def _operation_claim(
    conn: sqlite3.Connection,
    operation_id: str,
    explicit: object = None,
) -> TurnClaim:
    if type(explicit) is dict:
        return TurnClaim(**explicit)
    row = conn.execute(
        """
        SELECT project_id, turn_id
        FROM project_operations
        WHERE operation_id = ?
        """,
        (operation_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("probe operation is missing")
    state = runtime_db.runtime_state_for_project(
        conn, row["project_id"]
    )
    turn = runtime_db._runtime_turn_for_project(
        conn,
        project_id=row["project_id"],
        turn_id=row["turn_id"],
    )
    control = runtime_db._runtime_control_for_turn(
        conn,
        project_id=row["project_id"],
        turn_id=row["turn_id"],
    )
    lease = runtime_db._current_worker_lease_for_turn(
        conn,
        project_id=row["project_id"],
        turn_id=row["turn_id"],
    )
    if (
        state is None
        or turn is None
        or control is None
        or lease is None
        or turn.attempt_id is None
        or control.claim_worker_id is None
    ):
        raise RuntimeError("probe claim is incomplete")
    return TurnClaim(
        turn_id=turn.turn_id,
        project_id=turn.project_id,
        sequence=turn.sequence,
        worker_id=control.claim_worker_id,
        attempt_id=turn.attempt_id,
        lease_generation=turn.lease_generation,
        fencing_token=turn.fencing_token,
        lease_expires_at=lease.expires_at,
        canonical_session_id=state.conversation_tip_id,
    )


def _remote_receipt(
    ledger_path: str,
    *,
    idempotency_key: str,
) -> OperationReceipt:
    ledger = sqlite3.connect(ledger_path)
    try:
        row = ledger.execute(
            """
            SELECT receipt_id, receipt_json
            FROM remote_effects WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
    finally:
        ledger.close()
    if row is None:
        raise RuntimeError("remote effect is absent")
    return OperationReceipt(row[0], json.loads(row[1]))


def _remote_send(
    ledger_path: str,
    *,
    idempotency_key: str,
) -> OperationReceipt:
    ledger = sqlite3.connect(ledger_path)
    try:
        ledger.execute(
            """
            INSERT OR IGNORE INTO remote_effects (
                idempotency_key, receipt_id, receipt_json
            ) VALUES (?, 'remote-receipt-1', '{"provider_sequence":1}')
            """,
            (idempotency_key,),
        )
        ledger.commit()
    finally:
        ledger.close()
    return _remote_receipt(
        ledger_path, idempotency_key=idempotency_key
    )


class _LedgerReadback:
    def __init__(self, ledger_path: str) -> None:
        self._ledger_path = ledger_path

    def read_operation(self, request):
        ledger = sqlite3.connect(self._ledger_path)
        try:
            row = ledger.execute(
                """
                SELECT receipt_id, receipt_json
                FROM remote_effects WHERE idempotency_key = ?
                """,
                (request.idempotency_key,),
            ).fetchone()
        finally:
            ledger.close()
        if row is None:
            return OperationReadbackResult(
                "not_applied",
                {"ledger": "complete", "present": False},
                None,
            )
        return OperationReadbackResult(
            "applied",
            {"ledger": "complete", "present": True},
            OperationReceipt(row[0], json.loads(row[1])),
        )


class _CrashAfterReadback(_LedgerReadback):
    def read_operation(self, request):
        super().read_operation(request)
        os._exit(77)


def _operation_record(
    conn: sqlite3.Connection,
    operation_id: str,
):
    row = conn.execute(
        """
        SELECT project_id FROM project_operations
        WHERE operation_id = ?
        """,
        (operation_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("probe operation is missing")
    operation = runtime_db._project_operation_for_id(
        conn,
        project_id=row["project_id"],
        operation_id=operation_id,
    )
    if operation is None:
        raise RuntimeError("probe operation disappeared")
    return operation


def _execute_operation(
    guard: ProjectOperationGuard,
    claim: TurnClaim,
    operation_id: str,
    ledger_path: str,
    *,
    boundary: str | None,
):
    if boundary == "before_marker":
        os._exit(73)
    guard.mark_started(claim, operation_id)
    if boundary == "after_marker":
        os._exit(74)
    operation = _operation_record(guard._conn, operation_id)
    receipt = _remote_send(
        ledger_path,
        idempotency_key=operation.idempotency_key,
    )
    if boundary == "after_send":
        os._exit(75)
    guard.record_receipt(claim, operation_id, receipt)
    if boundary == "after_receipt":
        os._exit(76)
    readback = (
        _CrashAfterReadback(ledger_path)
        if boundary == "after_readback"
        else _LedgerReadback(ledger_path)
    )
    return guard.reconcile(claim, operation_id, readback)


def _park_expired_operation_turn(
    runtime: ProjectRuntime,
    operation_id: str,
    *,
    now: int,
):
    operation = _operation_record(runtime._conn, operation_id)
    selected = next(
        (
            candidate
            for candidate in runtime_db._recovery_candidates(
                runtime._conn, now=now, limit=100
            )
            if candidate.project_id == operation.project_id
            and candidate.turn_id == operation.turn_id
        ),
        None,
    )
    if selected is None:
        raise RuntimeError("expired operation candidate is missing")
    candidate = runtime._park_recovery_candidate(
        selected, now=now
    )
    if candidate is None:
        raise RuntimeError("expired operation candidate was not parked")
    return candidate


def main() -> int:
    if len(sys.argv) != 6:
        return 2
    db_path, mode, raw_now, outcome, binding_id = sys.argv[1:]
    try:
        now = int(raw_now)
    except ValueError:
        return 2
    if mode not in {
        "resolve",
        "expire",
        "crash_before",
        "crash_after",
        "rehydrate",
        "execute",
        "complete",
        "start",
        "reconcile",
        "rehydrate_config",
        "no_ready",
        "early_exit",
        "malformed_ready",
        "wrong_ready",
    }:
        return 2
    if mode == "early_exit":
        return 3
    if mode == "malformed_ready":
        print("{not-json", flush=True)
        return 3
    if mode == "wrong_ready":
        _emit({"phase": "not-ready", "pid": os.getpid()})
        return 3
    if mode == "no_ready":
        sys.stdin.readline()
        return 3
    _emit({"phase": "ready", "pid": os.getpid()})
    try:
        command = json.loads(sys.stdin.readline())
    except (EOFError, json.JSONDecodeError):
        return 2
    if command != {"command": "go"}:
        return 2
    if mode == "crash_before":
        os._exit(71)

    conn = projects_db.connect(Path(db_path))
    try:
        runtime = ProjectRuntime(conn, clock=lambda: now)
        guard = ProjectOperationGuard(runtime)
        actor = ActorContext(
            "owner-1", "desktop", binding_id, True
        )
        if mode in {
            "execute",
            "complete",
            "start",
            "reconcile",
            "rehydrate_config",
        }:
            config = _config(outcome)
            operation_id = config.get("operation_id")
            if type(operation_id) is not str or not operation_id:
                return 2
            if mode == "rehydrate_config":
                operation = _operation_record(conn, operation_id)
                worker_id = config.get("worker_id")
                if type(worker_id) is not str or not worker_id:
                    return 2
                claim = guard._rehydrate_approved_operation(
                    operation.project_id,
                    operation_id,
                    worker_id=worker_id,
                    lease_seconds=30,
                )
                result = {
                    "claim": (
                        None
                        if claim is None
                        else {
                            "attempt_id": claim.attempt_id,
                            "fencing_token": claim.fencing_token,
                            "lease_generation": (
                                claim.lease_generation
                            ),
                        }
                    )
                }
            else:
                claim = _operation_claim(
                    conn, operation_id, config.get("claim")
                )
                if mode == "start":
                    operation = guard.mark_started(
                        claim, operation_id
                    )
                elif mode == "reconcile":
                    ledger_path = config.get("ledger_path")
                    if (
                        type(ledger_path) is not str
                        or not ledger_path
                    ):
                        return 2
                    operation = guard.reconcile(
                        claim,
                        operation_id,
                        _LedgerReadback(ledger_path),
                    )
                elif mode == "execute":
                    ledger_path = config.get("ledger_path")
                    boundary = config.get("boundary")
                    if not (
                        type(ledger_path) is str
                        and ledger_path
                        and type(boundary) is str
                    ):
                        return 2
                    operation = _execute_operation(
                        guard,
                        claim,
                        operation_id,
                        ledger_path,
                        boundary=boundary,
                    )
                else:
                    ledger_path = config.get("ledger_path")
                    if (
                        type(ledger_path) is not str
                        or not ledger_path
                    ):
                        return 2
                    operation = _operation_record(
                        conn, operation_id
                    )
                    lease = (
                        runtime_db
                        ._current_worker_lease_for_turn(
                            conn,
                            project_id=operation.project_id,
                            turn_id=operation.turn_id,
                        )
                    )
                    if lease is not None and lease.expires_at <= now:
                        _park_expired_operation_turn(
                            runtime, operation_id, now=now
                        )
                    operation = _operation_record(
                        conn, operation_id
                    )
                    if operation.status in {
                        "effect_started",
                        "receipt_recorded",
                        "unknown",
                    }:
                        operation = guard.reconcile(
                            claim,
                            operation_id,
                            _LedgerReadback(ledger_path),
                        )
                    if operation.status == "approved":
                        fresh = (
                            guard._rehydrate_approved_operation(
                                operation.project_id,
                                operation_id,
                                worker_id="recovery-worker",
                                lease_seconds=30,
                            )
                        )
                        if fresh is None:
                            raise RuntimeError(
                                "approved operation was not rehydrated"
                            )
                        operation = _execute_operation(
                            guard,
                            fresh,
                            operation_id,
                            ledger_path,
                            boundary=None,
                        )
                result = {
                    "operation_status": operation.status
                }
        elif mode == "rehydrate":
            row = conn.execute(
                """
                SELECT project_id
                FROM project_operations
                WHERE operation_id = 'operation-1'
                """
            ).fetchone()
            if row is None:
                return 2
            claim = guard._rehydrate_approved_operation(
                row["project_id"],
                "operation-1",
                worker_id=outcome,
                lease_seconds=30,
            )
            result = {
                "claim": (
                    None
                    if claim is None
                    else {
                        "attempt_id": claim.attempt_id,
                        "fencing_token": claim.fencing_token,
                        "lease_expires_at": claim.lease_expires_at,
                        "lease_generation": claim.lease_generation,
                        "worker_id": claim.worker_id,
                    }
                )
            }
        elif mode == "expire":
            operations = guard.expire_due_operation_approvals(
                limit=1
            )
            result = {
                "count": len(operations),
                "operation_status": (
                    operations[0].status if operations else None
                ),
            }
        else:
            operation = guard.resolve_operation_approval(
                "approval-1",
                actor,
                outcome=outcome,
            )
            result = {"operation_status": operation.status}
        if mode == "crash_after":
            conn.close()
            os._exit(72)
        _emit(result)
        return 0
    except (ProjectOperationError, ProjectRuntimeError) as exc:
        _emit({"error_code": exc.code.value})
        return 0
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
