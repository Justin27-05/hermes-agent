"""Behavioral contract tests for the durable per-project FIFO runtime."""

from __future__ import annotations

import contextlib
import importlib
import inspect
import json
import math
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, fields, replace
from pathlib import Path
from typing import Literal, Mapping, get_type_hints

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import ActorContext


def _event_count(conn, project_id):
    return conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?", (project_id,)
    ).fetchone()[0]


def _dispatcher_lease_snapshot(conn):
    row = conn.execute(
        """
        SELECT lease_name, instance_id, generation, fencing_token,
               expires_at, updated_at
        FROM project_dispatcher_leases
        WHERE lease_name = 'core'
        """
    ).fetchone()
    return tuple(row) if row is not None else None


@contextlib.contextmanager
def _assert_dispatcher_write_free(conn):
    before_changes = conn.total_changes
    statements = []
    conn.set_trace_callback(statements.append)
    try:
        yield
    finally:
        conn.set_trace_callback(None)
        mutations = [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]
        assert conn.total_changes == before_changes
        assert mutations == []


def _runtime_mutation_snapshot(conn, project_id, turn_id):
    return (
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_approvals
                WHERE project_id = ?
                ORDER BY approval_id
                """,
                (project_id,),
            )
        ),
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?", (turn_id,)
        ).fetchone()),
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                ORDER BY lease_id
                """,
                (project_id, turn_id),
            )
        ),
        prdb.runtime_state_for_project(conn, project_id),
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ?
                ORDER BY sequence
                """,
                (project_id,),
            )
        ),
    )


def _adopt_bound_project(conn, *, name, root, binding, external):
    project_id = projects_db.create_project(conn, name=name)
    prdb.create_project_conversation(
        conn, project_id=project_id, conversation_id=root,
        current_phase="implementation", now=1,
    )
    prdb.bind_surface(
        conn, binding_id=binding, project_id=project_id, surface="desktop",
        external_binding_id=external, actor_id="owner-1", now=1,
    )
    return project_id, ActorContext("owner-1", "desktop", binding, True)


def _approval_request(
    project_id,
    *,
    approval_id="approval",
    expected_runtime_version=2,
    expected_lifecycle="active",
    expires_at=1000,
):
    return prdb.ApprovalRequest(
        approval_id=approval_id,
        project_id=project_id,
        requester_actor_id="owner-1",
        authorization_actor_id="owner-1",
        canonical_action="publish",
        approval_class="publish",
        command_revision=1,
        expected_runtime_version=expected_runtime_version,
        expected_lifecycle=expected_lifecycle,
        expected_phase="implementation",
        targets=("C:/work/runtime/release",),
        batch_id="batch",
        batch_items=("release",),
        status="pending",
        expires_at=expires_at,
    )


@pytest.fixture
def runtime_env(tmp_path):
    conn = projects_db.connect(tmp_path / "projects.db")
    project_id = projects_db.create_project(
        conn, name="Runtime", folders=("C:/work/runtime",)
    )
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="session-root",
        current_phase="implementation",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="desktop-owner",
        project_id=project_id,
        surface="desktop",
        external_binding_id="window-1",
        actor_id="owner-1",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="discord-owner",
        project_id=project_id,
        surface="discord",
        external_binding_id="channel-1",
        actor_id="owner-1",
        now=1,
    )
    module = importlib.import_module("hermes_cli.project_runtime")
    yield {
        "conn": conn,
        "project_id": project_id,
        "runtime": module.ProjectRuntime(conn, clock=lambda: 100),
        "desktop": ActorContext("owner-1", "desktop", "desktop-owner", True),
        "discord": ActorContext("owner-1", "discord", "discord-owner", True),
        "module": module,
    }
    conn.close()


def test_public_values_and_control_method_signatures_match_the_task4_contract():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert module.JSONScalar == str | int | float | bool | None
    assert module.JSONValue == (
        module.JSONScalar
        | tuple["JSONValue", ...]
        | Mapping[str, "JSONValue"]
    )
    assert module.TurnStatus == Literal[
        "queued",
        "claimed",
        "awaiting_approval",
        "stop_requested",
        "stopped",
        "reconciling",
        "succeeded",
        "failed",
        "cancelled",
    ]
    assert module.ControlState == Literal[
        "running",
        "stop_requested",
        "stopped",
        "resume_requested",
        "terminal",
    ]
    assert module.ApprovalRequest is prdb.ApprovalRequest
    assert tuple(field.name for field in fields(module.ProjectTurn)) == (
        "turn_id", "project_id", "sequence", "idempotency_key", "payload",
        "origin_binding_id", "status", "attempt_id", "lease_generation",
        "fencing_token", "created_at", "updated_at",
    )
    assert tuple(field.name for field in fields(module.RunControl)) == (
        "turn_id", "project_id", "control_state", "control_version",
        "last_idempotency_key", "attempt_id", "updated_at",
    )
    assert tuple(field.name for field in fields(module.TurnClaim)) == (
        "turn_id", "project_id", "sequence", "worker_id", "attempt_id",
        "lease_generation", "fencing_token", "lease_expires_at",
        "canonical_session_id",
    )
    assert tuple(field.name for field in fields(module.TurnApproval)) == (
        "turn_id", "approval",
    )
    assert tuple(inspect.signature(module.ProjectRuntime.cancel_queued_turn).parameters) == (
        "self", "project_id", "turn_id", "actor", "idempotency_key",
        "expected_version", "expected_control_version",
    )
    assert tuple(inspect.signature(module.ProjectRuntime.request_turn_approval).parameters) == (
        "self", "turn_id", "request", "actor", "expected_control_version",
    )
    assert tuple(
        inspect.signature(
            module.ProjectRuntime.renew_delivery
        ).parameters
    ) == ("self", "claim", "lease_seconds")
    assert tuple(
        inspect.signature(
            module.ProjectRuntime.complete_delivery
        ).parameters
    ) == ("self", "claim", "remote_message_ids")
    assert tuple(
        inspect.signature(
            module.ProjectRuntime.defer_delivery
        ).parameters
    ) == (
        "self",
        "claim",
        "error_code",
        "delay_seconds",
    )
    assert tuple(
        inspect.signature(
            module.ProjectRuntime.block_delivery
        ).parameters
    ) == ("self", "claim", "error_code")
    assert tuple(
        inspect.signature(
            module.ProjectRuntime.suppress_origin_delivery
        ).parameters
    ) == ("self", "claim")
    turn_hints = get_type_hints(module.ProjectTurn)
    control_hints = get_type_hints(module.RunControl)
    approval_hints = get_type_hints(module.TurnApproval)
    request_hints = get_type_hints(module.ProjectRuntime.request_turn_approval)
    assert (
        module.ProjectTurn.__annotations__["payload"]
        == "Mapping[str, JSONValue]"
    )
    assert turn_hints["status"] == module.TurnStatus
    assert control_hints["control_state"] == module.ControlState
    assert approval_hints["approval"] is module.ApprovalRequest
    assert request_hints["request"] is module.ApprovalRequest
    assert request_hints["return"] is module.TurnApproval


def test_rename_event_persists_its_immutable_surface_snapshot(runtime_env):
    """Later projection must not need the mutable project row for a rename."""
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    first = runtime.rename_project(
        project_id,
        "Immutable Rename",
        runtime_env["desktop"],
        idempotency_key="immutable-rename",
        expected_version=0,
    )

    row = runtime_env["conn"].execute(
        """
        SELECT payload_json FROM project_events
        WHERE project_id = ? AND kind = 'project.renamed'
        """,
        (project_id,),
    ).fetchone()

    assert row is not None
    assert json.loads(row["payload_json"])["surface"] == {
        "lifecycle": "active",
        "name": "Immutable Rename",
    }

    changes = runtime_env["conn"].total_changes
    replay = runtime.rename_project(
        project_id,
        "Immutable Rename",
        runtime_env["discord"],
        idempotency_key="immutable-rename",
        expected_version=0,
    )

    assert replay == first
    assert runtime_env["conn"].total_changes == changes
    assert runtime_env["conn"].execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'project.renamed'
        """,
        (project_id,),
    ).fetchone()[0] == 1
    assert runtime_env["conn"].execute(
        """
        SELECT payload_json FROM project_events
        WHERE project_id = ? AND kind = 'project.renamed'
        """,
        (project_id,),
    ).fetchone()["payload_json"] == row["payload_json"]


def test_rename_replay_rejects_changed_command_fingerprint(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    runtime.rename_project(
        project_id,
        "First Rename",
        runtime_env["desktop"],
        idempotency_key="rename-fingerprint",
        expected_version=0,
    )
    changes = runtime_env["conn"].total_changes

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as conflict:
        runtime.rename_project(
            project_id,
            "Different Rename",
            runtime_env["discord"],
            idempotency_key="rename-fingerprint",
            expected_version=0,
        )

    assert conflict.value.code is (
        runtime_env["module"].RuntimeErrorCode.IDEMPOTENCY_CONFLICT
    )
    assert runtime_env["conn"].total_changes == changes
    assert projects_db.get_project(
        runtime_env["conn"], project_id
    ).name == "First Rename"


@pytest.mark.parametrize(
    "payload_variant",
    ("malformed", "noncanonical", "missing", "mismatch"),
)
def test_rename_replay_fails_closed_for_untrusted_stored_fingerprint(
    runtime_env,
    payload_variant,
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    runtime.rename_project(
        project_id,
        "Trusted Rename",
        runtime_env["desktop"],
        idempotency_key="rename-stored-fingerprint",
        expected_version=0,
    )
    row = conn.execute(
        """
        SELECT event_id, payload_json FROM project_events
        WHERE project_id = ? AND kind = 'project.renamed'
        """,
        (project_id,),
    ).fetchone()
    assert row is not None
    original = json.loads(row["payload_json"])
    if payload_variant == "malformed":
        stored_payload = '{"command_fingerprint":'
    elif payload_variant == "noncanonical":
        stored_payload = json.dumps(original, ensure_ascii=False)
    elif payload_variant == "missing":
        stored_payload = json.dumps(
            {"surface": original["surface"]},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    else:
        stored_payload = json.dumps(
            {
                **original,
                "command_fingerprint": "0" * 64,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    conn.execute(
        "UPDATE project_events SET payload_json = ? WHERE event_id = ?",
        (stored_payload, row["event_id"]),
    )
    conn.commit()
    changes = conn.total_changes

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as conflict:
        runtime.rename_project(
            project_id,
            "Trusted Rename",
            runtime_env["discord"],
            idempotency_key="rename-stored-fingerprint",
            expected_version=0,
        )

    assert conflict.value.code is (
        runtime_env["module"].RuntimeErrorCode.IDEMPOTENCY_CONFLICT
    )
    assert conn.total_changes == changes
    assert _event_count(conn, project_id) == 1


def test_dispatcher_lease_public_contract_is_frozen_and_explicit():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert tuple(field.name for field in fields(module.DispatcherLease)) == (
        "instance_id",
        "generation",
        "fencing_token",
        "expires_at",
    )
    assert tuple(
        inspect.signature(
            module.ProjectRuntime.acquire_dispatcher_lease
        ).parameters
    ) == ("self", "instance_id", "lease_seconds")
    assert tuple(
        inspect.signature(
            module.ProjectRuntime.renew_dispatcher_lease
        ).parameters
    ) == ("self", "lease", "lease_seconds")
    assert tuple(
        inspect.signature(
            module.ProjectRuntime.release_dispatcher_lease
        ).parameters
    ) == ("self", "lease")
    assert (
        inspect.signature(
            module.ProjectRuntime.acquire_dispatcher_lease
        ).parameters["lease_seconds"].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    assert (
        inspect.signature(
            module.ProjectRuntime.renew_dispatcher_lease
        ).parameters["lease_seconds"].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    assert (
        module.RuntimeErrorCode.STALE_DISPATCHER_LEASE.value
        == "stale_dispatcher_lease"
    )
    lease = module.DispatcherLease(
        "11111111-1111-4111-8111-111111111111",
        1,
        1,
        130,
    )
    with pytest.raises(FrozenInstanceError):
        lease.expires_at = 131


def test_core_dispatcher_lease_acquire_is_sticky_and_profile_wide(
    runtime_env
):
    now = [100]
    module = runtime_env["module"]
    runtime = module.ProjectRuntime(
        runtime_env["conn"], clock=lambda: now[0]
    )
    leader_id = "11111111-1111-4111-8111-111111111111"
    standby_id = "22222222-2222-4222-8222-222222222222"

    acquired = runtime.acquire_dispatcher_lease(
        leader_id, lease_seconds=30
    )
    assert acquired == module.DispatcherLease(
        leader_id, 1, 1, 130
    )
    stored = _dispatcher_lease_snapshot(runtime_env["conn"])

    now[0] = 101
    with _assert_dispatcher_write_free(runtime_env["conn"]):
        assert runtime.acquire_dispatcher_lease(
            leader_id, lease_seconds=300
        ) == acquired
    with _assert_dispatcher_write_free(runtime_env["conn"]):
        assert runtime.acquire_dispatcher_lease(
            standby_id, lease_seconds=30
        ) is None
    assert _dispatcher_lease_snapshot(runtime_env["conn"]) == stored


def test_core_dispatcher_lease_scope_isolated_by_projects_database(
    tmp_path,
):
    module = importlib.import_module("hermes_cli.project_runtime")
    first_conn = projects_db.connect(tmp_path / "profile-a" / "projects.db")
    second_conn = projects_db.connect(
        tmp_path / "profile-b" / "projects.db"
    )
    try:
        first = module.ProjectRuntime(
            first_conn, clock=lambda: 100
        ).acquire_dispatcher_lease(
            "11111111-1111-4111-8111-111111111111",
            lease_seconds=30,
        )
        second = module.ProjectRuntime(
            second_conn, clock=lambda: 100
        ).acquire_dispatcher_lease(
            "22222222-2222-4222-8222-222222222222",
            lease_seconds=30,
        )

        assert first == module.DispatcherLease(
            "11111111-1111-4111-8111-111111111111",
            1,
            1,
            130,
        )
        assert second == module.DispatcherLease(
            "22222222-2222-4222-8222-222222222222",
            1,
            1,
            130,
        )
    finally:
        first_conn.close()
        second_conn.close()


def test_core_dispatcher_lease_boundary_takeover_fences_stale_owner(
    runtime_env
):
    now = [100]
    module = runtime_env["module"]
    runtime = module.ProjectRuntime(
        runtime_env["conn"], clock=lambda: now[0]
    )
    old = runtime.acquire_dispatcher_lease(
        "11111111-1111-4111-8111-111111111111",
        lease_seconds=30,
    )
    assert old is not None

    now[0] = 129
    with _assert_dispatcher_write_free(runtime_env["conn"]):
        assert runtime.acquire_dispatcher_lease(
            "22222222-2222-4222-8222-222222222222",
            lease_seconds=30,
        ) is None
    before_takeover = _dispatcher_lease_snapshot(
        runtime_env["conn"]
    )
    assert before_takeover[-2:] == (130, 100)

    now[0] = 130
    takeover = runtime.acquire_dispatcher_lease(
        "22222222-2222-4222-8222-222222222222",
        lease_seconds=30,
    )
    assert takeover == module.DispatcherLease(
        "22222222-2222-4222-8222-222222222222",
        2,
        2,
        160,
    )
    after_takeover = _dispatcher_lease_snapshot(runtime_env["conn"])

    now[0] = 131
    with pytest.raises(module.ProjectRuntimeError) as stale_renew:
        with _assert_dispatcher_write_free(runtime_env["conn"]):
            runtime.renew_dispatcher_lease(
                old, lease_seconds=30
            )
    assert (
        stale_renew.value.code
        is module.RuntimeErrorCode.STALE_DISPATCHER_LEASE
    )
    with _assert_dispatcher_write_free(runtime_env["conn"]):
        assert runtime.release_dispatcher_lease(old) is False
    assert _dispatcher_lease_snapshot(
        runtime_env["conn"]
    ) == after_takeover


def test_core_dispatcher_renew_rechecks_expiry_after_lock_wait(
    runtime_env,
    monkeypatch,
):
    now = [100]
    module = runtime_env["module"]
    conn = runtime_env["conn"]
    runtime = module.ProjectRuntime(
        conn,
        clock=lambda: now[0],
    )
    lease = runtime.acquire_dispatcher_lease(
        "11111111-1111-4111-8111-111111111111",
        lease_seconds=30,
    )
    assert lease is not None
    before = _dispatcher_lease_snapshot(conn)
    before_changes = conn.total_changes
    original_write_transaction = prdb.write_transaction
    crossed_boundary = False

    @contextlib.contextmanager
    def expire_before_begin(connection):
        nonlocal crossed_boundary
        if connection is conn and not crossed_boundary:
            crossed_boundary = True
            now[0] = lease.expires_at
        with original_write_transaction(connection):
            yield

    now[0] = lease.expires_at - 1
    monkeypatch.setattr(
        prdb,
        "write_transaction",
        expire_before_begin,
    )
    caught = None
    statements = []
    conn.set_trace_callback(statements.append)
    try:
        runtime.renew_dispatcher_lease(
            lease,
            lease_seconds=30,
        )
    except module.ProjectRuntimeError as exc:
        caught = exc
    finally:
        conn.set_trace_callback(None)
    mutations = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(
            ("INSERT", "UPDATE", "DELETE", "REPLACE")
        )
    ]

    assert crossed_boundary
    assert _dispatcher_lease_snapshot(conn) == before
    assert conn.total_changes == before_changes
    assert mutations == []
    assert type(caught) is module.ProjectRuntimeError
    assert (
        caught.code
        is module.RuntimeErrorCode.STALE_DISPATCHER_LEASE
    )


def test_core_dispatcher_acquire_rechecks_expiry_after_lock_wait(
    runtime_env,
    monkeypatch,
):
    now = [100]
    module = runtime_env["module"]
    conn = runtime_env["conn"]
    runtime = module.ProjectRuntime(
        conn,
        clock=lambda: now[0],
    )
    lease = runtime.acquire_dispatcher_lease(
        "11111111-1111-4111-8111-111111111111",
        lease_seconds=30,
    )
    assert lease is not None
    original_write_transaction = prdb.write_transaction
    crossed_boundary = False

    @contextlib.contextmanager
    def expire_before_begin(connection):
        nonlocal crossed_boundary
        if connection is conn and not crossed_boundary:
            crossed_boundary = True
            now[0] = lease.expires_at
        with original_write_transaction(connection):
            yield

    now[0] = lease.expires_at - 1
    monkeypatch.setattr(
        prdb,
        "write_transaction",
        expire_before_begin,
    )
    takeover = runtime.acquire_dispatcher_lease(
        "22222222-2222-4222-8222-222222222222",
        lease_seconds=30,
    )

    assert crossed_boundary
    assert takeover == module.DispatcherLease(
        "22222222-2222-4222-8222-222222222222",
        2,
        2,
        160,
    )
    assert _dispatcher_lease_snapshot(conn) == (
        "core",
        "22222222-2222-4222-8222-222222222222",
        2,
        2,
        160,
        130,
    )


def test_core_dispatcher_renew_release_and_reacquire_preserve_epoch_counters(
    runtime_env
):
    now = [100]
    module = runtime_env["module"]
    runtime = module.ProjectRuntime(
        runtime_env["conn"], clock=lambda: now[0]
    )
    first = runtime.acquire_dispatcher_lease(
        "11111111-1111-4111-8111-111111111111",
        lease_seconds=30,
    )
    assert first is not None

    now[0] = 110
    renewed = runtime.renew_dispatcher_lease(
        first, lease_seconds=50
    )
    assert renewed == replace(first, expires_at=160)
    with _assert_dispatcher_write_free(runtime_env["conn"]):
        assert runtime.release_dispatcher_lease(first) is False
    now[0] = 120
    assert runtime.renew_dispatcher_lease(
        renewed, lease_seconds=10
    ) == renewed

    now[0] = 121
    assert runtime.release_dispatcher_lease(renewed) is True
    released = _dispatcher_lease_snapshot(runtime_env["conn"])
    assert released == ("core", None, 1, 1, 121, 121)
    with _assert_dispatcher_write_free(runtime_env["conn"]):
        assert runtime.release_dispatcher_lease(renewed) is False
    assert _dispatcher_lease_snapshot(
        runtime_env["conn"]
    ) == released

    reacquired = runtime.acquire_dispatcher_lease(
        "22222222-2222-4222-8222-222222222222",
        lease_seconds=30,
    )
    assert reacquired == module.DispatcherLease(
        "22222222-2222-4222-8222-222222222222",
        2,
        2,
        151,
    )


def test_same_instance_expired_reacquire_mints_a_new_fenced_epoch(
    runtime_env
):
    now = [100]
    module = runtime_env["module"]
    runtime = module.ProjectRuntime(
        runtime_env["conn"], clock=lambda: now[0]
    )
    instance_id = "11111111-1111-4111-8111-111111111111"
    old = runtime.acquire_dispatcher_lease(
        instance_id, lease_seconds=30
    )
    assert old is not None

    now[0] = 130
    current = runtime.acquire_dispatcher_lease(
        instance_id, lease_seconds=30
    )
    assert current == module.DispatcherLease(
        instance_id,
        2,
        2,
        160,
    )
    after_reacquire = _dispatcher_lease_snapshot(runtime_env["conn"])

    now[0] = 131
    with pytest.raises(module.ProjectRuntimeError) as stale:
        with _assert_dispatcher_write_free(runtime_env["conn"]):
            runtime.renew_dispatcher_lease(
                old, lease_seconds=30
            )
    assert (
        stale.value.code
        is module.RuntimeErrorCode.STALE_DISPATCHER_LEASE
    )
    with _assert_dispatcher_write_free(runtime_env["conn"]):
        assert runtime.release_dispatcher_lease(old) is False
    assert _dispatcher_lease_snapshot(
        runtime_env["conn"]
    ) == after_reacquire


@pytest.mark.parametrize(
    ("instance_id", "lease_seconds"),
    (
        pytest.param("not-a-uuid", 30, id="non-uuid"),
        pytest.param(
            "11111111-1111-3111-8111-111111111111",
            30,
            id="non-v4",
        ),
        pytest.param(
            "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
            30,
            id="non-canonical-case",
        ),
        pytest.param(
            "11111111-1111-4111-8111-111111111111",
            True,
            id="boolean-ttl",
        ),
        pytest.param(
            "11111111-1111-4111-8111-111111111111",
            0,
            id="zero-ttl",
        ),
    ),
)
def test_core_dispatcher_acquire_rejects_invalid_identity_or_ttl_write_free(
    runtime_env, instance_id, lease_seconds
):
    module = runtime_env["module"]
    before = _dispatcher_lease_snapshot(runtime_env["conn"])

    with pytest.raises(module.ProjectRuntimeError) as invalid:
        runtime_env["runtime"].acquire_dispatcher_lease(
            instance_id, lease_seconds=lease_seconds
        )

    assert invalid.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
    assert _dispatcher_lease_snapshot(runtime_env["conn"]) == before


def test_core_dispatcher_takeover_counter_overflow_is_write_free(
    runtime_env
):
    maximum = (1 << 63) - 1
    module = runtime_env["module"]
    runtime_env["conn"].execute(
        """
        INSERT INTO project_dispatcher_leases (
            lease_name, instance_id, generation, fencing_token,
            expires_at, updated_at
        ) VALUES ('core', NULL, ?, ?, 100, 100)
        """,
        (maximum, maximum),
    )
    runtime_env["conn"].commit()
    before = _dispatcher_lease_snapshot(runtime_env["conn"])

    with pytest.raises(RuntimeError):
        with _assert_dispatcher_write_free(runtime_env["conn"]):
            runtime_env["runtime"].acquire_dispatcher_lease(
                "11111111-1111-4111-8111-111111111111",
                lease_seconds=30,
            )

    assert _dispatcher_lease_snapshot(runtime_env["conn"]) == before


@pytest.mark.parametrize("transition", ("renew", "takeover"))
def test_task7_c4_startauthority_queue_fences_at_write_boundary(
    runtime_env,
    monkeypatch,
    transition,
):
    module = runtime_env["module"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime_env["runtime"].enqueue_turn(
        project_id,
        {"message": "Core-fenced queue start"},
        runtime_env["desktop"],
        idempotency_key=f"c4-queue-{transition}",
        expected_version=0,
    )
    now = [100]
    identity_counts = {}
    attempt_observations = []
    traced_statements = []

    def identity(kind):
        identity_counts[kind] = identity_counts.get(kind, 0) + 1
        if kind == "attempt":
            normalized = [
                " ".join(statement.lower().split())
                for statement in traced_statements
            ]
            attempt_observations.append(
                (
                    conn.in_transaction,
                    any(
                        statement.startswith(
                            "select instance_id, generation, "
                            "fencing_token, expires_at from "
                            "project_dispatcher_leases "
                            "where lease_name = 'core'"
                        )
                        for statement in normalized
                    ),
                    not any(
                        statement.startswith(
                            ("insert", "update", "delete", "replace")
                        )
                        for statement in normalized
                    ),
                )
            )
        return f"task7-c4-{kind}-{identity_counts[kind]}"

    runtime = module.ProjectRuntime(
        conn,
        clock=lambda: now[0],
        id_factory=identity,
    )
    old_lease = runtime.acquire_dispatcher_lease(
        "11111111-1111-4111-8111-111111111111",
        lease_seconds=30,
    )
    assert old_lease is not None
    upper = runtime.runnable_project_membership_upper_watermark()
    assert upper is not None
    discovery = runtime.scan_runnable_projects(
        after=None,
        through_membership_sequence=upper,
        limit=100,
    )
    assert [item.project_id for item in discovery.projects] == [
        project_id
    ]

    database_path = Path(
        conn.execute("PRAGMA database_list").fetchone()["file"]
    )
    contender_conn = projects_db.connect(database_path)
    contender = module.ProjectRuntime(
        contender_conn,
        clock=lambda: now[0],
    )
    newest_lease = []
    original_write_transaction = prdb.write_transaction
    intercepted = False

    @contextlib.contextmanager
    def transition_before_begin(connection):
        nonlocal intercepted
        if connection is conn and not intercepted:
            intercepted = True
            prdb.write_transaction = original_write_transaction
            try:
                if transition == "renew":
                    newest_lease.append(
                        contender.renew_dispatcher_lease(
                            old_lease,
                            lease_seconds=50,
                        )
                    )
                else:
                    replacement = contender.acquire_dispatcher_lease(
                        "22222222-2222-4222-8222-222222222222",
                        lease_seconds=30,
                    )
                    assert replacement is not None
                    newest_lease.append(replacement)
            finally:
                prdb.write_transaction = transition_before_begin
        with original_write_transaction(connection):
            yield

    now[0] = 110 if transition == "renew" else 130
    monkeypatch.setattr(
        prdb,
        "write_transaction",
        transition_before_begin,
    )
    before = _runtime_mutation_snapshot(
        conn,
        project_id,
        turn.turn_id,
    )
    try:
        with pytest.raises(module.ProjectRuntimeError) as stale:
            with _assert_dispatcher_write_free(conn):
                runtime.claim_next_turn_for_dispatcher(
                    project_id,
                    "task7-c4-worker",
                    lease_seconds=30,
                    dispatcher_lease=old_lease,
                )
        assert (
            stale.value.code
            is module.RuntimeErrorCode.STALE_DISPATCHER_LEASE
        )
        assert intercepted
        assert len(newest_lease) == 1
        assert identity_counts.get("attempt", 0) == 0
        assert before == _runtime_mutation_snapshot(
            conn,
            project_id,
            turn.turn_id,
        )

        traced_statements.clear()
        conn.set_trace_callback(traced_statements.append)
        try:
            start = runtime.claim_next_turn_for_dispatcher(
                project_id,
                "task7-c4-worker",
                lease_seconds=30,
                dispatcher_lease=newest_lease[0],
            )
        finally:
            conn.set_trace_callback(None)
        from hermes_cli.project_runtime import WorkerStart

        assert type(start) is WorkerStart
        assert tuple(field.name for field in fields(WorkerStart)) == (
            "source",
            "claim",
            "operation",
            "dispatcher_lease",
        )
        assert start.source == "queued_turn"
        assert start.operation is None
        assert start.dispatcher_lease is newest_lease[0]
        assert start.claim.turn_id == turn.turn_id
        assert start.claim.attempt_id.startswith(
            "task7-c4-attempt-"
        )
        assert identity_counts["attempt"] == 1
        assert attempt_observations == [(True, True, True)]
        with pytest.raises(FrozenInstanceError):
            start.source = "approved_operation"

        with _assert_dispatcher_write_free(conn):
            assert runtime.claim_next_turn_for_dispatcher(
                project_id,
                "task7-c4-worker",
                lease_seconds=30,
                dispatcher_lease=newest_lease[0],
            ) is None
        assert identity_counts["attempt"] == 1
    finally:
        contender_conn.close()


def test_task7_c4_startauthority_queue_rechecks_expiry_after_lock_wait(
    runtime_env,
    monkeypatch,
):
    module = runtime_env["module"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime_env["runtime"].enqueue_turn(
        project_id,
        {"message": "Core expiry while waiting for write lock"},
        runtime_env["desktop"],
        idempotency_key="c4-queue-lock-expiry",
        expected_version=0,
    )
    now = [100]
    attempt_calls = []

    def identity(kind):
        if kind == "attempt":
            attempt_calls.append(kind)
            raise AssertionError(
                "attempt factory reached after Core expiry"
            )
        return f"task7-c4-{kind}"

    runtime = module.ProjectRuntime(
        conn,
        clock=lambda: now[0],
        id_factory=identity,
    )
    lease = runtime.acquire_dispatcher_lease(
        "11111111-1111-4111-8111-111111111111",
        lease_seconds=30,
    )
    assert lease is not None
    before_core = _dispatcher_lease_snapshot(conn)
    before_runtime = _runtime_mutation_snapshot(
        conn,
        project_id,
        turn.turn_id,
    )
    original_write_transaction = prdb.write_transaction
    crossed_boundary = False

    @contextlib.contextmanager
    def expire_before_begin(connection):
        nonlocal crossed_boundary
        if connection is conn and not crossed_boundary:
            crossed_boundary = True
            now[0] = lease.expires_at
        with original_write_transaction(connection):
            yield

    now[0] = lease.expires_at - 1
    monkeypatch.setattr(
        prdb,
        "write_transaction",
        expire_before_begin,
    )
    caught = None
    try:
        with _assert_dispatcher_write_free(conn):
            runtime.claim_next_turn_for_dispatcher(
                project_id,
                "task7-c4-worker",
                lease_seconds=30,
                dispatcher_lease=lease,
            )
    except Exception as exc:
        caught = exc

    assert crossed_boundary
    assert attempt_calls == []
    assert _dispatcher_lease_snapshot(conn) == before_core
    assert _runtime_mutation_snapshot(
        conn,
        project_id,
        turn.turn_id,
    ) == before_runtime
    assert type(caught) is module.ProjectRuntimeError
    assert (
        caught.code
        is module.RuntimeErrorCode.STALE_DISPATCHER_LEASE
    )


def test_task7_raw_epoch_runnable_pages_raw_members_before_eligibility(
    runtime_env,
):
    module = runtime_env["module"]
    cursor_type = module.RunnableProjectCursor
    conn = runtime_env["conn"]
    runtime = runtime_env["runtime"]
    first_project_id = runtime_env["project_id"]

    for ordinal in range(2, 101):
        _adopt_bound_project(
            conn,
            name=f"Raw member {ordinal}",
            root=f"raw-root-{ordinal}",
            binding=f"raw-binding-{ordinal}",
            external=f"raw-window-{ordinal}",
        )
    runnable_project_id, runnable_actor = _adopt_bound_project(
        conn,
        name="Raw member 101",
        root="raw-root-101",
        binding="raw-binding-101",
        external="raw-window-101",
    )
    runnable_turn = runtime.enqueue_turn(
        runnable_project_id,
        {"message": "page two"},
        runnable_actor,
        idempotency_key="raw-page-two",
        expected_version=0,
    )
    statements = []
    conn.set_trace_callback(statements.append)
    try:
        upper = runtime.runnable_project_membership_upper_watermark()
        assert upper == 101
        first = runtime.scan_runnable_projects(
            after=None,
            through_membership_sequence=upper,
            limit=100,
        )
    finally:
        conn.set_trace_callback(None)

    assert first.projects == ()
    assert first.scanned_through is not None
    assert first.scanned_through.dispatch_membership_sequence == 100
    assert first.reached_epoch_end is False
    raw_scan_sql = [
        statement
        for statement in statements
        if "from project_runtime_state indexed by "
        "idx_project_runtime_dispatch_membership"
        in " ".join(statement.lower().split())
    ]
    raw_statements = [
        " ".join(statement.lower().split())
        for statement in raw_scan_sql
    ]
    assert len(raw_statements) == 3
    assert sum(
        "order by dispatch_membership_sequence desc" in statement
        for statement in raw_statements
    ) == 1
    assert sum(
        "order by dispatch_membership_sequence, project_id limit 100"
        in statement
        for statement in raw_statements
    ) == 1
    assert sum(
        statement.startswith("select 1 ")
        for statement in raw_statements
    ) == 1
    assert all(
        "dispatch_membership_sequence is not null" in statement
        and "offset" not in statement
        and "project_turns" not in statement
        and "lifecycle" not in statement
        for statement in raw_statements
    )
    for sql in raw_scan_sql:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN " + sql,
        ).fetchall()
        details = " ".join(row["detail"] for row in plan)
        assert "idx_project_runtime_dispatch_membership" in details

    first_turn = runtime.enqueue_turn(
        first_project_id,
        {"message": "queued behind the cursor"},
        runtime_env["desktop"],
        idempotency_key="raw-later-queued",
        expected_version=0,
    )
    new_project_id, new_actor = _adopt_bound_project(
        conn,
        name="Raw member 102",
        root="raw-root-102",
        binding="raw-binding-102",
        external="raw-window-102",
    )
    new_turn = runtime.enqueue_turn(
        new_project_id,
        {"message": "new epoch only"},
        new_actor,
        idempotency_key="raw-new-member",
        expected_version=0,
    )

    second = runtime.scan_runnable_projects(
        after=first.scanned_through,
        through_membership_sequence=upper,
        limit=100,
    )
    assert second.projects == (
        module.RunnableProject(
            runnable_project_id,
            runnable_turn.turn_id,
            runnable_turn.sequence,
            101,
        ),
    )
    assert second.scanned_through == cursor_type(
        101, runnable_project_id
    )
    assert second.reached_epoch_end is True

    fresh_upper = runtime.runnable_project_membership_upper_watermark()
    assert fresh_upper == 102
    fresh_first = runtime.scan_runnable_projects(
        after=None,
        through_membership_sequence=fresh_upper,
        limit=100,
    )
    assert [project.project_id for project in fresh_first.projects] == [
        first_project_id
    ]
    assert fresh_first.projects[0].head_turn_id == first_turn.turn_id
    conn.execute(
        """
        UPDATE project_run_controls
        SET control_state = 'stopped'
        WHERE project_id = ? AND turn_id = ?
        """,
        (new_project_id, new_turn.turn_id),
    )
    conn.commit()
    corrupted_control = tuple(
        conn.execute(
            """
            SELECT * FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (new_project_id, new_turn.turn_id),
        ).fetchone()
    )
    with pytest.raises(
        RuntimeError,
        match="runtime turn/control/lease pair is inconsistent",
    ):
        with _assert_dispatcher_write_free(conn):
            runtime.scan_runnable_projects(
                after=fresh_first.scanned_through,
                through_membership_sequence=fresh_upper,
                limit=100,
            )
    assert tuple(
        conn.execute(
            """
            SELECT * FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (new_project_id, new_turn.turn_id),
        ).fetchone()
    ) == corrupted_control
    conn.execute(
        """
        UPDATE project_run_controls
        SET control_state = 'running'
        WHERE project_id = ? AND turn_id = ?
        """,
        (new_project_id, new_turn.turn_id),
    )
    conn.commit()
    fresh_second = runtime.scan_runnable_projects(
        after=fresh_first.scanned_through,
        through_membership_sequence=fresh_upper,
        limit=100,
    )
    assert [project.project_id for project in fresh_second.projects] == [
        runnable_project_id,
        new_project_id,
    ]
    assert fresh_second.projects[-1].head_turn_id == new_turn.turn_id
    assert fresh_second.reached_epoch_end is True

    invalid_calls = (
        lambda: runtime.scan_runnable_projects(
            after=None,
            through_membership_sequence=upper,
            limit=True,
        ),
        lambda: runtime.scan_runnable_projects(
            after=None,
            through_membership_sequence=upper,
            limit=0,
        ),
        lambda: runtime.scan_runnable_projects(
            after=None,
            through_membership_sequence=upper,
            limit=101,
        ),
        lambda: runtime.scan_runnable_projects(
            after=cursor_type(upper + 1, "beyond"),
            through_membership_sequence=upper,
            limit=1,
        ),
    )
    for invalid_call in invalid_calls:
        invalid_statements = []
        conn.set_trace_callback(invalid_statements.append)
        try:
            with pytest.raises(module.ProjectRuntimeError) as error:
                invalid_call()
        finally:
            conn.set_trace_callback(None)
        assert error.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert invalid_statements == []


def test_claim_and_controls_expose_exact_creation_and_lease_identity(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "identity"}, runtime_env["desktop"],
        idempotency_key="identity", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    stopped = runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )

    assert (turn.created_at, turn.updated_at) == (100, 100)
    assert claim.lease_expires_at == 130
    assert stopped.last_idempotency_key == "stop"
    assert stopped.updated_at == 100


def test_enqueue_returns_a_canonical_queued_turn_for_an_exact_owner_binding(runtime_env):
    runtime = runtime_env["runtime"]
    turn = runtime.enqueue_turn(
        runtime_env["project_id"],
        {"message": "first", "client_timestamp": 999999},
        runtime_env["desktop"],
        idempotency_key="enqueue-1",
        expected_version=0,
    )

    assert turn.sequence == 1
    assert turn.status == "queued"
    assert turn.origin_binding_id == "desktop-owner"
    assert turn.payload == {"client_timestamp": 999999, "message": "first"}
    control = prdb._runtime_control_for_turn(
        runtime_env["conn"], project_id=runtime_env["project_id"], turn_id=turn.turn_id
    )
    assert control.control_state == "running"
    assert control.control_version == 0
    assert turn.created_at == 100
    assert turn.updated_at == 100


def test_enqueue_is_canonical_idempotent_and_rejects_changed_origin_or_payload(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    first = runtime.enqueue_turn(
        project_id,
        {"b": [1, {"a": True}], "a": None},
        runtime_env["desktop"],
        idempotency_key="same-key",
        expected_version=0,
    )
    replay = runtime.enqueue_turn(
        project_id,
        {"a": None, "b": [1, {"a": True}]},
        runtime_env["desktop"],
        idempotency_key="same-key",
        expected_version=999,
    )
    assert replay == first
    assert prdb.runtime_state_for_project(runtime_env["conn"], project_id).version == 1
    assert _event_count(runtime_env["conn"], project_id) == 1
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as payload_conflict:
        runtime.enqueue_turn(
            project_id,
            {"a": "changed"},
            runtime_env["desktop"],
            idempotency_key="same-key",
            expected_version=1,
        )
    assert payload_conflict.value.code is runtime_env["module"].RuntimeErrorCode.IDEMPOTENCY_CONFLICT
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as origin_conflict:
        runtime.enqueue_turn(
            project_id,
            {"a": None, "b": [1, {"a": True}]},
            runtime_env["discord"],
            idempotency_key="same-key",
            expected_version=1,
        )
    assert origin_conflict.value.code is runtime_env["module"].RuntimeErrorCode.IDEMPOTENCY_CONFLICT


def test_fifo_sequences_are_project_scoped_and_ignore_client_timestamps(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turns = [
        runtime.enqueue_turn(project_id, {"client_timestamp": timestamp}, runtime_env["desktop"], idempotency_key=f"time-{timestamp}", expected_version=index)
        for index, timestamp in enumerate((300, 100, 200))
    ]
    other_id, other_actor = _adopt_bound_project(
        runtime_env["conn"], name="Other", root="other-root",
        binding="other-binding", external="other-window",
    )
    other = runtime.enqueue_turn(other_id, {"client_timestamp": 1}, other_actor, idempotency_key="other", expected_version=0)
    assert [turn.sequence for turn in turns] == [1, 2, 3]
    assert other.sequence == 1
    assert [turn.sequence for turn in runtime.list_queue(project_id, runtime_env["desktop"])] == [1, 2, 3]


def test_queue_reads_require_the_exact_durable_owner_binding(runtime_env):
    runtime = runtime_env["runtime"]
    runtime.enqueue_turn(runtime_env["project_id"], {"x": 1}, runtime_env["desktop"], idempotency_key="queued", expected_version=0)
    forged = ActorContext("owner-1", "discord", "desktop-owner", True)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as denied:
        runtime.list_queue(runtime_env["project_id"], forged)
    assert denied.value.code is runtime_env["module"].RuntimeErrorCode.ACTOR_NOT_AUTHORIZED


@pytest.mark.parametrize(
    "payload",
    [
        {"number": math.nan},
        {"number": math.inf},
        {1: "not-a-string-key"},
        {"tuple": ("not", "json")},
        {"set": {"not", "json"}},
    ],
)
def test_enqueue_rejects_noncanonical_json_before_writing(runtime_env, payload):
    runtime = runtime_env["runtime"]
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as error:
        runtime.enqueue_turn(
            runtime_env["project_id"], payload, runtime_env["desktop"],
            idempotency_key="invalid-json", expected_version=0,
        )
    assert error.value.code is runtime_env["module"].RuntimeErrorCode.INVALID_ARGUMENT
    assert _event_count(runtime_env["conn"], runtime_env["project_id"]) == 0


def test_enqueue_rejects_a_cyclic_json_object_before_sql(runtime_env):
    payload = {"cycle": []}
    payload["cycle"].append(payload)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as error:
        runtime_env["runtime"].enqueue_turn(
            runtime_env["project_id"], payload, runtime_env["desktop"],
            idempotency_key="cycle", expected_version=0,
        )
    assert error.value.code is runtime_env["module"].RuntimeErrorCode.INVALID_ARGUMENT
    assert _event_count(runtime_env["conn"], runtime_env["project_id"]) == 0


def test_project_fifo_claim_cancel_and_cross_project_isolation(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    first = runtime.enqueue_turn(project_id, {"order": 3}, runtime_env["desktop"], idempotency_key="q-3", expected_version=0)
    second = runtime.enqueue_turn(project_id, {"order": 1}, runtime_env["desktop"], idempotency_key="q-1", expected_version=1)
    queued = runtime.list_queue(project_id, runtime_env["desktop"])
    assert [turn.turn_id for turn in queued] == [first.turn_id, second.turn_id]
    claim = runtime.claim_next_turn(project_id, "worker-one", lease_seconds=30)
    assert claim is not None and claim.turn_id == first.turn_id and claim.sequence == 1
    assert runtime.claim_next_turn(project_id, "worker-two", lease_seconds=30) is None
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as not_queued:
        runtime.cancel_queued_turn(
            project_id,
            first.turn_id,
            runtime_env["desktop"],
            idempotency_key="cancel-first",
            expected_version=3,
            expected_control_version=1,
        )
    assert not_queued.value.code is runtime_env["module"].RuntimeErrorCode.TURN_NOT_QUEUED
    cancelled = runtime.cancel_queued_turn(
        project_id,
        second.turn_id,
        runtime_env["desktop"],
        idempotency_key="cancel-second",
        expected_version=3,
        expected_control_version=0,
    )
    assert cancelled.status == "cancelled"
    assert prdb._runtime_control_for_turn(runtime_env["conn"], project_id=project_id, turn_id=second.turn_id).control_state == "terminal"


def test_cancel_is_exactly_replayable_and_binds_both_cas_versions(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "cancel"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )

    first = runtime.cancel_queued_turn(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="cancel", expected_version=1,
        expected_control_version=0,
    )
    snapshot = (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE project_id = ? AND turn_id = ?",
            (project_id, turn.turn_id),
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE project_id = ? AND turn_id = ?",
            (project_id, turn.turn_id),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )
    replay = runtime.cancel_queued_turn(
        project_id, turn.turn_id, runtime_env["discord"],
        idempotency_key="cancel", expected_version=1,
        expected_control_version=0,
    )

    assert replay == first
    assert snapshot == (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE project_id = ? AND turn_id = ?",
            (project_id, turn.turn_id),
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE project_id = ? AND turn_id = ?",
            (project_id, turn.turn_id),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as conflict:
        runtime.cancel_queued_turn(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="cancel", expected_version=2,
            expected_control_version=1,
        )
    assert conflict.value.code is runtime_env["module"].RuntimeErrorCode.IDEMPOTENCY_CONFLICT


def test_cancel_rejects_control_split_brain_without_mutation(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "cancel"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    conn.execute(
        """UPDATE project_run_controls SET control_state = 'stopped'
           WHERE project_id = ? AND turn_id = ?""",
        (project_id, turn.turn_id),
    )
    conn.commit()
    before = (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises((runtime_env["module"].ProjectRuntimeError, RuntimeError)):
        runtime.cancel_queued_turn(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="cancel", expected_version=1,
            expected_control_version=0,
        )

    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "queued"
    assert before == (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


def test_stop_ack_resume_preserves_logical_turn_and_rotates_next_claim(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"message": "work"}, runtime_env["desktop"], idempotency_key="enqueue", expected_version=0)
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    stopped = runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"], idempotency_key="stop", expected_version=2, expected_control_version=1,
    )
    assert stopped.control_state == "stop_requested"
    replay = runtime.request_stop(
        project_id, turn.turn_id, runtime_env["discord"], idempotency_key="stop", expected_version=2, expected_control_version=1,
    )
    assert replay == stopped
    assert _event_count(runtime_env["conn"], project_id) == 3
    acknowledged = runtime.acknowledge_stopped(claim)
    assert acknowledged.control_state == "stopped"
    resumed = runtime.request_resume(
        project_id, turn.turn_id, runtime_env["discord"], idempotency_key="resume", expected_version=4, expected_control_version=3,
    )
    assert resumed.control_state == "resume_requested"
    same_turn = prdb._runtime_turn_for_project(
        runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id
    )
    assert same_turn.sequence == turn.sequence and same_turn.attempt_id == claim.attempt_id
    renewed = runtime.claim_next_turn(project_id, "worker-two", lease_seconds=30)
    assert renewed.turn_id == turn.turn_id
    assert renewed.attempt_id != claim.attempt_id
    assert renewed.lease_generation == claim.lease_generation + 1
    assert renewed.fencing_token == claim.fencing_token + 1


@pytest.mark.parametrize(
    "corruption_sql",
    [
        pytest.param(
            "DELETE FROM project_worker_leases WHERE project_id = ? AND turn_id = ?",
            id="missing-lease",
        ),
        pytest.param(
            """UPDATE project_run_controls SET attempt_id = 'forged-attempt'
               WHERE project_id = ? AND turn_id = ?""",
            id="control-attempt",
        ),
        pytest.param(
            """UPDATE project_worker_leases SET worker_id = 'forged-worker'
               WHERE project_id = ? AND turn_id = ?""",
            id="lease-worker",
        ),
    ],
)
def test_stop_requires_one_structurally_complete_current_claim(
    runtime_env, corruption_sql
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    conn.execute(corruption_sql, (project_id, turn.turn_id))
    conn.commit()
    before = (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises((runtime_env["module"].ProjectRuntimeError, RuntimeError)):
        runtime.request_stop(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="stop", expected_version=2,
            expected_control_version=1,
        )

    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "claimed"
    assert before == (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


@pytest.mark.parametrize(
    "field_name",
    [
        "sequence",
        "worker_id",
        "canonical_session_id",
        "lease_expires_at",
    ],
)
def test_stopped_acknowledgement_rejects_each_forged_claim_field(
    runtime_env, field_name
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )
    forged_value = (
        "forged"
        if field_name in {"worker_id", "canonical_session_id"}
        else getattr(claim, field_name) + 1
    )
    forged = replace(claim, **{field_name: forged_value})

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.acknowledge_stopped(forged)

    assert (
        rejected.value.code
        is runtime_env["module"].RuntimeErrorCode.TURN_NOT_STOP_REQUESTED
    )
    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "stop_requested"
    assert prdb.runtime_state_for_project(conn, project_id).version == 3
    assert _event_count(conn, project_id) == 3


def test_stopped_acknowledgement_rejects_a_split_brain_control_attempt(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )
    conn.execute(
        """UPDATE project_run_controls SET attempt_id = 'forged-attempt'
           WHERE project_id = ? AND turn_id = ?""",
        (project_id, turn.turn_id),
    )
    conn.commit()

    with pytest.raises(runtime_env["module"].ProjectRuntimeError):
        runtime.acknowledge_stopped(claim)

    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "stop_requested"
    assert prdb.runtime_state_for_project(conn, project_id).version == 3
    assert _event_count(conn, project_id) == 3


def test_stopped_acknowledgement_exact_replay_uses_durable_full_claim_identity(
    runtime_env,
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )
    first = runtime.acknowledge_stopped(claim)
    assert tuple(conn.execute(
        """SELECT claim_worker_id, claim_lease_expires_at,
                  claim_canonical_session_id
           FROM project_run_controls
           WHERE project_id = ? AND turn_id = ?""",
        (project_id, turn.turn_id),
    ).fetchone()) == (
        claim.worker_id,
        claim.lease_expires_at,
        claim.canonical_session_id,
    )
    assert conn.execute(
        """SELECT 1 FROM project_worker_leases
           WHERE project_id = ? AND turn_id = ?""",
        (project_id, turn.turn_id),
    ).fetchone() is None
    snapshot = (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    for field_name in (
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    ):
        value = getattr(claim, field_name)
        forged_value = f"{value}-forged" if type(value) is str else value + 1
        with pytest.raises(runtime_env["module"].ProjectRuntimeError):
            runtime.acknowledge_stopped(
                replace(claim, **{field_name: forged_value})
            )

    replay = runtime.acknowledge_stopped(claim)
    assert replay == first
    assert snapshot == (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


@pytest.mark.parametrize(
    "corruption_sql",
    [
        pytest.param(
            """UPDATE project_turns SET status = 'illegal'
               WHERE project_id = ? AND sequence = 1""",
            id="illegal-oldest-turn",
        ),
        pytest.param(
            """UPDATE project_turns SET status = 'cancelled'
               WHERE project_id = ? AND sequence = 1;
               UPDATE project_run_controls SET control_state = 'illegal'
               WHERE project_id = ? AND turn_id = (
                   SELECT turn_id FROM project_turns
                   WHERE project_id = ? AND sequence = 1
               )""",
            id="illegal-terminal-control",
        ),
    ],
)
def test_claim_fails_closed_on_any_corrupt_row_before_a_later_queued_turn(
    runtime_env, corruption_sql
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    first = runtime.enqueue_turn(
        project_id, {"order": 1}, runtime_env["desktop"],
        idempotency_key="first", expected_version=0,
    )
    second = runtime.enqueue_turn(
        project_id, {"order": 2}, runtime_env["desktop"],
        idempotency_key="second", expected_version=1,
    )
    if ";" in corruption_sql:
        statements = [part.strip() for part in corruption_sql.split(";")]
        conn.execute(statements[0], (project_id,))
        conn.execute(statements[1], (project_id, project_id))
    else:
        conn.execute(corruption_sql, (project_id,))
    conn.commit()
    before = (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises(RuntimeError):
        runtime.claim_next_turn(project_id, "worker", lease_seconds=30)

    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (second.turn_id,)
    ).fetchone()[0] == "queued"
    assert conn.execute(
        """SELECT COUNT(*) FROM project_worker_leases
           WHERE project_id = ?""",
        (project_id,),
    ).fetchone()[0] == 0
    assert before == (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )
    assert first.turn_id != second.turn_id


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed", "cancelled"])
def test_resume_rejects_every_terminal_status_without_mutation(
    runtime_env, terminal_status
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "terminal"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    conn.execute(
        "UPDATE project_turns SET status = ? WHERE turn_id = ?",
        (terminal_status, turn.turn_id),
    )
    conn.execute(
        """UPDATE project_run_controls SET control_state = 'terminal'
           WHERE turn_id = ?""",
        (turn.turn_id,),
    )
    conn.commit()
    before = (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.request_resume(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="resume", expected_version=1,
            expected_control_version=0,
        )

    assert rejected.value.code in {
        runtime_env["module"].RuntimeErrorCode.TURN_TERMINAL,
        runtime_env["module"].RuntimeErrorCode.TURN_NOT_RESUMABLE,
    }
    assert before == (
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


@pytest.mark.parametrize(
    "inactive_lifecycle",
    ["awaiting_acceptance", "completed", "archived"],
)
def test_resume_requires_an_active_project_lifecycle_without_mutation(
    runtime_env, inactive_lifecycle
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "stop"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(
        project_id, turn.turn_id, runtime_env["desktop"],
        idempotency_key="stop", expected_version=2,
        expected_control_version=1,
    )
    runtime.acknowledge_stopped(claim)
    conn.execute("PRAGMA ignore_check_constraints=ON")
    conn.execute(
        """UPDATE project_runtime_state
           SET lifecycle = ?, version = version + 1, updated_at = 101
           WHERE project_id = ?""",
        (inactive_lifecycle, project_id),
    )
    conn.execute("PRAGMA ignore_check_constraints=OFF")
    conn.commit()
    before = (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.request_resume(
            project_id, turn.turn_id, runtime_env["desktop"],
            idempotency_key="resume", expected_version=5,
            expected_control_version=3,
        )

    assert rejected.value.code in {
        runtime_env["module"].RuntimeErrorCode.PROJECT_NOT_ACTIVE,
        runtime_env["module"].RuntimeErrorCode.PROJECT_LINEAGE_INVALID,
    }
    assert before == (
        tuple(conn.execute(
            "SELECT * FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
        ).fetchone()),
        tuple(conn.execute(
            "SELECT * FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()),
        prdb.runtime_state_for_project(conn, project_id),
        _event_count(conn, project_id),
    )


def test_approval_request_is_atomic_nonwaiting_and_blocks_the_fifo_head(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"message": "approve"}, runtime_env["desktop"], idempotency_key="approval-turn", expected_version=0)
    runtime.enqueue_turn(project_id, {"message": "later"}, runtime_env["desktop"], idempotency_key="later-turn", expected_version=1)
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = prdb.ApprovalRequest(
        approval_id="approval-1", project_id=project_id,
        requester_actor_id="owner-1", authorization_actor_id="owner-1",
        canonical_action="publish", approval_class="publish", command_revision=1,
        expected_runtime_version=3, expected_lifecycle="active",
        expected_phase="implementation", targets=("C:/work/runtime/release",),
        batch_id="batch-1", batch_items=("release",), status="pending",
        expires_at=1000,
    )
    result = runtime.request_turn_approval(
        turn.turn_id,
        request,
        runtime_env["desktop"],
        expected_control_version=1,
    )
    assert result.turn_id == turn.turn_id
    assert result.approval.approval_id == request.approval_id
    assert result.approval.targets == ("c:/work/runtime/release",)
    assert prdb._runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).status == "awaiting_approval"
    assert runtime.claim_next_turn(project_id, "other-worker", lease_seconds=30) is None
    assert runtime_env["conn"].execute("SELECT turn_id FROM project_approvals WHERE approval_id = 'approval-1'").fetchone()[0] == turn.turn_id
    assert runtime_env["conn"].in_transaction is False


def test_turn_approval_keeps_preversion_identity_and_is_immediately_resolvable_and_consumable(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = _approval_request(project_id)

    result = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["desktop"],
        expected_control_version=1,
    )
    snapshot = conn.execute(
        """SELECT expected_runtime_version, effective_runtime_version,
                  turn_expected_control_version
           FROM project_approvals WHERE approval_id = ?""",
        (request.approval_id,),
    ).fetchone()

    assert result.approval.expected_runtime_version == 2
    assert tuple(snapshot) == (2, 3, 1)
    assert prdb.runtime_state_for_project(conn, project_id).version == 3
    resolved = prdb.resolve_approval(
        conn, approval_id=request.approval_id,
        resolver=runtime_env["desktop"], outcome="approved", now=101,
    )
    assert resolved is not None and resolved.status == "approved"
    assert prdb.consume_approval_authorization(
        conn,
        approval_id=request.approval_id,
        project_id=project_id,
        authorization_actor_id="owner-1",
        canonical_action="publish",
        approval_class="publish",
        command_revision=1,
        expected_runtime_version=2,
        expected_lifecycle="active",
        expected_phase="implementation",
        targets=("C:/work/runtime/release",),
        batch_id="batch",
        batch_items=("release",),
        now=102,
    )


def test_exact_turn_approval_replays_after_lifecycle_drift_resolution_and_expiry_without_writes(runtime_env):
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    now = [100]
    runtime = runtime_env["module"].ProjectRuntime(conn, clock=lambda: now[0])
    turn = runtime.enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = _approval_request(project_id)
    first = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["desktop"],
        expected_control_version=1,
    )
    resolved = prdb.resolve_approval(
        conn, approval_id=request.approval_id,
        resolver=runtime_env["desktop"], outcome="approved", now=101,
    )
    assert resolved is not None
    drifted = prdb.transition_lifecycle(
        conn,
        project_id=project_id,
        expected_version=3,
        lifecycle="awaiting_acceptance",
        updated_at=102,
    )
    assert drifted is not None
    conn.commit()
    before = _runtime_mutation_snapshot(conn, project_id, turn.turn_id)
    now[0] = 2000

    replay = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["discord"],
        expected_control_version=1,
    )

    assert first.turn_id == replay.turn_id
    assert replay.approval.status == "approved"
    assert before == _runtime_mutation_snapshot(conn, project_id, turn.turn_id)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as changed_cas:
        runtime.request_turn_approval(
            turn.turn_id,
            request,
            runtime_env["desktop"],
            expected_control_version=99,
        )
    assert (
        changed_cas.value.code
        is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT
    )
    assert before == _runtime_mutation_snapshot(conn, project_id, turn.turn_id)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as changed:
        runtime.request_turn_approval(
            turn.turn_id,
            replace(request, batch_id="changed"),
            runtime_env["desktop"],
            expected_control_version=1,
        )
    assert changed.value.code is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as wrong_turn:
        runtime.request_turn_approval(
            "another-turn", request, runtime_env["desktop"],
            expected_control_version=1,
        )
    assert wrong_turn.value.code is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT


@pytest.mark.parametrize(
    ("lifecycle", "transitions"),
    [
        pytest.param(
            "awaiting_acceptance",
            ("awaiting_acceptance",),
            id="awaiting-acceptance",
        ),
        pytest.param(
            "completed",
            ("awaiting_acceptance", "completed"),
            id="completed",
        ),
    ],
)
def test_fresh_turn_approval_requires_an_active_project_without_writes(
    runtime_env, lifecycle, transitions
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "approve"},
        runtime_env["desktop"],
        idempotency_key="enqueue",
        expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    state = prdb.runtime_state_for_project(conn, project_id)
    assert state is not None
    for target in transitions:
        state = prdb.transition_lifecycle(
            conn,
            project_id=project_id,
            expected_version=state.version,
            lifecycle=target,
            updated_at=99,
        )
        assert state is not None
    conn.commit()
    request = _approval_request(
        project_id,
        expected_runtime_version=state.version,
        expected_lifecycle=lifecycle,
    )
    before = _runtime_mutation_snapshot(conn, project_id, turn.turn_id)

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.request_turn_approval(
            turn.turn_id,
            request,
            runtime_env["desktop"],
            expected_control_version=1,
        )

    assert (
        rejected.value.code
        is runtime_env["module"].RuntimeErrorCode.PROJECT_NOT_ACTIVE
    )
    assert before == _runtime_mutation_snapshot(conn, project_id, turn.turn_id)


def test_migrated_linked_turn_approval_without_control_cas_fails_closed(
    runtime_env,
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "approve"},
        runtime_env["desktop"],
        idempotency_key="enqueue",
        expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = _approval_request(project_id)
    runtime.request_turn_approval(
        turn.turn_id,
        request,
        runtime_env["desktop"],
        expected_control_version=1,
    )
    conn.execute(
        """
        UPDATE project_approvals
        SET turn_expected_control_version = NULL
        WHERE approval_id = ?
        """,
        (request.approval_id,),
    )
    conn.commit()
    before = _runtime_mutation_snapshot(conn, project_id, turn.turn_id)

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.request_turn_approval(
            turn.turn_id,
            request,
            runtime_env["discord"],
            expected_control_version=1,
        )

    assert (
        rejected.value.code
        is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT
    )
    assert before == _runtime_mutation_snapshot(conn, project_id, turn.turn_id)


@pytest.mark.parametrize(
    "corruption_sql",
    [
        pytest.param(
            "DELETE FROM project_worker_leases WHERE project_id = ? AND turn_id = ?",
            id="missing-lease",
        ),
        pytest.param(
            """UPDATE project_run_controls SET attempt_id = 'forged-attempt'
               WHERE project_id = ? AND turn_id = ?""",
            id="control-attempt",
        ),
        pytest.param(
            """UPDATE project_worker_leases SET fencing_token = fencing_token + 1
               WHERE project_id = ? AND turn_id = ?""",
            id="lease-fence",
        ),
    ],
)
def test_turn_approval_requires_the_exact_current_claim_triple(
    runtime_env, corruption_sql
):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    conn.execute(corruption_sql, (project_id, turn.turn_id))
    conn.commit()

    with pytest.raises((runtime_env["module"].ProjectRuntimeError, RuntimeError)):
        runtime.request_turn_approval(
            turn.turn_id,
            _approval_request(project_id),
            runtime_env["desktop"],
            expected_control_version=1,
        )

    assert conn.execute(
        "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "claimed"
    assert prdb.runtime_state_for_project(conn, project_id).version == 2
    assert _event_count(conn, project_id) == 2


def test_turn_approval_rejects_wrong_preversion_and_control_version(runtime_env):
    runtime = runtime_env["runtime"]
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)

    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as wrong_state:
        runtime.request_turn_approval(
            turn.turn_id,
            _approval_request(project_id, expected_runtime_version=1),
            runtime_env["desktop"],
            expected_control_version=1,
        )
    assert wrong_state.value.code is runtime_env["module"].RuntimeErrorCode.APPROVAL_CONFLICT
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as wrong_control:
        runtime.request_turn_approval(
            turn.turn_id,
            _approval_request(project_id),
            runtime_env["desktop"],
            expected_control_version=0,
        )
    assert wrong_control.value.code is runtime_env["module"].RuntimeErrorCode.CONTROL_VERSION_CONFLICT
    assert conn.execute(
        "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == 0


def test_turn_approval_event_conflict_rolls_back_approval_link_and_version(runtime_env):
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    turn = runtime_env["runtime"].enqueue_turn(
        project_id, {"message": "approve"}, runtime_env["desktop"],
        idempotency_key="enqueue", expected_version=0,
    )
    runtime_env["runtime"].claim_next_turn(project_id, "worker", lease_seconds=30)
    conn.execute(
        """INSERT INTO project_events (
            event_id, project_id, sequence, kind, turn_id, payload_json, created_at
        ) VALUES ('approval-event-conflict', ?, 3, 'prior', NULL, '{}', 99)""",
        (project_id,),
    )
    conn.commit()
    runtime = runtime_env["module"].ProjectRuntime(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: (
            "approval-event-conflict" if kind == "event" else f"{kind}-fixed"
        ),
    )

    with pytest.raises(sqlite3.IntegrityError):
        runtime.request_turn_approval(
            turn.turn_id,
            _approval_request(project_id),
            runtime_env["desktop"],
            expected_control_version=1,
        )

    assert conn.execute(
        "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT status FROM project_turns WHERE turn_id = ?", (turn.turn_id,)
    ).fetchone()[0] == "claimed"
    assert prdb.runtime_state_for_project(conn, project_id).version == 2


def test_stop_crash_state_is_durable_and_recovery_blocked_resume_fails_closed(runtime_env, tmp_path):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"message": "stop"}, runtime_env["desktop"], idempotency_key="stop-turn", expected_version=0)
    runtime.enqueue_turn(project_id, {"message": "later"}, runtime_env["desktop"], idempotency_key="later-turn", expected_version=1)
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(project_id, turn.turn_id, runtime_env["desktop"], idempotency_key="stop", expected_version=3, expected_control_version=1)
    runtime_env["conn"].execute(
        """INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key, approval_id,
            command_revision, targets_json, payload_json, status, receipt_json,
            created_at, updated_at
        ) VALUES ('op-1', ?, ?, 'op-key', NULL, 1, '[]', '{}', 'unknown', NULL, 1, 1)""",
        (project_id, turn.turn_id),
    )
    runtime_env["conn"].commit()
    claim = runtime.claim_next_turn(project_id, "other", lease_seconds=30)
    assert claim is None
    original_claim = runtime_env["module"].TurnClaim(
        turn_id=turn.turn_id, project_id=project_id, sequence=1,
        lease_generation=1, fencing_token=1, canonical_session_id="session-root",
        attempt_id=prdb._runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).attempt_id,
        worker_id="worker",
        lease_expires_at=130,
    )
    runtime.acknowledge_stopped(original_claim)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as blocked:
        runtime.request_resume(project_id, turn.turn_id, runtime_env["desktop"], idempotency_key="resume", expected_version=5, expected_control_version=3)
    assert blocked.value.code is runtime_env["module"].RuntimeErrorCode.TURN_RECOVERY_BLOCKED


def test_forged_actor_and_boolean_cas_are_rejected_without_runtime_writes(runtime_env):
    runtime = runtime_env["runtime"]
    forged = ActorContext("owner-1", "desktop", "desktop-owner", False)
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as unauthorized:
        runtime.enqueue_turn(runtime_env["project_id"], {"x": 1}, forged, idempotency_key="x", expected_version=0)
    assert unauthorized.value.code is runtime_env["module"].RuntimeErrorCode.ACTOR_NOT_AUTHORIZED
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as invalid:
        runtime.enqueue_turn(runtime_env["project_id"], {"x": 1}, runtime_env["desktop"], idempotency_key="x", expected_version=True)
    assert invalid.value.code is runtime_env["module"].RuntimeErrorCode.INVALID_ARGUMENT
    assert _event_count(runtime_env["conn"], runtime_env["project_id"]) == 0


def test_exact_approval_retry_uses_canonical_immutable_request_identity(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"message": "approve"}, runtime_env["desktop"], idempotency_key="queued", expected_version=0)
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    request = prdb.ApprovalRequest(
        approval_id="approval-retry", project_id=project_id,
        requester_actor_id="owner-1", authorization_actor_id="owner-1",
        canonical_action="publish", approval_class="publish", command_revision=1,
        expected_runtime_version=2, expected_lifecycle="active", expected_phase="implementation",
        targets=("C:/work/runtime/release",), batch_id="batch", batch_items=("release",),
        status="pending", expires_at=1000,
    )
    first = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["desktop"], expected_control_version=1
    )
    replay = runtime.request_turn_approval(
        turn.turn_id, request, runtime_env["discord"], expected_control_version=1
    )
    assert replay == first
    assert _event_count(runtime_env["conn"], project_id) == 3


def test_event_conflict_rolls_back_turn_control_and_runtime_version(runtime_env):
    conn = runtime_env["conn"]
    project_id = runtime_env["project_id"]
    conn.execute(
        """INSERT INTO project_events (
            event_id, project_id, sequence, kind, turn_id, payload_json, created_at
        ) VALUES ('fixed-event', ?, 1, 'prior', NULL, '{}', 1)""",
        (project_id,),
    )
    conn.commit()
    module = runtime_env["module"]
    runtime = module.ProjectRuntime(
        conn, clock=lambda: 100,
        id_factory=lambda kind: "fixed-event" if kind == "event" else "fixed-turn",
    )
    with pytest.raises(sqlite3.IntegrityError):
        runtime.enqueue_turn(project_id, {"message": "must-roll-back"}, runtime_env["desktop"], idempotency_key="rollback", expected_version=0)
    assert prdb.runtime_state_for_project(conn, project_id).version == 0
    assert conn.execute("SELECT COUNT(*) FROM project_turns WHERE project_id = ?", (project_id,)).fetchone()[0] == 0
    assert _event_count(conn, project_id) == 1


def test_frozen_payload_and_stale_worker_acknowledgement_are_fail_closed(runtime_env):
    runtime = runtime_env["runtime"]
    project_id = runtime_env["project_id"]
    turn = runtime.enqueue_turn(project_id, {"outer": {"items": [1, 2]}}, runtime_env["desktop"], idempotency_key="freeze", expected_version=0)
    with pytest.raises(TypeError):
        turn.payload["new"] = "nope"
    assert turn.payload["outer"]["items"] == (1, 2)
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(project_id, turn.turn_id, runtime_env["desktop"], idempotency_key="stop", expected_version=2, expected_control_version=1)
    forged = runtime_env["module"].TurnClaim(
        turn_id=claim.turn_id, project_id=claim.project_id, sequence=claim.sequence,
        lease_generation=claim.lease_generation, fencing_token=claim.fencing_token,
        canonical_session_id=claim.canonical_session_id, attempt_id=claim.attempt_id,
        worker_id="forged-worker",
        lease_expires_at=claim.lease_expires_at,
    )
    with pytest.raises(runtime_env["module"].ProjectRuntimeError) as rejected:
        runtime.acknowledge_stopped(forged)
    assert rejected.value.code is runtime_env["module"].RuntimeErrorCode.TURN_NOT_STOP_REQUESTED
    assert prdb._runtime_turn_for_project(runtime_env["conn"], project_id=project_id, turn_id=turn.turn_id).status == "stop_requested"


def test_two_file_backed_connections_claim_one_oldest_turn_in_25_races(tmp_path):
    module = importlib.import_module("hermes_cli.project_runtime")
    path = tmp_path / "race.db"

    def persisted_project_snapshot(conn, project_id):
        queries = (
            """SELECT * FROM project_runtime_state
               WHERE project_id = ? ORDER BY project_id""",
            """SELECT * FROM project_turns
               WHERE project_id = ? ORDER BY sequence, turn_id""",
            """SELECT * FROM project_run_controls
               WHERE project_id = ? ORDER BY turn_id""",
            """SELECT * FROM project_events
               WHERE project_id = ? ORDER BY sequence, event_id""",
            """SELECT * FROM project_worker_leases
               WHERE project_id = ? ORDER BY turn_id, lease_id""",
        )
        return tuple(
            tuple(tuple(row) for row in conn.execute(query, (project_id,)))
            for query in queries
        )

    unrelated_conn = projects_db.connect(path)
    unrelated_id, unrelated_owner = _adopt_bound_project(
        unrelated_conn,
        name="Unrelated race isolation",
        root="unrelated-root",
        binding="unrelated-binding",
        external="unrelated-window",
    )
    module.ProjectRuntime(unrelated_conn, clock=lambda: 50).enqueue_turn(
        unrelated_id,
        {"scope": "must-not-change"},
        unrelated_owner,
        idempotency_key="unrelated-turn",
        expected_version=0,
    )
    unrelated_baseline = persisted_project_snapshot(
        unrelated_conn, unrelated_id
    )
    unrelated_conn.close()

    for index in range(25):
        bootstrap = projects_db.connect(path)
        project_id = projects_db.create_project(bootstrap, name=f"Race {index}")
        prdb.create_project_conversation(bootstrap, project_id=project_id, conversation_id=f"root-{index}", current_phase="implementation", now=1)
        prdb.bind_surface(bootstrap, binding_id=f"binding-{index}", project_id=project_id, surface="desktop", external_binding_id=f"window-{index}", actor_id="owner", now=1)
        owner = ActorContext("owner", "desktop", f"binding-{index}", True)
        service = module.ProjectRuntime(bootstrap, clock=lambda: 100)
        first = service.enqueue_turn(project_id, {"n": 1}, owner, idempotency_key=f"first-{index}", expected_version=0)
        second = service.enqueue_turn(project_id, {"n": 2}, owner, idempotency_key=f"second-{index}", expected_version=1)
        unrelated_before = persisted_project_snapshot(bootstrap, unrelated_id)
        assert unrelated_before == unrelated_baseline
        bootstrap.close()
        barrier = threading.Barrier(2)

        def claim(worker):
            conn = projects_db.connect(path)
            try:
                barrier.wait(timeout=5)
                return module.ProjectRuntime(conn, clock=lambda: 101).claim_next_turn(project_id, worker, lease_seconds=30)
            finally:
                conn.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [future.result(timeout=10) for future in (pool.submit(claim, "a"), pool.submit(claim, "b"))]
        winners = [claim for claim in results if claim is not None]
        assert (
            len(winners) == 1
            and winners[0].turn_id == first.turn_id
            and winners[0].sequence == 1
        )
        check = projects_db.connect(path)
        try:
            assert check.execute("SELECT COUNT(*) FROM project_turns WHERE project_id = ? AND status = 'claimed'", (project_id,)).fetchone()[0] == 1
            assert check.execute(
                """SELECT status FROM project_turns
                   WHERE project_id = ? AND turn_id = ?""",
                (project_id, second.turn_id),
            ).fetchone()[0] == "queued"
            assert check.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'turn.claimed'", (project_id,)).fetchone()[0] == 1
            assert check.execute("SELECT COUNT(*) FROM project_worker_leases WHERE project_id = ?", (project_id,)).fetchone()[0] == 1
            assert (
                persisted_project_snapshot(check, unrelated_id)
                == unrelated_before
            )
        finally:
            check.close()


def test_stop_request_survives_reopen_and_blocks_later_fifo_work(tmp_path):
    module = importlib.import_module("hermes_cli.project_runtime")
    path = tmp_path / "stop-crash.db"
    conn = projects_db.connect(path)
    project_id, owner = _adopt_bound_project(
        conn, name="Stop crash", root="stop-root", binding="stop-binding", external="stop-window"
    )
    runtime = module.ProjectRuntime(conn, clock=lambda: 100)
    first = runtime.enqueue_turn(project_id, {"message": "first"}, owner, idempotency_key="first", expected_version=0)
    runtime.enqueue_turn(project_id, {"message": "later"}, owner, idempotency_key="later", expected_version=1)
    runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    runtime.request_stop(project_id, first.turn_id, owner, idempotency_key="stop", expected_version=3, expected_control_version=1)
    conn.close()

    reopened = projects_db.connect(path)
    try:
        state = prdb._runtime_turn_for_project(reopened, project_id=project_id, turn_id=first.turn_id)
        control = prdb._runtime_control_for_turn(reopened, project_id=project_id, turn_id=first.turn_id)
        assert state.status == "stop_requested"
        assert control.control_state == "stop_requested"
        assert reopened.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'run.stop_requested'", (project_id,)).fetchone()[0] == 1
        assert reopened.execute("SELECT COUNT(*) FROM project_events WHERE project_id = ? AND kind = 'run.stopped'", (project_id,)).fetchone()[0] == 0
        assert module.ProjectRuntime(reopened, clock=lambda: 101).claim_next_turn(project_id, "after-crash", lease_seconds=30) is None
    finally:
        reopened.close()


def test_task7_c10_prepared_checkpoint_without_operation_waits_then_discards_only_on_persisted_stop(
    runtime_env,
    tmp_path,
):
    """A pre-operation approval batch stays pending until durable authority wins."""
    module = runtime_env["module"]
    decision_type = module.PreparedApprovalCheckpointDecision
    assert tuple(field.name for field in fields(decision_type)) == ("action",)
    assert decision_type.__dataclass_params__.frozen is True
    assert get_type_hints(decision_type) == {
        "action": Literal["wait", "discard"],
    }
    frozen_decision = decision_type("wait")
    with pytest.raises(FrozenInstanceError):
        frozen_decision.action = "discard"
    resolver_signature = inspect.signature(
        module.ProjectRuntime.resolve_prepared_approval_checkpoint
    )
    assert tuple(resolver_signature.parameters) == (
        "self",
        "attempt",
        "operation_id",
        "approval_id",
    )
    assert (
        resolver_signature.parameters["operation_id"].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    assert (
        resolver_signature.parameters["approval_id"].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    resolver_hints = get_type_hints(
        module.ProjectRuntime.resolve_prepared_approval_checkpoint
    )
    assert resolver_hints == {
        "attempt": module.TurnAttemptIdentity,
        "operation_id": str,
        "approval_id": str,
        "return": decision_type,
    }

    case_connections = []

    def attempt_for(claim):
        return module.TurnAttemptIdentity(
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

    def new_case(label, *, clock, started=True):
        case_conn = projects_db.connect(tmp_path / f"c10-{label}.db")
        case_connections.append(case_conn)
        project_id = projects_db.create_project(
            case_conn,
            name=f"C10 {label}",
            folders=(f"C:/work/c10/{label}",),
        )
        session_id = f"c10-{label}-session"
        binding_id = f"c10-{label}-owner"
        prdb.create_project_conversation(
            case_conn,
            project_id=project_id,
            conversation_id=session_id,
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            case_conn,
            binding_id=binding_id,
            project_id=project_id,
            surface="desktop",
            external_binding_id=f"c10-{label}-window",
            actor_id="owner",
            now=1,
        )
        case_runtime = module.ProjectRuntime(case_conn, clock=clock)
        actor = ActorContext("owner", "desktop", binding_id, True)
        turn = case_runtime.enqueue_turn(
            project_id,
            {"message": label},
            actor,
            idempotency_key=f"c10-{label}-turn",
            expected_version=0,
        )
        claim = case_runtime.claim_next_turn(
            project_id,
            f"c10-{label}-worker",
            lease_seconds=30,
        )
        assert claim is not None
        if started:
            claim = case_runtime.mark_turn_started(claim)
        return case_conn, case_runtime, project_id, turn, actor, claim

    def authority_snapshot(case_conn, project_id, turn_id):
        def rows(sql, parameters=()):
            return tuple(tuple(row) for row in case_conn.execute(sql, parameters))

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
                SELECT turn_id, project_id, sequence, idempotency_key,
                       payload_json, origin_binding_id, status, attempt_id,
                       lease_generation, fencing_token, execution_state,
                       terminal_result_id, recovery_block_key,
                       transcript_applied_batch_id, created_at, updated_at
                FROM project_turns
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ),
            "control": rows(
                """
                SELECT turn_id, project_id, control_state, control_version,
                       idempotency_key, command_fingerprint, attempt_id,
                       claim_worker_id, claim_lease_expires_at,
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
            "operations": rows(
                """
                SELECT operation_id, project_id, turn_id, idempotency_key,
                       approval_id, command_revision, targets_json,
                       payload_json, status, receipt_json, guard_revision,
                       guard_validated, canonical_action, batch_items_json,
                       readback_kind, attempt_id, lease_generation,
                       fencing_token, receipt_id, readback_json, blocked_reason,
                       remote_idempotency_supported,
                       approval_fingerprint_json, approval_checkpoint_id,
                       intent_event_id, recovery_membership_sequence,
                       created_at, updated_at
                FROM project_operations
                WHERE project_id = ?
                ORDER BY operation_id
                """,
                (project_id,),
            ),
            "approvals": rows(
                """
                SELECT approval_id, project_id, turn_id, operation_id,
                       operation_maintenance_seq, actor_id,
                       authorization_actor_id, canonical_action,
                       approval_class, command_revision,
                       expected_runtime_version, effective_runtime_version,
                       turn_expected_control_version, expected_lifecycle,
                       expected_phase, targets_json, batch_boundary_json,
                       status, expires_at, resolved_at,
                       resolved_by_actor_id, consumed_at, created_at
                FROM project_approvals
                WHERE project_id = ?
                ORDER BY approval_id
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
            "total_changes": case_conn.total_changes,
        }

    def resolve_write_free(
        case_runtime,
        case_conn,
        observed_attempt,
        *,
        operation_id="c10-operation",
        approval_id="c10-approval",
        expected_action=None,
        expected_error=None,
        expect_no_reads=False,
    ):
        before = authority_snapshot(
            case_conn,
            observed_attempt.project_id,
            observed_attempt.turn_id,
        )
        statements = []
        read_actions = []
        case_conn.set_trace_callback(statements.append)
        if expect_no_reads:
            def authorizer(action, arg1, arg2, database, source):
                if action == sqlite3.SQLITE_READ:
                    read_actions.append(
                        (action, arg1, arg2, database, source)
                    )
                return sqlite3.SQLITE_OK

            case_conn.set_authorizer(authorizer)
        try:
            if expected_error is None:
                actual = (
                    case_runtime.resolve_prepared_approval_checkpoint(
                        observed_attempt,
                        operation_id=operation_id,
                        approval_id=approval_id,
                    )
                )
            else:
                with pytest.raises(module.ProjectRuntimeError) as rejected:
                    case_runtime.resolve_prepared_approval_checkpoint(
                        observed_attempt,
                        operation_id=operation_id,
                        approval_id=approval_id,
                    )
                assert rejected.value.code is expected_error
                actual = None
        finally:
            if expect_no_reads:
                case_conn.set_authorizer(None)
            case_conn.set_trace_callback(None)
        assert authority_snapshot(
            case_conn,
            observed_attempt.project_id,
            observed_attempt.turn_id,
        ) == before
        dml = tuple(
            statement.split(None, 1)[0].upper()
            for statement in statements
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        )
        assert dml == ()
        if expect_no_reads:
            assert read_actions == []
        if expected_error is None:
            assert actual == decision_type(expected_action)
        return actual

    try:
        # The batch may carry the original horizon after a heartbeat; local
        # expiry is advisory while the exact persisted claim remains live.
        heartbeat_now = [100]
        (
            heartbeat_conn,
            heartbeat_runtime,
            _,
            _,
            _,
            heartbeat_claim,
        ) = new_case(
            "heartbeat-live",
            clock=lambda: heartbeat_now[0],
        )
        original_attempt = attempt_for(heartbeat_claim)
        for empty_operation_id, empty_approval_id in (
            ("", "c10-approval"),
            ("c10-operation", ""),
        ):
            resolve_write_free(
                heartbeat_runtime,
                heartbeat_conn,
                original_attempt,
                operation_id=empty_operation_id,
                approval_id=empty_approval_id,
                expected_error=module.RuntimeErrorCode.INVALID_ARGUMENT,
                expect_no_reads=True,
            )
        heartbeat_now[0] = 110
        renewed_claim = heartbeat_runtime.heartbeat_turn(
            heartbeat_claim,
            lease_seconds=60,
        )
        assert renewed_claim.lease_expires_at > original_attempt.lease_expires_at
        expired_runtime = module.ProjectRuntime(
            heartbeat_conn,
            clock=lambda: renewed_claim.lease_expires_at + 10_000,
        )
        resolve_write_free(
            expired_runtime,
            heartbeat_conn,
            original_attempt,
            expected_action="wait",
        )

        # A Task-5 lease-less parked attempt without a durable block is still
        # the exact authority and therefore remains waiting.
        parked_now = [100]
        (
            parked_conn,
            parked_runtime,
            parked_project,
            parked_turn,
            _,
            parked_claim,
        ) = new_case("parked", clock=lambda: parked_now[0])
        parked_attempt = attempt_for(parked_claim)
        parked_now[0] = parked_claim.lease_expires_at

        class ParkedReadback:
            calls = 0

            def read_turn(self, request):
                self.calls += 1
                assert parked_conn.in_transaction is False
                assert request == module.TurnReadbackRequest(
                    project_id=parked_claim.project_id,
                    turn_id=parked_claim.turn_id,
                    sequence=parked_claim.sequence,
                    worker_id=parked_claim.worker_id,
                    attempt_id=parked_claim.attempt_id,
                    lease_generation=parked_claim.lease_generation,
                    fencing_token=parked_claim.fencing_token,
                    lease_expires_at=parked_claim.lease_expires_at,
                    canonical_session_id=(
                        parked_claim.canonical_session_id
                    ),
                    source_status="claimed",
                    execution_state="started",
                )
                parked_row = parked_conn.execute(
                    """
                    SELECT status, attempt_id, lease_generation,
                           fencing_token, execution_state,
                           recovery_block_key
                    FROM project_turns
                    WHERE project_id = ? AND turn_id = ?
                    """,
                    (parked_project, parked_turn.turn_id),
                ).fetchone()
                assert tuple(parked_row) == (
                    "reconciling",
                    parked_claim.attempt_id,
                    parked_claim.lease_generation,
                    parked_claim.fencing_token,
                    "started",
                    None,
                )
                assert parked_conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM project_worker_leases
                    WHERE project_id = ? AND turn_id = ?
                    """,
                    (parked_project, parked_turn.turn_id),
                ).fetchone()[0] == 0
                assert tuple(
                    parked_conn.execute(
                        """
                        SELECT control_state, attempt_id, claim_worker_id,
                               claim_lease_expires_at,
                               claim_canonical_session_id
                        FROM project_run_controls
                        WHERE project_id = ? AND turn_id = ?
                        """,
                        (parked_project, parked_turn.turn_id),
                    ).fetchone()
                ) == (
                    "running",
                    parked_claim.attempt_id,
                    parked_claim.worker_id,
                    parked_claim.lease_expires_at,
                    parked_claim.canonical_session_id,
                )
                assert parked_conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM project_events
                    WHERE project_id = ? AND turn_id = ?
                      AND kind = 'turn.reconciling'
                    """,
                    (parked_project, parked_turn.turn_id),
                ).fetchone()[0] == 1
                resolve_write_free(
                    parked_runtime,
                    parked_conn,
                    parked_attempt,
                    expected_action="wait",
                )
                return module.TurnReadbackResult("unknown")

        parked_port = ParkedReadback()
        parked_result = parked_runtime.reconcile_inflight_turns(
            parked_port,
            limit=10,
        )
        assert parked_port.calls == 1
        assert len(parked_result) == 1
        assert parked_result[0].status == "reconciling"
        assert parked_conn.execute(
            """
            SELECT recovery_block_key
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (parked_project, parked_turn.turn_id),
        ).fetchone()[0]

        # A persisted stop is sufficient discard authority.
        (
            stop_conn,
            stop_runtime,
            stop_project,
            stop_turn,
            stop_actor,
            stop_claim,
        ) = new_case("stop", clock=lambda: 100)
        stop_attempt = attempt_for(stop_claim)
        stop_state = stop_conn.execute(
            """
            SELECT version FROM project_runtime_state
            WHERE project_id = ?
            """,
            (stop_project,),
        ).fetchone()[0]
        stop_control = stop_conn.execute(
            """
            SELECT control_version FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (stop_project, stop_turn.turn_id),
        ).fetchone()[0]
        stop_runtime.request_stop(
            stop_project,
            stop_turn.turn_id,
            stop_actor,
            idempotency_key="c10-persisted-stop",
            expected_version=stop_state,
            expected_control_version=stop_control,
        )
        resolve_write_free(
            stop_runtime,
            stop_conn,
            stop_attempt,
            expected_action="discard",
        )

        # Public not-started recovery followed by a fresh claim installs a
        # coherent newer attempt/generation/fence.
        successor_now = [100]
        (
            successor_conn,
            successor_runtime,
            successor_project,
            _,
            _,
            old_claim,
        ) = new_case(
            "successor",
            clock=lambda: successor_now[0],
            started=False,
        )
        old_attempt = attempt_for(old_claim)
        successor_now[0] = old_claim.lease_expires_at

        class NoReadbackForNotStarted:
            def read_turn(self, request):
                raise AssertionError("not-started recovery has no readback")

        successor_runtime.reconcile_inflight_turns(
            NoReadbackForNotStarted(),
            limit=10,
        )
        replacement_claim = successor_runtime.claim_next_turn(
            successor_project,
            "c10-successor-worker-2",
            lease_seconds=30,
        )
        assert replacement_claim is not None
        assert replacement_claim.attempt_id != old_claim.attempt_id
        assert (
            replacement_claim.lease_generation
            == old_claim.lease_generation + 1
        )
        assert replacement_claim.fencing_token == old_claim.fencing_token + 1
        resolve_write_free(
            successor_runtime,
            successor_conn,
            old_attempt,
            expected_action="discard",
        )

        # Unknown readback creates one exact persisted recovery block.
        blocked_now = [100]
        (
            blocked_conn,
            blocked_runtime,
            _,
            _,
            _,
            blocked_claim,
        ) = new_case("blocked", clock=lambda: blocked_now[0])
        blocked_attempt = attempt_for(blocked_claim)
        blocked_now[0] = blocked_claim.lease_expires_at

        class UnknownReadback:
            def read_turn(self, request):
                return module.TurnReadbackResult("unknown")

        blocked_runtime.reconcile_inflight_turns(
            UnknownReadback(),
            limit=10,
        )
        blocked_row = blocked_conn.execute(
            """
            SELECT status, recovery_block_key
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (blocked_attempt.project_id, blocked_attempt.turn_id),
        ).fetchone()
        assert tuple(blocked_row)[0] == "reconciling"
        assert tuple(blocked_row)[1]
        resolve_write_free(
            blocked_runtime,
            blocked_conn,
            blocked_attempt,
            expected_action="discard",
        )

        # Batch-side authority may never run ahead of the retained claim.
        (
            mismatch_conn,
            mismatch_runtime,
            _,
            _,
            _,
            mismatch_claim,
        ) = new_case("mismatch", clock=lambda: 100)
        mismatch_attempt = attempt_for(mismatch_claim)
        for malformed_attempt in (
            replace(
                mismatch_attempt,
                lease_expires_at=mismatch_attempt.lease_expires_at + 1,
            ),
            replace(
                mismatch_attempt,
                lease_generation=mismatch_attempt.lease_generation + 1,
            ),
            replace(
                mismatch_attempt,
                fencing_token=mismatch_attempt.fencing_token + 1,
            ),
        ):
            resolve_write_free(
                mismatch_runtime,
                mismatch_conn,
                malformed_attempt,
                expected_error=(
                    module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
                ),
            )

        # Missing retained audit authority and an unexpected dispatch gate
        # are storage conflicts, never opportunistic discard authority.
        (
            malformed_conn,
            malformed_runtime,
            malformed_project,
            malformed_turn,
            _,
            malformed_claim,
        ) = new_case("malformed", clock=lambda: 100)
        malformed_attempt = attempt_for(malformed_claim)
        malformed_conn.execute(
            """
            UPDATE project_run_controls
            SET claim_worker_id = NULL
            WHERE project_id = ? AND turn_id = ?
            """,
            (malformed_project, malformed_turn.turn_id),
        )
        malformed_conn.commit()
        resolve_write_free(
            malformed_runtime,
            malformed_conn,
            malformed_attempt,
            expected_error=module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
        )

        (
            gate_conn,
            gate_runtime,
            gate_project,
            gate_turn,
            _,
            gate_claim,
        ) = new_case("unexpected-gate", clock=lambda: 100)
        gate_attempt = attempt_for(gate_claim)
        gate_conn.execute(
            """
            UPDATE project_runtime_state
            SET transcript_pending_batch_id =
                '99999999-9999-4999-8999-999999999999'
            WHERE project_id = ?
            """,
            (gate_project,),
        )
        gate_conn.commit()
        resolve_write_free(
            gate_runtime,
            gate_conn,
            gate_attempt,
            expected_error=module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
        )
        assert gate_turn.turn_id == gate_attempt.turn_id
    finally:
        for case_conn in case_connections:
            case_conn.close()


def test_task7_c13_worker_context_execution_input_is_exact_started_snapshot_and_surface_origin(
    runtime_env,
):
    """Started dispatcher claims expose one detached, origin-bound snapshot.

    This catches a worker input bridge that reads before ``mark_turn_started``,
    loses the immutable Desktop/Discord binding, or derives a gateway-specific
    payload schema instead of returning the stored turn payload verbatim.
    """
    module = runtime_env["module"]
    clock_samples: list[int] = []
    runtime = module.ProjectRuntime(
        runtime_env["conn"],
        clock=lambda: (clock_samples.append(100) or 100),
    )

    assert hasattr(module, "TurnOrigin")
    assert hasattr(module, "TurnExecutionInput")
    assert tuple(field.name for field in fields(module.TurnOrigin)) == (
        "binding_id",
        "surface",
        "external_binding_id",
        "actor_id",
    )
    assert tuple(field.name for field in fields(module.TurnExecutionInput)) == (
        "attempt",
        "payload",
        "origin",
        "contract_revision",
    )

    turn = runtime.enqueue_turn(
        runtime_env["project_id"],
        {"message": "desktop-origin", "opaque": {"keep": True}},
        runtime_env["desktop"],
        idempotency_key="c13-desktop-input",
        expected_version=0,
    )
    lease = runtime.acquire_dispatcher_lease(
        "11111111-1111-4111-8111-111111111111", lease_seconds=30
    )
    assert lease is not None
    start = runtime.claim_next_turn_for_dispatcher(
        runtime_env["project_id"],
        "c13-worker",
        lease_seconds=30,
        dispatcher_lease=lease,
    )
    assert start is not None
    # A claimed-but-not-started attempt is not executable and the fresh input
    # read is strictly read-only.  This is intentionally asserted before the
    # successful snapshot so ``execution_input_for_claim`` cannot be a late
    # convenience wrapper around the worker start path.
    writes_before = runtime_env["conn"].total_changes
    statements: list[str] = []
    runtime_env["conn"].set_trace_callback(statements.append)
    before_prestart_clock = len(clock_samples)
    try:
        with pytest.raises(module.ProjectRuntimeError) as not_started:
            runtime.execution_input_for_claim(start.claim)
    finally:
        runtime_env["conn"].set_trace_callback(None)
    assert not_started.value.code is module.RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED
    assert len(clock_samples) == before_prestart_clock + 1
    assert runtime_env["conn"].total_changes == writes_before
    assert not any(
        statement.lstrip().upper().startswith(
            ("INSERT", "UPDATE", "DELETE", "REPLACE")
        )
        for statement in statements
    )

    runtime_env["conn"].execute(
        """
        INSERT INTO project_contracts (
            contract_id, project_id, revision, contract_json, status,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("c13-contract-7", runtime_env["project_id"], 7, "{}", "active", 1, 1),
    )
    runtime_env["conn"].commit()
    claim = runtime.mark_turn_started(start.claim)

    execution = runtime.execution_input_for_claim(claim)

    assert execution.attempt == module.TurnAttemptIdentity(
        project_id=runtime_env["project_id"],
        turn_id=turn.turn_id,
        sequence=turn.sequence,
        worker_id="c13-worker",
        attempt_id=claim.attempt_id,
        lease_generation=claim.lease_generation,
        fencing_token=claim.fencing_token,
        canonical_session_id="session-root",
        lease_expires_at=claim.lease_expires_at,
    )
    assert execution.payload == {
        "message": "desktop-origin",
        "opaque": {"keep": True},
    }
    assert execution.origin == module.TurnOrigin(
        binding_id="desktop-owner",
        surface="desktop",
        external_binding_id="window-1",
        actor_id="owner-1",
    )
    assert execution.contract_revision == 7

    # One malformed persisted revision invalidates the complete fresh
    # snapshot; it must not be silently ignored in favour of the valid max.
    runtime_env["conn"].execute(
        """
        INSERT INTO project_contracts (
            contract_id, project_id, revision, contract_json, status,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("c13-contract-bad", runtime_env["project_id"], "bad", "{}", "active", 1, 1),
    )
    runtime_env["conn"].commit()
    writes_before = runtime_env["conn"].total_changes
    statements = []
    runtime_env["conn"].set_trace_callback(statements.append)
    try:
        with pytest.raises(module.ProjectRuntimeError):
            runtime.execution_input_for_claim(claim)
    finally:
        runtime_env["conn"].set_trace_callback(None)
    assert runtime_env["conn"].total_changes == writes_before
    assert not any(
        statement.lstrip().upper().startswith(
            ("INSERT", "UPDATE", "DELETE", "REPLACE")
        )
        for statement in statements
    )
