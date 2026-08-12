from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from hermes_cli import projects_db
from hermes_cli.project_surface_operations import (
    SurfaceChannelCollision,
    SurfaceEffectClaim,
    SurfaceOperationConflict,
    claim_effect,
    init_schema,
    mark_effect_started,
    pending_for_recovery,
    prepare_or_replay,
    reconcile,
    renew_effect_claim,
)


def _install_authority(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE project_events (
            event_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES projects(id),
            sequence INTEGER NOT NULL,
            UNIQUE (project_id, sequence),
            UNIQUE (project_id, event_id)
        )
        """
    )
    conn.executemany(
        "INSERT INTO projects(id) VALUES (?)",
        (("project-1",), ("project-2",)),
    )
    conn.executemany(
        """
        INSERT INTO project_events(event_id, project_id, sequence)
        VALUES (?, ?, ?)
        """,
        (
            ("event-1", "project-1", 1),
            ("event-2", "project-1", 2),
            ("event-3", "project-2", 1),
        ),
    )
    init_schema(conn)


def _db(path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:" if path is None else path)
    conn.row_factory = sqlite3.Row
    _install_authority(conn)
    conn.commit()
    return conn


def test_normal_projects_schema_initializes_surface_operation_ledger():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    projects_db.init_schema(conn)

    tables = {
        row["name"]
        for row in conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table'
              AND name LIKE 'project_surface_operation%'
            """
        )
    }
    assert tables == {
        "project_surface_operations",
        "project_surface_operation_leases",
    }


def _prepare(
    conn: sqlite3.Connection,
    *,
    operation_id: str = "operation-1",
    project_id: str = "project-1",
    event_id: str = "event-1",
    desired_json: str = '{"category":"active","name":"alpha"}',
):
    return prepare_or_replay(
        conn,
        operation_id=operation_id,
        project_id=project_id,
        lifecycle_event_id=event_id,
        kind="ensure_channel",
        desired_json=desired_json,
        prestate_json="{}",
        ownership_marker="marker-1",
    )


def _claim_and_start(
    conn: sqlite3.Connection,
    operation_id: str = "operation-1",
    *,
    holder_id: str = "projector-1",
    now: int = 10,
) -> tuple[object, SurfaceEffectClaim]:
    claim = claim_effect(
        conn,
        operation_id,
        holder_id=holder_id,
        now=now,
        lease_seconds=30,
    )
    assert claim is not None
    operation = mark_effect_started(
        conn,
        operation_id,
        claim=claim,
        now=now,
    )
    return operation, claim


def test_prepare_is_canonical_write_free_and_rejects_contract_drift():
    conn = _db()
    first = _prepare(conn)
    changes = conn.total_changes

    replay = _prepare(
        conn,
        desired_json='{"name":"alpha", "category":"active"}',
    )

    assert replay == first
    assert conn.total_changes == changes
    assert replay.desired_json == '{"category":"active","name":"alpha"}'
    with pytest.raises(SurfaceOperationConflict):
        _prepare(conn, desired_json='{"name":"renamed"}')
    with pytest.raises(SurfaceOperationConflict):
        prepare_or_replay(
            conn,
            operation_id="operation-2",
            project_id="project-1",
            lifecycle_event_id="event-1",
            kind="ensure_channel",
            desired_json="{}",
            prestate_json="{}",
            ownership_marker="marker-1",
        )
    with pytest.raises(SurfaceOperationConflict):
        prepare_or_replay(
            conn,
            operation_id="operation-1",
            project_id="project-1",
            lifecycle_event_id="event-2",
            kind="ensure_channel",
            desired_json="{}",
            prestate_json="{}",
            ownership_marker="marker-1",
        )


@pytest.mark.parametrize(
    "invalid_json",
    [
        '{"name":"first","name":"second"}',
        '{"value":NaN}',
        '{"value":Infinity}',
        "[]",
    ],
)
def test_prepare_rejects_noncanonical_json_contracts(invalid_json):
    conn = _db()

    with pytest.raises(SurfaceOperationConflict):
        _prepare(conn, desired_json=invalid_json)


def test_prepare_requires_canonical_project_and_event_authority():
    conn = _db()

    with pytest.raises(SurfaceOperationConflict):
        prepare_or_replay(
            conn,
            operation_id="orphan-project",
            project_id="missing-project",
            lifecycle_event_id="event-1",
            kind="ensure_channel",
            desired_json="{}",
            prestate_json="{}",
            ownership_marker="marker",
        )
    with pytest.raises(SurfaceOperationConflict):
        prepare_or_replay(
            conn,
            operation_id="orphan-event",
            project_id="project-1",
            lifecycle_event_id="missing-event",
            kind="ensure_channel",
            desired_json="{}",
            prestate_json="{}",
            ownership_marker="marker",
        )
    with pytest.raises(SurfaceOperationConflict):
        prepare_or_replay(
            conn,
            operation_id="wrong-project-event",
            project_id="project-2",
            lifecycle_event_id="event-1",
            kind="ensure_channel",
            desired_json="{}",
            prestate_json="{}",
            ownership_marker="marker",
        )


def test_effect_and_terminal_boundaries_replay_without_new_writes():
    conn = _db()
    prepared = _prepare(conn)
    assert [row.operation_id for row in pending_for_recovery(conn)] == [
        prepared.operation_id
    ]

    started, claim = _claim_and_start(conn, prepared.operation_id)
    changes = conn.total_changes
    assert (
        mark_effect_started(
            conn,
            prepared.operation_id,
            claim=claim,
            now=10,
        )
        == started
    )
    assert conn.total_changes == changes

    pending = reconcile(
        conn,
        prepared.operation_id,
        claim=claim,
        now=10,
        readback_json='{"channel_id":"channel-1","complete":false}',
        external_channel_id="channel-1",
        outcome="partial",
    )
    assert pending.status == "sync_pending"
    assert pending_for_recovery(conn) == (pending,)

    _started, recovery_claim = _claim_and_start(
        conn,
        prepared.operation_id,
        holder_id="projector-2",
        now=11,
    )
    synchronized = reconcile(
        conn,
        prepared.operation_id,
        claim=recovery_claim,
        now=11,
        readback_json='{"complete":true,"channel_id":"channel-1"}',
        external_channel_id="channel-1",
        outcome="exact",
    )
    changes = conn.total_changes
    replay = reconcile(
        conn,
        prepared.operation_id,
        claim=recovery_claim,
        now=11,
        readback_json='{"channel_id":"channel-1","complete":true}',
        external_channel_id="channel-1",
        outcome="exact",
    )
    assert replay == synchronized
    assert conn.total_changes == changes
    assert pending_for_recovery(conn) == ()
    with pytest.raises(SurfaceOperationConflict):
        reconcile(
            conn,
            prepared.operation_id,
            claim=recovery_claim,
            now=11,
            readback_json='{"complete":false}',
            external_channel_id="channel-1",
            outcome="exact",
        )


def test_foreign_readback_blocks_without_claiming_its_channel():
    conn = _db()
    _prepare(conn)
    _started, claim = _claim_and_start(conn)

    blocked = reconcile(
        conn,
        "operation-1",
        claim=claim,
        now=10,
        readback_json='{"channel_id":"foreign","marker":"foreign"}',
        external_channel_id="foreign",
        outcome="foreign",
    )
    changes = conn.total_changes

    assert blocked.status == "blocked"
    assert blocked.external_channel_id is None
    assert (
        reconcile(
            conn,
            "operation-1",
            claim=claim,
            now=10,
            readback_json='{"marker":"foreign","channel_id":"foreign"}',
            external_channel_id="foreign",
            outcome="foreign",
        )
        == blocked
    )
    assert conn.total_changes == changes
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM project_surface_channel_claims"
        ).fetchone()[0]
        == 0
    )


def test_channel_claim_allows_same_project_lifecycle_but_blocks_other_project():
    conn = _db()
    _prepare(conn)
    _started, first_claim = _claim_and_start(conn)
    reconcile(
        conn,
        "operation-1",
        claim=first_claim,
        now=10,
        readback_json='{"channel_id":"channel-1"}',
        external_channel_id="channel-1",
        outcome="exact",
    )

    _prepare(
        conn,
        operation_id="operation-2",
        project_id="project-1",
        event_id="event-2",
    )
    _started, second_claim = _claim_and_start(
        conn,
        "operation-2",
        holder_id="projector-2",
        now=11,
    )
    assert (
        reconcile(
            conn,
            "operation-2",
            claim=second_claim,
            now=11,
            readback_json='{"channel_id":"channel-1"}',
            external_channel_id="channel-1",
            outcome="exact",
        ).status
        == "synchronized"
    )

    _prepare(
        conn,
        operation_id="operation-3",
        project_id="project-2",
        event_id="event-3",
    )
    _started, third_claim = _claim_and_start(
        conn,
        "operation-3",
        holder_id="projector-3",
        now=12,
    )
    with pytest.raises(SurfaceChannelCollision):
        reconcile(
            conn,
            "operation-3",
            claim=third_claim,
            now=12,
            readback_json='{"channel_id":"channel-1"}',
            external_channel_id="channel-1",
            outcome="exact",
        )


def test_concurrent_prepare_converges_and_never_leaks_sqlite_integrity(
    tmp_path,
):
    db_path = tmp_path / "surface-operations.db"
    seed = _db(db_path)
    seed.close()
    barrier = Barrier(2)

    def prepare_from_connection(desired_json: str):
        conn = sqlite3.connect(db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        barrier.wait()
        try:
            return _prepare(conn, desired_json=desired_json)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(
            pool.map(
                prepare_from_connection,
                (
                    '{"name":"alpha","category":"active"}',
                    '{"category":"active","name":"alpha"}',
                ),
            )
        )

    assert results[0] == results[1]

    first = sqlite3.connect(db_path)
    first.row_factory = sqlite3.Row
    first.execute("PRAGMA foreign_keys=ON")
    try:
        with pytest.raises(SurfaceOperationConflict):
            _prepare(first, desired_json='{"name":"drifted"}')
    finally:
        first.close()


def test_project_effect_claim_is_exclusive_expiring_and_fenced(tmp_path):
    db_path = tmp_path / "surface-effect-claim.db"
    first_conn = _db(db_path)
    second_conn = sqlite3.connect(db_path, timeout=5)
    second_conn.row_factory = sqlite3.Row
    second_conn.execute("PRAGMA foreign_keys=ON")
    try:
        _prepare(first_conn)
        first = claim_effect(
            first_conn,
            "operation-1",
            holder_id="projector-1",
            now=10,
            lease_seconds=5,
        )
        assert first is not None
        assert (
            claim_effect(
                second_conn,
                "operation-1",
                holder_id="projector-2",
                now=14,
                lease_seconds=5,
            )
            is None
        )

        replacement = claim_effect(
            second_conn,
            "operation-1",
            holder_id="projector-2",
            now=15,
            lease_seconds=5,
        )
        assert replacement is not None
        assert replacement.fencing_token > first.fencing_token

        with pytest.raises(SurfaceOperationConflict):
            mark_effect_started(
                first_conn,
                "operation-1",
                claim=first,
                now=15,
            )
        assert (
            mark_effect_started(
                second_conn,
                "operation-1",
                claim=replacement,
                now=15,
            ).status
            == "effect_started"
        )
    finally:
        first_conn.close()
        second_conn.close()


def test_effect_claim_renewal_fences_takeover_during_a_slow_remote_effect(
    tmp_path,
):
    db_path = tmp_path / "surface-effect-renewal.db"
    first_conn = _db(db_path)
    second_conn = sqlite3.connect(db_path, timeout=5)
    second_conn.row_factory = sqlite3.Row
    second_conn.execute("PRAGMA foreign_keys=ON")
    try:
        _prepare(first_conn)
        first = claim_effect(
            first_conn,
            "operation-1",
            holder_id="slow-projector",
            now=10,
            lease_seconds=5,
        )
        assert first is not None

        renewed = renew_effect_claim(
            first_conn,
            "operation-1",
            claim=first,
            now=14,
            lease_seconds=5,
        )

        assert renewed.lease_expires_at == 19
        assert renewed.fencing_token == first.fencing_token
        assert (
            claim_effect(
                second_conn,
                "operation-1",
                holder_id="replacement-projector",
                now=15,
                lease_seconds=5,
            )
            is None
        )
    finally:
        first_conn.close()
        second_conn.close()


def test_blocked_reconciliation_persists_the_concrete_reason():
    conn = _db()
    _prepare(conn)
    _started, claim = _claim_and_start(conn)

    blocked = reconcile(
        conn,
        "operation-1",
        claim=claim,
        now=10,
        readback_json='{"marker":"foreign"}',
        outcome="blocked",
        blocked_reason="foreign_marker",
    )

    assert blocked.status == "blocked"
    assert blocked.blocked_reason == "foreign_marker"


def test_idempotent_pending_reconcile_releases_claim_for_next_recovery():
    conn = _db()
    _prepare(conn)
    _started, first_claim = _claim_and_start(conn)
    pending = reconcile(
        conn,
        "operation-1",
        claim=first_claim,
        now=10,
        readback_json='{"channel_id":"channel-1","complete":false}',
        external_channel_id="channel-1",
        outcome="partial",
    )
    assert pending.status == "sync_pending"

    _started, replay_claim = _claim_and_start(
        conn,
        holder_id="projector-2",
        now=11,
    )
    replay = reconcile(
        conn,
        "operation-1",
        claim=replay_claim,
        now=11,
        readback_json='{"complete":false,"channel_id":"channel-1"}',
        external_channel_id="channel-1",
        outcome="partial",
    )

    assert replay == pending
    assert (
        claim_effect(
            conn,
            "operation-1",
            holder_id="projector-3",
            now=12,
            lease_seconds=30,
        )
        is not None
    )


def test_first_ambiguous_readback_never_claims_a_channel():
    conn = _db()
    _prepare(conn)
    _started, claim = _claim_and_start(conn)

    pending = reconcile(
        conn,
        "operation-1",
        claim=claim,
        now=10,
        readback_json='{"channel_id":"uncertain"}',
        external_channel_id="uncertain",
        outcome="ambiguous",
    )

    assert pending.status == "sync_pending"
    assert pending.external_channel_id is None
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM project_surface_channel_claims"
        ).fetchone()[0]
        == 0
    )
