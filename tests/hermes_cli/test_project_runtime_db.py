"""Behavioral tests for the durable per-project runtime store."""

from __future__ import annotations

import dataclasses
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from hermes_cli import projects_db as pdb
from hermes_cli import project_runtime_db as prdb
from hermes_cli.project_policy import ActorContext


RUNTIME_TABLES = {
    "project_contracts",
    "project_runtime_state",
    "project_conversations",
    "project_surface_bindings",
    "project_turns",
    "project_run_controls",
    "project_events",
    "project_deliveries",
    "project_approvals",
    "project_artifacts",
    "project_operations",
    "project_worker_leases",
}


@pytest.fixture
def runtime_conn(tmp_path):
    conn = sqlite3.connect(tmp_path / "projects.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    prdb.ensure_schema(conn)
    try:
        yield conn
    finally:
        conn.close()


def _insert_project(conn, project_id):
    conn.execute(
        """
        INSERT INTO projects (id, slug, name, created_at, archived)
        VALUES (?, ?, ?, 1, 0)
        """,
        (project_id, project_id, project_id),
    )
    conn.commit()


def _insert_turn(
    conn,
    *,
    turn_id,
    project_id,
    sequence,
    idempotency_key,
):
    conn.execute(
        """
        INSERT INTO project_turns (
            turn_id, project_id, sequence, idempotency_key, payload_json,
            status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, '{}', 'queued', 1, 1)
        """,
        (turn_id, project_id, sequence, idempotency_key),
    )


def _insert_event(
    conn,
    *,
    event_id,
    project_id,
    sequence,
    turn_id=None,
):
    conn.execute(
        """
        INSERT INTO project_events (
            event_id, project_id, sequence, kind, turn_id, payload_json,
            created_at
        ) VALUES (?, ?, ?, 'test.event', ?, '{}', 1)
        """,
        (event_id, project_id, sequence, turn_id),
    )


def test_migrates_existing_projects_without_changing_catalog_records(tmp_path):
    db_path = tmp_path / "projects.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            icon TEXT,
            color TEXT,
            board_slug TEXT,
            primary_path TEXT,
            created_at INTEGER NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE project_folders (
            project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
            path TEXT NOT NULL,
            label TEXT,
            is_primary INTEGER NOT NULL DEFAULT 0,
            added_at INTEGER NOT NULL,
            PRIMARY KEY (project_id, path)
        );
        CREATE TABLE project_meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE discovered_repos (
            root TEXT PRIMARY KEY,
            label TEXT,
            last_seen INTEGER NOT NULL
        );
        """
    )
    conn.executemany(
        """
        INSERT INTO projects (
            id, slug, name, description, icon, color, board_slug,
            primary_path, created_at, archived
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "p_active",
                "active",
                "Active project",
                "kept byte-for-value",
                "A",
                "#112233",
                "active-board",
                "C:/work/active",
                101,
                0,
            ),
            (
                "p_archived",
                "archived",
                "Archived project",
                None,
                None,
                None,
                None,
                None,
                202,
                1,
            ),
        ],
    )
    conn.execute(
        """
        INSERT INTO project_folders
            (project_id, path, label, is_primary, added_at)
        VALUES ('p_active', 'C:/work/active', 'repo', 1, 303)
        """
    )
    conn.execute(
        "INSERT INTO project_meta (key, value) VALUES ('active_id', 'p_active')"
    )
    conn.execute(
        """
        INSERT INTO discovered_repos (root, label, last_seen)
        VALUES ('C:/work/discovered', 'discovered', 404)
        """
    )
    conn.commit()

    before = {
        table: [
            tuple(row)
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid")
        ]
        for table in (
            "projects",
            "project_folders",
            "project_meta",
            "discovered_repos",
        )
    }

    conn.close()
    conn = pdb.connect(db_path=db_path)

    after = {
        table: [
            tuple(row)
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid")
        ]
        for table in before
    }
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }

    assert after == before
    assert RUNTIME_TABLES <= tables
    assert conn.execute("SELECT * FROM project_runtime_state").fetchall() == []
    conn.close()


def _schema_snapshot(conn):
    definitions = [
        tuple(row)
        for row in conn.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE name LIKE 'project_%'
            ORDER BY type, name
            """
        )
    ]
    columns = {
        table: [tuple(row) for row in conn.execute(f"PRAGMA table_info({table})")]
        for table in RUNTIME_TABLES
    }
    return definitions, columns


def test_ensure_schema_is_idempotent(runtime_conn):
    first = _schema_snapshot(runtime_conn)

    prdb.ensure_schema(runtime_conn)
    second = _schema_snapshot(runtime_conn)

    assert second == first


@pytest.mark.parametrize(
    ("schema_sql", "expected_tables"),
    [
        (
            "CREATE TABLE trailing_line(value TEXT);\n"
            "-- trailing line comment",
            {"trailing_line"},
        ),
        (
            "CREATE TABLE trailing_block(value TEXT);\n"
            "/* trailing block comment */",
            {"trailing_block"},
        ),
        (
            "CREATE TABLE same_line_one(value TEXT); "
            "CREATE TABLE same_line_two(value TEXT);",
            {"same_line_one", "same_line_two"},
        ),
    ],
)
def test_execute_schema_statements_accepts_valid_formatting(
    schema_sql, expected_tables
):
    conn = sqlite3.connect(":memory:")
    try:
        prdb.execute_schema_statements(conn, schema_sql)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        conn.close()

    assert expected_tables <= tables


def test_execute_schema_statements_rejects_incomplete_sql():
    conn = sqlite3.connect(":memory:")
    try:
        with pytest.raises(ValueError, match="incomplete schema SQL"):
            prdb.execute_schema_statements(
                conn,
                "CREATE TABLE genuinely_incomplete(",
            )
    finally:
        conn.close()


def test_every_runtime_table_has_project_fk_and_unique_identity(runtime_conn):
    for table in RUNTIME_TABLES:
        foreign_keys = runtime_conn.execute(
            f"PRAGMA foreign_key_list({table})"
        ).fetchall()
        assert any(
            row["from"] == "project_id"
            and row["table"] == "projects"
            and row["to"] == "id"
            for row in foreign_keys
        ), table

        indexes = runtime_conn.execute(f"PRAGMA index_list({table})").fetchall()
        assert any(row["unique"] for row in indexes), table


def test_foreign_keys_and_project_scoped_uniqueness_are_enforced(runtime_conn):
    with pytest.raises(sqlite3.IntegrityError):
        prdb.create_runtime_state(
            runtime_conn,
            project_id="missing",
            conversation_root_id="root",
            conversation_tip_id="tip",
            updated_at=1,
        )
    runtime_conn.rollback()

    _insert_project(runtime_conn, "p_one")
    _insert_project(runtime_conn, "p_two")
    _insert_turn(
        runtime_conn,
        turn_id="turn_one",
        project_id="p_one",
        sequence=1,
        idempotency_key="turn-key",
    )
    runtime_conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        _insert_turn(
            runtime_conn,
            turn_id="turn_duplicate_sequence",
            project_id="p_one",
            sequence=1,
            idempotency_key="other-key",
        )
    runtime_conn.rollback()

    with pytest.raises(sqlite3.IntegrityError):
        _insert_turn(
            runtime_conn,
            turn_id="turn_duplicate_key",
            project_id="p_one",
            sequence=2,
            idempotency_key="turn-key",
        )
    runtime_conn.rollback()

    _insert_event(
        runtime_conn,
        event_id="event_one",
        project_id="p_one",
        sequence=1,
        turn_id="turn_one",
    )
    runtime_conn.execute(
        """
        INSERT INTO project_surface_bindings (
            binding_id, project_id, surface, external_binding_id, created_at
        ) VALUES ('binding_one', 'p_one', 'desktop', 'window-one', 1)
        """
    )
    runtime_conn.execute(
        """
        INSERT INTO project_surface_bindings (
            binding_id, project_id, surface, external_binding_id, created_at
        ) VALUES ('binding_two', 'p_two', 'discord', 'channel-two', 1)
        """
    )
    runtime_conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        _insert_event(
            runtime_conn,
            event_id="event_cross_project",
            project_id="p_two",
            sequence=1,
            turn_id="turn_one",
        )
    runtime_conn.rollback()

    runtime_conn.execute(
        """
        INSERT INTO project_deliveries (
            delivery_id, project_id, binding_id, event_id, status, updated_at
        ) VALUES (
            'delivery_one', 'p_one', 'binding_one', 'event_one', 'pending', 1
        )
        """
    )
    runtime_conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        runtime_conn.execute(
            """
            INSERT INTO project_deliveries (
                delivery_id, project_id, binding_id, event_id, status, updated_at
            ) VALUES (
                'delivery_duplicate', 'p_one', 'binding_one', 'event_one',
                'pending', 1
            )
            """
        )
    runtime_conn.rollback()

    with pytest.raises(sqlite3.IntegrityError):
        runtime_conn.execute(
            """
            INSERT INTO project_deliveries (
                delivery_id, project_id, binding_id, event_id, status, updated_at
            ) VALUES (
                'delivery_cross_project', 'p_two', 'binding_two', 'event_one',
                'pending', 1
            )
            """
        )
    runtime_conn.rollback()


LEGAL_LIFECYCLE_EDGES = {
    ("active", "awaiting_acceptance"),
    ("awaiting_acceptance", "completed"),
    ("awaiting_acceptance", "active"),
    ("completed", "active"),
}


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (source, target)
        for source in ("active", "awaiting_acceptance", "completed")
        for target in ("active", "awaiting_acceptance", "completed")
    ],
)
def test_lifecycle_transition_graph_enforces_only_legal_edges(
    runtime_conn, source, target
):
    project_id = f"p_{source}_{target}"
    _insert_project(runtime_conn, project_id)
    with prdb.write_transaction(runtime_conn):
        prdb.create_runtime_state(
            runtime_conn,
            project_id=project_id,
            conversation_root_id="root",
            conversation_tip_id="tip",
            updated_at=10,
        )
        runtime_conn.execute(
            """
            UPDATE project_runtime_state
            SET lifecycle = ?, version = 4
            WHERE project_id = ?
            """,
            (source, project_id),
        )

    with prdb.write_transaction(runtime_conn):
        result = prdb.transition_lifecycle(
            runtime_conn,
            project_id=project_id,
            expected_version=4,
            lifecycle=target,
            updated_at=20,
        )

    stored = prdb.runtime_state_for_project(runtime_conn, project_id)
    if (source, target) in LEGAL_LIFECYCLE_EDGES:
        assert result == stored
        assert stored.lifecycle == target
        assert stored.version == 5
        assert stored.updated_at == 20
    else:
        assert result is None
        assert stored.lifecycle == source
        assert stored.version == 4
        assert stored.updated_at == 10


def test_runtime_state_row_is_immutable(runtime_conn):
    _insert_project(runtime_conn, "p_immutable")
    with prdb.write_transaction(runtime_conn):
        state = prdb.create_runtime_state(
            runtime_conn,
            project_id="p_immutable",
            conversation_root_id="root",
            conversation_tip_id="tip",
            updated_at=1,
        )

    with pytest.raises(dataclasses.FrozenInstanceError):
        state.lifecycle = "completed"


def test_runtime_state_rejects_lifecycle_outside_domain(runtime_conn):
    _insert_project(runtime_conn, "p_invalid_lifecycle")

    with pytest.raises(sqlite3.IntegrityError):
        runtime_conn.execute(
            """
            INSERT INTO project_runtime_state (
                project_id,
                lifecycle,
                version,
                conversation_root_id,
                conversation_tip_id,
                updated_at
            ) VALUES ('p_invalid_lifecycle', 'archived', 0, 'root', 'tip', 1)
            """
        )
    runtime_conn.rollback()


def test_stale_expected_version_does_not_change_runtime_state(runtime_conn):
    _insert_project(runtime_conn, "p_stale")
    with prdb.write_transaction(runtime_conn):
        original = prdb.create_runtime_state(
            runtime_conn,
            project_id="p_stale",
            conversation_root_id="root",
            conversation_tip_id="tip",
            updated_at=1,
        )

    with prdb.write_transaction(runtime_conn):
        result = prdb.transition_lifecycle(
            runtime_conn,
            project_id="p_stale",
            expected_version=99,
            lifecycle="awaiting_acceptance",
            updated_at=2,
        )

    assert result is None
    assert prdb.runtime_state_for_project(runtime_conn, "p_stale") == original


def test_concurrent_accept_and_reopen_have_exactly_one_cas_winner(tmp_path):
    db_path = tmp_path / "projects.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    prdb.ensure_schema(conn)
    _insert_project(conn, "p_race")
    with prdb.write_transaction(conn):
        prdb.create_runtime_state(
            conn,
            project_id="p_race",
            conversation_root_id="root",
            conversation_tip_id="tip",
            updated_at=1,
        )
        conn.execute(
            """
            UPDATE project_runtime_state
            SET lifecycle = 'awaiting_acceptance', version = 1
            WHERE project_id = 'p_race'
            """
        )
    conn.close()

    barrier = threading.Barrier(2)

    def attempt(target):
        worker_conn = sqlite3.connect(db_path, timeout=10)
        worker_conn.row_factory = sqlite3.Row
        worker_conn.execute("PRAGMA foreign_keys=ON")
        try:
            barrier.wait()
            with prdb.write_transaction(worker_conn):
                return prdb.transition_lifecycle(
                    worker_conn,
                    project_id="p_race",
                    expected_version=1,
                    lifecycle=target,
                    updated_at=2,
                )
        finally:
            worker_conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ("completed", "active")))

    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert winners[0].lifecycle in {"completed", "active"}
    assert winners[0].version == 2


def test_write_transaction_rolls_back_turn_and_event_on_original_exception(
    runtime_conn,
):
    _insert_project(runtime_conn, "p_rollback")

    class TransactionFailure(Exception):
        pass

    original = TransactionFailure("preserve me")
    with pytest.raises(TransactionFailure) as raised:
        with prdb.write_transaction(runtime_conn):
            _insert_turn(
                runtime_conn,
                turn_id="turn_rollback",
                project_id="p_rollback",
                sequence=1,
                idempotency_key="rollback-key",
            )
            _insert_event(
                runtime_conn,
                event_id="event_rollback",
                project_id="p_rollback",
                sequence=1,
                turn_id="turn_rollback",
            )
            raise original

    assert raised.value is original
    assert runtime_conn.execute(
        "SELECT 1 FROM project_turns WHERE turn_id = 'turn_rollback'"
    ).fetchone() is None
    assert runtime_conn.execute(
        "SELECT 1 FROM project_events WHERE event_id = 'event_rollback'"
    ).fetchone() is None


def test_ensure_schema_preserves_caller_transaction_rollback(runtime_conn):
    class TransactionFailure(Exception):
        pass

    with pytest.raises(TransactionFailure):
        with prdb.write_transaction(runtime_conn):
            runtime_conn.execute(
                """
                INSERT INTO projects (id, slug, name, created_at, archived)
                VALUES ('p_runtime_init_rollback', 'runtime-init-rollback',
                        'Runtime init rollback', 1, 0)
                """
            )
            prdb.ensure_schema(runtime_conn)
            raise TransactionFailure

    assert runtime_conn.execute(
        "SELECT 1 FROM projects WHERE id = 'p_runtime_init_rollback'"
    ).fetchone() is None


def test_catalog_init_preserves_caller_transaction_rollback(runtime_conn):
    class TransactionFailure(Exception):
        pass

    with pytest.raises(TransactionFailure):
        with prdb.write_transaction(runtime_conn):
            runtime_conn.execute(
                """
                INSERT INTO projects (id, slug, name, created_at, archived)
                VALUES ('p_catalog_init_rollback', 'catalog-init-rollback',
                        'Catalog init rollback', 1, 0)
                """
            )
            pdb.init_schema(runtime_conn)
            raise TransactionFailure

    assert runtime_conn.execute(
        "SELECT 1 FROM projects WHERE id = 'p_catalog_init_rollback'"
    ).fetchone() is None


def test_event_sequence_is_unique_within_each_project(runtime_conn):
    _insert_project(runtime_conn, "p_event_one")
    _insert_project(runtime_conn, "p_event_two")
    _insert_event(
        runtime_conn,
        event_id="event_sequence_one",
        project_id="p_event_one",
        sequence=7,
    )
    runtime_conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        _insert_event(
            runtime_conn,
            event_id="event_sequence_duplicate",
            project_id="p_event_one",
            sequence=7,
        )
    runtime_conn.rollback()

    _insert_event(
        runtime_conn,
        event_id="event_sequence_other_project",
        project_id="p_event_two",
        sequence=7,
    )
    runtime_conn.commit()

    assert runtime_conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE sequence = 7"
    ).fetchone()[0] == 2


def test_operation_idempotency_key_is_unique_within_each_project(runtime_conn):
    _insert_project(runtime_conn, "p_operation_one")
    _insert_project(runtime_conn, "p_operation_two")

    def insert_operation(operation_id, project_id):
        runtime_conn.execute(
            """
            INSERT INTO project_operations (
                operation_id,
                project_id,
                idempotency_key,
                command_revision,
                targets_json,
                payload_json,
                status,
                created_at,
                updated_at
            ) VALUES (?, ?, 'shared-operation-key', 1, '[]', '{}', 'intent', 1, 1)
            """,
            (operation_id, project_id),
        )

    insert_operation("operation_one", "p_operation_one")
    runtime_conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        insert_operation("operation_duplicate", "p_operation_one")
    runtime_conn.rollback()

    insert_operation("operation_other_project", "p_operation_two")
    runtime_conn.commit()

    assert runtime_conn.execute(
        """
        SELECT COUNT(*)
        FROM project_operations
        WHERE idempotency_key = 'shared-operation-key'
        """
    ).fetchone()[0] == 2


def _approval_request(*, approval_id="approval-1", expires_at=100):
    return prdb.ApprovalRequest(
        approval_id=approval_id,
        project_id="p_approval",
        requester_actor_id="owner-1",
        authorization_actor_id="owner-1",
        canonical_action="publish",
        approval_class="publish",
        command_revision=7,
        targets=("C:/work/project/src/a.py", "C:/work/project/src/b.py"),
        batch_id="batch-1",
        batch_items=("operation-a", "operation-b"),
        status="pending",
        expires_at=expires_at,
    )


def _insert_owner_binding(
    conn,
    *,
    project_id="p_approval",
    actor_id="owner-1",
    binding_id="desktop-1",
    surface="desktop",
):
    conn.execute(
        """
        INSERT INTO project_surface_bindings (
            binding_id, project_id, surface, external_binding_id, actor_id,
            created_at
        ) VALUES (?, ?, ?, ?, ?, 1)
        """,
        (binding_id, project_id, surface, f"external-{binding_id}", actor_id),
    )
    conn.commit()


def _authorization_args(request, *, now=13):
    return dict(
        approval_id=request.approval_id,
        project_id=request.project_id,
        authorization_actor_id=request.authorization_actor_id,
        canonical_action=request.canonical_action,
        approval_class=request.approval_class,
        command_revision=request.command_revision,
        targets=request.targets,
        batch_id=request.batch_id,
        batch_items=request.batch_items,
        now=now,
    )


def test_owner_resolution_and_single_consumption_bind_the_full_approval_batch(
    runtime_conn,
):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    request = prdb.create_approval_request(
        runtime_conn, _approval_request(), now=10
    )

    rejected = prdb.resolve_approval(
        runtime_conn,
        approval_id=request.approval_id,
        resolver=ActorContext("other-owner", "desktop", "desktop-2", True),
        outcome="approved",
        now=11,
    )
    assert rejected is None

    approved = prdb.resolve_approval(
        runtime_conn,
        approval_id=request.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="approved",
        now=12,
    )
    assert approved is not None
    assert approved.status == "approved"
    assert approved.resolved_by_actor_id == "owner-1"

    authorization = _authorization_args(request)
    assert prdb.consume_approval_authorization(runtime_conn, **authorization) is True
    assert prdb.consume_approval_authorization(runtime_conn, **authorization) is False


@pytest.mark.parametrize(
    "changed",
    [
        {"project_id": "other-project"},
        {"authorization_actor_id": "other-owner"},
        {"canonical_action": "release"},
        {"approval_class": "production"},
        {"command_revision": 8},
        {"targets": ("C:/work/project/src/a.py",)},
        {
            "targets": (
                "C:/work/project/src/b.py",
                "C:/work/project/src/a.py",
            )
        },
        {"batch_id": "batch-2"},
        {"batch_items": ("operation-a",)},
        {"batch_items": ("operation-b", "operation-a")},
    ],
)
def test_approval_never_authorizes_a_different_project_revision_or_batch(
    runtime_conn, changed
):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    request = prdb.create_approval_request(
        runtime_conn, _approval_request(), now=10
    )
    prdb.resolve_approval(
        runtime_conn,
        approval_id=request.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="approved",
        now=11,
    )
    authorization = _authorization_args(request, now=12)
    authorization.update(changed)

    assert prdb.consume_approval_authorization(runtime_conn, **authorization) is False


@pytest.mark.parametrize(
    ("stored_component", "mismatched_codepoint"),
    [
        pytest.param("ss", 0x00DF, id="eszett-does-not-match-ss"),
        pytest.param("ffi", 0xFB03, id="ligature-does-not-match-ffi"),
    ],
)
def test_approval_target_identity_never_uses_expanding_casefold(
    runtime_conn, stored_component, mismatched_codepoint
):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    request = dataclasses.replace(
        _approval_request(),
        targets=(
            f"C:/work/{stored_component}/a.py",
            f"C:/work/{stored_component}/b.py",
        ),
    )
    created = prdb.create_approval_request(runtime_conn, request, now=10)
    prdb.resolve_approval(
        runtime_conn,
        approval_id=created.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="approved",
        now=11,
    )
    authorization = _authorization_args(created, now=12)
    authorization["targets"] = (
        f"C:/work/{chr(mismatched_codepoint)}/a.py",
        f"C:/work/{stored_component}/b.py",
    )

    assert prdb.consume_approval_authorization(
        runtime_conn, **authorization
    ) is False


def test_approval_target_component_case_mismatch_is_a_different_identity(
    runtime_conn,
):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    request = dataclasses.replace(
        _approval_request(),
        targets=("C:/work/Project/a.py", "C:/work/Project/b.py"),
    )
    created = prdb.create_approval_request(runtime_conn, request, now=10)
    prdb.resolve_approval(
        runtime_conn,
        approval_id=created.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="approved",
        now=11,
    )
    authorization = _authorization_args(created, now=12)
    authorization["targets"] = (
        "C:/work/project/a.py",
        "C:/work/Project/b.py",
    )

    assert prdb.consume_approval_authorization(
        runtime_conn, **authorization
    ) is False


def test_non_owner_and_expired_approval_never_authorize(runtime_conn):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    request = prdb.create_approval_request(
        runtime_conn, _approval_request(expires_at=20), now=10
    )

    assert prdb.resolve_approval(
        runtime_conn,
        approval_id=request.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", False),
        outcome="approved",
        now=11,
    ) is None
    assert prdb.resolve_approval(
        runtime_conn,
        approval_id=request.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="approved",
        now=20,
    ) is None
    assert prdb.consume_approval_authorization(
        runtime_conn, **_authorization_args(request, now=20)
    ) is False

    denied = prdb.create_approval_request(
        runtime_conn,
        dataclasses.replace(
            _approval_request(approval_id="approval-denied"), batch_id="batch-denied"
        ),
        now=21,
    )
    assert prdb.resolve_approval(
        runtime_conn,
        approval_id=denied.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="denied",
        now=22,
    ).status == "denied"
    assert prdb.consume_approval_authorization(
        runtime_conn, **_authorization_args(denied, now=23)
    ) is False


@pytest.mark.parametrize(
    "targets",
    [
        ("C:/work/project/src/a.py", "C:/work/project/src/a.py"),
        ("C:/work/project/src/../secret.txt",),
        (r"C:/work/project/src\..\secret.txt",),
        ("relative/project/src/a.py",),
        ("//server/share/project/src/a.py",),
    ],
)
def test_approval_request_rejects_noncanonical_or_duplicate_targets(
    runtime_conn, targets
):
    _insert_project(runtime_conn, "p_approval")
    request = dataclasses.replace(_approval_request(), targets=targets)

    with pytest.raises(ValueError):
        prdb.create_approval_request(runtime_conn, request, now=10)


def test_concurrent_consumers_cannot_replay_one_approval(tmp_path):
    db_path = tmp_path / "projects.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY, slug TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
            created_at INTEGER NOT NULL, archived INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    prdb.ensure_schema(conn)
    _insert_project(conn, "p_approval")
    _insert_owner_binding(conn)
    request = prdb.create_approval_request(conn, _approval_request(), now=10)
    prdb.resolve_approval(
        conn,
        approval_id=request.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="approved",
        now=11,
    )
    conn.close()

    barrier = threading.Barrier(2)

    def consume_once():
        worker_conn = sqlite3.connect(db_path, timeout=10)
        worker_conn.row_factory = sqlite3.Row
        worker_conn.execute("PRAGMA foreign_keys=ON")
        try:
            barrier.wait()
            return prdb.consume_approval_authorization(
                worker_conn, **_authorization_args(request, now=12)
            )
        finally:
            worker_conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(lambda _: consume_once(), range(2))).count(True) == 1


def test_approval_schema_migration_adds_resolver_and_consumption_columns(
    runtime_conn,
):
    columns = {
        row["name"]
        for row in runtime_conn.execute("PRAGMA table_info(project_approvals)")
    }

    assert {
        "resolved_by_actor_id",
        "consumed_at",
        "canonical_action",
        "authorization_actor_id",
    } <= columns


def test_create_approval_is_idempotent_but_payload_collisions_fail_closed(
    runtime_conn,
):
    _insert_project(runtime_conn, "p_approval")
    request = _approval_request()

    first = prdb.create_approval_request(runtime_conn, request, now=10)
    replay = prdb.create_approval_request(runtime_conn, request, now=11)

    assert replay == first
    with pytest.raises(prdb.ApprovalConflictError):
        prdb.create_approval_request(
            runtime_conn,
            dataclasses.replace(request, canonical_action="release"),
            now=11,
        )
    with pytest.raises(prdb.ApprovalConflictError):
        prdb.create_approval_request(
            runtime_conn,
            dataclasses.replace(request, approval_id="approval-other"),
            now=11,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"canonical_action": "invented-action"},
        {"approval_class": "production"},
        {"resolved_by_actor_id": "owner-1", "resolved_at": 9},
        {"consumed_at": 9},
    ],
)
def test_create_rejects_noncanonical_action_or_prepopulated_state(
    runtime_conn, changes
):
    _insert_project(runtime_conn, "p_approval")
    request = dataclasses.replace(_approval_request(), **changes)

    with pytest.raises(ValueError):
        prdb.create_approval_request(runtime_conn, request, now=10)


def test_same_boundary_distinguishes_authorization_actor_and_canonical_action(
    runtime_conn,
):
    _insert_project(runtime_conn, "p_approval")
    first = _approval_request()
    second = dataclasses.replace(
        first,
        approval_id="approval-2",
        requester_actor_id="owner-2",
        authorization_actor_id="owner-2",
        canonical_action="release",
    )

    prdb.create_approval_request(runtime_conn, first, now=10)
    prdb.create_approval_request(runtime_conn, second, now=10)

    assert runtime_conn.execute(
        "SELECT COUNT(*) FROM project_approvals WHERE project_id = 'p_approval'"
    ).fetchone()[0] == 2


def test_terminal_boundary_requires_a_new_revision_or_batch(runtime_conn):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    first = prdb.create_approval_request(runtime_conn, _approval_request(), now=10)
    prdb.resolve_approval(
        runtime_conn,
        approval_id=first.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="denied",
        now=11,
    )

    with pytest.raises(prdb.ApprovalConflictError):
        prdb.create_approval_request(
            runtime_conn,
            dataclasses.replace(first, approval_id="approval-retry", status="pending"),
            now=12,
        )
    replacement = prdb.create_approval_request(
        runtime_conn,
        dataclasses.replace(
            first,
            approval_id="approval-new-batch",
            batch_id="batch-2",
            status="pending",
            resolved_by_actor_id=None,
            resolved_at=None,
        ),
        now=12,
    )
    assert replacement.batch_id == "batch-2"


def test_resolver_must_be_concrete_and_match_durable_project_binding(runtime_conn):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    request = prdb.create_approval_request(runtime_conn, _approval_request(), now=10)

    class ForgedResolver:
        actor_id = "owner-1"
        is_owner = True

    assert prdb.resolve_approval(
        runtime_conn,
        approval_id=request.approval_id,
        resolver=ForgedResolver(),
        outcome="approved",
        now=11,
    ) is None
    for resolver in (
        ActorContext("owner-1", "system", "desktop-1", True),
        ActorContext("owner-1", "discord", "desktop-1", True),
        ActorContext("owner-1", "desktop", "missing-binding", True),
    ):
        assert prdb.resolve_approval(
            runtime_conn,
            approval_id=request.approval_id,
            resolver=resolver,
            outcome="approved",
            now=11,
        ) is None


@pytest.mark.parametrize(
    "field, value, now",
    [
        ("command_revision", True, 10),
        ("expires_at", True, 0),
        (None, None, True),
    ],
)
def test_approval_rejects_boolean_revisions_and_timestamps(
    runtime_conn, field, value, now
):
    _insert_project(runtime_conn, "p_approval")
    request = _approval_request()
    if field is not None:
        request = dataclasses.replace(request, **{field: value})

    with pytest.raises(ValueError):
        prdb.create_approval_request(runtime_conn, request, now=now)


def test_approved_unconsumed_approval_becomes_expired_at_boundary(runtime_conn):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    request = prdb.create_approval_request(
        runtime_conn, _approval_request(expires_at=20), now=10
    )
    prdb.resolve_approval(
        runtime_conn,
        approval_id=request.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="approved",
        now=11,
    )

    assert prdb.consume_approval_authorization(
        runtime_conn, **_authorization_args(request, now=20)
    ) is False
    assert runtime_conn.execute(
        "SELECT status FROM project_approvals WHERE approval_id = ?",
        (request.approval_id,),
    ).fetchone()[0] == "expired"


def test_nested_write_transaction_rolls_back_caught_inner_failure(runtime_conn):
    _insert_project(runtime_conn, "p_approval")

    with prdb.write_transaction(runtime_conn):
        try:
            with prdb.write_transaction(runtime_conn):
                prdb.create_approval_request(
                    runtime_conn, _approval_request(), now=10
                )
                raise RuntimeError("inner failure")
        except RuntimeError:
            pass
        runtime_conn.execute(
            "UPDATE projects SET name = 'outer survived' WHERE id = 'p_approval'"
        )

    assert runtime_conn.execute(
        "SELECT 1 FROM project_approvals WHERE approval_id = 'approval-1'"
    ).fetchone() is None
    assert runtime_conn.execute(
        "SELECT name FROM projects WHERE id = 'p_approval'"
    ).fetchone()[0] == "outer survived"


def test_consume_composes_with_caller_owned_operation_transaction(runtime_conn):
    _insert_project(runtime_conn, "p_approval")
    _insert_owner_binding(runtime_conn)
    request = prdb.create_approval_request(runtime_conn, _approval_request(), now=10)
    prdb.resolve_approval(
        runtime_conn,
        approval_id=request.approval_id,
        resolver=ActorContext("owner-1", "desktop", "desktop-1", True),
        outcome="approved",
        now=11,
    )

    with pytest.raises(RuntimeError):
        with prdb.write_transaction(runtime_conn):
            assert prdb.consume_approval_authorization(
                runtime_conn, **_authorization_args(request, now=12)
            ) is True
            raise RuntimeError("operation did not commit")

    assert runtime_conn.execute(
        "SELECT consumed_at FROM project_approvals WHERE approval_id = ?",
        (request.approval_id,),
    ).fetchone()[0] is None


def test_new_schema_rejects_unknown_approval_status(runtime_conn):
    _insert_project(runtime_conn, "p_approval")

    with pytest.raises(sqlite3.IntegrityError):
        runtime_conn.execute(
            """
            INSERT INTO project_approvals (
                approval_id, project_id, actor_id, authorization_actor_id,
                canonical_action, approval_class, command_revision,
                targets_json, batch_boundary_json, status, expires_at, created_at
            ) VALUES (
                'invalid-status', 'p_approval', 'owner-1', 'owner-1', 'publish',
                'publish', 7, '["C:/work/project/a"]', '{}', 'unknown', 100, 1
            )
            """
        )
    runtime_conn.rollback()


def _insert_expired_approval_row(
    conn, *, approval_id, resolved_at, resolved_by_actor_id
):
    conn.execute(
        """
        INSERT INTO project_approvals (
            approval_id, project_id, actor_id, authorization_actor_id,
            canonical_action, approval_class, command_revision,
            targets_json, batch_boundary_json, status, expires_at,
            resolved_at, resolved_by_actor_id, created_at
        ) VALUES (
            ?, 'p_approval', 'owner-1', 'owner-1', 'publish', 'publish', 7,
            ?, '{}', 'expired', 100, ?, ?, 1
        )
        """,
        (
            approval_id,
            f'["C:/work/project/{approval_id}"]',
            resolved_at,
            resolved_by_actor_id,
        ),
    )


@pytest.mark.parametrize(
    ("resolved_at", "resolved_by_actor_id"),
    [
        pytest.param(None, "owner-1", id="resolver-without-timestamp"),
        pytest.param(2, None, id="timestamp-without-resolver"),
    ],
)
def test_new_schema_rejects_half_resolved_expired_approvals(
    runtime_conn, resolved_at, resolved_by_actor_id
):
    _insert_project(runtime_conn, "p_approval")

    with pytest.raises(sqlite3.IntegrityError):
        _insert_expired_approval_row(
            runtime_conn,
            approval_id="half-resolved",
            resolved_at=resolved_at,
            resolved_by_actor_id=resolved_by_actor_id,
        )
    runtime_conn.rollback()


def test_new_schema_accepts_both_valid_expired_approval_origins(runtime_conn):
    _insert_project(runtime_conn, "p_approval")

    _insert_expired_approval_row(
        runtime_conn,
        approval_id="expired-from-pending",
        resolved_at=None,
        resolved_by_actor_id=None,
    )
    _insert_expired_approval_row(
        runtime_conn,
        approval_id="expired-from-approved",
        resolved_at=2,
        resolved_by_actor_id="owner-1",
    )

    rows = runtime_conn.execute(
        """
        SELECT approval_id, resolved_at, resolved_by_actor_id
        FROM project_approvals
        ORDER BY approval_id
        """
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("expired-from-approved", 2, "owner-1"),
        ("expired-from-pending", None, None),
    ]


def _create_task1_legacy_approval_db(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
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
        INSERT INTO projects VALUES ('p_legacy', 'legacy', 'Legacy', 1, 0);
        CREATE TABLE project_approvals (
            approval_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
            turn_id TEXT,
            operation_id TEXT,
            actor_id TEXT NOT NULL,
            approval_class TEXT NOT NULL,
            command_revision INTEGER NOT NULL,
            targets_json TEXT NOT NULL,
            batch_boundary_json TEXT NOT NULL,
            status TEXT NOT NULL,
            expires_at INTEGER NOT NULL,
            resolved_at INTEGER,
            created_at INTEGER NOT NULL,
            UNIQUE (project_id, approval_id),
            UNIQUE (
                project_id, command_revision, approval_class, targets_json,
                batch_boundary_json
            )
        );
        INSERT INTO project_approvals VALUES (
            'legacy-approval', 'p_legacy', NULL, NULL, 'owner-legacy', 'publish',
            3, '["C:/legacy/a"]',
            '{"batch_id":"legacy-batch","batch_items":["a"]}',
            'approved', 999, 10, 1
        );
        """
    )
    conn.commit()
    return conn


def test_task1_legacy_approval_rows_survive_additive_migration(tmp_path):
    conn = _create_task1_legacy_approval_db(tmp_path / "projects.db")
    old_columns = (
        "approval_id", "project_id", "turn_id", "operation_id", "actor_id",
        "approval_class", "command_revision", "targets_json",
        "batch_boundary_json", "status", "expires_at", "resolved_at", "created_at",
    )
    before = tuple(conn.execute(
        f"SELECT {', '.join(old_columns)} FROM project_approvals"
    ).fetchone())

    prdb.ensure_schema(conn)

    after = tuple(conn.execute(
        f"SELECT {', '.join(old_columns)} FROM project_approvals"
    ).fetchone())
    migrated = conn.execute(
        """
        SELECT canonical_action, authorization_actor_id,
               resolved_by_actor_id, consumed_at
        FROM project_approvals WHERE approval_id = 'legacy-approval'
        """
    ).fetchone()
    assert after == before
    assert tuple(migrated) == (None, None, None, None)
    conn.close()


def test_two_connections_can_race_task1_approval_migration(tmp_path):
    db_path = tmp_path / "projects.db"
    _create_task1_legacy_approval_db(db_path).close()
    barrier = threading.Barrier(2)

    def migrate():
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            barrier.wait()
            prdb.ensure_schema(conn)
            conn.commit()
            return {
                row["name"]
                for row in conn.execute("PRAGMA table_info(project_approvals)")
            }
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: migrate(), range(2)))

    expected = {
        "canonical_action", "authorization_actor_id",
        "resolved_by_actor_id", "consumed_at",
    }
    assert all(expected <= columns for columns in results)


def test_concurrent_exact_create_returns_one_logical_approval(tmp_path):
    db_path = tmp_path / "projects.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY, slug TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
            created_at INTEGER NOT NULL, archived INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    prdb.ensure_schema(conn)
    _insert_project(conn, "p_approval")
    conn.close()
    barrier = threading.Barrier(2)

    def create():
        worker_conn = sqlite3.connect(db_path, timeout=10)
        worker_conn.row_factory = sqlite3.Row
        try:
            barrier.wait()
            return prdb.create_approval_request(
                worker_conn, _approval_request(), now=10
            )
        finally:
            worker_conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: create(), range(2)))

    assert results[0] == results[1]
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM project_approvals").fetchone()[0] == 1
    conn.close()
