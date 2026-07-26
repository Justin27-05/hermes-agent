"""Behavioral tests for canonical ProjectRuntime conversation lineage."""

from __future__ import annotations

import dataclasses
import importlib
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from hermes_cli import project_runtime_db as prdb


def _lineage():
    return importlib.import_module("hermes_cli.project_lineage")


def _connect(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _create_db(db_path):
    conn = _connect(db_path)
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
    return conn


def _insert_project(conn, project_id):
    conn.execute(
        """
        INSERT INTO projects (id, slug, name, created_at, archived)
        VALUES (?, ?, ?, 1, 0)
        """,
        (project_id, project_id, project_id),
    )
    conn.commit()


def _adopt(
    conn,
    project_id="p_one",
    conversation_id="conversation-root",
    *,
    current_phase="implementation",
    now=10,
):
    return prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id=conversation_id,
        current_phase=current_phase,
        now=now,
    )


def _state_snapshot(conn, project_id):
    state = conn.execute(
        "SELECT * FROM project_runtime_state WHERE project_id = ?",
        (project_id,),
    ).fetchone()
    conversations = conn.execute(
        """
        SELECT * FROM project_conversations
        WHERE project_id = ?
        ORDER BY conversation_id
        """,
        (project_id,),
    ).fetchall()
    return tuple(state) if state else None, [tuple(row) for row in conversations]


def test_pure_lineage_values_are_frozen_and_validate_root_and_child_shapes():
    lineage = _lineage()

    root = lineage.make_root_conversation(
        project_id="p_one",
        conversation_id="root",
        created_at=1,
    )
    child = lineage.make_child_conversation(
        project_id="p_one",
        conversation_id="child",
        parent_conversation_id="root",
        root_conversation_id="root",
        created_at=2,
    )

    assert dataclasses.asdict(root) == {
        "conversation_id": "root",
        "project_id": "p_one",
        "parent_conversation_id": None,
        "root_conversation_id": "root",
        "created_at": 1,
    }
    assert child.parent_conversation_id == "root"
    assert child.root_conversation_id == "root"
    with pytest.raises(dataclasses.FrozenInstanceError):
        root.root_conversation_id = "other"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"project_id": "", "conversation_id": "root", "created_at": 1},
        {"project_id": "p", "conversation_id": "", "created_at": 1},
        {"project_id": "p", "conversation_id": "root", "created_at": True},
    ],
)
def test_pure_root_validator_rejects_empty_or_noncanonical_values(kwargs):
    with pytest.raises(ValueError):
        _lineage().make_root_conversation(**kwargs)


def test_adoption_atomically_creates_one_self_root_and_active_state(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")

    root = _adopt(conn)

    state = prdb.runtime_state_for_project(conn, "p_one")
    assert root.parent_conversation_id is None
    assert root.root_conversation_id == root.conversation_id
    assert state is not None
    assert state.lifecycle == "active"
    assert state.current_phase == "implementation"
    assert state.version == 0
    assert state.conversation_root_id == "conversation-root"
    assert state.conversation_tip_id == "conversation-root"
    assert prdb.lineage_for_project(conn, project_id="p_one") == (root,)
    conn.close()


def test_duplicate_adoption_is_structured_conflict_and_changes_nothing(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    _adopt(conn)
    before = _state_snapshot(conn, "p_one")

    with pytest.raises(prdb.LineageConflictError):
        _adopt(conn, conversation_id="second-root", now=20)

    assert _state_snapshot(conn, "p_one") == before
    conn.close()


def test_compatibility_adoption_rejects_unequal_or_dangling_ids(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")

    with pytest.raises(ValueError):
        prdb.create_runtime_state(
            conn,
            project_id="p_one",
            current_phase="implementation",
            conversation_root_id="root",
            conversation_tip_id="tip",
            updated_at=1,
        )

    assert _state_snapshot(conn, "p_one") == (None, [])
    state = prdb.create_runtime_state(
        conn,
        project_id="p_one",
        current_phase="implementation",
        conversation_root_id="root",
        conversation_tip_id="root",
        updated_at=2,
    )
    assert state.conversation_root_id == state.conversation_tip_id == "root"
    assert len(prdb.lineage_for_project(conn, project_id="p_one")) == 1
    conn.close()


def test_partial_root_index_rejects_a_second_root(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    _adopt(conn)

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            INSERT INTO project_conversations (
                conversation_id, project_id, parent_conversation_id,
                root_conversation_id, created_at
            ) VALUES ('second', 'p_one', NULL, 'second', 2)
            """
        )
    conn.rollback()

    assert [row.conversation_id for row in prdb.lineage_for_project(
        conn, project_id="p_one"
    )] == ["conversation-root"]
    conn.close()


def test_migration_fails_closed_on_malformed_legacy_root(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    conn.execute("DROP INDEX idx_project_conversations_one_root")
    _insert_project(conn, "p_one")
    conn.execute(
        """
        INSERT INTO project_conversations (
            conversation_id, project_id, parent_conversation_id,
            root_conversation_id, created_at
        ) VALUES ('bad-root', 'p_one', NULL, NULL, 1)
        """
    )
    conn.commit()

    with pytest.raises(prdb.LineageMigrationError):
        prdb.ensure_schema(conn)

    assert conn.execute(
        "SELECT root_conversation_id FROM project_conversations"
    ).fetchone()[0] is None
    conn.close()


def test_migration_fails_closed_on_a_self_parented_legacy_child(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    conn.execute(
        """
        INSERT INTO project_conversations (
            conversation_id, project_id, parent_conversation_id,
            root_conversation_id, created_at
        ) VALUES ('root', 'p_one', NULL, 'root', 1)
        """
    )
    conn.execute(
        """
        INSERT INTO project_conversations (
            conversation_id, project_id, parent_conversation_id,
            root_conversation_id, created_at
        ) VALUES ('loop', 'p_one', 'loop', 'root', 2)
        """
    )
    conn.commit()

    with pytest.raises(prdb.LineageMigrationError):
        prdb.ensure_schema(conn)

    assert conn.execute(
        """
        SELECT parent_conversation_id FROM project_conversations
        WHERE conversation_id = 'loop'
        """
    ).fetchone()[0] == "loop"
    conn.close()


def test_two_connections_racing_adoption_have_exactly_one_winner(tmp_path):
    db_path = tmp_path / "projects.db"
    conn = _create_db(db_path)
    _insert_project(conn, "p_one")
    conn.close()
    barrier = threading.Barrier(2)

    def attempt(conversation_id):
        worker = _connect(db_path)
        prdb.ensure_schema(worker)
        try:
            barrier.wait(timeout=5)
            try:
                return _adopt(worker, conversation_id=conversation_id)
            except prdb.LineageConflictError:
                return None
        finally:
            worker.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ("root-a", "root-b")))

    check = _connect(db_path)
    assert len([result for result in results if result is not None]) == 1
    rows = prdb.lineage_for_project(check, project_id="p_one")
    state = prdb.runtime_state_for_project(check, "p_one")
    assert len(rows) == 1
    assert state is not None
    assert state.conversation_root_id == state.conversation_tip_id
    assert state.conversation_tip_id == rows[0].conversation_id
    check.close()


def test_child_tip_cas_retains_root_and_increments_state_version(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    _adopt(conn, now=10)

    child = prdb.advance_conversation_tip(
        conn,
        project_id="p_one",
        expected_tip_id="conversation-root",
        child_conversation_id="conversation-child",
        now=20,
    )

    state = prdb.runtime_state_for_project(conn, "p_one")
    assert child is not None
    assert child.parent_conversation_id == "conversation-root"
    assert child.root_conversation_id == "conversation-root"
    assert state is not None
    assert state.conversation_root_id == "conversation-root"
    assert state.conversation_tip_id == "conversation-child"
    assert state.version == 1
    assert state.updated_at == 20
    conn.close()


def test_stale_child_tip_cas_leaves_state_and_all_conversations_unchanged(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    _adopt(conn)
    assert prdb.advance_conversation_tip(
        conn,
        project_id="p_one",
        expected_tip_id="conversation-root",
        child_conversation_id="winner",
        now=20,
    )
    before = _state_snapshot(conn, "p_one")

    result = prdb.advance_conversation_tip(
        conn,
        project_id="p_one",
        expected_tip_id="conversation-root",
        child_conversation_id="orphan",
        now=30,
    )

    assert result is None
    assert _state_snapshot(conn, "p_one") == before
    conn.close()


def test_nested_stale_child_rolls_back_only_provisional_lineage_work(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    _adopt(conn)
    assert prdb.advance_conversation_tip(
        conn,
        project_id="p_one",
        expected_tip_id="conversation-root",
        child_conversation_id="winner",
        now=20,
    )

    with prdb.write_transaction(conn):
        conn.execute(
            "UPDATE projects SET name = 'outer survives' WHERE id = 'p_one'"
        )
        result = prdb.advance_conversation_tip(
            conn,
            project_id="p_one",
            expected_tip_id="conversation-root",
            child_conversation_id="orphan",
            now=30,
        )
        assert result is None

    assert conn.execute(
        "SELECT name FROM projects WHERE id = 'p_one'"
    ).fetchone()[0] == "outer survives"
    assert conn.execute(
        "SELECT 1 FROM project_conversations WHERE conversation_id = 'orphan'"
    ).fetchone() is None
    conn.close()


def test_two_connections_racing_child_cas_leave_no_loser_orphan(tmp_path):
    db_path = tmp_path / "projects.db"
    conn = _create_db(db_path)
    _insert_project(conn, "p_one")
    _adopt(conn)
    conn.close()
    barrier = threading.Barrier(2)

    def attempt(child_id):
        worker = _connect(db_path)
        prdb.ensure_schema(worker)
        try:
            barrier.wait(timeout=5)
            return prdb.advance_conversation_tip(
                worker,
                project_id="p_one",
                expected_tip_id="conversation-root",
                child_conversation_id=child_id,
                now=20,
            )
        finally:
            worker.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ("child-a", "child-b")))

    check = _connect(db_path)
    children = check.execute(
        """
        SELECT conversation_id FROM project_conversations
        WHERE project_id = 'p_one' AND parent_conversation_id IS NOT NULL
        """
    ).fetchall()
    assert len([result for result in results if result is not None]) == 1
    assert len(children) == 1
    assert prdb.runtime_state_for_project(
        check, "p_one"
    ).conversation_tip_id == children[0]["conversation_id"]
    check.close()


def test_surface_bindings_are_immutable_idempotent_and_project_scoped(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    _insert_project(conn, "p_two")
    _adopt(conn)

    desktop = prdb.bind_surface(
        conn,
        binding_id="binding-desktop",
        project_id="p_one",
        surface="desktop",
        external_binding_id="window-1",
        actor_id="owner-1",
        now=10,
    )
    discord = prdb.bind_surface(
        conn,
        binding_id="binding-discord",
        project_id="p_one",
        surface="discord",
        external_binding_id="channel-1",
        actor_id="owner-1",
        now=11,
    )
    assert prdb.bind_surface(
        conn,
        binding_id="binding-desktop",
        project_id="p_one",
        surface="desktop",
        external_binding_id="window-1",
        actor_id="owner-1",
        now=10,
    ) == desktop
    assert prdb.binding_for_id(
        conn, project_id="p_one", binding_id=discord.binding_id
    ) == discord
    assert prdb.binding_for_id(
        conn, project_id="p_two", binding_id=discord.binding_id
    ) is None

    before = tuple(conn.execute(
        "SELECT * FROM project_surface_bindings WHERE binding_id = ?",
        ("binding-desktop",),
    ).fetchone())
    collisions = (
        dict(
            binding_id="different-id",
            project_id="p_two",
            surface="desktop",
            external_binding_id="window-1",
            actor_id="owner-2",
            now=20,
        ),
        dict(
            binding_id="binding-desktop",
            project_id="p_one",
            surface="desktop",
            external_binding_id="other-window",
            actor_id="owner-1",
            now=20,
        ),
        dict(
            binding_id="binding-desktop",
            project_id="p_one",
            surface="desktop",
            external_binding_id="window-1",
            actor_id="different-owner",
            now=10,
        ),
    )
    for kwargs in collisions:
        with pytest.raises(prdb.BindingConflictError):
            prdb.bind_surface(conn, **kwargs)
        assert tuple(conn.execute(
            "SELECT * FROM project_surface_bindings WHERE binding_id = ?",
            ("binding-desktop",),
        ).fetchone()) == before

    assert {binding.surface for binding in prdb.bindings_for_project(
        conn, project_id="p_one"
    )} == {"desktop", "discord"}
    conn.close()


@pytest.mark.parametrize("surface", ["telegram", "", None, [], {}])
def test_surface_binding_rejects_unsupported_or_malformed_surfaces(
    tmp_path, surface
):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")

    with pytest.raises(ValueError):
        prdb.bind_surface(
            conn,
            binding_id="binding",
            project_id="p_one",
            surface=surface,
            external_binding_id="external",
            actor_id="owner",
            now=1,
        )

    assert conn.execute(
        "SELECT COUNT(*) FROM project_surface_bindings"
    ).fetchone()[0] == 0
    conn.close()


def test_per_turn_origin_fk_rejects_a_binding_from_another_project(tmp_path):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    _insert_project(conn, "p_two")
    _adopt(conn, "p_one", "root-one")
    _adopt(conn, "p_two", "root-two")
    prdb.bind_surface(
        conn,
        binding_id="binding-two",
        project_id="p_two",
        surface="discord",
        external_binding_id="channel-two",
        actor_id="owner-two",
        now=1,
    )

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            INSERT INTO project_turns (
                turn_id, project_id, sequence, idempotency_key, payload_json,
                origin_binding_id, status, created_at, updated_at
            ) VALUES (
                'turn-one', 'p_one', 1, 'key-one', '{}',
                'binding-two', 'queued', 1, 1
            )
            """
        )
    conn.rollback()

    assert conn.execute(
        "SELECT COUNT(*) FROM project_turns"
    ).fetchone()[0] == 0
    conn.close()


def test_desktop_discord_compression_desktop_keeps_one_ordered_lineage(
    tmp_path,
):
    conn = _create_db(tmp_path / "projects.db")
    _insert_project(conn, "p_one")
    _adopt(conn, "p_one", "root")
    for binding in (
        dict(
            binding_id="desktop-binding",
            project_id="p_one",
            surface="desktop",
            external_binding_id="window-one",
            actor_id="owner",
            now=1,
        ),
        dict(
            binding_id="discord-binding",
            project_id="p_one",
            surface="discord",
            external_binding_id="channel-one",
            actor_id="owner",
            now=2,
        ),
    ):
        prdb.bind_surface(conn, **binding)

    def record_origin(sequence, binding_id):
        conn.execute(
            """
            INSERT INTO project_turns (
                turn_id, project_id, sequence, idempotency_key, payload_json,
                origin_binding_id, status, created_at, updated_at
            ) VALUES (?, 'p_one', ?, ?, '{}', ?, 'queued', ?, ?)
            """,
            (
                f"turn-{sequence}",
                sequence,
                f"key-{sequence}",
                binding_id,
                sequence,
                sequence,
            ),
        )

    record_origin(1, "desktop-binding")
    record_origin(2, "discord-binding")
    child = prdb.advance_conversation_tip(
        conn,
        project_id="p_one",
        expected_tip_id="root",
        child_conversation_id="compressed-child",
        now=3,
    )
    record_origin(3, "desktop-binding")
    conn.commit()

    state = prdb.runtime_state_for_project(conn, "p_one")
    origins = conn.execute(
        """
        SELECT origin_binding_id FROM project_turns
        WHERE project_id = 'p_one'
        ORDER BY sequence
        """
    ).fetchall()
    assert child is not None
    assert state is not None
    assert state.conversation_root_id == "root"
    assert state.conversation_tip_id == "compressed-child"
    assert [row["origin_binding_id"] for row in origins] == [
        "desktop-binding",
        "discord-binding",
        "desktop-binding",
    ]
    assert {binding.surface for binding in prdb.bindings_for_project(
        conn, project_id="p_one"
    )} == {"desktop", "discord"}
    conn.close()
