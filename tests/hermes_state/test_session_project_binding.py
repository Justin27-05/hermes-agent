"""SessionDB's explicit, recoverable ProjectRuntime projection."""

from __future__ import annotations

import json
import sqlite3

import pytest

from hermes_state import SCHEMA_SQL, SessionDB


@pytest.fixture
def db(tmp_path):
    session_db = SessionDB(db_path=tmp_path / "state.db")
    try:
        yield session_db
    finally:
        session_db.close()


def test_fresh_session_exposes_a_null_project_projection(db):
    db.create_session("session-one", "desktop", cwd="C:/work/project")

    assert db.get_session("session-one")["project_id"] is None


def test_explicit_project_projection_is_set_once_and_idempotent(db):
    db.create_session("session-one", "desktop")

    assert db.set_session_project_id("session-one", "p_one") is True
    assert db.set_session_project_id("session-one", "p_one") is True
    assert db.set_session_project_id("session-one", "p_two") is False
    assert db.set_session_project_id("session-one", None) is False
    assert db.get_session("session-one")["project_id"] == "p_one"


@pytest.mark.parametrize(
    ("session_id", "project_id"),
    [
        ("", "p_one"),
        ("session-one", ""),
        ("session-one", True),
        ("session-one", []),
        ([], "p_one"),
    ],
)
def test_invalid_projection_inputs_fail_without_mutation(
    db, session_id, project_id
):
    db.create_session("session-one", "desktop")

    assert db.set_session_project_id(session_id, project_id) is False
    assert db.get_session("session-one")["project_id"] is None


def test_missing_session_cannot_create_a_projection(db):
    assert db.set_session_project_id("missing", "p_one") is False


def test_legacy_session_migration_preserves_existing_values_without_inference(
    tmp_path,
):
    db_path = tmp_path / "legacy-state.db"
    legacy_schema = SCHEMA_SQL.replace("    project_id TEXT,\n", "")
    conn = sqlite3.connect(db_path)
    conn.executescript(legacy_schema)
    conn.execute(
        """
        INSERT INTO sessions (
            id, source, user_id, session_key, chat_id, chat_type,
            display_name, origin_json, started_at, cwd, git_repo_root
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy",
            "discord",
            "owner-1",
            "agent:main:discord:channel:one",
            "channel-one",
            "channel",
            "Legacy channel",
            json.dumps({"platform": "discord", "chat_id": "channel-one"}),
            123.5,
            "C:/work/project",
            "C:/work/project",
        ),
    )
    before = tuple(conn.execute(
        """
        SELECT id, source, user_id, session_key, chat_id, chat_type,
               display_name, origin_json, started_at, cwd, git_repo_root
        FROM sessions WHERE id = 'legacy'
        """
    ).fetchone())
    conn.commit()
    conn.close()

    migrated = SessionDB(db_path=db_path)
    try:
        row = migrated.get_session("legacy")
        after = tuple(row[name] for name in (
            "id",
            "source",
            "user_id",
            "session_key",
            "chat_id",
            "chat_type",
            "display_name",
            "origin_json",
            "started_at",
            "cwd",
            "git_repo_root",
        ))
        assert after == before
        assert row["project_id"] is None
    finally:
        migrated.close()


def test_parent_cwd_origin_and_project_do_not_infer_child_project(db):
    db.create_session(
        "parent",
        "discord",
        cwd="C:/work/project",
        session_key="agent:main:discord:channel:one",
    )
    db.record_gateway_session_peer(
        "parent",
        source="discord",
        user_id="owner-1",
        session_key="agent:main:discord:channel:one",
        chat_id="channel-one",
        chat_type="channel",
        origin_json=json.dumps(
            {"platform": "discord", "chat_id": "channel-one"}
        ),
    )
    assert db.set_session_project_id("parent", "p_one") is True

    db.create_session(
        "child",
        "discord",
        parent_session_id="parent",
        cwd="C:/work/project",
    )

    child = db.get_session("child")
    assert child["cwd"] == "C:/work/project"
    assert child["project_id"] is None


def test_generic_compression_publication_does_not_inherit_project_id(db):
    db.create_session(
        "parent",
        "desktop",
        cwd="C:/work/project",
        session_key="agent:main:desktop:dm:owner",
    )
    assert db.set_session_project_id("parent", "p_one") is True

    db.publish_compression_child(
        parent_session_id="parent",
        child_session_id="child",
        source="desktop",
        messages=[{"role": "user", "content": "compressed handoff"}],
        require_compression_lease=False,
    )

    assert db.get_session("parent")["project_id"] == "p_one"
    assert db.get_session("child")["project_id"] is None
    assert db.set_session_project_id("child", "p_one") is True
    assert db.get_session("child")["project_id"] == "p_one"
