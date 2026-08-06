"""Tests for the per-profile Projects store (hermes_cli/projects_db)."""

from __future__ import annotations

import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from hermes_cli import projects_db as pdb


ALL_SCHEMA_TABLES = {
    "projects",
    "project_folders",
    "project_meta",
    "discovered_repos",
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


def _norm(path: str) -> str:
    return os.path.normpath(os.path.abspath(path))


@pytest.fixture
def conn(tmp_path):
    c = pdb.connect(db_path=tmp_path / "projects.db")
    try:
        yield c
    finally:
        c.close()


def test_record_and_list_discovered_repos(conn):
    n = pdb.record_discovered_repos(conn, [("/www/alpha", "alpha"), ("/www/beta", None)])
    assert n == 2

    rows = {r["root"]: r["label"] for r in pdb.list_discovered_repos(conn)}
    assert rows[_norm("/www/alpha")] == "alpha"
    # Label defaults to the basename when not given.
    assert rows[_norm("/www/beta")] == "beta"


def test_record_discovered_repos_upserts(conn):
    pdb.record_discovered_repos(conn, [("/www/alpha", "old")])
    pdb.record_discovered_repos(conn, [("/www/alpha", "new")])

    rows = pdb.list_discovered_repos(conn)
    assert len(rows) == 1
    assert rows[0]["label"] == "new"


def test_record_discovered_repos_replace_drops_stale_rows(conn):
    pdb.record_discovered_repos(conn, [("/www/alpha", "alpha"), ("/www/beta", "beta")])
    pdb.record_discovered_repos(conn, [("/www/alpha", "fresh")], replace=True)

    rows = {r["root"]: r["label"] for r in pdb.list_discovered_repos(conn)}
    assert rows == {_norm("/www/alpha"): "fresh"}


def test_discovery_policy_change_clears_only_discovered_rows(conn):
    project_id = pdb.create_project(conn, name="Explicit", folders=["/www/explicit"])
    pdb.record_discovered_repos(
        conn, [("/www/scanned", "scanned")], policy_key="policy-a"
    )

    assert pdb.reconcile_discovered_repos_policy(conn, "policy-b") is True
    assert pdb.list_discovered_repos(conn) == []
    assert pdb.get_project(conn, project_id) is not None
    assert pdb.get_discovery_policy_key(conn) == "policy-b"


def test_default_policy_adopts_unversioned_cache_without_clearing(conn):
    pdb.record_discovered_repos(conn, [("/www/scanned", "scanned")])

    assert (
        pdb.reconcile_discovered_repos_policy(
            conn, "default-policy", preserve_unversioned=True
        )
        is False
    )
    assert [row["root"] for row in pdb.list_discovered_repos(conn)] == [
        _norm("/www/scanned")
    ]
    assert pdb.get_discovery_policy_key(conn) == "default-policy"


def test_clear_discovered_repos_records_policy_atomically(conn):
    pdb.record_discovered_repos(conn, [("/www/scanned", "scanned")])

    pdb.clear_discovered_repos(conn, policy_key="disabled")

    assert pdb.list_discovered_repos(conn) == []
    assert pdb.get_discovery_policy_key(conn) == "disabled"


def test_create_get_list(conn):
    pid = pdb.create_project(conn, name="Hermes Agent", folders=["/tmp/hermes"])
    proj = pdb.get_project(conn, pid)

    assert proj is not None
    assert proj.slug == "hermes-agent"
    assert proj.name == "Hermes Agent"
    # First folder becomes primary.
    assert proj.primary_path == _norm("/tmp/hermes")
    assert [f.path for f in proj.folders] == [_norm("/tmp/hermes")]
    assert proj.folders[0].is_primary is True

    # Lookup by slug too.
    assert pdb.get_project(conn, "hermes-agent").id == pid
    assert len(pdb.list_projects(conn)) == 1


def test_delete_project_rejects_any_surface_managed_project(conn):
    from hermes_cli import project_runtime_db as prdb

    project_id = pdb.create_project(conn, name="Managed")
    prdb.bind_surface(
        conn,
        binding_id="managed-desktop",
        project_id=project_id,
        surface="desktop",
        external_binding_id="window-managed",
        actor_id="owner",
        now=1,
    )

    with pytest.raises(
        pdb.ManagedProjectDeleteError,
        match="PROJECT_MANAGED_DELETE_FORBIDDEN",
    ) as raised:
        pdb.delete_project(conn, project_id)

    assert raised.value.code == "PROJECT_MANAGED_DELETE_FORBIDDEN"
    assert pdb.get_project(conn, project_id) is not None


def test_managed_project_archive_requires_completed_lifecycle(conn):
    from hermes_cli import project_runtime_db as prdb

    project_id = pdb.create_project(conn, name="Managed archive")
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="managed-archive-session",
        current_phase="implementation",
        now=1,
    )

    with pytest.raises(
        pdb.ManagedProjectArchiveError,
        match="PROJECT_MANAGED_ARCHIVE_REQUIRES_COMPLETION",
    ):
        pdb.archive_project(conn, project_id)
    assert pdb.get_project(conn, project_id).archived is False

    conn.execute(
        """
        UPDATE project_runtime_state
        SET lifecycle = 'completed'
        WHERE project_id = ?
        """,
        (project_id,),
    )
    conn.commit()

    assert pdb.archive_project(conn, project_id) is True
    assert pdb.get_project(conn, project_id).archived is True


def test_managed_project_name_requires_canonical_command(conn):
    from hermes_cli import project_runtime_db as prdb

    project_id = pdb.create_project(conn, name="Managed name")
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="managed-name-session",
        current_phase="implementation",
        now=1,
    )

    with pytest.raises(
        pdb.ManagedProjectMutationError,
        match="PROJECT_MANAGED_MUTATION_REQUIRES_COMMAND",
    ):
        pdb.update_project(conn, project_id, name="Bypassed name")

    assert pdb.get_project(conn, project_id).name == "Managed name"
    assert conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == 0


def test_managed_delete_table_guard_matches_restrictive_runtime_schema(conn):
    runtime_tables = {
        row["name"]
        for row in conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name LIKE 'project_%'
            """
        )
        if any(
            fk["table"] == "projects"
            and fk["from"] == "project_id"
            and fk["on_delete"].upper() == "RESTRICT"
            for fk in conn.execute(
                f"PRAGMA foreign_key_list({row['name']})"
            )
        )
    }

    assert runtime_tables == set(pdb.MANAGED_PROJECT_RUNTIME_TABLES)


def test_delete_project_preserves_legacy_delete_for_never_managed_project(conn):
    project_id = pdb.create_project(conn, name="Legacy")

    assert pdb.delete_project(conn, project_id) is True
    assert pdb.get_project(conn, project_id) is None


def test_delete_active_legacy_project_clears_active_pointer(conn):
    project_id = pdb.create_project(conn, name="Active legacy")
    pdb.set_active(conn, project_id)

    assert pdb.delete_project(conn, project_id) is True
    assert pdb.get_active_id(conn) is None


def test_delete_unknown_project_is_a_noop(conn):
    assert pdb.delete_project(conn, "project-missing") is False


def test_slug_collision_disambiguates(conn):
    pdb.create_project(conn, name="Hermes Agent")
    pdb.create_project(conn, name="Hermes Agent")
    slugs = sorted(p.slug for p in pdb.list_projects(conn))

    assert slugs == ["hermes-agent", "hermes-agent-2"]


def test_empty_name_rejected(conn):
    with pytest.raises(ValueError):
        pdb.create_project(conn, name="   ")


def test_add_remove_folder_and_primary_repoint(conn):
    pid = pdb.create_project(conn, name="P", folders=["/a"])
    pdb.add_folder(conn, pid, "/b")
    pdb.add_folder(conn, pid, "/c", is_primary=True)

    proj = pdb.get_project(conn, pid)
    assert proj.primary_path == _norm("/c")
    assert {f.path for f in proj.folders} == {
        _norm("/a"),
        _norm("/b"),
        _norm("/c"),
    }

    # Removing the primary repoints to the oldest remaining folder.
    pdb.remove_folder(conn, pid, "/c")
    proj = pdb.get_project(conn, pid)
    assert proj.primary_path == _norm("/a")

    # Removing the last folder clears the primary.
    pdb.remove_folder(conn, pid, "/a")
    pdb.remove_folder(conn, pid, "/b")
    proj = pdb.get_project(conn, pid)
    assert proj.primary_path is None
    assert proj.folders == []


def test_set_primary_requires_existing_folder(conn):
    pid = pdb.create_project(conn, name="P", folders=["/a"])
    assert pdb.set_primary(conn, pid, "/nope") is False
    assert pdb.set_primary(conn, pid, "/a") is True


def test_paths_normalized(conn):
    pid = pdb.create_project(conn, name="P", folders=["/a/b/../c/"])
    proj = pdb.get_project(conn, pid)
    # Trailing slash stripped, .. collapsed.
    assert proj.primary_path == _norm("/a/c")


def test_project_for_path_longest_prefix(conn):
    outer = pdb.create_project(conn, name="Outer", folders=["/www"])
    inner = pdb.create_project(conn, name="Inner", folders=["/www/app"])

    assert pdb.project_for_path(conn, "/www/app/src/x.py").id == inner
    assert pdb.project_for_path(conn, "/www/other").id == outer
    assert pdb.project_for_path(conn, "/elsewhere") is None
    # Segment-wise prefix only: /www/app must not match /www/application.
    assert pdb.project_for_path(conn, "/www/application").id == outer


def test_project_for_path_skips_archived(conn):
    pid = pdb.create_project(conn, name="P", folders=["/www/app"])
    pdb.archive_project(conn, pid)

    assert pdb.project_for_path(conn, "/www/app/src") is None
    # Archived hidden from the default list but visible with include_archived.
    assert pdb.list_projects(conn) == []
    assert len(pdb.list_projects(conn, include_archived=True)) == 1

    pdb.restore_project(conn, pid)
    assert pdb.project_for_path(conn, "/www/app/src").id == pid


def test_active_pointer(conn):
    pid = pdb.create_project(conn, name="P")
    assert pdb.get_active_id(conn) is None

    pdb.set_active(conn, pid)
    assert pdb.get_active_id(conn) == pid

    pdb.set_active(conn, None)
    assert pdb.get_active_id(conn) is None


def test_branch_name_for_is_deterministic():
    proj = pdb.Project(id="p_1", slug="web-app", name="Web App", created_at=0)

    assert pdb.branch_name_for(proj, "t_abc") == "web-app/t_abc"
    assert pdb.branch_name_for(proj, "t_abc", title="Add login!") == "web-app/t_abc-add-login"
    # Stable across calls.
    assert pdb.branch_name_for(proj, "t_abc") == pdb.branch_name_for(proj, "t_abc")


def test_per_profile_isolation(tmp_path):
    # Two distinct DB paths stand in for two profiles' HERMES_HOME.
    a = pdb.connect(db_path=tmp_path / "a" / "projects.db")
    b = pdb.connect(db_path=tmp_path / "b" / "projects.db")
    try:
        pdb.create_project(a, name="Only In A", folders=["/a"])
        pdb.record_discovered_repos(a, [("/a/scanned", "scanned")])

        assert [p.slug for p in pdb.list_projects(a)] == ["only-in-a"]
        assert pdb.list_projects(b) == []
        assert [row["root"] for row in pdb.list_discovered_repos(a)] == [
            _norm("/a/scanned")
        ]
        assert pdb.list_discovered_repos(b) == []
    finally:
        a.close()
        b.close()


def test_db_path_under_hermes_home():
    # Resolves under HERMES_HOME (set by the autouse isolation fixture).
    assert pdb.projects_db_path().name == "projects.db"
    assert os.path.basename(str(pdb.projects_db_path().parent))  # non-empty parent


def test_connect_adds_runtime_schema_without_adopting_archived_project(conn):
    project_id = pdb.create_project(conn, name="Legacy archived")
    pdb.archive_project(conn, project_id)

    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }

    assert "project_runtime_state" in tables
    assert conn.execute(
        "SELECT 1 FROM project_runtime_state WHERE project_id = ?",
        (project_id,),
    ).fetchone() is None


def test_connect_reinitializes_catalog_and_runtime_after_same_path_replacement(
    tmp_path,
):
    db_path = tmp_path / "projects.db"
    first = pdb.connect(db_path=db_path)
    first.close()
    db_path.unlink()

    replacement = pdb.connect(db_path=db_path)
    try:
        tables = {
            row["name"]
            for row in replacement.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        replacement.close()

    assert ALL_SCHEMA_TABLES <= tables


def test_connect_retries_one_transient_journal_mode_lock(
    tmp_path, monkeypatch
):
    import hermes_state

    real_apply_wal = hermes_state.apply_wal_with_fallback
    attempts = []
    sleeps = []

    def locked_once(conn, *, db_label):
        attempts.append(db_label)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        return real_apply_wal(conn, db_label=db_label)

    monkeypatch.setattr(
        hermes_state,
        "apply_wal_with_fallback",
        locked_once,
    )
    monkeypatch.setattr(pdb.time, "sleep", sleeps.append)

    conn = pdb.connect(db_path=tmp_path / "projects.db")
    try:
        assert conn.execute(
            "SELECT 1 FROM project_runtime_state"
        ).fetchone() is None
    finally:
        conn.close()

    assert attempts == ["projects.db", "projects.db"]
    assert len(sleeps) == 1


def test_connect_does_not_retry_non_busy_journal_mode_error(
    tmp_path, monkeypatch
):
    import hermes_state

    attempts = []
    sleeps = []

    def fail_with_unrelated_error(conn, *, db_label):
        attempts.append(db_label)
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(
        hermes_state,
        "apply_wal_with_fallback",
        fail_with_unrelated_error,
    )
    monkeypatch.setattr(pdb.time, "sleep", sleeps.append)

    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        pdb.connect(db_path=tmp_path / "projects.db")

    assert attempts == ["projects.db"]
    assert sleeps == []


def test_connect_bounds_persistent_journal_mode_lock_retries(
    tmp_path, monkeypatch
):
    import hermes_state

    attempts = []

    def remain_locked(conn, *, db_label):
        attempts.append(db_label)
        raise sqlite3.OperationalError("database is busy")

    monkeypatch.setattr(
        hermes_state,
        "apply_wal_with_fallback",
        remain_locked,
    )
    monkeypatch.setattr(pdb.time, "sleep", lambda _delay: None)

    with pytest.raises(sqlite3.OperationalError, match="database is busy"):
        pdb.connect(db_path=tmp_path / "projects.db")

    assert 1 < len(attempts) < 20


def test_wal_reprobe_reenters_canonical_darwin_durability_path(
    tmp_path, monkeypatch
):
    import hermes_state

    db_path = tmp_path / "projects.db"
    seed = sqlite3.connect(db_path)
    try:
        assert seed.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    finally:
        seed.close()

    real_apply_wal = hermes_state.apply_wal_with_fallback
    attempts = []
    sleeps = []

    def locked_once_then_canonical(conn, *, db_label):
        attempts.append(db_label)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        return real_apply_wal(conn, db_label=db_label)

    monkeypatch.setattr(
        hermes_state,
        "apply_wal_with_fallback",
        locked_once_then_canonical,
    )
    monkeypatch.setattr(hermes_state.sys, "platform", "darwin")
    monkeypatch.setattr(pdb.time, "sleep", sleeps.append)

    statements = []
    conn = sqlite3.connect(db_path)
    conn.set_trace_callback(statements.append)
    try:
        assert pdb._apply_wal_with_retry(conn) == "wal"
    finally:
        conn.close()

    normalized = [
        statement.lower().replace(" ", "")
        for statement in statements
    ]
    assert "pragmacheckpoint_fullfsync=1" in normalized
    assert "pragmasynchronous=full" in normalized
    assert "pragmajournal_mode=delete" not in normalized
    assert attempts == ["projects.db", "projects.db"]
    assert sleeps == []


def test_connect_reprobe_keeps_existing_wal_without_sleep_or_downgrade(
    tmp_path, monkeypatch
):
    import hermes_state

    db_path = tmp_path / "projects.db"
    seed = sqlite3.connect(db_path)
    try:
        assert seed.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    finally:
        seed.close()

    real_apply_wal = hermes_state.apply_wal_with_fallback
    attempts = []
    sleeps = []

    def collide_with_existing_wal(conn, *, db_label):
        attempts.append(db_label)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        return real_apply_wal(conn, db_label=db_label)

    monkeypatch.setattr(
        hermes_state,
        "apply_wal_with_fallback",
        collide_with_existing_wal,
    )
    monkeypatch.setattr(pdb.time, "sleep", sleeps.append)

    conn = pdb.connect(db_path=db_path)
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        conn.close()

    assert mode == "wal"
    assert ALL_SCHEMA_TABLES <= tables
    assert attempts == ["projects.db", "projects.db"]
    assert sleeps == []


def test_concurrent_first_opens_both_receive_complete_schema(
    tmp_path, monkeypatch
):
    import hermes_state

    real_apply_wal = hermes_state.apply_wal_with_fallback

    for run in range(12):
        db_path = tmp_path / f"projects-{run}.db"
        connect_barrier = threading.Barrier(2)
        journal_barrier = threading.Barrier(2)
        call_lock = threading.Lock()
        journal_calls = 0

        def synchronize_first_journal_attempt(conn, *, db_label):
            nonlocal journal_calls
            with call_lock:
                journal_calls += 1
                synchronize = journal_calls <= 2
            if synchronize:
                journal_barrier.wait(timeout=5)
            return real_apply_wal(conn, db_label=db_label)

        monkeypatch.setattr(
            hermes_state,
            "apply_wal_with_fallback",
            synchronize_first_journal_attempt,
        )

        def open_and_read_schema():
            connect_barrier.wait(timeout=5)
            conn = pdb.connect(db_path=db_path)
            try:
                tables = {
                    row["name"]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
                return tables, mode
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda _index: open_and_read_schema(),
                    range(2),
                )
            )

        for tables, mode in results:
            assert ALL_SCHEMA_TABLES <= tables
            assert mode in {"wal", "delete"}
        assert len({mode for _tables, mode in results}) == 1
