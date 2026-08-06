"""Tests for the `hermes project` CLI dispatch (hermes_cli/projects_cmd)."""

from __future__ import annotations

import argparse

import pytest

from hermes_cli import projects_cmd
from hermes_cli import projects_db as pdb


def _run(argv):
    """Build the project subparser, parse argv, and dispatch. Returns rc."""
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    p = projects_cmd.build_parser(sub)
    p.set_defaults(func=projects_cmd.projects_command)
    args = parser.parse_args(["project", *argv])
    return projects_cmd.projects_command(args)


def test_create_list_show(capsys, tmp_path):
    assert _run(["create", "My App", str(tmp_path), "--use"]) == 0
    out = capsys.readouterr().out
    assert "Created project" in out

    with pdb.connect_closing() as conn:
        projects = pdb.list_projects(conn)
        assert len(projects) == 1
        assert projects[0].name == "My App"
        # --use set it active.
        assert pdb.get_active_id(conn) == projects[0].id

    assert _run(["list"]) == 0
    assert "my-app" in capsys.readouterr().out

    assert _run(["show", "my-app"]) == 0
    assert "My App" in capsys.readouterr().out


def test_add_remove_folder(tmp_path):
    _run(["create", "P", str(tmp_path / "a")])
    assert _run(["add-folder", "p", str(tmp_path / "b")]) == 0

    with pdb.connect_closing() as conn:
        proj = pdb.get_project(conn, "p")
        assert len(proj.folders) == 2

    assert _run(["remove-folder", "p", str(tmp_path / "b")]) == 0
    with pdb.connect_closing() as conn:
        assert len(pdb.get_project(conn, "p").folders) == 1


def test_rename_and_archive(tmp_path):
    _run(["create", "Old Name", str(tmp_path)])
    assert _run(["rename", "old-name", "New Name"]) == 0
    with pdb.connect_closing() as conn:
        assert pdb.get_project(conn, "old-name").name == "New Name"

    assert _run(["archive", "old-name"]) == 0
    with pdb.connect_closing() as conn:
        assert pdb.list_projects(conn) == []
        assert len(pdb.list_projects(conn, include_archived=True)) == 1

    assert _run(["restore", "old-name"]) == 0
    with pdb.connect_closing() as conn:
        assert len(pdb.list_projects(conn)) == 1


def test_use_clear(tmp_path):
    _run(["create", "P", str(tmp_path)])
    _run(["use", "p"])
    with pdb.connect_closing() as conn:
        assert pdb.get_active_id(conn) is not None

    _run(["use"])
    with pdb.connect_closing() as conn:
        assert pdb.get_active_id(conn) is None


def test_unknown_project_returns_error(capsys, tmp_path):
    assert _run(["show", "nope"]) == 1
    assert "no such project" in capsys.readouterr().err


def test_delete_legacy_project_and_reject_managed_project(
    capsys, tmp_path
):
    _run(["create", "Legacy", str(tmp_path / "legacy"), "--use"])
    assert _run(["delete", "legacy"]) == 0
    assert "Deleted legacy" in capsys.readouterr().out
    with pdb.connect_closing() as conn:
        assert pdb.get_project(conn, "legacy") is None
        assert pdb.get_active_id(conn) is None

    _run(["create", "Managed", str(tmp_path / "managed")])
    with pdb.connect_closing() as conn:
        from hermes_cli import project_runtime_db as prdb

        project = pdb.get_project(conn, "managed")
        prdb.bind_surface(
            conn,
            binding_id="desktop-managed",
            project_id=project.id,
            surface="desktop",
            external_binding_id="window-managed",
            actor_id="owner-1",
            now=1,
        )

    assert _run(["delete", "managed"]) == 2
    error = capsys.readouterr().err
    assert "PROJECT_MANAGED_DELETE_FORBIDDEN" in error
    assert "archive" in error.lower()
    assert "sqlite" not in error.lower()
    with pdb.connect_closing() as conn:
        assert pdb.get_project(conn, "managed") is not None


def test_managed_rename_and_active_archive_require_canonical_lifecycle(
    capsys, tmp_path
):
    _run(["create", "Managed lifecycle", str(tmp_path / "managed")])
    with pdb.connect_closing() as conn:
        from hermes_cli import project_runtime_db as prdb

        project = pdb.get_project(conn, "managed-lifecycle")
        prdb.create_project_conversation(
            conn,
            project_id=project.id,
            conversation_id="managed-cli-session",
            current_phase="implementation",
            now=1,
        )

    assert _run(
        ["rename", "managed-lifecycle", "Bypassed"]
    ) == 2
    assert (
        "PROJECT_MANAGED_MUTATION_REQUIRES_COMMAND"
        in capsys.readouterr().err
    )
    assert _run(["archive", "managed-lifecycle"]) == 2
    assert (
        "PROJECT_MANAGED_ARCHIVE_REQUIRES_COMPLETION"
        in capsys.readouterr().err
    )
    with pdb.connect_closing() as conn:
        project = pdb.get_project(conn, "managed-lifecycle")
        assert project.name == "Managed lifecycle"
        assert project.archived is False
