"""Canonical ProjectRuntime event, delivery, and artifact contracts."""

from __future__ import annotations

from dataclasses import fields
import itertools
import json
from pathlib import Path
import sqlite3

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db


@pytest.fixture
def event_env(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    conn = projects_db.connect(tmp_path / "projects.db")
    project_id = projects_db.create_project(
        conn,
        name="Canonical events",
        folders=(str(root),),
    )
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="event-root",
        current_phase="implementation",
        now=1,
    )
    for binding_id, surface in (
        ("desktop-owner", "desktop"),
        ("discord-owner", "discord"),
    ):
        prdb.bind_surface(
            conn,
            binding_id=binding_id,
            project_id=project_id,
            surface=surface,
            external_binding_id=f"{surface}-target",
            actor_id="owner-1",
            now=1,
        )
    counter = itertools.count(1)
    from hermes_cli.project_events import ProjectEventOutbox

    outbox = ProjectEventOutbox(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"{kind}-{next(counter)}",
    )
    yield {
        "conn": conn,
        "project_id": project_id,
        "root": root,
        "outbox": outbox,
    }
    conn.close()


def test_events_are_monotonic_gap_free_immutable_and_redacted(event_env):
    from hermes_cli.project_events import ProjectEvent

    outbox = event_env["outbox"]
    project_id = event_env["project_id"]
    first = outbox.append_event(
        project_id,
        "run.started",
        {
            "status": "running",
            "api_token": "must-not-persist",
            "nested": {
                "authorization": "Bearer secret",
                "private_key": "must-also-not-persist",
            },
        },
    )
    second = outbox.append_event(
        project_id,
        "run.progress",
        {"percent": 50},
    )
    third = outbox.append_event(
        project_id,
        "run.completed",
        {"status": "succeeded"},
    )

    assert tuple(field.name for field in fields(ProjectEvent)) == (
        "event_id",
        "project_id",
        "sequence",
        "kind",
        "turn_id",
        "payload",
        "created_at",
    )
    assert [first.sequence, second.sequence, third.sequence] == [1, 2, 3]
    assert first.created_at == "1970-01-01T00:01:40Z"
    assert first.payload["api_token"] == "[REDACTED]"
    assert first.payload["nested"]["authorization"] == "[REDACTED]"
    assert first.payload["nested"]["private_key"] == "[REDACTED]"

    page_one = outbox.events_after(project_id, 0, 2)
    page_two = outbox.events_after(
        project_id,
        page_one[-1].sequence,
        2,
    )
    assert page_one == (first, second)
    assert page_two == (third,)
    assert outbox.events_after(project_id, third.sequence, 2) == ()

    raw = event_env["conn"].execute(
        """
        SELECT payload_json FROM project_events
        WHERE project_id = ? AND sequence = 1
        """,
        (project_id,),
    ).fetchone()[0]
    assert "must-not-persist" not in raw
    assert "Bearer secret" not in raw
    assert json.loads(raw)["api_token"] == "[REDACTED]"


def test_event_and_all_binding_obligations_commit_atomically(event_env):
    conn = event_env["conn"]
    project_id = event_env["project_id"]
    outbox = event_env["outbox"]

    event = outbox.append_event(
        project_id,
        "project.changed",
        {"field": "name"},
    )
    obligations = conn.execute(
        """
        SELECT binding_id, event_id, status, cursor, attempt_count
        FROM project_deliveries
        WHERE project_id = ?
        ORDER BY binding_id
        """,
        (project_id,),
    ).fetchall()
    assert [tuple(row) for row in obligations] == [
        ("desktop-owner", event.event_id, "pending", None, 0),
        ("discord-owner", event.event_id, "pending", None, 0),
    ]

    conn.execute(
        """
        CREATE TRIGGER reject_discord_obligation
        BEFORE INSERT ON project_deliveries
        WHEN NEW.binding_id = 'discord-owner'
        BEGIN
            SELECT RAISE(ABORT, 'delivery rejected');
        END
        """
    )
    before_events = conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError, match="delivery rejected"):
        outbox.append_event(
            project_id,
            "surface.sync_pending",
            {"surface": "discord"},
        )
    assert conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == before_events


def test_fresh_delivery_schema_keeps_lease_retry_and_remote_state_separate(
    event_env,
):
    conn = event_env["conn"]
    event = event_env["outbox"].append_event(
        event_env["project_id"],
        "run.progress",
        {"step": 1},
    )

    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(project_deliveries)")
    }
    row = conn.execute(
        """
        SELECT status, cursor, lease_expires_at,
               remote_message_ids_json, next_attempt_at, last_error_code
        FROM project_deliveries
        WHERE binding_id = 'discord-owner' AND event_id = ?
        """,
        (event.event_id,),
    ).fetchone()

    assert {
        "lease_expires_at",
        "remote_message_ids_json",
        "next_attempt_at",
        "last_error_code",
    } <= columns
    assert tuple(row) == ("pending", None, None, None, None, None)


def test_legacy_in_flight_cursor_migrates_once_to_delivery_lease(event_env):
    conn = event_env["conn"]
    event = event_env["outbox"].append_event(
        event_env["project_id"],
        "run.progress",
        {"step": 1},
    )
    conn.execute(
        """
        UPDATE project_deliveries
        SET status = 'in_flight', cursor = 345, attempt_count = 2
        WHERE binding_id = 'discord-owner' AND event_id = ?
        """,
        (event.event_id,),
    )
    conn.commit()
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute(
        "ALTER TABLE project_deliveries RENAME TO delivery_schema_new"
    )
    conn.execute(
        """
        CREATE TABLE project_deliveries (
            delivery_id   TEXT PRIMARY KEY,
            project_id    TEXT NOT NULL,
            binding_id    TEXT NOT NULL,
            event_id      TEXT NOT NULL,
            status        TEXT NOT NULL,
            cursor        INTEGER,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            updated_at    INTEGER NOT NULL,
            UNIQUE (project_id, delivery_id),
            UNIQUE (binding_id, event_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO project_deliveries (
            delivery_id, project_id, binding_id, event_id,
            status, cursor, attempt_count, updated_at
        )
        SELECT delivery_id, project_id, binding_id, event_id,
               status, cursor, attempt_count, updated_at
        FROM delivery_schema_new
        """
    )
    conn.execute("DROP TABLE delivery_schema_new")
    conn.commit()
    conn.execute("PRAGMA foreign_keys=ON")

    prdb.ensure_schema(conn)
    prdb.ensure_schema(conn)

    row = conn.execute(
        """
        SELECT status, cursor, lease_expires_at,
               remote_message_ids_json, next_attempt_at, last_error_code
        FROM project_deliveries
        WHERE binding_id = 'discord-owner' AND event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    assert tuple(row) == ("in_flight", None, 345, None, None, None)


def test_delivery_claim_ack_nack_and_duplicate_ack_are_binding_local(
    event_env,
):
    conn = event_env["conn"]
    project_id = event_env["project_id"]
    outbox = event_env["outbox"]
    event = outbox.append_event(
        project_id,
        "run.completed",
        {"status": "succeeded"},
    )

    desktop = outbox.claim_delivery(
        project_id,
        "desktop-owner",
        lease_seconds=30,
    )
    discord = outbox.claim_delivery(
        project_id,
        "discord-owner",
        lease_seconds=30,
    )
    assert desktop is not None
    assert discord is not None
    assert desktop.event == event
    assert discord.event == event
    assert desktop.attempt == discord.attempt == 1

    assert outbox.acknowledge_delivery(desktop) == event.sequence
    assert outbox.acknowledge_delivery(desktop) == event.sequence
    outbox.reject_delivery(discord)
    rows = {
        row["binding_id"]: (row["status"], row["cursor"])
        for row in conn.execute(
            """
            SELECT binding_id, status, cursor
            FROM project_deliveries
            WHERE project_id = ?
            """,
            (project_id,),
        )
    }
    assert rows == {
        "desktop-owner": ("delivered", event.sequence),
        "discord-owner": ("pending", None),
    }


def test_delivery_claims_remain_strictly_ordered_per_binding(event_env):
    project_id = event_env["project_id"]
    outbox = event_env["outbox"]
    first_event = outbox.append_event(
        project_id,
        "run.progress",
        {"step": 1},
    )
    second_event = outbox.append_event(
        project_id,
        "run.progress",
        {"step": 2},
    )

    first_claim = outbox.claim_delivery(
        project_id,
        "desktop-owner",
        lease_seconds=30,
    )
    assert first_claim is not None
    assert first_claim.event == first_event
    assert (
        outbox.claim_delivery(
            project_id,
            "desktop-owner",
            lease_seconds=30,
        )
        is None
    )

    assert outbox.acknowledge_delivery(first_claim) == first_event.sequence
    second_claim = outbox.claim_delivery(
        project_id,
        "desktop-owner",
        lease_seconds=30,
    )
    assert second_claim is not None
    assert second_claim.event == second_event


def test_delivery_renew_defer_and_block_keep_oldest_sequence_as_barrier(
    event_env,
):
    from hermes_cli.project_events import (
        ProjectDeliveryConflictError,
        ProjectEventOutbox,
    )

    now = [100]
    ids = itertools.count(1)
    project_id = event_env["project_id"]
    outbox = ProjectEventOutbox(
        event_env["conn"],
        clock=lambda: now[0],
        id_factory=lambda kind: f"barrier-{kind}-{next(ids)}",
    )
    first_event = outbox.append_event(
        project_id,
        "run.progress",
        {"step": 1},
    )
    second_event = outbox.append_event(
        project_id,
        "run.progress",
        {"step": 2},
    )
    first_claim = outbox.claim_delivery(
        project_id,
        "discord-owner",
        lease_seconds=10,
    )
    assert first_claim is not None
    assert first_claim.event == first_event

    now[0] = 101
    renewed = outbox.renew_delivery(
        first_claim,
        lease_seconds=10,
    )
    assert renewed.attempt == first_claim.attempt
    assert renewed.lease_expires_at == 111
    with pytest.raises(ProjectDeliveryConflictError, match="stale"):
        outbox.defer_delivery(
            first_claim,
            error_code="transient",
            delay_seconds=8,
        )

    now[0] = 102
    assert outbox.defer_delivery(
        renewed,
        error_code="transient",
        delay_seconds=8,
    ) == 110
    now[0] = 109
    assert (
        outbox.claim_delivery(
            project_id,
            "discord-owner",
            lease_seconds=10,
        )
        is None
    )
    now[0] = 110
    retry = outbox.claim_delivery(
        project_id,
        "discord-owner",
        lease_seconds=10,
    )
    assert retry is not None
    assert retry.event == first_event
    assert retry.attempt == 2
    outbox.block_delivery(retry, error_code="forbidden")

    now[0] = 1_000
    assert (
        outbox.claim_delivery(
            project_id,
            "discord-owner",
            lease_seconds=10,
        )
        is None
    )
    rows = event_env["conn"].execute(
        """
        SELECT event_id, status, next_attempt_at, last_error_code
        FROM project_deliveries
        WHERE binding_id = 'discord-owner'
        ORDER BY rowid
        """
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (first_event.event_id, "blocked", None, "forbidden"),
        (second_event.event_id, "pending", None, None),
    ]


def test_expired_delivery_lease_is_reclaimed_with_a_new_fence(event_env):
    from hermes_cli.project_events import (
        ProjectDeliveryConflictError,
        ProjectEventOutbox,
    )

    now = [100]
    project_id = event_env["project_id"]
    outbox = ProjectEventOutbox(
        event_env["conn"],
        clock=lambda: now[0],
        id_factory=lambda kind: f"lease-{kind}",
    )
    event = outbox.append_event(
        project_id,
        "run.completed",
        {"status": "succeeded"},
    )
    stale = outbox.claim_delivery(
        project_id,
        "discord-owner",
        lease_seconds=10,
    )
    assert stale is not None

    now[0] = stale.lease_expires_at
    reclaimed = outbox.claim_delivery(
        project_id,
        "discord-owner",
        lease_seconds=10,
    )
    assert reclaimed is not None
    assert reclaimed.event == event
    assert reclaimed.attempt == stale.attempt + 1
    with pytest.raises(ProjectDeliveryConflictError):
        outbox.block_delivery(stale, error_code="conflict")


def test_terminal_remote_message_group_is_immutable_and_canonical(
    event_env,
):
    from hermes_cli.project_events import ProjectDeliveryConflictError

    outbox = event_env["outbox"]
    event = outbox.append_event(
        event_env["project_id"],
        "run.completed",
        {"status": "succeeded"},
    )
    claim = outbox.claim_delivery(
        event_env["project_id"],
        "discord-owner",
        lease_seconds=30,
    )
    assert claim is not None

    assert outbox.complete_delivery(
        claim,
        remote_message_ids=("remote-part-1", "remote-part-2"),
    ) == event.sequence
    assert outbox.complete_delivery(
        claim,
        remote_message_ids=("remote-part-1", "remote-part-2"),
    ) == event.sequence
    row = event_env["conn"].execute(
        """
        SELECT status, cursor, lease_expires_at,
               remote_message_ids_json, next_attempt_at, last_error_code
        FROM project_deliveries
        WHERE delivery_id = ?
        """,
        (claim.delivery_id,),
    ).fetchone()
    assert tuple(row) == (
        "delivered",
        event.sequence,
        None,
        '["remote-part-1","remote-part-2"]',
        None,
        None,
    )

    with pytest.raises(ProjectDeliveryConflictError, match="remote"):
        outbox.complete_delivery(
            claim,
            remote_message_ids=("remote-part-2", "remote-part-1"),
        )
    with pytest.raises(ValueError, match="unique"):
        outbox.complete_delivery(
            claim,
            remote_message_ids=("remote-part-1", "remote-part-1"),
        )


def test_only_the_origin_turn_queued_echo_can_be_suppressed(event_env):
    from hermes_cli.project_events import ProjectEventOutbox
    from hermes_cli.project_policy import ActorContext
    from hermes_cli.project_runtime import ProjectRuntime

    conn = event_env["conn"]
    project_id = event_env["project_id"]
    state = prdb.runtime_state_for_project(conn, project_id)
    assert state is not None
    runtime = ProjectRuntime(
        conn,
        clock=lambda: 100,
        id_factory=lambda kind: f"origin-{kind}",
    )
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "already shown optimistically"},
        ActorContext(
            "owner-1",
            "discord",
            "discord-owner",
            True,
        ),
        idempotency_key="origin-turn",
        expected_version=state.version,
    )
    outbox = ProjectEventOutbox(conn, clock=lambda: 100)
    discord_claim = outbox.claim_delivery(
        project_id,
        "discord-owner",
        lease_seconds=30,
    )
    desktop_claim = outbox.claim_delivery(
        project_id,
        "desktop-owner",
        lease_seconds=30,
    )
    assert discord_claim is not None
    assert desktop_claim is not None
    assert discord_claim.event.kind == "turn.queued"

    assert (
        outbox.suppress_origin_delivery(discord_claim)
        == discord_claim.event.sequence
    )
    suppressed = conn.execute(
        """
        SELECT status, remote_message_ids_json
        FROM project_deliveries WHERE delivery_id = ?
        """,
        (discord_claim.delivery_id,),
    ).fetchone()
    assert tuple(suppressed) == ("suppressed", "[]")
    with pytest.raises(ValueError, match="origin"):
        outbox.suppress_origin_delivery(desktop_claim)
    outbox.reject_delivery(desktop_claim)

    result_event = outbox.append_event(
        project_id,
        "turn.succeeded",
        {"turn_id": turn.turn_id},
        turn_id=turn.turn_id,
    )
    result_claim = outbox.claim_delivery(
        project_id,
        "discord-owner",
        lease_seconds=30,
    )
    assert result_claim is not None
    assert result_claim.event == result_event
    with pytest.raises(ValueError, match="origin"):
        outbox.suppress_origin_delivery(result_claim)


def test_expired_delivery_claim_cannot_ack_without_reclaim(event_env):
    from hermes_cli.project_events import (
        ProjectDeliveryConflictError,
        ProjectEventOutbox,
    )

    now = [100]
    project_id = event_env["project_id"]
    outbox = ProjectEventOutbox(
        event_env["conn"],
        clock=lambda: now[0],
        id_factory=lambda kind: f"expiring-{kind}",
    )
    outbox.append_event(
        project_id,
        "run.completed",
        {"status": "succeeded"},
    )
    claim = outbox.claim_delivery(
        project_id,
        "desktop-owner",
        lease_seconds=10,
    )
    assert claim is not None

    now[0] = claim.lease_expires_at
    with pytest.raises(ProjectDeliveryConflictError, match="expired"):
        outbox.acknowledge_delivery(claim)

    recovered = outbox.claim_delivery(
        project_id,
        "desktop-owner",
        lease_seconds=10,
    )
    assert recovered is not None
    assert recovered.delivery_id == claim.delivery_id
    assert recovered.attempt == claim.attempt + 1


def test_restart_reclaims_expired_delivery_without_duplicate_event(
    event_env,
):
    from hermes_cli.project_events import ProjectEventOutbox

    project_id = event_env["project_id"]
    outbox = event_env["outbox"]
    event = outbox.append_event(
        project_id,
        "run.completed",
        {"status": "succeeded"},
    )
    first = outbox.claim_delivery(
        project_id,
        "discord-owner",
        lease_seconds=10,
    )
    assert first is not None
    db_path = event_env["conn"].execute(
        "PRAGMA database_list"
    ).fetchone()["file"]
    event_env["conn"].close()

    restarted = projects_db.connect(Path(db_path))
    try:
        recovered = ProjectEventOutbox(
            restarted,
            clock=lambda: 111,
            id_factory=lambda kind: f"unexpected-{kind}",
        )
        second = recovered.claim_delivery(
            project_id,
            "discord-owner",
            lease_seconds=10,
        )
        assert second is not None
        assert second.delivery_id == first.delivery_id
        assert second.event.event_id == event.event_id
        assert second.attempt == 2
        assert recovered.acknowledge_delivery(second) == event.sequence
        assert recovered.events_after(project_id, 0, 10) == (event,)
        assert restarted.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'run.completed'
            """,
            (project_id,),
        ).fetchone()[0] == 1
    finally:
        restarted.close()


def test_artifact_is_published_only_after_safe_path_and_exact_readback(
    event_env,
    tmp_path,
):
    conn = event_env["conn"]
    project_id = event_env["project_id"]
    root = event_env["root"]
    outbox = event_env["outbox"]
    artifact_path = root / "report.txt"
    artifact_path.write_bytes(b"verified report")

    with pytest.raises(ValueError, match="readback"):
        outbox.register_verified_artifact(
            project_id,
            artifact_id="artifact-report",
            path=artifact_path,
            metadata={"kind": "report"},
            readback=lambda _path: b"different bytes",
        )
    assert conn.execute(
        "SELECT COUNT(*) FROM project_artifacts WHERE project_id = ?",
        (project_id,),
    ).fetchone()[0] == 0
    assert conn.execute(
        """
        SELECT COUNT(*) FROM project_events
        WHERE project_id = ? AND kind = 'artifact.verified'
        """,
        (project_id,),
    ).fetchone()[0] == 0

    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    with pytest.raises(PermissionError, match="project root"):
        outbox.register_verified_artifact(
            project_id,
            artifact_id="artifact-outside",
            path=outside,
            metadata={},
        )

    artifact = outbox.register_verified_artifact(
        project_id,
        artifact_id="artifact-report",
        path=artifact_path,
        metadata={
            "kind": "report",
            "provider_payload": "must-not-persist",
        },
        readback=lambda path: path.read_bytes(),
    )
    assert artifact.status == "verified"
    assert artifact.path == str(artifact_path.resolve())
    assert artifact.metadata["kind"] == "report"
    assert artifact.metadata["provider_payload"] == "[REDACTED]"
    assert artifact.metadata["size"] == len(b"verified report")
    assert len(artifact.metadata["sha256"]) == 64
    verified = outbox.events_after(project_id, 0, 10)[0]
    assert verified.kind == "artifact.verified"
    assert verified.payload["artifact_id"] == artifact.artifact_id
    assert "must-not-persist" not in conn.execute(
        """
        SELECT metadata_json FROM project_artifacts
        WHERE project_id = ? AND artifact_id = ?
        """,
        (project_id, artifact.artifact_id),
    ).fetchone()[0]


def test_live_stream_hint_projects_identity_not_event_payload(event_env):
    from gateway.stream_events import canonical_project_event_notice

    event = event_env["outbox"].append_event(
        event_env["project_id"],
        "run.progress",
        {"text": "sensitive project progress"},
    )
    notice = canonical_project_event_notice(event)

    assert notice.kind == "project_event"
    assert notice.extra == {
        "event_id": event.event_id,
        "project_id": event.project_id,
        "sequence": event.sequence,
    }
    assert "sensitive project progress" not in repr(notice)


def test_project_runtime_is_the_public_event_delivery_and_artifact_port(
    event_env,
):
    from hermes_cli.project_runtime import ProjectRuntime

    runtime = ProjectRuntime(
        event_env["conn"],
        clock=lambda: 200,
        id_factory=lambda kind: f"runtime-{kind}",
    )
    event = runtime.append_project_event(
        event_env["project_id"],
        "project.changed",
        {"field": "phase"},
    )
    assert runtime.events_after(event_env["project_id"], 0, 10) == (
        event,
    )
    claim = runtime.claim_delivery(
        event_env["project_id"],
        "desktop-owner",
        lease_seconds=30,
    )
    assert claim is not None
    assert runtime.ack_delivery(claim) == event.sequence


def test_project_runtime_exposes_fenced_delivery_lifecycle_wrappers(
    event_env,
):
    from hermes_cli.project_runtime import ProjectRuntime

    now = [200]
    counter = itertools.count(1)
    runtime = ProjectRuntime(
        event_env["conn"],
        clock=lambda: now[0],
        id_factory=lambda kind: (
            f"runtime-fenced-{kind}-{next(counter)}"
        ),
    )
    event = runtime.append_project_event(
        event_env["project_id"],
        "project.changed",
        {"field": "phase"},
    )
    claim = runtime.claim_delivery(
        event_env["project_id"],
        "desktop-owner",
        lease_seconds=30,
    )
    assert claim is not None

    renewed = runtime.renew_delivery(claim, lease_seconds=60)
    assert renewed.lease_expires_at == 260
    runtime.defer_delivery(
        renewed,
        error_code="transient",
        delay_seconds=2,
    )
    now[0] = 202
    retried = runtime.claim_delivery(
        event_env["project_id"],
        "desktop-owner",
        lease_seconds=30,
    )
    assert retried is not None
    assert retried.attempt == 2
    assert (
        runtime.complete_delivery(
            retried,
            remote_message_ids=("desktop-message-1",),
        )
        == event.sequence
    )

    blocked_event = runtime.append_project_event(
        event_env["project_id"],
        "project.changed",
        {"field": "status"},
    )
    blocked_claim = runtime.claim_delivery(
        event_env["project_id"],
        "desktop-owner",
        lease_seconds=30,
    )
    assert blocked_claim is not None
    assert (
        runtime.block_delivery(
            blocked_claim,
            error_code="forbidden",
        )
        is None
    )
    assert event_env["conn"].execute(
        """
        SELECT status FROM project_deliveries
        WHERE event_id = ?
        """,
        (blocked_event.event_id,),
    ).fetchone()[0] == "blocked"
