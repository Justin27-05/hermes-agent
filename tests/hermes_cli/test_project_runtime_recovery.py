"""Task 5 contract and recovery tests for the durable project runtime."""

from __future__ import annotations

import importlib
import inspect
import hashlib
import json
import os
import queue
import re
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, fields, replace
from enum import Enum
from pathlib import Path
from typing import Literal, Protocol, get_type_hints

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import ActorContext


def _make_runtime(path, *, now=100, clock=None):
    conn = projects_db.connect(path)
    project_id = projects_db.create_project(conn, name="Recovery")
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="session-root",
        current_phase="implementation",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="owner-binding",
        project_id=project_id,
        surface="desktop",
        external_binding_id=f"window-{project_id}",
        actor_id="owner",
        now=1,
    )
    module = importlib.import_module("hermes_cli.project_runtime")
    runtime = module.ProjectRuntime(conn, clock=clock or (lambda: now))
    actor = ActorContext("owner", "desktop", "owner-binding", True)
    return module, conn, runtime, project_id, actor


def _claim_snapshot(conn, project_id, turn_id):
    return (
        tuple(
            conn.execute(
                """
                SELECT * FROM project_turns
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        ),
        tuple(
            conn.execute(
                """
                SELECT * FROM project_run_controls
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        ),
        tuple(
            conn.execute(
                """
                SELECT * FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn_id),
            ).fetchone()
        )
        if conn.execute(
            """
            SELECT 1 FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn_id),
        ).fetchone()
        is not None
        else None,
        prdb.runtime_state_for_project(conn, project_id),
        tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? ORDER BY sequence
                """,
                (project_id,),
            )
        ),
    )


def _enqueue_and_claim(runtime, project_id, actor, *, key="turn"):
    turn = runtime.enqueue_turn(
        project_id,
        {"message": key},
        actor,
        idempotency_key=key,
        expected_version=0,
    )
    claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)
    assert claim is not None
    return turn, claim


def test_task7_c9_count_drift_projects_conflict_block_event_is_exact_atomic_and_replayable(
    tmp_path,
):
    """Projects records one retained C9 conflict proof, or writes nothing.

    Mutations caught: trusting a caller-supplied key/count, reading authority
    outside the owned transaction, partially committing the block/event, or
    rewriting the retained proof on replay after unrelated runtime drift.
    """
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "c9-projects.db"
    )
    opened = [conn]

    def all_user_tables(connection):
        tables = tuple(
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        )
        result = []
        for table in tables:
            quoted = table.replace('"', '""')
            columns = tuple(
                row[1]
                for row in connection.execute(
                    f'PRAGMA table_info("{quoted}")'
                )
            )
            order = ", ".join(
                str(position)
                for position in range(1, len(columns) + 1)
            )
            rows = tuple(
                tuple(row)
                for row in connection.execute(
                    f'SELECT * FROM "{quoted}" ORDER BY {order}'
                )
            )
            result.append((table, columns, rows))
        return tuple(result)

    def attempt_for(claim):
        return module.TurnAttemptIdentity(
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            sequence=claim.sequence,
            worker_id=claim.worker_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            canonical_session_id=claim.canonical_session_id,
            lease_expires_at=claim.lease_expires_at,
        )

    def identity_for(terminal, observed_count):
        attempt = terminal.attempt
        return {
            "attempt_id": attempt.attempt_id,
            "batch_id": terminal.batch_id,
            "canonical_session_id": attempt.canonical_session_id,
            "fencing_token": attempt.fencing_token,
            "lease_expires_at": attempt.lease_expires_at,
            "lease_generation": attempt.lease_generation,
            "observed_message_count": observed_count,
            "project_id": attempt.project_id,
            "result_id": terminal.result_id,
            "sequence": attempt.sequence,
            "status": terminal.status,
            "turn_id": attempt.turn_id,
            "worker_id": attempt.worker_id,
        }

    def key_for(terminal, observed_count):
        identity_json = json.dumps(
            identity_for(terminal, observed_count),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return "transcript-conflict-" + hashlib.sha256(
            identity_json.encode("utf-8")
        ).hexdigest()

    def make_case(label, *, batch_id):
        values = _make_runtime(tmp_path / f"c9-{label}.db")
        case_module, case_conn, case_runtime, case_project, case_actor = values
        opened.append(case_conn)
        case_turn, case_claim = _enqueue_and_claim(
            case_runtime, case_project, case_actor, key=f"c9-{label}"
        )
        case_claim = case_runtime.mark_turn_started(case_claim)
        case_runtime.commit_turn_with_task7_batch(
            case_claim,
            case_module.CanonicalTurnResult("succeeded", batch_id),
            transcript_batch_id=batch_id,
        )
        case_terminal = case_module.TerminalTranscriptAcknowledgement(
            batch_id=batch_id,
            attempt=attempt_for(case_claim),
            status="succeeded",
            result_id=batch_id,
        )
        case_conflict = case_module.TerminalTranscriptConflict(
            terminal=case_terminal,
            conflict_key=key_for(case_terminal, 1),
            observed_message_count=1,
        )
        return (
            case_conn,
            case_runtime,
            case_project,
            case_turn,
            case_claim,
            case_terminal,
            case_conflict,
        )

    def assert_authority_conflict_without_writes(
        case_conn, case_runtime, conflict
    ):
        before = all_user_tables(case_conn)
        before_changes = case_conn.total_changes
        assert (
            case_conn.execute("PRAGMA integrity_check").fetchone()[0]
            == "ok"
        )
        assert tuple(
            case_conn.execute("PRAGMA foreign_key_check")
        ) == ()
        trace = []
        case_conn.set_trace_callback(trace.append)
        try:
            with pytest.raises(module.ProjectRuntimeError) as raised:
                case_runtime.record_terminal_transcript_conflict(conflict)
        finally:
            case_conn.set_trace_callback(None)
        assert (
            raised.value.code
            is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
        )
        assert all_user_tables(case_conn) == before
        assert case_conn.total_changes == before_changes
        assert not [
            statement
            for statement in trace
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]

    try:
        # Frozen public contract and exact annotations/signatures.
        assert hasattr(module, "TerminalTranscriptConflict")
        conflict_type = module.TerminalTranscriptConflict
        assert tuple(field.name for field in fields(conflict_type)) == (
            "terminal",
            "conflict_key",
            "observed_message_count",
        )
        assert get_type_hints(conflict_type) == {
            "terminal": module.TerminalTranscriptAcknowledgement,
            "conflict_key": str,
            "observed_message_count": int,
        }
        frozen_probe = object.__new__(conflict_type)
        with pytest.raises(FrozenInstanceError):
            frozen_probe.conflict_key = "changed"
        unbound_signature = inspect.signature(
            module.ProjectRuntime.record_terminal_transcript_conflict
        )
        assert tuple(
            (name, parameter.kind)
            for name, parameter in unbound_signature.parameters.items()
        ) == (
            ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ("conflict", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        )
        assert get_type_hints(
            module.ProjectRuntime.record_terminal_transcript_conflict
        ) == {
            "conflict": conflict_type,
            "return": Literal["recorded", "already_recorded"],
        }

        derive_key = object()

        def independently_keyed_conflict(
            terminal_value,
            observed_count,
            *,
            supplied_key=derive_key,
        ):
            return conflict_type(
                terminal=terminal_value,
                conflict_key=(
                    key_for(terminal_value, observed_count)
                    if supplied_key is derive_key
                    else supplied_key
                ),
                observed_message_count=observed_count,
            )

        def append_conflict_event(
            case_conn,
            case_project,
            case_turn_id,
            extra_conflict,
            *,
            version,
        ):
            payload_json = json.dumps(
                {
                    **identity_for(
                        extra_conflict.terminal,
                        extra_conflict.observed_message_count,
                    ),
                    "conflict_key": extra_conflict.conflict_key,
                    "version": version,
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            next_sequence = case_conn.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1
                FROM project_events WHERE project_id = ?
                """,
                (case_project,),
            ).fetchone()[0]
            case_conn.execute(
                """
                INSERT INTO project_events (
                    event_id, project_id, sequence, kind, turn_id,
                    payload_json, created_at
                ) VALUES (
                    ?, ?, ?, 'turn.transcript_conflicted', ?, ?, 100
                )
                """,
                (
                    extra_conflict.conflict_key,
                    case_project,
                    next_sequence,
                    case_turn_id,
                    payload_json,
                ),
            )
            case_conn.commit()
            return payload_json

        turn, claim = _enqueue_and_claim(runtime, project_id, actor, key="c9")
        claim = runtime.mark_turn_started(claim)
        batch_id = "123e4567-e89b-42d3-a456-426614174009"
        runtime.commit_turn_with_task7_batch(
            claim,
            module.CanonicalTurnResult("succeeded", batch_id),
            transcript_batch_id=batch_id,
        )
        terminal = module.TerminalTranscriptAcknowledgement(
            batch_id=batch_id,
            attempt=attempt_for(claim),
            status="succeeded",
            result_id=batch_id,
        )
        expected_key = key_for(terminal, 1)
        assert len(expected_key) == 84
        assert re.fullmatch(
            r"transcript-conflict-[0-9a-f]{64}", expected_key
        )
        conflict = conflict_type(
            terminal=terminal,
            conflict_key=expected_key,
            observed_message_count=1,
        )
        with pytest.raises(FrozenInstanceError):
            conflict.observed_message_count = 2

        before = all_user_tables(conn)
        runtime_before = dict(
            conn.execute(
                "SELECT * FROM project_runtime_state WHERE project_id = ?",
                (project_id,),
            ).fetchone()
        )
        turn_before = dict(
            conn.execute(
                "SELECT * FROM project_turns WHERE turn_id = ?",
                (turn.turn_id,),
            ).fetchone()
        )
        event_ids_before = {
            row[0]
            for row in conn.execute(
                "SELECT event_id FROM project_events WHERE project_id = ?",
                (project_id,),
            )
        }
        trace = []
        conn.set_trace_callback(trace.append)
        try:
            assert (
                runtime.record_terminal_transcript_conflict(conflict)
                == "recorded"
            )
        finally:
            conn.set_trace_callback(None)

        normalized = [
            " ".join(statement.upper().split())
            for statement in trace
        ]
        begin = [
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("BEGIN")
        ]
        endings = [
            index
            for index, statement in enumerate(normalized)
            if statement in {"COMMIT", "ROLLBACK"}
        ]
        assert len(begin) == len(endings) == 1
        assert normalized[begin[0]] == "BEGIN IMMEDIATE"
        assert normalized[endings[0]] == "COMMIT"
        authority_tables = {
            "PROJECT_RUNTIME_STATE",
            "PROJECT_TURNS",
            "PROJECT_RUN_CONTROLS",
            "PROJECT_WORKER_LEASES",
            "PROJECT_EVENTS",
        }
        observed_tables = {
            table
            for table in authority_tables
            if any(
                re.search(
                    rf"(?:FROM|JOIN)\s+(?:MAIN\.)?[`\"\[]?{table}[`\"\]]?",
                    statement,
                )
                for statement in normalized
            )
        }
        assert observed_tables == authority_tables
        bounded = [
            index
            for index, statement in enumerate(normalized)
            if any(table in statement for table in authority_tables)
        ]
        assert bounded
        assert all(begin[0] < index < endings[0] for index in bounded)
        dml = {
            statement
            for statement in normalized
            if statement.startswith(
                ("INSERT ", "UPDATE ", "DELETE ", "REPLACE ")
            )
        }
        assert any(
            statement.startswith("UPDATE PROJECT_RUNTIME_STATE")
            for statement in dml
        )
        assert any(
            statement.startswith("INSERT INTO PROJECT_EVENTS")
            for statement in dml
        )
        assert any(
            statement.startswith("INSERT INTO PROJECT_DELIVERIES")
            for statement in dml
        )
        assert not any(
            table in statement
            for statement in dml
            for table in (
                "PROJECT_TURNS",
                "PROJECT_RUN_CONTROLS",
                "PROJECT_WORKER_LEASES",
            )
        )
        assert all(
            statement.startswith("UPDATE PROJECT_RUNTIME_STATE")
            or statement.startswith("INSERT INTO PROJECT_EVENTS")
            or statement.startswith("INSERT INTO PROJECT_DELIVERIES")
            for statement in dml
        )
        runtime_updates = [
            statement
            for statement in dml
            if statement.startswith("UPDATE PROJECT_RUNTIME_STATE")
        ]
        assert runtime_updates
        for statement in runtime_updates:
            set_clause = statement.split(" SET ", 1)[1].split(
                " WHERE ", 1
            )[0]
            assigned = {
                assignment.split("=", 1)[0].strip()
                for assignment in set_clause.split(",")
            }
            assert assigned == {
                "TRANSCRIPT_PENDING_BATCH_ID",
                "TRANSCRIPT_DISPATCH_BLOCK_KEY",
                "VERSION",
                "UPDATED_AT",
            }

        runtime_after = dict(
            conn.execute(
                "SELECT * FROM project_runtime_state WHERE project_id = ?",
                (project_id,),
            ).fetchone()
        )
        assert runtime_after["transcript_pending_batch_id"] is None
        assert runtime_after["transcript_dispatch_block_key"] == expected_key
        assert runtime_after["version"] == runtime_before["version"] + 1
        runtime_comparison_before = dict(runtime_before)
        runtime_comparison_after = dict(runtime_after)
        for column in (
            "transcript_pending_batch_id",
            "transcript_dispatch_block_key",
            "version",
            "updated_at",
        ):
            runtime_comparison_before.pop(column)
            runtime_comparison_after.pop(column)
        assert runtime_comparison_after == runtime_comparison_before
        turn_after = dict(
            conn.execute(
                "SELECT * FROM project_turns WHERE turn_id = ?",
                (turn.turn_id,),
            ).fetchone()
        )
        assert turn_after == turn_before
        assert turn_after["transcript_applied_batch_id"] is None
        new_events = tuple(
            dict(row)
            for row in conn.execute(
                "SELECT * FROM project_events WHERE project_id = ? ORDER BY sequence",
                (project_id,),
            )
            if row["event_id"] not in event_ids_before
        )
        assert len(new_events) == 1
        event = new_events[0]
        assert event["event_id"] == expected_key
        assert event["kind"] == "turn.transcript_conflicted"
        assert event["turn_id"] == turn.turn_id
        expected_payload = {
            **identity_for(terminal, 1),
            "conflict_key": expected_key,
            "version": runtime_after["version"],
        }
        expected_payload_json = json.dumps(
            expected_payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        assert event["payload_json"] == expected_payload_json
        assert json.loads(event["payload_json"]) == expected_payload
        after = all_user_tables(conn)
        before_by_table = {entry[0]: entry for entry in before}
        after_by_table = {entry[0]: entry for entry in after}
        assert before_by_table.keys() == after_by_table.keys()
        for table in before_by_table:
            if table not in {
                "project_runtime_state",
                "project_events",
                "project_deliveries",
            }:
                assert after_by_table[table] == before_by_table[table]

        # Projects has no Session count authority: a fresh, bounded count of
        # two is valid when its deterministic key is recomputed from that
        # count.  It becomes conflicting only against a different retained
        # count/key proof, which is exercised below on the primary case.
        (
            count_two_conn,
            count_two_runtime,
            count_two_project,
            count_two_turn,
            _,
            count_two_terminal,
            count_one_conflict,
        ) = make_case(
            "fresh-count-two",
            batch_id="173e4567-e89b-42d3-a456-426614174009",
        )
        count_two_conflict = independently_keyed_conflict(
            count_two_terminal,
            2,
        )
        assert (
            count_two_conflict.conflict_key
            != count_one_conflict.conflict_key
        )
        count_two_state_before = dict(
            count_two_conn.execute(
                """
                SELECT * FROM project_runtime_state WHERE project_id = ?
                """,
                (count_two_project,),
            ).fetchone()
        )
        assert (
            count_two_runtime.record_terminal_transcript_conflict(
                count_two_conflict
            )
            == "recorded"
        )
        count_two_state = dict(
            count_two_conn.execute(
                """
                SELECT * FROM project_runtime_state WHERE project_id = ?
                """,
                (count_two_project,),
            ).fetchone()
        )
        assert count_two_state["transcript_pending_batch_id"] is None
        assert (
            count_two_state["transcript_dispatch_block_key"]
            == count_two_conflict.conflict_key
        )
        assert (
            count_two_state["version"]
            == count_two_state_before["version"] + 1
        )
        count_two_event = dict(
            count_two_conn.execute(
                "SELECT * FROM project_events WHERE event_id = ?",
                (count_two_conflict.conflict_key,),
            ).fetchone()
        )
        assert count_two_event["kind"] == "turn.transcript_conflicted"
        assert count_two_event["turn_id"] == count_two_turn.turn_id
        assert json.loads(count_two_event["payload_json"]) == {
            **identity_for(count_two_terminal, 2),
            "conflict_key": count_two_conflict.conflict_key,
            "version": count_two_state["version"],
        }

        # Schema-valid text carriers may still be impossible to encode. A raw
        # Unicode serialization failure is classified as authority conflict.
        (
            surrogate_conn,
            surrogate_runtime,
            _,
            _,
            _,
            surrogate_terminal,
            surrogate_conflict,
        ) = make_case(
            "unpaired-surrogate",
            batch_id="1d3e4567-e89b-42d3-a456-426614174009",
        )
        surrogate_terminal = replace(
            surrogate_terminal,
            attempt=replace(
                surrogate_terminal.attempt,
                attempt_id="c9-unpaired-\ud800",
            ),
        )
        assert_authority_conflict_without_writes(
            surrogate_conn,
            surrogate_runtime,
            replace(
                surrogate_conflict,
                terminal=surrogate_terminal,
            ),
        )

        # First record rejects malformed runtime-version storage without
        # allocating the block, event, or a replacement version.
        for (
            version_label,
            version_batch_id,
            malformed_version,
            version_storage_class,
        ) in (
            (
                "version-negative",
                "1c3e4567-e89b-42d3-a456-426614174009",
                -1,
                "integer",
            ),
            (
                "version-real",
                "1b3e4567-e89b-42d3-a456-426614174009",
                1.5,
                "real",
            ),
        ):
            (
                version_conn,
                version_runtime,
                version_project,
                version_turn,
                _,
                version_terminal,
                version_conflict,
            ) = make_case(
                version_label,
                batch_id=version_batch_id,
            )
            assert version_conn.execute(
                """
                UPDATE project_runtime_state
                SET version = ?
                WHERE project_id = ?
                """,
                (malformed_version, version_project),
            ).rowcount == 1
            version_conn.commit()
            assert tuple(
                version_conn.execute(
                    """
                    SELECT version, typeof(version),
                           transcript_pending_batch_id,
                           transcript_dispatch_block_key
                    FROM project_runtime_state
                    WHERE project_id = ?
                    """,
                    (version_project,),
                ).fetchone()
            ) == (
                malformed_version,
                version_storage_class,
                version_terminal.batch_id,
                None,
            )
            assert version_conn.execute(
                """
                SELECT COUNT(*) FROM project_events
                WHERE project_id = ? AND turn_id = ?
                  AND kind = 'turn.transcript_conflicted'
                """,
                (version_project, version_turn.turn_id),
            ).fetchone()[0] == 0
            assert_authority_conflict_without_writes(
                version_conn,
                version_runtime,
                version_conflict,
            )

        # A differently keyed canonical conflict event for this project and
        # turn makes first-record authority ambiguous, even though the
        # expected event id is still absent.
        (
            extra_first_conn,
            extra_first_runtime,
            extra_first_project,
            extra_first_turn,
            _,
            extra_first_terminal,
            extra_first_conflict,
        ) = make_case(
            "extra-conflict-event-first",
            batch_id="1a3e4567-e89b-42d3-a456-426614174009",
        )
        extra_first_state = dict(
            extra_first_conn.execute(
                """
                SELECT * FROM project_runtime_state
                WHERE project_id = ?
                """,
                (extra_first_project,),
            ).fetchone()
        )
        assert (
            extra_first_state["transcript_pending_batch_id"]
            == extra_first_terminal.batch_id
        )
        assert (
            extra_first_state["transcript_dispatch_block_key"]
            is None
        )
        extra_first_event = independently_keyed_conflict(
            extra_first_terminal,
            2,
        )
        assert (
            extra_first_event.conflict_key
            != extra_first_conflict.conflict_key
        )
        append_conflict_event(
            extra_first_conn,
            extra_first_project,
            extra_first_turn.turn_id,
            extra_first_event,
            version=extra_first_state["version"] + 1,
        )
        assert extra_first_conn.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.transcript_conflicted'
            """,
            (
                extra_first_project,
                extra_first_turn.turn_id,
            ),
        ).fetchone()[0] == 1
        assert_authority_conflict_without_writes(
            extra_first_conn,
            extra_first_runtime,
            extra_first_conflict,
        )

        # Exact replay rejects a second canonical conflict proof for the same
        # project and turn even when the expected retained event is intact.
        (
            extra_replay_conn,
            extra_replay_runtime,
            extra_replay_project,
            extra_replay_turn,
            _,
            extra_replay_terminal,
            extra_replay_conflict,
        ) = make_case(
            "extra-conflict-event-replay",
            batch_id="193e4567-e89b-42d3-a456-426614174009",
        )
        assert extra_replay_runtime.record_terminal_transcript_conflict(
            extra_replay_conflict
        ) == "recorded"
        extra_replay_state = dict(
            extra_replay_conn.execute(
                """
                SELECT * FROM project_runtime_state
                WHERE project_id = ?
                """,
                (extra_replay_project,),
            ).fetchone()
        )
        extra_replay_event = independently_keyed_conflict(
            extra_replay_terminal,
            2,
        )
        assert (
            extra_replay_event.conflict_key
            != extra_replay_conflict.conflict_key
        )
        append_conflict_event(
            extra_replay_conn,
            extra_replay_project,
            extra_replay_turn.turn_id,
            extra_replay_event,
            version=extra_replay_state["version"],
        )
        assert tuple(
            tuple(row)
            for row in extra_replay_conn.execute(
                """
                SELECT event_id FROM project_events
                WHERE project_id = ? AND turn_id = ?
                  AND kind = 'turn.transcript_conflicted'
                ORDER BY event_id
                """,
                (
                    extra_replay_project,
                    extra_replay_turn.turn_id,
                ),
            )
        ) == tuple(
            (event_id,)
            for event_id in sorted(
                (
                    extra_replay_conflict.conflict_key,
                    extra_replay_event.conflict_key,
                )
            )
        )
        assert_authority_conflict_without_writes(
            extra_replay_conn,
            extra_replay_runtime,
            extra_replay_conflict,
        )

        # A terminal batch may retain the lease horizon observed before a
        # heartbeat. First-record authority accepts that exact older horizon
        # when it is no later than the durable control audit horizon.
        heartbeat_now = [100]
        (
            heartbeat_module,
            heartbeat_conn,
            heartbeat_runtime,
            heartbeat_project,
            heartbeat_actor,
        ) = _make_runtime(
            tmp_path / "c9-heartbeat-horizon.db",
            clock=lambda: heartbeat_now[0],
        )
        opened.append(heartbeat_conn)
        heartbeat_turn, observed_claim = _enqueue_and_claim(
            heartbeat_runtime,
            heartbeat_project,
            heartbeat_actor,
            key="c9-heartbeat-horizon",
        )
        assert observed_claim.lease_expires_at == 130
        heartbeat_now[0] = 110
        renewed_claim = heartbeat_runtime.heartbeat_turn(
            observed_claim,
            lease_seconds=50,
        )
        assert renewed_claim.lease_expires_at == 160
        renewed_claim = heartbeat_runtime.mark_turn_started(renewed_claim)
        heartbeat_batch_id = "183e4567-e89b-42d3-a456-426614174009"
        heartbeat_runtime.commit_turn_with_task7_batch(
            renewed_claim,
            heartbeat_module.CanonicalTurnResult(
                "succeeded",
                heartbeat_batch_id,
            ),
            transcript_batch_id=heartbeat_batch_id,
        )
        heartbeat_terminal = (
            heartbeat_module.TerminalTranscriptAcknowledgement(
                batch_id=heartbeat_batch_id,
                attempt=attempt_for(observed_claim),
                status="succeeded",
                result_id=heartbeat_batch_id,
            )
        )
        heartbeat_conflict = independently_keyed_conflict(
            heartbeat_terminal,
            1,
        )
        heartbeat_control_horizon = heartbeat_conn.execute(
            """
            SELECT claim_lease_expires_at
            FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (heartbeat_project, heartbeat_turn.turn_id),
        ).fetchone()[0]
        assert (
            heartbeat_terminal.attempt.lease_expires_at
            <= heartbeat_control_horizon
            <= heartbeat_module.MAX_PERSISTED_TIMESTAMP
        )
        heartbeat_state_before = dict(
            heartbeat_conn.execute(
                """
                SELECT * FROM project_runtime_state
                WHERE project_id = ?
                """,
                (heartbeat_project,),
            ).fetchone()
        )
        assert heartbeat_runtime.record_terminal_transcript_conflict(
            heartbeat_conflict
        ) == "recorded"
        heartbeat_state_after = dict(
            heartbeat_conn.execute(
                """
                SELECT * FROM project_runtime_state
                WHERE project_id = ?
                """,
                (heartbeat_project,),
            ).fetchone()
        )
        assert heartbeat_state_after["transcript_pending_batch_id"] is None
        assert heartbeat_state_after[
            "transcript_dispatch_block_key"
        ] == heartbeat_conflict.conflict_key
        assert heartbeat_state_after["version"] == (
            heartbeat_state_before["version"] + 1
        )
        heartbeat_event = dict(
            heartbeat_conn.execute(
                "SELECT * FROM project_events WHERE event_id = ?",
                (heartbeat_conflict.conflict_key,),
            ).fetchone()
        )
        assert heartbeat_event["kind"] == "turn.transcript_conflicted"
        assert heartbeat_event["turn_id"] == heartbeat_turn.turn_id
        assert json.loads(heartbeat_event["payload_json"]) == {
            **identity_for(heartbeat_terminal, 1),
            "conflict_key": heartbeat_conflict.conflict_key,
            "version": heartbeat_state_after["version"],
        }

        # Exact replay is zero DML before and after unrelated lifecycle,
        # conversation-tip and version drift.
        retained_after = all_user_tables(conn)
        changes_after = conn.total_changes
        replay_trace = []
        conn.set_trace_callback(replay_trace.append)
        try:
            assert (
                runtime.record_terminal_transcript_conflict(conflict)
                == "already_recorded"
            )
        finally:
            conn.set_trace_callback(None)
        assert all_user_tables(conn) == retained_after
        assert conn.total_changes == changes_after
        assert not [
            statement
            for statement in replay_trace
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
        ]

        retained_turn = dict(
            conn.execute(
                """
                SELECT * FROM project_turns
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn.turn_id),
            ).fetchone()
        )
        retained_control = dict(
            conn.execute(
                """
                SELECT * FROM project_run_controls
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn.turn_id),
            ).fetchone()
        )
        durable_retained_proof_mutations = (
            (
                "project_turns",
                "sequence",
                retained_turn["sequence"],
                retained_turn["sequence"] + 1_000,
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_turns",
                "attempt_id",
                retained_turn["attempt_id"],
                "c9-corrupt-turn-attempt",
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_turns",
                "lease_generation",
                retained_turn["lease_generation"],
                retained_turn["lease_generation"] + 1,
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_turns",
                "fencing_token",
                retained_turn["fencing_token"],
                retained_turn["fencing_token"] + 1,
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_turns",
                "status",
                retained_turn["status"],
                "failed",
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_turns",
                "terminal_result_id",
                retained_turn["terminal_result_id"],
                "273e4567-e89b-42d3-a456-426614174009",
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_run_controls",
                "control_state",
                retained_control["control_state"],
                "running",
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_run_controls",
                "attempt_id",
                retained_control["attempt_id"],
                "c9-corrupt-control-attempt",
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_run_controls",
                "claim_worker_id",
                retained_control["claim_worker_id"],
                "c9-corrupt-control-worker",
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_run_controls",
                "claim_lease_expires_at",
                retained_control["claim_lease_expires_at"],
                terminal.attempt.lease_expires_at - 1,
                "turn_id",
                turn.turn_id,
            ),
            (
                "project_run_controls",
                "claim_canonical_session_id",
                retained_control["claim_canonical_session_id"],
                "c9-corrupt-control-session",
                "turn_id",
                turn.turn_id,
            ),
        )
        row_retained_proof_mutations = (
            ("turn", "project_id", "c9-corrupt-turn-project"),
            ("turn", "turn_id", "c9-corrupt-turn-id"),
            (
                "turn",
                "transcript_applied_batch_id",
                retained_turn["terminal_result_id"],
            ),
            ("control", "project_id", "c9-corrupt-control-project"),
            ("control", "turn_id", "c9-corrupt-control-turn"),
        )
        assert retained_turn["execution_state"] == "started"
        assert retained_turn["transcript_applied_batch_id"] is None
        assert (
            retained_turn["terminal_result_id"]
            == terminal.result_id
        )

        def set_retained_proof_value(
            table,
            column,
            value,
            lookup_column,
            lookup_value,
        ):
            quoted_table = table.replace('"', '""')
            quoted_column = column.replace('"', '""')
            quoted_lookup = lookup_column.replace('"', '""')
            assert conn.execute(
                f"""
                UPDATE "{quoted_table}"
                SET "{quoted_column}" = ?
                WHERE "{quoted_lookup}" = ?
                """,
                (value, lookup_value),
            ).rowcount == 1
            conn.commit()

        def assert_replay_row_mutation_without_writes(
            row_kind,
            column,
            corrupted,
        ):
            before = all_user_tables(conn)
            before_changes = conn.total_changes
            assert conn.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0] == "ok"
            assert tuple(conn.execute("PRAGMA foreign_key_check")) == ()
            injected = []

            def one_shot_row_factory(cursor, values):
                row = sqlite3.Row(cursor, values)
                names = tuple(
                    item[0].casefold() for item in cursor.description
                )
                is_target_shape = (
                    row_kind == "turn"
                    and "terminal_result_id" in names
                    and "execution_state" in names
                ) or (
                    row_kind == "control"
                    and "control_state" in names
                    and "claim_worker_id" in names
                )
                if (
                    not injected
                    and is_target_shape
                    and column in names
                    and row["project_id"] == project_id
                    and row["turn_id"] == turn.turn_id
                ):
                    converted = list(values)
                    converted[names.index(column)] = corrupted
                    injected.append((row_kind, column))
                    return sqlite3.Row(cursor, tuple(converted))
                return row

            trace = []
            previous_row_factory = conn.row_factory
            conn.row_factory = one_shot_row_factory
            conn.set_trace_callback(trace.append)
            try:
                with pytest.raises(module.ProjectRuntimeError) as raised:
                    runtime.record_terminal_transcript_conflict(conflict)
            finally:
                conn.set_trace_callback(None)
                conn.row_factory = previous_row_factory
            assert (
                raised.value.code
                is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert injected == [(row_kind, column)]
            assert all_user_tables(conn) == before
            assert conn.total_changes == before_changes
            assert not [
                statement
                for statement in trace
                if statement.lstrip().upper().startswith(
                    ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
            ]

        def exercise_retained_replay_matrix():
            retained_baseline = all_user_tables(conn)
            for (
                table,
                column,
                original,
                corrupted,
                lookup_column,
                lookup_value,
            ) in durable_retained_proof_mutations:
                set_retained_proof_value(
                    table,
                    column,
                    corrupted,
                    lookup_column,
                    lookup_value,
                )
                assert conn.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0] == "ok"
                assert tuple(
                    conn.execute("PRAGMA foreign_key_check")
                ) == ()
                assert_authority_conflict_without_writes(
                    conn,
                    runtime,
                    conflict,
                )
                restore_lookup = (
                    corrupted if column == lookup_column else lookup_value
                )
                set_retained_proof_value(
                    table,
                    column,
                    original,
                    lookup_column,
                    restore_lookup,
                )
                assert all_user_tables(conn) == retained_baseline
                assert conn.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0] == "ok"
                assert tuple(
                    conn.execute("PRAGMA foreign_key_check")
                ) == ()
            for row_kind, column, corrupted in (
                row_retained_proof_mutations
            ):
                assert_replay_row_mutation_without_writes(
                    row_kind,
                    column,
                    corrupted,
                )
                assert all_user_tables(conn) == retained_baseline

        # Immediate replay recertifies every retained turn/attempt and control
        # proof member. Durable cases are domain/FK-clean before the call;
        # FK-coupled identities and the valid immutable applied alternative
        # use one faithful sqlite3.Row mutation.
        exercise_retained_replay_matrix()

        # Replay also revalidates the retained block and lease absence rather
        # than trusting the exact event alone.
        conn.execute(
            """
            UPDATE project_runtime_state
            SET transcript_dispatch_block_key = 'another-replay-block'
            WHERE project_id = ?
            """,
            (project_id,),
        )
        conn.commit()
        assert_authority_conflict_without_writes(conn, runtime, conflict)
        conn.execute(
            """
            UPDATE project_runtime_state
            SET transcript_dispatch_block_key = ?
            WHERE project_id = ?
            """,
            (expected_key, project_id),
        )
        conn.commit()
        conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id,
                lease_generation, fencing_token, expires_at, updated_at
            ) VALUES ('c9-replay-lease', ?, ?, ?, ?, ?, ?, 100)
            """,
            (
                project_id,
                turn.turn_id,
                claim.worker_id,
                claim.lease_generation,
                claim.fencing_token,
                claim.lease_expires_at,
            ),
        )
        conn.commit()
        assert_authority_conflict_without_writes(conn, runtime, conflict)
        conn.execute(
            "DELETE FROM project_worker_leases WHERE lease_id = 'c9-replay-lease'"
        )
        conn.commit()

        later_tip = "c9-later-tip"
        conn.execute(
            """
            INSERT INTO project_conversations (
                conversation_id, project_id, parent_conversation_id,
                root_conversation_id, created_at
            ) VALUES (?, ?, 'session-root', 'session-root', 101)
            """,
            (later_tip, project_id),
        )
        conn.execute(
            """
            UPDATE project_runtime_state
            SET lifecycle = 'awaiting_acceptance',
                conversation_tip_id = ?,
                version = version + 7,
                updated_at = 101
            WHERE project_id = ?
            """,
            (later_tip, project_id),
        )
        conn.commit()
        drifted = all_user_tables(conn)
        drifted_changes = conn.total_changes
        assert (
            runtime.record_terminal_transcript_conflict(conflict)
            == "already_recorded"
        )
        assert all_user_tables(conn) == drifted
        assert conn.total_changes == drifted_changes
        assert dict(
            conn.execute(
                "SELECT * FROM project_events WHERE event_id = ?",
                (expected_key,),
            ).fetchone()
        ) == event

        # Later lifecycle, conversation-tip and version drift must not weaken
        # recertification of any retained turn/attempt or control member.
        exercise_retained_replay_matrix()

        # Caller tuple/key and retained event corruption fail closed.  Count
        # two is conflicting here only because count one/key one was retained.
        for invalid in (
            replace(
                conflict,
                conflict_key=(
                    "transcript-conflict-" + "0" * 64
                ),
            ),
            independently_keyed_conflict(
                replace(terminal, status="failed"),
                1,
            ),
            independently_keyed_conflict(terminal, 2),
        ):
            assert_authority_conflict_without_writes(conn, runtime, invalid)
        corrupt_event_id = "transcript-conflict-" + "e" * 64
        assert corrupt_event_id != expected_key
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "UPDATE project_events SET event_id = ? WHERE event_id = ?",
            (corrupt_event_id, expected_key),
        )
        conn.execute(
            "UPDATE project_deliveries SET event_id = ? WHERE event_id = ?",
            (corrupt_event_id, expected_key),
        )
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON")
        assert tuple(conn.execute("PRAGMA foreign_key_check")) == ()
        assert_authority_conflict_without_writes(conn, runtime, conflict)
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "UPDATE project_events SET event_id = ? WHERE event_id = ?",
            (expected_key, corrupt_event_id),
        )
        conn.execute(
            "UPDATE project_deliveries SET event_id = ? WHERE event_id = ?",
            (expected_key, corrupt_event_id),
        )
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON")
        assert tuple(conn.execute("PRAGMA foreign_key_check")) == ()
        conn.execute(
            "UPDATE project_events SET payload_json = '{}' WHERE event_id = ?",
            (expected_key,),
        )
        conn.commit()
        assert_authority_conflict_without_writes(conn, runtime, conflict)
        conn.execute(
            "UPDATE project_events SET payload_json = ? WHERE event_id = ?",
            (expected_payload_json, expected_key),
        )
        conn.commit()

        # Each Projects mutation boundary rolls every user table back.
        for label, trigger in (
            (
                "gate",
                """
                CREATE TEMP TRIGGER c9_gate_fault
                BEFORE UPDATE ON project_runtime_state
                WHEN NEW.transcript_dispatch_block_key IS NOT NULL
                BEGIN SELECT RAISE(ABORT, 'c9 gate fault'); END
                """,
            ),
            (
                "event",
                """
                CREATE TEMP TRIGGER c9_event_fault
                BEFORE INSERT ON project_events
                WHEN NEW.kind = 'turn.transcript_conflicted'
                BEGIN SELECT RAISE(ABORT, 'c9 event fault'); END
                """,
            ),
        ):
            (
                fault_conn,
                fault_runtime,
                _,
                _,
                _,
                _,
                fault_conflict,
            ) = make_case(
                f"fault-{label}",
                batch_id=(
                    "223e4567-e89b-42d3-a456-426614174009"
                    if label == "gate"
                    else "323e4567-e89b-42d3-a456-426614174009"
                ),
            )
            fault_conn.executescript(trigger)
            fault_before = all_user_tables(fault_conn)
            with pytest.raises(sqlite3.IntegrityError, match=f"c9 {label} fault"):
                fault_runtime.record_terminal_transcript_conflict(
                    fault_conflict
                )
            assert all_user_tables(fault_conn) == fault_before
            fault_conn.execute(f"DROP TRIGGER c9_{label}_fault")
            assert (
                fault_runtime.record_terminal_transcript_conflict(
                    fault_conflict
                )
                == "recorded"
            )

        # Invalid carriers and authority/gate/lease proofs allocate nothing.
        (
            negative_conn,
            negative_runtime,
            negative_project,
            negative_turn,
            negative_claim,
            negative_terminal,
            negative_conflict,
        ) = make_case(
            "negatives",
            batch_id="423e4567-e89b-42d3-a456-426614174009",
        )
        retained_negative_horizon = negative_conn.execute(
            """
            SELECT claim_lease_expires_at FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (negative_project, negative_turn.turn_id),
        ).fetchone()[0]
        assert (
            retained_negative_horizon
            == negative_terminal.attempt.lease_expires_at
        )
        exact_tuple_mutations = {
            "ack.batch_id": replace(
                negative_terminal,
                batch_id="433e4567-e89b-42d3-a456-426614174009",
            ),
            "ack.status": replace(negative_terminal, status="failed"),
            "ack.result_id": replace(
                negative_terminal,
                result_id="443e4567-e89b-42d3-a456-426614174009",
            ),
            "attempt.project_id": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    project_id="c9-wrong-project",
                ),
            ),
            "attempt.turn_id": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    turn_id="c9-wrong-turn",
                ),
            ),
            "attempt.sequence": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    sequence=negative_terminal.attempt.sequence + 1,
                ),
            ),
            "attempt.worker_id": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    worker_id="c9-wrong-worker",
                ),
            ),
            "attempt.attempt_id": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    attempt_id="c9-wrong-attempt",
                ),
            ),
            "attempt.lease_generation": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    lease_generation=(
                        negative_terminal.attempt.lease_generation + 1
                    ),
                ),
            ),
            "attempt.fencing_token": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    fencing_token=(
                        negative_terminal.attempt.fencing_token + 1
                    ),
                ),
            ),
            "attempt.canonical_session_id": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    canonical_session_id="c9-wrong-conversation",
                ),
            ),
            "attempt.lease_expires_at": replace(
                negative_terminal,
                attempt=replace(
                    negative_terminal.attempt,
                    lease_expires_at=retained_negative_horizon + 1,
                ),
            ),
        }
        for tuple_mutation in exact_tuple_mutations.values():
            recomputed = independently_keyed_conflict(tuple_mutation, 1)
            assert recomputed.conflict_key == key_for(tuple_mutation, 1)
            assert recomputed.conflict_key != negative_conflict.conflict_key
            assert_authority_conflict_without_writes(
                negative_conn,
                negative_runtime,
                recomputed,
            )

        # Unsupported horizon is independent of retained-authority mismatch:
        # the carrier and durable control agree on the same out-of-range value.
        (
            horizon_conn,
            horizon_runtime,
            horizon_project,
            horizon_turn,
            _,
            horizon_terminal,
            _,
        ) = make_case(
            "unsupported-horizon",
            batch_id="453e4567-e89b-42d3-a456-426614174009",
        )
        unsupported_horizon = 253_402_300_800
        assert horizon_conn.execute(
            """
            UPDATE project_run_controls
            SET claim_lease_expires_at = ?
            WHERE project_id = ? AND turn_id = ?
            """,
            (
                unsupported_horizon,
                horizon_project,
                horizon_turn.turn_id,
            ),
        ).rowcount == 1
        horizon_conn.commit()
        assert horizon_conn.execute(
            """
            SELECT claim_lease_expires_at
            FROM project_run_controls
            WHERE project_id = ? AND turn_id = ?
            """,
            (horizon_project, horizon_turn.turn_id),
        ).fetchone()[0] == unsupported_horizon
        matching_unsupported_terminal = replace(
            horizon_terminal,
            attempt=replace(
                horizon_terminal.attempt,
                lease_expires_at=unsupported_horizon,
            ),
        )
        matching_unsupported_conflict = independently_keyed_conflict(
            matching_unsupported_terminal,
            1,
        )
        assert matching_unsupported_conflict.conflict_key == key_for(
            matching_unsupported_terminal,
            1,
        )
        assert_authority_conflict_without_writes(
            horizon_conn,
            horizon_runtime,
            matching_unsupported_conflict,
        )

        malformed_inputs = (
            independently_keyed_conflict(
                negative_terminal, 1, supplied_key=None
            ),
            independently_keyed_conflict(
                negative_terminal, 1, supplied_key=1
            ),
            independently_keyed_conflict(
                negative_terminal, 1, supplied_key=""
            ),
            independently_keyed_conflict(
                negative_terminal,
                1,
                supplied_key="transcript-conflict-" + "a" * 63,
            ),
            independently_keyed_conflict(
                negative_terminal,
                1,
                supplied_key="transcript-conflict-" + "a" * 65,
            ),
            independently_keyed_conflict(
                negative_terminal,
                1,
                supplied_key="wrong-prefix-" + "a" * 64,
            ),
            independently_keyed_conflict(
                negative_terminal,
                1,
                supplied_key="transcript-conflict-" + "A" * 64,
            ),
            independently_keyed_conflict(
                negative_terminal,
                1,
                supplied_key="transcript-conflict-" + "g" * 64,
            ),
            independently_keyed_conflict(negative_terminal, None),
            independently_keyed_conflict(negative_terminal, True),
            independently_keyed_conflict(negative_terminal, 1.0),
            independently_keyed_conflict(negative_terminal, "1"),
            independently_keyed_conflict(negative_terminal, -1),
            independently_keyed_conflict(negative_terminal, 2**63),
        )
        for invalid in malformed_inputs:
            assert_authority_conflict_without_writes(
                negative_conn, negative_runtime, invalid
            )

        def mutate_then_reject(sql, parameters=()):
            negative_conn.execute(sql, parameters)
            negative_conn.commit()
            assert_authority_conflict_without_writes(
                negative_conn,
                negative_runtime,
                negative_conflict,
            )

        # Each case uses a savepoint-like restore by writing the literal
        # retained value back outside the measured method call.
        mutate_then_reject(
            """
            UPDATE project_run_controls SET claim_worker_id = 'stale-worker'
            WHERE turn_id = ?
            """,
            (negative_turn.turn_id,),
        )
        negative_conn.execute(
            """
            UPDATE project_run_controls SET claim_worker_id = ?
            WHERE turn_id = ?
            """,
            (negative_claim.worker_id, negative_turn.turn_id),
        )
        negative_conn.commit()
        negative_conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id,
                lease_generation, fencing_token, expires_at, updated_at
            ) VALUES ('c9-negative-lease', ?, ?, ?, ?, ?, ?, 100)
            """,
            (
                negative_project,
                negative_turn.turn_id,
                negative_claim.worker_id,
                negative_claim.lease_generation,
                negative_claim.fencing_token,
                negative_claim.lease_expires_at,
            ),
        )
        negative_conn.commit()
        assert_authority_conflict_without_writes(
            negative_conn, negative_runtime, negative_conflict
        )
        negative_conn.execute(
            "DELETE FROM project_worker_leases WHERE lease_id = 'c9-negative-lease'"
        )
        negative_conn.commit()
        negative_conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id,
                lease_generation, fencing_token, expires_at, updated_at
            ) VALUES (
                'c9-mismatched-lease', ?, ?, 'mismatched-worker',
                ?, ?, ?, 100
            )
            """,
            (
                negative_project,
                negative_turn.turn_id,
                negative_claim.lease_generation + 1,
                negative_claim.fencing_token + 1,
                negative_claim.lease_expires_at,
            ),
        )
        negative_conn.commit()
        assert_authority_conflict_without_writes(
            negative_conn, negative_runtime, negative_conflict
        )
        negative_conn.execute(
            """
            DELETE FROM project_worker_leases
            WHERE lease_id = 'c9-mismatched-lease'
            """
        )
        negative_conn.commit()
        negative_conn.execute(
            """
            UPDATE project_turns SET attempt_id = 'corrupt-retained-attempt'
            WHERE turn_id = ?
            """,
            (negative_turn.turn_id,),
        )
        negative_conn.commit()
        assert_authority_conflict_without_writes(
            negative_conn, negative_runtime, negative_conflict
        )
        negative_conn.execute(
            """
            UPDATE project_turns SET attempt_id = ?
            WHERE turn_id = ?
            """,
            (negative_claim.attempt_id, negative_turn.turn_id),
        )
        negative_conn.commit()
        for pending, block in (
            (None, None),
            ("523e4567-e89b-42d3-a456-426614174009", None),
            (None, "another-block"),
        ):
            negative_conn.execute(
                """
                UPDATE project_runtime_state
                SET transcript_pending_batch_id = ?,
                    transcript_dispatch_block_key = ?
                WHERE project_id = ?
                """,
                (pending, block, negative_project),
            )
            negative_conn.commit()
            assert_authority_conflict_without_writes(
                negative_conn, negative_runtime, negative_conflict
            )

        (
            wrong_tip_conn,
            wrong_tip_runtime,
            wrong_tip_project,
            _,
            _,
            _,
            wrong_tip_conflict,
        ) = make_case(
            "wrong-first-record-tip",
            batch_id="573e4567-e89b-42d3-a456-426614174009",
        )
        wrong_tip_conn.execute(
            """
            INSERT INTO project_conversations (
                conversation_id, project_id, parent_conversation_id,
                root_conversation_id, created_at
            ) VALUES (
                'c9-wrong-first-record-tip', ?, 'session-root',
                'session-root', 101
            )
            """,
            (wrong_tip_project,),
        )
        wrong_tip_conn.execute(
            """
            UPDATE project_runtime_state
            SET conversation_tip_id = 'c9-wrong-first-record-tip'
            WHERE project_id = ?
            """,
            (wrong_tip_project,),
        )
        wrong_tip_conn.commit()
        assert_authority_conflict_without_writes(
            wrong_tip_conn, wrong_tip_runtime, wrong_tip_conflict
        )

        negative_conn.execute(
            """
                UPDATE project_runtime_state
                SET transcript_pending_batch_id = ?,
                    transcript_dispatch_block_key = NULL,
                    lifecycle = 'completed'
                WHERE project_id = ?
            """,
            (negative_terminal.batch_id, negative_project),
        )
        negative_conn.commit()
        assert_authority_conflict_without_writes(
            negative_conn, negative_runtime, negative_conflict
        )

        (
            applied_conn,
            applied_runtime,
            _,
            applied_turn,
            _,
            _,
            applied_conflict,
        ) = make_case(
            "applied",
            batch_id="623e4567-e89b-42d3-a456-426614174009",
        )
        applied_conn.execute(
            """
            UPDATE project_turns SET transcript_applied_batch_id = ?
            WHERE turn_id = ?
            """,
            (applied_conflict.terminal.batch_id, applied_turn.turn_id),
        )
        applied_conn.commit()
        assert_authority_conflict_without_writes(
            applied_conn, applied_runtime, applied_conflict
        )
    finally:
        for connection in reversed(opened):
            try:
                connection.close()
            except sqlite3.Error:
                pass


def test_task7_c8_publish_ack_terminal_resolver_and_projects_ack_are_exact_atomic_and_replayable(
    tmp_path,
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "c8-projects-ack.db"
    )
    extra_connections = []
    try:
        batch_id = "123e4567-e89b-42d3-a456-426614174000"
        later_batch_id = "223e4567-e89b-42d3-a456-426614174000"
        assert tuple(field.name for field in fields(module.PreparedTerminalDecision)) == (
            "action", "terminal", "discard_authority"
        )
        assert get_type_hints(module.PreparedTerminalDecision) == {
            "action": Literal["wait", "publish", "discard"],
            "terminal": module.TerminalTurnResult | None,
            "discard_authority": Literal[
                "stop_requested", "cancelled", "superseded_attempt",
                "superseded_terminal", "recovery_blocked",
            ] | None,
        }
        assert tuple(field.name for field in fields(module.TerminalTurnResult)) == (
            "attempt",
            "status",
            "result_id",
        )
        assert get_type_hints(module.TerminalTurnResult) == {
            "attempt": module.TurnAttemptIdentity,
            "status": Literal["succeeded", "failed"],
            "result_id": str,
        }
        assert tuple(
            field.name
            for field in fields(module.TerminalTranscriptAcknowledgement)
        ) == ("batch_id", "attempt", "status", "result_id")
        assert get_type_hints(module.TerminalTranscriptAcknowledgement) == {
            "batch_id": str,
            "attempt": module.TurnAttemptIdentity,
            "status": Literal["succeeded", "failed"],
            "result_id": str,
        }
        def attempt_for(value):
            return module.TurnAttemptIdentity(
                project_id=value.project_id,
                turn_id=value.turn_id,
                sequence=value.sequence,
                worker_id=value.worker_id,
                attempt_id=value.attempt_id,
                lease_generation=value.lease_generation,
                fencing_token=value.fencing_token,
                canonical_session_id=value.canonical_session_id,
                lease_expires_at=value.lease_expires_at,
            )

        turn, claim = _enqueue_and_claim(runtime, project_id, actor, key="c8")
        claim = runtime.mark_turn_started(claim)
        attempt = attempt_for(claim)

        waiting = runtime.resolve_prepared_terminal(
            attempt, prepared_result_id=batch_id, status="succeeded"
        )
        assert waiting.action == "wait"
        assert waiting.terminal is None
        assert waiting.discard_authority is None

        runtime.commit_turn_with_task7_batch(
            claim,
            module.CanonicalTurnResult("succeeded", batch_id),
            transcript_batch_id=batch_id,
        )
        resolver_before = _claim_snapshot(conn, project_id, turn.turn_id)
        decision = runtime.resolve_prepared_terminal(
            attempt, prepared_result_id=batch_id, status="succeeded"
        )
        assert decision.action == "publish"
        assert decision.terminal == module.TerminalTurnResult(
            attempt=attempt, status="succeeded", result_id=batch_id
        )
        assert decision.discard_authority is None
        with pytest.raises(FrozenInstanceError):
            decision.action = "wait"
        with pytest.raises(FrozenInstanceError):
            decision.terminal.result_id = "changed"
        superseded_terminal = runtime.resolve_prepared_terminal(
            attempt,
            prepared_result_id="223e4567-e89b-42d3-a456-426614174000",
            status="succeeded",
        )
        assert superseded_terminal.action == "discard"
        assert superseded_terminal.terminal is None
        assert superseded_terminal.discard_authority == "superseded_terminal"
        observed_discards = {
            "superseded_terminal": superseded_terminal.discard_authority
        }
        malformed = replace(attempt, canonical_session_id="wrong-session")
        with pytest.raises(module.ProjectRuntimeError) as malformed_proof:
            runtime.resolve_prepared_terminal(
                malformed, prepared_result_id=batch_id, status="succeeded"
            )
        assert (
            malformed_proof.value.code
            is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
        )
        assert _claim_snapshot(conn, project_id, turn.turn_id) == resolver_before
        for column, value in (
            ("transcript_pending_batch_id", later_batch_id),
            ("transcript_dispatch_block_key", "c8 resolver block"),
        ):
            if column == "transcript_dispatch_block_key":
                conn.execute(
                    "UPDATE project_runtime_state SET transcript_pending_batch_id = NULL, "
                    "transcript_dispatch_block_key = ? WHERE project_id = ?",
                    (value, project_id),
                )
            else:
                conn.execute(
                    "UPDATE project_runtime_state SET transcript_pending_batch_id = ? "
                    "WHERE project_id = ?",
                    (value, project_id),
                )
            conn.commit()
            before_gate_drift = _claim_snapshot(conn, project_id, turn.turn_id)
            with pytest.raises(module.ProjectRuntimeError) as gate_drift:
                runtime.resolve_prepared_terminal(
                    attempt, prepared_result_id=batch_id, status="succeeded"
                )
            assert gate_drift.value.code is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            assert _claim_snapshot(conn, project_id, turn.turn_id) == before_gate_drift
            conn.execute(
                "UPDATE project_runtime_state SET transcript_pending_batch_id = ?, "
                "transcript_dispatch_block_key = NULL WHERE project_id = ?",
                (batch_id, project_id),
            )
            conn.commit()
        (
            applied_module,
            applied_conn,
            applied_runtime,
            applied_project,
            applied_actor,
        ) = _make_runtime(tmp_path / "c8-resolver-applied-reject.db")
        try:
            applied_turn, applied_claim = _enqueue_and_claim(
                applied_runtime,
                applied_project,
                applied_actor,
                key="c8-applied",
            )
            applied_claim = applied_runtime.mark_turn_started(applied_claim)
            applied_runtime.commit_turn_with_task7_batch(
                applied_claim,
                applied_module.CanonicalTurnResult("succeeded", batch_id),
                transcript_batch_id=batch_id,
            )
            applied_conn.execute(
                """
                UPDATE project_turns
                SET transcript_applied_batch_id = ?
                WHERE turn_id = ?
                """,
                (batch_id, applied_turn.turn_id),
            )
            applied_conn.commit()
            before_applied_drift = _claim_snapshot(
                applied_conn,
                applied_project,
                applied_turn.turn_id,
            )
            with pytest.raises(applied_module.ProjectRuntimeError) as applied_drift:
                applied_runtime.resolve_prepared_terminal(
                    attempt_for(applied_claim),
                    prepared_result_id=batch_id,
                    status="succeeded",
                )
            assert (
                applied_drift.value.code
                is applied_module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert _claim_snapshot(
                applied_conn,
                applied_project,
                applied_turn.turn_id,
            ) == before_applied_drift
        finally:
            applied_conn.close()

        acknowledgement = module.TerminalTranscriptAcknowledgement(
            batch_id=batch_id,
            attempt=attempt,
            status="succeeded",
            result_id=batch_id,
        )
        with pytest.raises(FrozenInstanceError):
            acknowledgement.batch_id = later_batch_id

        ack_authority_tables = {
            "PROJECT_TURNS",
            "PROJECT_RUN_CONTROLS",
            "PROJECT_RUNTIME_STATE",
            "PROJECT_WORKER_LEASES",
        }

        def user_table_snapshot(connection):
            """Deterministically capture every mutable user-table row."""
            tables = tuple(
                row[0]
                for row in connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                      AND name NOT LIKE 'sqlite_%'
                    ORDER BY name
                    """
                )
            )
            snapshot = []
            for table in tables:
                quoted = table.replace('"', '""')
                columns = tuple(
                    row[1]
                    for row in connection.execute(
                        f'PRAGMA table_info("{quoted}")'
                    )
                )
                order = ", ".join(
                    str(position)
                    for position in range(1, len(columns) + 1)
                )
                rows = tuple(
                    tuple(row)
                    for row in connection.execute(
                        f'SELECT * FROM "{quoted}" ORDER BY {order}'
                    )
                )
                snapshot.append((table, columns, rows))
            return tuple(snapshot)

        def assert_owned_ack_trace(
            statements,
            *,
            ending,
            expect_ack_mutations,
            require_all_authority_reads=True,
        ):
            normalized = [
                " ".join(statement.upper().split())
                for statement in statements
            ]
            begin_positions = [
                index
                for index, statement in enumerate(normalized)
                if statement.startswith("BEGIN")
            ]
            assert len(begin_positions) == 1
            assert normalized[begin_positions[0]] == "BEGIN IMMEDIATE"
            terminal_positions = [
                index
                for index, statement in enumerate(normalized)
                if statement in {"COMMIT", "ROLLBACK"}
            ]
            assert len(terminal_positions) == 1
            terminal_position = terminal_positions[0]
            assert normalized[terminal_position] == ending
            authority_reads = []
            participating_tables = set()
            for index, statement in enumerate(normalized):
                for table in ack_authority_tables:
                    if re.search(
                        rf"(?:FROM|JOIN|,)\s+"
                        rf"(?:(?:MAIN|TEMP)\s*\.\s*)?"
                        rf"[`\"\[]?{table}[`\"\]]?(?:\s|$)",
                        statement,
                    ):
                        participating_tables.add(table)
                        authority_reads.append(index)
            if require_all_authority_reads:
                assert participating_tables == ack_authority_tables
            else:
                assert participating_tables <= ack_authority_tables
            assert authority_reads

            dml = [
                (index, statement)
                for index, statement in enumerate(normalized)
                if re.match(
                    r"^(?:INSERT|UPDATE|DELETE|REPLACE)\b",
                    statement,
                )
            ]
            classified_dml = []
            for index, statement in dml:
                if re.match(
                    r"^UPDATE\s+"
                    r"(?:(?:MAIN|TEMP)\s*\.\s*)?"
                    r"[`\"\[]?PROJECT_TURNS[`\"\]]?\s+"
                    r"SET\s+TRANSCRIPT_APPLIED_BATCH_ID\s*=",
                    statement,
                ) and "," not in statement.split(
                    " SET ", 1
                )[1].split(" WHERE ", 1)[0]:
                    classified_dml.append(
                        (
                            index,
                            "PROJECT_TURNS",
                            "TRANSCRIPT_APPLIED_BATCH_ID",
                        )
                    )
                    continue
                if re.match(
                    r"^UPDATE\s+"
                    r"(?:(?:MAIN|TEMP)\s*\.\s*)?"
                    r"[`\"\[]?PROJECT_RUNTIME_STATE[`\"\]]?\s+"
                    r"SET\s+TRANSCRIPT_PENDING_BATCH_ID\s*=",
                    statement,
                ) and "," not in statement.split(
                    " SET ", 1
                )[1].split(" WHERE ", 1)[0]:
                    classified_dml.append(
                        (
                            index,
                            "PROJECT_RUNTIME_STATE",
                            "TRANSCRIPT_PENDING_BATCH_ID",
                        )
                    )
                    continue
                raise AssertionError(
                    f"ack touched unrelated mutable state: {statement}"
                )

            if expect_ack_mutations:
                assert {
                    (table, column)
                    for _, table, column in classified_dml
                } == {
                    (
                        "PROJECT_TURNS",
                        "TRANSCRIPT_APPLIED_BATCH_ID",
                    ),
                    (
                        "PROJECT_RUNTIME_STATE",
                        "TRANSCRIPT_PENDING_BATCH_ID",
                    ),
                }
            else:
                assert dml == []
            bounded_authority = [
                *authority_reads,
                *(index for index, _ in dml),
            ]
            assert all(
                begin_positions[0] < index < terminal_position
                for index in bounded_authority
            )
            assert not [
                index
                for index in bounded_authority
                if index > terminal_position
            ]
            return normalized

        # Each first acknowledgement is isolated behind one invalid C7 gate
        # shape.  None may partially mark the turn applied or repair the gate.
        for gate_name, pending_batch, block_key in (
            ("pending-mismatch", later_batch_id, None),
            ("pending-null", None, None),
            ("dispatch-blocked", None, "c8-ack-blocked"),
        ):
            (
                gate_module,
                gate_conn,
                gate_runtime,
                gate_project,
                gate_actor,
            ) = _make_runtime(tmp_path / f"c8-ack-gate-{gate_name}.db")
            extra_connections.append(gate_conn)
            gate_turn, gate_claim = _enqueue_and_claim(
                gate_runtime,
                gate_project,
                gate_actor,
                key=f"c8-ack-{gate_name}",
            )
            gate_claim = gate_runtime.mark_turn_started(gate_claim)
            gate_runtime.commit_turn_with_task7_batch(
                gate_claim,
                gate_module.CanonicalTurnResult("succeeded", batch_id),
                transcript_batch_id=batch_id,
            )
            gate_conn.execute(
                """
                UPDATE project_runtime_state
                SET transcript_pending_batch_id = ?,
                    transcript_dispatch_block_key = ?
                WHERE project_id = ?
                """,
                (pending_batch, block_key, gate_project),
            )
            gate_conn.commit()
            gate_acknowledgement = (
                gate_module.TerminalTranscriptAcknowledgement(
                    batch_id=batch_id,
                    attempt=attempt_for(gate_claim),
                    status="succeeded",
                    result_id=batch_id,
                )
            )
            gate_before = user_table_snapshot(gate_conn)
            gate_changes = gate_conn.total_changes
            gate_trace = []
            gate_conn.set_trace_callback(gate_trace.append)
            try:
                with pytest.raises(
                    gate_module.ProjectRuntimeError
                ) as invalid_gate_ack:
                    gate_runtime.ack_terminal_transcript_applied(
                        gate_acknowledgement
                    )
            finally:
                gate_conn.set_trace_callback(None)
            assert (
                invalid_gate_ack.value.code
                is gate_module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert_owned_ack_trace(
                gate_trace,
                ending="ROLLBACK",
                expect_ack_mutations=False,
            )
            assert gate_conn.in_transaction is False
            assert gate_conn.total_changes == gate_changes
            assert user_table_snapshot(gate_conn) == gate_before

        negative_acknowledgements = [
            replace(acknowledgement, batch_id=later_batch_id),
            replace(
                acknowledgement,
                attempt=replace(attempt, project_id="different-project"),
            ),
            replace(
                acknowledgement,
                attempt=replace(attempt, turn_id="different-turn"),
            ),
            replace(
                acknowledgement,
                attempt=replace(attempt, sequence=attempt.sequence + 1),
            ),
            replace(
                acknowledgement,
                attempt=replace(attempt, worker_id="different-worker"),
            ),
            replace(
                acknowledgement,
                attempt=replace(attempt, attempt_id="different-attempt"),
            ),
            replace(
                acknowledgement,
                attempt=replace(
                    attempt,
                    lease_generation=attempt.lease_generation + 1,
                ),
            ),
            replace(
                acknowledgement,
                attempt=replace(
                    attempt,
                    fencing_token=attempt.fencing_token + 1,
                ),
            ),
            replace(
                acknowledgement,
                attempt=replace(
                    attempt,
                    canonical_session_id="different-session",
                ),
            ),
            replace(
                acknowledgement,
                attempt=replace(
                    attempt,
                    lease_expires_at=attempt.lease_expires_at + 1,
                ),
            ),
            replace(acknowledgement, status="failed"),
            replace(acknowledgement, result_id=later_batch_id),
        ]
        for invalid_acknowledgement in negative_acknowledgements:
            before_invalid_ack = user_table_snapshot(conn)
            invalid_trace = []
            before_invalid_changes = conn.total_changes
            conn.set_trace_callback(invalid_trace.append)
            try:
                with pytest.raises(
                    module.ProjectRuntimeError
                ) as invalid_ack:
                    runtime.ack_terminal_transcript_applied(
                        invalid_acknowledgement
                    )
            finally:
                conn.set_trace_callback(None)
            assert (
                invalid_ack.value.code
                is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert conn.in_transaction is False
            assert conn.total_changes == before_invalid_changes
            assert user_table_snapshot(conn) == before_invalid_ack
            assert_owned_ack_trace(
                invalid_trace,
                ending="ROLLBACK",
                expect_ack_mutations=False,
            )

        conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id,
                lease_generation, fencing_token, expires_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "c8-unexpected-terminal-lease",
                project_id,
                turn.turn_id,
                attempt.worker_id,
                attempt.lease_generation,
                attempt.fencing_token,
                attempt.lease_expires_at,
                100,
            ),
        )
        conn.commit()
        before_unexpected_lease = user_table_snapshot(conn)
        unexpected_lease_changes = conn.total_changes
        unexpected_lease_trace = []
        conn.set_trace_callback(unexpected_lease_trace.append)
        try:
            with pytest.raises(
                module.ProjectRuntimeError
            ) as unexpected_lease:
                runtime.ack_terminal_transcript_applied(
                    acknowledgement
                )
        finally:
            conn.set_trace_callback(None)
        assert (
            unexpected_lease.value.code
            is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
        )
        assert conn.in_transaction is False
        assert conn.total_changes == unexpected_lease_changes
        assert user_table_snapshot(conn) == before_unexpected_lease
        assert_owned_ack_trace(
            unexpected_lease_trace,
            ending="ROLLBACK",
            expect_ack_mutations=False,
        )
        conn.execute(
            "DELETE FROM project_worker_leases WHERE lease_id = ?",
            ("c8-unexpected-terminal-lease",),
        )
        conn.commit()

        caller_owned_before = user_table_snapshot(conn)
        caller_owned_changes = conn.total_changes
        conn.execute("BEGIN IMMEDIATE")
        caller_owned_trace = []
        conn.set_trace_callback(caller_owned_trace.append)
        try:
            with pytest.raises(module.ProjectRuntimeError) as nested_ack:
                runtime.ack_terminal_transcript_applied(acknowledgement)
        finally:
            conn.set_trace_callback(None)
        assert nested_ack.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert caller_owned_trace == []
        assert conn.total_changes == caller_owned_changes
        assert user_table_snapshot(conn) == caller_owned_before
        conn.rollback()

        before_ack_fault = user_table_snapshot(conn)
        conn.executescript(
            f"""
            CREATE TEMP TRIGGER c8_ack_rolls_back
            BEFORE UPDATE OF transcript_pending_batch_id
            ON project_runtime_state
            BEGIN
                SELECT CASE
                    WHEN (
                        SELECT transcript_applied_batch_id
                        FROM project_turns
                        WHERE turn_id = '{turn.turn_id}'
                    ) = '{batch_id}'
                    THEN RAISE(
                        ABORT,
                        'c8 fault after applied mutation before pending clear'
                    )
                    ELSE RAISE(
                        ABORT,
                        'c8 pending clear attempted before applied mutation'
                    )
                END;
            END;
            """
        )
        try:
            fault_trace = []
            conn.set_trace_callback(fault_trace.append)
            try:
                with pytest.raises(
                    sqlite3.IntegrityError,
                    match=(
                        "c8 fault after applied mutation "
                        "before pending clear"
                    ),
                ):
                    runtime.ack_terminal_transcript_applied(
                        acknowledgement
                    )
            finally:
                conn.set_trace_callback(None)
        finally:
            conn.execute("DROP TRIGGER c8_ack_rolls_back")
        assert_owned_ack_trace(
            fault_trace,
            ending="ROLLBACK",
            expect_ack_mutations=True,
        )
        assert conn.in_transaction is False
        assert user_table_snapshot(conn) == before_ack_fault

        def acknowledgement_immutable_snapshot():
            runtime_state = dict(
                conn.execute(
                    """
                    SELECT * FROM project_runtime_state
                    WHERE project_id = ?
                    """,
                    (project_id,),
                ).fetchone()
            )
            runtime_state.pop("transcript_pending_batch_id")
            turn_state = dict(
                conn.execute(
                    "SELECT * FROM project_turns WHERE turn_id = ?",
                    (turn.turn_id,),
                ).fetchone()
            )
            turn_state.pop("transcript_applied_batch_id")
            return (
                runtime_state,
                turn_state,
                tuple(
                    conn.execute(
                        """
                        SELECT * FROM project_run_controls
                        WHERE turn_id = ?
                        """,
                        (turn.turn_id,),
                    ).fetchone()
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_worker_leases
                        WHERE project_id = ? AND turn_id = ?
                        ORDER BY lease_id
                        """,
                        (project_id, turn.turn_id),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_events
                        WHERE project_id = ? ORDER BY sequence
                        """,
                        (project_id,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_deliveries
                        WHERE project_id = ? ORDER BY delivery_id
                        """,
                        (project_id,),
                    )
                ),
            )

        immutable_before_ack = acknowledgement_immutable_snapshot()
        ack_trace = []
        conn.set_trace_callback(ack_trace.append)
        try:
            assert runtime.ack_terminal_transcript_applied(acknowledgement) == "acknowledged"
        finally:
            conn.set_trace_callback(None)
        assert_owned_ack_trace(
            ack_trace,
            ending="COMMIT",
            expect_ack_mutations=True,
        )
        assert conn.in_transaction is False
        assert tuple(conn.execute(
            """
            SELECT transcript_pending_batch_id, transcript_dispatch_block_key
            FROM project_runtime_state WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()) == (None, None)
        assert conn.execute(
            "SELECT transcript_applied_batch_id FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == batch_id
        assert acknowledgement_immutable_snapshot() == immutable_before_ack

        # Already-applied replay still depends on every retained turn/control
        # field that proves the exact attempt.  Corrupt and restore one field
        # at a time; setup writes are outside the measured replay window.
        retained_turn = dict(
            conn.execute(
                """
                SELECT * FROM project_turns
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn.turn_id),
            ).fetchone()
        )
        retained_control = dict(
            conn.execute(
                """
                SELECT * FROM project_run_controls
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn.turn_id),
            ).fetchone()
        )
        retained_mutations = (
            (
                "turn-project",
                "project_turns",
                "project_id",
                retained_turn["project_id"],
                "c8-corrupt-project",
                "turn_id",
                turn.turn_id,
            ),
            (
                "turn-id",
                "project_turns",
                "turn_id",
                retained_turn["turn_id"],
                "c8-corrupt-turn",
                "turn_id",
                turn.turn_id,
            ),
            (
                "turn-sequence",
                "project_turns",
                "sequence",
                retained_turn["sequence"],
                retained_turn["sequence"] + 1_000,
                "turn_id",
                turn.turn_id,
            ),
            (
                "turn-attempt",
                "project_turns",
                "attempt_id",
                retained_turn["attempt_id"],
                "c8-corrupt-turn-attempt",
                "turn_id",
                turn.turn_id,
            ),
            (
                "turn-generation",
                "project_turns",
                "lease_generation",
                retained_turn["lease_generation"],
                retained_turn["lease_generation"] + 1,
                "turn_id",
                turn.turn_id,
            ),
            (
                "turn-fence",
                "project_turns",
                "fencing_token",
                retained_turn["fencing_token"],
                retained_turn["fencing_token"] + 1,
                "turn_id",
                turn.turn_id,
            ),
            (
                "turn-status",
                "project_turns",
                "status",
                retained_turn["status"],
                "failed",
                "turn_id",
                turn.turn_id,
            ),
            (
                "turn-result",
                "project_turns",
                "terminal_result_id",
                retained_turn["terminal_result_id"],
                later_batch_id,
                "turn_id",
                turn.turn_id,
            ),
            (
                "turn-execution",
                "project_turns",
                "execution_state",
                retained_turn["execution_state"],
                "not_started",
                "turn_id",
                turn.turn_id,
            ),
            (
                "control-project",
                "project_run_controls",
                "project_id",
                retained_control["project_id"],
                "c8-corrupt-project",
                "turn_id",
                turn.turn_id,
            ),
            (
                "control-turn",
                "project_run_controls",
                "turn_id",
                retained_control["turn_id"],
                "c8-corrupt-control-turn",
                "turn_id",
                turn.turn_id,
            ),
            (
                "control-attempt",
                "project_run_controls",
                "attempt_id",
                retained_control["attempt_id"],
                "c8-corrupt-control-attempt",
                "turn_id",
                turn.turn_id,
            ),
            (
                "control-worker",
                "project_run_controls",
                "claim_worker_id",
                retained_control["claim_worker_id"],
                "c8-corrupt-worker",
                "turn_id",
                turn.turn_id,
            ),
            (
                "control-horizon",
                "project_run_controls",
                "claim_lease_expires_at",
                retained_control["claim_lease_expires_at"],
                attempt.lease_expires_at - 1,
                "turn_id",
                turn.turn_id,
            ),
            (
                "control-session",
                "project_run_controls",
                "claim_canonical_session_id",
                retained_control["claim_canonical_session_id"],
                "c8-corrupt-session",
                "turn_id",
                turn.turn_id,
            ),
            (
                "control-state",
                "project_run_controls",
                "control_state",
                retained_control["control_state"],
                "running",
                "turn_id",
                turn.turn_id,
            ),
        )
        retained_baseline = user_table_snapshot(conn)

        def set_retained_value(
            table,
            column,
            value,
            lookup_column,
            lookup_value,
        ):
            relax_foreign_keys = column in {"project_id", "turn_id"}
            if relax_foreign_keys:
                conn.execute("PRAGMA foreign_keys = OFF")
            try:
                quoted_table = table.replace('"', '""')
                quoted_column = column.replace('"', '""')
                quoted_lookup = lookup_column.replace('"', '""')
                assert conn.execute(
                    f"""
                    UPDATE "{quoted_table}"
                    SET "{quoted_column}" = ?
                    WHERE "{quoted_lookup}" = ?
                    """,
                    (value, lookup_value),
                ).rowcount == 1
                conn.commit()
            finally:
                if conn.in_transaction:
                    conn.rollback()
                if relax_foreign_keys:
                    conn.execute("PRAGMA foreign_keys = ON")

        for (
            label,
            table,
            column,
            original,
            corrupted,
            lookup_column,
            lookup_value,
        ) in retained_mutations:
            if label == "turn-result":
                # Once the applied marker exists, the canonical schema makes
                # terminal_result_id immutable.  That trigger is itself the
                # retained-proof binding for this field.
                guarded_before = user_table_snapshot(conn)
                guarded_changes = conn.total_changes
                with pytest.raises(
                    sqlite3.IntegrityError,
                    match="invalid Task-7 applied transcript batch",
                ):
                    set_retained_value(
                        table,
                        column,
                        corrupted,
                        lookup_column,
                        lookup_value,
                    )
                assert conn.total_changes == guarded_changes
                assert user_table_snapshot(conn) == guarded_before
                continue
            set_retained_value(
                table,
                column,
                corrupted,
                lookup_column,
                lookup_value,
            )
            corrupt_before = user_table_snapshot(conn)
            corrupt_changes = conn.total_changes
            corrupt_trace = []
            conn.set_trace_callback(corrupt_trace.append)
            try:
                with pytest.raises(
                    module.ProjectRuntimeError
                ) as corrupt_replay:
                    runtime.ack_terminal_transcript_applied(
                        acknowledgement
                    )
            finally:
                conn.set_trace_callback(None)
            assert (
                corrupt_replay.value.code
                is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert conn.total_changes == corrupt_changes
            assert user_table_snapshot(conn) == corrupt_before
            assert_owned_ack_trace(
                corrupt_trace,
                ending="ROLLBACK",
                expect_ack_mutations=False,
                require_all_authority_reads=label not in {
                    "turn-project",
                    "turn-id",
                    "turn-execution",
                    "control-project",
                    "control-turn",
                },
            )
            restore_lookup = (
                corrupted
                if column == lookup_column
                else lookup_value
            )
            set_retained_value(
                table,
                column,
                original,
                lookup_column,
                restore_lookup,
            )
            assert user_table_snapshot(conn) == retained_baseline

        conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id,
                lease_generation, fencing_token, expires_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "c8-post-ack-lease",
                project_id,
                turn.turn_id,
                attempt.worker_id,
                attempt.lease_generation,
                attempt.fencing_token,
                attempt.lease_expires_at,
                100,
            ),
        )
        conn.commit()
        leased_before = user_table_snapshot(conn)
        leased_changes = conn.total_changes
        leased_trace = []
        conn.set_trace_callback(leased_trace.append)
        try:
            with pytest.raises(
                module.ProjectRuntimeError
            ) as leased_replay:
                runtime.ack_terminal_transcript_applied(
                    acknowledgement
                )
        finally:
            conn.set_trace_callback(None)
        assert (
            leased_replay.value.code
            is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
        )
        assert conn.total_changes == leased_changes
        assert user_table_snapshot(conn) == leased_before
        assert_owned_ack_trace(
            leased_trace,
            ending="ROLLBACK",
            expect_ack_mutations=False,
        )
        conn.execute(
            "DELETE FROM project_worker_leases WHERE lease_id = ?",
            ("c8-post-ack-lease",),
        )
        conn.commit()
        assert user_table_snapshot(conn) == retained_baseline

        restored_before = user_table_snapshot(conn)
        restored_changes = conn.total_changes
        restored_trace = []
        conn.set_trace_callback(restored_trace.append)
        try:
            assert (
                runtime.ack_terminal_transcript_applied(
                    acknowledgement
                )
                == "already_acknowledged"
            )
        finally:
            conn.set_trace_callback(None)
        assert conn.total_changes == restored_changes
        assert user_table_snapshot(conn) == restored_before
        assert_owned_ack_trace(
            restored_trace,
            ending="COMMIT",
            expect_ack_mutations=False,
        )

        # Create a real successor through the public queue/claim/commit flow.
        # The old applied acknowledgement must ignore this newer pending gate.
        successor_version = conn.execute(
            """
            SELECT version FROM project_runtime_state
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()[0]
        successor_turn = runtime.enqueue_turn(
            project_id,
            {"message": "real C8 successor"},
            actor,
            idempotency_key="c8-real-successor",
            expected_version=successor_version,
        )
        successor_claim = runtime.claim_next_turn(
            project_id,
            "c8-successor-worker",
            lease_seconds=30,
        )
        assert successor_claim is not None
        assert successor_claim.turn_id == successor_turn.turn_id
        successor_claim = runtime.mark_turn_started(
            successor_claim
        )
        runtime.commit_turn_with_task7_batch(
            successor_claim,
            module.CanonicalTurnResult(
                "succeeded",
                later_batch_id,
            ),
            transcript_batch_id=later_batch_id,
        )
        assert tuple(
            conn.execute(
                """
                SELECT transcript_pending_batch_id,
                       transcript_dispatch_block_key
                FROM project_runtime_state
                WHERE project_id = ?
                """,
                (project_id,),
            ).fetchone()
        ) == (later_batch_id, None)

        advanced_tip_id = "c8-post-terminal-tip"
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                """
                INSERT INTO project_conversations (
                    conversation_id, project_id,
                    parent_conversation_id, root_conversation_id,
                    created_at
                ) VALUES (?, ?, 'session-root', 'session-root', 101)
                """,
                (advanced_tip_id, project_id),
            )
            assert conn.execute(
                """
                UPDATE project_runtime_state
                SET lifecycle = 'awaiting_acceptance',
                    conversation_tip_id = ?,
                    version = version + 1,
                    updated_at = 101
                WHERE project_id = ?
                  AND lifecycle = 'active'
                  AND conversation_tip_id = 'session-root'
                  AND transcript_pending_batch_id = ?
                  AND transcript_dispatch_block_key IS NULL
                """,
                (
                    advanced_tip_id,
                    project_id,
                    later_batch_id,
                ),
            ).rowcount == 1
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

        successor_before = user_table_snapshot(conn)
        successor_changes = conn.total_changes
        successor_trace = []
        conn.set_trace_callback(successor_trace.append)
        try:
            assert (
                runtime.ack_terminal_transcript_applied(
                    acknowledgement
                )
                == "already_acknowledged"
            )
        finally:
            conn.set_trace_callback(None)
        assert conn.total_changes == successor_changes
        assert user_table_snapshot(conn) == successor_before
        assert_owned_ack_trace(
            successor_trace,
            ending="COMMIT",
            expect_ack_mutations=False,
        )
        assert tuple(
            conn.execute(
                """
                SELECT transcript_pending_batch_id,
                       transcript_dispatch_block_key
                FROM project_runtime_state
                WHERE project_id = ?
                """,
                (project_id,),
            ).fetchone()
        ) == (later_batch_id, None)

        def new_resolver_case(name, *, clock=None):
            values = _make_runtime(
                tmp_path / f"c8-resolver-{name}.db",
                clock=clock,
            )
            extra_connections.append(values[1])
            return values

        def resolve_without_writes(
            case_runtime,
            case_conn,
            case_attempt,
            *,
            prepared_result_id,
            expected_action,
            expected_authority=None,
            status="succeeded",
        ):
            before = _claim_snapshot(
                case_conn,
                case_attempt.project_id,
                case_attempt.turn_id,
            )
            before_changes = case_conn.total_changes
            statements = []
            case_conn.set_trace_callback(statements.append)
            try:
                resolved = case_runtime.resolve_prepared_terminal(
                    case_attempt,
                    prepared_result_id=prepared_result_id,
                    status=status,
                )
            finally:
                case_conn.set_trace_callback(None)
            assert case_conn.total_changes == before_changes
            assert _claim_snapshot(
                case_conn,
                case_attempt.project_id,
                case_attempt.turn_id,
            ) == before
            assert not [
                statement
                for statement in statements
                if statement.lstrip().upper().startswith(
                    ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
            ]
            assert resolved.action == expected_action
            assert resolved.discard_authority == expected_authority
            if expected_action == "publish":
                assert resolved.terminal == module.TerminalTurnResult(
                    attempt=case_attempt,
                    status=status,
                    result_id=prepared_result_id,
                )
            else:
                assert resolved.terminal is None
            return resolved

        # Public stop history deterministically dominates a positive prepared
        # result and carries the exact authority selected in that snapshot.
        (
            _,
            stop_conn,
            stop_runtime,
            stop_project,
            stop_actor,
        ) = new_resolver_case("stop")
        stop_turn, stop_claim = _enqueue_and_claim(
            stop_runtime,
            stop_project,
            stop_actor,
            key="c8-stop",
        )
        stop_attempt = attempt_for(stop_claim)
        stop_runtime.request_stop(
            stop_project,
            stop_turn.turn_id,
            stop_actor,
            idempotency_key="c8-stop-request",
            expected_version=2,
            expected_control_version=1,
        )
        stop_decision = resolve_without_writes(
            stop_runtime,
            stop_conn,
            stop_attempt,
            prepared_result_id=batch_id,
            expected_action="discard",
            expected_authority="stop_requested",
        )
        observed_discards["stop_requested"] = (
            stop_decision.discard_authority
        )

        # An expired persisted claim is still live authority until recovery
        # has durably classified it.
        expired_now = [100]
        (
            _,
            expired_conn,
            expired_runtime,
            expired_project,
            expired_actor,
        ) = new_resolver_case("expired-live", clock=lambda: expired_now[0])
        _, expired_claim = _enqueue_and_claim(
            expired_runtime,
            expired_project,
            expired_actor,
            key="c8-expired-live",
        )
        expired_runtime.mark_turn_started(expired_claim)
        expired_now[0] = expired_claim.lease_expires_at
        resolve_without_writes(
            expired_runtime,
            expired_conn,
            attempt_for(expired_claim),
            prepared_result_id=batch_id,
            expected_action="wait",
        )

        # Public not-started recovery requeues then replaces the expired
        # attempt; the old immutable proof maps to superseded_attempt.
        superseded_now = [100]
        (
            _,
            superseded_conn,
            superseded_runtime,
            superseded_project,
            superseded_actor,
        ) = new_resolver_case(
            "superseded-attempt",
            clock=lambda: superseded_now[0],
        )
        _, old_claim = _enqueue_and_claim(
            superseded_runtime,
            superseded_project,
            superseded_actor,
            key="c8-superseded",
        )
        old_attempt = attempt_for(old_claim)
        superseded_now[0] = old_claim.lease_expires_at

        class NoReadbackForNotStarted:
            def read_turn(self, request):
                raise AssertionError(
                    "not-started recovery must not call readback"
                )

        superseded_runtime.reconcile_inflight_turns(
            NoReadbackForNotStarted(),
            limit=10,
        )
        replacement_claim = superseded_runtime.claim_next_turn(
            superseded_project,
            "replacement-worker",
            lease_seconds=30,
        )
        assert replacement_claim is not None
        superseded_decision = resolve_without_writes(
            superseded_runtime,
            superseded_conn,
            old_attempt,
            prepared_result_id=batch_id,
            expected_action="discard",
            expected_authority="superseded_attempt",
        )
        observed_discards["superseded_attempt"] = (
            superseded_decision.discard_authority
        )

        # Started unknown recovery produces one durable block; that exact
        # snapshot, not an adapter inference, authorizes recovery_blocked.
        blocked_now = [100]
        (
            _,
            blocked_conn,
            blocked_runtime,
            blocked_project,
            blocked_actor,
        ) = new_resolver_case(
            "recovery-blocked",
            clock=lambda: blocked_now[0],
        )
        _, blocked_claim = _enqueue_and_claim(
            blocked_runtime,
            blocked_project,
            blocked_actor,
            key="c8-blocked",
        )
        blocked_runtime.mark_turn_started(blocked_claim)
        blocked_attempt = attempt_for(blocked_claim)
        blocked_now[0] = blocked_claim.lease_expires_at

        class UnknownReadback:
            def read_turn(self, request):
                return module.TurnReadbackResult("unknown")

        blocked_runtime.reconcile_inflight_turns(
            UnknownReadback(),
            limit=10,
        )
        blocked_decision = resolve_without_writes(
            blocked_runtime,
            blocked_conn,
            blocked_attempt,
            prepared_result_id=batch_id,
            expected_action="discard",
            expected_authority="recovery_blocked",
        )
        observed_discards["recovery_blocked"] = (
            blocked_decision.discard_authority
        )

        # This is a mapper-valid legacy/crash shape.  Public cancellation
        # cannot target an already-claimed attempt, so retain its complete
        # audit tuple, remove only the lease, advance version, and append the
        # matching terminal event in one test-owned transaction.
        (
            _,
            cancelled_conn,
            cancelled_runtime,
            cancelled_project,
            cancelled_actor,
        ) = new_resolver_case("cancelled")
        cancelled_turn, cancelled_claim = _enqueue_and_claim(
            cancelled_runtime,
            cancelled_project,
            cancelled_actor,
            key="c8-cancelled-crash",
        )
        cancelled_attempt = attempt_for(cancelled_claim)
        cancelled_conn.execute("BEGIN IMMEDIATE")
        try:
            cancelled_version = cancelled_conn.execute(
                """
                SELECT version
                FROM project_runtime_state
                WHERE project_id = ?
                """,
                (cancelled_project,),
            ).fetchone()[0]
            cancelled_event_sequence = cancelled_conn.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1
                FROM project_events
                WHERE project_id = ?
                """,
                (cancelled_project,),
            ).fetchone()[0]
            assert cancelled_conn.execute(
                """
                UPDATE project_turns
                SET status = 'cancelled', updated_at = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (100, cancelled_project, cancelled_turn.turn_id),
            ).rowcount == 1
            assert cancelled_conn.execute(
                """
                UPDATE project_run_controls
                SET control_state = 'terminal',
                    control_version = control_version + 1,
                    updated_at = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (100, cancelled_project, cancelled_turn.turn_id),
            ).rowcount == 1
            assert cancelled_conn.execute(
                """
                DELETE FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                """,
                (cancelled_project, cancelled_turn.turn_id),
            ).rowcount == 1
            assert cancelled_conn.execute(
                """
                UPDATE project_runtime_state
                SET version = version + 1, updated_at = ?
                WHERE project_id = ? AND version = ?
                """,
                (100, cancelled_project, cancelled_version),
            ).rowcount == 1
            cancelled_conn.execute(
                """
                INSERT INTO project_events (
                    event_id, project_id, sequence, kind, turn_id,
                    payload_json, created_at
                ) VALUES (?, ?, ?, 'turn.cancelled', ?, ?, ?)
                """,
                (
                    "c8-cancelled-crash-event",
                    cancelled_project,
                    cancelled_event_sequence,
                    cancelled_turn.turn_id,
                    json.dumps(
                        {
                            "turn_id": cancelled_turn.turn_id,
                            "version": cancelled_version + 1,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    100,
                ),
            )
            cancelled_conn.commit()
        except BaseException:
            cancelled_conn.rollback()
            raise
        cancelled_decision = resolve_without_writes(
            cancelled_runtime,
            cancelled_conn,
            cancelled_attempt,
            prepared_result_id=batch_id,
            expected_action="discard",
            expected_authority="cancelled",
        )
        observed_discards["cancelled"] = (
            cancelled_decision.discard_authority
        )

        # A lease-less reconciling attempt with no durable block remains
        # indeterminate.  It must wait rather than infer a discard.
        (
            _,
            lease_less_conn,
            lease_less_runtime,
            lease_less_project,
            lease_less_actor,
        ) = new_resolver_case("lease-less-reconciling")
        lease_less_turn, lease_less_claim = _enqueue_and_claim(
            lease_less_runtime,
            lease_less_project,
            lease_less_actor,
            key="c8-lease-less",
        )
        lease_less_runtime.mark_turn_started(lease_less_claim)
        lease_less_conn.execute(
            """
            UPDATE project_turns
            SET status = 'reconciling'
            WHERE turn_id = ?
            """,
            (lease_less_turn.turn_id,),
        )
        lease_less_conn.execute(
            "DELETE FROM project_worker_leases WHERE turn_id = ?",
            (lease_less_turn.turn_id,),
        )
        lease_less_conn.commit()
        resolve_without_writes(
            lease_less_runtime,
            lease_less_conn,
            attempt_for(lease_less_claim),
            prepared_result_id=batch_id,
            expected_action="wait",
        )

        # A batch may carry an earlier observed lease horizon after a
        # heartbeat, but never one beyond the retained control horizon.
        heartbeat_now = [100]
        (
            heartbeat_module,
            heartbeat_conn,
            heartbeat_runtime,
            heartbeat_project,
            heartbeat_actor,
        ) = new_resolver_case(
            "heartbeat-horizon",
            clock=lambda: heartbeat_now[0],
        )
        _, heartbeat_claim = _enqueue_and_claim(
            heartbeat_runtime,
            heartbeat_project,
            heartbeat_actor,
            key="c8-heartbeat",
        )
        assert heartbeat_claim.lease_expires_at == 130
        heartbeat_now[0] = 110
        renewed_claim = heartbeat_runtime.heartbeat_turn(
            heartbeat_claim,
            lease_seconds=50,
        )
        assert renewed_claim.lease_expires_at == 160
        renewed_claim = heartbeat_runtime.mark_turn_started(renewed_claim)
        heartbeat_batch_id = "323e4567-e89b-42d3-a456-426614174000"
        heartbeat_runtime.commit_turn_with_task7_batch(
            renewed_claim,
            heartbeat_module.CanonicalTurnResult(
                "succeeded",
                heartbeat_batch_id,
            ),
            transcript_batch_id=heartbeat_batch_id,
        )
        heartbeat_attempt = attempt_for(heartbeat_claim)
        resolve_without_writes(
            heartbeat_runtime,
            heartbeat_conn,
            heartbeat_attempt,
            prepared_result_id=heartbeat_batch_id,
            expected_action="publish",
        )
        assert heartbeat_runtime.ack_terminal_transcript_applied(
            heartbeat_module.TerminalTranscriptAcknowledgement(
                batch_id=heartbeat_batch_id,
                attempt=heartbeat_attempt,
                status="succeeded",
                result_id=heartbeat_batch_id,
            )
        ) == "acknowledged"
        assert tuple(
            heartbeat_conn.execute(
                """
                SELECT s.transcript_pending_batch_id,
                       s.transcript_dispatch_block_key,
                       t.transcript_applied_batch_id
                FROM project_runtime_state AS s
                JOIN project_turns AS t
                  ON t.project_id = s.project_id
                WHERE s.project_id = ? AND t.turn_id = ?
                """,
                (heartbeat_project, heartbeat_claim.turn_id),
            ).fetchone()
        ) == (None, None, heartbeat_batch_id)

        # Terminal authority rejects both an unexpected surviving lease and
        # any NULL, mistyped, or mismatched retained control audit field.
        (
            audit_module,
            audit_conn,
            audit_runtime,
            audit_project,
            audit_actor,
        ) = new_resolver_case("retained-audit")
        audit_turn, audit_claim = _enqueue_and_claim(
            audit_runtime,
            audit_project,
            audit_actor,
            key="c8-audit",
        )
        lease_columns = tuple(
            row["name"]
            for row in audit_conn.execute(
                "PRAGMA table_info(project_worker_leases)"
            )
        )
        live_lease = tuple(
            audit_conn.execute(
                """
                SELECT * FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                """,
                (audit_project, audit_turn.turn_id),
            ).fetchone()
        )
        audit_claim = audit_runtime.mark_turn_started(audit_claim)
        audit_batch_id = "423e4567-e89b-42d3-a456-426614174000"
        audit_runtime.commit_turn_with_task7_batch(
            audit_claim,
            audit_module.CanonicalTurnResult(
                "succeeded",
                audit_batch_id,
            ),
            transcript_batch_id=audit_batch_id,
        )
        audit_attempt = attempt_for(audit_claim)
        audit_conn.execute(
            f"""
            INSERT INTO project_worker_leases (
                {", ".join(lease_columns)}
            ) VALUES ({", ".join("?" for _ in lease_columns)})
            """,
            live_lease,
        )
        audit_conn.commit()
        before_unexpected_resolver_lease = _claim_snapshot(
            audit_conn,
            audit_project,
            audit_turn.turn_id,
        )
        with pytest.raises(audit_module.ProjectRuntimeError) as resolver_lease:
            audit_runtime.resolve_prepared_terminal(
                audit_attempt,
                prepared_result_id=audit_batch_id,
                status="succeeded",
            )
        assert (
            resolver_lease.value.code
            is audit_module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
        )
        assert _claim_snapshot(
            audit_conn,
            audit_project,
            audit_turn.turn_id,
        ) == before_unexpected_resolver_lease
        audit_conn.execute(
            "DELETE FROM project_worker_leases WHERE project_id = ?",
            (audit_project,),
        )
        audit_conn.commit()

        audit_fields = (
            "attempt_id",
            "claim_worker_id",
            "claim_lease_expires_at",
            "claim_canonical_session_id",
        )
        retained_audit = tuple(
            audit_conn.execute(
                f"""
                SELECT {", ".join(audit_fields)}
                FROM project_run_controls
                WHERE turn_id = ?
                """,
                (audit_turn.turn_id,),
            ).fetchone()
        )
        audit_mutations = (
            ("attempt_id", None),
            ("attempt_id", "mismatched-attempt"),
            ("claim_worker_id", None),
            ("claim_worker_id", "mismatched-worker"),
            ("claim_lease_expires_at", None),
            ("claim_lease_expires_at", "not-an-integer"),
            (
                "claim_lease_expires_at",
                1 << 62,
            ),
            ("claim_canonical_session_id", None),
            (
                "claim_canonical_session_id",
                "mismatched-session",
            ),
        )
        for audit_column, audit_value in audit_mutations:
            audit_conn.execute(
                f"""
                UPDATE project_run_controls
                SET {audit_column} = ?
                WHERE turn_id = ?
                """,
                (audit_value, audit_turn.turn_id),
            )
            audit_conn.commit()
            before_audit_conflict = _claim_snapshot(
                audit_conn,
                audit_project,
                audit_turn.turn_id,
            )
            with pytest.raises(
                audit_module.ProjectRuntimeError
            ) as audit_conflict:
                audit_runtime.resolve_prepared_terminal(
                    audit_attempt,
                    prepared_result_id=audit_batch_id,
                    status="succeeded",
                )
            assert (
                audit_conflict.value.code
                is audit_module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert _claim_snapshot(
                audit_conn,
                audit_project,
                audit_turn.turn_id,
            ) == before_audit_conflict
            audit_conn.execute(
                f"""
                UPDATE project_run_controls
                SET {", ".join(f"{field} = ?" for field in audit_fields)}
                WHERE turn_id = ?
                """,
                (*retained_audit, audit_turn.turn_id),
            )
            audit_conn.commit()

        # Mutate the gate from a second connection after SQLite has returned
        # the first concrete authority row.  This seam is below the runtime
        # mapper: it requires one coherent read transaction without prescribing
        # private helper calls or query decomposition.
        race_path = tmp_path / "c8-resolver-one-snapshot.db"
        (
            race_module,
            race_conn,
            race_runtime,
            race_project,
            race_actor,
        ) = _make_runtime(race_path)
        extra_connections.append(race_conn)
        race_turn, race_claim = _enqueue_and_claim(
            race_runtime,
            race_project,
            race_actor,
            key="c8-race",
        )
        race_claim = race_runtime.mark_turn_started(race_claim)
        race_batch_id = "623e4567-e89b-42d3-a456-426614174000"
        race_runtime.commit_turn_with_task7_batch(
            race_claim,
            race_module.CanonicalTurnResult(
                "succeeded",
                race_batch_id,
            ),
            transcript_batch_id=race_batch_id,
        )
        race_conn.close()

        class AuthoritySnapshotRaceCursor(sqlite3.Cursor):
            _statement = ""

            def execute(self, statement, parameters=()):
                self._statement = statement
                self.connection.before_statement(statement)
                return super().execute(statement, parameters)

            def fetchone(self):
                row = super().fetchone()
                self.connection.after_authority_row(
                    self._statement,
                    row is not None,
                )
                return row

            def fetchall(self):
                rows = super().fetchall()
                self.connection.after_authority_row(
                    self._statement,
                    bool(rows),
                )
                return rows

            def fetchmany(self, size=None):
                rows = (
                    super().fetchmany()
                    if size is None
                    else super().fetchmany(size)
                )
                self.connection.after_authority_row(
                    self._statement,
                    bool(rows),
                )
                return rows

            def __next__(self):
                row = super().__next__()
                self.connection.after_authority_row(
                    self._statement,
                    True,
                )
                return row

        class AuthoritySnapshotRaceConnection(sqlite3.Connection):
            race_callback = None
            race_fired = False
            owned_read_begins = 0
            authority_read_statements = ()
            authority_tables = (
                "PROJECT_TURNS",
                "PROJECT_RUN_CONTROLS",
                "PROJECT_RUNTIME_STATE",
                "PROJECT_WORKER_LEASES",
            )

            def cursor(self, factory=None):
                return super().cursor(
                    factory or AuthoritySnapshotRaceCursor
                )

            def execute(self, statement, parameters=()):
                return self.cursor().execute(statement, parameters)

            def is_authority_read(self, statement):
                normalized = " ".join(statement.upper().split())
                return (
                    normalized.startswith(("SELECT", "WITH"))
                    and any(
                        table in normalized
                        for table in self.authority_tables
                    )
                )

            def before_statement(self, statement):
                normalized = " ".join(statement.upper().split())
                if normalized.startswith("BEGIN"):
                    self.owned_read_begins += 1
                if self.is_authority_read(statement):
                    assert self.owned_read_begins == 1
                    assert self.in_transaction is True
                    self.authority_read_statements = (
                        *self.authority_read_statements,
                        normalized,
                    )

            def after_authority_row(self, statement, has_row):
                if (
                    has_row
                    and not self.race_fired
                    and self.is_authority_read(statement)
                ):
                    self.race_fired = True
                    assert self.in_transaction is True
                    assert self.race_callback is not None
                    self.race_callback()

        race_snapshot_conn = sqlite3.connect(
            str(race_path),
            factory=AuthoritySnapshotRaceConnection,
        )
        race_snapshot_conn.row_factory = sqlite3.Row
        race_snapshot_conn.execute("PRAGMA foreign_keys=ON")
        extra_connections.append(race_snapshot_conn)
        race_runtime = race_module.ProjectRuntime(
            race_snapshot_conn,
            clock=lambda: 100,
        )
        race_writer = projects_db.connect(race_path)
        extra_connections.append(race_writer)

        def mutate_gate_after_snapshot():
            race_writer.execute(
                """
                UPDATE project_runtime_state
                SET transcript_pending_batch_id = NULL,
                    transcript_dispatch_block_key = ?
                WHERE project_id = ?
                """,
                ("c8-race-after-snapshot", race_project),
            )
            race_writer.commit()

        race_snapshot_conn.race_callback = mutate_gate_after_snapshot
        race_changes = race_snapshot_conn.total_changes
        race_decision = race_runtime.resolve_prepared_terminal(
            attempt_for(race_claim),
            prepared_result_id=race_batch_id,
            status="succeeded",
        )
        assert race_decision.action == "publish"
        assert race_decision.discard_authority is None
        assert race_snapshot_conn.race_fired is True
        assert race_snapshot_conn.owned_read_begins == 1
        assert race_snapshot_conn.authority_read_statements
        assert race_snapshot_conn.in_transaction is False
        assert race_snapshot_conn.total_changes == race_changes
        assert tuple(
            race_writer.execute(
                """
                SELECT transcript_pending_batch_id,
                       transcript_dispatch_block_key
                FROM project_runtime_state
                WHERE project_id = ?
                """,
                (race_project,),
            ).fetchone()
        ) == (None, "c8-race-after-snapshot")

        assert observed_discards == {
            "stop_requested": "stop_requested",
            "cancelled": "cancelled",
            "superseded_attempt": "superseded_attempt",
            "superseded_terminal": "superseded_terminal",
            "recovery_blocked": "recovery_blocked",
        }
    finally:
        for extra_connection in extra_connections:
            extra_connection.close()
        conn.close()


def test_task7_c7_terminal_gate_live_commit_is_atomic_and_replayable(
    tmp_path,
    monkeypatch,
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "c7-live-commit.db"
    )
    try:
        def authority_snapshot():
            return (
                tuple(
                    conn.execute(
                        """
                        SELECT * FROM project_runtime_state
                        WHERE project_id = ?
                        """,
                        (project_id,),
                    ).fetchone()
                ),
                tuple(
                    conn.execute(
                        "SELECT * FROM project_turns WHERE turn_id = ?",
                        (turn.turn_id,),
                    ).fetchone()
                ),
                tuple(
                    conn.execute(
                        "SELECT * FROM project_run_controls WHERE turn_id = ?",
                        (turn.turn_id,),
                    ).fetchone()
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_worker_leases
                        WHERE project_id = ? AND turn_id = ?
                        ORDER BY lease_id
                        """,
                        (project_id, turn.turn_id),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_events
                        WHERE project_id = ? ORDER BY sequence
                        """,
                        (project_id,),
                    )
                ),
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM project_runtime_membership_counters
                        ORDER BY lane
                        """
                    )
                ),
            )

        def mutations(statements):
            return [
                statement
                for statement in statements
                if statement.lstrip().upper().startswith(
                    ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
            ]

        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        claim = runtime.mark_turn_started(claim)
        batch_id = "123e4567-e89b-42d3-a456-426614174000"
        other_batch = "223e4567-e89b-42d3-a456-426614174000"
        result = module.CanonicalTurnResult("succeeded", batch_id)
        before = authority_snapshot()

        for invalid_batch in ("not-a-canonical-uuid", other_batch):
            validation_trace = []
            conn.set_trace_callback(validation_trace.append)
            try:
                with pytest.raises(module.ProjectRuntimeError) as invalid:
                    runtime.commit_turn_with_task7_batch(
                        claim,
                        result,
                        transcript_batch_id=invalid_batch,
                    )
            finally:
                conn.set_trace_callback(None)
            assert (
                invalid.value.code
                is module.RuntimeErrorCode.INVALID_ARGUMENT
            )
            assert not any(
                statement.lstrip().upper().startswith("BEGIN")
                for statement in validation_trace
            )
            assert mutations(validation_trace) == []
            assert authority_snapshot() == before

        outer_before = authority_snapshot()
        conn.execute("BEGIN IMMEDIATE")
        assert conn.in_transaction is True
        outer_changes = conn.total_changes
        outer_trace = []
        conn.set_trace_callback(outer_trace.append)
        try:
            with pytest.raises(module.ProjectRuntimeError) as nested:
                runtime.commit_turn_with_task7_batch(
                    claim,
                    result,
                    transcript_batch_id=batch_id,
                )
        finally:
            conn.set_trace_callback(None)
        assert nested.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert conn.in_transaction is True
        assert not [
            statement
            for statement in outer_trace
            if statement.lstrip().upper().startswith(
                (
                    "BEGIN",
                    "COMMIT",
                    "ROLLBACK",
                    "SAVEPOINT",
                    "RELEASE",
                    "INSERT",
                    "UPDATE",
                    "DELETE",
                    "REPLACE",
                )
            )
        ]
        assert conn.total_changes == outer_changes
        assert authority_snapshot() == outer_before
        conn.rollback()
        assert conn.in_transaction is False
        assert authority_snapshot() == outer_before

        original_event = runtime._event

        def fail_after_terminal_event(*args, **kwargs):
            original_event(*args, **kwargs)
            raise RuntimeError("inject terminal event fault")

        monkeypatch.setattr(runtime, "_event", fail_after_terminal_event)
        with pytest.raises(RuntimeError, match="terminal event fault"):
            runtime.commit_turn_with_task7_batch(
                claim, result, transcript_batch_id=batch_id
            )
        assert authority_snapshot() == before

        monkeypatch.setattr(runtime, "_event", original_event)
        for gate_field, gate_value in (
            ("transcript_pending_batch_id", other_batch),
            (
                "transcript_dispatch_block_key",
                "test-owned late live block",
            ),
        ):
            late_gate_hits = []
            conn.create_function(
                "c7_note_late_live_gate",
                0,
                lambda field=gate_field: late_gate_hits.append(field),
            )
            conn.executescript(
                f"""
                CREATE TEMP TRIGGER c7_late_live_gate
                BEFORE UPDATE OF status ON project_turns
                WHEN OLD.turn_id = '{turn.turn_id}'
                  AND OLD.status = 'claimed'
                  AND NEW.status IN ('succeeded', 'failed')
                BEGIN
                    SELECT c7_note_late_live_gate();
                    UPDATE project_runtime_state
                    SET {gate_field} = '{gate_value}'
                    WHERE project_id = '{project_id}';
                END;
                """
            )
            late_before = authority_snapshot()
            try:
                with pytest.raises(
                    module.ProjectRuntimeError
                ) as late_gate:
                    runtime.commit_turn_with_task7_batch(
                        claim, result, transcript_batch_id=batch_id
                    )
            finally:
                conn.execute("DROP TRIGGER c7_late_live_gate")
            assert late_gate_hits == [gate_field]
            assert (
                late_gate.value.code
                is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert authority_snapshot() == late_before

        before_state = prdb.runtime_state_for_project(conn, project_id)
        assert before_state is not None
        before_events = conn.execute(
            "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
            (project_id,),
        ).fetchone()[0]
        success_trace = []
        conn.set_trace_callback(success_trace.append)
        try:
            committed = runtime.commit_turn_with_task7_batch(
                claim, result, transcript_batch_id=batch_id
            )
        finally:
            conn.set_trace_callback(None)
        assert committed.status == "succeeded"
        normalized_success = [
            " ".join(statement.upper().split())
            for statement in success_trace
        ]
        assert normalized_success.count("BEGIN IMMEDIATE") == 1
        assert normalized_success.count("COMMIT") == 1
        assert "ROLLBACK" not in normalized_success
        success_mutations = [
            " ".join(statement.lower().split())
            for statement in success_trace
            if statement.lstrip().upper().startswith(
                ("INSERT", "UPDATE", "DELETE", "REPLACE")
            )
        ]
        terminal_turn_cas = []
        pending_state_cas = []
        for statement in success_mutations:
            set_clause, separator, where_clause = statement.partition(
                " where "
            )
            if not separator:
                continue
            if (
                statement.startswith("update project_turns")
                and "status = 'succeeded'" in set_clause
                and "terminal_result_id" in set_clause
            ):
                terminal_turn_cas.append((set_clause, where_clause))
            if (
                statement.startswith("update project_runtime_state")
                and "transcript_pending_batch_id" in set_clause
            ):
                pending_state_cas.append((set_clause, where_clause))
        assert terminal_turn_cas
        assert all(
            "transcript_pending_batch_id" in where_clause
            and "transcript_dispatch_block_key" in where_clause
            and "transcript_applied_batch_id" in where_clause
            for _, where_clause in terminal_turn_cas
        )
        assert pending_state_cas
        assert all(
            "transcript_pending_batch_id" in where_clause
            and "transcript_dispatch_block_key" in where_clause
            for _, where_clause in pending_state_cas
        )
        assert tuple(conn.execute(
            """
            SELECT version, transcript_pending_batch_id,
                   transcript_dispatch_block_key
            FROM project_runtime_state WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()) == (before_state.version + 1, batch_id, None)
        assert tuple(conn.execute(
            """
            SELECT status, attempt_id, lease_generation, fencing_token,
                   execution_state, terminal_result_id,
                   transcript_applied_batch_id
            FROM project_turns WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()) == (
            "succeeded",
            claim.attempt_id,
            claim.lease_generation,
            claim.fencing_token,
            "started",
            batch_id,
            None,
        )
        assert tuple(conn.execute(
            """
            SELECT control_state, attempt_id, claim_worker_id,
                   claim_lease_expires_at, claim_canonical_session_id
            FROM project_run_controls WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()) == (
            "terminal",
            claim.attempt_id,
            claim.worker_id,
            claim.lease_expires_at,
            claim.canonical_session_id,
        )
        assert conn.execute(
            """
            SELECT COUNT(*) FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM project_events WHERE project_id = ?",
            (project_id,),
        ).fetchone()[0] == before_events + 1
        terminal_event = conn.execute(
            """
            SELECT kind, payload_json FROM project_events
            WHERE project_id = ? ORDER BY sequence DESC LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        assert terminal_event["kind"] == "turn.succeeded"
        assert json.loads(terminal_event["payload_json"]) == {
            "attempt_id": claim.attempt_id,
            "fencing_token": claim.fencing_token,
            "lease_generation": claim.lease_generation,
            "turn_id": claim.turn_id,
            "version": before_state.version + 1,
        }

        committed_snapshot = authority_snapshot()
        replay_trace = []
        before_replay_changes = conn.total_changes
        conn.set_trace_callback(replay_trace.append)
        try:
            replay = runtime.commit_turn_with_task7_batch(
                claim, result, transcript_batch_id=batch_id
            )
        finally:
            conn.set_trace_callback(None)
        assert replay == committed
        assert conn.total_changes == before_replay_changes
        assert mutations(replay_trace) == []
        assert authority_snapshot() == committed_snapshot

        with pytest.raises(module.ProjectRuntimeError) as changed:
            runtime.commit_turn_with_task7_batch(
                claim,
                module.CanonicalTurnResult("succeeded", other_batch),
                transcript_batch_id=other_batch,
            )
        assert changed.value.code in {
            module.RuntimeErrorCode.TERMINAL_RESULT_CONFLICT,
            module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
        }
        assert authority_snapshot() == committed_snapshot
        with pytest.raises(module.ProjectRuntimeError) as changed_status:
            runtime.commit_turn_with_task7_batch(
                claim,
                module.CanonicalTurnResult("failed", batch_id),
                transcript_batch_id=batch_id,
            )
        assert changed_status.value.code in {
            module.RuntimeErrorCode.TERMINAL_RESULT_CONFLICT,
            module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT,
        }
        assert authority_snapshot() == committed_snapshot

        conn.execute(
            """
            UPDATE project_runtime_state
            SET transcript_pending_batch_id = ?
            WHERE project_id = ?
            """,
            (other_batch, project_id),
        )
        conn.commit()
        changed_gate_snapshot = authority_snapshot()
        with pytest.raises(module.ProjectRuntimeError) as changed_gate:
            runtime.commit_turn_with_task7_batch(
                claim, result, transcript_batch_id=batch_id
            )
        assert (
            changed_gate.value.code
            is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
        )
        assert authority_snapshot() == changed_gate_snapshot
        conn.execute(
            """
            UPDATE project_runtime_state
            SET transcript_pending_batch_id = ?
            WHERE project_id = ?
            """,
            (batch_id, project_id),
        )
        conn.commit()

        def assert_write_free_replay_authority_conflict():
            drift_snapshot = authority_snapshot()
            drift_changes = conn.total_changes
            drift_trace = []
            conn.set_trace_callback(drift_trace.append)
            try:
                with pytest.raises(
                    module.ProjectRuntimeError
                ) as drift_conflict:
                    runtime.commit_turn_with_task7_batch(
                        claim, result, transcript_batch_id=batch_id
                    )
            finally:
                conn.set_trace_callback(None)
            assert (
                drift_conflict.value.code
                is module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert mutations(drift_trace) == []
            assert conn.total_changes == drift_changes
            assert authority_snapshot() == drift_snapshot

        control_audit = dict(conn.execute(
            """
            SELECT control_state, control_version, attempt_id,
                   claim_worker_id, claim_lease_expires_at,
                   claim_canonical_session_id
            FROM project_run_controls WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone())
        for audit_column, drift_value in (
            ("control_state", "running"),
            ("attempt_id", "c7-drifted-attempt"),
            (
                "control_version",
                control_audit["control_version"] + 1,
            ),
            ("claim_worker_id", "c7-drifted-worker"),
            (
                "claim_lease_expires_at",
                control_audit["claim_lease_expires_at"] + 1,
            ),
            (
                "claim_canonical_session_id",
                "c7-drifted-session",
            ),
        ):
            conn.execute(
                f"""
                UPDATE project_run_controls
                SET {audit_column} = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (drift_value, project_id, turn.turn_id),
            )
            conn.commit()
            assert_write_free_replay_authority_conflict()
            conn.execute(
                f"""
                UPDATE project_run_controls
                SET {audit_column} = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                (
                    control_audit[audit_column],
                    project_id,
                    turn.turn_id,
                ),
            )
            conn.commit()

        conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id,
                lease_generation, fencing_token, expires_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim.attempt_id,
                project_id,
                turn.turn_id,
                claim.worker_id,
                claim.lease_generation,
                claim.fencing_token,
                claim.lease_expires_at,
                100,
            ),
        )
        conn.commit()
        assert_write_free_replay_authority_conflict()
        conn.execute(
            """
            DELETE FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ? AND lease_id = ?
            """,
            (project_id, turn.turn_id, claim.attempt_id),
        )
        conn.commit()

        conn.execute(
            """
            UPDATE project_turns
            SET transcript_applied_batch_id = ?
            WHERE project_id = ? AND turn_id = ?
            """,
            (batch_id, project_id, turn.turn_id),
        )
        conn.commit()
        assert_write_free_replay_authority_conflict()

        for ordinal, authority_variant in enumerate(
            (
                "inactive",
                "not_started",
                "control_not_running",
                "lease_expired",
            ),
            1,
        ):
            variant_now = [100]
            (
                variant_module,
                variant_conn,
                variant_runtime,
                variant_project,
                variant_actor,
            ) = _make_runtime(
                tmp_path
                / f"c7-live-negative-{authority_variant}.db",
                clock=lambda: variant_now[0],
            )
            try:
                variant_turn, variant_claim = _enqueue_and_claim(
                    variant_runtime, variant_project, variant_actor
                )
                if authority_variant != "not_started":
                    variant_claim = variant_runtime.mark_turn_started(
                        variant_claim
                    )
                if authority_variant == "inactive":
                    variant_conn.execute(
                        """
                        UPDATE project_runtime_state
                        SET lifecycle = 'awaiting_acceptance'
                        WHERE project_id = ?
                        """,
                        (variant_project,),
                    )
                    variant_conn.commit()
                elif authority_variant == "control_not_running":
                    state = prdb.runtime_state_for_project(
                        variant_conn, variant_project
                    )
                    control = prdb._runtime_control_for_turn(
                        variant_conn,
                        project_id=variant_project,
                        turn_id=variant_turn.turn_id,
                    )
                    assert state is not None and control is not None
                    variant_runtime.request_stop(
                        variant_project,
                        variant_turn.turn_id,
                        variant_actor,
                        idempotency_key="c7-negative-stop",
                        expected_version=state.version,
                        expected_control_version=(
                            control.control_version
                        ),
                    )
                elif authority_variant == "lease_expired":
                    variant_now[0] = (
                        variant_claim.lease_expires_at + 1
                    )
                variant_before = _claim_snapshot(
                    variant_conn,
                    variant_project,
                    variant_turn.turn_id,
                )
                variant_batch = (
                    "323e4567-e89b-42d3-a456-42661417400"
                    f"{ordinal}"
                )
                with pytest.raises(
                    variant_module.ProjectRuntimeError
                ):
                    variant_runtime.commit_turn_with_task7_batch(
                        variant_claim,
                        variant_module.CanonicalTurnResult(
                            "succeeded", variant_batch
                        ),
                        transcript_batch_id=variant_batch,
                    )
                assert _claim_snapshot(
                    variant_conn,
                    variant_project,
                    variant_turn.turn_id,
                ) == variant_before
            finally:
                variant_conn.close()
    finally:
        conn.close()


def test_task7_c7_terminal_gate_readback_is_explicit_and_malformed_evidence_visibly_blocks(
    tmp_path,
    monkeypatch,
):
    module = importlib.import_module("hermes_cli.project_runtime")
    batch_id = "123e4567-e89b-42d3-a456-426614174000"
    other_batch = "223e4567-e89b-42d3-a456-426614174000"

    assert tuple(
        field.name for field in fields(module.Task7TerminalReadbackEvidence)
    ) == ("result", "transcript_batch_id")
    assert get_type_hints(module.Task7TerminalReadbackEvidence) == {
        "result": module.TurnReadbackResult,
        "transcript_batch_id": str | None,
    }
    evidence = module.Task7TerminalReadbackEvidence(
        module.TurnReadbackResult("succeeded", batch_id),
        batch_id,
    )
    with pytest.raises(FrozenInstanceError):
        evidence.transcript_batch_id = other_batch
    assert issubclass(module.Task7TerminalReadbackPort, Protocol)
    assert {
        name
        for name in module.Task7TerminalReadbackPort.__dict__
        if not name.startswith("_")
    } == {"read_turn_with_evidence"}
    port_method = (
        module.Task7TerminalReadbackPort.read_turn_with_evidence
    )
    assert tuple(inspect.signature(port_method).parameters) == (
        "self",
        "request",
    )
    assert get_type_hints(port_method) == {
        "request": module.TurnReadbackRequest,
        "return": module.Task7TerminalReadbackEvidence,
    }
    commit_method = module.ProjectRuntime.commit_turn_with_task7_batch
    commit_signature = inspect.signature(commit_method)
    assert tuple(commit_signature.parameters) == (
        "self",
        "claim",
        "result",
        "transcript_batch_id",
    )
    assert get_type_hints(commit_method) == {
        "claim": module.TurnClaim,
        "result": module.CanonicalTurnResult,
        "transcript_batch_id": str,
        "return": module.ProjectTurn,
    }
    assert (
        commit_signature.parameters["transcript_batch_id"].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    reconcile_signature = inspect.signature(
        module.ProjectRuntime.reconcile_inflight_turns_with_task7_evidence
    )
    assert tuple(reconcile_signature.parameters) == (
        "self",
        "readback",
        "limit",
    )
    assert reconcile_signature.parameters["limit"].default == 100
    assert (
        reconcile_signature.parameters["limit"].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    assert get_type_hints(
        module.ProjectRuntime
        .reconcile_inflight_turns_with_task7_evidence
    ) == {
        "readback": module.Task7TerminalReadbackPort,
        "limit": int,
        "return": tuple[module.ProjectTurn, ...],
    }

    def durable_snapshot(conn, project_id, turn_id):
        return (
            tuple(
                conn.execute(
                    """
                    SELECT * FROM project_runtime_state
                    WHERE project_id = ?
                    """,
                    (project_id,),
                ).fetchone()
            ),
            tuple(
                conn.execute(
                    "SELECT * FROM project_turns WHERE turn_id = ?",
                    (turn_id,),
                ).fetchone()
            ),
            tuple(
                conn.execute(
                    "SELECT * FROM project_run_controls WHERE turn_id = ?",
                    (turn_id,),
                ).fetchone()
            ),
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
            tuple(
                tuple(row)
                for row in conn.execute(
                    """
                    SELECT * FROM project_events
                    WHERE project_id = ? ORDER BY sequence
                    """,
                    (project_id,),
                )
            ),
            tuple(
                tuple(row)
                for row in conn.execute(
                    """
                    SELECT * FROM project_runtime_membership_counters
                    ORDER BY lane
                    """
                )
            ),
        )

    for ordinal, terminal_status in enumerate(("succeeded", "failed"), 1):
        now = [100]
        (
            positive_module,
            positive_conn,
            positive_runtime,
            positive_project,
            positive_actor,
        ) = _make_runtime(
            tmp_path / f"c7-positive-{terminal_status}.db",
            clock=lambda: now[0],
        )
        try:
            positive_turn, positive_claim = _enqueue_and_claim(
                positive_runtime, positive_project, positive_actor
            )
            positive_runtime.mark_turn_started(positive_claim)
            now[0] = positive_claim.lease_expires_at + 1
            selected_batch = (
                batch_id if ordinal == 1 else other_batch
            )
            before_state = prdb.runtime_state_for_project(
                positive_conn, positive_project
            )
            assert before_state is not None
            before_events = positive_conn.execute(
                """
                SELECT COUNT(*) FROM project_events WHERE project_id = ?
                """,
                (positive_project,),
            ).fetchone()[0]

            class PositiveEvidence:
                calls = 0

                def read_turn_with_evidence(self, request):
                    assert positive_conn.in_transaction is False
                    assert request.project_id == positive_project
                    assert request.turn_id == positive_turn.turn_id
                    self.calls += 1
                    return positive_module.Task7TerminalReadbackEvidence(
                        positive_module.TurnReadbackResult(
                            terminal_status, selected_batch
                        ),
                        selected_batch,
                    )

            positive = PositiveEvidence()
            positive_trace = []
            positive_conn.set_trace_callback(positive_trace.append)
            try:
                recovered = (
                    positive_runtime
                    .reconcile_inflight_turns_with_task7_evidence(
                        positive, limit=100
                    )
                )
            finally:
                positive_conn.set_trace_callback(None)
            assert [item.status for item in recovered] == [
                terminal_status
            ]
            assert positive.calls == 1
            positive_mutations = [
                " ".join(statement.lower().split())
                for statement in positive_trace
                if statement.lstrip().upper().startswith(
                    ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
            ]
            terminal_turn_cas = []
            pending_state_cas = []
            for statement in positive_mutations:
                set_clause, separator, where_clause = (
                    statement.partition(" where ")
                )
                if not separator:
                    continue
                if (
                    statement.startswith("update project_turns")
                    and f"status = '{terminal_status}'" in set_clause
                    and "terminal_result_id" in set_clause
                ):
                    terminal_turn_cas.append(
                        (set_clause, where_clause)
                    )
                if (
                    statement.startswith(
                        "update project_runtime_state"
                    )
                    and "transcript_pending_batch_id" in set_clause
                ):
                    pending_state_cas.append(
                        (set_clause, where_clause)
                    )
            assert terminal_turn_cas
            assert all(
                "transcript_pending_batch_id" in where_clause
                and "transcript_dispatch_block_key" in where_clause
                and "transcript_applied_batch_id" in where_clause
                for _, where_clause in terminal_turn_cas
            )
            assert pending_state_cas
            assert all(
                "transcript_pending_batch_id" in where_clause
                and "transcript_dispatch_block_key" in where_clause
                for _, where_clause in pending_state_cas
            )
            assert tuple(positive_conn.execute(
                """
                SELECT version, transcript_pending_batch_id,
                       transcript_dispatch_block_key
                FROM project_runtime_state WHERE project_id = ?
                """,
                (positive_project,),
            ).fetchone()) == (
                before_state.version + 2,
                selected_batch,
                None,
            )
            assert tuple(positive_conn.execute(
                """
                SELECT status, terminal_result_id,
                       transcript_applied_batch_id, recovery_block_key
                FROM project_turns WHERE turn_id = ?
                """,
                (positive_turn.turn_id,),
            ).fetchone()) == (
                terminal_status,
                selected_batch,
                None,
                None,
            )
            assert tuple(positive_conn.execute(
                """
                SELECT control_state, attempt_id, claim_worker_id,
                       claim_lease_expires_at,
                       claim_canonical_session_id
                FROM project_run_controls WHERE turn_id = ?
                """,
                (positive_turn.turn_id,),
            ).fetchone()) == (
                "terminal",
                positive_claim.attempt_id,
                positive_claim.worker_id,
                positive_claim.lease_expires_at,
                positive_claim.canonical_session_id,
            )
            assert positive_conn.execute(
                """
                SELECT COUNT(*) FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                """,
                (positive_project, positive_turn.turn_id),
            ).fetchone()[0] == 0
            assert positive_conn.execute(
                """
                SELECT COUNT(*) FROM project_events WHERE project_id = ?
                """,
                (positive_project,),
            ).fetchone()[0] == before_events + 2
            positive_snapshot = durable_snapshot(
                positive_conn,
                positive_project,
                positive_turn.turn_id,
            )
            positive_replay = (
                positive_runtime
                .reconcile_inflight_turns_with_task7_evidence(
                    positive, limit=100
                )
            )
            assert tuple(item.turn_id for item in positive_replay) in (
                (),
                (positive_turn.turn_id,),
            )
            assert positive.calls == 1
            assert durable_snapshot(
                positive_conn,
                positive_project,
                positive_turn.turn_id,
            ) == positive_snapshot
        finally:
            positive_conn.close()

    nonterminal_now = [100]
    (
        nonterminal_module,
        nonterminal_conn,
        nonterminal_runtime,
        nonterminal_project,
        nonterminal_actor,
    ) = _make_runtime(
        tmp_path / "c7-valid-nonterminal.db",
        clock=lambda: nonterminal_now[0],
    )
    try:
        nonterminal_turn, nonterminal_claim = _enqueue_and_claim(
            nonterminal_runtime,
            nonterminal_project,
            nonterminal_actor,
        )
        nonterminal_runtime.mark_turn_started(nonterminal_claim)
        nonterminal_now[0] = (
            nonterminal_claim.lease_expires_at + 1
        )

        class ValidNonterminalEvidence:
            calls = 0

            def read_turn_with_evidence(self, request):
                assert nonterminal_conn.in_transaction is False
                self.calls += 1
                return (
                    nonterminal_module.Task7TerminalReadbackEvidence(
                        nonterminal_module.TurnReadbackResult(
                            "unknown", None
                        ),
                        None,
                    )
                )

        nonterminal_port = ValidNonterminalEvidence()
        nonterminal_result = (
            nonterminal_runtime
            .reconcile_inflight_turns_with_task7_evidence(
                nonterminal_port, limit=100
            )
        )
        assert [item.status for item in nonterminal_result] == [
            "reconciling"
        ]
        assert nonterminal_port.calls == 1
        assert tuple(nonterminal_conn.execute(
            """
            SELECT transcript_pending_batch_id,
                   transcript_dispatch_block_key
            FROM project_runtime_state WHERE project_id = ?
            """,
            (nonterminal_project,),
        ).fetchone()) == (None, None)
        nonterminal_turn_storage = tuple(nonterminal_conn.execute(
            """
            SELECT transcript_applied_batch_id, recovery_block_key
            FROM project_turns WHERE turn_id = ?
            """,
            (nonterminal_turn.turn_id,),
        ).fetchone())
        assert nonterminal_turn_storage[0] is None
        assert (
            type(nonterminal_turn_storage[1]) is str
            and nonterminal_turn_storage[1]
        )
        assert nonterminal_conn.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (nonterminal_project, nonterminal_turn.turn_id),
        ).fetchone()[0] == 1
    finally:
        nonterminal_conn.close()

    fault_now = [100]
    (
        fault_module,
        fault_conn,
        fault_runtime,
        fault_project,
        fault_actor,
    ) = _make_runtime(
        tmp_path / "c7-readback-terminal-fault.db",
        clock=lambda: fault_now[0],
    )
    try:
        fault_turn, fault_claim = _enqueue_and_claim(
            fault_runtime, fault_project, fault_actor
        )
        fault_runtime.mark_turn_started(fault_claim)
        fault_now[0] = fault_claim.lease_expires_at + 1
        parked_snapshot = []

        class FaultEvidence:
            def read_turn_with_evidence(self, request):
                assert fault_conn.in_transaction is False
                parked_snapshot.append(
                    durable_snapshot(
                        fault_conn, fault_project, fault_turn.turn_id
                    )
                )
                return fault_module.Task7TerminalReadbackEvidence(
                    fault_module.TurnReadbackResult(
                        "succeeded", batch_id
                    ),
                    batch_id,
                )

        original_fault_event = fault_runtime._event

        def fail_after_readback_terminal_event(
            event_project_id,
            kind,
            event_turn_id,
            payload,
            now,
        ):
            original_fault_event(
                event_project_id,
                kind,
                event_turn_id,
                payload,
                now,
            )
            if kind == "turn.succeeded":
                raise RuntimeError("inject readback terminal event fault")

        monkeypatch.setattr(
            fault_runtime,
            "_event",
            fail_after_readback_terminal_event,
        )
        with pytest.raises(
            RuntimeError, match="readback terminal event fault"
        ):
            fault_runtime.reconcile_inflight_turns_with_task7_evidence(
                FaultEvidence(), limit=100
            )
        assert durable_snapshot(
            fault_conn, fault_project, fault_turn.turn_id
        ) == parked_snapshot[0]
        assert tuple(fault_conn.execute(
            """
            SELECT status, terminal_result_id,
                   transcript_applied_batch_id
            FROM project_turns WHERE turn_id = ?
            """,
            (fault_turn.turn_id,),
        ).fetchone()) == ("reconciling", None, None)
        assert tuple(fault_conn.execute(
            """
            SELECT transcript_pending_batch_id,
                   transcript_dispatch_block_key
            FROM project_runtime_state WHERE project_id = ?
            """,
            (fault_project,),
        ).fetchone()) == (None, None)
    finally:
        fault_conn.close()

    malformed_cases = (
        (
            module.TurnReadbackResult(
                "succeeded", "not-a-canonical-uuid"
            ),
            "not-a-canonical-uuid",
        ),
        (
            module.TurnReadbackResult("succeeded", batch_id),
            other_batch,
        ),
        (
            module.TurnReadbackResult("unknown", None),
            batch_id,
        ),
    )
    for ordinal, (readback_result, transcript_batch_id) in enumerate(
        malformed_cases, 1
    ):
        now = [100]
        (
            malformed_module,
            malformed_conn,
            malformed_runtime,
            malformed_project,
            malformed_actor,
        ) = _make_runtime(
            tmp_path / f"c7-malformed-{ordinal}.db",
            clock=lambda: now[0],
        )
        try:
            malformed_turn, malformed_claim = _enqueue_and_claim(
                malformed_runtime, malformed_project, malformed_actor
            )
            malformed_runtime.mark_turn_started(malformed_claim)
            now[0] = malformed_claim.lease_expires_at + 1

            class MalformedEvidence:
                calls = 0

                def read_turn_with_evidence(self, request):
                    assert malformed_conn.in_transaction is False
                    self.calls += 1
                    return (
                        malformed_module.Task7TerminalReadbackEvidence(
                            readback_result, transcript_batch_id
                        )
                    )

            malformed = MalformedEvidence()
            blocked = (
                malformed_runtime
                .reconcile_inflight_turns_with_task7_evidence(
                    malformed, limit=100
                )
            )
            assert [item.status for item in blocked] == ["reconciling"]
            assert malformed.calls == 1
            turn_storage = tuple(malformed_conn.execute(
                """
                SELECT status, terminal_result_id, recovery_block_key,
                       transcript_applied_batch_id
                FROM project_turns WHERE turn_id = ?
                """,
                (malformed_turn.turn_id,),
            ).fetchone())
            assert turn_storage[0:2] == ("reconciling", None)
            assert type(turn_storage[2]) is str and turn_storage[2]
            assert turn_storage[3] is None
            assert tuple(malformed_conn.execute(
                """
                SELECT transcript_pending_batch_id,
                       transcript_dispatch_block_key
                FROM project_runtime_state WHERE project_id = ?
                """,
                (malformed_project,),
            ).fetchone()) == (None, None)
            assert malformed_conn.execute(
                """
                SELECT COUNT(*) FROM project_events
                WHERE project_id = ? AND turn_id = ?
                  AND kind = 'turn.recovery_blocked'
                """,
                (malformed_project, malformed_turn.turn_id),
            ).fetchone()[0] == 1
            blocked_snapshot = durable_snapshot(
                malformed_conn,
                malformed_project,
                malformed_turn.turn_id,
            )
            replay = (
                malformed_runtime
                .reconcile_inflight_turns_with_task7_evidence(
                    malformed, limit=100
                )
            )
            assert tuple(item.turn_id for item in replay) in (
                (),
                (malformed_turn.turn_id,),
            )
            assert malformed.calls == 1
            assert durable_snapshot(
                malformed_conn,
                malformed_project,
                malformed_turn.turn_id,
            ) == blocked_snapshot
        finally:
            malformed_conn.close()

    for drift_kind in (
        "control",
        "turn",
        "unexpected_lease",
        "pending_gate",
        "block_gate",
    ):
        drift_now = [100]
        (
            drift_module,
            drift_conn,
            drift_runtime,
            drift_project,
            drift_actor,
        ) = _make_runtime(
            tmp_path / f"c7-authority-drift-{drift_kind}.db",
            clock=lambda: drift_now[0],
        )
        try:
            drift_turn, drift_claim = _enqueue_and_claim(
                drift_runtime, drift_project, drift_actor
            )
            drift_runtime.mark_turn_started(drift_claim)
            drift_now[0] = drift_claim.lease_expires_at + 1
            drift_snapshot = []
            drift_trace = []
            drift_trace_cutoff = []
            drift_change_cutoff = []

            class DriftingEvidence:
                def read_turn_with_evidence(self, request):
                    assert drift_conn.in_transaction is False
                    if drift_kind == "control":
                        drift_conn.execute(
                            """
                            UPDATE project_run_controls
                            SET claim_worker_id = 'drifted-worker'
                            WHERE project_id = ? AND turn_id = ?
                            """,
                            (drift_project, drift_turn.turn_id),
                        )
                    elif drift_kind == "turn":
                        drift_conn.execute(
                            """
                            UPDATE project_turns
                            SET fencing_token = fencing_token + 1
                            WHERE project_id = ? AND turn_id = ?
                            """,
                            (drift_project, drift_turn.turn_id),
                        )
                    elif drift_kind == "unexpected_lease":
                        drift_conn.execute(
                            """
                            INSERT INTO project_worker_leases (
                                lease_id, project_id, turn_id, worker_id,
                                lease_generation, fencing_token, expires_at,
                                updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                drift_claim.attempt_id,
                                drift_project,
                                drift_turn.turn_id,
                                drift_claim.worker_id,
                                drift_claim.lease_generation,
                                drift_claim.fencing_token,
                                drift_claim.lease_expires_at,
                                drift_now[0],
                            ),
                        )
                    elif drift_kind == "pending_gate":
                        drift_conn.execute(
                            """
                            UPDATE project_runtime_state
                            SET transcript_pending_batch_id = ?
                            WHERE project_id = ?
                            """,
                            (other_batch, drift_project),
                        )
                    else:
                        drift_conn.execute(
                            """
                            UPDATE project_runtime_state
                            SET transcript_dispatch_block_key =
                                'test-owned readback block'
                            WHERE project_id = ?
                            """,
                            (drift_project,),
                        )
                    drift_conn.commit()
                    drift_snapshot.append(
                        durable_snapshot(
                            drift_conn,
                            drift_project,
                            drift_turn.turn_id,
                        )
                    )
                    drift_trace_cutoff.append(len(drift_trace))
                    drift_change_cutoff.append(
                        drift_conn.total_changes
                    )
                    return (
                        drift_module.Task7TerminalReadbackEvidence(
                            drift_module.TurnReadbackResult(
                                "succeeded", batch_id
                            ),
                            batch_id,
                        )
                    )

            drift_conn.set_trace_callback(drift_trace.append)
            try:
                with pytest.raises(
                    drift_module.ProjectRuntimeError
                ) as drift:
                    (
                        drift_runtime
                        .reconcile_inflight_turns_with_task7_evidence(
                            DriftingEvidence(), limit=100
                        )
                    )
            finally:
                drift_conn.set_trace_callback(None)
            assert (
                drift.value.code
                is drift_module.RuntimeErrorCode.PROJECT_AUTHORITY_CONFLICT
            )
            assert drift_trace_cutoff and drift_change_cutoff
            assert not any(
                statement.lstrip().upper().startswith(
                    ("INSERT", "UPDATE", "DELETE", "REPLACE")
                )
                for statement in drift_trace[drift_trace_cutoff[0]:]
            )
            assert drift_conn.total_changes == drift_change_cutoff[0]
            assert durable_snapshot(
                drift_conn, drift_project, drift_turn.turn_id
            ) == drift_snapshot[0]
        finally:
            drift_conn.close()

    opaque_now = [100]
    (
        opaque_module,
        opaque_conn,
        opaque_runtime,
        opaque_project,
        opaque_actor,
    ) = _make_runtime(
        tmp_path / "c7-opaque-readback.db",
        clock=lambda: opaque_now[0],
    )
    try:
        opaque_turn, opaque_claim = _enqueue_and_claim(
            opaque_runtime, opaque_project, opaque_actor
        )
        opaque_runtime.mark_turn_started(opaque_claim)
        opaque_now[0] = opaque_claim.lease_expires_at + 1

        class OpaqueTask56Readback:
            def read_turn(self, request):
                assert opaque_conn.in_transaction is False
                return opaque_module.TurnReadbackResult(
                    "succeeded", "operation-pre-effect-blocked:opaque"
                )

        opaque = opaque_runtime.reconcile_inflight_turns(
            OpaqueTask56Readback(), limit=100
        )
        assert [item.status for item in opaque] == ["succeeded"]
        assert tuple(opaque_conn.execute(
            """
            SELECT transcript_pending_batch_id,
                   transcript_dispatch_block_key
            FROM project_runtime_state WHERE project_id = ?
            """,
            (opaque_project,),
        ).fetchone()) == (None, None)
        assert tuple(opaque_conn.execute(
            """
            SELECT transcript_applied_batch_id
            FROM project_turns WHERE turn_id = ?
            """,
            (opaque_turn.turn_id,),
        ).fetchone()) == (None,)
    finally:
        opaque_conn.close()


_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKER_PROBE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "project_runtime_worker_probe.py"
)


class _ProbeHandle:
    def __init__(self, process, probe_id):
        self.process = process
        self.probe_id = probe_id
        self.stdout = queue.Queue()
        self.stderr = []
        self.threads = [
            threading.Thread(
                target=self._pump_stdout,
                name=f"probe-stdout-{probe_id}",
                daemon=True,
            ),
            threading.Thread(
                target=self._pump_stderr,
                name=f"probe-stderr-{probe_id}",
                daemon=True,
            ),
        ]
        for thread in self.threads:
            thread.start()

    def _pump_stdout(self):
        for line in self.process.stdout:
            self.stdout.put(line)
        self.stdout.put(None)

    def _pump_stderr(self):
        self.stderr.extend(self.process.stderr)

    def send(self, event):
        self.process.stdin.write(
            json.dumps(event, sort_keys=True, separators=(",", ":"))
            + "\n"
        )
        self.process.stdin.flush()

    def expect(self, event, *, timeout=15):
        try:
            line = self.stdout.get(timeout=timeout)
        except queue.Empty as exc:
            raise AssertionError(
                f"probe {self.probe_id} timed out; "
                f"stderr={''.join(self.stderr)!r}"
            ) from exc
        if line is None:
            raise AssertionError(
                f"probe {self.probe_id} exited before {event}; "
                f"returncode={self.process.poll()}; "
                f"stderr={''.join(self.stderr)!r}"
            )
        payload = json.loads(line)
        assert payload["version"] == 1
        assert payload["probe_id"] == self.probe_id
        assert payload["event"] == event
        return payload

    def complete(self, *, returncode=0, timeout=15):
        actual = self.process.wait(timeout=timeout)
        for thread in self.threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
        extras = []
        while True:
            try:
                line = self.stdout.get_nowait()
            except queue.Empty:
                break
            if line is not None:
                extras.append(line)
        assert actual == returncode, (
            self.probe_id,
            actual,
            "".join(self.stderr),
        )
        assert extras == []


class _ProbeSet:
    def __init__(self):
        self.processes = []
        self.handles = []

    def __enter__(self):
        return self

    def spawn(self, prepare):
        environment = os.environ.copy()
        environment["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (str(_REPO_ROOT), environment.get("PYTHONPATH")),
            )
        )
        environment["PYTHONUTF8"] = "1"
        environment["PYTHONIOENCODING"] = "utf-8"
        process = subprocess.Popen(
            [sys.executable, str(_WORKER_PROBE)],
            cwd=_REPO_ROOT,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        self.processes.append(process)
        handle = _ProbeHandle(process, prepare["probe_id"])
        self.handles.append(handle)
        handle.send(prepare)
        return handle

    def __exit__(self, exc_type, exc, traceback):
        errors = []

        def is_alive(process, label):
            try:
                return process.poll() is None
            except BaseException as cleanup_error:
                errors.append(f"{label} poll: {cleanup_error!r}")
                return True

        def attempt(label, action):
            try:
                action()
            except BaseException as cleanup_error:
                errors.append(f"{label}: {cleanup_error!r}")

        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                attempt(
                    f"process {index} terminate", process.terminate
                )
        deadline = time.monotonic() + 5
        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                try:
                    process.wait(
                        timeout=max(0.01, deadline - time.monotonic())
                    )
                except subprocess.TimeoutExpired:
                    pass
                except BaseException as cleanup_error:
                    errors.append(
                        f"process {index} terminate wait: "
                        f"{cleanup_error!r}"
                    )
        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                attempt(f"process {index} kill", process.kill)
        deadline = time.monotonic() + 5
        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                try:
                    process.wait(
                        timeout=max(0.01, deadline - time.monotonic())
                    )
                except subprocess.TimeoutExpired as cleanup_error:
                    errors.append(
                        f"process {index} kill wait: "
                        f"{cleanup_error!r}"
                    )
                except BaseException as cleanup_error:
                    errors.append(
                        f"process {index} kill wait: "
                        f"{cleanup_error!r}"
                    )
        for process_index, process in enumerate(self.processes):
            for stream in (
                process.stdin,
                process.stdout,
                process.stderr,
            ):
                if stream is not None:
                    attempt(
                        f"process {process_index} stream close",
                        stream.close,
                    )
        for handle_index, handle in enumerate(self.handles):
            for thread_index, thread in enumerate(handle.threads):
                attempt(
                    f"handle {handle_index} thread {thread_index} join",
                    lambda thread=thread: thread.join(timeout=5),
                )
        for index, process in enumerate(self.processes):
            if is_alive(process, f"process {index}"):
                errors.append(f"process {index} is still alive")
        for handle_index, handle in enumerate(self.handles):
            for thread_index, thread in enumerate(handle.threads):
                try:
                    alive = thread.is_alive()
                except BaseException as cleanup_error:
                    errors.append(
                        f"handle {handle_index} thread {thread_index} "
                        f"status: {cleanup_error!r}"
                    )
                    continue
                if alive:
                    errors.append(
                        f"handle {handle_index} thread {thread_index} "
                        "is still alive"
                    )
        if errors:
            message = "probe cleanup failed: " + "; ".join(errors)
            if exc is not None:
                attempt("primary exception note", lambda: exc.add_note(message))
            else:
                raise AssertionError(message)
        return False


def _probe_prepare(
    probe_id,
    action,
    path,
    project_id,
    worker_id,
    now,
    **extra,
):
    return {
        "version": 1,
        "event": "prepare",
        "probe_id": probe_id,
        "action": action,
        "db_path": str(path),
        "project_id": project_id,
        "worker_id": worker_id,
        "now": now,
        **extra,
    }


def _release_probes(handles):
    for handle in handles:
        ready = handle.expect("ready")
        assert ready["stage"] == "before_begin_immediate"
    barrier = threading.Barrier(len(handles) + 1)
    errors = []

    def release(handle):
        try:
            barrier.wait(timeout=5)
            handle.send(
                {
                    "version": 1,
                    "event": "go",
                    "probe_id": handle.probe_id,
                }
            )
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=release, args=(handle,), daemon=True)
        for handle in handles
    ]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    assert errors == []


def _run_probe(
    probes,
    prepare,
    *,
    expected_event="result",
    returncode=0,
):
    handle = probes.spawn(prepare)
    _release_probes([handle])
    payload = handle.expect(expected_event)
    handle.complete(returncode=returncode)
    return payload


def test_probe_cleanup_visits_all_children_after_one_cleanup_error():
    class _Stream:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    class _Process:
        def __init__(self, *, fail_terminate=False):
            self.fail_terminate = fail_terminate
            self.returncode = None
            self.calls = []
            self.stdin = _Stream()
            self.stdout = _Stream()
            self.stderr = _Stream()

        def poll(self):
            return self.returncode

        def terminate(self):
            self.calls.append("terminate")
            if self.fail_terminate:
                raise OSError("terminate failed")
            self.returncode = -1

        def wait(self, timeout):
            self.calls.append("wait")
            if self.returncode is None:
                raise subprocess.TimeoutExpired("probe", timeout)
            return self.returncode

        def kill(self):
            self.calls.append("kill")
            self.returncode = -9

    class _Thread:
        def __init__(self):
            self.joined = False

        def join(self, timeout):
            self.joined = True

        def is_alive(self):
            return not self.joined

    class _Handle:
        def __init__(self):
            self.threads = [_Thread(), _Thread()]

    first = _Process(fail_terminate=True)
    second = _Process()
    probes = _ProbeSet()
    probes.processes = [first, second]
    probes.handles = [_Handle(), _Handle()]

    with pytest.raises(AssertionError, match="cleanup"):
        probes.__exit__(None, None, None)

    assert "kill" in first.calls
    assert "terminate" in second.calls
    assert all(
        stream.closed
        for process in (first, second)
        for stream in (process.stdin, process.stdout, process.stderr)
    )
    assert all(
        thread.joined
        for handle in probes.handles
        for thread in handle.threads
    )
    assert all(process.poll() is not None for process in (first, second))


class _RecordingReadback:
    def __init__(self, conn, result=None, *, error=None, barrier=None):
        self.conn = conn
        self.result = result
        self.error = error
        self.barrier = barrier
        self.calls = []

    def read_turn(self, request):
        assert self.conn.in_transaction is False
        self.calls.append(request)
        if self.barrier is not None:
            self.barrier.wait(timeout=5)
        if self.error is not None:
            raise self.error
        return self.result


def _legacy_task4_turn_database(path):
    conn = sqlite3.connect(path, timeout=10)
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
        INSERT INTO projects VALUES ('legacy', 'legacy', 'Legacy', 1, 0);
        CREATE TABLE project_turns (
            turn_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL
                REFERENCES projects(id) ON DELETE RESTRICT,
            sequence INTEGER NOT NULL,
            idempotency_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            origin_binding_id TEXT,
            status TEXT NOT NULL,
            attempt_id TEXT,
            lease_generation INTEGER NOT NULL DEFAULT 0,
            fencing_token INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE (project_id, turn_id),
            UNIQUE (project_id, sequence),
            UNIQUE (project_id, idempotency_key)
        );
        INSERT INTO project_turns VALUES
            ('queued', 'legacy', 1, 'q', '{}', NULL, 'queued',
             NULL, 0, 0, 1, 1),
            ('claimed', 'legacy', 2, 'c', '{}', NULL, 'claimed',
             'attempt-c', 1, 1, 2, 2),
            ('stopped', 'legacy', 3, 's', '{}', NULL, 'stopped',
             'attempt-s', 2, 2, 3, 3),
            ('terminal', 'legacy', 4, 't', '{}', NULL, 'succeeded',
             'attempt-t', 3, 3, 4, 4);
        """
    )
    conn.commit()
    return conn


def test_task4_public_values_remain_exact_and_task9_surface_stays_absent():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert tuple(field.name for field in fields(module.ProjectTurn)) == (
        "turn_id",
        "project_id",
        "sequence",
        "idempotency_key",
        "payload",
        "origin_binding_id",
        "status",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "created_at",
        "updated_at",
    )
    assert tuple(field.name for field in fields(module.RunControl)) == (
        "turn_id",
        "project_id",
        "control_state",
        "control_version",
        "last_idempotency_key",
        "attempt_id",
        "updated_at",
    )
    assert tuple(field.name for field in fields(module.TurnClaim)) == (
        "turn_id",
        "project_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    )
    assert not hasattr(module, "ProjectSnapshot")
    assert not hasattr(module.ProjectRuntime, "execute_command")


def test_task5_aliases_and_frozen_dtos_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert module.TerminalTurnStatus == Literal["succeeded", "failed"]
    assert module.ExecutionState == Literal["not_started", "started"]
    assert module.RecoverySourceStatus == Literal["claimed", "stop_requested"]
    assert module.ReadbackOutcome == Literal[
        "succeeded", "failed", "stopped", "unknown"
    ]
    assert tuple(field.name for field in fields(module.CanonicalTurnResult)) == (
        "status",
        "result_id",
    )
    assert tuple(field.name for field in fields(module.TurnReadbackRequest)) == (
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
        "source_status",
        "execution_state",
    )
    assert tuple(field.name for field in fields(module.TurnReadbackResult)) == (
        "outcome",
        "result_id",
    )
    assert module.CanonicalTurnResult.__dataclass_params__.frozen is True
    assert module.TurnReadbackRequest.__dataclass_params__.frozen is True
    assert module.TurnReadbackResult.__dataclass_params__.frozen is True
    assert get_type_hints(module.CanonicalTurnResult) == {
        "status": module.TerminalTurnStatus,
        "result_id": str,
    }
    assert get_type_hints(module.TurnReadbackResult) == {
        "outcome": module.ReadbackOutcome,
        "result_id": str | None,
    }


def test_task5_readback_protocol_has_one_exact_method():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert issubclass(module.TurnReadbackPort, Protocol)
    assert {
        name
        for name in module.TurnReadbackPort.__dict__
        if not name.startswith("_")
    } == {"read_turn"}
    signature = inspect.signature(module.TurnReadbackPort.read_turn)
    assert tuple(signature.parameters) == ("self", "request")
    assert get_type_hints(module.TurnReadbackPort.read_turn) == {
        "request": module.TurnReadbackRequest,
        "return": module.TurnReadbackResult,
    }


def test_task5_service_signatures_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")
    runtime = module.ProjectRuntime

    expected_parameters = {
        "heartbeat_turn": ("self", "claim", "lease_seconds"),
        "mark_turn_started": ("self", "claim"),
        "commit_turn": ("self", "claim", "result"),
        "reconcile_inflight_turns": ("self", "readback", "limit"),
    }
    expected_hints = {
        "heartbeat_turn": {
            "claim": module.TurnClaim,
            "lease_seconds": int,
            "return": module.TurnClaim,
        },
        "mark_turn_started": {
            "claim": module.TurnClaim,
            "return": module.TurnClaim,
        },
        "commit_turn": {
            "claim": module.TurnClaim,
            "result": module.CanonicalTurnResult,
            "return": module.ProjectTurn,
        },
        "reconcile_inflight_turns": {
            "readback": module.TurnReadbackPort,
            "limit": int,
            "return": tuple[module.ProjectTurn, ...],
        },
    }

    for name, parameters in expected_parameters.items():
        method = getattr(runtime, name)
        signature = inspect.signature(method)
        assert tuple(signature.parameters) == parameters
        assert get_type_hints(method) == expected_hints[name]
    assert (
        inspect.signature(runtime.heartbeat_turn).parameters[
            "lease_seconds"
        ].kind
        is inspect.Parameter.KEYWORD_ONLY
    )
    assert (
        inspect.signature(runtime.reconcile_inflight_turns).parameters[
            "limit"
        ].kind
        is inspect.Parameter.KEYWORD_ONLY
    )


def test_task5_stable_error_codes_are_exact():
    module = importlib.import_module("hermes_cli.project_runtime")

    assert module.RuntimeErrorCode.STALE_TURN_CLAIM.value == "stale_turn_claim"
    assert (
        module.RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED.value
        == "turn_execution_not_started"
    )
    assert (
        module.RuntimeErrorCode.TERMINAL_RESULT_CONFLICT.value
        == "terminal_result_conflict"
    )


def test_fresh_task5_turn_schema_enforces_metadata_and_indexes(tmp_path):
    conn = projects_db.connect(tmp_path / "fresh.db")
    try:
        columns = {
            row["name"]: row
            for row in conn.execute("PRAGMA table_info(project_turns)")
        }
        indexes = {
            row["name"]: row
            for row in conn.execute("PRAGMA index_list(project_turns)")
        }
        lease_indexes = {
            row["name"]
            for row in conn.execute("PRAGMA index_list(project_worker_leases)")
        }
        event_indexes = {
            row["name"]: row
            for row in conn.execute("PRAGMA index_list(project_events)")
        }
        assert columns["execution_state"]["notnull"] == 0
        assert columns["terminal_result_id"]["notnull"] == 0
        assert columns["recovery_block_key"]["notnull"] == 0
        assert indexes["idx_project_turns_terminal_result"]["unique"] == 1
        assert "idx_project_turns_project_sequence" in indexes
        assert "idx_project_turns_actionable_recovery" in indexes
        assert "idx_project_worker_leases_expiry" in lease_indexes
        assert (
            event_indexes[
                "idx_project_events_recovery_block_attempt"
            ]["unique"]
            == 1
        )

        project_id = projects_db.create_project(conn, name="Schema checks")
        common = (
            project_id,
            "{}",
            "claimed",
            "attempt",
            1,
            1,
            1,
            1,
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, created_at, updated_at, execution_state
                ) VALUES ('bad-execution', ?, 1, 'bad-execution', ?, ?, ?, ?,
                          ?, ?, ?, 'unknown')
                """,
                common,
            )
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, created_at, updated_at, terminal_result_id
                ) VALUES ('bad-result', ?, 1, 'bad-result', ?, ?, ?, ?, ?, ?,
                          ?, '')
                """,
                common,
            )
    finally:
        conn.close()


def test_task4_turn_rows_migrate_additively_without_backfill_or_events(tmp_path):
    conn = _legacy_task4_turn_database(tmp_path / "legacy.db")
    old_columns = (
        "turn_id",
        "project_id",
        "sequence",
        "idempotency_key",
        "payload_json",
        "origin_binding_id",
        "status",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "created_at",
        "updated_at",
    )
    before = tuple(
        tuple(row)
        for row in conn.execute(
            f"SELECT {', '.join(old_columns)} FROM project_turns ORDER BY sequence"
        )
    )

    prdb.ensure_schema(conn)
    prdb.ensure_schema(conn)

    after = tuple(
        tuple(row)
        for row in conn.execute(
            f"SELECT {', '.join(old_columns)} FROM project_turns ORDER BY sequence"
        )
    )
    task5_values = tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT execution_state, terminal_result_id, recovery_block_key
            FROM project_turns ORDER BY sequence
            """
        )
    )
    assert after == before
    assert task5_values == ((None, None, None),) * 4
    assert conn.execute(
        "SELECT COUNT(*) FROM project_events WHERE kind = 'turn.recovery_blocked'"
    ).fetchone()[0] == 0
    conn.close()


@pytest.mark.parametrize(
    ("execution_state", "terminal_result_id"),
    [
        pytest.param("unknown", None, id="execution-enum"),
        pytest.param(None, "", id="empty-terminal-result"),
    ],
)
def test_task5_turn_mapper_fails_closed_on_malformed_metadata(
    execution_state, terminal_result_id
):
    row = {
        "turn_id": "turn",
        "project_id": "project",
        "sequence": 1,
        "idempotency_key": "key",
        "payload_json": "{}",
        "origin_binding_id": "binding",
        "status": "queued",
        "attempt_id": None,
        "lease_generation": 0,
        "fencing_token": 0,
        "created_at": 1,
        "updated_at": 1,
        "execution_state": execution_state,
        "terminal_result_id": terminal_result_id,
        "recovery_block_key": None,
        "transcript_applied_batch_id": None,
    }

    with pytest.raises(RuntimeError):
        prdb.runtime_turn_from_row(row)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {"execution_state": "started"},
            id="pristine-queued-execution-state",
        ),
        pytest.param(
            {"terminal_result_id": "queued-result"},
            id="nonterminal-result",
        ),
        pytest.param(
            {
                "status": "succeeded",
                "terminal_result_id": "orphan-result",
            },
            id="terminal-result-without-attempt",
        ),
        pytest.param(
            {
                "status": "succeeded",
                "attempt_id": "attempt",
                "lease_generation": 1,
                "fencing_token": 1,
                "execution_state": "not_started",
                "terminal_result_id": "impossible-result",
            },
            id="terminal-result-before-start",
        ),
        pytest.param(
            {
                "status": "cancelled",
                "attempt_id": "attempt",
                "lease_generation": 1,
                "fencing_token": 1,
                "execution_state": "started",
                "terminal_result_id": "cancel-result",
            },
            id="result-on-non-result-terminal",
        ),
        pytest.param(
            {"recovery_block_key": "unexpected-block-key"},
            id="block-key-on-pristine-queued",
        ),
    ],
)
def test_task5_turn_mapper_rejects_impossible_metadata_combinations(
    overrides,
):
    row = {
        "turn_id": "turn",
        "project_id": "project",
        "sequence": 1,
        "idempotency_key": "key",
        "payload_json": "{}",
        "origin_binding_id": "binding",
        "status": "queued",
        "attempt_id": None,
        "lease_generation": 0,
        "fencing_token": 0,
        "execution_state": None,
        "terminal_result_id": None,
        "recovery_block_key": None,
        "transcript_applied_batch_id": None,
        "created_at": 1,
        "updated_at": 1,
    }
    row.update(overrides)

    with pytest.raises(RuntimeError):
        prdb.runtime_turn_from_row(row)


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
@pytest.mark.parametrize("execution_state", ["started", "not_started"])
def test_terminal_execution_marker_without_result_fails_mapper(
    terminal_status, execution_state
):
    row = {
        "turn_id": "terminal-turn",
        "project_id": "project",
        "sequence": 1,
        "idempotency_key": "terminal-key",
        "payload_json": "{}",
        "origin_binding_id": "binding",
        "status": terminal_status,
        "attempt_id": "attempt",
        "lease_generation": 1,
        "fencing_token": 1,
        "execution_state": execution_state,
        "terminal_result_id": None,
        "recovery_block_key": None,
        "transcript_applied_batch_id": None,
        "created_at": 1,
        "updated_at": 1,
    }

    with pytest.raises(RuntimeError):
        prdb.runtime_turn_from_row(row)


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
def test_legacy_terminal_without_task5_evidence_remains_mappable(
    terminal_status,
):
    row = {
        "turn_id": "legacy-terminal",
        "project_id": "project",
        "sequence": 1,
        "idempotency_key": "legacy-key",
        "payload_json": "{}",
        "origin_binding_id": "binding",
        "status": terminal_status,
        "attempt_id": "legacy-attempt",
        "lease_generation": 1,
        "fencing_token": 1,
        "execution_state": None,
        "terminal_result_id": None,
        "recovery_block_key": None,
        "transcript_applied_batch_id": None,
        "created_at": 1,
        "updated_at": 1,
    }

    mapped = prdb.runtime_turn_from_row(row)

    assert mapped.status == terminal_status
    assert mapped.execution_state is None
    assert mapped.terminal_result_id is None


def test_pair_validator_rejects_impossible_task5_metadata(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "pair-metadata.db"
    )
    try:
        turn, _ = _enqueue_and_claim(runtime, project_id, actor)
        persisted = prdb._runtime_turn_for_project(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        assert persisted is not None

        with pytest.raises(RuntimeError):
            prdb._validate_runtime_turn_pair(
                conn,
                turn=replace(
                    persisted, terminal_result_id="nonterminal-result"
                ),
            )
    finally:
        conn.close()


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
@pytest.mark.parametrize("execution_state", ["started", "not_started"])
def test_terminal_execution_marker_without_result_fails_pair_validation(
    tmp_path, terminal_status, execution_state
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"pair-missing-{terminal_status}-{execution_state}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        runtime.commit_turn(
            claim,
            module.CanonicalTurnResult(
                terminal_status, f"result-{terminal_status}"
            ),
        )
        persisted = prdb._runtime_turn_for_project(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        assert persisted is not None

        with pytest.raises(
            RuntimeError, match="inconsistent Task-5 metadata"
        ):
            prdb._validate_runtime_turn_pair(
                conn,
                turn=replace(
                    persisted,
                    execution_state=execution_state,
                    terminal_result_id=None,
                ),
            )
    finally:
        conn.close()


def test_claim_scan_rejects_historical_orphan_terminal_result(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "historical-orphan-result.db"
    )
    try:
        with prdb.write_transaction(conn):
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, origin_binding_id, status, attempt_id,
                    lease_generation, fencing_token, execution_state,
                    terminal_result_id, created_at, updated_at
                ) VALUES (
                    'historical', ?, 1, 'historical', '{}',
                    'owner-binding', 'succeeded', NULL, 0, 0, NULL,
                    'orphan-result', 1, 1
                )
                """,
                (project_id,),
            )
            conn.execute(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    idempotency_key, command_fingerprint, attempt_id,
                    claim_worker_id, claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (
                    'historical', ?, 'terminal', 0, NULL, NULL, NULL,
                    NULL, NULL, NULL, 1
                )
                """,
                (project_id,),
            )
        target = runtime.enqueue_turn(
            project_id,
            {"message": "must remain queued"},
            actor,
            idempotency_key="target",
            expected_version=0,
        )
        before = _claim_snapshot(conn, project_id, target.turn_id)

        with pytest.raises(RuntimeError):
            runtime.claim_next_turn(
                project_id, "worker", lease_seconds=30
            )

        assert _claim_snapshot(conn, project_id, target.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
@pytest.mark.parametrize("execution_state", ["started", "not_started"])
def test_fifo_claim_rejects_terminal_execution_marker_without_result(
    tmp_path, terminal_status, execution_state
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"history-missing-{terminal_status}-{execution_state}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        runtime.commit_turn(
            claim,
            module.CanonicalTurnResult(
                terminal_status, f"result-{terminal_status}"
            ),
        )
        conn.execute(
            """
            UPDATE project_turns
            SET execution_state = ?, terminal_result_id = NULL
            WHERE project_id = ? AND turn_id = ?
            """,
            (execution_state, project_id, turn.turn_id),
        )
        conn.commit()
        state = prdb.runtime_state_for_project(conn, project_id)
        target = runtime.enqueue_turn(
            project_id,
            {"message": "must remain queued"},
            actor,
            idempotency_key=f"after-{terminal_status}-{execution_state}",
            expected_version=state.version,
        )
        before = _claim_snapshot(conn, project_id, target.turn_id)

        with pytest.raises(RuntimeError):
            runtime.claim_next_turn(
                project_id, "later-worker", lease_seconds=30
            )

        assert _claim_snapshot(conn, project_id, target.turn_id) == before
    finally:
        conn.close()


def test_valid_resumed_queued_attempt_metadata_remains_accepted(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "valid-resumed-metadata.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
        runtime.acknowledge_stopped(claim)
        runtime.request_resume(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="resume",
            expected_version=4,
            expected_control_version=3,
        )

        resumed = prdb._runtime_turn_for_project(
            conn, project_id=project_id, turn_id=turn.turn_id
        )

        assert resumed is not None
        assert resumed.status == "queued"
        assert resumed.attempt_id == claim.attempt_id
        assert resumed.execution_state == "started"
        assert resumed.terminal_result_id is None
        prdb._validate_runtime_turn_pair(conn, turn=resumed)
    finally:
        conn.close()


def test_new_claim_persists_not_started_in_the_atomic_claim(tmp_path):
    _, conn, runtime, project_id, actor = _make_runtime(tmp_path / "claim.db")
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "claim"},
            actor,
            idempotency_key="claim",
            expected_version=0,
        )
        claim = runtime.claim_next_turn(project_id, "worker", lease_seconds=30)

        assert claim is not None
        row = conn.execute(
            """
            SELECT execution_state, terminal_result_id, recovery_block_key
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()
        assert tuple(row) == ("not_started", None, None)
    finally:
        conn.close()


def test_two_connections_racing_task5_migration_converge(tmp_path):
    path = tmp_path / "migration-race.db"
    _legacy_task4_turn_database(path).close()
    barrier = threading.Barrier(2)

    def migrate():
        conn = sqlite3.connect(path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            barrier.wait(timeout=5)
            prdb.ensure_schema(conn)
            conn.commit()
            return {
                row["name"]
                for row in conn.execute("PRAGMA table_info(project_turns)")
            }
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(migrate) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]

    assert all(
        {
            "execution_state",
            "terminal_result_id",
            "recovery_block_key",
        }
        <= columns
        for columns in results
    )


def test_preprojection_recovery_block_migrates_to_indexed_key(tmp_path):
    path = tmp_path / "legacy-recovery-block.db"
    project_id = "legacy-project"
    turn_id = "legacy-turn"
    attempt_id = "legacy-attempt"
    block_key = prdb._recovery_block_key(
        project_id=project_id,
        turn_id=turn_id,
        attempt_id=attempt_id,
        lease_generation=1,
        fencing_token=1,
    )
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO projects VALUES (
            'legacy-project', 'legacy-project', 'Legacy', 1, 0
        );
        CREATE TABLE project_conversations (
            conversation_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES projects(id),
            parent_conversation_id TEXT,
            root_conversation_id TEXT,
            created_at INTEGER NOT NULL,
            UNIQUE(project_id, conversation_id)
        );
        INSERT INTO project_conversations VALUES (
            'legacy-root', 'legacy-project', NULL, 'legacy-root', 1
        );
        CREATE TABLE project_runtime_state (
            project_id TEXT PRIMARY KEY,
            lifecycle TEXT NOT NULL,
            current_phase TEXT,
            version INTEGER NOT NULL,
            conversation_root_id TEXT,
            conversation_tip_id TEXT,
            updated_at INTEGER NOT NULL
        );
        INSERT INTO project_runtime_state VALUES (
            'legacy-project', 'active', 'implementation', 2,
            'legacy-root', 'legacy-root', 1
        );
        CREATE TABLE project_turns (
            turn_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES projects(id),
            sequence INTEGER NOT NULL,
            idempotency_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            origin_binding_id TEXT,
            status TEXT NOT NULL,
            attempt_id TEXT,
            lease_generation INTEGER NOT NULL,
            fencing_token INTEGER NOT NULL,
            execution_state TEXT,
            terminal_result_id TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(project_id, turn_id),
            UNIQUE(project_id, sequence),
            UNIQUE(project_id, idempotency_key)
        );
        INSERT INTO project_turns VALUES (
            'legacy-turn', 'legacy-project', 1, 'legacy', '{}', NULL,
            'reconciling', 'legacy-attempt', 1, 1, 'started', NULL, 1, 1
        );
        CREATE TABLE project_run_controls (
            turn_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            control_state TEXT NOT NULL,
            control_version INTEGER NOT NULL,
            idempotency_key TEXT,
            command_fingerprint TEXT,
            attempt_id TEXT,
            claim_worker_id TEXT,
            claim_lease_expires_at INTEGER,
            claim_canonical_session_id TEXT,
            updated_at INTEGER NOT NULL,
            UNIQUE(project_id, turn_id),
            UNIQUE(project_id, idempotency_key)
        );
        INSERT INTO project_run_controls VALUES (
            'legacy-turn', 'legacy-project', 'running', 1, NULL, NULL,
            'legacy-attempt', 'worker', 100, 'session', 1
        );
        CREATE TABLE project_events (
            event_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            kind TEXT NOT NULL,
            turn_id TEXT,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(project_id, event_id),
            UNIQUE(project_id, sequence)
        );
        """
    )
    conn.execute(
        """
        INSERT INTO project_events VALUES (
            ?, ?, 1, 'turn.recovery_blocked', ?, ?, 1
        )
        """,
        (
            block_key,
            project_id,
            turn_id,
            json.dumps(
                {
                    "attempt_id": attempt_id,
                    "fencing_token": 1,
                    "lease_generation": 1,
                    "source_status": "claimed",
                    "turn_id": turn_id,
                    "version": 2,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        ),
    )
    conn.commit()
    conn.close()

    migrated = projects_db.connect(path)
    try:
        assert migrated.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn_id,),
        ).fetchone()[0] == block_key
        turn = prdb._runtime_turn_for_project(
            migrated, project_id=project_id, turn_id=turn_id
        )
        prdb._validate_runtime_turn_pair(migrated, turn=turn)
    finally:
        migrated.close()


@pytest.mark.parametrize("boolean_field", ["lease_generation", "fencing_token"])
def test_preprojection_migration_rejects_boolean_block_identity(
    tmp_path, boolean_field
):
    path = tmp_path / f"legacy-boolean-{boolean_field}.db"
    block_key = prdb._recovery_block_key(
        project_id="legacy",
        turn_id="claimed",
        attempt_id="attempt-c",
        lease_generation=1,
        fencing_token=1,
    )
    conn = _legacy_task4_turn_database(path)
    conn.executescript(
        """
        ALTER TABLE project_turns ADD COLUMN execution_state TEXT;
        ALTER TABLE project_turns ADD COLUMN terminal_result_id TEXT;
        UPDATE project_turns
        SET status = 'reconciling', execution_state = 'started'
        WHERE turn_id = 'claimed';
        CREATE TABLE project_events (
            event_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            kind TEXT NOT NULL,
            turn_id TEXT,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(project_id, event_id),
            UNIQUE(project_id, sequence)
        );
        """
    )
    payload = {
        "attempt_id": "attempt-c",
        "fencing_token": 1,
        "lease_generation": 1,
        "source_status": "claimed",
        "turn_id": "claimed",
        "version": 1,
    }
    payload[boolean_field] = True
    conn.execute(
        """
        INSERT INTO project_events VALUES (
            ?, 'legacy', 1, 'turn.recovery_blocked',
            'claimed', ?, 1
        )
        """,
        (
            block_key,
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
        ),
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="block event payload"):
        projects_db.connect(path)

    raw = sqlite3.connect(path)
    try:
        columns = {
            row[1] for row in raw.execute("PRAGMA table_info(project_turns)")
        }
        assert "recovery_block_key" not in columns
    finally:
        raw.close()


def test_heartbeat_extends_both_horizons_without_versions_or_events(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 110

        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)

        assert renewed == replace(claim, lease_expires_at=160)
        lease = prdb._current_worker_lease_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        control = prdb._runtime_control_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        assert lease is not None and lease.expires_at == 160
        assert control is not None and control.claim_lease_expires_at == 160
        assert control.control_version == 1
        assert prdb.runtime_state_for_project(conn, project_id).version == 2
        assert len(
            conn.execute(
                "SELECT 1 FROM project_events WHERE project_id = ?",
                (project_id,),
            ).fetchall()
        ) == 2
        assert before[0][:-1] == _claim_snapshot(
            conn, project_id, turn.turn_id
        )[0][:-1]
        assert module.RuntimeErrorCode.STALE_TURN_CLAIM.value == "stale_turn_claim"
    finally:
        conn.close()


def test_heartbeat_lost_response_retry_uses_observed_horizon_and_never_shortens(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat-retry.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = 110
        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)
        first_snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 120

        replay = runtime.heartbeat_turn(claim, lease_seconds=10)

        assert replay == renewed
        assert _claim_snapshot(conn, project_id, turn.turn_id) == first_snapshot
        with pytest.raises(module.ProjectRuntimeError) as greater:
            runtime.heartbeat_turn(
                replace(renewed, lease_expires_at=161),
                lease_seconds=50,
            )
        assert greater.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == first_snapshot
    finally:
        conn.close()


@pytest.mark.parametrize(
    "field_name",
    [
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    ],
)
def test_heartbeat_rejects_each_forged_authority_field_without_writes(
    tmp_path, field_name
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-forged-{field_name}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        value = getattr(claim, field_name)
        forged_value = f"{value}-forged" if type(value) is str else value + 1
        forged = replace(claim, **{field_name: forged_value})
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(forged, lease_seconds=60)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize("lease_seconds", [True, 0, -1, 1.5])
def test_heartbeat_rejects_invalid_ttl_before_writing(tmp_path, lease_seconds):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-invalid-{lease_seconds}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=lease_seconds)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_heartbeat_rejects_sqlite_integer_overflow_without_writes(tmp_path):
    now = (1 << 63) - 2
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "heartbeat-overflow.db", clock=lambda: now
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "overflow"},
            actor,
            idempotency_key="overflow",
            expected_version=0,
        )
        conn.execute(
            """
            UPDATE project_turns
            SET status = 'claimed', attempt_id = 'attempt', lease_generation = 1,
                fencing_token = 1, execution_state = 'not_started'
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        )
        conn.execute(
            """
            UPDATE project_run_controls
            SET control_state = 'running', control_version = 1,
                attempt_id = 'attempt', claim_worker_id = 'worker',
                claim_lease_expires_at = ?,
                claim_canonical_session_id = 'session-root'
            WHERE project_id = ? AND turn_id = ?
            """,
            ((1 << 63) - 1, project_id, turn.turn_id),
        )
        conn.execute(
            """
            INSERT INTO project_worker_leases (
                lease_id, project_id, turn_id, worker_id, lease_generation,
                fencing_token, expires_at, updated_at
            ) VALUES ('attempt', ?, ?, 'worker', 1, 1, ?, 1)
            """,
            (project_id, turn.turn_id, (1 << 63) - 1),
        )
        conn.commit()
        claim = module.TurnClaim(
            turn.turn_id,
            project_id,
            turn.sequence,
            "worker",
            "attempt",
            1,
            1,
            (1 << 63) - 1,
            "session-root",
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=2)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("turn_status", "control_state"),
    [
        pytest.param("stop_requested", "stop_requested", id="stop-requested"),
        pytest.param("awaiting_approval", "running", id="awaiting-approval"),
    ],
)
def test_heartbeat_is_allowed_for_the_exact_live_task5_status_set(
    tmp_path, turn_status, control_state
):
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-{turn_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        conn.execute(
            "UPDATE project_turns SET status = ? WHERE turn_id = ?",
            (turn_status, turn.turn_id),
        )
        conn.execute(
            """
            UPDATE project_run_controls SET control_state = ?
            WHERE turn_id = ?
            """,
            (control_state, turn.turn_id),
        )
        conn.commit()

        renewed = runtime.heartbeat_turn(claim, lease_seconds=60)

        assert renewed.lease_expires_at == 160
    finally:
        conn.close()


@pytest.mark.parametrize(
    "condition",
    ["expired", "reconciling", "legacy", "inactive"],
)
def test_heartbeat_rejects_expired_or_nonlive_claim_state_without_writes(
    tmp_path, condition
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"heartbeat-{condition}.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if condition == "expired":
            now[0] = claim.lease_expires_at
        elif condition == "reconciling":
            conn.execute(
                "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "legacy":
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
        else:
            state = prdb.runtime_state_for_project(conn, project_id)
            prdb.transition_lifecycle(
                conn,
                project_id=project_id,
                expected_version=state.version,
                lifecycle="awaiting_acceptance",
                updated_at=101,
            )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.heartbeat_turn(claim, lease_seconds=60)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_mark_started_is_exact_idempotent_and_metadata_only(tmp_path):
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "started.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 101

        first = runtime.mark_turn_started(claim)
        after = _claim_snapshot(conn, project_id, turn.turn_id)
        replay = runtime.mark_turn_started(claim)

        assert first == replay == claim
        assert conn.execute(
            "SELECT execution_state FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "started"
        assert after == _claim_snapshot(conn, project_id, turn.turn_id)
        assert after[1][3] == before[1][3]
        assert after[3].version == before[3].version
        assert after[4] == before[4]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "condition",
    ["expired", "awaiting_approval", "stopped", "reconciling", "legacy"],
)
def test_mark_started_rejects_every_nonlive_execution_state_without_writes(
    tmp_path, condition
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"started-{condition}.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if condition == "expired":
            now[0] = claim.lease_expires_at
        elif condition == "awaiting_approval":
            conn.execute(
                "UPDATE project_turns SET status = 'awaiting_approval' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "stopped":
            conn.execute(
                "UPDATE project_turns SET status = 'stopped' WHERE turn_id = ?",
                (turn.turn_id,),
            )
            conn.execute(
                "UPDATE project_run_controls SET control_state = 'stopped' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        elif condition == "reconciling":
            conn.execute(
                "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
                (turn.turn_id,),
            )
        else:
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.mark_turn_started(claim)

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_expired_worker_cannot_request_approval_or_acknowledge_stop(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "existing-expiry-gaps.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        request = prdb.ApprovalRequest(
            approval_id="expired-approval",
            project_id=project_id,
            requester_actor_id="owner",
            authorization_actor_id="owner",
            canonical_action="publish",
            approval_class="publish",
            command_revision=1,
            expected_runtime_version=2,
            expected_lifecycle="active",
            expected_phase="implementation",
            targets=("C:/work/release",),
            batch_id="batch",
            batch_items=("release",),
            status="pending",
            expires_at=1000,
        )
        before_approval = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError):
            runtime.request_turn_approval(
                turn.turn_id,
                request,
                actor,
                expected_control_version=1,
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before_approval
        assert conn.execute(
            "SELECT COUNT(*) FROM project_approvals WHERE project_id = ?",
            (project_id,),
        ).fetchone()[0] == 0

        stopped = runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop-at-expiry",
            expected_version=2,
            expected_control_version=1,
        )
        assert stopped.control_state == "stop_requested"
        before_ack = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError):
            runtime.acknowledge_stopped(claim)

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before_ack
    finally:
        conn.close()


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
def test_commit_started_turn_is_one_atomic_terminal_transition(
    tmp_path, terminal_status
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-{terminal_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        result = module.CanonicalTurnResult(
            status=terminal_status,
            result_id=f"result-{terminal_status}",
        )

        committed = runtime.commit_turn(claim, result)

        assert committed.status == terminal_status
        stored = conn.execute(
            """
            SELECT status, execution_state, terminal_result_id
            FROM project_turns WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()
        control = prdb._runtime_control_for_turn(
            conn, project_id=project_id, turn_id=turn.turn_id
        )
        event = conn.execute(
            """
            SELECT kind, payload_json FROM project_events
            WHERE project_id = ? ORDER BY sequence DESC LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        assert tuple(stored) == (
            terminal_status,
            "started",
            f"result-{terminal_status}",
        )
        assert control.control_state == "terminal"
        assert control.control_version == before[1][3] + 1
        assert prdb.runtime_state_for_project(conn, project_id).version == (
            before[3].version + 1
        )
        assert conn.execute(
            """
            SELECT 1 FROM project_worker_leases
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone() is None
        assert event["kind"] == f"turn.{terminal_status}"
        assert f"result-{terminal_status}" not in event["payload_json"]
    finally:
        conn.close()


def test_commit_exact_replay_is_write_free_and_conflicts_on_changed_result(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-replay.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        result = module.CanonicalTurnResult("succeeded", "result-1")
        first = runtime.commit_turn(claim, result)
        snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        now[0] = 10_000

        replay = runtime.commit_turn(claim, result)

        assert replay == first
        assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
        for changed in (
            module.CanonicalTurnResult("failed", "result-1"),
            module.CanonicalTurnResult("succeeded", "result-2"),
        ):
            with pytest.raises(module.ProjectRuntimeError) as conflict:
                runtime.commit_turn(claim, changed)
            assert (
                conflict.value.code
                is module.RuntimeErrorCode.TERMINAL_RESULT_CONFLICT
            )
            assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
    finally:
        conn.close()


def test_commit_before_start_has_a_distinct_write_free_error(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-before-start.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                claim, module.CanonicalTurnResult("succeeded", "result")
            )

        assert (
            rejected.value.code
            is module.RuntimeErrorCode.TURN_EXECUTION_NOT_STARTED
        )
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "result_factory",
    [
        pytest.param(
            lambda module: module.CanonicalTurnResult("unknown", "result"),
            id="status",
        ),
        pytest.param(
            lambda module: module.CanonicalTurnResult("succeeded", ""),
            id="empty-result",
        ),
        pytest.param(lambda module: {"status": "succeeded"}, id="mapping"),
    ],
)
def test_commit_rejects_malformed_results_without_writes(tmp_path, result_factory):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-invalid-result.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(claim, result_factory(module))

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "field_name",
    [
        "project_id",
        "turn_id",
        "sequence",
        "worker_id",
        "attempt_id",
        "lease_generation",
        "fencing_token",
        "lease_expires_at",
        "canonical_session_id",
    ],
)
def test_commit_rejects_each_forged_claim_field_without_writes(
    tmp_path, field_name
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-forged-{field_name}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        value = getattr(claim, field_name)
        forged_value = f"{value}-forged" if type(value) is str else value + 1
        forged = replace(claim, **{field_name: forged_value})
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                forged, module.CanonicalTurnResult("succeeded", "result")
            )

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("turn_status", "control_state"),
    [
        pytest.param("stop_requested", "stop_requested", id="stop-requested"),
        pytest.param("awaiting_approval", "running", id="approval"),
        pytest.param("reconciling", "running", id="reconciling"),
    ],
)
def test_commit_rejects_nonclaimed_statuses_without_writes(
    tmp_path, turn_status, control_state
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"commit-{turn_status}.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        conn.execute(
            "UPDATE project_turns SET status = ? WHERE turn_id = ?",
            (turn_status, turn.turn_id),
        )
        conn.execute(
            "UPDATE project_run_controls SET control_state = ? WHERE turn_id = ?",
            (control_state, turn.turn_id),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.commit_turn(
                claim, module.CanonicalTurnResult("succeeded", "result")
            )

        assert rejected.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_commit_at_expiry_is_stale_but_old_observed_heartbeat_horizon_is_valid(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-expiry.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = 110
        renewed = runtime.heartbeat_turn(claim, lease_seconds=50)
        runtime.mark_turn_started(claim)
        committed = runtime.commit_turn(
            claim, module.CanonicalTurnResult("succeeded", "old-horizon-result")
        )
        assert committed.status == "succeeded"

        second = runtime.enqueue_turn(
            project_id,
            {"message": "expired"},
            actor,
            idempotency_key="expired",
            expected_version=3,
        )
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        now[0] = second_claim.lease_expires_at
        before = _claim_snapshot(conn, project_id, second.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as stale:
            runtime.commit_turn(
                second_claim,
                module.CanonicalTurnResult("failed", "expired-result"),
            )

        assert stale.value.code is module.RuntimeErrorCode.STALE_TURN_CLAIM
        assert _claim_snapshot(conn, project_id, second.turn_id) == before
        assert renewed.lease_expires_at == 160
    finally:
        conn.close()


def test_duplicate_terminal_result_id_rolls_back_the_second_commit(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "duplicate-result.db"
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, project_id, actor, key="first"
        )
        runtime.mark_turn_started(first_claim)
        runtime.commit_turn(
            first_claim,
            module.CanonicalTurnResult("succeeded", "shared-result"),
        )
        second = runtime.enqueue_turn(
            project_id,
            {"message": "second"},
            actor,
            idempotency_key="second",
            expected_version=3,
        )
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        before = _claim_snapshot(conn, project_id, second.turn_id)

        with pytest.raises(sqlite3.IntegrityError):
            runtime.commit_turn(
                second_claim,
                module.CanonicalTurnResult("succeeded", "shared-result"),
            )

        assert _claim_snapshot(conn, project_id, second.turn_id) == before
        assert first.turn_id != second.turn_id
    finally:
        conn.close()


def test_terminal_event_conflict_rolls_back_result_control_lease_and_version(
    tmp_path,
):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "commit-event-conflict.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        event_id = conn.execute(
            """
            SELECT event_id FROM project_events
            WHERE project_id = ? ORDER BY sequence LIMIT 1
            """,
            (project_id,),
        ).fetchone()[0]
        conflict_runtime = module.ProjectRuntime(
            conn,
            clock=lambda: 100,
            id_factory=lambda kind: event_id if kind == "event" else "unused",
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(sqlite3.IntegrityError):
            conflict_runtime.commit_turn(
                claim, module.CanonicalTurnResult("failed", "result")
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_requeues_expired_not_started_claim_without_readback(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-not-started.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1
        assert recovered[0].turn_id == turn.turn_id
        assert recovered[0].status == "queued"
        assert port.calls == []
        row = conn.execute(
            """
            SELECT status, attempt_id, lease_generation, fencing_token,
                   execution_state
            FROM project_turns WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()
        control = conn.execute(
            """
            SELECT control_state, control_version, attempt_id,
                   claim_worker_id, claim_lease_expires_at,
                   claim_canonical_session_id
            FROM project_run_controls WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()
        assert tuple(row) == ("queued", None, 1, 1, None)
        assert tuple(control) == ("running", before[1][3] + 2, None, None, None, None)
        assert prdb.runtime_state_for_project(conn, project_id).version == (
            before[3].version + 2
        )
        assert [
            row[0]
            for row in conn.execute(
                """
                SELECT kind FROM project_events
                WHERE project_id = ? ORDER BY sequence DESC LIMIT 2
                """,
                (project_id,),
            ).fetchall()[::-1]
        ] == ["turn.reconciling", "turn.requeued"]
        replacement = runtime.claim_next_turn(
            project_id, "worker-b", lease_seconds=30
        )
        assert replacement.turn_id == claim.turn_id
        assert replacement.attempt_id != claim.attempt_id
        assert replacement.lease_generation == claim.lease_generation + 1
        assert replacement.fencing_token == claim.fencing_token + 1
    finally:
        conn.close()


def test_recovery_stops_expired_not_started_stop_request_without_readback(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-stop.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == "stopped"
        assert port.calls == []
        assert conn.execute(
            "SELECT control_state FROM project_run_controls WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "stopped"
        assert [
            row[0]
            for row in conn.execute(
                """
                SELECT kind FROM project_events
                WHERE project_id = ? ORDER BY sequence DESC LIMIT 2
                """,
                (project_id,),
            ).fetchall()[::-1]
        ] == ["turn.reconciling", "run.stopped"]
    finally:
        conn.close()


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("execution_state", ["started", None])
def test_recovery_readback_terminalizes_started_and_legacy_attempts(
    tmp_path, execution_state, outcome
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"recover-{execution_state}-{outcome}.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        if execution_state == "started":
            runtime.mark_turn_started(claim)
        else:
            conn.execute(
                "UPDATE project_turns SET execution_state = NULL WHERE turn_id = ?",
                (turn.turn_id,),
            )
            conn.commit()
        port = _RecordingReadback(
            conn,
            module.TurnReadbackResult(outcome, f"result-{outcome}"),
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == outcome
        assert len(port.calls) == 1
        request = port.calls[0]
        assert request == module.TurnReadbackRequest(
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            sequence=claim.sequence,
            worker_id=claim.worker_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            lease_expires_at=claim.lease_expires_at,
            canonical_session_id=claim.canonical_session_id,
            source_status="claimed",
            execution_state=execution_state,
        )
        assert conn.execute(
            "SELECT terminal_result_id FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == f"result-{outcome}"
    finally:
        conn.close()


def test_recovery_legacy_stopped_readback_for_started_stop_fails_closed_once(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-stopped-proof.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("stopped")
        )
        now[0] = claim.lease_expires_at

        first = runtime.reconcile_inflight_turns(port, limit=10)
        second = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(first) == 1 and first[0].status == "reconciling"
        assert second == ()
        assert len(port.calls) == 1
        assert port.calls[0].source_status == "stop_requested"
        block_key = conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0]
        assert block_key is not None
        assert conn.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.parametrize(
    "port_factory",
    [
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            id="unknown",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("succeeded")
            ),
            id="missing-result",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("unknown", "extra")
            ),
            id="extra-result",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(conn, object()),
            id="wrong-type",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, error=RuntimeError("private readback detail")
            ),
            id="exception",
        ),
        pytest.param(
            lambda module, conn: _RecordingReadback(
                conn, module.TurnReadbackResult("stopped")
            ),
            id="source-outcome-mismatch",
        ),
    ],
)
def test_unknown_malformed_and_illegal_readback_blocks_once_per_attempt(
    tmp_path, port_factory
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-block.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        port = port_factory(module, conn)
        now[0] = claim.lease_expires_at

        first = runtime.reconcile_inflight_turns(port, limit=10)
        snapshot = _claim_snapshot(conn, project_id, turn.turn_id)
        second = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(first) == 1 and first[0].status == "reconciling"
        assert second == ()
        assert len(port.calls) == 1
        assert _claim_snapshot(conn, project_id, turn.turn_id) == snapshot
        events = conn.execute(
            """
            SELECT event_id, payload_json FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert len(events) == 1
        assert "private readback detail" not in events[0]["payload_json"]
        assert conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0] == events[0]["event_id"]
    finally:
        conn.close()


def test_recovery_block_event_identity_allows_a_later_attempt_to_block(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-block-attempt-scope.db", clock=lambda: now[0]
    )
    try:
        turn, first_claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(first_claim)
        first_port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = first_claim.lease_expires_at
        runtime.reconcile_inflight_turns(first_port, limit=10)
        first_key = conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0]

        conn.execute(
            """
            UPDATE project_turns
            SET status = 'queued', attempt_id = NULL,
                execution_state = NULL, recovery_block_key = NULL
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        )
        conn.execute(
            """
            UPDATE project_run_controls
            SET control_state = 'running', attempt_id = NULL,
                claim_worker_id = NULL, claim_lease_expires_at = NULL,
                claim_canonical_session_id = NULL
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        )
        conn.commit()
        second_claim = runtime.claim_next_turn(
            project_id, "worker-two", lease_seconds=30
        )
        runtime.mark_turn_started(second_claim)
        second_port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = second_claim.lease_expires_at

        runtime.reconcile_inflight_turns(second_port, limit=10)

        events = conn.execute(
            """
            SELECT event_id FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            ORDER BY sequence
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert len(events) == 2
        assert events[0]["event_id"] != events[1]["event_id"]
        second_key = conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE turn_id = ?
            """,
            (turn.turn_id,),
        ).fetchone()[0]
        assert first_key == events[0]["event_id"]
        assert second_key == events[1]["event_id"]
        assert second_key != first_key
    finally:
        conn.close()


def test_recovery_block_key_without_event_fails_closed(tmp_path):
    path = tmp_path / "recover-key-without-event.db"
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    now[0] = claim.lease_expires_at
    selected = prdb._recovery_candidates(conn, now=now[0], limit=1)[0]
    candidate = runtime._park_recovery_candidate(
        selected, now=now[0]
    )
    assert candidate is not None
    block_key = prdb._recovery_block_key(
        project_id=project_id,
        turn_id=turn.turn_id,
        attempt_id=claim.attempt_id,
        lease_generation=claim.lease_generation,
        fencing_token=claim.fencing_token,
    )
    conn.close()
    corrupt = sqlite3.connect(path)
    try:
        corrupt.execute(
            """
            UPDATE project_turns SET recovery_block_key = ?
            WHERE turn_id = ?
            """,
            (block_key, turn.turn_id),
        )
        corrupt.commit()
    finally:
        corrupt.close()

    check = projects_db.connect(path)
    try:
        persisted = prdb._runtime_turn_for_project(
            check, project_id=project_id, turn_id=turn.turn_id
        )
        with pytest.raises(RuntimeError, match="block"):
            prdb._validate_runtime_turn_pair(check, turn=persisted)
    finally:
        check.close()


def test_recovery_block_event_without_key_fails_closed(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-event-without-key.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        now[0] = claim.lease_expires_at
        selected = prdb._recovery_candidates(
            conn, now=now[0], limit=1
        )[0]
        candidate = runtime._park_recovery_candidate(
            selected, now=now[0]
        )
        assert candidate is not None
        block_key = prdb._recovery_block_key(
            project_id=project_id,
            turn_id=turn.turn_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
        )
        with prdb.write_transaction(conn):
            prdb._append_runtime_event(
                conn,
                event_id=block_key,
                project_id=project_id,
                kind="turn.recovery_blocked",
                turn_id=turn.turn_id,
                payload_json=module.canonical_json_object(
                    {
                        "attempt_id": claim.attempt_id,
                        "fencing_token": claim.fencing_token,
                        "lease_generation": claim.lease_generation,
                        "source_status": "claimed",
                        "turn_id": turn.turn_id,
                        "version": 3,
                    }
                ),
                created_at=now[0],
            )
        persisted = prdb._runtime_turn_for_project(
            conn, project_id=project_id, turn_id=turn.turn_id
        )

        with pytest.raises(RuntimeError, match="block"):
            prdb._validate_runtime_turn_pair(conn, turn=persisted)
    finally:
        conn.close()


@pytest.mark.parametrize("boolean_field", ["lease_generation", "fencing_token"])
def test_recovery_block_rejects_boolean_event_identity_without_writes(
    tmp_path, boolean_field
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"recover-boolean-{boolean_field}.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        now[0] = claim.lease_expires_at
        selected = prdb._recovery_candidates(
            conn, now=now[0], limit=1
        )[0]
        candidate = runtime._park_recovery_candidate(
            selected, now=now[0]
        )
        assert candidate is not None
        block_key = prdb._recovery_block_key(
            project_id=project_id,
            turn_id=turn.turn_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
        )
        payload = {
            "attempt_id": claim.attempt_id,
            "fencing_token": claim.fencing_token,
            "lease_generation": claim.lease_generation,
            "source_status": "claimed",
            "turn_id": turn.turn_id,
            "version": 3,
        }
        payload[boolean_field] = True

        with pytest.raises(RuntimeError, match="block event payload"):
            with prdb.write_transaction(conn):
                prdb._append_runtime_event(
                    conn,
                    event_id=block_key,
                    project_id=project_id,
                    kind="turn.recovery_blocked",
                    turn_id=turn.turn_id,
                    payload_json=module.canonical_json_object(payload),
                    created_at=now[0],
                )
                prdb._set_recovery_block_key(
                    conn,
                    candidate=candidate,
                    block_key=block_key,
                )

        assert conn.execute(
            "SELECT COUNT(*) FROM project_events WHERE event_id = ?",
            (block_key,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT recovery_block_key FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] is None
    finally:
        conn.close()


def test_awaiting_approval_expiry_is_inert_for_task5_recovery(tmp_path):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-approval-inert.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        conn.execute(
            "UPDATE project_turns SET status = 'awaiting_approval' WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        now[0] = claim.lease_expires_at

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert recovered == ()
        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_inactive_not_started_claim_blocks_but_terminal_proof_can_close(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-inactive.db", clock=lambda: now[0]
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, project_id, actor, key="inactive-not-started"
        )
        state = prdb.runtime_state_for_project(conn, project_id)
        with prdb.write_transaction(conn):
            state = prdb.transition_lifecycle(
                conn,
                project_id=project_id,
                expected_version=state.version,
                lifecycle="awaiting_acceptance",
                updated_at=101,
            )
        now[0] = first_claim.lease_expires_at
        no_call = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "unused")
        )

        blocked = runtime.reconcile_inflight_turns(no_call, limit=10)

        assert blocked[0].status == "reconciling"
        assert no_call.calls == []

        second_project = projects_db.create_project(conn, name="Inactive proof")
        prdb.create_project_conversation(
            conn,
            project_id=second_project,
            conversation_id="second-root",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="second-owner",
            project_id=second_project,
            surface="desktop",
            external_binding_id="second-window",
            actor_id="owner",
            now=1,
        )
        second_actor = ActorContext("owner", "desktop", "second-owner", True)
        second_runtime = module.ProjectRuntime(conn, clock=lambda: now[0])
        now[0] = 200
        second = second_runtime.enqueue_turn(
            second_project,
            {"message": "terminal proof"},
            second_actor,
            idempotency_key="terminal-proof",
            expected_version=0,
        )
        second_claim = second_runtime.claim_next_turn(
            second_project, "worker-two", lease_seconds=30
        )
        second_runtime.mark_turn_started(second_claim)
        second_state = prdb.runtime_state_for_project(conn, second_project)
        with prdb.write_transaction(conn):
            prdb.transition_lifecycle(
                conn,
                project_id=second_project,
                expected_version=second_state.version,
                lifecycle="awaiting_acceptance",
                updated_at=201,
            )
        proof = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "inactive-result")
        )
        now[0] = second_claim.lease_expires_at

        closed = second_runtime.reconcile_inflight_turns(proof, limit=10)

        assert closed[0].turn_id == second.turn_id
        assert closed[0].status == "succeeded"
    finally:
        conn.close()


def test_preexisting_reconciling_attempt_resumes_without_a_lease(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-preexisting.db"
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        conn.execute(
            "UPDATE project_turns SET status = 'reconciling' WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.execute(
            "DELETE FROM project_worker_leases WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.commit()
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("failed", "resumed-result")
        )

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(recovered) == 1 and recovered[0].status == "failed"
        assert len(port.calls) == 1
    finally:
        conn.close()


def test_claimed_attempt_without_lease_is_not_inferred_as_recoverable(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-orphan-claim.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        conn.execute(
            "DELETE FROM project_worker_leases WHERE turn_id = ?",
            (turn.turn_id,),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "must-not-read")
        )

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert recovered == ()
        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_rejects_selected_lease_pair_mismatch_without_writes(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-malformed-pair.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        conn.execute(
            """
            UPDATE project_run_controls SET claim_worker_id = 'forged-worker'
            WHERE project_id = ? AND turn_id = ?
            """,
            (project_id, turn.turn_id),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("succeeded", "must-not-read")
        )

        with pytest.raises(
            RuntimeError,
            match="turn/control/lease pair is inconsistent",
        ):
            runtime.reconcile_inflight_turns(port, limit=10)

        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize("limit", [True, 0, 101, 1.5])
def test_recovery_rejects_invalid_limit_before_port_or_write(tmp_path, limit):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / f"recover-limit-{limit}.db"
    )
    try:
        turn, _ = _enqueue_and_claim(runtime, project_id, actor)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.reconcile_inflight_turns(port, limit=limit)

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_rejects_outer_transaction_before_port_or_write(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-outer-transaction.db"
    )
    try:
        turn, _ = _enqueue_and_claim(runtime, project_id, actor)
        port = _RecordingReadback(
            conn, module.TurnReadbackResult("unknown")
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with prdb.write_transaction(conn):
            with pytest.raises(module.ProjectRuntimeError) as rejected:
                runtime.reconcile_inflight_turns(port, limit=10)
            assert (
                rejected.value.code
                is module.RuntimeErrorCode.INVALID_ARGUMENT
            )

        assert port.calls == []
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_two_terminal_reconcilers_commit_one_canonical_event(tmp_path):
    path = tmp_path / "recover-race.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    now[0] = claim.lease_expires_at
    bootstrap.close()
    barrier = threading.Barrier(2)

    def reconcile(readback_result):
        conn = projects_db.connect(path)
        try:
            port = _RecordingReadback(
                conn,
                module.TurnReadbackResult(*readback_result),
                barrier=barrier,
            )
            result = module.ProjectRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(port, limit=10)
            return result, len(port.calls)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(reconcile, ("succeeded", "race-success")),
            pool.submit(reconcile, ("failed", "race-failure")),
        ]
        results = [future.result(timeout=15) for future in futures]

    check = projects_db.connect(path)
    try:
        assert sum(calls for _, calls in results) == 2
        returned_statuses = {
            result[0].status for result, _ in results
        }
        assert len(returned_statuses) == 1
        phase_b = check.execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind IN ('turn.succeeded', 'turn.failed')
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert len(phase_b) == 1
        expected_event = {
            "succeeded": "turn.succeeded",
            "failed": "turn.failed",
        }[returned_statuses.pop()]
        assert phase_b[0]["kind"] == expected_event
    finally:
        check.close()


@pytest.mark.parametrize(
    ("source_status", "late_result"),
    [
        pytest.param(
            "claimed",
            ("succeeded", "late-success"),
            id="late-succeeded",
        ),
        pytest.param(
            "claimed",
            ("failed", "late-failure"),
            id="late-failed",
        ),
        pytest.param(
            "stop_requested",
            ("stopped", None),
            id="late-stopped",
        ),
    ],
)
def test_recovery_block_fences_late_mixed_readback_outcome(
    tmp_path, source_status, late_result
):
    path = tmp_path / f"recover-block-wins-{late_result[0]}.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    if source_status == "stop_requested":
        runtime.request_stop(
            project_id,
            turn.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
    version_before = prdb.runtime_state_for_project(
        bootstrap, project_id
    ).version
    now[0] = claim.lease_expires_at
    bootstrap.close()
    both_reading = threading.Barrier(2)
    release_late = threading.Event()

    def reconcile(result, *, wait_for_block):
        conn = projects_db.connect(path)
        try:
            class _OrderedReadback:
                def read_turn(self, request):
                    assert conn.in_transaction is False
                    both_reading.wait(timeout=5)
                    if wait_for_block:
                        assert release_late.wait(timeout=10)
                    return module.TurnReadbackResult(*result)

            return module.ProjectRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(_OrderedReadback(), limit=10)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        blocked_future = pool.submit(
            reconcile, ("unknown", None), wait_for_block=False
        )
        late_future = pool.submit(
            reconcile, late_result, wait_for_block=True
        )
        blocked = blocked_future.result(timeout=15)
        release_late.set()
        late = late_future.result(timeout=15)

    check = projects_db.connect(path)
    try:
        assert blocked[0].status == "reconciling"
        assert late[0].status == "reconciling"
        assert check.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "reconciling"
        phase_b_events = check.execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind IN (
                  'turn.recovery_blocked', 'turn.requeued',
                  'turn.succeeded', 'turn.failed', 'run.stopped'
              )
            ORDER BY sequence
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert [row["kind"] for row in phase_b_events] == [
            "turn.recovery_blocked"
        ]
        assert (
            prdb.runtime_state_for_project(check, project_id).version
            == version_before + 2
        )
    finally:
        check.close()


def test_terminal_recovery_winner_makes_late_block_write_free(tmp_path):
    path = tmp_path / "recover-terminal-wins-block.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    version_before = prdb.runtime_state_for_project(
        bootstrap, project_id
    ).version
    now[0] = claim.lease_expires_at
    bootstrap.close()
    both_reading = threading.Barrier(2)
    release_unknown = threading.Event()

    def reconcile(result, *, wait):
        conn = projects_db.connect(path)
        try:
            class _OrderedReadback:
                def read_turn(self, request):
                    assert conn.in_transaction is False
                    both_reading.wait(timeout=5)
                    if wait:
                        assert release_unknown.wait(timeout=10)
                    return result

            return module.ProjectRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(_OrderedReadback(), limit=10)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        terminal_future = pool.submit(
            reconcile,
            module.TurnReadbackResult("succeeded", "winner"),
            wait=False,
        )
        unknown_future = pool.submit(
            reconcile,
            module.TurnReadbackResult("unknown"),
            wait=True,
        )
        terminal = terminal_future.result(timeout=15)
        release_unknown.set()
        late = unknown_future.result(timeout=15)

    check = projects_db.connect(path)
    try:
        assert terminal[0].status == "succeeded"
        assert late[0].status == "succeeded"
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 0
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.succeeded'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 1
        assert (
            prdb.runtime_state_for_project(check, project_id).version
            == version_before + 2
        )
    finally:
        check.close()


def test_recovery_outcome_sql_cas_rejects_blocked_attempt(tmp_path):
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recovery-outcome-block-cas.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        now[0] = claim.lease_expires_at
        selected = prdb._recovery_candidates(
            conn, now=now[0], limit=1
        )[0]
        candidate = runtime._park_recovery_candidate(
            selected, now=now[0]
        )
        assert candidate is not None
        runtime._block_recovery(candidate, now=now[0])
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with prdb.write_transaction(conn):
            updated = prdb._apply_recovery_outcome(
                conn,
                candidate=candidate,
                outcome="succeeded",
                terminal_result_id="must-not-commit",
                now=now[0],
            )

        assert updated is None
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_requeue_sql_cas_requires_current_active_lifecycle(tmp_path):
    now = [100]
    _, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "requeue-lifecycle-cas.db",
        clock=lambda: now[0],
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        selected = prdb._recovery_candidates(
            conn, now=now[0], limit=1
        )[0]
        candidate = runtime._park_recovery_candidate(
            selected, now=now[0]
        )
        assert candidate is not None
        state = prdb.runtime_state_for_project(conn, project_id)
        with prdb.write_transaction(conn):
            prdb.transition_lifecycle(
                conn,
                project_id=project_id,
                expected_version=state.version,
                lifecycle="awaiting_acceptance",
                updated_at=now[0],
            )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with prdb.write_transaction(conn):
            updated = prdb._apply_recovery_outcome(
                conn,
                candidate=candidate,
                outcome="queued",
                terminal_result_id=None,
                now=now[0],
            )

        assert updated is None
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "next_lifecycle", ["awaiting_acceptance", "completed"]
)
def test_phase_b_revalidates_current_lifecycle_before_requeue(
    tmp_path, next_lifecycle
):
    path = tmp_path / f"recover-current-{next_lifecycle}.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    now[0] = claim.lease_expires_at
    bootstrap.close()
    finalize_entered = threading.Event()
    release_finalize = threading.Event()

    class _DelayedFinalizeRuntime(module.ProjectRuntime):
        def _finalize_recovery(self, *args, **kwargs):
            finalize_entered.set()
            assert release_finalize.wait(timeout=10)
            return super()._finalize_recovery(*args, **kwargs)

    def reconcile():
        conn = projects_db.connect(path)
        try:
            return _DelayedFinalizeRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(
                _RecordingReadback(
                    conn, module.TurnReadbackResult("unknown")
                ),
                limit=10,
            )
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(reconcile)
        assert finalize_entered.wait(timeout=10)
        lifecycle_conn = projects_db.connect(path)
        try:
            state = prdb.runtime_state_for_project(
                lifecycle_conn, project_id
            )
            with prdb.write_transaction(lifecycle_conn):
                state = prdb.transition_lifecycle(
                    lifecycle_conn,
                    project_id=project_id,
                    expected_version=state.version,
                    lifecycle="awaiting_acceptance",
                    updated_at=now[0],
                )
            if next_lifecycle == "completed":
                with prdb.write_transaction(lifecycle_conn):
                    state = prdb.transition_lifecycle(
                        lifecycle_conn,
                        project_id=project_id,
                        expected_version=state.version,
                        lifecycle="completed",
                        updated_at=now[0],
                    )
        finally:
            lifecycle_conn.close()
        release_finalize.set()
        recovered = future.result(timeout=15)

    check = projects_db.connect(path)
    try:
        assert recovered[0].status == "reconciling"
        assert check.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "reconciling"
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 1
        assert check.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.requeued'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 0
    finally:
        check.close()


def test_lifecycle_block_fences_late_requeue_after_phase_a(tmp_path):
    path = tmp_path / "recover-lifecycle-block-wins-requeue.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    turn, claim = _enqueue_and_claim(runtime, project_id, actor)
    now[0] = claim.lease_expires_at
    bootstrap.close()
    finalize_entered = threading.Event()
    release_finalize = threading.Event()

    class _DelayedFinalizeRuntime(module.ProjectRuntime):
        def _finalize_recovery(self, *args, **kwargs):
            finalize_entered.set()
            assert release_finalize.wait(timeout=10)
            return super()._finalize_recovery(*args, **kwargs)

    def late_requeue():
        conn = projects_db.connect(path)
        try:
            return _DelayedFinalizeRuntime(
                conn, clock=lambda: now[0]
            ).reconcile_inflight_turns(
                _RecordingReadback(
                    conn, module.TurnReadbackResult("unknown")
                ),
                limit=10,
            )
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(late_requeue)
        assert finalize_entered.wait(timeout=10)
        blocker_conn = projects_db.connect(path)
        try:
            state = prdb.runtime_state_for_project(
                blocker_conn, project_id
            )
            with prdb.write_transaction(blocker_conn):
                prdb.transition_lifecycle(
                    blocker_conn,
                    project_id=project_id,
                    expected_version=state.version,
                    lifecycle="awaiting_acceptance",
                    updated_at=now[0],
                )
            blocker = module.ProjectRuntime(
                blocker_conn, clock=lambda: now[0]
            )
            blocked = blocker.reconcile_inflight_turns(
                _RecordingReadback(
                    blocker_conn,
                    module.TurnReadbackResult("unknown"),
                ),
                limit=10,
            )
        finally:
            blocker_conn.close()
        release_finalize.set()
        late = future.result(timeout=15)

    check = projects_db.connect(path)
    try:
        assert blocked[0].status == "reconciling"
        assert late[0].status == "reconciling"
        assert check.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "reconciling"
        phase_b_events = check.execute(
            """
            SELECT kind FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind IN ('turn.recovery_blocked', 'turn.requeued')
            ORDER BY sequence
            """,
            (project_id, turn.turn_id),
        ).fetchall()
        assert [row["kind"] for row in phase_b_events] == [
            "turn.recovery_blocked"
        ]
    finally:
        check.close()


@pytest.mark.parametrize(
    ("source_status", "readback_result", "expected_status"),
    [
        pytest.param(
            "claimed",
            ("succeeded", "success-after-lifecycle"),
            "succeeded",
            id="succeeded",
        ),
        pytest.param(
            "claimed",
            ("failed", "failure-after-lifecycle"),
            "failed",
            id="failed",
        ),
        pytest.param(
            "stop_requested",
            ("stopped", None),
            "stopped",
            id="stopped",
        ),
    ],
)
def test_proven_terminal_recovery_can_close_after_phase_b_lifecycle_change(
    tmp_path, source_status, readback_result, expected_status
):
    path = tmp_path / f"recover-terminal-inactive-{expected_status}.db"
    now = [100]
    module, bootstrap, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    _, claim = _enqueue_and_claim(runtime, project_id, actor)
    runtime.mark_turn_started(claim)
    if source_status == "stop_requested":
        runtime.request_stop(
            project_id,
            claim.turn_id,
            actor,
            idempotency_key="stop",
            expected_version=2,
            expected_control_version=1,
        )
    now[0] = claim.lease_expires_at
    bootstrap.close()
    finalize_entered = threading.Event()
    release_finalize = threading.Event()

    class _DelayedFinalizeRuntime(module.ProjectRuntime):
        def _finalize_recovery(self, *args, **kwargs):
            finalize_entered.set()
            assert release_finalize.wait(timeout=10)
            return super()._finalize_recovery(*args, **kwargs)

    def reconcile():
        conn = projects_db.connect(path)
        try:
            delayed_runtime = _DelayedFinalizeRuntime(
                conn, clock=lambda: now[0]
            )
            if source_status == "stop_requested":
                class _StoppedTask7Evidence:
                    def read_turn_with_evidence(self, request):
                        assert conn.in_transaction is False
                        assert request.source_status == "stop_requested"
                        return module.Task7TerminalReadbackEvidence(
                            module.TurnReadbackResult(*readback_result),
                            None,
                        )

                return (
                    delayed_runtime
                    .reconcile_inflight_turns_with_task7_evidence(
                        _StoppedTask7Evidence(),
                        limit=10,
                    )
                )
            return delayed_runtime.reconcile_inflight_turns(
                _RecordingReadback(
                    conn, module.TurnReadbackResult(*readback_result)
                ),
                limit=10,
            )
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(reconcile)
        assert finalize_entered.wait(timeout=10)
        lifecycle_conn = projects_db.connect(path)
        try:
            state = prdb.runtime_state_for_project(
                lifecycle_conn, project_id
            )
            with prdb.write_transaction(lifecycle_conn):
                prdb.transition_lifecycle(
                    lifecycle_conn,
                    project_id=project_id,
                    expected_version=state.version,
                    lifecycle="awaiting_acceptance",
                    updated_at=now[0],
                )
        finally:
            lifecycle_conn.close()
        release_finalize.set()
        recovered = future.result(timeout=15)

    assert recovered[0].status == expected_status


class _OutcomeEnum(str, Enum):
    SUCCEEDED = "succeeded"


class _ExplosiveOutcome(str):
    def __hash__(self):
        raise AssertionError("outcome hash executed")

    def __eq__(self, other):
        raise AssertionError("outcome equality executed")


@pytest.mark.parametrize(
    "impostor_factory",
    [
        pytest.param(lambda: _OutcomeEnum.SUCCEEDED, id="str-enum"),
        pytest.param(
            lambda: _ExplosiveOutcome("succeeded"),
            id="side-effect-str-subclass",
        ),
    ],
)
def test_readback_outcome_requires_exact_string_and_blocks_impostors(
    tmp_path, impostor_factory
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-outcome-impostor.db", clock=lambda: now[0]
    )
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        runtime.mark_turn_started(claim)
        now[0] = claim.lease_expires_at
        port = _RecordingReadback(
            conn,
            module.TurnReadbackResult(
                impostor_factory(), "must-not-terminalize"
            ),
        )

        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert recovered[0].status == "reconciling"
        assert conn.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND turn_id = ?
              AND kind = 'turn.recovery_blocked'
            """,
            (project_id, turn.turn_id),
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_recovery_takeover_rotates_fence_and_stale_worker_cannot_write(
    tmp_path,
):
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "recover-takeover-fence.db", clock=lambda: now[0]
    )
    try:
        turn, stale_claim = _enqueue_and_claim(
            runtime, project_id, actor
        )
        now[0] = stale_claim.lease_expires_at
        runtime.reconcile_inflight_turns(
            _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            limit=10,
        )
        current_claim = runtime.claim_next_turn(
            project_id, "worker-current", lease_seconds=30
        )
        assert current_claim is not None
        assert current_claim.turn_id == turn.turn_id
        assert current_claim.attempt_id != stale_claim.attempt_id
        assert (
            current_claim.lease_generation
            == stale_claim.lease_generation + 1
        )
        assert current_claim.fencing_token == stale_claim.fencing_token + 1
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        stale_calls = (
            lambda: runtime.heartbeat_turn(
                stale_claim, lease_seconds=30
            ),
            lambda: runtime.mark_turn_started(stale_claim),
            lambda: runtime.commit_turn(
                stale_claim,
                module.CanonicalTurnResult("succeeded", "stale-result"),
            ),
            lambda: runtime.acknowledge_stopped(stale_claim),
        )
        for call in stale_calls:
            with pytest.raises(module.ProjectRuntimeError):
                call()
            assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_fresh_process_claim_crash_requeues_and_fences_live_stale_writer(
    tmp_path,
):
    path = tmp_path / "recover-process-takeover.db"
    _, conn, runtime, project_id, actor = _make_runtime(path)
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "cross-process"},
            actor,
            idempotency_key="cross-process",
            expected_version=0,
        )
    finally:
        conn.close()

    with _ProbeSet() as probes:
        crashed = _run_probe(
            probes,
            _probe_prepare(
                "claim-crash-a",
                "claim",
                path,
                project_id,
                "process-a",
                100,
                lease_seconds=30,
                crash_after="claim_commit",
            ),
            expected_event="boundary",
            returncode=91,
        )
        assert crashed["boundary"] == "claim_committed"
        stale_claim = crashed["claim"]
        assert stale_claim["turn_id"] == turn.turn_id

        check = projects_db.connect(path)
        try:
            claimed = check.execute(
                """
                SELECT status, execution_state
                FROM project_turns WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()
            assert tuple(claimed) == ("claimed", "not_started")
            assert check.execute(
                """
                SELECT COUNT(*) FROM project_worker_leases
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, turn.turn_id),
            ).fetchone()[0] == 1

            stale_writer = probes.spawn(
                _probe_prepare(
                    "stale-commit-a",
                    "commit",
                    path,
                    project_id,
                    stale_claim["worker_id"],
                    stale_claim["lease_expires_at"],
                    claim=stale_claim,
                    outcome="succeeded",
                    result_id="stale-result",
                )
            )
            stale_ready = stale_writer.expect("ready")
            assert stale_ready["stage"] == "before_begin_immediate"

            recovered = _run_probe(
                probes,
                _probe_prepare(
                    "recover-after-claim-crash",
                    "recover",
                    path,
                    project_id,
                    "recovery",
                    stale_claim["lease_expires_at"],
                    limit=10,
                ),
            )
            assert recovered["readback_requests"] == []
            assert recovered["turns"] == [
                {"status": "queued", "turn_id": turn.turn_id}
            ]

            takeover = _run_probe(
                probes,
                _probe_prepare(
                    "takeover-b",
                    "claim",
                    path,
                    project_id,
                    "process-b",
                    stale_claim["lease_expires_at"],
                    lease_seconds=30,
                ),
            )
            current_claim = takeover["claim"]
            assert current_claim["attempt_id"] != stale_claim["attempt_id"]
            assert (
                current_claim["lease_generation"]
                == stale_claim["lease_generation"] + 1
            )
            assert (
                current_claim["fencing_token"]
                == stale_claim["fencing_token"] + 1
            )
            before_stale = _claim_snapshot(
                check, project_id, turn.turn_id
            )

            stale_writer.send(
                {
                    "version": 1,
                    "event": "go",
                    "probe_id": stale_writer.probe_id,
                }
            )
            stale_result = stale_writer.expect("result")
            stale_writer.complete()
            assert stale_result["ok"] is False
            assert stale_result["error"] == {
                "code": "stale_turn_claim"
            }
            assert (
                _claim_snapshot(check, project_id, turn.turn_id)
                == before_stale
            )

            started = _run_probe(
                probes,
                _probe_prepare(
                    "start-b",
                    "start",
                    path,
                    project_id,
                    current_claim["worker_id"],
                    stale_claim["lease_expires_at"],
                    claim=current_claim,
                ),
            )
            assert started["claim"] == current_claim
            committed = _run_probe(
                probes,
                _probe_prepare(
                    "commit-b",
                    "commit",
                    path,
                    project_id,
                    current_claim["worker_id"],
                    stale_claim["lease_expires_at"],
                    claim=current_claim,
                    outcome="succeeded",
                    result_id="current-result",
                ),
            )
            assert committed["turn"] == {
                "status": "succeeded",
                "turn_id": turn.turn_id,
            }
            assert check.execute(
                """
                SELECT terminal_result_id FROM project_turns
                WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()[0] == "current-result"
            assert check.execute(
                """
                SELECT COUNT(*) FROM project_events
                WHERE turn_id = ? AND kind = 'turn.succeeded'
                  AND payload_json LIKE '%stale-result%'
                """,
                (turn.turn_id,),
            ).fetchone()[0] == 0
        finally:
            check.close()


def test_fresh_process_start_and_phase_a_crashes_recover_terminal(
    tmp_path,
):
    path = tmp_path / "recover-process-crash-boundaries.db"
    _, conn, runtime, project_id, actor = _make_runtime(path)
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "crash boundaries"},
            actor,
            idempotency_key="crash-boundaries",
            expected_version=0,
        )
    finally:
        conn.close()

    with _ProbeSet() as probes:
        claimed = _run_probe(
            probes,
            _probe_prepare(
                "claim-before-start-crash",
                "claim",
                path,
                project_id,
                "process-a",
                100,
                lease_seconds=30,
            ),
        )
        claim = claimed["claim"]
        started = _run_probe(
            probes,
            _probe_prepare(
                "start-crash",
                "start",
                path,
                project_id,
                claim["worker_id"],
                100,
                claim=claim,
                crash_after="start_commit",
            ),
            expected_event="boundary",
            returncode=92,
        )
        assert started["boundary"] == "start_committed"
        assert started["claim"] == claim

        check = projects_db.connect(path)
        try:
            assert check.execute(
                """
                SELECT execution_state FROM project_turns
                WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()[0] == "started"
        finally:
            check.close()

        parked = _run_probe(
            probes,
            _probe_prepare(
                "phase-a-crash",
                "recover",
                path,
                project_id,
                "recovery-a",
                claim["lease_expires_at"],
                limit=10,
                crash_after="phase_a_reconciling_commit",
            ),
            expected_event="boundary",
            returncode=93,
        )
        assert parked["boundary"] == "reconciling_committed"
        request = parked["request"]
        assert request["attempt_id"] == claim["attempt_id"]
        assert request["lease_generation"] == claim["lease_generation"]
        assert request["fencing_token"] == claim["fencing_token"]
        assert request["source_status"] == "claimed"
        assert request["execution_state"] == "started"

        check = projects_db.connect(path)
        try:
            assert check.execute(
                "SELECT status FROM project_turns WHERE turn_id = ?",
                (turn.turn_id,),
            ).fetchone()[0] == "reconciling"
            assert check.execute(
                """
                SELECT COUNT(*) FROM project_worker_leases
                WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()[0] == 0
            kinds = [
                row["kind"]
                for row in check.execute(
                    """
                    SELECT kind FROM project_events
                    WHERE turn_id = ?
                      AND kind IN (
                          'turn.reconciling',
                          'turn.recovery_blocked',
                          'turn.succeeded',
                          'turn.failed',
                          'run.stopped',
                          'turn.requeued'
                      )
                    ORDER BY sequence
                    """,
                    (turn.turn_id,),
                )
            ]
            assert kinds == ["turn.reconciling"]
        finally:
            check.close()

        recovered = _run_probe(
            probes,
            _probe_prepare(
                "recover-after-phase-a-crash",
                "recover",
                path,
                project_id,
                "recovery-b",
                claim["lease_expires_at"],
                limit=10,
                readback={
                    "outcome": "succeeded",
                    "result_id": "recovered-result",
                },
            ),
        )
        assert recovered["readback_requests"] == [request]
        assert recovered["turns"] == [
            {"status": "succeeded", "turn_id": turn.turn_id}
        ]

        check = projects_db.connect(path)
        try:
            terminal = check.execute(
                """
                SELECT status, terminal_result_id
                FROM project_turns WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()
            assert tuple(terminal) == ("succeeded", "recovered-result")
            assert [
                row["kind"]
                for row in check.execute(
                    """
                    SELECT kind FROM project_events
                    WHERE turn_id = ?
                      AND kind IN ('turn.reconciling', 'turn.succeeded')
                    ORDER BY sequence
                    """,
                    (turn.turn_id,),
                )
            ] == ["turn.reconciling", "turn.succeeded"]
        finally:
            check.close()


def test_fresh_process_claim_race_repeats_25_times_and_winner_commits(
    tmp_path,
):
    path = tmp_path / "recover-process-race-25.db"
    module, conn, runtime, first_project, first_actor = _make_runtime(path)
    try:
        with _ProbeSet() as probes:
            for iteration in range(25):
                if iteration == 0:
                    project_id = first_project
                    actor = first_actor
                    project_runtime = runtime
                else:
                    project_id = projects_db.create_project(
                        conn, name=f"Race {iteration}"
                    )
                    session_id = f"race-root-{iteration}"
                    binding_id = f"race-owner-{iteration}"
                    prdb.create_project_conversation(
                        conn,
                        project_id=project_id,
                        conversation_id=session_id,
                        current_phase="implementation",
                        now=1,
                    )
                    prdb.bind_surface(
                        conn,
                        binding_id=binding_id,
                        project_id=project_id,
                        surface="desktop",
                        external_binding_id=f"window-{iteration}",
                        actor_id="owner",
                        now=1,
                    )
                    actor = ActorContext(
                        "owner", "desktop", binding_id, True
                    )
                    project_runtime = module.ProjectRuntime(
                        conn, clock=lambda: 100
                    )
                turn = project_runtime.enqueue_turn(
                    project_id,
                    {"iteration": iteration},
                    actor,
                    idempotency_key=f"race-{iteration}",
                    expected_version=0,
                )
                workers = [
                    probes.spawn(
                        _probe_prepare(
                            f"race-{iteration}-{side}",
                            "claim",
                            path,
                            project_id,
                            f"worker-{side}-{iteration}",
                            100,
                            lease_seconds=30,
                        )
                    )
                    for side in ("a", "b")
                ]
                _release_probes(workers)
                results = [
                    worker.expect("result") for worker in workers
                ]
                for worker in workers:
                    worker.complete()
                claims = [
                    payload["claim"]
                    for payload in results
                    if payload["claim"] is not None
                ]
                assert len(claims) == 1
                claim = claims[0]

                started = _run_probe(
                    probes,
                    _probe_prepare(
                        f"race-{iteration}-start",
                        "start",
                        path,
                        project_id,
                        claim["worker_id"],
                        100,
                        claim=claim,
                    ),
                )
                assert started["claim"] == claim
                committed = _run_probe(
                    probes,
                    _probe_prepare(
                        f"race-{iteration}-commit",
                        "commit",
                        path,
                        project_id,
                        claim["worker_id"],
                        100,
                        claim=claim,
                        outcome="succeeded",
                        result_id=f"result-{iteration}",
                    ),
                )
                assert committed["turn"] == {
                    "status": "succeeded",
                    "turn_id": turn.turn_id,
                }
                terminal = conn.execute(
                    """
                    SELECT status, terminal_result_id
                    FROM project_turns
                    WHERE project_id = ? AND turn_id = ?
                    """,
                    (project_id, turn.turn_id),
                ).fetchone()
                assert tuple(terminal) == (
                    "succeeded",
                    f"result-{iteration}",
                )
                assert conn.execute(
                    """
                    SELECT COUNT(*) FROM project_worker_leases
                    WHERE project_id = ?
                    """,
                    (project_id,),
                ).fetchone()[0] == 0
                counts = {
                    row["kind"]: row["count"]
                    for row in conn.execute(
                        """
                        SELECT kind, COUNT(*) AS count
                        FROM project_events
                        WHERE project_id = ? AND turn_id = ?
                          AND kind IN ('turn.claimed', 'turn.succeeded')
                        GROUP BY kind
                        """,
                        (project_id, turn.turn_id),
                    )
                }
                assert counts == {
                    "turn.claimed": 1,
                    "turn.succeeded": 1,
                }
    finally:
        conn.close()


def test_claim_scan_is_set_based_and_bounded_by_the_fifo_head(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "bounded-claim-scan.db"
    )
    try:
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, origin_binding_id, status, attempt_id,
                    lease_generation, fencing_token, execution_state,
                    terminal_result_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, '{}', 'owner-binding', 'cancelled',
                          NULL, 0, 0, NULL, NULL, 1, 1)
                """,
                [
                    (
                        f"historical-{sequence}",
                        project_id,
                        sequence,
                        f"historical-{sequence}",
                    )
                    for sequence in range(1, 251)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    idempotency_key, command_fingerprint, attempt_id,
                    claim_worker_id, claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (?, ?, 'terminal', 0, NULL, NULL, NULL,
                          NULL, NULL, NULL, 1)
                """,
                [
                    (f"historical-{sequence}", project_id)
                    for sequence in range(1, 251)
                ],
            )
        target = runtime.enqueue_turn(
            project_id,
            {"message": "bounded"},
            actor,
            idempotency_key="bounded",
            expected_version=0,
        )
        statements = []
        conn.set_trace_callback(statements.append)
        try:
            claim = runtime.claim_next_turn(
                project_id, "bounded-worker", lease_seconds=30
            )
        finally:
            conn.set_trace_callback(None)

        assert claim is not None and claim.turn_id == target.turn_id
        normalized = [
            " ".join(statement.lower().split())
            for statement in statements
        ]
        turn_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_turns" in statement
        ]
        control_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_run_controls" in statement
        ]
        lease_selects = [
            statement
            for statement in normalized
            if statement.startswith("select")
            and " from project_worker_leases" in statement
        ]
        assert len(turn_selects) <= 4
        assert len(control_selects) <= 3
        assert len(lease_selects) <= 3
        assert all(
            "order by sequence, turn_id" not in statement
            or "limit 1" in statement
            for statement in turn_selects
        )
    finally:
        conn.close()


def test_recovery_parks_the_whole_batch_before_first_readback(tmp_path):
    now = [100]
    module, conn, runtime, first_project, first_actor = _make_runtime(
        tmp_path / "recover-whole-batch.db", clock=lambda: now[0]
    )
    try:
        first, first_claim = _enqueue_and_claim(
            runtime, first_project, first_actor, key="first"
        )
        runtime.mark_turn_started(first_claim)
        second_project = projects_db.create_project(conn, name="Second")
        prdb.create_project_conversation(
            conn,
            project_id=second_project,
            conversation_id="second-root",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="second-owner",
            project_id=second_project,
            surface="desktop",
            external_binding_id="second-window",
            actor_id="owner",
            now=1,
        )
        second_actor = ActorContext(
            "owner", "desktop", "second-owner", True
        )
        second_runtime = module.ProjectRuntime(
            conn, clock=lambda: now[0]
        )
        second = second_runtime.enqueue_turn(
            second_project,
            {"message": "second"},
            second_actor,
            idempotency_key="second",
            expected_version=0,
        )
        second_claim = second_runtime.claim_next_turn(
            second_project, "second-worker", lease_seconds=30
        )
        assert second_claim is not None
        second_runtime.mark_turn_started(second_claim)
        now[0] = first_claim.lease_expires_at

        class _BatchReadback:
            def __init__(self):
                self.calls = []

            def read_turn(self, request):
                assert conn.in_transaction is False
                if not self.calls:
                    assert {
                        row["status"]
                        for row in conn.execute(
                            """
                            SELECT status FROM project_turns
                            WHERE turn_id IN (?, ?)
                            """,
                            (first.turn_id, second.turn_id),
                        )
                    } == {"reconciling"}
                    assert conn.execute(
                        """
                        SELECT COUNT(*) FROM project_worker_leases
                        WHERE turn_id IN (?, ?)
                        """,
                        (first.turn_id, second.turn_id),
                    ).fetchone()[0] == 0
                self.calls.append(request)
                return module.TurnReadbackResult(
                    "succeeded", f"result-{request.turn_id}"
                )

        port = _BatchReadback()
        recovered = runtime.reconcile_inflight_turns(port, limit=10)

        assert len(port.calls) == 2
        assert {turn.status for turn in recovered} == {"succeeded"}
    finally:
        conn.close()


def test_claim_counter_overflow_is_rejected_without_writes(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "claim-counter-overflow.db"
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "overflow"},
            actor,
            idempotency_key="overflow",
            expected_version=0,
        )
        conn.execute(
            """
            UPDATE project_turns
            SET lease_generation = ?, fencing_token = ?
            WHERE turn_id = ?
            """,
            (module.SQLITE_INT_MAX, module.SQLITE_INT_MAX, turn.turn_id),
        )
        conn.commit()
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(RuntimeError, match="counter"):
            runtime.claim_next_turn(
                project_id, "overflow-worker", lease_seconds=30
            )

        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_claim_expiry_overflow_is_invalid_without_writes(tmp_path):
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "claim-expiry-overflow.db",
        clock=lambda: (1 << 63) - 1,
    )
    try:
        turn = runtime.enqueue_turn(
            project_id,
            {"message": "expiry overflow"},
            actor,
            idempotency_key="expiry-overflow",
            expected_version=0,
        )
        before = _claim_snapshot(conn, project_id, turn.turn_id)

        with pytest.raises(module.ProjectRuntimeError) as rejected:
            runtime.claim_next_turn(
                project_id, "overflow-worker", lease_seconds=1
            )

        assert rejected.value.code is module.RuntimeErrorCode.INVALID_ARGUMENT
        assert _claim_snapshot(conn, project_id, turn.turn_id) == before
    finally:
        conn.close()


def test_recovery_candidate_queries_are_bounded_and_indexed(tmp_path):
    _, conn, _, _, _ = _make_runtime(
        tmp_path / "recover-expiry-plan.db"
    )
    try:
        query_cases = (
            (
                prdb._RECOVERY_EXPIRED_LEASES_SQL,
                (100, 10),
                {"idx_project_worker_leases_expiry"},
            ),
            (
                prdb._RECOVERY_RECONCILING_SQL,
                (10,),
                {
                    "idx_project_turns_actionable_recovery",
                },
            ),
            (
                prdb._RECOVERY_BLOCK_LOOKUP_SQL,
                ("project", "turn", "attempt", 1, 1),
                {"idx_project_events_recovery_block_attempt"},
            ),
        )
        for sql, parameters, expected_indexes in query_cases:
            details = [
                row["detail"]
                for row in conn.execute(
                    f"EXPLAIN QUERY PLAN {sql}", parameters
                )
            ]

            assert all(
                expected in " ".join(details)
                for expected in expected_indexes
            )
            assert not any(
                "USE TEMP B-TREE" in detail for detail in details
            )
            assert not any(
                detail in {"SCAN turn", "SCAN event"}
                for detail in details
            )
    finally:
        conn.close()


def test_recovery_scan_work_does_not_grow_with_terminal_history(tmp_path):
    _, conn, _, project_id, _ = _make_runtime(
        tmp_path / "recover-history-plan.db"
    )
    try:
        def instruction_count():
            instructions = [0]

            def progress():
                instructions[0] += 1
                return 0

            conn.set_progress_handler(progress, 1)
            try:
                assert prdb._recovery_candidates(
                    conn, now=100, limit=10
                ) == ()
            finally:
                conn.set_progress_handler(None, 0)
            return instructions[0]

        conn.execute("ANALYZE")
        baseline = instruction_count()
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, origin_binding_id, status, attempt_id,
                    lease_generation, fencing_token, execution_state,
                    terminal_result_id, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, '{}', 'owner-binding', 'cancelled',
                    NULL, 0, 0, NULL, NULL, 1, 1
                )
                """,
                [
                    (
                        f"history-{sequence}",
                        project_id,
                        sequence,
                        f"history-{sequence}",
                    )
                    for sequence in range(1, 2001)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    idempotency_key, command_fingerprint, attempt_id,
                    claim_worker_id, claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (
                    ?, ?, 'terminal', 0, NULL, NULL, NULL,
                    NULL, NULL, NULL, 1
                )
                """,
                [
                    (f"history-{sequence}", project_id)
                    for sequence in range(1, 2001)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_events (
                    event_id, project_id, sequence, kind, turn_id,
                    payload_json, created_at
                ) VALUES (?, ?, ?, 'turn.history', ?, '{}', 1)
                """,
                [
                    (
                        f"history-event-{sequence}",
                        project_id,
                        sequence,
                        f"history-{sequence}",
                    )
                    for sequence in range(1, 2001)
                ],
            )
        conn.execute("ANALYZE")

        assert instruction_count() <= baseline + 200
    finally:
        conn.close()


def test_recovery_scan_work_does_not_grow_with_unexpired_claims(tmp_path):
    _, conn, _, _, _ = _make_runtime(
        tmp_path / "recover-unexpired-plan.db"
    )
    try:
        def instruction_count():
            instructions = [0]

            def progress():
                instructions[0] += 1
                return 0

            conn.set_progress_handler(progress, 1)
            try:
                assert prdb._recovery_candidates(
                    conn, now=100, limit=10
                ) == ()
            finally:
                conn.set_progress_handler(None, 0)
            return instructions[0]

        conn.execute("ANALYZE")
        baseline = instruction_count()
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO projects (
                    id, slug, name, created_at, archived
                ) VALUES (?, ?, ?, 1, 0)
                """,
                [
                    (
                        f"unexpired-project-{item}",
                        f"unexpired-project-{item}",
                        f"Unexpired {item}",
                    )
                    for item in range(500)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, execution_state, terminal_result_id,
                    created_at, updated_at
                ) VALUES (?, ?, 1, 'claim', '{}', 'claimed', ?, 1, 1,
                          'not_started', NULL, 1, 1)
                """,
                [
                    (
                        f"unexpired-turn-{item}",
                        f"unexpired-project-{item}",
                        f"unexpired-attempt-{item}",
                    )
                    for item in range(500)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    attempt_id, claim_worker_id,
                    claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (?, ?, 'running', 1, ?, 'worker', 1000,
                          'session', 1)
                """,
                [
                    (
                        f"unexpired-turn-{item}",
                        f"unexpired-project-{item}",
                        f"unexpired-attempt-{item}",
                    )
                    for item in range(500)
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_worker_leases (
                    lease_id, project_id, turn_id, worker_id,
                    lease_generation, fencing_token, expires_at,
                    updated_at
                ) VALUES (?, ?, ?, 'worker', 1, 1, 1000, 1)
                """,
                [
                    (
                        f"unexpired-attempt-{item}",
                        f"unexpired-project-{item}",
                        f"unexpired-turn-{item}",
                    )
                    for item in range(500)
                ],
            )
        conn.execute("ANALYZE")

        assert instruction_count() <= baseline + 200
    finally:
        conn.close()


def test_blocked_history_scan_is_bounded_and_does_not_starve_actionable(
    tmp_path,
):
    module, conn, _, project_id, _ = _make_runtime(
        tmp_path / "recover-blocked-history.db"
    )
    try:
        blocked = []
        for item in range(2000):
            turn_id = f"blocked-turn-{item}"
            attempt_id = f"blocked-attempt-{item}"
            block_key = prdb._recovery_block_key(
                project_id=project_id,
                turn_id=turn_id,
                attempt_id=attempt_id,
                lease_generation=1,
                fencing_token=1,
            )
            blocked.append((turn_id, attempt_id, block_key))
        with prdb.write_transaction(conn):
            conn.executemany(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, execution_state, terminal_result_id,
                    recovery_block_key, created_at, updated_at
                ) VALUES (?, ?, ?, ?, '{}', 'reconciling', ?, 1, 1,
                          'started', NULL, NULL, 1, 1)
                """,
                [
                    (
                        turn_id,
                        project_id,
                        item + 1,
                        f"blocked-{item}",
                        attempt_id,
                    )
                    for item, (turn_id, attempt_id, _) in enumerate(
                        blocked
                    )
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    attempt_id, claim_worker_id,
                    claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (?, ?, 'running', 1, ?, 'worker', 100,
                          'session', 1)
                """,
                [
                    (turn_id, project_id, attempt_id)
                    for turn_id, attempt_id, _ in blocked
                ],
            )
            conn.executemany(
                """
                INSERT INTO project_events (
                    event_id, project_id, sequence, kind, turn_id,
                    payload_json, created_at
                ) VALUES (?, ?, ?, 'turn.recovery_blocked', ?, ?, 1)
                """,
                [
                    (
                        block_key,
                        project_id,
                        item + 1,
                        turn_id,
                        module.canonical_json_object(
                            {
                                "attempt_id": attempt_id,
                                "fencing_token": 1,
                                "lease_generation": 1,
                                "source_status": "claimed",
                                "turn_id": turn_id,
                                "version": item + 1,
                            }
                        ),
                    )
                    for item, (
                        turn_id,
                        attempt_id,
                        block_key,
                    ) in enumerate(blocked)
                ],
            )
            conn.executemany(
                """
                UPDATE project_turns SET recovery_block_key = ?
                WHERE project_id = ? AND turn_id = ?
                """,
                [
                    (block_key, project_id, turn_id)
                    for turn_id, _, block_key in blocked
                ],
            )
            conn.execute(
                """
                INSERT INTO project_turns (
                    turn_id, project_id, sequence, idempotency_key,
                    payload_json, status, attempt_id, lease_generation,
                    fencing_token, execution_state, terminal_result_id,
                    recovery_block_key, created_at, updated_at
                ) VALUES (
                    'actionable-turn', ?, 2001, 'actionable', '{}',
                    'reconciling', 'actionable-attempt', 1, 1,
                    'started', NULL, NULL, 1, 1
                )
                """,
                (project_id,),
            )
            conn.execute(
                """
                INSERT INTO project_run_controls (
                    turn_id, project_id, control_state, control_version,
                    attempt_id, claim_worker_id,
                    claim_lease_expires_at,
                    claim_canonical_session_id, updated_at
                ) VALUES (
                    'actionable-turn', ?, 'running', 1,
                    'actionable-attempt', 'worker', 100, 'session', 1
                )
                """,
                (project_id,),
            )
        conn.execute("ANALYZE")
        instructions = [0]

        def progress():
            instructions[0] += 1
            return 0

        conn.set_progress_handler(progress, 1)
        try:
            candidates = prdb._recovery_candidates(
                conn, now=100, limit=1
            )
        finally:
            conn.set_progress_handler(None, 0)

        assert [candidate.turn_id for candidate in candidates] == [
            "actionable-turn"
        ]
        assert instructions[0] < 1000
    finally:
        conn.close()


def test_heartbeat_after_recovery_discovery_invalidates_candidate_cleanly(
    tmp_path, monkeypatch
):
    path = tmp_path / "recover-heartbeat-wins.db"
    now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        path, clock=lambda: now[0]
    )
    heartbeat_conn = None
    try:
        turn, claim = _enqueue_and_claim(runtime, project_id, actor)
        now[0] = claim.lease_expires_at
        heartbeat_conn = projects_db.connect(path)
        heartbeat_runtime = module.ProjectRuntime(
            heartbeat_conn, clock=lambda: claim.lease_expires_at - 1
        )
        original_candidates = prdb._recovery_candidates
        renewed = []

        def discover_then_heartbeat(*args, **kwargs):
            candidates = original_candidates(*args, **kwargs)
            renewed.append(
                heartbeat_runtime.heartbeat_turn(
                    claim, lease_seconds=30
                )
            )
            return candidates

        monkeypatch.setattr(
            prdb, "_recovery_candidates", discover_then_heartbeat
        )
        before_events = tuple(
            conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? ORDER BY sequence
                """,
                (project_id,),
            )
        )

        recovered = runtime.reconcile_inflight_turns(
            _RecordingReadback(
                conn, module.TurnReadbackResult("unknown")
            ),
            limit=10,
        )

        assert recovered == ()
        assert len(renewed) == 1
        assert conn.execute(
            "SELECT status FROM project_turns WHERE turn_id = ?",
            (turn.turn_id,),
        ).fetchone()[0] == "claimed"
        assert tuple(
            conn.execute(
                """
                SELECT * FROM project_events
                WHERE project_id = ? ORDER BY sequence
                """,
                (project_id,),
            )
        ) == before_events
    finally:
        if heartbeat_conn is not None:
            heartbeat_conn.close()
        conn.close()


def test_task7_c11_stop_closure_recovery_proof_matrix_and_operation_precedence(
    tmp_path,
    monkeypatch,
):
    """Only exact State evidence closes a parked Task-7 stop or terminal.

    Catches proof-shape drift, a State read inside Projects SQL scope, legacy
    bare-stop trust, and operation/approval recovery outranking persisted stop.
    """
    from gateway import session as session_module
    from hermes_state import SessionDB, _project_batch_fingerprint

    assert hasattr(session_module, "StateTask7TerminalReadbackAdapter"), (
        "C11 requires the concrete State Task-7 evidence adapter"
    )

    def create_case(
        label,
        *,
        source_status="stop_requested",
        terminal_status="succeeded",
        mutation=None,
    ):
        now = [100]
        module, conn, runtime, project_id, actor = _make_runtime(
            tmp_path / f"c11-{label}.projects.db",
            clock=lambda: now[0],
        )
        state = SessionDB(tmp_path / f"c11-{label}.state.db")
        state.create_session("session-root", source="cli")
        state.create_session("session-drift", source="cli")
        turn, claim = _enqueue_and_claim(
            runtime, project_id, actor, key=label
        )
        claim = runtime.mark_turn_started(claim)
        batch_id = "123e4567-e89b-42d3-a456-426614174000"
        state.prepare_terminal_result(
            claim,
            batch_id=batch_id,
            status=terminal_status,
            base_message_count=0,
            messages=(
                {"role": "user", "content": label, "timestamp": 1.0},
                {
                    "role": "assistant",
                    "content": "terminal",
                    "timestamp": 2.0,
                },
            ),
        )
        row = state._conn.execute(
            "SELECT * FROM project_turn_transcript_batches "
            "WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        assert row is not None
        fingerprint = _project_batch_fingerprint(row)

        if mutation == "discard-stop":
            assert state._discard_project_batch(
                fingerprint, "stop_requested"
            ) == "discarded"
        elif mutation == "discard-cancelled":
            assert state._discard_project_batch(
                fingerprint, "cancelled"
            ) == "discarded"
        elif mutation == "published":
            assert state._publish_project_batch(fingerprint) == "published"
        elif mutation in {"conflict_pending", "conflicted"}:
            state.append_message("session-root", "user", "drift")
            reserved = state._publish_project_batch(fingerprint)
            assert type(reserved).__name__ == "_ProjectBatchConflictReservation"
            if mutation == "conflicted":
                assert state._finalize_project_batch_conflict(
                    reserved
                ) == "conflicted"
        elif mutation == "missing":
            state._conn.execute(
                "DELETE FROM project_turn_transcript_batches "
                "WHERE batch_id = ?",
                (batch_id,),
            )
            state._conn.commit()
        elif mutation == "approval":
            state._conn.execute("PRAGMA ignore_check_constraints = ON")
            try:
                state._conn.execute(
                    """
                    UPDATE project_turn_transcript_batches
                    SET kind = 'approval_checkpoint',
                        terminal_status = NULL,
                        operation_id = 'c11-operation',
                        approval_id = 'c11-approval'
                    WHERE batch_id = ?
                    """,
                    (batch_id,),
                )
                state._conn.commit()
            finally:
                state._conn.execute(
                    "PRAGMA ignore_check_constraints = OFF"
                )
        elif mutation == "malformed-digest":
            state._conn.execute(
                "UPDATE project_turn_transcript_batches "
                "SET transcript_sha256 = ? WHERE batch_id = ?",
                ("0" * 64, batch_id),
            )
            state._conn.commit()
        elif mutation in {"noncanonical-transcript", "shape"}:
            transcript_json = (
                '[ { "role": "user", "content": "not canonical", '
                '"timestamp": 1.0 } ]'
                if mutation == "noncanonical-transcript"
                else '{"role":"user"}'
            )
            state._conn.execute(
                """
                UPDATE project_turn_transcript_batches
                SET transcript_json = ?, transcript_sha256 = ?
                WHERE batch_id = ?
                """,
                (
                    transcript_json,
                    hashlib.sha256(
                        transcript_json.encode("utf-8")
                    ).hexdigest(),
                    batch_id,
                ),
            )
            state._conn.commit()
        elif mutation is not None and mutation.startswith("identity-"):
            column, value = {
                "identity-project": ("project_id", "other-project"),
                "identity-turn": ("turn_id", "other-turn"),
                "identity-sequence": ("sequence", claim.sequence + 1),
                "identity-worker": ("worker_id", "other-worker"),
                "identity-attempt": ("attempt_id", "other-attempt"),
                "identity-generation": (
                    "lease_generation",
                    claim.lease_generation + 1,
                ),
                "identity-fence": (
                    "fencing_token",
                    claim.fencing_token + 1,
                ),
                "identity-session": ("session_id", "session-drift"),
                "identity-horizon": (
                    "lease_expires_at",
                    claim.lease_expires_at + 1,
                ),
            }[mutation]
            state._conn.execute(
                f"UPDATE project_turn_transcript_batches "
                f"SET {column} = ? WHERE batch_id = ?",
                (value, batch_id),
            )
            state._conn.commit()
        elif mutation == "exception":
            state._conn.execute(
                "DROP INDEX idx_project_batches_one_terminal_attempt"
            )
            state._conn.commit()
        elif mutation == "multiple":
            state._conn.execute(
                "DROP INDEX idx_project_batches_one_terminal_attempt"
            )
            state.prepare_terminal_result(
                claim,
                batch_id="223e4567-e89b-42d3-a456-426614174000",
                status=terminal_status,
                base_message_count=0,
                messages=(
                    {
                        "role": "user",
                        "content": f"{label}-duplicate",
                        "timestamp": 3.0,
                    },
                    {
                        "role": "assistant",
                        "content": "duplicate",
                        "timestamp": 4.0,
                    },
                ),
            )
            state._conn.execute(
                """
                CREATE INDEX idx_project_batches_one_terminal_attempt
                ON project_turn_transcript_batches(
                    project_id, turn_id, attempt_id,
                    lease_generation, fencing_token
                ) WHERE kind = 'terminal_result'
                """
            )
            state._conn.commit()

        if source_status == "stop_requested":
            assert runtime.request_stop(
                project_id,
                turn.turn_id,
                actor,
                idempotency_key=f"stop-{label}",
                expected_version=runtime._require_state(
                    project_id
                ).version,
                expected_control_version=runtime._control(
                    project_id, turn.turn_id
                ).control_version,
            ).control_state == "stop_requested"
        else:
            assert source_status == "claimed"
        now[0] = claim.lease_expires_at + 1
        request = module.TurnReadbackRequest(
            project_id=claim.project_id,
            turn_id=claim.turn_id,
            sequence=claim.sequence,
            worker_id=claim.worker_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
            lease_expires_at=claim.lease_expires_at,
            canonical_session_id=claim.canonical_session_id,
            source_status=source_status,
            execution_state="started",
        )
        return {
            "module": module,
            "conn": conn,
            "runtime": runtime,
            "state": state,
            "project_id": project_id,
            "turn": turn,
            "claim": claim,
            "batch_id": batch_id,
            "request": request,
        }

    class ObservedConcreteAdapter:
        def __init__(self, case):
            self._delegate = (
                session_module.StateTask7TerminalReadbackAdapter(
                    case["state"]
                )
            )
            self._projects = case["conn"]
            self.calls = []

        def read_turn_with_evidence(self, request):
            self.calls.append((request, self._projects.in_transaction))
            return self._delegate.read_turn_with_evidence(request)

    # Every row shape is first classified directly by the concrete adapter and
    # then driven through real Projects recovery.  Identity drift is persisted
    # in State rather than faked in the request.
    matrix = (
        ("claimed-succeeded", "claimed", "succeeded", None, "succeeded"),
        ("claimed-failed", "claimed", "failed", None, "failed"),
        ("stop-prepared", "stop_requested", "succeeded", None, "stopped"),
        (
            "stop-discarded",
            "stop_requested",
            "succeeded",
            "discard-stop",
            "stopped",
        ),
        (
            "missing",
            "stop_requested",
            "succeeded",
            "missing",
            "unknown",
        ),
        (
            "discarded-cancelled",
            "stop_requested",
            "succeeded",
            "discard-cancelled",
            "unknown",
        ),
        (
            "published",
            "stop_requested",
            "succeeded",
            "published",
            "unknown",
        ),
        (
            "conflict-pending",
            "stop_requested",
            "succeeded",
            "conflict_pending",
            "unknown",
        ),
        (
            "conflicted",
            "stop_requested",
            "succeeded",
            "conflicted",
            "unknown",
        ),
        (
            "approval",
            "stop_requested",
            "succeeded",
            "approval",
            "unknown",
        ),
        (
            "malformed-digest",
            "stop_requested",
            "succeeded",
            "malformed-digest",
            "unknown",
        ),
        (
            "noncanonical-transcript",
            "stop_requested",
            "succeeded",
            "noncanonical-transcript",
            "unknown",
        ),
        (
            "shape",
            "stop_requested",
            "succeeded",
            "shape",
            "unknown",
        ),
        *(
            (
                mutation,
                "stop_requested",
                "succeeded",
                mutation,
                "unknown",
            )
            for mutation in (
                "identity-project",
                "identity-turn",
                "identity-sequence",
                "identity-worker",
                "identity-attempt",
                "identity-generation",
                "identity-fence",
                "identity-session",
                "identity-horizon",
            )
        ),
        (
            "state-exception",
            "stop_requested",
            "succeeded",
            "exception",
            "storage-exception",
        ),
        (
            "state-multiplicity",
            "stop_requested",
            "succeeded",
            "multiple",
            "multiplicity",
        ),
    )
    for (
        label,
        source_status,
        terminal_status,
        mutation,
        expected,
    ) in matrix:
        case = create_case(
            label,
            source_status=source_status,
            terminal_status=terminal_status,
            mutation=mutation,
        )
        module = case["module"]
        conn = case["conn"]
        state = case["state"]
        try:
            direct = session_module.StateTask7TerminalReadbackAdapter(
                state
            )
            if expected == "storage-exception":
                with pytest.raises(sqlite3.DatabaseError):
                    direct.read_turn_with_evidence(case["request"])
            elif expected == "multiplicity":
                with pytest.raises(RuntimeError):
                    direct.read_turn_with_evidence(case["request"])
            else:
                expected_batch = (
                    case["batch_id"]
                    if expected in {"succeeded", "failed"}
                    else None
                )
                expected_result_id = expected_batch
                assert direct.read_turn_with_evidence(
                    case["request"]
                ) == module.Task7TerminalReadbackEvidence(
                    module.TurnReadbackResult(
                        expected, expected_result_id
                    ),
                    expected_batch,
                )

            observed = ObservedConcreteAdapter(case)
            recovered = case[
                "runtime"
            ].reconcile_inflight_turns_with_task7_evidence(observed)
            assert observed.calls == [(case["request"], False)]
            if expected in {"succeeded", "failed", "stopped"}:
                assert [item.status for item in recovered] == [expected]
                assert conn.execute(
                    "SELECT recovery_block_key FROM project_turns "
                    "WHERE turn_id = ?",
                    (case["turn"].turn_id,),
                ).fetchone()[0] is None
                assert conn.execute(
                    "SELECT COUNT(*) FROM project_worker_leases "
                    "WHERE turn_id = ?",
                    (case["turn"].turn_id,),
                ).fetchone()[0] == 0
                runtime_state = prdb.runtime_state_for_project(
                    conn, case["project_id"]
                )
                assert runtime_state is not None
                assert runtime_state.transcript_pending_batch_id == (
                    case["batch_id"]
                    if expected in {"succeeded", "failed"}
                    else None
                )
            else:
                assert [item.status for item in recovered] == [
                    "reconciling"
                ]
                block_key = prdb._recovery_block_key(
                    project_id=case["project_id"],
                    turn_id=case["turn"].turn_id,
                    attempt_id=case["claim"].attempt_id,
                    lease_generation=case["claim"].lease_generation,
                    fencing_token=case["claim"].fencing_token,
                )
                assert conn.execute(
                    "SELECT recovery_block_key FROM project_turns "
                    "WHERE turn_id = ?",
                    (case["turn"].turn_id,),
                ).fetchone()[0] == block_key
                assert conn.execute(
                    "SELECT COUNT(*) FROM project_events "
                    "WHERE event_id = ? AND kind = "
                    "'turn.recovery_blocked'",
                    (block_key,),
                ).fetchone()[0] == 1
        finally:
            conn.close()
            state.close()

    # Invalid stop proof cannot be suppressed by a real, certified
    # effect_started operation.  Stop recovery reads State exactly once
    # outside Projects scope, force-blocks once, and does not inspect,
    # rehydrate, retry, approve or mutate the operation.
    operation_case = create_case(
        "effect-started-operation", mutation="missing"
    )
    operation_conn = operation_case["conn"]
    operation_state = operation_case["state"]
    try:
        operation_id = "c11-real-effect-started-operation"
        assert prdb._insert_project_operation(
            operation_conn,
            operation_id=operation_id,
            project_id=operation_case["project_id"],
            turn_id=operation_case["turn"].turn_id,
            idempotency_key="c11-real-effect-started-key",
            command_revision=1,
            targets_json='["c:/work/c11"]',
            payload_json="{}",
            status="effect_started",
            canonical_action="local_code_edit",
            batch_items_json='["item-1"]',
            readback_kind="ledger",
            attempt_id=operation_case["claim"].attempt_id,
            lease_generation=operation_case[
                "claim"
            ].lease_generation,
            fencing_token=operation_case["claim"].fencing_token,
            blocked_reason=None,
            remote_idempotency_supported=True,
            approval_fingerprint_json=None,
            now=100,
        )
        prdb._certify_project_operation(
            operation_conn,
            project_id=operation_case["project_id"],
            operation_id=operation_id,
        )
        operation_conn.commit()
        operation_before = dict(
            operation_conn.execute(
                "SELECT * FROM project_operations "
                "WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
        )
        observed = ObservedConcreteAdapter(operation_case)
        projects_trace: list[str] = []
        forbidden_calls = []

        def forbidden_operation_path(*args, **kwargs):
            forbidden_calls.append((args, kwargs))
            raise AssertionError(
                "persisted stop must precede operation disposition"
            )

        with monkeypatch.context() as stop_patch:
            stop_patch.setattr(
                prdb,
                "_project_operation_disposition_for_turn",
                forbidden_operation_path,
            )
            stop_patch.setattr(
                prdb,
                "_operation_pending_for_turn",
                forbidden_operation_path,
            )
            operation_conn.set_trace_callback(projects_trace.append)
            try:
                blocked = operation_case[
                    "runtime"
                ].reconcile_inflight_turns_with_task7_evidence(
                    observed
                )
            finally:
                operation_conn.set_trace_callback(None)
        assert forbidden_calls == []
        assert observed.calls == [(operation_case["request"], False)]
        assert [item.status for item in blocked] == ["reconciling"]
        block_key = prdb._recovery_block_key(
            project_id=operation_case["project_id"],
            turn_id=operation_case["turn"].turn_id,
            attempt_id=operation_case["claim"].attempt_id,
            lease_generation=operation_case[
                "claim"
            ].lease_generation,
            fencing_token=operation_case["claim"].fencing_token,
        )
        assert operation_conn.execute(
            "SELECT recovery_block_key FROM project_turns "
            "WHERE turn_id = ?",
            (operation_case["turn"].turn_id,),
        ).fetchone()[0] == block_key
        assert dict(
            operation_conn.execute(
                "SELECT * FROM project_operations "
                "WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
        ) == operation_before
        assert not [
            statement
            for statement in projects_trace
            if "PROJECT_OPERATIONS" in statement.upper()
            or "PROJECT_APPROVALS" in statement.upper()
        ]
        events = tuple(
            operation_conn.execute(
                "SELECT event_id, kind FROM project_events "
                "WHERE turn_id = ? ORDER BY sequence",
                (operation_case["turn"].turn_id,),
            )
        )
        assert sum(
            row["event_id"] == block_key
            and row["kind"] == "turn.recovery_blocked"
            for row in events
        ) == 1
        assert not [
            row["kind"]
            for row in events
            if row["kind"].startswith("operation.")
            or "approval" in row["kind"]
            or "rehydrat" in row["kind"]
            or "effect" in row["kind"]
        ]
        replay_snapshot = (
            dict(
                operation_conn.execute(
                    "SELECT * FROM project_turns WHERE turn_id = ?",
                    (operation_case["turn"].turn_id,),
                ).fetchone()
            ),
            dict(
                operation_conn.execute(
                    "SELECT * FROM project_run_controls "
                    "WHERE turn_id = ?",
                    (operation_case["turn"].turn_id,),
                ).fetchone()
            ),
            dict(
                operation_conn.execute(
                    "SELECT * FROM project_operations "
                    "WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
            ),
            tuple(tuple(row) for row in events),
        )
        assert operation_case[
            "runtime"
        ].reconcile_inflight_turns_with_task7_evidence(
            observed
        ) == ()
        assert observed.calls == [(operation_case["request"], False)]
        assert (
            dict(
                operation_conn.execute(
                    "SELECT * FROM project_turns WHERE turn_id = ?",
                    (operation_case["turn"].turn_id,),
                ).fetchone()
            ),
            dict(
                operation_conn.execute(
                    "SELECT * FROM project_run_controls "
                    "WHERE turn_id = ?",
                    (operation_case["turn"].turn_id,),
                ).fetchone()
            ),
            dict(
                operation_conn.execute(
                    "SELECT * FROM project_operations "
                    "WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
            ),
            tuple(
                tuple(row)
                for row in operation_conn.execute(
                    "SELECT event_id, kind FROM project_events "
                    "WHERE turn_id = ? ORDER BY sequence",
                    (operation_case["turn"].turn_id,),
                )
            ),
        ) == replay_snapshot
    finally:
        operation_conn.close()
        operation_state.close()

    # The legacy port's bare stopped result is no longer terminal evidence for
    # a started Task-7 stop: it fails closed once and replay is inert.
    legacy_case = create_case("legacy-bare-stop")
    legacy_conn = legacy_case["conn"]
    legacy_state = legacy_case["state"]
    try:
        class BareStopped:
            def __init__(self):
                self.calls = []

            def read_turn(self, request):
                self.calls.append(
                    (request, legacy_conn.in_transaction)
                )
                return legacy_case["module"].TurnReadbackResult(
                    "stopped"
                )

        bare = BareStopped()
        legacy_forbidden_calls = []

        def forbidden_legacy_operation_path(*args, **kwargs):
            legacy_forbidden_calls.append((args, kwargs))
            raise AssertionError(
                "legacy started stop must precede operation disposition"
            )

        with monkeypatch.context() as legacy_patch:
            legacy_patch.setattr(
                prdb,
                "_project_operation_disposition_for_turn",
                forbidden_legacy_operation_path,
            )
            legacy_patch.setattr(
                prdb,
                "_operation_pending_for_turn",
                forbidden_legacy_operation_path,
            )
            legacy_result = legacy_case[
                "runtime"
            ].reconcile_inflight_turns(bare, limit=100)
        assert legacy_forbidden_calls == []
        assert [item.status for item in legacy_result] == [
            "reconciling"
        ]
        assert bare.calls == [(legacy_case["request"], False)]
        assert legacy_conn.execute(
            "SELECT COUNT(*) FROM project_events "
            "WHERE turn_id = ? AND kind = 'turn.recovery_blocked'",
            (legacy_case["turn"].turn_id,),
        ).fetchone()[0] == 1
        assert legacy_case["runtime"].reconcile_inflight_turns(
            bare, limit=100
        ) == ()
        assert bare.calls == [(legacy_case["request"], False)]
    finally:
        legacy_conn.close()
        legacy_state.close()

    # A not-started stop closes directly.  The concrete adapter, State batch
    # storage, operation disposition and approvals are all untouched.
    now = [100]
    (
        _,
        not_started_conn,
        not_started_runtime,
        not_started_project,
        not_started_actor,
    ) = _make_runtime(
        tmp_path / "c11-not-started.projects.db",
        clock=lambda: now[0],
    )
    not_started_state = SessionDB(
        tmp_path / "c11-not-started.state.db"
    )
    try:
        not_started_state.create_session("session-root", source="cli")
        not_started_turn, not_started_claim = _enqueue_and_claim(
            not_started_runtime,
            not_started_project,
            not_started_actor,
            key="not-started",
        )
        assert not_started_runtime.request_stop(
            not_started_project,
            not_started_turn.turn_id,
            not_started_actor,
            idempotency_key="not-started-stop",
            expected_version=not_started_runtime._require_state(
                not_started_project
            ).version,
            expected_control_version=not_started_runtime._control(
                not_started_project, not_started_turn.turn_id
            ).control_version,
        ).control_state == "stop_requested"
        now[0] = not_started_claim.lease_expires_at + 1
        not_started_case = {
            "state": not_started_state,
            "conn": not_started_conn,
        }
        observed = ObservedConcreteAdapter(not_started_case)
        state_trace: list[str] = []
        projects_trace: list[str] = []
        forbidden_calls = []

        def forbidden_not_started_path(*args, **kwargs):
            forbidden_calls.append((args, kwargs))
            raise AssertionError(
                "not-started stop must not inspect operation authority"
            )

        with monkeypatch.context() as direct_patch:
            direct_patch.setattr(
                prdb,
                "_project_operation_disposition_for_turn",
                forbidden_not_started_path,
            )
            direct_patch.setattr(
                prdb,
                "_operation_pending_for_turn",
                forbidden_not_started_path,
            )
            not_started_state._conn.set_trace_callback(
                state_trace.append
            )
            not_started_conn.set_trace_callback(projects_trace.append)
            try:
                direct = not_started_runtime.reconcile_inflight_turns_with_task7_evidence(
                    observed
                )
            finally:
                not_started_conn.set_trace_callback(None)
                not_started_state._conn.set_trace_callback(None)
        assert [item.status for item in direct] == ["stopped"]
        assert observed.calls == []
        assert forbidden_calls == []
        assert not [
            statement
            for statement in state_trace
            if "PROJECT_TURN_TRANSCRIPT_BATCHES" in statement.upper()
        ]
        assert not [
            statement
            for statement in projects_trace
            if "PROJECT_OPERATIONS" in statement.upper()
            or "PROJECT_APPROVALS" in statement.upper()
        ]
        assert not_started_conn.execute(
            "SELECT COUNT(*) FROM project_worker_leases "
            "WHERE turn_id = ?",
            (not_started_turn.turn_id,),
        ).fetchone()[0] == 0
    finally:
        not_started_conn.close()
        not_started_state.close()


def test_task7_c12_noncritical_reconciled_effect_without_terminal_batch_blocks_once(
    tmp_path,
    monkeypatch,
):
    """A reconciled noncritical effect is not legacy terminal evidence.

    Mutations caught: consulting legacy turn readback before the durable
    reconciled disposition, or letting an operation reconcile in the
    readback-to-final-CAS gap terminalize the turn.
    """
    from hermes_cli.project_operations import (
        OperationIntent,
        OperationReadbackResult,
        OperationReceipt,
        ProjectOperationGuard,
    )
    from gateway.project_runtime_worker import (
        BoundProjectOperationAuthority,
        ProjectPolicyDecisionCarrier,
    )
    from hermes_cli.project_policy import (
        ContractPolicyView,
        Decision,
        ProjectBindingView,
        ProjectCommand,
        ProjectPolicyView,
        decide as decide_project_policy,
    )

    def operation_snapshot(conn, operation_id):
        row = conn.execute(
            "SELECT * FROM project_operations WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        assert row is not None
        return dict(row)

    def reconciliation_events(conn, project_id, turn_id):
        return tuple(
            tuple(row)
            for row in conn.execute(
                """
                SELECT event_id, kind, payload_json
                FROM project_events
                WHERE project_id = ? AND turn_id = ?
                ORDER BY sequence
                """,
                (project_id, turn_id),
            )
        )

    def assert_blocked_once(
        *,
        conn,
        runtime,
        runtime_module,
        project_id,
        turn,
        claim,
        actor,
        operation_id,
        operation_before,
        events_before_recovery,
    ):
        block_key = prdb._recovery_block_key(
            project_id=project_id,
            turn_id=turn.turn_id,
            attempt_id=claim.attempt_id,
            lease_generation=claim.lease_generation,
            fencing_token=claim.fencing_token,
        )
        assert tuple(
            conn.execute(
                """
                SELECT status, terminal_result_id, recovery_block_key
                FROM project_turns WHERE turn_id = ?
                """,
                (turn.turn_id,),
            ).fetchone()
        ) == ("reconciling", None, block_key)
        assert conn.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE event_id = ? AND kind = 'turn.recovery_blocked'
            """,
            (block_key,),
        ).fetchone()[0] == 1
        assert operation_snapshot(conn, operation_id) == operation_before
        events_after_recovery = reconciliation_events(
            conn, project_id, turn.turn_id
        )
        assert (
            events_after_recovery[: len(events_before_recovery)]
            == events_before_recovery
        )
        recovery_event_delta = events_after_recovery[
            len(events_before_recovery) :
        ]
        assert [
            kind for _, kind, _ in recovery_event_delta
        ] == ["turn.reconciling", "turn.recovery_blocked"]
        forbidden = {
            "turn.succeeded",
            "turn.failed",
            "run.stopped",
            "turn.requeued",
            "operation.effect_started",
            "operation.rehydrated",
            "run.resume_requested",
        }
        assert not [
            kind
            for _, kind, _ in recovery_event_delta
            if kind in forbidden
        ]
        before_resume_changes = conn.total_changes
        before_resume_claim = _claim_snapshot(
            conn, project_id, turn.turn_id
        )
        before_resume_operation = operation_snapshot(conn, operation_id)
        before_resume_events = reconciliation_events(
            conn, project_id, turn.turn_id
        )
        state = prdb.runtime_state_for_project(conn, project_id)
        control = runtime._control(project_id, turn.turn_id)
        assert state is not None
        with pytest.raises(
            runtime_module.ProjectRuntimeError
        ) as resume_error:
            runtime.request_resume(
                project_id,
                turn.turn_id,
                actor,
                idempotency_key=f"resume-{turn.turn_id}",
                expected_version=state.version,
                expected_control_version=control.control_version,
            )
        assert (
            resume_error.value.code
            is runtime_module.RuntimeErrorCode.TURN_NOT_STOPPED
        )
        assert conn.total_changes == before_resume_changes
        assert (
            _claim_snapshot(conn, project_id, turn.turn_id)
            == before_resume_claim
        )
        assert (
            operation_snapshot(conn, operation_id)
            == before_resume_operation
        )
        assert (
            reconciliation_events(conn, project_id, turn.turn_id)
            == before_resume_events
        )

    def assert_replay_write_free(
        *,
        conn,
        runtime,
        readback,
        project_id,
        turn,
        operation_id,
    ):
        before_changes = conn.total_changes
        before_claim = _claim_snapshot(conn, project_id, turn.turn_id)
        before_operation = operation_snapshot(conn, operation_id)
        before_events = reconciliation_events(
            conn, project_id, turn.turn_id
        )
        assert runtime.reconcile_inflight_turns(
            readback, limit=100
        ) == ()
        assert conn.total_changes == before_changes
        assert (
            _claim_snapshot(conn, project_id, turn.turn_id)
            == before_claim
        )
        assert operation_snapshot(conn, operation_id) == before_operation
        assert (
            reconciliation_events(conn, project_id, turn.turn_id)
            == before_events
        )

    def reconcile_operation(
        *,
        runtime,
        conn,
        project_id,
        claim,
        operation_id,
    ):
        guard = ProjectOperationGuard(runtime)
        projects_db.add_folder(
            conn,
            project_id,
            "C:/work",
            is_primary=True,
        )
        stored_root = conn.execute(
            """
            SELECT path FROM project_folders
            WHERE project_id = ? AND is_primary = 1
            """,
            (project_id,),
        ).fetchone()[0].replace("\\", "/")
        contract_id = f"contract-{project_id}"
        contract_json = json.dumps(
            {
                "allowed_action_classes": ["local_code_edit"],
                "allowed_phases": ["implementation"],
                "approved_plan_ref": "plan-c12",
                "revision": 1,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        conn.execute(
            """
            INSERT INTO project_contracts (
                contract_id, project_id, revision, contract_json,
                status, created_at, updated_at
            ) VALUES (?, ?, 1, ?, 'active', 1, 1)
            """,
            (contract_id, project_id, contract_json),
        )
        conn.commit()
        intent = OperationIntent(
            operation_id=operation_id,
            project_id=project_id,
            turn_id=claim.turn_id,
            idempotency_key=f"remote-{operation_id}",
            canonical_action="local_code_edit",
            command_revision=1,
            targets=("c:/work/c12.txt",),
            batch_items=("write-c12",),
            payload={"contents": "C12"},
            readback_kind="ledger",
            remote_idempotency_supported=True,
        )
        state = prdb.runtime_state_for_project(conn, project_id)
        control = runtime.control_for_claim(claim)
        assert state is not None
        attempt = module.TurnAttemptIdentity(
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
        origin = module.TurnOrigin(
            "owner-binding",
            "desktop",
            f"window-{project_id}",
            "owner",
        )
        actor_view = ActorContext(
            "owner",
            "desktop",
            "owner-binding",
            True,
        )
        project_view = ProjectPolicyView(
            project_id,
            state.lifecycle,
            state.current_phase,
            (stored_root,),
            "plan-c12",
            (
                ProjectBindingView(
                    "owner-binding",
                    "desktop",
                    "owner",
                    project_id,
                ),
            ),
        )
        contract_view = ContractPolicyView(
            1,
            frozenset({"local_code_edit"}),
            frozenset({"implementation"}),
            "plan-c12",
        )
        command = ProjectCommand(
            "local_code_edit",
            project_id,
            1,
            "local_code_edit",
            intent.targets,
            None,
            intent.batch_items,
            {"phase": state.current_phase},
        )
        policy = decide_project_policy(
            command,
            project_view,
            contract_view,
            actor_view,
        )
        assert policy.decision is Decision.ALLOW
        effect_scope = {
            "targets": list(intent.targets),
            "batch_items": list(intent.batch_items),
            "payload_effects": dict(intent.payload),
        }
        effect_scope_json = json.dumps(
            effect_scope,
            sort_keys=True,
            separators=(",", ":"),
        )
        effect_scope_sha256 = hashlib.sha256(
            effect_scope_json.encode("utf-8")
        ).hexdigest()
        authority_json = json.dumps(
            {
                "command": {
                    "name": command.name,
                    "project_id": command.project_id,
                    "revision": command.revision,
                    "action_class": command.action_class,
                    "targets": list(command.targets),
                    "batch_id": command.batch_id,
                    "batch_items": list(command.batch_items),
                    "metadata": dict(command.metadata),
                },
                "intent": {
                    "operation_id": intent.operation_id,
                    "project_id": intent.project_id,
                    "turn_id": intent.turn_id,
                    "idempotency_key": intent.idempotency_key,
                    "canonical_action": intent.canonical_action,
                    "command_revision": intent.command_revision,
                    "targets": list(intent.targets),
                    "batch_items": list(intent.batch_items),
                    "payload": dict(intent.payload),
                    "readback_kind": intent.readback_kind,
                    "remote_idempotency_supported": (
                        intent.remote_idempotency_supported
                    ),
                },
                "policy_batch_id": None,
                "capability_fingerprint": [
                    intent.canonical_action,
                    intent.command_revision,
                    intent.readback_kind,
                    intent.remote_idempotency_supported,
                ],
                "effect_scope": effect_scope,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        authority = BoundProjectOperationAuthority(
            command,
            intent,
            None,
            effect_scope_json,
            effect_scope_sha256,
            authority_json,
            hashlib.sha256(authority_json.encode("utf-8")).hexdigest(),
        )
        carrier = ProjectPolicyDecisionCarrier(
            attempt,
            origin,
            control.control_version,
            state.version,
            authority,
            project_view,
            contract_id,
            "active",
            hashlib.sha256(contract_json.encode("utf-8")).hexdigest(),
            contract_view,
            actor_view,
            policy,
        )
        assert guard.prepare(
            claim,
            intent,
            policy=policy,
            approval=None,
            authority=authority,
            policy_authority=carrier,
        ).status == "approved"
        assert guard.mark_started(claim, operation_id).status == "effect_started"
        receipt = OperationReceipt(
            f"receipt-{operation_id}", {"provider_sequence": 12}
        )
        assert guard.record_receipt(
            claim, operation_id, receipt
        ).status == "receipt_recorded"

        class AppliedReadback:
            requests = []

            def read_operation(self, request):
                assert conn.in_transaction is False
                self.requests.append(request)
                return OperationReadbackResult(
                    "applied", {"ledger": "complete"}, receipt
                )

        readback = AppliedReadback()
        reconciled = guard.reconcile(claim, operation_id, readback)
        assert reconciled.status == "reconciled"
        assert len(readback.requests) == 1
        return operation_snapshot(conn, operation_id)

    def persist_race_reconciliation(
        *,
        conn,
        project_id,
        turn,
        claim,
        now,
        operation_id,
    ):
        """Persist the independent operation result at the final-CAS seam."""
        with prdb.write_transaction(conn):
            assert prdb._insert_project_operation(
                conn,
                operation_id=operation_id,
                project_id=project_id,
                turn_id=turn.turn_id,
                idempotency_key=f"remote-{operation_id}",
                command_revision=1,
                    targets_json='["c:/work/c12-race.txt"]',
                payload_json='{"contents":"C12 race"}',
                status="approved",
                canonical_action="local_code_edit",
                batch_items_json='["write-c12-race"]',
                readback_kind="ledger",
                attempt_id=claim.attempt_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                blocked_reason=None,
                remote_idempotency_supported=True,
                approval_fingerprint_json=None,
                now=now,
            )
            operation = prdb._certify_project_operation(
                conn, project_id=project_id, operation_id=operation_id
            )
            prdb._decertify_project_operation(conn, operation)
            assert prdb._mark_project_operation_started(
                conn,
                project_id=project_id,
                turn_id=turn.turn_id,
                operation_id=operation_id,
                attempt_id=claim.attempt_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                now=now,
            )
            operation = prdb._certify_project_operation(
                conn, project_id=project_id, operation_id=operation_id
            )
            prdb._decertify_project_operation(conn, operation)
            assert prdb._record_project_operation_receipt(
                conn,
                project_id=project_id,
                turn_id=turn.turn_id,
                operation_id=operation_id,
                attempt_id=claim.attempt_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                receipt_id=f"receipt-{operation_id}",
                receipt_json='{"provider_sequence":12}',
                now=now,
            )
            operation = prdb._certify_project_operation(
                conn, project_id=project_id, operation_id=operation_id
            )
            prdb._decertify_project_operation(conn, operation)
            assert prdb._park_project_operation_unknown(
                conn,
                project_id=project_id,
                turn_id=turn.turn_id,
                operation_id=operation_id,
                source_status="receipt_recorded",
                attempt_id=claim.attempt_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                now=now,
            )
            operation = prdb._certify_project_operation(
                conn, project_id=project_id, operation_id=operation_id
            )
            prdb._decertify_project_operation(conn, operation)
            assert prdb._finalize_project_operation_readback(
                conn,
                project_id=project_id,
                turn_id=turn.turn_id,
                operation_id=operation_id,
                attempt_id=claim.attempt_id,
                lease_generation=claim.lease_generation,
                fencing_token=claim.fencing_token,
                target_status="reconciled",
                receipt_id=f"receipt-{operation_id}",
                receipt_json='{"provider_sequence":12}',
                readback_json='{"evidence":{"ledger":"complete"},"outcome":"applied"}',
                blocked_reason=None,
                now=now,
            )
            prdb._certify_project_operation(
                conn, project_id=project_id, operation_id=operation_id
            )

    early_now = [100]
    module, conn, runtime, project_id, actor = _make_runtime(
        tmp_path / "c12-early.projects.db", clock=lambda: early_now[0]
    )
    try:
        early_turn, early_claim = _enqueue_and_claim(
            runtime, project_id, actor, key="c12-early"
        )
        early_claim = runtime.mark_turn_started(early_claim)
        early_before = reconcile_operation(
            runtime=runtime,
            conn=conn,
            project_id=project_id,
            claim=early_claim,
            operation_id="c12-early-operation",
        )
        early_events_before_recovery = reconciliation_events(
            conn, project_id, early_turn.turn_id
        )
        early_now[0] = early_claim.lease_expires_at + 1

        class NoLegacyReadback:
            calls = []

            def read_turn(self, request):
                assert conn.in_transaction is False
                self.calls.append(request)
                return module.TurnReadbackResult(
                    "succeeded", "legacy-must-not-terminalize"
                )

        early_readback = NoLegacyReadback()
        early_recovered = runtime.reconcile_inflight_turns(
            early_readback, limit=100
        )
        assert [turn.status for turn in early_recovered] == ["reconciling"]
        assert early_readback.calls == []
        assert_blocked_once(
            conn=conn,
            runtime=runtime,
            runtime_module=module,
            project_id=project_id,
            turn=early_turn,
            claim=early_claim,
            actor=actor,
            operation_id="c12-early-operation",
            operation_before=early_before,
            events_before_recovery=early_events_before_recovery,
        )
        assert_replay_write_free(
            conn=conn,
            runtime=runtime,
            readback=early_readback,
            project_id=project_id,
            turn=early_turn,
            operation_id="c12-early-operation",
        )
        assert early_readback.calls == []
    finally:
        conn.close()

    race_now = [100]
    race_module, race_conn, race_runtime, race_project, race_actor = _make_runtime(
        tmp_path / "c12-race.projects.db", clock=lambda: race_now[0]
    )
    try:
        race_turn, race_claim = _enqueue_and_claim(
            race_runtime, race_project, race_actor, key="c12-race"
        )
        race_claim = race_runtime.mark_turn_started(race_claim)
        race_events_before_recovery = reconciliation_events(
            race_conn, race_project, race_turn.turn_id
        )
        race_now[0] = race_claim.lease_expires_at + 1

        class TerminalLegacyReadback:
            calls = []

            def read_turn(self, request):
                assert race_conn.in_transaction is False
                self.calls.append(request)
                return race_module.TurnReadbackResult(
                    "succeeded", "legacy-race-result"
                )

        race_readback = TerminalLegacyReadback()
        original_finalize = race_runtime._finalize_recovery
        original_disposition = (
            prdb._project_operation_disposition_for_turn
        )
        race_before = [None]
        race_dispositions = []

        def reconcile_before_final_transaction(candidate, **kwargs):
            assert len(race_readback.calls) == 1
            persist_race_reconciliation(
                conn=race_conn,
                project_id=race_project,
                turn=race_turn,
                claim=race_claim,
                now=race_now[0],
                operation_id="c12-race-operation",
            )
            race_before[0] = operation_snapshot(
                race_conn, "c12-race-operation"
            )
            return original_finalize(candidate, **kwargs)

        def observe_race_disposition(
            observed_conn, *, project_id, turn_id
        ):
            disposition = original_disposition(
                observed_conn,
                project_id=project_id,
                turn_id=turn_id,
            )
            if (
                observed_conn is race_conn
                and project_id == race_project
                and turn_id == race_turn.turn_id
            ):
                race_dispositions.append(
                    (observed_conn.in_transaction, disposition)
                )
            return disposition

        with monkeypatch.context() as race_patch:
            race_patch.setattr(
                race_runtime,
                "_finalize_recovery",
                reconcile_before_final_transaction,
            )
            race_patch.setattr(
                prdb,
                "_project_operation_disposition_for_turn",
                observe_race_disposition,
            )
            race_recovered = race_runtime.reconcile_inflight_turns(
                race_readback, limit=100
            )
        assert [turn.status for turn in race_recovered] == ["reconciling"]
        assert len(race_readback.calls) == 1
        assert race_before[0] is not None
        assert race_dispositions == [
            (False, "clear"),
            (True, "reconciled"),
        ]
        assert_blocked_once(
            conn=race_conn,
            runtime=race_runtime,
            runtime_module=race_module,
            project_id=race_project,
            turn=race_turn,
            claim=race_claim,
            actor=race_actor,
            operation_id="c12-race-operation",
            operation_before=race_before[0],
            events_before_recovery=race_events_before_recovery,
        )
        assert_replay_write_free(
            conn=race_conn,
            runtime=race_runtime,
            readback=race_readback,
            project_id=race_project,
            turn=race_turn,
            operation_id="c12-race-operation",
        )
        assert len(race_readback.calls) == 1
    finally:
        race_conn.close()

    positive_now = [100]
    positive_module, positive_conn, positive_runtime, positive_project, positive_actor = _make_runtime(
        tmp_path / "c12-task7-positive.projects.db",
        clock=lambda: positive_now[0],
    )
    try:
        positive_turn, positive_claim = _enqueue_and_claim(
            positive_runtime, positive_project, positive_actor,
            key="c12-task7-positive",
        )
        positive_claim = positive_runtime.mark_turn_started(positive_claim)
        reconcile_operation(
            runtime=positive_runtime,
            conn=positive_conn,
            project_id=positive_project,
            claim=positive_claim,
            operation_id="c12-positive-operation",
        )
        positive_now[0] = positive_claim.lease_expires_at + 1
        batch_id = "123e4567-e89b-42d3-a456-426614174012"

        class ExactTask7Evidence:
            calls = []

            def read_turn_with_evidence(self, request):
                assert positive_conn.in_transaction is False
                self.calls.append(request)
                return positive_module.Task7TerminalReadbackEvidence(
                    positive_module.TurnReadbackResult("succeeded", batch_id),
                    batch_id,
                )

        evidence = ExactTask7Evidence()
        positive = positive_runtime.reconcile_inflight_turns_with_task7_evidence(
            evidence, limit=100
        )
        assert [turn.status for turn in positive] == ["succeeded"]
        assert len(evidence.calls) == 1
        assert tuple(
            positive_conn.execute(
                """
                SELECT terminal_result_id, transcript_applied_batch_id,
                       recovery_block_key
                FROM project_turns WHERE turn_id = ?
                """,
                (positive_turn.turn_id,),
            ).fetchone()
        ) == (batch_id, None, None)
        assert prdb.runtime_state_for_project(
            positive_conn, positive_project
        ).transcript_pending_batch_id == batch_id
    finally:
        positive_conn.close()
