"""End-to-end recovery proofs for one canonical ProjectRuntime.

These tests deliberately cross process and connection boundaries.  The
agent itself is represented only by the durable start marker and canonical
readback port; projects.db and state.db are real SQLite databases in one
temporary profile home.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_policy import ActorContext
from hermes_cli.project_runtime import (
    CanonicalTurnResult,
    ProjectRuntime,
    ProjectRuntimeError,
    RuntimeErrorCode,
    TurnClaim,
)
from hermes_state import SessionDB
from tests.gateway.project_runtime_test_helpers import (
    ProbeSet,
    release_probes,
    run_probe,
)


_WORKER_PROBE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "project_runtime_worker_probe.py"
)


@dataclass(frozen=True)
class _ProjectProfile:
    home: Path
    projects_path: Path
    state_path: Path
    project_id: str
    session_id: str
    desktop: ActorContext
    discord: ActorContext


def _seed_profile(tmp_path: Path, *, label: str) -> _ProjectProfile:
    home = tmp_path / label
    home.mkdir()
    projects_path = home / "projects.db"
    state_path = home / "state.db"
    session_id = f"{label}-canonical-session"

    connection = projects_db.connect(projects_path)
    try:
        project_id = projects_db.create_project(
            connection,
            name=f"Recovery {label}",
            folders=(str(home),),
        )
        prdb.create_project_conversation(
            connection,
            project_id=project_id,
            conversation_id=session_id,
            current_phase="implementation",
            now=1,
        )
        for surface, binding_id, external_id in (
            ("desktop", f"{label}-desktop", f"{label}-window"),
            ("discord", f"{label}-discord", f"{label}-thread"),
        ):
            prdb.bind_surface(
                connection,
                binding_id=binding_id,
                project_id=project_id,
                surface=surface,
                external_binding_id=external_id,
                actor_id="owner",
                now=1,
            )
    finally:
        connection.close()

    state = SessionDB(db_path=state_path)
    try:
        state.create_session(
            session_id,
            "desktop",
            user_id="owner",
            session_key=session_id,
            cwd=str(home),
            git_repo_root=str(home),
        )
        assert state.set_session_project_id(session_id, project_id)
    finally:
        state.close()

    return _ProjectProfile(
        home=home,
        projects_path=projects_path,
        state_path=state_path,
        project_id=project_id,
        session_id=session_id,
        desktop=ActorContext(
            "owner", "desktop", f"{label}-desktop", True
        ),
        discord=ActorContext(
            "owner", "discord", f"{label}-discord", True
        ),
    )


def _prepare(
    probe_id: str,
    action: str,
    profile: _ProjectProfile,
    worker_id: str,
    now: int,
    **extra: object,
) -> dict[str, object]:
    return {
        "version": 1,
        "event": "prepare",
        "probe_id": probe_id,
        "action": action,
        "db_path": str(profile.projects_path),
        "project_id": profile.project_id,
        "worker_id": worker_id,
        "now": now,
        **extra,
    }


def _crash_after_started(
    probes: ProbeSet,
    profile: _ProjectProfile,
    claim: dict[str, object],
) -> dict[str, object]:
    handle = probes.spawn(
        _prepare(
            "agent-result-before-commit",
            "start",
            profile,
            str(claim["worker_id"]),
            100,
            claim=claim,
            crash_after="start_commit",
        )
    )
    release_probes([handle])
    boundary = handle.expect("boundary")
    handle.complete(returncode=92)
    assert boundary["boundary"] == "start_committed"
    return boundary


def _runtime_versions(connection, project_id: str, turn_id: str):
    state = prdb.runtime_state_for_project(connection, project_id)
    control = prdb._runtime_control_for_turn(
        connection,
        project_id=project_id,
        turn_id=turn_id,
    )
    assert state is not None and control is not None
    return state.version, control.control_version


def _event_rows(
    connection, project_id: str, turn_id: str
) -> list[tuple[str, str]]:
    return [
        (row["kind"], row["payload_json"])
        for row in connection.execute(
            """
            SELECT kind, payload_json
            FROM project_events
            WHERE project_id = ? AND turn_id = ?
            ORDER BY sequence
            """,
            (project_id, turn_id),
        )
    ]


def test_agent_result_crash_before_commit_uses_readback_and_never_blind_success(
    tmp_path,
):
    """A missing terminal commit must require canonical recovery evidence.

    Mutations caught: treating ``started`` as success, losing the durable
    attempt at process exit, skipping readback, or replaying a terminal event.
    """
    profile = _seed_profile(tmp_path, label="agent-crash")
    connection = projects_db.connect(profile.projects_path)
    try:
        runtime = ProjectRuntime(connection, clock=lambda: 100)
        turn = runtime.enqueue_turn(
            profile.project_id,
            {"message": "produce one durable result"},
            profile.desktop,
            idempotency_key="agent-crash-turn",
            expected_version=0,
        )
    finally:
        connection.close()

    with ProbeSet(_WORKER_PROBE) as probes:
        claimed = run_probe(
            probes,
            _prepare(
                "claim-before-agent-crash",
                "claim",
                profile,
                "worker-before-crash",
                100,
                lease_seconds=30,
            ),
        )["claim"]
        assert claimed is not None
        boundary = _crash_after_started(probes, profile, claimed)
        assert boundary["claim"] == claimed

        before_recovery = projects_db.connect(profile.projects_path)
        try:
            stored = prdb._runtime_turn_for_project(
                before_recovery,
                project_id=profile.project_id,
                turn_id=turn.turn_id,
            )
            assert stored is not None
            assert stored.status == "claimed"
            assert stored.execution_state == "started"
            assert not [
                kind
                for kind, _ in _event_rows(
                    before_recovery, profile.project_id, turn.turn_id
                )
                if kind in {"turn.succeeded", "turn.failed"}
            ]
        finally:
            before_recovery.close()

        recovered = run_probe(
            probes,
            _prepare(
                "recover-proven-agent-result",
                "recover",
                profile,
                "recovery-core",
                int(claimed["lease_expires_at"]),
                limit=10,
                readback={
                    "outcome": "succeeded",
                    "result_id": "durable-agent-result",
                },
            ),
        )
        assert recovered["readback_requests"] == [
            {
                "attempt_id": claimed["attempt_id"],
                "canonical_session_id": profile.session_id,
                "execution_state": "started",
                "fencing_token": claimed["fencing_token"],
                "lease_expires_at": claimed["lease_expires_at"],
                "lease_generation": claimed["lease_generation"],
                "project_id": profile.project_id,
                "sequence": 1,
                "source_status": "claimed",
                "turn_id": turn.turn_id,
                "worker_id": "worker-before-crash",
            }
        ]
        assert recovered["turns"] == [
            {"status": "succeeded", "turn_id": turn.turn_id}
        ]

        replay_handle = probes.spawn(
            _prepare(
                "recover-terminal-replay",
                "recover",
                profile,
                "restarted-recovery-core",
                int(claimed["lease_expires_at"]) + 1,
                limit=10,
                readback={
                    "outcome": "succeeded",
                    "result_id": "must-not-replay",
                },
            )
        )
        # With no recovery candidate, the fresh process returns before opening
        # a write transaction.  Requiring the helper's write gate here would
        # turn the absence of replay into a false test failure.
        replay = replay_handle.expect("result")
        replay_handle.complete()
        assert replay == {
            "action": "recover",
            "event": "result",
            "ok": True,
            "probe_id": "recover-terminal-replay",
            "readback_requests": [],
            "turns": [],
            "version": 1,
        }

    check = projects_db.connect(profile.projects_path)
    try:
        terminal = check.execute(
            """
            SELECT status, terminal_result_id
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (profile.project_id, turn.turn_id),
        ).fetchone()
        assert tuple(terminal) == ("succeeded", "durable-agent-result")
        terminal_events = [
            (kind, json.loads(payload))
            for kind, payload in _event_rows(
                check, profile.project_id, turn.turn_id
            )
            if kind in {"turn.succeeded", "turn.failed"}
        ]
        assert len(terminal_events) == 1
        assert terminal_events[0][0] == "turn.succeeded"
        assert terminal_events[0][1]["attempt_id"] == claimed["attempt_id"]
    finally:
        check.close()

    restarted_state = SessionDB(db_path=profile.state_path)
    try:
        assert (
            restarted_state.get_session(profile.session_id)["project_id"]
            == profile.project_id
        )
    finally:
        restarted_state.close()


def test_desktop_stop_restart_discord_resume_rotates_attempt_without_terminal_replay(
    tmp_path,
):
    """Cross-surface resume keeps one turn and fences the pre-stop attempt.

    Mutations caught: creating a replacement turn, retaining the stale fence,
    binding idempotency to one surface, or terminalizing either attempt twice.
    """
    profile = _seed_profile(tmp_path, label="cross-surface")
    connection = projects_db.connect(profile.projects_path)
    try:
        runtime = ProjectRuntime(connection, clock=lambda: 100)
        turn = runtime.enqueue_turn(
            profile.project_id,
            {"message": "stop then continue remotely"},
            profile.desktop,
            idempotency_key="cross-surface-turn",
            expected_version=0,
        )
    finally:
        connection.close()

    with ProbeSet(_WORKER_PROBE) as probes:
        first_claim = run_probe(
            probes,
            _prepare(
                "claim-before-stop",
                "claim",
                profile,
                "desktop-worker",
                100,
                lease_seconds=30,
            ),
        )["claim"]
        assert first_claim is not None
        run_probe(
            probes,
            _prepare(
                "start-before-stop",
                "start",
                profile,
                "desktop-worker",
                100,
                claim=first_claim,
            ),
        )

        desktop_connection = projects_db.connect(profile.projects_path)
        try:
            desktop_runtime = ProjectRuntime(
                desktop_connection, clock=lambda: 100
            )
            stop_versions = _runtime_versions(
                desktop_connection, profile.project_id, turn.turn_id
            )
            stopped = desktop_runtime.request_stop(
                profile.project_id,
                turn.turn_id,
                profile.desktop,
                idempotency_key="stop-from-desktop",
                expected_version=stop_versions[0],
                expected_control_version=stop_versions[1],
            )
            assert stopped.control_state == "stop_requested"
            acknowledged = desktop_runtime.acknowledge_stopped(
                TurnClaim(**first_claim)
            )
            assert acknowledged.control_state == "stopped"
        finally:
            desktop_connection.close()

        discord_connection = projects_db.connect(profile.projects_path)
        try:
            restarted_runtime = ProjectRuntime(
                discord_connection, clock=lambda: 100
            )
            resume_versions = _runtime_versions(
                discord_connection, profile.project_id, turn.turn_id
            )
            resumed = restarted_runtime.request_resume(
                profile.project_id,
                turn.turn_id,
                profile.discord,
                idempotency_key="resume-from-discord",
                expected_version=resume_versions[0],
                expected_control_version=resume_versions[1],
            )
            assert resumed.control_state == "resume_requested"
        finally:
            discord_connection.close()

        second_claim = run_probe(
            probes,
            _prepare(
                "claim-after-discord-resume",
                "claim",
                profile,
                "discord-worker",
                100,
                lease_seconds=30,
            ),
        )["claim"]
        assert second_claim is not None
        assert second_claim["turn_id"] == first_claim["turn_id"] == turn.turn_id
        assert second_claim["attempt_id"] != first_claim["attempt_id"]
        assert (
            second_claim["lease_generation"]
            == first_claim["lease_generation"] + 1
        )
        assert (
            second_claim["fencing_token"]
            == first_claim["fencing_token"] + 1
        )

        stale_connection = projects_db.connect(profile.projects_path)
        try:
            stale_runtime = ProjectRuntime(
                stale_connection, clock=lambda: 100
            )
            before_stale_events = _event_rows(
                stale_connection, profile.project_id, turn.turn_id
            )
            before_stale_state = prdb.runtime_state_for_project(
                stale_connection, profile.project_id
            )
            assert before_stale_state is not None
            with pytest.raises(ProjectRuntimeError) as stale:
                stale_runtime.mark_turn_started(TurnClaim(**first_claim))
            assert stale.value.code is RuntimeErrorCode.STALE_TURN_CLAIM
            stale_batch_id = (
                "66666666-6666-4666-8666-666666666666"
            )
            with pytest.raises(ProjectRuntimeError) as stale_commit:
                stale_runtime.commit_turn_with_task7_batch(
                    TurnClaim(**first_claim),
                    CanonicalTurnResult("succeeded", stale_batch_id),
                    transcript_batch_id=stale_batch_id,
                )
            assert (
                stale_commit.value.code
                is RuntimeErrorCode.STALE_TURN_CLAIM
            )
            after_stale_state = prdb.runtime_state_for_project(
                stale_connection, profile.project_id
            )
            assert after_stale_state is not None
            assert (
                after_stale_state.transcript_pending_batch_id
                == before_stale_state.transcript_pending_batch_id
            )
            assert _event_rows(
                stale_connection, profile.project_id, turn.turn_id
            ) == before_stale_events
        finally:
            stale_connection.close()

        run_probe(
            probes,
            _prepare(
                "start-after-discord-resume",
                "start",
                profile,
                "discord-worker",
                100,
                claim=second_claim,
            ),
        )
        committed = run_probe(
            probes,
            _prepare(
                "commit-after-discord-resume",
                "commit",
                profile,
                "discord-worker",
                100,
                claim=second_claim,
                outcome="succeeded",
                result_id="result-after-resume",
            ),
        )
        assert committed["turn"] == {
            "status": "succeeded",
            "turn_id": turn.turn_id,
        }

    replay_connection = projects_db.connect(profile.projects_path)
    try:
        replay_runtime = ProjectRuntime(
            replay_connection, clock=lambda: 101
        )
        before_events = _event_rows(
            replay_connection, profile.project_id, turn.turn_id
        )
        stop_replay = replay_runtime.request_stop(
            profile.project_id,
            turn.turn_id,
            profile.discord,
            idempotency_key="stop-from-desktop",
            expected_version=stop_versions[0],
            expected_control_version=stop_versions[1],
        )
        resume_replay = replay_runtime.request_resume(
            profile.project_id,
            turn.turn_id,
            profile.desktop,
            idempotency_key="resume-from-discord",
            expected_version=resume_versions[0],
            expected_control_version=resume_versions[1],
        )
        assert stop_replay.control_state == "terminal"
        assert resume_replay == stop_replay
        assert (
            _event_rows(replay_connection, profile.project_id, turn.turn_id)
            == before_events
        )

        stored = prdb._runtime_turn_for_project(
            replay_connection,
            project_id=profile.project_id,
            turn_id=turn.turn_id,
        )
        assert stored is not None
        assert stored.status == "succeeded"
        assert stored.terminal_result_id == "result-after-resume"
        terminal_events = [
            (kind, json.loads(payload))
            for kind, payload in before_events
            if kind in {"turn.succeeded", "turn.failed"}
        ]
        assert len(terminal_events) == 1
        assert terminal_events[0][0] == "turn.succeeded"
        assert (
            terminal_events[0][1]["attempt_id"]
            == second_claim["attempt_id"]
        )
        assert first_claim["attempt_id"] not in terminal_events[0][1].values()
    finally:
        replay_connection.close()
