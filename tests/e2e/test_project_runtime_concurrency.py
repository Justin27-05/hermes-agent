"""Process-level concurrency contract for the canonical project runtime.

The test deliberately uses fresh Python processes and the on-disk profile
databases.  Threaded tests cannot exercise SQLite's inter-process locking or
the ownership boundaries used by a remote gateway and the Desktop at once.
"""

from __future__ import annotations

import json
import multiprocessing
from dataclasses import asdict
from pathlib import Path

import pytest

def _producer(
    db_path: str,
    project_id: str,
    binding_id: str,
    surface: str,
    label: str,
    ready: multiprocessing.Queue,
    release: multiprocessing.synchronize.Event,
    results: multiprocessing.Queue,
) -> None:
    """Enqueue one unique turn after both independent producers are ready."""
    from hermes_cli import project_runtime_db as runtime_db
    from hermes_cli import projects_db
    from hermes_cli.project_policy import ActorContext
    from hermes_cli.project_runtime import (
        ProjectRuntime,
        ProjectRuntimeError,
        RuntimeErrorCode,
    )

    conn = projects_db.connect(Path(db_path))
    try:
        runtime = ProjectRuntime(conn, clock=lambda: 100)
        actor = ActorContext("owner-1", surface, binding_id, True)
        ready.put(("ready", label))
        if not release.wait(timeout=20):
            raise TimeoutError("producer release timed out")

        # Both producers intentionally start from version zero.  The losing
        # CAS owner retries against the durable state instead of dropping its
        # independently submitted turn.
        expected_version = 0
        for _ in range(4):
            try:
                turn = runtime.enqueue_turn(
                    project_id,
                    {"message": label},
                    actor,
                    idempotency_key=f"e2e-producer-{label}",
                    expected_version=expected_version,
                )
                results.put(
                    (
                        "turn",
                        label,
                        {
                            "project_id": turn.project_id,
                            "sequence": turn.sequence,
                            "status": turn.status,
                            "turn_id": turn.turn_id,
                        },
                    )
                )
                return
            except ProjectRuntimeError as exc:
                if exc.code is not RuntimeErrorCode.PROJECT_VERSION_CONFLICT:
                    raise
                state = runtime_db.runtime_state_for_project(conn, project_id)
                assert state is not None
                expected_version = state.version
        raise AssertionError("producer did not converge on the durable version")
    except BaseException as exc:
        results.put(("error", label, f"{type(exc).__name__}: {exc}"))
        raise
    finally:
        conn.close()


def _worker_claim(
    db_path: str,
    project_id: str,
    worker_id: str,
    ready: multiprocessing.Queue,
    release: multiprocessing.synchronize.Event,
    results: multiprocessing.Queue,
) -> None:
    """Attempt one exact FIFO claim from an independent worker process."""
    from hermes_cli import projects_db
    from hermes_cli.project_runtime import ProjectRuntime

    conn = projects_db.connect(Path(db_path))
    try:
        runtime = ProjectRuntime(conn, clock=lambda: 100)
        ready.put(("ready", worker_id))
        if not release.wait(timeout=20):
            raise TimeoutError("worker release timed out")
        claim = runtime.claim_next_turn(
            project_id, worker_id, lease_seconds=60
        )
        results.put(
            ("claim", worker_id, asdict(claim) if claim is not None else None)
        )
    except BaseException as exc:
        results.put(("error", worker_id, f"{type(exc).__name__}: {exc}"))
        raise
    finally:
        conn.close()


def _start_concurrently(
    ctx: multiprocessing.context.BaseContext,
    target,
    process_args: list[tuple[object, ...]],
) -> list[object]:
    """Run children behind one parent-controlled release gate."""
    ready = ctx.Queue()
    results = ctx.Queue()
    release = ctx.Event()
    processes = [
        ctx.Process(
            target=target,
            args=(*args, ready, release, results),
            name=f"project-runtime-e2e-{index}",
        )
        for index, args in enumerate(process_args)
    ]
    for process in processes:
        process.start()
    observed: list[object] = []
    try:
        observed_ready = [ready.get(timeout=20) for _ in processes]
        assert {item[0] for item in observed_ready} == {"ready"}
        release.set()
        observed = [results.get(timeout=20) for _ in processes]
    finally:
        release.set()
        for process in processes:
            process.join(timeout=20)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
        failures = [
            (process.name, process.exitcode)
            for process in processes
            if process.exitcode != 0
        ]
        assert not failures, (
            f"child failures={failures}; results={observed!r}"
        )
    return observed


def test_two_process_producers_and_workers_preserve_one_project_fifo(
    tmp_path,
    monkeypatch,
):
    """Two surface submissions yield one live claim and two ordered terminals.

    A regression here would let two Desktop/Discord producers lose a turn,
    permit two workers to execute one project's queue in parallel, or publish
    duplicate terminal events after the claims are completed.
    """
    from hermes_cli import project_runtime_db as runtime_db
    from hermes_cli import projects_db
    from hermes_cli.project_runtime import (
        CanonicalTurnResult,
        ProjectRuntime,
        TurnClaim,
    )
    from gateway.session import AsyncSessionStore
    from hermes_state import SessionDB

    profile_home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    projects_path = profile_home / "projects.db"
    state_path = profile_home / "state.db"

    # The e2e profile owns both durable stores: ``state.db`` holds the
    # canonical conversation projection, while the queue and fencing live in
    # ``projects.db``.  No in-memory or mocked database participates.
    session_db = SessionDB(db_path=state_path)
    conn = projects_db.connect(projects_path)
    try:
        project_id = projects_db.create_project(
            conn,
            name="E2E concurrent project",
            folders=(str(profile_home / "workspace"),),
        )
        runtime_db.create_project_conversation(
            conn,
            project_id=project_id,
            conversation_id="e2e-session-root",
            current_phase="implementation",
            now=1,
        )
        for surface, binding_id, external_binding_id in (
            ("desktop", "desktop-owner", "desktop-window"),
            ("discord", "discord-owner", "discord-thread"),
        ):
            runtime_db.bind_surface(
                conn,
                binding_id=binding_id,
                project_id=project_id,
                surface=surface,
                external_binding_id=external_binding_id,
                actor_id="owner-1",
                principal_id="owner-1" if surface == "discord" else None,
                now=1,
            )
        session_db.create_session("e2e-session-root", "desktop")
        assert session_db.set_session_project_id(
            "e2e-session-root", project_id
        )
        assert projects_path.exists() and state_path.exists()

        ctx = multiprocessing.get_context("spawn")
        produced = _start_concurrently(
            ctx,
            _producer,
            [
                (
                    str(projects_path),
                    project_id,
                    "desktop-owner",
                    "desktop",
                    "desktop",
                ),
                (
                    str(projects_path),
                    project_id,
                    "discord-owner",
                    "discord",
                    "discord",
                ),
            ],
        )
        turns_by_message = {
            item[1]: item[2]
            for item in produced
            if item[0] == "turn"
        }
        assert set(turns_by_message) == {"desktop", "discord"}
        assert {turn["sequence"] for turn in turns_by_message.values()} == {
            1,
            2,
        }
        assert len({turn["turn_id"] for turn in turns_by_message.values()}) == 2
        message_by_sequence = {
            turn["sequence"]: label
            for label, turn in turns_by_message.items()
        }
        transcript_store = AsyncSessionStore(
            session_db,
            projects_db_factory=lambda: projects_db.connect(projects_path),
        )

        initial_claims = _start_concurrently(
            ctx,
            _worker_claim,
            [
                (str(projects_path), project_id, "worker-a"),
                (str(projects_path), project_id, "worker-b"),
            ],
        )
        nonempty = [item for item in initial_claims if item[2] is not None]
        assert len(nonempty) == 1, initial_claims
        first_claim = nonempty[0][2]
        assert first_claim["sequence"] == 1

        runtime = ProjectRuntime(conn, clock=lambda: 101)
        first = runtime.mark_turn_started(TurnClaim(**first_claim))
        first_batch_id = "11111111-1111-4111-8111-111111111111"
        first_label = message_by_sequence[1]
        session_db.prepare_terminal_result(
            first,
            batch_id=first_batch_id,
            status="succeeded",
            base_message_count=0,
            messages=(
                {
                    "role": "user",
                    "content": json.dumps(
                        {"message": first_label},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
                {
                    "role": "assistant",
                    "content": f"completed:{first_label}",
                },
            ),
        )
        first_terminal = runtime.commit_turn_with_task7_batch(
            first,
            CanonicalTurnResult("succeeded", first_batch_id),
            transcript_batch_id=first_batch_id,
        )
        assert first_terminal.status == "succeeded"
        assert (
            transcript_store._apply_project_batch_sync(
                first_batch_id
            ).outcome
            == "published"
        )

        second_claims = _start_concurrently(
            ctx,
            _worker_claim,
            [(str(projects_path), project_id, "worker-c")],
        )
        assert second_claims[0][2] is not None
        second_claim_payload = second_claims[0][2]
        assert second_claim_payload["sequence"] == 2
        second = runtime.mark_turn_started(TurnClaim(**second_claim_payload))
        second_batch_id = "22222222-2222-4222-8222-222222222222"
        second_label = message_by_sequence[2]
        session_db.prepare_terminal_result(
            second,
            batch_id=second_batch_id,
            status="succeeded",
            base_message_count=2,
            messages=(
                {
                    "role": "user",
                    "content": json.dumps(
                        {"message": second_label},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
                {
                    "role": "assistant",
                    "content": f"completed:{second_label}",
                },
            ),
        )
        second_terminal = runtime.commit_turn_with_task7_batch(
            second,
            CanonicalTurnResult("succeeded", second_batch_id),
            transcript_batch_id=second_batch_id,
        )
        assert second_terminal.status == "succeeded"
        assert (
            transcript_store._apply_project_batch_sync(
                second_batch_id
            ).outcome
            == "published"
        )

        # Durable queue order, terminal history, and the canonical state
        # transcript all converge through the worker's prepare -> Projects
        # terminal CAS -> State publish protocol.
        rows = conn.execute(
            """
            SELECT sequence, status, terminal_result_id
            FROM project_turns
            WHERE project_id = ?
            ORDER BY sequence
            """,
            (project_id,),
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            (1, "succeeded", first_batch_id),
            (2, "succeeded", second_batch_id),
        ]
        terminal_events = conn.execute(
            """
            SELECT turn_id, kind, COUNT(*)
            FROM project_events
            WHERE project_id = ?
              AND kind IN ('turn.succeeded', 'turn.failed')
            GROUP BY turn_id, kind
            ORDER BY turn_id
            """,
            (project_id,),
        ).fetchall()
        assert sorted(tuple(row) for row in terminal_events) == sorted(
            [
                (first_terminal.turn_id, "turn.succeeded", 1),
                (second_terminal.turn_id, "turn.succeeded", 1),
            ]
        )
        messages = session_db.get_messages("e2e-session-root")
        assert [
            (message["role"], message["content"])
            for message in messages
        ] == [
            (
                "user",
                json.dumps(
                    {"message": first_label},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
            ("assistant", f"completed:{first_label}"),
            (
                "user",
                json.dumps(
                    {"message": second_label},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
            ("assistant", f"completed:{second_label}"),
        ]
    finally:
        conn.close()
        session_db.close()


@pytest.mark.asyncio
async def test_canonical_worker_publishes_one_transcript_pair_per_surface_turn(
    tmp_path,
    monkeypatch,
):
    """Production worker wiring publishes exactly one pair for both surfaces."""
    import asyncio

    from gateway.config import GatewayConfig
    from gateway.project_runtime_worker import (
        CanonicalProjectRuntimeWorker,
        ProjectAgentRevisions,
        ProjectAgentRunResult,
        ProjectRuntimeWorkerFacade,
    )
    from gateway.session import ProjectBatchWorkerFacade
    from hermes_cli import project_runtime_db as runtime_db
    from hermes_cli import projects_db
    from hermes_cli.project_policy import ActorContext
    from hermes_cli.project_runtime import ProjectRuntime
    from hermes_state import SessionDB

    profile_home = tmp_path / "worker-profile"
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    projects_path = profile_home / "projects.db"
    state_path = profile_home / "state.db"
    session_id = "worker-e2e-session"
    projects = projects_db.connect(projects_path)
    state = SessionDB(db_path=state_path)
    try:
        project_id = projects_db.create_project(
            projects,
            name="Canonical worker transcript",
            folders=(str(profile_home / "workspace"),),
        )
        runtime_db.create_project_conversation(
            projects,
            project_id=project_id,
            conversation_id=session_id,
            current_phase="implementation",
            now=1,
        )
        for surface, binding_id, external_id in (
            ("desktop", "worker-desktop", "desktop-window"),
            ("discord", "worker-discord", "discord-thread"),
        ):
            runtime_db.bind_surface(
                projects,
                binding_id=binding_id,
                project_id=project_id,
                surface=surface,
                external_binding_id=external_id,
                actor_id="owner-1",
                principal_id=(
                    "owner-1" if surface == "discord" else None
                ),
                now=1,
            )
        state.create_session(session_id, "desktop")
        assert state.set_session_project_id(session_id, project_id)

        runtime = ProjectRuntime(projects)
        desktop_turn = runtime.enqueue_turn(
            project_id,
            {"message": "desktop work"},
            ActorContext(
                "owner-1", "desktop", "worker-desktop", True
            ),
            idempotency_key="worker-desktop-message",
            expected_version=0,
        )
        discord_turn = runtime.enqueue_turn(
            project_id,
            {"message": "discord work"},
            ActorContext(
                "owner-1", "discord", "worker-discord", True
            ),
            idempotency_key="worker-discord-message",
            expected_version=1,
        )
        dispatcher_lease = runtime.acquire_dispatcher_lease(
            "33333333-3333-4333-8333-333333333333",
            lease_seconds=60,
        )
        assert dispatcher_lease is not None

        async def io_runner(function, *args, **kwargs):
            return await asyncio.to_thread(
                function, *args, **kwargs
            )

        def projects_factory():
            return projects_db.connect(projects_path)

        def state_factory():
            return SessionDB(db_path=state_path)

        batches = ProjectBatchWorkerFacade(
            state_factory,
            projects_db_factory=projects_factory,
            io_runner=io_runner,
        )

        class Turn:
            def __init__(self, execution, base_message_count):
                self._execution = execution
                self._base_message_count = base_message_count

            def request_cancel(self):
                return True

            async def wait_quiescent(self):
                return None

            async def result(self):
                message = json.dumps(
                    dict(self._execution.payload),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                return ProjectAgentRunResult(
                    "succeeded",
                    self._base_message_count,
                    (
                        {"role": "user", "content": message},
                        {
                            "role": "assistant",
                            "content": f"completed:{message}",
                        },
                    ),
                )

        class Agent:
            def __init__(self, message_count):
                self._message_count = message_count

            def create_turn(self, execution, operation):
                assert operation is None
                turn = Turn(execution, self._message_count)
                self._message_count += 2
                return turn

        class Build:
            revisions = ProjectAgentRevisions(
                "e2e-base", "e2e-tools", "e2e-model"
            )

            async def create_project_agent(self, *, history):
                return Agent(history.message_count)

        class Factory:
            def __init__(self):
                self.released = []

            async def resolve_project_agent(
                self, *, context, contract_revision
            ):
                assert context.session_id == session_id
                assert contract_revision >= 0
                return Build()

            async def release_project_agent(self, agent):
                self.released.append(agent)

        factory = Factory()
        batch_ids = (
            "44444444-4444-4444-8444-444444444444",
            "55555555-5555-4555-8555-555555555555",
        )
        worker = CanonicalProjectRuntimeWorker(
            ProjectRuntimeWorkerFacade(
                projects_factory,
                io_runner=io_runner,
            ),
            batches,
            factory,
            GatewayConfig(),
            profile_home=profile_home,
            lease_seconds=60,
            heartbeat_interval_seconds=30,
            batch_id_factory=iter(batch_ids).__next__,
        )
        try:
            first_start = runtime.claim_next_turn_for_dispatcher(
                project_id,
                "canonical-worker",
                lease_seconds=60,
                dispatcher_lease=dispatcher_lease,
            )
            assert first_start is not None
            assert first_start.claim.turn_id == desktop_turn.turn_id
            await worker.run_start(first_start)

            second_start = runtime.claim_next_turn_for_dispatcher(
                project_id,
                "canonical-worker",
                lease_seconds=60,
                dispatcher_lease=dispatcher_lease,
            )
            assert second_start is not None
            assert second_start.claim.turn_id == discord_turn.turn_id
            await worker.run_start(second_start)
        finally:
            await worker.close()

        batches_rows = state._conn.execute(
            """
            SELECT turn_id, state, base_message_count, transcript_json
            FROM project_turn_transcript_batches
            ORDER BY batch_creation_sequence
            """
        ).fetchall()
        assert len(batches_rows) == 2
        assert [
            (row["turn_id"], row["state"], row["base_message_count"])
            for row in batches_rows
        ] == [
            (desktop_turn.turn_id, "published", 0),
            (discord_turn.turn_id, "published", 2),
        ]
        transcript_rows = [
            json.loads(row["transcript_json"]) for row in batches_rows
        ]
        assert [
            [message["role"] for message in transcript]
            for transcript in transcript_rows
        ] == [["user", "assistant"], ["user", "assistant"]]

        messages = state.get_messages(session_id)
        assert [message["role"] for message in messages] == [
            "user",
            "assistant",
            "user",
            "assistant",
        ]
        assert [
            (message["role"], message["content"])
            for message in messages
        ] == [
            (message["role"], message["content"])
            for transcript in transcript_rows
            for message in transcript
        ]
        terminal_rows = projects.execute(
            """
            SELECT turn_id, transcript_applied_batch_id
            FROM project_turns
            WHERE project_id = ?
            ORDER BY sequence
            """,
            (project_id,),
        ).fetchall()
        assert [tuple(row) for row in terminal_rows] == [
            (desktop_turn.turn_id, batch_ids[0]),
            (discord_turn.turn_id, batch_ids[1]),
        ]
        runtime_state = runtime_db.runtime_state_for_project(
            projects, project_id
        )
        assert runtime_state is not None
        assert runtime_state.transcript_pending_batch_id is None
        before_replay = state.get_messages(session_id)
        assert (
            await batches.apply_project_batch(batch_ids[0])
        ).outcome == "already_published"
        assert state.get_messages(session_id) == before_replay
    finally:
        projects.close()
        state.close()
