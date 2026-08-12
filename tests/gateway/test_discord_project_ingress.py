"""Contract tests for Discord messages entering managed projects."""

from pathlib import Path
from types import SimpleNamespace
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent
from gateway.project_runtime_ingress import ProjectRuntimeIngress
from gateway.project_surfaces import DiscordProjectSurface
from gateway.session import SessionSource
from gateway.slash_commands import ProjectSlashCommand
from hermes_cli import project_runtime_db as runtime_db
from hermes_cli import projects_db
from hermes_cli.project_command_service import ProjectCommandService
from hermes_cli.project_policy import ActorContext
from hermes_cli.project_runtime import ProjectRuntime


def _seed_bound_project(path: Path) -> tuple[str, str]:
    connection = projects_db.connect(path)
    try:
        project_id = projects_db.create_project(
            connection,
            name="Discord ingress contract",
            folders=(str(path.parent),),
        )
        runtime_db.create_project_conversation(
            connection,
            project_id=project_id,
            conversation_id="canonical-project-session",
            current_phase="implementation",
            now=1,
        )
        runtime_db.bind_surface(
            connection,
            binding_id="discord-owner",
            project_id=project_id,
            surface="discord",
            external_binding_id="discord-channel-1",
            actor_id="owner",
            principal_id="discord-owner-1",
            now=1,
        )
        return project_id, "discord-owner"
    finally:
        connection.close()


def _owner_message(message_id: str = "discord-message-1") -> MessageEvent:
    source = SessionSource(
        platform=Platform.DISCORD,
        chat_id="discord-channel-1",
        chat_type="group",
        user_id="discord-owner-1",
        message_id=message_id,
    )
    event = MessageEvent(
        text="Voer de volgende projectstap uit",
        source=source,
        message_id=message_id,
    )
    event.raw_message = SimpleNamespace(
        guild=SimpleNamespace(id="guild-1"),
        channel=SimpleNamespace(category_id="active-category"),
    )
    return event


def test_replay_after_restart_is_deduplicated_by_binding_and_discord_message_id(
    tmp_path,
):
    """Removing the durable binding from the key must create a new contract failure."""
    database_path = tmp_path / "projects.db"
    project_id, binding_id = _seed_bound_project(database_path)

    first = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    ).route(_owner_message())
    replay = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    ).route(_owner_message())

    assert first.accepted is True
    assert replay.turn_id == first.turn_id
    connection = projects_db.connect(database_path)
    try:
        turns = runtime_db._queued_turns_for_project(
            connection, project_id=project_id
        )
        assert len(turns) == 1
        assert turns[0].origin_binding_id == binding_id
        assert turns[0].idempotency_key == (
            "discord-message:discord-owner:discord-message-1"
        )
    finally:
        connection.close()


def test_complete_command_only_accepts_an_awaiting_project_from_the_bound_owner(
    tmp_path,
):
    """Skipping lifecycle confirmation must never create a completion event."""
    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    )
    command = ProjectSlashCommand("project.accept_completion", {})

    rejected = ingress.route_command(_owner_message("complete-1"), command)

    assert rejected.handled is True
    assert rejected.accepted is False
    connection = projects_db.connect(database_path)
    try:
        assert connection.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'project.completion_accepted'
            """,
            (project_id,),
        ).fetchone()[0] == 0
        runtime = ProjectRuntime(connection, clock=lambda: 100)
        dispatcher = runtime.acquire_dispatcher_lease(
            "b7090781-62d6-4e63-91ac-d32efad3b99e", lease_seconds=60
        )
        assert dispatcher is not None
        runtime.mark_technically_complete(
            project_id,
            dispatcher,
            idempotency_key="technical-complete-1",
            expected_version=0,
        )
    finally:
        connection.close()

    accepted = ingress.route_command(_owner_message("complete-2"), command)

    assert accepted.handled is True
    assert accepted.accepted is True
    connection = projects_db.connect(database_path)
    try:
        assert runtime_db.runtime_state_for_project(
            connection, project_id
        ).lifecycle == "completed"
        assert connection.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'project.completion_accepted'
            """,
            (project_id,),
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_mutating_project_command_replay_uses_the_discord_event_identity(
    tmp_path,
):
    """Replaying one Discord command must not recalculate a new mutation."""
    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    command = ProjectSlashCommand("project.rename", {"name": "Renamed"})

    first = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    ).route_command(_owner_message("rename-replay-1"), command)
    replay = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    ).route_command(_owner_message("rename-replay-1"), command)

    assert first.accepted is True
    assert replay.accepted is True
    connection = projects_db.connect(database_path)
    try:
        project = projects_db.get_project(connection, project_id)
        assert project is not None
        assert project.name == "Renamed"
        assert connection.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'project.renamed'
            """,
            (project_id,),
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_approval_command_replay_survives_restart_and_version_drift(
    tmp_path,
):
    database_path = tmp_path / "projects.db"
    project_id, binding_id = _seed_bound_project(database_path)
    approval_id = "approval-restart-safe"
    connection = projects_db.connect(database_path)
    try:
        runtime_db.create_approval_request(
            connection,
            runtime_db.ApprovalRequest(
                approval_id=approval_id,
                project_id=project_id,
                requester_actor_id="owner",
                authorization_actor_id="owner",
                canonical_action="publish",
                approval_class="publish",
                command_revision=1,
                expected_runtime_version=0,
                expected_lifecycle="active",
                expected_phase="implementation",
                targets=("C:/work/runtime/release",),
                batch_id="approval-batch",
                batch_items=("publish",),
                status="pending",
                expires_at=4_000_000_000,
            ),
            now=10,
        )
    finally:
        connection.close()
    command = ProjectSlashCommand(
        "approval.resolve",
        {"approval_id": approval_id, "outcome": "approved"},
    )
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    )

    first = ingress.route_command(
        _owner_message("approval-response-1"),
        command,
    )
    assert first.accepted is True

    connection = projects_db.connect(database_path)
    try:
        ProjectRuntime(connection, clock=lambda: 100).rename_project(
            project_id,
            "Version advanced",
            ActorContext(
                "owner",
                "discord",
                binding_id,
                True,
            ),
            idempotency_key="unrelated-version-drift",
            expected_version=0,
        )
    finally:
        connection.close()

    replay = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    ).route_command(
        _owner_message("approval-response-1"),
        command,
    )

    assert replay.accepted is True
    connection = projects_db.connect(database_path)
    try:
        row = connection.execute(
            """
            SELECT status, resolved_by_actor_id
            FROM project_approvals
            WHERE project_id = ? AND approval_id = ?
            """,
            (project_id, approval_id),
        ).fetchone()
        assert tuple(row) == ("approved", "owner")
        assert connection.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'approval.resolved'
            """,
            (project_id,),
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_native_and_typed_discord_commands_use_equivalent_service_ingress(
    tmp_path,
):
    import discord

    from gateway.slash_commands import parse_project_slash_command
    from plugins.platforms.discord.adapter import DiscordAdapter

    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    gateway_config = GatewayConfig.from_dict(
        {
            "platforms": {
                "discord": {
                    "enabled": True,
                    "allow_from": ["discord-owner-1"],
                    "project_workspaces": {
                        "enabled": True,
                        "guild_id": "guild-1",
                        "owner_user_id": "discord-owner-1",
                        "active_category_id": "active-category",
                        "completed_category_id": "completed-category",
                    },
                }
            }
        }
    )
    adapter = DiscordAdapter(gateway_config.platforms[Platform.DISCORD])
    channel = SimpleNamespace(
        id="discord-channel-1",
        name="project",
        category_id="active-category",
        guild=SimpleNamespace(id="guild-1", name="guild"),
    )
    author = SimpleNamespace(
        id="discord-owner-1",
        display_name="owner",
        bot=False,
    )
    typed_raw = SimpleNamespace(
        id="typed-status-1",
        content="/project status",
        channel=channel,
        guild=channel.guild,
        author=author,
        attachments=[],
        created_at=None,
        type=discord.MessageType.default,
    )
    typed_event = adapter._managed_project_message_event(typed_raw)
    assert typed_event is not None
    native_event = adapter._build_slash_event(
        SimpleNamespace(
            id="native-status-1",
            channel=channel,
            channel_id=channel.id,
            guild_id="guild-1",
            user=author,
        ),
        "/project status",
    )
    surface = DiscordProjectSurface(
        guild_id="guild-1",
        owner_user_id="discord-owner-1",
        active_category_id="active-category",
        completed_category_id="completed-category",
    )
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=surface,
    )

    typed_reserved = ingress.route(typed_event)
    typed = ingress.route_command(
        typed_event,
        parse_project_slash_command(typed_event.text),
    )
    native_reserved = ingress.route(native_event)
    native = ingress.route_command(
        native_event,
        parse_project_slash_command(native_event.text),
    )

    assert typed_reserved.handled is False
    assert native_reserved.handled is False
    assert (typed.accepted, typed.project_id, typed.error_code) == (
        native.accepted,
        native.project_id,
        native.error_code,
    ) == (True, project_id, None)

    rename_event = adapter._build_slash_event(
        SimpleNamespace(
            id="native-rename-1",
            channel=channel,
            channel_id=channel.id,
            guild_id="guild-1",
            user=author,
        ),
        "/project rename Native Rename",
    )
    rename_command = parse_project_slash_command(rename_event.text)
    first = ingress.route_command(rename_event, rename_command)
    replay = ingress.route_command(rename_event, rename_command)

    assert first.accepted is True
    assert replay.accepted is True
    connection = projects_db.connect(database_path)
    try:
        assert connection.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'project.renamed'
            """,
            (project_id,),
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_actual_discord_stop_ingress_replays_idempotently_from_desktop(
    tmp_path,
):
    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    )
    queued = ingress.route(_owner_message("cross-surface-turn-1"))
    assert queued.accepted is True
    connection = projects_db.connect(database_path)
    try:
        runtime_db.bind_surface(
            connection,
            binding_id="desktop-owner",
            project_id=project_id,
            surface="desktop",
            external_binding_id="local-profile",
            actor_id="owner",
            now=2,
        )
        runtime = ProjectRuntime(connection)
        claim = runtime.claim_next_turn(
            project_id,
            "cross-surface-worker",
            lease_seconds=30,
        )
        assert claim is not None
        turn = runtime_db._runtime_turn_for_project(
            connection,
            project_id=project_id,
            turn_id=queued.turn_id,
        )
        version_before_stop = runtime.snapshot_for_actor(
            project_id,
            ActorContext(
                "owner",
                "desktop",
                "desktop-owner",
                True,
            ),
        ).version
    finally:
        connection.close()
    stop_event = _owner_message("cross-surface-stop-1")
    idempotency_key = (
        "discord-command:discord-owner:"
        "cross-surface-stop-1:run.stop"
    )
    discord_stop = ingress.route_command(
        stop_event,
        ProjectSlashCommand(
            "run.stop",
            {
                "turn_id": turn.turn_id,
                "expected_control_version": 1,
            },
        ),
    )
    assert discord_stop.accepted is True

    connection = projects_db.connect(database_path)
    try:
        service = ProjectCommandService(runtime=ProjectRuntime(connection))
        desktop_replay = service.dispatch(
            "run.stop",
            project_id=project_id,
            payload={
                "turn_id": turn.turn_id,
                "expected_control_version": 1,
            },
            actor=ActorContext(
                "owner",
                "desktop",
                "desktop-owner",
                True,
            ),
            idempotency_key=idempotency_key,
            expected_version=version_before_stop,
        )

        assert not hasattr(desktop_replay, "code")
        assert connection.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'run.stop_requested'
            """,
            (project_id,),
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_project_create_fails_before_service_mutates_without_provisioning(
    tmp_path,
):
    database_path = tmp_path / "projects.db"
    _seed_bound_project(database_path)
    connection = projects_db.connect(database_path)
    try:
        before = (
            connection.execute("SELECT COUNT(*) FROM projects").fetchone()[0],
            connection.execute(
                "SELECT COUNT(*) FROM project_events"
            ).fetchone()[0],
        )
    finally:
        connection.close()

    result = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    ).route_command(
        _owner_message("create-without-provisioning-1"),
        ProjectSlashCommand(
            "project.create",
            {"name": "Must Not Exist", "current_phase": "planning"},
        ),
    )

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == "PROJECT_INGRESS_PROVISIONING_REQUIRED"
    connection = projects_db.connect(database_path)
    try:
        after = (
            connection.execute("SELECT COUNT(*) FROM projects").fetchone()[0],
            connection.execute(
                "SELECT COUNT(*) FROM project_events"
            ).fetchone()[0],
        )
    finally:
        connection.close()
    assert after == before


def test_bound_discord_bot_delivery_never_reenters_as_a_project_turn(tmp_path):
    """Treating a bot delivery as its owner's text would recurse into Hermes."""
    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    event = _owner_message("bot-output-1")
    event.source.is_bot = True

    result = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    ).route(event)

    assert result.handled is True
    assert result.accepted is False
    connection = projects_db.connect(database_path)
    try:
        assert runtime_db._queued_turns_for_project(
            connection, project_id=project_id
        ) == ()
    finally:
        connection.close()


def test_unbound_owner_message_in_managed_scope_fails_closed(tmp_path):
    """Configured project scope must never fall through as a legacy chat."""
    database_path = tmp_path / "projects.db"
    _seed_bound_project(database_path)
    event = _owner_message("unbound-managed-channel-1")
    event.source.chat_id = "unbound-managed-channel"

    result = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    ).route(event)

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == "PROJECT_INGRESS_UNBOUND_CHANNEL"


@pytest.mark.parametrize(
    ("user_id", "category_id", "is_bot"),
    [
        ("different-discord-user", "active-category", False),
        ("discord-owner-1", "completed-category", False),
        ("discord-owner-1", "active-category", True),
    ],
)
def test_reserved_workspace_location_fails_closed_before_binding_or_actor_checks(
    tmp_path,
    user_id,
    category_id,
    is_bot,
):
    database_path = tmp_path / "projects.db"
    _seed_bound_project(database_path)
    event = _owner_message(f"reserved-{user_id}-{category_id}-{is_bot}")
    event.source.chat_id = "unbound-managed-channel"
    event.source.user_id = user_id
    event.source.is_bot = is_bot
    event.raw_message.channel.category_id = category_id

    result = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    ).route(event)

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == "PROJECT_INGRESS_UNBOUND_CHANNEL"


def test_unbound_channel_outside_reserved_categories_remains_legacy(tmp_path):
    database_path = tmp_path / "projects.db"
    _seed_bound_project(database_path)
    event = _owner_message("legacy-category-1")
    event.source.chat_id = "unbound-legacy-channel"
    event.raw_message.channel.category_id = "ordinary-category"

    result = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    ).route(event)

    assert result.handled is False


@pytest.mark.parametrize("route_kind", ("message", "command"))
def test_private_managed_candidate_without_binding_fails_closed(
    tmp_path,
    route_kind,
):
    database_path = tmp_path / "projects.db"
    _seed_bound_project(database_path)
    event = _owner_message(f"unbound-private-candidate-{route_kind}")
    event.source.chat_id = "unbound-marker-channel"
    event.raw_message.channel.category_id = "ordinary-category"
    setattr(event, "_hermes_managed_project_candidate", True)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    )

    if route_kind == "command":
        result = ingress.route_command(
            event,
            ProjectSlashCommand("project.status", {}),
        )
    else:
        result = ingress.route(event)

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == "PROJECT_INGRESS_UNBOUND_CHANNEL"


@pytest.mark.parametrize(
    ("text", "metadata"),
    (
        ("_hermes_managed_project_candidate", None),
        ("ordinary user text", {"_hermes_managed_project_candidate": True}),
    ),
)
def test_user_payload_cannot_claim_private_managed_candidate(
    tmp_path,
    text,
    metadata,
):
    database_path = tmp_path / "projects.db"
    _seed_bound_project(database_path)
    event = _owner_message("unbound-private-candidate-spoof")
    event.source.chat_id = "unbound-legacy-channel"
    event.raw_message.channel.category_id = "ordinary-category"
    event.text = text
    event.metadata = metadata

    result = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    ).route(event)

    assert not hasattr(event, "_hermes_managed_project_candidate")
    assert result.handled is False


@pytest.mark.parametrize(
    ("user_id", "category_id"),
    [
        ("different-discord-user", "active-category"),
        ("discord-owner-1", "completed-category"),
    ],
)
def test_reserved_workspace_command_location_fails_closed_without_binding(
    tmp_path,
    user_id,
    category_id,
):
    database_path = tmp_path / "projects.db"
    _seed_bound_project(database_path)
    event = _owner_message(f"unbound-command-{user_id}-{category_id}")
    event.source.chat_id = "unbound-managed-command-channel"
    event.source.user_id = user_id
    event.raw_message.channel.category_id = category_id

    result = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    ).route_command(
        event,
        ProjectSlashCommand("project.status", {}),
    )

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == "PROJECT_INGRESS_UNBOUND_CHANNEL"


def test_completed_category_uses_canonical_lifecycle_checks_and_allows_reopen(
    tmp_path,
):
    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    )
    connection = projects_db.connect(database_path)
    try:
        runtime = ProjectRuntime(connection, clock=lambda: 100)
        dispatcher = runtime.acquire_dispatcher_lease(
            "2abdb5f2-1a44-42fd-b58d-25e4aa971419",
            lease_seconds=60,
        )
        assert dispatcher is not None
        runtime.mark_technically_complete(
            project_id,
            dispatcher,
            idempotency_key="technical-complete-for-category",
            expected_version=0,
        )
    finally:
        connection.close()
    completed = ingress.route_command(
        _owner_message("accept-for-category"),
        ProjectSlashCommand("project.accept_completion", {}),
    )
    assert completed.accepted is True

    status_event = _owner_message("completed-status-1")
    status_event.raw_message.channel.category_id = "completed-category"
    status = ingress.route_command(
        status_event,
        ProjectSlashCommand("project.status", {}),
    )
    blocked_event = _owner_message("completed-turn-1")
    blocked_event.raw_message.channel.category_id = "completed-category"
    blocked_turn = ingress.route(blocked_event)
    reopen_event = _owner_message("completed-reopen-1")
    reopen_event.raw_message.channel.category_id = "completed-category"
    reopened = ingress.route_command(
        reopen_event,
        ProjectSlashCommand("project.reopen", {}),
    )

    assert status.accepted is True
    assert blocked_turn.handled is True
    assert blocked_turn.accepted is False
    assert blocked_turn.error_code != "PROJECT_INGRESS_SURFACE_NOT_AUTHORIZED"
    assert reopened.accepted is True
    connection = projects_db.connect(database_path)
    try:
        assert runtime_db.runtime_state_for_project(
            connection,
            project_id,
        ).lifecycle == "active"
        assert runtime_db._queued_turns_for_project(
            connection,
            project_id=project_id,
        ) == ()
    finally:
        connection.close()


def test_managed_project_slash_control_is_reserved_for_command_router(tmp_path):
    """The user-turn ingress must leave valid controls to route_command()."""
    database_path = tmp_path / "projects.db"
    _seed_bound_project(database_path)
    event = _owner_message("project-status-reserved-1")
    event.text = "/project status"

    result = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path)
    ).route(event)

    assert result.handled is False


@pytest.mark.asyncio
async def test_gateway_routes_bound_project_slash_command_to_canonical_ingress(
    tmp_path, monkeypatch
):
    """The managed `/project` route must not fall through to a user turn."""
    from gateway.run import GatewayRunner

    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    original_connect = projects_db.connect
    monkeypatch.setattr(
        projects_db,
        "connect",
        lambda *args, **kwargs: original_connect(database_path),
    )
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig.from_dict(
        {
            "platforms": {
                "discord": {
                    "enabled": True,
                    "allow_from": ["discord-owner-1"],
                    "project_workspaces": {
                        "enabled": True,
                        "guild_id": "guild-1",
                        "owner_user_id": "discord-owner-1",
                        "active_category_id": "active-category",
                        "completed_category_id": "completed-category",
                    },
                }
            }
        }
    )
    runner._project_runtime_dispatcher = None

    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    runner._run_project_runtime_io = run_inline
    event = _owner_message("project-status-1")
    event.text = "/project status"

    result = await runner._route_bound_project_command(event)

    assert result.handled is True
    assert result.accepted is True
    assert result.project_id == project_id


@pytest.mark.asyncio
async def test_managed_project_control_bypasses_base_active_session_queue():
    """A valid project control must reach the runner even while another turn runs."""

    class StubAdapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect: bool = False):
            return None

        async def disconnect(self):
            return None

        async def send(self, chat_id, text, **kwargs):
            return None

        async def get_chat_info(self, chat_id):
            return {}

    from gateway.session import build_session_key
    from gateway.config import PlatformConfig

    adapter = StubAdapter(
        PlatformConfig(enabled=True, token="test-token"), Platform.DISCORD
    )
    adapter._busy_text_mode = ""
    event = _owner_message("busy-project-status-1")
    event.text = "/project status"
    session_key = build_session_key(event.source)
    adapter._active_sessions[session_key] = asyncio.Event()
    handled: list[str] = []
    responses: list[str] = []

    async def message_handler(received):
        handled.append(received.text)
        return "project control accepted"

    async def send_with_retry(chat_id, content, **kwargs):
        responses.append(content)

    adapter._message_handler = message_handler
    adapter._send_with_retry = send_with_retry

    await adapter.handle_message(event)

    assert adapter._pending_messages == {}
    assert handled == ["/project status"]
    assert responses == ["project control accepted"]


@pytest.mark.asyncio
async def test_trusted_project_candidate_bypasses_base_active_session_queue():
    """A project user turn must reach canonical ingress without local queuing."""

    class StubAdapter(BasePlatformAdapter):
        async def connect(self, *, is_reconnect: bool = False):
            return None

        async def disconnect(self):
            return None

        async def send(self, chat_id, text, **kwargs):
            return None

        async def get_chat_info(self, chat_id):
            return {}

    from gateway.config import PlatformConfig
    from gateway.session import build_session_key

    adapter = StubAdapter(
        PlatformConfig(enabled=True, token="test-token"), Platform.DISCORD
    )
    event = _owner_message("busy-project-turn-1")
    setattr(event, "_hermes_managed_project_candidate", True)
    session_key = build_session_key(event.source)
    adapter._active_sessions[session_key] = asyncio.Event()
    adapter._busy_handler = AsyncMock()
    handled: list[str] = []

    async def message_handler(received):
        handled.append(received.text)
        return "project turn accepted"

    adapter._message_handler = message_handler
    adapter._send_with_retry = AsyncMock()

    await adapter.handle_message(event)

    assert adapter._pending_messages == {}
    assert handled == ["Voer de volgende projectstap uit"]
    adapter._busy_handler.assert_not_awaited()


@pytest.mark.asyncio
async def test_project_control_short_circuits_runner_busy_queues():
    """Control handling occurs before the runner can interrupt or queue a turn."""
    from gateway.project_runtime_ingress import ProjectIngressResult
    from gateway.run import GatewayRunner
    from gateway.session import build_session_key

    runner = object.__new__(GatewayRunner)
    runner.config = MagicMock()
    runner.session_store = None
    runner._scale_to_zero_note_real_inbound = MagicMock()
    runner._is_user_authorized = lambda _source: True
    runner._route_bound_project_message = AsyncMock(
        return_value=ProjectIngressResult(handled=False)
    )
    runner._route_bound_project_command = AsyncMock(
        return_value=ProjectIngressResult(
            handled=True,
            accepted=True,
            response="Projectcommando geaccepteerd.",
        )
    )
    event = _owner_message("runner-busy-project-status-1")
    event.text = "/project status"
    session_key = build_session_key(event.source)
    active_agent = MagicMock()
    adapter = MagicMock()
    adapter._pending_messages = {}
    runner._running_agents = {session_key: active_agent}
    runner._pending_messages = {}
    runner.adapters = {Platform.DISCORD: adapter}

    result = await GatewayRunner._handle_message(runner, event)

    assert result == "Projectcommando geaccepteerd."
    assert runner._pending_messages == {}
    assert adapter._pending_messages == {}
    active_agent.interrupt.assert_not_called()


@pytest.mark.asyncio
async def test_trusted_project_turn_reaches_real_ingress_before_runner_busy_paths(
    tmp_path,
    monkeypatch,
):
    from gateway.run import GatewayRunner
    from gateway.session import build_session_key

    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    original_connect = projects_db.connect
    monkeypatch.setattr(
        projects_db,
        "connect",
        lambda *args, **kwargs: original_connect(database_path),
    )
    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "discord-owner-1")
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig.from_dict(
        {
            "platforms": {
                "discord": {
                    "enabled": True,
                    "allow_from": ["discord-owner-1"],
                    "project_workspaces": {
                        "enabled": True,
                        "guild_id": "guild-1",
                        "owner_user_id": "discord-owner-1",
                        "active_category_id": "active-category",
                        "completed_category_id": "completed-category",
                    },
                }
            }
        }
    )
    runner.session_store = None
    runner._project_runtime_dispatcher = None
    runner._scale_to_zero_note_real_inbound = MagicMock()
    runner._is_user_authorized = lambda _source: True

    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    runner._run_project_runtime_io = run_inline
    event = _owner_message("runner-busy-project-turn-1")
    setattr(event, "_hermes_managed_project_candidate", True)
    session_key = build_session_key(event.source)
    active_agent = MagicMock()
    active_agent.steer = MagicMock(return_value=True)
    active_agent.interrupt = MagicMock()
    adapter = MagicMock()
    adapter._pending_messages = {}
    runner._running_agents = {session_key: active_agent}
    runner._pending_messages = {}
    runner.adapters = {Platform.DISCORD: adapter}
    runner._queue_or_replace_pending_event = MagicMock()
    runner._enqueue_fifo = MagicMock()
    runner._busy_input_mode = "steer"

    result = await GatewayRunner._handle_message(runner, event)

    assert result == "Projecttaak toegevoegd aan Hermes."
    assert runner._pending_messages == {}
    assert adapter._pending_messages == {}
    runner._queue_or_replace_pending_event.assert_not_called()
    runner._enqueue_fifo.assert_not_called()
    active_agent.steer.assert_not_called()
    active_agent.interrupt.assert_not_called()
    connection = projects_db.connect(database_path)
    try:
        turns = runtime_db._queued_turns_for_project(
            connection,
            project_id=project_id,
        )
    finally:
        connection.close()
    assert len(turns) == 1


@pytest.mark.parametrize(
    (
        "external_binding_id",
        "parent_channel_id",
        "is_thread",
        "bind_thread",
        "expected_error",
    ),
    (
        (
            "discord-channel-1",
            "discord-channel-1",
            False,
            False,
            "PROJECT_INGRESS_SURFACE_NOT_AUTHORIZED",
        ),
        (
            "discord-thread-1",
            "discord-channel-1",
            True,
            True,
            "PROJECT_INGRESS_SURFACE_NOT_AUTHORIZED",
        ),
        (
            "unbound-marker-channel",
            "unbound-marker-channel",
            False,
            False,
            "PROJECT_INGRESS_UNBOUND_CHANNEL",
        ),
        (
            "unbound-marker-thread",
            "unbound-marker-parent",
            True,
            False,
            "PROJECT_INGRESS_UNBOUND_CHANNEL",
        ),
    ),
)
@pytest.mark.asyncio
async def test_moved_owned_channel_marker_reaches_ingress_before_all_legacy_paths(
    tmp_path,
    monkeypatch,
    external_binding_id,
    parent_channel_id,
    is_thread,
    bind_thread,
    expected_error,
):
    import plugins.platforms.discord.adapter as discord_adapter_module
    from gateway.session import build_session_key
    from plugins.platforms.discord.adapter import DiscordAdapter
    from plugins.platforms.discord.project_channels import (
        project_channel_marker,
    )

    database_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(database_path)
    if bind_thread:
        connection = projects_db.connect(database_path)
        try:
            runtime_db.bind_surface(
                connection,
                binding_id="discord-thread-owner",
                project_id=project_id,
                surface="discord",
                external_binding_id=external_binding_id,
                actor_id="owner",
                principal_id="discord-owner-1",
                now=2,
            )
        finally:
            connection.close()
    gateway_config = GatewayConfig.from_dict(
        {
            "platforms": {
                "discord": {
                    "enabled": True,
                    "allow_from": ["discord-owner-1"],
                    "project_workspaces": {
                        "enabled": True,
                        "guild_id": "guild-1",
                        "owner_user_id": "discord-owner-1",
                        "active_category_id": "active-category",
                        "completed_category_id": "completed-category",
                    },
                }
            }
        }
    )
    adapter = DiscordAdapter(
        gateway_config.platforms[Platform.DISCORD]
    )

    class FakeThread:
        pass

    monkeypatch.setattr(
        discord_adapter_module.discord,
        "Thread",
        FakeThread,
    )
    guild = SimpleNamespace(id="guild-1", name="guild")
    parent = SimpleNamespace(
        id=parent_channel_id,
        name="moved-project",
        category_id="ordinary-category",
        topic=project_channel_marker(project_id),
        guild=guild,
    )
    if is_thread:
        channel = FakeThread()
        channel.id = external_binding_id
        channel.name = "project-child"
        channel.category_id = None
        channel.topic = None
        channel.guild = guild
        channel.parent = parent
        channel.parent_id = parent.id
    else:
        channel = parent
        channel.parent_id = ""
    author = SimpleNamespace(
        id="discord-owner-1",
        display_name="owner",
        bot=False,
    )
    message = SimpleNamespace(
        id=f"moved-{external_binding_id}",
        content="must fail at canonical surface policy",
        channel=channel,
        guild=guild,
        author=author,
        attachments=[],
        mentions=[],
        reference=None,
        created_at=None,
        type=discord_adapter_module.discord.MessageType.default,
    )
    adapter._client = SimpleNamespace(
        user=SimpleNamespace(id="hermes-bot", bot=True),
    )
    adapter._allowed_user_ids = {"discord-owner-1"}
    adapter._ready_event.set()
    adapter._handle_message = AsyncMock(return_value=True)
    adapter._auto_create_thread = AsyncMock()
    adapter._enqueue_text_event = MagicMock()
    adapter._send_with_retry = AsyncMock()
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(database_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    )
    ingress_results = []
    runner_fallback = MagicMock()

    async def route_canonical(event):
        result = ingress.route(event)
        ingress_results.append(result)
        if not result.handled:
            runner_fallback(event)
        return result.response

    adapter._message_handler = route_canonical
    candidate_source = adapter.build_source(
        chat_id=external_binding_id,
        chat_name=channel.name,
        chat_type="thread" if is_thread else "group",
        user_id="discord-owner-1",
        user_name="owner",
        thread_id=external_binding_id if is_thread else None,
        guild_id="guild-1",
        parent_chat_id=parent.id if is_thread else None,
        message_id=str(message.id),
    )
    session_key = build_session_key(
        candidate_source,
        group_sessions_per_user=adapter.config.extra.get(
            "group_sessions_per_user",
            True,
        ),
        thread_sessions_per_user=adapter.config.extra.get(
            "thread_sessions_per_user",
            False,
        ),
    )
    adapter._active_sessions[session_key] = asyncio.Event()

    handled = await adapter._dispatch_discord_message(message)

    assert handled is True
    assert len(ingress_results) == 1
    assert ingress_results[0].handled is True
    assert ingress_results[0].accepted is False
    assert ingress_results[0].error_code == expected_error
    runner_fallback.assert_not_called()
    assert adapter._pending_messages == {}
    adapter._handle_message.assert_not_awaited()
    adapter._auto_create_thread.assert_not_awaited()
    adapter._enqueue_text_event.assert_not_called()
