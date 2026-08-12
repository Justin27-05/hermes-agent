from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.platforms.base import MessageEvent
from gateway.project_surfaces import DiscordProjectSurface
from gateway.project_runtime_ingress import ProjectRuntimeIngress
from gateway.session import SessionSource
from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db


def _seed_bound_project(path: Path) -> tuple[str, str]:
    conn = projects_db.connect(path)
    try:
        project_id = projects_db.create_project(
            conn,
            name="Discord ingress",
            folders=(str(path.parent),),
        )
        prdb.create_project_conversation(
            conn,
            project_id=project_id,
            conversation_id="canonical-project-session",
            current_phase="implementation",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="desktop-owner",
            project_id=project_id,
            surface="desktop",
            external_binding_id="desktop-window",
            actor_id="owner",
            now=1,
        )
        prdb.bind_surface(
            conn,
            binding_id="discord-owner",
            project_id=project_id,
            surface="discord",
            external_binding_id="discord-thread-1",
            actor_id="owner",
            now=1,
            principal_id="discord-user-1",
        )
        return project_id, "discord-owner"
    finally:
        conn.close()


def _discord_event(
    *,
    thread_id: str = "discord-thread-1",
    message_id: str | None = "discord-message-1",
    text: str = "Voer de volgende projectstap uit",
    guild_id: str = "guild-1",
    category_id: str = "active-category",
    user_id: str = "discord-user-1",
) -> MessageEvent:
    source = SessionSource(
        platform=Platform.DISCORD,
        chat_id="discord-parent-channel",
        chat_type="thread",
        user_id=user_id,
        thread_id=thread_id,
        message_id=message_id,
    )
    event = MessageEvent(
        text=text,
        source=source,
        message_id=message_id,
    )
    event.raw_message = SimpleNamespace(
        guild=SimpleNamespace(id=guild_id),
        channel=SimpleNamespace(category_id=category_id),
    )
    return event


def test_bound_discord_message_enqueues_once_and_wakes_dispatcher(tmp_path):
    db_path = tmp_path / "projects.db"
    project_id, binding_id = _seed_bound_project(db_path)
    wakes: list[str] = []
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(db_path),
        wake=lambda: wakes.append("wake"),
    )

    first = ingress.route(_discord_event())
    replay = ingress.route(_discord_event())

    assert first.handled is True
    assert first.accepted is True
    assert replay == first
    assert wakes == ["wake", "wake"]
    conn = projects_db.connect(db_path)
    try:
        turns = prdb._queued_turns_for_project(
            conn, project_id=project_id
        )
        assert len(turns) == 1
        assert turns[0].origin_binding_id == binding_id
        assert turns[0].idempotency_key == (
            "discord-message:discord-owner:discord-message-1"
        )
        assert conn.execute(
            """
            SELECT COUNT(*) FROM project_events
            WHERE project_id = ? AND kind = 'turn.queued'
            """,
            (project_id,),
        ).fetchone()[0] == 1
        obligations = conn.execute(
            """
            SELECT binding_id, status
            FROM project_deliveries
            WHERE project_id = ?
            ORDER BY binding_id
            """,
            (project_id,),
        ).fetchall()
        assert [tuple(row) for row in obligations] == [
            ("desktop-owner", "pending"),
            ("discord-owner", "pending"),
        ]
    finally:
        conn.close()


def test_unbound_discord_and_non_discord_messages_keep_legacy_route(tmp_path):
    db_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(db_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(db_path)
    )
    unbound = ingress.route(
        _discord_event(thread_id="different-thread")
    )
    local = ingress.route(
        MessageEvent(
            text="local",
            source=SessionSource(
                platform=Platform.LOCAL,
                chat_id="local",
                user_id="owner",
            ),
            message_id="local-message",
        )
    )

    assert unbound.handled is False
    assert local.handled is False
    conn = projects_db.connect(db_path)
    try:
        assert prdb._queued_turns_for_project(
            conn, project_id=project_id
        ) == ()
    finally:
        conn.close()


def test_bound_discord_message_without_stable_identity_fails_closed(tmp_path):
    db_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(db_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(db_path)
    )

    result = ingress.route(_discord_event(message_id=None))

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == "PROJECT_INGRESS_INVALID_MESSAGE"
    conn = projects_db.connect(db_path)
    try:
        assert prdb._queued_turns_for_project(
            conn, project_id=project_id
        ) == ()
    finally:
        conn.close()


def test_discord_fails_closed_when_project_store_is_unavailable():
    def unavailable():
        raise OSError("private local path")

    result = ProjectRuntimeIngress(
        db_factory=unavailable
    ).route(_discord_event())

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == "PROJECT_INGRESS_UNAVAILABLE"
    assert "private local path" not in result.response


def test_bound_discord_principal_mismatch_fails_closed(tmp_path):
    db_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(db_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(db_path)
    )
    event = _discord_event()
    event.source.user_id = "different-discord-user"

    result = ingress.route(event)

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == (
        "PROJECT_INGRESS_ACTOR_NOT_AUTHORIZED"
    )
    conn = projects_db.connect(db_path)
    try:
        assert prdb._queued_turns_for_project(
            conn, project_id=project_id
        ) == ()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "event",
    [
        _discord_event(guild_id="wrong-guild"),
        _discord_event(category_id="wrong-category"),
        _discord_event(user_id="different-discord-user"),
    ],
)
def test_configured_surface_rejects_wrong_scope_or_principal_before_runtime(
    tmp_path, monkeypatch, event
):
    db_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(db_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(db_path),
        surface=DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="discord-user-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    )

    monkeypatch.setattr(
        "gateway.project_runtime_ingress.ProjectRuntime",
        lambda _conn: (_ for _ in ()).throw(AssertionError("runtime used")),
    )

    result = ingress.route(event)

    assert result.handled is True
    assert result.accepted is False
    assert result.project_id == project_id
    assert result.error_code == "PROJECT_INGRESS_SURFACE_NOT_AUTHORIZED"


def test_configured_surface_accepts_discord_integer_snowflakes(tmp_path):
    db_path = tmp_path / "projects.db"
    _seed_bound_project(db_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(db_path),
        surface=DiscordProjectSurface(
            guild_id="1",
            owner_user_id="discord-user-1",
            active_category_id="2",
            completed_category_id="3",
        ),
    )

    result = ingress.route(_discord_event(guild_id=1, category_id=2))

    assert result.accepted is True


def test_bound_discord_media_and_slash_commands_fail_closed(tmp_path):
    db_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(db_path)
    ingress = ProjectRuntimeIngress(
        db_factory=lambda: projects_db.connect(db_path)
    )
    media = _discord_event(message_id="media-message")
    media.media_urls = [str(tmp_path / "private-image.png")]
    slash = _discord_event(
        message_id="slash-message", text="/stop"
    )

    media_result = ingress.route(media)
    slash_result = ingress.route(slash)

    assert media_result.error_code == (
        "PROJECT_INGRESS_UNSUPPORTED_PAYLOAD"
    )
    assert slash_result.error_code == (
        "PROJECT_INGRESS_COMMAND_UNSUPPORTED"
    )
    conn = projects_db.connect(db_path)
    try:
        assert prdb._queued_turns_for_project(
            conn, project_id=project_id
        ) == ()
    finally:
        conn.close()


def test_binding_lookup_is_exact_and_surface_scoped(tmp_path):
    db_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(db_path)
    conn = projects_db.connect(db_path)
    try:
        binding = prdb.binding_for_surface_identity(
            conn,
            surface="discord",
            external_binding_id="discord-thread-1",
        )
        wrong_surface = prdb.binding_for_surface_identity(
            conn,
            surface="desktop",
            external_binding_id="discord-thread-1",
        )
    finally:
        conn.close()

    assert binding is not None
    assert binding.project_id == project_id
    assert wrong_surface is None


@pytest.mark.asyncio
async def test_gateway_runner_composes_ingress_with_live_dispatcher_wake(
    tmp_path, monkeypatch
):
    from gateway.run import GatewayRunner

    db_path = tmp_path / "projects.db"
    project_id, _ = _seed_bound_project(db_path)
    original_connect = projects_db.connect
    monkeypatch.setattr(
        projects_db,
        "connect",
        lambda *args, **kwargs: original_connect(db_path),
    )
    wakes: list[str] = []
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig.from_dict(
        {
            "platforms": {
                "discord": {
                    "enabled": True,
                    "allow_from": ["discord-user-1"],
                    "project_workspaces": {
                        "enabled": True,
                        "guild_id": "guild-1",
                        "owner_user_id": "discord-user-1",
                        "active_category_id": "active-category",
                        "completed_category_id": "completed-category",
                    },
                }
            }
        }
    )
    runner._project_runtime_dispatcher = SimpleNamespace(
        wake=lambda: wakes.append("wake")
    )

    async def run_inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    runner._run_project_runtime_io = run_inline

    result = await runner._route_bound_project_message(
        _discord_event(message_id="gateway-message-1")
    )

    assert result.handled is True
    assert result.accepted is True
    assert wakes == ["wake"]
    conn = original_connect(db_path)
    try:
        turns = prdb._queued_turns_for_project(
            conn, project_id=project_id
        )
        assert [turn.idempotency_key for turn in turns] == [
            "discord-message:discord-owner:gateway-message-1"
        ]
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace",
    [None, {"enabled": False}, {"enabled": True}],
)
async def test_gateway_runner_does_not_construct_ingress_without_one_valid_surface(
    monkeypatch, workspace
):
    from gateway.run import GatewayRunner
    import gateway.project_runtime_ingress as ingress_module

    runner = object.__new__(GatewayRunner)
    platform_data = {"enabled": True, "allow_from": ["owner-1"]}
    if workspace is not None:
        platform_data["project_workspaces"] = workspace
    runner.config = GatewayConfig.from_dict(
        {"platforms": {"discord": platform_data}}
    ) if workspace != {"enabled": True} else SimpleNamespace(
        platforms={
            Platform.DISCORD: SimpleNamespace(
                project_workspaces={"enabled": True}, extra={}
            )
        }
    )
    runner._project_runtime_dispatcher = None

    async def run_inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    runner._run_project_runtime_io = run_inline
    monkeypatch.setattr(
        ingress_module,
        "ProjectRuntimeIngress",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("constructed")),
    )

    result = await runner._route_bound_project_message(_discord_event())

    assert result.handled is False


@pytest.mark.asyncio
async def test_gateway_runner_routes_project_ingress_to_source_profile(
    tmp_path,
):
    from gateway.run import GatewayRunner, _profile_runtime_scope

    profile_home = tmp_path / "secondary-profile"
    profile_home.mkdir()
    db_path = profile_home / "projects.db"
    project_id, _ = _seed_bound_project(db_path)
    runner = object.__new__(GatewayRunner)
    runner.config = SimpleNamespace(multiplex_profiles=True)
    runner._project_runtime_dispatcher = None
    runner._project_runtime_dispatcher_home = (
        tmp_path / "default-profile"
    )
    runner._resolve_profile_home_for_source = (
        lambda _source: profile_home
    )

    async def run_inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    runner._run_project_runtime_io = run_inline

    # Secondary adapter handlers already run under this context.  The
    # process-local dispatcher home must remain the fixed startup home rather
    # than following the contextvar into a profile with no dispatcher.
    with _profile_runtime_scope(profile_home):
        result = await runner._route_bound_project_message(
            _discord_event(message_id="secondary-profile-message")
        )

    assert result.handled is True
    assert result.accepted is False
    assert result.error_code == (
        "PROJECT_INGRESS_PROFILE_DISPATCHER_UNAVAILABLE"
    )
    conn = projects_db.connect(db_path)
    try:
        turns = prdb._queued_turns_for_project(
            conn, project_id=project_id
        )
        assert turns == ()
    finally:
        conn.close()
