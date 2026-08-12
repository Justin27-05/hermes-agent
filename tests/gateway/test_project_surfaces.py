"""Pure policy tests for Discord project-workspace registration."""

from __future__ import annotations

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig, load_gateway_config
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from gateway.project_surfaces import (
    DiscordProjectSurface,
    ProjectSurfaceBinding,
    ProjectSurfaceConfigurationError,
    SurfaceActor,
    project_surfaces_for_config,
    resolve_desktop_owner_for_local_profile,
)
from gateway.session import SessionSource


def _config(*, workspace: object, allow_from: object = ["owner-1"]):
    return GatewayConfig.from_dict(
        {
            "platforms": {
                "discord": {
                    "enabled": True,
                    "allow_from": allow_from,
                    "project_workspaces": workspace,
                }
            }
        }
    )


def _enabled_workspace(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "enabled": True,
        "guild_id": "guild-1",
        "owner_user_id": "owner-1",
        "active_category_id": "active-category",
        "completed_category_id": "completed-category",
    }
    value.update(overrides)
    return value


def test_disabled_workspace_registers_no_project_surfaces():
    config = _config(workspace={"enabled": False})

    assert config.platforms[Platform.DISCORD].project_workspaces.enabled is False
    assert project_surfaces_for_config(config) == ()


@pytest.mark.parametrize(
    "field,value",
    [
        ("guild_id", ""),
        ("owner_user_id", " "),
        ("active_category_id", None),
        ("completed_category_id", 0),
    ],
)
def test_enabled_workspace_requires_every_external_id(field, value):
    with pytest.raises(ProjectSurfaceConfigurationError):
        _config(workspace=_enabled_workspace(**{field: value}))


def test_workspace_owner_must_be_in_existing_discord_allowlist():
    with pytest.raises(ProjectSurfaceConfigurationError):
        project_surfaces_for_config(
            _config(
                workspace=_enabled_workspace(),
                allow_from=["someone-else"],
            )
        )


def test_workspace_wildcard_is_not_owner_proof():
    with pytest.raises(ProjectSurfaceConfigurationError):
        project_surfaces_for_config(
            _config(workspace=_enabled_workspace(), allow_from=["*"])
        )


def test_workspace_owner_can_be_proven_by_profile_scoped_env_allowlist():
    surfaces = project_surfaces_for_config(
        _config(workspace=_enabled_workspace(), allow_from=[]),
        discord_allowed_users="owner-1",
    )

    assert len(surfaces) == 1


def test_enabled_workspace_registers_one_exact_discord_surface():
    surface = project_surfaces_for_config(
        _config(workspace=_enabled_workspace())
    )

    assert surface == (
        DiscordProjectSurface(
            guild_id="guild-1",
            owner_user_id="owner-1",
            active_category_id="active-category",
            completed_category_id="completed-category",
        ),
    )


def test_desktop_owner_is_only_accepted_from_the_local_profile_binding():
    surface = project_surfaces_for_config(
        _config(workspace=_enabled_workspace())
    )[0]

    assert surface.desktop_owner_is_local_profile(
        profile_home="C:/profiles/local",
        dispatcher_home="C:/profiles/local",
    ) is True
    assert surface.desktop_owner_is_local_profile(
        profile_home="C:/profiles/other",
        dispatcher_home="C:/profiles/local",
    ) is False


@pytest.mark.parametrize(
    "source,expected",
    [
        (
            SessionSource(
                platform=Platform.DISCORD,
                chat_id="thread-1",
                thread_id="thread-1",
                user_id="owner-1",
                chat_type="thread",
            ),
            True,
        ),
        (
            SessionSource(
                platform=Platform.DISCORD,
                chat_id="thread-1",
                thread_id="thread-1",
                user_id="other-user",
                chat_type="thread",
            ),
            False,
        ),
    ],
)
def test_workspace_rejects_wrong_principal_before_runtime(source, expected):
    surface = project_surfaces_for_config(
        _config(workspace=_enabled_workspace())
    )[0]
    assert surface.accepts_discord_source(
        source, guild_id="guild-1", category_id="active-category"
    ) is expected


def test_workspace_rejects_wrong_guild_and_category_before_runtime():
    surface = project_surfaces_for_config(
        _config(workspace=_enabled_workspace())
    )[0]
    source = SessionSource(
        platform=Platform.DISCORD,
        chat_id="thread-1",
        thread_id="thread-1",
        user_id="owner-1",
        chat_type="thread",
    )

    assert surface.accepts_discord_source(source, guild_id="wrong") is False
    assert surface.accepts_discord_source(
        source, guild_id="guild-1", category_id="wrong-category"
    ) is False


def test_platform_config_preserves_workspace_in_the_extension_roundtrip():
    config = _config(workspace=_enabled_workspace())
    restored = GatewayConfig.from_dict(config.to_dict())

    assert restored.platforms[Platform.DISCORD].extra[
        "project_workspaces"
    ] == _enabled_workspace()
    assert restored.platforms[Platform.DISCORD].project_workspaces.enabled is True


def test_direct_platform_config_normalizes_a_discord_workspace_mapping():
    config = PlatformConfig.from_dict(
        {"project_workspaces": _enabled_workspace()}
    )

    assert config.project_workspaces.to_dict() == _enabled_workspace()
    assert config.to_dict()["project_workspaces"] == _enabled_workspace()


def test_programmatic_workspace_mapping_normalizes_and_loader_roundtrips():
    direct = PlatformConfig(project_workspaces=_enabled_workspace())
    config = GatewayConfig.from_dict(
        {"platforms": {"discord": direct.to_dict()}}
    )

    assert direct.project_workspaces.enabled is True
    assert config.platforms[Platform.DISCORD].project_workspaces.to_dict() == (
        _enabled_workspace()
    )


def test_config_loader_roundtrips_typed_discord_workspace(tmp_path):
    (tmp_path / "config.yaml").write_text(
        """discord:
  allow_from: [owner-1]
  project_workspaces:
    enabled: true
    guild_id: guild-1
    owner_user_id: owner-1
    active_category_id: active-category
    completed_category_id: completed-category
""",
        encoding="utf-8",
    )
    token = set_hermes_home_override(str(tmp_path))
    try:
        config = load_gateway_config()
    finally:
        reset_hermes_home_override(token)

    assert config.platforms[Platform.DISCORD].project_workspaces.to_dict() == (
        _enabled_workspace()
    )


def test_surface_binding_resolves_only_the_exact_actor_and_local_desktop_owner():
    owner = SurfaceActor(
        surface="desktop",
        surface_id="desktop-local",
        external_user_id="owner-1",
        is_owner=True,
    )
    binding = ProjectSurfaceBinding(
        binding_id="binding-1",
        project_id="project-1",
        surface="desktop",
        external_scope_id="desktop-local",
        external_channel_id="desktop-window",
        owner_actor_id="owner-1",
    )

    assert binding.resolve_for_actor(owner) == binding
    assert binding.resolve_for_actor(
        SurfaceActor(
            surface="desktop",
            surface_id="desktop-local",
            external_user_id="other",
            is_owner=True,
        )
    ) is None
    assert binding.resolve_for_actor(
        SurfaceActor(
            surface="desktop",
            surface_id="other-local-profile",
            external_user_id="owner-1",
            is_owner=True,
        )
    ) is None
    assert ProjectSurfaceBinding(
        binding_id="desktop-root",
        project_id="project-1",
        surface="desktop",
        external_scope_id="desktop-local",
        external_channel_id=None,
        owner_actor_id="owner-1",
    ).external_channel_id is None
    assert resolve_desktop_owner_for_local_profile(
        owner,
        profile_home="C:/profiles/local",
        dispatcher_home="C:/profiles/local",
    ) == owner
    assert resolve_desktop_owner_for_local_profile(
        owner,
        profile_home="C:/profiles/other",
        dispatcher_home="C:/profiles/local",
    ) is None
    assert binding.resolve_for_actor(
        SurfaceActor(
            surface="desktop",
            surface_id="desktop-local",
            external_user_id="owner-1",
            is_owner=False,
        )
    ) is None


@pytest.mark.parametrize("surface,is_owner", [("slack", True), ("desktop", 1)])
def test_surface_actor_fails_closed_for_invalid_surface_or_owner_flag(
    surface, is_owner
):
    with pytest.raises(ProjectSurfaceConfigurationError):
        SurfaceActor(
            surface=surface,
            surface_id="surface-1",
            external_user_id="owner-1",
            is_owner=is_owner,
        )
