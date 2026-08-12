"""Pure, fail-closed registration policy for Discord project workspaces.

This module deliberately knows nothing about Discord clients or the project
command service.  It only turns trusted configuration and inbound source
attributes into an exact surface policy; durable binding/principal lookup stays
in :mod:`gateway.project_runtime_ingress`.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Literal, Mapping
import unicodedata

from hermes_cli.project_events import ProjectEvent
from plugins.platforms.discord.project_channels import ProjectChannelSpec


class ProjectSurfaceConfigurationError(ValueError):
    """A project workspace was enabled without a safe, complete identity."""


def _identity(value: object, name: str) -> str:
    if type(value) is not str or not value.strip():
        raise ProjectSurfaceConfigurationError(
            f"{name} must be a non-empty external id"
        )
    return value.strip()


def _external_id(value: object, name: str) -> str:
    if type(value) is not str or not value.strip():
        raise ProjectSurfaceConfigurationError(
            f"project_workspaces.{name} must be a non-empty external id"
        )
    return value.strip()


def _allow_set(value: object) -> frozenset[str]:
    if type(value) is str:
        values = value.split(",")
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        values = ()
    return frozenset(
        item.strip()
        for item in values
        if type(item) is str and item.strip()
    )


@dataclass(frozen=True)
class DiscordProjectWorkspaceConfig:
    """One explicit Discord workspace, disabled unless opted in."""

    enabled: bool = False
    guild_id: str | None = None
    owner_user_id: str | None = None
    active_category_id: str | None = None
    completed_category_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {"enabled": self.enabled}
        if self.enabled:
            result.update(
                guild_id=self.guild_id,
                owner_user_id=self.owner_user_id,
                active_category_id=self.active_category_id,
                completed_category_id=self.completed_category_id,
            )
        return result


@dataclass(frozen=True)
class SurfaceActor:
    """An exact principal on one project surface."""

    surface: Literal["desktop", "discord"]
    surface_id: str
    external_user_id: str
    is_owner: bool

    def __post_init__(self) -> None:
        if self.surface not in {"desktop", "discord"}:
            raise ProjectSurfaceConfigurationError(
                "surface must be desktop or discord"
            )
        object.__setattr__(self, "surface_id", _identity(self.surface_id, "surface_id"))
        object.__setattr__(
            self,
            "external_user_id",
            _identity(self.external_user_id, "external_user_id"),
        )
        if type(self.is_owner) is not bool:
            raise ProjectSurfaceConfigurationError("is_owner must be a boolean")


@dataclass(frozen=True)
class ProjectSurfaceBinding:
    """Pure binding plan, resolved only for its exact surface actor."""

    binding_id: str
    project_id: str
    surface: str
    external_scope_id: str
    external_channel_id: str | None
    owner_actor_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "binding_id", _identity(self.binding_id, "binding_id"))
        object.__setattr__(self, "project_id", _identity(self.project_id, "project_id"))
        object.__setattr__(self, "surface", _identity(self.surface, "surface"))
        if self.surface not in {"desktop", "discord"}:
            raise ProjectSurfaceConfigurationError(
                "binding surface must be desktop or discord"
            )
        object.__setattr__(
            self,
            "external_scope_id",
            _identity(self.external_scope_id, "external_scope_id"),
        )
        if self.external_channel_id is not None:
            object.__setattr__(
                self,
                "external_channel_id",
                _identity(self.external_channel_id, "external_channel_id"),
            )
        object.__setattr__(
            self,
            "owner_actor_id",
            _identity(self.owner_actor_id, "owner_actor_id"),
        )

    def resolve_for_actor(self, actor: object) -> "ProjectSurfaceBinding | None":
        if not isinstance(actor, SurfaceActor) or not actor.is_owner:
            return None
        if actor.surface != self.surface:
            return None
        if actor.surface_id != self.external_scope_id:
            return None
        return self if actor.external_user_id == self.owner_actor_id else None


def resolve_desktop_owner_for_local_profile(
    owner: object,
    *,
    profile_home: object,
    dispatcher_home: object,
) -> SurfaceActor | None:
    """Return the desktop owner only for the dispatcher-owned local profile."""
    if (
        not isinstance(owner, SurfaceActor)
        or owner.surface != "desktop"
        or not owner.is_owner
    ):
        return None
    if not isinstance(profile_home, (str, Path)) or not isinstance(
        dispatcher_home, (str, Path)
    ):
        return None
    try:
        return owner if Path(profile_home).resolve() == Path(dispatcher_home).resolve() else None
    except (OSError, ValueError):
        return None


def parse_discord_project_workspace(
    value: object,
    *,
    allowed_users: object = None,
) -> DiscordProjectWorkspaceConfig:
    """Validate one workspace without granting authority through a wildcard."""
    if value is None:
        return DiscordProjectWorkspaceConfig()
    if not isinstance(value, Mapping):
        raise ProjectSurfaceConfigurationError(
            "project_workspaces must be an object"
        )
    enabled = value.get("enabled", False)
    if type(enabled) is not bool:
        raise ProjectSurfaceConfigurationError(
            "project_workspaces.enabled must be a boolean"
        )
    if not enabled:
        return DiscordProjectWorkspaceConfig()
    guild_id = _external_id(value.get("guild_id"), "guild_id")
    owner_user_id = _external_id(
        value.get("owner_user_id"), "owner_user_id"
    )
    active_category_id = _external_id(
        value.get("active_category_id"), "active_category_id"
    )
    completed_category_id = _external_id(
        value.get("completed_category_id"), "completed_category_id"
    )
    # A wildcard may admit ordinary Discord traffic, but is never evidence that
    # this particular owner identity is trusted for a project workspace.  The
    # raw config parser intentionally does not require this proof: the final
    # allowlist can arrive via a profile-scoped environment override.
    if allowed_users is not None and owner_user_id not in _allow_set(allowed_users):
        raise ProjectSurfaceConfigurationError(
            "project workspace owner must be explicitly listed in Discord "
            "allowed_users/allow_from"
        )
    return DiscordProjectWorkspaceConfig(
        enabled=True,
        guild_id=guild_id,
        owner_user_id=owner_user_id,
        active_category_id=active_category_id,
        completed_category_id=completed_category_id,
    )


@dataclass(frozen=True)
class DiscordProjectSurface:
    """Pure pre-command gate for one configured Discord project workspace."""

    guild_id: str
    owner_user_id: str
    active_category_id: str
    completed_category_id: str

    @classmethod
    def from_config(
        cls, config: DiscordProjectWorkspaceConfig
    ) -> "DiscordProjectSurface":
        if not config.enabled:
            raise ProjectSurfaceConfigurationError("workspace is disabled")
        assert config.guild_id is not None
        assert config.owner_user_id is not None
        assert config.active_category_id is not None
        assert config.completed_category_id is not None
        return cls(
            config.guild_id,
            config.owner_user_id,
            config.active_category_id,
            config.completed_category_id,
        )

    def desktop_owner_is_local_profile(
        self, *, profile_home: object, dispatcher_home: object
    ) -> bool:
        return resolve_desktop_owner_for_local_profile(
            SurfaceActor(
                surface="desktop",
                surface_id=self.owner_user_id,
                external_user_id=self.owner_user_id,
                is_owner=True,
            ),
            profile_home=profile_home,
            dispatcher_home=dispatcher_home,
        ) is not None

    def accepts_discord_source(
        self,
        source: object,
        *,
        guild_id: object,
        category_id: object | None = None,
    ) -> bool:
        platform = getattr(getattr(source, "platform", None), "value", None)
        if platform != "discord":
            return False
        if not self.reserves_discord_location(
            guild_id=guild_id,
            category_id=category_id,
        ):
            return False
        if getattr(source, "user_id", None) != self.owner_user_id:
            return False
        # Both lifecycle categories enter the canonical runtime.  The runtime
        # rejects new turns and unsupported controls for completed projects,
        # while still allowing status and reopen.
        thread_id = getattr(source, "thread_id", None)
        chat_id = getattr(source, "chat_id", None)
        return (
            type(thread_id) is str
            and bool(thread_id)
            or type(chat_id) is str
            and bool(chat_id)
        )

    def reserves_discord_location(
        self,
        *,
        guild_id: object,
        category_id: object | None,
    ) -> bool:
        """Return whether a guild/category location belongs to this workspace."""
        return (
            guild_id == self.guild_id
            and category_id
            in {self.active_category_id, self.completed_category_id}
        )


@dataclass(frozen=True)
class ProjectLifecycleSnapshot:
    """Minimal immutable project state needed for one surface operation."""

    project_id: str
    name: str
    lifecycle: Literal["active", "awaiting_acceptance", "completed"]
    channel_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "project_id", _identity(self.project_id, "project_id")
        )
        object.__setattr__(self, "name", _identity(self.name, "name"))
        if self.lifecycle not in {
            "active",
            "awaiting_acceptance",
            "completed",
        }:
            raise ProjectSurfaceConfigurationError(
                "invalid project lifecycle snapshot"
            )
        if self.channel_id is not None:
            object.__setattr__(
                self,
                "channel_id",
                _identity(self.channel_id, "channel_id"),
            )


_PROJECT_LIFECYCLE_TARGETS = {
    "project.created": "active",
    "project.renamed": "active",
    "project.technically_completed": "awaiting_acceptance",
    "project.completion_accepted": "completed",
    "project.reopened": "active",
}


def discord_project_channel_name(project_id: str, name: str) -> str:
    """Return stable, conservative Discord text-channel display metadata."""
    project_id = _identity(project_id, "project_id")
    name = _identity(name, "name")
    ascii_name = unicodedata.normalize("NFKD", name).encode(
        "ascii", "ignore"
    ).decode("ascii")
    normalized = re.sub(
        r"[^a-z0-9]+", "-", ascii_name.casefold()
    ).strip("-")
    normalized = normalized[:100].rstrip("-")
    if normalized:
        return normalized
    digest = hashlib.sha256(project_id.encode("utf-8")).hexdigest()[:12]
    return f"project-{digest}"


def project_channel_spec_for_lifecycle_event(
    event: ProjectEvent,
    snapshot: ProjectLifecycleSnapshot,
    surface: DiscordProjectSurface,
) -> ProjectChannelSpec | None:
    """Translate one allowlisted canonical lifecycle event to desired state."""
    if not isinstance(event, ProjectEvent):
        raise TypeError("event must be a canonical ProjectEvent")
    if event.kind.startswith("surface."):
        return None
    event_target = _PROJECT_LIFECYCLE_TARGETS.get(event.kind)
    if event_target is None:
        return None
    if event.project_id != snapshot.project_id:
        raise ValueError("event and lifecycle snapshot project differ")
    target = (
        snapshot.lifecycle
        if event.kind == "project.renamed"
        else event_target
    )
    completed = target == "completed"
    return ProjectChannelSpec(
        project_id=snapshot.project_id,
        guild_id=surface.guild_id,
        owner_user_id=surface.owner_user_id,
        name=discord_project_channel_name(
            snapshot.project_id, snapshot.name
        ),
        category_id=(
            surface.completed_category_id
            if completed
            else surface.active_category_id
        ),
        owner_can_send=not completed,
        channel_id=snapshot.channel_id,
    )


def project_surfaces_for_config(
    config: object,
    *,
    discord_allowed_users: object = None,
) -> tuple[DiscordProjectSurface, ...]:
    """Return only enabled surfaces; a disabled workspace registers nothing."""
    platforms = getattr(config, "platforms", {})
    discord = next(
        (
            value
            for key, value in getattr(platforms, "items", lambda: ())()
            if getattr(key, "value", key) == "discord"
        ),
        None,
    )
    workspace = getattr(discord, "project_workspaces", None)
    if not isinstance(workspace, DiscordProjectWorkspaceConfig):
        return ()
    if not workspace.enabled:
        return ()
    configured_users = getattr(discord, "extra", {}).get("allow_from")
    allowed_users: list[object] = []
    if configured_users is not None:
        allowed_users.append(configured_users)
    if discord_allowed_users is not None:
        allowed_users.append(discord_allowed_users)
    parse_discord_project_workspace(
        workspace.to_dict(),
        allowed_users=tuple(
            user
            for values in allowed_users
            for user in _allow_set(values)
        ),
    )
    return (DiscordProjectSurface.from_config(workspace),)


__all__ = [
    "DiscordProjectSurface",
    "DiscordProjectWorkspaceConfig",
    "ProjectLifecycleSnapshot",
    "ProjectSurfaceBinding",
    "ProjectSurfaceConfigurationError",
    "SurfaceActor",
    "discord_project_channel_name",
    "parse_discord_project_workspace",
    "project_channel_spec_for_lifecycle_event",
    "project_surfaces_for_config",
    "resolve_desktop_owner_for_local_profile",
]
