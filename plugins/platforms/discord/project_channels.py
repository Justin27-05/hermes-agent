"""Discord-independent contract for managed project text channels."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
import hashlib
from typing import Protocol

from gateway.platforms.base import SendResult
from hermes_cli.project_events import ProjectEvent


_PROJECT_MARKER_PREFIX = "hermes-project:v1:"
_EVENT_MARKER_PREFIX = "hermes-event:v1:"
ProjectSegmentFence = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class ProjectChannelSpec:
    project_id: str
    guild_id: str
    owner_user_id: str
    name: str
    category_id: str
    owner_can_send: bool
    channel_id: str | None = None


@dataclass(frozen=True)
class ProjectChannelState:
    guild_id: str
    channel_id: str
    name: str
    category_id: str | None
    ownership_marker: str | None
    only_owner_and_bot_can_view: bool
    owner_can_view: bool
    owner_can_send: bool
    owner_can_read_history: bool
    bot_can_view: bool
    bot_can_send: bool
    bot_can_read_history: bool


class DiscordProjectErrorCode(str, Enum):
    INVALID_ARGUMENT = "invalid_argument"
    UNAVAILABLE = "unavailable"
    NOT_FOUND = "not_found"
    FORBIDDEN = "forbidden"
    RATE_LIMITED = "rate_limited"
    TRANSIENT = "transient"
    AMBIGUOUS = "ambiguous"
    CONFLICT = "conflict"
    STATE_MISMATCH = "state_mismatch"
    PARTIAL_DELIVERY = "partial_delivery"


class DiscordProjectPortError(RuntimeError):
    """Stable, redacted failure which leaves canonical project state intact."""

    def __init__(
        self,
        code: DiscordProjectErrorCode,
        *,
        retryable: bool = False,
        retry_after: float | None = None,
        operation_id: str | None = None,
    ) -> None:
        super().__init__(code.value)
        self.code = code
        self.retryable = retryable
        self.retry_after = retry_after
        self.operation_id = operation_id


class DiscordProjectPort(Protocol):
    """Remote surface port used only under one durable operation claim.

    The caller must hold the cross-process project-surface operation claim for
    every ``ensure_channel`` call.  An adapter's process-local lock can prevent
    overlap inside one client, but is not cross-process authority.
    """

    async def ensure_channel(
        self,
        spec: ProjectChannelSpec,
        *,
        operation_id: str,
    ) -> ProjectChannelState: ...

    async def read_channel(
        self,
        *,
        guild_id: str,
        channel_id: str,
    ) -> ProjectChannelState | None: ...

    async def find_event_message(
        self,
        *,
        channel_id: str,
        event_id: str,
        before_segment: ProjectSegmentFence | None = None,
    ) -> str | None: ...

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment: ProjectSegmentFence | None = None,
    ) -> SendResult: ...


def _marker(prefix: str, identifier: str) -> str:
    if type(identifier) is not str or not identifier:
        raise ValueError("marker identifier must be a non-empty string")
    return prefix + hashlib.sha256(identifier.encode("utf-8")).hexdigest()


def project_channel_marker(project_id: str) -> str:
    return _marker(_PROJECT_MARKER_PREFIX, project_id)


def parse_project_channel_marker(value: object) -> str | None:
    """Return one exact Hermes project digest from a channel topic."""
    if type(value) is not str or not value.startswith(
        _PROJECT_MARKER_PREFIX
    ):
        return None
    digest = value[len(_PROJECT_MARKER_PREFIX):]
    if len(digest) != 64 or any(
        character not in "0123456789abcdef"
        for character in digest
    ):
        return None
    return digest


def project_event_marker(
    event_id: str,
    *,
    part: int,
    total: int,
) -> str:
    if (
        type(part) is not int
        or type(total) is not int
        or part < 1
        or total < 1
        or part > total
    ):
        raise ValueError("invalid event marker part")
    return f"{_marker(_EVENT_MARKER_PREFIX, event_id)}:{part}/{total}"


def state_matches_spec(
    spec: ProjectChannelSpec,
    state: ProjectChannelState,
) -> bool:
    return (
        state.guild_id == spec.guild_id
        and (
            spec.channel_id is None
            or state.channel_id == spec.channel_id
        )
        and state.name == spec.name
        and state.category_id == spec.category_id
        and state.ownership_marker
        == project_channel_marker(spec.project_id)
        and state.only_owner_and_bot_can_view
        and state.owner_can_view
        and state.owner_can_send is spec.owner_can_send
        and state.owner_can_read_history
        and state.bot_can_view
        and state.bot_can_send
        and state.bot_can_read_history
    )


__all__ = [
    "DiscordProjectErrorCode",
    "DiscordProjectPort",
    "DiscordProjectPortError",
    "ProjectChannelSpec",
    "ProjectChannelState",
    "ProjectSegmentFence",
    "parse_project_channel_marker",
    "project_channel_marker",
    "project_event_marker",
    "state_matches_spec",
]
