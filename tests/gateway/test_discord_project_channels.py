from __future__ import annotations

import asyncio
from dataclasses import replace
import sys
from types import MappingProxyType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult
from hermes_cli.project_events import ProjectEvent
from plugins.platforms.discord import project_channels
from plugins.platforms.discord.project_channels import (
    DiscordProjectErrorCode,
    DiscordProjectPort,
    DiscordProjectPortError,
    ProjectChannelSpec,
    ProjectChannelState,
    project_channel_marker,
    project_event_marker,
    state_matches_spec,
)


@pytest.mark.parametrize(
    ("topic", "expected"),
    (
        (
            "hermes-project:v1:"
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        (None, None),
        ("foreign-project:v1:" + "a" * 64, None),
        ("hermes-project:v1:" + "a" * 63, None),
        ("hermes-project:v1:" + "A" * 64, None),
        ("hermes-project:v1:" + "a" * 64 + "-extra", None),
        (
            "hermes-project:v1:"
            + "a" * 64
            + " hermes-project:v1:"
            + "b" * 64,
            None,
        ),
    ),
)
def test_project_channel_marker_parser_requires_one_exact_owned_topic(
    topic,
    expected,
):
    parser = getattr(
        project_channels,
        "parse_project_channel_marker",
        None,
    )

    assert parser is not None
    assert parser(topic) == expected


def _active_spec(*, channel_id: str | None = None) -> ProjectChannelSpec:
    return ProjectChannelSpec(
        project_id="project-1",
        guild_id="guild-1",
        owner_user_id="owner-1",
        name="project-alpha",
        category_id="active-category",
        owner_can_send=True,
        channel_id=channel_id,
    )


def _active_state() -> ProjectChannelState:
    return ProjectChannelState(
        guild_id="guild-1",
        channel_id="channel-1",
        name="project-alpha",
        category_id="active-category",
        ownership_marker=(
            "hermes-project:v1:"
            "a33e35d302125bbd8e647043a4025b29"
            "f659aad51c4a80d6244a45fabcdcd235"
        ),
        only_owner_and_bot_can_view=True,
        owner_can_view=True,
        owner_can_send=True,
        owner_can_read_history=True,
        bot_can_view=True,
        bot_can_send=True,
        bot_can_read_history=True,
    )


def test_markers_are_deterministic_and_do_not_disclose_internal_ids():
    assert project_channel_marker("project-1") == (
        "hermes-project:v1:"
        "a33e35d302125bbd8e647043a4025b29"
        "f659aad51c4a80d6244a45fabcdcd235"
    )
    assert project_event_marker("event-1", part=2, total=3) == (
        "hermes-event:v1:"
        "ce36863f51b6baf9d16397ffb3e9af50"
        "6b284a816f72d487e55943c1fd974d6d:2/3"
    )
    assert "project-1" not in project_channel_marker("project-1")
    assert "event-1" not in project_event_marker(
        "event-1", part=2, total=3
    )


def test_state_comparison_requires_exact_binding_privacy_and_history():
    spec = _active_spec(channel_id="channel-1")
    state = _active_state()

    assert state_matches_spec(spec, state) is True

    unsafe_variants = (
        replace(state, channel_id="another-channel"),
        replace(state, ownership_marker="foreign"),
        replace(state, only_owner_and_bot_can_view=False),
        replace(state, owner_can_read_history=False),
        replace(state, bot_can_send=False),
    )
    assert all(
        state_matches_spec(spec, candidate) is False
        for candidate in unsafe_variants
    )


def test_structured_error_exposes_stable_nonterminal_fields_only():
    error = DiscordProjectPortError(
        DiscordProjectErrorCode.RATE_LIMITED,
        retryable=True,
        retry_after=4.5,
        operation_id="operation-1",
    )

    assert str(error) == "rate_limited"
    assert error.code is DiscordProjectErrorCode.RATE_LIMITED
    assert error.retryable is True
    assert error.retry_after == 4.5
    assert error.operation_id == "operation-1"


def test_public_port_has_exactly_four_operations_and_no_delete():
    public_methods = {
        name
        for name, value in vars(DiscordProjectPort).items()
        if callable(value) and not name.startswith("_")
    }

    assert public_methods == {
        "ensure_channel",
        "read_channel",
        "find_event_message",
        "publish_event",
    }
    assert not hasattr(DiscordProjectPort, "delete_channel")


class InMemoryDiscordProjectPort:
    """Test-local contract fake; production deliberately has no fake state."""

    def __init__(self) -> None:
        self.channels: dict[str, ProjectChannelState] = {}
        self.project_channels: dict[str, str] = {}
        self.operations: dict[str, ProjectChannelSpec] = {}
        self.event_messages: dict[tuple[str, str], str] = {}
        self.calls: list[tuple[str, str]] = []

    async def ensure_channel(
        self,
        spec: ProjectChannelSpec,
        *,
        operation_id: str,
    ) -> ProjectChannelState:
        self.calls.append(("ensure_channel", operation_id))
        prior = self.operations.get(operation_id)
        if prior is not None and prior != spec:
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.CONFLICT,
                operation_id=operation_id,
            )
        self.operations[operation_id] = spec
        channel_id = self.project_channels.get(spec.project_id)
        if channel_id is None:
            channel_id = spec.channel_id or f"channel-{len(self.channels) + 1}"
            self.project_channels[spec.project_id] = channel_id
        elif spec.channel_id is not None and spec.channel_id != channel_id:
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.CONFLICT,
                operation_id=operation_id,
            )
        state = ProjectChannelState(
            guild_id=spec.guild_id,
            channel_id=channel_id,
            name=spec.name,
            category_id=spec.category_id,
            ownership_marker=project_channel_marker(spec.project_id),
            only_owner_and_bot_can_view=True,
            owner_can_view=True,
            owner_can_send=spec.owner_can_send,
            owner_can_read_history=True,
            bot_can_view=True,
            bot_can_send=True,
            bot_can_read_history=True,
        )
        self.channels[channel_id] = state
        return state

    async def read_channel(
        self,
        *,
        guild_id: str,
        channel_id: str,
    ) -> ProjectChannelState | None:
        self.calls.append(("read_channel", channel_id))
        state = self.channels.get(channel_id)
        return state if state is None or state.guild_id == guild_id else None

    async def find_event_message(
        self,
        *,
        channel_id: str,
        event_id: str,
        before_segment=None,
    ) -> str | None:
        if before_segment is not None:
            await before_segment()
        self.calls.append(("find_event_message", event_id))
        return self.event_messages.get((channel_id, event_id))

    async def publish_event(
        self,
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
        before_segment=None,
    ) -> SendResult:
        if before_segment is not None:
            await before_segment()
        self.calls.append(("publish_event", nonce))
        key = (channel_id, event.event_id)
        message_id = self.event_messages.setdefault(
            key,
            f"message-{len(self.event_messages) + 1}",
        )
        return SendResult(success=True, message_id=message_id)


@pytest.mark.asyncio
async def test_in_memory_fake_converges_lifecycle_without_replacing_channel():
    port = InMemoryDiscordProjectPort()
    active = _active_spec()

    created = await port.ensure_channel(active, operation_id="create-1")
    replay = await port.ensure_channel(active, operation_id="create-1")
    completed = await port.ensure_channel(
        replace(
            active,
            channel_id=created.channel_id,
            name="project-renamed",
            category_id="completed-category",
            owner_can_send=False,
        ),
        operation_id="complete-1",
    )

    assert replay == created
    assert completed.channel_id == created.channel_id
    assert completed.name == "project-renamed"
    assert completed.category_id == "completed-category"
    assert completed.owner_can_send is False
    assert len(port.channels) == 1


@pytest.mark.asyncio
async def test_in_memory_fake_delivers_one_remote_message_per_event():
    port = InMemoryDiscordProjectPort()
    event = ProjectEvent(
        event_id="event-1",
        project_id="project-1",
        sequence=1,
        kind="turn.queued",
        turn_id="turn-1",
        payload={},
        created_at="2026-07-29T00:00:00Z",
    )

    first = await port.publish_event(
        channel_id="channel-1",
        event=event,
        nonce="nonce-1",
    )
    second = await port.publish_event(
        channel_id="channel-1",
        event=event,
        nonce="nonce-1",
    )

    assert first.message_id == second.message_id == "message-1"
    assert len(port.event_messages) == 1


class _StubPermissionOverwrite:
    def __init__(
        self,
        *,
        view_channel=None,
        send_messages=None,
        read_message_history=None,
    ) -> None:
        self.view_channel = view_channel
        self.send_messages = send_messages
        self.read_message_history = read_message_history


class _StubAllowedMentions:
    def __init__(
        self,
        *,
        everyone=False,
        roles=False,
        users=False,
        replied_user=False,
    ) -> None:
        self.everyone = everyone
        self.roles = roles
        self.users = users
        self.replied_user = replied_user

    @classmethod
    def none(cls):
        return cls()


class _DiscordForbidden(RuntimeError):
    status = 403


class _DiscordNotFound(RuntimeError):
    status = 404


class _DiscordRateLimited(RuntimeError):
    status = 429

    def __init__(self, retry_after: float = 2.5) -> None:
        super().__init__("limited")
        self.retry_after = retry_after


def _ensure_discord_stub() -> None:
    if "discord" in sys.modules and hasattr(sys.modules["discord"], "__file__"):
        return
    discord_mod = MagicMock()
    discord_mod.Intents.default.return_value = MagicMock()
    discord_mod.Client = MagicMock
    discord_mod.File = MagicMock
    discord_mod.Message = object
    discord_mod.DMChannel = type("DMChannel", (), {})
    discord_mod.Thread = type("Thread", (), {})
    discord_mod.TextChannel = type("TextChannel", (), {})
    discord_mod.CategoryChannel = type("CategoryChannel", (), {})
    discord_mod.ForumChannel = type("ForumChannel", (), {})
    discord_mod.Interaction = object
    discord_mod.PermissionOverwrite = _StubPermissionOverwrite
    discord_mod.AllowedMentions = _StubAllowedMentions
    discord_mod.Forbidden = _DiscordForbidden
    discord_mod.NotFound = _DiscordNotFound
    discord_mod.RateLimited = _DiscordRateLimited
    discord_mod.HTTPException = type("HTTPException", (RuntimeError,), {})
    discord_mod.MessageType = SimpleNamespace(default=0, reply=19)
    discord_mod.ui = SimpleNamespace(
        View=object,
        button=lambda *args, **kwargs: (lambda function: function),
        Button=object,
    )
    discord_mod.ButtonStyle = SimpleNamespace(
        success=1,
        primary=2,
        secondary=2,
        danger=3,
        green=1,
        grey=2,
        blurple=2,
        red=3,
    )
    discord_mod.Color = SimpleNamespace(
        orange=lambda: 1,
        green=lambda: 2,
        blue=lambda: 3,
        red=lambda: 4,
        purple=lambda: 5,
        greyple=lambda: 6,
    )
    discord_mod.Embed = MagicMock
    discord_mod.app_commands = SimpleNamespace(
        describe=lambda **kwargs: (lambda function: function),
        choices=lambda **kwargs: (lambda function: function),
        Choice=lambda **kwargs: SimpleNamespace(**kwargs),
    )
    ext_mod = MagicMock()
    commands_mod = MagicMock()
    commands_mod.Bot = MagicMock
    ext_mod.commands = commands_mod
    sys.modules["discord"] = discord_mod
    sys.modules["discord.ext"] = ext_mod
    sys.modules["discord.ext.commands"] = commands_mod


_ensure_discord_stub()

from plugins.platforms.discord import adapter as discord_adapter_module  # noqa: E402
from plugins.platforms.discord.adapter import DiscordAdapter  # noqa: E402

if getattr(discord_adapter_module.discord, "__file__", None) is None:
    discord_adapter_module.discord = sys.modules["discord"]


class _Target:
    def __init__(self, target_id: int) -> None:
        self.id = target_id


class _FakeMessage:
    def __init__(
        self,
        *,
        message_id: int,
        author,
        content: str,
        nonce: str | None,
    ) -> None:
        self.id = message_id
        self.author = author
        self.content = content
        self.clean_content = content
        self.nonce = nonce


class _FakeCategory:
    def __init__(self, category_id: int, guild) -> None:
        self.id = category_id
        self.guild = guild
        self.type = SimpleNamespace(value=4)
        self.overwrites = {}


class _FakeTextChannel:
    def __init__(
        self,
        *,
        channel_id: int,
        name: str,
        guild,
        category,
        topic: str | None,
        overwrites: dict,
    ) -> None:
        self.id = channel_id
        self.name = name
        self.guild = guild
        self.type = SimpleNamespace(value=0)
        self.category = category
        self.category_id = category.id if category is not None else None
        self.topic = topic
        self.overwrites = dict(overwrites)
        self.edit_calls: list[dict] = []
        self.send_calls: list[dict] = []
        self.segment_events: list[str] = []
        self.messages: list[_FakeMessage] = []
        self.fail_edit_after: str | None = None
        self.edit_error: Exception = TimeoutError("response lost")
        self.fail_send_before_number: int | None = None
        self.fail_send_after_number: int | None = None

    def overwrites_for(self, target):
        for existing, overwrite in self.overwrites.items():
            if existing.id == target.id:
                return overwrite
        return discord_adapter_module.discord.PermissionOverwrite()

    async def edit(self, **kwargs):
        self.edit_calls.append(dict(kwargs))
        if self.fail_edit_after == "name":
            self.name = kwargs.get("name", self.name)
            raise self.edit_error
        self.name = kwargs.get("name", self.name)
        self.topic = kwargs.get("topic", self.topic)
        if "category" in kwargs:
            self.category = kwargs["category"]
            self.category_id = (
                kwargs["category"].id
                if kwargs["category"] is not None
                else None
            )
        if kwargs.get("sync_permissions"):
            self.overwrites = dict(getattr(self.category, "overwrites", {}))
        elif "overwrites" in kwargs:
            self.overwrites = dict(kwargs["overwrites"])
        if self.fail_edit_after == "all":
            raise self.edit_error
        return self

    async def send(self, **kwargs):
        self.segment_events.append("send")
        call_number = len(self.send_calls) + 1
        self.send_calls.append(dict(kwargs))
        if self.fail_send_before_number == call_number:
            raise TimeoutError("send unavailable")
        nonce = kwargs.get("nonce")
        for message in self.messages:
            if nonce is not None and message.nonce == nonce:
                return message
        message = _FakeMessage(
            message_id=7000 + len(self.messages) + 1,
            author=self.guild.me,
            content=kwargs["content"],
            nonce=nonce,
        )
        self.messages.append(message)
        if self.fail_send_after_number == call_number:
            raise TimeoutError("response lost")
        return message

    def history(self, *, limit: int | None, oldest_first: bool = False):
        self.segment_events.append("history")
        del oldest_first

        async def iterator():
            messages = list(reversed(self.messages))
            for message in messages if limit is None else messages[:limit]:
                yield message

        return iterator()


class _FakeGuild:
    def __init__(self, guild_id: int, owner_id: int, bot_id: int) -> None:
        self.id = guild_id
        self.default_role = _Target(guild_id)
        self.owner = _Target(owner_id)
        self.me = _Target(bot_id)
        self.members = {
            owner_id: self.owner,
            bot_id: self.me,
        }
        self.channels: list[object] = []
        self.create_calls: list[dict] = []
        self.create_error: Exception | None = None
        self.create_response_loss = False

    def get_member(self, member_id: int):
        return self.members.get(member_id)

    async def fetch_member(self, member_id: int):
        member = self.members.get(member_id)
        if member is None:
            raise _DiscordNotFound("member")
        return member

    async def fetch_channels(self):
        return list(self.channels)

    async def create_text_channel(self, name: str, **kwargs):
        self.create_calls.append({"name": name, **kwargs})
        if self.create_error is not None:
            raise self.create_error
        channel = _FakeTextChannel(
            channel_id=9000 + len(
                [
                    candidate
                    for candidate in self.channels
                    if isinstance(candidate, _FakeTextChannel)
                ]
            ),
            name=name,
            guild=self,
            category=kwargs.get("category"),
            topic=kwargs.get("topic"),
            overwrites=kwargs.get("overwrites", {}),
        )
        self.channels.append(channel)
        if self.create_response_loss:
            raise TimeoutError("response lost")
        return channel


class _FakeClient:
    def __init__(self, guild: _FakeGuild) -> None:
        self.guild = guild
        self.user = guild.me
        self.fetch_channel_error: Exception | None = None
        self.segment_events: list[str] | None = None

    def get_guild(self, guild_id: int):
        return self.guild if guild_id == self.guild.id else None

    async def fetch_guild(self, guild_id: int):
        if guild_id != self.guild.id:
            raise _DiscordNotFound("guild")
        return self.guild

    def get_channel(self, channel_id: int):
        if self.segment_events is not None:
            self.segment_events.append("channel")
        return next(
            (
                channel
                for channel in self.guild.channels
                if channel.id == channel_id
            ),
            None,
        )

    async def fetch_channel(self, channel_id: int):
        if self.fetch_channel_error is not None:
            raise self.fetch_channel_error
        channel = self.get_channel(channel_id)
        if channel is None:
            raise _DiscordNotFound("channel")
        return channel


def _adapter_world():
    guild = _FakeGuild(444, owner_id=111, bot_id=999)
    active = _FakeCategory(10, guild)
    completed = _FakeCategory(20, guild)
    guild.channels.extend((active, completed))
    adapter = DiscordAdapter(
        PlatformConfig(
            enabled=True,
            token="***",
            project_workspaces={
                "enabled": True,
                "guild_id": "444",
                "owner_user_id": "111",
                "active_category_id": "10",
                "completed_category_id": "20",
            },
        )
    )
    adapter._client = _FakeClient(guild)
    return adapter, guild, active, completed


def _discord_spec(
    *,
    channel_id: str | None = None,
    name: str = "project-alpha",
    category_id: str = "10",
    owner_can_send: bool = True,
) -> ProjectChannelSpec:
    return ProjectChannelSpec(
        project_id="project-1",
        guild_id="444",
        owner_user_id="111",
        name=name,
        category_id=category_id,
        owner_can_send=owner_can_send,
        channel_id=channel_id,
    )


def _event(*, payload=None) -> ProjectEvent:
    return ProjectEvent(
        event_id="event-1",
        project_id="project-1",
        sequence=7,
        kind="turn.succeeded",
        turn_id="turn-1",
        payload=payload or {"status": "succeeded"},
        created_at="2026-07-29T00:00:00Z",
    )


@pytest.mark.asyncio
async def test_adapter_creates_private_channel_atomically_with_exact_overwrites():
    adapter, guild, _active, _completed = _adapter_world()

    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )

    assert state_matches_spec(_discord_spec(), state)
    assert len(guild.create_calls) == 1
    create = guild.create_calls[0]
    assert create["topic"] == project_channel_marker("project-1")
    assert create["category"].id == 10
    overwrite_by_id = {
        target.id: overwrite
        for target, overwrite in create["overwrites"].items()
    }
    assert set(overwrite_by_id) == {111, 444, 999}
    assert overwrite_by_id[444].view_channel is False
    assert overwrite_by_id[444].send_messages is False
    assert overwrite_by_id[444].read_message_history is False
    assert overwrite_by_id[111].view_channel is True
    assert overwrite_by_id[111].send_messages is True
    assert overwrite_by_id[111].read_message_history is True
    assert overwrite_by_id[999].view_channel is True
    assert overwrite_by_id[999].send_messages is True
    assert overwrite_by_id[999].read_message_history is True


@pytest.mark.asyncio
async def test_adapter_blocks_foreign_same_name_instead_of_adopting_or_creating():
    adapter, guild, active, _completed = _adapter_world()
    foreign = _FakeTextChannel(
        channel_id=8000,
        name="project-alpha",
        guild=guild,
        category=active,
        topic="foreign",
        overwrites={},
    )
    guild.channels.append(foreign)

    with pytest.raises(DiscordProjectPortError) as error:
        await adapter.ensure_channel(
            _discord_spec(),
            operation_id="operation-create",
        )

    assert error.value.code is DiscordProjectErrorCode.CONFLICT
    assert len(guild.create_calls) == 0


@pytest.mark.asyncio
async def test_adapter_bound_channel_renames_moves_and_locks_without_replacement():
    adapter, guild, _active, _completed = _adapter_world()
    created = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )

    completed_spec = _discord_spec(
        channel_id=created.channel_id,
        name="project-renamed",
        category_id="20",
        owner_can_send=False,
    )
    completed = await adapter.ensure_channel(
        completed_spec,
        operation_id="operation-complete",
    )

    assert state_matches_spec(completed_spec, completed)
    assert completed.channel_id == created.channel_id
    assert len(guild.create_calls) == 1
    channel = adapter._client.get_channel(int(created.channel_id))
    assert channel.edit_calls[-1]["sync_permissions"] is False
    assert channel.category_id == 20
    assert channel.overwrites_for(guild.owner).send_messages is False
    assert channel.overwrites_for(guild.owner).read_message_history is True


@pytest.mark.asyncio
async def test_adapter_bound_marker_drift_blocks_instead_of_creating_replacement():
    adapter, guild, _active, _completed = _adapter_world()
    created = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(created.channel_id))
    channel.topic = "foreign-marker"

    with pytest.raises(DiscordProjectPortError) as blocked:
        await adapter.ensure_channel(
            _discord_spec(channel_id=created.channel_id),
            operation_id="operation-rename",
        )

    assert blocked.value.code is DiscordProjectErrorCode.CONFLICT
    assert len(guild.create_calls) == 1


@pytest.mark.asyncio
async def test_adapter_create_response_loss_reads_back_without_second_mutation():
    adapter, guild, _active, _completed = _adapter_world()
    guild.create_response_loss = True

    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )

    assert state_matches_spec(_discord_spec(), state)
    assert len(guild.create_calls) == 1


@pytest.mark.asyncio
async def test_adapter_partial_edit_stays_pending_after_readback():
    adapter, _guild, _active, _completed = _adapter_world()
    created = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(created.channel_id))
    channel.fail_edit_after = "name"

    with pytest.raises(DiscordProjectPortError) as pending:
        await adapter.ensure_channel(
            _discord_spec(
                channel_id=created.channel_id,
                name="project-renamed",
                category_id="20",
                owner_can_send=False,
            ),
            operation_id="operation-complete",
        )

    assert pending.value.code is DiscordProjectErrorCode.STATE_MISMATCH
    assert pending.value.retryable is True
    assert len(channel.edit_calls) == 1


@pytest.mark.parametrize(
    "edit_error,expected_code,expected_retryable,expected_retry_after",
    [
        (
            _DiscordForbidden("denied"),
            DiscordProjectErrorCode.FORBIDDEN,
            False,
            None,
        ),
        (
            _DiscordNotFound("gone"),
            DiscordProjectErrorCode.NOT_FOUND,
            False,
            None,
        ),
        (
            _DiscordRateLimited(4.25),
            DiscordProjectErrorCode.RATE_LIMITED,
            True,
            4.25,
        ),
        (
            TimeoutError("response lost"),
            DiscordProjectErrorCode.STATE_MISMATCH,
            True,
            None,
        ),
    ],
)
@pytest.mark.asyncio
async def test_adapter_edit_error_reconciles_then_preserves_provider_authority(
    edit_error,
    expected_code,
    expected_retryable,
    expected_retry_after,
):
    adapter, _guild, _active, _completed = _adapter_world()
    created = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(created.channel_id))
    channel.fail_edit_after = "name"
    channel.edit_error = edit_error

    with pytest.raises(DiscordProjectPortError) as failure:
        await adapter.ensure_channel(
            _discord_spec(
                channel_id=created.channel_id,
                name="project-renamed",
                category_id="20",
                owner_can_send=False,
            ),
            operation_id="operation-complete",
        )

    assert failure.value.code is expected_code
    assert failure.value.retryable is expected_retryable
    assert failure.value.retry_after == expected_retry_after
    assert failure.value.operation_id == "operation-complete"
    assert len(channel.edit_calls) == 1


@pytest.mark.asyncio
async def test_adapter_edit_response_loss_after_full_mutation_reads_as_success():
    adapter, _guild, _active, _completed = _adapter_world()
    created = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(created.channel_id))
    channel.fail_edit_after = "all"
    completed_spec = _discord_spec(
        channel_id=created.channel_id,
        name="project-renamed",
        category_id="20",
        owner_can_send=False,
    )

    completed = await adapter.ensure_channel(
        completed_spec,
        operation_id="operation-complete",
    )

    assert state_matches_spec(completed_spec, completed)
    assert len(channel.edit_calls) == 1


@pytest.mark.asyncio
async def test_adapter_state_rejects_extra_principal_view_grant():
    adapter, _guild, _active, _completed = _adapter_world()
    created = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(created.channel_id))
    channel.overwrites[_Target(222)] = (
        discord_adapter_module.discord.PermissionOverwrite(view_channel=True)
    )

    unsafe = await adapter.read_channel(
        guild_id="444",
        channel_id=created.channel_id,
    )

    assert unsafe is not None
    assert unsafe.only_owner_and_bot_can_view is False
    assert state_matches_spec(
        _discord_spec(channel_id=created.channel_id), unsafe
    ) is False


@pytest.mark.asyncio
async def test_adapter_rate_limit_is_structured_after_confirmed_no_create():
    adapter, guild, _active, _completed = _adapter_world()
    guild.create_error = _DiscordRateLimited(3.25)

    with pytest.raises(DiscordProjectPortError) as limited:
        await adapter.ensure_channel(
            _discord_spec(),
            operation_id="operation-create",
        )

    assert limited.value.code is DiscordProjectErrorCode.RATE_LIMITED
    assert limited.value.retryable is True
    assert limited.value.retry_after == 3.25
    assert limited.value.operation_id == "operation-create"
    assert len(guild.create_calls) == 1


@pytest.mark.asyncio
async def test_adapter_read_returns_none_only_for_confirmed_not_found():
    adapter, _guild, _active, _completed = _adapter_world()

    assert (
        await adapter.read_channel(guild_id="444", channel_id="404")
        is None
    )

    adapter._client.fetch_channel_error = _DiscordForbidden("denied")
    with pytest.raises(DiscordProjectPortError) as forbidden:
        await adapter.read_channel(guild_id="444", channel_id="404")
    assert forbidden.value.code is DiscordProjectErrorCode.FORBIDDEN

    adapter._client.fetch_channel_error = TimeoutError("offline")
    with pytest.raises(DiscordProjectPortError) as transient:
        await adapter.read_channel(guild_id="444", channel_id="404")
    assert transient.value.code is DiscordProjectErrorCode.TRANSIENT
    assert transient.value.retryable is True


@pytest.mark.asyncio
async def test_adapter_publish_is_idempotent_and_marker_readback_is_self_authored():
    adapter, guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    event = _event()
    spoof = _FakeMessage(
        message_id=6000,
        author=guild.owner,
        content=(
            "spoof\n-# "
            + project_event_marker("event-1", part=1, total=1)
        ),
        nonce=None,
    )
    channel.messages.append(spoof)

    assert (
        await adapter.find_event_message(
            channel_id=state.channel_id,
            event_id=event.event_id,
        )
        is None
    )

    first = await adapter.publish_event(
        channel_id=state.channel_id,
        event=event,
        nonce="delivery-nonce",
    )
    replay = await adapter.publish_event(
        channel_id=state.channel_id,
        event=event,
        nonce="delivery-nonce",
    )

    assert first.success is True
    assert replay.message_id == first.message_id
    assert len(channel.send_calls) == 1
    sent = channel.send_calls[0]
    assert project_event_marker("event-1", part=1, total=1) in sent[
        "content"
    ]
    assert sent["allowed_mentions"].everyone is False
    assert sent["allowed_mentions"].roles is False
    assert sent["allowed_mentions"].users is False
    assert sent["allowed_mentions"].replied_user is False
    assert len(sent["nonce"]) == 24
    assert sent["enforce_nonce"] is True


@pytest.mark.asyncio
async def test_adapter_fences_marker_lookup_before_channel_and_history():
    adapter, _guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    events = channel.segment_events
    adapter._client.segment_events = events

    async def before_segment():
        events.append("fence")

    result = await adapter.find_event_message(
        channel_id=state.channel_id,
        event_id="event-1",
        before_segment=before_segment,
    )

    assert result is None
    assert events == ["fence", "channel", "fence", "history"]
    assert channel.send_calls == []


@pytest.mark.asyncio
async def test_adapter_marker_lookup_fence_loss_stops_before_history():
    adapter, _guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    events = channel.segment_events
    adapter._client.segment_events = events
    calls = 0

    async def before_segment():
        nonlocal calls
        calls += 1
        events.append("fence")
        if calls == 2:
            raise RuntimeError("delivery lease lost")

    with pytest.raises(RuntimeError, match="delivery lease lost"):
        await adapter.find_event_message(
            channel_id=state.channel_id,
            event_id="event-1",
            before_segment=before_segment,
        )

    assert events == ["fence", "channel", "fence"]
    assert channel.send_calls == []


@pytest.mark.asyncio
async def test_adapter_marker_lookup_cancellation_while_read_fence_awaits():
    adapter, _guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    events = channel.segment_events
    adapter._client.segment_events = events
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def before_segment():
        nonlocal calls
        calls += 1
        events.append("fence")
        if calls == 2:
            entered.set()
            await release.wait()

    lookup = asyncio.create_task(
        adapter.find_event_message(
            channel_id=state.channel_id,
            event_id="event-1",
            before_segment=before_segment,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)

    lookup.cancel()
    with pytest.raises(asyncio.CancelledError):
        await lookup
    assert events == ["fence", "channel", "fence"]
    assert channel.send_calls == []


@pytest.mark.asyncio
async def test_adapter_publish_response_loss_finds_delivered_nonce_and_marker():
    adapter, _guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    channel.fail_send_after_number = 1

    result = await adapter.publish_event(
        channel_id=state.channel_id,
        event=_event(),
        nonce="delivery-nonce",
    )

    assert result.success is True
    assert result.message_id == "7001"
    assert len(channel.send_calls) == 1


@pytest.mark.asyncio
async def test_adapter_replay_finds_event_older_than_two_hundred_messages():
    adapter, guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    channel.fail_send_after_number = 1
    event = _event()

    first = await adapter.publish_event(
        channel_id=state.channel_id,
        event=event,
        nonce="delivery-nonce",
    )
    channel.messages.extend(
        _FakeMessage(
            message_id=8000 + index,
            author=guild.me,
            content=f"later message {index}",
            nonce=None,
        )
        for index in range(201)
    )
    replay = await adapter.publish_event(
        channel_id=state.channel_id,
        event=event,
        nonce="delivery-nonce",
    )

    assert first.success is True
    assert replay.message_id == first.message_id == "7001"
    assert len(channel.send_calls) == 1


@pytest.mark.asyncio
async def test_adapter_event_marker_collision_fails_closed():
    adapter, guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    channel.messages.extend(
        [
            _FakeMessage(
                message_id=6001,
                author=guild.me,
                content=(
                    "older\n-# "
                    + project_event_marker("event-1", part=1, total=2)
                ),
                nonce=None,
            ),
            _FakeMessage(
                message_id=6002,
                author=guild.me,
                content=(
                    "newer\n-# "
                    + project_event_marker("event-1", part=1, total=1)
                ),
                nonce=None,
            ),
        ]
    )

    with pytest.raises(DiscordProjectPortError) as collision:
        await adapter.find_event_message(
            channel_id=state.channel_id,
            event_id="event-1",
        )

    assert collision.value.code is DiscordProjectErrorCode.CONFLICT


@pytest.mark.asyncio
async def test_adapter_publishes_canonical_frozen_event_payload():
    adapter, _guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    event = _event(
        payload=MappingProxyType(
            {
                "result": MappingProxyType(
                    {"steps": ("planned", "verified")}
                )
            }
        )
    )

    result = await adapter.publish_event(
        channel_id=state.channel_id,
        event=event,
        nonce="delivery-nonce",
    )

    assert result.success is True


@pytest.mark.asyncio
async def test_adapter_partial_event_chunks_remain_pending_and_unacknowledged():
    adapter, _guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    channel.fail_send_before_number = 2
    event = _event(payload={"body": "x" * 6000})

    result = await adapter.publish_event(
        channel_id=state.channel_id,
        event=event,
        nonce="delivery-nonce",
    )

    assert result.success is False
    assert result.error == "partial_delivery"
    assert result.retryable is True
    assert result.raw_response["delivered_chunks"] == 1
    assert result.raw_response["total_chunks"] > 1
    with pytest.raises(DiscordProjectPortError) as partial:
        await adapter.find_event_message(
            channel_id=state.channel_id,
            event_id=event.event_id,
        )
    assert partial.value.code is DiscordProjectErrorCode.PARTIAL_DELIVERY
    assert partial.value.retryable is True


@pytest.mark.asyncio
async def test_adapter_fences_every_event_history_and_send_segment():
    adapter, _guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))

    async def before_segment():
        channel.segment_events.append("fence")

    result = await adapter.publish_event(
        channel_id=state.channel_id,
        event=_event(payload={"body": "x" * 6000}),
        nonce="delivery-nonce",
        before_segment=before_segment,
    )

    assert result.success is True
    assert len(channel.send_calls) == 4
    assert channel.segment_events == [
        "fence",
        "fence",
        "history",
        *(
            ["fence", "send", "fence", "history"]
            * 4
        ),
    ]


@pytest.mark.asyncio
async def test_adapter_cancellation_during_segment_fence_stays_cancelled():
    adapter, _guild, _active, _completed = _adapter_world()
    state = await adapter.ensure_channel(
        _discord_spec(),
        operation_id="operation-create",
    )
    channel = adapter._client.get_channel(int(state.channel_id))
    entered = asyncio.Event()
    release = asyncio.Event()

    async def before_segment():
        entered.set()
        await release.wait()

    delivery = asyncio.create_task(
        adapter.publish_event(
            channel_id=state.channel_id,
            event=_event(payload={"body": "x" * 6000}),
            nonce="delivery-nonce",
            before_segment=before_segment,
        )
    )
    await asyncio.sleep(0)
    assert delivery.done() is False
    assert entered.is_set()

    delivery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivery
    assert channel.send_calls == []
    assert channel.segment_events == []


def test_adapter_project_port_has_no_delete_channel_operation():
    adapter, _guild, _active, _completed = _adapter_world()

    assert not hasattr(adapter, "delete_channel")
