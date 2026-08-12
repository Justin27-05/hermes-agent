"""Trusted Discord ingress for the canonical project runtime."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from typing import Callable

from gateway.project_surfaces import DiscordProjectSurface
from hermes_cli import project_runtime_db as runtime_db
from hermes_cli.project_command_service import (
    ProjectCommandError,
    ProjectCommandService,
)
from hermes_cli.project_policy import ActorContext
from hermes_cli.project_runtime import (
    ProjectRuntime,
)


@dataclass(frozen=True)
class ProjectIngressResult:
    handled: bool
    accepted: bool = False
    project_id: str | None = None
    turn_id: str | None = None
    error_code: str | None = None
    response: str | None = None


class ProjectRuntimeIngress:
    """Route an authenticated, bound Discord message to one durable turn."""

    def __init__(
        self,
        *,
        db_factory: Callable[[], object],
        wake: Callable[[], None] | None = None,
        accept_new_turns: bool = True,
        surface: DiscordProjectSurface | None = None,
    ) -> None:
        if not callable(db_factory):
            raise TypeError("db_factory must be callable")
        if wake is not None and not callable(wake):
            raise TypeError("wake must be callable")
        if type(accept_new_turns) is not bool:
            raise TypeError("accept_new_turns must be bool")
        if surface is not None and not isinstance(surface, DiscordProjectSurface):
            raise TypeError("surface must be a DiscordProjectSurface")
        self._db_factory = db_factory
        self._wake = wake
        self._accept_new_turns = accept_new_turns
        self._surface = surface

    @staticmethod
    def _discord_identity(source: object) -> str | None:
        thread_id = getattr(source, "thread_id", None)
        chat_id = getattr(source, "chat_id", None)
        if type(thread_id) is str and thread_id:
            return thread_id
        if type(chat_id) is str and chat_id:
            return chat_id
        return None

    @staticmethod
    def _message_identity(event: object, source: object) -> str | None:
        event_id = getattr(event, "message_id", None)
        source_id = getattr(source, "message_id", None)
        supplied = tuple(
            value
            for value in (event_id, source_id)
            if value is not None
        )
        if (
            not supplied
            or any(type(value) is not str or not value for value in supplied)
            or len(set(supplied)) != 1
        ):
            return None
        return supplied[0]

    @staticmethod
    def _command_error_code(error: ProjectCommandError) -> str:
        code = error.code
        if type(code) is not str or not code:
            return "PROJECT_INGRESS_REJECTED"
        return code

    @staticmethod
    def _event_kind_for_mutating_command(name: str) -> str | None:
        """Map the public command name to the Runtime's durable event kind."""
        return {
            "project.rename": "project.renamed",
            "run.stop": "run.stop_requested",
            "run.resume": "run.resume_requested",
            "approval.resolve": "approval.resolved",
            "project.accept_completion": "project.completion_accepted",
            "project.reopen": "project.reopened",
        }.get(name)

    @staticmethod
    def _discord_scope(event: object, source: object) -> tuple[str | None, str | None]:
        """Read trusted guild/category values from the Discord event only."""
        def snowflake(value: object) -> str | None:
            if type(value) is str and value:
                return value
            if type(value) is int and value > 0:
                return str(value)
            return None

        scope_id = getattr(source, "scope_id", None)
        raw_message = getattr(event, "raw_message", None)
        guild_id = snowflake(scope_id)
        if guild_id is None:
            source_guild_id = getattr(source, "guild_id", None)
            guild_id = snowflake(source_guild_id)
        if guild_id is None:
            raw_guild_id = getattr(getattr(raw_message, "guild", None), "id", None)
            guild_id = snowflake(raw_guild_id)
        channel = getattr(raw_message, "channel", None)
        category_id = getattr(channel, "category_id", None)
        if category_id is None:
            category_id = getattr(getattr(channel, "parent", None), "category_id", None)
        return (
            guild_id,
            snowflake(category_id),
        )

    def route_command(
        self, event: object, command: object
    ) -> ProjectIngressResult:
        """Dispatch one already-parsed managed-project command via the Service."""
        source = getattr(event, "source", None)
        platform = getattr(source, "platform", None)
        platform_value = getattr(platform, "value", platform)
        if platform_value != "discord":
            return ProjectIngressResult(handled=False)
        external_binding_id = self._discord_identity(source)
        if external_binding_id is None:
            return ProjectIngressResult(handled=False)
        managed_project_candidate = (
            getattr(event, "_hermes_managed_project_candidate", False) is True
        )
        name = getattr(command, "name", None)
        raw_payload = getattr(command, "payload", None)
        if (
            type(name) is not str
            or name not in ProjectCommandService.command_names()
            or not isinstance(raw_payload, dict)
        ):
            return ProjectIngressResult(
                handled=True,
                error_code="PROJECT_INGRESS_COMMAND_INVALID",
                response="Dit projectcommando is ongeldig en niet uitgevoerd.",
            )
        message_id = self._message_identity(event, source)
        if message_id is None:
            return ProjectIngressResult(
                handled=True,
                error_code="PROJECT_INGRESS_INVALID_MESSAGE",
                response=(
                    "Dit projectcommando mist een stabiele Discord-identiteit "
                    "en is niet uitgevoerd."
                ),
            )
        try:
            connection = self._db_factory()
        except Exception:
            return ProjectIngressResult(
                handled=True,
                error_code="PROJECT_INGRESS_UNAVAILABLE",
                response=(
                    "De projectruntime is tijdelijk niet beschikbaar; "
                    "het commando is niet uitgevoerd."
                ),
            )
        with closing(connection) as conn:
            reserved_location = False
            managed_scope = False
            if self._surface is not None:
                guild_id, category_id = self._discord_scope(event, source)
                reserved_location = self._surface.reserves_discord_location(
                    guild_id=guild_id,
                    category_id=category_id,
                )
                managed_scope = self._surface.accepts_discord_source(
                    source,
                    guild_id=guild_id,
                    category_id=category_id,
                )
            try:
                binding = runtime_db.binding_for_surface_identity(
                    conn,
                    surface="discord",
                    external_binding_id=external_binding_id,
                )
            except Exception:
                return ProjectIngressResult(
                    handled=True,
                    error_code="PROJECT_INGRESS_UNAVAILABLE",
                    response=(
                        "De projectruntime is tijdelijk niet beschikbaar; "
                        "het commando is niet uitgevoerd."
                    ),
                )
            if binding is None:
                if reserved_location or managed_project_candidate:
                    return ProjectIngressResult(
                        handled=True,
                        error_code="PROJECT_INGRESS_UNBOUND_CHANNEL",
                        response=(
                            "Dit beheerde Discord-kanaal is niet duurzaam "
                            "aan een project gebonden; het commando is niet uitgevoerd."
                        ),
                    )
                return ProjectIngressResult(handled=False)
            if bool(getattr(source, "is_bot", False)):
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code="PROJECT_INGRESS_BOT_DELIVERY",
                )
            if self._surface is not None:
                if not managed_scope:
                    return ProjectIngressResult(
                        handled=True,
                        project_id=binding.project_id,
                        error_code="PROJECT_INGRESS_SURFACE_NOT_AUTHORIZED",
                        response=(
                            "Dit Discord-kanaal of deze identiteit is niet "
                            "toegestaan voor dit project."
                        ),
                    )
            user_id = getattr(source, "user_id", None)
            principal_id = runtime_db.principal_for_surface_binding(
                conn,
                project_id=binding.project_id,
                binding_id=binding.binding_id,
            )
            if (
                type(user_id) is not str
                or not user_id
                or principal_id != user_id
            ):
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code="PROJECT_INGRESS_ACTOR_NOT_AUTHORIZED",
                    response=(
                        "Deze Discord-identiteit is niet aan het project "
                        "gebonden; het commando is niet uitgevoerd."
                    ),
                )
            if name == "project.create":
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code=(
                        "PROJECT_INGRESS_PROVISIONING_REQUIRED"
                    ),
                    response=(
                        "Projectaanmaak via Discord is pas beschikbaar "
                        "wanneer kanaal- en bindingprovisioning is gekoppeld; "
                        "er is niets aangemaakt."
                    ),
                )
            actor = ActorContext(
                binding.actor_id,
                "discord",
                binding.binding_id,
                True,
            )
            service = ProjectCommandService(runtime=ProjectRuntime(conn))
            idempotency_key = (
                f"discord-command:{binding.binding_id}:{message_id}:{name}"
            )
            replay_kind = self._event_kind_for_mutating_command(name)
            if replay_kind is not None:
                event_id = ProjectRuntime._command_event_id(
                    binding.project_id, idempotency_key
                )
                replay = conn.execute(
                    "SELECT kind FROM project_events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if replay is not None:
                    if replay["kind"] != replay_kind:
                        return ProjectIngressResult(
                            handled=True,
                            project_id=binding.project_id,
                            error_code="PROJECT_INGRESS_REPLAY_CONFLICT",
                            response="Dit projectcommando kon niet veilig worden uitgevoerd.",
                        )
                    snapshot = service.dispatch(
                        "project.status",
                        project_id=binding.project_id,
                        payload={},
                        actor=actor,
                    )
                    if isinstance(snapshot, ProjectCommandError):
                        return ProjectIngressResult(
                            handled=True,
                            project_id=binding.project_id,
                            error_code=self._command_error_code(snapshot),
                            response="Dit projectcommando kon niet veilig worden uitgevoerd.",
                        )
                    project_id = snapshot.project_id
                    replayed = True
                else:
                    replayed = False
            else:
                replayed = False
            expected_version: int | None = None
            if not replayed and name in {
                "project.rename",
                "run.stop",
                "run.resume",
                "approval.resolve",
                "project.accept_completion",
                "project.reopen",
            }:
                snapshot = service.dispatch(
                    "project.status",
                    project_id=binding.project_id,
                    payload={},
                    actor=actor,
                )
                if isinstance(snapshot, ProjectCommandError):
                    return ProjectIngressResult(
                        handled=True,
                        project_id=binding.project_id,
                        error_code=self._command_error_code(snapshot),
                        response="Dit projectcommando kon niet veilig worden uitgevoerd.",
                    )
                expected_version = snapshot.version
            if not replayed:
                result = service.dispatch(
                    name,
                    project_id=binding.project_id,
                    payload=raw_payload,
                    actor=actor,
                    idempotency_key=(
                        idempotency_key if expected_version is not None else None
                    ),
                    expected_version=expected_version,
                )
                if isinstance(result, ProjectCommandError):
                    return ProjectIngressResult(
                        handled=True,
                        project_id=binding.project_id,
                        error_code=self._command_error_code(result),
                        response="Dit projectcommando kon niet veilig worden uitgevoerd.",
                    )
                project_id = result.project_id
        if name in {"run.resume", "turn.enqueue"} and self._wake is not None:
            try:
                self._wake()
            except Exception:
                pass
        return ProjectIngressResult(
            handled=True,
            accepted=True,
            project_id=project_id,
            response="Projectcommando geaccepteerd.",
        )

    def route(self, event: object) -> ProjectIngressResult:
        source = getattr(event, "source", None)
        platform = getattr(source, "platform", None)
        platform_value = getattr(platform, "value", platform)
        if platform_value != "discord":
            return ProjectIngressResult(handled=False)
        external_binding_id = self._discord_identity(source)
        if external_binding_id is None:
            return ProjectIngressResult(handled=False)
        managed_project_candidate = (
            getattr(event, "_hermes_managed_project_candidate", False) is True
        )

        try:
            connection = self._db_factory()
        except Exception:
            return ProjectIngressResult(
                handled=True,
                error_code="PROJECT_INGRESS_UNAVAILABLE",
                response=(
                    "De projectruntime is tijdelijk niet beschikbaar; "
                    "het bericht is niet uitgevoerd."
                ),
            )
        with closing(connection) as conn:
            reserved_location = False
            managed_scope = False
            if self._surface is not None:
                guild_id, category_id = self._discord_scope(event, source)
                reserved_location = self._surface.reserves_discord_location(
                    guild_id=guild_id,
                    category_id=category_id,
                )
                managed_scope = self._surface.accepts_discord_source(
                    source,
                    guild_id=guild_id,
                    category_id=category_id,
                )
            try:
                binding = runtime_db.binding_for_surface_identity(
                    conn,
                    surface="discord",
                    external_binding_id=external_binding_id,
                )
            except Exception:
                return ProjectIngressResult(
                    handled=True,
                    error_code="PROJECT_INGRESS_UNAVAILABLE",
                    response=(
                        "De projectruntime is tijdelijk niet beschikbaar; "
                        "het bericht is niet uitgevoerd."
                    ),
                )
            if binding is None:
                if reserved_location or managed_project_candidate:
                    return ProjectIngressResult(
                        handled=True,
                        error_code="PROJECT_INGRESS_UNBOUND_CHANNEL",
                        response=(
                            "Dit beheerde Discord-kanaal is niet duurzaam "
                            "aan een project gebonden; het bericht is niet uitgevoerd."
                        ),
                    )
                return ProjectIngressResult(handled=False)
            if bool(getattr(source, "is_bot", False)):
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code="PROJECT_INGRESS_BOT_DELIVERY",
                )
            if self._surface is not None:
                if not managed_scope:
                    return ProjectIngressResult(
                        handled=True,
                        project_id=binding.project_id,
                        error_code="PROJECT_INGRESS_SURFACE_NOT_AUTHORIZED",
                        response=(
                            "Dit Discord-kanaal of deze identiteit is niet "
                            "toegestaan voor dit project."
                        ),
                    )
            message_id = self._message_identity(event, source)
            message = getattr(event, "text", None)
            user_id = getattr(source, "user_id", None)
            if (
                message_id is None
                or type(message) is not str
                or not message.strip()
                or type(user_id) is not str
                or not user_id
            ):
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code="PROJECT_INGRESS_INVALID_MESSAGE",
                    response=(
                        "Dit projectbericht mist een stabiele "
                        "Discord-identiteit en is niet uitgevoerd."
                    ),
                )
            principal_id = runtime_db.principal_for_surface_binding(
                conn,
                project_id=binding.project_id,
                binding_id=binding.binding_id,
            )
            if principal_id != user_id:
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code=(
                        "PROJECT_INGRESS_ACTOR_NOT_AUTHORIZED"
                    ),
                    response=(
                        "Deze Discord-identiteit is niet aan het "
                        "project gebonden; het bericht is niet uitgevoerd."
                    ),
                )

            from gateway.slash_commands import parse_project_slash_command

            project_control = parse_project_slash_command(message)
            message_type = getattr(
                getattr(event, "message_type", None),
                "value",
                getattr(event, "message_type", None),
            )
            if (
                message_type not in {None, "text"}
                and not (
                    message_type == "command" and project_control is not None
                )
                or bool(getattr(event, "media_urls", None))
                or bool(getattr(event, "media_types", None))
                or getattr(event, "prompt_response", None) is not None
                or bool(getattr(event, "metadata", None))
            ):
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code=(
                        "PROJECT_INGRESS_UNSUPPORTED_PAYLOAD"
                    ),
                    response=(
                        "Dit projectkanaal ondersteunt nu alleen "
                        "tekstberichten; de inhoud is niet uitgevoerd."
                    ),
                )
            if message.lstrip().startswith("/"):
                if project_control is not None:
                    # The command route repeats the same binding/owner/surface
                    # checks before it invokes ProjectCommandService.  Leave a
                    # recognized control out of user-turn ingestion so it never
                    # becomes an agent run.
                    return ProjectIngressResult(handled=False)
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code=(
                        "PROJECT_INGRESS_COMMAND_UNSUPPORTED"
                    ),
                    response=(
                        "Dit projectcommando is nog niet aan de "
                        "canonieke runtime gekoppeld en is niet uitgevoerd."
                    ),
                )
            if not self._accept_new_turns:
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code="PROJECT_INGRESS_DRAINING",
                    response=(
                        "Hermes voert onderhoud uit en accepteert nu geen "
                        "nieuwe projecttaken; stuur het bericht later opnieuw."
                    ),
                )

            payload: dict[str, object] = {"message": message}
            reply = {
                "author_id": getattr(
                    event, "reply_to_author_id", None
                ),
                "author_name": getattr(
                    event, "reply_to_author_name", None
                ),
                "is_own_message": bool(
                    getattr(event, "reply_to_is_own_message", False)
                ),
                "message_id": getattr(
                    event, "reply_to_message_id", None
                ),
                "text": getattr(event, "reply_to_text", None),
            }
            if any(
                value not in {None, False, ""}
                for value in reply.values()
            ):
                payload["reply"] = reply
            channel_context = getattr(
                event, "channel_context", None
            )
            channel_prompt = getattr(event, "channel_prompt", None)
            auto_skill = getattr(event, "auto_skill", None)
            if type(channel_context) is str and channel_context:
                payload["channel_context"] = channel_context
            if type(channel_prompt) is str and channel_prompt:
                payload["channel_prompt"] = channel_prompt
            if (
                type(auto_skill) is str
                and auto_skill
            ) or (
                type(auto_skill) is list
                and all(
                    type(skill) is str and skill
                    for skill in auto_skill
                )
            ):
                payload["auto_skill"] = auto_skill

            actor = ActorContext(
                binding.actor_id,
                "discord",
                binding.binding_id,
                True,
            )
            command_service = ProjectCommandService(
                runtime=ProjectRuntime(conn)
            )
            idempotency_key = (
                f"discord-message:{binding.binding_id}:{message_id}"
            )
            for _attempt in range(3):
                snapshot = command_service.dispatch(
                    "project.status",
                    project_id=binding.project_id,
                    payload={},
                    actor=actor,
                )
                if isinstance(snapshot, ProjectCommandError):
                    return ProjectIngressResult(
                        handled=True,
                        project_id=binding.project_id,
                        error_code=self._command_error_code(snapshot),
                        response=(
                            "Dit projectbericht kon niet veilig worden "
                            "toegevoegd."
                        ),
                    )
                result = command_service.dispatch(
                    "turn.enqueue",
                    project_id=binding.project_id,
                    payload=payload,
                    actor=actor,
                    idempotency_key=idempotency_key,
                    expected_version=snapshot.version,
                )
                if isinstance(result, ProjectCommandError):
                    if (
                        result.code
                        == "PROJECT_RUNTIME_PROJECT_VERSION_CONFLICT"
                    ):
                        continue
                    return ProjectIngressResult(
                        handled=True,
                        project_id=binding.project_id,
                        error_code=self._command_error_code(result),
                        response=(
                            "Dit projectbericht kon niet veilig worden "
                            "toegevoegd."
                        ),
                    )
                turn_row = conn.execute(
                    """
                    SELECT turn_id FROM project_turns
                    WHERE project_id = ?
                      AND idempotency_key = ?
                      AND origin_binding_id = ?
                    """,
                    (
                        binding.project_id,
                        idempotency_key,
                        binding.binding_id,
                    ),
                ).fetchone()
                if turn_row is None:
                    return ProjectIngressResult(
                        handled=True,
                        project_id=binding.project_id,
                        error_code="PROJECT_INGRESS_UNAVAILABLE",
                        response=(
                            "De projectruntime is tijdelijk niet "
                            "beschikbaar; het bericht is niet uitgevoerd."
                        ),
                    )
                turn_id = turn_row["turn_id"]
                break
            else:
                return ProjectIngressResult(
                    handled=True,
                    project_id=binding.project_id,
                    error_code="PROJECT_RUNTIME_PROJECT_VERSION_CONFLICT",
                    response=(
                        "Het project veranderde gelijktijdig; "
                        "stuur het bericht opnieuw."
                    ),
                )

        if self._wake is not None:
            try:
                self._wake()
            except Exception:
                pass
        return ProjectIngressResult(
            handled=True,
            accepted=True,
            project_id=binding.project_id,
            turn_id=turn_id,
            response="Projecttaak toegevoegd aan Hermes.",
        )


__all__ = ["ProjectIngressResult", "ProjectRuntimeIngress"]
