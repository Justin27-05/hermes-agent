"""One thin, capability-free command boundary for ProjectRuntime adapters.

The service deliberately owns no persistence, cache, or policy copy.  Its
callers supply the already-authoritative runtime and snapshot projection; the
service only validates the public command envelope and dispatches to a narrow
existing runtime port.  Commands whose required public runtime port does not
yet exist fail closed instead of reconstructing a second state machine here.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import os
from types import MappingProxyType
from typing import Callable, Mapping

from hermes_cli.project_policy import ActorContext


_CANONICAL_COMMANDS = (
    "project.create",
    "project.rename",
    "project.status",
    "turn.enqueue",
    "queue.status",
    "run.stop",
    "run.resume",
    "approval.resolve",
    "artifact.get",
    "project.mark_technically_complete",
    "project.accept_completion",
    "project.reopen",
)

_MUTATING_COMMANDS = frozenset(
    {
        "project.create",
        "project.rename",
        "turn.enqueue",
        "run.stop",
        "run.resume",
        "approval.resolve",
        "project.mark_technically_complete",
        "project.accept_completion",
        "project.reopen",
    }
)


def _copy_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError("project command payload must be a mapping")
    copied: dict[str, object] = {}
    for key, item in value.items():
        if type(key) is not str:
            raise TypeError("project command payload keys must be strings")
        copied[key] = item
    return MappingProxyType(copied)


@dataclass(frozen=True)
class ProjectCommandRequest:
    """Immutable request normalized before any project authority is invoked."""

    name: str
    project_id: str | None
    payload: Mapping[str, object] = field(default_factory=dict)
    actor: ActorContext | None = None
    idempotency_key: str | None = None
    expected_version: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", _copy_mapping(self.payload))


@dataclass(frozen=True)
class ProjectSnapshot:
    """Adapter-safe projection of the one canonical ProjectRuntime state."""

    project_id: str
    lifecycle: str
    version: int
    canonical_session_id: str | None
    queue_depth: int
    active_turn_id: str | None
    active_run_control: str | None
    pending_approval_id: str | None
    last_event_sequence: int
    artifact: Mapping[str, object] | None = None
    current_phase: str | None = None
    accepted_turn_id: str | None = None
    active_control_version: int | None = None

    def __post_init__(self) -> None:
        if self.artifact is not None:
            object.__setattr__(self, "artifact", _copy_mapping(self.artifact))


@dataclass(frozen=True)
class ProjectCommandError:
    """Stable, secret-free command failure for CLI and local RPC mapping."""

    code: str
    message: str
    project_id: str | None = None
    current_version: int | None = None
    current_control_version: int | None = None


SnapshotReader = Callable[..., ProjectSnapshot]


class ProjectCommandService:
    """Dispatch canonical command names without becoming a second authority."""

    _DISPATCH = MappingProxyType(
        {
            "project.create": "_project_create",
            "project.rename": "_project_rename",
            "project.status": "_project_status",
            "turn.enqueue": "_enqueue_turn",
            "queue.status": "_queue_status",
            "run.stop": "_request_stop",
            "run.resume": "_request_resume",
            "approval.resolve": "_approval_resolve",
            "artifact.get": "_artifact_get",
            "project.mark_technically_complete": "_mark_technically_complete",
            "project.accept_completion": "_accept_completion",
            "project.reopen": "_reopen",
        }
    )

    def __init__(
        self,
        *,
        runtime: object | None = None,
        snapshot_reader: SnapshotReader | None = None,
        operations: object | None = None,
        catalog: object | None = None,
        hermes_authority_factory: Callable[[], object] | None = None,
    ) -> None:
        # `operations` and `catalog` document the only future collaborators.
        # They are retained as capabilities, never inspected as storage.
        self._runtime = runtime
        self._snapshot_reader = snapshot_reader
        self._operations = operations
        self._catalog = catalog
        self._hermes_authority_factory = hermes_authority_factory

    @classmethod
    def command_names(cls) -> tuple[str, ...]:
        return _CANONICAL_COMMANDS

    def dispatch(
        self,
        name: str,
        *,
        project_id: str | None,
        payload: Mapping[str, object],
        actor: ActorContext | None = None,
        idempotency_key: str | None = None,
        expected_version: int | None = None,
    ) -> ProjectSnapshot | ProjectCommandError:
        try:
            request = ProjectCommandRequest(
                name=name,
                project_id=project_id,
                payload=payload,
                actor=actor,
                idempotency_key=idempotency_key,
                expected_version=expected_version,
            )
        except (TypeError, ValueError):
            return self._invalid_argument(project_id)
        return self.execute(request)

    def execute(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        if type(request) is not ProjectCommandRequest:
            return self._invalid_argument(None)
        handler_name = self._DISPATCH.get(request.name)
        if handler_name is None:
            return ProjectCommandError(
                "PROJECT_COMMAND_UNKNOWN",
                "unknown canonical project command",
                request.project_id,
            )
        if request.name in _MUTATING_COMMANDS:
            precondition = self._mutation_precondition(request)
            if precondition is not None:
                return precondition
        elif not self._valid_project_actor(request):
            return ProjectCommandError(
                "PROJECT_COMMAND_ACTOR_REQUIRED",
                "a durable owner actor is required",
                request.project_id,
            )
        return getattr(self, handler_name)(request)

    @staticmethod
    def _valid_project_actor(request: ProjectCommandRequest) -> bool:
        return (
            type(request.project_id) is str
            and bool(request.project_id)
            and ProjectCommandService._valid_owner_actor(request.actor)
        )

    @staticmethod
    def _valid_owner_actor(actor: object) -> bool:
        return isinstance(actor, ActorContext) and actor.is_owner is True

    def _mutation_precondition(
        self, request: ProjectCommandRequest
    ) -> ProjectCommandError | None:
        actor_is_valid = (
            isinstance(request.actor, ActorContext)
            and (
                (
                    request.name == "project.mark_technically_complete"
                    and request.actor.actor_id == "hermes"
                    and request.actor.surface == "system"
                    and request.actor.binding_id == "core"
                    and request.actor.is_owner is False
                )
                or self._valid_owner_actor(request.actor)
            )
        )
        if not (
            actor_is_valid
            and (
                request.name == "project.create"
                or (
                    type(request.project_id) is str
                    and bool(request.project_id)
                )
            )
            and type(request.idempotency_key) is str
            and bool(request.idempotency_key)
            and type(request.expected_version) is int
            and request.expected_version >= 0
        ):
            return ProjectCommandError(
                "PROJECT_COMMAND_MUTATION_PRECONDITION_REQUIRED",
                "actor, idempotency key, and expected version are required",
                request.project_id,
            )
        return None

    @staticmethod
    def _invalid_argument(project_id: str | None) -> ProjectCommandError:
        return ProjectCommandError(
            "PROJECT_COMMAND_INVALID_ARGUMENT",
            "invalid project command arguments",
            project_id if type(project_id) is str else None,
        )

    @staticmethod
    def _port_unavailable(
        project_id: str | None,
    ) -> ProjectCommandError:
        return ProjectCommandError(
            "PROJECT_COMMAND_PORT_UNAVAILABLE",
            "canonical runtime port is unavailable",
            project_id,
        )

    def _snapshot(
        self,
        project_id: str,
        *,
        actor: ActorContext | None = None,
        hermes_authority: object | None = None,
        artifact: Mapping[str, object] | None = None,
        accepted_turn_id: str | None = None,
    ) -> ProjectSnapshot | ProjectCommandError:
        if hermes_authority is not None:
            runtime_snapshot = getattr(
                self._runtime, "snapshot_for_core", None
            )
            snapshot_argument = hermes_authority
        else:
            runtime_snapshot = getattr(
                self._runtime, "snapshot_for_actor", None
            )
            snapshot_argument = actor
        if callable(runtime_snapshot) and snapshot_argument is not None:
            try:
                view = runtime_snapshot(project_id, snapshot_argument)
                return ProjectSnapshot(
                    project_id=view.project_id,
                    lifecycle=view.lifecycle,
                    version=view.version,
                    canonical_session_id=view.canonical_session_id,
                    queue_depth=view.queue_depth,
                    active_turn_id=view.active_turn_id,
                    active_run_control=view.active_run_control,
                    pending_approval_id=view.pending_approval_id,
                    last_event_sequence=view.last_event_sequence,
                    artifact=artifact,
                    current_phase=view.current_phase,
                    accepted_turn_id=accepted_turn_id,
                    active_control_version=(
                        getattr(
                            view,
                            "active_control_version",
                            None,
                        )
                    ),
                )
            except Exception as exc:
                return self._runtime_error(project_id, exc)
        if not callable(self._snapshot_reader):
            return self._port_unavailable(project_id)
        try:
            snapshot = self._snapshot_reader(
                project_id,
                artifact=artifact,
            )
            return (
                replace(
                    snapshot,
                    accepted_turn_id=accepted_turn_id,
                )
                if (
                    type(snapshot) is ProjectSnapshot
                    and accepted_turn_id is not None
                )
                else snapshot
            )
        except TypeError:
            # A simple snapshot reader may intentionally not expose artifacts.
            if artifact is None:
                try:
                    snapshot = self._snapshot_reader(project_id)
                    return (
                        replace(
                            snapshot,
                            accepted_turn_id=accepted_turn_id,
                        )
                        if (
                            type(snapshot) is ProjectSnapshot
                            and accepted_turn_id is not None
                        )
                        else snapshot
                    )
                except Exception:
                    return self._port_unavailable(project_id)
            return self._port_unavailable(project_id)
        except Exception:
            return self._port_unavailable(project_id)

    def _project_status(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        assert request.project_id is not None
        if request.payload:
            return self._invalid_argument(request.project_id)
        authorization = self._authorize_read(request)
        if authorization is not None:
            return authorization
        return self._snapshot(
            request.project_id, actor=request.actor
        )

    def _queue_status(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        if request.payload:
            return self._invalid_argument(request.project_id)
        authorization = self._authorize_read(request)
        if authorization is not None:
            return authorization
        assert request.project_id is not None
        return self._snapshot(
            request.project_id, actor=request.actor
        )

    def _project_create(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        method = getattr(self._runtime, "create_managed_project", None)
        if not callable(method):
            return self._port_unavailable(None)
        name = request.payload.get("name")
        current_phase = request.payload.get(
            "current_phase", "planning"
        )
        folders = request.payload.get("folders", ())
        primary_path = request.payload.get("primary_path")
        allowed = {
            "board_slug",
            "color",
            "current_phase",
            "description",
            "folders",
            "icon",
            "name",
            "primary_path",
            "slug",
        }
        if (
            set(request.payload) - allowed
            or type(name) is not str
            or not name
            or type(current_phase) is not str
            or not current_phase
            or not isinstance(folders, (tuple, list))
            or any(
                type(folder) is not str
                or not folder.strip()
                or not os.path.isabs(folder)
                for folder in folders
            )
            or (
                primary_path is not None
                and (
                    type(primary_path) is not str
                    or not primary_path.strip()
                    or not os.path.isabs(primary_path)
                )
            )
            or any(
                value is not None and type(value) is not str
                for value in (
                    request.payload.get("board_slug"),
                    request.payload.get("color"),
                    request.payload.get("description"),
                    request.payload.get("icon"),
                    request.payload.get("slug"),
                )
            )
        ):
            return self._invalid_argument(None)
        assert (
            request.actor is not None
            and request.idempotency_key is not None
            and request.expected_version is not None
        )
        try:
            state = method(
                request.actor,
                name=name,
                idempotency_key=request.idempotency_key,
                expected_version=request.expected_version,
                current_phase=current_phase,
                folders=tuple(folders),
                slug=request.payload.get("slug"),
                primary_path=primary_path,
                description=request.payload.get("description"),
                icon=request.payload.get("icon"),
                color=request.payload.get("color"),
                board_slug=request.payload.get("board_slug"),
            )
        except Exception as exc:
            return self._runtime_error(None, exc)
        return self._snapshot(
            state.project_id, actor=request.actor
        )

    def _project_rename(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        method = getattr(self._runtime, "rename_project", None)
        name = request.payload.get("name")
        if not callable(method):
            return self._port_unavailable(request.project_id)
        if (
            set(request.payload) != {"name"}
            or type(name) is not str
            or not name
        ):
            return self._invalid_argument(request.project_id)
        assert (
            request.project_id is not None
            and request.actor is not None
            and request.idempotency_key is not None
            and request.expected_version is not None
        )
        try:
            method(
                request.project_id,
                name,
                request.actor,
                idempotency_key=request.idempotency_key,
                expected_version=request.expected_version,
            )
        except Exception as exc:
            return self._runtime_error(request.project_id, exc)
        return self._snapshot(
            request.project_id, actor=request.actor
        )

    def _enqueue_turn(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        runtime = self._runtime
        if not callable(getattr(runtime, "enqueue_turn", None)):
            return self._port_unavailable(request.project_id)
        assert (
            request.project_id is not None
            and request.actor is not None
            and request.idempotency_key is not None
            and request.expected_version is not None
        )
        try:
            turn = runtime.enqueue_turn(
                request.project_id,
                dict(request.payload),
                request.actor,
                idempotency_key=request.idempotency_key,
                expected_version=request.expected_version,
            )
        except Exception as exc:
            return self._runtime_error(request.project_id, exc)
        accepted_turn_id = getattr(turn, "turn_id", None)
        if type(accepted_turn_id) is not str or not accepted_turn_id:
            return self._port_unavailable(request.project_id)
        return self._snapshot(
            request.project_id,
            actor=request.actor,
            accepted_turn_id=accepted_turn_id,
        )

    def _request_stop(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        return self._request_control("request_stop", request)

    def _request_resume(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        return self._request_control("request_resume", request)

    def _request_control(
        self, method_name: str, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        method = getattr(self._runtime, method_name, None)
        turn_id = request.payload.get("turn_id")
        control_version = request.payload.get("expected_control_version")
        if not callable(method):
            return self._port_unavailable(request.project_id)
        if (
            set(request.payload)
            != {"turn_id", "expected_control_version"}
            or
            type(turn_id) is not str
            or not turn_id
            or type(control_version) is not int
            or control_version < 0
        ):
            return self._invalid_argument(request.project_id)
        assert (
            request.project_id is not None
            and request.actor is not None
            and request.idempotency_key is not None
            and request.expected_version is not None
        )
        try:
            method(
                request.project_id,
                turn_id,
                request.actor,
                idempotency_key=request.idempotency_key,
                expected_version=request.expected_version,
                expected_control_version=control_version,
            )
        except Exception as exc:
            return self._runtime_error(request.project_id, exc)
        return self._snapshot(
            request.project_id, actor=request.actor
        )

    def _approval_resolve(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        method = getattr(self._runtime, "resolve_approval", None)
        approval_id = request.payload.get("approval_id")
        outcome = request.payload.get("outcome")
        if not callable(method):
            return self._port_unavailable(request.project_id)
        if (
            set(request.payload) != {"approval_id", "outcome"}
            or type(approval_id) is not str
            or not approval_id
            or outcome not in {"approved", "denied"}
        ):
            return self._invalid_argument(request.project_id)
        assert (
            request.project_id is not None
            and request.actor is not None
            and request.idempotency_key is not None
            and request.expected_version is not None
        )
        try:
            method(
                request.project_id,
                approval_id,
                outcome,
                request.actor,
                idempotency_key=request.idempotency_key,
                expected_version=request.expected_version,
            )
        except Exception as exc:
            code = getattr(getattr(exc, "code", None), "value", None)
            operation_resolver = getattr(
                self._operations, "resolve_operation_approval", None
            )
            if (
                code != "operation_approval_required"
                or not callable(operation_resolver)
            ):
                return self._runtime_error(request.project_id, exc)
            try:
                operation = operation_resolver(
                    approval_id,
                    request.actor,
                    outcome=outcome,
                )
            except Exception as operation_exc:
                return self._runtime_error(
                    request.project_id, operation_exc
                )
            if (
                getattr(operation, "project_id", None)
                != request.project_id
            ):
                return ProjectCommandError(
                    "PROJECT_COMMAND_APPROVAL_CONFLICT",
                    "project approval conflicts with canonical state",
                    request.project_id,
                )
        return self._snapshot(
            request.project_id, actor=request.actor
        )

    def _artifact_get(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        artifact_id = request.payload.get("artifact_id")
        method = getattr(self._runtime, "artifact_for_actor", None)
        actor_bound = callable(method)
        if not actor_bound:
            method = getattr(self._runtime, "artifact_for_id", None)
        if not callable(method):
            return self._port_unavailable(request.project_id)
        if (
            set(request.payload) != {"artifact_id"}
            or type(artifact_id) is not str
            or not artifact_id
        ):
            return self._invalid_argument(request.project_id)
        assert request.project_id is not None
        authorization = self._authorize_read(request)
        if authorization is not None:
            return authorization
        try:
            artifact = (
                method(request.project_id, artifact_id, request.actor)
                if actor_bound
                else method(request.project_id, artifact_id)
            )
        except Exception as exc:
            return self._runtime_error(request.project_id, exc)
        if not isinstance(artifact, Mapping):
            return ProjectCommandError(
                "PROJECT_COMMAND_ARTIFACT_NOT_FOUND",
                "project artifact was not found",
                request.project_id,
            )
        return self._snapshot(
            request.project_id,
            actor=request.actor,
            artifact=artifact,
        )

    def _mark_technically_complete(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        return self._lifecycle_command(
            "mark_technically_complete", request
        )

    def _accept_completion(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        return self._lifecycle_command(
            "accept_completion", request
        )

    def _reopen(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError:
        return self._lifecycle_command("reopen_project", request)

    def _lifecycle_command(
        self,
        method_name: str,
        request: ProjectCommandRequest,
    ) -> ProjectSnapshot | ProjectCommandError:
        method = getattr(self._runtime, method_name, None)
        if not callable(method):
            return self._port_unavailable(request.project_id)
        if request.payload:
            return self._invalid_argument(request.project_id)
        assert (
            request.project_id is not None
            and request.actor is not None
            and request.idempotency_key is not None
            and request.expected_version is not None
        )
        hermes_authority: object | None = None
        invocation_authority: object = request.actor
        if method_name == "mark_technically_complete":
            if not callable(self._hermes_authority_factory):
                return self._port_unavailable(request.project_id)
            try:
                hermes_authority = self._hermes_authority_factory()
            except Exception:
                return self._port_unavailable(request.project_id)
            invocation_authority = hermes_authority
        try:
            method(
                request.project_id,
                invocation_authority,
                idempotency_key=request.idempotency_key,
                expected_version=request.expected_version,
            )
        except Exception as exc:
            return self._runtime_error(request.project_id, exc)
        return self._snapshot(
            request.project_id,
            actor=request.actor,
            hermes_authority=hermes_authority,
        )

    def _authorize_read(
        self, request: ProjectCommandRequest
    ) -> ProjectCommandError | None:
        """Reuse Runtime's durable binding check for read-only projections."""
        method = getattr(self._runtime, "list_queue", None)
        if not callable(method):
            return self._port_unavailable(request.project_id)
        assert request.project_id is not None and request.actor is not None
        try:
            method(request.project_id, request.actor)
        except Exception as exc:
            return self._runtime_error(request.project_id, exc)
        return None

    def _unavailable(
        self, request: ProjectCommandRequest
    ) -> ProjectCommandError:
        # The current runtime deliberately has no public, transaction-owning
        # ports for catalog mutation, completion lifecycle, or command-level
        # approval resolution with idempotency/CAS. Reconstructing any of those
        # here would add a competing authority, so expose the concrete blocker.
        return self._port_unavailable(request.project_id)

    @staticmethod
    def _runtime_error(
        project_id: str | None,
        error: Exception,
    ) -> ProjectCommandError:
        code = getattr(error, "code", None)
        code_value = getattr(code, "value", None)
        stable_code = (
            f"PROJECT_RUNTIME_{str(code_value).upper()}"
            if type(code_value) is str and code_value
            else "PROJECT_COMMAND_REJECTED"
        )
        current_version = getattr(error, "current_version", None)
        current_control_version = getattr(
            error,
            "current_control_version",
            None,
        )
        return ProjectCommandError(
            stable_code,
            "canonical project runtime rejected command",
            project_id,
            current_version if type(current_version) is int else None,
            (
                current_control_version
                if type(current_control_version) is int
                else None
            ),
        )
