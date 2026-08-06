"""Strict, local-only RPC seam for canonical project runtime access.

This module owns wire validation, actor construction, and deliberately narrow
response projections.  :class:`ProjectCommandService` remains the sole command
authority.  Desktop reads use the canonical ProjectRuntime/database authority
and a bounded cross-database stable cut for transcript projection.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import ipaddress
import json
import math
import ntpath
import re
import sqlite3
from typing import Callable, Protocol
import unicodedata
from urllib.parse import parse_qsl, unquote, urlsplit

from hermes_cli import project_runtime_db as runtime_db
from hermes_cli.project_command_service import (
    ProjectCommandError,
    ProjectCommandRequest,
    ProjectCommandService,
    ProjectSnapshot,
)
from hermes_cli.project_policy import ActorContext
from hermes_cli.project_runtime import ProjectRuntime, ProjectRuntimeError


_COMMAND_METHOD = "project.command"
_SNAPSHOT_METHOD = "project.runtime.snapshot"
_EVENTS_METHOD = "project.runtime.events"
_ACK_METHOD = "project.runtime.ack"
_RUNTIME_READ_METHODS = frozenset(
    {_SNAPSHOT_METHOD, _EVENTS_METHOD, _ACK_METHOD}
)
_REQUEST_FIELDS = frozenset({"jsonrpc", "id", "method", "params"})
_COMMAND_PARAMS = frozenset(
    {"name", "project_id", "payload", "idempotency_key", "expected_version"}
)
_SNAPSHOT_PARAMS = frozenset({"project_id"})
_EVENTS_PARAMS = frozenset({"project_id", "after_sequence", "limit"})
_ACK_PARAMS = frozenset({"project_id", "binding_id", "cursor"})
_COMMAND_NAMES = frozenset(ProjectCommandService.command_names())
_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,95}$")
_MAX_JSON_DEPTH = 128
_MAX_JSON_NODES = 10_000
_MAX_EVENT_PAGE = 500
_MAX_SAFE_INTEGER = (1 << 53) - 1
_MAX_URL_PARAMETER_CHARS = 4096
_MAX_PERCENT_DECODE_PASSES = 4
_STABLE_CUT_ATTEMPTS = 3
_ACTIVE_CONTROL_STATES = frozenset(
    {
        "running",
        "awaiting_approval",
        "stop_requested",
        "stopped",
        "resume_requested",
    }
)
_DELIVERY_STATES = frozenset(
    {
        "not_configured",
        "caught_up",
        "pending",
        "in_flight",
        "blocked",
    }
)
_BLOCK_KINDS = frozenset({"runtime", "operation", "delivery"})
_ARTIFACT_KINDS = frozenset({"file", "image", "link"})
_SAFE_PUBLIC_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SAFE_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")
_CREDENTIAL_URL_KEY_FRAGMENTS = (
    "accesskey",
    "apikey",
    "auth",
    "authorization",
    "credential",
    "password",
    "privatekey",
    "secret",
    "signature",
    "token",
)
_TRANSCRIPT_FIELDS = frozenset(
    {
        "codex_reasoning_items",
        "content",
        "context",
        "display_kind",
        "display_metadata",
        "name",
        "reasoning",
        "reasoning_content",
        "reasoning_details",
        "role",
        "text",
        "timestamp",
        "tool_call_id",
        "tool_calls",
        "tool_name",
    }
)
_FORBIDDEN_PUBLIC_KEY_FRAGMENTS = (
    "apikey",
    "authorization",
    "credential",
    "delivery",
    "externalbinding",
    "fencing",
    "leaseexpires",
    "leasegeneration",
    "leaseid",
    "password",
    "privatekey",
    "provider",
    "secret",
    "token",
)
_FORBIDDEN_PUBLIC_PATH_SUFFIXES = (
    "absolutepath",
    "artifactpath",
    "cachepath",
    "canonicalpath",
    "directorypath",
    "filepath",
    "filesystempath",
    "folderpath",
    "inputpath",
    "localpath",
    "outputpath",
    "rootpath",
    "temppath",
    "workspacepath",
    "worktreepath",
)


class StrictJsonError(ValueError):
    """The supplied wire payload is not strict JSON."""


class ProjectRuntimeRpcService(Protocol):
    """Injected command authority; intentionally not a database interface."""

    def execute(
        self, request: ProjectCommandRequest
    ) -> ProjectSnapshot | ProjectCommandError: ...


class ProjectRuntimeReadRpcService(Protocol):
    """Injected canonical Desktop read authority."""

    def snapshot(
        self,
        project_id: str,
        actor: ActorContext,
    ) -> "DesktopProjectRuntimeSnapshot": ...

    def events(
        self,
        project_id: str,
        after_sequence: int,
        limit: int,
        actor: ActorContext,
    ) -> "DesktopProjectRuntimeEventPage": ...

    def acknowledge(
        self,
        project_id: str,
        binding_id: str,
        cursor: int,
        actor: ActorContext,
    ) -> "DesktopProjectRuntimeAck": ...


class ProjectRuntimeReadError(RuntimeError):
    """Expected read-boundary failure with one allowlisted safe code."""

    def __init__(self, code: str) -> None:
        if not _SAFE_CODE.fullmatch(code):
            raise ValueError("invalid project runtime read error code")
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class DesktopProjectRuntimeSnapshot:
    project_id: str
    binding_id: str
    canonical_session_id: str
    lifecycle: str
    version: int
    transcript_revision: int
    current_phase: str
    active_run: Mapping[str, object] | None
    delivery_status: Mapping[str, object]
    block: Mapping[str, object] | None
    last_sequence: int
    queue: tuple[Mapping[str, object], ...]
    pending_approval: Mapping[str, object] | None
    transcript: tuple[Mapping[str, object], ...]
    artifacts: tuple[Mapping[str, object], ...]


@dataclass(frozen=True)
class DesktopProjectRuntimeEventPage:
    project_id: str
    after_sequence: int
    last_sequence: int
    events: tuple[object, ...]


@dataclass(frozen=True)
class DesktopProjectRuntimeAck:
    project_id: str
    binding_id: str
    cursor: int


@dataclass(frozen=True)
class _RuntimeIdentity:
    version: int
    canonical_session_id: str
    lifecycle: str
    current_phase: str
    active_turn_id: str | None
    active_run_control: str | None
    active_control_version: int | None
    last_sequence: int
    transcript_pending_batch_id: str | None
    transcript_dispatch_block_key: str | None
    delivery_state: str
    delivery_error_code: str | None
    block_kind: str | None
    block_code: str | None

    @property
    def transcript_ready(self) -> bool:
        return (
            self.transcript_pending_batch_id is None
            and self.transcript_dispatch_block_key is None
        )


@dataclass(frozen=True)
class DesktopActorFactory:
    """Creates the one owner identity permitted on the desktop lane."""

    actor_id: str
    binding_id: str

    def __post_init__(self) -> None:
        if type(self.actor_id) is not str or not self.actor_id:
            raise TypeError("actor_id must be a non-empty string")
        if type(self.binding_id) is not str or not self.binding_id:
            raise TypeError("binding_id must be a non-empty string")

    def __call__(self) -> ActorContext:
        return ActorContext(
            actor_id=self.actor_id,
            surface="desktop",
            binding_id=self.binding_id,
            is_owner=True,
        )


class _InvalidRequest(ValueError):
    def __init__(self, request_id: str | None = None) -> None:
        self.request_id = request_id


def _without_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise StrictJsonError("duplicate object key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> object:
    raise StrictJsonError("non-finite JSON number")


def strict_json_loads(raw: str) -> object:
    """Decode JSON while rejecting duplicate keys and non-finite constants.

    The helper is deliberately public so each local transport can apply the
    same wire rule before it passes a payload to this façade.
    """

    if type(raw) is not str:
        raise StrictJsonError("JSON payload must be text")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_without_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except StrictJsonError:
        raise
    except (TypeError, json.JSONDecodeError, RecursionError) as exc:
        raise StrictJsonError("invalid JSON") from exc
    pending: list[tuple[object, int]] = [(value, 0)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if depth > _MAX_JSON_DEPTH or nodes > _MAX_JSON_NODES:
            raise StrictJsonError("JSON structure limit exceeded")
        if type(item) is dict:
            pending.extend((child, depth + 1) for child in item.values())
        elif type(item) is list:
            pending.extend((child, depth + 1) for child in item)
    return value


def _request_id(value: object) -> str | None:
    return value if type(value) is str and value else None


def parse_project_command_params(
    params: object,
) -> ProjectCommandRequest:
    """Validate the shared parsed-transport command object."""
    if type(params) is not dict or set(params) != _COMMAND_PARAMS:
        raise StrictJsonError("invalid project command params")
    name = params["name"]
    project_id = params["project_id"]
    payload = params["payload"]
    idempotency_key = params["idempotency_key"]
    expected_version = params["expected_version"]
    if (
        type(name) is not str
        or name not in _COMMAND_NAMES
        or type(payload) is not dict
        or (
            idempotency_key is not None
            and (
                type(idempotency_key) is not str
                or not idempotency_key
            )
        )
        or (
            expected_version is not None
            and (
                type(expected_version) is not int
                or expected_version < 0
                or expected_version > _MAX_SAFE_INTEGER
            )
        )
        or (name == "project.create" and project_id is not None)
        or (
            name != "project.create"
            and (type(project_id) is not str or not project_id)
        )
    ):
        raise StrictJsonError("invalid project command params")
    try:
        return ProjectCommandRequest(
            name=name,
            project_id=project_id,
            payload=payload,
            idempotency_key=idempotency_key,
            expected_version=expected_version,
        )
    except (TypeError, ValueError):
        raise StrictJsonError(
            "invalid project command params"
        ) from None


def _parse_request(raw: str) -> tuple[str, str, dict[str, object]]:
    try:
        value = strict_json_loads(raw)
    except StrictJsonError:
        raise _InvalidRequest() from None
    if type(value) is not dict:
        raise _InvalidRequest()

    request_id = _request_id(value.get("id"))
    if set(value) != _REQUEST_FIELDS or request_id is None:
        raise _InvalidRequest(request_id)
    if (
        value["jsonrpc"] != "2.0"
        or type(value["method"]) is not str
        or value["method"] not in {_COMMAND_METHOD, *_RUNTIME_READ_METHODS}
        or type(value["params"]) is not dict
    ):
        raise _InvalidRequest(request_id)
    return request_id, value["method"], value["params"]


def _parse_command_request(params: object) -> ProjectCommandRequest:
    try:
        return parse_project_command_params(params)
    except StrictJsonError:
        raise _InvalidRequest() from None


def _project_id_params(
    params: object,
    fields: frozenset[str],
) -> tuple[dict[str, object], str]:
    if type(params) is not dict or set(params) != fields:
        raise _InvalidRequest()
    project_id = params["project_id"]
    if type(project_id) is not str or not project_id:
        raise _InvalidRequest()
    return params, project_id


def _parse_read_params(
    method: str,
    params: object,
) -> tuple[object, ...]:
    if method == _SNAPSHOT_METHOD:
        _params, project_id = _project_id_params(
            params,
            _SNAPSHOT_PARAMS,
        )
        return (project_id,)
    if method == _EVENTS_METHOD:
        typed, project_id = _project_id_params(params, _EVENTS_PARAMS)
        after_sequence = typed["after_sequence"]
        limit = typed["limit"]
        if (
            type(after_sequence) is not int
            or after_sequence < 0
            or after_sequence > _MAX_SAFE_INTEGER
            or type(limit) is not int
            or not 1 <= limit <= _MAX_EVENT_PAGE
        ):
            raise _InvalidRequest()
        return project_id, after_sequence, limit
    if method == _ACK_METHOD:
        typed, project_id = _project_id_params(params, _ACK_PARAMS)
        binding_id = typed["binding_id"]
        cursor = typed["cursor"]
        if (
            type(binding_id) is not str
            or not binding_id
            or type(cursor) is not int
            or cursor < 0
            or cursor > _MAX_SAFE_INTEGER
        ):
            raise _InvalidRequest()
        return project_id, binding_id, cursor
    raise _InvalidRequest()


def parse_project_runtime_read_params(
    method: str,
    params: object,
) -> tuple[object, ...]:
    """Validate one parsed Desktop runtime-read parameter object."""
    if method not in _RUNTIME_READ_METHODS:
        raise StrictJsonError("invalid project runtime method")
    try:
        return _parse_read_params(method, params)
    except _InvalidRequest:
        raise StrictJsonError(
            "invalid project runtime params"
        ) from None


def _safe_text(value: object, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is str and value:
        return value
    raise TypeError("unsupported public text value")


def _safe_non_negative_int(value: object, *, nullable: bool = False) -> int | None:
    if value is None and nullable:
        return None
    if (
        type(value) is int
        and 0 <= value <= _MAX_SAFE_INTEGER
    ):
        return value
    raise TypeError("unsupported public integer value")


def _safe_public_code(
    value: object,
    *,
    nullable: bool = False,
) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is str and _SAFE_PUBLIC_CODE.fullmatch(value):
        return value
    raise TypeError("unsupported public code")


def _active_run_projection(
    turn_id: object,
    control_state: object,
    control_version: object,
) -> dict[str, object] | None:
    values = (turn_id, control_state, control_version)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise TypeError("incoherent active run")
    if control_state not in _ACTIVE_CONTROL_STATES:
        raise TypeError("unsupported active control state")
    return {
        "turn_id": _safe_text(turn_id),
        "control_state": control_state,
        "control_version": _safe_non_negative_int(
            control_version
        ),
    }


def _delivery_status_projection(
    value: object,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "state",
        "error_code",
    }:
        raise TypeError("unsupported delivery status")
    state = value["state"]
    if state not in _DELIVERY_STATES:
        raise TypeError("unsupported delivery state")
    error_code = _safe_public_code(
        value["error_code"],
        nullable=True,
    )
    if state in {"not_configured", "caught_up", "in_flight"}:
        if error_code is not None:
            raise TypeError("incoherent delivery error")
    return {"state": state, "error_code": error_code}


def _block_projection(
    value: object,
) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {
        "kind",
        "code",
    }:
        raise TypeError("unsupported runtime block")
    kind = value["kind"]
    if kind not in _BLOCK_KINDS:
        raise TypeError("unsupported runtime block kind")
    return {
        "kind": kind,
        "code": _safe_public_code(value["code"]),
    }


def _normalized_public_key(value: str) -> str:
    return "".join(
        character
        for character in value.casefold()
        if character.isalnum()
    )


def _has_unicode_control(value: str) -> bool:
    return any(
        unicodedata.category(character).startswith("C")
        for character in value
    )


def _legacy_ipv4_address(
    host: str,
) -> tuple[bool, ipaddress.IPv4Address | None]:
    """Parse the browser-compatible numeric IPv4 forms without DNS."""
    parts = host.split(".")
    numeric_candidate = bool(parts) and all(
        (
            part.isascii()
            and (
                part.isdecimal()
                or (
                    part.casefold().startswith("0x")
                    and len(part) > 2
                )
            )
        )
        for part in parts
    )
    if not numeric_candidate:
        return False, None
    if len(parts) > 4:
        return True, None
    values: list[int] = []
    for part in parts:
        lowered = part.casefold()
        if lowered.startswith("0x"):
            digits = lowered[2:]
            base = 16
        elif len(lowered) > 1 and lowered.startswith("0"):
            digits = lowered[1:]
            base = 8
        else:
            digits = lowered
            base = 10
        allowed = (
            "0123456789abcdef"
            if base == 16
            else "01234567"
            if base == 8
            else "0123456789"
        )
        if not digits or any(character not in allowed for character in digits):
            return True, None
        values.append(int(digits, base))
    if any(value > 255 for value in values[:-1]):
        return True, None
    last_bit_count = 8 * (5 - len(values))
    if values[-1] >= 1 << last_bit_count:
        return True, None
    address_value = values[-1]
    for index, value in enumerate(values[:-1]):
        address_value |= value << (8 * (3 - index))
    return True, ipaddress.IPv4Address(address_value)


def _non_external_host(host: str) -> bool:
    canonical = _canonical_url_host(host)
    if canonical is None:
        return True
    normalized = canonical.casefold().rstrip(".")
    if (
        not normalized
        or normalized == "localhost"
        or normalized.endswith(".localhost")
        or normalized == "local"
        or normalized.endswith(".local")
        or normalized == "home.arpa"
        or normalized.endswith(".home.arpa")
    ):
        return True
    try:
        address: ipaddress.IPv4Address | ipaddress.IPv6Address | None = (
            ipaddress.ip_address(normalized)
        )
    except ValueError:
        numeric_candidate, address = _legacy_ipv4_address(normalized)
        if numeric_candidate and address is None:
            return True
    if address is None:
        return False
    if isinstance(address, ipaddress.IPv6Address):
        address = address.ipv4_mapped or address
    return (
        not address.is_global
        or address.is_multicast
        or (
            isinstance(address, ipaddress.IPv6Address)
            and address.is_site_local
        )
    )


def _canonical_url_parameters(value: str) -> str | None:
    if (
        len(value) > _MAX_URL_PARAMETER_CHARS
        or _has_unicode_control(value)
    ):
        return None
    current = value
    for _pass in range(_MAX_PERCENT_DECODE_PASSES):
        if "%" not in current:
            return current
        index = 0
        while True:
            percent = current.find("%", index)
            if percent < 0:
                break
            escape = current[percent + 1 : percent + 3]
            if (
                len(escape) != 2
                or any(character not in _HEX_DIGITS for character in escape)
            ):
                return None
            index = percent + 3
        try:
            decoded = unquote(
                current,
                encoding="utf-8",
                errors="strict",
            )
        except (UnicodeDecodeError, ValueError):
            return None
        if (
            len(decoded) > _MAX_URL_PARAMETER_CHARS
            or _has_unicode_control(decoded)
        ):
            return None
        current = decoded
    if "%" in current:
        return None
    return current


def _canonical_url_host(value: str) -> str | None:
    decoded = _canonical_url_parameters(value)
    if decoded is None or "\\" in decoded:
        return None
    try:
        canonical = decoded.encode("idna").decode("ascii")
    except UnicodeError:
        return None
    if (
        not canonical
        or len(canonical.rstrip(".")) > 253
        or _has_unicode_control(canonical)
    ):
        return None
    return canonical


def _forbidden_public_key(value: str) -> bool:
    normalized = _normalized_public_key(value)
    return (
        normalized == "path"
        or any(
            normalized.endswith(suffix)
            for suffix in _FORBIDDEN_PUBLIC_PATH_SUFFIXES
        )
        or any(
            fragment in normalized
            for fragment in _FORBIDDEN_PUBLIC_KEY_FRAGMENTS
        )
    )


def _public_json_projection(
    value: object,
    *,
    depth: int = 0,
) -> object:
    if depth > _MAX_JSON_DEPTH:
        raise TypeError("unsupported public JSON depth")
    if value is None or type(value) in {bool, str}:
        return value
    if type(value) is int:
        if -_MAX_SAFE_INTEGER <= value <= _MAX_SAFE_INTEGER:
            return value
        raise TypeError("unsupported public JSON integer")
    if type(value) is float:
        if not math.isfinite(value):
            raise TypeError("unsupported non-finite public number")
        if (
            value.is_integer()
            and abs(value) > _MAX_SAFE_INTEGER
        ):
            raise TypeError("unsupported public JSON integer")
        return value
    if isinstance(value, (list, tuple)):
        return [
            _public_json_projection(
                item,
                depth=depth + 1,
            )
            for item in value
        ]
    if isinstance(value, Mapping):
        copied: dict[str, object] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("unsupported public JSON key")
            if _forbidden_public_key(key):
                continue
            copied[key] = _public_json_projection(
                item,
                depth=depth + 1,
            )
        return copied
    raise TypeError("unsupported public JSON value")


def _sanitize_transcript(
    messages: object,
) -> tuple[Mapping[str, object], ...]:
    if not isinstance(messages, (list, tuple)):
        raise TypeError("unsupported transcript projection")
    projected: list[Mapping[str, object]] = []
    for message in messages:
        if not isinstance(message, Mapping):
            raise TypeError("unsupported transcript message")
        role = message.get("role")
        if role not in {"assistant", "system", "tool", "user"}:
            raise TypeError("unsupported transcript role")
        if "content" not in message:
            raise TypeError("transcript message has no content")
        item: dict[str, object] = {}
        for key in _TRANSCRIPT_FIELDS:
            if key in message:
                item[key] = _public_json_projection(
                    message[key]
                )
        if item.get("role") != role or "content" not in item:
            raise TypeError("invalid transcript projection")
        projected.append(item)
    return tuple(projected)


def _runtime_identity(
    state: object,
    *,
    view: object,
    delivery_status: object,
    block: object,
) -> _RuntimeIdentity:
    if not isinstance(state, runtime_db.RuntimeState):
        raise TypeError("runtime state disappeared")
    if (
        state.version != getattr(view, "version", None)
        or state.conversation_tip_id
        != getattr(view, "canonical_session_id", None)
        or state.lifecycle != getattr(view, "lifecycle", None)
        or state.current_phase
        != getattr(view, "current_phase", None)
    ):
        raise TypeError("runtime identity projection changed")
    active_run = _active_run_projection(
        getattr(view, "active_turn_id", None),
        getattr(view, "active_run_control", None),
        getattr(view, "active_control_version", None),
    )
    delivery = _delivery_status_projection(delivery_status)
    projected_block = _block_projection(block)
    return _RuntimeIdentity(
        version=_safe_non_negative_int(view.version),
        canonical_session_id=_safe_text(
            view.canonical_session_id
        ),
        lifecycle=_safe_text(view.lifecycle),
        current_phase=_safe_text(view.current_phase),
        active_turn_id=(
            active_run["turn_id"]
            if active_run is not None
            else None
        ),
        active_run_control=(
            active_run["control_state"]
            if active_run is not None
            else None
        ),
        active_control_version=(
            active_run["control_version"]
            if active_run is not None
            else None
        ),
        last_sequence=_safe_non_negative_int(
            view.last_event_sequence
        ),
        transcript_pending_batch_id=state.transcript_pending_batch_id,
        transcript_dispatch_block_key=state.transcript_dispatch_block_key,
        delivery_state=delivery["state"],
        delivery_error_code=delivery["error_code"],
        block_kind=(
            projected_block["kind"]
            if projected_block is not None
            else None
        ),
        block_code=(
            projected_block["code"]
            if projected_block is not None
            else None
        ),
    )


def _credential_free_external_url(value: object) -> str | None:
    if (
        type(value) is not str
        or not value
        or any(character.isspace() for character in value)
        or _has_unicode_control(value)
    ):
        return None
    try:
        parsed = urlsplit(value)
        _port = parsed.port
    except (TypeError, ValueError):
        return None
    if not (
        parsed.scheme in {"http", "https"}
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
    ):
        return None
    if _non_external_host(parsed.hostname):
        return None
    decoded_query = _canonical_url_parameters(parsed.query)
    decoded_fragment = _canonical_url_parameters(parsed.fragment)
    if decoded_query is None or decoded_fragment is None:
        return None
    parameter_groups = [decoded_query]
    if "=" in decoded_fragment:
        parameter_groups.append(decoded_fragment)
    for encoded_parameters in parameter_groups:
        try:
            parameters = parse_qsl(
                encoded_parameters,
                keep_blank_values=True,
                strict_parsing=False,
            )
        except ValueError:
            return None
        for key, _item in parameters:
            decoded_key = _canonical_url_parameters(key)
            if decoded_key is None:
                return None
            normalized = _normalized_public_key(decoded_key)
            if any(
                fragment in normalized
                for fragment in _CREDENTIAL_URL_KEY_FRAGMENTS
            ):
                return None
    return value


def _artifact_presentation(
    *,
    path: object,
    metadata: object,
    created_at: object,
) -> dict[str, object]:
    if type(path) is not str or not path:
        raise TypeError("unsupported artifact path")
    if not isinstance(metadata, Mapping):
        raise TypeError("unsupported artifact metadata")
    label = ntpath.basename(path.rstrip("/\\"))
    if not label or label in {".", ".."}:
        raise TypeError("unsupported artifact label")
    requested_kind = metadata.get("kind")
    kind = (
        requested_kind
        if requested_kind in _ARTIFACT_KINDS
        else "file"
    )
    size = metadata.get("size")
    if size is not None:
        size = _safe_non_negative_int(size)
    digest = metadata.get("sha256")
    if digest is not None and not (
        type(digest) is str
        and _SAFE_SHA256.fullmatch(digest)
    ):
        raise TypeError("unsupported artifact digest")
    target_url = (
        _credential_free_external_url(metadata.get("url"))
        if kind == "link"
        else None
    )
    return {
        "kind": kind,
        "label": label,
        "created_at": _safe_non_negative_int(created_at),
        "size_bytes": size,
        "sha256": digest,
        "open_target": (
            {
                "kind": "external_url",
                "href": target_url,
            }
            if target_url is not None
            else None
        ),
    }


def _serialize_runtime_artifact(
    artifact: object,
) -> dict[str, object]:
    if not isinstance(artifact, Mapping):
        raise TypeError("unsupported runtime artifact")
    if set(artifact) != {"artifact_id", "presentation"}:
        raise TypeError("unsupported runtime artifact fields")
    presentation = artifact["presentation"]
    if not isinstance(presentation, Mapping) or set(
        presentation
    ) != {
        "kind",
        "label",
        "created_at",
        "size_bytes",
        "sha256",
        "open_target",
    }:
        raise TypeError("unsupported artifact presentation")
    kind = presentation["kind"]
    if kind not in _ARTIFACT_KINDS:
        raise TypeError("unsupported artifact kind")
    sha256 = presentation["sha256"]
    if not (
        sha256 is None
        or (
            type(sha256) is str
            and _SAFE_SHA256.fullmatch(sha256)
        )
    ):
        raise TypeError("unsupported artifact digest")
    target = presentation["open_target"]
    if target is not None:
        if not (
            kind == "link"
            and isinstance(target, Mapping)
            and set(target) == {"kind", "href"}
            and target["kind"] == "external_url"
            and _credential_free_external_url(target["href"])
            == target["href"]
        ):
            raise TypeError("unsupported artifact open target")
    return {
        "artifact_id": _safe_text(artifact["artifact_id"]),
        "presentation": {
            "kind": kind,
            "label": _safe_text(presentation["label"]),
            "created_at": _safe_non_negative_int(
                presentation["created_at"]
            ),
            "size_bytes": _safe_non_negative_int(
                presentation["size_bytes"],
                nullable=True,
            ),
            "sha256": sha256,
            "open_target": (
                None
                if target is None
                else {
                    "kind": "external_url",
                    "href": target["href"],
                }
            ),
        },
    }


def _serialize_runtime_snapshot(
    snapshot: object,
) -> dict[str, object]:
    if type(snapshot) is not DesktopProjectRuntimeSnapshot:
        raise TypeError("unsupported runtime snapshot")
    if snapshot.lifecycle not in {
        "active",
        "awaiting_acceptance",
        "completed",
    }:
        raise TypeError("unsupported runtime lifecycle")
    queue: list[dict[str, object]] = []
    for turn in snapshot.queue:
        if not isinstance(turn, Mapping) or set(turn) != {
            "turn_id",
            "sequence",
            "status",
        }:
            raise TypeError("unsupported queue projection")
        sequence = _safe_non_negative_int(turn["sequence"])
        if sequence == 0:
            raise TypeError("unsupported queue sequence")
        queue.append(
            {
                "turn_id": _safe_text(turn["turn_id"]),
                "sequence": sequence,
                "status": _safe_text(turn["status"]),
            }
        )
    approval = snapshot.pending_approval
    if approval is not None:
        if not isinstance(approval, Mapping) or set(approval) != {
            "approval_id",
            "kind",
        }:
            raise TypeError("unsupported approval projection")
        serialized_approval: dict[str, object] | None = {
            "approval_id": _safe_text(approval["approval_id"]),
            "kind": _safe_text(approval["kind"]),
        }
    else:
        serialized_approval = None
    transcript = [
        dict(message)
        for message in _sanitize_transcript(snapshot.transcript)
    ]
    active_run = (
        None
        if snapshot.active_run is None
        else _active_run_projection(
            snapshot.active_run.get("turn_id")
            if isinstance(snapshot.active_run, Mapping)
            else None,
            snapshot.active_run.get("control_state")
            if isinstance(snapshot.active_run, Mapping)
            else None,
            snapshot.active_run.get("control_version")
            if isinstance(snapshot.active_run, Mapping)
            else None,
        )
    )
    if (
        snapshot.active_run is not None
        and (
            not isinstance(snapshot.active_run, Mapping)
            or set(snapshot.active_run)
            != {
                "turn_id",
                "control_state",
                "control_version",
            }
        )
    ):
        raise TypeError("unsupported active run projection")
    return {
        "project_id": _safe_text(snapshot.project_id),
        "binding_id": _safe_text(snapshot.binding_id),
        "canonical_session_id": _safe_text(
            snapshot.canonical_session_id
        ),
        "lifecycle": snapshot.lifecycle,
        "version": _safe_non_negative_int(snapshot.version),
        "transcript_revision": _safe_non_negative_int(
            snapshot.transcript_revision
        ),
        "current_phase": _safe_text(snapshot.current_phase),
        "active_run": active_run,
        "delivery_status": _delivery_status_projection(
            snapshot.delivery_status
        ),
        "block": _block_projection(snapshot.block),
        "last_sequence": _safe_non_negative_int(
            snapshot.last_sequence
        ),
        "queue": queue,
        "pending_approval": serialized_approval,
        "transcript": transcript,
        "artifacts": [
            _serialize_runtime_artifact(item)
            for item in snapshot.artifacts
        ],
    }


def _serialize_runtime_event(event: object) -> dict[str, object]:
    required = (
        "event_id",
        "project_id",
        "sequence",
        "kind",
        "turn_id",
        "payload",
        "created_at",
    )
    if any(not hasattr(event, field) for field in required):
        raise TypeError("unsupported runtime event")
    sequence = _safe_non_negative_int(event.sequence)
    if sequence == 0:
        raise TypeError("unsupported runtime event sequence")
    payload = _public_json_projection(event.payload)
    if not isinstance(payload, dict):
        raise TypeError("unsupported runtime event payload")
    return {
        "event_id": _safe_text(event.event_id),
        "project_id": _safe_text(event.project_id),
        "sequence": sequence,
        "kind": _safe_text(event.kind),
        "turn_id": _safe_text(event.turn_id, nullable=True),
        "payload": payload,
        "created_at": _safe_text(event.created_at),
    }


def _serialize_runtime_event_page(
    page: object,
) -> dict[str, object]:
    if type(page) is not DesktopProjectRuntimeEventPage:
        raise TypeError("unsupported runtime event page")
    return {
        "project_id": _safe_text(page.project_id),
        "after_sequence": _safe_non_negative_int(
            page.after_sequence
        ),
        "last_sequence": _safe_non_negative_int(
            page.last_sequence
        ),
        "events": [
            _serialize_runtime_event(event)
            for event in page.events
        ],
    }


def _serialize_runtime_ack(ack: object) -> dict[str, object]:
    if type(ack) is not DesktopProjectRuntimeAck:
        raise TypeError("unsupported runtime acknowledgement")
    return {
        "project_id": _safe_text(ack.project_id),
        "binding_id": _safe_text(ack.binding_id),
        "cursor": _safe_non_negative_int(ack.cursor),
    }


def _serialize_artifact(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("unsupported artifact projection")
    if value.get("status") != "verified":
        raise TypeError("unsupported artifact status")
    return {
        "artifact_id": _safe_text(value.get("artifact_id")),
        "presentation": _artifact_presentation(
            path=value.get("path"),
            metadata=value.get("metadata"),
            created_at=value.get("created_at"),
        ),
    }


def _serialize_snapshot(snapshot: object) -> dict[str, object]:
    if type(snapshot) is not ProjectSnapshot:
        raise TypeError("service returned an unsupported snapshot type")
    active_run = _active_run_projection(
        snapshot.active_turn_id,
        snapshot.active_run_control,
        snapshot.active_control_version,
    )
    return {
        "accepted_turn_id": _safe_text(
            snapshot.accepted_turn_id,
            nullable=True,
        ),
        "active_control_version": _safe_non_negative_int(
            snapshot.active_control_version,
            nullable=True,
        ),
        "project_id": _safe_text(snapshot.project_id),
        "lifecycle": _safe_text(snapshot.lifecycle),
        "version": _safe_non_negative_int(snapshot.version),
        "canonical_session_id": _safe_text(
            snapshot.canonical_session_id, nullable=True
        ),
        "queue_depth": _safe_non_negative_int(snapshot.queue_depth),
        "active_turn_id": (
            active_run["turn_id"]
            if active_run is not None
            else None
        ),
        "active_run_control": (
            active_run["control_state"]
            if active_run is not None
            else None
        ),
        "pending_approval_id": _safe_text(
            snapshot.pending_approval_id, nullable=True
        ),
        "last_event_sequence": _safe_non_negative_int(
            snapshot.last_event_sequence
        ),
        "current_phase": _safe_text(snapshot.current_phase, nullable=True),
        "artifact": _serialize_artifact(snapshot.artifact),
    }


def _serialize_command_error(error: object) -> dict[str, object]:
    if type(error) is not ProjectCommandError or not _SAFE_CODE.fullmatch(error.code):
        raise TypeError("service returned an unsupported command error")
    payload: dict[str, object] = {"code": error.code}
    if error.project_id is not None:
        payload["project_id"] = _safe_text(error.project_id)
    if error.current_version is not None:
        payload["current_version"] = _safe_non_negative_int(error.current_version)
    if error.current_control_version is not None:
        payload["current_control_version"] = (
            _safe_non_negative_int(
                error.current_control_version
            )
        )
    # Intentionally omit `.message`: it is useful for trusted logs but is not a
    # wire contract and could include details added by a future adapter.
    return payload


def _encode_response(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


class ProjectRuntimeReadService:
    """Canonical Desktop snapshot/replay/ack service.

    Project runtime data and transcript messages live in separate databases.
    A snapshot therefore uses a bounded double-read of the runtime identity
    around the already-canonical transcript loader.  This is a stable-cut
    protocol, not a claim of cross-database transaction atomicity.
    """

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        runtime: ProjectRuntime,
        transcript_loader: Callable[
            [str],
            tuple[tuple[Mapping[str, object], ...], int],
        ],
        clock: Callable[[], int],
        stable_cut_attempts: int = _STABLE_CUT_ATTEMPTS,
    ) -> None:
        if not isinstance(conn, sqlite3.Connection):
            raise TypeError("conn must be a sqlite3.Connection")
        if not isinstance(runtime, ProjectRuntime):
            raise TypeError("runtime must be a ProjectRuntime")
        if not callable(transcript_loader):
            raise TypeError("transcript_loader must be callable")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if (
            type(stable_cut_attempts) is not int
            or stable_cut_attempts <= 0
        ):
            raise TypeError("stable_cut_attempts must be positive")
        self._conn = conn
        self._runtime = runtime
        self._transcript_loader = transcript_loader
        self._clock = clock
        self._stable_cut_attempts = stable_cut_attempts

    def _now(self) -> int:
        now = self._clock()
        if type(now) is not int or now < 0:
            raise RuntimeError("runtime read clock returned invalid time")
        return now

    def _require_idle_connection(self) -> None:
        if self._conn.in_transaction:
            raise RuntimeError(
                "runtime stable cut requires an idle connection"
            )

    def _authorize_unique_desktop_owner(
        self,
        project_id: str,
        actor: ActorContext,
    ) -> None:
        matches = tuple(
            binding
            for binding in runtime_db.bindings_for_project(
                self._conn,
                project_id=project_id,
            )
            if (
                binding.surface == "desktop"
                and binding.actor_id == actor.actor_id
            )
        )
        if (
            actor.surface != "desktop"
            or actor.is_owner is not True
            or len(matches) != 1
            or matches[0].binding_id != actor.binding_id
        ):
            raise ProjectRuntimeReadError(
                "PROJECT_RUNTIME_REJECTED"
            )

    def _delivery_status(
        self,
        project_id: str,
    ) -> Mapping[str, object]:
        binding_count = self._conn.execute(
            """
            SELECT COUNT(*)
            FROM project_surface_bindings
            WHERE project_id = ? AND surface = 'discord'
            """,
            (project_id,),
        ).fetchone()[0]
        if binding_count == 0:
            return {
                "state": "not_configured",
                "error_code": None,
            }
        rows = self._conn.execute(
            """
            SELECT delivery.status, delivery.last_error_code
            FROM project_deliveries AS delivery
            JOIN project_surface_bindings AS binding
              ON binding.project_id = delivery.project_id
             AND binding.binding_id = delivery.binding_id
            JOIN project_events AS event
              ON event.project_id = delivery.project_id
             AND event.event_id = delivery.event_id
            WHERE delivery.project_id = ?
              AND binding.surface = 'discord'
            ORDER BY event.sequence, binding.created_at,
                     binding.binding_id, delivery.delivery_id
            """,
            (project_id,),
        ).fetchall()
        if not rows:
            return {"state": "caught_up", "error_code": None}
        allowed = {
            "pending",
            "in_flight",
            "delivered",
            "suppressed",
            "blocked",
        }
        if any(row["status"] not in allowed for row in rows):
            raise TypeError("unsupported delivery state")
        state = next(
            (
                candidate
                for candidate in (
                    "blocked",
                    "in_flight",
                    "pending",
                )
                if any(
                    row["status"] == candidate
                    for row in rows
                )
            ),
            "caught_up",
        )
        error_code = None
        if state in {"blocked", "pending"}:
            error_code = next(
                (
                    row["last_error_code"]
                    for row in rows
                    if (
                        row["status"] == state
                        and row["last_error_code"] is not None
                    )
                ),
                None,
            )
            if error_code is not None:
                error_code = _safe_public_code(error_code)
        return {"state": state, "error_code": error_code}

    def _runtime_block(
        self,
        project_id: str,
        *,
        delivery_status: Mapping[str, object],
    ) -> Mapping[str, object] | None:
        state = runtime_db.runtime_state_for_project(
            self._conn,
            project_id,
        )
        if (
            state is not None
            and state.transcript_dispatch_block_key is not None
        ):
            return {
                "kind": "runtime",
                "code": "transcript_dispatch_blocked",
            }
        recovery = self._conn.execute(
            """
            SELECT 1 FROM project_turns
            WHERE project_id = ? AND recovery_block_key IS NOT NULL
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if recovery is not None:
            return {
                "kind": "runtime",
                "code": "turn_recovery_blocked",
            }
        operation = self._conn.execute(
            """
            SELECT blocked_reason
            FROM project_operations
            WHERE project_id = ? AND status = 'blocked'
            ORDER BY updated_at DESC, operation_id
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if operation is not None:
            return {
                "kind": "operation",
                "code": _safe_public_code(
                    operation["blocked_reason"]
                ),
            }
        if delivery_status["state"] == "blocked":
            return {
                "kind": "delivery",
                "code": _safe_public_code(
                    delivery_status["error_code"]
                ),
            }
        return None

    def _identity_in_transaction(
        self,
        project_id: str,
        actor: ActorContext,
    ) -> tuple[object, _RuntimeIdentity]:
        view = self._runtime.snapshot_for_actor(project_id, actor)
        self._authorize_unique_desktop_owner(project_id, actor)
        state = runtime_db.runtime_state_for_project(
            self._conn,
            project_id,
        )
        delivery_status = self._delivery_status(project_id)
        block = self._runtime_block(
            project_id,
            delivery_status=delivery_status,
        )
        identity = _runtime_identity(
            state,
            view=view,
            delivery_status=delivery_status,
            block=block,
        )
        return view, identity

    def _read_identity(
        self,
        project_id: str,
        actor: ActorContext,
    ) -> _RuntimeIdentity:
        self._require_idle_connection()
        self._conn.execute("BEGIN")
        try:
            _view, identity = self._identity_in_transaction(
                project_id,
                actor,
            )
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")
        return identity

    def _read_snapshot_projection(
        self,
        project_id: str,
        actor: ActorContext,
    ) -> tuple[DesktopProjectRuntimeSnapshot, _RuntimeIdentity]:
        self._require_idle_connection()
        self._conn.execute("BEGIN")
        try:
            view, identity = self._identity_in_transaction(
                project_id,
                actor,
            )
            turns = self._runtime.list_queue(project_id, actor)
            approval_row = self._conn.execute(
                """
                SELECT approval_id, approval_class
                FROM project_approvals
                WHERE project_id = ? AND status = 'pending'
                ORDER BY created_at, approval_id
                LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            artifact_rows = self._conn.execute(
                """
                SELECT artifact_id, path, metadata_json,
                       status, created_at
                FROM project_artifacts
                WHERE project_id = ? AND status = 'verified'
                ORDER BY created_at, artifact_id
                """,
                (project_id,),
            ).fetchall()
            artifacts: list[Mapping[str, object]] = []
            for row in artifact_rows:
                metadata = strict_json_loads(row["metadata_json"])
                if not isinstance(metadata, dict):
                    raise TypeError(
                        "stored artifact metadata is not an object"
                    )
                artifacts.append(
                    {
                        "artifact_id": row["artifact_id"],
                        "presentation": _artifact_presentation(
                            path=row["path"],
                            metadata=metadata,
                            created_at=row["created_at"],
                        ),
                    }
                )
            active_run = _active_run_projection(
                view.active_turn_id,
                view.active_run_control,
                view.active_control_version,
            )
            delivery_status = {
                "state": identity.delivery_state,
                "error_code": identity.delivery_error_code,
            }
            block = (
                {
                    "kind": identity.block_kind,
                    "code": identity.block_code,
                }
                if identity.block_kind is not None
                else None
            )
            snapshot = DesktopProjectRuntimeSnapshot(
                project_id=view.project_id,
                binding_id=actor.binding_id,
                canonical_session_id=view.canonical_session_id,
                lifecycle=view.lifecycle,
                version=view.version,
                transcript_revision=0,
                current_phase=view.current_phase,
                active_run=active_run,
                delivery_status=delivery_status,
                block=block,
                last_sequence=view.last_event_sequence,
                queue=tuple(
                    {
                        "turn_id": turn.turn_id,
                        "sequence": turn.sequence,
                        "status": turn.status,
                    }
                    for turn in turns
                ),
                pending_approval=(
                    {
                        "approval_id": approval_row["approval_id"],
                        "kind": approval_row["approval_class"],
                    }
                    if approval_row is not None
                    else None
                ),
                transcript=(),
                artifacts=tuple(artifacts),
            )
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")
        return snapshot, identity

    def _load_transcript_snapshot(
        self,
        canonical_session_id: str,
    ) -> tuple[tuple[Mapping[str, object], ...], int]:
        loaded = self._transcript_loader(canonical_session_id)
        if (
            type(loaded) is not tuple
            or len(loaded) != 2
            or type(loaded[1]) is not int
            or loaded[1] < 0
        ):
            raise TypeError(
                "transcript loader returned an invalid snapshot"
            )
        return _sanitize_transcript(loaded[0]), loaded[1]

    def snapshot(
        self,
        project_id: str,
        actor: ActorContext,
    ) -> DesktopProjectRuntimeSnapshot:
        for _attempt in range(self._stable_cut_attempts):
            projection, before = self._read_snapshot_projection(
                project_id,
                actor,
            )
            if not before.transcript_ready:
                continue
            transcript, transcript_revision = (
                self._load_transcript_snapshot(
                    before.canonical_session_id
                )
            )
            middle = self._read_identity(project_id, actor)
            if not (
                before == middle
                and middle.transcript_ready
            ):
                continue
            confirmed_transcript, confirmed_revision = (
                self._load_transcript_snapshot(
                    middle.canonical_session_id
                )
            )
            after = self._read_identity(project_id, actor)
            if (
                before == middle == after
                and after.transcript_ready
                and transcript_revision == confirmed_revision
                and transcript == confirmed_transcript
            ):
                return DesktopProjectRuntimeSnapshot(
                    project_id=projection.project_id,
                    binding_id=projection.binding_id,
                    canonical_session_id=(
                        projection.canonical_session_id
                    ),
                    lifecycle=projection.lifecycle,
                    version=projection.version,
                    transcript_revision=transcript_revision,
                    current_phase=projection.current_phase,
                    active_run=projection.active_run,
                    delivery_status=projection.delivery_status,
                    block=projection.block,
                    last_sequence=projection.last_sequence,
                    queue=projection.queue,
                    pending_approval=projection.pending_approval,
                    transcript=transcript,
                    artifacts=projection.artifacts,
                )
        raise ProjectRuntimeReadError("PROJECT_RUNTIME_TRANSIENT")

    def events(
        self,
        project_id: str,
        after_sequence: int,
        limit: int,
        actor: ActorContext,
    ) -> DesktopProjectRuntimeEventPage:
        self._require_idle_connection()
        self._conn.execute("BEGIN")
        try:
            view, _identity = self._identity_in_transaction(
                project_id,
                actor,
            )
            if after_sequence > view.last_event_sequence:
                raise ProjectRuntimeReadError(
                    "PROJECT_RUNTIME_REJECTED"
                )
            events = tuple(
                self._runtime.events_after(
                    project_id,
                    after_sequence,
                    limit,
                )
            )
            page = DesktopProjectRuntimeEventPage(
                project_id=project_id,
                after_sequence=after_sequence,
                last_sequence=view.last_event_sequence,
                events=events,
            )
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")
        return page

    def acknowledge(
        self,
        project_id: str,
        binding_id: str,
        cursor: int,
        actor: ActorContext,
    ) -> DesktopProjectRuntimeAck:
        try:
            effective = runtime_db.acknowledge_desktop_read_cursor(
                self._conn,
                project_id=project_id,
                binding_id=binding_id,
                cursor=cursor,
                actor=actor,
                now=self._now(),
            )
        except runtime_db.DesktopReadCursorUnavailableError:
            raise ProjectRuntimeReadError(
                "PROJECT_RUNTIME_TRANSIENT"
            ) from None
        except (PermissionError, ValueError):
            raise ProjectRuntimeReadError(
                "PROJECT_RUNTIME_REJECTED"
            ) from None
        return DesktopProjectRuntimeAck(
            project_id=project_id,
            binding_id=binding_id,
            cursor=effective,
        )


class ProjectRuntimeRpc:
    """Allowlisted raw-JSON dispatcher for canonical project access."""

    def __init__(
        self,
        *,
        service: ProjectRuntimeRpcService,
        read_service: ProjectRuntimeReadRpcService | None = None,
        actor_factory: Callable[[], ActorContext],
    ) -> None:
        if not callable(getattr(service, "execute", None)):
            raise TypeError("service must provide execute(request)")
        if read_service is not None and any(
            not callable(getattr(read_service, name, None))
            for name in ("snapshot", "events", "acknowledge")
        ):
            raise TypeError(
                "read_service must provide snapshot/events/acknowledge"
            )
        if not callable(actor_factory):
            raise TypeError("actor_factory must be callable")
        self._service = service
        self._read_service = read_service
        self._actor_factory = actor_factory

    def handle_raw(self, raw: str) -> str:
        """Return a safe JSON response for one local RPC payload."""

        request_id: str | None = None
        try:
            request_id, method, params = _parse_request(raw)
            parsed = (
                _parse_command_request(params)
                if method == _COMMAND_METHOD
                else _parse_read_params(method, params)
            )
        except _InvalidRequest as exc:
            return _encode_response(
                {
                    "id": (
                        exc.request_id
                        if exc.request_id is not None
                        else request_id
                    ),
                    "ok": False,
                    "error": {"code": "invalid_request"},
                }
            )

        try:
            actor = self._actor_factory()
            if (
                type(actor) is not ActorContext
                or actor.surface != "desktop"
                or actor.is_owner is not True
                or type(actor.actor_id) is not str
                or not actor.actor_id
                or type(actor.binding_id) is not str
                or not actor.binding_id
            ):
                raise TypeError("desktop actor factory returned an invalid actor")
            if method != _COMMAND_METHOD:
                if self._read_service is None:
                    raise TypeError("runtime read service is unavailable")
                if method == _SNAPSHOT_METHOD:
                    result = self._read_service.snapshot(
                        parsed[0],
                        actor,
                    )
                    serialized = _serialize_runtime_snapshot(result)
                elif method == _EVENTS_METHOD:
                    result = self._read_service.events(
                        parsed[0],
                        parsed[1],
                        parsed[2],
                        actor,
                    )
                    serialized = _serialize_runtime_event_page(result)
                elif method == _ACK_METHOD:
                    result = self._read_service.acknowledge(
                        parsed[0],
                        parsed[1],
                        parsed[2],
                        actor,
                    )
                    serialized = _serialize_runtime_ack(result)
                else:
                    raise TypeError("unsupported runtime read method")
                return _encode_response(
                    {
                        "id": request_id,
                        "ok": True,
                        "result": serialized,
                    }
                )
            request = parsed
            if type(request) is not ProjectCommandRequest:
                raise TypeError("unsupported project command")
            request = ProjectCommandRequest(
                name=request.name,
                project_id=request.project_id,
                payload=request.payload,
                actor=actor,
                idempotency_key=request.idempotency_key,
                expected_version=request.expected_version,
            )
            result = self._service.execute(request)
            if type(result) is ProjectSnapshot:
                return _encode_response(
                    {
                        "id": request_id,
                        "ok": True,
                        "result": _serialize_snapshot(result),
                    }
                )
            return _encode_response(
                {
                    "id": request_id,
                    "ok": False,
                    "error": _serialize_command_error(result),
                }
            )
        except ProjectRuntimeReadError as exc:
            return _encode_response(
                {
                    "id": request_id,
                    "ok": False,
                    "error": {"code": exc.code},
                }
            )
        except ProjectRuntimeError:
            return _encode_response(
                {
                    "id": request_id,
                    "ok": False,
                    "error": {"code": "PROJECT_RUNTIME_REJECTED"},
                }
            )
        except Exception:
            # Error details (including service exceptions and their arguments)
            # intentionally stay server-side and never become JSON output.
            return _encode_response(
                {"id": request_id, "ok": False, "error": {"code": "internal_error"}}
            )
