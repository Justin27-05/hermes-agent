"""Pure, fail-closed authorization decisions for ProjectRuntime commands."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from string import ascii_letters, ascii_lowercase, ascii_uppercase
from types import MappingProxyType
from typing import Literal, Mapping


class Decision(str, Enum):
    ALLOW = "allow"
    REQUIRE_APPROVAL = "require_approval"
    DENY = "deny"


@dataclass(frozen=True)
class PolicyDecision:
    decision: Decision
    rule_id: str
    reason: str
    approval_class: str | None = None


_METADATA_SCALAR_TYPES = (str, int, bool, type(None))
_ASCII_UPPER_TRANSLATION = str.maketrans(ascii_lowercase, ascii_uppercase)
_WINDOWS_FORBIDDEN_COMPONENT_CHARACTERS = frozenset('<>:"|?*\\')
_WINDOWS_RESERVED_DEVICE_BASENAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "CLOCK$",
        "CONIN$",
        "CONOUT$",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
        "COM¹",
        "COM²",
        "COM³",
        "LPT¹",
        "LPT²",
        "LPT³",
    }
)


def _windows_component_is_valid(component: str) -> bool:
    if component.startswith(" ") or component.endswith((" ", ".")):
        return False
    if any(
        ord(character) <= 0x1F
        or character in _WINDOWS_FORBIDDEN_COMPONENT_CHARACTERS
        for character in component
    ):
        return False
    basename = component.partition(".")[0].rstrip(" ")
    basename = basename.translate(_ASCII_UPPER_TRANSLATION)
    return basename not in _WINDOWS_RESERVED_DEVICE_BASENAMES


@dataclass(frozen=True)
class ProjectCommand:
    name: str
    project_id: str
    revision: int
    action_class: str
    targets: tuple[str, ...]
    batch_id: str | None
    batch_items: tuple[str, ...]
    metadata: Mapping[str, object]

    def __post_init__(self) -> None:
        if not isinstance(self.metadata, Mapping):
            raise TypeError("metadata must be a mapping")
        copied: dict[str, object] = {}
        for key, value in self.metadata.items():
            if type(key) is not str or type(value) not in _METADATA_SCALAR_TYPES:
                raise TypeError("metadata must contain string keys and scalar values")
            copied[key] = value
        object.__setattr__(self, "metadata", MappingProxyType(copied))


@dataclass(frozen=True)
class ProjectBindingView:
    """A durably registered owner binding scoped to one canonical project."""

    binding_id: str
    surface: Literal["desktop", "discord"]
    owner_actor_id: str
    project_id: str


@dataclass(frozen=True)
class ProjectPolicyView:
    project_id: str
    lifecycle: str
    current_phase: str
    roots: tuple[str, ...]
    approved_plan_ref: str | None
    delivery_bindings: tuple[ProjectBindingView, ...] = ()
    canonical_event_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class ContractPolicyView:
    revision: int
    allowed_action_classes: frozenset[str]
    allowed_phases: frozenset[str]
    approved_plan_ref: str | None


@dataclass(frozen=True)
class ActorContext:
    actor_id: str
    surface: Literal["desktop", "discord", "system"]
    binding_id: str
    is_owner: bool


@dataclass(frozen=True)
class CanonicalPath:
    """One validated forward-slash absolute path and its comparison identity."""

    value: str
    flavor: Literal["posix", "windows"]
    anchor: str
    components: tuple[str, ...]


def parse_canonical_path(value: object) -> CanonicalPath | None:
    """Parse a forward-slash POSIX or drive path without touching the filesystem."""
    if type(value) is not str or not value or "\\" in value or "\x00" in value:
        return None
    if value.startswith("//"):  # UNC is deliberately unsupported in Task 2.
        return None

    if value.startswith("/"):
        flavor: Literal["posix", "windows"] = "posix"
        anchor = "/"
        raw_components = value[1:]
    elif (
        len(value) >= 3
        and value[0] in ascii_letters
        and value[1:3] == ":/"
    ):
        flavor = "windows"
        anchor = value[:2].lower()
        raw_components = value[3:]
    else:
        return None

    if raw_components:
        components = tuple(raw_components.split("/"))
        if any(
            not component or component in {".", ".."} or ":" in component
            for component in components
        ):
            return None
    else:
        components = ()

    if flavor == "windows" and any(
        not _windows_component_is_valid(component) for component in components
    ):
        return None

    if flavor == "windows":
        canonical = f"{anchor}/" + "/".join(components)
    else:
        canonical = "/" + "/".join(components)
    return CanonicalPath(canonical, flavor, anchor, components)


def canonicalize_targets(targets: object) -> tuple[str, ...] | None:
    """Return one semantic canonical target tuple, or ``None`` when malformed."""
    if not isinstance(targets, tuple):
        return None
    parsed = tuple(parse_canonical_path(target) for target in targets)
    if any(path is None for path in parsed):
        return None
    canonical = tuple(path.value for path in parsed if path is not None)
    if len(set(canonical)) != len(canonical):
        return None
    return canonical


def path_is_within(target: str, root: str) -> bool:
    target_path = parse_canonical_path(target)
    root_path = parse_canonical_path(root)
    if target_path is None or root_path is None:
        return False
    if (
        target_path.flavor != root_path.flavor
        or target_path.anchor != root_path.anchor
        or len(target_path.components) < len(root_path.components)
    ):
        return False
    return (
        target_path.components[: len(root_path.components)]
        == root_path.components
    )


@dataclass(frozen=True)
class _CriticalRule:
    rule_id: str
    approval_class: str


CRITICAL_ACTION_RULES: Mapping[str, _CriticalRule] = MappingProxyType(
    {
        "credentials": _CriticalRule(
            "policy.approval.credentials", "credentials"
        ),
        "money_quota": _CriticalRule(
            "policy.approval.money_quota", "money_quota"
        ),
        "money": _CriticalRule("policy.approval.money_quota", "money_quota"),
        "quota": _CriticalRule("policy.approval.money_quota", "money_quota"),
        "external_communication": _CriticalRule(
            "policy.approval.external_communication", "external_communication"
        ),
        "publish": _CriticalRule("policy.approval.publish", "publish"),
        "push": _CriticalRule("policy.approval.publish", "publish"),
        "pull_request": _CriticalRule("policy.approval.publish", "publish"),
        "release": _CriticalRule("policy.approval.publish", "publish"),
        "production": _CriticalRule("policy.approval.production", "production"),
        "admin_service": _CriticalRule(
            "policy.approval.admin_service", "admin_service"
        ),
        "admin": _CriticalRule("policy.approval.admin_service", "admin_service"),
        "service": _CriticalRule(
            "policy.approval.admin_service", "admin_service"
        ),
        "startup": _CriticalRule(
            "policy.approval.admin_service", "admin_service"
        ),
        "destructive": _CriticalRule(
            "policy.approval.destructive", "destructive"
        ),
        "live_canary": _CriticalRule(
            "policy.approval.live_canary", "live_canary"
        ),
        "final_acceptance": _CriticalRule(
            "policy.approval.final_acceptance", "final_acceptance"
        ),
    }
)


def approval_class_for_action(canonical_action: object) -> str | None:
    """Return the closed critical approval class for one canonical action."""
    if type(canonical_action) is not str:
        return None
    rule = CRITICAL_ACTION_RULES.get(canonical_action)
    return rule.approval_class if rule is not None else None

_ROUTINE_ACTIONS = frozenset({"status", "local_code_edit", "local_test"})
_DELIVERY_CLASS = "internal_delivery"
_CANONICAL_ACTION_CLASSES: Mapping[str, str] = MappingProxyType(
    {
        **{action: action for action in _ROUTINE_ACTIONS},
        **{action: action for action in CRITICAL_ACTION_RULES},
        "event.deliver": _DELIVERY_CLASS,
    }
)


def _deny(rule_id: str, reason: str) -> PolicyDecision:
    return PolicyDecision(Decision.DENY, rule_id, reason)


def _is_text(value: object) -> bool:
    return type(value) is str and bool(value)


def _items_are_canonical(items: object) -> bool:
    return (
        isinstance(items, tuple)
        and bool(items)
        and all(_is_text(item) for item in items)
        and len(set(items)) == len(items)
    )


def _valid_actor_for_project(
    actor: object, project: object
) -> bool:
    if not (
        isinstance(actor, ActorContext)
        and isinstance(project, ProjectPolicyView)
        and _is_text(actor.actor_id)
        and _is_text(actor.surface)
        and actor.surface in {"desktop", "discord"}
        and _is_text(actor.binding_id)
        and actor.is_owner is True
        and isinstance(project.delivery_bindings, tuple)
    ):
        return False
    matches = tuple(
        binding
        for binding in project.delivery_bindings
        if isinstance(binding, ProjectBindingView)
        and binding.project_id == project.project_id
        and binding.binding_id == actor.binding_id
        and binding.surface == actor.surface
        and binding.owner_actor_id == actor.actor_id
    )
    return len(matches) == 1


def _valid_command_shape(command: object) -> bool:
    if not isinstance(command, ProjectCommand):
        return False
    canonical_targets = canonicalize_targets(command.targets)
    if not (
        _is_text(command.name)
        and _is_text(command.project_id)
        and type(command.revision) is int
        and command.revision > 0
        and _is_text(command.action_class)
        and canonical_targets is not None
        and _CANONICAL_ACTION_CLASSES.get(command.name) == command.action_class
    ):
        return False
    if command.batch_id is None:
        return (
            command.batch_items == ()
            or _items_are_canonical(command.batch_items)
        )
    return _is_text(command.batch_id) and _items_are_canonical(command.batch_items)


def _valid_scope(
    command: ProjectCommand,
    project: object,
    contract: object,
) -> str | None:
    if not isinstance(project, ProjectPolicyView) or not isinstance(
        contract, ContractPolicyView
    ):
        return "policy.project.mismatch"
    if command.project_id != project.project_id:
        return "policy.project.mismatch"
    canonical_roots = canonicalize_targets(project.roots)
    if not (
        _is_text(project.project_id)
        and canonical_roots is not None
        and bool(canonical_roots)
        and _is_text(project.approved_plan_ref)
        and _is_text(contract.approved_plan_ref)
        and project.approved_plan_ref == contract.approved_plan_ref
        and type(contract.revision) is int
        and contract.revision == command.revision
        and type(contract.allowed_action_classes) is frozenset
        and all(_is_text(item) for item in contract.allowed_action_classes)
        and type(contract.allowed_phases) is frozenset
        and all(_is_text(item) for item in contract.allowed_phases)
        and command.action_class in contract.allowed_action_classes
    ):
        return "policy.contract.unapproved"
    return None


def _phase_is_valid(
    command: ProjectCommand,
    project: ProjectPolicyView,
    contract: ContractPolicyView,
) -> bool:
    phase = command.metadata.get("phase")
    return (
        _is_text(project.current_phase)
        and project.current_phase in contract.allowed_phases
        and phase == project.current_phase
    )


def _lifecycle_allows_action(
    project: ProjectPolicyView, action_class: str
) -> bool:
    if not _is_text(project.lifecycle) or project.lifecycle not in {
        "active",
        "awaiting_acceptance",
        "completed",
    }:
        return False
    if action_class in {"status", _DELIVERY_CLASS}:
        return True
    if action_class == "final_acceptance":
        return project.lifecycle == "awaiting_acceptance"
    return project.lifecycle == "active"


def _delivery_is_valid(
    command: ProjectCommand,
    project: ProjectPolicyView,
    actor: ActorContext,
) -> bool:
    facts = command.metadata
    return (
        command.name == "event.deliver"
        and command.action_class == _DELIVERY_CLASS
        and command.targets == ()
        and command.batch_id is None
        and command.batch_items == ()
        and type(project.canonical_event_ids) is frozenset
        and all(_is_text(event_id) for event_id in project.canonical_event_ids)
        and set(facts)
        == {
            "phase",
            "event_id",
            "binding_id",
            "binding_project_id",
            "binding_surface",
            "binding_owner_actor_id",
        }
        and _is_text(facts["event_id"])
        and facts["event_id"] in project.canonical_event_ids
        and facts["binding_id"] == actor.binding_id
        and facts["binding_project_id"] == project.project_id
        and facts["binding_surface"] == actor.surface
        and facts["binding_owner_actor_id"] == actor.actor_id
    )


def decide(
    command: ProjectCommand,
    project: ProjectPolicyView,
    contract: ContractPolicyView,
    actor: ActorContext,
) -> PolicyDecision:
    """Return a deterministic authorization decision without external I/O."""
    if not _valid_actor_for_project(actor, project):
        return _deny("policy.actor.unknown", "actor binding is not a project owner")
    if not _valid_command_shape(command):
        return _deny("policy.command.ambiguous", "command facts are malformed")
    scope_error = _valid_scope(command, project, contract)
    if scope_error is not None:
        return _deny(scope_error, "project contract does not match this command")
    assert isinstance(project, ProjectPolicyView)
    assert isinstance(contract, ContractPolicyView)
    if not all(
        any(path_is_within(target, root) for root in project.roots)
        for target in command.targets
    ):
        return _deny("policy.scope.outside_root", "target is outside registered roots")

    if command.action_class == _DELIVERY_CLASS:
        if not _delivery_is_valid(command, project, actor):
            return _deny(
                "policy.command.ambiguous",
                "delivery is not a canonical event projection",
            )
    else:
        if set(command.metadata) != {"phase"}:
            return _deny("policy.command.ambiguous", "command facts are ambiguous")
    if not _phase_is_valid(command, project, contract):
        return _deny("policy.phase.invalid", "command phase is not current")
    if not _lifecycle_allows_action(project, command.action_class):
        return _deny(
            "policy.phase.invalid",
            "action is invalid for the current project lifecycle",
        )

    if command.action_class == "status" and command.targets:
        return _deny("policy.command.ambiguous", "status does not accept targets")
    if command.action_class not in {"status", _DELIVERY_CLASS} and not command.targets:
        return _deny(
            "policy.scope.outside_root",
            "non-status work requires an exact in-root target",
        )

    critical_rule = CRITICAL_ACTION_RULES.get(command.action_class)
    if critical_rule is not None:
        return PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            critical_rule.rule_id,
            "critical action requires owner approval",
            critical_rule.approval_class,
        )
    if command.action_class == _DELIVERY_CLASS:
        return PolicyDecision(
            Decision.ALLOW,
            "policy.delivery.owner_bound_internal",
            "registered owner-bound canonical event delivery",
        )
    if command.action_class not in _ROUTINE_ACTIONS:
        return _deny("policy.command.ambiguous", "action class is not approved")
    return PolicyDecision(
        Decision.ALLOW,
        "policy.allow.routine_in_plan",
        "routine owner work is within the approved plan and scope",
    )
