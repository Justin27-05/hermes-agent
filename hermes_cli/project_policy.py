"""Pure, fail-closed authorization decisions for ProjectRuntime commands."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
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


@dataclass(frozen=True)
class ProjectBindingView:
    """A pre-registered owner-bound delivery surface, supplied by runtime state."""

    binding_id: str
    surface: Literal["desktop", "discord"]
    owner_actor_id: str


@dataclass(frozen=True)
class ProjectPolicyView:
    project_id: str
    lifecycle: str
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
class _CriticalRule:
    rule_id: str
    approval_class: str


# This is the only critical-action classification table.  Aliases resolve to
# the same durable approval class instead of being inferred from command text.
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
        "destructive": _CriticalRule("policy.approval.destructive", "destructive"),
        "live_canary": _CriticalRule(
            "policy.approval.live_canary", "live_canary"
        ),
        "final_acceptance": _CriticalRule(
            "policy.approval.final_acceptance", "final_acceptance"
        ),
    }
)

_ROUTINE_ACTIONS = frozenset({"status", "local_code_edit", "local_test"})
_DELIVERY_ACTION = "internal_delivery"
_CANONICAL_PATH = re.compile(
    r"^(?:[A-Za-z]:/(?:[^/]+(?:/[^/]+)*)?|/(?:[^/]+(?:/[^/]+)*)?)$"
)


def _deny(rule_id: str, reason: str) -> PolicyDecision:
    return PolicyDecision(Decision.DENY, rule_id, reason)


def _is_text(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _is_canonical_path(value: object) -> bool:
    if not _is_text(value) or not _CANONICAL_PATH.fullmatch(value):
        return False
    path_parts = value[3:] if len(value) > 1 and value[1:3] == ":/" else value[1:]
    return all(part not in {".", ".."} for part in path_parts.split("/"))


def _targets_are_canonical(targets: object) -> bool:
    return (
        isinstance(targets, tuple)
        and len(set(targets)) == len(targets)
        and all(_is_canonical_path(target) for target in targets)
    )


def _items_are_canonical(items: object) -> bool:
    return (
        isinstance(items, tuple)
        and len(items) > 0
        and len(set(items)) == len(items)
        and all(_is_text(item) for item in items)
    )


def _inside_project_roots(targets: tuple[str, ...], roots: tuple[str, ...]) -> bool:
    for target in targets:
        if not any(target == root or target.startswith(f"{root}/") for root in roots):
            return False
    return True


def _valid_actor(actor: object) -> bool:
    return (
        isinstance(actor, ActorContext)
        and _is_text(actor.actor_id)
        and actor.surface in {"desktop", "discord", "system"}
        and _is_text(actor.binding_id)
        and actor.is_owner is True
    )


def _valid_command_shape(command: object) -> bool:
    if not isinstance(command, ProjectCommand):
        return False
    if not all(
        (
            _is_text(command.name),
            _is_text(command.project_id),
            isinstance(command.revision, int) and command.revision > 0,
            _is_text(command.action_class),
            _targets_are_canonical(command.targets),
            isinstance(command.metadata, Mapping),
        )
    ):
        return False
    if command.batch_id is None:
        return command.batch_items == ()
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
    if not (
        _is_text(project.project_id)
        and project.lifecycle in {"active", "awaiting_acceptance", "completed"}
        and isinstance(project.roots, tuple)
        and len(project.roots) > 0
        and len(set(project.roots)) == len(project.roots)
        and all(_is_canonical_path(root) for root in project.roots)
        and _is_text(project.approved_plan_ref)
        and _is_text(contract.approved_plan_ref)
        and project.approved_plan_ref == contract.approved_plan_ref
        and isinstance(contract.revision, int)
        and contract.revision == command.revision
        and command.action_class in contract.allowed_action_classes
    ):
        return "policy.contract.unapproved"
    return None


def _phase_is_valid(command: ProjectCommand, contract: ContractPolicyView) -> bool:
    phase = command.metadata.get("phase")
    return (
        set(command.metadata) == {"phase"}
        and _is_text(phase)
        and phase in contract.allowed_phases
    )


def _delivery_is_valid(
    command: ProjectCommand,
    project: ProjectPolicyView,
    actor: ActorContext,
    contract: ContractPolicyView,
) -> bool:
    facts = command.metadata
    if not (
        isinstance(project.delivery_bindings, tuple)
        and isinstance(project.canonical_event_ids, frozenset)
        and all(
            isinstance(binding, ProjectBindingView)
            and _is_text(binding.binding_id)
            and binding.surface in {"desktop", "discord"}
            and _is_text(binding.owner_actor_id)
            for binding in project.delivery_bindings
        )
    ):
        return False
    matching_bindings = tuple(
        binding
        for binding in project.delivery_bindings
        if binding.binding_id == actor.binding_id
    )
    return (
        command.name == "event.deliver"
        and command.targets == ()
        and command.batch_id is None
        and command.batch_items == ()
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
        and facts["binding_id"] == actor.binding_id
        and facts["binding_project_id"] == project.project_id
        and facts["binding_surface"] == actor.surface
        and facts["binding_surface"] in {"desktop", "discord"}
        and facts["binding_owner_actor_id"] == actor.actor_id
        and facts["phase"] in contract.allowed_phases
        and len(matching_bindings) == 1
        and matching_bindings[0].surface == actor.surface
        and matching_bindings[0].owner_actor_id == actor.actor_id
        and facts["event_id"] in project.canonical_event_ids
    )


def decide(
    command: ProjectCommand,
    project: ProjectPolicyView,
    contract: ContractPolicyView,
    actor: ActorContext,
) -> PolicyDecision:
    """Return a deterministic authorization decision without any external I/O."""
    if not _valid_actor(actor):
        return _deny("policy.actor.unknown", "actor is not a registered owner")
    if not _valid_command_shape(command):
        return _deny("policy.command.ambiguous", "command facts are malformed")
    scope_error = _valid_scope(command, project, contract)
    if scope_error is not None:
        return _deny(scope_error, "project contract does not match this command")
    assert isinstance(project, ProjectPolicyView)
    assert isinstance(contract, ContractPolicyView)
    if not _inside_project_roots(command.targets, project.roots):
        return _deny("policy.scope.outside_root", "target is outside registered roots")

    critical_rule = CRITICAL_ACTION_RULES.get(command.action_class)
    if command.action_class == _DELIVERY_ACTION:
        if not _delivery_is_valid(command, project, actor, contract):
            return _deny("policy.command.ambiguous", "delivery is not a canonical projection")
    else:
        if set(command.metadata) != {"phase"}:
            return _deny("policy.command.ambiguous", "command facts are ambiguous")
        if not _phase_is_valid(command, contract):
            return _deny("policy.phase.invalid", "command phase is not approved")

    if critical_rule is not None:
        return PolicyDecision(
            Decision.REQUIRE_APPROVAL,
            critical_rule.rule_id,
            "critical action requires owner approval",
            critical_rule.approval_class,
        )
    if command.action_class == _DELIVERY_ACTION:
        return PolicyDecision(
            Decision.ALLOW,
            "policy.delivery.owner_bound_internal",
            "registered owner-bound canonical event delivery",
        )
    if command.action_class not in _ROUTINE_ACTIONS:
        return _deny("policy.command.ambiguous", "action class is not approved")
    if command.action_class == "status" and command.targets:
        return _deny("policy.command.ambiguous", "status does not accept targets")
    if command.action_class != "status" and not command.targets:
        return _deny("policy.scope.outside_root", "local work requires an in-root target")
    return PolicyDecision(
        Decision.ALLOW,
        "policy.allow.routine_in_plan",
        "routine owner work is within the approved plan and scope",
    )
