"""Behavioral tests for the pure ProjectRuntime policy boundary."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from hermes_cli.project_policy import (
    ActorContext,
    ContractPolicyView,
    Decision,
    ProjectCommand,
    ProjectBindingView,
    ProjectPolicyView,
    decide,
)


ROOT = "C:/work/project"
PROJECT = ProjectPolicyView(
    project_id="project-1",
    lifecycle="active",
    roots=(ROOT,),
    approved_plan_ref="plans/project-1/v1",
    delivery_bindings=(
        ProjectBindingView(
            binding_id="desktop-1",
            surface="desktop",
            owner_actor_id="owner-1",
            project_id="project-1",
        ),
    ),
)
OWNER = ActorContext(
    actor_id="owner-1",
    surface="desktop",
    binding_id="desktop-1",
    is_owner=True,
)


def _contract(*action_classes: str) -> ContractPolicyView:
    return ContractPolicyView(
        revision=7,
        allowed_action_classes=frozenset(action_classes),
        allowed_phases=frozenset({"implementation"}),
        approved_plan_ref="plans/project-1/v1",
    )


def _command(
    action_class: str,
    *,
    name: str | None = None,
    targets: tuple[str, ...] = (),
    metadata: dict[str, object] | None = None,
    project_id: str = "project-1",
    revision: int = 7,
    batch_id: str | None = None,
    batch_items: tuple[str, ...] = (),
) -> ProjectCommand:
    return ProjectCommand(
        name=name or action_class,
        project_id=project_id,
        revision=revision,
        action_class=action_class,
        targets=targets,
        batch_id=batch_id,
        batch_items=batch_items,
        metadata=metadata if metadata is not None else {"phase": "implementation"},
    )


@pytest.mark.parametrize(
    ("command", "rule_id"),
    [
        (_command("status"), "policy.allow.routine_in_plan"),
        (
            _command("local_code_edit", targets=(f"{ROOT}/src/app.py",)),
            "policy.allow.routine_in_plan",
        ),
        (
            _command("local_test", targets=(f"{ROOT}/tests/test_app.py",)),
            "policy.allow.routine_in_plan",
        ),
    ],
)
def test_routine_owner_commands_inside_approved_scope_are_allowed(
    command, rule_id
):
    result = decide(command, PROJECT, _contract(command.action_class), OWNER)

    assert result.decision is Decision.ALLOW
    assert result.rule_id == rule_id
    assert result.approval_class is None


def test_routine_scope_accepts_a_canonical_posix_project_root():
    project = replace(PROJECT, roots=("/workspace/project",))
    command = _command(
        "local_code_edit", targets=("/workspace/project/src/app.py",)
    )

    result = decide(command, project, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.ALLOW
    assert result.rule_id == "policy.allow.routine_in_plan"


@pytest.mark.parametrize(
    ("action_class", "rule_id", "approval_class"),
    [
        ("credentials", "policy.approval.credentials", "credentials"),
        ("money_quota", "policy.approval.money_quota", "money_quota"),
        (
            "external_communication",
            "policy.approval.external_communication",
            "external_communication",
        ),
        ("publish", "policy.approval.publish", "publish"),
        ("production", "policy.approval.production", "production"),
        ("admin_service", "policy.approval.admin_service", "admin_service"),
        ("destructive", "policy.approval.destructive", "destructive"),
        ("live_canary", "policy.approval.live_canary", "live_canary"),
        (
            "final_acceptance",
            "policy.approval.final_acceptance",
            "final_acceptance",
        ),
    ],
)
def test_critical_commands_require_their_stable_approval_class(
    action_class, rule_id, approval_class
):
    command = _command(action_class, targets=(f"{ROOT}/deployment",))

    result = decide(command, PROJECT, _contract(action_class), OWNER)

    assert result.decision is Decision.REQUIRE_APPROVAL
    assert result.rule_id == rule_id
    assert result.approval_class == approval_class


@pytest.mark.parametrize(
    ("command", "project", "contract", "actor", "rule_id"),
    [
        (
            _command("status"),
            PROJECT,
            _contract("status"),
            ActorContext("unknown", "desktop", "desktop-1", False),
            "policy.actor.unknown",
        ),
        (
            _command("status"),
            replace(PROJECT, approved_plan_ref=None),
            _contract("status"),
            OWNER,
            "policy.contract.unapproved",
        ),
        (
            _command("status", project_id="other-project"),
            PROJECT,
            _contract("status"),
            OWNER,
            "policy.project.mismatch",
        ),
        (
            _command("local_code_edit", targets=("C:/work/project-other/a.py",)),
            PROJECT,
            _contract("local_code_edit"),
            OWNER,
            "policy.scope.outside_root",
        ),
        (
            _command("unclassified_action"),
            PROJECT,
            _contract("unclassified_action"),
            OWNER,
            "policy.command.ambiguous",
        ),
        (
            _command("credentials", metadata={"phase": "implementation", "extra": True}),
            PROJECT,
            _contract("credentials"),
            OWNER,
            "policy.command.ambiguous",
        ),
        (
            _command("credentials"),
            PROJECT,
            _contract("credentials"),
            ActorContext("owner-1", "desktop", "desktop-1", False),
            "policy.actor.unknown",
        ),
    ],
)
def test_invalid_or_ambiguous_commands_are_denied_before_critical_rules(
    command, project, contract, actor, rule_id
):
    result = decide(command, project, contract, actor)

    assert result.decision is Decision.DENY
    assert result.rule_id == rule_id


@pytest.mark.parametrize("surface", ["desktop", "discord"])
def test_owner_bound_canonical_event_delivery_is_internal(surface):
    actor = ActorContext("owner-1", surface, f"{surface}-1", True)
    command = _command(
        "internal_delivery",
        name="event.deliver",
        metadata={
            "phase": "implementation",
            "event_id": "event-1",
            "binding_id": f"{surface}-1",
            "binding_project_id": "project-1",
            "binding_surface": surface,
            "binding_owner_actor_id": "owner-1",
        },
    )

    registered_project = replace(
        PROJECT,
        delivery_bindings=(
            ProjectBindingView(
                binding_id=f"{surface}-1",
                surface=surface,
                owner_actor_id="owner-1",
                project_id="project-1",
            ),
        ),
        canonical_event_ids=frozenset({"event-1"}),
    )
    result = decide(command, registered_project, _contract("internal_delivery"), actor)

    assert result.decision is Decision.ALLOW
    assert result.rule_id == "policy.delivery.owner_bound_internal"


def test_delivery_metadata_cannot_claim_an_unregistered_binding_or_event():
    command = _command(
        "internal_delivery",
        name="event.deliver",
        metadata={
            "phase": "implementation",
            "event_id": "invented-event",
            "binding_id": "desktop-1",
            "binding_project_id": "project-1",
            "binding_surface": "desktop",
            "binding_owner_actor_id": "owner-1",
        },
    )

    result = decide(command, PROJECT, _contract("internal_delivery"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.command.ambiguous"


def test_delivery_with_a_supplied_destination_is_denied():
    command = _command(
        "internal_delivery",
        name="event.deliver",
        targets=("C:/work/project/destination",),
        metadata={
            "phase": "implementation",
            "event_id": "event-1",
            "binding_id": "desktop-1",
            "binding_project_id": "project-1",
            "binding_surface": "desktop",
            "binding_owner_actor_id": "owner-1",
        },
    )

    result = decide(command, PROJECT, _contract("internal_delivery"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.command.ambiguous"


@pytest.mark.parametrize(
    ("name", "action_class"),
    [
        ("publish", "status"),
        ("status", "publish"),
        ("event.deliver", "status"),
        ("publish", "internal_delivery"),
    ],
)
def test_canonical_action_name_cannot_disagree_with_action_class(
    name, action_class
):
    command = _command(
        action_class,
        name=name,
        targets=() if action_class in {"status", "internal_delivery"} else (f"{ROOT}/x",),
    )

    result = decide(command, PROJECT, _contract(action_class), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.command.ambiguous"


@pytest.mark.parametrize(
    "target",
    [
        r"C:\work\project\outside.txt",
        r"C:/work/project/sub\..\..\outside.txt",
        "work/project/file.py",
        "C:/work/project/./file.py",
        "C:/work/project/src/../file.py",
        "C:work/project/file.py",
        "C://work/project/file.py",
        "//server/share/project/file.py",
    ],
)
def test_noncanonical_paths_are_denied_before_scope_authorization(target):
    command = _command("local_code_edit", targets=(target,))

    result = decide(command, PROJECT, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.command.ambiguous"


@pytest.mark.parametrize(
    ("root", "target"),
    [
        ("/", "/workspace/project/file.py"),
        ("C:/", "c:/WORK/project/file.py"),
        ("C:/Work/Project", "c:/work/project/SRC/file.py"),
    ],
)
def test_component_containment_handles_drive_and_filesystem_roots(root, target):
    project = replace(PROJECT, roots=(root,))
    command = _command("local_code_edit", targets=(target,))

    result = decide(command, project, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.ALLOW


def test_component_containment_rejects_a_sibling_with_the_same_prefix():
    command = _command(
        "local_code_edit", targets=("C:/work/project-sibling/file.py",)
    )

    result = decide(command, PROJECT, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.scope.outside_root"


def test_project_command_copies_and_freezes_authorization_metadata():
    metadata = {"phase": "implementation"}
    command = _command("status", metadata=metadata)

    metadata["phase"] = "production"

    assert command.metadata == {"phase": "implementation"}
    assert isinstance(command.metadata, MappingProxyType)
    with pytest.raises(TypeError):
        command.metadata["phase"] = "production"


@pytest.mark.parametrize("value", [["implementation"], {"nested": "value"}])
def test_project_command_rejects_non_scalar_metadata(value):
    with pytest.raises(TypeError):
        _command("status", metadata={"phase": value})


@pytest.mark.parametrize(
    "actor",
    [
        ActorContext("attacker", "desktop", "desktop-1", True),
        ActorContext("owner-1", "discord", "desktop-1", True),
        ActorContext("owner-1", "desktop", "forged-binding", True),
        ActorContext("owner-1", "system", "desktop-1", True),
    ],
)
def test_routine_commands_require_exact_registered_project_owner_binding(actor):
    registered_project = replace(
        PROJECT,
        delivery_bindings=(
            ProjectBindingView(
                "desktop-1", "desktop", "owner-1", "project-1"
            ),
        ),
    )

    result = decide(_command("status"), registered_project, _contract("status"), actor)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.actor.unknown"


def test_bool_revision_is_not_an_integer_revision():
    result = decide(
        _command("status", revision=True),
        PROJECT,
        replace(_contract("status"), revision=True),
        OWNER,
    )

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.command.ambiguous"


def test_targetless_critical_action_is_denied_before_approval():
    result = decide(
        _command("publish", targets=()),
        PROJECT,
        _contract("publish"),
        OWNER,
    )

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.scope.outside_root"
