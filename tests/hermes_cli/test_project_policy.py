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
    current_phase="implementation",
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
WINDOWS_RESERVED_DEVICE_BASENAMES = (
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


def _project_with_current_phase(
    current_phase: object, *, lifecycle: str = "active"
) -> ProjectPolicyView:
    return ProjectPolicyView(
        project_id=PROJECT.project_id,
        lifecycle=lifecycle,
        current_phase=current_phase,
        roots=PROJECT.roots,
        approved_plan_ref=PROJECT.approved_plan_ref,
        delivery_bindings=PROJECT.delivery_bindings,
        canonical_event_ids=PROJECT.canonical_event_ids,
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
    ("action_class", "rule_id", "approval_class", "lifecycle"),
    [
        ("credentials", "policy.approval.credentials", "credentials", "active"),
        (
            "money_quota",
            "policy.approval.money_quota",
            "money_quota",
            "active",
        ),
        (
            "external_communication",
            "policy.approval.external_communication",
            "external_communication",
            "active",
        ),
        ("publish", "policy.approval.publish", "publish", "active"),
        ("production", "policy.approval.production", "production", "active"),
        (
            "admin_service",
            "policy.approval.admin_service",
            "admin_service",
            "active",
        ),
        (
            "destructive",
            "policy.approval.destructive",
            "destructive",
            "active",
        ),
        (
            "live_canary",
            "policy.approval.live_canary",
            "live_canary",
            "active",
        ),
        (
            "final_acceptance",
            "policy.approval.final_acceptance",
            "final_acceptance",
            "awaiting_acceptance",
        ),
    ],
)
def test_critical_commands_require_their_stable_approval_class(
    action_class, rule_id, approval_class, lifecycle
):
    command = _command(action_class, targets=(f"{ROOT}/deployment",))
    project = replace(PROJECT, lifecycle=lifecycle)

    result = decide(command, project, _contract(action_class), OWNER)

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


@pytest.mark.parametrize(
    "malformed_item",
    [
        pytest.param([], id="list"),
        pytest.param({}, id="mapping"),
        pytest.param(set(), id="set"),
    ],
)
def test_unhashable_batch_items_are_denied_without_raising(malformed_item):
    command = _command(
        "local_code_edit",
        targets=(f"{ROOT}/src/app.py",),
        batch_id="batch-1",
        batch_items=(malformed_item,),
    )

    result = decide(command, PROJECT, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.command.ambiguous"


@pytest.mark.parametrize(
    ("surface", "lifecycle"),
    [
        pytest.param(surface, lifecycle, id=f"{surface}-{lifecycle}")
        for surface in ("desktop", "discord")
        for lifecycle in ("active", "awaiting_acceptance", "completed")
    ],
)
def test_owner_bound_canonical_event_delivery_is_internal(surface, lifecycle):
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
        lifecycle=lifecycle,
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


def test_delivery_phase_must_match_the_trusted_project_phase():
    command = _command(
        "internal_delivery",
        name="event.deliver",
        metadata={
            "phase": "implementation",
            "event_id": "event-1",
            "binding_id": "desktop-1",
            "binding_project_id": "project-1",
            "binding_surface": "desktop",
            "binding_owner_actor_id": "owner-1",
        },
    )
    project = replace(
        _project_with_current_phase("verification"),
        canonical_event_ids=frozenset({"event-1"}),
    )
    contract = replace(
        _contract("internal_delivery"),
        allowed_phases=frozenset({"implementation", "verification"}),
    )

    result = decide(command, project, contract, OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.phase.invalid"


def test_command_phase_must_match_the_trusted_project_phase():
    project = _project_with_current_phase("verification")
    contract = replace(
        _contract("local_code_edit"),
        allowed_phases=frozenset({"implementation", "verification"}),
    )
    command = _command(
        "local_code_edit",
        targets=(f"{ROOT}/src/app.py",),
        metadata={"phase": "implementation"},
    )

    result = decide(command, project, contract, OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.phase.invalid"


@pytest.mark.parametrize(
    "current_phase",
    [
        pytest.param(None, id="missing"),
        pytest.param("", id="empty"),
        pytest.param(True, id="boolean"),
        pytest.param("verification", id="not-approved-by-contract"),
    ],
)
def test_trusted_project_phase_must_be_valid_and_contract_approved(current_phase):
    project = _project_with_current_phase(current_phase)
    command = _command(
        "local_code_edit",
        targets=(f"{ROOT}/src/app.py",),
        metadata={"phase": current_phase},
    )

    result = decide(command, project, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.phase.invalid"


@pytest.mark.parametrize(
    "lifecycle", ["active", "awaiting_acceptance", "completed"]
)
def test_status_is_allowed_in_each_valid_lifecycle(lifecycle):
    result = decide(
        _command("status"),
        replace(PROJECT, lifecycle=lifecycle),
        _contract("status"),
        OWNER,
    )

    assert result.decision is Decision.ALLOW
    assert result.rule_id == "policy.allow.routine_in_plan"


@pytest.mark.parametrize(
    ("lifecycle", "action_class"),
    [
        ("awaiting_acceptance", "local_code_edit"),
        ("completed", "local_code_edit"),
        ("awaiting_acceptance", "local_test"),
        ("completed", "local_test"),
        ("awaiting_acceptance", "publish"),
        ("completed", "publish"),
        ("active", "final_acceptance"),
        ("completed", "final_acceptance"),
    ],
)
def test_lifecycle_matrix_denies_actions_outside_their_runtime_state(
    lifecycle, action_class
):
    command = _command(action_class, targets=(f"{ROOT}/artifact",))

    result = decide(
        command,
        replace(PROJECT, lifecycle=lifecycle),
        _contract(action_class),
        OWNER,
    )

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.phase.invalid"


def test_unknown_lifecycle_is_denied_with_the_stable_phase_rule():
    result = decide(
        _command("status"),
        replace(PROJECT, lifecycle="archived"),
        _contract("status"),
        OWNER,
    )

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.phase.invalid"


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
        ("C:/Work/Project", "c:/Work/Project/SRC/file.py"),
    ],
)
def test_component_containment_handles_drive_and_filesystem_roots(root, target):
    project = replace(PROJECT, roots=(root,))
    command = _command("local_code_edit", targets=(target,))

    result = decide(command, project, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.ALLOW


@pytest.mark.parametrize(
    "component",
    [
        pytest.param(
            f"{basename}{suffix}",
            id=f"device-{index}-{suffix or 'bare'}",
        )
        for index, basename in enumerate(WINDOWS_RESERVED_DEVICE_BASENAMES)
        for suffix in ("", ".txt")
    ],
)
def test_windows_reserved_device_components_are_not_command_targets(component):
    command = _command(
        "local_code_edit",
        targets=(f"{ROOT}/{component}",),
    )

    result = decide(command, PROJECT, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.command.ambiguous"


@pytest.mark.parametrize(
    "component",
    [
        pytest.param("cOn.TxT", id="mixed-case-extension"),
        pytest.param("cLoCk$.log", id="mixed-case-clock"),
        pytest.param("cOm¹.bin", id="mixed-case-superscript"),
        pytest.param("ordinary.", id="trailing-dot"),
        pytest.param("ordinary ", id="trailing-space"),
        *[
            pytest.param(f"bad{character}name", id=f"forbidden-{ord(character):02x}")
            for character in '<>:"|?*\\'
        ],
        *[
            pytest.param(f"bad{chr(codepoint)}name", id=f"control-{codepoint:02x}")
            for codepoint in range(0x20)
        ],
    ],
)
def test_windows_components_with_nonliteral_win32_identity_are_denied(component):
    command = _command(
        "local_code_edit",
        targets=(f"{ROOT}/{component}",),
    )

    result = decide(command, PROJECT, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.command.ambiguous"


@pytest.mark.parametrize(
    "component",
    [
        pytest.param("NUL.txt", id="reserved-device"),
        pytest.param("ordinary.", id="trailing-dot"),
        pytest.param("bad|name", id="forbidden-character"),
        pytest.param("bad\x1fname", id="control-character"),
    ],
)
def test_windows_roots_with_nonliteral_win32_identity_are_unapproved(component):
    project = replace(PROJECT, roots=(f"C:/work/{component}",))
    command = _command(
        "local_code_edit",
        targets=(f"{ROOT}/file.py",),
    )

    result = decide(command, project, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.contract.unapproved"


def test_posix_reserved_device_spelling_remains_an_ordinary_component():
    project = replace(PROJECT, roots=("/workspace/COM1",))
    command = _command(
        "local_code_edit",
        targets=("/workspace/COM1/file.py",),
    )

    result = decide(command, project, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.ALLOW


@pytest.mark.parametrize(
    ("root_component", "target_codepoint"),
    [
        pytest.param("ss", 0x00DF, id="eszett-does-not-equal-ss"),
        pytest.param("ffi", 0xFB03, id="ligature-does-not-equal-ffi"),
    ],
)
def test_windows_component_identity_never_uses_expanding_casefold(
    root_component, target_codepoint
):
    project = replace(PROJECT, roots=(f"C:/work/{root_component}",))
    target = f"C:/work/{chr(target_codepoint)}/outside.py"
    command = _command("local_code_edit", targets=(target,))

    result = decide(command, project, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.scope.outside_root"


def test_windows_component_case_mismatch_is_outside_root():
    project = replace(PROJECT, roots=("C:/Work/Project",))
    command = _command(
        "local_code_edit", targets=("c:/work/project/outside.py",)
    )

    result = decide(command, project, _contract("local_code_edit"), OWNER)

    assert result.decision is Decision.DENY
    assert result.rule_id == "policy.scope.outside_root"


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
