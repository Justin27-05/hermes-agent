"""Canonical `/project` command grammar at the gateway boundary."""

import pytest

from gateway import slash_commands


@pytest.mark.parametrize(
    ("text", "name", "payload"),
    (
        (
            "/project create Project Atlas",
            "project.create",
            {"name": "Project Atlas", "current_phase": "planning"},
        ),
        (
            "/project rename Renamed Atlas",
            "project.rename",
            {"name": "Renamed Atlas"},
        ),
        ("/project status", "project.status", {}),
        ("/project queue", "queue.status", {}),
        (
            "/project stop turn-7 3",
            "run.stop",
            {"turn_id": "turn-7", "expected_control_version": 3},
        ),
        (
            "/project resume turn-7 5",
            "run.resume",
            {"turn_id": "turn-7", "expected_control_version": 5},
        ),
        (
            "/project approval 'approval durable 7' approve",
            "approval.resolve",
            {
                "approval_id": "approval durable 7",
                "outcome": "approved",
            },
        ),
        (
            "/project approval approval-8 deny",
            "approval.resolve",
            {
                "approval_id": "approval-8",
                "outcome": "denied",
            },
        ),
        ("/project complete confirm", "project.accept_completion", {}),
        ("/project reopen", "project.reopen", {}),
    ),
)
def test_project_command_uses_one_canonical_command_and_payload(
    text, name, payload
):
    """Changing a verb or payload field must be observable at the Service seam."""
    command = slash_commands.parse_project_slash_command(text)

    assert command is not None
    assert command.name == name
    assert dict(command.payload) == payload


@pytest.mark.parametrize(
    "text",
    (
        "/project complete",
        "/project complete yes",
        "/project stop turn-7",
        "/project resume turn-7",
        "/project approval approval-7",
        "/project approval approval-7 later",
        "/project approval approval-7 approve extra",
        "/project use project-1",
        "/project clear",
        "/project unknown",
    ),
)
def test_project_command_rejects_ambiguous_and_unsupported_controls(
    text,
):
    """Unclaimed input must never silently mutate a managed project."""
    assert slash_commands.parse_project_slash_command(text) is None
