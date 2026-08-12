"""Pure values and validation for canonical project conversation lineage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Surface = Literal["desktop", "discord"]


@dataclass(frozen=True)
class ProjectConversation:
    conversation_id: str
    project_id: str
    parent_conversation_id: str | None
    root_conversation_id: str
    created_at: int


@dataclass(frozen=True)
class SurfaceBinding:
    binding_id: str
    project_id: str
    surface: Surface
    external_binding_id: str
    actor_id: str
    created_at: int


def _nonempty_text(value: object, field_name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _timestamp(value: object, field_name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{field_name} must be an integer timestamp")
    return value


def make_root_conversation(
    *,
    project_id: str,
    conversation_id: str,
    created_at: int,
) -> ProjectConversation:
    """Build the sole legal initial conversation: a self-root."""
    project = _nonempty_text(project_id, "project_id")
    conversation = _nonempty_text(conversation_id, "conversation_id")
    return ProjectConversation(
        conversation_id=conversation,
        project_id=project,
        parent_conversation_id=None,
        root_conversation_id=conversation,
        created_at=_timestamp(created_at, "created_at"),
    )


def make_child_conversation(
    *,
    project_id: str,
    conversation_id: str,
    parent_conversation_id: str,
    root_conversation_id: str,
    created_at: int,
) -> ProjectConversation:
    """Build a child that preserves its project's existing root."""
    project = _nonempty_text(project_id, "project_id")
    conversation = _nonempty_text(conversation_id, "conversation_id")
    parent = _nonempty_text(
        parent_conversation_id, "parent_conversation_id"
    )
    root = _nonempty_text(root_conversation_id, "root_conversation_id")
    if conversation in {parent, root}:
        raise ValueError("a child conversation must have a distinct identity")
    return ProjectConversation(
        conversation_id=conversation,
        project_id=project,
        parent_conversation_id=parent,
        root_conversation_id=root,
        created_at=_timestamp(created_at, "created_at"),
    )


def make_surface_binding(
    *,
    binding_id: str,
    project_id: str,
    surface: Surface,
    external_binding_id: str,
    actor_id: str,
    created_at: int,
) -> SurfaceBinding:
    """Build one immutable Desktop or Discord project binding."""
    if type(surface) is not str or surface not in {"desktop", "discord"}:
        raise ValueError("surface must be 'desktop' or 'discord'")
    return SurfaceBinding(
        binding_id=_nonempty_text(binding_id, "binding_id"),
        project_id=_nonempty_text(project_id, "project_id"),
        surface=surface,
        external_binding_id=_nonempty_text(
            external_binding_id, "external_binding_id"
        ),
        actor_id=_nonempty_text(actor_id, "actor_id"),
        created_at=_timestamp(created_at, "created_at"),
    )
