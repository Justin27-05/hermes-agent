"""Crash-safe projection of canonical project lifecycle into Discord."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import asdict, replace
import hashlib
import inspect
import json
import sqlite3
import time
from typing import Callable

from gateway.project_surfaces import (
    DiscordProjectSurface,
    ProjectLifecycleSnapshot,
    project_channel_spec_for_lifecycle_event,
)
from hermes_cli import project_runtime_db as runtime_db
from hermes_cli import project_surface_operations as surface_ops
from hermes_cli import projects_db
from hermes_cli.project_events import ProjectEvent, ProjectEventOutbox
from plugins.platforms.discord.project_channels import (
    DiscordProjectErrorCode,
    DiscordProjectPort,
    DiscordProjectPortError,
    ProjectChannelSpec,
    ProjectChannelState,
    project_channel_marker,
    state_matches_spec,
)


_MANAGED_BINDING_PREFIX = "discord-project-"
_LIFECYCLE_EVENT_KINDS = frozenset(
    {
        "project.created",
        "project.renamed",
        "project.technically_completed",
        "project.completion_accepted",
        "project.reopened",
    }
)
_BLOCKING_ERROR_CODES = frozenset(
    {
        DiscordProjectErrorCode.CONFLICT,
        DiscordProjectErrorCode.FORBIDDEN,
        DiscordProjectErrorCode.NOT_FOUND,
        DiscordProjectErrorCode.INVALID_ARGUMENT,
    }
)


def _digest(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


def discord_surface_binding_id(
    project_id: str,
    guild_id: str,
    channel_id: str,
) -> str:
    """Return a stable internal binding reference without exposing Discord IDs."""
    return _MANAGED_BINDING_PREFIX + _digest(
        project_id,
        guild_id,
        channel_id,
    )[:32]


def _operation_id(event: ProjectEvent) -> str:
    return "surface-operation-" + _digest(
        event.project_id,
        event.event_id,
        "discord",
    )[:32]


def _surface_event_id(operation_id: str, kind: str) -> str:
    return "surface-event-" + _digest(operation_id, kind)[:32]


def _actor_id(surface: DiscordProjectSurface) -> str:
    return "discord-owner-" + _digest(
        surface.guild_id,
        surface.owner_user_id,
    )[:24]


def _json_object(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _spec_from_json(value: str) -> ProjectChannelSpec:
    try:
        payload = json.loads(value)
        return ProjectChannelSpec(**payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("malformed persisted surface desired state") from exc


def _state_json(state: ProjectChannelState | None) -> str:
    return _json_object({} if state is None else asdict(state))


class SurfaceLifecycleProjector:
    """Project one canonical lifecycle event under a fenced SQLite claim."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        surface: DiscordProjectSurface,
        port: DiscordProjectPort,
        worker_id: str,
        lease_seconds: int = 30,
        lease_heartbeat_seconds: float | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if not isinstance(conn, sqlite3.Connection):
            raise TypeError("conn must be a sqlite3.Connection")
        if not isinstance(surface, DiscordProjectSurface):
            raise TypeError("surface must be a DiscordProjectSurface")
        if type(worker_id) is not str or not worker_id:
            raise ValueError("worker_id must be non-empty")
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._conn = conn
        self._surface = surface
        self._port = port
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._lease_heartbeat_seconds = (
            max(0.1, lease_seconds / 3)
            if lease_heartbeat_seconds is None
            else lease_heartbeat_seconds
        )
        if self._lease_heartbeat_seconds <= 0:
            raise ValueError("lease_heartbeat_seconds must be positive")
        self._clock = clock or (lambda: int(time.time()))

    def _now(self) -> int:
        now = self._clock()
        if type(now) is not int or now < 0:
            raise RuntimeError("surface projector clock returned invalid time")
        return now

    def _event(self, event_id: str) -> ProjectEvent | None:
        row = self._conn.execute(
            """
            SELECT project_id, sequence
            FROM project_events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        events = ProjectEventOutbox(self._conn).events_after(
            row["project_id"],
            row["sequence"] - 1,
            1,
        )
        if len(events) != 1 or events[0].event_id != event_id:
            raise RuntimeError("canonical lifecycle event lookup drifted")
        return events[0]

    def _managed_channel_id(self, project_id: str) -> str | None:
        bindings = tuple(
            binding
            for binding in runtime_db.bindings_for_project(
                self._conn,
                project_id=project_id,
            )
            if (
                binding.surface == "discord"
                and binding.binding_id.startswith(_MANAGED_BINDING_PREFIX)
            )
        )
        if len(bindings) > 1:
            raise RuntimeError("project has multiple managed Discord bindings")
        return bindings[0].external_binding_id if bindings else None

    def _snapshot(
        self,
        event: ProjectEvent,
    ) -> ProjectLifecycleSnapshot:
        project = projects_db.get_project(self._conn, event.project_id)
        surface = event.payload.get("surface")
        if project is None:
            raise RuntimeError("lifecycle event project is not managed")
        if not isinstance(surface, Mapping):
            raise RuntimeError("lifecycle event lacks immutable surface state")
        try:
            return ProjectLifecycleSnapshot(
                project_id=event.project_id,
                name=surface["name"],
                lifecycle=surface["lifecycle"],
                channel_id=self._managed_channel_id(event.project_id),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "lifecycle event has invalid immutable surface state"
            ) from exc

    def _prepare(
        self,
        event: ProjectEvent,
    ) -> tuple[surface_ops.SurfaceOperation, ProjectChannelSpec] | None:
        existing = surface_ops.operation_for_lifecycle_event(
            self._conn,
            project_id=event.project_id,
            lifecycle_event_id=event.event_id,
        )
        if existing is not None:
            return existing, _spec_from_json(existing.desired_json)
        spec = project_channel_spec_for_lifecycle_event(
            event,
            self._snapshot(event),
            self._surface,
        )
        if spec is None:
            return None
        operation = surface_ops.prepare_or_replay(
            self._conn,
            operation_id=_operation_id(event),
            project_id=event.project_id,
            lifecycle_event_id=event.event_id,
            kind="discord.ensure_channel",
            desired_json=_json_object(asdict(spec)),
            prestate_json=_json_object(
                {"channel_id": spec.channel_id}
                if spec.channel_id is not None
                else {}
            ),
            ownership_marker=project_channel_marker(event.project_id),
        )
        return operation, spec

    async def project_event(
        self,
        event_id: str,
    ) -> surface_ops.SurfaceOperation | None:
        event = self._event(event_id)
        if event is None or event.kind not in _LIFECYCLE_EVENT_KINDS:
            return None
        existing = surface_ops.operation_for_lifecycle_event(
            self._conn,
            project_id=event.project_id,
            lifecycle_event_id=event.event_id,
        )
        if existing is not None and existing.status in {
            "synchronized",
            "blocked",
        }:
            return existing
        if (
            existing is not None
            and existing.kind == "discord.unprojectable_legacy_event"
        ):
            return self._block_legacy_surface_event(event, operation=existing)
        try:
            prepared = self._prepare(event)
        except RuntimeError as exc:
            if str(exc) not in {
                "lifecycle event lacks immutable surface state",
                "lifecycle event has invalid immutable surface state",
            }:
                raise
            return self._block_legacy_surface_event(event)
        if prepared is None:
            return None
        operation, desired = prepared
        if operation.status in {"synchronized", "blocked"}:
            return operation
        now = self._now()
        claim = surface_ops.claim_effect(
            self._conn,
            operation.operation_id,
            holder_id=self._worker_id,
            now=now,
            lease_seconds=self._lease_seconds,
        )
        if claim is None:
            current = surface_ops.operation_for_lifecycle_event(
                self._conn,
                project_id=event.project_id,
                lifecycle_event_id=event.event_id,
            )
            return current
        prior_status = operation.status
        operation = surface_ops.mark_effect_started(
            self._conn,
            operation.operation_id,
            claim=claim,
            now=now,
        )
        effective_spec = (
            replace(desired, channel_id=operation.external_channel_id)
            if operation.external_channel_id is not None
            and desired.channel_id is None
            else desired
        )
        claim_box = [claim]
        try:
            state = await self._with_lease_heartbeat(
                operation.operation_id,
                claim_box,
                self._readback_before_effect(
                effective_spec,
                recovering=prior_status in {"effect_started", "sync_pending"},
                ),
            )
            if state is None or not state_matches_spec(effective_spec, state):
                state = await self._with_lease_heartbeat(
                    operation.operation_id,
                    claim_box,
                    self._port.ensure_channel(
                    effective_spec,
                    operation_id=operation.operation_id,
                    ),
                )
            claim = claim_box[0]
            if not state_matches_spec(effective_spec, state):
                raise DiscordProjectPortError(
                    DiscordProjectErrorCode.STATE_MISMATCH,
                    retryable=True,
                    operation_id=operation.operation_id,
                )
        except DiscordProjectPortError as exc:
            claim = claim_box[0]
            try:
                readback = await self._with_lease_heartbeat(
                    operation.operation_id,
                    claim_box,
                    self._readback_after_failure(
                        effective_spec,
                        operation,
                    ),
                )
            except DiscordProjectPortError as readback_error:
                exc = readback_error
                readback = None
            claim = claim_box[0]
            if (
                readback is not None
                and state_matches_spec(effective_spec, readback)
            ):
                return self._record_exact_or_collision(
                    event,
                    operation,
                    claim,
                    readback,
                )
            return self._record_failure(
                event,
                operation,
                claim,
                effective_spec,
                exc,
                readback=readback,
            )
        return self._record_exact_or_collision(
            event,
            operation,
            claim,
            state,
        )

    async def _with_lease_heartbeat(
        self,
        operation_id: str,
        claim_box: list[surface_ops.SurfaceEffectClaim],
        awaitable,
    ):
        """Renew a fenced lease while one remote operation is in flight."""
        try:
            claim_box[0] = surface_ops.renew_effect_claim(
                self._conn,
                operation_id,
                claim=claim_box[0],
                now=self._now(),
                lease_seconds=self._lease_seconds,
            )
        except BaseException:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            else:
                pending = asyncio.ensure_future(awaitable)
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            raise
        remote = asyncio.create_task(awaitable)
        renewal_error: list[BaseException] = []

        async def renew() -> None:
            try:
                while not remote.done():
                    await asyncio.sleep(self._lease_heartbeat_seconds)
                    if remote.done():
                        return
                    claim_box[0] = surface_ops.renew_effect_claim(
                        self._conn,
                        operation_id,
                        claim=claim_box[0],
                        now=self._now(),
                        lease_seconds=self._lease_seconds,
                    )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                renewal_error.append(exc)
                remote.cancel()

        heartbeat = asyncio.create_task(renew())
        try:
            try:
                result = await remote
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    remote.cancel()
                    raise
                if renewal_error:
                    raise DiscordProjectPortError(
                        DiscordProjectErrorCode.AMBIGUOUS,
                        retryable=True,
                        operation_id=operation_id,
                    ) from renewal_error[0]
                raise
        finally:
            heartbeat.cancel()
            await asyncio.gather(
                remote,
                heartbeat,
                return_exceptions=True,
            )
        if renewal_error:
            raise DiscordProjectPortError(
                DiscordProjectErrorCode.AMBIGUOUS,
                retryable=True,
                operation_id=operation_id,
            ) from renewal_error[0]
        return result

    def _block_legacy_surface_event(
        self,
        event: ProjectEvent,
        *,
        operation: surface_ops.SurfaceOperation | None = None,
    ) -> surface_ops.SurfaceOperation:
        """Terminally record a pre-snapshot event without reconstructing it."""
        if operation is None:
            operation = surface_ops.operation_for_lifecycle_event(
                self._conn,
                project_id=event.project_id,
                lifecycle_event_id=event.event_id,
            )
        if operation is None:
            operation = surface_ops.prepare_or_replay(
                self._conn,
                operation_id=_operation_id(event),
                project_id=event.project_id,
                lifecycle_event_id=event.event_id,
                kind="discord.unprojectable_legacy_event",
                desired_json="{}",
                prestate_json="{}",
                ownership_marker=project_channel_marker(event.project_id),
            )
        if operation.status in {"synchronized", "blocked"}:
            return operation
        now = self._now()
        claim = surface_ops.claim_effect(
            self._conn,
            operation.operation_id,
            holder_id=self._worker_id,
            now=now,
            lease_seconds=self._lease_seconds,
        )
        if claim is None:
            current = surface_ops.operation_for_lifecycle_event(
                self._conn,
                project_id=event.project_id,
                lifecycle_event_id=event.event_id,
            )
            if current is None:
                raise RuntimeError("legacy surface operation disappeared")
            return current
        operation = surface_ops.mark_effect_started(
            self._conn,
            operation.operation_id,
            claim=claim,
            now=now,
        )
        with runtime_db.write_transaction(self._conn):
            blocked = surface_ops.reconcile(
                self._conn,
                operation.operation_id,
                claim=claim,
                now=now,
                readback_json=_json_object({"reason": "legacy_missing_surface_state"}),
                outcome="blocked",
                blocked_reason="legacy_missing_surface_state",
            )
            self._append_surface_event(
                event=event,
                operation=blocked,
                kind="surface.sync_blocked",
                now=now,
                retryable=False,
                reason=blocked.blocked_reason,
            )
        return blocked

    def _record_local_collision(
        self,
        event: ProjectEvent,
        operation: surface_ops.SurfaceOperation,
        claim: surface_ops.SurfaceEffectClaim,
        *,
        reason: str,
    ) -> surface_ops.SurfaceOperation:
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            blocked = surface_ops.reconcile(
                self._conn,
                operation.operation_id,
                claim=claim,
                now=now,
                readback_json=_json_object({"reason": reason}),
                outcome="blocked",
                blocked_reason=reason,
            )
            self._append_surface_event(
                event=event,
                operation=blocked,
                kind="surface.sync_blocked",
                now=now,
                retryable=False,
                reason=blocked.blocked_reason,
            )
        return blocked

    def _record_exact_or_collision(
        self,
        event: ProjectEvent,
        operation: surface_ops.SurfaceOperation,
        claim: surface_ops.SurfaceEffectClaim,
        state: ProjectChannelState,
    ) -> surface_ops.SurfaceOperation:
        try:
            return self._record_exact(event, operation, claim, state)
        except surface_ops.SurfaceChannelCollision:
            return self._record_local_collision(
                event,
                operation,
                claim,
                reason="local_channel_claim_collision",
            )
        except runtime_db.BindingConflictError:
            return self._record_local_collision(
                event,
                operation,
                claim,
                reason="local_binding_identity_collision",
            )

    async def _readback_before_effect(
        self,
        spec: ProjectChannelSpec,
        *,
        recovering: bool,
    ) -> ProjectChannelState | None:
        if not recovering or spec.channel_id is None:
            return None
        return await self._port.read_channel(
            guild_id=spec.guild_id,
            channel_id=spec.channel_id,
        )

    async def _readback_after_failure(
        self,
        spec: ProjectChannelSpec,
        operation: surface_ops.SurfaceOperation,
    ) -> ProjectChannelState | None:
        channel_id = spec.channel_id or operation.external_channel_id
        if channel_id is None:
            return None
        try:
            return await self._port.read_channel(
                guild_id=spec.guild_id,
                channel_id=channel_id,
            )
        except DiscordProjectPortError:
            return None

    def _pending_event_ids(self, limit: int) -> tuple[str, ...]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("surface projection limit must be 1..1000")
        placeholders = ",".join("?" for _ in _LIFECYCLE_EVENT_KINDS)
        rows = self._conn.execute(
            f"""
            WITH pending_events AS (
                SELECT
                    event.event_id,
                    event.project_id,
                    event.sequence,
                    event.created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY event.project_id
                        ORDER BY event.sequence, event.event_id
                    ) AS project_rank
                FROM project_events AS event
                LEFT JOIN project_surface_operations AS operation
                  ON operation.project_id = event.project_id
                 AND operation.lifecycle_event_id = event.event_id
                WHERE event.kind IN ({placeholders})
                  AND (
                        operation.operation_id IS NULL
                        OR operation.status IN (
                            'prepared', 'effect_started', 'sync_pending'
                        )
                  )
            )
            SELECT event_id
            FROM pending_events
            WHERE project_rank = 1
            ORDER BY created_at, project_id, sequence, event_id
            LIMIT ?
            """,
            (*tuple(sorted(_LIFECYCLE_EVENT_KINDS)), limit),
        ).fetchall()
        return tuple(row["event_id"] for row in rows)

    async def project_pending(self, limit: int = 100) -> int:
        """Attempt a bounded fair batch; leased projects do not block others."""
        projected = 0
        for event_id in self._pending_event_ids(limit):
            if await self.project_event(event_id) is not None:
                projected += 1
        return projected

    async def project_next(self) -> surface_ops.SurfaceOperation | None:
        """Project the oldest missing or nonterminal lifecycle operation."""
        event_ids = self._pending_event_ids(1)
        return (
            None
            if not event_ids
            else await self.project_event(event_ids[0])
        )

    def _append_surface_event(
        self,
        *,
        event: ProjectEvent,
        operation: surface_ops.SurfaceOperation,
        kind: str,
        now: int,
        retryable: bool,
        reason: str | None,
    ) -> None:
        terminal_event_id = _surface_event_id(operation.operation_id, kind)
        if self._conn.execute(
            "SELECT 1 FROM project_events WHERE event_id = ?",
            (terminal_event_id,),
        ).fetchone() is not None:
            return
        runtime_db._append_runtime_event(
            self._conn,
            event_id=terminal_event_id,
            project_id=event.project_id,
            kind=kind,
            turn_id=None,
            payload_json=_json_object(
                {
                    "lifecycle_event_id": event.event_id,
                    "operation_id": operation.operation_id,
                    "reason": reason,
                    "retryable": retryable,
                }
            ),
            created_at=now,
        )

    def _record_exact(
        self,
        event: ProjectEvent,
        operation: surface_ops.SurfaceOperation,
        claim: surface_ops.SurfaceEffectClaim,
        state: ProjectChannelState,
    ) -> surface_ops.SurfaceOperation:
        now = self._now()
        with runtime_db.write_transaction(self._conn):
            synchronized = surface_ops.reconcile(
                self._conn,
                operation.operation_id,
                claim=claim,
                now=now,
                readback_json=_state_json(state),
                external_channel_id=state.channel_id,
                outcome="exact",
            )
            binding_id = discord_surface_binding_id(
                event.project_id,
                self._surface.guild_id,
                state.channel_id,
            )
            runtime_db.bind_surface(
                self._conn,
                binding_id=binding_id,
                project_id=event.project_id,
                surface="discord",
                external_binding_id=state.channel_id,
                actor_id=_actor_id(self._surface),
                principal_id=self._surface.owner_user_id,
                now=now,
            )
            self._append_surface_event(
                event=event,
                operation=synchronized,
                kind="surface.synchronized",
                now=now,
                retryable=False,
                reason=None,
            )
        return synchronized

    def _record_failure(
        self,
        event: ProjectEvent,
        operation: surface_ops.SurfaceOperation,
        claim: surface_ops.SurfaceEffectClaim,
        spec: ProjectChannelSpec,
        error: DiscordProjectPortError,
        *,
        readback: ProjectChannelState | None,
    ) -> surface_ops.SurfaceOperation:
        now = self._now()
        owns_readback = (
            readback is not None
            and readback.ownership_marker
            == project_channel_marker(event.project_id)
        )
        foreign_readback = readback is not None and not owns_readback
        blocked = (
            foreign_readback
            or error.code in _BLOCKING_ERROR_CODES
            and not error.retryable
        )
        outcome = (
            "blocked"
            if blocked
            else "partial"
            if owns_readback
            else "ambiguous"
        )
        kind = "surface.sync_blocked" if blocked else "surface.sync_pending"
        reason = "foreign_marker" if foreign_readback else error.code.value
        with runtime_db.write_transaction(self._conn):
            result = surface_ops.reconcile(
                self._conn,
                operation.operation_id,
                claim=claim,
                now=now,
                readback_json=(
                    _state_json(readback)
                    if readback is not None
                    else _json_object({"code": error.code.value})
                ),
                external_channel_id=(
                    readback.channel_id
                    if owns_readback
                    else operation.external_channel_id
                ),
                outcome=outcome,
                blocked_reason=reason if blocked else None,
            )
            self._append_surface_event(
                event=event,
                operation=result,
                kind=kind,
                now=now,
                retryable=not blocked,
                reason=reason,
            )
        return result


__all__ = [
    "SurfaceLifecycleProjector",
    "discord_surface_binding_id",
]
