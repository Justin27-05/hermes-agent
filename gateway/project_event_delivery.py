"""Crash-safe projection of canonical project events into Discord."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import hashlib
import inspect
from pathlib import Path
import re
import shlex
import sqlite3
import time
from types import MappingProxyType
from typing import Callable, Literal

from gateway.platforms.base import SendResult
from hermes_cli import project_runtime_db as runtime_db
from hermes_cli.project_events import (
    ProjectDeliveryClaim,
    ProjectDeliveryConflictError,
    ProjectEvent,
    ProjectEventOutbox,
)
from plugins.platforms.discord.project_channels import (
    DiscordProjectErrorCode,
    DiscordProjectPort,
    DiscordProjectPortError,
)


_PORT_ERROR_CODES = frozenset(code.value for code in DiscordProjectErrorCode)
_PERMANENT_ERROR_CODES = frozenset(
    {
        DiscordProjectErrorCode.CONFLICT.value,
        DiscordProjectErrorCode.FORBIDDEN.value,
        DiscordProjectErrorCode.INVALID_ARGUMENT.value,
        DiscordProjectErrorCode.NOT_FOUND.value,
        "approval_unverified",
        "artifact_unverified",
    }
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class _DeliveryLeaseLost(RuntimeError):
    pass


@dataclass(frozen=True)
class ProjectEventDeliveryResult:
    delivery_id: str
    event_id: str
    binding_id: str
    status: Literal["delivered", "suppressed", "deferred", "blocked"]


class ProjectEventDeliveryWorker:
    """Deliver one binding-local outbox head under a fenced lease."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        port: DiscordProjectPort,
        worker_id: str,
        lease_seconds: int = 30,
        heartbeat_interval_seconds: float | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if not isinstance(conn, sqlite3.Connection):
            raise TypeError("conn must be a sqlite3.Connection")
        if type(worker_id) is not str or not worker_id:
            raise ValueError("worker_id must be non-empty")
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        heartbeat = (
            max(0.1, lease_seconds / 3)
            if heartbeat_interval_seconds is None
            else heartbeat_interval_seconds
        )
        if heartbeat <= 0:
            raise ValueError(
                "heartbeat_interval_seconds must be positive"
            )
        if not all(
            callable(getattr(port, method, None))
            for method in ("find_event_message", "publish_event")
        ):
            raise TypeError("port must implement DiscordProjectPort")
        self._conn = conn
        self._port = port
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._heartbeat_interval_seconds = heartbeat
        self._clock = clock or (lambda: int(time.time()))
        self._outbox = ProjectEventOutbox(conn, clock=self._clock)

    @staticmethod
    def _nonce(claim: ProjectDeliveryClaim) -> str:
        return hashlib.sha256(
            (
                claim.delivery_id
                + "\0"
                + claim.event.event_id
            ).encode("utf-8")
        ).hexdigest()

    def _candidate_bindings(self) -> tuple[tuple[str, str], ...]:
        rows = self._conn.execute(
            """
            SELECT binding.project_id, binding.binding_id,
                   MIN(event.created_at) AS first_created_at,
                   MIN(event.sequence) AS first_sequence
            FROM project_surface_bindings AS binding
            JOIN project_deliveries AS delivery
              ON delivery.project_id = binding.project_id
             AND delivery.binding_id = binding.binding_id
            JOIN project_events AS event
              ON event.project_id = delivery.project_id
             AND event.event_id = delivery.event_id
            WHERE binding.surface = 'discord'
              AND delivery.status NOT IN ('delivered', 'suppressed')
            GROUP BY binding.project_id, binding.binding_id
            ORDER BY first_created_at, binding.project_id,
                     first_sequence, binding.binding_id
            """
        ).fetchall()
        return tuple(
            (row["project_id"], row["binding_id"])
            for row in rows
        )

    async def _with_heartbeat(
        self,
        claim_box: list[ProjectDeliveryClaim],
        awaitable,
    ):
        try:
            claim_box[0] = self._outbox.renew_delivery(
                claim_box[0],
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
                    await asyncio.sleep(
                        self._heartbeat_interval_seconds
                    )
                    if remote.done():
                        return
                    claim_box[0] = self._outbox.renew_delivery(
                        claim_box[0],
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
                    raise _DeliveryLeaseLost(
                        "delivery lease lost during remote I/O"
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
            raise _DeliveryLeaseLost(
                "delivery lease lost during remote I/O"
            ) from renewal_error[0]
        return result

    @staticmethod
    def _remote_message_ids(result: SendResult) -> tuple[str, ...]:
        raw = result.raw_response
        values = (
            raw.get("message_ids")
            if isinstance(raw, dict)
            else None
        )
        if values is None and result.message_id is not None:
            values = [
                result.message_id,
                *result.continuation_message_ids,
            ]
        if (
            type(values) is not list
            or not values
            or any(
                type(message_id) is not str or not message_id
                for message_id in values
            )
            or len(set(values)) != len(values)
        ):
            raise RuntimeError(
                "Discord delivery omitted its complete message group"
            )
        return tuple(values)

    @staticmethod
    def _failure_code(result: SendResult) -> str:
        if result.error in _PORT_ERROR_CODES:
            return result.error
        if result.error_kind in {
            "forbidden",
            "not_found",
            "rate_limited",
            "transient",
        }:
            return result.error_kind
        return "ambiguous"

    @staticmethod
    def _retry_delay(attempt: int) -> int:
        return min(300, 2 ** min(max(attempt - 1, 0), 8))

    def _is_origin_turn_queued(
        self,
        claim: ProjectDeliveryClaim,
    ) -> bool:
        if claim.event.kind != "turn.queued":
            return False
        row = self._conn.execute(
            """
            SELECT origin_binding_id
            FROM project_turns
            WHERE project_id = ? AND turn_id = ?
            """,
            (claim.project_id, claim.event.turn_id),
        ).fetchone()
        return (
            row is not None
            and row["origin_binding_id"] == claim.binding_id
        )

    def _delivery_event(
        self,
        claim: ProjectDeliveryClaim,
    ) -> tuple[ProjectEvent | None, str | None]:
        event = claim.event
        if event.kind == "artifact.verified":
            artifact_id = event.payload.get("artifact_id")
            if type(artifact_id) is not str or not artifact_id:
                return None, "artifact_unverified"
            artifact = self._outbox.artifact_for_id(
                claim.project_id,
                artifact_id,
            )
            if artifact is None or artifact.status != "verified":
                return None, "artifact_unverified"
            basename = Path(artifact.path).name
            sha256 = artifact.metadata.get("sha256")
            size = artifact.metadata.get("size")
            if (
                not basename
                or basename in {".", ".."}
                or len(basename) > 255
                or any(
                    character in {"/", "\\"}
                    or not character.isprintable()
                    for character in basename
                )
                or type(sha256) is not str
                or _SHA256.fullmatch(sha256) is None
                or type(size) is not int
                or size < 0
            ):
                return None, "artifact_unverified"
            return (
                replace(
                    event,
                    payload=MappingProxyType(
                        {
                            "artifact_id": artifact.artifact_id,
                            "basename": basename,
                            "sha256": sha256,
                            "size": size,
                            "status": "verified",
                        }
                    ),
                ),
                None,
            )
        if event.kind == "approval.requested":
            approval_id = event.payload.get("approval_id")
            if type(approval_id) is not str or not approval_id:
                return None, "approval_unverified"
            row = self._conn.execute(
                """
                SELECT approval_id, turn_id, status
                FROM project_approvals
                WHERE project_id = ? AND approval_id = ?
                  AND operation_id IS NULL
                """,
                (claim.project_id, approval_id),
            ).fetchone()
            payload_turn_id = event.payload.get("turn_id")
            if (
                row is None
                or row["status"]
                not in {"pending", "approved", "denied", "expired"}
                or (
                    payload_turn_id is not None
                    and payload_turn_id != row["turn_id"]
                )
                or (
                    event.turn_id is not None
                    and event.turn_id != row["turn_id"]
                )
                or any(
                    not character.isprintable()
                    for character in approval_id
                )
                or len(approval_id) > 255
            ):
                return None, "approval_unverified"
            safe_payload: dict[str, object] = {
                "approval_id": row["approval_id"],
                "status": row["status"],
            }
            if row["turn_id"] is not None:
                safe_payload["turn_id"] = row["turn_id"]
            if row["status"] == "pending":
                safe_payload["commands"] = {
                    choice: shlex.join(
                        ["/project", "approval", approval_id, choice]
                    )
                    for choice in ("approve", "deny")
                }
            return (
                replace(
                    event,
                    payload=MappingProxyType(safe_payload),
                ),
                None,
            )
        return event, None

    @staticmethod
    def _exception_code(exc: Exception) -> str:
        if isinstance(exc, DiscordProjectPortError):
            return exc.code.value
        return DiscordProjectErrorCode.AMBIGUOUS.value

    def _finish_failure(
        self,
        claim: ProjectDeliveryClaim,
        *,
        error_code: str,
    ) -> ProjectEventDeliveryResult:
        if error_code in _PERMANENT_ERROR_CODES:
            self._outbox.block_delivery(
                claim,
                error_code=error_code,
            )
            status = "blocked"
        else:
            self._outbox.defer_delivery(
                claim,
                error_code=error_code,
                delay_seconds=self._retry_delay(claim.attempt),
            )
            status = "deferred"
        return ProjectEventDeliveryResult(
            delivery_id=claim.delivery_id,
            event_id=claim.event.event_id,
            binding_id=claim.binding_id,
            status=status,
        )

    async def _publish(
        self,
        claim_box: list[ProjectDeliveryClaim],
        *,
        channel_id: str,
        event: ProjectEvent,
        nonce: str,
    ) -> SendResult:
        async def before_segment() -> None:
            try:
                claim_box[0] = self._outbox.renew_delivery(
                    claim_box[0],
                    lease_seconds=self._lease_seconds,
                )
            except ProjectDeliveryConflictError as exc:
                raise _DeliveryLeaseLost(
                    "delivery lease lost before remote segment"
                ) from exc

        result = await self._with_heartbeat(
            claim_box,
            self._port.publish_event(
                channel_id=channel_id,
                event=event,
                nonce=nonce,
                before_segment=before_segment,
            ),
        )
        if not isinstance(result, SendResult):
            raise RuntimeError("Discord event delivery failed")
        return result

    async def _read_marker_before_publish(
        self,
        claim_box: list[ProjectDeliveryClaim],
        *,
        channel_id: str,
    ) -> None:
        async def before_segment() -> None:
            try:
                claim_box[0] = self._outbox.renew_delivery(
                    claim_box[0],
                    lease_seconds=self._lease_seconds,
                )
            except ProjectDeliveryConflictError as exc:
                raise _DeliveryLeaseLost(
                    "delivery lease lost before remote segment"
                ) from exc

        try:
            await self._with_heartbeat(
                claim_box,
                self._port.find_event_message(
                    channel_id=channel_id,
                    event_id=claim_box[0].event.event_id,
                    before_segment=before_segment,
                ),
            )
        except DiscordProjectPortError as exc:
            if (
                exc.code
                is DiscordProjectErrorCode.PARTIAL_DELIVERY
            ):
                return
            raise

    def _claim_binding(
        self,
        project_id: str,
        binding_id: str,
    ) -> ProjectDeliveryClaim | None:
        return self._outbox.claim_delivery(
            project_id,
            binding_id,
            lease_seconds=self._lease_seconds,
        )

    async def deliver_pending(
        self,
        *,
        limit: int = 25,
    ) -> tuple[ProjectEventDeliveryResult, ...]:
        if type(limit) is not int or limit <= 0:
            raise ValueError(
                "delivery batch limit must be a positive integer"
            )
        candidates = self._candidate_bindings()
        results: list[ProjectEventDeliveryResult] = []
        for project_id, binding_id in candidates:
            if len(results) >= limit:
                break
            claim = self._claim_binding(project_id, binding_id)
            if claim is None:
                continue
            results.append(await self._deliver_claim(claim))
        return tuple(results)

    async def deliver_next(
        self,
    ) -> ProjectEventDeliveryResult | None:
        for project_id, binding_id in self._candidate_bindings():
            claim = self._claim_binding(project_id, binding_id)
            if claim is not None:
                return await self._deliver_claim(claim)
        return None

    async def _deliver_claim(
        self,
        claim: ProjectDeliveryClaim,
    ) -> ProjectEventDeliveryResult:
        if self._is_origin_turn_queued(claim):
            self._outbox.suppress_origin_delivery(claim)
            return ProjectEventDeliveryResult(
                delivery_id=claim.delivery_id,
                event_id=claim.event.event_id,
                binding_id=claim.binding_id,
                status="suppressed",
            )
        delivery_event, preparation_error = self._delivery_event(claim)
        if delivery_event is None:
            return self._finish_failure(
                claim,
                error_code=preparation_error or "ambiguous",
            )
        binding = runtime_db.binding_for_id(
            self._conn,
            project_id=claim.project_id,
            binding_id=claim.binding_id,
        )
        if not (
            binding is not None
            and binding.surface == "discord"
            and type(binding.external_binding_id) is str
            and binding.external_binding_id
        ):
            raise RuntimeError("delivery binding is not exact Discord state")
        claim_box = [claim]
        channel_id = binding.external_binding_id
        nonce = self._nonce(claim)
        try:
            await self._read_marker_before_publish(
                claim_box,
                channel_id=channel_id,
            )
        except asyncio.CancelledError:
            raise
        except _DeliveryLeaseLost:
            raise
        except Exception as exc:
            return self._finish_failure(
                claim_box[0],
                error_code=self._exception_code(exc),
            )
        try:
            result = await self._publish(
                claim_box,
                channel_id=binding.external_binding_id,
                event=delivery_event,
                nonce=nonce,
            )
        except asyncio.CancelledError:
            raise
        except _DeliveryLeaseLost:
            raise
        except Exception as exc:
            error_code = self._exception_code(exc)
            if error_code in _PERMANENT_ERROR_CODES:
                return self._finish_failure(
                    claim_box[0],
                    error_code=error_code,
                )
            try:
                await self._read_marker_before_publish(
                    claim_box,
                    channel_id=channel_id,
                )
                result = await self._publish(
                    claim_box,
                    channel_id=channel_id,
                    event=delivery_event,
                    nonce=nonce,
                )
            except asyncio.CancelledError:
                raise
            except _DeliveryLeaseLost:
                raise
            except Exception as recovery_exc:
                return self._finish_failure(
                    claim_box[0],
                    error_code=self._exception_code(recovery_exc),
                )
        claim = claim_box[0]
        if not result.success:
            return self._finish_failure(
                claim,
                error_code=self._failure_code(result),
            )
        self._outbox.complete_delivery(
            claim,
            remote_message_ids=self._remote_message_ids(result),
        )
        return ProjectEventDeliveryResult(
            delivery_id=claim.delivery_id,
            event_id=claim.event.event_id,
            binding_id=claim.binding_id,
            status="delivered",
        )


__all__ = [
    "ProjectEventDeliveryResult",
    "ProjectEventDeliveryWorker",
]
