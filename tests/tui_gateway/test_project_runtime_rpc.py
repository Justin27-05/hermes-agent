"""Contract tests for the strict local ProjectCommand RPC boundary."""

from __future__ import annotations

import dataclasses
import itertools
import json
import sqlite3
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from hermes_cli import project_runtime_db as prdb
from hermes_cli import projects_db
from hermes_cli.project_command_service import ProjectCommandError, ProjectSnapshot
from hermes_cli.project_events import ProjectEventOutbox
from hermes_cli.project_policy import ActorContext
from hermes_cli.project_runtime import ProjectRuntime
from tui_gateway.project_runtime_rpc import (
    DesktopActorFactory,
    DesktopProjectRuntimeSnapshot,
    ProjectRuntimeReadService,
    ProjectRuntimeRpc,
    StrictJsonError,
    strict_json_loads,
)


class _CommandService:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[object] = []

    def execute(self, request: object) -> object:
        self.calls.append(request)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def _snapshot(*, artifact=None) -> ProjectSnapshot:
    return ProjectSnapshot(
        project_id="project-1",
        lifecycle="active",
        version=7,
        canonical_session_id="session-1",
        queue_depth=2,
        active_turn_id="turn-1",
        active_run_control="running",
        pending_approval_id=None,
        last_event_sequence=9,
        current_phase="execution",
        artifact=artifact,
        accepted_turn_id="turn-accepted",
        active_control_version=3,
    )


def _rpc(service: _CommandService) -> ProjectRuntimeRpc:
    return ProjectRuntimeRpc(
        service=service,
        actor_factory=DesktopActorFactory(
            actor_id="desktop-owner",
            binding_id="desktop-binding",
        ),
    )


def _command(*, name="turn.enqueue", project_id="project-1", payload=None,
             idempotency_key="command-1", expected_version=6) -> str:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "req-1",
            "method": "project.command",
            "params": {
                "name": name,
                "project_id": project_id,
                "payload": {} if payload is None else payload,
                "idempotency_key": idempotency_key,
                "expected_version": expected_version,
            },
        }
    )


def _read_request(method, params, *, request_id="read-1"):
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
    )


@pytest.fixture
def _desktop_cursor_env(tmp_path):
    conn = projects_db.connect(tmp_path / "projects.db")
    project_id = projects_db.create_project(
        conn,
        name="Desktop cursor",
        folders=(str(tmp_path),),
    )
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="session-root",
        current_phase="implementation",
        now=1,
    )
    for binding_id, surface in (
        ("desktop-owner", "desktop"),
        ("discord-owner", "discord"),
    ):
        prdb.bind_surface(
            conn,
            binding_id=binding_id,
            project_id=project_id,
            surface=surface,
            external_binding_id=f"{surface}-target",
            actor_id="owner-1",
            now=1,
        )
    outbox = ProjectEventOutbox(
        conn,
        clock=lambda: 10,
        id_factory=lambda kind: f"{kind}-unused",
    )
    outbox.append_event(
        project_id,
        "project.created",
        {"status": "active"},
        event_id="event-1",
    )
    outbox.append_event(
        project_id,
        "turn.queued",
        {"status": "queued"},
        event_id="event-2",
    )
    yield conn, project_id
    conn.close()


def _runtime_read_service(
    conn,
    *,
    transcript_loader=lambda _session_id: ((), 0),
):
    runtime = ProjectRuntime(conn, clock=lambda: 20)
    return ProjectRuntimeReadService(
        conn=conn,
        runtime=runtime,
        transcript_loader=transcript_loader,
        clock=lambda: 20,
    )


def _runtime_read_rpc(
    conn,
    *,
    transcript_loader=lambda _session_id: ((), 0),
    actor_id="owner-1",
    binding_id="desktop-owner",
):
    read_service = _runtime_read_service(
        conn,
        transcript_loader=transcript_loader,
    )
    return ProjectRuntimeRpc(
        service=_CommandService(_snapshot()),
        read_service=read_service,
        actor_factory=DesktopActorFactory(
            actor_id=actor_id,
            binding_id=binding_id,
        ),
    )


def test_desktop_read_cursor_is_additive_monotone_and_delivery_independent(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    actor = ActorContext("owner-1", "desktop", "desktop-owner", True)
    delivery_before = tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT * FROM project_deliveries
            WHERE project_id = ?
            ORDER BY delivery_id
            """,
            (project_id,),
        )
    )

    assert prdb.desktop_read_cursor(
        conn,
        project_id=project_id,
        binding_id="desktop-owner",
    ) == 0
    assert prdb.acknowledge_desktop_read_cursor(
        conn,
        project_id=project_id,
        binding_id="desktop-owner",
        cursor=2,
        actor=actor,
        now=20,
    ) == 2
    assert prdb.acknowledge_desktop_read_cursor(
        conn,
        project_id=project_id,
        binding_id="desktop-owner",
        cursor=1,
        actor=actor,
        now=21,
    ) == 2
    assert prdb.desktop_read_cursor(
        conn,
        project_id=project_id,
        binding_id="desktop-owner",
    ) == 2
    assert tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT * FROM project_deliveries
            WHERE project_id = ?
            ORDER BY delivery_id
            """,
            (project_id,),
        )
    ) == delivery_before


@pytest.mark.parametrize("cursor", [-1, True, 3])
def test_desktop_read_cursor_rejects_invalid_or_future_values(
    _desktop_cursor_env,
    cursor,
):
    conn, project_id = _desktop_cursor_env

    with pytest.raises(ValueError):
        prdb.acknowledge_desktop_read_cursor(
            conn,
            project_id=project_id,
            binding_id="desktop-owner",
            cursor=cursor,
            actor=ActorContext(
                "owner-1",
                "desktop",
                "desktop-owner",
                True,
            ),
            now=20,
        )


@pytest.mark.parametrize(
    "binding_id,actor",
    [
        (
            "desktop-owner",
            ActorContext("forged-owner", "desktop", "desktop-owner", True),
        ),
        (
            "desktop-owner",
            ActorContext("owner-1", "desktop", "discord-owner", True),
        ),
        (
            "discord-owner",
            ActorContext("owner-1", "discord", "discord-owner", True),
        ),
        (
            "desktop-owner",
            ActorContext("owner-1", "desktop", "desktop-owner", False),
        ),
    ],
)
def test_desktop_read_cursor_requires_the_exact_desktop_owner_binding(
    _desktop_cursor_env,
    binding_id,
    actor,
):
    conn, project_id = _desktop_cursor_env

    with pytest.raises(PermissionError):
        prdb.acknowledge_desktop_read_cursor(
            conn,
            project_id=project_id,
            binding_id=binding_id,
            cursor=1,
            actor=actor,
            now=20,
        )


def test_desktop_read_cursor_schema_rejects_non_desktop_future_and_regression(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env

    with pytest.raises(sqlite3.IntegrityError):
        with prdb.write_transaction(conn):
            conn.execute(
                """
                INSERT INTO project_desktop_read_cursors (
                    project_id, binding_id, cursor, updated_at
                ) VALUES (?, 'discord-owner', 0, 20)
                """,
                (project_id,),
            )
    with pytest.raises(sqlite3.IntegrityError):
        with prdb.write_transaction(conn):
            conn.execute(
                """
                INSERT INTO project_desktop_read_cursors (
                    project_id, binding_id, cursor, updated_at
                ) VALUES (?, 'desktop-owner', 3, 20)
                """,
                (project_id,),
            )
    assert prdb.acknowledge_desktop_read_cursor(
        conn,
        project_id=project_id,
        binding_id="desktop-owner",
        cursor=2,
        actor=ActorContext(
            "owner-1",
            "desktop",
            "desktop-owner",
            True,
        ),
        now=20,
    ) == 2
    with pytest.raises(sqlite3.IntegrityError):
        with prdb.write_transaction(conn):
            conn.execute(
                """
                UPDATE project_desktop_read_cursors
                SET cursor = 1
                WHERE project_id = ? AND binding_id = 'desktop-owner'
                """,
                (project_id,),
            )
    with pytest.raises(sqlite3.IntegrityError):
        with prdb.write_transaction(conn):
            conn.execute(
                """
                DELETE FROM project_desktop_read_cursors
                WHERE project_id = ? AND binding_id = 'desktop-owner'
                """,
                (project_id,),
            )


def test_runtime_snapshot_is_an_exact_sanitized_projection(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    state = prdb.runtime_state_for_project(conn, project_id)
    assert state is not None
    prdb.create_approval_request(
        conn,
        prdb.ApprovalRequest(
            approval_id="approval-1",
            project_id=project_id,
            requester_actor_id="owner-1",
            authorization_actor_id="owner-1",
            canonical_action="publish",
            approval_class="publish",
            command_revision=1,
            expected_runtime_version=state.version,
            expected_lifecycle=state.lifecycle,
            expected_phase=state.current_phase,
            targets=("C:/safe/release",),
            batch_id="batch-1",
            batch_items=("release",),
            status="pending",
            expires_at=100,
        ),
        now=20,
    )
    conn.execute(
        """
        INSERT INTO project_artifacts (
            artifact_id, project_id, turn_id, path, metadata_json,
            status, verified_at, created_at
        ) VALUES (?, ?, NULL, ?, ?, 'verified', 20, 20)
        """,
        (
            "artifact-1",
            project_id,
            "C:/private/build/release.zip",
            (
                '{"kind":"file","label":"must-not-win","nested":{'
                '"ArTiFaCt-PaTh":"C:/private/build/release.zip",'
                '"Credential.Bundle":"artifact-credential",'
                '"DELIVERY-Claim":"artifact-delivery",'
                '"External Binding ID":"artifact-external",'
                '"FENCING.Token":"artifact-fence",'
                '"Lease_Generation":99,'
                '"Provider_Payload":"artifact-provider",'
                '"critical_path":"artifact-domain-path",'
                '"safe_neighbor":"artifact-safe"},'
                '"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
                '"size":12,'
                '"token":"[REDACTED]"}'
            ),
        ),
    )
    conn.commit()
    runtime = ProjectRuntime(
        conn,
        clock=lambda: 20,
        id_factory=lambda kind: f"{kind}-queue",
    )
    runtime.enqueue_turn(
        project_id,
        {"message": "ship"},
        ActorContext("owner-1", "desktop", "desktop-owner", True),
        idempotency_key="enqueue-1",
        expected_version=state.version,
    )
    external_binding_id = "desktop-target"
    transcript_loader = lambda session_id: (
        (
            {
                "role": "user",
                "content": (
                    "ship token credential from C:/ordinary-user-text"
                ),
                "context": {
                    "safe_neighbor": "context-safe",
                    "critical_path": "context-domain-path",
                    "Api-Token": "context-token",
                    "LOCAL.Path": "C:/private/context",
                },
                "platform_message_id": "must-not-leak",
            },
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "text",
                        "text": "working",
                        "Provider-Payload": "assistant-provider",
                    }
                ],
                "reasoning": None,
                "reasoning_details": {
                    "safeNeighbor": "reasoning-safe",
                    "Lease.Generation": 7,
                    "FENCING-TOKEN": 8,
                },
            },
        ),
        2,
    )
    read_service = _runtime_read_service(
        conn,
        transcript_loader=transcript_loader,
    )
    projection = read_service.snapshot(
        project_id,
        ActorContext("owner-1", "desktop", "desktop-owner", True),
    )
    assert projection.last_sequence == 3
    response_text = ProjectRuntimeRpc(
        service=_CommandService(_snapshot()),
        read_service=read_service,
        actor_factory=DesktopActorFactory(
            actor_id="owner-1",
            binding_id="desktop-owner",
        ),
    ).handle_raw(
        _read_request(
            "project.runtime.snapshot",
            {"project_id": project_id},
        )
    )

    response = json.loads(response_text)
    assert response["ok"] is True
    assert response["result"] == {
        "project_id": project_id,
        "binding_id": "desktop-owner",
        "canonical_session_id": "session-root",
        "lifecycle": "active",
        "version": 1,
        "transcript_revision": 2,
        "current_phase": "implementation",
        "active_run": None,
        "delivery_status": {
            "state": "pending",
            "error_code": None,
        },
        "block": None,
        "last_sequence": 3,
        "queue": [
            {
                "turn_id": "turn-queue",
                "sequence": 1,
                "status": "queued",
            }
        ],
        "pending_approval": {
            "approval_id": "approval-1",
            "kind": "publish",
        },
        "transcript": [
            {
                "role": "user",
                "content": (
                    "ship token credential from C:/ordinary-user-text"
                ),
                "context": {
                    "safe_neighbor": "context-safe",
                    "critical_path": "context-domain-path",
                },
            },
            {
                "role": "assistant",
                "content": [{"type": "text", "text": "working"}],
                "reasoning": None,
                "reasoning_details": {
                    "safeNeighbor": "reasoning-safe",
                },
            },
        ],
        "artifacts": [
            {
                "artifact_id": "artifact-1",
                "presentation": {
                    "kind": "file",
                    "label": "release.zip",
                    "created_at": 20,
                    "size_bytes": 12,
                    "sha256": (
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    "open_target": None,
                },
            }
        ],
    }
    assert "C:/private/build/release.zip" not in response_text
    assert "artifact_path" not in response_text
    assert external_binding_id not in response_text
    assert "platform_message_id" not in response_text
    for forbidden_value in (
        "artifact-credential",
        "artifact-delivery",
        "artifact-external",
        "artifact-fence",
        "artifact-provider",
        "context-token",
        "C:/private/context",
        "assistant-provider",
    ):
        assert forbidden_value not in response_text


def test_runtime_snapshot_projects_safe_external_artifact_targets_only(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    conn.executemany(
        """
        INSERT INTO project_artifacts (
            artifact_id, project_id, turn_id, path, metadata_json,
            status, verified_at, created_at
        ) VALUES (?, ?, NULL, ?, ?, 'verified', 20, ?)
        """,
        (
            (
                "artifact-link-safe",
                project_id,
                "C:/private/gateway/safe.url",
                (
                    '{"kind":"link","url":'
                    '"https://example.com/releases/latest?view=summary'
                    '#%2561uthentication",'
                    '"size":1,"sha256":null}'
                ),
                21,
            ),
            (
                "artifact-link-credential",
                project_id,
                "C:/private/gateway/credential.url",
                (
                    '{"kind":"link","url":'
                    '"https://user:password@example.com/private",'
                    '"size":2,"sha256":null}'
                ),
                22,
            ),
            (
                "artifact-local",
                project_id,
                "C:/private/gateway/local.txt",
                (
                    '{"kind":"file","url":'
                    '"https://example.com/must-not-open",'
                    '"size":3,"sha256":null}'
                ),
                23,
            ),
            (
                "artifact-link-query-credential",
                project_id,
                "C:/private/gateway/signature.url",
                (
                    '{"kind":"link","url":'
                    '"https://example.com/private?signature=not-public",'
                    '"size":4,"sha256":null}'
                ),
                24,
            ),
            (
                "artifact-link-fragment-credential",
                project_id,
                "C:/private/gateway/token.url",
                (
                    '{"kind":"link","url":'
                    '"https://example.com/private'
                    '#access_token=not-public-fragment",'
                    '"size":5,"sha256":null}'
                ),
                25,
            ),
            (
                "artifact-link-loopback",
                project_id,
                "C:/private/gateway/preview.url",
                (
                    '{"kind":"link","url":'
                    '"http://127.0.0.1:3000/preview#authentication",'
                    '"size":6,"sha256":null}'
                ),
                26,
            ),
            (
                "artifact-link-public-ip",
                project_id,
                "C:/private/gateway/public-ip.url",
                (
                    '{"kind":"link","url":'
                    '"https://8.8.8.8/preview?view=summary'
                    '#authentication",'
                    '"size":7,"sha256":null}'
                ),
                27,
            ),
        ),
    )
    conn.commit()

    response_text = _runtime_read_rpc(conn).handle_raw(
        _read_request(
            "project.runtime.snapshot",
            {"project_id": project_id},
        )
    )
    response = json.loads(response_text)

    assert response["ok"] is True
    by_id = {
        artifact["artifact_id"]: artifact["presentation"]
        for artifact in response["result"]["artifacts"]
    }
    assert by_id["artifact-link-safe"] == {
        "kind": "link",
        "label": "safe.url",
        "created_at": 21,
        "size_bytes": 1,
        "sha256": None,
        "open_target": {
            "kind": "external_url",
            "href": (
                "https://example.com/releases/latest?view=summary"
                "#%2561uthentication"
            ),
        },
    }
    assert by_id["artifact-link-credential"]["open_target"] is None
    assert (
        by_id["artifact-link-query-credential"]["open_target"]
        is None
    )
    assert (
        by_id["artifact-link-fragment-credential"]["open_target"]
        is None
    )
    assert by_id["artifact-link-loopback"]["open_target"] is None
    assert by_id["artifact-link-public-ip"]["open_target"] == {
        "kind": "external_url",
        "href": (
            "https://8.8.8.8/preview?view=summary"
            "#authentication"
        ),
    }
    assert by_id["artifact-local"]["open_target"] is None
    assert "C:/private/gateway" not in response_text
    assert "user:password" not in response_text
    assert "not-public" not in response_text
    assert "not-public-fragment" not in response_text


@pytest.mark.parametrize(
    "target_url",
    [
        "http://localhost:3000/preview",
        "http://ui.localhost/preview",
        "http://LOCALHOST./preview",
        "http://local/preview",
        "http://printer.local/preview",
        "http://home.arpa/preview",
        "http://router.home.arpa/preview",
        "http://127。0。0。1/preview",
        "http://１２７．０．０．１/preview",
        "http://ⓛⓞⓒⓐⓛⓗⓞⓢⓣ/preview",
        "http://127.0.0.2/preview",
        "http://127.1/preview",
        "http://2130706433/preview",
        "http://0x7f000001/preview",
        "http://017700000001/preview",
        "http://0x7f.1/preview",
        "http://%31%32%37.0.0.1/preview",
        "http://[::1]/preview",
        "http://[::ffff:127.0.0.1]/preview",
        "http://10.0.0.1/preview",
        "http://0x0a000001/preview",
        "http://172.16.0.1/preview",
        "http://192.168.1.1/preview",
        "http://169.254.1.1/preview",
        "http://0.0.0.0/preview",
        "http://[fc00::1]/preview",
        "http://[fe80::1]/preview",
        "http://[::ffff:10.0.0.1]/preview",
        "http://224.0.0.1/preview",
        "http://[ff02::1]/preview",
        "http://[fec0::1]/preview",
        "http://100.64.0.1/preview",
    ],
)
def test_runtime_snapshot_rejects_non_external_artifact_targets(
    _desktop_cursor_env,
    target_url,
):
    conn, project_id = _desktop_cursor_env
    conn.execute(
        """
        INSERT INTO project_artifacts (
            artifact_id, project_id, turn_id, path, metadata_json,
            status, verified_at, created_at
        ) VALUES (
            'artifact-non-external', ?, NULL,
            'C:/private/gateway/non-external.url', ?,
            'verified', 20, 21
        )
        """,
        (
            project_id,
            json.dumps(
                {
                    "kind": "link",
                    "url": target_url,
                    "size": 1,
                    "sha256": None,
                },
                separators=(",", ":"),
            ),
        ),
    )
    conn.commit()

    response_text = _runtime_read_rpc(conn).handle_raw(
        _read_request(
            "project.runtime.snapshot",
            {"project_id": project_id},
        )
    )
    response = json.loads(response_text)

    assert response["ok"] is True
    artifact = next(
        item
        for item in response["result"]["artifacts"]
        if item["artifact_id"] == "artifact-non-external"
    )
    assert artifact["presentation"]["open_target"] is None
    assert target_url not in response_text


@pytest.mark.parametrize(
    "target_url",
    [
        "https://example.com/release#access_token%3Dsecret",
        "https://example.com/release#%61ccess_token%3Dsecret",
        (
            "https://example.com/release"
            "#%2561%2563%2563%2565%2573%2573"
            "%255F%2574%256F%256B%2565%256E%253Dx"
        ),
        "https://example.com/release?%61%75%74%68%3Dx",
        "https://example.com/release?%2561%2575%2574%2568%253Dx",
        "https://example.com/release#access_token%ZZsecret",
        "https://example.com/release?view=%ZZ",
        (
            "https://example.com/release"
            "#access_token%252525253Dx"
        ),
        "https://example.com/release#" + ("a" * 4097),
        "https://example.com/release?\x7fview=summary",
        "https://example.com/release?view=%C2%80",
        "https://example.com/release#section=%C2%80",
    ],
)
def test_runtime_snapshot_rejects_encoded_or_malformed_credential_targets(
    _desktop_cursor_env,
    target_url,
):
    conn, project_id = _desktop_cursor_env
    conn.execute(
        """
        INSERT INTO project_artifacts (
            artifact_id, project_id, turn_id, path, metadata_json,
            status, verified_at, created_at
        ) VALUES (
            'artifact-encoded-credential', ?, NULL,
            'C:/private/gateway/encoded.url', ?,
            'verified', 20, 21
        )
        """,
        (
            project_id,
            json.dumps(
                {
                    "kind": "link",
                    "url": target_url,
                    "size": 1,
                    "sha256": None,
                },
                separators=(",", ":"),
            ),
        ),
    )
    conn.commit()

    response_text = _runtime_read_rpc(conn).handle_raw(
        _read_request(
            "project.runtime.snapshot",
            {"project_id": project_id},
        )
    )
    response = json.loads(response_text)

    assert response["ok"] is True
    artifact = next(
        item
        for item in response["result"]["artifacts"]
        if item["artifact_id"] == "artifact-encoded-credential"
    )
    assert artifact["presentation"]["open_target"] is None
    assert target_url not in response_text


def test_runtime_snapshot_delivery_block_is_a_sanitized_aggregate(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    conn.execute(
        """
        UPDATE project_deliveries
        SET status = 'blocked', last_error_code = 'channel_forbidden',
            lease_expires_at = NULL, next_attempt_at = NULL
        WHERE project_id = ? AND binding_id = 'discord-owner'
        """,
        (project_id,),
    )
    conn.commit()

    response_text = _runtime_read_rpc(conn).handle_raw(
        _read_request(
            "project.runtime.snapshot",
            {"project_id": project_id},
        )
    )
    response = json.loads(response_text)

    assert response["ok"] is True
    assert response["result"]["delivery_status"] == {
        "state": "blocked",
        "error_code": "channel_forbidden",
    }
    assert response["result"]["block"] == {
        "kind": "delivery",
        "code": "channel_forbidden",
    }
    for forbidden in (
        "discord-owner",
        "discord-target",
        "delivery-",
    ):
        assert forbidden not in response_text


def test_runtime_snapshot_delivery_status_priority_and_desktop_exclusion(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env

    def delivery_status():
        response_text = _runtime_read_rpc(conn).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": project_id},
            )
        )
        response = json.loads(response_text)
        assert response["ok"] is True
        assert "desktop-target" not in response_text
        assert "discord-target" not in response_text
        assert "delivery-" not in response_text
        return response["result"]["delivery_status"]

    assert delivery_status() == {
        "state": "pending",
        "error_code": None,
    }

    conn.execute(
        """
        UPDATE project_deliveries
        SET status = 'delivered',
            cursor = (
                SELECT sequence FROM project_events AS event
                WHERE event.project_id = project_deliveries.project_id
                  AND event.event_id = project_deliveries.event_id
            ),
            lease_expires_at = NULL,
            remote_message_ids_json = NULL,
            next_attempt_at = NULL,
            last_error_code = NULL
        WHERE project_id = ? AND binding_id = 'discord-owner'
        """,
        (project_id,),
    )
    conn.commit()
    assert delivery_status() == {
        "state": "caught_up",
        "error_code": None,
    }

    conn.execute(
        """
        UPDATE project_deliveries
        SET status = 'pending', cursor = NULL,
            next_attempt_at = 40, last_error_code = 'retry_later'
        WHERE project_id = ? AND binding_id = 'discord-owner'
        """,
        (project_id,),
    )
    conn.commit()
    assert delivery_status() == {
        "state": "pending",
        "error_code": "retry_later",
    }

    conn.execute(
        """
        UPDATE project_deliveries
        SET status = 'in_flight', next_attempt_at = NULL,
            lease_expires_at = 50, last_error_code = NULL
        WHERE project_id = ? AND binding_id = 'discord-owner'
        """,
        (project_id,),
    )
    conn.commit()
    assert delivery_status() == {
        "state": "in_flight",
        "error_code": None,
    }

    delivery_ids = [
        row["delivery_id"]
        for row in conn.execute(
            """
            SELECT delivery_id FROM project_deliveries
            WHERE project_id = ? AND binding_id = 'discord-owner'
            ORDER BY delivery_id
            """,
            (project_id,),
        )
    ]
    conn.execute(
        """
        UPDATE project_deliveries
        SET status = 'blocked', lease_expires_at = NULL,
            last_error_code = 'channel_forbidden'
        WHERE delivery_id = ?
        """,
        (delivery_ids[0],),
    )
    conn.commit()
    assert delivery_status() == {
        "state": "blocked",
        "error_code": "channel_forbidden",
    }


def test_runtime_snapshot_without_discord_is_not_configured(
    _desktop_cursor_env,
):
    conn, _project_id = _desktop_cursor_env
    project_id = projects_db.create_project(
        conn,
        name="Desktop only delivery",
    )
    prdb.create_project_conversation(
        conn,
        project_id=project_id,
        conversation_id="session-desktop-only",
        current_phase="implementation",
        now=1,
    )
    prdb.bind_surface(
        conn,
        binding_id="desktop-only-owner",
        project_id=project_id,
        surface="desktop",
        external_binding_id="private-desktop-only-window",
        actor_id="owner-1",
        now=1,
    )
    ProjectEventOutbox(conn, clock=lambda: 10).append_event(
        project_id,
        "project.created",
        {"status": "active"},
        event_id="event-desktop-only",
    )

    response_text = _runtime_read_rpc(
        conn,
        binding_id="desktop-only-owner",
    ).handle_raw(
        _read_request(
            "project.runtime.snapshot",
            {"project_id": project_id},
        )
    )
    response = json.loads(response_text)

    assert response["ok"] is True
    assert response["result"]["delivery_status"] == {
        "state": "not_configured",
        "error_code": None,
    }
    assert response["result"]["block"] is None
    assert "private-desktop-only-window" not in response_text


def test_runtime_snapshot_projects_operation_block_without_raw_authority(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    conn.execute(
        """
        INSERT INTO project_operations (
            operation_id, project_id, turn_id, idempotency_key,
            approval_id, command_revision, targets_json, payload_json,
            status, receipt_json, created_at, updated_at,
            blocked_reason
        ) VALUES (
            'operation-private-id', ?, NULL, NULL, NULL, 1,
            '[]', '{}', 'blocked', NULL, 20, 20,
            'approval_denied'
        )
        """,
        (project_id,),
    )
    conn.commit()

    response_text = _runtime_read_rpc(conn).handle_raw(
        _read_request(
            "project.runtime.snapshot",
            {"project_id": project_id},
        )
    )
    response = json.loads(response_text)

    assert response["ok"] is True
    assert response["result"]["block"] == {
        "kind": "operation",
        "code": "approval_denied",
    }
    assert "operation-private-id" not in response_text


def test_runtime_snapshot_active_run_is_atomic_and_includes_control_version(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    ids = itertools.count(1)
    runtime = ProjectRuntime(
        conn,
        clock=lambda: 20,
        id_factory=lambda kind: f"{kind}-active-{next(ids)}",
    )
    actor = ActorContext(
        "owner-1",
        "desktop",
        "desktop-owner",
        True,
    )
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "run"},
        actor,
        idempotency_key="active-run-version",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id,
        "worker-active",
        lease_seconds=100,
    )
    assert claim is not None

    response = json.loads(
        _runtime_read_rpc(conn).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": project_id},
            )
        )
    )

    assert response["ok"] is True
    assert response["result"]["active_run"] == {
        "turn_id": turn.turn_id,
        "control_state": "running",
        "control_version": 1,
    }


def test_runtime_snapshot_rejects_incoherent_or_bool_active_run_fields():
    class ReadService:
        def snapshot(self, project_id, _actor):
            return DesktopProjectRuntimeSnapshot(
                project_id=project_id,
                binding_id="desktop-owner",
                canonical_session_id="session-root",
                lifecycle="active",
                version=1,
                transcript_revision=0,
                current_phase="implementation",
                active_run={
                    "turn_id": "turn-1",
                    "control_state": "running",
                    "control_version": True,
                },
                delivery_status={
                    "state": "not_configured",
                    "error_code": None,
                },
                block=None,
                last_sequence=1,
                queue=(),
                pending_approval=None,
                transcript=(),
                artifacts=(),
            )

        def events(self, *_args):
            raise AssertionError

        def acknowledge(self, *_args):
            raise AssertionError

    response = json.loads(
        ProjectRuntimeRpc(
            service=_CommandService(_snapshot()),
            read_service=ReadService(),
            actor_factory=DesktopActorFactory(
                actor_id="owner-1",
                binding_id="desktop-owner",
            ),
        ).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": "project-1"},
            )
        )
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "internal_error"},
    }


def test_runtime_events_are_bounded_exact_and_keep_nullable_turn_id(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env

    response = json.loads(
        _runtime_read_rpc(conn).handle_raw(
            _read_request(
                "project.runtime.events",
                {
                    "project_id": project_id,
                    "after_sequence": 0,
                    "limit": 100,
                },
            )
        )
    )

    assert response["ok"] is True
    assert response["result"] == {
        "project_id": project_id,
        "after_sequence": 0,
        "last_sequence": 2,
        "events": [
            {
                "event_id": "event-1",
                "project_id": project_id,
                "sequence": 1,
                "kind": "project.created",
                "turn_id": None,
                "payload": {"status": "active"},
                "created_at": "1970-01-01T00:00:10Z",
            },
            {
                "event_id": "event-2",
                "project_id": project_id,
                "sequence": 2,
                "kind": "turn.queued",
                "turn_id": None,
                "payload": {"status": "queued"},
                "created_at": "1970-01-01T00:00:10Z",
            },
        ],
    }


@pytest.mark.parametrize(
    "unsafe_number",
    [
        1 << 53,
        -(1 << 53),
        float(1 << 53),
        -float(1 << 53),
    ],
)
def test_runtime_snapshot_rejects_unsafe_integer_numbers_in_transcript(
    _desktop_cursor_env,
    unsafe_number,
):
    conn, project_id = _desktop_cursor_env
    transcript_loader = lambda _session_id: (
        (
            {
                "role": "assistant",
                "content": {
                    "unsafe_number": unsafe_number,
                },
            },
        ),
        1,
    )

    response = json.loads(
        _runtime_read_rpc(
            conn,
            transcript_loader=transcript_loader,
        ).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": project_id},
            )
        )
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "internal_error"},
    }


@pytest.mark.parametrize(
    "unsafe_number",
    [
        1 << 53,
        -(1 << 53),
        float(1 << 53),
        -float(1 << 53),
    ],
)
def test_runtime_events_reject_unsafe_integer_numbers_in_payload(
    _desktop_cursor_env,
    unsafe_number,
):
    conn, project_id = _desktop_cursor_env
    ProjectEventOutbox(conn, clock=lambda: 20).append_event(
        project_id,
        "projection.unsafe_number",
        {"unsafe_number": unsafe_number},
        event_id="event-unsafe-number",
    )

    response = json.loads(
        _runtime_read_rpc(conn).handle_raw(
            _read_request(
                "project.runtime.events",
                {
                    "project_id": project_id,
                    "after_sequence": 0,
                    "limit": 100,
                },
            )
        )
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "internal_error"},
    }


def test_runtime_public_json_keeps_bool_safe_integers_and_finite_floats(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    safe_max = (1 << 53) - 1
    public_numbers = {
        "enabled": True,
        "minimum": -safe_max,
        "maximum": safe_max,
        "ratio": 1.25,
    }
    ProjectEventOutbox(conn, clock=lambda: 20).append_event(
        project_id,
        "projection.public_numbers",
        public_numbers,
        event_id="event-public-numbers",
    )

    snapshot = json.loads(
        _runtime_read_rpc(
            conn,
            transcript_loader=lambda _session_id: (
                (
                    {
                        "role": "assistant",
                        "content": public_numbers,
                    },
                ),
                1,
            ),
        ).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": project_id},
            )
        )
    )
    events = json.loads(
        _runtime_read_rpc(conn).handle_raw(
            _read_request(
                "project.runtime.events",
                {
                    "project_id": project_id,
                    "after_sequence": 0,
                    "limit": 100,
                },
            )
        )
    )

    assert snapshot["result"]["transcript"][0]["content"] == public_numbers
    projected_event = next(
        event
        for event in events["result"]["events"]
        if event["kind"] == "projection.public_numbers"
    )
    assert projected_event["payload"] == public_numbers


def test_runtime_events_strip_authority_and_normalized_forbidden_keys(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    now = [20]
    counter = itertools.count(1)
    runtime = ProjectRuntime(
        conn,
        clock=lambda: now[0],
        id_factory=lambda kind: (
            f"{kind}-sanitization-{next(counter)}"
        ),
    )
    actor = ActorContext(
        "owner-1",
        "desktop",
        "desktop-owner",
        True,
    )
    turn = runtime.enqueue_turn(
        project_id,
        {"message": "recover"},
        actor,
        idempotency_key="sanitization-recovery",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id,
        "worker-private",
        lease_seconds=1,
    )
    assert claim is not None
    now[0] = claim.lease_expires_at

    class _NoReadback:
        def read_turn(self, _request):
            raise AssertionError(
                "not-started recovery must not call readback"
            )

    recovered = runtime.reconcile_inflight_turns(
        _NoReadback(),
        limit=10,
    )
    assert recovered[0].turn_id == turn.turn_id
    assert recovered[0].status == "queued"
    ProjectEventOutbox(conn, clock=lambda: now[0]).append_event(
        project_id,
        "projection.mixed",
        {
            "safeTop": "top-safe",
            "nested": {
                "Api-Token": "event-token",
                "Credential.Bundle": "event-credential",
                "DELIVERY-Claim": "event-delivery",
                "External Binding ID": "event-external",
                "FENCING.Token": "event-fence",
                "Lease Expires At": 123,
                "LeAsE_GeNeRaTiOn": 9,
                "LOCAL.Path": "C:/private/event",
                "Provider_Payload": "event-provider",
                "critical_path": "event-domain-path",
                "safeNeighbor": "nested-safe",
                "ordinary": (
                    "token credential C:/ordinary-event-text"
                ),
            },
        },
        event_id="event-mixed-sanitization",
    )

    response_text = _runtime_read_rpc(conn).handle_raw(
        _read_request(
            "project.runtime.events",
            {
                "project_id": project_id,
                "after_sequence": 0,
                "limit": 100,
            },
        )
    )
    response = json.loads(response_text)
    assert response["ok"] is True
    events = {
        event["kind"]: event
        for event in response["result"]["events"]
    }
    requeued = events["turn.requeued"]["payload"]
    assert requeued == {
        "attempt_id": claim.attempt_id,
        "source_status": "claimed",
        "turn_id": turn.turn_id,
        "version": requeued["version"],
        "attempt": {
            "project_id": project_id,
            "turn_id": turn.turn_id,
            "sequence": turn.sequence,
            "worker_id": "worker-private",
            "attempt_id": claim.attempt_id,
            "canonical_session_id": "session-root",
        },
    }
    assert events["projection.mixed"]["payload"] == {
        "safeTop": "top-safe",
        "nested": {
            "critical_path": "event-domain-path",
            "safeNeighbor": "nested-safe",
            "ordinary": "token credential C:/ordinary-event-text",
        },
    }
    for forbidden_value in (
        "event-token",
        "event-credential",
        "event-delivery",
        "event-external",
        "event-fence",
        "C:/private/event",
        "event-provider",
    ):
        assert forbidden_value not in response_text


def test_runtime_ack_is_exact_idempotent_and_monotone(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    rpc = _runtime_read_rpc(conn)

    first = json.loads(
        rpc.handle_raw(
            _read_request(
                "project.runtime.ack",
                {
                    "project_id": project_id,
                    "binding_id": "desktop-owner",
                    "cursor": 2,
                },
            )
        )
    )
    older = json.loads(
        rpc.handle_raw(
            _read_request(
                "project.runtime.ack",
                {
                    "project_id": project_id,
                    "binding_id": "desktop-owner",
                    "cursor": 1,
                },
            )
        )
    )

    expected = {
        "project_id": project_id,
        "binding_id": "desktop-owner",
        "cursor": 2,
    }
    assert first == {"id": "read-1", "ok": True, "result": expected}
    assert older == {"id": "read-1", "ok": True, "result": expected}


@pytest.mark.parametrize(
    "method,params",
    [
        (
            "project.runtime.snapshot",
            {"project_id": "project", "actor_id": "forged"},
        ),
        (
            "project.runtime.events",
            {
                "project_id": "project",
                "after_sequence": True,
                "limit": 100,
            },
        ),
        (
            "project.runtime.events",
            {
                "project_id": "project",
                "after_sequence": 1 << 53,
                "limit": 100,
            },
        ),
        (
            "project.runtime.events",
            {
                "project_id": "project",
                "after_sequence": 0,
                "limit": 0,
            },
        ),
        (
            "project.runtime.ack",
            {
                "project_id": "project",
                "binding_id": "binding",
                "cursor": 1.5,
            },
        ),
        (
            "project.runtime.ack",
            {
                "project_id": "project",
                "binding_id": "binding",
                "cursor": 1 << 53,
            },
        ),
    ],
)
def test_runtime_read_methods_require_exact_strict_params(method, params):
    rpc = ProjectRuntimeRpc(
        service=_CommandService(_snapshot()),
        read_service=None,
        actor_factory=DesktopActorFactory(
            actor_id="owner-1",
            binding_id="desktop-owner",
        ),
    )

    response = json.loads(
        rpc.handle_raw(_read_request(method, params))
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "invalid_request"},
    }


def test_runtime_read_rejects_future_and_cross_binding_cursors_safely(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    rpc = _runtime_read_rpc(conn)

    for params in (
        {
            "project_id": project_id,
            "binding_id": "desktop-owner",
            "cursor": 3,
        },
        {
            "project_id": project_id,
            "binding_id": "discord-owner",
            "cursor": 1,
        },
    ):
        response_text = rpc.handle_raw(
            _read_request("project.runtime.ack", params)
        )
        assert json.loads(response_text) == {
            "id": "read-1",
            "ok": False,
            "error": {"code": "PROJECT_RUNTIME_REJECTED"},
        }
        assert "desktop-target" not in response_text
        assert "discord-target" not in response_text


def test_runtime_ack_rejects_a_newly_ambiguous_desktop_owner_binding(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    rpc = _runtime_read_rpc(conn)
    prdb.bind_surface(
        conn,
        binding_id="desktop-owner-second",
        project_id=project_id,
        surface="desktop",
        external_binding_id="desktop-target-second",
        actor_id="owner-1",
        now=2,
    )

    response_text = rpc.handle_raw(
        _read_request(
            "project.runtime.ack",
            {
                "project_id": project_id,
                "binding_id": "desktop-owner",
                "cursor": 2,
            },
        )
    )

    assert json.loads(response_text) == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "PROJECT_RUNTIME_REJECTED"},
    }
    assert "desktop-target-second" not in response_text
    assert prdb.desktop_read_cursor(
        conn,
        project_id=project_id,
        binding_id="desktop-owner",
    ) == 0


def test_runtime_snapshot_rejects_pending_transcript_application(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    conn.execute(
        """
        UPDATE project_runtime_state
        SET transcript_pending_batch_id = ?
        WHERE project_id = ?
        """,
        (str(uuid.uuid4()), project_id),
    )
    conn.commit()
    loader_calls = []
    read_service = _runtime_read_service(
        conn,
        transcript_loader=lambda session_id: loader_calls.append(
            session_id
        ),
    )
    with pytest.raises(
        RuntimeError,
        match="PROJECT_RUNTIME_TRANSIENT",
    ):
        read_service.snapshot(
            project_id,
            ActorContext(
                "owner-1",
                "desktop",
                "desktop-owner",
                True,
            ),
        )

    response = json.loads(
        ProjectRuntimeRpc(
            service=_CommandService(_snapshot()),
            read_service=read_service,
            actor_factory=DesktopActorFactory(
                actor_id="owner-1",
                binding_id="desktop-owner",
            ),
        ).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": project_id},
            )
        )
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "PROJECT_RUNTIME_TRANSIENT"},
    }
    assert loader_calls == []


def test_runtime_ack_cannot_pass_pending_transcript_application(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    conn.execute(
        """
        UPDATE project_runtime_state
        SET transcript_pending_batch_id = ?
        WHERE project_id = ?
        """,
        (str(uuid.uuid4()), project_id),
    )
    conn.commit()

    response = json.loads(
        _runtime_read_rpc(conn).handle_raw(
            _read_request(
                "project.runtime.ack",
                {
                    "project_id": project_id,
                    "binding_id": "desktop-owner",
                    "cursor": 2,
                },
            )
        )
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "PROJECT_RUNTIME_TRANSIENT"},
    }
    assert prdb.desktop_read_cursor(
        conn,
        project_id=project_id,
        binding_id="desktop-owner",
    ) == 0


def test_runtime_snapshot_never_returns_an_ackable_unstable_cut(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    attempts = 0

    def racing_transcript_loader(_session_id):
        nonlocal attempts
        attempts += 1
        ProjectEventOutbox(conn, clock=lambda: 20).append_event(
            project_id,
            "transcript.changed",
            {"attempt": attempts},
            event_id=f"race-{attempts}",
        )
        return ({"role": "assistant", "content": "stale"},), 1

    response = json.loads(
        _runtime_read_rpc(
            conn,
            transcript_loader=racing_transcript_loader,
        ).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": project_id},
            )
        )
    )

    assert attempts == 3
    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "PROJECT_RUNTIME_TRANSIENT"},
    }
    assert prdb.desktop_read_cursor(
        conn,
        project_id=project_id,
        binding_id="desktop-owner",
    ) == 0


@pytest.mark.parametrize(
    "mutation",
    ("version", "control_version"),
)
def test_runtime_snapshot_retries_when_cas_or_control_identity_changes(
    _desktop_cursor_env,
    mutation,
):
    conn, project_id = _desktop_cursor_env
    ids = itertools.count(1)
    runtime = ProjectRuntime(
        conn,
        clock=lambda: 20,
        id_factory=lambda kind: (
            f"{kind}-{mutation}-{next(ids)}"
        ),
    )
    actor = ActorContext(
        "owner-1",
        "desktop",
        "desktop-owner",
        True,
    )
    runtime.enqueue_turn(
        project_id,
        {"message": mutation},
        actor,
        idempotency_key=f"enqueue-{mutation}",
        expected_version=0,
    )
    claim = runtime.claim_next_turn(
        project_id,
        f"worker-{mutation}",
        lease_seconds=100,
    )
    assert claim is not None

    def racing_transcript_loader(_session_id):
        if mutation == "version":
            conn.execute(
                """
                UPDATE project_runtime_state
                SET version = version + 1
                WHERE project_id = ?
                """,
                (project_id,),
            )
        else:
            conn.execute(
                """
                UPDATE project_run_controls
                SET control_version = control_version + 1
                WHERE project_id = ? AND turn_id = ?
                """,
                (project_id, claim.turn_id),
            )
        conn.commit()
        return (), 0

    response = json.loads(
        _runtime_read_rpc(
            conn,
            transcript_loader=racing_transcript_loader,
        ).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": project_id},
            )
        )
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "PROJECT_RUNTIME_TRANSIENT"},
    }


def test_runtime_snapshot_retries_when_transcript_revision_changes(
    _desktop_cursor_env,
):
    conn, project_id = _desktop_cursor_env
    revision = itertools.count(1)

    response = json.loads(
        _runtime_read_rpc(
            conn,
            transcript_loader=lambda _session_id: (
                (),
                next(revision),
            ),
        ).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": project_id},
            )
        )
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "PROJECT_RUNTIME_TRANSIENT"},
    }


def test_command_constructs_the_actor_only_from_the_injected_factory():
    service = _CommandService(_snapshot())

    response = json.loads(_rpc(service).handle_raw(_command(payload={"message": "go"})))

    assert response["ok"] is True
    assert response["result"] == {
        "accepted_turn_id": "turn-accepted",
        "active_control_version": 3,
        "active_run_control": "running",
        "active_turn_id": "turn-1",
        "artifact": None,
        "canonical_session_id": "session-1",
        "current_phase": "execution",
        "last_event_sequence": 9,
        "lifecycle": "active",
        "pending_approval_id": None,
        "project_id": "project-1",
        "queue_depth": 2,
        "version": 7,
    }
    assert set(response["result"]) == {
        "accepted_turn_id",
        "active_control_version",
        "active_run_control",
        "active_turn_id",
        "artifact",
        "canonical_session_id",
        "current_phase",
        "last_event_sequence",
        "lifecycle",
        "pending_approval_id",
        "project_id",
        "queue_depth",
        "version",
    }
    request = service.calls[0]
    assert request.name == "turn.enqueue"
    assert request.payload == {"message": "go"}
    assert request.actor.actor_id == "desktop-owner"
    assert request.actor.surface == "desktop"
    assert request.actor.binding_id == "desktop-binding"
    assert request.actor.is_owner is True


@pytest.mark.parametrize(
    (
        "active_turn_id",
        "active_run_control",
        "active_control_version",
    ),
    [
        (None, "running", 1),
        ("turn-1", None, 1),
        ("turn-1", "running", None),
        ("turn-1", "running", True),
    ],
)
def test_command_receipt_rejects_incoherent_active_control_projection(
    active_turn_id,
    active_run_control,
    active_control_version,
):
    snapshot = dataclasses.replace(
        _snapshot(),
        active_turn_id=active_turn_id,
        active_run_control=active_run_control,
        active_control_version=active_control_version,
    )

    response = json.loads(
        _rpc(_CommandService(snapshot)).handle_raw(
            _command(payload={"message": "go"})
        )
    )

    assert response == {
        "id": "req-1",
        "ok": False,
        "error": {"code": "internal_error"},
    }


def test_command_receipt_accepts_all_null_active_control_projection():
    snapshot = dataclasses.replace(
        _snapshot(),
        active_turn_id=None,
        active_run_control=None,
        active_control_version=None,
    )

    response = json.loads(
        _rpc(_CommandService(snapshot)).handle_raw(
            _command(payload={"message": "go"})
        )
    )

    assert response["ok"] is True
    assert response["result"]["active_turn_id"] is None
    assert response["result"]["active_run_control"] is None
    assert response["result"]["active_control_version"] is None


def test_command_receipt_rejects_integer_above_javascript_safe_max():
    snapshot = dataclasses.replace(
        _snapshot(),
        version=1 << 53,
    )

    response = json.loads(
        _rpc(_CommandService(snapshot)).handle_raw(
            _command(payload={"message": "go"})
        )
    )

    assert response == {
        "id": "req-1",
        "ok": False,
        "error": {"code": "internal_error"},
    }


def test_command_request_rejects_integer_above_javascript_safe_max():
    service = _CommandService(_snapshot())

    response = json.loads(
        _rpc(service).handle_raw(
            _command(
                payload={"message": "go"},
                expected_version=1 << 53,
            )
        )
    )

    assert response == {
        "id": "req-1",
        "ok": False,
        "error": {"code": "invalid_request"},
    }
    assert service.calls == []


def test_runtime_snapshot_rejects_integer_above_javascript_safe_max():
    class ReadService:
        def snapshot(self, project_id, _actor):
            return DesktopProjectRuntimeSnapshot(
                project_id=project_id,
                binding_id="desktop-owner",
                canonical_session_id="session-root",
                lifecycle="active",
                version=1 << 53,
                transcript_revision=0,
                current_phase="implementation",
                active_run=None,
                delivery_status={
                    "state": "not_configured",
                    "error_code": None,
                },
                block=None,
                last_sequence=0,
                queue=(),
                pending_approval=None,
                transcript=(),
                artifacts=(),
            )

        def events(self, *_args):
            raise AssertionError

        def acknowledge(self, *_args):
            raise AssertionError

    response = json.loads(
        ProjectRuntimeRpc(
            service=_CommandService(_snapshot()),
            read_service=ReadService(),
            actor_factory=DesktopActorFactory(
                actor_id="owner-1",
                binding_id="desktop-owner",
            ),
        ).handle_raw(
            _read_request(
                "project.runtime.snapshot",
                {"project_id": "project-1"},
            )
        )
    )

    assert response == {
        "id": "read-1",
        "ok": False,
        "error": {"code": "internal_error"},
    }


@pytest.mark.parametrize(
    "name,project_id",
    [
        ("project.create", None),
        ("project.rename", "project-1"),
        ("project.status", "project-1"),
        ("turn.enqueue", "project-1"),
        ("queue.status", "project-1"),
        ("run.stop", "project-1"),
        ("run.resume", "project-1"),
        ("approval.resolve", "project-1"),
        ("artifact.get", "project-1"),
        ("project.mark_technically_complete", "project-1"),
        ("project.accept_completion", "project-1"),
        ("project.reopen", "project-1"),
    ],
)
def test_every_canonical_command_reaches_the_shared_service(name, project_id):
    service = _CommandService(_snapshot())

    response = json.loads(
        _rpc(service).handle_raw(_command(name=name, project_id=project_id))
    )

    assert response["ok"] is True
    assert service.calls[0].name == name
    assert service.calls[0].project_id == project_id


@pytest.mark.parametrize(
    "raw",
    [
        '{"jsonrpc":"2.0","id":"req-2","id":"forged","method":"project.command","params":{}}',
        '{"jsonrpc":"2.0","id":"req-2","method":"project.command","params":{"name":"project.status","name":"turn.enqueue","project_id":"project-1","payload":{},"idempotency_key":null,"expected_version":null}}',
        '{"jsonrpc":"2.0","id":"req-2","method":"project.command","params":{"name":"project.status","project_id":"project-1","payload":{"a":1,"a":2},"idempotency_key":null,"expected_version":null}}',
    ],
)
def test_duplicate_keys_at_every_json_level_are_rejected_before_dispatch(raw):
    service = _CommandService(_snapshot())

    assert json.loads(_rpc(service).handle_raw(raw)) == {
        "id": None,
        "ok": False,
        "error": {"code": "invalid_request"},
    }
    assert service.calls == []


@pytest.mark.parametrize("raw", ['{"value":NaN}', '{"value":Infinity}', '{"value":-Infinity}'])
def test_strict_json_rejects_non_finite_constants(raw):
    with pytest.raises(StrictJsonError):
        strict_json_loads(raw)


def test_strict_json_rejects_excessive_nesting_without_recursion_escape():
    raw = "[" * 2000 + "0" + "]" * 2000

    with pytest.raises(StrictJsonError):
        strict_json_loads(raw)


def test_project_command_requires_exact_jsonrpc_2_envelope():
    service = _CommandService(_snapshot())
    missing_version = json.loads(_command())
    missing_version.pop("jsonrpc")
    wrong_version = json.loads(_command())
    wrong_version["jsonrpc"] = "1.0"

    for request in (missing_version, wrong_version):
        response = json.loads(
            _rpc(service).handle_raw(json.dumps(request))
        )
        assert response["error"] == {"code": "invalid_request"}
    assert service.calls == []


def test_exact_command_shape_rejects_client_actor_fields_and_invalid_create_id():
    service = _CommandService(_snapshot())
    forged = json.loads(_command())
    forged["params"]["actor"] = {"actor_id": "hermes", "surface": "system"}
    wrong_create = json.loads(_command(name="project.create", project_id="project-1"))

    assert json.loads(_rpc(service).handle_raw(json.dumps(forged)))["error"] == {
        "code": "invalid_request"
    }
    assert json.loads(_rpc(service).handle_raw(json.dumps(wrong_create)))["error"] == {
        "code": "invalid_request"
    }
    assert service.calls == []


def test_project_command_error_and_snapshot_serializers_do_not_leak_secrets():
    command_error = ProjectCommandError(
        "PROJECT_RUNTIME_REJECTED",
        "private_key=not-for-client",
        "project-1",
        4,
        6,
    )
    error_response = _rpc(_CommandService(command_error)).handle_raw(_command())
    snapshot_response = _rpc(
        _CommandService(
            _snapshot(
                artifact={
                    "artifact_id": "a-1",
                    "status": "verified",
                    "path": "C:/secret/report.pem",
                    "private_key": "not-for-client",
                    "created_at": 20,
                    "metadata": {
                        "kind": "link",
                        "size": 42,
                        "sha256": (
                            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        ),
                        "url": "https://example.com/public?view=1",
                    },
                }
            )
        )
    ).handle_raw(_command(name="artifact.get", idempotency_key=None, expected_version=None))

    assert json.loads(error_response) == {
        "id": "req-1",
        "ok": False,
        "error": {
            "code": "PROJECT_RUNTIME_REJECTED",
            "current_control_version": 6,
            "current_version": 4,
            "project_id": "project-1",
        },
    }
    artifact = json.loads(snapshot_response)["result"]["artifact"]
    assert artifact == {
        "artifact_id": "a-1",
        "presentation": {
            "kind": "link",
            "label": "report.pem",
            "created_at": 20,
            "size_bytes": 42,
            "sha256": (
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            "open_target": {
                "kind": "external_url",
                "href": "https://example.com/public?view=1",
            },
        },
    }
    assert "private_key" not in error_response
    assert "private_key" not in snapshot_response


@pytest.mark.parametrize(
    "target_url",
    [
        "http://224.0.0.1/preview",
        "http://[ff02::1]/preview",
        "http://[fec0::1]/preview",
        "http://100.64.0.1/preview",
        "http://local/preview",
        "http://printer.local/preview",
        "http://home.arpa/preview",
        "http://router.home.arpa/preview",
    ],
)
def test_artifact_get_rejects_non_external_targets(target_url):
    response_text = _rpc(
        _CommandService(
            _snapshot(
                artifact={
                    "artifact_id": "a-non-global",
                    "status": "verified",
                    "path": "C:/private/non-global.url",
                    "created_at": 20,
                    "metadata": {
                        "kind": "link",
                        "size": 1,
                        "sha256": None,
                        "url": target_url,
                    },
                }
            )
        )
    ).handle_raw(
        _command(
            name="artifact.get",
            idempotency_key=None,
            expected_version=None,
        )
    )

    artifact = json.loads(response_text)["result"]["artifact"]
    assert artifact["presentation"]["open_target"] is None
    assert target_url not in response_text


def test_unexpected_service_exception_is_a_generic_safe_error():
    raw_response = _rpc(
        _CommandService(RuntimeError("token=not-for-client"))
    ).handle_raw(_command())

    assert json.loads(raw_response) == {
        "id": "req-1",
        "ok": False,
        "error": {"code": "internal_error"},
    }
    assert "not-for-client" not in raw_response


def _server_create_managed_project(server):
    response = server._methods["project.command"](
        "create-server-runtime",
        {
            "name": "project.create",
            "project_id": None,
            "payload": {
                "name": f"Desktop runtime {uuid.uuid4().hex[:8]}",
                "current_phase": "implementation",
            },
            "idempotency_key": f"create-{uuid.uuid4().hex}",
            "expected_version": 0,
        },
    )
    assert "error" not in response, response.get("error")
    return response["result"]


def test_server_registers_real_runtime_read_factories_and_closes_connections(
    monkeypatch,
):
    import tui_gateway.server as server

    for method in (
        "project.runtime.snapshot",
        "project.runtime.events",
        "project.runtime.ack",
    ):
        assert method in server._methods
        assert method in server._LONG_HANDLERS
    created = _server_create_managed_project(server)
    project_id = created["project_id"]
    monkeypatch.setattr(
        server,
        "_db",
        SimpleNamespace(
            _project_history_snapshot=lambda session_id: ((), 0)
        ),
    )
    from hermes_cli import projects_db as pdb

    real_connect_closing = pdb.connect_closing
    opened = []

    @contextmanager
    def tracked_connection():
        with real_connect_closing() as conn:
            opened.append(conn)
            yield conn

    monkeypatch.setattr(pdb, "connect_closing", tracked_connection)
    snapshot = server._methods["project.runtime.snapshot"](
        "snapshot-server-runtime",
        {"project_id": project_id},
    )

    assert "error" not in snapshot, snapshot.get("error")
    assert set(snapshot["result"]) == {
        "project_id",
        "binding_id",
        "canonical_session_id",
        "lifecycle",
        "version",
        "transcript_revision",
        "current_phase",
        "active_run",
        "delivery_status",
        "block",
        "last_sequence",
        "queue",
        "pending_approval",
        "transcript",
        "artifacts",
    }
    assert snapshot["result"]["project_id"] == project_id
    assert "window" not in json.dumps(snapshot)
    assert opened
    with pytest.raises(sqlite3.ProgrammingError):
        opened[-1].execute("SELECT 1")

    events = server._methods["project.runtime.events"](
        "events-server-runtime",
        {
            "project_id": project_id,
            "after_sequence": 0,
            "limit": 100,
        },
    )
    ack = server._methods["project.runtime.ack"](
        "ack-server-runtime",
        {
            "project_id": project_id,
            "binding_id": snapshot["result"]["binding_id"],
            "cursor": events["result"]["last_sequence"],
        },
    )
    forged = server._methods["project.runtime.snapshot"](
        "forged-server-runtime",
        {"project_id": project_id, "actor_id": "forged"},
    )

    assert "error" not in events, events.get("error")
    assert "error" not in ack, ack.get("error")
    assert ack["result"] == {
        "project_id": project_id,
        "binding_id": snapshot["result"]["binding_id"],
        "cursor": events["result"]["last_sequence"],
    }
    assert forged["error"]["data"] == {"code": "invalid_request"}


def test_server_runtime_ack_rejects_ambiguous_local_owner_bindings(
    monkeypatch,
):
    import tui_gateway.server as server

    created = _server_create_managed_project(server)
    project_id = created["project_id"]
    monkeypatch.setattr(
        server,
        "_db",
        SimpleNamespace(
            _project_history_snapshot=lambda session_id: ((), 0)
        ),
    )
    from hermes_cli import projects_db as pdb

    with pdb.connect_closing() as conn:
        binding = next(
            item
            for item in prdb.bindings_for_project(
                conn,
                project_id=project_id,
            )
            if item.surface == "desktop"
        )
        prdb.bind_surface(
            conn,
            binding_id=f"second-{uuid.uuid4().hex}",
            project_id=project_id,
            surface="desktop",
            external_binding_id=f"private-window-{uuid.uuid4().hex}",
            actor_id="local-owner",
            now=2,
        )

    for method, params in (
        (
            "project.runtime.snapshot",
            {"project_id": project_id},
        ),
        (
            "project.runtime.ack",
            {
                "project_id": project_id,
                "binding_id": binding.binding_id,
                "cursor": 0,
            },
        ),
    ):
        response = server._methods[method]("ambiguous-runtime", params)
        response_text = json.dumps(response)
        assert response["error"]["data"] == {
            "code": "PROJECT_RUNTIME_REJECTED"
        }
        assert "private-window-" not in response_text


def test_project_command_broadcasts_only_a_post_commit_minimal_hint(
    monkeypatch,
):
    import tui_gateway.server as server
    from hermes_cli import projects_db as pdb

    created = _server_create_managed_project(server)
    project_id = created["project_id"]
    observed = []

    def capture_committed_hint(event, payload):
        with pdb.connect_closing() as conn:
            highest = conn.execute(
                """
                SELECT COALESCE(MAX(sequence), 0)
                FROM project_events
                WHERE project_id = ?
                """,
                (payload["project_id"],),
            ).fetchone()[0]
        observed.append((event, payload, highest))

    monkeypatch.setattr(
        server,
        "_broadcast_global_event",
        capture_committed_hint,
    )
    response = server._methods["project.command"](
        "enqueue-server-runtime",
        {
            "name": "turn.enqueue",
            "project_id": project_id,
            "payload": {"message": "ship"},
            "idempotency_key": f"enqueue-{uuid.uuid4().hex}",
            "expected_version": 0,
        },
    )

    assert "error" not in response, response.get("error")
    assert observed == [
        (
            "project.event",
            {
                "project_id": project_id,
                "highest_sequence": response["result"][
                    "last_event_sequence"
                ],
            },
            response["result"]["last_event_sequence"],
        )
    ]
    assert set(observed[0][1]) == {
        "project_id",
        "highest_sequence",
    }
