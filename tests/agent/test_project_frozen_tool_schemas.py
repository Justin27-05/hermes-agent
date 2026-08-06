"""Project-only frozen tool-schema construction contract."""

from types import MappingProxyType

import pytest


def test_project_frozen_tool_schemas_thaw_to_normal_agent_tool_structures():
    """Project construction consumes the frozen snapshot without mutation."""
    from agent import agent_init

    frozen_schemas = (
        MappingProxyType(
            {
                "type": "function",
                "function": MappingProxyType(
                    {
                        "name": "project_status",
                        "parameters": MappingProxyType(
                            {
                                "type": "object",
                                "required": ("project_id",),
                            }
                        ),
                    }
                ),
            }
        ),
    )

    schemas = agent_init._thaw_project_tool_schemas(frozen_schemas)

    assert schemas == [
        {
            "type": "function",
            "function": {
                "name": "project_status",
                "parameters": {
                    "type": "object",
                    "required": ["project_id"],
                },
            },
        }
    ]
    assert type(schemas) is list
    assert type(schemas[0]) is dict
    assert type(schemas[0]["function"]) is dict
    assert type(schemas[0]["function"]["parameters"]["required"]) is list
    assert type(frozen_schemas[0]) is MappingProxyType


def test_project_bedrock_uses_frozen_guardrail_config_without_live_read(
    monkeypatch,
):
    """A published project snapshot is the sole Bedrock guardrail source."""
    import hermes_cli.config as config_module
    from run_agent import AIAgent

    live_reads = []

    def forbidden_live_config_read():
        live_reads.append("load_config")
        raise AssertionError("project agent read live config")

    monkeypatch.setattr(
        config_module,
        "load_config",
        forbidden_live_config_read,
    )

    agent = AIAgent(
        model="anthropic.claude-test",
        provider="bedrock",
        api_mode="bedrock_converse",
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
        project_execution_gate=object(),
        project_tool_schemas=(),
        project_registry_generation=0,
        project_request_timeout=30.0,
        project_bedrock_guardrail_config=MappingProxyType(
            {
                "guardrailIdentifier": "frozen-guardrail",
                "guardrailVersion": "3",
                "streamProcessingMode": "async",
                "trace": "enabled",
            }
        ),
        provider_metadata_prewarm=False,
        external_memory_sync=False,
        memory_review=False,
        skill_review=False,
        plugin_lifecycle=False,
    )

    assert agent._bedrock_guardrail_config == {
        "guardrailIdentifier": "frozen-guardrail",
        "guardrailVersion": "3",
        "streamProcessingMode": "async",
        "trace": "enabled",
    }
    assert live_reads == []


def test_project_moa_route_is_rejected_before_its_live_config_facade(
    monkeypatch,
):
    """Project execution must not enter MoA's live per-turn route resolver."""
    import agent.moa_loop as moa_loop
    from run_agent import AIAgent

    def unexpected_moa_facade(*_args, **_kwargs):
        raise AssertionError("project agent entered the live MoA facade")

    monkeypatch.setattr(moa_loop, "build_moa_facade", unexpected_moa_facade)

    with pytest.raises(
        PermissionError,
        match="MoA runtime is unavailable for project execution",
    ):
        AIAgent(
            model="closed",
            provider="moa",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
            project_execution_gate=object(),
            project_tool_schemas=(),
            project_registry_generation=0,
            project_request_timeout=30.0,
            provider_metadata_prewarm=False,
            external_memory_sync=False,
            memory_review=False,
            skill_review=False,
            plugin_lifecycle=False,
        )
