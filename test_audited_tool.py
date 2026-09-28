"""Every tool call arriving over MCP writes an audit line; internal calls do not."""

import logging

import anyio

import main


@main.audited_tool()
async def audit_probe_tool() -> str:
    """A tool registered the way every tool in main.py is."""
    return "probed"


def _audit_lines(caplog):
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith("AUDIT ")]


def test_a_call_over_mcp_is_audited_with_the_caller_and_the_tool(caplog):
    caplog.set_level(logging.INFO, logger=main.logger.name)

    anyio.run(main.mcp.call_tool, "audit_probe_tool", {})

    # No auth context outside an HTTP request, which esme_mcp reports as "local".
    assert _audit_lines(caplog) == ["AUDIT user=local tool=audit_probe_tool"]


def test_a_direct_call_from_another_tool_is_not_audited(caplog):
    caplog.set_level(logging.INFO, logger=main.logger.name)

    result = anyio.run(audit_probe_tool)

    assert result == "probed"
    assert _audit_lines(caplog) == []


def test_every_superset_tool_is_registered():
    registered_names = {tool.name for tool in anyio.run(main.mcp.list_tools)}
    superset_names = {name for name in registered_names if name.startswith("superset_")}

    assert len(superset_names) == 67
