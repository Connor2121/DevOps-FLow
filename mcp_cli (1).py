#!/usr/bin/env python3
"""
List and call tools on a local or deployed MCP server.

Usage:
  # List all tools on the local server
  python mcp_cli.py list

  # Show the full schema for one tool
  python mcp_cli.py describe get_recent_orders

  # Call a tool with JSON arguments
  python mcp_cli.py call health
  python mcp_cli.py call get_recent_orders '{"customer_id": 42, "days_back": 30}'

  # Interactive REPL: list tools, then call them by name
  python mcp_cli.py repl

  # Point at a different server (default is http://localhost:8000/mcp)
  python mcp_cli.py --url http://localhost:8000/mcp list
  python mcp_cli.py --url https://my-app.databricksapps.com/mcp --profile DEFAULT list
"""

import argparse
import json
import sys

from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient


DEFAULT_URL = "http://localhost:8000/mcp"


def _is_local(url: str) -> bool:
    lowered = url.lower()
    return (
        lowered.startswith("http://localhost")
        or lowered.startswith("http://127.")
        or lowered.startswith("http://0.0.0.0")
    )


def make_client(url: str, profile: str | None = None) -> DatabricksMCPClient:
    """Build a client.

    For localhost URLs, pass a workspace client with dummy credentials so that
    the Databricks SDK does not try to resolve default auth (which would fail
    if you have no Databricks config on this machine).

    For remote URLs, authenticate normally via the provided profile or the
    SDK's default auth chain (env vars, ~/.databrickscfg, OAuth, etc.).
    """
    if _is_local(url):
        ws = WorkspaceClient(
            host="http://localhost",
            token="not-used-for-local-mcp",
        )
        return DatabricksMCPClient(server_url=url, workspace_client=ws)

    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    return DatabricksMCPClient(server_url=url, workspace_client=ws)


def cmd_list(client: DatabricksMCPClient) -> None:
    tools = client.list_tools()
    if not tools:
        print("No tools registered.")
        return

    print(f"\n{len(tools)} tool(s) available:\n")
    for t in tools:
        first_line = (t.description or "").strip().split("\n")[0]
        print(f"  \u2022 {t.name}")
        if first_line:
            print(f"      {first_line}")
    print()


def cmd_describe(client: DatabricksMCPClient, name: str) -> None:
    tools = client.list_tools()
    match = next((t for t in tools if t.name == name), None)
    if match is None:
        print(f"Tool '{name}' not found. Available:")
        for t in tools:
            print(f"  - {t.name}")
        sys.exit(1)

    print(f"\n{match.name}")
    print("=" * len(match.name))
    if match.description:
        print(f"\n{match.description.strip()}\n")

    schema = getattr(match, "inputSchema", None) or {}
    props = schema.get("properties", {})
    required = set(schema.get("required", []))

    if props:
        print("Parameters:")
        for pname, pinfo in props.items():
            ptype = pinfo.get("type", "any")
            req = " (required)" if pname in required else " (optional)"
            pdesc = pinfo.get("description", "")
            default = pinfo.get("default")
            default_str = f" [default: {default}]" if default is not None else ""
            print(f"  - {pname}: {ptype}{req}{default_str}")
            if pdesc:
                print(f"      {pdesc}")
    else:
        print("Parameters: none")
    print()


def cmd_call(client: DatabricksMCPClient, name: str, args_json: str) -> None:
    try:
        args = json.loads(args_json) if args_json else {}
    except json.JSONDecodeError as e:
        print(f"Invalid JSON for arguments: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(args, dict):
        print("Arguments must be a JSON object, e.g. '{\"x\": 1}'", file=sys.stderr)
        sys.exit(1)

    print(f"\nCalling {name}({args}) ...\n")
    result = client.call_tool(name, args)

    # call_tool returns a CallToolResult with a .content list of blocks
    content = getattr(result, "content", None) or []
    if not content:
        print("(empty result)")
        print(f"\nraw: {result}")
        return

    for block in content:
        text = getattr(block, "text", None)
        if text is None:
            print(f"[non-text block: {block}]")
            continue
        # Try to pretty-print JSON-shaped text
        try:
            parsed = json.loads(text)
            print(json.dumps(parsed, indent=2, default=str))
        except (json.JSONDecodeError, TypeError):
            print(text)

    if getattr(result, "isError", False):
        print("\n\u26a0  Tool reported an error.")


def cmd_repl(client: DatabricksMCPClient) -> None:
    print("\nMCP REPL. Commands: list, describe <n>, call <n> <json>, quit\n")
    while True:
        try:
            line = input("mcp> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        if line in {"quit", "exit", "q"}:
            break

        parts = line.split(maxsplit=2)
        cmd = parts[0].lower()

        try:
            if cmd == "list":
                cmd_list(client)
            elif cmd == "describe" and len(parts) >= 2:
                cmd_describe(client, parts[1])
            elif cmd == "call" and len(parts) >= 2:
                args_json = parts[2] if len(parts) == 3 else "{}"
                cmd_call(client, parts[1], args_json)
            else:
                print("Usage: list | describe <n> | call <n> <json> | quit")
        except Exception as e:
            print(f"Error: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="List and call MCP tools.")
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"MCP server URL (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile (for remote servers only)",
    )

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="List all tools")

    p_desc = sub.add_parser("describe", help="Show a tool's schema")
    p_desc.add_argument("name")

    p_call = sub.add_parser("call", help="Call a tool")
    p_call.add_argument("name")
    p_call.add_argument(
        "args_json",
        nargs="?",
        default="{}",
        help="JSON object of arguments, e.g. '{\"customer_id\": 42}'",
    )

    sub.add_parser("repl", help="Interactive REPL")

    args = parser.parse_args()
    client = make_client(args.url, args.profile)

    if args.command == "list":
        cmd_list(client)
    elif args.command == "describe":
        cmd_describe(client, args.name)
    elif args.command == "call":
        cmd_call(client, args.name, args.args_json)
    elif args.command == "repl":
        cmd_repl(client)


if __name__ == "__main__":
    main()
