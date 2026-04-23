from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from apache_incubator_releases_mcp import tools

TOOLS = tools.TOOLS
JSONRPC_VERSION = "2.0"
JsonRpcResponse = dict[str, Any]
JsonRpcPayloadResponse = JsonRpcResponse | list[JsonRpcResponse]


def valid_message_id(value: Any) -> bool:
    return value is None or (isinstance(value, (str, int, float)) and not isinstance(value, bool))


def request_id(message: Any) -> Any:
    if isinstance(message, dict) and valid_message_id(message.get("id")):
        return message.get("id")
    return None


def jsonrpc_result(message_id: Any, result: Any) -> JsonRpcResponse:
    return {"jsonrpc": JSONRPC_VERSION, "id": message_id, "result": result}


def jsonrpc_error(
    message_id: Any,
    code: int,
    message: str,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": JSONRPC_VERSION, "id": request_id({"id": message_id}), "error": error}


def invalid_request(message_id: Any, message: str, field: str | None = None) -> JsonRpcResponse:
    data: dict[str, Any] = {"type": "invalid_request"}
    if field is not None:
        data["field"] = field
    return jsonrpc_error(message_id, -32600, message, data)


def invalid_params(message_id: Any, message: str, field: str | None = None) -> JsonRpcResponse:
    data: dict[str, Any] = {"type": "invalid_params"}
    if field is not None:
        data["field"] = field
    return jsonrpc_error(message_id, -32602, message, data)


def _json_text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)


def tool_response(payload: Any, is_error: bool = False) -> dict[str, Any]:
    if isinstance(payload, str):
        result: dict[str, Any] = {"content": [{"type": "text", "text": payload}]}
    else:
        result = {
            "content": [{"type": "text", "text": _json_text(payload)}],
            "structuredContent": payload,
        }
    if is_error:
        result["isError"] = True
    return result


def list_tools_payload() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "description": meta["description"],
            "inputSchema": meta["inputSchema"],
        }
        for name, meta in TOOLS.items()
    ]


def validate_tool_arguments(name: str, arguments: dict[str, Any]) -> str | None:
    schema = TOOLS[name]["inputSchema"]
    properties = schema.get("properties", {})
    required = schema.get("required", [])

    for key in required:
        if key not in arguments:
            return f"Missing required tool argument: {key}"

    if not schema.get("additionalProperties", True):
        unknown = sorted(set(arguments) - set(properties))
        if unknown:
            return "Unknown tool argument(s): " + ", ".join(unknown)

    return None


def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if name not in TOOLS:
        raise ValueError(f"Unknown tool: {name}")
    validation_error = validate_tool_arguments(name, arguments)
    if validation_error is not None:
        raise ValueError(validation_error)
    try:
        return tool_response(TOOLS[name]["handler"](**arguments))
    except Exception as exc:
        payload = {"ok": False, "error": str(exc), "tool": name}
        return tool_response(payload, is_error=True)


def handle_message(message: Any) -> JsonRpcResponse:
    if not isinstance(message, dict):
        return invalid_request(None, "JSON-RPC request must be an object")

    message_id = request_id(message)
    if "id" in message and not valid_message_id(message["id"]):
        return invalid_request(None, "Request id must be a string, number, or null", "id")

    if message.get("jsonrpc") != JSONRPC_VERSION:
        return invalid_request(message_id, "JSON-RPC version must be '2.0'", "jsonrpc")

    method = message.get("method")
    if not isinstance(method, str) or not method:
        return invalid_request(message_id, "Request method must be a non-empty string", "method")

    params = message.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        return invalid_params(message_id, "Request params must be an object", "params")

    if "id" not in message and method.startswith("notifications/"):
        return {}

    if method == "initialize":
        return jsonrpc_result(
            message_id,
            {
                "protocolVersion": params.get("protocolVersion", "2024-11-05"),
                "serverInfo": {"name": "apache-incubator-releases-mcp", "version": "0.1.0"},
                "capabilities": {"tools": {}},
            },
        )

    if method == "tools/list":
        return jsonrpc_result(message_id, {"tools": list_tools_payload()})

    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str):
            return invalid_params(message_id, "Tool name must be a string", "name")
        if not isinstance(arguments, dict):
            return invalid_params(message_id, "Tool arguments must be an object", "arguments")
        try:
            return jsonrpc_result(message_id, call_tool(name, arguments))
        except ValueError as exc:
            return invalid_params(message_id, str(exc), "arguments")

    return jsonrpc_error(
        message_id,
        -32601,
        f"Method '{method}' not found",
        {"type": "method_not_found", "method": method},
    )


def handle_payload(payload: Any) -> JsonRpcPayloadResponse:
    if isinstance(payload, list):
        if not payload:
            return invalid_request(None, "JSON-RPC batch must contain at least one request")
        responses = [response for item in payload if (response := handle_message(item))]
        return responses
    return handle_message(payload)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apache Incubator Releases MCP server")
    parser.add_argument("--dist-base", help="dist.apache.org base URL or local release directory")
    parser.add_argument("--archive-base", help="archive.apache.org base URL or local archive directory")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    tools.configure_defaults(
        dist_base=args.dist_base,
        archive_base=args.archive_base,
    )

    for line in sys.stdin:
        raw = line.strip()
        if not raw:
            continue
        response: JsonRpcPayloadResponse
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            response = jsonrpc_error(None, -32700, "Parse error", {"type": "parse_error"})
        else:
            response = handle_payload(payload)
        if response:
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()

    return 0
