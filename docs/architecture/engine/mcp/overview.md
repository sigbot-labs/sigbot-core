# MCP exposure through mcpfather

[中文](overview_ZH.md) · [mcpfather](https://github.com/flowgent-labs/mcpfather)

Sigbot maintains its HTTP/A2A APIs. MCP servers are generated and deployed independently with mcpfather from Sigbot's HTTP OpenAPI document. This is the real Sigbot integration case for mcpfather; the Rust workspace no longer includes an MCP server or a sigbot mcp command.

The former implementation and its latest uncommitted Starter changes are preserved under .archived/20261109382032_builtin_mcp_server_replaced_by_mcpfather/, including protocol/authentication code, tests, documentation and integration snapshots. SHA256SUMS verifies the moved files. The archive follows the repository's existing .gitignore and is local unless deliberately versioned later.

## Generate the external server

1. Start Sigbot HTTP with sigbot api --config etc/sigbot.yaml.
2. Obtain its actual OpenAPI JSON through an authorized HTTP endpoint. The shipped YAML uses swagger.openapi_url=/swagger-ui/openapi.json; account for server.context_path and the Gateway route/policy. Save the document as sigbot.openapi.json. Do not maintain a second handwritten API schema.
3. Use the adjacent project's installed generator (build it there first if needed):

```sh
MCPFATHER_ROOT=/Users/agent/Documents/mcpfather
"${MCPFATHER_ROOT}/bin/mcpfather" \
  --input /path/to/sigbot.openapi.json \
  --output /path/to/generated/sigbot-mcp

make -C /path/to/generated/sigbot-mcp
export MCP__UPSTREAM__DEFAULT__ENDPOINT="${SIGBOT_HTTP_ORIGIN:?set the real Sigbot upstream origin}"
/path/to/generated/sigbot-mcp/bin/sigbot-mcp --transport http --port 8080

# Inspect the generated names before invoking a tool.
/path/to/generated/sigbot-mcp/bin/sigbot-mcp --transport cli list
```

The local generator's normal mode is used for this OpenAPI 3.1 case. Its optional --validation currently rejects the valid JSON Schema propertyNames keyword in ResourceCommand, so strict-validator compatibility is not claimed; the source schema is not rewritten to hide that keyword.

All 50 ten-domain CRUD operations have unique operationId values in the maintained HTTP schema. mcpfather owns generated tool names, MCP transport, OAuth discovery, inbound authentication and deployment. Keep generated source in its independent project, outside the Sigbot Rust workspace.

## Authentication and verification

Configure mcpfather's inbound OIDC and its separate outbound upstream credentials using that project's configuration. The AI client's token is not blindly forwarded upstream. Send upstream calls through the Authguard/Gateway policy for the appropriate tenant and service principal; retain environment/domain grants and W3C trace context. Never put privileged credentials in the OpenAPI document or loosen Sigbot tenant checks for the generated server.

The opt-in HTTP schema integration test exports the actual SwaggerHttpApi document to a temporary directory and invokes the local generator:

```sh
MCPFATHER_BIN=/Users/agent/Documents/mcpfather/bin/mcpfather \
  cargo test -p sigbot-http --test swagger mcpfather_generates_from_sigbot_openapi -- --ignored
```

This verifies schema/generator interoperability, not a live OIDC login or trading execution. Datafeed's outbound MCP client and external-source fixtures have independent purposes and remain active.
