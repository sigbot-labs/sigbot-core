use sigbot_cmd::cmd::mcp_server_starter::SigbotMCPServer;

#[test]
fn retained_mcp_flags_parse_for_explicit_migration_failure() {
    let matches = SigbotMCPServer::build()
        .try_get_matches_from([
            "mcp-server",
            "--bind-address",
            "127.0.0.1:19090",
            "--transport",
            "stdio",
            "--auth-enabled",
            "true",
            "--external-mcp-services",
            "services.yaml",
        ])
        .unwrap();
    assert_eq!(matches.get_one::<String>("TRANSPORT").unwrap(), "stdio");
}
