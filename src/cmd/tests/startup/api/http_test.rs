#[test]
fn http_command_parses_directly_without_a_second_start_level() {
    let matches = crate::startup::SigbotCli::build()
        .try_get_matches_from(["sigbot", "api", "-c", "sigbot.yaml"])
        .unwrap();
    assert_eq!(matches.subcommand_name(), Some("api"));
    assert!(matches.subcommand_matches("api").unwrap().subcommand_name().is_none());
}

#[test]
fn obsolete_start_and_config_flags_are_rejected() {
    for args in [
        vec!["sigbot", "api", "-c", "sigbot.yaml", "start"],
        vec!["sigbot", "api", "-c", "sigbot.yaml", "invalid"],
    ] {
        assert_eq!(
            crate::startup::SigbotCli::build()
                .try_get_matches_from(args)
                .unwrap_err()
                .kind(),
            clap::error::ErrorKind::UnknownArgument
        );
    }
}

#[test]
fn help_is_available_without_starting_the_server() {
    let error = crate::startup::SigbotCli::build()
        .try_get_matches_from(["sigbot", "api", "--help"])
        .unwrap_err();
    assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
    assert!(error
        .to_string()
        .contains("Run Sigbot Platformization HTTP API for Operations."));
}

#[test]
fn api_is_registered_once_with_the_actual_command_dispatcher() {
    let registry = crate::startup::register_subcommand_handles();
    assert_eq!(registry.iter().filter(|(name, _)| *name == "api").count(), 1);
    for (name, (build, _)) in registry {
        assert_eq!(build().get_name(), *name);
    }
}

#[test]
fn official_starters_expose_the_expected_descriptions() {
    let expected = [
        ("api", "Run Sigbot Platformization HTTP API for Operations."),
        ("a2a", "Run Sigbot Platformization A2A API for Operations."),
        (
            "controller",
            "Run Sigbot Platformization Controller for Components Orchestration.",
        ),
        (
            "datafeed",
            "Run Sigbot Tenantization Datafeed Service for External Ingestion.",
        ),
        (
            "evaluator",
            "Run Sigbot Tenantization Evaluator Service for AI-powered Evaluation.",
        ),
        (
            "exporter",
            "Run Sigbot Tenantization Exporter Service for Data Exportation.",
        ),
        ("logger", "Run Sigbot Tenantization Logging Service for Trading Audit."),
        (
            "notifier",
            "Run Sigbot Tenantization Notifier Service for Message Notification.",
        ),
        (
            "order",
            "Run Sigbot Tenantization Order Service for Position Execution.",
        ),
        (
            "strategy",
            "Run Sigbot Tenantization Strategy Service for Code Execution.",
        ),
        (
            "wallet",
            "Run Sigbot Tenantization Wallet Service for Ledger Management.",
        ),
        (
            "backtest",
            "Run Sigbot Tenantization Backtest Service for Strategies Performance.",
        ),
    ];
    let registry = crate::startup::register_subcommand_handles();
    for (name, description) in expected {
        let (build, _) = registry
            .iter()
            .find_map(|(registered, handler)| (*registered == name).then_some(*handler))
            .expect("official starter must be registered");
        assert_eq!(
            build().get_about().map(ToString::to_string).as_deref(),
            Some(description)
        );
    }
}

#[test]
fn every_starter_requires_only_a_yaml_path() {
    let modules = [
        "api",
        "a2a",
        "controller",
        "datafeed",
        "evaluator",
        "exporter",
        "logger",
        "strategy",
        "backtest",
        "notifier",
        "order",
        "wallet",
    ];
    let registry = crate::startup::register_subcommand_handles();
    for module in modules {
        assert!(crate::startup::SigbotCli::build()
            .try_get_matches_from(["sigbot", module])
            .is_err());
        for flag in ["-c", "--config"] {
            let matches = crate::startup::SigbotCli::build()
                .try_get_matches_from(["sigbot", module, flag, "runtime.yaml"])
                .unwrap();
            assert_eq!(
                matches
                    .subcommand_matches(module)
                    .unwrap()
                    .get_one::<String>("config")
                    .unwrap(),
                "runtime.yaml"
            );
        }
        assert!(crate::startup::SigbotCli::build()
            .try_get_matches_from([
                "sigbot",
                module,
                "--config",
                "runtime.yaml",
                "--messager-provider",
                "MQTT",
            ])
            .is_err());
    }
    assert!(!registry.iter().any(|(name, _)| *name == "logservice"));
    assert!(!registry.iter().any(|(name, _)| *name == "mcp"));
    assert!(crate::startup::SigbotCli::build()
        .try_get_matches_from(["sigbot", "mcp", "-c", "runtime.yaml"])
        .is_err());
}
