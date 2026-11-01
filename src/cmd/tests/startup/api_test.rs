use super::*;

#[test]
fn api_command_parses_directly_without_a_second_start_level() {
    let matches = Command::new("sigbot")
        .subcommand(SigbotAPIServer::build())
        .try_get_matches_from(["sigbot", "api"])
        .unwrap();
    assert_eq!(matches.subcommand_name(), Some("api"));
    assert!(matches.subcommand_matches("api").unwrap().subcommand_name().is_none());
}

#[test]
fn obsolete_start_and_config_flags_are_rejected() {
    for args in [
        vec!["api", "start"],
        vec!["api", "--config", "sigbot.yaml"],
        vec!["api", "invalid"],
    ] {
        assert_eq!(
            SigbotAPIServer::build().try_get_matches_from(args).unwrap_err().kind(),
            clap::error::ErrorKind::UnknownArgument
        );
    }
}

#[test]
fn help_is_available_without_starting_the_server() {
    let error = SigbotAPIServer::build()
        .try_get_matches_from(["api", "--help"])
        .unwrap_err();
    assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
    assert!(error.to_string().contains("API Server"));
}

#[test]
fn api_is_registered_once_with_the_actual_command_dispatcher() {
    let registry = crate::startup::register_subcommand_handles();
    assert_eq!(registry.iter().filter(|(name, _)| *name == "api").count(), 1);
    for (name, (build, _)) in registry {
        assert_eq!(build().get_name(), *name);
    }
}
