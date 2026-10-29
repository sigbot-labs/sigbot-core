// SPDX-License-Identifier: GPL-3.0-only
use super::notification_runtime::NotificationRuntime;
use sigbot_messager::client::messager_factory::SigbotMessagerClientFactory;
use sigbot_types::modules::{decode_arg_config, notification::SigbotNotificationArgument};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

pub struct SigbotNotificationForwarder;
impl SigbotNotificationForwarder {
    fn runtime() -> &'static Mutex<Option<Arc<NotificationRuntime>>> {
        static RUNTIME: OnceLock<Mutex<Option<Arc<NotificationRuntime>>>> = OnceLock::new();
        RUNTIME.get_or_init(|| Mutex::new(None))
    }
    pub async fn new() -> Arc<Self> {
        Arc::new(Self)
    }
    pub async fn startup(matches: &clap::ArgMatches, _verbose: bool) {
        let encoded = matches
            .get_one::<String>("NOTIFICATION_CONFIGURATION")
            .expect("notification configuration required");
        let json = decode_arg_config(encoded).unwrap_or_else(|_| panic!("notification configuration encoding invalid"));
        let argument = SigbotNotificationArgument::from_json(&json)
            .unwrap_or_else(|_| panic!("notification configuration invalid"));
        let properties = argument
            .notification_config
            .properties
            .as_ref()
            .expect("notification runtime properties required");
        assert!(
            properties.get("runtime_v2").is_some_and(|value| value == "true"),
            "notification requires scoped runtime_v2 migration"
        );
        let runtime = NotificationRuntime::from_properties(properties)
            .await
            .unwrap_or_else(|_| panic!("notification runtime initialization failed"));
        let messager = SigbotMessagerClientFactory::init(matches, argument.messager_config)
            .await
            .expect("notification messager initialization failed");
        runtime
            .start(messager)
            .await
            .unwrap_or_else(|_| panic!("notification runtime startup failed"));
        *Self::runtime().lock().await = Some(runtime);
    }
    pub async fn shutdown() {
        if let Some(runtime) = Self::runtime().lock().await.take() {
            runtime.close().await;
        }
        SigbotMessagerClientFactory::shutdown().await;
    }
}
