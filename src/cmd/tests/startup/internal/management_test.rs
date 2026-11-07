use super::SigbotManagementStarter;
use common_context::ApplicationContext;
use sigbot_core::config::config::ConfigLoader;

#[tokio::test]
async fn disabled_listener_still_initializes_infrastructure_and_cancelled_wait_shuts_down_context() -> anyhow::Result<()>
{
    let directory = std::env::temp_dir().join(format!("sigbot-management-{}", sigbot_types::Uuid::now_v7()));
    std::fs::create_dir(&directory)?;
    let configuration = directory.join("sigbot.yaml");
    std::fs::write(&configuration, "mgmt:\n  enabled: false\n  otel:\n    enabled: false\n")?;
    ConfigLoader::initialize(&configuration)?;
    let management = SigbotManagementStarter;
    let handle = management.start().await;
    // Re-initializing an already active context succeeds without replacing it.
    ApplicationContext::global().init().await?;
    handle.abort();
    assert!(management.wait(handle).await.is_err());
    assert!(ApplicationContext::global().init().await.is_err());
    std::fs::remove_dir_all(directory)?;
    Ok(())
}
