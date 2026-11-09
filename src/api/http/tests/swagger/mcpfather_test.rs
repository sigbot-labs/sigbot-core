//! Opt-in interoperability check against the real, independently maintained generator.
use sigbot_core::config::config::{AppConfig, AppConfigProperties};
use sigbot_http::swagger::SwaggerHttpApi;
use std::path::PathBuf;

struct McpFatherFixture {
    directory: PathBuf,
}
impl McpFatherFixture {
    fn new() -> anyhow::Result<Self> {
        let directory = std::env::temp_dir().join(format!("sigbot-mcpfather-{}", sigbot_types::Uuid::now_v7()));
        std::fs::create_dir(&directory)?;
        Ok(Self { directory })
    }
}
impl Drop for McpFatherFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
#[ignore = "requires MCPFATHER_BIN pointing to the actual mcpfather generator"]
fn mcpfather_generates_from_sigbot_openapi() -> anyhow::Result<()> {
    let generator = std::env::var("MCPFATHER_BIN")?;
    let fixture = McpFatherFixture::new()?;
    let schema = fixture.directory.join("sigbot.openapi.json");
    let document = SwaggerHttpApi::document(&AppConfig::new(&AppConfigProperties::default()));
    std::fs::write(&schema, serde_json::to_vec_pretty(&document)?)?;
    let output = fixture.directory.join("sigbot-mcp");
    let result = std::process::Command::new(generator)
        .arg("--input")
        .arg(&schema)
        .arg("--output")
        .arg(&output)
        .output()?;
    anyhow::ensure!(
        result.status.success(),
        "mcpfather rejected the actual Sigbot OpenAPI: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    anyhow::ensure!(output.join("go.mod").is_file(), "generator did not produce its module");
    anyhow::ensure!(
        output.join("Makefile").is_file(),
        "generator did not produce build entry point"
    );
    let registry = std::fs::read_to_string(output.join("pkg/mcptools/registry.go"))?;
    let tool_count = registry
        .lines()
        .filter(|line| line.contains("Handler:") && line.contains("MCPTool()"))
        .count();
    anyhow::ensure!(
        tool_count == 50,
        "expected all 50 Sigbot CRUD operations, generated {tool_count}"
    );
    Ok(())
}
