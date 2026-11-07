use serde_json::json;
#[cfg(target_os = "linux")]
use serde_json::Value;
use std::io::Write;
use std::process::Command;
use std::process::Stdio;

struct WorkerFixture;
impl WorkerFixture {
    fn run(code: &str) -> std::process::Output {
        let cwd = tempfile::tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_sigbot-strategy-worker"))
            .env_clear()
            .env("LC_ALL", "C.UTF-8")
            .env("PYTHONHASHSEED", "0")
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .current_dir(cwd.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let input = json!({"code":code,"class_name":"Example","method":"on_data",
            "context":{"state":{"user":{}}},"input":{"source":"fixture"}});
        child
            .stdin
            .take()
            .unwrap()
            .write_all(serde_json::to_string(&input).unwrap().as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }
}

#[test]
#[cfg(target_os = "linux")]
fn real_rust_python_worker_denies_network_and_host_files_but_returns_bounded_effects() {
    let output = WorkerFixture::run(
        r#"
from sigbot_sdk import Effects
class Example:
    def on_data(self, ctx, event):
        import socket, os
        try:
            socket.socket()
            raise AssertionError('network socket was allowed')
        except PermissionError:
            pass
        try:
            open('/etc/passwd').read()
            raise AssertionError('host filesystem was allowed')
        except PermissionError:
            pass
        assert 'DEEPSEEK_API_KEY' not in os.environ
        ctx.state['user']['observed'] = event.source
        print('strategy output captured')
        return Effects()
"#,
    );
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["state"]["user"]["observed"], "fixture");
    assert!(result["stdout"].as_str().unwrap().contains("strategy output captured"));
}

#[test]
#[cfg(target_os = "linux")]
fn worker_failure_returns_no_committable_state_or_effects() {
    let output = WorkerFixture::run(
        r#"
class Example:
    def on_data(self, ctx, event):
        ctx.state['user']['mutated'] = True
        raise ValueError('deliberate failure')
"#,
    );
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(result["error"].as_str().unwrap().contains("deliberate failure"));
    assert!(result.get("state").is_none() && result.get("effects").is_none());
}

#[test]
#[cfg(not(target_os = "linux"))]
fn unsupported_host_rejects_isolated_worker_without_committable_output() {
    let output = WorkerFixture::run("class Example: pass");
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("require Linux Landlock and seccomp"));
}
