// SPDX-License-Identifier: GPL-3.0-only
//! Class-based v2 callbacks. Host this executor only in a restricted Rust worker;
//! Python is not an in-process security boundary.

use crate::python::worker::worker::WorkerRequest;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use sigbot_strategy_sdk::sdk::runtime::Strategy;
use sigbot_strategy_sdk::sdk::runtime::StrategyContext;
use sigbot_strategy_sdk::sdk::runtime::StrategyState;
use sigbot_types::strategy::runtime::Effects;
use sigbot_types::strategy::runtime::HyperparametersUpdate;
use sigbot_types::strategy::runtime::StrategyEvent;
use std::io::Read;
use std::io::Write;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

pub struct WorkerStrategy {
    code: String,
    class_name: String,
}

#[derive(Deserialize)]
struct CallbackResult {
    state: Option<StrategyState>,
    effects: Option<Effects>,
    error: Option<String>,
    stdout: String,
    stderr: String,
}

impl WorkerStrategy {
    pub fn new(code: String, class_name: String) -> Result<Self> {
        ensure!(
            !code.is_empty() && code.len() <= 262_144,
            "strategy source must be between 1 and 256 KiB"
        );
        ensure!(
            !class_name.is_empty() && class_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "invalid strategy class name"
        );
        Ok(Self { code, class_name })
    }

    fn invoke(&self, method: &str, context: &mut StrategyContext, input: Value) -> Result<Effects> {
        let request = WorkerRequest {
            code: self.code.clone(),
            class_name: self.class_name.clone(),
            method: method.to_owned(),
            context: json!({"identity":context.identity(),"clock":{"now":context.now()},"configuration":context.configuration(),
                "hyperparameters":context.hyperparameters(),"portfolio":context.portfolio(),"state":context.state}),
            input,
        };
        let output = match Self::execute_worker(serde_json::to_vec(&request)?) {
            Ok(output) => output,
            Err(error) => {
                context.record_log("ERROR", "python.exception", error.to_string())?;
                return Err(error);
            }
        };
        ensure!(output.len() <= 2_097_152, "Python result exceeds 2 MiB bound");
        let result: CallbackResult = serde_json::from_slice(&output)?;
        context.record_log("INFO", "python.stdout", result.stdout)?;
        context.record_log("WARN", "python.stderr", result.stderr)?;
        if let Some(error) = result.error {
            context.record_log("ERROR", "python.exception", error.clone())?;
            anyhow::bail!("{}", error);
        }
        let effects = result.effects.context("successful worker response needs Effects")?;
        effects.validate(context.now())?;
        context.state = result.state.context("successful worker response needs a state draft")?;
        Ok(effects)
    }

    fn execute_worker(request: Vec<u8>) -> Result<Vec<u8>> {
        ensure!(request.len() <= 2_097_152, "worker request exceeds 2 MiB");
        let binary = std::env::var_os("SIGBOT_STRATEGY_WORKER_BINARY")
            .map(std::path::PathBuf::from)
            .unwrap_or(std::env::current_exe()?.with_file_name("sigbot-strategy-worker"));
        ensure!(binary.is_absolute(), "strategy worker path must be absolute");
        let directory = tempfile::tempdir()?;
        let mut command = Command::new(binary);
        command
            .env_clear()
            .env("PYTHONHASHSEED", "0")
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("PYTHONNOUSERSITE", "1")
            .env("LC_ALL", "C.UTF-8")
            .current_dir(directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::CommandExt;
            let parent_pid = std::process::id() as libc::pid_t;
            // SAFETY: only async-signal-safe syscalls run between fork and exec.
            unsafe {
                command.pre_exec(move || {
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::getppid() != parent_pid {
                        return Err(std::io::Error::other("strategy host exited during worker creation"));
                    }
                    if libc::syscall(libc::SYS_close_range, 3_u32, u32::MAX, libc::CLOSE_RANGE_CLOEXEC) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        let mut child = command
            .spawn()
            .context("could not start credential-free Rust strategy worker")?;
        let mut stdin = child.stdin.take().context("worker stdin missing")?;
        let stdout = child.stdout.take().context("worker stdout missing")?;
        let stderr = child.stderr.take().context("worker stderr missing")?;
        let writer = std::thread::spawn(move || stdin.write_all(&request));
        let output = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stdout.take(2_097_153).read_to_end(&mut bytes).map(|_| bytes)
        });
        let errors = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stderr.take(65_537).read_to_end(&mut bytes).map(|_| bytes)
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        let result = loop {
            if let Some(status) = child.try_wait()? {
                break Ok(status);
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                break Err(anyhow::anyhow!("strategy worker exceeded 5 second wall deadline"));
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let _ = writer.join();
        let output = output
            .join()
            .map_err(|_| anyhow::anyhow!("worker output reader failed"))??;
        let errors = errors
            .join()
            .map_err(|_| anyhow::anyhow!("worker error reader failed"))??;
        let status = result?;
        ensure!(
            status.success(),
            "strategy worker failed: {}",
            String::from_utf8_lossy(&errors)
        );
        Ok(output)
    }
}

impl Strategy for WorkerStrategy {
    fn on_hyperparameters(&mut self, context: &mut StrategyContext, update: &HyperparametersUpdate) -> Result<Effects> {
        self.invoke("on_hyperparameters", context, serde_json::to_value(update)?)
    }

    fn on_data(&mut self, context: &mut StrategyContext, event: &StrategyEvent) -> Result<Effects> {
        self.invoke("on_data", context, serde_json::to_value(event)?)
    }
}
