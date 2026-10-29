// SPDX-License-Identifier: GPL-3.0-only
//! Credential-free, one-callback Rust/PyO3 worker. Restrictions precede user code.

use anyhow::{ensure, Result};
use pyo3::{prelude::*, types::PyModule};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Read, Write};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRequest {
    pub code: String,
    pub class_name: String,
    pub method: String,
    pub context: Value,
    pub input: Value,
}

pub struct StrategyWorker;

impl StrategyWorker {
    pub fn run() -> Result<()> {
        for name in std::env::vars_os().map(|(name, _)| name).collect::<Vec<_>>() {
            if ![
                "PYTHONHASHSEED",
                "PYTHONDONTWRITEBYTECODE",
                "PYTHONNOUSERSITE",
                "LC_ALL",
            ]
            .iter()
            .any(|allowed| name == *allowed)
            {
                std::env::remove_var(name);
            }
        }
        let mut input = Vec::new();
        std::io::stdin().take(2_097_153).read_to_end(&mut input)?;
        ensure!(input.len() <= 2_097_152, "worker request exceeds 2 MiB");
        let request: WorkerRequest = serde_json::from_slice(&input)?;
        ensure!(
            request.code.len() <= 262_144 && matches!(request.method.as_str(), "on_data" | "on_hyperparameters"),
            "invalid worker callback"
        );
        Self::restrict()?;
        let output = Self::execute(request)?;
        ensure!(output.len() <= 2_097_152, "worker result exceeds 2 MiB");
        std::io::stdout().write_all(output.as_bytes())?;
        Ok(())
    }

    fn execute(request: WorkerRequest) -> Result<String> {
        let context = serde_json::to_string(&request.context)?;
        let input = serde_json::to_string(&request.input)?;
        Python::with_gil(|py| -> PyResult<String> {
            let sdk = PyModule::from_code_bound(
                py,
                include_str!("../../../../sdk/src/sdk/runtime.py"),
                "sigbot_sdk.py",
                "sigbot_sdk",
            )?;
            let modules = PyModule::import_bound(py, "sys")?.getattr("modules")?;
            modules.set_item("sigbot_sdk", &sdk)?;
            let captures = sdk.getattr("OutputCapture")?.call0()?;
            captures.call_method0("install")?;
            let result = (|| -> PyResult<String> {
                let module = PyModule::from_code_bound(py, &request.code, "strategy.py", "sigbot_strategy")?;
                let strategy = module.getattr(request.class_name.as_str())?.call0()?;
                let context = sdk.getattr("Context")?.call1((context,))?;
                let input = sdk.getattr("Input")?.call_method1("from_json", (input,))?;
                let effects = strategy.call_method1(request.method.as_str(), (&context, input))?;
                context.call_method1("finish", (effects,))?.extract()
            })();
            match result {
                Ok(result) => captures.call_method1("finish", (result,))?.extract(),
                Err(error) => captures.call_method1("fail", (error.to_string(),))?.extract(),
            }
        })
        .map_err(Into::into)
    }

    #[cfg(target_os = "linux")]
    fn restrict() -> Result<()> {
        use landlock::{
            Access, AccessFs, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr,
            RulesetStatus, ABI,
        };
        use seccompiler::{BpfProgram, SeccompAction, SeccompFilter};
        // The process is still single threaded. New processes/threads are forbidden.
        for (resource, value) in [
            (libc::RLIMIT_AS, 536_870_912),
            (libc::RLIMIT_CPU, 2),
            (libc::RLIMIT_FSIZE, 1_048_576),
            (libc::RLIMIT_NOFILE, 32),
            (libc::RLIMIT_CORE, 0),
        ] {
            let limit = libc::rlimit {
                rlim_cur: value,
                rlim_max: value,
            };
            // SAFETY: setrlimit reads the valid stack value and only restricts this process.
            ensure!(
                unsafe { libc::setrlimit(resource, &limit) } == 0,
                "could not apply worker resource limit"
            );
        }
        let abi = ABI::V3;
        let mut rules = Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(abi))?
            .create()?;
        for path in ["/usr/lib", "/usr/local/lib", "/lib", "/lib64", "/dev/urandom"] {
            if std::path::Path::new(path).exists() {
                let access = if std::path::Path::new(path).is_dir() {
                    AccessFs::from_read(abi)
                } else {
                    AccessFs::ReadFile.into()
                };
                rules = rules.add_rule(PathBeneath::new(PathFd::new(path)?, access))?;
            }
        }
        let working_directory = std::env::current_dir()?;
        rules = rules.add_rule(PathBeneath::new(
            PathFd::new(working_directory)?,
            AccessFs::from_all(abi),
        ))?;
        let status = rules.restrict_self()?;
        ensure!(
            status.ruleset == RulesetStatus::FullyEnforced && status.no_new_privs,
            "Landlock sandbox is not fully enforced"
        );
        let blocked = [
            libc::SYS_socket,
            libc::SYS_socketpair,
            libc::SYS_connect,
            libc::SYS_bind,
            libc::SYS_listen,
            libc::SYS_accept,
            libc::SYS_accept4,
            libc::SYS_execve,
            libc::SYS_execveat,
            libc::SYS_clone,
            libc::SYS_clone3,
            libc::SYS_ptrace,
            libc::SYS_process_vm_readv,
            libc::SYS_process_vm_writev,
            libc::SYS_mount,
            libc::SYS_setns,
            libc::SYS_unshare,
            libc::SYS_bpf,
            libc::SYS_open_by_handle_at,
            libc::SYS_io_uring_setup,
            libc::SYS_io_uring_enter,
            libc::SYS_io_uring_register,
            libc::SYS_kill,
            libc::SYS_tkill,
            libc::SYS_tgkill,
            libc::SYS_pidfd_getfd,
            libc::SYS_pidfd_send_signal,
            libc::SYS_prlimit64,
            libc::SYS_sched_setaffinity,
            libc::SYS_setpriority,
            libc::SYS_fchmod,
            libc::SYS_fchmodat,
            libc::SYS_fchown,
            libc::SYS_fchownat,
            libc::SYS_utimensat,
            libc::SYS_setxattr,
            libc::SYS_lsetxattr,
            libc::SYS_fsetxattr,
            libc::SYS_removexattr,
            libc::SYS_lremovexattr,
            libc::SYS_fremovexattr,
        ];
        let mut rules = blocked
            .into_iter()
            .map(|syscall| (syscall, vec![]))
            .collect::<std::collections::BTreeMap<_, _>>();
        #[cfg(target_arch = "x86_64")]
        {
            for syscall in [
                libc::SYS_fork,
                libc::SYS_vfork,
                libc::SYS_chmod,
                libc::SYS_chown,
                libc::SYS_lchown,
                libc::SYS_utime,
                libc::SYS_utimes,
                libc::SYS_futimesat,
            ] {
                rules.insert(syscall, vec![]);
            }
        }
        let filter: BpfProgram = SeccompFilter::new(
            rules,
            SeccompAction::Allow,
            SeccompAction::Errno(libc::EPERM as u32),
            std::env::consts::ARCH.try_into()?,
        )?
        .try_into()?;
        seccompiler::apply_filter(&filter)?;
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn restrict() -> Result<()> {
        anyhow::bail!("isolated strategy workers require Linux Landlock and seccomp")
    }
}
