//! Shared compile-time metadata for binaries and libraries, including source archives.
use chrono::{DateTime, SecondsFormat, Utc};
use std::{env, process::Command};

pub struct BuildMetadata;
impl BuildMetadata {
    fn git(arguments: &[&str]) -> Option<String> {
        let output = Command::new("git").args(arguments).output().ok()?;
        if !output.status.success() {
            return None;
        }
        let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
        (!value.is_empty()).then_some(value)
    }

    pub fn emit() {
        for key in ["BUILD_TAG", "BUILD_COMMIT_ID", "SOURCE_DATE_EPOCH"] {
            println!("cargo:rerun-if-env-changed={key}");
        }
        let version = env::var("BUILD_TAG")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| Self::git(&["describe", "--tags", "--always", "--dirty"]))
            .unwrap_or_else(|| env::var("CARGO_PKG_VERSION").expect("package version required"));
        let commit = env::var("BUILD_COMMIT_ID")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| Self::git(&["rev-parse", "HEAD"]))
            .unwrap_or_else(|| "unknown".into());
        assert!(
            !version.contains(['\n', '\r']) && !commit.contains(['\n', '\r']),
            "invalid build metadata"
        );
        let epoch = env::var("SOURCE_DATE_EPOCH")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| Self::git(&["show", "-s", "--format=%ct", "HEAD"]))
            .and_then(|value| value.parse::<i64>().ok());
        let date = epoch
            .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0))
            .unwrap_or_else(Utc::now);
        println!("cargo:rustc-env=GIT_VERSION={version}");
        println!("cargo:rustc-env=GIT_COMMIT_HASH={commit}");
        println!(
            "cargo:rustc-env=GIT_BUILD_DATE={}",
            date.to_rfc3339_opts(SecondsFormat::Secs, true)
        );
    }
}
