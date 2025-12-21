// Copyright 2024 Zinc Labs Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use chrono::{DateTime, SecondsFormat, Utc};
use std::{io::Result, process::Command};

// Include shared Python library path setup from strategy-runner
// This ensures the final binary has the correct rpath settings for Python/OpenSSL
// Path: from src/cmd to workspace root/build/build_setpy.rs
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../build/build_setpy.rs"));

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rerun-if-changed={}/../../build/build_setpy.rs",
        env!("CARGO_MANIFEST_DIR")
    );

    // build information
    let output = Command::new("git")
        .args(["describe", "--tags", "--abbrev=0"])
        .output()
        .unwrap();
    let git_tag = String::from_utf8(output.stdout).unwrap();
    // If there is no tag, use the branch name.
    let git_tag = if git_tag.is_empty() {
        let output = Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .output()
            .unwrap();
        String::from_utf8(output.stdout).unwrap()
    } else {
        git_tag
    };
    println!("cargo:rustc-env=GIT_VERSION={git_tag}");

    let output = Command::new("git").args(["rev-parse", "HEAD"]).output().unwrap();
    let git_commit = String::from_utf8(output.stdout).unwrap();
    println!("cargo:rustc-env=GIT_COMMIT_HASH={git_commit}");

    let now: DateTime<Utc> = Utc::now();
    let build_date = now.to_rfc3339_opts(SecondsFormat::Secs, true);
    println!("cargo:rustc-env=GIT_BUILD_DATE={build_date}");

    // Set Python and OpenSSL library paths dynamically based on platform
    // This is required for PyO3 to work correctly, especially on macOS
    // Since cmd is the final binary, we need to set rpath here as well
    setup_python_library_path()?;

    Ok(())
}
