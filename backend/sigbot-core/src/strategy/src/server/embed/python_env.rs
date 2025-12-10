// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use anyhow::{Context, Result};
use common_telemetry::{info, warn};
use pyo3::prelude::*;

/// Required Python packages for strategy execution
pub const REQUIRED_PACKAGES: &[&str] = &["polars", "pandas", "pyarrow"];

/// Check if a Python package is installed
pub fn check_package_installed(py: Python<'_>, package_name: &str) -> bool {
    let check_code = format!("import {}", package_name);
    py.run_bound(&check_code, None, None).is_ok()
}

/// Verify required packages are installed
pub fn verify_required_packages(py: Python<'_>) -> Result<Vec<String>> {
    let mut missing = Vec::new();

    for package in REQUIRED_PACKAGES {
        if !check_package_installed(py, package) {
            missing.push(package.to_string());
        }
    }

    if !missing.is_empty() {
        warn!(
            "Missing required Python packages: {:?}. Please install them using: pip install {}",
            missing,
            missing.join(" ")
        );
    } else {
        info!("All required Python packages are installed: {:?}", REQUIRED_PACKAGES);
    }

    Ok(missing)
}

/// Attempt to install Python packages (requires pip to be available)
pub fn install_packages(py: Python<'_>, packages: &[String]) -> Result<()> {
    if packages.is_empty() {
        return Ok(());
    }

    // Try to use subprocess to install packages
    let install_code = format!(
        r#"
import subprocess
import sys

packages = {}
try:
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet"] + packages,
        capture_output=True,
        text=True,
        timeout=60
    )
    if result.returncode != 0:
        raise Exception(f"Failed to install packages: {{result.stderr}}")
except Exception as e:
    raise Exception(f"Error installing packages: {{str(e)}}")
"#,
        format!("{:?}", packages)
    );

    py.run_bound(&install_code, None, None)
        .context("Failed to install Python packages")?;

    info!("Successfully installed Python packages: {:?}", packages);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_package_installed() {
        Python::with_gil(|py| {
            // json should always be available
            assert!(check_package_installed(py, "json"));
            // non-existent package should return false
            assert!(!check_package_installed(py, "nonexistent_package_xyz123"));
        });
    }
}
