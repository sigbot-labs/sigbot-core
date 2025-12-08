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
use std::{env, io::Result, path::PathBuf, process::Command};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=build.rs");

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

    // Set Python library path dynamically based on platform
    setup_python_library_path()?;

    Ok(())
}

fn setup_python_library_path() -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        setup_macos_python_rpath()?;
    }

    #[cfg(target_os = "linux")]
    {
        setup_linux_python_rpath()?;
    }

    #[cfg(target_os = "windows")]
    {
        setup_windows_python_path()?;
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn setup_macos_python_rpath() -> Result<()> {
    // Check if PYO3_PYTHON is set (pyo3's way to specify Python)
    if let Ok(python_path) = env::var("PYO3_PYTHON") {
        if let Some(lib_dir) = find_python_lib_dir(&python_path) {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
            return Ok(());
        }
    }

    // Try to find Python via python3-config (works for any Python version)
    if let Ok(output) = Command::new("python3-config").args(["--ldflags"]).output() {
        let ldflags = String::from_utf8_lossy(&output.stdout);
        // Extract -L paths from ldflags
        for line in ldflags.lines() {
            if let Some(path) = line.strip_prefix("-L") {
                let path = path.trim();
                if PathBuf::from(path).exists() {
                    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path);
                }
            }
        }
    }

    // Generate common Python installation paths dynamically (supports Python 3.8-3.13)
    let mut common_paths = Vec::new();

    // Homebrew Python (supports all versions from 3.8 to 3.13)
    for version in (8..=13).rev() {
        common_paths.push(format!(
            "/opt/homebrew/opt/python@3.{}/Frameworks/Python.framework/Versions/3.{}/lib",
            version, version
        ));
        common_paths.push(format!(
            "/usr/local/opt/python@3.{}/Frameworks/Python.framework/Versions/3.{}/lib",
            version, version
        ));
    }

    // Anaconda/Miniconda (version-agnostic)
    common_paths.extend(vec![
        "/opt/homebrew/anaconda3/lib".to_string(),
        "/opt/homebrew/miniconda3/lib".to_string(),
        "/usr/local/anaconda3/lib".to_string(),
        "/usr/local/miniconda3/lib".to_string(),
        // Generic homebrew lib
        "/opt/homebrew/lib".to_string(),
        "/usr/local/lib".to_string(),
    ]);

    for path_str in common_paths {
        let path = PathBuf::from(path_str);
        if path.exists() {
            // Check if it contains Python library
            if path
                .read_dir()
                .map(|mut entries| {
                    entries.any(|e| {
                        e.map(|entry| entry.file_name().to_string_lossy().contains("libpython"))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
            {
                println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path.display());
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn setup_linux_python_rpath() -> Result<()> {
    // Linux typically uses standard library paths, but we can set RPATH for non-standard installations

    // Check if PYO3_PYTHON is set
    if let Ok(python_path) = env::var("PYO3_PYTHON") {
        if let Some(lib_dir) = find_python_lib_dir(&python_path) {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
            return Ok(());
        }
    }

    // Try python3-config (works for any Python version)
    if let Ok(output) = Command::new("python3-config").args(["--ldflags"]).output() {
        let ldflags = String::from_utf8_lossy(&output.stdout);
        for line in ldflags.lines() {
            if let Some(path) = line.strip_prefix("-L") {
                let path = path.trim();
                if PathBuf::from(path).exists() {
                    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path);
                }
            }
        }
    }

    // Common Linux Python paths (version-agnostic)
    let common_paths = vec![
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib64",
        "/usr/lib",
        "/usr/local/lib",
        // Conda environments
        "/opt/conda/lib",
        "/home/*/anaconda3/lib",
        "/home/*/miniconda3/lib",
    ];

    for path_str in common_paths {
        // Handle glob patterns
        if path_str.contains('*') {
            continue; // Skip glob patterns, they're just for reference
        }

        let path = PathBuf::from(path_str);
        if path.exists() {
            if path
                .read_dir()
                .map(|mut entries| {
                    entries.any(|e| {
                        e.map(|entry| entry.file_name().to_string_lossy().contains("libpython"))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
            {
                println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path.display());
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn setup_windows_python_path() -> Result<()> {
    // Windows uses different mechanisms (DLL search path)
    // pyo3-build-config usually handles this automatically
    // But we can add custom paths if needed via PYO3_PYTHON

    if let Ok(python_path) = env::var("PYO3_PYTHON") {
        if let Some(lib_dir) = find_python_lib_dir(&python_path) {
            // On Windows, we might need to add the path to the DLL search path
            // This is usually handled by pyo3, but we can log it for debugging
            println!("cargo:warning=Python library path on Windows: {}", lib_dir.display());
        }
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn find_python_lib_dir(python_path: &str) -> Option<PathBuf> {
    // Try to get Python library directory from Python executable
    if let Ok(output) = Command::new(python_path)
        .args(["-c", "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"])
        .output()
    {
        let lib_dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !lib_dir.is_empty() && PathBuf::from(&lib_dir).exists() {
            return Some(PathBuf::from(lib_dir));
        }
    }

    // Fallback: try to find libpython near the Python executable
    let python_path = PathBuf::from(python_path);
    if let Some(parent) = python_path.parent() {
        let possible_lib = parent.join("lib");
        if possible_lib.exists() {
            return Some(possible_lib);
        }
    }

    None
}
