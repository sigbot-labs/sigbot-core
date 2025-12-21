// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Note: This file is included via include!() macro, so we use fully qualified paths
// to avoid conflicts with use statements in the including file.

pub fn setup_python_library_path() -> std::io::Result<()> {
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
fn setup_macos_python_rpath() -> std::io::Result<()> {
    // Check if PYO3_PYTHON is set (pyo3's way to specify Python)
    if let Ok(python_path) = std::env::var("PYO3_PYTHON") {
        if let Some(lib_dir) = find_python_lib_dir(&python_path) {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
            return Ok(());
        }
    }

    // Try to find Python via python3-config (works for any Python version)
    if let Ok(output) = std::process::Command::new("python3-config")
        .args(["--ldflags"])
        .output()
    {
        let ldflags = String::from_utf8_lossy(&output.stdout);
        // Extract -L paths from ldflags
        for line in ldflags.lines() {
            if let Some(path) = line.strip_prefix("-L") {
                let path = path.trim();
                if std::path::PathBuf::from(path).exists() {
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
        let path = std::path::PathBuf::from(path_str);
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

    // Add OpenSSL library paths (required for Python's SSL support)
    // Python depends on OpenSSL, so we need to add OpenSSL library paths to rpath
    let openssl_paths = vec![
        "/opt/homebrew/opt/openssl@3/lib".to_string(),
        "/opt/homebrew/opt/openssl/lib".to_string(),
        "/usr/local/opt/openssl@3/lib".to_string(),
        "/usr/local/opt/openssl/lib".to_string(),
    ];

    for path_str in openssl_paths {
        let path = std::path::PathBuf::from(path_str);
        if path.exists() {
            // Check if it contains OpenSSL libraries
            if path
                .read_dir()
                .map(|mut entries| {
                    entries.any(|e| {
                        e.map(|entry| {
                            let file_name = entry.file_name();
                            let name = file_name.to_string_lossy();
                            name.contains("libssl") || name.contains("libcrypto")
                        })
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
fn setup_linux_python_rpath() -> std::io::Result<()> {
    // Linux typically uses standard library paths, but we can set RPATH for non-standard installations

    // Check if PYO3_PYTHON is set
    if let Ok(python_path) = std::env::var("PYO3_PYTHON") {
        if let Some(lib_dir) = find_python_lib_dir(&python_path) {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
            return Ok(());
        }
    }

    // Try python3-config (works for any Python version)
    if let Ok(output) = std::process::Command::new("python3-config")
        .args(["--ldflags"])
        .output()
    {
        let ldflags = String::from_utf8_lossy(&output.stdout);
        for line in ldflags.lines() {
            if let Some(path) = line.strip_prefix("-L") {
                let path = path.trim();
                if std::path::PathBuf::from(path).exists() {
                    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path);
                }
            }
        }
    }

    // Common Linux Python paths (version-agnostic)
    let common_paths = vec![
        "/usr/lib/x86_64-linux-gnu".to_string(),
        "/usr/lib64".to_string(),
        "/usr/lib".to_string(),
        "/usr/local/lib".to_string(),
        // Conda environments
        "/opt/conda/lib".to_string(),
    ];

    for path_str in common_paths {
        let path = std::path::PathBuf::from(path_str);
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
fn setup_windows_python_path() -> std::io::Result<()> {
    // Windows uses different mechanisms (DLL search path)
    // pyo3-build-config usually handles this automatically
    // But we can add custom paths if needed via PYO3_PYTHON

    if let Ok(python_path) = std::env::var("PYO3_PYTHON") {
        if let Some(lib_dir) = find_python_lib_dir(&python_path) {
            // On Windows, we might need to add the path to the DLL search path
            // This is usually handled by pyo3, but we can log it for debugging
            println!("cargo:warning=Python library path on Windows: {}", lib_dir.display());
        }
    }

    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
fn find_python_lib_dir(python_path: &str) -> Option<std::path::PathBuf> {
    // Try to get Python library directory from Python executable
    if let Ok(output) = std::process::Command::new(python_path)
        .args(["-c", "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"])
        .output()
    {
        let lib_dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !lib_dir.is_empty() && std::path::PathBuf::from(&lib_dir).exists() {
            return Some(std::path::PathBuf::from(lib_dir));
        }
    }

    // Fallback: try to find libpython near the Python executable
    let python_path = std::path::PathBuf::from(python_path);
    if let Some(parent) = python_path.parent() {
        let possible_lib = parent.join("lib");
        if possible_lib.exists() {
            return Some(possible_lib);
        }
    }

    None
}
