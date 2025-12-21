// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Include shared Python library path setup
// Path: from src/strategy/runner to workspace root/build/build_setpy.rs
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../../build/build_setpy.rs"));

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rerun-if-changed={}/../../../build/build_setpy.rs",
        env!("CARGO_MANIFEST_DIR")
    );

    // Set Python and OpenSSL library paths dynamically based on platform
    // This is required for PyO3 to work correctly, especially on macOS
    // The rpath settings will be propagated to any binary that links against this crate
    setup_python_library_path()?;

    Ok(())
}
