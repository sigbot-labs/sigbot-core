// SPDX-License-Identifier: GPL-3.0-only
#[path = "../../build/metadata.rs"]
mod metadata;
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../build/build_setpy.rs"));

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../build/metadata.rs");
    metadata::BuildMetadata::emit();
    println!("cargo:rerun-if-changed=../../build/build_setpy.rs");
    setup_python_library_path()?;
    Ok(())
}
