// SPDX-License-Identifier: GPL-3.0-only
#[path = "../../build/metadata.rs"]
mod metadata;

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../build/metadata.rs");
    metadata::BuildMetadata::emit();
    Ok(())
}
