// Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
// SPDX-License-Identifier: Apache-2.0

fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        // Cargo otherwise records its absolute target path as the install name,
        // making an otherwise portable cdylib impossible to relocate.
        println!("cargo:rustc-link-arg=-Wl,-install_name,@rpath/libdsflower_dp_primitives.dylib");
    }
}
