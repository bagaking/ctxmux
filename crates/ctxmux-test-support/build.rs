use std::{env, path::PathBuf, process::Command};

fn main() {
    let source = "src/bin/fixture-executable.rs";
    println!("cargo:rerun-if-changed={source}");
    let executable = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo provides OUT_DIR"))
        .join("ctxmux-fixture-executable");
    let status = Command::new(env::var_os("RUSTC").expect("Cargo provides RUSTC"))
        .args(["--edition=2024", "--target"])
        .arg(env::var_os("HOST").expect("Cargo provides HOST"))
        .arg(source)
        .arg("-o")
        .arg(&executable)
        .status()
        .expect("compile the host-native test fixture executable");
    assert!(status.success(), "native fixture compilation failed");
    println!(
        "cargo:rustc-env=CTXMUX_FIXTURE_EXECUTABLE={}",
        executable.display()
    );
}
