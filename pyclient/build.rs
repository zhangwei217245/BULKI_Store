use regex::Regex;

use std::fs;
use std::path::Path;

fn main() {
    // First, disable the default behavior where Cargo watches all source files
    println!("cargo:rerun-if-changed=");

    // Then explicitly list only the files we want to watch
    println!("cargo:rerun-if-changed=../Cargo.toml");
    println!("cargo:rerun-if-changed=../pyproject.toml");
    println!("cargo:rerun-if-changed=build.rs");

    // Sync versions between workspace Cargo.toml and pyproject.toml
    sync_versions();
}

fn sync_versions() {
    // Read version from workspace Cargo.toml
    let workspace_path = Path::new("../Cargo.toml");
    let workspace_content = match fs::read_to_string(workspace_path) {
        Ok(content) => content,
        Err(e) => {
            println!("cargo:warning=Failed to read workspace Cargo.toml: {}", e);
            return;
        }
    };

    // Extract the workspace.package.version using regex
    let re = Regex::new(r#"\[workspace\.package\][\s\S]*?version\s*=\s*"([^"]+)""#).unwrap();
    let cargo_version = match re.captures(&workspace_content) {
        Some(captures) => captures.get(1).unwrap().as_str(),
        None => {
            println!("cargo:warning=Could not find workspace.package.version in Cargo.toml");
            return;
        }
    };

    println!("cargo:warning=Found workspace version: {}", cargo_version);

    // Update pyproject.toml with this version
    let pyproject_path = Path::new("../pyproject.toml");
    let pyproject_content = match fs::read_to_string(pyproject_path) {
        Ok(content) => content,
        Err(e) => {
            println!("cargo:warning=Failed to read pyproject.toml: {}", e);
            return;
        }
    };

    // Replace the version in pyproject.toml
    let py_re = Regex::new(r#"version\s*=\s*"([^"]+)""#).unwrap();
    let updated_content = py_re
        .replace(
            &pyproject_content,
            &format!("version = \"{}\"", cargo_version),
        )
        .to_string();

    if updated_content != pyproject_content {
        match fs::write(pyproject_path, updated_content) {
            Ok(_) => println!(
                "cargo:warning=Updated pyproject.toml version to {}",
                cargo_version
            ),
            Err(e) => println!("cargo:warning=Failed to write to pyproject.toml: {}", e),
        }
    }
}
