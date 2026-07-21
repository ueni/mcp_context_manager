use std::{env, fs, path::Path, process::Command};

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const WORKSPACE_PACKAGES: &[&str] = &[
    "context-core",
    "context-index",
    "context-store",
    "context-testkit",
    "contextd",
    "xtask",
];

fn main() -> Result<()> {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("release-version") => release_version(arguments.collect()),
        Some("sbom") => generate_sbom(arguments.next().as_deref()),
        Some("license-check") => license_check(),
        Some(command) => bail!("unknown xtask command: {command}"),
        None => {
            println!("mcp-context-manager xtask {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

fn release_version(arguments: Vec<String>) -> Result<()> {
    let print_only = arguments
        .iter()
        .any(|argument| argument == "--print-canonical");
    let version = arguments
        .iter()
        .find(|argument| !argument.starts_with('-'))
        .context("release-version requires a version")?;
    validate_version(version)?;
    if print_only {
        println!("{version}");
        return Ok(());
    }
    replace_prefixed_line(
        Path::new("Cargo.toml"),
        "version = ",
        &format!("version = \"{version}\""),
    )?;
    update_workspace_lock_versions(Path::new("Cargo.lock"), version)?;
    if Path::new("monitor.py").exists() {
        replace_prefixed_line(
            Path::new("monitor.py"),
            "EXPECTED_SERVER_VERSION = \"",
            &format!("EXPECTED_SERVER_VERSION = \"{version}\""),
        )?;
    }
    println!("updated Rust workspace release version to {version}");
    Ok(())
}

fn validate_version(version: &str) -> Result<()> {
    ensure!(!version.starts_with('v'), "version must not start with v");
    let (core, suffix) = version.split_once('-').unwrap_or((version, ""));
    let parts = core.split('.').collect::<Vec<_>>();
    ensure!(parts.len() == 3, "version must contain major.minor.patch");
    ensure!(
        parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())),
        "version core must be numeric"
    );
    ensure!(
        suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-'),
        "version pre-release contains invalid characters"
    );
    Ok(())
}

fn replace_prefixed_line(path: &Path, prefix: &str, replacement: &str) -> Result<()> {
    let source = fs::read_to_string(path)?;
    let mut matches = 0;
    let mut output = String::with_capacity(source.len());
    for line in source.lines() {
        if line.starts_with(prefix) {
            output.push_str(replacement);
            matches += 1;
        } else {
            output.push_str(line);
        }
        output.push('\n');
    }
    ensure!(
        matches == 1,
        "expected exactly one {prefix:?} line in {}",
        path.display()
    );
    fs::write(path, output)?;
    Ok(())
}

fn update_workspace_lock_versions(path: &Path, version: &str) -> Result<()> {
    let source = fs::read_to_string(path)?;
    let mut output = String::with_capacity(source.len());
    let mut package_name: Option<String> = None;
    let mut updated = 0;
    for line in source.lines() {
        if line == "[[package]]" {
            package_name = None;
        }
        if let Some(name) = line
            .strip_prefix("name = \"")
            .and_then(|value| value.strip_suffix('"'))
        {
            package_name = Some(name.to_owned());
        }
        if line.starts_with("version = \"")
            && package_name
                .as_deref()
                .is_some_and(|name| WORKSPACE_PACKAGES.contains(&name))
        {
            output.push_str(&format!("version = \"{version}\"\n"));
            updated += 1;
        } else {
            output.push_str(line);
            output.push('\n');
        }
    }
    ensure!(
        updated == WORKSPACE_PACKAGES.len(),
        "workspace lock package set is incomplete"
    );
    fs::write(path, output)?;
    Ok(())
}

fn cargo_metadata() -> Result<Value> {
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--filter-platform",
            "x86_64-unknown-linux-gnu",
        ])
        .output()
        .context("run cargo metadata")?;
    ensure!(output.status.success(), "cargo metadata failed");
    serde_json::from_slice(&output.stdout).map_err(Into::into)
}

fn generate_sbom(output: Option<&str>) -> Result<()> {
    let output = output.unwrap_or("dist/mcp-context-manager.cdx.json");
    let metadata = cargo_metadata()?;
    let mut components = metadata["packages"]
        .as_array()
        .context("cargo metadata packages")?
        .iter()
        .map(|package| {
            let name = package["name"].as_str().unwrap_or_default();
            let version = package["version"].as_str().unwrap_or_default();
            json!({
                "type": "library",
                "bom-ref": package["id"],
                "name": name,
                "version": version,
                "purl": format!("pkg:cargo/{name}@{version}"),
                "licenses": [{"expression": package["license"].as_str().unwrap_or("NOASSERTION")}],
            })
        })
        .collect::<Vec<_>>();
    components.sort_by(|left, right| {
        left["name"]
            .as_str()
            .cmp(&right["name"].as_str())
            .then_with(|| left["version"].as_str().cmp(&right["version"].as_str()))
    });
    let dependencies = metadata
        .pointer("/resolve/nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|node| {
            json!({
                "ref": node["id"],
                "dependsOn": node["dependencies"],
            })
        })
        .collect::<Vec<_>>();
    let component_bytes = serde_json::to_vec(&components)?;
    let serial = Sha256::digest(&component_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let sbom = json!({
        "$schema": "https://cyclonedx.org/schema/bom-1.6.schema.json",
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "version": 1,
        "serialNumber": format!("urn:sha256:{serial}"),
        "metadata": {"component": {"type": "application", "name": "mcp-context-manager", "version": env!("CARGO_PKG_VERSION")}},
        "components": components,
        "dependencies": dependencies,
    });
    let path = Path::new(output);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(&sbom)?)?;
    println!("wrote {}", path.display());
    Ok(())
}

fn license_check() -> Result<()> {
    let metadata = cargo_metadata()?;
    let allowed = [
        "Apache-2.0",
        "MIT",
        "MPL-2.0",
        "BSL-1.0",
        "BSD-3-Clause",
        "CC0-1.0",
        "ISC",
        "Unicode-3.0",
        "Unlicense",
        "Zlib",
        "zlib-acknowledgement",
        "LLVM-exception",
    ];
    let mut denied = Vec::new();
    for package in metadata["packages"]
        .as_array()
        .context("metadata packages")?
    {
        let license = package["license"].as_str().unwrap_or_default();
        let normalized = license.replace('/', " OR ");
        let tokens = normalized
            .split(|character: char| character.is_whitespace() || matches!(character, '(' | ')'))
            .filter(|token| !token.is_empty())
            .filter(|token| !matches!(*token, "AND" | "OR" | "WITH"))
            .collect::<Vec<_>>();
        if tokens.is_empty() || tokens.iter().any(|token| !allowed.contains(token)) {
            denied.push(format!(
                "{} {}: {}",
                package["name"].as_str().unwrap_or_default(),
                package["version"].as_str().unwrap_or_default(),
                license
            ));
        }
    }
    ensure!(
        denied.is_empty(),
        "unapproved dependency licenses:\n{}",
        denied.join("\n")
    );
    println!("all dependency license expressions are approved");
    Ok(())
}
