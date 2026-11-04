//! Bounded Git identity and registration discovery shared by routing and freshness.

use std::{
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitCheckout {
    pub root: PathBuf,
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    pub branch: Option<String>,
    pub head: String,
}

/// Resolve Git identity through Git itself, handling worktree pointers, packed refs, and detached HEAD.
pub fn checkout(root: &Path) -> Result<GitCheckout> {
    checkout_with_mappings(root, &[])
}

pub fn checkout_with_mappings(root: &Path, mappings: &[(PathBuf, PathBuf)]) -> Result<GitCheckout> {
    let root = root.canonicalize().context("canonicalize Git checkout")?;
    let git_dir = mapped_git_dir(&root, mappings)?;
    let top = git_output(&root, &["rev-parse", "--show-toplevel"], git_dir.as_deref())?;
    if PathBuf::from(top.trim()).canonicalize()? != root {
        return Err(anyhow!("requested path is not the Git checkout root"));
    }
    let git_dir = match git_dir {
        Some(path) => path,
        None => git_path(&root, "--absolute-git-dir")?,
    };
    let common_raw = git_output(&root, &["rev-parse", "--git-common-dir"], Some(&git_dir))?;
    let common_dir = resolve_git_path(&root, common_raw.trim())?;
    let branch = git_output(
        &root,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        Some(&git_dir),
    )
    .ok()
    .map(|output| output.trim().to_owned())
    .filter(|branch| !branch.is_empty());
    let head = git_output(&root, &["rev-parse", "--verify", "HEAD"], Some(&git_dir))?
        .trim()
        .to_owned();
    Ok(GitCheckout {
        root,
        git_dir,
        common_dir,
        branch,
        head,
    })
}

pub fn registered_worktrees(root: &Path) -> Result<Vec<PathBuf>> {
    registered_worktrees_with_mappings(root, &[])
}

pub fn registered_worktrees_with_mappings(
    root: &Path,
    mappings: &[(PathBuf, PathBuf)],
) -> Result<Vec<PathBuf>> {
    let identity = checkout_with_mappings(root, mappings)?;
    let output = git_output(
        &identity.root,
        &["worktree", "list", "--porcelain"],
        Some(&identity.git_dir),
    )?;
    let mut roots = Vec::new();
    for line in output.lines() {
        let Some(path) = line.strip_prefix("worktree ") else {
            continue;
        };
        let path = PathBuf::from(decode_porcelain_path(path)?);
        let mapped = mappings
            .iter()
            .filter_map(|(host, local)| {
                path.strip_prefix(host)
                    .ok()
                    .map(|relative| (local, relative, host.as_os_str().len()))
            })
            .max_by_key(|(_, _, host_len)| *host_len)
            .map(|(local, relative, _)| local.join(relative))
            .unwrap_or(path);
        roots.push(resolve_git_path(&identity.root, &mapped.to_string_lossy())?);
    }
    roots.sort();
    roots.dedup();
    Ok(roots)
}

/// Resolve HEAD through Git so loose, packed, and detached refs share one path.
pub fn head_stamp(root: &Path) -> String {
    checkout(root)
        .map(|identity| identity.head)
        .unwrap_or_default()
}

fn git_path(root: &Path, argument: &str) -> Result<PathBuf> {
    let output = git_output(root, &["rev-parse", argument], None)?;
    resolve_git_path(root, output.trim())
}

fn mapped_git_dir(root: &Path, mappings: &[(PathBuf, PathBuf)]) -> Result<Option<PathBuf>> {
    let pointer = root.join(".git");
    let metadata = match std::fs::symlink_metadata(&pointer) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(pointer)?;
    let Some(path) = text.trim().strip_prefix("gitdir:") else {
        return Ok(None);
    };
    let path = PathBuf::from(path.trim());
    let path = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    let mapped = mappings
        .iter()
        .filter_map(|(host, local)| {
            path.strip_prefix(host)
                .ok()
                .map(|relative| (local, relative, host.as_os_str().len()))
        })
        .max_by_key(|(_, _, host_len)| *host_len)
        .map(|(local, relative, _)| local.join(relative))
        .unwrap_or(path);
    let git_dir = mapped
        .canonicalize()
        .context("mapped Git directory is unavailable")?;
    let backlink = git_dir.join("gitdir");
    let metadata = std::fs::symlink_metadata(&backlink)
        .context("linked worktree Git directory has no backlink")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 4096 {
        return Err(anyhow!("linked worktree Git backlink is unsafe"));
    }
    let backlink_text = std::fs::read_to_string(backlink)?;
    let backlink_path = PathBuf::from(backlink_text.trim());
    let backlink_path = if backlink_path.is_absolute() {
        backlink_path
    } else {
        git_dir.join(backlink_path)
    };
    let backlink_path = map_embedded_path(&backlink_path, mappings);
    if backlink_path.canonicalize()? != root.join(".git").canonicalize()? {
        return Err(anyhow!(
            "linked worktree Git pointer does not point back to checkout"
        ));
    }
    Ok(Some(git_dir))
}

fn map_embedded_path(path: &Path, mappings: &[(PathBuf, PathBuf)]) -> PathBuf {
    mappings
        .iter()
        .filter_map(|(host, local)| {
            path.strip_prefix(host)
                .ok()
                .map(|relative| (local, relative, host.as_os_str().len()))
        })
        .max_by_key(|(_, _, host_len)| *host_len)
        .map(|(local, relative, _)| local.join(relative))
        .unwrap_or_else(|| path.to_owned())
}

fn resolve_git_path(root: &Path, value: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    let path = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    path.canonicalize()
        .or(Ok::<_, std::io::Error>(path))
        .map_err(|error| anyhow!("resolve Git path: {error}"))
}

fn git_output(root: &Path, arguments: &[&str], git_dir: Option<&Path>) -> Result<String> {
    let mut command = Command::new("git");
    command
        .current_dir(root)
        .args(arguments)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::piped());
    if let Some(git_dir) = git_dir {
        command.env("GIT_DIR", git_dir).env("GIT_WORK_TREE", root);
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("run git {}", arguments.join(" ")))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("Git stdout is unavailable"))?;
    let reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
        Ok::<_, std::io::Error>(bytes)
    });
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() > Duration::from_secs(3) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(anyhow!("Git identity command timed out"));
        }
        thread::sleep(Duration::from_millis(10));
    };
    let bytes = reader
        .join()
        .map_err(|_| anyhow!("Git output reader failed"))??;
    if !status.success() || bytes.len() > 1024 * 1024 {
        return Err(anyhow!(
            "Git identity command failed or exceeded its output bound"
        ));
    }
    String::from_utf8(bytes).context("Git output is not UTF-8")
}

fn decode_porcelain_path(path: &str) -> Result<String> {
    if !path.starts_with('"') {
        return Ok(path.to_owned());
    }
    let bytes = path.as_bytes();
    if bytes.len() < 2 || bytes.last() != Some(&b'"') {
        return Err(anyhow!("invalid quoted Git worktree path"));
    }
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut cursor = 1;
    while cursor + 1 < bytes.len() {
        let byte = bytes[cursor];
        cursor += 1;
        if byte != b'\\' {
            decoded.push(byte);
            continue;
        }
        let escape = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("truncated Git path escape"))?;
        cursor += 1;
        match escape {
            b'\\' | b'"' => decoded.push(escape),
            b'a' => decoded.push(0x07),
            b'b' => decoded.push(0x08),
            b't' => decoded.push(b'\t'),
            b'n' => decoded.push(b'\n'),
            b'v' => decoded.push(0x0b),
            b'f' => decoded.push(0x0c),
            b'r' => decoded.push(b'\r'),
            b'0'..=b'7' => {
                let mut value = u16::from(escape - b'0');
                for _ in 0..2 {
                    let digit = *bytes
                        .get(cursor)
                        .ok_or_else(|| anyhow!("truncated octal Git path escape"))?;
                    if !(b'0'..=b'7').contains(&digit) {
                        return Err(anyhow!("invalid octal Git path escape"));
                    }
                    cursor += 1;
                    value = value * 8 + u16::from(digit - b'0');
                }
                decoded.push(u8::try_from(value).context("invalid Git path byte")?);
            }
            _ => return Err(anyhow!("unsupported Git path escape")),
        }
    }
    String::from_utf8(decoded).context("Git worktree path is not UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_git_quoted_worktree_paths() {
        assert_eq!(
            decode_porcelain_path(r#""/tmp/a\040b\"c""#).unwrap(),
            "/tmp/a b\"c"
        );
    }

    #[test]
    fn resolves_linked_worktree_to_common_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let main = temporary.path().join("main");
        let linked = temporary.path().join("linked worktree");
        std::fs::create_dir(&main).unwrap();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(&main)
            .status()
            .unwrap();
        Command::new("git")
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                "initial",
            ])
            .current_dir(&main)
            .status()
            .unwrap();
        Command::new("git")
            .args([
                "worktree",
                "add",
                "-q",
                "-b",
                "linked",
                linked.to_str().unwrap(),
            ])
            .current_dir(&main)
            .status()
            .unwrap();
        let identity = checkout(&linked).unwrap();
        assert_eq!(identity.branch.as_deref(), Some("linked"));
        assert_eq!(identity.common_dir, checkout(&main).unwrap().common_dir);
        assert!(
            registered_worktrees(&main)
                .unwrap()
                .contains(&linked.canonicalize().unwrap())
        );
    }

    #[test]
    fn maps_host_absolute_worktree_pointers_into_container_paths() {
        let fixture = tempfile::tempdir().unwrap();
        let repository = fixture.path().join("repo");
        let linked = fixture.path().join("linked");
        std::fs::create_dir_all(&repository).unwrap();
        let run = |directory: &Path, arguments: &[&str]| {
            let output = Command::new("git")
                .args(arguments)
                .current_dir(directory)
                .output()
                .unwrap();
            assert!(output.status.success());
        };
        run(&repository, &["init", "-q", "-b", "main"]);
        run(
            &repository,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                "initial",
            ],
        );
        run(
            &repository,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "feature",
                linked.to_str().unwrap(),
            ],
        );
        let pointer = linked.join(".git");
        let admin = std::fs::read_to_string(&pointer).unwrap();
        let local_admin = PathBuf::from(admin.trim().strip_prefix("gitdir:").unwrap().trim());
        let fake_host = fixture.path().join("host-mount");
        let host_admin = fake_host.join(local_admin.strip_prefix(fixture.path()).unwrap());
        std::fs::write(&pointer, format!("gitdir: {}\n", host_admin.display())).unwrap();

        let identity =
            checkout_with_mappings(&linked, &[(fake_host, fixture.path().to_owned())]).unwrap();
        assert_eq!(identity.root, linked.canonicalize().unwrap());
        assert_eq!(
            identity.common_dir,
            repository.join(".git").canonicalize().unwrap()
        );
        assert_eq!(identity.branch.as_deref(), Some("feature"));
    }
}
