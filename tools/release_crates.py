#!/usr/bin/env python3
"""Package and publish the yeetz-s3-kernel crate closure to crates.io.

Usage (from the root of a clean checkout):

    python3 tools/release_crates.py package \
        --source-sha <full-40-hex-sha> --version <version> --output <dir>

    python3 tools/release_crates.py publish \
        --source-sha <full-40-hex-sha> --version <version> --output <dir>

Both modes enforce, before any cargo invocation:

  * ``git``, ``cargo``, and Python >= 3.11 (``tomllib``) exist on the host;
    missing host tools are a hard error, never installed here.
  * ``HEAD`` equals ``--source-sha`` (full 40-hex) and the working tree is
    clean (``git status --porcelain`` empty), re-checked after cargo runs so
    ``--locked`` provably preserved ``Cargo.lock``.
  * ``--version`` matches the workspace ``[workspace.package]`` version and
    every released crate's manifest (literal or ``workspace = true``).
  * ``--output`` lies outside the checkout and is empty at start (the
    workflow passes a fresh ``RUNNER_TEMP/release-<run_id>-<run_attempt>``
    directory per attempt so stale artifacts cannot contaminate the
    exact-four release set).

Both modes run standard Cargo 1.96 packaging with verification retained
(``cargo package --locked --registry crates-io -p ...`` for the four crates
in dependency order) and then inspect every produced archive:

  * package name/version and normalized manifest (internal dependencies
    registry-versioned to the release version; external dependencies pinned
    to the exact ``[workspace.dependencies]`` versions; no ``workspace`` or
    ``path`` references in any dependency section);
  * ``.cargo_vcs_info.json`` carries the exact source SHA under
    ``path_in_vcs`` = ``crates/<name>`` with no dirty-tree marker;
  * every payload file (README, src, tests, ...) is byte-identical to the
    tracked committed source; all tracked ``src/**`` and ``tests/**`` files
    plus ``README.md`` must be present, and ``Cargo.toml.orig`` must equal
    the crate's own ``Cargo.toml``;
  * the archive ``Cargo.lock`` resolves every internal crate from the
    crates.io registry at the release version, draws every package from
    ``registry+https://github.com/rust-lang/crates.io-index`` with a
    checksum, and contains no rigs;
  * the packaged ``LICENSE`` is byte-identical to the repository root
    ``LICENSE``;
  * duplicate, absolute, or dot-dot member paths and non-regular payloads
    are rejected;
  * SHA256 of each ``.crate`` recorded in ``SHA256SUMS`` and
    ``release-manifest.json``.

``publish`` additionally requires that the source SHA is an ancestor of
``origin/main`` and that ``CARGO_REGISTRY_TOKEN`` is present in the
environment (supplied only to the publish job step; never printed or
stored; the publish subprocess receives exactly that one standard key
while every other cargo-token key is dropped, and packaging subprocesses
receive no credentials at all). Publication proceeds
one crate at a time in dependency order (yeetz-sdk-core -> yeetz-sdk-s3 ->
yeetz-s3-kernel -> yeetz-s3-streams):

  * an upload is attempted only when the sparse-index lookup returns a
    trusted "absent"; a failed or malformed lookup fails before any upload
    rather than implying an unused version;
  * a registry version already present is skipped only when its index
    checksum equals the verified local artifact checksum; a mismatch fails
    closed;
  * after each upload, the freshly produced archive (the one this attempt
    created, not the earlier pre-packaging copy) is re-inspected and
    re-hashed, the sparse index is polled (bounded) until the version is
    visible, and its checksum must equal that fresh artifact; the verified
    artifact and its receipt are retained in ``--output``;
  * an upload is never repeated merely because a response was ambiguous --
    ambiguity is resolved against the index, and only exact-checksum
    visibility counts as success;
  * per-crate status (``not-attempted`` / ``unconfirmed`` / ``published`` /
    ``verified-already-present`` / ``verified-after-ambiguous-error``) is
    persisted to ``release-manifest.json`` before and after every attempt
    and on the failure path, so confirmed crates and the untouched tail
    survive any failure.

Failures stop publication, exit non-zero, and leave whatever artifacts were
produced in ``--output`` for diagnosis.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import NoReturn

try:
    import tomllib
except ImportError:  # pragma: no cover - host guard
    print(
        "release_crates: error: this script requires Python >= 3.11 (tomllib)",
        file=sys.stderr,
    )
    sys.exit(1)

# Dependency (and publication) order. rigs/ is never released.
CRATE_ORDER = [
    "yeetz-sdk-core",
    "yeetz-sdk-s3",
    "yeetz-s3-kernel",
    "yeetz-s3-streams",
]
CRATE_DIR = {name: Path("crates") / name for name in CRATE_ORDER}
# Internal workspace dependencies that carry a registry version pin.
INTERNAL_VERSIONED_DEPS = ["yeetz-sdk-core", "yeetz-sdk-s3", "yeetz-s3-kernel"]
INTERNAL_CRATES = set(CRATE_ORDER)

SPARSE_INDEX = "https://index.crates.io"
REGISTRY_NAME = "crates-io"
POST_VERIFY_TIMEOUT_S = 300  # wait for index visibility after a clean upload
AMBIGUOUS_TIMEOUT_S = 180  # wait for index visibility after a failed upload
POLL_INTERVAL_S = 10

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")
# Files cargo generates inside a .crate archive; each is validated by a
# dedicated check rather than against crate-local source bytes. The special
# mappings Cargo.toml.orig (-> the crate's own Cargo.toml) and LICENSE (->
# the repository root LICENSE) are handled explicitly in inspect_archive.
GENERATED_ARCHIVE_FILES = {"Cargo.toml", ".cargo_vcs_info.json", ".cargo_ok",
                           "Cargo.lock"}
# The only dependency source permitted inside a packaged crate's Cargo.lock.
CRATES_IO_REGISTRY_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
# Credential-carrying environment keys, stripped from packaging subprocesses.
TOKEN_ENV_KEYS = (
    "CARGO_REGISTRY_TOKEN",
    "CARGO_REGISTRIES_CRATES_IO_TOKEN",
    "CARGO_TOKEN",
)


def fail(message: str) -> NoReturn:
    print(f"release_crates: error: {message}", file=sys.stderr)
    sys.exit(1)


def info(message: str) -> None:
    print(f"release_crates: {message}")


def run_tool(argv: list[str], *, env: dict[str, str] | None = None) -> str:
    """Run a host tool, echoing its (redacted) output; fail on non-zero."""
    redactions = [
        (os.environ[k], "***")
        for k in TOKEN_ENV_KEYS
        if k in os.environ and os.environ[k]
    ]

    def redact(text: str) -> str:
        for secret, mask in redactions:
            text = text.replace(secret, mask)
        return text

    proc = subprocess.run(argv, capture_output=True, text=True, env=env)
    if proc.stdout:
        print(redact(proc.stdout), end="" if proc.stdout.endswith("\n") else "\n")
    if proc.stderr:
        print(redact(proc.stderr), end="" if proc.stderr.endswith("\n") else "\n",
              file=sys.stderr)
    if proc.returncode != 0:
        fail(f"command failed ({proc.returncode}): {' '.join(argv)}")
    return proc.stdout


def require_host_tools() -> None:
    for tool in ("git", "cargo"):
        if shutil.which(tool) is None:
            fail(f"required host tool '{tool}' is not available on this host; "
                 "install it on the runner (this script installs nothing)")
        version = run_tool([tool, "--version"]).strip()
        info(f"host tool: {version}")


# ---------------------------------------------------------------------------
# Source and version guards
# ---------------------------------------------------------------------------


def guard_source(source_sha: str) -> Path:
    if not SHA_RE.match(source_sha):
        fail(f"--source-sha must be a full 40-hex lowercase SHA (got {source_sha!r})")
    try:
        root = Path(run_tool(["git", "rev-parse", "--show-toplevel"]).strip())
    except SystemExit:
        fail("not inside a git repository")
    head = run_tool(["git", "rev-parse", "HEAD"]).strip()
    if head != source_sha:
        fail(f"HEAD {head} does not match --source-sha {source_sha}")
    status = run_tool(["git", "status", "--porcelain"]).strip()
    if status:
        fail(f"working tree is not clean:\n{status}")
    return root


def guard_clean_unchanged(root: Path, source_sha: str) -> None:
    head = run_tool(["git", "rev-parse", "HEAD"]).strip()
    if head != source_sha:
        fail(f"HEAD moved during the run ({head} != {source_sha})")
    status = run_tool(["git", "status", "--porcelain"]).strip()
    if status:
        fail("working tree changed during the run (Cargo.lock or sources modified):\n"
             f"{status}")


def guard_merged_into_main(source_sha: str) -> None:
    probe = subprocess.run(
        ["git", "rev-parse", "--verify", "refs/remotes/origin/main"],
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0:
        fail("cannot resolve refs/remotes/origin/main; publish requires a "
             f"full checkout (fetch-depth 0): {probe.stderr.strip()}")
    origin_main = probe.stdout.strip()
    probe = subprocess.run(
        ["git", "merge-base", "--is-ancestor", source_sha, origin_main],
        capture_output=True,
        text=True,
    )
    if probe.returncode == 0:
        return
    if probe.returncode == 1:
        fail(f"publish source {source_sha} is not an ancestor of origin/main "
             f"({origin_main}); publish only from merged commits")
    fail(f"git merge-base failed ({probe.returncode}): {probe.stderr.strip()}")


def guard_versions(root: Path, version: str) -> None:
    ws_manifest = load_toml(root / "Cargo.toml")
    ws_version = ws_manifest.get("workspace", {}).get("package", {}).get("version")
    if ws_version != version:
        fail(f"workspace [workspace.package] version is {ws_version!r}, "
             f"expected {version!r}")
    for name in CRATE_ORDER:
        manifest = load_toml(root / CRATE_DIR[name] / "Cargo.toml")
        declared = manifest.get("package", {}).get("version")
        if isinstance(declared, str):
            if declared != version:
                fail(f"{name} declares version {declared!r}, expected {version!r}")
        elif isinstance(declared, dict) and declared.get("workspace") is True:
            pass  # inherits the (already-checked) workspace version
        else:
            fail(f"{name} has unusable version declaration {declared!r}")
    deps = ws_manifest.get("workspace", {}).get("dependencies", {})
    for name in INTERNAL_VERSIONED_DEPS:
        entry = deps.get(name)
        if isinstance(entry, dict) and "version" in entry:
            if entry["version"] != version:
                fail(f"workspace dependency {name} pins version "
                     f"{entry['version']!r}, expected {version!r}")
        elif not isinstance(entry, dict) or "version" not in entry:
            fail(f"workspace dependency {name} carries no registry version pin; "
                 "published dependents could not resolve it")


def load_toml(path: Path) -> dict:
    try:
        with open(path, "rb") as fh:
            return tomllib.load(fh)
    except FileNotFoundError:
        fail(f"required manifest is missing: {path}")
    except tomllib.TOMLDecodeError as exc:
        fail(f"cannot parse {path}: {exc}")


def guard_output_dir(output: Path, root: Path) -> Path:
    output = output.resolve()
    if output == root or output.is_relative_to(root):
        fail(f"--output must be outside the checkout (got {output} inside {root})")
    if output.exists() and any(output.iterdir()):
        fail(f"output directory {output} is not empty; stale artifacts would "
             "contaminate the exact-four release set -- use a fresh directory "
             "per run/attempt (the workflow passes "
             "$RUNNER_TEMP/release-<run_id>-<run_attempt>)")
    output.mkdir(parents=True, exist_ok=True)
    return output


# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------


def credential_free_env() -> dict[str, str]:
    """The normal build environment with every cargo credential key removed."""
    return {k: v for k, v in os.environ.items() if k not in TOKEN_ENV_KEYS}


def package_env() -> dict[str, str]:
    """Environment for cargo package: credentials stripped."""
    return credential_free_env()


def publish_env() -> dict[str, str]:
    """Environment for cargo publish: the normal build environment carrying
    exactly one credential -- the step-injected standard CARGO_REGISTRY_TOKEN.
    Inherited alternative cargo-token keys are dropped, not forwarded."""
    env = credential_free_env()
    token = os.environ.get("CARGO_REGISTRY_TOKEN", "")
    if token:
        env["CARGO_REGISTRY_TOKEN"] = token
    return env


def stage_packages(root: Path, version: str, source_sha: str,
                   output: Path) -> list[dict]:
    build_dir = output / "build"
    argv = ["cargo", "package", "--locked", "--registry", REGISTRY_NAME]
    for name in CRATE_ORDER:
        argv += ["-p", name]
    argv += ["--target-dir", str(build_dir)]
    info(f"packaging with verification: {' '.join(argv)}")
    run_tool(argv, env=package_env())
    guard_clean_unchanged(root, source_sha)

    package_dir = build_dir / "package"
    entries: list[dict] = []
    for name in CRATE_ORDER:
        file_name = f"{name}-{version}.crate"
        src = package_dir / file_name
        if not src.is_file():
            fail(f"cargo package produced no {file_name} in {package_dir}")
        dest = output / file_name
        shutil.copy2(src, dest)
        data = dest.read_bytes()
        inspect_archive(dest, name, version, root, source_sha)
        entries.append({
            "name": name,
            "version": version,
            "file": file_name,
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
        })
        info(f"verified {file_name} (sha256 {entries[-1]['sha256']})")

    write_checksums(output, entries)
    write_manifest(output, version, source_sha, entries)
    return entries


def unsafe_member_relpath(relpath: str) -> bool:
    """Absolute, backslash-bearing, dot/dot-dot, or empty-segment paths."""
    if not relpath or relpath.startswith("/") or "\\" in relpath:
        return True
    return any(part in ("", ".", "..") for part in relpath.split("/"))


def git_tracked_files(root: Path, name: str) -> list[str]:
    """Repository-relative tracked files under crates/<name>."""
    proc = subprocess.run(["git", "ls-files", "--", str(CRATE_DIR[name])],
                          capture_output=True, text=True, cwd=root)
    if proc.returncode != 0:
        fail(f"git ls-files failed for {CRATE_DIR[name]}: {proc.stderr.strip()}")
    return [line for line in proc.stdout.splitlines() if line.strip()]


def expected_external_pins(root: Path) -> dict[str, str]:
    """name -> pinned version from [workspace.dependencies].

    Internal crates are excluded: their normalized pins are checked against
    the release version instead.
    """
    ws = load_toml(root / "Cargo.toml")
    pins: dict[str, str] = {}
    for dep, entry in ws.get("workspace", {}).get("dependencies", {}).items():
        if dep in INTERNAL_CRATES:
            continue
        if isinstance(entry, str):
            pins[dep] = entry
        elif isinstance(entry, dict) and isinstance(entry.get("version"), str):
            pins[dep] = entry["version"]
    return pins


def inspect_archive_lock(label: str, payload: bytes, name: str,
                         version: str) -> None:
    """Validate the Cargo.lock cargo generated inside the .crate archive."""
    try:
        lock = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        fail(f"{label}: cannot parse archive Cargo.lock: {exc}")
    packages = lock.get("package", [])
    if not isinstance(packages, list) or not packages:
        fail(f"{label}: archive Cargo.lock has no [[package]] entries")
    roots = [p for p in packages
             if isinstance(p, dict) and p.get("name") == name]
    if len(roots) != 1:
        fail(f"{label}: archive Cargo.lock must list {name} exactly once")
    if roots[0].get("version") != version:
        fail(f"{label}: archive Cargo.lock pins {name} at "
             f"{roots[0].get('version')!r}, expected {version!r}")
    if "source" in roots[0] or "checksum" in roots[0]:
        fail(f"{label}: released crate's own Cargo.lock entry must carry no "
             "source or checksum")
    for pkg in packages:
        if not isinstance(pkg, dict):
            fail(f"{label}: unusable Cargo.lock package entry")
        pkg_name = pkg.get("name")
        if pkg_name == "yeetz-rigs":
            fail(f"{label}: rigs leaked into the archive Cargo.lock")
        if pkg_name == name:
            continue
        if pkg_name in INTERNAL_CRATES and pkg.get("version") != version:
            fail(f"{label}: internal package {pkg_name} locked at "
                 f"{pkg.get('version')!r}, expected {version!r}")
        source = pkg.get("source")
        if source != CRATES_IO_REGISTRY_SOURCE:
            fail(f"{label}: package {pkg_name} has source {source!r}; only "
                 f"'{CRATES_IO_REGISTRY_SOURCE}' is allowed (no path, git, or "
                 "other-registry dependencies)")
        checksum = pkg.get("checksum")
        if not isinstance(checksum, str) or not checksum:
            fail(f"{label}: package {pkg_name} has no registry checksum")


def inspect_archive(archive: Path, name: str, version: str, root: Path,
                    source_sha: str) -> None:
    prefix = f"{name}-{version}/"
    crate_dir = root / CRATE_DIR[name]
    crate_prefix = f"{CRATE_DIR[name]}/"
    tracked = set(git_tracked_files(root, name))
    files: dict[str, bytes] = {}
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            if member.isdir():
                continue
            if not member.name.startswith(prefix):
                fail(f"{archive.name}: member {member.name!r} outside {prefix!r}")
            relpath = member.name[len(prefix):]
            if unsafe_member_relpath(relpath):
                fail(f"{archive.name}: unsafe member path {member.name!r}")
            if relpath in files:
                fail(f"{archive.name}: duplicate member path {member.name!r}")
            if not member.isfile():
                fail(f"{archive.name}: non-regular member {member.name!r} "
                     f"(tar type {member.type!r})")
            handle = tar.extractfile(member)
            if handle is None:
                fail(f"{archive.name}: no payload for member {member.name!r}")
            files[relpath] = handle.read()

    if not files:
        fail(f"{archive.name}: archive is empty")

    # Every payload byte is accounted for: cargo-generated files by their
    # dedicated checks below; Cargo.toml.orig and LICENSE by their special
    # mappings; everything else by byte identity with a tracked file at the
    # same crate-relative path.
    for relpath, payload in files.items():
        if relpath in GENERATED_ARCHIVE_FILES or relpath in ("Cargo.toml.orig",
                                                             "LICENSE"):
            continue
        if f"{crate_prefix}{relpath}" not in tracked:
            fail(f"{archive.name}: archive contains {relpath!r} with no tracked "
                 "counterpart in the checkout")
        if (crate_dir / relpath).read_bytes() != payload:
            fail(f"{archive.name}: {relpath!r} differs from committed source bytes")

    # Required payload: all tracked src/** and tests/** files plus README.md.
    required = [path[len(crate_prefix):] for path in tracked
                if path[len(crate_prefix):] == "README.md"
                or path[len(crate_prefix):].startswith(("src/", "tests/"))]
    for relpath in required:
        if relpath not in files:
            fail(f"{archive.name}: required file {relpath!r} is missing from "
                 "the archive")

    # The verbatim original manifest.
    orig = files.get("Cargo.toml.orig")
    if orig is None:
        fail(f"{archive.name}: Cargo.toml.orig is missing")
    if orig != (crate_dir / "Cargo.toml").read_bytes():
        fail(f"{archive.name}: Cargo.toml.orig does not match the source manifest")

    # VCS provenance: exact source SHA, the crate's repository path, and a
    # clean (non-dirty) packaging tree.
    vcs_raw = files.get(".cargo_vcs_info.json")
    if vcs_raw is None:
        fail(f"{archive.name}: .cargo_vcs_info.json is missing")
    try:
        vcs = json.loads(vcs_raw)
    except json.JSONDecodeError as exc:
        fail(f"{archive.name}: cannot parse .cargo_vcs_info.json: {exc}")
    if not isinstance(vcs, dict):
        fail(f"{archive.name}: .cargo_vcs_info.json is not an object")
    if vcs.get("path_in_vcs") != f"crates/{name}":
        fail(f"{archive.name}: .cargo_vcs_info.json path_in_vcs is "
             f"{vcs.get('path_in_vcs')!r}, expected 'crates/{name}'")
    git_info = vcs.get("git")
    if not isinstance(git_info, dict):
        fail(f"{archive.name}: .cargo_vcs_info.json has no git object")
    if git_info.get("sha1") != source_sha:
        fail(f"{archive.name}: .cargo_vcs_info.json sha1 is "
             f"{git_info.get('sha1')!r}, expected {source_sha}")
    if git_info.get("dirty") not in (None, False):
        fail(f"{archive.name}: packaged from a dirty working tree "
             f"(git.dirty = {git_info.get('dirty')!r})")

    # Normalized manifest: identity, license-file, dependency normalization.
    if "Cargo.toml" not in files:
        fail(f"{archive.name}: normalized Cargo.toml is missing")
    normalized = load_toml_bytes(archive.name, files["Cargo.toml"])
    package = normalized.get("package", {})
    if package.get("name") != name:
        fail(f"{archive.name}: normalized manifest names {package.get('name')!r}")
    if package.get("version") != version:
        fail(f"{archive.name}: normalized manifest version is "
             f"{package.get('version')!r}, expected {version!r}")
    ws_pkg = load_toml(root / "Cargo.toml").get("workspace", {}).get("package", {})
    for key in ("license", "license-file"):
        expected = ws_pkg.get(key)
        if key not in package or package.get(key) != expected:
            fail(f"{archive.name}: normalized {key} is "
                 f"{package.get(key)!r}, expected the workspace value "
                 f"{expected!r} (key must be present)")
    pins = expected_external_pins(root)
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        for dep_name, entry in normalized.get(section, {}).items():
            declared: str | None
            if isinstance(entry, str):
                declared = entry
                fields: dict = {}
            elif isinstance(entry, dict):
                declared = entry.get("version")
                fields = entry
            else:
                fail(f"{archive.name}: unusable [{section}] entry for {dep_name}")
            if "workspace" in fields or "path" in fields:
                fail(f"{archive.name}: [{section}] entry {dep_name} retains a "
                     "workspace or path reference")
            if dep_name in INTERNAL_CRATES:
                if declared != version:
                    fail(f"{archive.name}: internal dependency {dep_name} in "
                         f"[{section}] resolves to {declared!r}, expected "
                         f"{version!r}")
            elif dep_name in pins:
                if declared != pins[dep_name]:
                    fail(f"{archive.name}: external dependency {dep_name} in "
                         f"[{section}] resolves to {declared!r}, expected the "
                         f"workspace pin {pins[dep_name]!r}")
            else:
                fail(f"{archive.name}: dependency {dep_name} in [{section}] is "
                     "neither an internal crate nor a pinned workspace "
                     "dependency")

    # The generated Cargo.lock: registry-only resolution, checksummed, no
    # rigs, internal crates at the release version.
    lock_raw = files.get("Cargo.lock")
    if lock_raw is None:
        fail(f"{archive.name}: Cargo.lock is missing from the archive")
    inspect_archive_lock(archive.name, lock_raw, name, version)

    # License bytes must be the repository root LICENSE, byte for byte.
    license_bytes = files.get("LICENSE")
    if license_bytes is None:
        fail(f"{archive.name}: LICENSE is missing from the archive")
    if license_bytes != (root / "LICENSE").read_bytes():
        fail(f"{archive.name}: packaged LICENSE differs from the root LICENSE")


def load_toml_bytes(label: str, payload: bytes) -> dict:
    try:
        return tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        fail(f"{label}: cannot parse normalized Cargo.toml: {exc}")


def write_checksums(output: Path, entries: list[dict]) -> None:
    lines = [f"{e['sha256']}  {e['file']}\n" for e in entries]
    (output / "SHA256SUMS").write_text("".join(lines), encoding="utf-8")


def write_manifest(output: Path, version: str, source_sha: str,
                   entries: list[dict], publish: dict | None = None) -> None:
    manifest = {
        "schema": 1,
        "mode": "publish" if publish else "package",
        "version": version,
        "source_sha": source_sha,
        "crates": entries,
    }
    if publish:
        manifest["publish"] = publish
    path = output / "release-manifest.json"
    tmp = output / "release-manifest.json.tmp"
    tmp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)  # atomic: readers never see a torn manifest


# ---------------------------------------------------------------------------
# Sparse index lookups
# ---------------------------------------------------------------------------


def sparse_index_url(name: str) -> str:
    if len(name) == 1:
        return f"{SPARSE_INDEX}/1/{name}"
    if len(name) == 2:
        return f"{SPARSE_INDEX}/2/{name}"
    if len(name) == 3:
        return f"{SPARSE_INDEX}/3/{name[0]}/{name[1:]}"
    return f"{SPARSE_INDEX}/{name[:2]}/{name[2:4]}/{name}"


def index_lookup(name: str, version: str) -> tuple[str, str | None]:
    """Return ("present", cksum), ("absent", None), or ("error", detail).

    Malformed index data (bad JSON, a record naming another crate, an
    unusable checksum) is an error, never an absence: an untrusted lookup
    is never treated as an unused version.
    """
    try:
        with urllib.request.urlopen(sparse_index_url(name), timeout=30) as resp:
            body = resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return "absent", None
        return "error", f"HTTP {exc.code}"
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        return "error", f"{type(exc).__name__}: {exc}"
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        return "error", f"index response is not UTF-8: {exc}"
    cksum: str | None = None
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            return "error", f"malformed index JSON: {exc}"
        if not isinstance(record, dict):
            return "error", "index record is not a JSON object"
        record_name = record.get("name")
        if record_name is not None and record_name != name:
            return "error", (f"index record names {record_name!r}, "
                             f"expected {name!r}")
        if record.get("vers") == version:
            if cksum is not None:
                return "error", (f"duplicate sparse-index records for "
                                 f"{version}; refusing to pick one even if "
                                 "checksums agree")
            value = record.get("cksum")
            if not isinstance(value, str) or not value:
                return "error", (f"index record for {version} has no usable "
                                 "cksum")
            cksum = value
    if cksum is None:
        return "absent", None
    return "present", cksum


def wait_for_index_cksum(name: str, version: str,
                         timeout_s: int) -> tuple[str, str | None]:
    """Poll the index until the version is visible or the bound expires.

    Returns ("visible", cksum) or ("timeout", None). Network errors during
    polling do not abort the wait; only the bound does.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        state, cksum = index_lookup(name, version)
        if state == "present":
            return "visible", cksum
        if time.monotonic() >= deadline:
            return "timeout", None
        time.sleep(POLL_INTERVAL_S)


def check_cksum(label: str, crate: str, version: str, cksum: str | None,
                expected: str) -> None:
    if cksum != expected:
        fail(f"{label}: sparse-index checksum for {crate} {version} is {cksum!r}, "
             f"expected the verified artifact checksum {expected!r}")


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------


def guard_registry_token() -> str:
    token = os.environ.get("CARGO_REGISTRY_TOKEN", "")
    if not token:
        fail("CARGO_REGISTRY_TOKEN is not set; publish requires it "
             "(supply it only to the publish step via secrets)")
    return token


def publish_crates(root: Path, version: str, source_sha: str, output: Path,
                   entries: list[dict]) -> None:
    build_dir = output / "build"
    statuses = {entry["name"]: "not-attempted" for entry in entries}
    retention = {entry["name"]: "untouched" for entry in entries}

    def persist() -> None:
        write_manifest(output, version, source_sha, entries, publish={
            "registry": REGISTRY_NAME,
            "results": [{"name": entry["name"], "status": statuses[entry["name"]]}
                        for entry in entries],
            "retention": dict(retention),
        })

    persist()  # the record starts with every crate not-attempted
    try:
        for entry in entries:
            name, packaged_cksum = entry["name"], entry["sha256"]

            # An upload is attempted only on a trusted "absent"; an errored
            # or malformed lookup never implies an unused version.
            state, detail = index_lookup(name, version)
            if state == "error":
                fail(f"sparse-index lookup for {name} {version} failed "
                     f"({detail}); refusing to upload on an untrusted lookup")
            if state == "present":
                check_cksum("pre-check", name, version, detail, packaged_cksum)
                statuses[name] = "verified-already-present"
                retention[name] = "retained"  # output copy is registry-verified
                persist()
                info(f"{name} {version} already on {REGISTRY_NAME} with the "
                     "verified checksum; skipping upload")
                continue

            # Rotate any earlier package output away so the archive examined
            # below is provably the one this publish attempt produced.
            fresh = build_dir / "package" / entry["file"]
            if fresh.exists():
                fresh.unlink()

            statuses[name] = "unconfirmed"  # attempted, outcome pending
            persist()
            argv = ["cargo", "publish", "--locked", "--registry", REGISTRY_NAME,
                    "-p", name, "--target-dir", str(build_dir)]
            info(f"publishing: {' '.join(argv)}")
            proc = subprocess.run(argv, capture_output=True, text=True,
                                  env=publish_env())
            for stream, sink in ((proc.stdout, sys.stdout),
                                 (proc.stderr, sys.stderr)):
                if stream:
                    redacted = stream
                    for key in TOKEN_ENV_KEYS:
                        secret = os.environ.get(key, "")
                        if secret:
                            redacted = redacted.replace(secret, "***")
                    print(redacted,
                          end="" if redacted.endswith("\n") else "\n",
                          file=sink)

            # Reinspect and hash the archive cargo publish actually produced;
            # never claim equality against the earlier pre-packaging copy.
            if not fresh.is_file():
                fail(f"publish attempt for {name} {version} (cargo exit "
                     f"{proc.returncode}) left no fresh archive at {fresh}; "
                     "the produced artifact cannot be examined")
            inspect_archive(fresh, name, version, root, source_sha)
            fresh_data = fresh.read_bytes()
            fresh_cksum = hashlib.sha256(fresh_data).hexdigest()

            visibility, cksum = wait_for_index_cksum(
                name, version,
                POST_VERIFY_TIMEOUT_S if proc.returncode == 0
                else AMBIGUOUS_TIMEOUT_S)
            if visibility != "visible":
                if proc.returncode == 0:
                    fail(f"published {name} {version} but it was not visible "
                         f"in the sparse index within {POST_VERIFY_TIMEOUT_S}s; "
                         "not reporting success and not retrying the upload "
                         "(status remains 'unconfirmed')")
                fail(f"publish of {name} {version} failed (cargo exit "
                     f"{proc.returncode}) and the version was not visible in "
                     f"the sparse index within {AMBIGUOUS_TIMEOUT_S}s; "
                     "publication stops and the upload is not retried "
                     "(status remains 'unconfirmed')")
            check_cksum("post-verify", name, version, cksum, fresh_cksum)

            # Registry confirmation first: persist the confirmed receipt
            # (atomic manifest replacement) before any fallible artifact
            # retention, so a known registry confirmation is never left
            # unconfirmed.
            entry["sha256"] = fresh_cksum
            entry["size"] = len(fresh_data)
            statuses[name] = ("published" if proc.returncode == 0
                              else "verified-after-ambiguous-error")
            persist()
            info(f"confirmed receipt: {name} {version} status={statuses[name]} "
                 f"registry checksum={fresh_cksum}")
            # Artifact retention is fallible and tracked separately from the
            # registry receipt; it never flips a confirmed status back.
            try:
                shutil.copy2(fresh, output / entry["file"])
                write_checksums(output, entries)
                retention[name] = "retained"
            except OSError as exc:
                retention[name] = f"failed: {exc}"
                print(f"release_crates: warning: {name} {version} is confirmed "
                      f"on {REGISTRY_NAME} but artifact retention failed: {exc}",
                      file=sys.stderr)
            persist()

        guard_clean_unchanged(root, source_sha)
        broken = [crate for crate, state in retention.items()
                  if state.startswith("failed")]
        if broken:
            fail("artifact retention failed for " + ", ".join(sorted(broken))
                 + "; registry statuses are recorded in release-manifest.json "
                 "and are not affected")
    finally:
        persist()  # failure path keeps confirmed crates and the untouched tail


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="release_crates.py",
        description="Package (and optionally publish) the four release crates.",
    )
    parser.add_argument("mode", choices=["package", "publish"])
    parser.add_argument("--source-sha", required=True,
                        help="full 40-hex commit SHA the checkout must match")
    parser.add_argument("--version", required=True,
                        help="exact release version (must match every manifest)")
    parser.add_argument("--output", required=True,
                        help="artifact directory outside the checkout "
                             "(workflow uses $RUNNER_TEMP/release)")
    args = parser.parse_args(argv)

    if not SEMVER_RE.match(args.version):
        fail(f"--version must be an exact semver like 0.5.0 (got {args.version!r})")

    sha = args.source_sha.lower()
    require_host_tools()
    root = guard_source(sha)
    output = guard_output_dir(Path(args.output), root)
    guard_versions(root, args.version)
    if args.mode == "publish":
        guard_merged_into_main(sha)
        guard_registry_token()

    entries = stage_packages(root, args.version, sha, output)

    if args.mode == "publish":
        publish_crates(root, args.version, sha, output, entries)

    info(f"{args.mode} complete: {len(entries)} crates at {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
