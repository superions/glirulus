#!/usr/bin/env python3
"""Validate one committed Glirulus cohort; never publish or alter registry state."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tomllib


VERSION = "1.0.0-glirulus.1"
REPOSITORY = "https://github.com/superions/glirulus"
UPSTREAM_REVISION = "7e6318225398ad7a418efb866156d2cc327ee43f"
ARCHIVES = {
    "glirulus-core": "59026c3d3594059fa7314c11edf35a5cc489acf8a2a022904dfcafd46248ad28",
    "glirulus-persistence": "6e66b5bad8268ae6717213078e391ff2b9488ea3aaac3e18a87ce9d1744e5c6a",
    "glirulus-macros": "f0a8982ba58b85f72e3e5f1c1d9da1ff23bcd7ee37106dd11c5fa95357d2ade8",
    "glirulus-runtime": "fb257d3c08a51588607ea53dba9d6cdd0724c3ce5ff814f1ba35c39e5f471eb3",
    "glirulus-postgres": "42bb11cb0ed558787c59517d12819c1213261ed1b7bd498ec358868be53f3633",
}
SIBLINGS = {
    "glirulus-core": set(),
    "glirulus-persistence": {"glirulus-core"},
    "glirulus-macros": set(),
    "glirulus-runtime": {"glirulus-core", "glirulus-persistence", "glirulus-macros"},
    "glirulus-postgres": {"glirulus-core", "glirulus-persistence"},
}
MIT_GRANT = """Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE."""
ROOT = Path(__file__).resolve().parent.parent


class InvalidCohort(Exception):
    """A bounded validation failure safe to report without credential details."""


def require(condition, message):
    if not condition:
        raise InvalidCohort(message)


def git(*args):
    result = subprocess.run(
        ["git", "-C", str(ROOT), *args], capture_output=True, timeout=30, check=False
    )
    require(result.returncode == 0, "Git could not read the selected committed source")
    return result.stdout


def unchanged_checkpoint(revision):
    require(git("rev-parse", "HEAD").decode().strip() == revision, "Release HEAD changed during validation")
    require(not git("status", "--porcelain", "--untracked-files=all"), "Release requires a clean committed checkout")


def target_path(path):
    target = (ROOT / "target").resolve()
    require(target == ROOT / "target", "Checkout target must not point at another cache")
    path = Path(path)
    if not path.is_absolute():
        path = ROOT / path
    path = path.resolve()
    require(path.is_relative_to(target) and path != target, "Evidence belongs under checkout target/")
    return path


def write_json(path, data, immutable=False):
    path = target_path(path)
    content = (json.dumps(data, indent=2, sort_keys=True) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    if immutable:
        try:
            with path.open("xb") as handle:
                handle.write(content)
        except FileExistsError:
            require(path.read_bytes() == content, "Reviewed package bytes changed for this release revision")
    else:
        path.write_bytes(content)


def dependencies(manifest):
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        for name, spec in manifest.get(section, {}).items():
            yield section, name, spec
    for target in manifest.get("target", {}).values():
        yield from dependencies(target)


def validate_manifest(name, manifest, packaged=False):
    package = manifest.get("package", {})
    require(package.get("name") == name and package.get("version") == VERSION, f"Wrong identity/version: {name}")
    require(manifest.get("lib", {}).get("name") == name.replace("-", "_"), f"Wrong library identity: {name}")
    require(package.get("repository") == REPOSITORY and package.get("homepage") == REPOSITORY, f"Wrong repository metadata: {name}")
    require(package.get("documentation") == f"https://docs.rs/{name}", f"Wrong documentation metadata: {name}")
    require(package.get("license") == "MIT" and package.get("readme") == "README.md", f"Wrong legal/README metadata: {name}")
    require("Glirulus" in package.get("description", ""), f"Wrong product description: {name}")
    require(not manifest.get("patch") and not manifest.get("replace"), f"Unexpected source replacement: {name}")
    seen = set()
    for section, dependency, spec in dependencies(manifest):
        spec = {"version": spec} if isinstance(spec, str) else spec
        actual = spec.get("package", dependency)
        require(not actual.startswith("sayiir") and not dependency.startswith("sayiir"), f"Upstream package edge: {name}")
        require("git" not in spec, f"Git dependency: {name}")
        require("path" not in spec or dependency in ARCHIVES, f"Unexpected local dependency: {name}")
        if actual.startswith("glirulus") or dependency.startswith("glirulus"):
            require(dependency in ARCHIVES and actual == dependency, f"Engine package alias: {name}")
            require(spec.get("version") == f"={VERSION}", f"Non-exact sibling version: {name} -> {dependency}")
            require("registry" not in spec and "registry-index" not in spec, f"Alternate sibling registry: {name}")
            if packaged:
                require("path" not in spec, f"Packaged sibling path: {name} -> {dependency}")
            else:
                require(spec.get("path") == f"../{dependency}", f"Wrong sibling source path: {name} -> {dependency}")
            if section == "dependencies":
                seen.add(dependency)
    require(seen == SIBLINGS[name], f"Incomplete sibling graph: {name}")


def validate_legal(name, files):
    license_text = files["LICENSE"].decode()
    normalized = " ".join(license_text.split())
    require(" ".join(MIT_GRANT.split()) in normalized, f"Incomplete MIT grant: {name}")
    for author in ("Copyright (c) 2026 Sayiir", "Copyright (c) 2026 Daniel Scharf"):
        require(author in license_text, f"Missing license attribution: {name}")
    notice = files["NOTICE"].decode()
    original = name.replace("glirulus-", "sayiir-")
    for text in (original, ARCHIVES[name], UPSTREAM_REVISION, "MIT", "Daniel Scharf", "Sayiir"):
        require(text in notice, f"Incomplete source notice: {name}")
    require(name in files["README.md"].decode(), f"README does not identify its package: {name}")


def source(revision):
    require(revision == "HEAD" or re.fullmatch(r"[0-9a-fA-F]{40}", revision), "Select HEAD or the full reviewed commit")
    revision = git("rev-parse", "--verify", f"{revision}^{{commit}}").decode().strip()
    require(re.fullmatch(r"[0-9a-f]{40}", revision), "Expected a full release commit")
    unchanged_checkpoint(revision)
    tree = {}
    for record in git("ls-tree", "-rz", revision).split(b"\0"):
        if record:
            info, path = record.split(b"\t", 1)
            mode, kind, oid = info.decode().split()
            require(kind == "blob", "Release source must not contain submodules")
            tree[path.decode()] = (mode, oid)

    def committed(path):
        require(path in tree, f"Missing committed release input: {path}")
        mode, oid = tree[path]
        require(mode in ("100644", "100755"), f"Unexpected symlink/source mode: {path}")
        data = git("cat-file", "blob", oid)
        local = ROOT / path
        require(local.is_file() and not local.is_symlink() and local.read_bytes() == data, f"Source changed during review: {path}")
        return data

    for path in ("README.md", "UPSTREAM.md", "LICENSE", "scripts/release.sh", "scripts/release-cohort.py"):
        committed(path)
    workspace = tomllib.loads(committed("Cargo.toml").decode())
    require(set(workspace.get("workspace", {}).get("members", [])) == {f"crates/{name}" for name in ARCHIVES}, "Workspace must contain the five selected packages")
    require(not workspace.get("patch") and not workspace.get("replace"), "Workspace source replacements are forbidden")
    require(not workspace["workspace"].get("dependencies"), "Use explicit auditable sibling edges")
    lock = tomllib.loads(committed("Cargo.lock").decode())
    engines = []
    for package in lock.get("package", []):
        require(not package["name"].startswith("sayiir"), "Upstream engine remains in Cargo.lock")
        require(not package.get("source", "").startswith("git+"), "Git source remains in Cargo.lock")
        if package["name"].startswith("glirulus"):
            require(package["name"] in ARCHIVES and package["version"] == VERSION and "source" not in package, "Wrong locked engine identity/source")
            engines.append(package["name"])
    require(sorted(engines) == sorted(ARCHIVES), "Cargo.lock must have one copy of every engine package")
    packages = {}
    for name in ARCHIVES:
        prefix = f"crates/{name}/"
        files = {path[len(prefix):]: committed(path) for path in tree if path.startswith(prefix)}
        require("Cargo.toml.orig" not in files and ".cargo_vcs_info.json" not in files, f"Stale upstream package metadata: {name}")
        for required in ("Cargo.toml", "README.md", "LICENSE", "NOTICE", "src/lib.rs"):
            require(required in files, f"Missing {required}: {name}")
        manifest = tomllib.loads(files["Cargo.toml"].decode())
        validate_manifest(name, manifest)
        validate_legal(name, files)
        includes = manifest["package"].get("include", [])
        require({"src/**", "README.md", "LICENSE", "NOTICE"}.issubset(includes), f"Incomplete package inclusion: {name}")
        if name == "glirulus-postgres":
            require("migrations/**" in includes and "tests/**" in includes, "PostgreSQL migration/test packaging is incomplete")
            migrations = sorted(path for path in files if path.startswith("migrations/") and path.endswith(".sql"))
            require(len(migrations) == 10 and [int(Path(p).name.split("_")[0]) for p in migrations] == list(range(1, 11)), "Expected PostgreSQL migrations 001 through 010")
            require(any(t.get("name") == "integration" and t.get("path") == "tests/integration.rs" for t in manifest.get("test", [])), "PostgreSQL integration target is not explicit")
        if name == "glirulus-runtime":
            require("tests/**" in includes, "Runtime consumer test must be packaged")
            require(any(t.get("name") == "macros" and t.get("path") == "tests/macros.rs" and set(t.get("required-features", [])) == {"json", "macros"} for t in manifest.get("test", [])), "Runtime macro consumer target is not explicit")
        packages[name] = files
    unchanged_checkpoint(revision)
    return revision, packages


def package_cohort(revision, package_dir):
    revision, sources = source(revision)
    package_dir = Path(package_dir).resolve()
    cohort = {"release_commit": revision, "version": VERSION, "registry_verified": False, "packages": []}
    generated = {"Cargo.toml", "Cargo.toml.orig", ".cargo_vcs_info.json", "Cargo.lock"}
    for name, files in sources.items():
        archive = package_dir / f"{name}-{VERSION}.crate"
        require(archive.is_file() and not archive.is_symlink(), f"Missing package archive: {name}")
        packed = {}
        with tarfile.open(archive, "r:gz") as tar:
            for member in tar:
                path = PurePosixPath(member.name)
                require(path.parts and not path.is_absolute() and ".." not in path.parts and path.parts[0] == f"{name}-{VERSION}", f"Unsafe archive path: {name}")
                if member.isdir():
                    continue
                require(member.isfile() and member.size <= 16 * 1024 * 1024, f"Unexpected archive entry: {name}")
                relative = str(PurePosixPath(*path.parts[1:]))
                require(relative not in packed, f"Duplicate archive entry: {name}")
                packed[relative] = tar.extractfile(member).read()
        for required in generated | {"LICENSE", "NOTICE", "README.md", "src/lib.rs"}:
            require(required in packed, f"Missing packaged {required}: {name}")
        validate_manifest(name, tomllib.loads(packed["Cargo.toml"].decode()), packaged=True)
        validate_legal(name, packed)
        locked_engines = []
        for package in tomllib.loads(packed["Cargo.lock"].decode()).get("package", []):
            dependency = package["name"]
            require(not dependency.startswith("sayiir"), f"Upstream package in archive lock: {name}")
            require(not package.get("source", "").startswith("git+"), f"Git source in archive lock: {name}")
            if dependency.startswith("glirulus"):
                require(dependency in ARCHIVES and package["version"] == VERSION, f"Wrong engine version in archive lock: {name}")
                registry_source = package.get("source") == "registry+https://github.com/rust-lang/crates.io-index"
                require(registry_source or (dependency == name and "source" not in package), f"Non-registry sibling in archive lock: {name} -> {dependency}")
                locked_engines.append(dependency)
        require(name in locked_engines and len(locked_engines) == len(set(locked_engines)), f"Duplicate or missing engine in archive lock: {name}")
        require(packed["Cargo.toml.orig"] == files["Cargo.toml"], f"Packaged authoring manifest differs from release commit: {name}")
        vcs = json.loads(packed[".cargo_vcs_info.json"])
        # Cargo omits `dirty` for clean source; explicit true is never accepted.
        require(vcs.get("git", {}).get("sha1") == revision and vcs.get("git", {}).get("dirty", False) is False, f"Wrong or dirty packaged VCS identity: {name}")
        require(vcs.get("path_in_vcs") == f"crates/{name}", f"Wrong packaged VCS path: {name}")
        for path, data in packed.items():
            if path not in generated:
                require(files.get(path) == data, f"Packaged file differs from committed source: {name}/{path}")
        for path in files:
            if path.startswith(("src/", "tests/", "migrations/")) or path in {"README.md", "LICENSE", "NOTICE"}:
                require(packed.get(path) == files[path], f"Committed package input omitted: {name}/{path}")
        data = archive.read_bytes()
        cohort["packages"].append({"name": name, "version": VERSION, "archive_sha256": hashlib.sha256(data).hexdigest(), "archive_bytes": len(data)})
    witness = target_path(f"target/release-cohort/cohort-{revision}.json")
    unchanged_checkpoint(revision)
    # Never silently redefine the reviewed byte cohort for the same revision.
    write_json(witness, cohort, immutable=True)
    return cohort


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    for mode in ("source", "packages"):
        command = sub.add_parser(mode)
        command.add_argument("--revision", required=True)
        command.add_argument("--output")
        if mode != "source":
            command.add_argument("--package-dir", required=True)
    args = parser.parse_args()
    if args.mode == "source":
        revision, _ = source(args.revision)
        result = {"release_commit": revision, "version": VERSION, "packages": list(ARCHIVES), "registry_verified": False}
    else:
        result = package_cohort(args.revision, args.package_dir)
    default = f"target/release-cohort/{args.mode}.json"
    write_json(args.output or default, result)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except InvalidCohort as error:
        print(f"release-cohort: {error}", file=sys.stderr)
        sys.exit(1)
    except (OSError, ValueError, KeyError, tarfile.TarError, subprocess.TimeoutExpired):
        # Report bounded failures without dumping subprocess output or file data.
        print("release-cohort: validation unavailable or malformed input; no registry readiness is established", file=sys.stderr)
        sys.exit(1)
