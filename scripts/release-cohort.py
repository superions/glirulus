#!/usr/bin/env python3
"""Validate one committed Glirulus cohort; never publish or alter registry state."""

import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tomllib
import zlib
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener


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
        require(not re.fullmatch(r"cohort-[0-9a-f]{40}\.json", path.name), "A summary must not overwrite an immutable cohort witness")
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
    if name == "glirulus-runtime":
        require(any(e.get("name") == "basic" and e.get("path") == "examples/basic.rs" and e.get("required-features") == ["json"] for e in manifest.get("example", [])), "Runtime basic example target is not explicit")


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
            require("examples/**" in includes and "examples/basic.rs" in files, "Runtime quick start must be packaged")
        packages[name] = files
    unchanged_checkpoint(revision)
    return revision, packages


def package_cohort(revision, package_dir, *, establish):
    revision, sources = source(revision)
    witness = ROOT / f"target/release-cohort/cohort-{revision}.json"
    require(not witness.is_symlink(), "Pinned cohort witness must not be a symlink")
    witness = target_path(witness)
    pinned = None
    if not establish:
        require(witness.is_file() and not witness.is_symlink(), "Prepare and pin the reviewed package cohort before registry validation")
        pinned = witness.read_bytes()
    package_dir = Path(package_dir).resolve()
    cohort = {"release_commit": revision, "version": VERSION, "registry_verified": False, "packages": []}
    generated = {"Cargo.toml", "Cargo.toml.orig", ".cargo_vcs_info.json", "Cargo.lock"}
    for name, files in sources.items():
        archive = package_dir / f"{name}-{VERSION}.crate"
        require(archive.is_file() and not archive.is_symlink(), f"Missing package archive: {name}")
        with archive.open("rb") as handle:
            archive_data = handle.read(32 * 1024 * 1024 + 1)
        require(len(archive_data) <= 32 * 1024 * 1024, f"Oversized package archive: {name}")
        # Bound the whole tar stream, including PAX headers parsed before members.
        with gzip.GzipFile(fileobj=io.BytesIO(archive_data)) as compressed:
            tar_data = compressed.read(64 * 1024 * 1024 + 1)
        require(len(tar_data) <= 64 * 1024 * 1024, f"Oversized uncompressed package archive: {name}")
        packed = {}
        with tarfile.open(fileobj=io.BytesIO(tar_data), mode="r:") as tar:
            for entry_count, member in enumerate(tar, 1):
                require(entry_count <= 4096, f"Too many package archive entries: {name}")
                path = PurePosixPath(member.name)
                require(path.parts and not path.is_absolute() and ".." not in path.parts and path.parts[0] == f"{name}-{VERSION}", f"Unsafe archive path: {name}")
                if member.isdir():
                    continue
                require(member.isfile() and 0 <= member.size <= 16 * 1024 * 1024, f"Unexpected archive entry: {name}")
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
            if path.startswith(("src/", "tests/", "examples/", "migrations/")) or path in {"README.md", "LICENSE", "NOTICE"}:
                require(packed.get(path) == files[path], f"Committed package input omitted: {name}/{path}")
        cohort["packages"].append({"name": name, "version": VERSION, "archive_sha256": hashlib.sha256(archive_data).hexdigest(), "archive_bytes": len(archive_data)})
    unchanged_checkpoint(revision)
    # Registry resumption requires the original witness and cannot create one.
    if establish:
        write_json(witness, cohort, immutable=True)
    else:
        require(witness.read_bytes() == pinned and json.loads(pinned) == cohort, "Local package bytes differ from the original pinned release cohort")
    return cohort


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def public_get(url, *, missing=False, limit=2 * 1024 * 1024):
    # Fixed public HTTPS endpoints only; no Cargo credentials enter this helper.
    request = Request(url, headers={"User-Agent": f"glirulus-release-cohort ({REPOSITORY})"})
    try:
        with build_opener(NoRedirect()).open(request, timeout=30) as response:
            data = response.read(limit + 1)
            require(len(data) <= limit, "Registry response exceeds the bounded read limit")
            return data
    except HTTPError as error:
        if missing and error.code == 404:
            return None
        raise InvalidCohort(f"Public registry request failed (HTTP {error.code}); no publication preflight established") from None
    except (URLError, TimeoutError):
        raise InvalidCohort("Public registry request unavailable; no publication preflight established") from None


def public_json(path, *, missing=False):
    data = public_get(f"https://crates.io/api/v1/{path}", missing=missing)
    return None if data is None else json.loads(data)


def registry_package(name, owner_id, pin, package_dir):
    metadata = public_json(f"crates/{name}", missing=True)
    result = {"name": name, "version": VERSION, "published": False}
    if metadata is None:
        result["name_status"] = "absent_public_record"
        return result
    require(metadata["crate"]["id"] == name, f"Registry name collision: {name}")
    # Individual owners are deliberate: team membership is not inferred publicly.
    owners = public_json(f"crates/{name}/owner_user")["users"]
    require(any(owner.get("kind") == "user" and owner.get("id") == owner_id for owner in owners), f"Intended account is not a public individual owner of {name}")
    result["name_status"] = "intended_account_is_public_owner"
    selected = public_json(f"crates/{name}/{VERSION}", missing=True)
    if selected is None:
        return result
    version = selected["version"]
    require(version["crate"] == name and version["num"] == VERSION, f"Wrong registry version identity: {name}")
    require(version.get("yanked") is False, f"Selected registry version is yanked: {name}")
    require(version["checksum"] == pin["archive_sha256"], f"Published version checksum differs from reviewed cohort: {name}")
    downloaded = public_get(f"https://static.crates.io/crates/{name}/{name}-{VERSION}.crate", limit=32 * 1024 * 1024)
    require(len(downloaded) == pin["archive_bytes"] and hashlib.sha256(downloaded).hexdigest() == pin["archive_sha256"], f"Published archive differs from reviewed cohort: {name}")
    local = Path(package_dir).resolve() / f"{name}-{VERSION}.crate"
    require(downloaded == local.read_bytes(), f"Published archive and local reviewed bytes differ: {name}")
    result.update(published=True, archive_sha256=pin["archive_sha256"])
    return result


def registry(revision, package_dir, owner, name=None):
    require(re.fullmatch(r"[A-Za-z0-9_-]{1,64}", owner), "Select a nonsecret individual crates.io username")
    cohort = package_cohort(revision, package_dir, establish=False)
    revision = cohort["release_commit"]
    witness = target_path(f"target/release-cohort/cohort-{revision}.json")
    original_witness = witness.read_bytes()
    require(json.loads(original_witness) == cohort, "Pinned release cohort changed before registry validation")
    account = public_json(f"users/{owner}")["user"]
    require(isinstance(account.get("id"), int) and account.get("login", "").casefold() == owner.casefold(), "Public intended account identity does not match --owner")
    names = [name] if name else list(ARCHIVES)
    pins = {package["name"]: package for package in cohort["packages"]}
    packages = [registry_package(selected, account["id"], pins[selected], package_dir) for selected in names]
    for pin in cohort["packages"]:
        archive = Path(package_dir).resolve() / f"{pin['name']}-{VERSION}.crate"
        with archive.open("rb") as handle:
            data = handle.read(pin["archive_bytes"] + 1)
        require(len(data) == pin["archive_bytes"] and hashlib.sha256(data).hexdigest() == pin["archive_sha256"], f"Local archive changed during registry validation: {pin['name']}")
    unchanged_checkpoint(revision)
    require(witness.read_bytes() == original_witness, "Pinned release cohort changed during registry validation")
    # Public ownership is evidence about the name, never the active Cargo identity
    # or token scopes. The publisher requires separate operator confirmation.
    result = {"release_commit": revision, "version": VERSION, "owner": owner,
              "owner_id": account["id"], "public_preflight_passed": True,
              "authenticated_account_unproven": True, "packages": packages,
              "complete": all(package["published"] for package in packages)}
    if name:
        result["published"] = packages[0]["published"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    for mode in ("source", "packages", "registry", "status"):
        command = sub.add_parser(mode)
        command.add_argument("--revision", required=True)
        command.add_argument("--output")
        if mode != "source":
            command.add_argument("--package-dir", required=True)
        if mode in ("registry", "status"):
            command.add_argument("--owner", required=True)
        if mode == "status":
            command.add_argument("--name", choices=list(ARCHIVES), required=True)
    args = parser.parse_args()
    if args.mode == "source":
        revision, _ = source(args.revision)
        result = {"release_commit": revision, "version": VERSION, "packages": list(ARCHIVES), "registry_verified": False}
    elif args.mode == "packages":
        result = package_cohort(args.revision, args.package_dir, establish=True)
    else:
        result = registry(args.revision, args.package_dir, args.owner, getattr(args, "name", None))
    default = f"target/release-cohort/{args.mode}.json"
    write_json(args.output or default, result)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except InvalidCohort as error:
        print(f"release-cohort: {error}", file=sys.stderr)
        sys.exit(1)
    except (OSError, EOFError, ValueError, KeyError, TypeError, AttributeError, zlib.error, tarfile.TarError, subprocess.TimeoutExpired):
        # Report bounded failures without dumping subprocess output or file data.
        print("release-cohort: validation unavailable or malformed input; no registry readiness is established", file=sys.stderr)
        sys.exit(1)
