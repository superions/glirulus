#!/usr/bin/env bash
# Standalone source, verified packaging and explicit immutable-cohort publication.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/release.sh MODE [--revision SHA] [--owner LOGIN --account-verified] [--timeout-seconds N]

  source, check  Read-only source/locked-resolution preflight at clean HEAD.
  package        Normally verify all five Cargo packages at a reviewed --revision.
  publish        Explicit publication/resume; requires --revision, --owner and
                 --account-verified.

Source/check default to the current HEAD. Package/publish require its full reviewed
commit SHA. Source/package need no credentials. Publication requires the existing
secure Cargo credential mechanism and an already pinned, normally verified
five-archive cohort. Before using --account-verified, the operator must establish
that Cargo's current credential belongs to --owner with the required publish
permissions. This flag confirms that prerequisite; it is not automated account
proof. The helper checks public names/owners/bytes only. Never pass a token here.
Unpublished sibling versions may prevent normal Cargo package verification;
such a failure is not a verified cohort. No mode creates commits, tags or releases.

Each command is bounded (default 1200 seconds, then at most 10 seconds to stop).
Caller Cargo job limits are preserved. The checkout target is shared exclusively
with scripts/check.sh; inherited CARGO_TARGET_DIR is ignored. Requires Linux,
Bash, Cargo, Python 3.11+, Git, GNU timeout, setsid and flock.
USAGE
}

mode=${1:-}
if [[ $mode == -h || $mode == --help ]]; then usage; exit 0; fi
case "$mode" in source|check|package|publish) shift ;; *) usage >&2; exit 2 ;; esac
revision=
owner=
account_verified=false
limit=1200
while (( $# )); do
  case "$1" in
    --account-verified) account_verified=true; shift ;;
    --revision|--owner|--timeout-seconds)
      if (( $# < 2 )); then usage >&2; exit 2; fi
      case "$1" in
        --revision) revision=$2 ;;
        --owner) owner=$2 ;;
        --timeout-seconds) limit=$2 ;;
      esac
      shift 2 ;;
    *) usage >&2; exit 2 ;;
  esac
done
if [[ ! $limit =~ ^[1-9][0-9]*$ ]]; then usage >&2; exit 2; fi
if [[ $mode == package || $mode == publish ]]; then
  if [[ ! $revision =~ ^[[:xdigit:]]{40}$ ]]; then
    echo 'Package/publish requires --revision with the full reviewed HEAD SHA.' >&2
    exit 2
  fi
fi
if [[ $mode == publish && -z $owner ]]; then
  echo 'Explicit publication requires --owner with the intended crates.io login.' >&2
  exit 2
fi
if [[ $mode == publish && $account_verified != true ]]; then
  echo 'Publication requires --account-verified after the operator establishes Cargo credential identity and permissions for --owner; this flag is not automated account proof.' >&2
  exit 2
fi
if [[ $mode != publish && ( -n $owner || $account_verified == true ) ]]; then
  echo '--owner and --account-verified apply only to explicit publication.' >&2
  exit 2
fi
for command in cargo python3 git timeout setsid flock; do
  command -v "$command" >/dev/null || { echo "Missing required command: $command" >&2; exit 1; }
done
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$root"
mkdir -p "$root/target"
target=$(cd "$root/target" && pwd -P)
if [[ $target != "$root/target" ]]; then
  echo 'The standalone target directory must not be a symlink to another checkout/cache.' >&2
  exit 1
fi
exec 9>"$target/.check.lock"
flock -n 9 || { echo 'Another check/release command owns this checkout target.' >&2; exit 1; }
export CARGO_TARGET_DIR="$target" CARGO_INCREMENTAL=0

active_group=
stop_group() {
  if [[ -n $active_group ]]; then
    kill -TERM -- "-$active_group" 2>/dev/null || true
    if kill -0 -- "-$active_group" 2>/dev/null; then
      sleep 1
      kill -KILL -- "-$active_group" 2>/dev/null || true
    fi
    wait "$active_group" 2>/dev/null || true
    active_group=
  fi
}
cleanup() {
  local status=$?
  trap - EXIT INT TERM HUP
  stop_group
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

run() {
  local status=0
  printf '\nRunning:' >&2
  printf ' %q' "$@" >&2
  printf '\n' >&2
  setsid timeout --signal=TERM --kill-after=10s "${limit}s" "$@" &
  active_group=$!
  wait "$active_group" || status=$?
  stop_group
  if (( status )); then
    echo "Release command failed (exit $status; timeout exits 124 or 137)." >&2
    return "$status"
  fi
}

# Recheck at stage boundaries so a moving checkout cannot silently change a cohort.
assert_revision() {
  local head
  run git rev-parse --verify HEAD > "$target/release-head.txt"
  IFS= read -r head < "$target/release-head.txt"
  run git status --porcelain --untracked-files=all --ignore-submodules=none > "$target/release-worktree.txt"
  if [[ -s $target/release-worktree.txt ]]; then
    echo 'Release input must be clean, including untracked source files.' >&2
    exit 1
  fi
  if [[ -n $revision && $head != "$revision" ]]; then
    echo 'HEAD differs from the selected reviewed release revision.' >&2
    exit 1
  fi
  revision=$head
}

read_published() {
  run python3 -c 'import json,sys; v=json.load(open(sys.argv[1]))["published"]; assert isinstance(v,bool); print(str(v).lower())' "$1" > "$target/release-published.txt"
  IFS= read -r published < "$target/release-published.txt"
}

helper=(python3 "$root/scripts/release-cohort.py")
version=1.0.0
packages=(glirulus-core glirulus-persistence glirulus-macros glirulus-runtime glirulus-postgres)
assert_revision
run "${helper[@]}" source --revision "$revision" --output "$target/release-source.json"
run cargo metadata --locked --format-version 1 --no-deps > /dev/null
assert_revision

case "$mode" in
  source|check)
    echo "Source preflight passed at $revision; no Cargo package or registry acceptance is claimed."
    ;;
  package)
    # Stable Cargo workspace packaging verifies sibling crates through Cargo's
    # own packaging implementation; no local registry or path fallback is added.
    if ! run cargo package --locked --workspace --registry crates-io; then
      echo 'Normal Cargo package verification failed. Unpublished sibling versions may be a registry prerequisite; inspect the Cargo error. No verified cohort is claimed.' >&2
      exit 1
    fi
    assert_revision
    run "${helper[@]}" packages --revision "$revision" --package-dir "$target/package" --output "$target/release-packages.json"
    echo "Cargo package verification passed at $revision; registry publication remains separate."
    ;;
  publish)
    # Publication may only consume a previously reviewed byte cohort. A package
    # check here revalidates its pin; it must not create the initial witness.
    witness="$target/release-cohort/cohort-$revision.json"
    if [[ ! -f $witness || -L $witness ]]; then
      echo 'Publication requires the existing immutable package witness; run normal package preparation first.' >&2
      exit 1
    fi
    run "${helper[@]}" packages --revision "$revision" --package-dir "$target/package" --output "$target/release-packages.json"
    # Operator-confirmed credential identity is separate from this public check.
    # Check all names/owners and immutable versions before the first write.
    run "${helper[@]}" registry --owner "$owner" --revision "$revision" --package-dir "$target/package" --output "$target/release-registry.json"
    run python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["public_preflight_passed"] is True, "public registry preflight is incomplete"' "$target/release-registry.json"
    for package in "${packages[@]}"; do
      assert_revision
      status_file="$target/release-status-$package.json"
      run "${helper[@]}" status --name "$package" --owner "$owner" --revision "$revision" --package-dir "$target/package" --output "$status_file"
      read_published "$status_file"
      if [[ $published == true ]]; then
        echo "Retaining matching immutable $package@$version."
        continue
      fi
      # Workspace preparation resolves siblings through Cargo's overlay. Recheck
      # normal single-package verification against the now-published registry
      # dependencies and the original byte pin before this irreversible upload.
      run cargo package --locked --registry crates-io --package "$package@$version"
      assert_revision
      run "${helper[@]}" packages --revision "$revision" --package-dir "$target/package" --output "$target/release-packages.json"
      run cargo publish --locked --registry crates-io --package "$package@$version"
      # Index propagation can lag the upload acknowledgement. The pinned witness
      # still governs comparison if Cargo regenerates a local archive on publish.
      for attempt in {1..6}; do
        run "${helper[@]}" status --name "$package" --owner "$owner" --revision "$revision" --package-dir "$target/package" --output "$status_file"
        read_published "$status_file"
        if [[ $published == true ]]; then break; fi
        if (( attempt < 6 )); then sleep 10; fi
      done
      if [[ $published != true ]]; then
        echo "$package publication is not byte-verified; retain the immutable versions and resume the same cohort later." >&2
        exit 1
      fi
    done
    assert_revision
    run "${helper[@]}" registry --owner "$owner" --revision "$revision" --package-dir "$target/package" --output "$target/release-registry.json"
    run python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["complete"] is True, "registry cohort remains incomplete"' "$target/release-registry.json"
    echo 'All five registry archives match the pinned reviewed cohort. External-consumer, versioned Docs.rs and GitHub release acceptance remain separate.'
    ;;
esac
