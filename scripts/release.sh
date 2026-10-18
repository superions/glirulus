#!/usr/bin/env bash
# Standalone release-source preflight and normal Cargo packaging.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/release.sh MODE [--revision SHA] [--timeout-seconds N]

  source, check  Read-only source/locked-resolution preflight at clean HEAD.
  package        Normally verify all five Cargo packages at a reviewed --revision.

Source/check default to the current HEAD. Package requires its full reviewed
commit SHA. This source-preparation entry point does not publish packages or
create commits, tags or releases. It needs no publication credentials.
Unpublished sibling registry versions may prevent Cargo verification; such a
failure is not a verified package cohort.

Each command is bounded (default 1200 seconds, then at most 10 seconds to stop).
Caller Cargo job limits are preserved. The checkout target is shared exclusively
with scripts/check.sh; inherited CARGO_TARGET_DIR is ignored. Requires Linux,
Bash, Cargo, Python 3.11+, Git, GNU timeout, setsid and flock.
USAGE
}

mode=${1:-}
if [[ $mode == -h || $mode == --help ]]; then usage; exit 0; fi
case "$mode" in source|check|package) shift ;; *) usage >&2; exit 2 ;; esac
revision=
limit=1200
while (( $# )); do
  case "$1" in
    --revision|--timeout-seconds)
      if (( $# < 2 )); then usage >&2; exit 2; fi
      case "$1" in
        --revision) revision=$2 ;;
        --timeout-seconds) limit=$2 ;;
      esac
      shift 2 ;;
    *) usage >&2; exit 2 ;;
  esac
done
if [[ ! $limit =~ ^[1-9][0-9]*$ ]]; then usage >&2; exit 2; fi
if [[ $mode == package ]]; then
  if [[ ! $revision =~ ^[[:xdigit:]]{40}$ ]]; then
    echo 'Package requires --revision with the full reviewed HEAD SHA.' >&2
    exit 2
  fi
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

helper=(python3 "$root/scripts/release-cohort.py")
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
esac
