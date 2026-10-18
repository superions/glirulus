#!/usr/bin/env bash
# Standalone Linux checks; no Myoxus checkout, tools or target cache is needed.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/check.sh MODE [--timeout-seconds SECONDS]

  build     One locked workspace library build (baseline buildability).
  tests     Workspace library tests and doctests once, plus the public macro consumer.
  macros    The public macro consumer alone (targeted recovery).
  example   Execute the basic public-API example with the reduced JSON profile.
  features  Default, no-default and all-feature library compilation, plus the
            isolated Core/Tokio, Runtime/JSON and Runtime/rkyv consumer profiles.
  postgres  Restored PostgreSQL integration target once, using disposable Docker data.
  all       tests, features and postgres (qualification, not baseline import).

Each Cargo command has its own timeout (default 1200 seconds), followed by at
most 10 seconds for termination. CARGO_BUILD_JOBS is preserved; test harnesses
default to two threads unless RUST_TEST_THREADS is set. Cargo always uses this
checkout's target directory, ignoring inherited CARGO_TARGET_DIR. Concurrent
checks in the same checkout fail rather than sharing an active target owner.
Requires Bash, Cargo, GNU timeout, setsid and flock; postgres/all also need Docker.
EOF
}

mode=${1:-}
if [[ $mode == -h || $mode == --help ]]; then usage; exit 0; fi
case "$mode" in build|tests|macros|example|features|postgres|all) shift ;; *) usage >&2; exit 2 ;; esac
limit=1200
if [[ ${1:-} == --timeout-seconds && $# == 2 ]]; then
  limit=$2
  shift 2
fi
if [[ $# != 0 || ! $limit =~ ^[1-9][0-9]*$ ]]; then usage >&2; exit 2; fi

for command in cargo timeout setsid flock; do
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
flock -n 9 || { echo 'Another check owns this checkout target directory.' >&2; exit 1; }
export CARGO_TARGET_DIR="$target" CARGO_INCREMENTAL=0
export RUST_TEST_THREADS=${RUST_TEST_THREADS:-2}

prefix=glirulus

active_group=
postgres_started=0
run_id="glirulus-check-$$-$(date +%s%N)-$RANDOM"

stop_group() {
  if [[ -n $active_group ]]; then
    # The timeout supervisor and all Cargo/rustc/test descendants share this
    # private process group. Also reap descendants left after a successful Cargo.
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
  local status=$? ids
  trap - EXIT INT TERM HUP
  stop_group
  if (( postgres_started )); then
    # Testcontainers normally removes containers on Drop. Its asynchronous Drop
    # cannot run after a forced timeout, so the harness tags only this run's
    # containers. Remove their anonymous PostgreSQL volumes as well.
    if ids=$(timeout --kill-after=5s 20s docker ps -aq --filter "label=org.glirulus.check-run=$run_id"); then
      if [[ -n $ids ]]; then
        local -a containers
        mapfile -t containers <<<"$ids"
        if ! timeout --kill-after=5s 30s docker rm -fv "${containers[@]}"; then
          echo "Failed to remove PostgreSQL containers for check run $run_id" >&2
          status=1
        fi
      fi
    else
      echo "Failed to inventory PostgreSQL cleanup for check run $run_id" >&2
      status=1
    fi
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

run() {
  local status=0
  printf '\nRunning:'
  printf ' %q' "$@"
  printf '\n'
  setsid timeout --signal=TERM --kill-after=10s "${limit}s" "$@" &
  active_group=$!
  wait "$active_group" || status=$?
  stop_group
  if (( status )); then
    echo "Check command failed (exit $status; timeout exits 124 or 137)." >&2
    return "$status"
  fi
}

tests() {
  # --lib and --doc deliberately exclude the separately owned PostgreSQL target.
  run cargo test --locked --workspace --lib
  run cargo test --locked --workspace --doc
  macros
}

macros() {
  run cargo test --locked -p "$prefix-runtime" --test macros --features json,macros
}

features() {
  run cargo check --locked --workspace --lib
  # Runtime intentionally requires a codec even when defaults are disabled.
  run cargo check --locked --workspace --lib --no-default-features --features "$prefix-runtime/json"
  run cargo check --locked --workspace --lib --all-features
  # Separate package-specific invocations are essential: workspace/dev-dependency
  # feature unification would otherwise hide missing reduced-profile imports.
  run cargo check --locked -p "$prefix-core" --lib --no-default-features --features tokio
  run cargo check --locked -p "$prefix-runtime" --lib --no-default-features --features json
  # Defaults/all-features enable JSON and cannot protect the independent rkyv codec.
  run cargo check --locked -p "$prefix-runtime" --lib --no-default-features --features rkyv
}

postgres() {
  command -v docker >/dev/null || { echo 'PostgreSQL checks require Docker.' >&2; return 1; }
  run docker info --format '{{.ServerVersion}}'
  export GLIRULUS_TEST_RUN_ID="$run_id" TESTCONTAINERS_COMMAND=remove
  postgres_started=1
  run cargo test --locked -p "$prefix-postgres" --test integration
}

case "$mode" in
  build) run cargo build --locked --workspace --lib ;;
  tests) tests ;;
  macros) macros ;;
  example) run cargo run --locked -p "$prefix-runtime" --example basic --no-default-features --features json ;;
  features) features ;;
  postgres) postgres ;;
  all) tests; features; postgres ;;
esac
