#!/usr/bin/env bash
# The local end-to-end harness for agentspaces-connect-celld (see README.md).
#
#   ./run.sh all       setup, start, seed, run the live test, stop (the default)
#   ./run.sh setup     download celld into .tools and install the npm tools
#   ./run.sh up        start Azurite and one celld node with the inventory application deployed
#   ./run.sh seed      put the test objects into the application's `models` bucket
#   ./run.sh test      run CelldLiveTest against the running node
#   ./run.sh down      stop the node and Azurite
#   ./run.sh clean     stop, then remove .state
#
# Environment: CELLD_VERSION (v0.6.3), E2E_PUBLIC (127.0.0.1:18787), E2E_INTERNAL (127.0.0.1:18788), E2E_BIG_MB (300, the large object), E2E_KEEP=1 to leave the
# node running after `all`.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
CELLD_VERSION="${CELLD_VERSION:-v0.6.3}"
TOOLS="$HERE/.tools"
STATE="$HERE/.state"
CELLD="$TOOLS/celld"
CONTAINER="celld-e2e"
PUBLIC="${E2E_PUBLIC:-127.0.0.1:18787}"
INTERNAL="${E2E_INTERNAL:-127.0.0.1:18788}"

export AZURE_STORAGE_USE_EMULATOR=true
export CELLD_BUCKET="az://$CONTAINER"
export CELLD_ESBUILD="$HERE/node_modules/.bin/esbuild"

say() { printf '\n== %s\n' "$*"; }

wait_for() { # url, seconds, what
  local deadline=$((SECONDS + $2))
  until curl -fsS -o /dev/null "$1" 2>/dev/null; do
    if (( SECONDS > deadline )); then echo "$3 did not answer at $1 within $2 s" >&2; return 1; fi
    sleep 0.5
  done
}

setup() {
  say "setup: celld $CELLD_VERSION and the npm tools"
  mkdir -p "$TOOLS"
  if [[ ! -x "$CELLD" ]] || ! "$CELLD" --version 2>/dev/null | grep -q "${CELLD_VERSION#v}"; then
    case "$(uname -s)-$(uname -m)" in
      Darwin-arm64) target=aarch64-apple-darwin ;;
      Linux-x86_64) target=x86_64-unknown-linux-gnu ;;
      Linux-aarch64) target=aarch64-unknown-linux-gnu ;;
      *) echo "no celld release binary for $(uname -s)-$(uname -m)" >&2; exit 1 ;;
    esac
    curl -fsSL "https://github.com/denoland/celld/releases/download/$CELLD_VERSION/celld-$target.gz" \
      | gunzip > "$CELLD.tmp"
    chmod +x "$CELLD.tmp" && mv "$CELLD.tmp" "$CELLD"
  fi
  "$CELLD" --version || true
  if [[ ! -d node_modules/azurite || ! -d node_modules/esbuild ]]; then
    npm install --no-audit --no-fund
  fi
}

up() {
  mkdir -p "$STATE/azurite" "$STATE/node"
  if [[ ! -f "$STATE/azurite.pid" ]] || ! kill -0 "$(cat "$STATE/azurite.pid")" 2>/dev/null; then
    say "Azurite (blob service) on 127.0.0.1:10000"
    nohup node_modules/.bin/azurite-blob --blobHost 127.0.0.1 --blobPort 10000 --location "$STATE/azurite" \
      --skipApiVersionCheck --loose --silent > "$STATE/azurite.log" 2>&1 &
    echo $! > "$STATE/azurite.pid"
    for _ in $(seq 1 60); do nc -z 127.0.0.1 10000 2>/dev/null && break; sleep 0.5; done
  fi
  node scripts/container.mjs "$CONTAINER"

  say "celld diagnose: the storage contract against Azurite"
  "$CELLD" diagnose --bucket "$CELLD_BUCKET" | tail -20

  say "celld deploy: the inventory application"
  "$CELLD" deploy worker --bucket "$CELLD_BUCKET"

  if [[ ! -f "$STATE/celld.pid" ]] || ! kill -0 "$(cat "$STATE/celld.pid")" 2>/dev/null; then
    say "celld node: public $PUBLIC, internal $INTERNAL"
    port_free "$PUBLIC" "the public listener"
    port_free "$INTERNAL" "the internal listener"
    CELLD_WATCH="$STATE/node" nohup "$CELLD" --bucket "$CELLD_BUCKET" --listen "$PUBLIC" \
      --internal-listen "$INTERNAL" > "$STATE/celld.log" 2>&1 &
    echo $! > "$STATE/celld.pid"
  else
    curl -fsS -X POST "http://$INTERNAL/reload" > /dev/null || true
  fi
  wait_for "http://$PUBLIC/api/health" 90 "the celld node"
  curl -fsS "http://$PUBLIC/api/stock" && echo
}

seed() {
  say "seed: objects in the models bucket"
  mkdir -p "$STATE/seed"
  local big="${E2E_BIG_MB:-300}"
  [[ -f "$STATE/seed/ckpt-small.bin" ]] || head -c 307200 /dev/urandom > "$STATE/seed/ckpt-small.bin"
  [[ -f "$STATE/seed/ckpt-mid.bin" ]] || head -c 6291456 /dev/urandom > "$STATE/seed/ckpt-mid.bin"
  [[ -f "$STATE/seed/ckpt-big.bin" && $(wc -c < "$STATE/seed/ckpt-big.bin") -eq $((big * 1048576)) ]] \
    || head -c $((big * 1048576)) /dev/urandom > "$STATE/seed/ckpt-big.bin"
  [[ -f "$STATE/seed/tile-7.bin" ]] || head -c 92160 /dev/urandom > "$STATE/seed/tile-7.bin"
  put() { "$CELLD" r2 put models "$1" --path "$2" --content-type application/octet-stream --metadata "$3" \
            --bucket "$CELLD_BUCKET" > /dev/null; echo "  models/$1 ($(wc -c < "$2" | tr -d ' ') bytes)"; }
  put ckpt-small "$STATE/seed/ckpt-small.bin" '{"kind":"checkpoint","model":"m1"}'
  put ckpt-mid "$STATE/seed/ckpt-mid.bin" '{"kind":"checkpoint","model":"m2"}'
  put ckpt-big "$STATE/seed/ckpt-big.bin" '{"kind":"checkpoint","model":"m3"}'
  put tiles/7 "$STATE/seed/tile-7.bin" '{"kind":"tile"}'
  curl -fsS "http://$PUBLIC/api/models" | head -c 600 && echo
}

live() {
  say "CelldLiveTest against http://$PUBLIC"
  # Maven runs on a copy of the project, so an IDE compiling into the project's target/ meanwhile
  # cannot hand the run its half-built classes.
  local build="$STATE/build"
  mkdir -p "$build"
  rsync -a --delete --exclude target --exclude e2e --exclude .classpath --exclude .project --exclude .settings \
    --exclude .factorypath "$HERE/../" "$build/"
  (cd "$build" && CELLD_E2E_URL="http://$PUBLIC" CELLD_E2E_DIR="$HERE" CELLD_E2E_BIN="$CELLD" \
    mvn -o -B -q test -Dtest=CelldLiveTest -Dsurefire.failIfNoSpecifiedTests=false)
  grep -h "Tests run" "$build"/target/surefire-reports/*CelldLiveTest.txt
}

down() {
  say "down"
  for p in celld azurite; do
    if [[ -f "$STATE/$p.pid" ]]; then
      pid="$(cat "$STATE/$p.pid")"
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done   # a graceful stop
      kill -9 "$pid" 2>/dev/null || true
      rm -f "$STATE/$p.pid"
    fi
  done
}

port_free() { # host:port, what
  if lsof -nP -iTCP:"${1##*:}" -sTCP:LISTEN > /dev/null 2>&1; then
    echo "$2: port ${1##*:} is taken by:" >&2
    lsof -nP -iTCP:"${1##*:}" -sTCP:LISTEN >&2
    echo "set E2E_PUBLIC / E2E_INTERNAL to free ports, or stop that process" >&2
    return 1
  fi
}

case "${1:-all}" in
  setup) setup ;;
  up) up ;;
  seed) seed ;;
  test) live ;;
  down) down ;;
  clean) down; rm -rf "$STATE" ;;
  all)
    setup
    if [[ "${E2E_KEEP:-0}" != 1 ]]; then trap down EXIT; fi
    up; seed; live
    say "all green" ;;
  *) echo "usage: $0 [all|setup|up|seed|test|down|clean]" >&2; exit 2 ;;
esac
