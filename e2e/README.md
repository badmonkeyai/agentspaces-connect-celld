# The local end-to-end harness

This directory runs the celld connector against a real celld node on one machine, with no cloud
account and no Docker. Azurite, the Azure Storage emulator, serves as the fleet bucket, because
celld accepts it for local work (`AZURE_STORAGE_USE_EMULATOR=true`). celld itself runs as the
v0.6.3 release binary. The harness deploys the inventory test application in `worker/`, seeds its
R2 bucket, starts the connector, and runs `CelldLiveTest` from an agent peer on the content swarm.

```sh
./run.sh            # setup, up, seed, test, down; the live test itself takes about ten seconds
E2E_KEEP=1 ./run.sh # leave Azurite and the node running afterwards
./run.sh down       # stop them
./run.sh clean      # stop them and remove .state
```

The steps also run one at a time: `setup`, `up`, `seed`, `test`, `down`.

## What it needs

- macOS on Apple Silicon, or Linux on x86-64 or ARM64 (the platforms celld publishes binaries for).
- Node 22 or newer, and npm.
- Java 21 or newer and Maven, with the AgentSpaces 0.5.0 artifacts in the local Maven repository
  (`mvn install` of the core reactor and of `agentspaces-langchain4j`).
- The TypeScript packages built once: `npm test` in `agentspaces-typescript/packages/workers`
  writes the `build/` the Worker imports from.
- Network access on the first run only, to download celld and the npm tools.
- Ports 10000 (Azurite), 18787 and 18788 (the node's public and internal listeners), and 7740 (the
  connector's peer). `E2E_PUBLIC` and `E2E_INTERNAL` move the node's ports.

`run.sh setup` puts celld in `.tools/` and installs Azurite, esbuild (celld bundles the Worker
with it), and the Azure Storage client in `node_modules/`. Nothing is installed system-wide.

## What runs

| Piece | Where | Role |
|---|---|---|
| Azurite | `127.0.0.1:10000`, state in `.state/azurite` | The fleet bucket: container `celld-e2e`, the bucket URL `az://celld-e2e` |
| celld | public `127.0.0.1:18787`, internal `127.0.0.1:18788`, state in `.state/node` | One node running the inventory application |
| `worker/` | deployed by `celld deploy worker` | One Durable Object (`Inventory`, SQLite) and two R2 buckets (`models`, `ingest`) |
| The connector | peer `127.0.0.1:7740`, state in `.state/connector` | `fleet.yaml` and `manifest.yaml`, started by the live test |
| The agent | a free port | The live test's own peer on the content swarm |

The Worker's routes are the ones the connector's manifest allowlists:

| Route | Built with | What the live test checks |
|---|---|---|
| `GET /api/stock`, `POST /api/stock/reserve`, `POST /api/stock/set` | the Durable Object's SQLite and its `once` table | a read, and an invocation that runs once under a retry |
| `GET /api/exports/nightly?mb=` | a generated NDJSON stream | a 64 MiB result as one artifact from one call; no `Content-Length` |
| `GET /api/models`, `GET /api/models/object?key=` | `R2.list`, and `serveRange` from `@agentspaces/workers/routes` | the objects hashed once, a 300 MiB object served by ranged reads, a replaced object forgotten |
| `POST /api/ingest`, `GET /api/ingested?key=` | `receiveArtifact`, `receivePiece`, SHA-256 in the object | a 48 MiB artifact sent whole and a 9 MiB one piece by piece, both byte-identical |
| `GET /api/changes`, `/api/changes/stream` | `ChangeLog` and `serveChangeStream` | a stock write as an `AssetChange`, a fresh read, and a new snapshot |
| `GET /api/stock/snapshot` | every row read inside one request | the snapshot holds the written value |

`run.sh seed` writes four objects with `celld r2 put`: 300 KiB, 6 MiB, 300 MiB (`E2E_BIG_MB` changes
it; above 256 MiB shows the Java connector past the in-deployment cell's bound), and a 90 KiB tile.

## What it does not cover

The node runs plaintext on loopback, so the run tests no proxy; a deployment puts one in front of
celld (the connector README has a Caddy example). celld does not qualify Azurite for a production
fleet, so this harness checks behaviour, not the storage guarantees a cloud bucket gives. One node
means no cell moves, so the change stream's reconnect after a move is not exercised.

## When it fails

- `port ... is taken`: another process holds a port; stop it or set `E2E_PUBLIC` and `E2E_INTERNAL`.
- `the celld node did not answer`: read `.state/celld.log`.
- A deploy refusal names the `wrangler.jsonc` key celld does not accept.
- The live test's output and the connector's counters print at the end of `run.sh test`; the
  surefire report is under `.state/build/target/surefire-reports`.
