# agentspaces-connect-celld

The Java connector that brings a celld or Workers deployment into an AgentSpaces fleet. It
publishes the deployment's routes as AssetCards, so any agent in the fleet can discover them, query
them, and invoke them through a space. An operator lists the Worker routes to share in a manifest,
and the connector turns each route group into a `workers://` AssetCard on
`agentspaces-connect-core`. Agents read through the GET routes with `aspace:cap/data-query` and act
through the POST routes with `aspace:cap/asset-invoke`. The connector makes every call over HTTPS
with the operator's bearer or mTLS credential and sends the invocation id as a header, so the
deployment can recognize a retry. The manifest serves as the allowlist: the connector publishes
the routes and operations it names, and refuses any other route, operation, or parameter before
it calls the deployment. The connector runs beside the deployment and applies the same
route-group mapping that `@agentspaces/workers` applies inside one, so a team can open an existing
deployment to the fleet without changing its code or its bindings.

**Architecture position.** Gateways and served interfaces. The connector joins the fleet as a
non-agent member that the other members see as a set of assets. It sits between the fleet and
the deployment's HTTPS interface, holds the deployment's address and credential on its own host,
and answers each query and invocation with a signed result through the data-query and
asset-invoke protocols of `agentspaces-connect-core`.

## Coordinates

```xml
<dependency>
  <groupId>ai.badmonkey.agentspaces</groupId>
  <artifactId>agentspaces-connect-celld</artifactId>
  <version>0.5.0</version>
</dependency>
```

It brings `agentspaces-connect-core` (the SPI and the protocols) and `agentspaces-runtime` (the
plain-Java peer from a fleet file), both 0.5.0, plus Jackson for the manifest and the JSON bodies.
Java 21. The jar's main class is `CelldConnector`.

## What you can build with it

The connector turns a deployment's routes into assets any member of the fleet can find, read, and
act on. The agents that use them never hold the deployment's credential and never learn its
address. They see a signed AssetCard, write a query or an invocation into a space, and read back
the connector's signed result. That opens the deployment to kinds of agents that could not safely
call it before.

### Browser agents that read and act on a deployment

A web page runs its agent in the page's worker, through `@agentspaces/browser`. The worker holds a
fleet peer, and `@agentspaces/connect` gives that peer the same `DataQueryClient` and `invoke` the
Java agents use. So a page can look up stock, reserve it, or start a job on a Worker route while the
bearer token stays on the connector's host. The browser reaches the fleet over WebSocket, and the
connector itself can be the seed it dials.

`worker.ts`, bundled for the page with esbuild (`--format=esm --target=es2022`):

```typescript
import { host } from "@agentspaces/browser/worker";
import { DataQueryClient, assetCards, invoke } from "@agentspaces/connect";

host.onResume(async () => {
  const peer = host.peer!;
  const client = new DataQueryClient(peer, { requester: "stock-page" });
  let card: Record<string, unknown> | null = null;
  while (!card) {
    peer.pullAds();                                   // a late joiner asks for the cards the group holds
    card = assetCards(peer, "route/stock")[0] ?? null;
    if (!card) await new Promise((r) => setTimeout(r, 200));
  }
  const stock = await client.fetch("route/stock", { op: "getStock", sku: "A-1" }, "15s");
  const reserved = await invoke(client.space, card, "reserveStock", { sku: "A-1", qty: "1" });
  // stock.result.rows holds the Worker's JSON rows; reserved.result holds its JSON answer.
});
```

`page.ts`:

```typescript
import { connect } from "@agentspaces/browser";

const host = await connect({
  founding: "inventory-v1",
  seeds: ["ws://127.0.0.1:7502/aspace"],
  worker: new URL("./worker.js", import.meta.url),
});
```

To let pages dial the connector directly, give its fleet file a WebSocket listener with the page's
origin on the allowlist, and put `agentspaces-transport-http` 0.5.0 beside the connector (as a
dependency of the application that runs it, or added to this project's `pom.xml` before
`copy-dependencies`). The connector does not bring that module on its own, and without it the
listener does not start.

```yaml
agentspaces:
  security:
    profile: dev-local                # a laptop only; on a shared network use wss and a TLS profile
  bind: 127.0.0.1:7500
  transport:
    ws:
      enabled: true
      bind: 127.0.0.1:7502
      allowed-origins: [ "http://127.0.0.1:8080" ]
```

The connector does not have to be the seed. A page can dial any member with a WebSocket listener,
such as the `@agentspaces/node` seed in the `@agentspaces/browser` quick start, while the
connector joins the same group over TCP. The cards and results reach the page through the group.

The TypeScript packages are not on npm yet. Until they are, install them from a clone of
`agentspaces-typescript`, as the `@agentspaces/browser` README shows.

### Results that every agent shares

A query's result is a live entry in the group's data space, keyed by the hash of the asset and its
parameters. The next agent that asks the same question, in another browser, a Java service, or a
Python script, reads that entry and the deployment is not called again until the result's
freshness runs out. One page load can warm the answer for a whole fleet, and a dashboard with a
thousand viewers costs the deployment about one call per distinct question per freshness window.
Set the window per route group with `freshness` in the manifest.

### Actions that run once, wherever they come from

An invocation carries an id. The connector remembers the answer for each id, so a browser that
retries after a dropped connection gets the first answer back and the route runs once. The route
also receives the id as `Aspace-Invocation-Id`, and a handler that deduplicates on it stays safe
across a connector restart; mark those routes `idempotent: true` so agents can see it on the card.
The manifest's `invoke-ops` and the fleet's `aspace:connector-invoke:<asset>:<operation>` scopes
decide who may run what, before any call leaves the connector.

### Agents in any language on one deployment

The connector speaks the fleet's data-query and asset-invoke entries, which the Java, TypeScript,
and Python clients encode identically. A Spring AI or LangChain4j agent on the JVM, a Node
service, a browser tab, and a Python notebook can all read and act on the same Worker routes
through one connector and one credential.

### What was run

`CelldFleetTest` and `CelldConnectorTest` cover Java agents, in process and over TCP. On
2026-10-25 a Node peer over TCP and a headless Chromium page over WebSocket ran the flow above
against this connector's jar and a stand-in Worker. Each discovered the card, queried the GET
route, and invoked the POST route. Under the Node peer, a retry under the same invocation id
reached the Worker zero times, an unlisted operation was refused by its `invoke-ops` rule, and an
identical query from a second peer was answered from the space without a call to the Worker. The
Python client is tested against the same connector runtime in `agentspaces-connect-core`
(`PythonConnectorClientTest`). Neither of those cross-language runs is part of this project's
test suite yet.

## Using it

Two files, or one. The fleet file is the `agentspaces:` subtree the Node CLI, the Spring starter,
the Micronaut library, and `FleetRuntime` read (keystore, bind, groups with `founding` or `join`,
seeds, security profile); the manifest is a `celld:` subtree that may live in the same file or in
its own.

```yaml
agentspaces:
  keystore: keys                      # the connector's identity, created on first run
  bind: 0.0.0.0:7500
  tick-millis: 250
  security:
    profile: mtls                     # or dev-local, or an oidc profile with the connector scopes
  groups:
    - name: inventory-fleet
      founding: inventory-v1          # or join: <group id>
      seeds: [ 10.0.0.5:7460 ]
      spaces:
        - name: data                  # queries, results, invocations, and results flow here

celld:
  application: inventory              # workers://inventory/route/<name>
  base-url: https://inventory.internal.example
  runtime: celld                      # celld | workerd | cloudflare (access.runtime)
  connector-name: inventory-connector # stamped into results; default celld-<application>
  space: data                         # the group's space to serve from; default data
  credential:
    bearer-env: INVENTORY_TOKEN       # or bearer: <token>, bearer-file: /run/secrets/token,
                                      # or mtls: { key-store, key-store-password, key-store-type,
                                      #            trust-store, trust-store-password, trust-store-type }
  timeouts:
    connect: 5s                       # default 5s
    request: 30s                      # default 30s; a route that misses it answers `timeout`
  max-body-bytes: 1048576             # default 1 MiB; a larger body answers `target-error`
  freshness: PT1M                     # card refresh cadence; the card TTL is twice it
  allow-plaintext: false              # true admits an http:// base URL on a private network
  fleet-authorizer: true              # false serves without the fleet's scopes (development)
  routes:
    - name: stock                     # the asset route/stock
      description: Stock levels by SKU and warehouse
      shape: com.example.Stock#v1
      freshness: PT5M                 # per group; default the manifest's
      operations:
        - name: getStock              # GET = a read operation (access.read-ops)
          method: GET
          path: /api/stock
          params: [sku, warehouse]    # the only parameters the connector will pass
        - name: reserveStock          # POST = an invoke operation (access.invoke-ops)
          method: POST
          path: /api/stock/reserve
          params: [sku, qty, warehouse]
          idempotent: true            # the handler deduplicates on Aspace-Invocation-Id
        - name: auditStock
          method: POST
          path: /api/stock/audit
          params: [warehouse]
          idempotent: false           # the card says so: a retry may run it twice
```

The jar names `CelldConnector` as its main class but carries no dependencies, so copy them beside
it and start it with a classpath. Add an SLF4J binding (for example `slf4j-simple`) to `target/lib`
to see the connector's log; without one it logs nothing.

```
mvn -q clean package
mvn -q dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib
java -cp 'target/agentspaces-connect-celld-0.5.0.jar:target/lib/*' \
     ai.badmonkey.agentspaces.connect.celld.CelldConnector fleet.yaml               # manifest = the file's celld: subtree
java -cp 'target/agentspaces-connect-celld-0.5.0.jar:target/lib/*' \
     ai.badmonkey.agentspaces.connect.celld.CelldConnector fleet.yaml manifest.yaml
```

In code: `CelldConnector.run(fleetFile, manifestFile)` returns the `FleetRuntime`; or build the
pieces, `new CelldConnector(runtime.context("inventory-fleet"), CelldManifest.load(path))`, and add
it as a `FleetParticipant`. Durations take ISO-8601 (`PT5M`) or the short form (`5s`, `250ms`,
`2m`, `1h`, `1d`). Unknown keys fail the parse, naming the place.

### What the fleet sees

One `AssetCard` per route group (SPEC §6.1a fields, nothing added), id `<application>:route/<name>`
so a refresh replaces the earlier card (`<application>:route/<name>@<connector-name>` in
active-active mode, below):

| Field | Value |
|---|---|
| `asset` | `route/<name>` |
| `uri` | `workers://<application>/route/<name>` |
| `shape`, `description`, `freshness` | the manifest's; the TTL is twice the cadence |
| `costHints.operations` | the number of operations |
| `access.mode` | `read-only`, `invoke`, or `read-only,invoke` |
| `access.query` | `aspace:cap/data-query`, when a GET route exists |
| `access.invoke` | `aspace:cap/asset-invoke`, when a POST route exists |
| `access.read-ops`, `access.invoke-ops` | the GET and POST operation names, comma lists |
| `access.idempotent` | the invoke operations whose route deduplicates on the invocation id |
| `access.params-<op>` | each operation's declared parameter names |
| `access.runtime`, `access.connector` | the manifest's runtime; `java` |

An agent reads with `DataQueryClient.fetch("route/stock", Map.of("op", "getStock", "sku", "A-1"), timeout)`:
the connector sends `GET /api/stock?sku=A-1` with `Authorization: Bearer ...`, and the JSON body
becomes the result rows (an array of objects is one row per object; an object is one row; a scalar
is one row under `value`; nested values stay JSON text). When a group has exactly one read
operation `op` may be left out. An agent invokes with
`AssetInvocations.invoke(space, card, "reserveStock", Map.of("sku", "A-1", "qty", "2"))`: the
connector takes the invocation under the claim lattice, sends `POST /api/stock/reserve` with the
parameters as the JSON body and `Aspace-Invocation-Id: <invocationId>`, and the JSON body becomes
the result map. A retry under the same id is answered from the connector's remembered result and
never reaches the route; a handler written for the connector deduplicates on the header as well,
because a connector restart forgets, and the manifest says which routes do (`idempotent`).

### Refusals

Each refusal names its rule and is answered without a call to the deployment:

| Request | Answer |
|---|---|
| an asset the manifest does not list | query error `routes: ...`; invoke `unknown-operation`, rule `routes` |
| a read of an operation not in `read-ops`, or a POST route asked as a read | query error `read-ops: ...` |
| an invoke of an operation not in `invoke-ops` | `unknown-operation`, rule `invoke-ops` (the runtime refuses on the card before the provider is asked) |
| a parameter the operation does not declare | query error `params: ...`; invoke `bad-params`, rule `params` |
| a requester without `aspace:connector-invoke:<asset>:<operation>` | `refused-scope`, rule the scope name |
| a non-2xx status, an unreachable route, a non-JSON body | `target-error`, rule `http-status`, `route`, `json` |
| a body over `max-body-bytes` | `target-error`, rule `max-body-bytes` |
| a route past `timeouts.request` | `timeout`, rule `timeouts.request` |

## Large data, objects, and changes

With the fleet's content swarm on (`agentspaces.capabilities.enabled: true` and
`agentspaces.content.enabled: true` in the fleet file), the connector moves data of any size
between the deployment and the fleet. Every feature below is opt-in by a manifest key, and a
manifest without them behaves exactly as before. A manifest that uses one while the swarm is off
fails at startup, naming `agentspaces.content.enabled`.

```yaml
celld:
  # ... application, base-url, credential, as above ...
  state: /var/lib/celld-connector/inventory   # the object index and the change cursor
  timeouts: { request: 30s, transfer: 30m, fetch: 30m }
  artifacts:
    max-bytes: 4GiB          # the largest body the connector publishes
    inline-max: 4MiB         # result: auto answers rows below this, an artifact above it
    piece-size: 1MiB         # default: the fleet's agentspaces.content.piece-size
    lease: P7D               # the manifest lease
    replicate: true          # write the manifest to the content space
  routes:
    - name: exports
      operations:
        - { name: nightly, method: GET, path: /api/exports/nightly, result: artifact, media-type: application/x-ndjson }
    - name: stock
      operations:
        - { name: getStock, method: GET, path: /api/stock, params: [sku] }
        - { name: snapshot, method: GET, path: /api/stock/snapshot, result: artifact,
            media-type: application/x-ndjson, snapshot: true, consistency: snapshot, refresh: PT1H, keep: 2 }
    - name: ingest
      operations:
        - { name: load, method: POST, path: /api/ingest, params: [source, batch],
            body: artifact, artifact-param: source, max-upload-bytes: 1GiB }
  objects:
    - name: models
      list: /api/models              # GET ?after=&limit= -> {items: [{key, size, etag, mediaType, attrs}], after}
      get: /api/models/object        # GET ?key=<key> with Range and If-Match -> 206
      cadence: PT5M
      piece-size: 4MiB
      hash-budget: 2GiB              # bytes hashed per list pass
      collection: { name: models, description: Model checkpoints of the inventory deployment }
  changes:
    path: /api/changes               # GET ?after=<cursor>&limit=<n> -> {cursor, changes}
    poll: 10s
    stream: /api/changes/stream      # optional Server-Sent Events over the same cursor
  replicas: { mode: single }         # or active-active
```

### Large results

An operation with `result: artifact` streams its body into the swarm and answers one artifact
row: `root`, `pieces`, `size`, `mediaType`, `name`, and `taken`. `result: auto` answers rows below
`inline-max` and an artifact above it. The connector counts the bytes as it reads them, so the
bound holds although celld removes `Content-Length` from a Worker's response; a body over
`max-bytes`, or still open after `timeouts.transfer`, publishes nothing and answers an error naming
the bound. A repeated query within the freshness window reads the same row from the space, so
twenty agents asking for the morning export cost one call to the deployment, and the agents then
fetch the pieces from the connector and from each other. `max-body-bytes` keeps its meaning: the
bound on an inline JSON body.

```java
DataQueryClient client = new DataQueryClient(fleet.group().space("data"), "planner");
Map<String, String> row = client.fetch("route/exports", Map.of("op", "nightly"), Duration.ofSeconds(30))
        .orElseThrow().result().rows().getFirst();
ArtifactRows.Artifact export = ArtifactRows.parse(row).orElseThrow();   // agentspaces-connect-core
Fetch fetch = fleet.content().orElseThrow().fetch(export.root());        // from every holder at once
```

### Snapshots

A GET operation with `snapshot: true` serializes state the Worker chooses, such as a Durable
Object's rows walked with `ctx.storage.sql.exec`. celld offers no Durable Object export or
point-in-time recovery, so the Worker author owns the consistency the manifest declares
(`snapshot` or `best-effort`). The connector takes the snapshot on `refresh`, and again when a
change notice names the route, at most once per `min-interval`. It tags each manifest
`snapshot-of=<application>:route/<name>` and `taken=<instant>`, so `Content.search` finds the
newest, and keeps the newest `keep` roots pinned.

### Objects served where they lie

An object source serves a deployment's objects to the swarm without a copy. Each pass lists the
objects, hashes each new key and ETag once (within `hash-budget`), and publishes it through an
`http-range` store: every piece a member asks for is one `GET <get>?key=<key>` with `Range` and
`If-Match`, hashed whole at its first serve and cached on the connector's disk for the sub-ranges
that follow. A 412, a 416, a whole body in answer to a range, or a changed ETag forgets the root,
and the next pass hashes the new version; a key that leaves the list leaves the collection. Keys
travel only as an encoded query value, and a key over 1,024 bytes or with control characters is
refused. The index under `state` lets a restart register unchanged objects without reading them.
A source with a `collection` advertises a `content-collection` card, `content:<name>`, which
answers `tag` (an attribute value), `mediaType`, `namePrefix`, `origin`, `maxAgeMillis`, `after`,
and `limit`. The get route must honour ranges; `@agentspaces/workers/routes` has `serveRange` for
an R2 bucket.

### Artifact inputs

An operation with `body: artifact` carries an artifact from the fleet into the deployment. The
requester passes a content root in `artifact-param`; the connector checks the requester may fetch
that root (`aspace:content-fetch`), refuses an artifact over `max-upload-bytes` before fetching it,
fetches it from every holder within `timeouts.fetch`, and POSTs the plaintext with
`Content-Length`, the other parameters as query parameters, `Aspace-Invocation-Id`, and
`Aspace-Content-Root`. The connector keeps the invocation claimed while it works, so a retry waits
for the one answer. `body: artifact-chunked` sends the artifact piece by piece (`Aspace-Upload:
piece` with `Aspace-Piece-Index`, then `Aspace-Upload: finish`), for an artifact above the
deployment's body bound: celld accepts 1 GiB by default (`CELLD_MAX_REQUEST_BODY_BYTES`), and
Cloudflare's Free and Pro plans accept 100 MB.

### Change notices

celld offers no change feed or outside Queue consumer, so the Worker serves its own: a change
route that answers `{cursor, changes: [{kind, route | object, key, row?}]}` after a cursor
(`kind` is `upsert`, `delete`, or `object`). The connector polls it, or holds its Server-Sent
Events stream and reconnects with `Last-Event-ID`, and keeps the cursor under `state`. A route
change becomes a leased `AssetChange` for `route/<name>`, withdraws the connector's cached results
for it so the next query reaches the deployment, and refreshes its snapshots; an object change
lists the object source again. A first start takes the cursor and replays no history. A change
never runs an operation, and an unknown kind, route, or object is dropped and counted.
`ChangeLog` in `@agentspaces/workers/routes` keeps the log in a Durable Object's SQLite.

### Several connectors for one deployment

With `replicas: { mode: active-active }`, two or more connectors with their own identities serve
one manifest. Each card id carries the connector name (`inventory:route/stock@inv-a`), because the
fleet's card cache keys cards by id. Before it runs an invocation, a connector looks for a result
another replica already wrote and answers with it, and each replica writes the others' results
again under its own identity, so a requester gets its answer through whichever card it used; the
Worker still deduplicates on `Aspace-Invocation-Id` for the race in between. A non-idempotent
operation needs `non-idempotent-on: <connector-name>`, and only that connector advertises it. Equal
bytes give equal roots, so the replicas' holdings merge, and the live replica with the lowest peer
id polls the change route, leaving its cursor in the space for the next one.

### A deployment behind NAT

A connector that sets no `agentspaces.bind` only dials out and reaches the fleet through relays,
on a pinned path of up to four. Its cards, results, and pieces reach cloud agents that way, and a
relay that loses a link tells the origin, which finds another path. On a metered uplink, set
`agentspaces.content.upload-bytes-per-day` and `seed-mode: complete-only` on the connector, and
`agentspaces.relay.budget-bytes-per-day` on the site's relay; fetchers that complete serve each
other, so the uplink carries each piece about once.

### The proxy in front of celld

celld terminates TLS on neither of its listeners and authenticates no user: its documentation
says to terminate TLS and authenticate in a proxy or the application. Point `base-url` at a proxy
in front of celld's public listener that checks the connector's bearer token or client
certificate, and never at the internal listener, which carries the operator API. With Caddy:

```
inventory.internal.example {
    @connector header Authorization "Bearer {$INVENTORY_TOKEN}"
    handle @connector {
        reverse_proxy 127.0.0.1:8787
    }
    respond 401
}
```

### Card keys and refusals

The route-group card adds these `access` keys when the manifest uses the features:

| Key | Value |
|---|---|
| `artifact-ops`, `media-type-<op>` | the operations that answer with an artifact, and each one's media type |
| `snapshot-ops`, `consistency-<op>` | the snapshot operations, and each one's declared consistency |
| `upload-ops`, `artifact-param-<op>` | the operations that take an artifact body, and the parameter carrying the root |
| `changes` | `true` when a change route covers the group |

| Request | Answer |
|---|---|
| an artifact operation while the swarm is off | query error `artifacts: ...`; invoke `target-error`, rule `artifacts` |
| a body over `artifacts.max-bytes`, or open past `timeouts.transfer` | `target-error`, rule `artifacts.max-bytes`; `timeout`, rule `timeouts.transfer` |
| an artifact parameter that is not a content root | `bad-params`, rule `artifact-param` |
| a requester that may not fetch the root | `refused-scope`, rule `content-fetch` |
| an artifact over `max-upload-bytes`, or a 413 from the route | `target-error`, rule `max-upload-bytes` |
| an artifact not fetched within `timeouts.fetch` | `timeout`, rule `timeouts.fetch` |

`CelldConnector.stats()` counts the calls, the queries and invocations, the artifacts and their
bytes, the ranged reads and misses, the uploads, the changes seen and dropped, the keys refused,
the roots forgotten, and the relay signals.

## Trust boundary

The connector's identity holds the authority over the routes it lists: it signs the cards and
the results, and an agent that trusts a `workers://` card trusts that peer for those routes and
nothing else. The credential is the operator's, read from the manifest (a token, an environment
variable, a file, or a key store) and sent only to the manifest's base URL; agents never see it,
and no parameter, path, or header from a request reaches the deployment unless the manifest
declared it. The fleet's authorizer grants the `aspace:connector-serve:<asset>` scope, without
which the connector does not advertise the asset, and `aspace:connector-invoke:<asset>:<operation>`,
judged against the requesting entry's signer, without which an invocation is answered with
`refused-scope` and the route is never called. Under the membership-rooted profiles (`dev-local`,
`mtls`) an ungranted operation defaults to any admitted member; under the OIDC profiles the
identity provider issues the scopes. The connector never runs SQL text, never evaluates code,
never calls a route or sends a parameter the manifest does not name, and keeps no application
state: the deployment stays authoritative, and the fleet carries requests, results, and cards.

## Key files

| File | What it is |
| --- | --- |
| `CelldManifest` | The manifest record and parser: application, base URL, runtime, credential (`Credential`), timeouts, body bound, freshness, the plaintext and authorizer switches, and `RouteGroup`s of `Operation`s (name, `GET`/`POST`, path, declared params, `idempotent`); `load(Path)`, `load(InputStream)`, `parse(String)`, `fromDocument` (the `celld:` subtree of a shared file). |
| `CelldClient` | The `java.net.http.HttpClient` over the deployment: bearer header or client certificate, connect and request timeouts, the body bound, `get(operation, params)` and `post(operation, params, invocationId)`; `RouteException` carries the error code (`target-error` or `timeout`) and the rule. |
| `CelldAssetProvider` | The `AssetProvider`: `describeAssets` (the cards above), `query` (GET), `invoke` (POST); refuses unlisted routes and operations and undeclared parameters before any call. |
| `CelldConnector` | The main and the `FleetParticipant`: `run(fleetFile, manifestFile)` (or `launch`, which returns the connector too) starts a `FleetRuntime` from the fleet file with the object stores in the swarm and wires a `ConnectorRuntime` on the manifest's space with the group's discovery and, by default, the fleet's authorizer; wires the parts below when the manifest names them; elects the change poller among replicas; refreshes cards on the card cadence; `stats()`; closes with the runtime. |
| `ArtifactResults` | Large results and snapshots: streams a body into `Content.publish`, bounded by bytes read and the transfer deadline, and answers the artifact row; keeps each operation's newest roots pinned. |
| `ArtifactUploads` | Artifact inputs: checks the artifact parameter and `CONTENT_FETCH`, fetches the root, and streams it to the route whole or piece by piece. |
| `HttpRangePieceStore` | The read-only `http-range` content store: one ranged, conditional GET per piece read; a miss forgets the root. |
| `ObjectSources` | Lists each object source, hashes within the budget, publishes through `Content.publishExternal`, forgets what left, keeps the restart index, and builds the collection cards. |
| `ChangeWatcher` | Polls or streams the change route, keeps the cursor, and passes route and object changes on. |
| `SnapshotScheduler` | Takes snapshots on their cadence and on change, at most once per `min-interval`. |
| `BoundedBody` | A response body bounded by bytes read and by a deadline, whatever the headers say. |

## Key dependencies

`agentspaces-connect-core` (SPI, `ConnectorRuntime`, `AssetInvocations`, `DataQueryClient`,
`ArtifactRows`), `agentspaces-content` (the swarm, `ExternalStore`, `CollectionAssetProvider`),
`agentspaces-runtime` (the plain-Java peer, `PropertiesLoader`), `jackson-databind`,
`jackson-dataformat-yaml`, `slf4j-api`. No framework: the enforcer bans Spring and Micronaut.

## Requirements to tests

| Requirement | Test |
| --- | --- |
| The manifest format: base URL, bearer or mTLS credential, route groups with mode, params, shape, `idempotent` | `CelldManifestTest.theReadmeManifestParsesWithEveryFieldBound`, `defaultsFillWhatAMinimalManifestLeavesOut`, `credentialsAreABearerOrAnMtlsKeyStore`, `durationsTakeIsoAndTheShortForm`, `routeGroupsExposeTheirOperationsByKind` |
| The manifest shares the fleet file or stands alone; typos and unsafe settings fail at startup naming the place | `CelldManifestTest.theManifestMayStandAloneOrShareTheFleetFile`, `thePlaintextRuleAndTheUrlRules`, `unknownKeysAndBadShapesFailNamingThePlace` |
| One `workers://<application>/route/<name>` card per route group with `read-ops`, `invoke-ops`, `idempotent`, `runtime`, `connector = java`; ids stable across refreshes | `CelldAssetProviderTest.cardsFollowAppendixFWithStableIds` |
| A read is a GET with the declared parameters as query parameters and the bearer header; the JSON body is the rows; `op` defaults on a single read operation | `CelldAssetProviderTest.aReadIsAGetWithTheDeclaredParametersAsQueryParametersAndTheBearer`, `aReadWithoutOpOnAGroupWithSeveralReadOperationsIsRefused` |
| An invocation is a POST with the parameters as the JSON body and `Aspace-Invocation-Id`; the JSON body is the result | `CelldAssetProviderTest.anInvocationIsAPostWithTheJsonBodyAndTheInvocationIdHeader` |
| An unlisted route or operation, or an undeclared parameter, is refused with an error naming the rule and never called | `CelldAssetProviderTest.refusalsNameTheRuleAndNeverCallTheDeployment`, `CelldFleetTest.anUnlistedOperationAndAnUndeclaredParameterNeverReachTheWorker` |
| Every bound is a manifest setting: `timeouts.request`, `max-body-bytes`; target failures become error results | `CelldAssetProviderTest.targetFailuresBecomeErrorResultsNamingTheBound`, `theMtlsStoresAreLoadedWhenNamed` |
| An agent discovers the card, queries through `DataQueryClient` (one GET per freshness window), and invokes through `AssetInvocations.invoke` exactly once under a retry (the stub Worker counts calls per invocation id) | `CelldFleetTest.anAgentDiscoversTheCardQueriesTheGetRouteAndInvokesThePostRouteOnceUnderARetry` |
| Under an authorizer only a requester holding `aspace:connector-invoke:<asset>:<operation>` reaches the route | `CelldFleetTest.underAnAuthorizerOnlyARequesterHoldingTheInvokeScopeReachesTheWorker` |
| The main joins a group from a fleet file of the `agentspaces:` shape and serves the manifest; a second peer joins through the seed over TCP, discovers, queries, and invokes | `CelldConnectorTest.theMainJoinsFromTheFleetFileAndServesTheManifest`, `aSeparateManifestFileIsReadBesideTheFleetFile`, `aManifestNamingASpaceTheGroupLacksFailsAtStartupNamingTheProperty` |
| Every new manifest key binds with its default; a manifest without them uses no swarm; each artifact, snapshot, upload, size, piece-size, version, and replica rule fails at startup naming the place | `CelldManifestPhase2Test.everyPhase2KeyBindsWithItsValue`, `defaultsFillTheNewBlocksAndAZeroPointFiveManifestUsesNoSwarm`, `theArtifactRulesFailAtStartupNamingThePlace`, `activeActiveRefusesANonIdempotentOperationUnlessOneConnectorOwnsIt`, `sizesTakeBytesAndTheBinarySuffixes` |
| A ranged read accepts only a matching 206 and misses on 412, 416, a short range, or 404; a body is bounded by bytes read and by the transfer deadline; a hostile key travels only as one query value; the event stream resumes after the last id | `CelldClientPhase2Test.aRangedReadAcceptsOnlyAMatching206AndMissesOnEveryOtherAnswer`, `aBodyIsBoundedByTheBytesReadWhateverTheHeadersSay`, `aStalledBodyEndsAtTheTransferDeadline`, `hostileKeysAreRefusedAndOrdinaryOnesTravelEncoded`, `theEventStreamFollowsTheSseFormatAndResumesAfterTheLastId` |
| A large result leaves as one artifact from one call, and every fetcher reads it; a body over the bound publishes nothing; `auto` answers rows below the bound and an artifact above it | `CelldSwarmFleetTest.aLargeResultLeavesAsOneArtifactAndEveryFetcherReadsItFromOneCall`, `aBodyOverTheBoundPublishesNothingAndAutoAnswersInlineBelowIt` |
| Objects are served where they lie with one ranged read per piece, listed in a collection, and forgotten when they change or leave; a restart registers indexed objects without reading them | `CelldSwarmFleetTest.objectsAreServedWhereTheyLieListedInACollectionAndForgottenWhenTheyChange`, `aRestartRegistersIndexedObjectsWithoutReadingThemAgain` |
| An artifact reaches the ingest route once under a retry, whole or piece by piece; an oversize artifact and a value that is not a root are refused before any fetch | `CelldSwarmFleetTest.anArtifactReachesTheIngestRouteOnceWholeOrPieceByPieceAndBoundsRefuseBeforeFetching` |
| A change becomes an `AssetChange`, withdraws the cached result, and refreshes the snapshot | `CelldSwarmFleetTest.aChangeBecomesAnAssetChangeWithdrawsTheCachedResultAndRefreshesTheSnapshot` |
| Two connectors share one deployment with their own cards, one change poller, and one effect per invocation | `CelldSwarmFleetTest.twoConnectorsShareOneDeploymentWithTheirOwnCardsAndOneEffectPerInvocation` |
| An artifact manifest without the swarm fails at startup naming the property | `CelldSwarmFleetTest.anArtifactManifestWithoutTheSwarmFailsAtStartupNamingTheProperty` |

## Building

```
mvn -B -ntp verify
```

The tests run against a local `com.sun.net.httpserver.HttpServer` standing in for the Worker
and an in-process SimNetwork fleet (plus one loopback TCP fleet for the main); no network, no
celld. The mTLS path is exercised to the point of loading the stores; a live mTLS exchange needs
a deployment with a client-certificate listener.

## The end-to-end harness

`e2e/run.sh` runs the connector against a real celld node on one machine: Azurite as the fleet
bucket, the celld release binary, a test application with a Durable Object and two R2 buckets,
and `CelldLiveTest` from an agent peer. It needs no cloud account and no Docker; `e2e/README.md`
lists what it needs and what it checks.

## Notes

- A team that wants the connector inside its deployment can add the connector cell
  (`@agentspaces/workers`), which reads bindings from the deployment's own `env` and needs no
  manifest beyond the allowlist. This project serves the same routes from beside the deployment,
  driven by the operator's manifest, and leaves the deployment as it is.
- `ConnectorRuntime` remembers an answered invocation for 24 h, so a retry under the same id
  never re-runs the route here; the handler's own `once(invocationId)` covers a connector that
  restarted in between, and `idempotent: false` tells agents which routes have none.

## Releasing

The `central-publish` profile attaches the sources and javadoc jars, signs
every artifact with GPG (the passphrase from `MAVEN_GPG_PASSPHRASE`, or a GPG
agent), and uploads through the Sonatype Central Portal using the `central`
server id in `settings.xml`. The dry run builds the jars without signing or
uploading anything:

```sh
mvn -Pcentral-publish -DskipTests -Dgpg.skip=true clean package
```

The release signs the artifacts and uploads them:

```sh
mvn -Pcentral-publish clean deploy
```

`autoPublish` is off, so the upload waits on the Portal as a validated bundle
until you publish it there. CI runs the same profile unsigned on every push,
so a javadoc or packaging problem fails a pull request before it can fail a
release.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) and the
[code of conduct](CODE_OF_CONDUCT.md). Report a vulnerability privately, as
[SECURITY.md](SECURITY.md) describes.

## Licence

Apache License 2.0, Copyright 2026 Bad Monkey, Inc. See `LICENSE` and `NOTICE`.
