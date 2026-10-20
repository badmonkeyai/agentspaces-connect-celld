# agentspaces-connect-celld

The Java connector for a celld or Workers deployment its team cannot change: an operator's
manifest of allowlisted Worker routes becomes `workers://` AssetCards on `agentspaces-connect-core`,
GET routes answer `aspace:cap/data-query`, POST routes answer `aspace:cap/asset-invoke`, and every
call goes over HTTPS with the operator's bearer or mTLS credential and the invocation id as a
header. It runs the same route-group mapping `@agentspaces/workers` runs inside a deployment,
from outside it. It cannot enumerate bindings, so it publishes only what the manifest names.

**Architecture position.** Gateways and served interfaces: a connector (a non-agent member the
fleet sees as assets). SPEC §6.1a, §6.3; TECH-SPEC §6. Design of record:
`srad/agentspaces-js.md` §3.8, §4.7 (JS-C-07, JS-C-08, JS-C-09), §6.4 scenario S5a, §7.2,
appendix F (the route-group row); `srad/celld-assetcard-analysis.md` §3 and §6.

## Coordinates

```xml
<dependency>
  <groupId>ai.badmonkey.agentspaces</groupId>
  <artifactId>agentspaces-connect-celld</artifactId>
  <version>0.4.0</version>
</dependency>
```

It brings `agentspaces-connect-core` (the SPI and the protocols) and `agentspaces-runtime` (the
plain-Java peer from a fleet file), both 0.4.0, plus Jackson for the manifest and the JSON bodies.
Java 21. The jar's main class is `CelldConnector`.

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

```
java -jar agentspaces-connect-celld-0.4.0.jar fleet.yaml             # manifest = the file's celld: subtree
java -jar agentspaces-connect-celld-0.4.0.jar fleet.yaml manifest.yaml
```

In code: `CelldConnector.run(fleetFile, manifestFile)` returns the `FleetRuntime`; or build the
pieces, `new CelldConnector(runtime.context("inventory-fleet"), CelldManifest.load(path))`, and add
it as a `FleetParticipant`. Durations take ISO-8601 (`PT5M`) or the short form (`5s`, `250ms`,
`2m`, `1h`, `1d`). Unknown keys fail the parse, naming the place.

### What the fleet sees

One `AssetCard` per route group (SPEC §6.1a fields, nothing added), id `<application>:route/<name>`
so a refresh replaces the earlier card:

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
| `CelldClient` | The `java.net.http.HttpClient` over the deployment: bearer header or client certificate, connect and request timeouts, the body bound, `get(operation, params)` and `post(operation, params, invocationId)`; `RouteException` carries the appendix F error code and the rule. |
| `CelldAssetProvider` | The `AssetProvider`: `describeAssets` (the cards above), `query` (GET), `invoke` (POST); refuses unlisted routes and operations and undeclared parameters before any call. |
| `CelldConnector` | The main and the `FleetParticipant`: `run(fleetFile, manifestFile)` starts a `FleetRuntime` from the fleet file and wires a `ConnectorRuntime` on the manifest's space with the group's discovery and, by default, the fleet's authorizer; refreshes cards on the card cadence; closes with the runtime. |

## Key dependencies

`agentspaces-connect-core` (SPI, `ConnectorRuntime`, `AssetInvocations`, `DataQueryClient`),
`agentspaces-runtime` (the plain-Java peer, `PropertiesLoader`), `jackson-databind`,
`jackson-dataformat-yaml`, `slf4j-api`. No framework: the enforcer bans Spring and Micronaut.

## Requirements to tests

| Row (srad/agentspaces-js.md) | Test |
| --- | --- |
| JS-C-09 the manifest format: base URL, bearer or mTLS credential, route groups with mode, params, shape, `idempotent` | `CelldManifestTest.theReadmeManifestParsesWithEveryFieldBound`, `defaultsFillWhatAMinimalManifestLeavesOut`, `credentialsAreABearerOrAnMtlsKeyStore`, `durationsTakeIsoAndTheShortForm`, `routeGroupsExposeTheirOperationsByKind` |
| JS-C-09 the manifest shares the fleet file or stands alone; typos and unsafe settings fail at startup naming the place | `CelldManifestTest.theManifestMayStandAloneOrShareTheFleetFile`, `thePlaintextRuleAndTheUrlRules`, `unknownKeysAndBadShapesFailNamingThePlace` |
| JS-C-08 / appendix F the `workers://<application>/route/<name>` cards with `read-ops`, `invoke-ops`, `idempotent`, `runtime`, `connector = java`; ids stable across refreshes | `CelldAssetProviderTest.cardsFollowAppendixFWithStableIds` |
| JS-C-09 a read is a GET with the declared parameters as query parameters and the bearer header; the JSON body is the rows; `op` defaults on a single read operation | `CelldAssetProviderTest.aReadIsAGetWithTheDeclaredParametersAsQueryParametersAndTheBearer`, `aReadWithoutOpOnAGroupWithSeveralReadOperationsIsRefused` |
| JS-C-09 / appendix F an invocation is a POST with the parameters as the JSON body and `Aspace-Invocation-Id`; the JSON body is the result | `CelldAssetProviderTest.anInvocationIsAPostWithTheJsonBodyAndTheInvocationIdHeader` |
| JS-C-07 an unlisted route or operation, or an undeclared parameter, is refused with an error naming the rule and never called | `CelldAssetProviderTest.refusalsNameTheRuleAndNeverCallTheDeployment`, `CelldFleetTest.anUnlistedOperationAndAnUndeclaredParameterNeverReachTheWorker` |
| JS-C-09 every bound is a manifest setting: `timeouts.request`, `max-body-bytes`; target failures become error results | `CelldAssetProviderTest.targetFailuresBecomeErrorResultsNamingTheBound`, `theMtlsStoresAreLoadedWhenNamed` |
| S5a / JS-C-03, JS-C-10 an agent discovers the card, queries through `DataQueryClient` (one GET per freshness window), and invokes through `AssetInvocations.invoke` exactly once under a retry (the stub Worker counts calls per invocation id) | `CelldFleetTest.anAgentDiscoversTheCardQueriesTheGetRouteAndInvokesThePostRouteOnceUnderARetry` |
| JS-C-02 under an authorizer only a requester holding `aspace:connector-invoke:<asset>:<operation>` reaches the route | `CelldFleetTest.underAnAuthorizerOnlyARequesterHoldingTheInvokeScopeReachesTheWorker` |
| JS-C-09 the main joins a group from a fleet file of the `agentspaces:` shape and serves the manifest; a second peer joins through the seed over TCP, discovers, queries, and invokes | `CelldConnectorTest.theMainJoinsFromTheFleetFileAndServesTheManifest`, `aSeparateManifestFileIsReadBesideTheFleetFile`, `aManifestNamingASpaceTheGroupLacksFailsAtStartupNamingTheProperty` |

## Building

```
mvn -q -o verify
```

The tests run against a local `com.sun.net.httpserver.HttpServer` standing in for the Worker
and an in-process SimNetwork fleet (plus one loopback TCP fleet for the main); no network, no
celld. The mTLS path is exercised to the point of loading the stores; a live mTLS exchange needs
a deployment with a client-certificate listener.

## Notes

- The connector cell (`@agentspaces/workers`) is the first choice for a deployment a team can
  change: it reads bindings from its own `env` and needs no manifest beyond the allowlist. This
  project is for the deployment that cannot change (decision 15, `srad/agentspaces-js.md` §6.8).
- `ConnectorRuntime` remembers an answered invocation for 24 h, so a retry under the same id
  never re-runs the route here; the handler's own `once(invocationId)` covers a connector that
  restarted in between, and `idempotent: false` tells agents which routes have none.

## Licence

Apache License 2.0, Copyright 2026 Bad Monkey, Inc.
