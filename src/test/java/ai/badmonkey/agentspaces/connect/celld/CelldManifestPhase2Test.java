/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.connect.celld;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The phase 2 manifest blocks: every key binds with its default, a manifest with none of them
 * behaves as 0.5.0, and each rule that guards an artifact feature fails at startup naming the place.
 */
class CelldManifestPhase2Test {

    private static final String BASE = """
            celld:
              application: shop
              base-url: https://shop.example
              credential: { bearer: t }
            """;

    private static final String FULL = BASE + """
              state: /var/lib/celld-connector/shop
              timeouts: { transfer: 10m, fetch: 5m }
              artifacts: { v: 1, max-bytes: 2GiB, inline-max: 1MiB, piece-size: 512KiB, lease: P3D, replicate: false }
              routes:
                - name: exports
                  operations:
                    - { name: nightly, method: GET, path: /api/exports/nightly, result: artifact, media-type: application/x-ndjson }
                    - { name: auto, method: GET, path: /api/exports/auto, result: auto }
                    - { name: snapshot, method: GET, path: /api/snapshot, result: artifact, snapshot: true, consistency: snapshot, refresh: PT1H, min-interval: 2m, keep: 3 }
                    - { name: load, method: POST, path: /api/ingest, params: [source, batch], body: artifact, artifact-param: source, max-upload-bytes: 1GiB }
                    - { name: loadChunked, method: POST, path: /api/ingest, params: [source], body: artifact-chunked, artifact-param: source }
              objects:
                - name: models
                  list: /api/models
                  get: /api/models/object
                  cadence: PT10M
                  piece-size: 4MiB
                  hash-budget: 10GiB
                  replicate: false
                  collection: { name: models, description: Checkpoints, freshness: PT30M }
              changes: { v: 1, path: /api/changes, poll: 5s, stream: /api/changes/stream, max-changes: 500 }
              replicas: { mode: active-active, non-idempotent-on: shop-a }
            """;

    private static String minimal(String operation) {
        return BASE + "  routes:\n    - name: r\n      operations:\n        - " + operation + "\n";
    }

    @Test
    void everyPhase2KeyBindsWithItsValue() {
        CelldManifest m = CelldManifest.parse(FULL);
        assertThat(m.state()).isEqualTo(Path.of("/var/lib/celld-connector/shop"));
        assertThat(m.transferTimeout()).isEqualTo(Duration.ofMinutes(10));
        assertThat(m.fetchTimeout()).isEqualTo(Duration.ofMinutes(5));
        assertThat(m.artifacts()).isEqualTo(new CelldManifest.Artifacts(2L << 30, 1L << 20, 512 * 1024,
                Duration.ofDays(3), false));
        CelldManifest.RouteGroup exports = m.routes().getFirst();
        CelldManifest.Operation nightly = exports.operation("nightly");
        assertThat(nightly.result()).isEqualTo(CelldManifest.Result.ARTIFACT);
        assertThat(nightly.mediaType()).isEqualTo("application/x-ndjson");
        assertThat(exports.operation("auto").result()).isEqualTo(CelldManifest.Result.AUTO);
        CelldManifest.Operation snapshot = exports.operation("snapshot");
        assertThat(snapshot.snapshot()).isEqualTo(new CelldManifest.Snapshot("snapshot", Duration.ofHours(1),
                Duration.ofMinutes(2), 3));
        CelldManifest.Operation load = exports.operation("load");
        assertThat(load.body()).isEqualTo(CelldManifest.Body.ARTIFACT);
        assertThat(load.artifactParam()).isEqualTo("source");
        assertThat(load.maxUploadBytes()).isEqualTo(1L << 30);
        assertThat(exports.operation("loadChunked").body()).isEqualTo(CelldManifest.Body.ARTIFACT_CHUNKED);
        CelldManifest.ObjectSource models = m.objectSource("models");
        assertThat(models).isEqualTo(new CelldManifest.ObjectSource("models", "/api/models", "/api/models/object",
                Duration.ofMinutes(10), 4 << 20, 10L << 30, false,
                new CelldManifest.CollectionSpec("models", "Checkpoints", Duration.ofMinutes(30))));
        assertThat(m.changes()).isEqualTo(new CelldManifest.Changes("/api/changes", Duration.ofSeconds(5),
                "/api/changes/stream", 500));
        assertThat(m.replicas()).isEqualTo(new CelldManifest.Replicas("active-active", "shop-a"));
        assertThat(m.usesSwarm()).isTrue();
        assertThat(m.activeActive()).isTrue();
    }

    @Test
    void defaultsFillTheNewBlocksAndAZeroPointFiveManifestUsesNoSwarm() {
        CelldManifest m = CelldManifest.parse(minimal("{ name: get, method: GET, path: /x }"));
        assertThat(m.artifacts()).isEqualTo(CelldManifest.Artifacts.defaults());
        assertThat(m.artifacts().maxBytes()).isEqualTo(4L << 30);
        assertThat(m.artifacts().inlineMax()).isEqualTo(4L << 20);
        assertThat(m.objects()).isEmpty();
        assertThat(m.changes()).isNull();
        assertThat(m.replicas()).isEqualTo(CelldManifest.Replicas.single());
        assertThat(m.transferTimeout()).isEqualTo(Duration.ofMinutes(30));
        assertThat(m.state()).isEqualTo(Path.of(System.getProperty("user.home"), ".agentspaces", "celld", "shop"));
        CelldManifest.Operation get = m.routes().getFirst().operation("get");
        assertThat(get.result()).isEqualTo(CelldManifest.Result.INLINE);
        assertThat(get.mediaType()).isEqualTo("application/json");
        assertThat(get.body()).isEqualTo(CelldManifest.Body.JSON);
        assertThat(m.usesSwarm()).isFalse();
        CelldManifest.ObjectSource source = CelldManifest.parse(BASE + """
                  routes: [ { name: r, operations: [ { name: g, method: GET, path: /g } ] } ]
                  objects: [ { name: o, list: /l, get: /g } ]
                """).objectSource("o");
        assertThat(source.cadence()).isEqualTo(Duration.ofMinutes(5));
        assertThat(source.hashBudget()).isEqualTo(2L << 30);
        assertThat(source.replicate()).isTrue();
        assertThat(source.collection()).isNull();
    }

    @Test
    void theArtifactRulesFailAtStartupNamingThePlace() {
        assertThatThrownBy(() -> CelldManifest.parse(minimal("{ name: g, method: GET, path: /x, media-type: text/csv }")))
                .hasMessageContaining("celld.routes[r].operations[g].media-type 'text/csv' is not JSON, so the operation"
                        + " needs result: artifact");
        assertThatThrownBy(() -> CelldManifest.parse(minimal("{ name: g, method: GET, path: /x, result: sometimes }")))
                .hasMessageContaining("result 'sometimes' must be one of [inline, artifact, auto]");
        assertThatThrownBy(() -> CelldManifest.parse(minimal("{ name: g, method: POST, path: /x, result: artifact, snapshot: true }")))
                .hasMessageContaining("is a snapshot, which must be a GET with result: artifact");
        assertThatThrownBy(() -> CelldManifest.parse(minimal("{ name: g, method: GET, path: /x, keep: 2 }")))
                .hasMessageContaining("celld.routes[r].operations[g].keep applies to snapshot: true only");
        assertThatThrownBy(() -> CelldManifest.parse(minimal(
                "{ name: g, method: GET, path: /x, result: artifact, snapshot: true, consistency: strong }")))
                .hasMessageContaining("consistency 'strong' must be snapshot or best-effort");
        assertThatThrownBy(() -> CelldManifest.parse(minimal("{ name: g, method: GET, path: /x, body: artifact, artifact-param: s, params: [s] }")))
                .hasMessageContaining("an artifact body needs a POST route");
        assertThatThrownBy(() -> CelldManifest.parse(minimal("{ name: p, method: POST, path: /x, body: artifact, params: [s] }")))
                .hasMessageContaining("artifact-param must name one of the params [s]");
        assertThatThrownBy(() -> CelldManifest.parse(minimal("{ name: p, method: POST, path: /x, max-upload-bytes: 1MiB }")))
                .hasMessageContaining("apply to an artifact body only");
        assertThatThrownBy(() -> CelldManifest.parse(BASE + "  artifacts: { piece-size: 100KiB }\n"
                + "  routes: [ { name: r, operations: [ { name: g, method: GET, path: /g } ] } ]\n"))
                .hasMessageContaining("celld.artifacts.piece-size 102400 must be a power of two from 64KiB to 4MiB");
        assertThatThrownBy(() -> CelldManifest.parse(BASE + "  artifacts: { max-bytes: lots }\n"
                + "  routes: [ { name: r, operations: [ { name: g, method: GET, path: /g } ] } ]\n"))
                .hasMessageContaining("celld.artifacts.max-bytes 'lots' is not a size");
        assertThatThrownBy(() -> CelldManifest.parse(BASE + "  artifacts: { max-bytes: 1MiB, inline-max: 2MiB }\n"
                + "  routes: [ { name: r, operations: [ { name: g, method: GET, path: /g } ] } ]\n"))
                .hasMessageContaining("inline-max must not exceed max-bytes");
        assertThatThrownBy(() -> CelldManifest.parse(BASE + "  changes: { v: 2, path: /c }\n"
                + "  routes: [ { name: r, operations: [ { name: g, method: GET, path: /g } ] } ]\n"))
                .hasMessageContaining("celld.changes.v 2 is not a version this connector reads (1)");
        assertThatThrownBy(() -> CelldManifest.parse(BASE + "  objects: [ { name: o, list: /l, get: 'http://evil/x' } ]\n"
                + "  routes: [ { name: r, operations: [ { name: g, method: GET, path: /g } ] } ]\n"))
                .hasMessageContaining("celld.objects[o].get 'http://evil/x' must start with '/'");
        assertThatThrownBy(() -> CelldManifest.parse(BASE + "  objects: [ { name: o, list: /l, get: /g, prefix: x } ]\n"
                + "  routes: [ { name: r, operations: [ { name: g, method: GET, path: /g } ] } ]\n"))
                .hasMessageContaining("celld.objects[o] has unknown keys [prefix]");
    }

    @Test
    void activeActiveRefusesANonIdempotentOperationUnlessOneConnectorOwnsIt() {
        String routes = "  routes: [ { name: r, operations: [ { name: audit, method: POST, path: /a, idempotent: false } ] } ]\n";
        assertThatThrownBy(() -> CelldManifest.parse(BASE + "  replicas: { mode: active-active }\n" + routes))
                .hasMessageContaining("celld.routes[r].operations[audit] is not idempotent, and replicas.mode is"
                        + " active-active: name the one connector that advertises it in replicas.non-idempotent-on");
        assertThatThrownBy(() -> CelldManifest.parse(BASE + "  replicas: { mode: both }\n" + routes))
                .hasMessageContaining("celld.replicas.mode 'both' must be single or active-active");
        CelldManifest owner = CelldManifest.parse(BASE + "  connector-name: shop-a\n"
                + "  replicas: { mode: active-active, non-idempotent-on: shop-a }\n" + routes);
        CelldManifest other = CelldManifest.parse(BASE + "  connector-name: shop-b\n"
                + "  replicas: { mode: active-active, non-idempotent-on: shop-a }\n" + routes);
        CelldManifest.Operation audit = owner.routes().getFirst().operation("audit");
        assertThat(new CelldAssetProvider(owner).advertises(audit)).isTrue();
        assertThat(new CelldAssetProvider(other).advertises(audit)).isFalse();
        assertThat(new CelldAssetProvider(other).card(other.routes().getFirst(),
                ai.badmonkey.agentspaces.common.id.GroupId.of("zG"),
                ai.badmonkey.agentspaces.identity.PeerIdentity.generate().peerId(), java.time.Instant.EPOCH).id())
                .isEqualTo("shop:route/r@shop-b");
    }

    @Test
    void sizesTakeBytesAndTheBinarySuffixes() {
        assertThat(CelldManifest.parseSize("4194304", "x")).isEqualTo(4L << 20);
        assertThat(CelldManifest.parseSize("64KiB", "x")).isEqualTo(64L << 10);
        assertThat(CelldManifest.parseSize("4 MiB", "x")).isEqualTo(4L << 20);
        assertThat(CelldManifest.parseSize("2GiB", "x")).isEqualTo(2L << 30);
        for (String bad : List.of("4MB", "-", "1.5GiB", "99999999999GiB")) {
            assertThatThrownBy(() -> CelldManifest.parseSize(bad, "where")).hasMessageContaining("where '" + bad + "'");
        }
    }
}
