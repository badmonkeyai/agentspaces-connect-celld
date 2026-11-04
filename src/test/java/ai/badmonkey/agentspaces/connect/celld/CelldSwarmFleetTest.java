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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.connect.ArtifactRows;
import ai.badmonkey.agentspaces.connect.AssetInvocations;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.connect.DataQueryClient;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.AssetChange;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.Fetch;
import ai.badmonkey.agentspaces.runtime.FleetContext;
import ai.badmonkey.agentspaces.runtime.FleetRuntime;
import ai.badmonkey.agentspaces.runtime.PropertiesLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static ai.badmonkey.agentspaces.connect.celld.CelldConnectorTest.await;
import static ai.badmonkey.agentspaces.connect.celld.CelldConnectorTest.freePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The connector on the content swarm (srad/celld-phase2.md scenarios 1 to 6 and 8), two or three
 * loopback TCP peers against a Worker that behaves as celld does: large results leave as artifacts
 * in one call, a body over the bound publishes nothing, objects are served where they lie with a
 * collection card and forgotten when they change, an artifact reaches an ingest route once, whole
 * or piece by piece, changes become asset changes that withdraw cached results and refresh the
 * snapshot, an index restart reads nothing, and two connectors share one deployment.
 */
class CelldSwarmFleetTest {

    private static final String FLEET = """
            agentspaces:
              bind: 127.0.0.1:%d
              tick-millis: 100
              capabilities: { enabled: true }
              content:
                enabled: true
                piece-size: 262144
                store: { backends: [memory], cache-external: true }
              groups:
                - name: celld-fleet
                  founding: celld-swarm-v1
                  seeds: [%s]
                  spaces:
                    - { name: data, settle-window-millis: 100 }
            celld:
              application: inventory
              base-url: %s
              allow-plaintext: true
              credential: { bearer: swarm-token }
              connector-name: %s
              state: %s
              freshness: PT1M
              artifacts: { max-bytes: 8MiB, inline-max: 64KiB, piece-size: 256KiB }
            %s
            """;

    private static final String ROUTES = """
              routes:
                - name: exports
                  operations:
                    - { name: nightly, method: GET, path: /api/exports/nightly, result: artifact, media-type: application/x-ndjson }
                    - { name: endless, method: GET, path: /api/exports/endless, result: artifact, media-type: application/octet-stream }
                    - { name: auto, method: GET, path: /api/exports/auto, result: auto }
                - name: stock
                  operations:
                    - { name: getStock, method: GET, path: /api/stock, params: [sku] }
                    - { name: snapshot, method: GET, path: /api/stock/snapshot, result: artifact, media-type: application/x-ndjson, snapshot: true, consistency: snapshot, keep: 2, min-interval: 1s }
                - name: ingest
                  operations:
                    - { name: load, method: POST, path: /api/ingest, params: [source, batch], body: artifact, artifact-param: source, max-upload-bytes: 4MiB }
                    - { name: loadSmall, method: POST, path: /api/ingest, params: [source], body: artifact, artifact-param: source, max-upload-bytes: 100KiB }
                    - { name: loadChunked, method: POST, path: /api/ingest, params: [source], body: artifact-chunked, artifact-param: source }
                    - { name: note, method: POST, path: /api/ingest, params: [text] }
              objects:
                - name: models
                  list: /api/models
                  get: /api/models/object
                  cadence: PT2S
                  piece-size: 256KiB
                  collection: { name: models, description: Model checkpoints of the inventory deployment }
              changes: { path: /api/changes, poll: 300ms }
            """;

    private SwarmWorker worker;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @BeforeEach
    void startWorker() throws IOException {
        worker = new SwarmWorker();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
        worker.close();
    }

    private CelldConnector.Running connector(Path dir, String name, int port, String seed, String extra)
            throws IOException {
        Path state = dir.resolve("state-" + name);
        Path fleetFile = dir.resolve("fleet-" + name + ".yaml");
        Files.writeString(fleetFile, FLEET.formatted(port, seed == null ? "" : seed, worker.baseUrl(), name, state,
                ROUTES + (extra == null ? "" : extra)));
        CelldConnector.Running running = CelldConnector.launch(fleetFile, null);
        closeables.add(running);
        return running;
    }

    private FleetRuntime agent(int seedPort) {
        Map<String, Object> props = new HashMap<>();
        props.put("agentspaces.bind", "127.0.0.1:" + freePort());
        props.put("agentspaces.tick-millis", "100");
        props.put("agentspaces.capabilities.enabled", "true");
        props.put("agentspaces.content.enabled", "true");
        props.put("agentspaces.content.store.backends[0]", "memory");
        props.put("agentspaces.groups[0].name", "celld-fleet");
        props.put("agentspaces.groups[0].founding", "celld-swarm-v1");
        props.put("agentspaces.groups[0].seeds[0]", "127.0.0.1:" + seedPort);
        props.put("agentspaces.groups[0].spaces[0].name", "data");
        props.put("agentspaces.groups[0].spaces[0].settle-window-millis", "100");
        FleetRuntime runtime = FleetRuntime.start(PropertiesLoader.flat(props));
        closeables.add(runtime);
        return runtime;
    }

    private static AssetCard card(FleetContext fleet, String uri) throws InterruptedException {
        assertThat(await(Duration.ofSeconds(30), () -> !fleet.group().discovery()
                .find(AssetCard.class, c -> uri.equals(c.uri())).isEmpty())).as("card " + uri).isTrue();
        return fleet.group().discovery().find(AssetCard.class, c -> uri.equals(c.uri())).getFirst();
    }

    private static Map<String, String> query(FleetContext fleet, String asset, Map<String, String> params) {
        Optional<DataQueryClient.Fetched> fetched = new DataQueryClient(fleet.group().space("data"), "analyst")
                .fetch(asset, params, Duration.ofSeconds(30));
        assertThat(fetched).as("an answer for " + asset + " " + params).isPresent();
        assertThat(fetched.get().result().error()).isNull();
        return fetched.get().result().rows().getFirst();
    }

    private static Optional<Map<String, String>> tryQuery(FleetContext fleet, String asset, Map<String, String> params) {
        return new DataQueryClient(fleet.group().space("data"), "analyst").fetch(asset, params, Duration.ofSeconds(10))
                .filter(f -> f.result().error() == null && !f.result().rows().isEmpty())
                .map(f -> f.result().rows().getFirst());
    }

    private static byte[] fetchAll(Content content, String root) throws Exception {
        Fetch fetch = content.fetch(root);
        assertThat(await(Duration.ofSeconds(60), fetch::complete)).as(fetch.trace().toString()).isTrue();
        try (SeekableByteChannel channel = content.openPlain(root)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            return buffer.array();
        }
    }

    // ------------------------------------------------------------------ scenario 1: large results

    @Test
    @Timeout(120)
    void aLargeResultLeavesAsOneArtifactAndEveryFetcherReadsItFromOneCall(@TempDir Path dir) throws Exception {
        byte[] export = SwarmWorker.random(3 * 1024 * 1024 + 123, 1);
        worker.export = export;
        int port = freePort();
        CelldConnector.Running running = connector(dir, "inv-a", port, null, null);
        FleetContext agent = agent(port).context();
        AssetCard exports = card(agent, "workers://inventory/route/exports");
        assertThat(exports.access()).containsEntry(CelldAssetProvider.ARTIFACT_OPS, "nightly,endless,auto")
                .containsEntry("media-type-nightly", "application/x-ndjson");

        Map<String, String> row = query(agent, "route/exports", Map.of("op", "nightly"));
        ArtifactRows.Artifact artifact = ArtifactRows.parse(row).orElseThrow();
        assertThat(artifact.size()).isEqualTo(export.length);
        assertThat(artifact.pieces()).isEqualTo((export.length + 262143) / 262144);
        assertThat(artifact.mediaType()).isEqualTo("application/x-ndjson");
        assertThat(artifact.name()).isEqualTo("inventory/exports/nightly");
        assertThat(worker.lastAccept).isEqualTo("application/x-ndjson");
        for (int i = 0; i < 5; i++) {
            assertThat(query(agent, "route/exports", Map.of("op", "nightly"))).containsEntry("root", artifact.root());
        }
        assertThat(worker.calls("nightly")).as("one call to the deployment for every fetcher in the window").isEqualTo(1);

        assertThat(fetchAll(agent.content().orElseThrow(), artifact.root())).isEqualTo(export);
        assertThat(running.connector().stats()).containsEntry("artifacts-published", 1L)
                .containsEntry("artifact-bytes-read", (long) export.length);
    }

    @Test
    @Timeout(120)
    void aBodyOverTheBoundPublishesNothingAndAutoAnswersInlineBelowIt(@TempDir Path dir) throws Exception {
        int port = freePort();
        CelldConnector.Running running = connector(dir, "inv-a", port, null, null);
        FleetContext agent = agent(port).context();
        card(agent, "workers://inventory/route/exports");
        Optional<DataQueryClient.Fetched> endless = new DataQueryClient(agent.group().space("data"), "analyst")
                .fetch("route/exports", Map.of("op", "endless"), Duration.ofSeconds(30));
        assertThat(endless).isPresent();
        assertThat(endless.get().result().error()).startsWith("artifacts.max-bytes:");
        assertThat(running.connector().stats()).containsEntry("artifacts-published", 0L);
        assertThat(running.runtime().context().content().orElseThrow().list(100)).as("no manifest").isEmpty();

        assertThat(query(agent, "route/exports", Map.of("op", "auto"))).containsEntry("sku", "A-1");
        StringBuilder big = new StringBuilder("[");
        for (int i = 0; i < 4000; i++) {
            big.append(i == 0 ? "" : ",").append("{\"sku\":\"S-").append(i).append("\",\"note\":\"").append("x".repeat(20))
                    .append("\"}");
        }
        worker.auto = big.append("]").toString();
        running.connector().connector().withdrawResults("route/exports");
        assertThat(await(Duration.ofSeconds(20), () -> tryQuery(agent, "route/exports", Map.of("op", "auto"))
                .filter(ArtifactRows::isArtifact).isPresent())).as("above inline-max, an artifact").isTrue();
        Map<String, String> artifactRow = query(agent, "route/exports", Map.of("op", "auto"));
        assertThat(ArtifactRows.parse(artifactRow).orElseThrow().size()).isEqualTo(worker.auto.length());
    }

    // ------------------------------------------------------------------ scenario 3: objects in place

    @Test
    @Timeout(120)
    void objectsAreServedWhereTheyLieListedInACollectionAndForgottenWhenTheyChange(@TempDir Path dir)
            throws Exception {
        byte[] ckpt1 = SwarmWorker.random(1_200_000, 2);
        worker.putObject("ckpt-1", ckpt1, Map.of("kind", "checkpoint", "model", "m3"));
        worker.putObject("ckpt-2", SwarmWorker.random(300_000, 3), Map.of("kind", "checkpoint"));
        worker.putObject("tiles/7", SwarmWorker.random(90_000, 4), Map.of("kind", "tile"));
        worker.putObject("bad\u0001key", SwarmWorker.random(10, 5), Map.of());
        int port = freePort();
        CelldConnector.Running running = connector(dir, "inv-a", port, null, null);
        ObjectSources objects = running.connector().objects();
        assertThat(await(Duration.ofSeconds(30), () -> objects.store("models").roots().size() == 3)).isTrue();
        assertThat(objects.keysRefused()).as("a key with a control character").isGreaterThanOrEqualTo(1);
        assertThat(worker.wholeReads.get()).as("each object hashed once").isEqualTo(3);

        FleetContext agent = agent(port).context();
        AssetCard collection = card(agent, findCollectionUri(agent));
        assertThat(collection.asset()).isEqualTo("content:models");
        Optional<DataQueryClient.Fetched> listed = new DataQueryClient(agent.group().space("data"), "analyst")
                .fetch("content:models", Map.of("tag", "checkpoint"), Duration.ofSeconds(30));
        assertThat(listed).isPresent();
        List<Map<String, String>> rows = listed.get().result().rows();
        assertThat(rows).extracting(r -> r.get("name")).containsExactlyInAnyOrder("ckpt-1", "ckpt-2");
        String root = rows.stream().filter(r -> r.get("name").equals("ckpt-1")).findFirst().orElseThrow().get("root");

        int before = worker.rangeReads.get();
        assertThat(fetchAll(agent.content().orElseThrow(), root)).isEqualTo(ckpt1);
        int pieces = (ckpt1.length + 262143) / 262144;
        assertThat(worker.rangeReads.get() - before).as("one ranged read per piece, the cache serves sub-ranges: " + worker.ranges)
                .isEqualTo(pieces);

        worker.putObject("ckpt-1", SwarmWorker.random(1_200_000, 6), Map.of("kind", "checkpoint"));
        worker.removeObject("tiles/7");
        objects.cycleNow("models");
        assertThat(objects.store("models").roots()).as("the old version and the removed object stop serving")
                .doesNotContain(root).hasSize(2);
        assertThat(objects.rootsForgotten()).isGreaterThanOrEqualTo(2);
    }

    private static String findCollectionUri(FleetContext agent) throws InterruptedException {
        assertThat(await(Duration.ofSeconds(30), () -> !agent.group().discovery()
                .find(AssetCard.class, c -> c.asset().equals("content:models")).isEmpty())).isTrue();
        return agent.group().discovery().find(AssetCard.class, c -> c.asset().equals("content:models")).getFirst().uri();
    }

    @Test
    @Timeout(120)
    void aRestartRegistersIndexedObjectsWithoutReadingThemAgain(@TempDir Path dir) throws Exception {
        worker.putObject("ckpt-1", SwarmWorker.random(500_000, 7), Map.of("kind", "checkpoint"));
        int port = freePort();
        CelldConnector.Running first = connector(dir, "inv-a", port, null, null);
        assertThat(await(Duration.ofSeconds(30), () -> first.connector().objects().store("models").roots().size() == 1))
                .isTrue();
        String root = first.connector().objects().store("models").roots().iterator().next();
        first.close();
        closeables.remove(first);
        int reads = worker.wholeReads.get();
        CelldConnector.Running second = connector(dir, "inv-a", freePort(), null, null);
        assertThat(second.connector().objects().store("models").roots()).as("registered from the index at start")
                .containsExactly(root);
        second.connector().objects().cycleNow("models");
        assertThat(worker.wholeReads.get()).as("the unchanged object is not read again").isEqualTo(reads);
    }

    // ------------------------------------------------------------------ scenario 4: artifact inputs

    @Test
    @Timeout(120)
    void anArtifactReachesTheIngestRouteOnceWholeOrPieceByPieceAndBoundsRefuseBeforeFetching(@TempDir Path dir)
            throws Exception {
        int port = freePort();
        CelldConnector.Running running = connector(dir, "inv-a", port, null, null);
        FleetContext agent = agent(port).context();
        AssetCard ingest = card(agent, "workers://inventory/route/ingest");
        assertThat(ingest.access()).containsEntry(CelldAssetProvider.UPLOAD_OPS, "load,loadSmall,loadChunked")
                .containsEntry("artifact-param-load", "source");
        Content content = agent.content().orElseThrow();
        byte[] dataset = SwarmWorker.random(700_000, 8);
        Manifest manifest = content.publish(dataset);
        Space data = agent.group().space("data");

        AssetInvocationResult result = AssetInvocations.invoke(data, ingest, "load",
                Map.of("source", manifest.root(), "batch", "17"), Duration.ofSeconds(60)).get(90, TimeUnit.SECONDS);
        assertThat(result.ok()).as(String.valueOf(result.result())).isTrue();
        assertThat(result.result()).containsEntry("sha256", SwarmWorker.sha(dataset)).containsEntry("batch", "17")
                .containsEntry("bytes", Integer.toString(dataset.length));
        assertThat(worker.lastRoot).isEqualTo(manifest.root());
        assertThat(worker.lastLength).isEqualTo(dataset.length);
        AssetInvocationResult again = AssetInvocations.retry(data, ingest, new AssetInvocation(AssetInvocations.VERSION,
                        "route/ingest", "load", Map.of("source", manifest.root(), "batch", "17"), result.invocationId()),
                Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(again.ok()).isTrue();
        assertThat(worker.ingestCalls.get(result.invocationId()).get()).as("a retry is answered by the connector")
                .isEqualTo(1);

        AssetInvocationResult small = AssetInvocations.invoke(data, ingest, "loadSmall",
                Map.of("source", manifest.root()), Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(small.ok()).isFalse();
        assertThat(small.error()).isEqualTo("target-error");
        assertThat(small.result()).containsEntry("rule", "max-upload-bytes");

        AssetInvocationResult notRoot = AssetInvocations.invoke(data, ingest, "load",
                Map.of("source", "zQmNotARoot", "batch", "1"), Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(notRoot.error()).isEqualTo("bad-params");
        assertThat(notRoot.result()).containsEntry("rule", "artifact-param");

        AssetInvocationResult chunked = AssetInvocations.invoke(data, ingest, "loadChunked",
                Map.of("source", manifest.root()), Duration.ofSeconds(60)).get(90, TimeUnit.SECONDS);
        assertThat(chunked.ok()).as(String.valueOf(chunked.result())).isTrue();
        assertThat(chunked.result()).containsEntry("sha256", SwarmWorker.sha(dataset));
        assertThat(running.connector().stats()).containsEntry("uploads", 2L);
    }

    // ------------------------------------------------------------------ scenarios 2 and 5: changes, snapshots

    @Test
    @Timeout(120)
    void aChangeBecomesAnAssetChangeWithdrawsTheCachedResultAndRefreshesTheSnapshot(@TempDir Path dir)
            throws Exception {
        int port = freePort();
        CelldConnector.Running running = connector(dir, "inv-a", port, null, null);
        FleetContext agent = agent(port).context();
        AssetCard stock = card(agent, "workers://inventory/route/stock");
        assertThat(stock.access()).containsEntry(CelldAssetProvider.CHANGES, "true")
                .containsEntry(CelldAssetProvider.SNAPSHOT_OPS, "snapshot").containsEntry("consistency-snapshot", "snapshot");
        assertThat(query(agent, "route/stock", Map.of("op", "getStock", "sku", "A-1"))).containsEntry("qty", "7");
        ChangeWatcher changes = running.connector().changes();
        assertThat(await(Duration.ofSeconds(20), () -> changes.cursor() != null)).as("primed").isTrue();

        worker.setStock(3);
        Space data = agent.group().space("data");
        assertThat(await(Duration.ofSeconds(30), () -> data.readAll(Template.of(AssetChange.class)
                .where("asset", eq("route/stock")), 16).stream().anyMatch(c -> "3".equals(c.row().get("qty")))))
                .as("the change reaches the fleet as an AssetChange").isTrue();
        assertThat(await(Duration.ofSeconds(20), () -> running.connector().connector().withdrawn() >= 1)).isTrue();
        assertThat(await(Duration.ofSeconds(20), () -> tryQuery(agent, "route/stock",
                Map.of("op", "getStock", "sku", "A-1")).map(r -> "3".equals(r.get("qty"))).orElse(false)))
                .as("the next query reaches the deployment").isTrue();
        assertThat(await(Duration.ofSeconds(20), () -> running.connector().snapshots().taken() >= 1))
                .as("the change took a snapshot").isTrue();
        Content content = agent.content().orElseThrow();
        assertThat(await(Duration.ofSeconds(30), () -> !content.search(null, null,
                Map.of(ArtifactResults.SNAPSHOT_OF, "inventory:route/stock"), null, null, 4).isEmpty())).isTrue();
        String root = content.search(null, null, Map.of(ArtifactResults.SNAPSHOT_OF, "inventory:route/stock"),
                null, null, 4).getFirst().manifest().root();
        assertThat(new String(fetchAll(content, root), StandardCharsets.UTF_8)).contains("\"qty\":3");
    }

    // ------------------------------------------------------------------ scenario 6: replicas

    @Test
    @Timeout(150)
    void twoConnectorsShareOneDeploymentWithTheirOwnCardsAndOneEffectPerInvocation(@TempDir Path dir)
            throws Exception {
        String replicas = "  replicas: { mode: active-active }\n";
        int portA = freePort();
        CelldConnector.Running a = connector(dir, "inv-a", portA, null, replicas);
        CelldConnector.Running b = connector(dir, "inv-b", freePort(), "127.0.0.1:" + portA, replicas);
        FleetContext agent = agent(portA).context();
        assertThat(await(Duration.ofSeconds(40), () -> agent.group().discovery().find(AssetCard.class,
                c -> "workers://inventory/route/ingest".equals(c.uri())).size() == 2)).as("both cards").isTrue();
        List<AssetCard> cards = agent.group().discovery().find(AssetCard.class,
                c -> "workers://inventory/route/ingest".equals(c.uri()));
        assertThat(cards).extracting(AssetCard::id)
                .containsExactlyInAnyOrder("inventory:route/ingest@inv-a", "inventory:route/ingest@inv-b");
        assertThat(await(Duration.ofSeconds(20), () -> a.connector().replicaPeers().size() == 2
                && b.connector().replicaPeers().size() == 2)).isTrue();
        assertThat(a.connector().elected() ^ b.connector().elected()).as("one change poller").isTrue();

        Space data = agent.group().space("data");
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            AssetInvocationResult result = AssetInvocations.invoke(data, cards.get(i % 2), "note",
                    Map.of("text", "n" + i), Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
            assertThat(result.ok()).as("answered through either card").isTrue();
            ids.add(result.invocationId());
        }
        for (String id : ids) {
            assertThat(worker.ingestEffects.get(id).get()).as("one effect per invocation; two replicas may both"
                    + " call inside the settle window, and the Worker's once absorbs it").isEqualTo(1);
        }
    }

    // ------------------------------------------------------------------ startup refusals

    @Test
    void anArtifactManifestWithoutTheSwarmFailsAtStartupNamingTheProperty(@TempDir Path dir) throws Exception {
        String fleet = FLEET.formatted(freePort(), "", worker.baseUrl(), "inv-a", dir.resolve("s"), ROUTES)
                .replace("  content:\n    enabled: true\n", "  content:\n    enabled: false\n");
        Path fleetFile = dir.resolve("fleet.yaml");
        Files.writeString(fleetFile, fleet);
        assertThatThrownBy(() -> CelldConnector.run(fleetFile, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.content.enabled");
    }
}
