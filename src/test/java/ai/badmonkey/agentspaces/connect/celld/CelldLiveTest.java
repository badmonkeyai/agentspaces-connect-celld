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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static ai.badmonkey.agentspaces.connect.celld.CelldConnectorTest.await;
import static ai.badmonkey.agentspaces.connect.celld.CelldConnectorTest.freePort;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connector against a real celld node (srad/celld-phase2.md appendix D): the inventory
 * application of {@code e2e/worker} deployed to one node over Azurite, the connector started from
 * {@code e2e/fleet.yaml} and {@code e2e/manifest.yaml}, and an agent peer on the content swarm.
 * {@code e2e/run.sh} starts everything and sets the variables this test needs; without
 * {@code CELLD_E2E_URL} it does not run.
 */
@EnabledIfEnvironmentVariable(named = "CELLD_E2E_URL", matches = "https?://.+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CelldLiveTest {

    private static final String URL = System.getenv("CELLD_E2E_URL");
    private static final Path DIR = Path.of(System.getenv().getOrDefault("CELLD_E2E_DIR", "e2e")).toAbsolutePath();
    private static CelldConnector.Running connector;
    private static FleetRuntime agentRuntime;
    private static FleetContext agent;

    @BeforeAll
    static void start() throws Exception {
        Path work = Files.createTempDirectory("celld-live");
        String state = DIR.resolve(".state").toString() + "/";
        Path fleet = work.resolve("fleet.yaml");
        Files.writeString(fleet, Files.readString(DIR.resolve("fleet.yaml")).replace(".state/", state));
        Path manifest = work.resolve("manifest.yaml");
        Files.writeString(manifest, Files.readString(DIR.resolve("manifest.yaml")).replace(".state/", state)
                .replaceAll("base-url: \\S+", "base-url: " + URL));
        connector = CelldConnector.launch(fleet, manifest);

        Map<String, Object> props = new HashMap<>();
        props.put("agentspaces.bind", "127.0.0.1:" + freePort());
        props.put("agentspaces.tick-millis", "100");
        props.put("agentspaces.capabilities.enabled", "true");
        props.put("agentspaces.content.enabled", "true");
        props.put("agentspaces.content.store.backends[0]", "file");
        props.put("agentspaces.content.store.path", work.resolve("agent-content").toString());
        props.put("agentspaces.groups[0].name", "celld-e2e");
        props.put("agentspaces.groups[0].founding", "celld-e2e-v1");
        props.put("agentspaces.groups[0].seeds[0]", "127.0.0.1:7740");
        props.put("agentspaces.groups[0].spaces[0].name", "data");
        props.put("agentspaces.groups[0].spaces[0].settle-window-millis", "100");
        agentRuntime = FleetRuntime.start(PropertiesLoader.flat(props));
        agent = agentRuntime.context();
        assertThat(await(Duration.ofSeconds(60), () -> !agent.group().discovery()
                .find(AssetCard.class, c -> "workers://inventory/route/stock".equals(c.uri())).isEmpty()))
                .as("the agent discovers the connector's cards").isTrue();
    }

    @AfterAll
    static void stop() {
        if (agentRuntime != null) {
            agentRuntime.close();
        }
        if (connector != null) {
            System.out.println("connector stats: " + connector.connector().stats());
            connector.close();
        }
    }

    private static AssetCard card(String uri) throws InterruptedException {
        assertThat(await(Duration.ofSeconds(60), () -> !agent.group().discovery()
                .find(AssetCard.class, c -> uri.equals(c.uri())).isEmpty())).as(uri).isTrue();
        return agent.group().discovery().find(AssetCard.class, c -> uri.equals(c.uri())).getFirst();
    }

    private static Optional<Map<String, String>> query(String asset, Map<String, String> params) {
        return new DataQueryClient(agent.group().space("data"), "e2e").fetch(asset, params, Duration.ofSeconds(120))
                .filter(f -> f.result().error() == null && !f.result().rows().isEmpty())
                .map(f -> f.result().rows().getFirst());
    }

    private static AssetInvocationResult invoke(AssetCard card, String operation, Map<String, String> params)
            throws Exception {
        return AssetInvocations.invoke(agent.group().space("data"), card, operation, params, Duration.ofMinutes(5))
                .get(6, TimeUnit.MINUTES);
    }

    private static String fetchSha(String root) throws Exception {
        Content content = agent.content().orElseThrow();
        Fetch fetch = content.fetch(root);
        assertThat(await(Duration.ofMinutes(5), fetch::isDoneOrFailed)).as("fetch of " + root).isTrue();
        assertThat(fetch.complete()).as(fetch.trace().toString()).isTrue();
        try (SeekableByteChannel channel = content.openPlain(root); InputStream in = Channels.newInputStream(channel)) {
            return sha(in);
        }
    }

    private static String sha(InputStream in) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] buffer = new byte[1 << 16];
        for (int n; (n = in.read(buffer)) > 0; ) {
            digest.update(buffer, 0, n);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static long stat(String name) {
        return connector.connector().stats().getOrDefault(name, 0L);
    }

    @Test
    @Order(1)
    @Timeout(180)
    void aReadAndAnInvocationReachTheDurableObjectOnceUnderARetry() throws Exception {
        AssetCard stock = card("workers://inventory/route/stock");
        assertThat(stock.access()).containsEntry("runtime", "celld").containsEntry(CelldAssetProvider.CHANGES, "true")
                .containsEntry(CelldAssetProvider.SNAPSHOT_OPS, "snapshot");
        // The Durable Object keeps its state between runs: top the stock up, then reserve from it.
        AssetInvocationResult topUp = invoke(stock, "setStock", Map.of("sku", "C-3", "qty", "12"));
        assertThat(topUp.ok()).isTrue();
        assertThat(await(Duration.ofSeconds(30), () -> query("route/stock", Map.of("op", "getStock", "sku", "C-3"))
                .map(r -> "12".equals(r.get("qty"))).orElse(false))).as("the read reaches the object").isTrue();
        AssetInvocationResult reserved = invoke(stock, "reserveStock", Map.of("sku", "C-3", "qty", "2"));
        assertThat(reserved.ok()).as(String.valueOf(reserved.result())).isTrue();
        assertThat(reserved.result()).containsEntry("reserved", "2").containsEntry("remaining", "10");
        AssetInvocationResult again = AssetInvocations.retry(agent.group().space("data"), stock, new AssetInvocation(
                        AssetInvocations.VERSION, "route/stock", "reserveStock", Map.of("sku", "C-3", "qty", "2"),
                        reserved.invocationId()), Duration.ofMinutes(1)).get(2, TimeUnit.MINUTES);
        assertThat(again.result()).containsEntry("remaining", "10");
        assertThat(query("route/stock", Map.of("op", "getStock", "sku", "C-3", "x", "y"))).as("undeclared").isEmpty();
    }

    @Test
    @Order(2)
    @Timeout(600)
    void theNightlyExportLeavesAsOneArtifactFromOneCall() throws Exception {
        card("workers://inventory/route/exports");
        long executed = stat("queries-executed");
        Map<String, String> row = query("route/exports", Map.of("op", "nightly", "mb", "64")).orElseThrow();
        ArtifactRows.Artifact export = ArtifactRows.parse(row).orElseThrow();
        assertThat(export.size()).isEqualTo(64L << 20);
        assertThat(export.mediaType()).isEqualTo("application/x-ndjson");
        for (int i = 0; i < 4; i++) {
            assertThat(query("route/exports", Map.of("op", "nightly", "mb", "64"))).get()
                    .extracting(r -> r.get("root")).isEqualTo(export.root());
        }
        assertThat(stat("queries-executed") - executed).as("one call to celld for five queries").isEqualTo(1);
        HttpResponse<InputStream> direct = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                URI.create(URL + "/api/exports/nightly?mb=64")).build(), HttpResponse.BodyHandlers.ofInputStream());
        assertThat(direct.headers().firstValue("Content-Length")).as("celld removes it").isEmpty();
        String expected;
        try (InputStream in = direct.body()) {
            expected = sha(in);
        }
        assertThat(fetchSha(export.root())).as("the fetched artifact is the export's bytes").isEqualTo(expected);
    }

    @Test
    @Order(3)
    @Timeout(900)
    void theModelsBucketIsServedWhereItLiesAndAReplacedObjectIsForgotten() throws Exception {
        ObjectSources objects = connector.connector().objects();
        HttpRangePieceStore store = objects.store("models");
        assertThat(await(Duration.ofMinutes(5), () -> store.roots().size() == 4)).as("four objects hashed").isTrue();
        assertThat(await(Duration.ofSeconds(60), () -> !agent.group().discovery().find(AssetCard.class,
                c -> c.asset().equals("content:models")).isEmpty())).as("the collection card").isTrue();
        AssetCard collection = agent.group().discovery().find(AssetCard.class,
                c -> c.asset().equals("content:models")).getFirst();
        assertThat(collection.access()).containsEntry("kind", "content-collection");
        List<Map<String, String>> rows = new DataQueryClient(agent.group().space("data"), "e2e")
                .fetch("content:models", Map.of("tag", "checkpoint"), Duration.ofSeconds(60)).orElseThrow()
                .result().rows();
        assertThat(rows).extracting(r -> r.get("name")).containsExactlyInAnyOrder("ckpt-small", "ckpt-mid", "ckpt-big");
        Map<String, String> big = rows.stream().filter(r -> r.get("name").equals("ckpt-big")).findFirst().orElseThrow();
        long size = Long.parseLong(big.get("size"));
        assertThat(size).as("past the 256 MiB bound of the in-cell R2 store").isGreaterThan(256L << 20);

        long reads = stat("ranged-reads");
        String expected;
        try (InputStream in = Files.newInputStream(DIR.resolve(".state/seed/ckpt-big.bin"))) {
            expected = sha(in);
        }
        assertThat(fetchSha(big.get("root"))).isEqualTo(expected);
        long pieces = Long.parseLong(big.get("pieces"));
        // On a clean state every piece costs one ranged read; the connector's file cache keeps the
        // verified pieces, so a later run reads fewer or none.
        assertThat(stat("ranged-reads") - reads).as("at most one ranged read per piece").isLessThanOrEqualTo(pieces);

        String smallRoot = rows.stream().filter(r -> r.get("name").equals("ckpt-small")).findFirst().orElseThrow()
                .get("root");
        Path replacement = Files.createTempFile("ckpt-small", ".bin");
        byte[] bytes = new byte[307200];
        new Random(System.nanoTime()).nextBytes(bytes); // new bytes every run, so a new root
        Files.write(replacement, bytes);
        Process put = new ProcessBuilder(System.getenv("CELLD_E2E_BIN"), "r2", "put", "models", "ckpt-small",
                "--path", replacement.toString(), "--content-type", "application/octet-stream",
                "--metadata", "{\"kind\":\"checkpoint\",\"model\":\"m1b\"}").inheritIO().start();
        assertThat(put.waitFor(60, TimeUnit.SECONDS) && put.exitValue() == 0).as("celld r2 put").isTrue();
        objects.cycleNow("models");
        assertThat(store.roots()).as("the old version stops serving").doesNotContain(smallRoot).hasSize(4);
    }

    @Test
    @Order(4)
    @Timeout(900)
    void anArtifactReachesTheIngestRouteWholeAndPieceByPiece() throws Exception {
        AssetCard ingest = card("workers://inventory/route/ingest");
        Content content = agent.content().orElseThrow();
        Path dataset = Files.createTempFile("dataset", ".bin");
        byte[] bytes = new byte[48 << 20];
        new Random(7).nextBytes(bytes);
        Files.write(dataset, bytes);
        Manifest published = content.publish(dataset, content.publishOptions().withName("dataset-48m"));
        String expected = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));

        AssetInvocationResult whole = invoke(ingest, "load", Map.of("source", published.root(), "batch", "17"));
        assertThat(whole.ok()).as(String.valueOf(whole.result())).isTrue();
        assertThat(whole.result()).containsEntry("sha256", expected).containsEntry("batch", "17")
                .containsEntry("size", Integer.toString(bytes.length));

        byte[] small = new byte[9 << 20];
        new Random(8).nextBytes(small);
        Manifest chunkedRoot = content.publish(small);
        AssetInvocationResult chunked = invoke(ingest, "loadChunked", Map.of("source", chunkedRoot.root()));
        assertThat(chunked.ok()).as(String.valueOf(chunked.result())).isTrue();
        assertThat(chunked.result()).containsEntry("sha256",
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(small)));
    }

    @Test
    @Order(5)
    @Timeout(300)
    void aStockWriteBecomesAnAssetChangeAFreshReadAndANewSnapshot() throws Exception {
        AssetCard stock = card("workers://inventory/route/stock");
        ChangeWatcher changes = connector.connector().changes();
        assertThat(await(Duration.ofSeconds(60), () -> changes.cursor() != null)).as("primed").isTrue();
        assertThat(query("route/stock", Map.of("op", "getStock", "sku", "A-1"))).isPresent();
        AssetInvocationResult set = invoke(stock, "setStock", Map.of("sku", "A-1", "qty", "5"));
        assertThat(set.ok()).isTrue();
        Space data = agent.group().space("data");
        assertThat(await(Duration.ofSeconds(60), () -> data.readAll(Template.of(AssetChange.class)
                .where("asset", eq("route/stock")), 64).stream().anyMatch(c -> "5".equals(c.row().get("qty")))))
                .as("the change reaches the fleet").isTrue();
        assertThat(await(Duration.ofSeconds(60), () -> query("route/stock", Map.of("op", "getStock", "sku", "A-1"))
                .map(r -> "5".equals(r.get("qty"))).orElse(false))).as("the cached result was withdrawn").isTrue();
        Content content = agent.content().orElseThrow();
        assertThat(await(Duration.ofSeconds(60), () -> content.search(null, null,
                Map.of(ArtifactResults.SNAPSHOT_OF, "inventory:route/stock"), null, null, 8).stream()
                .anyMatch(d -> d.manifest().attrs() != null))).as("a snapshot was taken").isTrue();
        String newest = content.search(null, null, Map.of(ArtifactResults.SNAPSHOT_OF, "inventory:route/stock"),
                        null, null, 8).stream()
                .max(java.util.Comparator.comparing(d -> d.manifest().attrs().get(ArtifactResults.TAKEN)))
                .orElseThrow().manifest().root();
        Fetch fetch = content.fetch(newest);
        assertThat(await(Duration.ofSeconds(60), fetch::complete)).isTrue();
        try (InputStream in = Channels.newInputStream(content.openPlain(newest))) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).contains("{\"sku\":\"A-1\",\"qty\":\"5\"}");
        }
    }
}
