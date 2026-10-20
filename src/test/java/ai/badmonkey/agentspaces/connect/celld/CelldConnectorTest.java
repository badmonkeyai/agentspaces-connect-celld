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

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.connect.AssetInvocations;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.connect.DataQueryClient;
import ai.badmonkey.agentspaces.runtime.FleetContext;
import ai.badmonkey.agentspaces.runtime.FleetRuntime;
import ai.badmonkey.agentspaces.runtime.PropertiesLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The main's wiring (JS-C-09): {@link CelldConnector#run} joins a group from
 * a fleet file of the {@code agentspaces:} shape, with the manifest as its
 * {@code celld:} subtree, and a second plain-Java peer that joins through the
 * seed discovers the card, queries, and invokes over real TCP on loopback.
 */
class CelldConnectorTest {

    private static final String FLEET = """
            agentspaces:
              bind: 127.0.0.1:%d
              tick-millis: 200
              groups:
                - name: celld-fleet
                  founding: celld-it-fleet-v1
                  spaces:
                    - name: data
                      settle-window-millis: 100
            celld:
              application: inventory
              base-url: %s
              allow-plaintext: true
              credential: { bearer: it-token }
              connector-name: celld-java
              routes:
                - name: stock
                  description: Stock by SKU
                  shape: com.example.Stock#v1
                  operations:
                    - { name: getStock, method: GET, path: /api/stock, params: [sku] }
                    - { name: reserveStock, method: POST, path: /api/stock/reserve, params: [sku, qty] }
            """;

    static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static boolean await(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(100);
        }
        return condition.getAsBoolean();
    }

    private static FleetRuntime agentPeer(int port, int seedPort) {
        return FleetRuntime.start(PropertiesLoader.flat(Map.of(
                "agentspaces.bind", "127.0.0.1:" + port,
                "agentspaces.tick-millis", "200",
                "agentspaces.groups[0].name", "celld-fleet",
                "agentspaces.groups[0].founding", "celld-it-fleet-v1",
                "agentspaces.groups[0].seeds[0]", "127.0.0.1:" + seedPort,
                "agentspaces.groups[0].spaces[0].name", "data",
                "agentspaces.groups[0].spaces[0].settle-window-millis", "100")));
    }

    @Test
    @Timeout(90)
    void theMainJoinsFromTheFleetFileAndServesTheManifest(@TempDir Path dir) throws Exception {
        int connectorPort = freePort();
        try (StubWorker worker = new StubWorker()) {
            Path fleetFile = dir.resolve("fleet.yaml");
            Files.writeString(fleetFile, FLEET.formatted(connectorPort, worker.baseUrl()));
            try (FleetRuntime connectorRuntime = CelldConnector.run(fleetFile, null);
                 FleetRuntime agentRuntime = agentPeer(freePort(), connectorPort)) {
                FleetContext agent = agentRuntime.context();
                assertThat(await(Duration.ofSeconds(30), () -> !agent.group().discovery()
                        .find(AssetCard.class, c -> c.uri().equals("workers://inventory/route/stock")).isEmpty()))
                        .as("the agent discovers the card through the seed").isTrue();
                List<AssetCard> cards = agent.group().discovery().find(AssetCard.class, c -> true);
                AssetCard card = cards.getFirst();
                assertThat(card.id()).isEqualTo("inventory:route/stock");
                assertThat(card.issuer()).isEqualTo(connectorRuntime.identity().peerId());
                assertThat(card.access()).containsEntry("connector", "java").containsEntry("runtime", "celld");

                DataQueryClient client = new DataQueryClient(agent.group().space("data"), "estimator",
                        agent.authorizer());
                Optional<DataQueryClient.Fetched> fetched = client.fetch(card.asset(),
                        Map.of("op", "getStock", "sku", "B-2"), Duration.ofSeconds(20));
                assertThat(fetched).isPresent();
                assertThat(fetched.get().result().error()).isNull();
                assertThat(fetched.get().result().rows()).containsExactly(
                        Map.of("sku", "B-2", "stock", "0", "warehouse", "w7"));
                assertThat(worker.lastAuthorization).isEqualTo("Bearer it-token");

                AssetInvocationResult result = AssetInvocations.invoke(agent.group().space("data"), card,
                        "reserveStock", Map.of("sku", "B-2", "qty", "5"), Duration.ofSeconds(20))
                        .get(30, TimeUnit.SECONDS);
                assertThat(result.ok()).isTrue();
                assertThat(result.result()).containsEntry("reserved", "5");
                assertThat(worker.calls(result.invocationId())).isEqualTo(1);
            }
        }
    }

    @Test
    void aManifestNamingASpaceTheGroupLacksFailsAtStartupNamingTheProperty(@TempDir Path dir) throws Exception {
        try (StubWorker worker = new StubWorker()) {
            Path fleetFile = dir.resolve("fleet.yaml");
            Files.writeString(fleetFile, FLEET.formatted(freePort(), worker.baseUrl())
                    .replace("connector-name: celld-java", "connector-name: celld-java\n  space: routes"));
            assertThatThrownBy(() -> CelldConnector.run(fleetFile, null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("'routes'").hasMessageContaining("celld.space");
        }
    }

    @Test
    void aSeparateManifestFileIsReadBesideTheFleetFile(@TempDir Path dir) throws Exception {
        try (StubWorker worker = new StubWorker()) {
            String document = FLEET.formatted(freePort(), worker.baseUrl());
            Path fleetFile = dir.resolve("fleet.yaml");
            Files.writeString(fleetFile, document.substring(0, document.indexOf("celld:")));
            Path manifestFile = dir.resolve("manifest.yaml");
            Files.writeString(manifestFile, document.substring(document.indexOf("celld:")));
            try (FleetRuntime runtime = CelldConnector.run(fleetFile, manifestFile)) {
                assertThat(runtime.isRunning()).isTrue();
                assertThat(await(Duration.ofSeconds(10), () -> !runtime.context().group().discovery()
                        .find(AssetCard.class, c -> c.id().equals("inventory:route/stock")).isEmpty()))
                        .as("the connector's own discovery holds its card").isTrue();
            }
            assertThatThrownBy(() -> CelldConnector.run(fleetFile, null))
                    .hasMessageContaining("no 'celld' key");
        }
    }
}
