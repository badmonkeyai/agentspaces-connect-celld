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
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.connect.ConnectorRuntime;
import ai.badmonkey.agentspaces.connect.DataQueryClient;
import ai.badmonkey.agentspaces.connect.DataSpaces;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connector in a fleet (JS-C-09, scenario S5a) on a SimNetwork: a Java
 * agent discovers the {@code workers://} card, queries the GET route through
 * {@link DataQueryClient}, and invokes the POST route through
 * {@link AssetInvocations#invoke} exactly once under a retry (the stub Worker
 * counts calls per invocation id); an unlisted operation and an undeclared
 * parameter are answered with error results and never reach the Worker;
 * under an authorizer only a requester holding the invoke scope is served.
 */
class CelldFleetTest {

    private static final String MANIFEST = """
            celld:
              application: inventory
              base-url: %s
              allow-plaintext: true
              credential: { bearer: fleet-token }
              connector-name: celld-java
              routes:
                - name: stock
                  description: Stock by SKU
                  shape: com.example.Stock#v1
                  freshness: PT5M
                  operations:
                    - { name: getStock, method: GET, path: /api/stock, params: [sku] }
                    - { name: reserveStock, method: POST, path: /api/stock/reserve, params: [sku, qty] }
            """;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zCelld");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zCelld", PeerIdentity.generate().peerId(), groupId,
            Instant.EPOCH, Duration.ofDays(1), "celld",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                         DiscoveryService discovery, ReplicatedSpace data) {
    }

    private StubWorker worker;
    private CelldAssetProvider provider;
    private ConnectorRuntime connector;

    @BeforeEach
    void startWorker() throws IOException {
        worker = new StubWorker();
        provider = new CelldAssetProvider(CelldManifest.parse(MANIFEST.formatted(worker.baseUrl())));
    }

    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.close();
        }
        nodes.forEach(PeerNode::close);
        worker.close();
    }

    private Wired newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        ReplicatedSpace data = DataSpaces.builder(runtime, DataSpaces.DEFAULT_NAME, identity, address,
                        new BlockExchange(runtime, codec))
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        return new Wired(node, identity, runtime, discovery, data);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
        }
    }

    private static void await(IntSupplier counter, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (counter.getAsInt() < expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(counter.getAsInt()).isEqualTo(expected);
    }

    private static List<AssetInvocationResult> resultsFor(Space space, String invocationId) {
        return space.readAllIssued(Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(invocationId)), 16).stream().map(Space.Issued::entry).toList();
    }

    @Test
    @Timeout(60)
    void anAgentDiscoversTheCardQueriesTheGetRouteAndInvokesThePostRouteOnceUnderARetry() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "celld-java", provider, groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);

        // Discovery: the appendix F card, found by its workers:// uri.
        AssetCard card = agent.discovery().find(AssetCard.class,
                c -> c.uri().equals("workers://inventory/route/stock")).getFirst();
        assertThat(card.id()).isEqualTo("inventory:route/stock");
        assertThat(card.issuer()).isEqualTo(connectorPeer.identity().peerId());
        assertThat(card.access()).containsEntry("read-ops", "getStock").containsEntry("invoke-ops", "reserveStock")
                .containsEntry("connector", "java");

        // A read through the data-query protocol: GET /api/stock?sku=A-1 with the bearer.
        DataQueryClient client = new DataQueryClient(agent.data(), "estimator", java.util.Set.of(card.issuer()));
        Optional<DataQueryClient.Fetched> fetched = client.fetch(card.asset(),
                Map.of("op", "getStock", "sku", "A-1"), Duration.ofSeconds(10));
        assertThat(fetched).isPresent();
        assertThat(fetched.get().result().error()).isNull();
        assertThat(fetched.get().result().servedBy()).isEqualTo("celld-java");
        assertThat(fetched.get().result().rows()).containsExactly(Map.of("sku", "A-1", "stock", "7", "warehouse", "w7"));
        assertThat(worker.lastMethod).isEqualTo("GET");
        assertThat(worker.lastAuthorization).isEqualTo("Bearer fleet-token");
        assertThat(connector.executed()).isEqualTo(1);

        // The same query again is served from the space: the Worker sees one GET per freshness window.
        Optional<DataQueryClient.Fetched> cached = client.fetch(card.asset(),
                Map.of("op", "getStock", "sku", "A-1"), Duration.ofSeconds(10));
        assertThat(cached).isPresent();
        assertThat(cached.get().fromCache()).isTrue();
        assertThat(worker.requests.get()).isEqualTo(1);

        // An invocation: POST /api/stock/reserve with the JSON body and the id header, once.
        AssetInvocationResult result = AssetInvocations.invoke(agent.data(), card, "reserveStock",
                Map.of("sku", "A-1", "qty", "2"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(result.ok()).isTrue();
        assertThat(result.error()).isNull();
        assertThat(result.asset()).isEqualTo("route/stock");
        assertThat(result.operation()).isEqualTo("reserveStock");
        assertThat(result.result()).containsEntry("reserved", "2").containsEntry("invocationId", result.invocationId());
        assertThat(worker.lastMethod).isEqualTo("POST");
        assertThat(worker.lastInvocationId).isEqualTo(result.invocationId());
        assertThat(worker.lastBody.get("qty").asText()).isEqualTo("2");
        assertThat(worker.calls(result.invocationId())).isEqualTo(1);
        assertThat(connector.invoked()).isEqualTo(1);
        tickAll(2);
        assertThat(agent.data().read(Template.of(AssetInvocation.class))).as("the take completed").isEmpty();

        // A retry under the same id: answered from the remembered result, the Worker not called again.
        AssetInvocationResult again = AssetInvocations.retry(agent.data(), card, new AssetInvocation(
                AssetInvocations.VERSION, card.asset(), "reserveStock", Map.of("sku", "A-1", "qty", "2"),
                result.invocationId()), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(again.ok()).isTrue();
        assertThat(again.result()).containsEntry("reserved", "2");
        await(connector::repeated, 1);
        assertThat(worker.calls(result.invocationId())).as("exactly once at the Worker").isEqualTo(1);
        assertThat(connector.invoked()).isEqualTo(1);
        tickAll(2);
        assertThat(resultsFor(agent.data(), result.invocationId())).hasSize(1);
    }

    @Test
    @Timeout(60)
    void anUnlistedOperationAndAnUndeclaredParameterNeverReachTheWorker() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "celld-java", provider, groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);
        AssetCard card = agent.discovery().find(AssetCard.class, c -> true).getFirst();

        // JS-C-07: the runtime refuses on the card's invoke-ops, naming the rule.
        AssetInvocationResult unlisted = AssetInvocations.invoke(agent.data(), card, "deleteStock",
                Map.of("sku", "A-1"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(unlisted.ok()).isFalse();
        assertThat(unlisted.error()).isEqualTo("unknown-operation");
        assertThat(unlisted.result()).containsEntry("rule", AssetInvocations.INVOKE_OPS);
        assertThat(unlisted.result().get("detail")).contains("'deleteStock' is not in the invoke-ops of asset 'route/stock'");
        assertThat(connector.refused()).isEqualTo(1);
        assertThat(connector.invoked()).isZero();

        // A listed operation with a parameter the manifest does not declare: the provider refuses.
        AssetInvocationResult undeclared = AssetInvocations.invoke(agent.data(), card, "reserveStock",
                Map.of("sku", "A-1", "qty", "1", "discount", "100"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(undeclared.ok()).isFalse();
        assertThat(undeclared.error()).isEqualTo("bad-params");
        assertThat(undeclared.result()).containsEntry("rule", "params");
        assertThat(undeclared.result().get("detail")).contains("'discount' is not declared for operation 'reserveStock'");

        // A read of an undeclared parameter, and of a POST route as a read: refused through the data-query result.
        DataQueryClient client = new DataQueryClient(agent.data(), "estimator");
        Optional<DataQueryClient.Fetched> badRead = client.fetch(card.asset(),
                Map.of("op", "getStock", "warehouse", "w7"), Duration.ofSeconds(10));
        assertThat(badRead).isPresent();
        assertThat(badRead.get().result().error()).startsWith("params:").contains("'warehouse'");
        Optional<DataQueryClient.Fetched> postAsRead = client.fetch(card.asset(),
                Map.of("op", "reserveStock"), Duration.ofSeconds(10));
        assertThat(postAsRead).isPresent();
        assertThat(postAsRead.get().result().error()).startsWith("read-ops:");

        assertThat(worker.requests.get()).as("nothing reached the Worker").isZero();
        assertThat(provider.client().calls()).isZero();
    }

    @Test
    @Timeout(60)
    void underAnAuthorizerOnlyARequesterHoldingTheInvokeScopeReachesTheWorker() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired planner = newPeer("planner", 2, "conn");
        Wired stranger = newPeer("stranger", 3, "conn");
        tickAll(4);
        PeerId connectorId = connectorPeer.identity().peerId();
        PeerId plannerId = planner.identity().peerId();
        Authorizer authorizer = (peer, operation, scope) -> switch (operation) {
            case CONNECTOR_SERVE -> connectorId.equals(peer);
            case CONNECTOR_INVOKE -> plannerId.equals(peer) && "route/stock:reserveStock".equals(scope);
            default -> false;
        };
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "celld-java", provider, groupId, clock, Duration.ofMinutes(1)).authorizer(authorizer);
        connector.start();
        tickAll(2);
        AssetCard card = planner.discovery().find(AssetCard.class, c -> true).getFirst();

        AssetInvocationResult granted = AssetInvocations.invoke(planner.data(), card, "reserveStock",
                Map.of("qty", "1"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(granted.ok()).isTrue();
        assertThat(worker.calls(granted.invocationId())).isEqualTo(1);

        AssetInvocationResult refused = AssetInvocations.invoke(stranger.data(), card, "reserveStock",
                Map.of("qty", "1"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(refused.ok()).isFalse();
        assertThat(refused.error()).isEqualTo("refused-scope");
        assertThat(refused.result()).containsEntry("rule", "aspace:connector-invoke:route/stock:reserveStock");
        assertThat(worker.requests.get()).as("the stranger's request never reached the Worker").isEqualTo(1);
    }
}
