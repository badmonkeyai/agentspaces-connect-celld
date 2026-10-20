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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations;
import ai.badmonkey.agentspaces.connect.AssetProvider;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The provider against a local HttpServer standing in for the Worker: the
 * cards (JS-C-08, appendix F), GET reads with query parameters, POST
 * invocations with the JSON body and {@code Aspace-Invocation-Id}, the
 * bearer header, and the refusals that never reach the deployment (JS-C-07).
 */
class CelldAssetProviderTest {

    private static final String MANIFEST = """
            celld:
              application: inventory
              base-url: %s
              allow-plaintext: true
              runtime: celld
              credential: { bearer: test-token }
              timeouts: { connect: 2s, request: 1s }
              max-body-bytes: 1024
              freshness: PT1M
              routes:
                - name: stock
                  description: Stock by SKU
                  shape: com.example.Stock#v1
                  freshness: PT5M
                  operations:
                    - { name: getStock, method: GET, path: /api/stock, params: [sku, warehouse] }
                    - { name: reserveStock, method: POST, path: /api/stock/reserve, params: [sku, qty] }
                    - { name: auditStock, method: POST, path: /api/stock/audit, params: [warehouse], idempotent: false }
                - name: rates
                  description: Repair rates
                  shape: com.example.Rate#v1
                  operations:
                    - { name: listRates, method: GET, path: /api/rates, params: [part] }
                    - { name: listAll, method: GET, path: /api/rates }
                - name: broken
                  operations:
                    - { name: fail, method: POST, path: /api/fail }
                    - { name: slow, method: GET, path: /api/slow }
                    - { name: big, method: GET, path: /api/big }
                    - { name: text, method: GET, path: /api/text }
                    - { name: scalar, method: GET, path: /api/scalar }
                    - { name: empty, method: POST, path: /api/empty }
            """;

    private StubWorker worker;
    private CelldAssetProvider provider;

    @BeforeEach
    void start() throws IOException {
        worker = new StubWorker();
        provider = new CelldAssetProvider(CelldManifest.parse(MANIFEST.formatted(worker.baseUrl())));
    }

    @AfterEach
    void stop() {
        worker.close();
    }

    @Test
    void cardsFollowAppendixFWithStableIds() {
        GroupId group = GroupId.of("zGroup");
        PeerId issuer = PeerIdentity.generate().peerId();
        List<AssetCard> cards = provider.describeAssets(group, issuer, Instant.EPOCH);
        assertThat(cards).extracting(AssetCard::asset).containsExactly("route/stock", "route/rates", "route/broken");

        AssetCard stock = cards.getFirst();
        assertThat(stock.id()).isEqualTo("inventory:route/stock");
        assertThat(stock.uri()).isEqualTo("workers://inventory/route/stock");
        assertThat(stock.issuer()).isEqualTo(issuer);
        assertThat(stock.group()).isEqualTo(group);
        assertThat(stock.description()).isEqualTo("Stock by SKU");
        assertThat(stock.shape()).isEqualTo("com.example.Stock#v1");
        assertThat(stock.freshness()).isEqualTo("PT5M");
        assertThat(stock.ttl()).as("the TTL is twice the cadence").isEqualTo(Duration.ofMinutes(10));
        assertThat(stock.costHints()).containsEntry("operations", "3");
        assertThat(stock.access())
                .containsEntry("mode", "read-only,invoke")
                .containsEntry("query", "aspace:cap/data-query")
                .containsEntry("invoke", AssetInvocations.CAPABILITY)
                .containsEntry("read-ops", "getStock")
                .containsEntry("invoke-ops", "reserveStock,auditStock")
                .containsEntry("idempotent", "reserveStock")
                .containsEntry("params-getStock", "sku,warehouse")
                .containsEntry("params-reserveStock", "sku,qty")
                .containsEntry("params-auditStock", "warehouse")
                .containsEntry("runtime", "celld")
                .containsEntry("connector", "java");
        assertThat(AssetInvocations.invokeOps(stock)).containsExactly("reserveStock", "auditStock");

        AssetCard rates = cards.get(1);
        assertThat(rates.access()).containsEntry("mode", "read-only").containsEntry("read-ops", "listRates,listAll")
                .doesNotContainKeys("invoke", "invoke-ops", "idempotent");
        assertThat(rates.freshness()).as("the manifest default").isEqualTo("PT1M");
        assertThat(rates.ttl()).isEqualTo(Duration.ofMinutes(2));

        // JS-C-08: the same ids on a later refresh, so the refreshed card replaces the earlier one.
        List<AssetCard> later = provider.describeAssets(group, issuer, Instant.EPOCH.plusSeconds(3_600));
        assertThat(later).extracting(AssetCard::id).containsExactlyElementsOf(cards.stream().map(AssetCard::id).toList());
        assertThat(later.getFirst().issued()).isEqualTo(Instant.EPOCH.plusSeconds(3_600));
    }

    @Test
    void aReadIsAGetWithTheDeclaredParametersAsQueryParametersAndTheBearer() {
        AssetProvider.QueryResult result = provider.query("route/stock", Map.of("op", "getStock", "sku", "A-1",
                "warehouse", "w 7"));
        assertThat(result.error()).isNull();
        assertThat(result.rows()).containsExactly(Map.of("sku", "A-1", "stock", "7", "warehouse", "w7"));
        assertThat(worker.lastMethod).isEqualTo("GET");
        assertThat(worker.lastPath).isEqualTo("/api/stock");
        assertThat(StubWorker.query(worker.lastQuery)).containsOnly(Map.entry("sku", "A-1"), Map.entry("warehouse", "w 7"));
        assertThat(worker.lastQuery).as("the 'op' parameter is the connector's, not the route's").doesNotContain("op=");
        assertThat(worker.lastAuthorization).isEqualTo("Bearer test-token");
        assertThat(worker.lastAccept).isEqualTo("application/json");
        assertThat(worker.lastInvocationId).isNull();
        assertThat(worker.lastBody).isNull();
        assertThat(provider.client().calls()).isEqualTo(1);

        // One read operation on the group: 'op' may be left out.
        AssetProvider.QueryResult all = provider.query("route/stock", Map.of());
        assertThat(all.error()).isNull();
        assertThat(all.rows()).hasSize(2);
        assertThat(worker.lastQuery).isNull();
    }

    @Test
    void aReadWithoutOpOnAGroupWithSeveralReadOperationsIsRefused() {
        AssetProvider.QueryResult result = provider.query("route/rates", Map.of("part", "door"));
        assertThat(result.error()).contains("read-ops").contains("names no 'op'").contains("listRates,listAll");
        assertThat(result.rows()).isEmpty();
        assertThat(worker.requests.get()).isZero();

        AssetProvider.QueryResult named = provider.query("route/rates", Map.of("op", "listRates", "part", "door"));
        assertThat(named.error()).isNull();
        assertThat(worker.lastPath).isEqualTo("/api/rates");
    }

    @Test
    void refusalsNameTheRuleAndNeverCallTheDeployment() {
        // An asset the manifest does not list.
        AssetProvider.QueryResult unlisted = provider.query("route/secrets", Map.of());
        assertThat(unlisted.error()).startsWith("routes:").contains("'route/secrets'");

        // A read operation the group does not list, and a POST route asked as a read.
        AssetProvider.QueryResult unknownOp = provider.query("route/stock", Map.of("op", "dropStock"));
        assertThat(unknownOp.error()).startsWith("read-ops:").contains("'dropStock' is not a GET route of asset 'route/stock'")
                .contains("(getStock)");
        AssetProvider.QueryResult postAsRead = provider.query("route/stock", Map.of("op", "reserveStock"));
        assertThat(postAsRead.error()).startsWith("read-ops:");

        // A parameter the operation does not declare.
        AssetProvider.QueryResult undeclared = provider.query("route/stock", Map.of("op", "getStock", "sku", "A-1",
                "limit", "10"));
        assertThat(undeclared.error()).startsWith("params:").contains("'limit' is not declared for operation 'getStock'")
                .contains("(declared: sku,warehouse)");

        // The same three rules on the invoke side, with the appendix F error codes.
        AssetProvider.InvokeResult noAsset = provider.invoke("route/secrets", "dump", Map.of(), "inv-1");
        assertThat(noAsset.ok()).isFalse();
        assertThat(noAsset.error()).isEqualTo("unknown-operation");
        assertThat(noAsset.result()).containsEntry("rule", "routes");

        AssetProvider.InvokeResult noOp = provider.invoke("route/stock", "dropStock", Map.of(), "inv-2");
        assertThat(noOp.error()).isEqualTo("unknown-operation");
        assertThat(noOp.result()).containsEntry("rule", "invoke-ops");
        assertThat(noOp.result().get("detail")).contains("'dropStock' is not a POST route of asset 'route/stock'")
                .contains("(reserveStock,auditStock)");

        AssetProvider.InvokeResult getAsInvoke = provider.invoke("route/stock", "getStock", Map.of(), "inv-3");
        assertThat(getAsInvoke.error()).isEqualTo("unknown-operation");

        AssetProvider.InvokeResult badParams = provider.invoke("route/stock", "reserveStock",
                Map.of("sku", "A-1", "qty", "2", "price", "0"), "inv-4");
        assertThat(badParams.error()).isEqualTo("bad-params");
        assertThat(badParams.result()).containsEntry("rule", "params");
        assertThat(badParams.result().get("detail")).contains("'price' is not declared for operation 'reserveStock'");

        assertThat(worker.requests.get()).as("none of the refused requests reached the deployment").isZero();
        assertThat(provider.client().calls()).isZero();
    }

    @Test
    void anInvocationIsAPostWithTheJsonBodyAndTheInvocationIdHeader() {
        AssetProvider.InvokeResult result = provider.invoke("route/stock", "reserveStock",
                Map.of("sku", "A-1", "qty", "2"), "inv-42");
        assertThat(result.ok()).isTrue();
        assertThat(result.error()).isNull();
        assertThat(result.result()).containsEntry("reserved", "2").containsEntry("invocationId", "inv-42")
                .containsEntry("nested", "{\"ok\":true}");
        assertThat(worker.lastMethod).isEqualTo("POST");
        assertThat(worker.lastPath).isEqualTo("/api/stock/reserve");
        assertThat(worker.lastQuery).isNull();
        assertThat(worker.lastContentType).isEqualTo("application/json");
        assertThat(worker.lastInvocationId).isEqualTo("inv-42");
        assertThat(worker.lastAuthorization).isEqualTo("Bearer test-token");
        assertThat(worker.lastBody.get("sku").asText()).isEqualTo("A-1");
        assertThat(worker.lastBody.get("qty").asText()).isEqualTo("2");
        assertThat(worker.lastBody.size()).isEqualTo(2);
        assertThat(worker.calls("inv-42")).isEqualTo(1);

        // The provider carries the id; the application's handler is what deduplicates on it.
        AssetProvider.InvokeResult again = provider.invoke("route/stock", "reserveStock",
                Map.of("sku", "A-1", "qty", "9"), "inv-42");
        assertThat(again.result()).as("the stored result, not a second reservation").containsEntry("reserved", "2");
        assertThat(worker.calls("inv-42")).isEqualTo(2);
    }

    @Test
    void targetFailuresBecomeErrorResultsNamingTheBound() {
        AssetProvider.InvokeResult failed = provider.invoke("route/broken", "fail", Map.of(), "inv-f");
        assertThat(failed.ok()).isFalse();
        assertThat(failed.error()).isEqualTo("target-error");
        assertThat(failed.result()).containsEntry("rule", "http-status");
        assertThat(failed.result().get("detail")).contains("500 from POST /api/fail");

        AssetProvider.QueryResult slow = provider.query("route/broken", Map.of("op", "slow"));
        assertThat(slow.error()).startsWith("timeouts.request:").contains("did not answer within PT1S");

        AssetProvider.QueryResult big = provider.query("route/broken", Map.of("op", "big"));
        assertThat(big.error()).startsWith("max-body-bytes:").contains("1024");

        AssetProvider.QueryResult text = provider.query("route/broken", Map.of("op", "text"));
        assertThat(text.error()).startsWith("json:").contains("not JSON");

        AssetProvider.QueryResult scalar = provider.query("route/broken", Map.of("op", "scalar"));
        assertThat(scalar.error()).isNull();
        assertThat(scalar.rows()).containsExactly(Map.of("value", "42"));

        AssetProvider.InvokeResult empty = provider.invoke("route/broken", "empty", Map.of(), "inv-e");
        assertThat(empty.ok()).isTrue();
        assertThat(empty.result()).isEmpty();

        // An unreachable deployment is a target error too, not an exception.
        worker.close();
        AssetProvider.InvokeResult down = provider.invoke("route/stock", "reserveStock", Map.of("sku", "A-1"), "inv-d");
        assertThat(down.error()).isEqualTo("target-error");
        assertThat(down.result()).containsEntry("rule", "route");
    }

    @Test
    void theMtlsStoresAreLoadedWhenNamed(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        // A missing store fails at construction, naming the problem, rather than on the first call.
        CelldManifest manifest = CelldManifest.parse(MANIFEST.formatted(worker.baseUrl()).replace(
                "credential: { bearer: test-token }",
                "credential: { mtls: { key-store: " + dir.resolve("absent.p12") + " } }"));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new CelldClient(manifest))
                .isInstanceOf(java.io.UncheckedIOException.class).hasMessageContaining("mtls stores");
    }
}
