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

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The manifest parser (JS-C-09): the format, the defaults, and the rules a typo or an unsafe setting fails on. */
class CelldManifestTest {

    private static final String MINIMAL = """
            celld:
              application: shop
              base-url: https://shop.example
              credential: { bearer: t }
              routes:
                - name: orders
                  operations:
                    - { name: listOrders, method: GET, path: /orders }
            """;

    @Test
    void theReadmeManifestParsesWithEveryFieldBound() {
        CelldManifest manifest;
        try (InputStream in = getClass().getResourceAsStream("/inventory.yaml")) {
            manifest = CelldManifest.load(in);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        assertThat(manifest.application()).isEqualTo("inventory");
        assertThat(manifest.baseUrl()).as("a trailing slash is dropped")
                .isEqualTo(URI.create("https://inventory.internal.example"));
        assertThat(manifest.runtime()).isEqualTo("workerd");
        assertThat(manifest.connectorName()).isEqualTo("inventory-connector");
        assertThat(manifest.space()).isEqualTo("data");
        assertThat(manifest.group()).isNull();
        assertThat(manifest.credential().bearer()).isEqualTo("s3cr3t-token");
        assertThat(manifest.credential().hasKeyStore()).isFalse();
        assertThat(manifest.credential().toString()).as("the token never prints").doesNotContain("s3cr3t");
        assertThat(manifest.connectTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(manifest.requestTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(manifest.maxBodyBytes()).isEqualTo(65536);
        assertThat(manifest.freshness()).isEqualTo(Duration.ofMinutes(2));
        assertThat(manifest.allowPlaintext()).isFalse();
        assertThat(manifest.fleetAuthorizer()).isTrue();
        assertThat(manifest.routes()).hasSize(2);

        CelldManifest.RouteGroup stock = manifest.route("route/stock");
        assertThat(stock.name()).isEqualTo("stock");
        assertThat(stock.asset()).isEqualTo("route/stock");
        assertThat(stock.description()).isEqualTo("Stock levels by SKU and warehouse");
        assertThat(stock.shape()).isEqualTo("com.example.Stock#v1");
        assertThat(stock.freshness()).isEqualTo(Duration.ofSeconds(30));
        assertThat(stock.operations()).extracting(CelldManifest.Operation::name)
                .containsExactly("getStock", "reserveStock", "auditStock");
        assertThat(stock.readOperations()).extracting(CelldManifest.Operation::name).containsExactly("getStock");
        assertThat(stock.invokeOperations()).extracting(CelldManifest.Operation::name)
                .containsExactly("reserveStock", "auditStock");
        CelldManifest.Operation reserve = stock.operation("reserveStock");
        assertThat(reserve.method()).isEqualTo(CelldManifest.Method.POST);
        assertThat(reserve.path()).isEqualTo("/api/stock/reserve");
        assertThat(reserve.params()).containsExactly("sku", "qty", "warehouse");
        assertThat(reserve.idempotent()).isTrue();
        assertThat(stock.operation("auditStock").idempotent()).isFalse();
        assertThat(stock.operation("auditStock").method()).as("the method is case-insensitive")
                .isEqualTo(CelldManifest.Method.POST);
        assertThat(stock.operation("nope")).isNull();

        CelldManifest.RouteGroup rates = manifest.route("route/rates");
        assertThat(rates.freshness()).as("no per-group cadence: the provider falls back to the manifest's").isNull();
        assertThat(rates.operations().getFirst().idempotent()).as("a GET is idempotent").isTrue();
        assertThat(manifest.route("route/other")).isNull();
    }

    @Test
    void defaultsFillWhatAMinimalManifestLeavesOut() {
        CelldManifest manifest = CelldManifest.parse(MINIMAL);
        assertThat(manifest.runtime()).isEqualTo("celld");
        assertThat(manifest.connectorName()).isEqualTo("celld-shop");
        assertThat(manifest.space()).isEqualTo("data");
        assertThat(manifest.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(manifest.requestTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(manifest.maxBodyBytes()).isEqualTo(1L << 20);
        assertThat(manifest.freshness()).isEqualTo(Duration.ofMinutes(1));
        CelldManifest.RouteGroup orders = manifest.routes().getFirst();
        assertThat(orders.description()).isEqualTo("Route group orders of the deployment");
        assertThat(orders.shape()).isEqualTo("json");
        assertThat(orders.operations().getFirst().params()).isEmpty();
    }

    @Test
    void theManifestMayStandAloneOrShareTheFleetFile(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        Path shared = dir.resolve("fleet.yaml");
        Files.writeString(shared, """
                agentspaces:
                  bind: 127.0.0.1:7500
                  groups:
                    - name: shop-fleet
                      founding: shop-v1
                      spaces: [ { name: data } ]
                """ + MINIMAL);
        CelldManifest fromShared = CelldManifest.load(shared);
        assertThat(fromShared.application()).isEqualTo("shop");

        Path alone = dir.resolve("manifest.yaml");
        Files.writeString(alone, MINIMAL.replace("celld:\n", "").replaceAll("(?m)^  ", ""));
        CelldManifest fromAlone = CelldManifest.load(alone);
        assertThat(fromAlone.routes()).hasSize(1);

        assertThatThrownBy(() -> CelldManifest.parse("agentspaces:\n  bind: 127.0.0.1:1\n"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no 'celld' key");
        assertThatThrownBy(() -> CelldManifest.parse("")).hasMessageContaining("empty");
    }

    @Test
    void durationsTakeIsoAndTheShortForm() {
        assertThat(CelldManifest.parseDuration("PT5M", "x")).isEqualTo(Duration.ofMinutes(5));
        assertThat(CelldManifest.parseDuration("250ms", "x")).isEqualTo(Duration.ofMillis(250));
        assertThat(CelldManifest.parseDuration("5s", "x")).isEqualTo(Duration.ofSeconds(5));
        assertThat(CelldManifest.parseDuration("2m", "x")).isEqualTo(Duration.ofMinutes(2));
        assertThat(CelldManifest.parseDuration("1h", "x")).isEqualTo(Duration.ofHours(1));
        assertThat(CelldManifest.parseDuration("1d", "x")).isEqualTo(Duration.ofDays(1));
        assertThatThrownBy(() -> CelldManifest.parseDuration("soon", "celld.freshness"))
                .hasMessageContaining("celld.freshness 'soon' is not a duration");
    }

    @Test
    void credentialsAreABearerOrAnMtlsKeyStore(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        Path tokenFile = dir.resolve("token");
        Files.writeString(tokenFile, "  from-file \n");
        CelldManifest fromFile = CelldManifest.parse(MINIMAL.replace("{ bearer: t }",
                "{ bearer-file: " + tokenFile + " }"));
        assertThat(fromFile.credential().bearer()).isEqualTo("from-file");

        CelldManifest mtls = CelldManifest.parse(MINIMAL.replace("{ bearer: t }", """
                {
                      mtls: { key-store: /etc/celld/client.p12, key-store-password: pw,
                              trust-store: /etc/celld/ca.jks, trust-store-type: JKS } }"""));
        assertThat(mtls.credential().hasBearer()).isFalse();
        assertThat(mtls.credential().keyStore()).isEqualTo(Path.of("/etc/celld/client.p12"));
        assertThat(mtls.credential().keyStorePassword()).containsExactly('p', 'w');
        assertThat(mtls.credential().keyStoreType()).isEqualTo("PKCS12");
        assertThat(mtls.credential().trustStore()).isEqualTo(Path.of("/etc/celld/ca.jks"));
        assertThat(mtls.credential().trustStoreType()).isEqualTo("JKS");

        CelldManifest both = CelldManifest.parse(MINIMAL.replace("{ bearer: t }",
                "{ bearer: t, mtls: { trust-store: /etc/celld/ca.p12 } }"));
        assertThat(both.credential().hasBearer()).isTrue();
        assertThat(both.credential().hasTrustStore()).isTrue();

        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("  credential: { bearer: t }\n", "")))
                .hasMessageContaining("celld.credential is required");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("{ bearer: t }", "{ bearer: a, bearer-env: B }")))
                .hasMessageContaining("more than one of bearer");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("{ bearer: t }",
                "{ bearer-env: AGENTSPACES_CELLD_TEST_UNSET_VARIABLE }")))
                .hasMessageContaining("AGENTSPACES_CELLD_TEST_UNSET_VARIABLE").hasMessageContaining("not set");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("{ bearer: t }", "{ mtls: { } }")))
                .hasMessageContaining("neither key-store nor trust-store");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("{ bearer: t }", "{ token: t }")))
                .hasMessageContaining("celld.credential has unknown keys [token]");
    }

    @Test
    void thePlaintextRuleAndTheUrlRules() {
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("https://shop.example", "http://shop.example")))
                .hasMessageContaining("plaintext http").hasMessageContaining("allow-plaintext");
        CelldManifest plain = CelldManifest.parse(MINIMAL.replace("https://shop.example", "http://shop.example")
                .replace("application: shop", "application: shop\n  allow-plaintext: true"));
        assertThat(plain.baseUrl()).isEqualTo(URI.create("http://shop.example"));
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("https://shop.example", "ftp://shop.example")))
                .hasMessageContaining("must be an https URL");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("https://shop.example",
                "https://shop.example/base?x=1"))).hasMessageContaining("no query or fragment");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("path: /orders", "path: orders")))
                .hasMessageContaining("must start with '/'");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("path: /orders", "path: \"/orders?all=1\"")))
                .hasMessageContaining("no query or fragment");
    }

    @Test
    void unknownKeysAndBadShapesFailNamingThePlace() {
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("application: shop", "application: shop\n  app: x")))
                .hasMessageContaining("celld has unknown keys [app]");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("- name: orders", "- name: orders\n      sql: x")))
                .hasMessageContaining("celld.routes[orders] has unknown keys [sql]");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("path: /orders }", "path: /orders, body: x }")))
                .hasMessageContaining("celld.routes[orders].operations[listOrders] has unknown keys [body]");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("method: GET", "method: DELETE")))
                .hasMessageContaining("'DELETE' must be GET (a read operation) or POST (an invoke operation)");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("path: /orders }", "path: /orders, idempotent: false }")))
                .hasMessageContaining("GET route").hasMessageContaining("always idempotent");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("  routes:\n", "  routes: []\n")
                .replaceAll("(?s)    - name: orders.*", ""))).hasMessageContaining("at least one route group");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("path: /orders }", "path: /orders, params: [op] }")))
                .hasMessageContaining("may not name 'op'");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("path: /orders }", "path: /orders, params: [a, a] }")))
                .hasMessageContaining("names 'a' twice");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("name: listOrders", "name: op")))
                .hasMessageContaining("other than 'op'");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL
                + "        - { name: listOrders, method: POST, path: /orders/x }\n"))
                .hasMessageContaining("names 'listOrders' twice");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL + MINIMAL.substring(MINIMAL.indexOf("    - name"))))
                .hasMessageContaining("celld.routes names 'orders' twice");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("application: shop", "application: shop\n  runtime: lambda")))
                .hasMessageContaining("celld.runtime 'lambda'");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("application: shop", "application: shop\n  max-body-bytes: 0")))
                .hasMessageContaining("max-body-bytes must be positive");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("application: shop", "application: shop\n  timeouts: { read: 1s }")))
                .hasMessageContaining("celld.timeouts has unknown keys [read]");
        assertThatThrownBy(() -> CelldManifest.parse(MINIMAL.replace("application: shop", "application: a shop")))
                .hasMessageContaining("celld.application 'a shop'");
    }

    @Test
    void routeGroupsExposeTheirOperationsByKind() {
        CelldManifest manifest = CelldManifest.parse(MINIMAL.replace("path: /orders }", "path: /orders }\n"
                + "        - { name: cancel, method: POST, path: /orders/cancel, params: [id] }"));
        CelldManifest.RouteGroup orders = manifest.route("route/orders");
        assertThat(orders.readOperations()).extracting(CelldManifest.Operation::name).containsExactly("listOrders");
        assertThat(orders.invokeOperations()).extracting(CelldManifest.Operation::name).containsExactly("cancel");
        assertThat(orders.operation("cancel").isInvoke()).isTrue();
        assertThat(orders.operation("listOrders").isRead()).isTrue();
        assertThat(List.of(orders.asset())).containsExactly("route/orders");
    }
}
