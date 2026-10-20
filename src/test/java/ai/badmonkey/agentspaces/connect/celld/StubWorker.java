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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local {@code com.sun.net.httpserver.HttpServer} standing in for the
 * Worker: {@code GET /api/stock} answers rows filtered by {@code sku},
 * {@code POST /api/stock/reserve} counts calls per {@code Aspace-Invocation-Id}
 * and answers the stored result on a repeat (the application's {@code once}),
 * and a few routes misbehave on purpose (a 500, a slow answer, an oversize
 * body, a non-JSON body). Every request's headers, query, and body are kept
 * for assertions.
 */
final class StubWorker implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    final HttpServer server;
    final Map<String, AtomicInteger> callsByInvocation = new ConcurrentHashMap<>();
    final Map<String, String> resultByInvocation = new ConcurrentHashMap<>();
    final AtomicInteger requests = new AtomicInteger();
    volatile String lastMethod;
    volatile String lastPath;
    volatile String lastQuery;
    volatile String lastAuthorization;
    volatile String lastInvocationId;
    volatile String lastAccept;
    volatile String lastContentType;
    volatile JsonNode lastBody;

    StubWorker() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/api/stock/reserve", this::reserve);
        server.createContext("/api/stock/audit", this::reserve);
        server.createContext("/api/stock", this::stock);
        server.createContext("/api/rates", this::stock);
        server.createContext("/api/fail", exchange -> answer(exchange, 500, "{\"error\":\"boom\"}"));
        server.createContext("/api/slow", exchange -> {
            record(exchange);
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            answer(exchange, 200, "[]");
        });
        server.createContext("/api/big", exchange -> answer(exchange, 200,
                "[" + "{\"k\":\"" + "x".repeat(2_000) + "\"}".repeat(1) + "]"));
        server.createContext("/api/text", exchange -> answer(exchange, 200, "not json at all"));
        server.createContext("/api/scalar", exchange -> answer(exchange, 200, "42"));
        server.createContext("/api/empty", exchange -> answer(exchange, 204, ""));
        server.start();
    }

    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    int calls(String invocationId) {
        AtomicInteger n = callsByInvocation.get(invocationId);
        return n == null ? 0 : n.get();
    }

    private void record(HttpExchange exchange) throws IOException {
        requests.incrementAndGet();
        lastMethod = exchange.getRequestMethod();
        lastPath = exchange.getRequestURI().getPath();
        lastQuery = exchange.getRequestURI().getRawQuery();
        lastAuthorization = exchange.getRequestHeaders().getFirst("Authorization");
        lastInvocationId = exchange.getRequestHeaders().getFirst(CelldClient.INVOCATION_HEADER);
        lastAccept = exchange.getRequestHeaders().getFirst("Accept");
        lastContentType = exchange.getRequestHeaders().getFirst("Content-Type");
        byte[] body = exchange.getRequestBody().readAllBytes();
        lastBody = body.length == 0 ? null : JSON.readTree(body);
    }

    static Map<String, String> query(String raw) {
        Map<String, String> params = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) {
            return params;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            params.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return params;
    }

    private void stock(HttpExchange exchange) throws IOException {
        record(exchange);
        Map<String, String> params = query(lastQuery);
        List<Map<String, String>> rows = List.of(
                Map.of("sku", "A-1", "stock", "7", "warehouse", "w7"),
                Map.of("sku", "B-2", "stock", "0", "warehouse", "w7"));
        List<Map<String, String>> matched = rows.stream()
                .filter(r -> !params.containsKey("sku") || r.get("sku").equals(params.get("sku")))
                .toList();
        answer(exchange, 200, JSON.writeValueAsString(matched));
    }

    private void reserve(HttpExchange exchange) throws IOException {
        record(exchange);
        String id = lastInvocationId == null ? "" : lastInvocationId;
        callsByInvocation.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
        String stored = resultByInvocation.get(id);
        if (stored == null) {
            String qty = lastBody != null && lastBody.has("qty") ? lastBody.get("qty").asText() : "0";
            stored = JSON.writeValueAsString(Map.of("reserved", qty, "invocationId", id,
                    "nested", Map.of("ok", true)));
            resultByInvocation.put(id, stored);
        }
        answer(exchange, 200, stored);
    }

    private static void answer(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        if (bytes.length == 0) {
            exchange.sendResponseHeaders(status, -1);
        } else {
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
