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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Worker that behaves as celld does for the phase 2 routes: every body goes
 * out chunked with no {@code Content-Length} (celld removes it), objects answer
 * ranged reads with {@code ETag}, {@code If-Match}, 206, 412, and 416, a change
 * log answers after a cursor and as Server-Sent Events, and ingest routes
 * deduplicate on the invocation id, whole or piece by piece.
 */
final class SwarmWorker implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern RANGE = Pattern.compile("bytes=(\\d+)-(\\d+)");

    /** One stored object. */
    record StoredObject(byte[] bytes, String etag, String mediaType, Map<String, String> attrs) {
    }

    final HttpServer server;
    final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
    final Map<String, StoredObject> objects = new TreeMap<>();
    final List<Map<String, Object>> changeLog = new CopyOnWriteArrayList<>();
    final Map<String, byte[]> ingested = new ConcurrentHashMap<>();
    final Map<String, AtomicInteger> ingestCalls = new ConcurrentHashMap<>();
    /** Effects per invocation id: what once(invocationId) stored, whatever the calls. */
    final Map<String, AtomicInteger> ingestEffects = new ConcurrentHashMap<>();
    final Map<String, Map<Integer, byte[]>> pieces = new ConcurrentHashMap<>();
    final AtomicInteger rangeReads = new AtomicInteger();
    final List<String> ranges = new CopyOnWriteArrayList<>();
    final AtomicInteger wholeReads = new AtomicInteger();
    volatile byte[] export;
    volatile String snapshot = "{\"sku\":\"A-1\",\"qty\":7}\n";
    volatile String auto = "[{\"sku\":\"A-1\"}]";
    volatile String lastAccept;
    volatile String lastRoot;
    volatile long lastLength = -1;
    volatile int ingestStatus = 200;

    SwarmWorker() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/api/exports/nightly", ex -> {
            count("nightly");
            lastAccept = ex.getRequestHeaders().getFirst("Accept");
            chunked(ex, 200, export, "application/x-ndjson");
        });
        server.createContext("/api/exports/endless", ex -> {
            count("endless");
            ex.sendResponseHeaders(200, 0);
            byte[] block = new byte[64 * 1024];
            try (OutputStream out = ex.getResponseBody()) {
                for (int i = 0; i < 100_000; i++) {
                    out.write(block);
                }
            } catch (IOException e) {
                // the connector closed the stream at its bound
            }
        });
        server.createContext("/api/exports/auto", ex -> {
            count("auto");
            chunked(ex, 200, auto.getBytes(StandardCharsets.UTF_8), "application/json");
        });
        server.createContext("/api/stock/snapshot", ex -> {
            count("snapshot");
            chunked(ex, 200, snapshot.getBytes(StandardCharsets.UTF_8), "application/x-ndjson");
        });
        server.createContext("/api/stock", ex -> {
            count("stock");
            chunked(ex, 200, ("[{\"sku\":\"A-1\",\"qty\":\"" + qty() + "\"}]").getBytes(StandardCharsets.UTF_8),
                    "application/json");
        });
        server.createContext("/api/models/object", this::object);
        server.createContext("/api/models", this::list);
        server.createContext("/api/changes", this::changes);
        server.createContext("/api/ingest", this::ingest);
        server.start();
    }

    private volatile int qty = 7;

    int qty() {
        return qty;
    }

    /** Writes stock, as the Worker would, and records the change. */
    void setStock(int value) {
        qty = value;
        snapshot = "{\"sku\":\"A-1\",\"qty\":" + value + "}\n";
        Map<String, Object> change = new LinkedHashMap<>();
        change.put("kind", "upsert");
        change.put("route", "stock");
        change.put("key", "A-1");
        change.put("row", Map.of("sku", "A-1", "qty", Integer.toString(value)));
        changeLog.add(change);
    }

    /** Records an object change, as the Worker would. */
    void objectChanged(String key) {
        Map<String, Object> change = new LinkedHashMap<>();
        change.put("kind", "object");
        change.put("object", "models");
        change.put("key", key);
        changeLog.add(change);
    }

    synchronized void putObject(String key, byte[] bytes, Map<String, String> attrs) {
        objects.put(key, new StoredObject(bytes, "\"" + sha(bytes).substring(0, 16) + "\"",
                "application/octet-stream", attrs));
    }

    synchronized void removeObject(String key) {
        objects.remove(key);
    }

    int calls(String route) {
        AtomicInteger n = calls.get(route);
        return n == null ? 0 : n.get();
    }

    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    // ------------------------------------------------------------------ routes

    private void list(HttpExchange ex) throws IOException {
        count("list");
        Map<String, String> query = StubWorker.query(ex.getRequestURI().getRawQuery());
        int limit = Integer.parseInt(query.getOrDefault("limit", "1000"));
        String after = query.get("after");
        List<Map<String, Object>> items = new ArrayList<>();
        String last = null;
        synchronized (this) {
            for (Map.Entry<String, StoredObject> e : objects.entrySet()) {
                if (after != null && e.getKey().compareTo(after) <= 0) {
                    continue;
                }
                if (items.size() >= Math.min(limit, 2)) { // small pages, so paging is exercised
                    break;
                }
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("key", e.getKey());
                item.put("size", e.getValue().bytes().length);
                item.put("etag", e.getValue().etag());
                item.put("mediaType", e.getValue().mediaType());
                item.put("attrs", e.getValue().attrs());
                items.add(item);
                last = e.getKey();
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("items", items);
        if (last != null) {
            body.put("after", last);
        }
        chunked(ex, 200, JSON.writeValueAsBytes(body), "application/json");
    }

    private void object(HttpExchange ex) throws IOException {
        Map<String, String> query = StubWorker.query(ex.getRequestURI().getRawQuery());
        StoredObject object;
        synchronized (this) {
            object = objects.get(query.get("key"));
        }
        if (object == null) {
            chunked(ex, 404, new byte[0], "text/plain");
            return;
        }
        String ifMatch = ex.getRequestHeaders().getFirst("If-Match");
        if (ifMatch != null && !ifMatch.equals(object.etag())) {
            chunked(ex, 412, new byte[0], "text/plain");
            return;
        }
        ex.getResponseHeaders().set("ETag", object.etag());
        String range = ex.getRequestHeaders().getFirst("Range");
        if (range == null) {
            wholeReads.incrementAndGet();
            chunked(ex, 200, object.bytes(), object.mediaType());
            return;
        }
        Matcher m = RANGE.matcher(range);
        if (!m.matches()) {
            chunked(ex, 416, new byte[0], "text/plain");
            return;
        }
        long from = Long.parseLong(m.group(1));
        long to = Math.min(Long.parseLong(m.group(2)), object.bytes().length - 1L);
        if (from >= object.bytes().length || to < from) {
            ex.getResponseHeaders().set("Content-Range", "bytes */" + object.bytes().length);
            chunked(ex, 416, new byte[0], "text/plain");
            return;
        }
        rangeReads.incrementAndGet();
        ranges.add(query.get("key") + "@" + from + "-" + to);
        ex.getResponseHeaders().set("Content-Range", "bytes " + from + "-" + to + "/" + object.bytes().length);
        chunked(ex, 206, java.util.Arrays.copyOfRange(object.bytes(), (int) from, (int) to + 1), object.mediaType());
    }

    private void changes(HttpExchange ex) throws IOException {
        count(ex.getRequestURI().getPath().endsWith("/stream") ? "stream" : "changes");
        Map<String, String> query = StubWorker.query(ex.getRequestURI().getRawQuery());
        if (ex.getRequestURI().getPath().endsWith("/stream")) {
            String last = ex.getRequestHeaders().getFirst("Last-Event-ID");
            int from = last == null ? changeLog.size() : Integer.parseInt(last);
            ex.getResponseHeaders().set("Content-Type", "text/event-stream");
            ex.sendResponseHeaders(200, 0);
            try (OutputStream out = ex.getResponseBody()) {
                out.write(": keep-alive\n\n".getBytes(StandardCharsets.UTF_8));
                for (int i = from; i < changeLog.size(); i++) {
                    out.write(("id: " + (i + 1) + "\ndata: " + JSON.writeValueAsString(changeLog.get(i)) + "\n\n")
                            .getBytes(StandardCharsets.UTF_8));
                }
            }
            return;
        }
        int after = query.containsKey("after") ? Integer.parseInt(query.get("after")) : changeLog.size();
        int limit = Integer.parseInt(query.getOrDefault("limit", "1000"));
        List<Map<String, Object>> page = new ArrayList<>();
        for (int i = after; i < changeLog.size() && page.size() < limit; i++) {
            page.add(changeLog.get(i));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("cursor", Integer.toString(after + page.size()));
        body.put("changes", page);
        chunked(ex, 200, JSON.writeValueAsBytes(body), "application/json");
    }

    private void ingest(HttpExchange ex) throws IOException {
        String id = ex.getRequestHeaders().getFirst(CelldClient.INVOCATION_HEADER);
        String root = ex.getRequestHeaders().getFirst(CelldClient.ROOT_HEADER);
        String step = ex.getRequestHeaders().getFirst(CelldClient.UPLOAD_HEADER);
        lastRoot = root;
        String length = ex.getRequestHeaders().getFirst("Content-Length");
        lastLength = length == null ? -1 : Long.parseLong(length);
        byte[] body = ex.getRequestBody().readAllBytes();
        if (ingestStatus != 200) {
            chunked(ex, ingestStatus, new byte[0], "text/plain");
            return;
        }
        if ("piece".equals(step)) {
            int index = Integer.parseInt(ex.getRequestHeaders().getFirst(CelldClient.PIECE_INDEX_HEADER));
            pieces.computeIfAbsent(id, k -> new ConcurrentHashMap<>()).put(index, body);
            chunked(ex, 200, "{}".getBytes(StandardCharsets.UTF_8), "application/json");
            return;
        }
        if ("finish".equals(step)) {
            int count = Integer.parseInt(ex.getRequestHeaders().getFirst(CelldClient.PIECE_COUNT_HEADER));
            Map<Integer, byte[]> got = pieces.getOrDefault(id, Map.of());
            ByteArrayOutputStream whole = new ByteArrayOutputStream();
            for (int i = 0; i < count; i++) {
                whole.write(got.get(i));
            }
            body = whole.toByteArray();
        }
        AtomicInteger n = ingestCalls.computeIfAbsent(id, k -> new AtomicInteger());
        if (n.incrementAndGet() == 1) {
            ingested.put(id, body); // once(invocationId): a retry stores nothing new
            ingestEffects.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
        }
        Map<String, Object> answer = new LinkedHashMap<>();
        answer.put("stored", "ingest/" + root);
        answer.put("bytes", ingested.get(id).length);
        answer.put("sha256", sha(ingested.get(id)));
        answer.put("batch", StubWorker.query(ex.getRequestURI().getRawQuery()).getOrDefault("batch", ""));
        chunked(ex, 200, JSON.writeValueAsBytes(answer), "application/json");
    }

    // ------------------------------------------------------------------ helpers

    private void count(String route) {
        calls.computeIfAbsent(route, k -> new AtomicInteger()).incrementAndGet();
    }

    /** Answers with a chunked body: no Content-Length, as celld sends a Worker's response. */
    private static void chunked(HttpExchange ex, int status, byte[] body, String contentType) throws IOException {
        ex.getResponseHeaders().set("Content-Type", contentType);
        if (body == null) {
            body = new byte[0];
        }
        ex.sendResponseHeaders(status, body.length == 0 && status != 200 ? -1 : 0);
        if (body.length > 0 || status == 200) {
            try (OutputStream out = ex.getResponseBody()) {
                for (int at = 0; at < body.length; at += 64 * 1024) {
                    out.write(body, at, Math.min(64 * 1024, body.length - at));
                }
            }
        } else {
            ex.close();
        }
    }

    static String sha(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] random(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }
}
