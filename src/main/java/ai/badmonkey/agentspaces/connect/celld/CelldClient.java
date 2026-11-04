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
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The HTTPS client over the deployment's allowlisted routes: one
 * {@link HttpClient} carrying the operator's credential (a bearer token as
 * {@code Authorization: Bearer}, or a client certificate from the manifest's
 * key store), the manifest's connect and request timeouts, and the response
 * body bound. A read is a {@code GET} with the parameters as query
 * parameters; an invocation is a {@code POST} with the parameters as a JSON
 * object and the invocation id as the header {@value #INVOCATION_HEADER}
 * (appendix F), which the target deduplicates on. The client never builds a
 * URL from anything but a manifest route and declared parameters: the
 * provider checks both before calling it.
 */
public final class CelldClient {

    /** The header carrying the invocation id to the route (appendix F). */
    public static final String INVOCATION_HEADER = "Aspace-Invocation-Id";
    /** The header carrying the content root of an artifact body. */
    public static final String ROOT_HEADER = "Aspace-Content-Root";
    /** The header naming a chunked upload's step: {@code piece} or {@code finish}. */
    public static final String UPLOAD_HEADER = "Aspace-Upload";
    /** The header carrying a chunked upload's piece index. */
    public static final String PIECE_INDEX_HEADER = "Aspace-Piece-Index";
    /** The header carrying a chunked upload's piece count. */
    public static final String PIECE_COUNT_HEADER = "Aspace-Piece-Count";
    /** The query parameter that names an object on a ranged get route. */
    public static final String KEY_PARAMETER = "key";

    private static final ObjectMapper JSON = new ObjectMapper();

    private final CelldManifest manifest;
    private final HttpClient http;
    private final AtomicInteger calls = new AtomicInteger();

    /**
     * One route's answer.
     *
     * @param status the HTTP status
     * @param body   the parsed JSON body, or {@code null} for an empty body
     */
    public record Response(int status, JsonNode body) {

        /** Whether the status is 2xx. */
        public boolean ok() {
            return status >= 200 && status < 300;
        }
    }

    /** Thrown when a route answered outside its bounds or not at all. */
    public static final class RouteException extends RuntimeException {

        private final String error;
        private final String rule;

        /**
         * @param error  the appendix F error code ({@code target-error} or {@code timeout})
         * @param rule   the rule or bound that produced it
         * @param detail what happened
         */
        public RouteException(String error, String rule, String detail, Throwable cause) {
            super(detail, cause);
            this.error = error;
            this.rule = rule;
        }

        /** The appendix F error code. */
        public String error() {
            return error;
        }

        /** The rule or bound named in the result. */
        public String rule() {
            return rule;
        }
    }

    /**
     * A streamed answer: the status, the media type, and the body, bounded by
     * {@code artifacts.max-bytes} and {@code timeouts.transfer}. Close it.
     *
     * @param status      the HTTP status
     * @param contentType the {@code Content-Type}, or {@code null}
     * @param body        the body; reading past the bound or the deadline throws a {@link RouteException}
     *                    wrapped in an {@link IOException}
     */
    public record Streamed(int status, String contentType, InputStream body) implements AutoCloseable {

        /** Whether the status is 2xx. */
        public boolean ok() {
            return status >= 200 && status < 300;
        }

        @Override
        public void close() {
            try {
                body.close();
            } catch (IOException e) {
                // nothing to release beyond the connection
            }
        }
    }

    /** Thrown when a ranged read cannot serve the version the swarm hashed. */
    public static final class RangeMiss extends RuntimeException {

        private final int status;

        /**
         * @param status the HTTP status, or 0 when the answer's range did not match
         * @param detail what happened
         */
        public RangeMiss(int status, String detail) {
            super(detail);
            this.status = status;
        }

        /** The HTTP status. */
        public int status() {
            return status;
        }
    }

    /** One Server-Sent Event. */
    public record Event(String id, String data) {
    }

    /** Creates a client for the manifest's deployment and credential. */
    public CelldClient(CelldManifest manifest) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(manifest.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER);
        SSLContext tls = sslContext(manifest.credential());
        if (tls != null) {
            builder.sslContext(tls);
        }
        this.http = builder.build();
    }

    /** The manifest this client calls. */
    public CelldManifest manifest() {
        return manifest;
    }

    /** How many HTTP calls this client made. */
    public int calls() {
        return calls.get();
    }

    /**
     * A read: {@code GET} on the route with the parameters as query parameters.
     *
     * @param operation the GET route
     * @param params    the declared parameters and their values
     * @return the route's answer
     * @throws RouteException on a timeout, an unreachable route, or a body over the bound
     */
    public Response get(CelldManifest.Operation operation, Map<String, String> params) {
        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            query.append(query.isEmpty() ? '?' : '&')
                    .append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8))
                    .append('=')
                    .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
        }
        HttpRequest request = base(URI.create(manifest.baseUrl() + operation.path() + query))
                .GET()
                .build();
        return send(request, operation);
    }

    /**
     * An invocation: {@code POST} on the route with the parameters as the
     * JSON body and the invocation id as {@value #INVOCATION_HEADER}.
     *
     * @param operation    the POST route
     * @param params       the declared parameters and their values
     * @param invocationId the invocation's id, constant across retries
     * @return the route's answer
     * @throws RouteException on a timeout, an unreachable route, or a body over the bound
     */
    public Response post(CelldManifest.Operation operation, Map<String, String> params, String invocationId) {
        ObjectNode body = JSON.createObjectNode();
        params.forEach(body::put);
        String json;
        try {
            json = JSON.writeValueAsString(body);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        HttpRequest request = base(URI.create(manifest.baseUrl() + operation.path()))
                .header("Content-Type", "application/json")
                .header(INVOCATION_HEADER, Objects.requireNonNull(invocationId, "invocationId"))
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build();
        return send(request, operation);
    }

    /**
     * A JSON read of a route the manifest names outside the route groups: an object list or
     * the change route. The body bound is {@code max-body-bytes}.
     *
     * @param path   the manifest path
     * @param params the query parameters
     * @return the answer
     */
    public Response getJson(String path, Map<String, String> params) {
        HttpRequest request = base(URI.create(manifest.baseUrl() + path + query(params))).GET().build();
        return send(request, "GET " + path);
    }

    /**
     * Opens a route's body as a stream, for an operation whose answer leaves as an artifact
     * (CC-B-01): a {@code GET} with the parameters as query parameters, or a {@code POST} with them
     * as a JSON object and the invocation id. The request timeout bounds the time to the headers;
     * {@code timeouts.transfer} and {@code artifacts.max-bytes} bound the body as it is read, and
     * nothing depends on {@code Content-Length} (celld removes it from Worker responses).
     *
     * @param operation    the route
     * @param params       the declared parameters
     * @param invocationId the invocation id for a POST, or {@code null} for a GET
     * @return the streamed answer
     */
    public Streamed open(CelldManifest.Operation operation, Map<String, String> params, String invocationId) {
        HttpRequest.Builder builder;
        if (operation.isRead()) {
            builder = base(URI.create(manifest.baseUrl() + operation.path() + query(params)),
                    operation.mediaType()).GET();
        } else {
            builder = base(URI.create(manifest.baseUrl() + operation.path()), operation.mediaType())
                    .header("Content-Type", "application/json")
                    .header(INVOCATION_HEADER, Objects.requireNonNull(invocationId, "invocationId"))
                    .POST(HttpRequest.BodyPublishers.ofString(json(params), StandardCharsets.UTF_8));
        }
        String where = operation.method() + " " + operation.path();
        HttpResponse<InputStream> response = exchange(builder.build(), where);
        long declared = response.headers().firstValueAsLong("Content-Length").orElse(-1L);
        long max = manifest.artifacts().maxBytes();
        if (declared > max) {
            close(response.body());
            throw new RouteException("target-error", "artifacts.max-bytes", where + " answered " + declared
                    + " bytes, over the manifest's " + max, null);
        }
        InputStream body = new BoundedBody(response.body(), max, manifest.transferTimeout(), where);
        return new Streamed(response.statusCode(), response.headers().firstValue("Content-Type").orElse(null), body);
    }

    /**
     * A ranged read of one object (CC-R-01): {@code GET <path>?key=<key>} with {@code Range} and,
     * when known, {@code If-Match}. Only a 206 whose {@code Content-Range} matches the request is
     * an answer; a 200, 412, 416, or another range is a {@link RangeMiss}.
     *
     * @param path   the object source's get path
     * @param key    the object key
     * @param offset the first byte
     * @param length the byte count, at least 1
     * @param etag   the ETag the swarm hashed, or {@code null}
     * @return the bytes
     */
    public byte[] range(String path, String key, long offset, int length, String etag) {
        String where = "GET " + path;
        HttpRequest.Builder builder = base(URI.create(manifest.baseUrl() + path
                + query(Map.of(KEY_PARAMETER, key))), "application/octet-stream")
                .header("Range", "bytes=" + offset + "-" + (offset + length - 1)).GET();
        if (etag != null) {
            builder.header("If-Match", etag);
        }
        HttpResponse<InputStream> response = exchange(builder.build(), where);
        try (InputStream in = response.body()) {
            int status = response.statusCode();
            if (status != 206) {
                throw new RangeMiss(status, where + " answered " + status + " to a range of " + key);
            }
            String range = response.headers().firstValue("Content-Range").orElse("");
            String expected = "bytes " + offset + "-" + (offset + length - 1) + "/";
            if (!range.startsWith(expected)) {
                throw new RangeMiss(0, where + " answered the range '" + range + "' for '" + expected + "*'");
            }
            byte[] bytes = in.readNBytes(length + 1);
            if (bytes.length != length) {
                throw new RangeMiss(0, where + " answered " + bytes.length + " bytes for a range of " + length);
            }
            return bytes;
        } catch (IOException e) {
            throw new RouteException("target-error", "route", where + " body unreadable: " + e, e);
        }
    }

    /**
     * Streams a whole object, for hashing (CC-R-02): {@code GET <path>?key=<key>} with
     * {@code If-Match} when known. The caller closes the stream.
     *
     * @param path    the get path
     * @param key     the object key
     * @param etag    the ETag to require, or {@code null}
     * @param maxSize the bound on the body
     * @return the body, and the ETag the answer carries
     */
    public Map.Entry<InputStream, String> object(String path, String key, String etag, long maxSize) {
        String where = "GET " + path;
        HttpRequest.Builder builder = base(URI.create(manifest.baseUrl() + path
                + query(Map.of(KEY_PARAMETER, key))), "application/octet-stream").GET();
        if (etag != null) {
            builder.header("If-Match", etag);
        }
        HttpResponse<InputStream> response = exchange(builder.build(), where);
        if (response.statusCode() != 200) {
            close(response.body());
            throw new RangeMiss(response.statusCode(), where + " answered " + response.statusCode() + " for " + key);
        }
        return Map.entry(new BoundedBody(response.body(), maxSize, manifest.transferTimeout(), where),
                response.headers().firstValue("ETag").orElse(etag == null ? "" : etag));
    }

    /**
     * An artifact body (CC-I-03): a {@code POST} streaming the artifact with its length, the media
     * type, the other parameters as query parameters, the invocation id, and the root.
     *
     * @param operation    the POST route
     * @param query        the declared parameters other than the artifact parameter
     * @param body         opens the artifact's plaintext
     * @param length       the artifact's size
     * @param mediaType    the artifact's media type
     * @param invocationId the invocation id
     * @param root         the artifact's root
     * @return the route's answer
     */
    public Response upload(CelldManifest.Operation operation, Map<String, String> query, Supplier<InputStream> body,
                           long length, String mediaType, String invocationId, String root) {
        HttpRequest request = base(URI.create(manifest.baseUrl() + operation.path() + query(query)))
                .timeout(manifest.transferTimeout())
                .header("Content-Type", mediaType == null ? "application/octet-stream" : mediaType)
                .header(INVOCATION_HEADER, Objects.requireNonNull(invocationId, "invocationId"))
                .header(ROOT_HEADER, Objects.requireNonNull(root, "root"))
                .POST(HttpRequest.BodyPublishers.fromPublisher(HttpRequest.BodyPublishers.ofInputStream(body), length))
                .build();
        return send(request, operation.method() + " " + operation.path());
    }

    /**
     * One step of a chunked artifact body (CC-I-07): a piece, or the finish once every piece landed.
     *
     * @param operation    the POST route
     * @param query        the declared parameters other than the artifact parameter
     * @param piece        the piece's bytes, or {@code null} for the finish
     * @param index        the piece index (ignored for the finish)
     * @param count        the piece count
     * @param invocationId the invocation id
     * @param root         the artifact's root
     * @return the route's answer
     */
    public Response uploadPiece(CelldManifest.Operation operation, Map<String, String> query, byte[] piece, int index,
                                int count, String invocationId, String root) {
        HttpRequest.Builder builder = base(URI.create(manifest.baseUrl() + operation.path() + query(query)))
                .header(INVOCATION_HEADER, Objects.requireNonNull(invocationId, "invocationId"))
                .header(ROOT_HEADER, Objects.requireNonNull(root, "root"))
                .header(PIECE_COUNT_HEADER, Integer.toString(count));
        if (piece == null) {
            builder.header(UPLOAD_HEADER, "finish").POST(HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header(UPLOAD_HEADER, "piece").header(PIECE_INDEX_HEADER, Integer.toString(index))
                    .header("Content-Type", "application/octet-stream")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(piece));
        }
        return send(builder.build(), operation.method() + " " + operation.path());
    }

    /**
     * Reads a Server-Sent Events route until it ends or fails (CC-N-06), passing each event on.
     *
     * @param path        the stream path
     * @param lastEventId the cursor to resume after, or {@code null}
     * @param onEvent     receives each event with data
     */
    public void events(String path, String lastEventId, Consumer<Event> onEvent) {
        String where = "GET " + path;
        HttpRequest.Builder builder = base(URI.create(manifest.baseUrl() + path), "text/event-stream").GET();
        if (lastEventId != null) {
            builder.header("Last-Event-ID", lastEventId);
        }
        HttpResponse<java.util.stream.Stream<String>> response;
        calls.incrementAndGet();
        try {
            response = http.send(builder.build(), HttpResponse.BodyHandlers.ofLines());
        } catch (HttpTimeoutException e) {
            throw new RouteException("timeout", "timeouts.request", where + " did not answer within "
                    + manifest.requestTimeout(), e);
        } catch (IOException e) {
            throw new RouteException("target-error", "route", where + " failed: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RouteException("target-error", "route", where + " interrupted", e);
        }
        if (response.statusCode() != 200) {
            response.body().close();
            throw new RouteException("target-error", "http-status", where + " answered " + response.statusCode(), null);
        }
        String[] id = {null};
        StringBuilder data = new StringBuilder();
        long[] bytes = {0};
        try (java.util.stream.Stream<String> lines = response.body()) {
            lines.forEach(line -> {
                bytes[0] += line.length();
                if (data.length() > manifest.maxBodyBytes()) {
                    throw new RouteException("target-error", "max-body-bytes", where + " sent an event over the bound",
                            null);
                }
                if (line.isEmpty()) {
                    if (!data.isEmpty()) {
                        onEvent.accept(new Event(id[0], data.toString()));
                    }
                    data.setLength(0);
                    id[0] = null;
                } else if (line.startsWith(":")) {
                    // a comment: the keep-alive
                } else if (line.startsWith("data:")) {
                    if (!data.isEmpty()) {
                        data.append('\n');
                    }
                    data.append(line.substring(5).stripLeading());
                } else if (line.startsWith("id:")) {
                    id[0] = line.substring(3).strip();
                }
            });
        } catch (UncheckedIOException e) {
            throw new RouteException("target-error", "route", where + " stream failed: " + e.getCause(), e);
        }
    }

    private String query(Map<String, String> params) {
        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            query.append(query.isEmpty() ? '?' : '&')
                    .append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8))
                    .append('=')
                    .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
        }
        return query.toString();
    }

    private static String json(Map<String, String> params) {
        ObjectNode body = JSON.createObjectNode();
        params.forEach(body::put);
        try {
            return JSON.writeValueAsString(body);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void close(InputStream in) {
        try {
            in.close();
        } catch (IOException e) {
            // releasing the connection only
        }
    }

    private HttpRequest.Builder base(URI uri) {
        return base(uri, "application/json");
    }

    private HttpRequest.Builder base(URI uri, String accept) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(manifest.requestTimeout())
                .header("Accept", accept);
        if (manifest.credential().hasBearer()) {
            builder.header("Authorization", "Bearer " + manifest.credential().bearer());
        }
        return builder;
    }

    private HttpResponse<InputStream> exchange(HttpRequest request, String where) {
        calls.incrementAndGet();
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpTimeoutException e) {
            throw new RouteException("timeout", "timeouts.request", where + " did not answer within "
                    + manifest.requestTimeout(), e);
        } catch (IOException e) {
            throw new RouteException("target-error", "route", where + " failed: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RouteException("target-error", "route", where + " interrupted", e);
        }
    }

    private Response send(HttpRequest request, CelldManifest.Operation operation) {
        return send(request, operation.method() + " " + operation.path());
    }

    private Response send(HttpRequest request, String where) {
        calls.incrementAndGet();
        HttpResponse<InputStream> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpTimeoutException e) {
            throw new RouteException("timeout", "timeouts.request", where + " did not answer within "
                    + manifest.requestTimeout(), e);
        } catch (IOException e) {
            throw new RouteException("target-error", "route", where + " failed: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RouteException("target-error", "route", where + " interrupted", e);
        }
        long declared = response.headers().firstValueAsLong("Content-Length").orElse(-1L);
        long max = manifest.maxBodyBytes();
        if (declared > max) {
            throw new RouteException("target-error", "max-body-bytes", where + " answered " + declared
                    + " bytes, over the manifest's " + max, null);
        }
        byte[] bytes;
        try (InputStream in = response.body()) {
            bytes = in.readNBytes((int) Math.min(max + 1, Integer.MAX_VALUE));
        } catch (IOException e) {
            throw new RouteException("target-error", "route", where + " body unreadable: " + e, e);
        }
        if (bytes.length > max) {
            throw new RouteException("target-error", "max-body-bytes", where + " answered more than " + max
                    + " bytes", null);
        }
        JsonNode body = null;
        if (bytes.length > 0) {
            try {
                body = JSON.readTree(bytes);
            } catch (IOException e) {
                throw new RouteException("target-error", "json", where + " answered a body that is not JSON: "
                        + e.getMessage(), e);
            }
        }
        return new Response(response.statusCode(), body);
    }

    private static SSLContext sslContext(CelldManifest.Credential credential) {
        if (!credential.hasKeyStore() && !credential.hasTrustStore()) {
            return null;
        }
        try {
            KeyManagerFactory keys = null;
            if (credential.hasKeyStore()) {
                KeyStore store = KeyStore.getInstance(credential.keyStoreType());
                try (InputStream in = Files.newInputStream(credential.keyStore())) {
                    store.load(in, credential.keyStorePassword());
                }
                keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keys.init(store, credential.keyStorePassword());
            }
            TrustManagerFactory trust = null;
            if (credential.hasTrustStore()) {
                KeyStore store = KeyStore.getInstance(credential.trustStoreType());
                try (InputStream in = Files.newInputStream(credential.trustStore())) {
                    store.load(in, credential.trustStorePassword());
                }
                trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trust.init(store);
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys == null ? null : keys.getKeyManagers(),
                    trust == null ? null : trust.getTrustManagers(), null);
            return context;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the mtls stores: " + e.getMessage(), e);
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("cannot load the mtls stores: " + e.getMessage(), e);
        }
    }
}
