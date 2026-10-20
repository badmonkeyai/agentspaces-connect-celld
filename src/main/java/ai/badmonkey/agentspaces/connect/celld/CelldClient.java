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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

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

    private HttpRequest.Builder base(URI uri) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(manifest.requestTimeout())
                .header("Accept", "application/json");
        if (manifest.credential().hasBearer()) {
            builder.header("Authorization", "Bearer " + manifest.credential().bearer());
        }
        return builder;
    }

    private Response send(HttpRequest request, CelldManifest.Operation operation) {
        calls.incrementAndGet();
        String where = operation.method() + " " + operation.path();
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
