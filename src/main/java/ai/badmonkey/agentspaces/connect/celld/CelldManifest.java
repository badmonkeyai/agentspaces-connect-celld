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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The operator's manifest (srad/agentspaces-js.md JS-C-09, §7.2): the
 * deployment's base URL, the credential, and each allowlisted route group
 * with its operations. The connector publishes only what the manifest names
 * and calls only the routes it lists (JS-C-07); everything the connector may
 * do is written here, so the manifest is the allowlist and the trust
 * boundary at once.
 *
 * <p>The document is YAML under a top-level {@code celld:} key, so it can
 * share the fleet file whose {@code agentspaces:} subtree the Node CLI and
 * {@code FleetRuntime} read, or stand alone:
 *
 * <pre>{@code
 * celld:
 *   application: inventory               # workers://inventory/route/<name>
 *   base-url: https://inventory.internal.example
 *   runtime: celld                       # celld | workerd | cloudflare
 *   credential:
 *     bearer-env: INVENTORY_TOKEN        # or bearer, bearer-file, or mtls: { key-store, ... }
 *   timeouts: { connect: 5s, request: 30s }
 *   max-body-bytes: 1048576
 *   freshness: PT1M
 *   routes:
 *     - name: stock
 *       description: Stock levels by SKU
 *       shape: com.example.Stock#v1
 *       operations:
 *         - { name: getStock, method: GET, path: /api/stock, params: [sku, warehouse] }
 *         - { name: reserveStock, method: POST, path: /api/stock/reserve, params: [sku, qty], idempotent: true }
 * }</pre>
 *
 * <p>Unknown keys fail the parse so a typo surfaces at startup; every bound
 * (timeouts, the response body size) is a setting here, and nothing counts
 * routes, operations, parameters, or members.
 *
 * @param application     the application name in {@code workers://<application>/...}
 * @param baseUrl         the deployment's base URL; {@code https} unless {@code allowPlaintext}
 * @param runtime         the {@code access.runtime} value: {@code celld}, {@code workerd}, or {@code cloudflare}
 * @param connectorName   the connector's name, stamped into results
 * @param group           the fleet group to serve in; {@code null} for the first configured group
 * @param space           the group's space queries and invocations flow through
 * @param credential      the operator's credential
 * @param connectTimeout  the HTTP connect timeout
 * @param requestTimeout  the per-request timeout
 * @param maxBodyBytes    the largest response body the connector reads
 * @param freshness       the default card refresh cadence (the card TTL is twice it)
 * @param allowPlaintext  whether an {@code http://} base URL is accepted (tests and private networks)
 * @param fleetAuthorizer whether the fleet's authorizer gates serving and invoking
 * @param routes          the allowlisted route groups, one card each
 */
public record CelldManifest(String application, URI baseUrl, String runtime, String connectorName,
                            String group, String space, Credential credential,
                            Duration connectTimeout, Duration requestTimeout, long maxBodyBytes,
                            Duration freshness, boolean allowPlaintext, boolean fleetAuthorizer,
                            List<RouteGroup> routes) {

    /** The top-level key of the manifest document. */
    public static final String KEY = "celld";

    /** The reserved query parameter naming the read operation (appendix F). */
    public static final String OP_PARAMETER = "op";

    private static final Set<String> RUNTIMES = Set.of("celld", "workerd", "cloudflare");
    private static final Set<String> TOP_KEYS = Set.of("application", "base-url", "runtime", "connector-name",
            "group", "space", "credential", "timeouts", "max-body-bytes", "freshness", "allow-plaintext",
            "fleet-authorizer", "routes");
    private static final Set<String> CREDENTIAL_KEYS = Set.of("bearer", "bearer-env", "bearer-file", "mtls");
    private static final Set<String> MTLS_KEYS = Set.of("key-store", "key-store-password", "key-store-type",
            "trust-store", "trust-store-password", "trust-store-type");
    private static final Set<String> TIMEOUT_KEYS = Set.of("connect", "request");
    private static final Set<String> ROUTE_KEYS = Set.of("name", "description", "shape", "freshness", "operations");
    private static final Set<String> OPERATION_KEYS = Set.of("name", "method", "path", "params", "idempotent");

    public CelldManifest {
        Objects.requireNonNull(application, "application");
        Objects.requireNonNull(baseUrl, "baseUrl");
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(connectorName, "connectorName");
        Objects.requireNonNull(space, "space");
        Objects.requireNonNull(credential, "credential");
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        Objects.requireNonNull(freshness, "freshness");
        routes = List.copyOf(Objects.requireNonNull(routes, "routes"));
    }

    /** The HTTP mode of an operation: a {@code GET} is a read operation, a {@code POST} an invoke operation. */
    public enum Method {
        GET, POST
    }

    /**
     * One allowlisted route.
     *
     * @param name       the operation name agents use ({@code op} on a query, {@code operation} on an invocation)
     * @param method     {@link Method#GET} (read) or {@link Method#POST} (invoke)
     * @param path       the route's path under the base URL
     * @param params     the declared parameter names; any other parameter is refused
     * @param idempotent whether the route deduplicates on {@code Aspace-Invocation-Id}
     */
    public record Operation(String name, Method method, String path, List<String> params, boolean idempotent) {

        public Operation {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(method, "method");
            Objects.requireNonNull(path, "path");
            params = List.copyOf(Objects.requireNonNull(params, "params"));
        }

        /** Whether a read operation. */
        public boolean isRead() {
            return method == Method.GET;
        }

        /** Whether an invoke operation. */
        public boolean isInvoke() {
            return method == Method.POST;
        }
    }

    /**
     * One route group, one {@code workers://<application>/route/<name>} card.
     *
     * @param name        the group name; the asset is {@code route/<name>}
     * @param description the card's description
     * @param shape       the declared shape of the results
     * @param freshness   this card's refresh cadence, or {@code null} for the manifest default
     * @param operations  the allowlisted operations
     */
    public record RouteGroup(String name, String description, String shape, Duration freshness,
                             List<Operation> operations) {

        public RouteGroup {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(description, "description");
            Objects.requireNonNull(shape, "shape");
            operations = List.copyOf(Objects.requireNonNull(operations, "operations"));
        }

        /** The asset name on the card. */
        public String asset() {
            return "route/" + name;
        }

        /** The operation by name, or {@code null}. */
        public Operation operation(String operationName) {
            for (Operation operation : operations) {
                if (operation.name().equals(operationName)) {
                    return operation;
                }
            }
            return null;
        }

        /** The read operations, in manifest order. */
        public List<Operation> readOperations() {
            return operations.stream().filter(Operation::isRead).toList();
        }

        /** The invoke operations, in manifest order. */
        public List<Operation> invokeOperations() {
            return operations.stream().filter(Operation::isInvoke).toList();
        }
    }

    /**
     * The operator's credential: a bearer token or an mTLS key store, with an
     * optional trust store either way.
     *
     * @param bearer             the bearer token, or {@code null}
     * @param keyStore           the client key store (mTLS), or {@code null}
     * @param keyStorePassword   its password
     * @param keyStoreType       its type ({@code PKCS12} by default)
     * @param trustStore         the trust store the deployment's certificate chains to, or {@code null} for the JDK's
     * @param trustStorePassword its password
     * @param trustStoreType     its type ({@code PKCS12} by default)
     */
    public record Credential(String bearer, Path keyStore, char[] keyStorePassword, String keyStoreType,
                             Path trustStore, char[] trustStorePassword, String trustStoreType) {

        /** A bearer token. */
        public static Credential bearer(String token) {
            return new Credential(Objects.requireNonNull(token, "token"), null, null, null, null, null, null);
        }

        /** No credential at all (a deployment reachable only on a private network). */
        public static Credential none() {
            return new Credential(null, null, null, null, null, null, null);
        }

        /** Whether a bearer token is configured. */
        public boolean hasBearer() {
            return bearer != null;
        }

        /** Whether a client key store is configured. */
        public boolean hasKeyStore() {
            return keyStore != null;
        }

        /** Whether a trust store is configured. */
        public boolean hasTrustStore() {
            return trustStore != null;
        }

        @Override
        public String toString() {
            return "Credential[bearer=" + (bearer == null ? "none" : "***") + ", keyStore=" + keyStore
                    + ", trustStore=" + trustStore + "]";
        }
    }

    /** The route group by asset name ({@code route/<name>}), or {@code null}. */
    public RouteGroup route(String asset) {
        for (RouteGroup route : routes) {
            if (route.asset().equals(asset)) {
                return route;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ parsing

    /** Parses a manifest file (YAML; a JSON file parses too). */
    public static CelldManifest load(Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            return load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
    }

    /** Parses a manifest document from a stream. */
    @SuppressWarnings("unchecked")
    public static CelldManifest load(InputStream in) {
        try {
            byte[] bytes = in.readAllBytes();
            if (new String(bytes, StandardCharsets.UTF_8).isBlank()) {
                throw new IllegalArgumentException("the manifest is empty");
            }
            Object document = new ObjectMapper(new YAMLFactory()).readValue(bytes, Object.class);
            if (document == null) {
                throw new IllegalArgumentException("the manifest is empty");
            }
            if (!(document instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("the manifest must be a map with a '" + KEY + "' key");
            }
            return fromDocument((Map<String, Object>) map);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot parse the manifest", e);
        }
    }

    /** Parses a manifest from YAML text. */
    public static CelldManifest parse(String yaml) {
        return load(new java.io.ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Binds a parsed document: the {@code celld} subtree when the document
     * has one (a shared fleet file), else the document itself.
     */
    @SuppressWarnings("unchecked")
    public static CelldManifest fromDocument(Map<String, Object> document) {
        Object sub = document.get(KEY);
        Map<String, Object> tree = sub instanceof Map<?, ?> m ? (Map<String, Object>) m : document;
        if (sub == null && !document.containsKey("routes")) {
            throw new IllegalArgumentException("the manifest has no '" + KEY + "' key");
        }
        return fromTree(tree);
    }

    /** Binds the {@code celld} subtree. */
    public static CelldManifest fromTree(Map<String, Object> tree) {
        rejectUnknown(tree, TOP_KEYS, KEY);
        String application = required(tree, "application", KEY);
        if (!application.matches("[A-Za-z0-9][A-Za-z0-9._-]*")) {
            throw new IllegalArgumentException(KEY + ".application '" + application
                    + "' must be a name of letters, digits, '.', '_', and '-'");
        }
        boolean allowPlaintext = bool(tree, "allow-plaintext", false);
        URI baseUrl = baseUrl(required(tree, "base-url", KEY), allowPlaintext);
        String runtime = string(tree, "runtime", "celld").toLowerCase(Locale.ROOT);
        if (!RUNTIMES.contains(runtime)) {
            throw new IllegalArgumentException(KEY + ".runtime '" + runtime + "' must be one of " + RUNTIMES);
        }
        Credential credential = credential(tree.get("credential"));
        Map<String, Object> timeouts = subtree(tree, "timeouts");
        rejectUnknown(timeouts, TIMEOUT_KEYS, KEY + ".timeouts");
        Duration connect = duration(timeouts, "connect", Duration.ofSeconds(5), KEY + ".timeouts");
        Duration request = duration(timeouts, "request", Duration.ofSeconds(30), KEY + ".timeouts");
        long maxBody = longValue(tree, "max-body-bytes", 1L << 20);
        if (maxBody <= 0) {
            throw new IllegalArgumentException(KEY + ".max-body-bytes must be positive");
        }
        Duration freshness = duration(tree, "freshness", Duration.ofMinutes(1), KEY);
        Object routesValue = tree.get("routes");
        if (!(routesValue instanceof List<?> routeList) || routeList.isEmpty()) {
            throw new IllegalArgumentException(KEY + ".routes must list at least one route group");
        }
        List<RouteGroup> routes = new ArrayList<>();
        Set<String> names = new LinkedHashSet<>();
        for (Object item : routeList) {
            RouteGroup route = route(item);
            if (!names.add(route.name())) {
                throw new IllegalArgumentException(KEY + ".routes names '" + route.name() + "' twice");
            }
            routes.add(route);
        }
        return new CelldManifest(application, baseUrl, runtime,
                string(tree, "connector-name", "celld-" + application), string(tree, "group", null),
                string(tree, "space", "data"), credential, connect, request, maxBody, freshness,
                allowPlaintext, bool(tree, "fleet-authorizer", true), routes);
    }

    private static URI baseUrl(String text, boolean allowPlaintext) {
        URI uri;
        try {
            uri = URI.create(text);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(KEY + ".base-url '" + text + "' is not a URL", e);
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("http") && !allowPlaintext) {
            throw new IllegalArgumentException(KEY + ".base-url '" + text
                    + "' is plaintext http; use https, or set allow-plaintext: true for a private network");
        }
        if (!scheme.equals("https") && !scheme.equals("http")) {
            throw new IllegalArgumentException(KEY + ".base-url '" + text + "' must be an https URL");
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException(KEY + ".base-url '" + text + "' has no host");
        }
        if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new IllegalArgumentException(KEY + ".base-url '" + text + "' must carry no query or fragment");
        }
        String path = uri.getRawPath() == null ? "" : uri.getRawPath();
        while (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return URI.create(scheme + "://" + uri.getRawAuthority() + path);
    }

    @SuppressWarnings("unchecked")
    private static Credential credential(Object value) {
        if (value == null) {
            throw new IllegalArgumentException(KEY + ".credential is required: a bearer token (bearer, bearer-env,"
                    + " bearer-file) or an mtls key-store");
        }
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException(KEY + ".credential must be a map");
        }
        Map<String, Object> tree = (Map<String, Object>) raw;
        rejectUnknown(tree, CREDENTIAL_KEYS, KEY + ".credential");
        int bearers = (tree.containsKey("bearer") ? 1 : 0) + (tree.containsKey("bearer-env") ? 1 : 0)
                + (tree.containsKey("bearer-file") ? 1 : 0);
        if (bearers > 1) {
            throw new IllegalArgumentException(KEY + ".credential names more than one of bearer, bearer-env,"
                    + " bearer-file");
        }
        String bearer = null;
        if (tree.containsKey("bearer")) {
            bearer = required(tree, "bearer", KEY + ".credential");
        } else if (tree.containsKey("bearer-env")) {
            String variable = required(tree, "bearer-env", KEY + ".credential");
            bearer = System.getenv(variable);
            if (bearer == null || bearer.isBlank()) {
                throw new IllegalArgumentException(KEY + ".credential.bearer-env names '" + variable
                        + "', which is not set");
            }
            bearer = bearer.strip();
        } else if (tree.containsKey("bearer-file")) {
            Path file = Path.of(required(tree, "bearer-file", KEY + ".credential"));
            try {
                bearer = Files.readString(file).strip();
            } catch (IOException e) {
                throw new UncheckedIOException(KEY + ".credential.bearer-file " + file + " cannot be read", e);
            }
            if (bearer.isEmpty()) {
                throw new IllegalArgumentException(KEY + ".credential.bearer-file " + file + " is empty");
            }
        }
        Path keyStore = null;
        char[] keyStorePassword = null;
        String keyStoreType = null;
        Path trustStore = null;
        char[] trustStorePassword = null;
        String trustStoreType = null;
        if (tree.containsKey("mtls")) {
            if (!(tree.get("mtls") instanceof Map<?, ?> rawTls)) {
                throw new IllegalArgumentException(KEY + ".credential.mtls must be a map");
            }
            Map<String, Object> tls = (Map<String, Object>) rawTls;
            rejectUnknown(tls, MTLS_KEYS, KEY + ".credential.mtls");
            if (tls.containsKey("key-store")) {
                keyStore = Path.of(required(tls, "key-store", KEY + ".credential.mtls"));
                keyStorePassword = string(tls, "key-store-password", "").toCharArray();
                keyStoreType = string(tls, "key-store-type", "PKCS12");
            }
            if (tls.containsKey("trust-store")) {
                trustStore = Path.of(required(tls, "trust-store", KEY + ".credential.mtls"));
                trustStorePassword = string(tls, "trust-store-password", "").toCharArray();
                trustStoreType = string(tls, "trust-store-type", "PKCS12");
            }
            if (keyStore == null && trustStore == null) {
                throw new IllegalArgumentException(KEY + ".credential.mtls names neither key-store nor trust-store");
            }
        }
        if (bearer == null && keyStore == null) {
            throw new IllegalArgumentException(KEY + ".credential must name a bearer token (bearer, bearer-env,"
                    + " bearer-file) or an mtls key-store");
        }
        return new Credential(bearer, keyStore, keyStorePassword, keyStoreType, trustStore, trustStorePassword,
                trustStoreType);
    }

    @SuppressWarnings("unchecked")
    private static RouteGroup route(Object item) {
        if (!(item instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException(KEY + ".routes entries must be maps");
        }
        Map<String, Object> tree = (Map<String, Object>) raw;
        String where = KEY + ".routes[" + tree.get("name") + "]";
        rejectUnknown(tree, ROUTE_KEYS, where);
        String name = required(tree, "name", where);
        if (!name.matches("[A-Za-z0-9][A-Za-z0-9._-]*")) {
            throw new IllegalArgumentException(where + ".name '" + name
                    + "' must be a name of letters, digits, '.', '_', and '-'");
        }
        Object operationsValue = tree.get("operations");
        if (!(operationsValue instanceof List<?> operationList) || operationList.isEmpty()) {
            throw new IllegalArgumentException(where + ".operations must list at least one operation");
        }
        List<Operation> operations = new ArrayList<>();
        Set<String> names = new LinkedHashSet<>();
        for (Object operationItem : operationList) {
            Operation operation = operation(operationItem, where);
            if (!names.add(operation.name())) {
                throw new IllegalArgumentException(where + ".operations names '" + operation.name() + "' twice");
            }
            operations.add(operation);
        }
        return new RouteGroup(name, string(tree, "description", "Route group " + name + " of the deployment"),
                string(tree, "shape", "json"), duration(tree, "freshness", null, where), operations);
    }

    @SuppressWarnings("unchecked")
    private static Operation operation(Object item, String route) {
        if (!(item instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException(route + ".operations entries must be maps");
        }
        Map<String, Object> tree = (Map<String, Object>) raw;
        String where = route + ".operations[" + tree.get("name") + "]";
        rejectUnknown(tree, OPERATION_KEYS, where);
        String name = required(tree, "name", where);
        if (name.equals(OP_PARAMETER) || !name.matches("[A-Za-z0-9][A-Za-z0-9._-]*")) {
            throw new IllegalArgumentException(where + ".name '" + name
                    + "' must be a name of letters, digits, '.', '_', and '-' other than '" + OP_PARAMETER + "'");
        }
        String methodText = required(tree, "method", where).toUpperCase(Locale.ROOT);
        Method method;
        try {
            method = Method.valueOf(methodText);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(where + ".method '" + methodText
                    + "' must be GET (a read operation) or POST (an invoke operation)");
        }
        String path = required(tree, "path", where);
        if (!path.startsWith("/") || path.contains("?") || path.contains("#")) {
            throw new IllegalArgumentException(where + ".path '" + path
                    + "' must start with '/' and carry no query or fragment");
        }
        List<String> params = new ArrayList<>();
        Object paramsValue = tree.get("params");
        if (paramsValue != null) {
            if (!(paramsValue instanceof List<?> list)) {
                throw new IllegalArgumentException(where + ".params must be a list of names");
            }
            for (Object p : list) {
                String param = String.valueOf(p);
                if (param.isBlank() || param.equals(OP_PARAMETER)) {
                    throw new IllegalArgumentException(where + ".params may not name '" + param + "'");
                }
                if (params.contains(param)) {
                    throw new IllegalArgumentException(where + ".params names '" + param + "' twice");
                }
                params.add(param);
            }
        }
        boolean idempotent = bool(tree, "idempotent", true);
        if (method == Method.GET && tree.containsKey("idempotent") && !idempotent) {
            throw new IllegalArgumentException(where + " is a GET route, which is a read operation and always"
                    + " idempotent");
        }
        return new Operation(name, method, path, params, idempotent);
    }

    // ------------------------------------------------------------------ helpers

    private static void rejectUnknown(Map<String, Object> tree, Set<String> known, String where) {
        List<String> unknown = new ArrayList<>();
        for (String key : tree.keySet()) {
            if (!known.contains(key)) {
                unknown.add(key);
            }
        }
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException(where + " has unknown keys " + unknown + "; known keys are "
                    + new java.util.TreeSet<>(known));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> subtree(Map<String, Object> tree, String key) {
        Object value = tree.get(key);
        if (value == null) {
            return new LinkedHashMap<>();
        }
        if (!(value instanceof Map<?, ?> m)) {
            throw new IllegalArgumentException(KEY + "." + key + " must be a map");
        }
        return (Map<String, Object>) m;
    }

    private static String required(Map<String, Object> tree, String key, String where) {
        Object value = tree.get(key);
        if (value == null || String.valueOf(value).isBlank()) {
            throw new IllegalArgumentException(where + "." + key + " is required");
        }
        return String.valueOf(value).strip();
    }

    private static String string(Map<String, Object> tree, String key, String fallback) {
        Object value = tree.get(key);
        return value == null ? fallback : String.valueOf(value).strip();
    }

    private static boolean bool(Map<String, Object> tree, String key, boolean fallback) {
        Object value = tree.get(key);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        String text = String.valueOf(value).strip().toLowerCase(Locale.ROOT);
        if (text.equals("true") || text.equals("false")) {
            return Boolean.parseBoolean(text);
        }
        throw new IllegalArgumentException(KEY + "." + key + " must be true or false");
    }

    private static long longValue(Map<String, Object> tree, String key, long fallback) {
        Object value = tree.get(key);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).strip());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(KEY + "." + key + " must be a whole number of bytes");
        }
    }

    private static Duration duration(Map<String, Object> tree, String key, Duration fallback, String where) {
        Object value = tree.get(key);
        if (value == null) {
            return fallback;
        }
        return parseDuration(String.valueOf(value).strip(), where + "." + key);
    }

    /**
     * A duration in ISO-8601 ({@code PT5M}) or the short form the fleet files
     * use ({@code 5s}, {@code 250ms}, {@code 2m}, {@code 1h}, {@code 1d}).
     */
    static Duration parseDuration(String text, String where) {
        String t = text.strip();
        try {
            if (t.toUpperCase(Locale.ROOT).startsWith("P")) {
                return Duration.parse(t);
            }
            String lower = t.toLowerCase(Locale.ROOT);
            if (lower.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(lower.substring(0, lower.length() - 2).strip()));
            }
            char unit = lower.charAt(lower.length() - 1);
            long amount = Long.parseLong(lower.substring(0, lower.length() - 1).strip());
            return switch (unit) {
                case 's' -> Duration.ofSeconds(amount);
                case 'm' -> Duration.ofMinutes(amount);
                case 'h' -> Duration.ofHours(amount);
                case 'd' -> Duration.ofDays(amount);
                default -> throw new IllegalArgumentException("unit");
            };
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(where + " '" + text + "' is not a duration (PT5M, 5s, 250ms, 2m,"
                    + " 1h, 1d)");
        }
    }
}
