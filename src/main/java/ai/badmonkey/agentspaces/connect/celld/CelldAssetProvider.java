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
import ai.badmonkey.agentspaces.connect.MaterializingAssetProvider;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The {@link AssetProvider} over a manifest of allowlisted Worker routes: the
 * same mapping {@code @agentspaces/workers} runs inside a deployment, run
 * beside it over HTTPS, so a team serves an existing deployment to the fleet
 * without changing its code or bindings.
 *
 * <ul>
 *   <li>{@link #describeAssets} publishes one {@code workers://<application>/route/<name>}
 *       card per route group, id {@code <application>:route/<name>} so a refresh
 *       replaces the earlier card (JS-C-08), with {@code access.read-ops} (the GET
 *       routes), {@code invoke-ops} (the POST routes), {@code idempotent} (the
 *       invoke operations whose route deduplicates on the invocation id),
 *       {@code params-<op>} (the declared names), {@code runtime}, and
 *       {@code connector = java}.</li>
 *   <li>{@link #query} runs the read operation named by the {@code op} parameter
 *       (or the group's only read operation) as a GET with the declared parameters
 *       as query parameters; the JSON body becomes the result rows.</li>
 *   <li>{@link #invoke} runs the invoke operation as a POST with the parameters as
 *       the JSON body and {@code Aspace-Invocation-Id}; the JSON body becomes the
 *       result map.</li>
 * </ul>
 *
 * <p>An operation with {@code result: artifact} (or {@code auto}, above the inline
 * bound) answers one artifact row through {@link ArtifactResults}; an operation with
 * an artifact body sends the artifact the requester names through
 * {@link ArtifactUploads}. Both need the group's content swarm. Changes the
 * {@link ChangeWatcher} reads become {@code AssetChange} entries through
 * {@link #materialize(Consumer)}.
 *
 * <p>A route group or operation the manifest does not list, or a parameter an
 * operation does not declare, is refused with an error naming the rule and the
 * deployment is never called (JS-C-07). The {@code ConnectorRuntime} applies
 * the card's {@code invoke-ops} and the fleet's scopes before this provider
 * sees an invocation; the checks here hold even for a caller that bypasses it.
 */
public final class CelldAssetProvider implements MaterializingAssetProvider {

    /** The {@code access} key listing the read operations as a comma list. */
    public static final String READ_OPS = "read-ops";
    /** The {@code access} key listing the invoke operations whose route deduplicates on the invocation id. */
    public static final String IDEMPOTENT = "idempotent";
    /** The prefix of the {@code access} keys listing each operation's declared parameters. */
    public static final String PARAMS_PREFIX = "params-";
    /** The {@code access.connector} value of this connector. */
    public static final String CONNECTOR = "java";
    /** The {@code access} key listing the operations whose result is an artifact. */
    public static final String ARTIFACT_OPS = "artifact-ops";
    /** The prefix of the {@code access} keys naming an artifact operation's media type. */
    public static final String MEDIA_TYPE_PREFIX = "media-type-";
    /** The {@code access} key listing the snapshot operations. */
    public static final String SNAPSHOT_OPS = "snapshot-ops";
    /** The prefix of the {@code access} keys naming a snapshot's declared consistency. */
    public static final String CONSISTENCY_PREFIX = "consistency-";
    /** The {@code access} key listing the operations that take an artifact body. */
    public static final String UPLOAD_OPS = "upload-ops";
    /** The prefix of the {@code access} keys naming the parameter that carries an artifact's root. */
    public static final String ARTIFACT_PARAM_PREFIX = "artifact-param-";
    /** The {@code access} key saying a change route covers the group. */
    public static final String CHANGES = "changes";

    private final CelldManifest manifest;
    private final CelldClient client;
    private volatile ArtifactResults artifacts;
    private volatile ArtifactUploads uploads;
    private volatile Consumer<Change> sink;

    /** Creates the provider with a client over the manifest's deployment. */
    public CelldAssetProvider(CelldManifest manifest) {
        this(manifest, new CelldClient(manifest));
    }

    /** Creates the provider over a given client. */
    public CelldAssetProvider(CelldManifest manifest, CelldClient client) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.client = Objects.requireNonNull(client, "client");
    }

    /** The manifest this provider serves. */
    public CelldManifest manifest() {
        return manifest;
    }

    /** The HTTP client, whose {@link CelldClient#calls()} counts every call made. */
    public CelldClient client() {
        return client;
    }

    /** Gives the provider the large-result path; without it an artifact operation answers an error. */
    public CelldAssetProvider artifacts(ArtifactResults artifacts) {
        this.artifacts = artifacts;
        return this;
    }

    /** Gives the provider the artifact-body path; without it an upload operation answers an error. */
    public CelldAssetProvider uploads(ArtifactUploads uploads) {
        this.uploads = uploads;
        return this;
    }

    @Override
    public AutoCloseable materialize(Consumer<Change> changes) {
        this.sink = changes;
        return () -> this.sink = null;
    }

    /** Pushes one change to the runtime's change space; a no-op before the runtime starts. */
    void emit(Change change) {
        Consumer<Change> current = sink;
        if (current != null) {
            current.accept(change);
        }
    }

    /**
     * Whether this connector advertises an operation: in active-active mode a non-idempotent
     * operation is advertised only by the connector {@code replicas.non-idempotent-on} names (CC-H-04).
     */
    boolean advertises(CelldManifest.Operation operation) {
        if (!manifest.activeActive() || !operation.isInvoke() || operation.idempotent()) {
            return true;
        }
        return manifest.connectorName().equals(manifest.replicas().nonIdempotentOn());
    }

    @Override
    public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
        List<AssetCard> cards = new ArrayList<>();
        for (CelldManifest.RouteGroup route : manifest.routes()) {
            cards.add(card(route, group, issuer, now));
        }
        return cards;
    }

    /** The card of one route group (appendix F, the route-group row). */
    public AssetCard card(CelldManifest.RouteGroup route, GroupId group, PeerId issuer, Instant now) {
        Duration freshness = route.freshness() != null ? route.freshness() : manifest.freshness();
        List<CelldManifest.Operation> reads = route.readOperations();
        List<CelldManifest.Operation> invokes = route.invokeOperations().stream().filter(this::advertises).toList();
        Map<String, String> access = new LinkedHashMap<>();
        String mode = !reads.isEmpty() && !invokes.isEmpty() ? "read-only,invoke"
                : invokes.isEmpty() ? "read-only" : "invoke";
        access.put("mode", mode);
        if (!reads.isEmpty()) {
            access.put("query", "aspace:cap/data-query");
            access.put(READ_OPS, names(reads));
        }
        if (!invokes.isEmpty()) {
            access.put("invoke", AssetInvocations.CAPABILITY);
            access.put(AssetInvocations.INVOKE_OPS, names(invokes));
            access.put(IDEMPOTENT, names(invokes.stream().filter(CelldManifest.Operation::idempotent).toList()));
        }
        List<CelldManifest.Operation> advertised = route.operations().stream().filter(this::advertises).toList();
        for (CelldManifest.Operation operation : advertised) {
            access.put(PARAMS_PREFIX + operation.name(), String.join(",", operation.params()));
        }
        List<CelldManifest.Operation> artifactOps = advertised.stream()
                .filter(CelldManifest.Operation::mayAnswerWithArtifact).toList();
        if (!artifactOps.isEmpty()) {
            access.put(ARTIFACT_OPS, names(artifactOps));
            artifactOps.forEach(o -> access.put(MEDIA_TYPE_PREFIX + o.name(), o.mediaType()));
        }
        List<CelldManifest.Operation> snapshots = advertised.stream().filter(CelldManifest.Operation::isSnapshot).toList();
        if (!snapshots.isEmpty()) {
            access.put(SNAPSHOT_OPS, names(snapshots));
            snapshots.forEach(o -> access.put(CONSISTENCY_PREFIX + o.name(), o.snapshot().consistency()));
        }
        List<CelldManifest.Operation> uploadOps = advertised.stream()
                .filter(CelldManifest.Operation::takesArtifact).toList();
        if (!uploadOps.isEmpty()) {
            access.put(UPLOAD_OPS, names(uploadOps));
            uploadOps.forEach(o -> access.put(ARTIFACT_PARAM_PREFIX + o.name(), o.artifactParam()));
        }
        if (manifest.changes() != null) {
            access.put(CHANGES, "true");
        }
        access.put("runtime", manifest.runtime());
        access.put("connector", CONNECTOR);
        Map<String, String> costHints = new LinkedHashMap<>();
        costHints.put("operations", Integer.toString(route.operations().size()));
        // The ad cache keys cards by id alone, so each replica's card needs an id of its own (CC-H-01).
        String id = manifest.application() + ":" + route.asset()
                + (manifest.activeActive() ? "@" + manifest.connectorName() : "");
        return new AssetCard(id, issuer, group, now,
                freshness.multipliedBy(2), route.asset(),
                "workers://" + manifest.application() + "/" + route.asset(),
                route.description(), route.shape(), freshness.toString(), costHints, access);
    }

    @Override
    public QueryResult query(String asset, Map<String, String> parameters) {
        CelldManifest.RouteGroup route = manifest.route(asset);
        if (route == null) {
            return QueryResult.failed("routes: no route group named '" + asset + "' in the manifest");
        }
        List<CelldManifest.Operation> reads = route.readOperations();
        String opName = parameters.get(CelldManifest.OP_PARAMETER);
        if (opName == null) {
            if (reads.size() != 1) {
                return QueryResult.failed(READ_OPS + ": the query names no '" + CelldManifest.OP_PARAMETER
                        + "' and asset '" + asset + "' has " + reads.size() + " read operations ("
                        + names(reads) + ")");
            }
            opName = reads.getFirst().name();
        }
        CelldManifest.Operation operation = route.operation(opName);
        if (operation == null || !operation.isRead()) {
            return QueryResult.failed(READ_OPS + ": operation '" + opName + "' is not a GET route of asset '"
                    + asset + "' (" + (reads.isEmpty() ? "none" : names(reads)) + ")");
        }
        Map<String, String> declared = new LinkedHashMap<>(parameters);
        declared.remove(CelldManifest.OP_PARAMETER);
        String undeclared = undeclared(operation, declared);
        if (undeclared != null) {
            return QueryResult.failed(undeclared);
        }
        if (operation.mayAnswerWithArtifact()) {
            ArtifactResults path = artifacts;
            if (path == null) {
                return QueryResult.failed("artifacts: operation '" + opName + "' answers with an artifact, and the"
                        + " fleet's content swarm is off (agentspaces.content.enabled)");
            }
            try {
                return QueryResult.of(path.answer(route, operation, declared, null).rows());
            } catch (CelldClient.RouteException e) {
                return QueryResult.failed(e.rule() + ": " + e.getMessage());
            }
        }
        CelldClient.Response response;
        try {
            response = client.get(operation, declared);
        } catch (CelldClient.RouteException e) {
            return QueryResult.failed(e.rule() + ": " + e.getMessage());
        }
        if (!response.ok()) {
            return QueryResult.failed("http-status: " + response.status() + " from " + operation.method() + " "
                    + operation.path());
        }
        return QueryResult.of(rows(response.body()));
    }

    @Override
    public InvokeResult invoke(String asset, String operationName, Map<String, String> params,
                               String invocationId) {
        return invoke(asset, operationName, params, invocationId, null);
    }

    @Override
    public InvokeResult invoke(String asset, String operationName, Map<String, String> params,
                               String invocationId, PeerId requester) {
        CelldManifest.RouteGroup route = manifest.route(asset);
        if (route == null) {
            return InvokeResult.failed("unknown-operation", "routes", "no route group named '" + asset
                    + "' in the manifest");
        }
        CelldManifest.Operation operation = route.operation(operationName);
        if (operation == null || !operation.isInvoke() || !advertises(operation)) {
            List<CelldManifest.Operation> invokes = route.invokeOperations().stream().filter(this::advertises).toList();
            return InvokeResult.failed("unknown-operation", AssetInvocations.INVOKE_OPS, "operation '"
                    + operationName + "' is not a POST route of asset '" + asset + "' ("
                    + (invokes.isEmpty() ? "none" : names(invokes)) + ")");
        }
        String undeclared = undeclared(operation, params);
        if (undeclared != null) {
            return InvokeResult.failed("bad-params", "params", undeclared.substring("params: ".length()));
        }
        if (operation.takesArtifact()) {
            ArtifactUploads path = uploads;
            if (path == null) {
                return InvokeResult.failed("target-error", "artifacts", "operation '" + operationName
                        + "' takes an artifact body, and the fleet's content swarm is off (agentspaces.content.enabled)");
            }
            return path.upload(operation, params, invocationId, requester);
        }
        if (operation.mayAnswerWithArtifact()) {
            ArtifactResults path = artifacts;
            if (path == null) {
                return InvokeResult.failed("target-error", "artifacts", "operation '" + operationName
                        + "' answers with an artifact, and the fleet's content swarm is off (agentspaces.content.enabled)");
            }
            try {
                List<Map<String, String>> rows = path.answer(route, operation, params, invocationId).rows();
                return InvokeResult.of(rows.isEmpty() ? Map.of() : rows.getFirst());
            } catch (CelldClient.RouteException e) {
                return InvokeResult.failed(e.error(), e.rule(), e.getMessage());
            }
        }
        CelldClient.Response response;
        try {
            response = client.post(operation, params, invocationId);
        } catch (CelldClient.RouteException e) {
            return InvokeResult.failed(e.error(), e.rule(), e.getMessage());
        }
        if (!response.ok()) {
            return InvokeResult.failed("target-error", "http-status", response.status() + " from "
                    + operation.method() + " " + operation.path());
        }
        return InvokeResult.of(result(response.body()));
    }

    // ------------------------------------------------------------------ mapping

    /** The refusal message for a parameter the operation does not declare, or {@code null}. */
    private static String undeclared(CelldManifest.Operation operation, Map<String, String> params) {
        for (String name : params.keySet()) {
            if (!operation.params().contains(name)) {
                return "params: parameter '" + name + "' is not declared for operation '" + operation.name()
                        + "' (declared: " + (operation.params().isEmpty() ? "none"
                        : String.join(",", operation.params())) + ")";
            }
        }
        return null;
    }

    /**
     * A JSON body as result rows: an array of objects is one row per object,
     * an object is one row, a scalar is one row under {@code value}, an empty
     * body is no row. Nested values stay JSON text.
     */
    static List<Map<String, String>> rows(JsonNode body) {
        List<Map<String, String>> rows = new ArrayList<>();
        if (body == null || body.isNull()) {
            return rows;
        }
        if (body.isArray()) {
            for (JsonNode element : body) {
                rows.add(row(element));
            }
            return rows;
        }
        rows.add(row(body));
        return rows;
    }

    /** A JSON body as an invocation result: an object's fields, else the value under {@code result}. */
    static Map<String, String> result(JsonNode body) {
        if (body == null || body.isNull()) {
            return Map.of();
        }
        if (body.isObject()) {
            return row(body);
        }
        return Map.of("result", text(body));
    }

    private static Map<String, String> row(JsonNode node) {
        Map<String, String> row = new LinkedHashMap<>();
        if (node.isObject()) {
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> field = it.next();
                if (!field.getValue().isNull()) {
                    row.put(field.getKey(), text(field.getValue()));
                }
            }
        } else {
            row.put("value", text(node));
        }
        return row;
    }

    private static String text(JsonNode node) {
        return node.isValueNode() ? node.asText() : node.toString();
    }

    private static String names(List<CelldManifest.Operation> operations) {
        return String.join(",", operations.stream().map(CelldManifest.Operation::name).toList());
    }
}
