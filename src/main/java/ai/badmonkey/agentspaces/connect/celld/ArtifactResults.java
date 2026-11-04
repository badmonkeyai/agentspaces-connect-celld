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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.connect.ArtifactRows;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.PublishOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Large results (CC-B) and snapshots (CC-D): streams a route's body into the
 * content swarm and answers with one artifact row ({@link ArtifactRows}). The
 * body is counted as it is read and never held in memory whole; a body cut by
 * a bound or a failure publishes nothing, because {@link Content#publish(InputStream, PublishOptions)}
 * removes its spool on any failure. Each operation keeps its newest roots
 * pinned ({@code keep} for a snapshot, one otherwise); older roots become
 * evictable under the store quota.
 */
public final class ArtifactResults {

    /** The attribute naming the route group a snapshot copies. */
    public static final String SNAPSHOT_OF = "snapshot-of";
    /** The attribute carrying the instant the connector finished reading. */
    public static final String TAKEN = "taken";

    private static final ObjectMapper JSON = new ObjectMapper();

    /** An answer: inline rows, or one artifact row with its manifest. */
    public record Answer(List<Map<String, String>> rows, Manifest manifest) {

        /** Whether the answer is an artifact. */
        public boolean artifact() {
            return manifest != null;
        }
    }

    private final CelldManifest manifest;
    private final CelldClient client;
    private final Content content;
    private final InstantSource clock;
    private final Map<String, Deque<String>> pinnedByOperation = new ConcurrentHashMap<>();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();

    /**
     * @param manifest the manifest
     * @param client   the client over the deployment
     * @param content  the group's content swarm
     * @param clock    the clock stamping {@code taken}
     */
    public ArtifactResults(CelldManifest manifest, CelldClient client, Content content, InstantSource clock) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.client = Objects.requireNonNull(client, "client");
        this.content = Objects.requireNonNull(content, "content");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** How many artifacts this connector published from route bodies. */
    public long published() {
        return published.get();
    }

    /** How many body bytes it read for them. */
    public long bytesRead() {
        return bytesRead.get();
    }

    /**
     * Runs an artifact operation and answers it.
     *
     * @param route        the route group
     * @param operation    the operation, whose result is {@code artifact} or {@code auto}
     * @param params       the declared parameters
     * @param invocationId the invocation id for a POST, or {@code null} for a GET
     * @return the answer
     * @throws CelldClient.RouteException naming the bound or the status that failed it
     */
    public Answer answer(CelldManifest.RouteGroup route, CelldManifest.Operation operation, Map<String, String> params,
                         String invocationId) {
        String where = operation.method() + " " + operation.path();
        try (CelldClient.Streamed streamed = client.open(operation, params, invocationId)) {
            if (!streamed.ok()) {
                throw new CelldClient.RouteException("target-error", "http-status", streamed.status() + " from "
                        + where, null);
            }
            InputStream body = streamed.body();
            if (operation.result() == CelldManifest.Result.AUTO) {
                long inlineMax = manifest.artifacts().inlineMax();
                byte[] head = body.readNBytes((int) Math.min(inlineMax + 1, Integer.MAX_VALUE - 8));
                if (head.length <= inlineMax) {
                    bytesRead.addAndGet(head.length);
                    JsonNode json = head.length == 0 ? null : parse(head, where);
                    return new Answer(CelldAssetProvider.rows(json), null);
                }
                body = new SequenceInputStream(new ByteArrayInputStream(head), body);
            }
            Instant started = clock.instant();
            PublishOptions options = options(route, operation, started);
            Manifest published = content.publish(body, options);
            this.published.incrementAndGet();
            bytesRead.addAndGet(published.size());
            keepPinned(route, operation, published.root());
            ArtifactRows.Artifact artifact = new ArtifactRows.Artifact(published.root(), published.pieces(),
                    published.size(), published.mediaType(), published.name(), options.attrs().get(TAKEN));
            return new Answer(List.of(artifact.toRow()), published);
        } catch (BoundedBody.Exceeded e) {
            throw e.route();
        } catch (IOException e) {
            if (e.getCause() instanceof CelldClient.RouteException cause) {
                throw cause;
            }
            throw new CelldClient.RouteException("target-error", "route", where + " body failed: " + e, e);
        }
    }

    private PublishOptions options(CelldManifest.RouteGroup route, CelldManifest.Operation operation, Instant now) {
        PublishOptions options = content.publishOptions()
                .withLease(Lease.of(manifest.artifacts().lease()))
                .withReplicate(manifest.artifacts().replicate())
                .withName(manifest.application() + "/" + route.name() + "/" + operation.name())
                .withMediaType(operation.mediaType());
        if (manifest.artifacts().pieceSize() > 0) {
            options = options.withPieceSize(manifest.artifacts().pieceSize());
        }
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("celld-application", manifest.application());
        attrs.put("celld-route", route.name());
        attrs.put("celld-operation", operation.name());
        attrs.put(TAKEN, now.toString());
        if (operation.isSnapshot()) {
            attrs.put(SNAPSHOT_OF, manifest.application() + ":" + route.asset());
            attrs.put("consistency", operation.snapshot().consistency());
        }
        return options.withAttrs(attrs);
    }

    /** Keeps an operation's newest roots pinned and unpins the rest (CC-D-04). */
    private void keepPinned(CelldManifest.RouteGroup route, CelldManifest.Operation operation, String root) {
        int keep = operation.isSnapshot() ? operation.snapshot().keep() : 1;
        Deque<String> pinned = pinnedByOperation.computeIfAbsent(route.name() + "/" + operation.name(),
                k -> new ArrayDeque<>());
        synchronized (pinned) {
            pinned.remove(root);
            pinned.addFirst(root);
            while (pinned.size() > keep) {
                content.unpin(pinned.removeLast());
            }
        }
    }

    /** The roots an operation keeps pinned, newest first. */
    public List<String> pinned(String routeName, String operationName) {
        Deque<String> pinned = pinnedByOperation.get(routeName + "/" + operationName);
        if (pinned == null) {
            return List.of();
        }
        synchronized (pinned) {
            return List.copyOf(pinned);
        }
    }

    private static JsonNode parse(byte[] bytes, String where) {
        try {
            return JSON.readTree(bytes);
        } catch (IOException e) {
            throw new CelldClient.RouteException("target-error", "json", where + " answered a body that is not JSON: "
                    + e.getMessage(), e);
        }
    }
}
