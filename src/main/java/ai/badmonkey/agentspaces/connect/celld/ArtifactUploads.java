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
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetProvider.InvokeResult;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.ContentRoots;
import ai.badmonkey.agentspaces.content.Fetch;
import ai.badmonkey.agentspaces.content.FetchOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Artifact inputs (CC-I): an invoke operation whose body is an artifact the
 * requester names by root. The connector checks the requester may fetch the
 * root, refuses an artifact over {@code max-upload-bytes} before fetching,
 * fetches it from every holder, and streams the plaintext to the route with its
 * length, the invocation id, and the root, either whole or piece by piece. The
 * root stays pinned while it travels.
 */
public final class ArtifactUploads {

    private final CelldManifest manifest;
    private final CelldClient client;
    private final Content content;
    private volatile Authorizer authorizer;
    private final AtomicLong uploads = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();

    /**
     * @param manifest the manifest
     * @param client   the client over the deployment
     * @param content  the group's content swarm
     */
    public ArtifactUploads(CelldManifest manifest, CelldClient client, Content content) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.client = Objects.requireNonNull(client, "client");
        this.content = Objects.requireNonNull(content, "content");
    }

    /** Gives the uploads the fleet's authorizer, which then judges {@code CONTENT_FETCH} per requester. */
    public ArtifactUploads authorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
        return this;
    }

    /** How many artifacts this connector sent to routes. */
    public long uploads() {
        return uploads.get();
    }

    /** How many artifact bytes it sent. */
    public long bytesSent() {
        return bytesSent.get();
    }

    /**
     * Runs an operation whose body is an artifact.
     *
     * @param operation    the POST operation, {@code body: artifact} or {@code artifact-chunked}
     * @param params       the declared parameters, the artifact parameter among them
     * @param invocationId the invocation id
     * @param requester    the requesting peer, or {@code null} when unknown
     * @return the route's answer as an invocation result, or the refusal naming the rule
     */
    public InvokeResult upload(CelldManifest.Operation operation, Map<String, String> params, String invocationId,
                               PeerId requester) {
        String root = params.get(operation.artifactParam());
        if (root == null || !ContentRoots.isRoot(root)) {
            return InvokeResult.failed("bad-params", "artifact-param", "parameter '" + operation.artifactParam()
                    + "' must carry a content root (multibase b), got '" + root + "'");
        }
        Authorizer gate = authorizer;
        if (gate != null && (requester == null
                || !gate.permits(requester, Authorizer.Operation.CONTENT_FETCH, root))) {
            return InvokeResult.failed("refused-scope", "content-fetch", "requester "
                    + (requester == null ? "unknown" : requester.value()) + " may not fetch " + root
                    + ", so the connector will not send it");
        }
        Optional<Manifest> known = content.describe(root).map(Content.Description::manifest);
        if (known.isPresent() && known.get().size() > operation.maxUploadBytes()) {
            return tooLarge(operation, known.get().size());
        }
        Fetch fetch;
        try {
            fetch = content.fetch(root, FetchOptions.defaults().withTimeout(manifest.fetchTimeout()));
        } catch (RuntimeException e) {
            return InvokeResult.failed("target-error", "content-fetch", "cannot fetch " + root + ": " + e.getMessage());
        }
        if (!await(fetch, manifest.fetchTimeout())) {
            fetch.cancel();
            return InvokeResult.failed("timeout", "timeouts.fetch", root + " was not fetched within "
                    + manifest.fetchTimeout() + fetch.failure().map(f -> ": " + f.getMessage()).orElse(""));
        }
        Manifest artifact = fetch.manifest().orElseThrow();
        if (artifact.size() > operation.maxUploadBytes()) {
            return tooLarge(operation, artifact.size());
        }
        Map<String, String> query = new LinkedHashMap<>(params);
        query.remove(operation.artifactParam());
        content.pin(root);
        try {
            CelldClient.Response response = operation.body() == CelldManifest.Body.ARTIFACT_CHUNKED
                    ? chunked(operation, query, artifact, invocationId)
                    : client.upload(operation, query, () -> open(root), artifact.size(),
                    artifact.mediaType(), invocationId, root);
            if (response.status() == 413) {
                return InvokeResult.failed("target-error", "max-upload-bytes", "the route refused " + artifact.size()
                        + " bytes with 413; the deployment's body bound is lower");
            }
            if (!response.ok()) {
                return InvokeResult.failed("target-error", "http-status", response.status() + " from "
                        + operation.method() + " " + operation.path());
            }
            uploads.incrementAndGet();
            bytesSent.addAndGet(artifact.size());
            return InvokeResult.of(CelldAssetProvider.result(response.body()));
        } catch (CelldClient.RouteException e) {
            return InvokeResult.failed(e.error(), e.rule(), e.getMessage());
        } catch (UncheckedIOException e) {
            return InvokeResult.failed("target-error", "content", root + " could not be read: " + e.getMessage());
        } finally {
            content.unpin(root);
        }
    }

    private CelldClient.Response chunked(CelldManifest.Operation operation, Map<String, String> query,
                                         Manifest artifact, String invocationId) {
        try (SeekableByteChannel channel = content.openPlain(artifact.root())) {
            ByteBuffer buffer = ByteBuffer.allocate(artifact.pieceSize());
            for (int i = 0; i < artifact.pieces(); i++) {
                buffer.clear();
                buffer.limit(artifact.pieceLength(i));
                channel.position((long) i * artifact.pieceSize());
                while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                    // fill the piece
                }
                byte[] piece = java.util.Arrays.copyOf(buffer.array(), buffer.position());
                CelldClient.Response step = client.uploadPiece(operation, query, piece, i, artifact.pieces(),
                        invocationId, artifact.root());
                if (!step.ok()) {
                    return step;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return client.uploadPiece(operation, query, null, 0, artifact.pieces(), invocationId, artifact.root());
    }

    private InputStream open(String root) {
        try {
            return Channels.newInputStream(content.openPlain(root));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static InvokeResult tooLarge(CelldManifest.Operation operation, long size) {
        return InvokeResult.failed("target-error", "max-upload-bytes", "the artifact is " + size
                + " bytes, over the operation's max-upload-bytes " + operation.maxUploadBytes());
    }

    private static boolean await(Fetch fetch, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!fetch.isDoneOrFailed()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return fetch.complete();
    }
}
