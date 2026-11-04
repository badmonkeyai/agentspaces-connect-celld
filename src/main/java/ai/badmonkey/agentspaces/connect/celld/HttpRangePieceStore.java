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
import ai.badmonkey.agentspaces.content.ExternalStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.BitSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A read-only content store over a deployment's objects (CC-R, kind
 * {@code http-range}): each registered root is an object the deployment serves
 * by ranged GET, and every read sends one {@code GET <get>?key=<key>} with
 * {@code Range} and {@code If-Match: <etag>}. Nothing is copied; the swarm
 * hashes each piece whole at its first serve, because the store is
 * {@linkplain #external() external}. A read that the deployment cannot answer
 * for the hashed version (412, 416, a 200 to a range, another range) forgets
 * the root, and the {@code onForget} hook tells the object source to hash the
 * object again.
 */
public final class HttpRangePieceStore implements ExternalStore {

    /** The backend kind. */
    public static final String KIND = "http-range";

    /** One registered object. */
    record Held(Manifest manifest, String key, String etag) {
    }

    private final CelldClient client;
    private final CelldManifest.ObjectSource source;
    private final Map<String, Held> byRoot = new ConcurrentHashMap<>();
    private final Map<String, String> pendingEtags = new ConcurrentHashMap<>();
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private volatile Consumer<Held> onForget = held -> { };

    /**
     * @param client the client over the deployment
     * @param source the object source this store serves
     */
    public HttpRangePieceStore(CelldClient client, CelldManifest.ObjectSource source) {
        this.client = client;
        this.source = source;
    }

    /** The object source. */
    public CelldManifest.ObjectSource source() {
        return source;
    }

    /** Called with each root this store forgets, from a range miss or the object source. */
    void onForget(Consumer<Held> hook) {
        this.onForget = hook;
    }

    /** Names the ETag the next {@link #register(Manifest, String)} of a key was hashed at. */
    void expect(String key, String etag) {
        pendingEtags.put(key, etag == null ? "" : etag);
    }

    /** The registered object of a root, or empty. */
    Optional<Held> held(String root) {
        return Optional.ofNullable(byRoot.get(root));
    }

    /** The root registered for a key, or empty. */
    Optional<Held> byKey(String key) {
        return byRoot.values().stream().filter(h -> h.key().equals(key)).findFirst();
    }

    /** How many ranged reads this store sent. */
    public long reads() {
        return reads.get();
    }

    /** How many reads the deployment could not answer for the hashed version. */
    public long misses() {
        return misses.get();
    }

    @Override
    public void register(Manifest manifest, String key) {
        String etag = pendingEtags.remove(key);
        byRoot.put(manifest.root(), new Held(manifest, key, etag == null || etag.isEmpty() ? null : etag));
    }

    @Override
    public String kind() {
        return KIND;
    }

    @Override
    public boolean writable() {
        return false;
    }

    @Override
    public Set<String> roots() {
        return Set.copyOf(byRoot.keySet());
    }

    @Override
    public Optional<Manifest> manifest(String root) {
        Held held = byRoot.get(root);
        return held == null ? Optional.empty() : Optional.of(held.manifest());
    }

    @Override
    public void open(Manifest manifest) {
        // registration is explicit; an object is never opened for writes
    }

    @Override
    public BitSet have(String root) {
        BitSet bits = new BitSet();
        Held held = byRoot.get(root);
        if (held != null) {
            bits.set(0, held.manifest().pieces());
        }
        return bits;
    }

    @Override
    public Optional<byte[]> read(String root, int index, int offset, int length) {
        Held held = byRoot.get(root);
        if (held == null || index < 0 || index >= held.manifest().pieces()) {
            return Optional.empty();
        }
        int pieceLength = held.manifest().pieceLength(index);
        if (offset < 0 || offset >= pieceLength || length <= 0) {
            return Optional.empty();
        }
        int n = Math.min(length, pieceLength - offset);
        long at = (long) index * held.manifest().pieceSize() + offset;
        reads.incrementAndGet();
        try {
            return Optional.of(client.range(source.get(), held.key(), at, n, held.etag()));
        } catch (CelldClient.RangeMiss e) {
            misses.incrementAndGet();
            forget(root);
            return Optional.empty();
        } catch (CelldClient.RouteException e) {
            return Optional.empty(); // the deployment is unreachable now; the fetcher asks another holder
        }
    }

    @Override
    public void write(String root, int index, byte[] piece) {
        throw new IllegalStateException("the http-range store is read-only");
    }

    @Override
    public SeekableByteChannel openArtifact(String root) throws IOException {
        Held held = byRoot.get(root);
        if (held == null) {
            throw new IOException("root " + root + " is not registered here");
        }
        return new RangeChannel(held);
    }

    @Override
    public void pin(String root) {
    }

    @Override
    public void unpin(String root) {
    }

    @Override
    public boolean pinned(String root) {
        return byRoot.containsKey(root);
    }

    @Override
    public long bytesUsed() {
        return 0;
    }

    @Override
    public long quota() {
        return 0;
    }

    @Override
    public void evict() {
    }

    @Override
    public void forget(String root) {
        Held held = byRoot.remove(root);
        if (held != null) {
            onForget.accept(held);
        }
    }

    /** A read-only channel over an object by ranged reads of at most one piece. */
    private final class RangeChannel implements SeekableByteChannel {
        private final Held held;
        private long position;
        private boolean open = true;

        RangeChannel(Held held) {
            this.held = held;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            long size = held.manifest().size();
            if (position >= size) {
                return -1;
            }
            int want = (int) Math.min(Math.min(dst.remaining(), held.manifest().pieceSize()), size - position);
            if (want == 0) {
                return 0;
            }
            byte[] bytes;
            try {
                bytes = client.range(source.get(), held.key(), position, want, held.etag());
            } catch (RuntimeException e) {
                throw new IOException("ranged read of " + held.key() + " failed: " + e.getMessage(), e);
            }
            dst.put(bytes);
            position += bytes.length;
            return bytes.length;
        }

        @Override
        public int write(ByteBuffer src) {
            throw new NonWritableChannelException();
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public SeekableByteChannel position(long newPosition) {
            this.position = newPosition;
            return this;
        }

        @Override
        public long size() {
            return held.manifest().size();
        }

        @Override
        public SeekableByteChannel truncate(long size) {
            throw new NonWritableChannelException();
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
