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
import ai.badmonkey.agentspaces.content.CollectionAssetProvider;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.ContentRoots;
import ai.badmonkey.agentspaces.content.PublishOptions;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Object sources (CC-R, CC-K): lists each source's objects on its cadence,
 * hashes each new key and ETag once within the hash budget, publishes it as an
 * external artifact served by its {@link HttpRangePieceStore}, forgets keys
 * that leave the list, and advertises a {@code content-collection} card for a
 * source that names one. An index file per source maps each key and ETag to
 * its table, so a restart registers unchanged objects without reading them.
 */
public final class ObjectSources implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ObjectSources.class.getName());
    private static final ObjectMapper JSON = new ObjectMapper();
    /** The longest key the connector passes to a get route, in UTF-8 bytes. */
    static final int MAX_KEY_BYTES = 1024;
    private static final int PAGE = 1000;
    private static final int MAX_PAGES = 10_000;

    /** One indexed object: the version hashed and its table. */
    record Indexed(String key, String etag, String root, long size, int pieceSize, String table, String mediaType,
                   Map<String, String> attrs) {
    }

    private final CelldManifest manifest;
    private final CelldClient client;
    private final Content content;
    private final Map<String, HttpRangePieceStore> stores = new LinkedHashMap<>();
    private final Map<String, Map<String, Indexed>> indexes = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> wake = new ConcurrentHashMap<>();
    private final List<Thread> loops = new ArrayList<>();
    private final AtomicLong bytesHashed = new AtomicLong();
    private final AtomicLong keysRefused = new AtomicLong();
    private final AtomicLong rootsForgotten = new AtomicLong();
    private final AtomicLong reused = new AtomicLong();
    private final AtomicLong cycles = new AtomicLong();
    private volatile boolean running;

    /**
     * The stores a host adds to the swarm before it starts, one per object source.
     *
     * @param manifest the manifest
     * @param client   the client over the deployment
     * @return the stores, in manifest order
     */
    public static List<HttpRangePieceStore> stores(CelldManifest manifest, CelldClient client) {
        List<HttpRangePieceStore> stores = new ArrayList<>();
        for (CelldManifest.ObjectSource source : manifest.objects()) {
            stores.add(new HttpRangePieceStore(client, source));
        }
        return stores;
    }

    /**
     * @param manifest the manifest
     * @param client   the client over the deployment
     * @param content  the group's content swarm, whose store composes every one of {@code stores}
     * @param stores   the stores from {@link #stores(CelldManifest, CelldClient)}
     */
    public ObjectSources(CelldManifest manifest, CelldClient client, Content content, List<HttpRangePieceStore> stores) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.client = Objects.requireNonNull(client, "client");
        this.content = Objects.requireNonNull(content, "content");
        for (HttpRangePieceStore store : stores) {
            String name = store.source().name();
            this.stores.put(name, store);
            this.wake.put(name, new Semaphore(0));
            store.onForget(held -> {
                rootsForgotten.incrementAndGet();
                Map<String, Indexed> index = indexes.get(name);
                if (index != null && index.remove(held.key()) != null) {
                    save(name);
                }
                wake(name); // a forgotten object is hashed again on the next pass
            });
        }
    }

    /** The store of a source, or {@code null}. */
    public HttpRangePieceStore store(String source) {
        return stores.get(source);
    }

    /** The collection provider over the sources that name a collection, or {@code null} when none does. */
    public CollectionAssetProvider collections() {
        List<CollectionAssetProvider.Collection> collections = new ArrayList<>();
        for (HttpRangePieceStore store : stores.values()) {
            CelldManifest.CollectionSpec spec = store.source().collection();
            if (spec != null) {
                collections.add(new CollectionAssetProvider.Collection(spec.name(), spec.description(),
                        m -> store.roots().contains(m.root()), spec.freshness()));
            }
        }
        return collections.isEmpty() ? null : new CollectionAssetProvider(content, collections);
    }

    /** Registers indexed objects and starts each source's list cycle. */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        for (HttpRangePieceStore store : stores.values()) {
            String name = store.source().name();
            Map<String, Indexed> index = load(name);
            indexes.put(name, index);
            for (Indexed indexed : List.copyOf(index.values())) {
                try {
                    publish(store, indexed);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "object " + indexed.key() + " of source " + name
                            + " could not be registered from its index: " + e.getMessage());
                    index.remove(indexed.key());
                }
            }
            loops.add(Thread.ofVirtual().name("celld-objects-" + name).start(() -> loop(store)));
        }
    }

    /** Runs a source's next list pass now (a change notice named one of its objects, CC-N-04). */
    public void wake(String source) {
        Semaphore semaphore = wake.get(source);
        if (semaphore != null && semaphore.availablePermits() == 0) {
            semaphore.release();
        }
    }

    /** Runs one list pass of a source and waits for it; for tests and the first start. */
    public void cycleNow(String source) {
        HttpRangePieceStore store = stores.get(source);
        if (store != null) {
            cycle(store);
        }
    }

    /** Bytes read for hashing. */
    public long bytesHashed() {
        return bytesHashed.get();
    }

    /** Keys refused for their length or characters. */
    public long keysRefused() {
        return keysRefused.get();
    }

    /** Roots forgotten: objects that changed or left the list. */
    public long rootsForgotten() {
        return rootsForgotten.get();
    }

    /** Objects registered from a replica's manifest without hashing (CC-H-06). */
    public long reused() {
        return reused.get();
    }

    /** List passes completed. */
    public long cycles() {
        return cycles.get();
    }

    @Override
    public synchronized void close() {
        running = false;
        loops.forEach(Thread::interrupt);
        loops.clear();
    }

    // ------------------------------------------------------------------ the cycle

    private void loop(HttpRangePieceStore store) {
        Semaphore semaphore = wake.get(store.source().name());
        while (running) {
            try {
                cycle(store);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "object source " + store.source().name() + " pass failed: "
                        + e.getMessage());
            }
            try {
                semaphore.tryAcquire(store.source().cadence().toMillis(), TimeUnit.MILLISECONDS);
                semaphore.drainPermits();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private synchronized void cycle(HttpRangePieceStore store) {
        CelldManifest.ObjectSource source = store.source();
        Map<String, Indexed> index = indexes.computeIfAbsent(source.name(), this::load);
        Set<String> seen = new HashSet<>();
        long budget = source.hashBudget();
        boolean complete = false;
        String after = null;
        for (int page = 0; page < MAX_PAGES; page++) {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("limit", Integer.toString(PAGE));
            if (after != null) {
                params.put("after", after);
            }
            CelldClient.Response response = client.getJson(source.list(), params);
            if (!response.ok()) {
                throw new CelldClient.RouteException("target-error", "http-status", response.status() + " from GET "
                        + source.list(), null);
            }
            JsonNode body = response.body();
            JsonNode items = body == null ? null : body.isArray() ? body : body.get("items");
            if (items != null && items.isArray()) {
                for (JsonNode item : items) {
                    budget -= item(store, index, item, seen, budget);
                }
            }
            JsonNode next = body == null || body.isArray() ? null : body.get("after");
            if (next == null || next.isNull() || next.asText().isEmpty() || items == null || items.isEmpty()) {
                complete = true;
                break;
            }
            after = next.asText();
        }
        if (complete) {
            // CC-R-08: a key absent from a full pass leaves, with its holding and collection row.
            for (String key : List.copyOf(index.keySet())) {
                if (!seen.contains(key)) {
                    Indexed gone = index.remove(key);
                    store.forget(gone.root());
                }
            }
            for (String root : store.roots()) {
                store.held(root).filter(h -> !seen.contains(h.key())).ifPresent(h -> store.forget(root));
            }
        }
        save(source.name());
        cycles.incrementAndGet();
    }

    /** One listed object: registered, hashed within the budget, or refused; returns the bytes hashed. */
    private long item(HttpRangePieceStore store, Map<String, Indexed> index, JsonNode item, Set<String> seen,
                      long budget) {
        CelldManifest.ObjectSource source = store.source();
        String key = item.path("key").asText(null);
        if (key == null || !acceptable(key)) {
            keysRefused.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, "object source " + source.name() + " listed a key the connector"
                    + " refuses (empty, over " + MAX_KEY_BYTES + " bytes, or with control characters)");
            return 0;
        }
        seen.add(key);
        long size = item.path("size").asLong(-1);
        String etag = item.hasNonNull("etag") ? item.get("etag").asText() : null;
        Indexed known = index.get(key);
        if (known != null && Objects.equals(known.etag(), etag) && (size < 0 || known.size() == size)) {
            if (store.held(known.root()).isEmpty() || content.describe(known.root()).isEmpty()) {
                publish(store, known);
            }
            return 0;
        }
        if (known != null) {
            index.remove(key);
            store.forget(known.root()); // the object changed: the old version stops serving
        }
        int pieceSize = pieceSize(source);
        Map<String, String> attrs = attrs(source, key, etag, item.get("attrs"));
        String mediaType = item.hasNonNull("mediaType") ? item.get("mediaType").asText() : "application/octet-stream";
        Indexed shared = shared(source, key, etag, pieceSize);
        if (shared != null) {
            index.put(key, shared);
            publish(store, shared);
            reused.incrementAndGet();
            return 0;
        }
        if (size > budget) {
            return 0; // CC-R-06: a later pass hashes it
        }
        long max = size >= 0 ? size : manifest.artifacts().maxBytes();
        Map.Entry<InputStream, String> body = client.object(source.get(), key, etag, max);
        ContentRoots.Hashed hashed;
        try (InputStream in = body.getKey()) {
            hashed = ContentRoots.hash(in, pieceSize);
        } catch (BoundedBody.Exceeded e) {
            throw e.route();
        } catch (IOException e) {
            throw new CelldClient.RouteException("target-error", "route", "GET " + source.get() + " for " + key
                    + " failed: " + e, e);
        }
        bytesHashed.addAndGet(hashed.size());
        if (size >= 0 && hashed.size() != size) {
            LOG.log(System.Logger.Level.WARNING, "object " + key + " of source " + source.name() + " is "
                    + hashed.size() + " bytes, but the list says " + size + "; not published");
            return hashed.size();
        }
        String version = etag != null ? etag : body.getValue().isEmpty() ? null : body.getValue();
        if (etag == null && version != null) {
            attrs.put("celld-etag", version);
        }
        Indexed indexed = new Indexed(key, version, hashed.root(), hashed.size(), pieceSize,
                Base64.getEncoder().encodeToString(hashed.table()), mediaType, attrs);
        index.put(key, indexed);
        publish(store, indexed);
        return hashed.size();
    }

    /** CC-H-06: a replica's manifest for the same key and ETag, with an inline table, reused unhashed. */
    private Indexed shared(CelldManifest.ObjectSource source, String key, String etag, int pieceSize) {
        if (etag == null) {
            return null;
        }
        Map<String, String> wanted = Map.of("celld-application", manifest.application(), "celld-object",
                source.name(), "celld-key", key, "celld-etag", etag);
        for (Content.Description d : content.search(null, null, wanted, null, null, 4)) {
            Manifest m = d.manifest();
            if (m.table() != null && m.pieceSize() == pieceSize) {
                return new Indexed(key, etag, m.root(), m.size(), m.pieceSize(),
                        Base64.getEncoder().encodeToString(m.table()), m.mediaType(), m.attrs());
            }
        }
        return null;
    }

    private void publish(HttpRangePieceStore store, Indexed indexed) {
        byte[] table = Base64.getDecoder().decode(indexed.table());
        ContentRoots.Hashed hashed = new ContentRoots.Hashed(table, indexed.size(), indexed.root());
        if (!ContentRoots.rootOf(table).equals(indexed.root())) {
            throw new IllegalStateException("the index entry for " + indexed.key() + " does not match its root");
        }
        PublishOptions options = content.publishOptions()
                .withPieceSize(indexed.pieceSize())
                .withName(indexed.key())
                .withMediaType(indexed.mediaType())
                .withAttrs(indexed.attrs())
                .withReplicate(store.source().replicate())
                .withLease(Lease.of(manifest.artifacts().lease()));
        store.expect(indexed.key(), indexed.etag());
        content.publishExternal(store, indexed.key(), hashed, options);
    }

    private int pieceSize(CelldManifest.ObjectSource source) {
        if (source.pieceSize() > 0) {
            return source.pieceSize();
        }
        return manifest.artifacts().pieceSize() > 0 ? manifest.artifacts().pieceSize()
                : content.publishOptions().pieceSize();
    }

    private Map<String, String> attrs(CelldManifest.ObjectSource source, String key, String etag, JsonNode listed) {
        Map<String, String> attrs = new LinkedHashMap<>();
        if (listed != null && listed.isObject()) {
            for (Iterator<Map.Entry<String, JsonNode>> it = listed.fields(); it.hasNext() && attrs.size() < 28; ) {
                Map.Entry<String, JsonNode> field = it.next();
                if (field.getValue().isValueNode() && !field.getKey().startsWith("celld-")) {
                    attrs.put(field.getKey(), field.getValue().asText());
                }
            }
        }
        attrs.put("celld-application", manifest.application());
        attrs.put("celld-object", source.name());
        attrs.put("celld-key", key);
        if (etag != null) {
            attrs.put("celld-etag", etag);
        }
        return attrs;
    }

    /** CC-R-05: a key travels only as an encoded query parameter, and only a sane one. */
    static boolean acceptable(String key) {
        if (key.isEmpty() || key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            return false;
        }
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c < 0x20 || c == 0x7f) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------ the index

    private Path indexFile(String source) {
        return manifest.state().resolve("objects-" + source + ".json");
    }

    private Map<String, Indexed> load(String source) {
        Map<String, Indexed> index = new ConcurrentHashMap<>();
        Path file = indexFile(source);
        if (Files.isRegularFile(file)) {
            try {
                List<Indexed> entries = JSON.readValue(file.toFile(), new TypeReference<List<Indexed>>() { });
                for (Indexed entry : entries) {
                    index.put(entry.key(), entry);
                }
            } catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "the object index " + file + " is unreadable and is rebuilt: "
                        + e.getMessage());
            }
        }
        return index;
    }

    private void save(String source) {
        Map<String, Indexed> index = indexes.get(source);
        if (index == null) {
            return;
        }
        Path file = indexFile(source);
        try {
            Files.createDirectories(file.getParent());
            Path temp = file.resolveSibling(file.getFileName() + ".tmp");
            JSON.writeValue(temp.toFile(), List.copyOf(index.values()));
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "the object index " + file + " could not be written: "
                    + e.getMessage());
        }
    }
}
