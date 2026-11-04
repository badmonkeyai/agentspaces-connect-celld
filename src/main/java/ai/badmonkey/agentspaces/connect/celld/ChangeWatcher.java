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

import ai.badmonkey.agentspaces.connect.MaterializingAssetProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Change notices (CC-N): polls the deployment's change route after a cursor,
 * or holds its Server-Sent Events stream, and fans each change out. A route
 * change becomes an {@code AssetChange} through the provider's
 * {@link MaterializingAssetProvider} sink and tells the connector the route
 * moved, so it withdraws its cached results and refreshes snapshots; an object
 * change wakes the object source. A change never runs an operation: the only
 * calls it causes are reads of allowlisted routes. The cursor persists in the
 * state directory, so a restart resumes after it.
 *
 * <p>The change record is {@code {cursor, changes: [{kind, route | object, key, row?}]}};
 * {@code kind} is {@code upsert}, {@code delete}, or {@code object}, and an unknown
 * kind, route, or object is dropped and counted.
 */
public final class ChangeWatcher implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ChangeWatcher.class.getName());
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> KINDS = Set.of("upsert", "delete", "object");

    /** What a change means to the rest of the connector. */
    public interface Listener {
        /** A route group's data moved. */
        void routeChanged(CelldManifest.RouteGroup route);

        /** An object source's object moved. */
        void objectChanged(CelldManifest.ObjectSource source, String key);
    }

    private final CelldManifest manifest;
    private final CelldManifest.Changes changes;
    private final CelldClient client;
    private final Consumer<MaterializingAssetProvider.Change> sink;
    private final Listener listener;
    private final BooleanSupplier elected;
    private final AtomicLong seen = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong polls = new AtomicLong();
    private volatile String cursor;
    private volatile boolean primed;
    private volatile boolean running;
    private Thread loop;

    /**
     * @param manifest the manifest, whose {@code changes} block is set
     * @param client   the client over the deployment
     * @param sink     receives each route change as an asset change
     * @param listener receives each route and object that moved
     * @param elected  whether this connector polls now (always, for a single connector)
     */
    public ChangeWatcher(CelldManifest manifest, CelldClient client, Consumer<MaterializingAssetProvider.Change> sink,
                         Listener listener, BooleanSupplier elected) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.changes = Objects.requireNonNull(manifest.changes(), "manifest.changes");
        this.client = Objects.requireNonNull(client, "client");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.elected = Objects.requireNonNull(elected, "elected");
        this.cursor = readCursor();
        this.primed = cursor != null;
    }

    /** Changes passed on. */
    public long seen() {
        return seen.get();
    }

    /** Changes dropped: an unknown kind, route, or object. */
    public long dropped() {
        return dropped.get();
    }

    /** Polls or stream connections made. */
    public long polls() {
        return polls.get();
    }

    /** The current cursor, or {@code null} before the first answer. */
    public String cursor() {
        return cursor;
    }

    /** Takes over the cursor a previous poller left (CC-H-05), when it is ahead of ours. */
    public void adopt(String other) {
        if (other != null && !other.equals(cursor)) {
            cursor = other;
            primed = true;
            writeCursor();
        }
    }

    /** Starts the poll or stream loop. */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        loop = Thread.ofVirtual().name("celld-changes-" + manifest.application()).start(this::run);
    }

    @Override
    public synchronized void close() {
        running = false;
        if (loop != null) {
            loop.interrupt();
            loop = null;
        }
    }

    /** One poll now; for tests and the first start. */
    public void pollNow() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("limit", Integer.toString(changes.maxChanges()));
        if (cursor != null) {
            params.put("after", cursor);
        }
        polls.incrementAndGet();
        CelldClient.Response response = client.getJson(changes.path(), params);
        if (!response.ok()) {
            throw new CelldClient.RouteException("target-error", "http-status", response.status() + " from GET "
                    + changes.path(), null);
        }
        apply(response.body());
    }

    private void run() {
        int streamFailures = 0;
        long streamFailureWindow = 0;
        long pollUntil = 0;
        while (running) {
            try {
                if (!elected.getAsBoolean()) {
                    sleep(changes.poll());
                    continue;
                }
                long now = System.nanoTime();
                if (changes.stream() != null && primed && now >= pollUntil) {
                    try {
                        polls.incrementAndGet();
                        client.events(changes.stream(), cursor, event -> {
                            if (running) {
                                applyEvent(event);
                            }
                        });
                    } catch (CelldClient.RouteException e) {
                        if (now - streamFailureWindow > Duration.ofMinutes(1).toNanos()) {
                            streamFailureWindow = now;
                            streamFailures = 0;
                        }
                        if (++streamFailures >= 3) {
                            LOG.log(System.Logger.Level.WARNING, "change stream " + changes.stream()
                                    + " failed three times in a minute; polling for five minutes: " + e.getMessage());
                            pollUntil = System.nanoTime() + Duration.ofMinutes(5).toNanos();
                            streamFailures = 0;
                        }
                    }
                    if (!sleep(Duration.ofSeconds(1))) { // a stream that ended reconnects after the cursor
                        return;
                    }
                    continue;
                }
                pollNow();
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "change route " + changes.path() + " failed: " + e.getMessage());
            }
            if (!sleep(changes.poll())) {
                return;
            }
        }
    }

    private void applyEvent(CelldClient.Event event) {
        JsonNode body;
        try {
            body = JSON.readTree(event.data().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            dropped.incrementAndGet();
            return;
        }
        if (body.has("changes")) {
            apply(body);
        } else {
            List<JsonNode> one = List.of(body);
            fanOut(one);
            advance(event.id() != null ? event.id() : body.path("cursor").asText(null));
        }
    }

    /** Applies one answer: the changes in order, then the cursor. */
    private void apply(JsonNode body) {
        if (body == null) {
            return;
        }
        String next = body.hasNonNull("cursor") ? body.get("cursor").asText() : null;
        if (!primed) {
            // CC-N-02: a first start takes the cursor and emits no history.
            primed = true;
            advance(next);
            return;
        }
        List<JsonNode> list = new ArrayList<>();
        JsonNode items = body.get("changes");
        if (items != null && items.isArray()) {
            for (JsonNode item : items) {
                if (list.size() >= changes.maxChanges()) {
                    break;
                }
                list.add(item);
            }
        }
        fanOut(list);
        advance(next);
    }

    private void fanOut(List<JsonNode> items) {
        Set<CelldManifest.RouteGroup> moved = new LinkedHashSet<>();
        for (JsonNode item : items) {
            String kind = item.path("kind").asText("upsert");
            String key = item.path("key").asText(null);
            if (!KINDS.contains(kind) || key == null) {
                dropped.incrementAndGet();
                continue;
            }
            if (kind.equals("object")) {
                CelldManifest.ObjectSource source = manifest.objectSource(item.path("object").asText(""));
                if (source == null) {
                    dropped.incrementAndGet();
                    continue;
                }
                seen.incrementAndGet();
                listener.objectChanged(source, key);
                continue;
            }
            CelldManifest.RouteGroup route = manifest.route("route/" + item.path("route").asText(""));
            if (route == null) {
                dropped.incrementAndGet();
                continue;
            }
            Map<String, String> row = new LinkedHashMap<>();
            JsonNode raw = item.get("row");
            if (raw != null && raw.isObject()) {
                for (Iterator<Map.Entry<String, JsonNode>> it = raw.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> field = it.next();
                    if (!field.getValue().isNull()) {
                        row.put(field.getKey(), field.getValue().isValueNode() ? field.getValue().asText()
                                : field.getValue().toString());
                    }
                }
            }
            row.put("kind", kind);
            seen.incrementAndGet();
            sink.accept(new MaterializingAssetProvider.Change(route.asset(), key, row));
            moved.add(route);
        }
        moved.forEach(listener::routeChanged);
    }

    private void advance(String next) {
        if (next != null && !next.isEmpty() && !next.equals(cursor)) {
            cursor = next;
            writeCursor();
        }
    }

    private Path cursorFile() {
        return manifest.state().resolve("changes.cursor");
    }

    private String readCursor() {
        try {
            Path file = cursorFile();
            if (Files.isRegularFile(file)) {
                String text = Files.readString(file).strip();
                return text.isEmpty() ? null : text;
            }
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "the change cursor is unreadable; starting fresh: " + e.getMessage());
        }
        return null;
    }

    private void writeCursor() {
        Path file = cursorFile();
        try {
            Files.createDirectories(file.getParent());
            Path temp = file.resolveSibling("changes.cursor.tmp");
            Files.writeString(temp, cursor == null ? "" : cursor);
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "the change cursor could not be written: " + e.getMessage());
        }
    }

    private boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}
