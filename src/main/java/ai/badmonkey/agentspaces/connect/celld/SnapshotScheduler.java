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

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Snapshots on a cadence and on change (CC-D-03): each snapshot operation with a
 * {@code refresh} runs on that cadence, and a change notice naming its route
 * runs it early, at most once per {@code min-interval}. A snapshot is a large
 * result like any other; {@link ArtifactResults} tags it and keeps the newest
 * roots pinned.
 */
public final class SnapshotScheduler implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(SnapshotScheduler.class.getName());

    private final CelldManifest manifest;
    private final ArtifactResults artifacts;
    private final InstantSource clock;
    private final Map<String, Instant> lastTaken = new ConcurrentHashMap<>();
    private final AtomicLong taken = new AtomicLong();
    private volatile boolean running;
    private Thread loop;

    /**
     * @param manifest  the manifest
     * @param artifacts the large-result path the snapshots take
     * @param clock     the clock
     */
    public SnapshotScheduler(CelldManifest manifest, ArtifactResults artifacts, InstantSource clock) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Whether the manifest declares any snapshot. */
    public boolean any() {
        return manifest.routes().stream().flatMap(r -> r.operations().stream())
                .anyMatch(CelldManifest.Operation::isSnapshot);
    }

    /** Snapshots taken. */
    public long taken() {
        return taken.get();
    }

    /** Starts the cadence loop. */
    public synchronized void start() {
        if (running || !any()) {
            return;
        }
        running = true;
        loop = Thread.ofVirtual().name("celld-snapshots-" + manifest.application()).start(this::run);
    }

    @Override
    public synchronized void close() {
        running = false;
        if (loop != null) {
            loop.interrupt();
            loop = null;
        }
    }

    /** A change notice named a route: its snapshots run now unless one ran within {@code min-interval}. */
    public void routeChanged(CelldManifest.RouteGroup route) {
        for (CelldManifest.Operation operation : route.operations()) {
            if (operation.isSnapshot()) {
                Thread.ofVirtual().name("celld-snapshot-" + operation.name()).start(() -> take(route, operation, false));
            }
        }
    }

    /**
     * Takes one snapshot now.
     *
     * @param route     the route group
     * @param operation the snapshot operation
     * @param force     whether to ignore {@code min-interval}
     * @return whether a snapshot was taken
     */
    public boolean take(CelldManifest.RouteGroup route, CelldManifest.Operation operation, boolean force) {
        String key = route.name() + "/" + operation.name();
        synchronized (this) {
            Instant last = lastTaken.get(key);
            Instant now = clock.instant();
            if (!force && last != null && Duration.between(last, now).compareTo(operation.snapshot().minInterval()) < 0) {
                return false;
            }
            lastTaken.put(key, now);
        }
        try {
            artifacts.answer(route, operation, Map.of(), null);
            taken.incrementAndGet();
            return true;
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "snapshot " + key + " failed: " + e.getMessage());
            return false;
        }
    }

    private void run() {
        while (running) {
            Instant now = clock.instant();
            for (CelldManifest.RouteGroup route : manifest.routes()) {
                for (CelldManifest.Operation operation : route.operations()) {
                    if (!operation.isSnapshot() || operation.snapshot().refresh() == null) {
                        continue;
                    }
                    Instant last = lastTaken.get(route.name() + "/" + operation.name());
                    if (last == null || !now.isBefore(last.plus(operation.snapshot().refresh()))) {
                        take(route, operation, true);
                    }
                }
            }
            try {
                Thread.sleep(Duration.ofSeconds(1));
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}
