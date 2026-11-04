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

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.ConnectorRuntime;
import ai.badmonkey.agentspaces.content.CollectionAssetProvider;
import ai.badmonkey.agentspaces.content.CompositeStore;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.PieceStore;
import ai.badmonkey.agentspaces.runtime.FleetContext;
import ai.badmonkey.agentspaces.runtime.FleetParticipant;
import ai.badmonkey.agentspaces.runtime.FleetRuntime;
import ai.badmonkey.agentspaces.runtime.PropertiesLoader;
import ai.badmonkey.agentspaces.spring.AgentSpacesProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * The Java connector for a celld or Workers deployment: joins a group from a
 * fleet file of the same {@code agentspaces:} shape the Node CLI and the
 * Spring and Micronaut hosts read, and serves the manifest's routes as
 * {@code workers://} AssetCards through {@link ConnectorRuntime}.
 *
 * <pre>
 * java -jar agentspaces-connect-celld.jar fleet.yaml [manifest.yaml]
 * </pre>
 *
 * <p>With the fleet's content swarm on, the connector also answers large results
 * as artifacts, takes snapshots, serves the deployment's objects where they lie
 * with a collection card, and sends artifacts into routes; with a change route it
 * turns the deployment's changes into {@code AssetChange} entries; in
 * active-active mode it shares the deployment with other connectors. With one
 * argument the manifest is the {@code celld:} subtree of the fleet file; with two
 * it is its own file. As a {@link FleetParticipant} the connector starts after the
 * node ticks, refreshes its cards on the fleet's card cadence, and closes before
 * the node leaves.
 */
public final class CelldConnector implements FleetParticipant, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CelldConnector.class);

    /**
     * The change cursor the polling replica leaves for the next one (CC-H-05).
     *
     * @param application the application
     * @param cursor      the cursor after the last change applied
     * @param at          when it was written, in epoch milliseconds
     */
    public record ChangeCursor(String application, String cursor, long at) {
    }

    private final FleetContext fleet;
    private final CelldManifest manifest;
    private final CelldAssetProvider provider;
    private final ConnectorRuntime connector;
    private final Content content;
    private final ArtifactResults artifacts;
    private final ArtifactUploads uploads;
    private final ObjectSources objects;
    private final ConnectorRuntime collections;
    private final SnapshotScheduler snapshots;
    private final ChangeWatcher changes;
    private final Space space;
    private volatile java.util.function.LongSupplier relaySignals;
    private volatile boolean started;

    /**
     * Wires the connector into a joined group. A manifest with object sources needs their stores
     * in the swarm before it starts: use {@link #run(Path, Path)} or
     * {@link #CelldConnector(FleetContext, CelldManifest, CelldAssetProvider, List)}.
     *
     * @param fleet    the group context the cards and entries live in
     * @param manifest the manifest of routes
     */
    public CelldConnector(FleetContext fleet, CelldManifest manifest) {
        this(fleet, manifest, new CelldAssetProvider(manifest));
    }

    /**
     * Wires the connector into a joined group over a given provider.
     *
     * @param fleet    the group context the cards and entries live in
     * @param manifest the manifest of routes
     * @param provider the provider over the manifest
     */
    public CelldConnector(FleetContext fleet, CelldManifest manifest, CelldAssetProvider provider) {
        this(fleet, manifest, provider, List.of());
    }

    /**
     * Wires the connector into a joined group over a given provider and the object stores the
     * runtime added to the swarm ({@link ObjectSources#stores(CelldManifest, CelldClient)}, passed
     * to {@code FleetRuntime.Builder.pieceStores} before the runtime started).
     *
     * @param fleet    the group context the cards and entries live in
     * @param manifest the manifest of routes
     * @param provider the provider over the manifest
     * @param stores   the object stores, one per object source, already in the swarm's store
     */
    public CelldConnector(FleetContext fleet, CelldManifest manifest, CelldAssetProvider provider,
                          List<HttpRangePieceStore> stores) {
        this.fleet = Objects.requireNonNull(fleet, "fleet");
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.provider = Objects.requireNonNull(provider, "provider");
        AgentSpaces.GroupContext group = fleet.group();
        this.space = fleet.requiredSpace(manifest.space(), "celld.space");
        this.content = fleet.content().orElse(null);
        if (manifest.usesSwarm() && content == null) {
            throw new IllegalStateException("celld: the manifest's artifact results, artifact bodies, or object"
                    + " sources need the fleet's content swarm; set agentspaces.content.enabled: true (with"
                    + " agentspaces.capabilities.enabled: true) in the fleet file");
        }
        this.connector = new ConnectorRuntime(space, group.discovery(), fleet.identity(),
                manifest.connectorName(), provider, group.id(), group.clock(), manifest.freshness());
        if (manifest.fleetAuthorizer()) {
            connector.authorizer(fleet.authorizer());
        }
        if (manifest.activeActive()) {
            connector.replicas(this::isReplica);
        }
        if (content != null) {
            this.artifacts = new ArtifactResults(manifest, provider.client(), content, group.clock());
            this.uploads = new ArtifactUploads(manifest, provider.client(), content);
            if (manifest.fleetAuthorizer()) {
                uploads.authorizer(fleet.authorizer());
            }
            provider.artifacts(artifacts).uploads(uploads);
            this.snapshots = new SnapshotScheduler(manifest, artifacts, group.clock());
        } else {
            this.artifacts = null;
            this.uploads = null;
            this.snapshots = null;
        }
        if (!manifest.objects().isEmpty()) {
            requireComposed(stores);
            this.objects = new ObjectSources(manifest, provider.client(), content, stores);
            CollectionAssetProvider collectionProvider = objects.collections();
            this.collections = collectionProvider == null ? null : new ConnectorRuntime(space, group.discovery(),
                    fleet.identity(), manifest.connectorName() + "-collections", collectionProvider, group.id(),
                    group.clock(), Duration.ofMinutes(15));
            if (collections != null && manifest.fleetAuthorizer()) {
                collections.authorizer(fleet.authorizer());
            }
        } else {
            this.objects = null;
            this.collections = null;
        }
        if (manifest.changes() != null) {
            this.changes = new ChangeWatcher(manifest, provider.client(), provider::emit, new ChangeWatcher.Listener() {
                @Override
                public void routeChanged(CelldManifest.RouteGroup route) {
                    connector.withdrawResults(route.asset());
                    if (snapshots != null) {
                        snapshots.routeChanged(route);
                    }
                }

                @Override
                public void objectChanged(CelldManifest.ObjectSource source, String key) {
                    if (objects != null) {
                        objects.wake(source.name());
                    }
                }
            }, this::elected);
        } else {
            this.changes = null;
        }
    }

    private void requireComposed(List<HttpRangePieceStore> stores) {
        Set<String> named = stores.stream().map(s -> s.source().name()).collect(Collectors.toSet());
        PieceStore store = content.store();
        List<PieceStore> backends = store instanceof CompositeStore composite ? composite.backends() : List.of(store);
        for (CelldManifest.ObjectSource source : manifest.objects()) {
            HttpRangePieceStore mine = stores.stream().filter(s -> s.source().name().equals(source.name()))
                    .findFirst().orElse(null);
            if (!named.contains(source.name()) || backends.stream().noneMatch(b -> b == mine)) {
                throw new IllegalStateException("celld.objects[" + source.name() + "]: the object store must join"
                        + " the swarm before it starts; build the runtime with FleetRuntime.builder(...).pieceStores("
                        + "group, ObjectSources.stores(manifest, client)), or start with CelldConnector.run");
            }
        }
    }

    /**
     * Counts the relay unreachable signals the node received (CC-S-04), for {@link #stats()};
     * {@link #run(Path, Path)} wires it from the runtime's node.
     *
     * @param signals the node's {@code unreachableSignalsReceived}
     * @return this connector
     */
    public CelldConnector relaySignals(java.util.function.LongSupplier signals) {
        this.relaySignals = signals;
        return this;
    }

    /** The provider over the manifest. */
    public CelldAssetProvider provider() {
        return provider;
    }

    /** The SDK runtime publishing the cards and serving the protocols. */
    public ConnectorRuntime connector() {
        return connector;
    }

    /** The manifest served. */
    public CelldManifest manifest() {
        return manifest;
    }

    /** The large-result path, or {@code null} without the swarm. */
    public ArtifactResults artifacts() {
        return artifacts;
    }

    /** The artifact-body path, or {@code null} without the swarm. */
    public ArtifactUploads uploads() {
        return uploads;
    }

    /** The object sources, or {@code null} when the manifest has none. */
    public ObjectSources objects() {
        return objects;
    }

    /** The collection runtime, or {@code null} when no object source names a collection. */
    public ConnectorRuntime collections() {
        return collections;
    }

    /** The snapshot scheduler, or {@code null} without the swarm. */
    public SnapshotScheduler snapshots() {
        return snapshots;
    }

    /** The change watcher, or {@code null} without a change route. */
    public ChangeWatcher changes() {
        return changes;
    }

    /**
     * What this connector did, by counter (CC-O-04): the calls to the deployment, the queries and
     * invocations, the artifacts and their bytes, the ranged reads, the uploads, the changes, and
     * the object-source work.
     *
     * @return the counters
     */
    public Map<String, Long> stats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("http-calls", (long) provider.client().calls());
        stats.put("queries-executed", (long) connector.executed());
        stats.put("queries-served-from-space", (long) connector.servedFromSpace());
        stats.put("invocations-run", (long) connector.invoked());
        stats.put("invocations-refused", (long) connector.refused());
        stats.put("invocations-repeated", (long) connector.repeated());
        stats.put("invocations-mirrored", (long) connector.mirrored());
        stats.put("claim-renewals", (long) connector.renewals());
        stats.put("results-withdrawn", (long) connector.withdrawn());
        stats.put("changes-materialized", (long) connector.materialized());
        if (artifacts != null) {
            stats.put("artifacts-published", artifacts.published());
            stats.put("artifact-bytes-read", artifacts.bytesRead());
            stats.put("uploads", uploads.uploads());
            stats.put("upload-bytes-sent", uploads.bytesSent());
            stats.put("snapshots-taken", snapshots.taken());
            stats.put("swarm-bytes-served", content.stats().bytesServed());
            stats.put("swarm-bytes-received", content.stats().bytesReceived());
            stats.put("swarm-wants-refused", content.stats().wantsRefused());
        }
        if (objects != null) {
            stats.put("object-bytes-hashed", objects.bytesHashed());
            stats.put("object-keys-refused", objects.keysRefused());
            stats.put("object-roots-forgotten", objects.rootsForgotten());
            stats.put("object-roots-reused", objects.reused());
            long reads = 0;
            long misses = 0;
            for (CelldManifest.ObjectSource source : manifest.objects()) {
                reads += objects.store(source.name()).reads();
                misses += objects.store(source.name()).misses();
            }
            stats.put("ranged-reads", reads);
            stats.put("range-misses", misses);
        }
        if (changes != null) {
            stats.put("changes-seen", changes.seen());
            stats.put("changes-dropped", changes.dropped());
            stats.put("change-polls", changes.polls());
        }
        java.util.function.LongSupplier signals = relaySignals;
        if (signals != null) {
            stats.put("relay-unreachable-signals", signals.getAsLong());
        }
        return stats;
    }

    // ------------------------------------------------------------------ replicas

    /** The peers serving this application's route cards, this connector among them (CC-H). */
    Set<PeerId> replicaPeers() {
        String prefix = "workers://" + manifest.application() + "/";
        Set<PeerId> peers = fleet.group().discovery().find(AssetCard.class, c -> c.uri() != null
                        && c.uri().startsWith(prefix) && c.group().equals(fleet.group().id()))
                .stream().map(AssetCard::issuer).collect(Collectors.toSet());
        peers.add(fleet.peerId());
        return peers;
    }

    private boolean isReplica(PeerId peer) {
        return !peer.equals(fleet.peerId()) && replicaPeers().contains(peer);
    }

    /** Whether this connector polls the change route: always alone, the lowest live peer id among replicas. */
    boolean elected() {
        if (!manifest.activeActive()) {
            return true;
        }
        PeerId lowest = replicaPeers().stream().min(Comparator.comparing(PeerId::value)).orElse(fleet.peerId());
        boolean mine = lowest.equals(fleet.peerId());
        if (mine && changes != null) {
            adoptCursor();
        }
        return mine;
    }

    private volatile String sharedCursor;

    private void adoptCursor() {
        Set<PeerId> peers = replicaPeers();
        space.readAllIssued(Template.of(ChangeCursor.class).where("application", eq(manifest.application())), 32)
                .stream()
                .filter(i -> peers.contains(i.issuer().peer()) && !i.issuer().peer().equals(fleet.peerId()))
                .map(Space.Issued::entry)
                .max(Comparator.comparingLong(ChangeCursor::at))
                .filter(c -> !c.cursor().equals(sharedCursor))
                .ifPresent(c -> {
                    sharedCursor = c.cursor();
                    changes.adopt(c.cursor());
                });
    }

    private void shareCursor() {
        if (changes == null || !manifest.activeActive() || changes.cursor() == null
                || changes.cursor().equals(sharedCursor)) {
            return;
        }
        try {
            space.write(new ChangeCursor(manifest.application(), changes.cursor(),
                    fleet.group().clock().millis()), Lease.of(manifest.changes().poll().multipliedBy(30)));
            sharedCursor = changes.cursor();
        } catch (RuntimeException e) {
            LOG.warn("writing the change cursor failed: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        connector.start();
        if (objects != null) {
            objects.start();
        }
        if (collections != null) {
            collections.start();
        }
        if (snapshots != null) {
            snapshots.start();
        }
        if (changes != null) {
            changes.start();
        }
        started = true;
        LOG.info("celld connector {} serving {} route group(s) and {} object source(s) of {} at {} in group {}",
                manifest.connectorName(), manifest.routes().size(), manifest.objects().size(),
                manifest.application(), manifest.baseUrl(), fleet.groupName());
    }

    @Override
    public void refresh() {
        if (started) {
            connector.refreshCards();
            if (collections != null) {
                collections.refreshCards();
            }
            if (manifest.activeActive() && elected()) {
                shareCursor();
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (started) {
            if (changes != null) {
                changes.close();
            }
            if (snapshots != null) {
                snapshots.close();
            }
            if (collections != null) {
                collections.close();
            }
            if (objects != null) {
                objects.close();
            }
            connector.close();
            started = false;
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * A running connector and the fleet runtime it serves in; closing it closes the runtime, which
     * stops the connector.
     *
     * @param runtime   the fleet runtime
     * @param connector the connector
     */
    public record Running(FleetRuntime runtime, CelldConnector connector) implements AutoCloseable {
        @Override
        public void close() {
            runtime.close();
        }
    }

    /**
     * Starts a fleet runtime from the fleet file, with the manifest's object stores in the
     * swarm, adds the connector for the manifest, and returns the runtime; closing it stops the
     * connector.
     *
     * @param fleetFile    the fleet YAML ({@code agentspaces:} subtree)
     * @param manifestFile the manifest YAML, or {@code null} to read the fleet file's {@code celld:} subtree
     * @return the running fleet runtime
     */
    public static FleetRuntime run(Path fleetFile, Path manifestFile) {
        return launch(fleetFile, manifestFile).runtime();
    }

    /**
     * As {@link #run(Path, Path)}, returning the connector with the runtime.
     *
     * @param fleetFile    the fleet YAML ({@code agentspaces:} subtree)
     * @param manifestFile the manifest YAML, or {@code null} to read the fleet file's {@code celld:} subtree
     * @return the running connector and runtime
     */
    public static Running launch(Path fleetFile, Path manifestFile) {
        Map<String, Object> document = PropertiesLoader.tree(fleetFile);
        CelldManifest manifest = manifestFile != null ? CelldManifest.load(manifestFile)
                : CelldManifest.fromDocument(document);
        AgentSpacesProperties properties = PropertiesLoader.map(PropertiesLoader.agentspacesSubtree(document));
        CelldClient client = new CelldClient(manifest);
        List<HttpRangePieceStore> stores = ObjectSources.stores(manifest, client);
        FleetRuntime.Builder builder = FleetRuntime.builder(properties);
        String groupName = manifest.group() != null ? manifest.group()
                : properties.getGroups().isEmpty() ? null : properties.getGroups().getFirst().getName();
        if (!stores.isEmpty() && groupName != null) {
            builder.pieceStores(groupName, stores);
        }
        FleetRuntime runtime = builder.start();
        try {
            FleetContext fleet = manifest.group() == null ? runtime.context() : runtime.context(manifest.group());
            CelldConnector connector = new CelldConnector(fleet, manifest, new CelldAssetProvider(manifest, client),
                    stores).relaySignals(runtime.node()::unreachableSignalsReceived);
            runtime.participant(connector);
            return new Running(runtime, connector);
        } catch (RuntimeException e) {
            runtime.close();
            throw e;
        }
    }

    /** {@code fleet.yaml [manifest.yaml]}: runs until the process is signalled. */
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1 || args.length > 2) {
            System.err.println("usage: CelldConnector fleet.yaml [manifest.yaml]");
            System.exit(2);
        }
        FleetRuntime runtime = run(Path.of(args[0]), args.length == 2 ? Path.of(args[1]) : null);
        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            runtime.close();
            stop.countDown();
        }, "celld-connector-shutdown"));
        stop.await();
    }
}
