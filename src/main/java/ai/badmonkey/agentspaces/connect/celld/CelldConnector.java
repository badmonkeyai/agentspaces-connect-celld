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
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.connect.ConnectorRuntime;
import ai.badmonkey.agentspaces.runtime.FleetContext;
import ai.badmonkey.agentspaces.runtime.FleetParticipant;
import ai.badmonkey.agentspaces.runtime.FleetRuntime;
import ai.badmonkey.agentspaces.runtime.PropertiesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * The Java connector for a celld or Workers deployment its team cannot
 * change (srad/agentspaces-js.md JS-C-09, scenario 9): joins a group from a
 * fleet file of the same {@code agentspaces:} shape the Node CLI and the
 * Spring and Micronaut hosts read, and serves the manifest's routes as
 * {@code workers://} AssetCards through {@link ConnectorRuntime}.
 *
 * <pre>
 * java -jar agentspaces-connect-celld.jar fleet.yaml [manifest.yaml]
 * </pre>
 *
 * <p>With one argument the manifest is the {@code celld:} subtree of the
 * fleet file; with two it is its own file. As a {@link FleetParticipant} the
 * connector starts after the node ticks, refreshes its cards on the fleet's
 * card cadence, and closes before the node leaves.
 */
public final class CelldConnector implements FleetParticipant, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CelldConnector.class);

    private final FleetContext fleet;
    private final CelldManifest manifest;
    private final CelldAssetProvider provider;
    private final ConnectorRuntime connector;
    private volatile boolean started;

    /**
     * Wires the connector into a joined group.
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
        this.fleet = Objects.requireNonNull(fleet, "fleet");
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.provider = Objects.requireNonNull(provider, "provider");
        AgentSpaces.GroupContext group = fleet.group();
        Space space = fleet.requiredSpace(manifest.space(), "celld.space");
        this.connector = new ConnectorRuntime(space, group.discovery(), fleet.identity(),
                manifest.connectorName(), provider, group.id(), group.clock(), manifest.freshness());
        if (manifest.fleetAuthorizer()) {
            connector.authorizer(fleet.authorizer());
        }
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

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        connector.start();
        started = true;
        LOG.info("celld connector {} serving {} route group(s) of {} at {} in group {}", manifest.connectorName(),
                manifest.routes().size(), manifest.application(), manifest.baseUrl(), fleet.groupName());
    }

    @Override
    public void refresh() {
        if (started) {
            connector.refreshCards();
        }
    }

    @Override
    public synchronized void stop() {
        if (started) {
            connector.close();
            started = false;
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Starts a fleet runtime from the fleet file, adds the connector for the
     * manifest, and returns the runtime; closing it stops the connector.
     *
     * @param fleetFile    the fleet YAML ({@code agentspaces:} subtree)
     * @param manifestFile the manifest YAML, or {@code null} to read the fleet file's {@code celld:} subtree
     * @return the running fleet runtime
     */
    public static FleetRuntime run(Path fleetFile, Path manifestFile) {
        Map<String, Object> document = PropertiesLoader.tree(fleetFile);
        CelldManifest manifest = manifestFile != null ? CelldManifest.load(manifestFile)
                : CelldManifest.fromDocument(document);
        FleetRuntime runtime = FleetRuntime.start(PropertiesLoader.map(PropertiesLoader.agentspacesSubtree(document)));
        try {
            FleetContext fleet = manifest.group() == null ? runtime.context() : runtime.context(manifest.group());
            runtime.participant(new CelldConnector(fleet, manifest));
        } catch (RuntimeException e) {
            runtime.close();
            throw e;
        }
        return runtime;
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
