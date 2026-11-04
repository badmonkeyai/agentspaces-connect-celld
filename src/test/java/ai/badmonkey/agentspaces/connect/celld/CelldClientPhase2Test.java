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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The client's new paths against a Worker that strips {@code Content-Length} as celld does: ranged
 * reads accept only a matching 206 and miss on 412, 416, or a whole body (RFC 9110 §14, §13); a body
 * is bounded by bytes read and by the transfer deadline; hostile keys never reach a path; the event
 * stream follows the Server-Sent Events format.
 */
class CelldClientPhase2Test {

    private SwarmWorker worker;
    private CelldClient client;

    @BeforeEach
    void start() throws IOException {
        worker = new SwarmWorker();
        client = new CelldClient(CelldManifest.parse("""
                celld:
                  application: shop
                  base-url: %s
                  allow-plaintext: true
                  credential: { bearer: t }
                  timeouts: { transfer: 2s }
                  artifacts: { max-bytes: 1MiB }
                  routes:
                    - name: exports
                      operations:
                        - { name: endless, method: GET, path: /api/exports/endless, result: artifact, media-type: application/octet-stream }
                """.formatted(worker.baseUrl())));
    }

    @AfterEach
    void stop() {
        worker.close();
    }

    @Test
    void aRangedReadAcceptsOnlyAMatching206AndMissesOnEveryOtherAnswer() {
        byte[] object = SwarmWorker.random(100_000, 11);
        worker.putObject("k", object, Map.of());
        String etag = worker.objects.get("k").etag();
        assertThat(client.range("/api/models/object", "k", 1000, 500, etag))
                .isEqualTo(java.util.Arrays.copyOfRange(object, 1000, 1500));
        assertThatThrownBy(() -> client.range("/api/models/object", "k", 0, 10, "\"stale\""))
                .isInstanceOfSatisfying(CelldClient.RangeMiss.class, e -> assertThat(e.status()).isEqualTo(412));
        assertThatThrownBy(() -> client.range("/api/models/object", "k", 200_000, 10, etag))
                .isInstanceOfSatisfying(CelldClient.RangeMiss.class, e -> assertThat(e.status()).isEqualTo(416));
        assertThatThrownBy(() -> client.range("/api/models/object", "k", 99_990, 100, etag))
                .as("a shorter range than asked is a miss").isInstanceOf(CelldClient.RangeMiss.class);
        assertThatThrownBy(() -> client.range("/api/models/object", "absent", 0, 10, null))
                .isInstanceOfSatisfying(CelldClient.RangeMiss.class, e -> assertThat(e.status()).isEqualTo(404));
    }

    @Test
    @org.junit.jupiter.api.Timeout(30)
    void aBodyIsBoundedByTheBytesReadWhateverTheHeadersSay() throws Exception {
        CelldManifest.Operation endless = client.manifest().routes().getFirst().operation("endless");
        try (CelldClient.Streamed streamed = client.open(endless, Map.of(), null)) {
            assertThat(streamed.ok()).isTrue();
            InputStream body = streamed.body();
            assertThatThrownBy(() -> body.transferTo(java.io.OutputStream.nullOutputStream()))
                    .isInstanceOfSatisfying(BoundedBody.Exceeded.class,
                            e -> assertThat(e.route().rule()).isEqualTo("artifacts.max-bytes"));
        }
    }

    /** A body that sends ten bytes, then stalls until it is closed, as a hung connection does. */
    private static final class StalledBody extends InputStream {
        private final java.util.concurrent.CountDownLatch closed = new java.util.concurrent.CountDownLatch(1);
        private int sent;

        @Override
        public int read() throws IOException {
            if (sent < 10) {
                sent++;
                return 'x';
            }
            try {
                closed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IOException("closed");
        }

        @Override
        public void close() {
            closed.countDown();
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(20)
    void aStalledBodyEndsAtTheTransferDeadline() throws Exception {
        long start = System.nanoTime();
        try (BoundedBody body = new BoundedBody(new StalledBody(), 1 << 20, Duration.ofMillis(300), "GET /x")) {
            assertThat(body.readNBytes(10)).hasSize(10);
            assertThatThrownBy(body::read).isInstanceOfSatisfying(BoundedBody.Exceeded.class,
                    e -> assertThat(e.route().rule()).isEqualTo("timeouts.transfer"));
        }
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void hostileKeysAreRefusedAndOrdinaryOnesTravelEncoded() {
        assertThat(ObjectSources.acceptable("models/ckpt 7?.bin")).isTrue();
        assertThat(ObjectSources.acceptable("../../etc/passwd")).as("only ever a query value").isTrue();
        assertThat(ObjectSources.acceptable("")).isFalse();
        assertThat(ObjectSources.acceptable("a\nb")).isFalse();
        assertThat(ObjectSources.acceptable("a\u007fb")).isFalse();
        assertThat(ObjectSources.acceptable("x".repeat(1025))).isFalse();
        byte[] object = SwarmWorker.random(64, 12);
        worker.putObject("../../etc/passwd&key=other", object, Map.of());
        assertThat(client.range("/api/models/object", "../../etc/passwd&key=other", 0, 64,
                worker.objects.get("../../etc/passwd&key=other").etag()))
                .as("the key reaches the route verbatim as one query value").isEqualTo(object);
    }

    @Test
    @org.junit.jupiter.api.Timeout(30)
    void theEventStreamFollowsTheSseFormatAndResumesAfterTheLastId() {
        worker.setStock(1);
        worker.setStock(2);
        worker.setStock(3);
        List<CelldClient.Event> events = new ArrayList<>();
        client.events("/api/changes/stream", "1", events::add);
        assertThat(events).extracting(CelldClient.Event::id).containsExactly("2", "3");
        assertThat(events.getFirst().data()).contains("\"qty\":\"2\"");
    }
}
