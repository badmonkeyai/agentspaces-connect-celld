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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

/**
 * A response body bounded as it is read (CC-B-01, CC-B-03, CC-B-07): more than
 * {@code max} bytes, or a body still open at the deadline, fails the read with
 * a {@link CelldClient.RouteException} as the cause, naming the bound. The
 * count never depends on {@code Content-Length}. A watchdog closes the
 * connection at the deadline, so a stalled body cannot hold a read forever.
 */
final class BoundedBody extends FilterInputStream {

    /** Thrown from a read past a bound; its cause is the {@link CelldClient.RouteException}. */
    static final class Exceeded extends IOException {
        Exceeded(CelldClient.RouteException cause) {
            super(cause.getMessage(), cause);
        }

        CelldClient.RouteException route() {
            return (CelldClient.RouteException) getCause();
        }
    }

    private final long max;
    private final String where;
    private final Duration transfer;
    private final Thread watchdog;
    private volatile boolean expired;
    private long count;

    BoundedBody(InputStream in, long max, Duration transfer, String where) {
        super(in);
        this.max = max;
        this.where = where;
        this.transfer = transfer;
        this.watchdog = Thread.ofVirtual().name("celld-transfer-watchdog").start(() -> {
            try {
                Thread.sleep(transfer);
            } catch (InterruptedException e) {
                return;
            }
            expired = true;
            try {
                in.close();
            } catch (IOException e) {
                // the read that follows fails anyway
            }
        });
    }

    /** The bytes read so far. */
    long count() {
        return count;
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n;
        try {
            n = super.read(b, off, len);
        } catch (IOException e) {
            throw expired ? timeout(e) : e;
        }
        if (expired) {
            throw timeout(null);
        }
        if (n > 0) {
            count += n;
            if (count > max) {
                throw new Exceeded(new CelldClient.RouteException("target-error", "artifacts.max-bytes",
                        where + " answered more than " + max + " bytes", null));
            }
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        throw new IOException("a bounded body is read, never skipped");
    }

    @Override
    public void close() throws IOException {
        watchdog.interrupt();
        super.close();
    }

    private Exceeded timeout(IOException cause) {
        return new Exceeded(new CelldClient.RouteException("timeout", "timeouts.transfer",
                where + " did not finish its body within " + transfer, cause));
    }
}
