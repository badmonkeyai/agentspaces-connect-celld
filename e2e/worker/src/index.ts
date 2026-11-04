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

/**
 * The inventory application the end-to-end harness runs on a local celld. One Durable Object,
 * `Inventory`, owns the stock in its SQLite storage, records every write in a change log, answers
 * the snapshot, and receives artifacts exactly once per invocation id. The Worker serves the
 * nightly export as a generated stream and the `models` bucket by ranged reads. Each route is one
 * the Java connector's manifest (../manifest.yaml) allowlists.
 */

import { ChangeLog, receiveArtifact, receivePiece, serveChangeStream, serveRange } from "@agentspaces/workers/routes";
import type { RangeBucket } from "@agentspaces/workers/routes";

interface R2Like extends RangeBucket {
  put(key: string, value: ReadableStream<Uint8Array> | ArrayBuffer | Uint8Array | string,
      options?: { httpMetadata?: { contentType?: string } }): Promise<unknown>;
  list(options?: { limit?: number; startAfter?: string; include?: string[] }): Promise<{
    objects: { key: string; size: number; httpEtag: string; httpMetadata?: { contentType?: string };
               customMetadata?: Record<string, string> }[];
    truncated: boolean;
  }>;
  get(key: string, options?: { range?: { offset: number; length: number }; onlyIf?: { etagMatches?: string } }):
    Promise<({ size: number; httpEtag: string; body?: ReadableStream<Uint8Array> | null;
               arrayBuffer?(): Promise<ArrayBuffer> }) | null>;
}

interface Env {
  INVENTORY: { idFromName(name: string): unknown; get(id: unknown): { fetch(request: Request): Promise<Response> } };
  MODELS: R2Like;
  INGEST: R2Like;
}

interface SqlCursor<T> extends Iterable<T> {
  toArray(): T[];
  one(): T;
}

interface Sql {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlCursor<T>;
}

interface State {
  storage: { sql: Sql };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/api/exports/nightly":
        return nightly(Number(url.searchParams.get("mb") ?? "8"));
      case "/api/models":
        return listModels(env.MODELS, url);
      case "/api/models/object":
        return serveRange(env.MODELS, request, url.searchParams.get("key") ?? "");
      case "/api/health":
        return Response.json({ ok: true });
      default:
        // Stock, snapshot, changes, and ingest belong to the one Inventory object.
        return env.INVENTORY.get(env.INVENTORY.idFromName("inventory")).fetch(request);
    }
  },
};

/** The nightly export: `mb` MiB of NDJSON, generated as it streams, the same bytes every time. */
function nightly(mb: number): Response {
  const total = Math.max(0, Math.min(mb, 2048)) * 1024 * 1024;
  const encoder = new TextEncoder();
  let sent = 0;
  let line = 0;
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (sent >= total) {
        controller.close();
        return;
      }
      let chunk = "";
      while (chunk.length < 65536) {
        chunk += JSON.stringify({ line, sku: `S-${line % 9973}`, qty: (line * 7919) % 1000 }) + "\n";
        line += 1;
      }
      const bytes = encoder.encode(chunk).subarray(0, Math.min(65536 * 2, total - sent));
      sent += bytes.length;
      controller.enqueue(bytes);
    },
  });
  return new Response(stream, { headers: { "Content-Type": "application/x-ndjson" } });
}

/** The object list the connector's object source reads: pages of {key, size, etag, mediaType, attrs}. */
async function listModels(bucket: R2Like, url: URL): Promise<Response> {
  const limit = Math.max(1, Math.min(Number(url.searchParams.get("limit") ?? "1000"), 1000));
  const after = url.searchParams.get("after") ?? undefined;
  const page = await bucket.list({ limit, ...(after ? { startAfter: after } : {}),
    include: ["httpMetadata", "customMetadata"] });
  const items = page.objects.map((o) => ({
    key: o.key, size: o.size, etag: o.httpEtag,
    mediaType: o.httpMetadata?.contentType ?? "application/octet-stream",
    attrs: o.customMetadata ?? {},
  }));
  const last = items.length ? items[items.length - 1]!.key : undefined;
  return Response.json({ items, ...(page.truncated && last ? { after: last } : {}) });
}

/** The Durable Object that owns the stock, its change log, and the artifacts it received. */
export class Inventory {
  private readonly sql: Sql;
  private readonly log: ChangeLog;
  private readonly pending = new Map<string, Promise<string>>();

  constructor(state: State, private readonly env: Env) {
    this.sql = state.storage.sql;
    this.log = new ChangeLog(this.sql as never);
    this.sql.exec("CREATE TABLE IF NOT EXISTS stock (sku TEXT PRIMARY KEY, qty INTEGER NOT NULL)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS once_results (id TEXT PRIMARY KEY, result TEXT NOT NULL)");
    this.sql.exec("INSERT OR IGNORE INTO stock (sku, qty) VALUES ('A-1', 40), ('B-2', 0), ('C-3', 12)");
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/api/stock":
        return Response.json(this.stock(url.searchParams.get("sku")));
      case "/api/stock/reserve":
        return this.once(request, async () => this.reserve(await request.json() as Record<string, string>));
      case "/api/stock/set":
        return this.once(request, async () => this.set(await request.json() as Record<string, string>));
      case "/api/stock/snapshot":
        return this.snapshot();
      case "/api/changes":
        return this.log.respond(request);
      case "/api/changes/stream":
        return serveChangeStream(this.log, request, { maxMillis: 50_000 });
      case "/api/ingest":
        return this.ingest(request, url);
      case "/api/ingested":
        return this.ingested(url.searchParams.get("key") ?? "");
      default:
        return new Response("not found", { status: 404 });
    }
  }

  private stock(sku: string | null) {
    const rows = sku === null
      ? this.sql.exec<{ sku: string; qty: number }>("SELECT sku, qty FROM stock ORDER BY sku").toArray()
      : this.sql.exec<{ sku: string; qty: number }>("SELECT sku, qty FROM stock WHERE sku = ?", sku).toArray();
    return rows.map((r) => ({ sku: r.sku, qty: String(r.qty) }));
  }

  private reserve(body: Record<string, string>) {
    const sku = body.sku ?? "";
    const qty = Number(body.qty ?? "0");
    const row = this.sql.exec<{ qty: number }>("SELECT qty FROM stock WHERE sku = ?", sku).toArray()[0];
    if (!row || row.qty < qty) {
      return { reserved: "0", remaining: String(row?.qty ?? 0) };
    }
    this.sql.exec("UPDATE stock SET qty = qty - ? WHERE sku = ?", qty, sku);
    this.log.record({ kind: "upsert", route: "stock", key: sku, row: { sku, qty: String(row.qty - qty) } });
    return { reserved: String(qty), remaining: String(row.qty - qty) };
  }

  private set(body: Record<string, string>) {
    const sku = body.sku ?? "";
    const qty = Number(body.qty ?? "0");
    this.sql.exec("INSERT INTO stock (sku, qty) VALUES (?, ?) ON CONFLICT(sku) DO UPDATE SET qty = excluded.qty",
      sku, qty);
    this.log.record({ kind: "upsert", route: "stock", key: sku, row: { sku, qty: String(qty) } });
    return { sku, qty: String(qty) };
  }

  /** One consistent view: every row read synchronously inside this request, then sent. */
  private snapshot(): Response {
    const lines = this.stock(null).map((r) => JSON.stringify(r)).join("\n") + "\n";
    return new Response(lines, { headers: { "Content-Type": "application/x-ndjson" } });
  }

  private async ingest(request: Request, url: URL): Promise<Response> {
    const root = request.headers.get("Aspace-Content-Root") ?? "none";
    const key = `ingest/${root}`;
    const batch = url.searchParams.get("batch") ?? "";
    if (request.headers.get("Aspace-Upload")) {
      // Pieces are idempotent by index; only the finish step needs once.
      if (request.headers.get("Aspace-Upload") === "piece") {
        await receivePiece(request, this.env.INGEST as never, key);
        return Response.json({});
      }
      return this.once(request, async () => {
        await receivePiece(request, this.env.INGEST as never, key);
        return { stored: key, batch, ...(await this.digest(key)) };
      });
    }
    return this.once(request, async () => {
      const received = await receiveArtifact(request, this.env.INGEST, key);
      return { stored: key, batch, bytes: String(received.bytes), ...(await this.digest(key)) };
    });
  }

  private async ingested(key: string): Promise<Response> {
    return Response.json(await this.digest(key));
  }

  /** The SHA-256 of a stored artifact, whole or reassembled from its pieces. */
  private async digest(key: string): Promise<Record<string, string>> {
    const index = await this.env.INGEST.get(`${key}/pieces.json`);
    const parts: string[] = index?.arrayBuffer
      ? (JSON.parse(new TextDecoder().decode(await index.arrayBuffer())) as { pieces: string[] }).pieces : [key];
    const chunks: Uint8Array[] = [];
    let size = 0;
    for (const part of parts) {
      const object = await this.env.INGEST.get(part);
      if (!object?.arrayBuffer) {
        return { sha256: "", size: "0" };
      }
      const bytes = new Uint8Array(await object.arrayBuffer());
      chunks.push(bytes);
      size += bytes.length;
    }
    const whole = new Uint8Array(size);
    let at = 0;
    for (const c of chunks) {
      whole.set(c, at);
      at += c.length;
    }
    const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", whole));
    return { sha256: [...hash].map((b) => b.toString(16).padStart(2, "0")).join(""), size: String(size) };
  }

  /** Runs `fn` once per Aspace-Invocation-Id, in this object's SQLite; a retry gets the stored answer. */
  private async once(request: Request, fn: () => Promise<Record<string, string>>): Promise<Response> {
    const id = request.headers.get("Aspace-Invocation-Id");
    if (!id) {
      return Response.json(await fn());
    }
    const stored = this.sql.exec<{ result: string }>("SELECT result FROM once_results WHERE id = ?", id).toArray()[0];
    if (stored) {
      return new Response(stored.result, { headers: { "Content-Type": "application/json" } });
    }
    let running = this.pending.get(id);
    if (!running) {
      running = fn().then((result) => {
        const text = JSON.stringify(result);
        this.sql.exec("INSERT OR IGNORE INTO once_results (id, result) VALUES (?, ?)", id, text);
        return text;
      }).finally(() => this.pending.delete(id));
      this.pending.set(id, running);
    }
    return new Response(await running, { headers: { "Content-Type": "application/json" } });
  }
}
