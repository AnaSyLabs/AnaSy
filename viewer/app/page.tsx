"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type Store = "kafka" | "clickhouse" | "duckdb";

type Status = {
  kafka: { ok: boolean; error?: string; rows: Row[] };
  clickhouse: { ok: boolean; error?: string; rows: Row[] };
  duckdb: { ok: boolean; error?: string; rows: Row[]; count?: number };
};

type Row = {
  event_id: string;
  topic?: string;
  kafka_offset?: number;
  event_ts?: string;
  payload?: string;
};

type Lookup = {
  id: string;
  where: { kafka: boolean; clickhouse: boolean; duckdb: boolean };
};

const STORES: { id: Store; title: string; blurb: string }[] = [
  { id: "kafka", title: "Kafka", blurb: "The product boundary. Key is eventId." },
  { id: "clickhouse", title: "ClickHouse", blurb: "anas-sink-clickhouse · ReplacingMergeTree" },
  { id: "duckdb", title: "DuckDB", blurb: "anas-sink-duckdb · INSERT OR REPLACE" },
];

export default function Home() {
  const [store, setStore] = useState<Store>("kafka");
  const [status, setStatus] = useState<Status | null>(null);
  const [id, setId] = useState("");
  const [lookup, setLookup] = useState<Lookup | null>(null);
  const [posting, setPosting] = useState(false);

  const refresh = useCallback(async () => {
    const res = await fetch("/api/status", { cache: "no-store" });
    setStatus(await res.json());
  }, []);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 4000);
    return () => clearInterval(t);
  }, [refresh]);

  async function onLookup(e: React.FormEvent) {
    e.preventDefault();
    if (!id.trim()) return;
    const res = await fetch(`/api/lookup?id=${encodeURIComponent(id.trim())}`);
    setLookup(await res.json());
  }

  async function publishSample() {
    setPosting(true);
    try {
      const res = await fetch("/api/publish", { method: "POST" });
      const body = await res.json();
      if (body.id) {
        setId(body.id);
        setLookup(null);
        await refresh();
      }
    } finally {
      setPosting(false);
    }
  }

  const rows = status?.[store].rows ?? [];
  const clock = useMemo(
    () => new Date().toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit" }),
    [status],
  );

  return (
    <div className="shell">
      <header className="top">
        <div className="logo">ANASy.</div>
        <div className="meta">
          <span><i className={`dot ${status?.kafka.ok ? "ok" : "bad"}`} />Kafka</span>
          <span><i className={`dot ${status?.clickhouse.ok ? "ok" : "bad"}`} />ClickHouse</span>
          <span><i className={`dot ${status?.duckdb.ok ? "ok" : "bad"}`} />DuckDB</span>
          <span><i className="dot ok" />{clock} IST</span>
        </div>
      </header>

      <section className="hero">
        <h1>Where is the event.</h1>
        <p>
          Fat events leave OLTP once. Kafka fans them out. Each warehouse is its own
          consumer group. Look up an <code>event_id</code> to see which hop has it.
        </p>
      </section>

      <form className="search" onSubmit={onLookup}>
        <input
          value={id}
          onChange={(e) => setId(e.target.value)}
          placeholder="event id — Kafka key"
        />
        <button type="submit">Find</button>
      </form>

      {lookup && (
        <div className="where">
          {STORES.map((s) => (
            <div className="chip" key={s.id}>
              <b>{s.title}</b>
              <span>{lookup.where[s.id] ? "found" : "not yet"}</span>
            </div>
          ))}
        </div>
      )}

      <div className="cards">
        {STORES.map((s) => (
          <button
            key={s.id}
            className={`card ${store === s.id ? "active" : ""}`}
            onClick={() => setStore(s.id)}
          >
            <h3>{s.title}</h3>
            <p>{s.blurb}</p>
          </button>
        ))}
      </div>

      <div className="panel">
        <h2>{STORES.find((s) => s.id === store)?.title}</h2>
        <p className="desc">
          {status?.[store].ok ? "Live · last 50" : status?.[store].error || "Unreachable"}
        </p>
        {rows.length === 0 ? (
          <div className="empty">No rows yet. Publish an order, then wait for the sink group.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>event_id</th>
                <th>topic</th>
                <th>payload</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 12).map((r, i) => (
                <tr key={r.event_id + i}>
                  <td>{r.event_id}</td>
                  <td>{r.topic}</td>
                  <td>{typeof r.payload === "string" ? r.payload : JSON.stringify(r.payload)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <div className="post">
          <button type="button" onClick={publishSample} disabled={posting}>
            {posting ? "Publishing…" : "Publish sample order ↗"}
          </button>
        </div>
      </div>

      <nav className="dock">
        <button className="on" title="Home">⌂</button>
        <button title="Kafka" onClick={() => setStore("kafka")}>⌁</button>
        <button title="ClickHouse" onClick={() => setStore("clickhouse")}>▣</button>
        <button title="DuckDB" onClick={() => setStore("duckdb")}>▦</button>
      </nav>
    </div>
  );
}
