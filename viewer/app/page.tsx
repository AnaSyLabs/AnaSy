"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { GoldChart } from "@/components/GoldChart";
import { DEFAULT_DSL } from "@/lib/medallion";

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

type MedallionStatus = {
  ok: boolean;
  bronze: { table: string; rows: number | null };
  silver: { table: string; rows: number | null }[];
  gold: { table: string; rows: number | null }[];
};

type GoldPayload = {
  ok: boolean;
  error?: string;
  table: string;
  groupBy: string[];
  metrics: string[];
  rows: Record<string, unknown>[];
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

  const [dsl, setDsl] = useState(DEFAULT_DSL);
  const [promoting, setPromoting] = useState(false);
  const [promoteMsg, setPromoteMsg] = useState<string | null>(null);
  const [medallion, setMedallion] = useState<MedallionStatus | null>(null);
  const [goldData, setGoldData] = useState<GoldPayload[]>([]);

  const refresh = useCallback(async () => {
    const res = await fetch("/api/status", { cache: "no-store" });
    setStatus(await res.json());
  }, []);

  const refreshMedallion = useCallback(async () => {
    const res = await fetch("/api/medallion/status", { cache: "no-store" });
    setMedallion(await res.json());
  }, []);

  const refreshGold = useCallback(async (src: string) => {
    const tables = [...src.matchAll(/^gold\s+(\S+)/gim)].map((m) => m[1]);
    const payloads = await Promise.all(
      tables.map(async (table) => {
        const q = new URLSearchParams({ table, dsl: src });
        const res = await fetch(`/api/medallion/gold?${q}`, { cache: "no-store" });
        return (await res.json()) as GoldPayload;
      }),
    );
    setGoldData(payloads.filter((p) => p.ok || p.error));
  }, []);

  useEffect(() => {
    refresh();
    refreshMedallion();
    const t = setInterval(() => {
      refresh();
      refreshMedallion();
    }, 4000);
    return () => clearInterval(t);
  }, [refresh, refreshMedallion]);

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

  async function promote(tier: "silver" | "gold" | "all") {
    setPromoting(true);
    setPromoteMsg(null);
    try {
      const res = await fetch("/api/medallion/promote", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dsl, backfill: true, tier }),
      });
      const body = await res.json();
      if (!body.ok) {
        setPromoteMsg(body.error || "promote failed");
        return;
      }
      const summary = (body.applied as { tier: string; table: string; steps: string[] }[])
        .map((a) => `${a.tier}:${a.table}[${a.steps.join("+")}]`)
        .join(", ");
      setPromoteMsg(summary || "ok");
      await refreshMedallion();
      await refreshGold(dsl);
    } finally {
      setPromoting(false);
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
          <span>
            <i className={`dot ${status?.kafka.ok ? "ok" : "bad"}`} />
            Kafka
          </span>
          <span>
            <i className={`dot ${status?.clickhouse.ok ? "ok" : "bad"}`} />
            ClickHouse
          </span>
          <span>
            <i className={`dot ${status?.duckdb.ok ? "ok" : "bad"}`} />
            DuckDB
          </span>
          <span>
            <i className="dot ok" />
            {clock} IST
          </span>
        </div>
      </header>

      <section className="hero">
        <h1>Where is the event.</h1>
        <p>
          Fat events leave OLTP once. Kafka fans them out. Each warehouse is its own consumer group.
          Look up an <code>event_id</code> to see which hop has it.
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

      <section className="medallion">
        <h2>Medallion</h2>
        <p className="lede">
          Define ClickHouse silver (typed from bronze payload) and gold (rollups) with a small DSL.
          Promote runs table + incremental MV, then one backfill <code>INSERT SELECT</code>.
        </p>

        <div className="tier-row">
          <div className="tier">
            <b>Bronze</b>
            <span>{medallion?.bronze?.rows ?? "—"} rows · fat_events</span>
          </div>
          <div className="tier">
            <b>Silver</b>
            <span>
              {medallion?.silver?.length
                ? medallion.silver.map((s) => `${s.table}:${s.rows}`).join(" · ")
                : "none yet"}
            </span>
          </div>
          <div className="tier">
            <b>Gold</b>
            <span>
              {medallion?.gold?.length
                ? medallion.gold.map((g) => `${g.table}:${g.rows}`).join(" · ")
                : "none yet"}
            </span>
          </div>
        </div>

        <label className="dsl-label" htmlFor="dsl">
          DSL
        </label>
        <textarea
          id="dsl"
          className="dsl"
          value={dsl}
          onChange={(e) => setDsl(e.target.value)}
          spellCheck={false}
          rows={12}
        />

        <div className="post medallion-actions">
          <button type="button" onClick={() => promote("silver")} disabled={promoting}>
            {promoting ? "Working…" : "Bronze → silver"}
          </button>
          <button type="button" onClick={() => promote("gold")} disabled={promoting}>
            Silver → gold
          </button>
          <button type="button" onClick={() => promote("all")} disabled={promoting}>
            Promote all
          </button>
          <button type="button" className="ghost" onClick={() => refreshGold(dsl)} disabled={promoting}>
            Refresh charts
          </button>
        </div>
        {promoteMsg && <p className="promote-msg">{promoteMsg}</p>}

        <div className="widgets">
          {goldData.map((g) => {
            const metric = g.metrics?.[0] ?? "value";
            const points =
              g.rows?.map((r) => ({
                label: String(r[g.groupBy[0]] ?? ""),
                value: Number(r[metric] ?? 0),
              })) ?? [];
            return (
              <GoldChart
                key={g.table}
                title={g.table}
                metric={g.error ? g.error : metric}
                points={g.error ? [] : points}
                embedUrl={`/widgets/${g.table}`}
              />
            );
          })}
        </div>
      </section>
    </div>
  );
}
