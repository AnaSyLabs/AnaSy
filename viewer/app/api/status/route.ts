import { NextResponse } from "next/server";
import { clickhouseQuery, duckdbGet } from "@/lib/env";
import { ensureKafkaConsumer, kafkaSnapshot } from "@/lib/kafka";

export const dynamic = "force-dynamic";

async function probeClickhouse() {
  try {
    const ping = await clickhouseQuery("SELECT 1");
    const raw = await clickhouseQuery(
      "SELECT event_id, topic, kafka_offset, toString(event_ts) AS event_ts, payload FROM anas.fat_events ORDER BY event_ts DESC LIMIT 50 FORMAT JSONEachRow",
    );
    const rows = raw
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
    return { ok: ping.trim() === "1", rows };
  } catch (e) {
    return { ok: false, rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

async function probeDuckdb() {
  try {
    const health = await duckdbGet("/internal/health");
    if (!health.ok) throw new Error(`duckdb sink ${health.status}`);
    const body = await health.json();
    const events = await duckdbGet("/internal/events");
    const rows = events.ok ? await events.json() : [];
    return { ok: Boolean(body.ok), rows, count: body.rows };
  } catch (e) {
    return { ok: false, rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

export async function GET() {
  await ensureKafkaConsumer();
  const kafka = kafkaSnapshot();
  const [clickhouse, duckdb] = await Promise.all([probeClickhouse(), probeDuckdb()]);
  return NextResponse.json({
    kafka: { ok: kafka.ok, error: kafka.error, rows: kafka.recent },
    clickhouse,
    duckdb,
  });
}
