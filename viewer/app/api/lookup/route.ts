import { NextRequest, NextResponse } from "next/server";
import { clickhouseQuery, duckdbGet } from "@/lib/env";
import { ensureKafkaConsumer, kafkaSnapshot } from "@/lib/kafka";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  const id = req.nextUrl.searchParams.get("id")?.trim();
  if (!id) return NextResponse.json({ error: "id required" }, { status: 400 });

  await ensureKafkaConsumer();
  const kafka = kafkaSnapshot().byId.get(id) ?? null;

  let clickhouse = null;
  try {
    const raw = await clickhouseQuery(
      `SELECT event_id, topic, kafka_offset, toString(event_ts) AS event_ts, payload FROM anas.fat_events WHERE event_id = '${id.replaceAll("'", "")}' LIMIT 1 FORMAT JSONEachRow`,
    );
    const line = raw.trim().split("\n").filter(Boolean)[0];
    clickhouse = line ? JSON.parse(line) : null;
  } catch {
    clickhouse = null;
  }

  let duckdb = null;
  try {
    const res = await duckdbGet(`/internal/events/${encodeURIComponent(id)}`);
    if (res.ok) {
      const body = await res.json();
      duckdb = body && body.event_id ? body : null;
    }
  } catch {
    duckdb = null;
  }

  return NextResponse.json({
    id,
    kafka: kafka !== null,
    clickhouse,
    duckdb,
    where: {
      kafka: kafka !== null,
      clickhouse: clickhouse !== null,
      duckdb: duckdb !== null,
    },
  });
}
