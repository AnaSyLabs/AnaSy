import { NextResponse } from "next/server";
import { DEFAULT_DSL, parseMedallion } from "@/lib/medallion";

export const dynamic = "force-dynamic";

/** Validate DSL without touching ClickHouse. */
export async function POST(req: Request) {
  try {
    const body = (await req.json()) as { dsl?: string };
    const dsl = body.dsl ?? DEFAULT_DSL;
    const spec = parseMedallion(dsl);
    return NextResponse.json({
      ok: true,
      silver: spec.silver.map((s) => ({ table: s.table, topic: s.topic, fields: s.fields.length })),
      gold: spec.gold.map((g) => ({ table: g.table, from: g.from, metrics: g.metrics.length })),
    });
  } catch (e) {
    return NextResponse.json(
      { ok: false, error: e instanceof Error ? e.message : String(e) },
      { status: 400 },
    );
  }
}
