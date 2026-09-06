import { NextResponse } from "next/server";
import { clickhouseQuery } from "@/lib/env";
import { DEFAULT_DSL, goldDdl, parseMedallion } from "@/lib/medallion";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const table = url.searchParams.get("table");
    const dsl = url.searchParams.get("dsl") || DEFAULT_DSL;

    if (!table) {
      const raw = await clickhouseQuery(
        "SELECT name FROM system.tables WHERE database = 'anas' AND name LIKE 'gold_%' ORDER BY name FORMAT JSONEachRow",
      );
      const tables = raw
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line).name as string);
      return NextResponse.json({ ok: true, tables });
    }

    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(table)) {
      return NextResponse.json({ error: "bad table" }, { status: 400 });
    }

    const spec = parseMedallion(dsl);
    const gold = spec.gold.find((g) => g.table === table);
    let querySql: string;
    let metrics: string[] = [];
    let groupBy: string[] = [];

    if (gold) {
      const ddl = goldDdl(gold);
      querySql = ddl.querySql;
      metrics = gold.metrics.map((m) => m.name);
      groupBy = gold.groupBy;
    } else {
      // Fallback: assume first column is label, rest are merged numerics already stored as simple types — not used for AggregateFunction.
      return NextResponse.json(
        { error: `table ${table} not in dsl; pass ?dsl= with matching gold block` },
        { status: 400 },
      );
    }

    const raw = await clickhouseQuery(querySql);
    const rows = raw
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));

    return NextResponse.json({ ok: true, table, groupBy, metrics, rows });
  } catch (e) {
    return NextResponse.json(
      { ok: false, error: e instanceof Error ? e.message : String(e) },
      { status: 400 },
    );
  }
}
