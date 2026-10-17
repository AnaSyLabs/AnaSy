import { NextResponse } from "next/server";
import { clickhouseQuery } from "@/lib/env";
import { goldDdl, parseMedallion, silverDdl } from "@/lib/medallion";

export const dynamic = "force-dynamic";

async function exec(sql: string) {
  await clickhouseQuery(sql);
}

export async function POST(req: Request) {
  try {
    const body = (await req.json()) as {
      dsl?: string;
      backfill?: boolean;
      tier?: "silver" | "gold" | "all";
    };
    const dsl = body.dsl?.trim();
    if (!dsl) return NextResponse.json({ error: "dsl required" }, { status: 400 });

    const tier = body.tier ?? "all";
    const backfill = body.backfill !== false;
    const spec = parseMedallion(dsl);
    const applied: { tier: string; table: string; steps: string[] }[] = [];

    if (tier === "silver" || tier === "all") {
      for (const s of spec.silver) {
        const ddl = silverDdl(s);
        const steps: string[] = [];
        await exec(ddl.tableSql);
        steps.push("table");
        await exec(ddl.mvSql);
        steps.push("mv");
        if (backfill) {
          await exec(ddl.backfillSql);
          steps.push("backfill");
        }
        applied.push({ tier: "silver", table: s.table, steps });
      }
    }

    if (tier === "gold" || tier === "all") {
      for (const g of spec.gold) {
        const ddl = goldDdl(g, spec.silver);
        const steps: string[] = [];
        await exec(ddl.tableSql);
        steps.push("table");
        await exec(ddl.mvSql);
        steps.push("mv");
        if (backfill) {
          // Not idempotent, so clear first. Reads silver FINAL, which also drops redelivered duplicates.
          await exec(`TRUNCATE TABLE IF EXISTS anas.${g.table}`);
          await exec(ddl.backfillSql);
          steps.push("backfill");
        }
        applied.push({ tier: "gold", table: g.table, steps });
      }
    }

    return NextResponse.json({ ok: true, applied });
  } catch (e) {
    return NextResponse.json(
      { ok: false, error: e instanceof Error ? e.message : String(e) },
      { status: 400 },
    );
  }
}
