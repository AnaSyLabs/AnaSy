import { NextResponse } from "next/server";
import { clickhouseQuery } from "@/lib/env";

export const dynamic = "force-dynamic";

async function count(sql: string): Promise<number | null> {
  try {
    const text = await clickhouseQuery(sql);
    return Number(text.trim());
  } catch {
    return null;
  }
}

export async function GET() {
  const bronze = await count("SELECT count() FROM anas.fat_events");
  const silverTables = await clickhouseQuery(
    "SELECT name FROM system.tables WHERE database = 'anas' AND name LIKE 'silver_%' AND engine NOT LIKE '%View' FORMAT JSONEachRow",
  ).catch(() => "");
  const goldTables = await clickhouseQuery(
    "SELECT name FROM system.tables WHERE database = 'anas' AND name LIKE 'gold_%' AND engine NOT LIKE '%View' FORMAT JSONEachRow",
  ).catch(() => "");

  const parseNames = (raw: string) =>
    raw
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line).name as string);

  const silver = [];
  for (const name of parseNames(silverTables)) {
    silver.push({ table: name, rows: await count(`SELECT count() FROM anas.${name}`) });
  }
  const gold = [];
  for (const name of parseNames(goldTables)) {
    gold.push({ table: name, rows: await count(`SELECT count() FROM anas.${name}`) });
  }

  return NextResponse.json({
    ok: bronze !== null,
    bronze: { table: "fat_events", rows: bronze },
    silver,
    gold,
  });
}
