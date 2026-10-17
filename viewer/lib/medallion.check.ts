/** Runnable check: node --experimental-strip-types lib/medallion.check.ts */
import { DEFAULT_DSL, goldDdl, parseMedallion, silverDdl } from "./medallion.ts";

const spec = parseMedallion(DEFAULT_DSL);
if (spec.silver.length !== 1 || spec.gold.length !== 1) throw new Error("expected 1 silver + 1 gold");
const s = silverDdl(spec.silver[0]);
const g = goldDdl(spec.gold[0], spec.silver);
for (const sql of [s.tableSql, s.mvSql, s.backfillSql, g.tableSql, g.mvSql, g.querySql]) {
  if (!sql.includes("anas.")) throw new Error("missing anas. qualifier");
}
if (!s.mvSql.includes("JSONExtractString(payload, 'customerName')")) throw new Error("bad extract");
if (!g.querySql.includes("countMerge(orders)")) throw new Error("bad merge");
if (!g.tableSql.includes("event_date Date")) throw new Error("event_date should be Date");
if (!g.backfillSql.includes("FROM anas.silver_orders FINAL")) throw new Error("gold backfill must read FINAL");

const typed = parseMedallion(`silver s from t
  region:Int64 = region
  amount:Float64 = amount
gold g from s
  group region
  n:count`);
if (!goldDdl(typed.gold[0], typed.silver).tableSql.includes("region Int64")) throw new Error("group column keeps silver type");

const bad = parseMedallion(`silver s from t
  amount:Float64 = amount
gold g from s
  group nope
  n:count`);
let rejected = false;
try { goldDdl(bad.gold[0], bad.silver); } catch { rejected = true; }
if (!rejected) throw new Error("unknown group column should be rejected");

console.log("medallion dsl ok");
