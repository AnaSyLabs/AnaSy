/** Runnable check: node --experimental-strip-types lib/medallion.check.ts */
import { DEFAULT_DSL, goldDdl, parseMedallion, silverDdl } from "./medallion.ts";

const spec = parseMedallion(DEFAULT_DSL);
if (spec.silver.length !== 1 || spec.gold.length !== 1) throw new Error("expected 1 silver + 1 gold");
const s = silverDdl(spec.silver[0]);
const g = goldDdl(spec.gold[0]);
for (const sql of [s.tableSql, s.mvSql, s.backfillSql, g.tableSql, g.mvSql, g.querySql]) {
  if (!sql.includes("anas.")) throw new Error("missing anas. qualifier");
}
if (!s.mvSql.includes("JSONExtractString(payload, 'customerName')")) throw new Error("bad extract");
if (!g.querySql.includes("countMerge(orders)")) throw new Error("bad merge");
console.log("medallion dsl ok");
