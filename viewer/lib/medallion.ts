/** Tiny DSL → ClickHouse silver/gold DDL. See docs/medallion.md. */

export type FieldType = "String" | "Float64" | "Int64" | "DateTime" | "UUID" | "Bool";

export type SilverField = { name: string; type: FieldType; path: string };

export type SilverSpec = {
  kind: "silver";
  table: string;
  topic: string;
  fields: SilverField[];
};

export type GoldMetric =
  | { name: string; fn: "count" }
  | { name: string; fn: "sum" | "avg"; column: string };

export type GoldSpec = {
  kind: "gold";
  table: string;
  from: string;
  groupBy: string[];
  metrics: GoldMetric[];
};

export type MedallionSpec = { silver: SilverSpec[]; gold: GoldSpec[] };

const IDENT = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const TABLE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const TOPIC = /^[a-zA-Z0-9._-]+$/;
const TYPES = new Set<FieldType>(["String", "Float64", "Int64", "DateTime", "UUID", "Bool"]);

export const DEFAULT_DSL = `# bronze → silver (one row per event_id)
silver silver_orders from orders.events
  customer:String = customerName
  amount:Float64 = totalAmount

# silver → gold (dashboard rollup)
gold gold_orders_daily from silver_orders
  group event_date
  orders:count
  revenue:sum(amount)
`;

function fail(msg: string, line: number): never {
  throw new Error(`line ${line}: ${msg}`);
}

function extractFn(type: FieldType, path: string): string {
  const lit = path.replace(/'/g, "\\'");
  switch (type) {
    case "String":
      return `JSONExtractString(payload, '${lit}')`;
    case "Float64":
      return `JSONExtractFloat(payload, '${lit}')`;
    case "Int64":
      return `JSONExtractInt(payload, '${lit}')`;
    case "Bool":
      return `JSONExtractBool(payload, '${lit}')`;
    case "UUID":
      return `toUUIDOrZero(JSONExtractString(payload, '${lit}'))`;
    case "DateTime":
      return `parseDateTimeBestEffortOrZero(JSONExtractString(payload, '${lit}'))`;
  }
}

export function parseMedallion(dsl: string): MedallionSpec {
  const silver: SilverSpec[] = [];
  const gold: GoldSpec[] = [];
  let cur: SilverSpec | GoldSpec | null = null;
  const lines = dsl.split(/\r?\n/);

  for (let i = 0; i < lines.length; i++) {
    const n = i + 1;
    const raw = lines[i];
    const line = raw.replace(/#.*$/, "").trim();
    if (!line) continue;

    const silverHead = /^silver\s+(\S+)\s+from\s+(\S+)$/i.exec(line);
    if (silverHead) {
      const table = silverHead[1];
      const topic = silverHead[2];
      if (!TABLE.test(table)) fail(`bad silver table '${table}'`, n);
      if (!TOPIC.test(topic)) fail(`bad topic '${topic}'`, n);
      cur = { kind: "silver", table, topic, fields: [] };
      silver.push(cur);
      continue;
    }

    const goldHead = /^gold\s+(\S+)\s+from\s+(\S+)$/i.exec(line);
    if (goldHead) {
      const table = goldHead[1];
      const from = goldHead[2];
      if (!TABLE.test(table)) fail(`bad gold table '${table}'`, n);
      if (!TABLE.test(from)) fail(`bad gold source '${from}'`, n);
      cur = { kind: "gold", table, from, groupBy: [], metrics: [] };
      gold.push(cur);
      continue;
    }

    if (!cur) fail("expected 'silver …' or 'gold …' header", n);

    if (cur.kind === "silver") {
      const m = /^(\w+)\s*:\s*(\w+)\s*=\s*(\S+)$/.exec(line);
      if (!m) fail("expected 'name:Type = jsonPath'", n);
      const name = m[1];
      const type = m[2] as FieldType;
      const path = m[3];
      if (!IDENT.test(name)) fail(`bad column '${name}'`, n);
      if (!TYPES.has(type)) fail(`unsupported type '${type}'`, n);
      if (!/^[a-zA-Z0-9_.]+$/.test(path)) fail(`bad json path '${path}'`, n);
      cur.fields.push({ name, type, path });
      continue;
    }

    const group = /^group\s+(.+)$/i.exec(line);
    if (group) {
      const cols = group[1].split(/[,\s]+/).filter(Boolean);
      for (const c of cols) {
        if (!IDENT.test(c)) fail(`bad group column '${c}'`, n);
      }
      cur.groupBy.push(...cols);
      continue;
    }

    const count = /^(\w+)\s*:\s*count$/i.exec(line);
    if (count) {
      if (!IDENT.test(count[1])) fail(`bad metric '${count[1]}'`, n);
      cur.metrics.push({ name: count[1], fn: "count" });
      continue;
    }

    const agg = /^(\w+)\s*:\s*(sum|avg)\((\w+)\)$/i.exec(line);
    if (agg) {
      if (!IDENT.test(agg[1]) || !IDENT.test(agg[3])) fail("bad metric", n);
      cur.metrics.push({
        name: agg[1],
        fn: agg[2].toLowerCase() as "sum" | "avg",
        column: agg[3],
      });
      continue;
    }

    fail("expected group / metric under gold, or field under silver", n);
  }

  for (const s of silver) {
    if (s.fields.length === 0) throw new Error(`silver ${s.table}: need at least one field`);
  }
  for (const g of gold) {
    if (g.groupBy.length === 0) throw new Error(`gold ${g.table}: need 'group …'`);
    if (g.metrics.length === 0) throw new Error(`gold ${g.table}: need at least one metric`);
  }
  return { silver, gold };
}

export function silverDdl(spec: SilverSpec): { tableSql: string; mvSql: string; backfillSql: string } {
  const cols = spec.fields
    .map((f) => `    ${f.name} ${f.type}`)
    .join(",\n");
  const tableSql = `
CREATE TABLE IF NOT EXISTS anas.${spec.table}
(
    event_id    UUID,
    event_date  Date,
    event_ts    DateTime,
${cols},
    ingested_at DateTime
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_id)
`.trim();

  const selects = spec.fields
    .map((f) => `    ${extractFn(f.type, f.path)} AS ${f.name}`)
    .join(",\n");
  const mvSql = `
CREATE MATERIALIZED VIEW IF NOT EXISTS anas.${spec.table}_mv TO anas.${spec.table} AS
SELECT
    event_id,
    event_date,
    event_ts,
${selects},
    ingested_at
FROM anas.fat_events
WHERE topic = '${spec.topic.replace(/'/g, "\\'")}'
`.trim();

  const backfillSql = `
INSERT INTO anas.${spec.table}
SELECT
    event_id,
    event_date,
    event_ts,
${selects},
    ingested_at
FROM anas.fat_events
WHERE topic = '${spec.topic.replace(/'/g, "\\'")}'
`.trim();

  return { tableSql, mvSql, backfillSql };
}

const BASE_COLUMNS: Record<string, string> = { event_id: "UUID", event_date: "Date", event_ts: "DateTime" };

/** Column types of the gold source. Unknown when the source is not defined in the same DSL. */
function sourceColumns(spec: GoldSpec, silver: SilverSpec[]): Record<string, string> | null {
  const src = silver.find((s) => s.table === spec.from);
  if (!src) return null;
  return { ...BASE_COLUMNS, ...Object.fromEntries(src.fields.map((f) => [f.name, f.type])) };
}

export function goldDdl(
  spec: GoldSpec,
  silver: SilverSpec[] = [],
): { tableSql: string; mvSql: string; backfillSql: string; querySql: string } {
  const known = sourceColumns(spec, silver);
  const used = [...spec.groupBy, ...spec.metrics.flatMap((m) => ("column" in m ? [m.column] : []))];
  for (const c of used) {
    if (known && !(c in known)) throw new Error(`gold ${spec.table}: '${c}' is not a column of ${spec.from}`);
  }
  const groupCols = spec.groupBy.map((c) => `    ${c} ${known?.[c] ?? BASE_COLUMNS[c] ?? "String"}`).join(",\n");
  const metricCols = spec.metrics
    .map((m) => {
      if (m.fn === "count") return `    ${m.name} AggregateFunction(count)`;
      if (m.fn === "sum") return `    ${m.name} AggregateFunction(sum, Float64)`;
      return `    ${m.name} AggregateFunction(avg, Float64)`;
    })
    .join(",\n");

  const tableSql = `
CREATE TABLE IF NOT EXISTS anas.${spec.table}
(
${groupCols},
${metricCols}
)
ENGINE = AggregatingMergeTree()
ORDER BY (${spec.groupBy.join(", ")})
`.trim();

  const states = spec.metrics
    .map((m) => {
      if (m.fn === "count") return `    countState() AS ${m.name}`;
      if (m.fn === "sum") return `    sumState(toFloat64(${m.column})) AS ${m.name}`;
      return `    avgState(toFloat64(${m.column})) AS ${m.name}`;
    })
    .join(",\n");

  const mvSql = `
CREATE MATERIALIZED VIEW IF NOT EXISTS anas.${spec.table}_mv TO anas.${spec.table} AS
SELECT
    ${spec.groupBy.join(", ")},
${states}
FROM anas.${spec.from}
GROUP BY ${spec.groupBy.join(", ")}
`.trim();

  const backfillSql = `
INSERT INTO anas.${spec.table}
SELECT
    ${spec.groupBy.join(", ")},
${states}
FROM anas.${spec.from} FINAL
GROUP BY ${spec.groupBy.join(", ")}
`.trim();

  const merges = spec.metrics
    .map((m) => {
      if (m.fn === "count") return `countMerge(${m.name}) AS ${m.name}`;
      if (m.fn === "sum") return `sumMerge(${m.name}) AS ${m.name}`;
      return `avgMerge(${m.name}) AS ${m.name}`;
    })
    .join(", ");

  const querySql = `
SELECT ${spec.groupBy.join(", ")}, ${merges}
FROM anas.${spec.table}
GROUP BY ${spec.groupBy.join(", ")}
ORDER BY ${spec.groupBy[0]}
FORMAT JSONEachRow
`.trim();

  return { tableSql, mvSql, backfillSql, querySql };
}
