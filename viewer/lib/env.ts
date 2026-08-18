export type FatEvent = {
  event_id: string;
  topic: string;
  kafka_offset?: number;
  event_ts?: string;
  payload: string;
  source: "kafka" | "clickhouse" | "duckdb";
};

function env(name: string, fallback: string) {
  return process.env[name] || fallback;
}

export function clickhouseBase() {
  const url = env("CLICKHOUSE_URL", "http://localhost:8123");
  const user = env("CLICKHOUSE_USER", "default");
  const password = env("CLICKHOUSE_PASSWORD", "anas");
  return { url, user, password };
}

export async function clickhouseQuery(sql: string): Promise<string> {
  const { url, user, password } = clickhouseBase();
  const res = await fetch(`${url}/?user=${encodeURIComponent(user)}&password=${encodeURIComponent(password)}`, {
    method: "POST",
    body: sql,
  });
  const text = await res.text();
  if (!res.ok) throw new Error(text || `clickhouse ${res.status}`);
  return text;
}

export async function duckdbGet(path: string): Promise<Response> {
  const base = env("DUCKDB_SINK_URL", "http://localhost:8081");
  return fetch(`${base}${path}`, { cache: "no-store" });
}

export function kafkaBrokers() {
  return env("KAFKA_BROKERS", "localhost:9092").split(",");
}

export function kafkaTopics() {
  return env("KAFKA_TOPICS", "orders.events").split(",");
}
