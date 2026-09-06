"use client";

import { useEffect, useState } from "react";
import { GoldChart } from "@/components/GoldChart";
import { DEFAULT_DSL } from "@/lib/medallion";

type GoldPayload = {
  ok: boolean;
  error?: string;
  table: string;
  groupBy: string[];
  metrics: string[];
  rows: Record<string, unknown>[];
};

export default function WidgetPage({ params }: { params: Promise<{ table: string }> }) {
  const [table, setTable] = useState("");
  const [data, setData] = useState<GoldPayload | null>(null);

  useEffect(() => {
    params.then((p) => setTable(p.table));
  }, [params]);

  useEffect(() => {
    if (!table) return;
    const q = new URLSearchParams({ table, dsl: DEFAULT_DSL });
    fetch(`/api/medallion/gold?${q}`, { cache: "no-store" })
      .then((r) => r.json())
      .then(setData);
  }, [table]);

  const metric = data?.metrics?.[0] ?? "value";
  const points =
    data?.rows?.map((r) => ({
      label: String(r[data.groupBy[0]] ?? ""),
      value: Number(r[metric] ?? 0),
    })) ?? [];

  return (
    <div className="embed">
      {data?.error ? (
        <div className="empty">{data.error}</div>
      ) : (
        <GoldChart
          title={table || "gold"}
          metric={metric}
          points={points}
          embedUrl={typeof window !== "undefined" ? window.location.href : `/widgets/${table}`}
        />
      )}
    </div>
  );
}
