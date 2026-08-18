import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

export async function POST() {
  const base = process.env.EXAMPLE_URL || "http://localhost:8080";
  const res = await fetch(`${base}/orders`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ customerName: "Ada", totalAmount: 42.5 }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    return NextResponse.json({ error: body }, { status: res.status });
  }
  return NextResponse.json({ id: body.id, order: body });
}
