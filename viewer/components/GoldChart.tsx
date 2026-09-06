"use client";

type Point = { label: string; value: number };

export function GoldChart({
  title,
  metric,
  points,
  embedUrl,
}: {
  title: string;
  metric: string;
  points: Point[];
  embedUrl: string;
}) {
  const w = 560;
  const h = 220;
  const pad = { t: 24, r: 16, b: 36, l: 44 };
  const max = Math.max(1, ...points.map((p) => p.value));
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const gap = 8;
  const barW = points.length ? (innerW - gap * (points.length - 1)) / points.length : innerW;

  const svg = buildSvg(title, metric, points, max);

  function download() {
    const blob = new Blob([svg], { type: "image/svg+xml" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `${title.replace(/\s+/g, "_")}.svg`;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  async function copyEmbed() {
    const snippet = `<iframe src="${embedUrl}" width="600" height="280" style="border:0;border-radius:12px" title="${title}"></iframe>`;
    await navigator.clipboard.writeText(snippet);
  }

  return (
    <div className="widget">
      <div className="widget-head">
        <div>
          <h3>{title}</h3>
          <p>{metric}</p>
        </div>
        <div className="widget-actions">
          <button type="button" onClick={download}>
            Export SVG
          </button>
          <button type="button" onClick={copyEmbed}>
            Copy embed
          </button>
        </div>
      </div>
      {points.length === 0 ? (
        <div className="empty">No gold rows yet. Promote after bronze has events.</div>
      ) : (
        <svg viewBox={`0 0 ${w} ${h}`} className="chart" role="img" aria-label={title}>
          <line x1={pad.l} y1={pad.t} x2={pad.l} y2={h - pad.b} stroke="currentColor" opacity="0.2" />
          <line
            x1={pad.l}
            y1={h - pad.b}
            x2={w - pad.r}
            y2={h - pad.b}
            stroke="currentColor"
            opacity="0.2"
          />
          {points.map((p, i) => {
            const bh = (p.value / max) * innerH;
            const x = pad.l + i * (barW + gap);
            const y = h - pad.b - bh;
            return (
              <g key={p.label + i}>
                <rect x={x} y={y} width={barW} height={bh} rx="4" fill="currentColor" opacity="0.85" />
                <text
                  x={x + barW / 2}
                  y={h - pad.b + 14}
                  textAnchor="middle"
                  fontSize="10"
                  fill="currentColor"
                  opacity="0.65"
                >
                  {p.label}
                </text>
                <text
                  x={x + barW / 2}
                  y={y - 6}
                  textAnchor="middle"
                  fontSize="10"
                  fill="currentColor"
                  opacity="0.8"
                >
                  {formatN(p.value)}
                </text>
              </g>
            );
          })}
        </svg>
      )}
    </div>
  );
}

function formatN(n: number) {
  if (Number.isInteger(n)) return String(n);
  return n.toFixed(1);
}

function buildSvg(title: string, metric: string, points: Point[], max: number) {
  const w = 560;
  const h = 240;
  const pad = { t: 40, r: 16, b: 36, l: 44 };
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const gap = 8;
  const barW = points.length ? (innerW - gap * (points.length - 1)) / points.length : innerW;
  const bars = points
    .map((p, i) => {
      const bh = (p.value / max) * innerH;
      const x = pad.l + i * (barW + gap);
      const y = h - pad.b - bh;
      return `<rect x="${x}" y="${y}" width="${barW}" height="${bh}" rx="4" fill="#c6f25a"/>
        <text x="${x + barW / 2}" y="${h - pad.b + 14}" text-anchor="middle" font-size="10" fill="#8b8b8b">${escapeXml(p.label)}</text>
        <text x="${x + barW / 2}" y="${y - 6}" text-anchor="middle" font-size="10" fill="#f4f4f4">${formatN(p.value)}</text>`;
    })
    .join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
  <rect width="100%" height="100%" fill="#0a0a0a"/>
  <text x="${pad.l}" y="22" font-family="ui-sans-serif,system-ui" font-size="14" fill="#f4f4f4">${escapeXml(title)}</text>
  <text x="${pad.l}" y="38" font-family="ui-sans-serif,system-ui" font-size="11" fill="#8b8b8b">${escapeXml(metric)}</text>
  ${bars}
</svg>`;
}

function escapeXml(s: string) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
