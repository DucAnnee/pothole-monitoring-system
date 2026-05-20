const PROM = process.env.PROMETHEUS_URL ?? "http://localhost:9091";

export async function promQuery(expr: string): Promise<number | null> {
  try {
    const url = `${PROM}/api/v1/query?query=${encodeURIComponent(expr)}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(2500) });
    if (!res.ok) return null;
    const json = await res.json();
    const r = json?.data?.result;
    if (!r?.length) return null;
    return Number(r[0].value[1]);
  } catch {
    return null;
  }
}
