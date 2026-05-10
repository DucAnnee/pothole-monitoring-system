import type { Polygon } from "~/lib/annotation-contract";

const TRITON_HTTP_URL = process.env.TRITON_HTTP_URL ?? "http://localhost:8000";
const SAM3_MODEL = "sam3_assist";

interface TritonBytesInput {
  name: string;
  shape: number[];
  datatype: "BYTES";
  data: string[];
}

interface TritonFp32Input {
  name: string;
  shape: number[];
  datatype: "FP32";
  data: number[];
}

interface TritonInferResponse {
  outputs: Array<{
    name: string;
    datatype: string;
    shape: number[];
    data: string[];
  }>;
  error?: string;
}

export interface Sam3Result {
  polygon: Polygon;
  confidence: number;
  source: "sam3_assist" | "sam3_assist_stub";
}

function fromBase64(s: string): string {
  return Buffer.from(s, "base64").toString("utf-8");
}

function polygonBbox(polygon: Polygon): [number, number, number, number] {
  const xs = polygon.map(([x]) => x);
  const ys = polygon.map(([, y]) => y);
  return [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)];
}

export async function callSam3Assist(
  imageObjectKey: string,
  currentPolygon: Polygon,
): Promise<Sam3Result> {
  const bbox = polygonBbox(currentPolygon.length >= 3 ? currentPolygon : [[150, 100], [450, 300]]);

  const imageKeyInput: TritonBytesInput = {
    name: "image_object_key",
    shape: [1],
    datatype: "BYTES",
    data: [imageObjectKey],
  };

  const bboxInput: TritonFp32Input = {
    name: "bbox",
    shape: [4],
    datatype: "FP32",
    data: bbox,
  };

  const url = `${TRITON_HTTP_URL}/v2/models/${SAM3_MODEL}/infer`;

  let raw: TritonInferResponse;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        inputs: [imageKeyInput, bboxInput],
        outputs: [{ name: "polygon_json" }],
      }),
      signal: AbortSignal.timeout(30_000),
    });

    if (!res.ok) {
      throw new Error(`Triton HTTP ${res.status}: ${await res.text()}`);
    }
    raw = (await res.json()) as TritonInferResponse;
  } catch (err) {
    console.warn("[sam3] Triton unreachable, using stub:", err instanceof Error ? err.message : err);
    return stubResult(currentPolygon);
  }

  try {
    const encoded = raw.outputs[0]?.data[0];
    if (!encoded) throw new Error("empty polygon_json output");
    // Triton may return STRING output as base64 or as a raw JSON string depending on config
    let parsed: { polygon?: unknown[]; confidence?: number; error?: string };
    try {
      parsed = JSON.parse(encoded);
    } catch {
      parsed = JSON.parse(fromBase64(encoded));
    }

    if (parsed.error || !Array.isArray(parsed.polygon) || parsed.polygon.length < 3) {
      throw new Error(parsed.error ?? "invalid polygon from SAM3");
    }

    const polygon = parsed.polygon.map((pt) => {
      const [x, y] = pt as [number, number];
      return [x, y] as [number, number];
    });

    return {
      polygon,
      confidence: typeof parsed.confidence === "number" ? parsed.confidence : 0.75,
      source: "sam3_assist",
    };
  } catch (err) {
    console.warn("[sam3] Result parse error, using stub:", err instanceof Error ? err.message : err);
    return stubResult(currentPolygon);
  }
}

function stubResult(currentPolygon: Polygon): Sam3Result {
  const offsets: Polygon = [[-3, -3], [5, 5], [3, 3], [-3, -3]];
  const polygon =
    currentPolygon.length >= 3
      ? currentPolygon.map(([x, y], i): [number, number] => {
          const [dx, dy] = offsets[i % offsets.length];
          return [Math.max(0, x + dx), Math.max(0, y + dy)];
        })
      : ([[150, 100], [450, 100], [450, 300], [150, 300]] as Polygon);
  return { polygon, confidence: 0.0, source: "sam3_assist_stub" };
}
