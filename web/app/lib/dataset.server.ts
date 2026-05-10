import pg from "pg";
import { getClient as getMinioClient } from "~/lib/minio.server";

const { Pool } = pg;

const POSTGIS_HOST = process.env.POSTGIS_HOST ?? "localhost";
const POSTGIS_PORT = Number(process.env.POSTGIS_PORT ?? "5437");
const POSTGIS_USER = process.env.POSTGIS_USER ?? "serving";
const POSTGIS_PASSWORD = process.env.POSTGIS_PASSWORD ?? "servingpassword";
const POSTGIS_DATABASE = process.env.POSTGIS_DATABASE ?? "postgis_serving";

const TRINO_HOST = process.env.TRINO_HOST ?? "localhost";
const TRINO_PORT = process.env.TRINO_PORT ?? "8081";
const TRINO_USER = process.env.TRINO_USER ?? "admin";

const MINIO_BUCKET = process.env.MINIO_BUCKET ?? "warehouse";

let pool: pg.Pool | null = null;

function getPool() {
  pool ??= new Pool({
    host: POSTGIS_HOST,
    port: POSTGIS_PORT,
    user: POSTGIS_USER,
    password: POSTGIS_PASSWORD,
    database: POSTGIS_DATABASE,
    max: 5,
  });
  return pool;
}

async function queryPostgis<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  const result = await getPool().query(sql, params);
  return result.rows as T[];
}

async function trinoQuery(sql: string): Promise<Record<string, unknown>[]> {
  const baseUrl = `http://${TRINO_HOST}:${TRINO_PORT}`;
  const headers = {
    "X-Trino-User": TRINO_USER,
    "X-Trino-Catalog": "iceberg",
    "X-Trino-Schema": "ml",
    "X-Trino-Source": "pothole-web-dataset",
    "Content-Type": "text/plain",
  };

  const init = await fetch(`${baseUrl}/v1/statement`, {
    method: "POST",
    headers,
    body: sql,
    signal: AbortSignal.timeout(10_000),
  });
  if (!init.ok) throw new Error(`Trino ${init.status}`);

  let state = (await init.json()) as {
    nextUri?: string;
    columns?: Array<{ name: string }>;
    data?: unknown[][];
    stats: { state: string };
    error?: { message: string };
  };

  const columns: string[] = [];
  const rows: Record<string, unknown>[] = [];

  function absorb(s: typeof state) {
    if (s.columns && columns.length === 0) columns.push(...s.columns.map((c) => c.name));
    if (s.data) {
      for (const row of s.data) {
        rows.push(Object.fromEntries(columns.map((c, i) => [c, row[i]])));
      }
    }
  }

  absorb(state);

  while (state.nextUri) {
    await new Promise((r) => setTimeout(r, 50));
    const res = await fetch(state.nextUri, { headers, signal: AbortSignal.timeout(10_000) });
    state = (await res.json()) as typeof state;
    if (state.error) throw new Error(state.error.message);
    absorb(state);
  }

  return rows;
}

export interface DatasetImage {
  objectKey: string;
  name: string;
  severity: string;
  defectId: string | null;
}

export interface DatasetStats {
  totalImages: number;
  annotations: number;
  train: number;
  valid: number;
  test: number;
  version: string;
}

export interface DatasetLoaderData {
  stats: DatasetStats;
  images: DatasetImage[];
}

export async function queryDatasetStats(): Promise<DatasetStats> {
  const [annotationRows, defectRows] = await Promise.all([
    queryPostgis<{ count: string }>("SELECT COUNT(*) AS count FROM serving.annotations").catch(
      () => [{ count: "0" }],
    ),
    queryPostgis<{ count: string }>(
      "SELECT COUNT(*) AS count FROM serving.current_road_defects WHERE latest_raw_image_object_key IS NOT NULL",
    ).catch(() => [{ count: "0" }]),
  ]);

  const annotations = Number(annotationRows[0]?.count ?? 0);
  const totalImages = Number(defectRows[0]?.count ?? 0);

  let train = 0;
  let valid = 0;
  let test = 0;
  let version = "—";

  try {
    const [splitRows, versionRows] = await Promise.all([
      trinoQuery(`
        SELECT split_name, COUNT(*) AS cnt
        FROM iceberg.ml.training_dataset_items
        GROUP BY split_name
      `),
      trinoQuery(`
        SELECT version_label
        FROM iceberg.ml.dataset_versions
        ORDER BY created_at DESC
        LIMIT 1
      `),
    ]);

    for (const r of splitRows) {
      const split = String(r.split_name ?? "").toLowerCase();
      const cnt = Number(r.cnt ?? 0);
      if (split === "train") train = cnt;
      else if (split === "val" || split === "valid") valid = cnt;
      else if (split === "test") test = cnt;
    }

    if (versionRows[0]?.version_label) {
      version = String(versionRows[0].version_label);
    }
  } catch {
    // Trino unavailable — leave splits at 0, version at "—"
  }

  return { totalImages, annotations, train, valid, test, version };
}

export async function queryDatasetImages(limit = 48): Promise<DatasetImage[]> {
  const rows = await queryPostgis<{
    latest_raw_image_object_key: string;
    severity_level: string | null;
    defect_id: string;
  }>(
    `SELECT latest_raw_image_object_key, severity_level, defect_id
     FROM serving.current_road_defects
     WHERE latest_raw_image_object_key IS NOT NULL
     ORDER BY last_seen_at DESC NULLS LAST
     LIMIT $1`,
    [limit],
  ).catch(() => []);

  if (rows.length > 0) {
    return rows.map((r) => ({
      objectKey: r.latest_raw_image_object_key,
      name: r.latest_raw_image_object_key.split("/").pop() ?? r.latest_raw_image_object_key,
      severity: (r.severity_level ?? "minor").toLowerCase(),
      defectId: r.defect_id,
    }));
  }

  // Fallback: list MinIO objects when PostGIS is empty
  try {
    const client = getMinioClient();
    const stream = client.listObjectsV2(MINIO_BUCKET, "raw_images/", false);
    const objects: DatasetImage[] = [];

    await new Promise<void>((resolve, reject) => {
      stream.on("data", (obj) => {
        if (objects.length >= limit) return;
        const key = obj.name ?? "";
        objects.push({
          objectKey: key,
          name: key.split("/").pop() ?? key,
          severity: "minor",
          defectId: null,
        });
      });
      stream.on("end", resolve);
      stream.on("error", reject);
    });

    return objects;
  } catch {
    return [];
  }
}
