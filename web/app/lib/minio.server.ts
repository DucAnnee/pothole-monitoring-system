import { Client } from "minio";
import { cached } from "./redis.server";

let _client: Client | null = null;

function getClient(): Client {
  if (!_client) {
    _client = new Client({
      endPoint: process.env.MINIO_ENDPOINT ?? "localhost",
      port: parseInt(process.env.MINIO_PORT ?? "9000"),
      useSSL: process.env.MINIO_USE_SSL === "true",
      accessKey: process.env.MINIO_ACCESS_KEY ?? "minioadmin",
      secretKey: process.env.MINIO_SECRET_KEY ?? "minioadmin",
    });
  }
  return _client;
}

const BUCKET = () => process.env.MINIO_BUCKET ?? "warehouse";

export async function presignedUrl(s3Path: string): Promise<string> {
  const key = `minio:presigned:${s3Path}`;
  return cached(key, 55 * 60, async () => {
    const client = getClient();
    const objectName = s3Path.startsWith("/") ? s3Path.slice(1) : s3Path;
    return client.presignedGetObject(BUCKET(), objectName, 3600);
  });
}

export async function proxyStream(s3Path: string): Promise<Response> {
  const client = getClient();
  const objectName = s3Path.startsWith("/") ? s3Path.slice(1) : s3Path;
  const stream = await client.getObject(BUCKET(), objectName);

  const chunks: Buffer[] = [];
  await new Promise<void>((resolve, reject) => {
    stream.on("data", (chunk: Buffer) => chunks.push(chunk));
    stream.on("end", resolve);
    stream.on("error", reject);
  });

  const body = Buffer.concat(chunks);
  return new Response(body, {
    headers: { "Content-Type": "image/jpeg", "Cache-Control": "public, max-age=3600" },
  });
}
