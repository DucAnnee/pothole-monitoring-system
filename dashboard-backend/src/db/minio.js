import { Client } from 'minio';
import config from '../config/index.js';

/**
 * MinIO client for S3-compatible object storage
 */
const minioClient = new Client({
  endPoint: config.minio.endPoint,
  port: config.minio.port,
  useSSL: config.minio.useSSL,
  accessKey: config.minio.accessKey,
  secretKey: config.minio.secretKey,
});

export default minioClient;
