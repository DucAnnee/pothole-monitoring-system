import minioClient from '../db/minio.js';
import redis from '../db/redis.js';
import config from '../config/index.js';

/**
 * Parse S3 URI to extract bucket and object key
 * Supports formats:
 * - s3://bucket/path/to/object
 * - bucket/path/to/object
 * @param {string} s3Uri 
 * @returns {{bucket: string, key: string} | null}
 */
function parseS3Uri(s3Uri) {
  if (!s3Uri) return null;

  // Handle s3:// format
  if (s3Uri.startsWith('s3://')) {
    const withoutProtocol = s3Uri.slice(5); // Remove "s3://"
    const slashIndex = withoutProtocol.indexOf('/');
    if (slashIndex === -1) return null;
    
    return {
      bucket: withoutProtocol.slice(0, slashIndex),
      key: withoutProtocol.slice(slashIndex + 1),
    };
  }

  // Handle direct bucket/key format
  const slashIndex = s3Uri.indexOf('/');
  if (slashIndex === -1) return null;
  
  return {
    bucket: s3Uri.slice(0, slashIndex),
    key: s3Uri.slice(slashIndex + 1),
  };
}

/**
 * Generate cache key for presigned URL
 * @param {string} s3Uri 
 * @returns {string}
 */
function getCacheKey(s3Uri) {
  return `api:image:presigned:${s3Uri}`;
}

/**
 * GET /api/v1/image/presigned
 * Generate a presigned URL for accessing an image in MinIO
 * 
 * Query params:
 * - path: S3 URI (s3://bucket/path or bucket/path)
 */
export async function getPresignedUrl(req, res) {
  try {
    const { path } = req.query;

    if (!path) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'path query parameter is required',
      });
    }

    // Check cache first
    const cacheKey = getCacheKey(path);
    const cached = await redis.get(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Parse S3 URI
    const parsed = parseS3Uri(path);
    if (!parsed) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Invalid S3 URI format. Expected s3://bucket/key or bucket/key',
      });
    }

    // Generate presigned URL
    const presignedUrl = await minioClient.presignedGetObject(
      parsed.bucket,
      parsed.key,
      config.minio.presignedUrlExpiry
    );

    const response = {
      url: presignedUrl,
      expiresIn: config.minio.presignedUrlExpiry,
      path: path,
    };

    // Cache the presigned URL (slightly less than expiry time)
    await redis.set(cacheKey, response, config.cacheTTL.imageUrl);

    return res.json(response);
  } catch (error) {
    console.error('[ImagePresigned] Error:', error);
    
    // Handle specific MinIO errors
    if (error.code === 'NoSuchKey') {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Image not found in storage',
      });
    }

    if (error.code === 'NoSuchBucket') {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Storage bucket not found',
      });
    }

    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to generate presigned URL',
      details: config.nodeEnv === 'development' ? error.message : undefined,
    });
  }
}

/**
 * GET /api/v1/image/proxy
 * Proxy an image from MinIO (useful when presigned URLs don't work due to CORS)
 * 
 * Query params:
 * - path: S3 URI (s3://bucket/path or bucket/path)
 */
export async function proxyImage(req, res) {
  try {
    const { path } = req.query;

    if (!path) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'path query parameter is required',
      });
    }

    // Parse S3 URI
    const parsed = parseS3Uri(path);
    if (!parsed) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Invalid S3 URI format. Expected s3://bucket/key or bucket/key',
      });
    }

    // Get object stream from MinIO
    const dataStream = await minioClient.getObject(parsed.bucket, parsed.key);

    // Set content type based on file extension
    const extension = parsed.key.split('.').pop()?.toLowerCase();
    const contentTypes = {
      jpg: 'image/jpeg',
      jpeg: 'image/jpeg',
      png: 'image/png',
      gif: 'image/gif',
      webp: 'image/webp',
    };
    const contentType = contentTypes[extension] || 'application/octet-stream';
    
    res.setHeader('Content-Type', contentType);
    res.setHeader('Cache-Control', 'public, max-age=86400'); // Cache for 1 day

    // Pipe the data stream to response
    dataStream.pipe(res);
  } catch (error) {
    console.error('[ImageProxy] Error:', error);
    
    // Handle specific MinIO errors
    if (error.code === 'NoSuchKey') {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Image not found in storage',
      });
    }

    if (error.code === 'NoSuchBucket') {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Storage bucket not found',
      });
    }

    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to proxy image',
      details: config.nodeEnv === 'development' ? error.message : undefined,
    });
  }
}
