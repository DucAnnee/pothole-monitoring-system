import { Client } from 'presto-client';
import config from '../config/index.js';

/**
 * Trino client singleton for querying Iceberg tables
 */
class TrinoClient {
  constructor() {
    this.client = null;
    this.isConnected = false;
  }

  /**
   * Initialize the Trino client
   */
  connect() {
    if (this.client) {
      return this.client;
    }

    this.client = new Client({
      host: config.trino.host,
      port: config.trino.port,
      user: config.trino.user,
      catalog: config.trino.catalog,
      schema: config.trino.schema,
      // Send basic auth header for Trino
      basic_auth: {
        user: config.trino.user,
        password: '',
      },
      // Custom headers for Trino authentication
      custom_headers: {
        'X-Trino-User': config.trino.user,
      },
    });

    this.isConnected = true;
    console.log(`[Trino] Client initialized - ${config.trino.host}:${config.trino.port}`);
    return this.client;
  }

  /**
   * Execute a SQL query and return results
   * @param {string} sql - SQL query to execute
   * @returns {Promise<Array>} - Query results
   */
  async query(sql) {
    return new Promise((resolve, reject) => {
      if (!this.client) {
        this.connect();
      }

      const results = [];
      let columns = [];

      this.client.execute({
        query: sql,
        catalog: config.trino.catalog,
        schema: config.trino.schema,
        columns: (error, data) => {
          if (error) {
            console.error('[Trino] Column error:', error);
            return;
          }
          columns = data.map(col => col.name);
        },
        data: (error, data, _columns, stats) => {
          if (error) {
            console.error('[Trino] Data error:', error);
            reject(error);
            return;
          }
          // Convert array rows to objects
          for (const row of data) {
            const obj = {};
            columns.forEach((col, i) => {
              obj[col] = row[i];
            });
            results.push(obj);
          }
        },
        success: (_stats) => {
          resolve(results);
        },
        error: (error) => {
          console.error('[Trino] Query error:', error);
          reject(error);
        },
      });
    });
  }

  /**
   * Check if Trino is healthy
   * @returns {Promise<boolean>}
   */
  async healthCheck() {
    try {
      await this.query('SELECT 1');
      return true;
    } catch (error) {
      console.error('[Trino] Health check failed:', error.message);
      return false;
    }
  }

  /**
   * Get connection status
   */
  getStatus() {
    return {
      connected: this.isConnected,
      host: config.trino.host,
      port: config.trino.port,
      catalog: config.trino.catalog,
      schema: config.trino.schema,
    };
  }
}

// Export singleton instance
const trinoClient = new TrinoClient();
export default trinoClient;
