import { describe, expect, it } from 'vitest';
import { createDatabasePool } from './client.js';
import { DEFAULT_DATABASE_URL } from './config.js';
import { DatabaseConnectionError, sanitizeConnectionString } from './errors.js';

describe('DatabasePool & Client', () => {
  it('connects and passes health check against live PostgreSQL', async () => {
    const pool = createDatabasePool({
      connectionString: DEFAULT_DATABASE_URL,
    });

    try {
      const isHealthy = await pool.healthCheck();
      expect(isHealthy).toBe(true);

      const res = await pool.query('SELECT 1 + 1 AS sum;');
      expect(res.rows[0]?.sum).toBe(2);
    } finally {
      await pool.close();
    }
  });

  it('fails health check gracefully when port is unreachable', async () => {
    const pool = createDatabasePool({
      connectionString: 'postgresql://forge:forge@127.0.0.1:5439/forge',
      connectionTimeoutMillis: 1000,
    });

    try {
      const isHealthy = await pool.healthCheck();
      expect(isHealthy).toBe(false);
    } finally {
      await pool.close();
    }
  });

  it('throws DatabaseConnectionError on invalid connection attempt without leaking credentials', async () => {
    const secretPassword = 'super_secret_password_123';
    const connStr = `postgresql://forge:${secretPassword}@127.0.0.1:5439/forge`;
    const pool = createDatabasePool({
      connectionString: connStr,
      connectionTimeoutMillis: 1000,
    });

    try {
      await expect(pool.connect()).rejects.toThrow(DatabaseConnectionError);

      try {
        await pool.connect();
      } catch (err) {
        expect((err as Error).message).not.toContain(secretPassword);
      }
    } finally {
      await pool.close();
    }
  });

  it('sanitizes connection string passwords correctly', () => {
    const sensitive = 'postgresql://admin:super_secret_pass@db.example.com:5432/production';
    const sanitized = sanitizeConnectionString(sensitive);

    expect(sanitized).toBe('postgresql://admin:***@db.example.com:5432/production');
    expect(sanitized).not.toContain('super_secret_pass');
  });
});
