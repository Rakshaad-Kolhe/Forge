import { describe, it, expect } from 'vitest';
import { loadConfig, ConfigValidationError } from './index.js';

describe('loadConfig', () => {
  it('loads valid default configuration when environment is empty', () => {
    const config = loadConfig({});
    expect(config).toEqual({
      nodeEnv: 'development',
      logLevel: 'info',
      apiPort: 3000,
      databaseUrl: 'postgresql://forge:forge@127.0.0.1:5432/forge',
    });
  });

  it('loads valid configuration when valid overrides are provided', () => {
    const config = loadConfig({
      NODE_ENV: 'production',
      LOG_LEVEL: 'warn',
      API_PORT: '8080',
      DATABASE_URL: 'postgresql://custom:custom@localhost:5433/custom_db',
    });
    expect(config).toEqual({
      nodeEnv: 'production',
      logLevel: 'warn',
      apiPort: 8080,
      databaseUrl: 'postgresql://custom:custom@localhost:5433/custom_db',
    });
  });

  it('fails clearly when NODE_ENV is invalid', () => {
    expect(() => {
      loadConfig({
        NODE_ENV: 'staging' as unknown as string,
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when LOG_LEVEL is invalid', () => {
    expect(() => {
      loadConfig({
        LOG_LEVEL: 'verbose',
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when API_PORT is not a number', () => {
    expect(() => {
      loadConfig({
        API_PORT: 'not-a-number',
      });
    }).toThrow(ConfigValidationError);
  });

  it('fails clearly when API_PORT is out of range', () => {
    expect(() => {
      loadConfig({
        API_PORT: '70000',
      });
    }).toThrow(ConfigValidationError);

    expect(() => {
      loadConfig({
        API_PORT: '0',
      });
    }).toThrow(ConfigValidationError);
  });
});
