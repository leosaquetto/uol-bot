import { cloudflareTest } from '@cloudflare/vitest-pool-workers';
import { defineConfig } from 'vitest/config';

// Never load account secrets or send an unmatched request to a real service.
process.env.CLOUDFLARE_LOAD_DEV_VARS_FROM_DOT_ENV = 'false';
process.env.CLOUDFLARE_INCLUDE_PROCESS_ENV = 'false';
process.env.WRANGLER_SEND_METRICS = 'false';

export default defineConfig({
  plugins: [cloudflareTest({
    wrangler: { configPath: './wrangler.test.jsonc' },
    remoteBindings: false,
    miniflare: {
      outboundService() { throw new Error('Real network disabled in sentinel tests'); },
    },
  })],
  test: {
    include: ['test-worker/**/*.test.js'],
    testTimeout: 20_000,
    fileParallelism: false,
  },
});
