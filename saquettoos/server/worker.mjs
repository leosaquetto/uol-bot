import { DurableObject } from 'cloudflare:workers';
import { createAccess, errorResult, initialState, prepareRequest, responseFor, webCrypto } from './access.mjs';

export class AccessWorkspace extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    const cryptography = webCrypto(crypto);
    ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS workspace (id INTEGER PRIMARY KEY CHECK (id = 1), data TEXT NOT NULL)');
    ctx.storage.sql.exec('INSERT OR IGNORE INTO workspace (id, data) VALUES (1, ?)', JSON.stringify(initialState(cryptography)));
    const read = () => JSON.parse(ctx.storage.sql.exec('SELECT data FROM workspace WHERE id = 1').one().data);
    this.access = createAccess({ crypto: cryptography, bootstrapToken: env.BOOTSTRAP_TOKEN || '', store: {
      read,
      transact(fn) {
        return ctx.storage.transactionSync(() => {
          const state = read();
          const before = JSON.stringify(state);
          const outcome = fn(state);
          const after = JSON.stringify(state);
          if (before !== after) ctx.storage.sql.exec('UPDATE workspace SET data = ? WHERE id = 1', after);
          return outcome;
        });
      },
    } });
  }

  async handle(input) { return await this.access.handle(input); }
}

export default {
  async fetch(request, env) {
    try {
      const input = await prepareRequest(request);
      if (input.path === '/health') return responseFor({ status: 200, body: { status: 'ready', service: 'saquettoos-access', version: '1.0.0' } });
      // One small, bounded household workspace: approval and token uniqueness share one transaction.
      const workspace = env.ACCESS.getByName('saquettoos-personal');
      return responseFor(await workspace.handle(input));
    } catch (error) { return responseFor(errorResult(error)); }
  },
};
