import { BufferJSON, initAuthCreds, proto } from '@whiskeysockets/baileys';

// Atomic batches preserve Signal keys; SQLite WAL is private and included in backups.
export function sqliteAuth(store) {
  const read = (kind, id) => {
    const row = store.db.prepare('SELECT value FROM auth WHERE kind=? AND id=?').get(kind,id);
    return row ? JSON.parse(row.value, BufferJSON.reviver) : null;
  };
  const write = (kind,id,value) => {
    if (value == null) store.db.prepare('DELETE FROM auth WHERE kind=? AND id=?').run(kind,id);
    else store.db.prepare('INSERT OR REPLACE INTO auth VALUES(?,?,?)').run(kind,id,JSON.stringify(value,BufferJSON.replacer));
  };
  const creds = read('credentials','current') || initAuthCreds();
  return {
    state: { creds, keys: {
      async get(type, ids) {
        return Object.fromEntries(ids.map(id => {
          let value = read(type,id);
          if (type === 'app-state-sync-key' && value) value = proto.Message.AppStateSyncKeyData.fromObject(value);
          return [id,value];
        }));
      },
      async set(data) {
        store.transaction(() => {
          for (const [kind, values] of Object.entries(data)) {
            for (const [id,value] of Object.entries(values)) write(kind,id,value);
          }
        });
      },
    } },
    saveCreds() { store.transaction(() => write('credentials','current',creds)); },
  };
}
