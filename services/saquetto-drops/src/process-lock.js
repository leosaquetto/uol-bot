import { openSync,readFileSync,writeFileSync,closeSync,unlinkSync } from 'node:fs';

export function acquireLock(path) {
  const create = () => {
    const fd=openSync(path,'wx',0o600);writeFileSync(fd,String(process.pid));closeSync(fd);
  };
  try { create(); }
  catch(error) {
    if(error.code!=='EEXIST')throw error;
    const pid=Number(readFileSync(path,'utf8'));
    if(!Number.isInteger(pid)||pid<1)throw new Error('invalid_process_lock');
    try {process.kill(pid,0);throw new Error('service_already_running');}
    catch(e) {if(e.code!=='ESRCH')throw e;}
    unlinkSync(path);create();
  }
  return () => {try {if(readFileSync(path,'utf8')===String(process.pid))unlinkSync(path);}catch{}};
}
