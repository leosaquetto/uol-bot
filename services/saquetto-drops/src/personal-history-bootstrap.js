import {BufferJSON} from '@whiskeysockets/baileys';
import {DatabaseSync} from 'node:sqlite';
import {chmodSync,mkdirSync,unlinkSync,writeFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {openStore} from './store.js';
import {createPersonalInbox} from './personal-inbox.js';
import {startWhatsApp} from './whatsapp.js';
import {acquireLock} from './process-lock.js';

const unlinkQuiet=path=>{try{unlinkSync(path);}catch{}};

export function phoneFromCredentialsValue(value){
  try{
    const creds=JSON.parse(value,BufferJSON.reviver);
    const id=creds?.me?.id;
    const digits=typeof id==='string'?id.split('@',1)[0].split(':',1)[0]:'';
    if(/^\d{8,15}$/.test(digits))return digits;
  }catch{}
  throw new Error('primary_identity_unavailable');
}

export function readPrimaryPhone(dbPath){
  const db=new DatabaseSync(dbPath,{readOnly:true});
  try{
    const row=db.prepare("SELECT value FROM auth WHERE kind='credentials' AND id='current'").get();
    if(!row?.value)throw new Error('primary_identity_unavailable');
    return phoneFromCredentialsValue(row.value);
  }finally{db.close();}
}

export async function runHistoryBootstrap({
  primaryDb='/var/lib/baileys-personal/baileys.sqlite',
  data='/var/lib/baileys-personal-history-bootstrap',
  logger=console,
}={}){
  mkdirSync(data,{recursive:true,mode:0o700});chmodSync(data,0o700);
  const releaseLock=acquireLock(join(data,'bootstrap.lock'));
  const store=openStore(join(data,'baileys.sqlite'));
  const inbox=createPersonalInbox({store});
  const codePath=join(data,'pairing-code.private.txt');
  const qrPath=join(data,'pairing-qr.private.txt');
  const existing=store.db.prepare("SELECT value FROM auth WHERE kind='credentials' AND id='current'").get();
  let registered=false;
  if(existing?.value){
    try{registered=JSON.parse(existing.value,BufferJSON.reviver)?.registered===true;}catch{}
  }
  const whatsapp=startWhatsApp({
    store,personalInbox:inbox,historyBootstrap:true,
    onQr(qr){if(qr)writeFileSync(qrPath,qr,{mode:0o600});else unlinkQuiet(qrPath);},
    logger,
  });
  let closed=false;
  const close=async()=>{
    if(closed)return;closed=true;clearInterval(timer);
    unlinkQuiet(codePath);unlinkQuiet(qrPath);
    await whatsapp.stop().catch(()=>{});store.close();releaseLock();
  };
  if(!registered){
    const phone=readPrimaryPhone(primaryDb);
    const socket=whatsapp.socket();
    if(!socket||typeof socket.requestPairingCode!=='function'){await close();throw new Error('pairing_code_unavailable');}
    const code=await socket.requestPairingCode(phone);
    if(typeof code!=='string'||!code.trim()){await close();throw new Error('pairing_code_unavailable');}
    writeFileSync(codePath,code.trim(),{mode:0o600});
    logger.log(JSON.stringify({event:'history_bootstrap_pairing_code_ready'}));
  }
  let previous='';
  const timer=setInterval(()=>{
    const chats=Number(store.db.prepare('SELECT COUNT(*) AS n FROM personal_chats').get()?.n||0);
    const messages=Number(store.db.prepare('SELECT COUNT(*) AS n FROM personal_messages').get()?.n||0);
    const state=whatsapp.state();
    const current=JSON.stringify({state,chats,messages});
    if(current!==previous){
      previous=current;logger.log(JSON.stringify({event:'history_bootstrap_status',state,chats,messages}));
    }
    if(state==='connected'){unlinkQuiet(codePath);unlinkQuiet(qrPath);}
  },2000);
  return {whatsapp,store,data,codePath,qrPath,close};
}

async function main(){
  process.umask(0o077);
  const runner=await runHistoryBootstrap({
    primaryDb:process.env.BAILEYS_PERSONAL_PRIMARY_DB||'/var/lib/baileys-personal/baileys.sqlite',
    data:process.env.BAILEYS_PERSONAL_BOOTSTRAP_DATA||'/var/lib/baileys-personal-history-bootstrap',
  });
  const shutdown=async()=>{await runner.close();process.exit(0);};
  process.on('SIGTERM',shutdown);process.on('SIGINT',shutdown);
}

if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){
  main().catch(error=>{console.error(JSON.stringify({event:'history_bootstrap_failed',code:error?.message||'bootstrap_failed'}));process.exitCode=1;});
}
