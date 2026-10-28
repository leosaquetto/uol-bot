import {randomUUID} from 'node:crypto';

const MAX_PAGE=100;
const WRAPPERS=['ephemeralMessage','viewOnceMessage','viewOnceMessageV2','viewOnceMessageV2Extension','documentWithCaptionMessage'];
const ignoredJid=jid=>jid==='status@broadcast'||jid.endsWith('@newsletter');
const toTimestamp=value=>{
  if(value===undefined||value===null)return 0;
  const raw=typeof value?.toNumber==='function'?value.toNumber():value;
  const number=Number(raw);
  return Number.isFinite(number)&&number>=0?Math.trunc(number):0;
};
const kindFor=jid=>jid.endsWith('@g.us')?'group':'contact';
const cleanName=value=>typeof value==='string'&&value.trim()?value.trim().slice(0,200):null;

function unwrapMessage(message){
  let current=message&&typeof message==='object'?message:null;
  for(let depth=0;current&&depth<5;depth++){
    const key=WRAPPERS.find(name=>current[name]?.message&&typeof current[name].message==='object');
    if(!key)break;
    current=current[key].message;
  }
  return current;
}

function content(message){
  const current=unwrapMessage(message);
  if(!current)return {contentType:'unknown',text:null};
  const contentType=Object.keys(current)[0]||'unknown';
  const value=current[contentType];
  let text=null;
  if(contentType==='conversation'&&typeof value==='string')text=value;
  else if(value&&typeof value==='object'){
    if(typeof value.text==='string')text=value.text;
    else if(typeof value.caption==='string')text=value.caption;
  }
  if(contentType==='protocolMessage'||contentType==='senderKeyDistributionMessage')text=null;
  return {contentType,text};
}

export function createPersonalInbox({store,now=Date.now}){
  const {db,transaction}=store;
  db.exec(`
    CREATE TABLE IF NOT EXISTS personal_chats(
      chat_ref TEXT PRIMARY KEY,
      jid TEXT NOT NULL UNIQUE,
      name TEXT,
      kind TEXT NOT NULL,
      last_message_ref TEXT,
      last_message_id TEXT,
      last_message_timestamp INTEGER,
      indexed_message_count INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS personal_chats_activity ON personal_chats(last_message_timestamp DESC);
    CREATE INDEX IF NOT EXISTS personal_chats_name ON personal_chats(name);
    CREATE TABLE IF NOT EXISTS personal_messages(
      message_ref TEXT PRIMARY KEY,
      chat_ref TEXT NOT NULL,
      message_id TEXT NOT NULL,
      from_me INTEGER NOT NULL,
      participant TEXT,
      message_timestamp INTEGER NOT NULL,
      text TEXT,
      content_type TEXT NOT NULL,
      deleted INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL,
      UNIQUE(chat_ref,message_id)
    );
    CREATE INDEX IF NOT EXISTS personal_messages_chat_time ON personal_messages(chat_ref,message_timestamp DESC);
    CREATE INDEX IF NOT EXISTS personal_messages_text ON personal_messages(text);
  `);

  const chatByJid=jid=>db.prepare('SELECT * FROM personal_chats WHERE jid=?').get(jid);
  const chatByRef=chatRef=>db.prepare('SELECT * FROM personal_chats WHERE chat_ref=?').get(chatRef);
  const ensureChat=(jid,{name=null,kind=kindFor(jid)}={})=>{
    if(typeof jid!=='string'||!jid||ignoredJid(jid))return null;
    const existing=chatByJid(jid);
    const safeName=cleanName(name);
    if(existing){
      if(safeName&&safeName!==existing.name)db.prepare('UPDATE personal_chats SET name=?,updated_at=? WHERE chat_ref=?').run(safeName,now(),existing.chat_ref);
      return chatByJid(jid);
    }
    const ref=randomUUID();
    db.prepare('INSERT INTO personal_chats(chat_ref,jid,name,kind,updated_at) VALUES(?,?,?,?,?)')
      .run(ref,jid,safeName,kind,now());
    return chatByRef(ref);
  };
  const refreshChat=chatRef=>{
    const last=db.prepare('SELECT * FROM personal_messages WHERE chat_ref=? AND deleted=0 ORDER BY message_timestamp DESC,rowid DESC LIMIT 1').get(chatRef);
    const count=db.prepare('SELECT COUNT(*) AS count FROM personal_messages WHERE chat_ref=? AND deleted=0').get(chatRef)?.count||0;
    db.prepare(`UPDATE personal_chats SET last_message_ref=?,last_message_id=?,last_message_timestamp=?,indexed_message_count=?,updated_at=? WHERE chat_ref=?`)
      .run(last?.message_ref||null,last?.message_id||null,last?.message_timestamp||null,count,now(),chatRef);
  };
  const upsertMessage=message=>{
    const key=message?.key;
    const jid=key?.remoteJid;
    if(typeof jid!=='string'||ignoredJid(jid)||typeof key?.id!=='string'||!key.id)return null;
    const chat=ensureChat(jid);
    if(!chat)return null;
    const payload=content(message.message);
    const timestamp=toTimestamp(message.messageTimestamp);
    const existing=db.prepare('SELECT * FROM personal_messages WHERE chat_ref=? AND message_id=?').get(chat.chat_ref,key.id);
    if(existing){
      db.prepare(`UPDATE personal_messages SET from_me=?,participant=?,message_timestamp=?,text=?,content_type=?,deleted=0,updated_at=? WHERE message_ref=?`)
        .run(key.fromMe===true?1:0,typeof key.participant==='string'?key.participant:null,timestamp||existing.message_timestamp,payload.text,payload.contentType,now(),existing.message_ref);
      refreshChat(chat.chat_ref);
      return existing.message_ref;
    }
    const ref=randomUUID();
    db.prepare(`INSERT INTO personal_messages(message_ref,chat_ref,message_id,from_me,participant,message_timestamp,text,content_type,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`)
      .run(ref,chat.chat_ref,key.id,key.fromMe===true?1:0,typeof key.participant==='string'?key.participant:null,timestamp,payload.text,payload.contentType,now());
    refreshChat(chat.chat_ref);
    return ref;
  };

  const publicChat=row=>({
    chatRef:row.chat_ref,
    name:row.name,
    kind:row.kind,
    lastMessageAt:row.last_message_timestamp??null,
    indexedMessageCount:Number(row.indexed_message_count||0),
    lastMessageRef:row.last_message_ref||null,
  });
  const publicMessage=row=>({
    messageRef:row.message_ref,
    messageTimestamp:Number(row.message_timestamp||0),
    fromMe:row.from_me===1,
    contentType:row.content_type,
    text:row.deleted===1?null:row.text,
    deleted:row.deleted===1,
  });
  const internalMessage=row=>row?{
    messageRef:row.message_ref,
    chatRef:row.chat_ref,
    key:{remoteJid:chatByRef(row.chat_ref)?.jid,id:row.message_id,fromMe:row.from_me===1,...(row.participant?{participant:row.participant}:{})},
    messageTimestamp:Number(row.message_timestamp||0),
    deleted:row.deleted===1,
  }:null;

  return {
    ingestChats(chats=[]){
      for(const item of chats||[]){
        if(typeof item?.id!=='string')continue;
        ensureChat(item.id,{name:cleanName(item.name)||cleanName(item.subject)||null});
      }
    },
    ingestContacts(contacts=[]){
      for(const item of contacts||[]){
        if(typeof item?.id!=='string')continue;
        const name=cleanName(item.name)||cleanName(item.notify)||cleanName(item.verifiedName);
        const row=chatByJid(item.id);
        if(row&&!row.name&&name)ensureChat(item.id,{name});
      }
    },
    ingestHistory({chats=[],contacts=[],messages=[]}={}){
      const contactNames=new Map();
      for(const item of contacts||[]){if(typeof item?.id==='string')contactNames.set(item.id,cleanName(item.name)||cleanName(item.notify)||cleanName(item.verifiedName));}
      for(const item of chats||[]){
        if(typeof item?.id!=='string')continue;
        ensureChat(item.id,{name:cleanName(item.name)||cleanName(item.subject)||contactNames.get(item.id)||null});
      }
      this.ingestContacts(contacts||[]);
      this.ingestMessages(messages||[]);
    },
    ingestMessages(messages=[]){for(const message of messages||[])upsertMessage(message);},
    ingestUpdates(updates=[]){
      for(const entry of updates||[]){
        const key=entry?.key;
        if(!entry?.update?.message||typeof key?.remoteJid!=='string'||typeof key?.id!=='string')continue;
        const chat=chatByJid(key.remoteJid);if(!chat)continue;
        const old=db.prepare('SELECT * FROM personal_messages WHERE chat_ref=? AND message_id=?').get(chat.chat_ref,key.id);if(!old)continue;
        const payload=content(entry.update.message);
        db.prepare('UPDATE personal_messages SET text=?,content_type=?,updated_at=? WHERE message_ref=?')
          .run(payload.text,payload.contentType,now(),old.message_ref);
        refreshChat(chat.chat_ref);
      }
    },
    ingestDeletes(payload={}){
      if(Array.isArray(payload?.keys)){
        for(const key of payload.keys){
          if(typeof key?.remoteJid!=='string'||typeof key?.id!=='string')continue;
          const chat=chatByJid(key.remoteJid);if(!chat)continue;
          db.prepare('UPDATE personal_messages SET deleted=1,text=NULL,updated_at=? WHERE chat_ref=? AND message_id=?')
            .run(now(),chat.chat_ref,key.id);
          refreshChat(chat.chat_ref);
        }
      }
      if(payload?.all===true&&typeof payload?.jid==='string'){
        const chat=chatByJid(payload.jid);if(chat){db.prepare('UPDATE personal_messages SET deleted=1,text=NULL,updated_at=? WHERE chat_ref=?').run(now(),chat.chat_ref);refreshChat(chat.chat_ref);}
      }
    },
    listChats({query='',limit=50,before}={}){
      const bounded=Math.max(1,Math.min(MAX_PAGE,Number(limit)||50));
      const clauses=[];const args=[];
      if(typeof query==='string'&&query.trim()){clauses.push('LOWER(COALESCE(name,\'\')) LIKE ?');args.push(`%${query.trim().toLowerCase()}%`);}
      if(before!==undefined){clauses.push('COALESCE(last_message_timestamp,0) < ?');args.push(toTimestamp(before));}
      const where=clauses.length?`WHERE ${clauses.join(' AND ')}`:'';
      return db.prepare(`SELECT * FROM personal_chats ${where} ORDER BY COALESCE(last_message_timestamp,0) DESC,name COLLATE NOCASE LIMIT ?`).all(...args,bounded).map(publicChat);
    },
    listMessages(chatRef,{limit=50,before}={}){
      const chat=chatByRef(chatRef);if(!chat)return [];
      const bounded=Math.max(1,Math.min(MAX_PAGE,Number(limit)||50));
      const args=[chatRef];let filter='';
      if(before!==undefined){filter=' AND message_timestamp < ?';args.push(toTimestamp(before));}
      args.push(bounded);
      return db.prepare(`SELECT * FROM personal_messages WHERE chat_ref=?${filter} ORDER BY message_timestamp DESC,rowid DESC LIMIT ?`).all(...args).map(publicMessage);
    },
    searchMessages(query,{chatRef,limit=50}={}){
      if(typeof query!=='string'||!query.trim())return [];
      const bounded=Math.max(1,Math.min(MAX_PAGE,Number(limit)||50));
      const args=[`%${query.trim().toLowerCase()}%`];let filter='';
      if(chatRef){filter=' AND chat_ref=?';args.push(chatRef);}
      args.push(bounded);
      return db.prepare(`SELECT * FROM personal_messages WHERE deleted=0 AND LOWER(COALESCE(text,'')) LIKE ?${filter} ORDER BY message_timestamp DESC LIMIT ?`).all(...args).map(publicMessage);
    },
    resolveChat(chatRef){const row=chatByRef(chatRef);return row?{chatRef:row.chat_ref,jid:row.jid,name:row.name,kind:row.kind}:null;},
    resolveMessage(messageRef){return internalMessage(db.prepare('SELECT * FROM personal_messages WHERE message_ref=?').get(messageRef));},
    removeChat(chatRef){return transaction(()=>{db.prepare('DELETE FROM personal_messages WHERE chat_ref=?').run(chatRef);return db.prepare('DELETE FROM personal_chats WHERE chat_ref=?').run(chatRef).changes===1;});},
    oldestMessage(chatRef){return internalMessage(db.prepare('SELECT * FROM personal_messages WHERE chat_ref=? AND deleted=0 ORDER BY message_timestamp ASC,rowid ASC LIMIT 1').get(chatRef));},
    lastMessage(chatRef){return internalMessage(db.prepare('SELECT * FROM personal_messages WHERE chat_ref=? AND deleted=0 ORDER BY message_timestamp DESC,rowid DESC LIMIT 1').get(chatRef));},
  };
}
