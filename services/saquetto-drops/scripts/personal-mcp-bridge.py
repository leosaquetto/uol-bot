#!/usr/bin/env python3
"""Narrow SSH stdin bridge for the personal Baileys MCP connection."""
from __future__ import annotations
import http.client, json, re, sys, uuid
from pathlib import Path
from urllib.parse import urlencode
ENV_FILE=Path('/etc/baileys-personal/service.env'); HOST='127.0.0.1'; PORT=8790
MAX_ENVELOPE=16*1024; MAX_RESPONSE=64*1024
TOKEN=re.compile(r'^[a-f0-9]{64}$'); ALIAS=re.compile(r'^[A-Za-z][A-Za-z0-9_-]{0,79}$')
SAFE_STATE=re.compile(r'^[a-z][a-z0-9_-]{0,39}$'); MASKED_SENDER=re.compile(r'^\*{4}\d{4}$')
CONTROL=re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')
class RelayError(Exception):
    def __init__(self,code): super().__init__(code); self.code=code
def credentials(path=ENV_FILE):
    try: lines=path.read_text(encoding='utf-8').splitlines()
    except OSError: raise RelayError('credentials_unavailable') from None
    for line in lines:
        if line.startswith('BAILEYS_PERSONAL_TOKEN='):
            token=line.split('=',1)[1].strip().strip('"').strip("'")
            if TOKEN.fullmatch(token): return token
    raise RelayError('credentials_unavailable')
def read_envelope(source):
    raw=source.read(MAX_ENVELOPE+1)
    if len(raw)>MAX_ENVELOPE or not raw.endswith(b'\n') or raw.count(b'\n')!=1: raise RelayError('invalid_envelope')
    try: value=json.loads(raw[:-1].decode('utf-8',errors='strict'))
    except (UnicodeDecodeError,ValueError): raise RelayError('invalid_envelope') from None
    if not isinstance(value,dict): raise RelayError('invalid_envelope')
    return value
def api(method,path,token,body=None):
    connection=http.client.HTTPConnection(HOST,PORT,timeout=15)
    headers={'Authorization':'Bearer '+token,'Accept':'application/json'}; payload=None
    if body is not None:
        payload=json.dumps(body,ensure_ascii=False,separators=(',',':')).encode('utf-8'); headers['Content-Type']='application/json'
    try:
        connection.request(method,path,body=payload,headers=headers); response=connection.getresponse(); raw=response.read(MAX_RESPONSE+1)
        if len(raw)>MAX_RESPONSE: raise RelayError('response_too_large')
        value=json.loads(raw.decode('utf-8',errors='strict'))
        if not isinstance(value,dict): raise ValueError()
        return response.status,value
    except RelayError: raise
    except (OSError,UnicodeDecodeError,ValueError,http.client.HTTPException): raise RelayError('transport_unconfirmed') from None
    finally: connection.close()
def exact(value,keys): return isinstance(value,dict) and set(value)==set(keys)
def status_response(code,value):
    if code!=200 or not exact(value,{'ok','whatsapp','connected','pairingRequired','sender'}): raise RelayError('status_unavailable')
    if value.get('ok') is not True or not isinstance(value.get('connected'),bool) or not isinstance(value.get('pairingRequired'),bool): raise RelayError('status_invalid')
    state=value.get('whatsapp'); sender=value.get('sender')
    if not isinstance(state,str) or not SAFE_STATE.fullmatch(state): raise RelayError('status_invalid')
    if sender is not None and (not isinstance(sender,str) or not MASKED_SENDER.fullmatch(sender)): raise RelayError('status_invalid')
    return {'ok':True,'whatsapp':state,'connected':value['connected'],'pairingRequired':value['pairingRequired'],'sender':sender}
def recipients_response(code,value):
    if code!=200 or not exact(value,{'destinations'}) or not isinstance(value.get('destinations'),list) or len(value['destinations'])>200: raise RelayError('recipients_unavailable')
    recipients=[]; seen=set()
    for item in value['destinations']:
        if not exact(item,{'id','name','type'}): raise RelayError('recipients_invalid')
        alias,label,kind=item['id'],item['name'],item['type']
        if not isinstance(alias,str) or not ALIAS.fullmatch(alias) or alias in seen: raise RelayError('recipients_invalid')
        if not isinstance(label,str) or not label.strip() or len(label)>150 or CONTROL.search(label): raise RelayError('recipients_invalid')
        if kind not in {'contact','group'}: raise RelayError('recipients_invalid')
        seen.add(alias); recipients.append({'alias':alias,'label':label})
    return {'ok':True,'recipients':recipients}
def valid_request_id(value):
    if not isinstance(value,str): return False
    try: uuid.UUID(value)
    except (ValueError,AttributeError): return False
    return bool(re.fullmatch(r'[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[1-8][a-fA-F0-9]{3}-[89aAbB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}',value))
def send_response(code,value,recipient,request_id,allow_uncertain=False):
    if code not in {200,202} or not exact(value,{'requestId','jobs'}) or value.get('requestId')!=request_id: raise RelayError('send_rejected')
    jobs=value.get('jobs')
    if not isinstance(jobs,list) or len(jobs)!=1: raise RelayError('send_state_invalid')
    job=jobs[0]
    if not exact(job,{'destination','state','code','confirmation'}): raise RelayError('send_state_invalid')
    destination=job.get('destination')
    if not isinstance(destination,str) or (not ALIAS.fullmatch(destination) and not valid_request_id(destination)) or (recipient is not None and destination!=recipient): raise RelayError('send_state_invalid')
    state=job.get('state')
    if state not in {'queued','dispatching','accepted','confirmed','unknown','failed'}: raise RelayError('send_state_invalid')
    if state in {'unknown','failed'} and not allow_uncertain: raise RelayError('send_unconfirmed' if state=='unknown' else 'send_failed')
    return {'ok':True,'requestId':request_id,'recipient':destination,'state':state}
def bounded_int(value,minimum,maximum):
    return isinstance(value,int) and not isinstance(value,bool) and minimum<=value<=maximum
def query_path(path,params):
    clean=[(key,value) for key,value in params if value is not None]
    return path+('?' + urlencode(clean) if clean else '')
def chat_item(value,preview=False):
    keys={'chatRef','name','kind','lastMessageAt','indexedMessageCount','lastMessageRef'}|({'deletable'} if preview else set())
    if not exact(value,keys) or not valid_request_id(value.get('chatRef')): raise RelayError('chats_invalid')
    name=value.get('name'); kind=value.get('kind'); last_at=value.get('lastMessageAt'); count=value.get('indexedMessageCount'); last_ref=value.get('lastMessageRef')
    if name is not None and (not isinstance(name,str) or len(name)>200 or CONTROL.search(name)): raise RelayError('chats_invalid')
    if kind not in {'contact','group'} or (last_at is not None and not bounded_int(last_at,0,2**63-1)) or not bounded_int(count,0,1000000): raise RelayError('chats_invalid')
    if last_ref is not None and not valid_request_id(last_ref): raise RelayError('chats_invalid')
    if preview and not isinstance(value.get('deletable'),bool): raise RelayError('chats_invalid')
    return dict(value)
def message_item(value):
    if not exact(value,{'messageRef','messageTimestamp','fromMe','contentType','text','deleted'}) or not valid_request_id(value.get('messageRef')): raise RelayError('messages_invalid')
    text=value.get('text'); kind=value.get('contentType')
    if not bounded_int(value.get('messageTimestamp'),0,2**63-1) or not isinstance(value.get('fromMe'),bool) or not isinstance(value.get('deleted'),bool): raise RelayError('messages_invalid')
    if not isinstance(kind,str) or not kind or len(kind)>100 or CONTROL.search(kind): raise RelayError('messages_invalid')
    if text is not None and (not isinstance(text,str) or len(text)>20000 or CONTROL.search(text)): raise RelayError('messages_invalid')
    if value.get('deleted') is True and text is not None: raise RelayError('messages_invalid')
    return dict(value)
def chats_response(code,value):
    if code!=200 or not exact(value,{'chats'}) or not isinstance(value.get('chats'),list) or len(value['chats'])>100: raise RelayError('chats_unavailable')
    return {'ok':True,'chats':[chat_item(item) for item in value['chats']]}
def messages_response(code,value):
    if code!=200 or not exact(value,{'messages'}) or not isinstance(value.get('messages'),list) or len(value['messages'])>100: raise RelayError('messages_unavailable')
    return {'ok':True,'messages':[message_item(item) for item in value['messages']]}
def history_response(code,value,chat_ref):
    if code not in {200,202} or not exact(value,{'accepted','chatRef','count'}) or value.get('accepted') is not True or value.get('chatRef')!=chat_ref or not bounded_int(value.get('count'),1,50): raise RelayError('history_invalid')
    return {'ok':True,'accepted':True,'chatRef':chat_ref,'count':value['count']}
def chat_action_response(code,value,chat_ref,state):
    if code!=200 or not exact(value,{'state','chatRef'}) or value.get('state')!=state or value.get('chatRef')!=chat_ref: raise RelayError('chat_action_invalid')
    return {'ok':True,'state':state,'chatRef':chat_ref}
def message_action_response(code,value,message_ref,mode):
    if code!=200 or not exact(value,{'state','messageRef','mode'}) or value.get('state')!='deleted' or value.get('messageRef')!=message_ref or value.get('mode')!=mode: raise RelayError('message_action_invalid')
    return {'ok':True,'state':'deleted','messageRef':message_ref,'mode':mode}
def cleanup_preview_response(code,value):
    if code!=200 or not exact(value,{'snapshotId','expiresAt','chats'}) or not valid_request_id(value.get('snapshotId')) or not bounded_int(value.get('expiresAt'),1,2**63-1) or not isinstance(value.get('chats'),list) or len(value['chats'])>100: raise RelayError('cleanup_preview_invalid')
    return {'ok':True,'snapshotId':value['snapshotId'],'expiresAt':value['expiresAt'],'chats':[chat_item(item,True) for item in value['chats']]}
def cleanup_apply_response(code,value):
    if code!=200 or not exact(value,{'receipts'}) or not isinstance(value.get('receipts'),list) or len(value['receipts'])>100: raise RelayError('cleanup_apply_invalid')
    receipts=[]
    for item in value['receipts']:
        if not isinstance(item,dict) or set(item) not in ({'chatRef','state'},{'chatRef','state','code'}) or not valid_request_id(item.get('chatRef')) or item.get('state') not in {'deleted','failed','skipped'}: raise RelayError('cleanup_apply_invalid')
        if 'code' in item and (not isinstance(item['code'],str) or not SAFE_STATE.fullmatch(item['code'])): raise RelayError('cleanup_apply_invalid')
        receipts.append(dict(item))
    return {'ok':True,'receipts':receipts}
def execute(envelope,token,api_call=api):
    if not TOKEN.fullmatch(token or '') or not isinstance(envelope,dict): raise RelayError('invalid_request')
    action=envelope.get('action')
    if action=='status':
        if set(envelope)!={'action'}: raise RelayError('invalid_request')
        return status_response(*api_call('GET','/v1/status',token,None))
    if action=='recipients':
        if set(envelope)!={'action'}: raise RelayError('invalid_request')
        return recipients_response(*api_call('GET','/v1/whatsapp/destinations',token,None))
    if action=='request_status':
        if set(envelope)!={'action','requestId'} or not valid_request_id(envelope.get('requestId')): raise RelayError('invalid_request')
        request_id=envelope['requestId']
        return send_response(*api_call('GET',f'/v1/whatsapp/requests/{request_id}',token,None),None,request_id,allow_uncertain=True)
    if action=='chats':
        if not {'action'}<=set(envelope)<= {'action','query','limit','before'}: raise RelayError('invalid_request')
        query=envelope.get('query'); limit=envelope.get('limit'); before=envelope.get('before')
        if query is not None and (not isinstance(query,str) or len(query)>200 or CONTROL.search(query)): raise RelayError('invalid_query')
        if limit is not None and not bounded_int(limit,1,100): raise RelayError('invalid_limit')
        if before is not None and not bounded_int(before,0,2**63-1): raise RelayError('invalid_before')
        path=query_path('/v1/whatsapp/chats',[('query',query),('limit',limit),('before',before)])
        return chats_response(*api_call('GET',path,token,None))
    if action=='messages':
        if not {'action','chatRef'}<=set(envelope)<= {'action','chatRef','limit','before'} or not valid_request_id(envelope.get('chatRef')): raise RelayError('invalid_request')
        limit=envelope.get('limit'); before=envelope.get('before')
        if limit is not None and not bounded_int(limit,1,100): raise RelayError('invalid_limit')
        if before is not None and not bounded_int(before,0,2**63-1): raise RelayError('invalid_before')
        path=query_path(f"/v1/whatsapp/chats/{envelope['chatRef']}/messages",[('limit',limit),('before',before)])
        return messages_response(*api_call('GET',path,token,None))
    if action=='search_messages':
        if not {'action','query'}<=set(envelope)<= {'action','query','chatRef','limit'}: raise RelayError('invalid_request')
        query=envelope.get('query'); chat_ref=envelope.get('chatRef'); limit=envelope.get('limit')
        if not isinstance(query,str) or not query.strip() or len(query)>500 or CONTROL.search(query): raise RelayError('invalid_query')
        if chat_ref is not None and not valid_request_id(chat_ref): raise RelayError('invalid_chat_ref')
        if limit is not None and not bounded_int(limit,1,100): raise RelayError('invalid_limit')
        path=query_path('/v1/whatsapp/search',[('query',query),('chatRef',chat_ref),('limit',limit)])
        return messages_response(*api_call('GET',path,token,None))
    if action=='history_more':
        if not {'action','chatRef'}<=set(envelope)<= {'action','chatRef','count'} or not valid_request_id(envelope.get('chatRef')): raise RelayError('invalid_request')
        count=envelope.get('count',50)
        if not bounded_int(count,1,50): raise RelayError('invalid_count')
        chat_ref=envelope['chatRef']
        return history_response(*api_call('POST',f'/v1/whatsapp/chats/{chat_ref}/history',token,{'count':count}),chat_ref)
    if action in {'send','send_chat'}:
        expected={'action','recipient','message','requestId'} if action=='send' else {'action','chatRef','message','requestId'}
        if set(envelope)!=expected: raise RelayError('invalid_request')
        target=envelope.get('recipient') if action=='send' else envelope.get('chatRef')
        message,request_id=envelope.get('message'),envelope.get('requestId')
        if action=='send' and (not isinstance(target,str) or not ALIAS.fullmatch(target)): raise RelayError('recipient_denied')
        if action=='send_chat' and not valid_request_id(target): raise RelayError('invalid_chat_ref')
        if not isinstance(message,str) or not message.strip() or len(message)>8000 or CONTROL.search(message): raise RelayError('invalid_message')
        if not valid_request_id(request_id): raise RelayError('invalid_request_id')
        if action=='send':
            payload={'requestId':request_id,'destinations':[target],'text':message,'mediaIds':[]}; path='/v1/whatsapp/send'
        else:
            payload={'requestId':request_id,'chatRef':target,'text':message}; path='/v1/whatsapp/send-chat'
        return send_response(*api_call('POST',path,token,payload),target,request_id)
    if action in {'archive_chat','delete_chat'}:
        if set(envelope)!={'action','chatRef','lastMessageRef'} or not valid_request_id(envelope.get('chatRef')) or not valid_request_id(envelope.get('lastMessageRef')): raise RelayError('invalid_request')
        chat_ref=envelope['chatRef']; operation='archive' if action=='archive_chat' else 'delete'; state='archived' if action=='archive_chat' else 'deleted'
        return chat_action_response(*api_call('POST',f'/v1/whatsapp/chats/{chat_ref}/{operation}',token,{'lastMessageRef':envelope['lastMessageRef']}),chat_ref,state)
    if action=='delete_message':
        if not {'action','messageRef'}<=set(envelope)<= {'action','messageRef','mode'} or not valid_request_id(envelope.get('messageRef')): raise RelayError('invalid_request')
        mode=envelope.get('mode','for_me')
        if mode not in {'for_me','everyone'}: raise RelayError('invalid_delete_mode')
        message_ref=envelope['messageRef']
        return message_action_response(*api_call('POST',f'/v1/whatsapp/messages/{message_ref}/delete',token,{'mode':mode}),message_ref,mode)
    if action=='cleanup_preview':
        if not {'action','inactiveBefore'}<=set(envelope)<= {'action','inactiveBefore','maxIndexedMessages','limit'}: raise RelayError('invalid_request')
        inactive=envelope.get('inactiveBefore'); max_messages=envelope.get('maxIndexedMessages',3); limit=envelope.get('limit',50)
        if not bounded_int(inactive,1,2**63-1) or not bounded_int(max_messages,0,100) or not bounded_int(limit,1,100): raise RelayError('invalid_request')
        payload={'inactiveBefore':inactive,'maxIndexedMessages':max_messages,'limit':limit}
        return cleanup_preview_response(*api_call('POST','/v1/whatsapp/cleanup/preview',token,payload))
    if action=='cleanup_apply':
        if set(envelope)!={'action','snapshotId','chatRefs'} or not valid_request_id(envelope.get('snapshotId')): raise RelayError('invalid_request')
        refs=envelope.get('chatRefs')
        if not isinstance(refs,list) or not 1<=len(refs)<=100 or len(set(refs))!=len(refs) or any(not valid_request_id(ref) for ref in refs): raise RelayError('invalid_chat_refs')
        payload={'snapshotId':envelope['snapshotId'],'chatRefs':refs}
        return cleanup_apply_response(*api_call('POST','/v1/whatsapp/cleanup/apply',token,payload))
    raise RelayError('invalid_request')
def main():
    try: result=execute(read_envelope(sys.stdin.buffer),credentials())
    except RelayError as error: result={'ok':False,'error':error.code}
    except Exception: result={'ok':False,'error':'bridge_unavailable'}
    print(json.dumps(result,ensure_ascii=True,separators=(',',':')),flush=True)
if __name__=='__main__': main()
