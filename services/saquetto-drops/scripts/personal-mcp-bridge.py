#!/usr/bin/env python3
"""Narrow SSH stdin bridge for the personal Baileys MCP connection."""
from __future__ import annotations
import http.client, json, re, sys, uuid
from pathlib import Path
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
    if not isinstance(destination,str) or not ALIAS.fullmatch(destination) or (recipient is not None and destination!=recipient): raise RelayError('send_state_invalid')
    state=job.get('state')
    if state not in {'queued','dispatching','accepted','confirmed','unknown','failed'}: raise RelayError('send_state_invalid')
    if state in {'unknown','failed'} and not allow_uncertain: raise RelayError('send_unconfirmed' if state=='unknown' else 'send_failed')
    return {'ok':True,'requestId':request_id,'recipient':destination,'state':state}
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
    if action!='send' or set(envelope)!={'action','recipient','message','requestId'}: raise RelayError('invalid_request')
    recipient,message,request_id=envelope['recipient'],envelope['message'],envelope['requestId']
    if not isinstance(recipient,str) or not ALIAS.fullmatch(recipient): raise RelayError('recipient_denied')
    if not isinstance(message,str) or not message.strip() or len(message)>8000 or CONTROL.search(message): raise RelayError('invalid_message')
    if not valid_request_id(request_id): raise RelayError('invalid_request_id')
    payload={'requestId':request_id,'destinations':[recipient],'text':message,'mediaIds':[]}
    return send_response(*api_call('POST','/v1/whatsapp/send',token,payload),recipient,request_id)
def main():
    try: result=execute(read_envelope(sys.stdin.buffer),credentials())
    except RelayError as error: result={'ok':False,'error':error.code}
    except Exception: result={'ok':False,'error':'bridge_unavailable'}
    print(json.dumps(result,ensure_ascii=True,separators=(',',':')),flush=True)
if __name__=='__main__': main()
