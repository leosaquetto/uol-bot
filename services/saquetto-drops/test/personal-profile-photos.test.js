import test from 'node:test';
import assert from 'node:assert/strict';
import {Readable} from 'node:stream';
import {createPersonalProfilePhotos,safeFetch} from '../src/personal-profile-photos.js';

const numbers=['5511999990001','5511999990002','5521999990003'];
const endpoint=new URL('http://localhost/v1/whatsapp/profile-photos');
function request(value){
  const req=Readable.from([Buffer.from(JSON.stringify(value))]);
  req.method='POST';
  req.headers={'content-type':'application/json'};
  return req;
}
function fixture(options={}){
  const calls=[];
  const socket={
    async profilePictureUrl(jid,kind){
      calls.push({jid,kind});
      if(jid===numbers[2]+'@s.whatsapp.net')throw Object.assign(new Error('not found'),{output:{statusCode:404}});
      return 'https://pps.whatsapp.net/picture';
    },
  };
  const whatsapp={isReady:()=>true,socket:()=>socket};
  return {api:createPersonalProfilePhotos({whatsapp,...options}),calls};
}
test('returns visible photos without echoing contact numbers or image URLs',async()=>{
  const image=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL/nwAAAABJRU5ErkJggg==','base64');
  const {api,calls}=fixture({fetchImage:async()=>image});
  const r=await api.route(request({numbers}),endpoint);
  assert.equal(r.status,200);
  assert.deepEqual(r.body.photos.map(v=>v.status),['available','available','unavailable']);
  assert.equal(calls.length,3);
  assert.equal(calls[0].kind,'image');
  assert.equal(r.body.photos[0].mime,'image/png');
  assert.ok(Buffer.from(r.body.photos[0].base64,'base64').length>40);
  for(const n of numbers)assert.equal(JSON.stringify(r.body).includes(n),false);
  assert.equal(JSON.stringify(r.body).includes('https://'),false);
});
test('rejects malformed, duplicate and oversized batches before contacting WhatsApp',async()=>{
  const {api,calls}=fixture();
  for(const values of [[],['55 11 99999-0001'],[numbers[0],numbers[0]],['123'],Array(4).fill(numbers[0])]){
    const r=await api.route(request({numbers:values}),endpoint);
    assert.equal(r.status,400);
  }
  assert.equal((await api.route(request({numbers:[numbers[0]],extra:true}),endpoint)).status,400);
  assert.equal(calls.length,0);
});
test('fails closed when disconnected or image source is untrusted',async()=>{
  const disconnected=createPersonalProfilePhotos({whatsapp:{isReady:()=>false}});
  assert.equal((await disconnected.route(request({numbers:[numbers[0]]}),endpoint)).status,409);
  let fetched=false;
  const api=createPersonalProfilePhotos({
    whatsapp:{isReady:()=>true,socket:()=>({profilePictureUrl:async()=> 'http://127.0.0.1/secret'})},
    fetchImage:async()=>{fetched=true;}
  });
  const result=await api.route(request({numbers:[numbers[0]]}),endpoint);
  assert.equal(result.body.photos[0].status,'failed');
  assert.equal(fetched,false);
});
test('is limited to the private POST endpoint',()=>{
  const {api}=fixture();
  assert.equal(api.matches('POST','/v1/whatsapp/profile-photos'),true);
  assert.equal(api.matches('GET','/v1/whatsapp/profile-photos'),false);
  assert.equal(api.matches('POST','/v1/whatsapp/media'),false);
});

test('fetches only HTTPS images and validates each redirect destination',async()=>{
  const original=globalThis.fetch;
  const image=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL/nwAAAABJRU5ErkJggg==','base64');
  const requested=[];
  try{
    globalThis.fetch=async input=>{
      requested.push(String(input));
      if(requested.length===1)return new Response(null,{status:302,headers:{location:'https://mmg.whatsapp.net/picture'}});
      return new Response(image,{status:200,headers:{'content-type':'image/png'}});
    };
    const bytes=await safeFetch('https://pps.whatsapp.net/picture');
    assert.deepEqual(bytes,image);
    assert.deepEqual(requested,['https://pps.whatsapp.net/picture','https://mmg.whatsapp.net/picture']);
    let count=0;
    globalThis.fetch=async()=>{count++;return new Response(null,{status:302,headers:{location:'http://127.0.0.1/private'}});};
    await assert.rejects(safeFetch('https://pps.whatsapp.net/picture'),/image_host_denied/);
    assert.equal(count,1,'a forbidden redirect must never be fetched');
  }finally{globalThis.fetch=original;}
});
