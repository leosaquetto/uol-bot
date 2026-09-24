import test from 'node:test';
import assert from 'node:assert/strict';
import { parsePost } from '../src/x-post.js';

const id='2103211906144297462',target=`https://x.com/taylorswift13/status/${id}`;
const html=(tweetExtra='',detailsExtra='',foreignName='taylorswift13')=>`<script>
($R=>$R[0]={records:{
  tweet:$R[1]={__id:"tweet",__typename:"Tweet",rest_id:"${id}",core:$R[2]={__ref:"core"},details:$R[3]={__ref:"details"},
    reply_to_results:null,quoted_tweet_results:null,note_tweet:null,article:null,media_entities2:$R[4]={__refs:$R[5]=["media"]}${tweetExtra}},
  core:$R[6]={__id:"core",user_results:$R[7]={__ref:"ur"}},
  ur:$R[8]={__id:"ur",result:$R[9]={__ref:"user"}},
  user:$R[10]={__id:"user",core:$R[11]={__ref:"uc"},avatar:$R[12]={__ref:"avatar"}},
  uc:$R[13]={__id:"uc",screen_name:"${foreignName}",name:"Taylor Swift"},
  avatar:$R[14]={__id:"avatar",image_url:"https://pbs.twimg.com/profile_images/1/a_normal.jpg"},
  details:$R[15]={__id:"details",full_text:"Meu texto ♥\\nlinha completa"${detailsExtra}},
  media:$R[16]={__id:"media",type:"video",media_url_https:"https://pbs.twimg.com/amplify_video_thumb/2/frame.jpg",expanded_url:"https://x.com/other/status/2103211906144297455/video/1"},
  quoted:$R[17]={__id:"quoted",full_text:"TEXTO DE OUTRO AUTOR"}
}})($R["tsr"]);window.sideEffectShouldNeverRun();</script>`;

test('literal X bootstrap preserves own full text and does not mistake an embedded video for a quote',()=>{
  const p=parsePost(html(),target);
  assert.equal(p.type,'post');assert.equal(p.text,'Meu texto ♥\nlinha completa');assert.equal(p.name,'Taylor Swift');
  assert.match(p.imageUrl,/frame\.jpg$/);assert.match(p.avatarUrl,/_400x400\.jpg$/);
});
test('structured quote and reply flags exclude foreign text and reject long-form content until supported',()=>{
  assert.equal(parsePost(html(',quoted_tweet_results:$R[18]={__ref:"quoted"}'),target).type,'quote');
  assert.equal(parsePost(html(',reply_to_results:$R[18]={__ref:"quoted"}'),target).type,'reply');
  assert.throws(()=>parsePost(html(',note_tweet:$R[18]={__ref:"quoted"}'),target),/incomplete/);
  assert.throws(()=>parsePost(html('','','wrongaccount'),target),/author_mismatch/);
});
test('bootstrap parser never executes expressions disguised as content',()=>{
  assert.throws(()=>parsePost(html('',',full_text:fetch("https://evil.test")'),target),/incomplete/);
  assert.throws(()=>parsePost(html('',',__proto__:{injected:true}'),target),/incomplete/);
});
