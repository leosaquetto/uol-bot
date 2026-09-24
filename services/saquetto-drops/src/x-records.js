import { parse } from 'acorn';

// Read literal Relay records from the public x-web bootstrap. Never execute it.
export function readXRecords(document) {
  const records=new Map(),references=new Map(),objects=[];
  const refIndex=node=>node?.type==='MemberExpression' && node.computed &&
    node.object.type==='Identifier' && node.object.name==='$R' &&
    Number.isSafeInteger(node.property.value) ? node.property.value : null;
  for (const script of document.querySelectorAll('script')) {
    const text=script.textContent;
    if (!text.includes('__typename:"Tweet"') || !text.includes('$R')) continue;
    if (text.length>1500000) throw new Error('x_bootstrap_too_large');
    const ast=parse(text,{ecmaVersion:'latest',sourceType:'script'}),stack=[ast];
    let visited=0;
    while(stack.length){
      const node=stack.pop();if(++visited>150000)throw new Error('x_bootstrap_too_complex');
      if(node.type==='AssignmentExpression' && node.operator==='=' && refIndex(node.left)!==null) references.set(refIndex(node.left),node.right);
      if(node.type==='ObjectExpression')objects.push(node);
      for(const [key,value] of Object.entries(node)){
        if(['start','end','type'].includes(key))continue;
        if(Array.isArray(value)){for(let i=value.length-1;i>=0;i--)if(value[i]?.type)stack.push(value[i]);}
        else if(value?.type)stack.push(value);
      }
    }
  }
  const literal=(node,seen=new Set(),depth=0)=>{
    if(!node || depth>40 || seen.has(node))throw new Error('unsupported_x_literal');
    const next=new Set(seen);next.add(node);
    const read=n=>literal(n,next,depth+1);
    if(node.type==='Literal' && !node.regex && !node.bigint)return node.value;
    if(node.type==='AssignmentExpression' && node.operator==='=' && refIndex(node.left)!==null)return read(node.right);
    if(refIndex(node)!==null)return read(references.get(refIndex(node)));
    if(node.type==='ArrayExpression')return node.elements.map(read);
    if(node.type==='UnaryExpression' && node.operator==='!' && node.argument.type==='Literal')return !node.argument.value;
    if(node.type==='ObjectExpression'){
      const out=Object.create(null);
      for(const p of node.properties){
        if(p.type!=='Property'||p.computed||p.method||p.kind!=='init')throw new Error('unsupported_x_property');
        const key=p.key.type==='Identifier'?p.key.name:p.key.value;
        if(typeof key!=='string'||['__proto__','constructor','prototype'].includes(key))throw new Error('unsafe_x_property');
        out[key]=read(p.value);
      }
      return out;
    }
    throw new Error('unsupported_x_literal');
  };
  for(const object of objects){
    const id=object.properties.find(p=>p.type==='Property'&&!p.computed&&(p.key.name||p.key.value)==='__id');
    if(id?.value.type!=='Literal'||typeof id.value.value!=='string')continue;
    // An unsupported property makes that record unavailable rather than executing code.
    try{records.set(id.value.value,literal(object));}catch{}
  }
  return records;
}

export function structuredPost(document,identity) {
  const records=readXRecords(document);
  if(!records.size)return null;
  const ref=value=>value?.__ref ? records.get(value.__ref) : null;
  const many=value=>value?.__refs?.map(id=>records.get(id)).filter(Boolean)||[];
  const tweet=[...records.values()].find(r=>r.__typename==='Tweet'&&r.rest_id===identity.id);
  if(!tweet)throw new Error('post_record_missing');
  const core=ref(tweet.core),user=ref(ref(core?.user_results)?.result),author=ref(user?.core);
  if(author?.screen_name?.toLowerCase()!==identity.author)throw new Error('post_author_mismatch');
  const details=ref(tweet.details);
  if(typeof details?.full_text!=='string'||!details.full_text.trim()||tweet.note_tweet||tweet.article)throw new Error('post_text_incomplete');
  if(!Object.hasOwn(tweet,'reply_to_results')||!Object.hasOwn(tweet,'quoted_tweet_results'))throw new Error('post_type_missing');
  const legacy=ref(tweet.legacy);
  const type=legacy?.retweeted_status_result || tweet.retweeted_status_result ? 'repost' :
    tweet.reply_to_results || tweet.reply_to_user_results ? 'reply' : tweet.quoted_tweet_results ? 'quote' : 'post';
  let text=details.full_text;
  for(const entity of many(tweet.url_entities)){
    if(typeof entity.url==='string'&&typeof entity.expanded_url==='string')text=text.replaceAll(entity.url,entity.expanded_url);
  }
  const media=many(tweet.media_entities2);
  // Display-only media t.co tokens are not part of the author's visible body.
  for(const item of media){
    const range=item.indices;
    if(Array.isArray(range)&&range.length===2){
      const token=[...details.full_text].slice(...range).join('');
      if(/^https:\/\/t\.co\/[A-Za-z0-9]+$/.test(token))text=text.replace(token,'').trim();
    }
  }
  const avatar=ref(user?.avatar)?.image_url||'';
  const avatarUrl=avatar.replace(/_(?:mini|normal|bigger|reasonably_small|200x200|x96)(\.[a-z]+)(?=[?#]|$)/i,'_400x400$1');
  const image=media.find(m=>typeof m.media_url_https==='string')?.media_url_https||'';
  return {...identity,type,name:author.name||identity.author,text,
    publishedAt:new Date(Number((BigInt(identity.id)>>22n)+1288834974657n)).toISOString(),
    imageUrl:image||avatarUrl,avatarUrl:image?avatarUrl:''};
}
