import test from 'node:test';
import assert from 'node:assert/strict';
import sharp from 'sharp';
import { generateWAMessageContent } from '@whiskeysockets/baileys';
import { prepareContent } from '../src/sender.js';

test('WhatsApp wire preview preserves a sharp embedded image and uploaded original dimensions',async()=>{
  const source=await sharp({create:{width:1200,height:674,channels:3,background:'#888888'}}).png().toBuffer();
  const avatar=await sharp({create:{width:400,height:400,channels:3,background:'#00ff00'}}).png().toBuffer();
  let uploads=0;
  const content=await prepareContent({text:'Example https://x.com/example/status/123',link:'https://x.com/example/status/123',
    preview:{title:'Example',imageUrl:'https://pbs.twimg.com/media/fixture.jpg',avatarUrl:'https://pbs.twimg.com/profile_images/avatar.jpg'}},
    {waUploadToServer:async()=>{uploads++;return {mediaUrl:'https://example.invalid/image',directPath:'/test/image'};}},
    async url=>new Response(url.includes('profile_images')?avatar:source,{headers:{'Content-Type':'image/png'}}));
  const wire=(await generateWAMessageContent(content,{})).extendedTextMessage;
  const embedded=await sharp(wire.jpegThumbnail).metadata();
  assert.equal(uploads,1);assert.ok(embedded.width>=1000,'embedded preview must not degrade into a 240px image');
  assert.equal(wire.thumbnailWidth,1200);assert.equal(wire.thumbnailHeight,674);
  assert.equal(wire.thumbnailDirectPath,'/test/image');assert.equal(wire.mediaKey.length,32);
  assert.equal(wire.thumbnailSha256.length,32);assert.equal(wire.title,'Example');
});
