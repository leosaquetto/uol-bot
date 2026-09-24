import assert from "node:assert/strict";
import test from "node:test";
import sharp from "sharp";
import { createPersonalThumbnail } from "../src/personal-thumbnail.js";

test("selo preserva avatar 400x400 e toca o topo com margem à esquerda", async () => {
  const source = await sharp({ create: { width: 400, height: 400, channels: 3, background: "#0050a0" } }).png().toBuffer();
  const result = await createPersonalThumbnail(source);
  assert.deepEqual(result.imgSize, { width: 400, height: 400 });
  assert.equal(result.imgType, "image/jpeg");
  const { data, info } = await sharp(result.bytes).raw().toBuffer({ resolveWithObject: true });
  const pixel = (x, y) => [...data.subarray((y * info.width + x) * info.channels, (y * info.width + x) * info.channels + 3)];
  assert.ok(pixel(20, 2).every(v => v > 240), "white badge starts at top");
  assert.ok(pixel(3, 2)[2] > 120 && pixel(3, 2)[0] < 30, "left margin retains original image");
  assert.ok(pixel(200, 200)[2] > 120 && pixel(200, 200)[0] < 30, "photo outside badge is retained");
  let dark = 0;
  for (let y = 20; y < 80; y++) for (let x = 20; x < 110; x++) {
    if (pixel(x, y).every(v => v < 70)) dark++;
  }
  assert.ok(dark > 200, "black logo paths are rendered");
});

test("limita mídia grande sem deformar e não amplia imagem pequena", async () => {
  for (const [width, height, expected] of [[2400, 1200, { width: 1600, height: 800 }], [20, 10, { width: 20, height: 10 }]]) {
    const input = await sharp({ create: { width, height, channels: 3, background: "#ff8000" } }).png().toBuffer();
    assert.deepEqual((await createPersonalThumbnail(input)).imgSize, expected);
  }
});

test("imagem inválida falha sem gerar arquivo parcial", async () => {
  await assert.rejects(createPersonalThumbnail(Buffer.from("not an image")), /preview_image_render_failed/);
});
