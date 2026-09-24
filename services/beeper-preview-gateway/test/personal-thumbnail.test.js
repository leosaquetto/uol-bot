import assert from "node:assert/strict";
import test from "node:test";
import sharp from "sharp";
import { createPersonalThumbnail } from "../src/personal-thumbnail.js";

test("selo preserva avatar 400x400 e mantém margens no topo e à direita", async () => {
  const source = await sharp({ create: { width: 400, height: 400, channels: 3, background: "#0050a0" } }).png().toBuffer();
  const result = await createPersonalThumbnail(source);
  assert.deepEqual(result.imgSize, { width: 400, height: 400 });
  assert.equal(result.imgType, "image/jpeg");
  const { data, info } = await sharp(result.bytes).raw().toBuffer({ resolveWithObject: true });
  const pixel = (x, y) => [...data.subarray((y * info.width + x) * info.channels, (y * info.width + x) * info.channels + 3)];
  for (const [x, y] of [[335, 3], [397, 40], [20, 30], [335, 92]]) {
    assert.ok(pixel(x, y)[2] > 120 && pixel(x, y)[0] < 30, "margins and area outside rectangular badge retain photo");
  }
  assert.ok(pixel(200, 200)[2] > 120 && pixel(200, 200)[0] < 30, "photo outside badge is retained");
  let dark = 0, white = 0, enlargedArea = 0;
  for (let y = 6; y < 76; y++) for (let x = 250; x < 394; x++) {
    if (pixel(x, y).every(v => v < 70)) dark++;
    if (pixel(x, y).every(v => v > 230)) white++;
    if (x < 280 && pixel(x, y).every(v => v > 230)) enlargedArea++;
  }
  assert.ok(dark > 200, "black logo paths are rendered");
  assert.ok(white > 200, "white logo paths are rendered");
  assert.ok(enlargedArea > 200, "enlarged badge extends beyond the former smaller footprint");
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
