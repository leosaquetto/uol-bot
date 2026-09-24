import assert from "node:assert/strict";
import test from "node:test";
import sharp from "sharp";
import { createPersonalThumbnail } from "../src/personal-thumbnail.js";

test("selo usa base 1080px para avatar e mantém margens no topo e à direita", async () => {
  const source = await sharp({ create: { width: 400, height: 400, channels: 3, background: "#0050a0" } }).png().toBuffer();
  const result = await createPersonalThumbnail(source);
  assert.deepEqual(result.imgSize, { width: 1080, height: 1080 });
  assert.equal(result.imgType, "image/jpeg");
  const { data, info } = await sharp(result.bytes).raw().toBuffer({ resolveWithObject: true });
  const pixel = (x, y) => {
    const offset = (Math.round(y * info.height / 400) * info.width + Math.round(x * info.width / 400)) * info.channels;
    return [...data.subarray(offset, offset + 3)];
  };
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

test("limita mídia grande e amplia base pequena preservando a proporção", async () => {
  for (const [width, height, expected] of [
    [2400, 1200, { width: 1600, height: 800 }],
    [20, 10, { width: 1080, height: 540 }],
    [200, 400, { width: 540, height: 1080 }],
    [1200, 800, { width: 1200, height: 800 }],
  ]) {
    const input = await sharp({ create: { width, height, channels: 3, background: "#ff8000" } }).png().toBuffer();
    assert.deepEqual((await createPersonalThumbnail(input)).imgSize, expected);
  }
});

test("frame 138px recebe o mesmo selo nítido de uma base 1080px, sem ampliar o selo pronto", async () => {
  const render = async size => createPersonalThumbnail(await sharp({
    create: { width: size, height: size, channels: 3, background: "#0050a0" },
  }).png().toBuffer());
  const [small, large] = await Promise.all([render(138), render(1080)]);
  assert.deepEqual(small.imgSize, { width: 1080, height: 1080 });
  assert.deepEqual(small.bytes, large.bytes);
});

test("imagem inválida falha sem gerar arquivo parcial", async () => {
  await assert.rejects(createPersonalThumbnail(Buffer.from("not an image")), /preview_image_render_failed/);
});

test("avatar é circular no canto inferior esquerdo e a sombra desaparece gradualmente", async () => {
  const source = await sharp({ create: { width: 1080, height: 1080, channels: 3, background: "white" } }).png().toBuffer();
  const avatarBytes = await sharp({ create: { width: 400, height: 250, channels: 3, background: "#00ff00" } }).png().toBuffer();
  const result = await createPersonalThumbnail(source, { avatarBytes });
  const { data, info } = await sharp(result.bytes).raw().toBuffer({ resolveWithObject: true });
  const pixel = (x, y) => [...data.subarray((y * info.width + x) * info.channels, (y * info.width + x) * info.channels + 3)];
  assert.ok(pixel(103, 978)[1] > 240 && pixel(103, 978)[0] < 15, "avatar center at 1.5% margins");
  for (const [x, y] of [[20, 895], [8, 978], [103, 1075]]) {
    const [r, g, b] = pixel(x, y);
    assert.ok(Math.abs(r - g) < 5 && Math.abs(g - b) < 5, "corners and margins contain no square avatar");
  }
  const near = pixel(202, 978)[0], far = pixel(240, 978)[0];
  assert.ok(near < far && far < 254, "shadow fades out instead of a solid rectangle");
  assert.ok(pixel(350, 978).every(value => value > 250), "distant background remains untouched");
});

test("avatar inválido conserva o cartão sem círculo nem sombra", async () => {
  const source = await sharp({ create: { width: 400, height: 400, channels: 3, background: "white" } }).png().toBuffer();
  const plain = await createPersonalThumbnail(source);
  const invalid = await createPersonalThumbnail(source, { avatarBytes: Buffer.from("invalid avatar") });
  assert.deepEqual(invalid.bytes, plain.bytes);
});
