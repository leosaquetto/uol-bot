import { readFileSync } from "node:fs";
import sharp from "sharp";

const badge = readFileSync(new URL("../assets/pushpushpushsaquetto.svg", import.meta.url));

// Preserve the supplied artwork, aspect ratio and transparency.
export async function createPersonalThumbnail(bytes, { avatarBytes } = {}) {
  try {
    let base = await sharp(bytes, { limitInputPixels: 40_000_000 })
      .rotate()
      .resize({ width: 1600, height: 1600, fit: "inside", withoutEnlargement: true })
      .flatten({ background: "#ffffff" })
      .png()
      .toBuffer({ resolveWithObject: true });
    // Expand a small source before adding the vector badge, never after it.
    // This keeps the brand sharp even when X only supplies a tiny video frame.
    if (Math.max(base.info.width, base.info.height) < 1080) {
      base = await sharp(base.data)
        .resize({ width: 1080, height: 1080, fit: "inside" })
        .png()
        .toBuffer({ resolveWithObject: true });
    }
    const { width, height } = base.info;
    const size = Math.max(1, Math.round(Math.min(width, height) * 0.36));
    const overlay = await sharp(badge, { density: 216 }).resize({ width: size }).png()
      .toBuffer({ resolveWithObject: true });
    const left = Math.max(0, width - overlay.info.width - Math.round(width * 0.015));
    const top = Math.min(height - overlay.info.height, Math.round(height * 0.015));
    const layers = [{ input: overlay.data, top, left }];
    if (avatarBytes) {
      try {
        const diameter = Math.max(1, Math.round(Math.min(width, height) * 0.16));
        const avatarLeft = Math.round(width * 0.015);
        const avatarTop = height - diameter - Math.round(height * 0.015);
        const radius = diameter / 2;
        const mask = Buffer.from(`<svg width="${diameter}" height="${diameter}"><circle cx="${radius}" cy="${radius}" r="${radius}" fill="white"/></svg>`);
        const avatar = await sharp(avatarBytes, { limitInputPixels: 16_000_000 })
          .rotate().resize(diameter, diameter, { fit: "cover" }).ensureAlpha()
          .composite([{ input: mask, blend: "dest-in" }]).png().toBuffer();
        const shadow = Buffer.from(`<svg width="${width}" height="${height}">
          <defs><radialGradient id="shadow"><stop offset="0" stop-color="black" stop-opacity="0.60"/>
          <stop offset="0.55" stop-color="black" stop-opacity="0.42"/>
          <stop offset="1" stop-color="black" stop-opacity="0"/></radialGradient></defs>
          <circle cx="${avatarLeft + radius}" cy="${avatarTop + radius}" r="${diameter * 0.9}" fill="url(#shadow)"/>
        </svg>`);
        layers.push({ input: shadow, top: 0, left: 0 }, { input: avatar, top: avatarTop, left: avatarLeft });
      } catch {
        // An unavailable or corrupt optional avatar must not discard the post.
      }
    }
    const output = await sharp(base.data)
      .composite(layers)
      .jpeg({ quality: 92, chromaSubsampling: "4:4:4" })
      .toBuffer();
    return { bytes: output, imgType: "image/jpeg", imgSize: { width, height } };
  } catch {
    throw new Error("preview_image_render_failed");
  }
}
