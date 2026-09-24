import { readFileSync } from "node:fs";
import sharp from "sharp";

const badge = readFileSync(new URL("../assets/pushpushpushsaquetto.svg", import.meta.url));

// Preserve the supplied artwork, aspect ratio and transparency.
export async function createPersonalThumbnail(bytes) {
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
    const output = await sharp(base.data)
      .composite([{ input: overlay.data, top, left }])
      .jpeg({ quality: 92, chromaSubsampling: "4:4:4" })
      .toBuffer();
    return { bytes: output, imgType: "image/jpeg", imgSize: { width, height } };
  } catch {
    throw new Error("preview_image_render_failed");
  }
}
