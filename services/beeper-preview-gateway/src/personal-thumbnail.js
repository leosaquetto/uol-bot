import { readFileSync } from "node:fs";
import sharp from "sharp";

const badge = readFileSync(new URL("../assets/pushpushpushsaquetto.svg", import.meta.url));

// Preserve the supplied artwork, including its white background.
export async function createPersonalThumbnail(bytes) {
  try {
    const base = await sharp(bytes, { limitInputPixels: 40_000_000 })
      .rotate()
      .resize({ width: 1600, height: 1600, fit: "inside", withoutEnlargement: true })
      .flatten({ background: "#ffffff" })
      .png()
      .toBuffer({ resolveWithObject: true });
    const { width, height } = base.info;
    const size = Math.max(1, Math.round(Math.min(width, height) * 0.24));
    const left = Math.min(width - size, Math.round(width * 0.04));
    const overlay = await sharp(badge).resize(size, size).png().toBuffer();
    const output = await sharp(base.data)
      .composite([{ input: overlay, top: 0, left }])
      .jpeg({ quality: 92, chromaSubsampling: "4:4:4" })
      .toBuffer();
    return { bytes: output, imgType: "image/jpeg", imgSize: { width, height } };
  } catch {
    throw new Error("preview_image_render_failed");
  }
}
