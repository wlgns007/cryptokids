import { Buffer } from "node:buffer";
import zlib from "node:zlib";

const BRAND = Object.freeze({
  primary: hexToRgba("#2D2A6A"),
  accent: hexToRgba("#7C4DFF"),
  success: hexToRgba("#10B981"),
  cta: hexToRgba("#2563EB"),
  background: hexToRgba("#FAFAFA"),
  text: hexToRgba("#0F172A")
});

const ICON_SPECS = Object.freeze({
  "ck-wallet-icon-192.v1.png": { size: 192, maskable: false, apple: false },
  "ck-wallet-icon-512.v1.png": { size: 512, maskable: false, apple: false },
  "ck-wallet-icon-maskable-512.v1.png": { size: 512, maskable: true, apple: false },
  "ck-wallet-apple-touch-152.v1.png": { size: 152, maskable: false, apple: true },
  "ck-wallet-apple-touch-180.v1.png": { size: 180, maskable: false, apple: true }
});

const ICON_CACHE = new Map();

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i += 1) {
    let c = i;
    for (let k = 0; k < 8; k += 1) {
      c = (c & 1) ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[i] = c >>> 0;
  }
  return table;
})();

export function knownIcon(name) {
  return Object.prototype.hasOwnProperty.call(ICON_SPECS, name);
}

export function generateIcon(name) {
  if (!knownIcon(name)) return null;
  if (ICON_CACHE.has(name)) return ICON_CACHE.get(name);
  const spec = ICON_SPECS[name];
  const pixels = paintPixels(spec);
  const png = encodePng(spec.size, spec.size, pixels);
  ICON_CACHE.set(name, png);
  return png;
}

function paintPixels({ size, maskable, apple }) {
  const total = size * size * 4;
  const pixels = new Uint8ClampedArray(total);
  const base = apple ? BRAND.background : BRAND.primary;
  fillRect(pixels, size, 0, 0, size, size, base);

  const accentRadius = size * (maskable ? 0.44 : 0.38);
  drawCircle(pixels, size, size / 2, size / 2, accentRadius, BRAND.accent);

  const highlightRadius = accentRadius * 0.82;
  drawCircle(
    pixels,
    size,
    size / 2.9,
    size / 2.9,
    highlightRadius,
    { ...BRAND.background, a: 180 }
  );

  const successRadius = size * 0.19;
  drawCircle(pixels, size, size * 0.72, size * 0.7, successRadius, BRAND.success);

  const ctaHeight = Math.max(6, Math.round(size * 0.17));
  fillRect(
    pixels,
    size,
    0,
    size - ctaHeight,
    size,
    size,
    maskable ? BRAND.accent : BRAND.cta,
    maskable ? 230 : 255
  );

  const notchPadding = Math.max(4, Math.round(size * 0.08));
  drawRoundedRectStroke(
    pixels,
    size,
    notchPadding,
    notchPadding,
    size - notchPadding * 2,
    size - notchPadding * 2,
    Math.max(8, Math.round(size * 0.16)),
    apple ? BRAND.primary : BRAND.background,
    apple ? 120 : 80
  );

  softenEdges(pixels, size, apple ? BRAND.primary : BRAND.background, maskable);

  return pixels;
}

function softenEdges(pixels, size, edgeColor, maskable) {
  const inset = Math.max(1, Math.round(size * 0.02));
  for (let y = 0; y < size; y += 1) {
    for (let x = 0; x < size; x += 1) {
      if (x < inset || y < inset || x >= size - inset || y >= size - inset) {
        const blendAlpha = maskable ? 0.18 : 0.12;
        blendPixel(pixels, size, x, y, edgeColor, blendAlpha);
      }
    }
  }
}

function encodePng(width, height, pixels) {
  const raw = Buffer.alloc((width * 4 + 1) * height);
  let offset = 0;
  for (let y = 0; y < height; y += 1) {
    raw[offset] = 0; // filter type: None
    offset += 1;
    const rowStart = y * width * 4;
    for (let x = 0; x < width * 4; x += 1) {
      raw[offset] = pixels[rowStart + x];
      offset += 1;
    }
  }

  const header = Buffer.alloc(13);
  header.writeUInt32BE(width, 0);
  header.writeUInt32BE(height, 4);
  header.writeUInt8(8, 8); // bit depth
  header.writeUInt8(6, 9); // color type: RGBA
  header.writeUInt8(0, 10); // compression
  header.writeUInt8(0, 11); // filter
  header.writeUInt8(0, 12); // interlace

  const idat = zlib.deflateSync(raw, { level: 9 });

  const chunks = [
    chunk("IHDR", header),
    chunk("IDAT", idat),
    chunk("IEND", Buffer.alloc(0))
  ];

  return Buffer.concat([Buffer.from("\x89PNG\r\n\x1a\n", "binary"), ...chunks]);
}

function chunk(type, data) {
  const typeBuffer = Buffer.from(type, "ascii");
  const length = Buffer.alloc(4);
  length.writeUInt32BE(data.length, 0);
  const crcValue = crc32(Buffer.concat([typeBuffer, data]));
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crcValue, 0);
  return Buffer.concat([length, typeBuffer, data, crc]);
}

function crc32(buffer) {
  let crc = 0xffffffff;
  for (let i = 0; i < buffer.length; i += 1) {
    crc = CRC_TABLE[(crc ^ buffer[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function hexToRgba(hex) {
  const clean = hex.replace(/[^0-9a-f]/gi, "");
  if (clean.length === 3) {
    const [r, g, b] = clean.split("");
    return {
      r: parseInt(r + r, 16),
      g: parseInt(g + g, 16),
      b: parseInt(b + b, 16),
      a: 255
    };
  }
  return {
    r: parseInt(clean.slice(0, 2), 16),
    g: parseInt(clean.slice(2, 4), 16),
    b: parseInt(clean.slice(4, 6), 16),
    a: 255
  };
}

function fillRect(pixels, size, x0, y0, x1, y1, color, alphaOverride) {
  const minX = Math.max(0, Math.floor(x0));
  const minY = Math.max(0, Math.floor(y0));
  const maxX = Math.min(size, Math.ceil(x1));
  const maxY = Math.min(size, Math.ceil(y1));
  for (let y = minY; y < maxY; y += 1) {
    for (let x = minX; x < maxX; x += 1) {
      blendPixel(pixels, size, x, y, color, alphaOverride !== undefined ? alphaOverride / 255 : 1);
    }
  }
}

function drawCircle(pixels, size, cx, cy, radius, color) {
  const r2 = radius * radius;
  const minX = Math.max(0, Math.floor(cx - radius));
  const maxX = Math.min(size - 1, Math.ceil(cx + radius));
  const minY = Math.max(0, Math.floor(cy - radius));
  const maxY = Math.min(size - 1, Math.ceil(cy + radius));
  for (let y = minY; y <= maxY; y += 1) {
    for (let x = minX; x <= maxX; x += 1) {
      const dx = x + 0.5 - cx;
      const dy = y + 0.5 - cy;
      const dist = dx * dx + dy * dy;
      if (dist <= r2) {
        blendPixel(pixels, size, x, y, color);
      }
    }
  }
}

function drawRoundedRectStroke(pixels, size, x, y, width, height, radius, color, alphaOverride = 1) {
  const r = Math.max(0, radius);
  const xEnd = x + width;
  const yEnd = y + height;
  const thickness = Math.max(1, Math.round(size * 0.02));
  for (let yy = Math.max(0, Math.floor(y)); yy < Math.min(size, Math.ceil(yEnd)); yy += 1) {
    for (let xx = Math.max(0, Math.floor(x)); xx < Math.min(size, Math.ceil(xEnd)); xx += 1) {
      const inX = xx >= x + r && xx < xEnd - r;
      const inY = yy >= y + r && yy < yEnd - r;
      let onEdge = false;
      if (inX) {
        onEdge = Math.abs(yy - y) < thickness || Math.abs(yy - yEnd + 1) < thickness;
      } else if (inY) {
        onEdge = Math.abs(xx - x) < thickness || Math.abs(xx - xEnd + 1) < thickness;
      } else {
        const cx = xx < x + r ? x + r : xEnd - r - 1;
        const cy = yy < y + r ? y + r : yEnd - r - 1;
        const dx = xx + 0.5 - cx;
        const dy = yy + 0.5 - cy;
        const distance = Math.sqrt(dx * dx + dy * dy);
        onEdge = distance >= r - thickness && distance <= r;
      }
      if (onEdge) {
        blendPixel(pixels, size, xx, yy, color, alphaOverride / 255);
      }
    }
  }
}

function blendPixel(pixels, size, x, y, color, alphaOverride) {
  const idx = (y * size + x) * 4;
  const srcR = pixels[idx];
  const srcG = pixels[idx + 1];
  const srcB = pixels[idx + 2];
  const srcA = pixels[idx + 3] / 255;

  const newA = (alphaOverride !== undefined ? alphaOverride : color.a / 255);
  const outA = newA + srcA * (1 - newA);

  const apply = (channel, srcChannel) => {
    if (outA === 0) return 0;
    return Math.round(((color[channel] || 0) * newA + srcChannel * srcA * (1 - newA)) / outA);
  };

  pixels[idx] = apply("r", srcR);
  pixels[idx + 1] = apply("g", srcG);
  pixels[idx + 2] = apply("b", srcB);
  pixels[idx + 3] = Math.round(outA * 255);
}

export function listIconNames() {
  return Object.keys(ICON_SPECS);
}
