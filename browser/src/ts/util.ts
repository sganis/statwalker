import { fromUnixTime, formatDistanceToNow, isFuture, format } from "date-fns";

export function humanTime(unix: number): string {
  const d = fromUnixTime(unix);
  if (isFuture(d)) {
    return `in the future? (${format(d, "yyyy-MM-dd")})`;
  }
  return formatDistanceToNow(d, { addSuffix: true });
}


export function formatBytes(bytes, decimals = 1) {
  if (!+bytes) return '0 B';

  const k = 1000;
  const dm = decimals < 0 ? 0 : decimals;
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${units[i]}`;
}

export const humanCount = (n, maxFrac = 1, locale = 'en') =>
  new Intl.NumberFormat(locale, { notation: 'compact', compactDisplay: 'short', maximumFractionDigits: maxFrac }).format(n);
// compact(1234) -> "1.2K"


// Examples:
// formatBytes(1000) → "1 KB"
// formatBytes(1024) → "1 KB" (still 1KB since we use 1000 as base)
// formatBytes(1000000) → "1 MB"
// formatBytes(1500000) → "1.5 MB"
// formatBytes(1500000, 2) → "1.50 MB"
// formatBytes(0) → "0 B"
// formatBytes(500) → "500 B"


export function getParent(inputPath) {
  if (!inputPath || typeof inputPath !== 'string') {
    return '.';
  }

  let s = inputPath.trim();
  
  // Normalize separators - convert all to forward slashes for processing
  s = s.replace(/\\/g, '/');
  
  // Detect if this is a Windows path (has drive letter)
  const isWindows = /^[a-zA-Z]:/.test(s);
  
  // Handle Windows drive root cases
  if (isWindows) {
    // "C:" -> "C:/"
    if (/^[a-zA-Z]:$/.test(s)) {
      s += '/';
    }
    // "C:/" is root, return as-is (will be converted back to backslashes)
    if (/^[a-zA-Z]:\/$/.test(s)) {
      return s.replace(/\//g, '\\');
    }
  } else {
    // Unix root case
    if (s === '/') {
      return '/';
    }
  }
  
  // Remove trailing slashes
  s = s.replace(/\/+$/, '');
  
  // Split by forward slash (already normalized)
  const parts = s.split('/');
  
  if (parts.length <= 1) {
    return '.';
  }
  
  // Remove the last part
  parts.pop();
  
  let parent = parts.join('/');
  
  // Handle Windows drive roots - ensure they end with backslash
  if (isWindows && /^[a-zA-Z]:$/.test(parent)) {
    parent += '/';
  }
  
  // Handle empty parent (shouldn't happen with proper paths)
  if (!parent) {
    return isWindows ? 'C:\\' : '/';
  }
  
  // Convert back to appropriate separators
  if (isWindows) {
    parent = parent.replace(/\//g, '\\');
  }
  
  return parent;
}

export function capitalize(str) {
  if (!str) return "";
  return str.charAt(0).toUpperCase() + str.slice(1);
}

// export const COLORS = [
//   "#3B82F6","#EAB308","#F43F5E","#F59E0B","#0EA5E9","#10B981","#22C55E",
//   "#A855F7","#6366F1","#06B6D4","#84CC16","#EF4444","#F97316","#14B8A6",
//   "#8B5CF6","#7C3AED","#4F46E5","#0891B2","#16A34A","#A3E635","#BE123C",
//   "#EA580C","#0D9488","#9333EA","#6D28D9","#4338CA","#155E75","#166534",
//   "#65A30D","#9F1239","#C2410C","#0F766E","#7E22CE","#5B21B6","#3730A3",
//   "#0E7490","#15803D","#4D7C0F","#881337","#9A3412","#115E59","#6B21A8",
//   "#4C1D95","#1E40AF","#1D4ED8","#2563EB","#1F2937","#10A37F","#D97706",
//   "#22D3EE","#059669","#D946EF","#F43F3F","#E11D48","#FB7185","#111827",
// ];

// Dark + Vivid Palette for Black Backgrounds
export const COLORS = [
  // Tier 1: Strong primaries
  '#C53030', // Dark Red
  '#2F855A', // Deep Green
  '#2B6CB0', // Deep Blue
  '#B7791F', // Dark Gold
  '#6B46C1', // Dark Purple
  '#C05621', // Burnt Orange
  '#2C7A7B', // Dark Teal
  '#C2185B', // Vivid Magenta

  // Tier 2: Secondary strongs
  '#22543D', // Forest Green
  '#1A365D', // Navy
  '#97266D', // Hot Pink / Fuchsia
  '#9C4221', // Rust Red
  '#44337A', // Indigo
  '#00838F', // Cyan/Teal
  '#D35400', // Orange Red
  '#0891B2', // Sky Cyan

  // Tier 3: Mid-dark variants (still visible with white text)
  '#276749', // Medium-Dark Green
  '#1E40AF', // Medium-Dark Blue
  '#9B2C2C', // Medium-Dark Crimson
  '#553C9A', // Deep Indigo
  '#115E59', // Teal Shade
  '#9F1239', // Deep Pinkish Red
  '#A16207', // Dark Amber
  '#0F766E', // Deep Aquamarine

  // Tier 4: Extra deep anchors
  '#742A2A', // Very Dark Red
  '#234E52', // Very Dark Teal
  '#312E81', // Very Dark Indigo
  '#5A1E1E', // Brick
  '#513C06', // Dark Mustard
  '#3C096C', // Royal Violet
  '#7C2D12', // Ember
  '#14532D', // Pine Green
];

// Smart color assignment function
export const getOptimalColors = (numUsers) => {
  if (numUsers === 1) {
    return [COLORS[0]]; // Single vibrant color
  }
  
  if (numUsers === 2) {
    return [COLORS[0], COLORS[2]]; // Red and Blue - maximum contrast
  }
  
  if (numUsers === 3) {
    return [COLORS[0], COLORS[1], COLORS[2]]; // Red, Green, Blue
  }
  
  if (numUsers <= 8) {
    // Use tier 1 colors for optimal distinction
    return COLORS.slice(0, numUsers);
  }
  
  if (numUsers <= 16) {
    // Mix tier 1 and tier 2
    return COLORS.slice(0, numUsers);
  }
  
  if (numUsers <= 24) {
    // Use first 24 colors
    return COLORS.slice(0, numUsers);
  }
  
  // For more than 24 users, cycle through with slight variations
  const colors = [];
  for (let i = 0; i < numUsers; i++) {
    const baseIndex = i % COLORS.length;
    const cycle = Math.floor(i / COLORS.length);
    let color = COLORS[baseIndex];
    
    // Apply slight modifications for cycles
    if (cycle > 0) {
      const hsl = hexToHsl(color);
      hsl.l = Math.max(0.2, Math.min(0.8, hsl.l + (cycle * 0.15 * (i % 2 === 0 ? 1 : -1))));
      color = hslToHex(hsl);
    }
    
    colors.push(color);
  }
  
  return colors;
}

// Helper functions for color manipulation
const hexToHsl = (hex) => {
  const r = parseInt(hex.slice(1, 3), 16) / 255;
  const g = parseInt(hex.slice(3, 5), 16) / 255;
  const b = parseInt(hex.slice(5, 7), 16) / 255;
  
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  let h, s, l = (max + min) / 2;
  
  if (max === min) {
    h = s = 0;
  } else {
    const d = max - min;
    s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
    switch (max) {
      case r: h = (g - b) / d + (g < b ? 6 : 0); break;
      case g: h = (b - r) / d + 2; break;
      case b: h = (r - g) / d + 4; break;
    }
    h /= 6;
  }
  
  return { h, s, l };
}

const hslToHex = ({ h, s, l }) => {
  const hue2rgb = (p, q, t) => {
    if (t < 0) t += 1;
    if (t > 1) t -= 1;
    if (t < 1/6) return p + (q - p) * 6 * t;
    if (t < 1/2) return q;
    if (t < 2/3) return p + (q - p) * (2/3 - t) * 6;
    return p;
  };
  
  const q = l < 0.5 ? l * (1 + s) : l + s - l * s;
  const p = 2 * l - q;
  const r = hue2rgb(p, q, h + 1/3);
  const g = hue2rgb(p, q, h);
  const b = hue2rgb(p, q, h - 1/3);
  
  return `#${Math.round(r * 255).toString(16).padStart(2, '0')}${Math.round(g * 255).toString(16).padStart(2, '0')}${Math.round(b * 255).toString(16).padStart(2, '0')}`;
}
