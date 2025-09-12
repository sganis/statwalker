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

export const COLORS = [
  "#3B82F6","#22C55E","#EAB308","#F43F5E","#F59E0B","#0EA5E9","#10B981",
  "#A855F7","#6366F1","#06B6D4","#84CC16","#EF4444","#F97316","#14B8A6",
  "#8B5CF6","#7C3AED","#4F46E5","#0891B2","#16A34A","#A3E635","#BE123C",
  "#EA580C","#0D9488","#9333EA","#6D28D9","#4338CA","#155E75","#166534",
  "#65A30D","#9F1239","#C2410C","#0F766E","#7E22CE","#5B21B6","#3730A3",
  "#0E7490","#15803D","#4D7C0F","#881337","#9A3412","#115E59","#6B21A8",
  "#4C1D95","#1E40AF","#1D4ED8","#2563EB","#1F2937","#10A37F","#D97706",
  "#22D3EE","#059669","#D946EF","#F43F3F","#E11D48","#FB7185","#111827",
];


  // Svelte action: use:copyOnDblclick={{ selector?: string, text?: string }}
export function copyOnDblclick(
  node: HTMLElement,
  opts: { selector?: string; text?: string } = {}
) {
  const dispatch = (type: string, detail?: any) =>
    node.dispatchEvent(new CustomEvent(type, { detail }));

  async function onDblClick(e: MouseEvent) {
    e.preventDefault();

    const target =
      (opts.selector ? node.querySelector<HTMLElement>(opts.selector) : null) ||
      node;

    const text =
      opts.text ??
      (target.innerText ?? target.textContent ?? '').trim();

    // Temporarily allow selection if disabled
    const hadSelectNone = node.classList.contains('select-none');
    if (hadSelectNone) {
      node.classList.remove('select-none');
      node.classList.add('select-text');
    }

    // Visually select contents
    const range = document.createRange();
    range.selectNodeContents(target);
    const sel = window.getSelection();
    sel?.removeAllRanges();
    sel?.addRange(range);

    // Copy (with fallback)
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text);
      } else {
        document.execCommand('copy');
      }
      dispatch('copied', { text });
    } catch (err) {
      dispatch('copyerror', { error: err });
    }

    // Cleanup selection & classes
    setTimeout(() => {
      sel?.removeAllRanges();
      if (hadSelectNone) {
        node.classList.add('select-none');
        node.classList.remove('select-text');
      }
    }, 600);
  }

  node.addEventListener('dblclick', onDblClick);
  return {
    update(newOpts?: { selector?: string; text?: string }) {
      opts = newOpts ?? {};
    },
    destroy() {
      node.removeEventListener('dblclick', onDblClick);
    }
  };
}