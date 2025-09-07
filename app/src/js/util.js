export function formatBytes(bytes, decimals = 1) {
  if (!+bytes) return '0 B';

  const k = 1000;
  const dm = decimals < 0 ? 0 : decimals;
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${units[i]}`;
}

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

