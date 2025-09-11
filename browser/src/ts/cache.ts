// curveCache.js
import { openDB } from 'idb';

const DB_NAME = 'ApCacheDB';
const STORE_NAME = 'ap';
const DB_VERSION = 2;
const EXPIRATION_MS = 1 * 60 * 1000; // 1 minute

// Initialize or open the database
async function getDB() {
  return openDB(DB_NAME, DB_VERSION, {
    upgrade(db) {
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
    }
  });
}

// Store curve data with a timestamp
export async function setCache(key, data) {
  const db = await getDB();
  await db.put(STORE_NAME, { data, timestamp: Date.now() }, key);
}

// Get cached curve if not expired
export async function getCache(key) {
  const db = await getDB();
  const entry = await db.get(STORE_NAME, key);

  if (!entry) return null;

  const isExpired = (Date.now() - entry.timestamp) > EXPIRATION_MS;
  if (isExpired) {
    await db.delete(STORE_NAME, key);
    return null;
  }

  return entry.data;
}

// Clear one or all cached entries
export async function clearCache(key) {
  const db = await getDB();
  await db.delete(STORE_NAME, key);
}

export async function clearAll() {
  const db = await getDB();
  await db.clear(STORE_NAME);
}
