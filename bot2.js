// watcher.js
// Watches data/index.js (tokens) and notifies for NEW / UPDATED token entries.
// Run with: BOT_TOKEN=xxx NOTIFY_CHAT=@cloudbackup2025 node watcher.js

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { notifyBatch } = require('./notifier');

const ROOT = path.resolve(__dirname);
const DATA_DIR = path.join(ROOT, 'data');
const INDEX_FILE = path.join(DATA_DIR, 'index.js');
const DEBOUNCE_MS = 600;

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// state: token -> name (value)
const knownTokens = new Map();
let indexTimer = null;

function safeStat(p) {
  try { return fs.statSync(p); } catch (e) { return null; }
}

/**
 * Load tokens from index.js.
 * Tries require() first (clearing cache). If require fails, falls back to text regex.
 * Returns object mapping token -> name (string).
 */
function loadTokensFromIndex(indexPath) {
  // 1) try require
  try {
    delete require.cache[require.resolve(indexPath)];
    const mod = require(indexPath);
    if (mod && typeof mod === 'object') {
      if (mod.tokens && typeof mod.tokens === 'object') {
        // ensure values are strings
        const out = {};
        for (const [k, v] of Object.entries(mod.tokens)) {
          if (typeof v === 'string') out[k] = v.trim();
          else if (v && typeof v.name === 'string') out[k] = v.name.trim();
          else if (v && typeof v.title === 'string') out[k] = v.title.trim();
          else out[k] = String(v).trim();
        }
        return out;
      }
      // handle case: module.exports = { tokens: { ... }, order: [...] } or similar covered above
    }
  } catch (e) {
    // require may fail (dynamic code / syntax) -> fallback
  }

  // 2) fallback: read file text and regex for tokens object
  try {
    const txt = fs.readFileSync(indexPath, 'utf8');

    // find the "tokens": { ... } block
    const tokensBlockMatch = txt.match(/["']tokens["']\s*:\s*\{([\s\S]*?)\}\s*(?:,|\n|\})/m);
    const out = {};
    if (tokensBlockMatch && tokensBlockMatch[1]) {
      const body = tokensBlockMatch[1];

      // match pairs like "TOKEN": "Name"  (allow single/double quotes)
      const pairRe = /["']\s*([A-Za-z0-9_-]+)\s*["']\s*:\s*["']\s*([^"']+?)\s*["']/g;
      let m;
      while ((m = pairRe.exec(body)) !== null) {
        const key = m[1].trim();
        const val = m[2].trim();
        if (key) out[key] = val;
      }
      return out;
    }

    // if nothing found, return empty
    return {};
  } catch (e) {
    return {};
  }
}

/**
 * Scan index.js, compute diffs vs knownTokens, and notify:
 *  - NEW token keys -> notifyBatch(value, 'new')
 *  - existing token key whose value changed -> notifyBatch(newValue, 'updated')
 */
function scanIndexNow() {
  if (!fs.existsSync(INDEX_FILE)) {
    console.warn('[watch] index.js not found:', INDEX_FILE);
    knownTokens.clear();
    return;
  }

  const tokens = loadTokensFromIndex(INDEX_FILE);
  const currentKeys = Object.keys(tokens);

  // detect added tokens
  for (const key of currentKeys) {
    const name = tokens[key] || key;
    if (!knownTokens.has(key)) {
      // NEW upload
      notifyBatch(name, 'new');
    } else {
      const prevName = knownTokens.get(key);
      if (prevName !== name) {
        // name changed -> UPDATED
        notifyBatch(name, 'updated');
      }
    }
  }

  // (optional) If you want to detect deletions, you can compare knownTokens keys not present in currentKeys.
  // we won't announce deletions as per your request.

  // update knownTokens to current snapshot
  knownTokens.clear();
  for (const k of currentKeys) knownTokens.set(k, tokens[k]);
}

// Debounced handler when index.js changes
function handleIndexChange() {
  if (indexTimer) clearTimeout(indexTimer);
  indexTimer = setTimeout(() => {
    indexTimer = null;
    console.log('[watch] index.js changed — rescanning tokens');
    scanIndexNow();
  }, DEBOUNCE_MS);
}

// initial load on startup
function initialLoad() {
  if (!fs.existsSync(INDEX_FILE)) {
    console.warn('[watch] index.js not present at startup.');
    return;
  }
  const tokens = loadTokensFromIndex(INDEX_FILE);
  for (const [k, v] of Object.entries(tokens)) knownTokens.set(k, v);
  console.log('[watch] loaded tokens (count):', knownTokens.size);
}

function startWatch() {
  initialLoad();

  // watch the data directory for index.js changes
  const watcher = fs.watch(DATA_DIR, { persistent: true }, (eventType, filename) => {
    if (!filename) return;
    const full = path.join(DATA_DIR, filename);
    if (path.resolve(full) === path.resolve(INDEX_FILE)) {
      handleIndexChange();
    }
  });

  watcher.on('error', err => console.error('[watch] error', err));
  console.log('[watch] watching', INDEX_FILE);
}

startWatch();
