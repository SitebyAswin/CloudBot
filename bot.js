// bot.js — full updated bot with fixes for /start, /browse, inline placeholder, and regex bug fixed.
// Requirements:
//   npm install dotenv node-telegram-bot-api
// .env should contain:
//   BOT_TOKEN=123:ABC...
//   ADMIN_ID=123456789

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const { URL } = require('url');
const TelegramBot = require('node-telegram-bot-api');
const crypto = require('crypto');

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const USER_DIR = path.join(__dirname, 'userdata');
if (!fs.existsSync(USER_DIR)) fs.mkdirSync(USER_DIR, { recursive: true });

const INDEX_FILE = path.join(DATA_DIR, 'index.js');
const META_FILE = path.join(DATA_DIR, 'meta.json');

const BOT_TOKEN = process.env.BOT_TOKEN;

const ADMIN_ID = process.env.ADMIN_ID ? Number(process.env.ADMIN_ID) : NaN;

if (!BOT_TOKEN) throw new Error('Please set BOT_TOKEN in .env');
if (!ADMIN_ID) console.warn('ADMIN_ID not set — admin-only checks will only warn.');

process.on('unhandledRejection', (r) => console.error('[UNHANDLED REJECTION]', r));
process.on('uncaughtException', (e) => console.error('[UNCAUGHT EXCEPTION]', e && e.stack ? e.stack : e));

// ---------- file helpers ----------
function atomicWriteFileSync(filepath, data) {
  const tmp = filepath + '.tmp';
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filepath);
}
function readIndex() {
  try { delete require.cache[require.resolve(INDEX_FILE)]; return require(INDEX_FILE); } catch { return { tokens: {}, order: [] }; }
}
function writeIndex(obj) { atomicWriteFileSync(INDEX_FILE, 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n'); }
function readMeta() {
  try { return fs.existsSync(META_FILE) ? JSON.parse(fs.readFileSync(META_FILE)) : { batch_meta: {}, release_cache: {}, index_page_size: 8 }; } catch { return { batch_meta: {}, release_cache: {}, index_page_size: 8 }; }
}
function writeMeta(obj) { atomicWriteFileSync(META_FILE, JSON.stringify(obj, null, 2)); }

// ---------- token & filenames ----------
function generateToken(len = 12) {
  const CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  const rnd = crypto.randomBytes(len);
  let out = '';
  for (let i = 0; i < len; i++) out += CHARS[rnd[i] % CHARS.length];
  return out;
}
function sanitizeFilenameForDisk(name) {
  if (!name) return null;
  let s = String(name).trim();
  s = s.replace(/^[\u{1F300}-\u{1F9FF}\u2600-\u26FF\p{So}\s]+/u, '');
  s = s.replace(/^(?:🎬\s*)?(?:Movie|TV Series|TV|Series|Show|🎞️)\s*[:\-–—]\s*/i, '');
  s = s.replace(/\s*\[[0-9]{4}\]\s*$/,'');
  s = s.replace(/\s*[-–—]\s*[0-9]{4}\s*$/,'');
  s = s.replace(/[\u{1F300}-\u{1F9FF}\u2600-\u26FF\p{So}]/gu, '').trim();
  s = s.split(/\r?\n/)[0].trim();
  s = s.replace(/[^a-zA-Z0-9 \-_.]/g, '');
  s = s.replace(/\s+/g, ' ').trim();
  if (s.length > 60) s = s.slice(0,60).trim();
  if (!s) return null;
  return s;
}
function filenameToPath(filename) {
  const safe = filename.replace(/[^a-zA-Z0-9-_.]/g, '_');
  return path.join(DATA_DIR, safe + '.js');
}

function createBatchFile(filename, token, adminId) {
  const obj = { token, filename, adminId, createdAt: new Date().toISOString(), files: [], ratings: {}, display_name: filename };
  atomicWriteFileSync(filenameToPath(filename), 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n');
  return obj;
}
function readBatchFile(filename) {
  try { delete require.cache[require.resolve(filenameToPath(filename))]; return require(filenameToPath(filename)); } catch { return null; }
}
function writeBatchFile(filename, obj) { atomicWriteFileSync(filenameToPath(filename), 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n'); }

function registerTokenInIndex(token, filename) {
  const idx = readIndex();
  if (!idx.tokens) idx.tokens = {};
  idx.tokens[token] = filename;
  if (!idx.order) idx.order = [];
  if (!idx.order.includes(filename)) idx.order.push(filename);
  writeIndex(idx);
}

function renameBatchFileOnDisk(oldFilename, newFilenameBase, token, displayNameFull) {
  try {
    const oldPath = filenameToPath(oldFilename);
    let finalNewFilename = newFilenameBase ? `${newFilenameBase}_${token}` : `batch_${token}`;
    let finalNewPath = filenameToPath(finalNewFilename);
    let suffix = 1;
    while (fs.existsSync(finalNewPath)) {
      finalNewFilename = `${newFilenameBase}_${token}_${suffix}`;
      finalNewPath = filenameToPath(finalNewFilename);
      suffix++;
    }
    const batch = readBatchFile(oldFilename);
    if (!batch) return null;
    batch.filename = finalNewFilename;
    batch.display_name = displayNameFull ? String(displayNameFull).trim().slice(0,200) : finalNewFilename;
    atomicWriteFileSync(finalNewPath, 'module.exports = ' + JSON.stringify(batch, null, 2) + ';\n');
    try { if (fs.existsSync(oldPath)) fs.unlinkSync(oldPath); } catch (e) { console.warn('unlink failed', e && e.message); }
    const idx = readIndex();
    if (!idx.tokens) idx.tokens = {};
    if (token) idx.tokens[token] = finalNewFilename;
    if (!idx.order) idx.order = [];
    const pos = idx.order.indexOf(oldFilename);
    if (pos !== -1) idx.order[pos] = finalNewFilename;
    else idx.order.push(finalNewFilename);
    writeIndex(idx);
    return finalNewFilename;
  } catch (e) { console.warn('renameBatchFileOnDisk failed', e && e.message); return null; }
}

// ---------- pending flows (admin) ----------
const pendingBatches = {}; // chatId -> pending add/new batch state
const pendingAddTo = {}; // chatId -> { token, filename, files: [] } when admin uses /addto

function startPendingBatch(adminChatId, filename) {
  const token = generateToken();
  const initialFilename = filename && String(filename).trim().length > 0 ? filename.trim() : (`batch_${token}`);
  pendingBatches[adminChatId] = { filename: initialFilename, token, files: [], createdAt: new Date().toISOString(), autoNamed: !filename || String(filename).trim().length===0 };
  createBatchFile(initialFilename, token, adminChatId);
  registerTokenInIndex(token, initialFilename);
  return pendingBatches[adminChatId];
}
function startPendingAddTo(adminChatId, token) {
  const idx = readIndex();
  const filename = idx.tokens && idx.tokens[token];
  if (!filename) return null;
  pendingAddTo[adminChatId] = { token, filename, files: [] };
  return pendingAddTo[adminChatId];
}

// ---------- bot startup ----------
const bot = new TelegramBot(BOT_TOKEN, { polling: true, filepath: true });
let BOT_USERNAME = null;
(async () => {
  try {
    const me = await bot.getMe();
    BOT_USERNAME = me && me.username ? me.username : null;
    console.log('Bot username:', BOT_USERNAME);
  } catch (e) {
    console.warn('Could not get bot username', e && e.message);
  }
})();
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---------- formatting helpers ----------
function escapeHtml(s) { if (s === undefined || s === null) return ''; return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function formatCaptionHtmlForPreview(rawCaption) {
  if (!rawCaption) return '';
  const text = String(rawCaption);
  const linesRaw = text.split(/\r?\n/);
  const lines = linesRaw.map(l => l.replace(/\s+$/,'').replace(/^\s+/,''));
  let storyIndex = -1;
  for (let i = 0; i < lines.length; i++) {
    const l = lines[i];
    if (/story\s*line\s*[:\-]?/i.test(l) || /storyline\s*[:\-]?/i.test(l) || /📖\s*story/i.test(l) || /^📖/i.test(l)) { storyIndex = i; break; }
  }
  const outParts = [];
  if (storyIndex === -1) {
    for (const ln of lines) { if (ln.trim()==='') outParts.push(''); else outParts.push(`<b>${escapeHtml(ln)}</b>`); }
    return outParts.join('\n');
  }
  for (let i=0;i<storyIndex;i++){ const ln=lines[i]; if (ln.trim()==='') outParts.push(''); else outParts.push(`<b>${escapeHtml(ln)}</b>`); }
  const storyLineRaw = lines[storyIndex] || '';
  const colonIdx = storyLineRaw.indexOf(':');
  let storyLabel = storyLineRaw; let storyRest = '';
  if (colonIdx !== -1 && colonIdx < storyLineRaw.length - 1) { storyLabel = storyLineRaw.slice(0, colonIdx).trim(); storyRest = storyLineRaw.slice(colonIdx+1).trim(); }
  const subsequent = []; for (let j=storyIndex+1;j<lines.length;j++) subsequent.push(lines[j]);
  const storyBodyPieces = []; if (storyRest) storyBodyPieces.push(storyRest); for (const s of subsequent) if (s!==undefined && s!==null) storyBodyPieces.push(s);
  const storyBodyJoined = storyBodyPieces.join('\n').trim();
  if (storyLabel && storyLabel.trim()!=='') outParts.push(`<b>${escapeHtml(storyLabel.replace(/^📖\s*/i,'📖 ').trim())}</b>`); else outParts.push(`<b>Story Line:</b>`);
  outParts.push('');
  outParts.push(`“${escapeHtml(storyBodyJoined)}”`);
  return outParts.join('\n');
}

// ---------- detection ----------
async function detectNameFromFile(fileMeta) {
  try {
    if (fileMeta && fileMeta.caption) {
      const firstLine = String(fileMeta.caption).split(/\r?\n/).map(l=>l.trim()).find(l=>l && l.length>0);
      if (firstLine) {
        const raw = firstLine.trim().slice(0,200);
        const sanitized = sanitizeFilenameForDisk(raw) || null;
        if (sanitized) return { rawLine: raw, sanitized };
        return { rawLine: raw, sanitized: null };
      }
    }
    const fn = (fileMeta.file_name||'').toLowerCase();
    const mime = (fileMeta.mime_type||'').toLowerCase();
    const textLike = mime.startsWith('text/') || /\.(txt|nfo|srt|ass|sub|md|csv|log)$/i.test(fn);
    if (fileMeta.file_id && textLike) {
      try {
        const fileUrl = await bot.getFileLink(fileMeta.file_id);
        const firstChunk = await new Promise((resolve, reject) => {
          let got = '';
          const req = https.get(fileUrl, (res) => {
            res.setTimeout(5000);
            res.on('data', (d) => {
              try { got += d.toString('utf8'); } catch (e) {}
              if (got.length > 8192) { req.destroy(); resolve(got.slice(0,8192)); }
            });
            res.on('end', () => resolve(got));
          });
          req.on('error', (err) => reject(err));
          req.on('timeout', () => { req.destroy(); resolve(got); });
        });
        if (firstChunk && firstChunk.length>0) {
          const lines = firstChunk.split(/\r?\n/).map(l=>l.trim()).filter(l=>l && l.length>0);
          if (lines.length>0) {
            const raw = lines[0].trim().slice(0,200);
            const sanitized = sanitizeFilenameForDisk(raw) || null;
            if (sanitized) return { rawLine: raw, sanitized };
            return { rawLine: raw, sanitized: null };
          }
        }
      } catch (e) { console.warn('detectNameFromFile: read failed', e && e.message); }
    }
    if (fileMeta && fileMeta.file_name) {
      const base = String(fileMeta.file_name).replace(/\.[^/.]+$/, '');
      const raw = base.trim().slice(0,200);
      const sanitized = sanitizeFilenameForDisk(raw) || null;
      if (sanitized) return { rawLine: raw, sanitized };
      return { rawLine: raw, sanitized: null };
    }
    return null;
  } catch (e) { console.warn('detectNameFromFile error', e && e.message); return null; }
}

// ---------- add file to pending batch ----------
async function addFileToPending(adminChatId, fileMeta) {
  const cur = pendingBatches[adminChatId];
  if (!cur) return null;
  cur.files.push(fileMeta);
  let batch = readBatchFile(cur.filename) || createBatchFile(cur.filename, cur.token, adminChatId);
  batch.files.push(fileMeta);
  writeBatchFile(cur.filename, batch);

  if (cur.autoNamed && cur.files.length === 1) {
    try {
      const detected = await detectNameFromFile(fileMeta);
      if (detected) {
        const raw = detected.rawLine || null;
        const sanitized = detected.sanitized || null;
        const token = cur.token;
        const newBase = sanitized || (`batch`);
        const finalName = renameBatchFileOnDisk(cur.filename, newBase, token, raw || newBase);
        if (finalName) {
          cur.filename = finalName;
          batch = readBatchFile(finalName);
        }
      }
    } catch (e) { console.warn('auto detect failed', e && e.message); }
    cur.autoNamed = false;
  }
  return batch;
}

// ---------- add file to existing batch (admin) ----------
async function addFileToExistingBatch(adminChatId, token, fileMeta) {
  const idx = readIndex();
  const filename = idx.tokens && idx.tokens[token];
  if (!filename) return null;
  const batch = readBatchFile(filename);
  if (!batch) return null;
  batch.files.push(fileMeta);
  writeBatchFile(filename, batch);
  return batch;
}

// ---------- send helpers ----------
async function attemptSendWithRetry(fn) {
  try { return await fn(); } catch (err) {
    const transient = err && (err.code==='ECONNRESET' || (err.message && err.message.includes('ECONNRESET')));
    if (transient) { await sleep(500); return await fn(); }
    throw err;
  }
}
async function sendBatchItemToChat(chatId, batch, f) {
  try {
    const captionHtml = f.caption ? formatCaptionHtmlForPreview(f.caption) : null;
    const opts = captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {};
    if (f.type === 'text' && f.text) return await attemptSendWithRetry(() => bot.sendMessage(chatId, captionHtml ? captionHtml : (f.text||''), captionHtml ? { parse_mode: 'HTML' } : {}));
    if (f.type === 'document' && f.file_id) return await attemptSendWithRetry(() => bot.sendDocument(chatId, f.file_id, opts));
    if (f.type === 'photo' && f.file_id) return await attemptSendWithRetry(() => bot.sendPhoto(chatId, f.file_id, opts));
    if (f.type === 'video' && f.file_id) return await attemptSendWithRetry(() => bot.sendVideo(chatId, f.file_id, opts));
    if (f.type === 'audio' && f.file_id) return await attemptSendWithRetry(() => bot.sendAudio(chatId, f.file_id, opts));
    if (f.type === 'forward' && f.source_chat_id && f.source_message_id) {
      try { return await attemptSendWithRetry(() => bot.copyMessage(chatId, f.source_chat_id, f.source_message_id)); }
      catch (copyErr) {
        console.warn('copyMessage failed', copyErr && (copyErr.response && copyErr.response.body ? copyErr.response.body : copyErr.message));
        if (f.file_id) {
          try {
            if (f.mime_type && f.mime_type.startsWith('video')) return await bot.sendVideo(chatId, f.file_id, opts);
            if (f.mime_type && f.mime_type.startsWith('audio')) return await bot.sendAudio(chatId, f.file_id, opts);
            if (f.file_name && /\.(jpg|jpeg|png|gif|webp)$/i.test(f.file_name)) return await bot.sendPhoto(chatId, f.file_id, opts);
            return await bot.sendDocument(chatId, f.file_id, opts);
          } catch (fallbackErr) { console.warn('fallback failed', fallbackErr); await safeSendMessage(chatId, '⚠️ One item could not be delivered (fallback failed).'); return; }
        } else {
          await safeSendMessage(chatId, '⚠️ One item could not be retrieved from source. It may be private/deleted.');
          if (ADMIN_ID) try { await safeSendMessage(ADMIN_ID, `Failed to copy message for token ${batch.token} — source:${f.source_chat_id}, msg:${f.source_message_id}`); } catch(_) {}
          return;
        }
      }
    }
    await safeSendMessage(chatId, 'Unsupported file type or metadata missing.');
  } catch (e) { console.warn('send file fail', e && (e.response && e.response.body ? e.response.body : e.message)); }
}

// ---------- browse helpers ----------
const browseSessions = {};
function makeBrowseKeyboardForIndex(pos, total, token) {
  const left = { text: '◀️Prev', callback_data: 'browse_left' };
  const view = { text: '🔲 Show files', callback_data: 'browse_view' };
  const right = { text: 'Next▶️', callback_data: 'browse_right' };
  const random = { text: '🎲 Random', callback_data: 'browse_random' };
  const viewList = { text: '📃 View as list', callback_data: 'browse_list' };
  const viewIndex = { text: '🗂️ View index', callback_data: 'view_index' };
  return { inline_keyboard: [[left, view, right], [random, viewList], [viewIndex]] };
}
function buildFilesKeyboardForBatch(token, batch, asAdmin = false) {
  const buttons = [];
  for (let i = 0; i < (batch.files||[]).length; i++) {
    const small = batch.files[i].file_name ? (' — ' + batch.files[i].file_name.slice(0,20)) : '';
    buttons.push({ text: `${i+1}${small}`, callback_data: `browse_file_${token}_${i}` });
  }
  const rows = [];
  for (let i = 0; i < buttons.length; i += 3) rows.push(buttons.slice(i, i+3));
  if (asAdmin) {
    for (let i=0;i<(batch.files||[]).length;i++) {
      const up = { text: '🔼', callback_data: `file_up_${token}_${i}` };
      const down = { text: '🔽', callback_data: `file_down_${token}_${i}` };
      rows.push([ { text: `Edit #${i+1}`, callback_data: `file_edit_${token}_${i}` }, up, down ]);
    }
  }
  rows.push([{ text: 'Close', callback_data: 'browse_files_close' }]);
  return { inline_keyboard: rows };
}
function buildListViewForBatch(token, batch, asAdmin = false) {
  const lines = [];
  for (let i = 0; i < (batch.files||[]).length; i++) {
    const f = batch.files[i];
    const title = (f.caption || f.text || f.file_name || '').split(/\r?\n/)[0] || `File ${i+1}`;
    const short = escapeHtml(String(title).slice(0, 200));
    lines.push(`${i+1}. ${short}`);
  }
  const text = `<b>${escapeHtml(batch.display_name || batch.filename)}</b>\n\n` + lines.join('\n');
  const kb = buildFilesKeyboardForBatch(token, batch, asAdmin);
  kb.inline_keyboard.push([{ text: '🔙 Back to preview', callback_data: 'browse_back_to_preview' }]);
  return { text, keyboard: kb };
}
async function replaceBrowseMessage(chatId, oldMessageId, fileObj, captionHtml) {
  try {
    if (fileObj.type === 'photo' && fileObj.file_id) {
      try {
        await bot.editMessageMedia({ type: 'photo', media: fileObj.file_id }, { chat_id: chatId, message_id: oldMessageId });
        if (captionHtml) try { await bot.editMessageCaption(captionHtml, { chat_id: chatId, message_id: oldMessageId, parse_mode: 'HTML' }); } catch (_) {}
        return { edited: true, message_id: oldMessageId };
      } catch (e) {}
    }
    let newMsg;
    const opts = captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {};
    if (fileObj.type === 'document' && fileObj.file_id) newMsg = await bot.sendDocument(chatId, fileObj.file_id, opts);
    else if (fileObj.type === 'photo' && fileObj.file_id) newMsg = await bot.sendPhoto(chatId, fileObj.file_id, opts);
    else if (fileObj.type === 'video' && fileObj.file_id) newMsg = await bot.sendVideo(chatId, fileObj.file_id, opts);
    else if (fileObj.type === 'text' && fileObj.text) newMsg = await bot.sendMessage(chatId, captionHtml ? captionHtml : fileObj.text, captionHtml ? { parse_mode: 'HTML' } : {});
    else if (fileObj.type === 'forward' && fileObj.source_chat_id && fileObj.source_message_id) {
      try { newMsg = await bot.copyMessage(chatId, fileObj.source_chat_id, fileObj.source_message_id); } catch (err) {
        if (fileObj.file_id) newMsg = await bot.sendDocument(chatId, fileObj.file_id, opts);
        else newMsg = await bot.sendMessage(chatId, captionHtml || 'Item unavailable', captionHtml ? { parse_mode: 'HTML' } : {});
      }
    } else newMsg = await bot.sendMessage(chatId, captionHtml || 'Item', captionHtml ? { parse_mode: 'HTML' } : {});
    try { await bot.deleteMessage(chatId, oldMessageId); } catch (_) {}
    return { edited: false, newMessage: newMsg };
  } catch (e) { console.warn('replaceBrowseMessage failed', e && (e.response && e.response.body ? e.response.body : e.message)); return null; }
}

// ---------- per-user tracking ----------
function userFilePath(userId) { return path.join(USER_DIR, `${userId}.js`); }
function readUserData(userId) {
  const p = userFilePath(userId);
  try { delete require.cache[require.resolve(p)]; return require(p); } catch { return { id: userId, username: null, first_name: null, last_name: null, actions: [] }; }
}
function writeUserData(userId, obj) { const p = userFilePath(userId); atomicWriteFileSync(p, 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n'); }
function recordUserAction(user, action) {
  try {
    const uid = user.id;
    const obj = readUserData(uid);
    obj.id = uid;
    if (user.username) obj.username = user.username;
    if (user.first_name) obj.first_name = user.first_name;
    if (user.last_name) obj.last_name = user.last_name;
    obj.actions = obj.actions || [];
    obj.actions.push(Object.assign({ ts: new Date().toISOString() }, action));
    if (obj.actions.length > 200) obj.actions = obj.actions.slice(obj.actions.length - 200);
    writeUserData(uid, obj);
  } catch (e) { console.warn('recordUserAction failed', e && e.message); }
}

// ---------- index builder (quick, uses cached meta) ----------
// global in-memory lookup for callbacks (keeps callback_data short)
const __callbackTokenMap = {}; // key -> { token, display, createdAt }

// generate a short stable key
function makeCbKey() {
  return 'k' + Math.random().toString(36).slice(2, 9);
}

// cleanup old keys every 10 minutes (keys older than 30 min)
setInterval(() => {
  const cutoff = Date.now() - 30 * 60 * 1000;
  for (const k of Object.keys(__callbackTokenMap)) {
    if (!__callbackTokenMap[k] || __callbackTokenMap[k].createdAt < cutoff) delete __callbackTokenMap[k];
  }
}, 10 * 60 * 1000);

// Escaper for MarkdownV2
function escapeMarkdownV2(s = '') {
  return String(s)
    .replace(/\\/g, '\\\\')
    .replace(/_/g, '\\_')
    .replace(/\*/g, '\\*')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)')
    .replace(/~/g, '\\~')
    .replace(/`/g, '\\`')
    .replace(/>/g, '\\>')
    .replace(/#/g, '\\#')
    .replace(/\+/g, '\\+')
    .replace(/-/g, '\\-')
    .replace(/=/g, '\\=')
    .replace(/\|/g, '\\|')
    .replace(/\{/g, '\\{')
    .replace(/\}/g, '\\}')
    .replace(/\./g, '\\.')
    .replace(/!/g, '\\!');
}

// View-only index builder (MarkdownV2) — bold text for everything except token (monospace)
function buildIndexTextAndKeyboardQuick(page = 0, _requesterIsAdmin = false) {
  const idx = readIndex();
  const meta = readMeta();
  const order = Array.isArray(idx.order) ? idx.order.slice() : [];
  const pageSize = (meta && (meta.index_page_size || meta.indexpagesize)) ? Number(meta.index_page_size || meta.indexpagesize) : 8;

  const total = order.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  let p = Number(page) || 0;
  if (p < 0) p = 0;
  if (p >= totalPages) p = totalPages - 1;

  const start = p * pageSize;
  const slice = order.slice(start, start + pageSize);

  // name -> token (reverse lookup)
  const rev = {};
  for (const t of Object.keys(idx.tokens || {})) {
    const val = idx.tokens[t];
    if (val) rev[String(val)] = t;
  }

  const lines = [];
  const itemKeyboardRows = [];

  function shortLabel(s, max = 36) {
    s = String(s || '').trim();
    if (s.length <= max) return s;
    return s.slice(0, max - 1) + '…';
  }

  for (let i = 0; i < slice.length; i++) {
    const n = start + i + 1;
    const fname = slice[i];
    const batch = readBatchFile(fname) || { filename: fname, display_name: fname, files: [] };
    const displayRaw = batch.display_name || fname;

    // sanitize display: remove bracket-like symbols and extra spaces
    const cleanedDisplay = String(displayRaw).replace(/[\(\)\[\]\{\}\/\*]/g, '').replace(/\s+/g, ' ').trim();

    const token =
      rev[displayRaw] ||
      Object.keys(idx.tokens || {}).find(t => idx.tokens[t] === fname) ||
      '';

    // Plain text line (no Markdown, no escaping)
    lines.push(`${n}. ${cleanedDisplay}`);

    if (token) {
      // store a short key so callback_data stays tiny (ensure __callbackTokenMap exists)
      const key = makeCbKey();
      __callbackTokenMap[key] = { token: String(token), display: cleanedDisplay, createdAt: Date.now() };

      const openLabel = shortLabel(cleanedDisplay, 28);
      const encodedToken = encodeURIComponent(String(token));
      const openUrl = (typeof BOT_USERNAME !== 'undefined' && BOT_USERNAME) ? `https://t.me/${BOT_USERNAME}?start=${encodedToken}` : `https://t.me/?start=${encodedToken}`;

      // PREVIEW removed — keep only Open (URL) and Token (callback)
      const copyCb = `copytoken|${key}`;

      const row = [
        { text: `🔗 ${openLabel}`, url: openUrl },
        { text: `🔐 Token`, callback_data: copyCb }
      ];
      itemKeyboardRows.push(row);
    } else {
      itemKeyboardRows.push([{ text: `${n}. ${shortLabel(cleanedDisplay, 40)}`, callback_data: `noop` }]);
    }
  }

  // Pagination rows
  const keyboardRows = [];
  const navRowTop = [];
  if (p > 0) navRowTop.push({ text: '⬅️ Prev', callback_data: `index_prev_${p - 1}` });
  navRowTop.push({ text: `Page ${p + 1}/${totalPages}`, callback_data: `index_page_${p}` });
  if (p < totalPages - 1) navRowTop.push({ text: 'Next ➡️', callback_data: `index_next_${p + 1}` });

  if (navRowTop.length) keyboardRows.push(navRowTop);
  for (const r of itemKeyboardRows) keyboardRows.push(r);
  if (navRowTop.length) keyboardRows.push(navRowTop);

  const headerTitle = 'FILE INDEX : Click on the buttons to view files or to view token';
  const rangeText = `SHOWING ${start + 1} – ${Math.min(start + pageSize, total)} OF ${total}`;

  // Plain text result (no MarkdownV2 escaping)
  const text = [headerTitle, rangeText, ...(lines.length ? lines : ['NO ITEMS FOUND'])].join('\n');

  return {
    text,
    keyboard: { inline_keyboard: keyboardRows },
    page: p,
    totalPages,
    pageSize
  };
}

// ---------- small wrappers ----------
async function safeSendMessage(chatId, text, opts = {}) { try { return await bot.sendMessage(chatId, String(text || ''), opts); } catch (e) { console.warn('safeSendMessage failed', e && (e.response && e.response.body ? e.response.body : e.message)); return null; } }
async function safeAnswerCallbackQuery(id, opts = {}) { try { return await bot.answerCallbackQuery(id, opts); } catch (e) { console.warn('safeAnswerCallbackQuery failed', e && (e.response && e.response.body ? e.response.body : e.message)); return null; } }

// ---------- message handler ----------
bot.on('message', async (msg) => {
  try {
    const chatId = msg.chat.id;
    const from = msg.from || {};
    const fromId = from.id;
    const text = msg.text || '';

    if (fromId) {
      const action = { type: (text && text.startsWith('/')) ? 'command' : 'message', text: text ? (text.slice(0, 1000)) : '', chat_type: msg.chat && msg.chat.type ? msg.chat.type : 'private' };
      recordUserAction({ id: fromId, username: from.username || null, first_name: from.first_name || null, last_name: from.last_name || null }, action);
    }

    // ---------- admin commands (sendfile/addto/doneadd/doneaddto/edit_caption etc) ----------
    if (text && text.startsWith('/sendfile')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /sendfile.');
      pendingBatches[chatId] = pendingBatches[chatId] || {};
      pendingBatches[chatId].awaitingFilename = true;
      return safeSendMessage(chatId, 'Send filename to save this batch as (no extension) — or just send files and the bot will auto-detect a name from the first file. Example: Surrender 2025\nIf you want auto-detect, just start uploading/forwarding files now and finish with /doneadd');
    }
    if (fromId === ADMIN_ID && pendingBatches[chatId] && pendingBatches[chatId].awaitingFilename && text && !text.startsWith('/')) {
      const filename = text.trim();
      const pending = startPendingBatch(chatId, filename);
      pendingBatches[chatId].awaitingFilename = false;
      return safeSendMessage(chatId, `Batch started as "${filename}" with token: /start_${pending.token}\nNow upload files, send text, or forward messages. When finished, send /doneadd`);
    }

    if (text && text.startsWith('/addto')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /addto.');
      const parts = text.split(/\s+/);
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /addto <TOKEN>');
      const token = parts[1].replace(/^\/start_?/, '').trim();
      const started = startPendingAddTo(chatId, token);
      if (!started) return safeSendMessage(chatId, 'Token not found.');
      return safeSendMessage(chatId, `Now forward or upload files/text to be appended to batch (token: ${token}). Finish with /doneaddto`);
    }
    if (text && text === '/doneaddto') {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /doneaddto.');
      const pending = pendingAddTo[chatId];
      if (!pending) return safeSendMessage(chatId, 'No pending add-to session. Start with /addto <TOKEN>');
      delete pendingAddTo[chatId];
      const batch = readBatchFile(pending.filename);
      if (!batch) return safeSendMessage(chatId, 'Batch not found after add.');
      return safeSendMessage(chatId, `Added ${pending.files.length} items to ${batch.display_name || batch.filename}.`);
    }

    if (text === '/doneadd') {
      if (fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may finish a batch.');
      const pending = pendingBatches[chatId];
      if (!pending) return safeSendMessage(chatId, 'No pending batch found. Start with /sendfile and then name the batch or upload files.');
      delete pendingBatches[chatId];
      const filename = pending.filename;
      const batch = readBatchFile(filename);
      if (!batch) return safeSendMessage(chatId, 'Batch finalized but could not find batch file.');
      const kb = { inline_keyboard: [] };
      const row = [];
      if (BOT_USERNAME) {
        const link = `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(batch.token)}`;
        row.push({ text: 'Preview (open batch)', url: link });
      }
      row.push({ text: 'Browse preview', callback_data: 'browse_open_from_done' });
      kb.inline_keyboard.push(row);
      kb.inline_keyboard.push([{ text: 'Contact Admin', url: 'https://t.me/aswinlalus' }]);
      const previewText = batch.display_name ? `Saved ${batch.filename}\n${batch.display_name}` : `Saved ${batch.filename}`;
      await safeSendMessage(chatId, `${previewText}\nPreview link available.`, { reply_markup: kb });
      return;
    }

    // edit caption flow
    if (text && text.startsWith('/edit_caption')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /edit_caption.');
      const parts = text.split(' ');
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /edit_caption <TOKEN>');
      const token = parts[1].replace(/^\/start_?/,'').trim();
      const idx = readIndex();
      const filename = idx.tokens[token];
      if (!filename) return safeSendMessage(chatId, 'Token not found.');
      const batch = readBatchFile(filename);
      if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Batch has no files.');
      pendingBatches[chatId] = pendingBatches[chatId] || {};
      pendingBatches[chatId].editCaptionFlow = { token, filename, stage: 'await_index' };
      let list = `Editing captions for ${batch.filename} (token: ${token}). Files:\n`;
      batch.files.forEach((f,i)=> { const n = f.file_name || (f.caption ? (f.caption.split(/\r?\n/)[0].slice(0,50)) : 'text'); list += `${i+1}. ${n}\n`; });
      list += '\nReply with the file number to edit (1..' + batch.files.length + ')';
      return safeSendMessage(chatId, list);
    }
    if (pendingBatches[chatId] && pendingBatches[chatId].editCaptionFlow && pendingBatches[chatId].editCaptionFlow.stage === 'await_index' && fromId === ADMIN_ID && text && !text.startsWith('/')) {
      const flow = pendingBatches[chatId].editCaptionFlow;
      const idxNum = Number(text.trim());
      const batch = readBatchFile(flow.filename);
      if (isNaN(idxNum) || idxNum < 1 || idxNum > (batch.files.length||0)) {
        return safeSendMessage(chatId, 'Invalid number. Please send a number between 1 and ' + (batch.files.length||0));
      }
      flow.fileIndex = idxNum - 1;
      flow.stage = 'await_caption';
      pendingBatches[chatId].editCaptionFlow = flow;
      return safeSendMessage(chatId, `Send the new caption for file #${idxNum} (you can include Storyline: etc)`);
    }
    if (pendingBatches[chatId] && pendingBatches[chatId].editCaptionFlow && pendingBatches[chatId].editCaptionFlow.stage === 'await_caption' && fromId === ADMIN_ID && text) {
      const flow = pendingBatches[chatId].editCaptionFlow;
      const batch = readBatchFile(flow.filename);
      batch.files[flow.fileIndex].caption = text;
      writeBatchFile(flow.filename, batch);
      delete pendingBatches[chatId].editCaptionFlow;
      const preview = formatCaptionHtmlForPreview(text);
      await safeSendMessage(chatId, 'Caption updated. Preview (first lines):');
      await safeSendMessage(chatId, preview, { parse_mode: 'HTML' });
      return;
    }

    // /listfiles with pagination (e.g., "/listfiles 2")
    const PAGE_SIZE = 10;

    if (text && text.startsWith('/listfiles')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) {
        return safeSendMessage(chatId, 'Only admin may use /listfiles.');
      }

      const idx = readIndex();
      const order = (idx && idx.order) ? idx.order : [];
      if (order.length === 0) {
        return safeSendMessage(chatId, 'No batches found.');
      }

      // Parse optional page arg: "/listfiles 2"
      const parts = text.trim().split(/\s+/);
      const requestedPage = parts[1] ? parseInt(parts[1], 10) : 1;

      const total = order.length;
      const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
      const page = Math.min(totalPages, Math.max(1, isNaN(requestedPage) ? 1 : requestedPage));

      const start = (page - 1) * PAGE_SIZE;
      const end = Math.min(start + PAGE_SIZE, total);

      let out = `Batches (send order) — page ${page}/${totalPages}:\n`;

      order.slice(start, end).forEach((fname, i) => {
        const token = Object.keys(idx.tokens).find(t => idx.tokens[t] === fname);
        const batch = readBatchFile(fname);
        const name = batch && batch.display_name ? batch.display_name : fname;
        const n = start + i + 1; // global ordinal
        out += `${n}. ${name} — token: /start_${token}\n`;
      });

      // Text navigation hints
      if (totalPages > 1) {
        out += `\nNavigate:\n`;
        if (page > 1) out += `← Prev: /listfiles ${page - 1}\n`;
        if (page < totalPages) out += `Next → /listfiles ${page + 1}\n`;
      }

      return safeSendMessage(chatId, out);
    }

    // deletefile
    if (text && text.startsWith('/deletefile')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may delete.');
      const parts = text.split(' ');
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /deletefile <TOKEN>');
      const token = parts[1].trim().replace(/^\/start_?/, '');
      const idx = readIndex();
      const filename = idx.tokens[token];
      if (!filename) return safeSendMessage(chatId, 'Token not found');
      const filePath = filenameToPath(filename);
      try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); delete idx.tokens[token]; idx.order = idx.order.filter(f=>f!==filename); writeIndex(idx); const meta = readMeta(); if (meta.batch_meta) { delete meta.batch_meta[filename]; writeMeta(meta); } return safeSendMessage(chatId, `Deleted ${filename} (token ${token})`); } catch(e){ console.error(e); return safeSendMessage(chatId, 'Delete failed: '+(e && e.message)); }
    }

    // /start (no token) - 2+1+1 inline keyboard with Contact Admin
    if (text && (text === '/start' || text.trim() === `/start@${BOT_USERNAME}`)) {
      const kb = {
        inline_keyboard: [
          [
            { text: '🧭 Browse', callback_data: 'browse_open' },
            { text: '🔎 Search for Movie/Series', switch_inline_query_current_chat: '' }
          ],
          [
            { text: '🗂️ View index', callback_data: 'view_index' }
          ],
          [
            { text: '📨 Contact Admin', url: 'https://t.me/aswinlalus' }
          ]
        ]
      };
      return safeSendMessage(
        chatId,
        `Use Browse to preview latest uploads or use inline search (type @${BOT_USERNAME} in any chat).`,
        { reply_markup: kb }
      );
    }

    // /start with token - show batch files
    if (text && text.startsWith('/start')) {
      const m = text.match(/^\/start(?:@[\w_]+)?(?:[_ ](.+))?$/);
      const payload = (m && m[1]) ? m[1].trim() : '';
      if (!payload) {
        const kb = { inline_keyboard: [[ { text: 'Browse', callback_data: 'browse_open' }, { text: 'Search inline', switch_inline_query_current_chat: '' } ], [ { text: 'Contact Admin', url: 'https://t.me/aswinlalus' } ]] };
        return safeSendMessage(chatId, `Token missing. Use Browse or inline search.`, { reply_markup: kb });
      }
      const token = payload;
      const idx = readIndex();
      const filename = idx.tokens[token];
      if (!filename) {
        const kb = { inline_keyboard: [[ { text: 'Browse', callback_data: 'browse_open' }, { text: 'Search inline', switch_inline_query_current_chat: '' } ], [ { text: 'Contact Admin', url: 'https://t.me/aswinlalus' } ]] };
        return safeSendMessage(chatId, `Token not found. Try Browse or inline search.`, { reply_markup: kb });
      }
      const batch = readBatchFile(filename);
      if (!batch) return safeSendMessage(chatId, 'Batch missing.');
      for (let i=0;i<batch.files.length;i++){
        await sendBatchItemToChat(chatId, batch, batch.files[i]);
        await sleep(120);
      }
      const row1=[], row2=[]; for(let s=1;s<=5;s++) row1.push({ text:`${s}⭐`, callback_data:`rate_${batch.token}_${s}` }); for(let s=6;s<=10;s++) row2.push({ text:`${s}⭐`, callback_data:`rate_${batch.token}_${s}` });
      return safeSendMessage(chatId, 'Rate this batch (1–10):', { reply_markup: { inline_keyboard: [row1, row2] } });
    }

    // help
    if (text && text === '/help') {
      const kb = {
        inline_keyboard: [
          [
            { text: '👤 User', callback_data: 'help_user' },
            { text: '🛠️ Admin', callback_data: 'help_admin' }
          ],
          [
            { text: '🔎 Try inline', switch_inline_query_current_chat: '' }
          ],
          [
            { text: '📨 Contact Admin', url: 'https://t.me/aswinlalus' }
          ]
        ]
      };
      const helpText = 'Choose User or Admin help. Admin button requires admin privileges.';
      return safeSendMessage(chatId, helpText, { reply_markup: kb });
    }

    // /browse -> preview latest
    if (text && text === '/browse') {
      const idx = readIndex();
      const order = idx.order || [];
      if (!order || order.length === 0) return safeSendMessage(chatId, 'No batches available.');
      const lastIndex = order.length - 1;
      const filename = order[lastIndex];
      const batch = readBatchFile(filename);
      if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Latest batch has no files.');
      const firstFile = batch.files[0];
      const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
      let baseMsg;
      try {
        if (firstFile.type === 'photo' && firstFile.file_id) baseMsg = await bot.sendPhoto(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
        else if (firstFile.type === 'document' && firstFile.file_id) baseMsg = await bot.sendDocument(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
        else if (firstFile.type === 'video' && firstFile.file_id) baseMsg = await bot.sendVideo(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
        else if (firstFile.type === 'text' && firstFile.text) baseMsg = await bot.sendMessage(chatId, captionHtml ? captionHtml : formatCaptionHtmlForPreview(firstFile.text), { parse_mode: 'HTML' });
        else if (firstFile.type === 'forward' && firstFile.source_chat_id && firstFile.source_message_id) {
          try { baseMsg = await bot.copyMessage(chatId, firstFile.source_chat_id, firstFile.source_message_id); } catch (e) { baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml?{parse_mode:'HTML'}:{}); }
        } else baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml?{parse_mode:'HTML'}:{});
      } catch (e) { console.warn('browse send failed', e); baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml?{parse_mode:'HTML'}:{}); }
      browseSessions[chatId] = { pos: lastIndex, order: order, messageId: baseMsg.message_id };
      const kb = makeBrowseKeyboardForIndex(lastIndex, order.length, batch.token);
      try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: baseMsg.message_id }); } catch (e) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch(_){} }
      return;
    }

    // listusers/getuser
    if (text && text.startsWith('/listusers')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /listusers.');
      try {
        const files = fs.readdirSync(USER_DIR).filter(f => f.endsWith('.js'));
        if (!files.length) return safeSendMessage(chatId, 'No users recorded yet.');
        let out = 'Known users:\n';
        for (const file of files) {
          try {
            const p = path.join(USER_DIR, file);
            delete require.cache[require.resolve(p)];
            const u = require(p);
            const display = u.username ? '@' + u.username : (u.first_name ? u.first_name : 'unknown');
            out += `${display} — id: ${u.id} — actions: ${u.actions ? u.actions.length : 0}\n`;
          } catch (e) { /* skip bad file */ }
        }
        return safeSendMessage(chatId, out);
      } catch (e) { console.error(e); return safeSendMessage(chatId, 'Failed to list users: ' + (e && e.message)); }
    }
    if (text && text.startsWith('/getuser')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /getuser.');
      const parts = text.split(' ');
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /getuser <userId>');
      const uid = parts[1].trim();
      const p = path.join(USER_DIR, `${uid}.js`);
      try {
        delete require.cache[require.resolve(p)];
        const u = require(p);
        let out = `User ${u.username ? '@'+u.username : ''} id: ${u.id}\nName: ${u.first_name || ''} ${u.last_name || ''}\nActions (${(u.actions||[]).length}):\n`;
        (u.actions||[]).slice(-50).forEach((a,i)=>{ out += `${i+1}. [${a.ts}] ${a.type} ${a.text ? (' — '+ (a.text.length>100? a.text.slice(0,100)+'…':a.text)) : ''}\n`; });
        return safeSendMessage(chatId, out);
      } catch (e) { return safeSendMessage(chatId, 'User not found or read error.'); }
    }

    // Admin uploads/forwards while pending are handled below (in media section)
    const isMedia = !!(msg.document || msg.photo || msg.video || msg.audio || msg.voice || msg.caption || msg.forward_from || msg.forward_from_chat || (msg.text && !msg.text.startsWith('/')));
    if (fromId === ADMIN_ID && isMedia) {
      // priority: if pendingAddTo active -> append to existing batch
      if (pendingAddTo[chatId]) {
        const pending = pendingAddTo[chatId];
        const fileMeta = {};
        if (msg.caption) fileMeta.caption = msg.caption;
        if (msg.document) { fileMeta.type='document'; fileMeta.file_id=msg.document.file_id; fileMeta.file_name=msg.document.file_name; fileMeta.mime_type=msg.document.mime_type; fileMeta.size=msg.document.file_size; }
        else if (msg.photo) { const photo = msg.photo[msg.photo.length-1]; fileMeta.type='photo'; fileMeta.file_id=photo.file_id; fileMeta.mime_type='image/jpeg'; fileMeta.size=photo.file_size; }
        else if (msg.video) { fileMeta.type='video'; fileMeta.file_id=msg.video.file_id; fileMeta.mime_type=msg.video.mime_type; fileMeta.size=msg.video.file_size; }
        else if (msg.audio) { fileMeta.type='audio'; fileMeta.file_id=msg.audio.file_id; fileMeta.mime_type=msg.audio.mime_type; fileMeta.size=msg.audio.file_size; }
        else if (msg.voice) { fileMeta.type='audio'; fileMeta.file_id=msg.voice.file_id; fileMeta.mime_type=msg.voice.mime_type; fileMeta.size=msg.voice.file_size; }
        else if (msg.forward_from || msg.forward_from_chat) {
          fileMeta.type='forward';
          fileMeta.source_chat_id = (msg.forward_from_chat && msg.forward_from_chat.id) || (msg.forward_from && msg.forward_from.id) || null;
          fileMeta.source_message_id = msg.forward_from_message_id || msg.message_id || null;
          if (msg.caption) fileMeta.caption = msg.caption;
          if (msg.document) { fileMeta.file_id = msg.document.file_id; fileMeta.file_name = msg.document.file_name; fileMeta.mime_type = msg.document.mime_type; }
          if (msg.photo) { const photo = msg.photo[msg.photo.length-1]; fileMeta.file_id = photo.file_id; }
        } else if (msg.text && !msg.text.startsWith('/')) { fileMeta.type='text'; fileMeta.text = msg.text; }
        else { fileMeta.type='unknown'; }
        try {
          const appended = await addFileToExistingBatch(chatId, pending.token, fileMeta);
          pending.files.push(fileMeta);
          await safeSendMessage(chatId, `Appended item to batch "${appended.display_name || appended.filename}" (now total ${appended.files.length}).`);
        } catch (e) {
          console.warn('append failed', e && e.message);
          await safeSendMessage(chatId, 'Failed to append item.');
        }
        return;
      }

      // else, if pendingBatches active -> add to new batch
      let pending = pendingBatches[chatId];
      if (!pending) {
        pending = startPendingBatch(chatId, '');
      } else if (pending.awaitingFilename) {
        if (!pending.filename || pending.filename.startsWith('batch_')) {
          const newPending = startPendingBatch(chatId, '');
          pending = pendingBatches[chatId] = newPending;
        } else pending.awaitingFilename = false;
      }

      const fileMeta = {};
      if (msg.caption) fileMeta.caption = msg.caption;
      if (msg.document) { fileMeta.type='document'; fileMeta.file_id=msg.document.file_id; fileMeta.file_name=msg.document.file_name; fileMeta.mime_type=msg.document.mime_type; fileMeta.size=msg.document.file_size; }
      else if (msg.photo) { const photo = msg.photo[msg.photo.length-1]; fileMeta.type='photo'; fileMeta.file_id=photo.file_id; fileMeta.mime_type='image/jpeg'; fileMeta.size=photo.file_size; }
      else if (msg.video) { fileMeta.type='video'; fileMeta.file_id=msg.video.file_id; fileMeta.mime_type=msg.video.mime_type; fileMeta.size=msg.video.file_size; }
      else if (msg.audio) { fileMeta.type='audio'; fileMeta.file_id=msg.audio.file_id; fileMeta.mime_type=msg.audio.mime_type; fileMeta.size=msg.audio.file_size; }
      else if (msg.voice) { fileMeta.type='audio'; fileMeta.file_id=msg.voice.file_id; fileMeta.mime_type=msg.voice.mime_type; fileMeta.size=msg.voice.file_size; }
      else if (msg.forward_from || msg.forward_from_chat) {
        fileMeta.type='forward';
        fileMeta.source_chat_id = (msg.forward_from_chat && msg.forward_from_chat.id) || (msg.forward_from && msg.forward_from.id) || null;
        fileMeta.source_message_id = msg.forward_from_message_id || msg.message_id || null;
        if (msg.caption) fileMeta.caption = msg.caption;
        if (msg.document) { fileMeta.file_id = msg.document.file_id; fileMeta.file_name = msg.document.file_name; fileMeta.mime_type = msg.document.mime_type; }
        if (msg.photo) { const photo = msg.photo[msg.photo.length-1]; fileMeta.file_id = photo.file_id; }
      } else if (msg.text && !msg.text.startsWith('/')) { fileMeta.type='text'; fileMeta.text = msg.text; }
      else { fileMeta.type='unknown'; }

      try {
        const updatedBatch = await addFileToPending(chatId, fileMeta);
        const count = updatedBatch && updatedBatch.files ? updatedBatch.files.length : '?';
        await safeSendMessage(chatId, `Added item to batch "${updatedBatch.display_name || updatedBatch.filename}" (total items: ${count}).`);
      } catch (e) {
        console.warn('Failed to add file to pending', e && e.message);
        await safeSendMessage(chatId, 'Failed to add file to batch.');
      }
      return;
    }

  } catch (err) { console.error('on message error', err && (err.stack || err.message)); }
});

// ---------- inline query handler ----------
bot.on('inline_query', async (q) => {
  try {
    const qid = q.id; const query = (q.query||'').trim();
    const idxObj = readIndex();
    const results = [];
    const tokens = Object.keys(idxObj.tokens || {});
    let candidates = [];
    if (!query) {
      const order = idxObj.order || [];
      const recent = order.slice(-24).reverse();
      for (const fname of recent) {
        const token = Object.keys(idxObj.tokens || {}).find(t=>idxObj.tokens[t]===fname);
        const batch = readBatchFile(fname); if (!batch) continue;
        candidates.push({ token, batch });
      }
    } else {
      const qLower = query.toLowerCase();
      for (const t of tokens) {
        const fname = idxObj.tokens[t];
        const batch = readBatchFile(fname); if (!batch) continue;
        const name = (batch.display_name || batch.filename).toLowerCase();
        if (name.includes(qLower)) candidates.push({ token: t, batch });
        else {
          const firstCap = (batch.files && batch.files[0] && (batch.files[0].caption || batch.files[0].text)) || '';
          if (String(firstCap).toLowerCase().includes(qLower)) candidates.push({ token: t, batch });
        }
      }
    }

    for (let i=0;i<Math.min(25, candidates.length); i++) {
      const c = candidates[i];
      const id = `res_${c.token}_${i}`;
      const display = c.batch.display_name || c.batch.filename;
      const title = display.length > 80 ? display.slice(0,77) + '…' : display;
      let thumb_url = null;
      const firstFile = c.batch.files && c.batch.files[0];
      if (firstFile && firstFile.file_id) {
        try {
          const link = await bot.getFileLink(firstFile.file_id);
          thumb_url = link;
        } catch (e) { thumb_url = null; }
      }
      const messageText = `${display}\nOpen: https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(c.token)}`;
      const kb = { inline_keyboard: [ [ { text: 'Open in bot', url: `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(c.token)}` } ] ] };
      results.push({
        type: 'article',
        id,
        title,
        description: `Open batch`,
        input_message_content: { message_text: messageText },
        reply_markup: kb,
        thumb_url
      });
    }

    await bot.answerInlineQuery(qid, results, { cache_time: 0, is_personal: true });
  } catch (e) { console.error('inline_query error', e && e.message); try{ await bot.answerInlineQuery(q.id, [], { cache_time:0 }); } catch(_){} }
});

// ---------- callback_query handler ----------
bot.on('callback_query', async (q) => {
  try {
    const data = q.data || '';
    const chatId = q.message && q.message.chat && q.message.chat.id;
    const msgId = q.message && q.message.message_id;
    if (!data) return safeAnswerCallbackQuery(q.id); // acknowledge empty presses

    // help user/admin
    if (data === 'help_user' || data === 'help_admin') {
      await safeAnswerCallbackQuery(q.id); // acknowledge tap to clear spinner

      if (data === 'help_user') {
        const text = `👤 User help — use @${BOT_USERNAME} inline or /browse or open a token with /start_<TOKEN>.\nExample: /start_OBQUMJSSK9YB`;
        const replyMarkup = {
          inline_keyboard: [
            [
              // Prefill inline mode in current chat (opens input with @bot)
              { text: '🔎 Search ', switch_inline_query_current_chat: '' },
              // Route to a browse flow in your bot
              { text: '🧭 Browse', callback_data: 'browse' }
            ],
            [
              { text: 'ℹ️ More help', callback_data: 'help_user_more' }
            ]
          ]
        };
        await safeSendMessage(chatId, text, { reply_markup: replyMarkup });
        return;
      }

      if (data === 'help_admin') {
        if (q.from && q.from.id !== ADMIN_ID) {
          return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
        }

        const helpText =
`🛠️ Admin help — available commands:
• /sendfile — start a new batch (reply filename or just upload files to auto-detect). Finish with /doneadd
• /addto <TOKEN> — append files to an existing batch. Finish with /doneaddto
• /doneadd — finalize new batch
• /doneaddto — finalize appending
• /edit_caption <TOKEN> — edit a specific file caption
• /listfiles — list batches and tokens
• /deletefile <TOKEN> — delete a batch
• /listusers and /getuser <id> — user tracking`;

        const replyMarkup = {
          inline_keyboard: [
            [
              { text: '📤 Send files', callback_data: 'admin_sendfile' },
              { text: '➕ Add to batch', callback_data: 'admin_addto' }
            ],
            [
              { text: '✅ Finalize', callback_data: 'admin_finalize' },
              { text: '🗂 List files', callback_data: 'admin_listfiles' }
            ],
            [
              { text: '📝 Edit caption', callback_data: 'admin_edit_caption' },
              { text: '🗑 Delete batch', callback_data: 'admin_deletefile' }
            ],
            [
              { text: '👥 Users', callback_data: 'admin_users' }
            ]
          ]
        };
        await safeSendMessage(chatId, helpText, { reply_markup: replyMarkup });
        return;
      }
    }

    // Handle new help actions (examples)
    if (data === 'browse') {
      await safeAnswerCallbackQuery(q.id, { text: 'Browse' });
      await safeSendMessage(chatId, 'Use /browse to explore files and tokens.');
      return;
    }

    if (data === 'help_user_more') {
      await safeAnswerCallbackQuery(q.id, { text: 'More help' });
      await safeSendMessage(chatId, 'Tip: use inline mode with “ @${BOT_USERNAME} query” to search from any chat.');
      return;
    }

    // Admin action examples (wire up to your flows)
    if (data === 'admin_sendfile') {
      await safeAnswerCallbackQuery(q.id, { text: 'Send files' });
      await safeSendMessage(chatId, 'Reply with a filename or upload files to begin /sendfile.');
      return;
    }

    if (data === 'admin_addto') {
      await safeAnswerCallbackQuery(q.id, { text: 'Add to batch' });
      await safeSendMessage(chatId, 'Use /addto <TOKEN> then upload files to append.');
      return;
    }

    if (data === 'admin_finalize') {
      await safeAnswerCallbackQuery(q.id, { text: 'Finalize' });
      await safeSendMessage(chatId, 'Use /doneadd or /doneaddto to finalize.');
      return;
    }

    if (data === 'admin_listfiles') {
      await safeAnswerCallbackQuery(q.id, { text: 'List files' });
      await safeSendMessage(chatId, 'Run /listfiles to list batches and tokens.');
      return;
    }

    if (data === 'admin_edit_caption') {
      await safeAnswerCallbackQuery(q.id, { text: 'Edit caption' });
      await safeSendMessage(chatId, 'Use /edit_caption <TOKEN> to edit a caption.');
      return;
    }

    if (data === 'admin_deletefile') {
      await safeAnswerCallbackQuery(q.id, { text: 'Delete batch' });
      await safeSendMessage(chatId, 'Use /deletefile <TOKEN> to delete a batch.');
      return;
    }

    if (data === 'admin_users') {
      await safeAnswerCallbackQuery(q.id, { text: 'Users' });
      await safeSendMessage(chatId, 'Use /listusers or /getuser <id> for user tracking.');
      return;
    }

    // index & browse callbacks
    if (data === 'view_index' || data.startsWith('index_') || data.startsWith('browse_') || data.startsWith('file_') || data.startsWith('browse_file_')) {
      await safeAnswerCallbackQuery(q.id);

      // view index
      if (data === 'view_index') {
        const idxPayload = buildIndexTextAndKeyboardQuick(0, (q.from && q.from.id === ADMIN_ID));
        try { await bot.sendMessage(chatId, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        catch (e) { await safeSendMessage(chatId, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        return;
      }

      // index pagination handlers
      if (data.startsWith('index_prev_') || data.startsWith('index_next_') || data.startsWith('index_page_') || data.startsWith('index_refresh_')) {
        const parts = data.split('_');
        const page = Number(parts[2]) || 0;
        const idxPayload = buildIndexTextAndKeyboardQuick(page, (q.from && q.from.id === ADMIN_ID));
        try { await bot.editMessageText(idxPayload.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        catch (e) { await safeSendMessage(q.message.chat.id, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        return;
      }

      // index actions with token
      if (data.startsWith('index_view_') || data.startsWith('index_up_') || data.startsWith('index_down_')) {
        const parts = data.split('_');
        const action = parts[1];
        const token = parts[2];
        const page = Number(parts[3]) || 0;
        const idx = readIndex();
        const filename = idx.tokens && idx.tokens[token];
        if (!filename) return safeSendMessage(q.message.chat.id, 'Batch not found.');
        if (action === 'view') {
          const batch = readBatchFile(filename);
          if (!batch) return safeSendMessage(q.message.chat.id, 'Batch missing.');
          const asAdmin = (q.from && q.from.id === ADMIN_ID);
          const listView = buildListViewForBatch(token, batch, asAdmin);
          try { await bot.editMessageText(listView.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode: 'HTML', reply_markup: listView.keyboard.inline_keyboard }); }
          catch (e) { await safeSendMessage(q.message.chat.id, listView.text, { parse_mode: 'HTML', reply_markup: listView.keyboard }); }
          return;
        }
        // reorder batch in index — admin only
        if ((action === 'up' || action === 'down') && q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
        const order = idx.order || [];
        const pos = order.indexOf(filename);
        if (pos === -1) return safeSendMessage(q.message.chat.id, 'Batch not in index order.');
        if (action === 'up') {
          if (pos <= 0) return safeAnswerCallbackQuery(q.id, { text: 'Already top' });
          [order[pos-1], order[pos]] = [order[pos], order[pos-1]];
          idx.order = order; writeIndex(idx);
          const idxPayload = buildIndexTextAndKeyboardQuick(page, true);
          try { await bot.editMessageText(idxPayload.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
          catch (e) { await safeSendMessage(q.message.chat.id, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
          return;
        }
        if (action === 'down') {
          if (pos >= order.length - 1) return safeAnswerCallbackQuery(q.id, { text: 'Already bottom' });
          [order[pos+1], order[pos]] = [order[pos], order[pos+1]];
          idx.order = order; writeIndex(idx);
          const idxPayload = buildIndexTextAndKeyboardQuick(page, true);
          try { await bot.editMessageText(idxPayload.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
          catch (e) { await safeSendMessage(q.message.chat.id, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
          return;
        }
      }

      // browse open (same as /browse)
      if (data === 'browse_open' || data === 'browse_open_from_done') {
        const idx = readIndex();
        const order = idx.order || [];
        if (!order || order.length === 0) return safeSendMessage(chatId, 'No batches available.');
        const pos = order.length - 1; const filename = order[pos];
        const batch = readBatchFile(filename);
        if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Latest batch has no files.');
        const firstFile = batch.files[0];
        const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
        let baseMsg;
        try {
          if (firstFile.type === 'photo' && firstFile.file_id) baseMsg = await bot.sendPhoto(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
          else if (firstFile.type === 'document' && firstFile.file_id) baseMsg = await bot.sendDocument(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
          else baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml ? { parse_mode: 'HTML' } : {});
        } catch (e) { baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml ? { parse_mode: 'HTML' } : {}); }
        browseSessions[chatId] = { pos, order, messageId: baseMsg.message_id };
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: baseMsg.message_id }); } catch (e) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch (_) {} }
        return;
      }

      // session checks
      const session = browseSessions[chatId];
      if (!session) return safeSendMessage(chatId, 'No active browse session. Use /browse.');

      const order = session.order || []; let pos = session.pos || 0;

      if (data === 'browse_left') {
        // left shows previous upload -> older -> increase pos
        pos = Math.min(order.length - 1, pos + 1); session.pos = pos;
        const fname = order[pos]; const batch = readBatchFile(fname); if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Batch empty.');
        const fileObj = batch.files[0]; const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_right') {
        pos = Math.max(0, pos - 1); session.pos = pos;
        const fname = order[pos]; const batch = readBatchFile(fname); if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Batch empty.');
        const fileObj = batch.files[0]; const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_random') {
        if (!order || order.length === 0) return safeSendMessage(chatId, 'No batches.');
        const r = Math.floor(Math.random() * order.length); session.pos = r;
        const batch = readBatchFile(order[r]); if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Random batch empty.');
        const fileObj = batch.files[0]; const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(r, order.length, batch.token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_view') {
        const fname = order[session.pos];
        const batch = readBatchFile(fname);
        if (!batch) return safeSendMessage(chatId, 'Batch missing.');

        // try to find token from index (fallback to batch.token if present)
        const token = Object.keys(readIndex().tokens || {}).find(t => readIndex().tokens[t] === fname) || batch.token || '';

        const asAdmin = (q.from && q.from.id === ADMIN_ID);

        // build & update files keyboard (keeps existing behaviour)
        const filesKb = buildFilesKeyboardForBatch(token, batch, asAdmin);
        try {
          await bot.editMessageReplyMarkup({ inline_keyboard: filesKb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId });
        } catch (e) {
          await safeSendMessage(chatId, 'Files:', { reply_markup: filesKb });
        }

        // --- NEW: send all files in the batch to the user (same as /start with token) ---
        try {
          if (!Array.isArray(batch.files) || batch.files.length === 0) {
            return safeSendMessage(chatId, 'No files in this batch.');
          }

          for (let i = 0; i < batch.files.length; i++) {
            const item = batch.files[i];
            await sendBatchItemToChat(chatId, batch, item);
            // small pause between sends to avoid flood limits
            await sleep(120);
          }

          // after sending all items, prompt for rating (1–10) same as /start handler
          const row1 = [], row2 = [];
          for (let s = 1; s <= 5; s++) row1.push({ text: `${s}⭐`, callback_data: `rate_${token}_${s}` });
          for (let s = 6; s <= 10; s++) row2.push({ text: `${s}⭐`, callback_data: `rate_${token}_${s}` });

          return safeSendMessage(chatId, 'Rate this batch (1–10):', { reply_markup: { inline_keyboard: [row1, row2] } });

        } catch (err) {
          console.error('Error sending batch files:', err);
          return safeSendMessage(chatId, 'Failed to send batch files. Try again later.');
        }
      }

      if (data === 'browse_list') {
        const fname = order[session.pos]; const batch = readBatchFile(fname); if (!batch) return safeSendMessage(chatId, 'Batch missing.');
        const token = Object.keys(readIndex().tokens || {}).find(t => readIndex().tokens[t] === fname);
        const asAdmin = (q.from && q.from.id === ADMIN_ID);
        const listView = buildListViewForBatch(token, batch, asAdmin);
        try { await bot.editMessageText(listView.text, { chat_id: chatId, message_id: session.messageId, parse_mode: 'HTML', reply_markup: listView.keyboard.inline_keyboard }); } catch (e) { await safeSendMessage(chatId, listView.text, { parse_mode: 'HTML', reply_markup: listView.keyboard }); }
        return;
      }

      if (data === 'browse_back_to_preview') {
        const s = browseSessions[chatId]; if (!s) return safeSendMessage(chatId, 'No active browse session.');
        const fname = s.order[s.pos]; const batch = readBatchFile(fname); if (!batch) return safeSendMessage(chatId, 'Batch missing.');
        const firstFile = batch.files[0]; const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
        try { const res = await replaceBrowseMessage(chatId, s.messageId, firstFile, captionHtml); if (res && res.newMessage) s.messageId = res.newMessage.message_id; const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: s.messageId }); } catch (_) {} } catch (e) { const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: s.messageId }); } catch (_) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch (_) {} } }
        return;
      }

      // show a specific file from files list
      if (data.startsWith('browse_file_')) {
        const parts = data.split('_'); const token = parts[2]; const indexStr = parts[3]; const fileIdx = Number(indexStr);
        if (isNaN(fileIdx)) return safeSendMessage(chatId, 'Invalid file index');
        const idxObj = readIndex(); const fname = idxObj.tokens[token]; if (!fname) return safeSendMessage(chatId, 'Batch not found for that token');
        const batch = readBatchFile(fname); if (!batch) return safeSendMessage(chatId, 'Batch missing');
        const fileObj = batch.files[fileIdx]; if (!fileObj) return safeSendMessage(chatId, 'File not found in batch');
        const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(session.pos, session.order.length, token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      // close files view
      if (data === 'browse_files_close') {
        const s = browseSessions[chatId]; if (!s) return safeSendMessage(chatId, 'No active browse session.');
        const fname = s.order[s.pos]; const batch = readBatchFile(fname); if (!batch) return safeSendMessage(chatId, 'Batch missing');
        const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: s.messageId }); } catch (_) {}
        return;
      }

      // file-level admin actions: file_up_, file_down_, file_edit_
      if (data.startsWith('file_up_') || data.startsWith('file_down_') || data.startsWith('file_edit_')) {
        const parts = data.split('_');
        const action = parts[1];
        const token = parts[2];
        const idxNum = Number(parts[3]);
        if ((action === 'up' || action === 'down') && q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
        const idxObj = readIndex();
        const filename = idxObj.tokens[token];
        if (!filename) return safeSendMessage(chatId, 'Batch not found');
        const batch = readBatchFile(filename);
        if (!batch) return safeSendMessage(chatId, 'Batch missing');
        if (isNaN(idxNum) || idxNum < 0 || idxNum >= (batch.files||[]).length) return safeSendMessage(chatId, 'Invalid file index');
        if (action === 'up') {
          if (idxNum <= 0) return safeAnswerCallbackQuery(q.id, { text: 'Already at top' });
          [batch.files[idxNum-1], batch.files[idxNum]] = [batch.files[idxNum], batch.files[idxNum-1]];
          writeBatchFile(filename, batch);
          await safeAnswerCallbackQuery(q.id, { text: 'Moved up' });
        } else if (action === 'down') {
          if (idxNum >= batch.files.length - 1) return safeAnswerCallbackQuery(q.id, { text: 'Already at bottom' });
          [batch.files[idxNum+1], batch.files[idxNum]] = [batch.files[idxNum], batch.files[idxNum+1]];
          writeBatchFile(filename, batch);
          await safeAnswerCallbackQuery(q.id, { text: 'Moved down' });
        } else if (action === 'edit') {
          if (q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
          pendingBatches[chatId] = pendingBatches[chatId] || {};
          pendingBatches[chatId].editCaptionFlow = { token, filename: filename, stage: 'await_caption', fileIndex: idxNum };
          return safeSendMessage(chatId, `Send a new caption for file #${idxNum+1} in that batch.`);
        }
        // re-render the list view if message belongs to the bot
        try {
          const asAdmin = true;
          const listView = buildListViewForBatch(token, batch, asAdmin);
          await bot.editMessageText(listView.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode: 'HTML', reply_markup: listView.keyboard.inline_keyboard });
        } catch (e) { /* ignore */ }
        return;
      }

      return;
    }

    // rating
    if (data && data.startsWith('rate_')) {
      const parts = data.split('_'); const token = parts[1]; const score = Number(parts[2] || 0);
      const idx = readIndex(); const filename = idx.tokens[token]; if (!filename) return safeAnswerCallbackQuery(q.id, { text: 'Batch not found' });
      const batch = readBatchFile(filename); if (!batch) return safeAnswerCallbackQuery(q.id, { text: 'Batch missing' });
      batch.ratings = batch.ratings || {}; batch.ratings[q.from.id] = { score, ts: new Date().toISOString() }; writeBatchFile(filename, batch);
      return safeAnswerCallbackQuery(q.id, { text: `Thanks — you rated ${score}⭐` });
    }

    return safeAnswerCallbackQuery(q.id, { text: 'Unknown action' });

  } catch (e) {
    console.error('callback_query handler error', e && (e.stack || e.message));
    try { await safeAnswerCallbackQuery(q.id, { text: 'Error handling action' }); } catch (_) {}
  }
});

bot.on('callback_query', async (callbackQuery) => {
  try {
    const data = callbackQuery.data || '';
    const parts = data.split('|');
    const cmd = parts[0];
    const key = parts[1];

    if (cmd === 'copytoken') {
      const rec = key ? __callbackTokenMap[key] : null;
      if (!rec || !rec.token) {
        return bot.answerCallbackQuery(callbackQuery.id, { text: 'Token expired or unavailable', show_alert: true });
      }
      await bot.answerCallbackQuery(callbackQuery.id); // remove spinner
      const safeText = `Token for "${rec.display || ''}":\n\`${rec.token}\``;
      try {
        await bot.sendMessage(callbackQuery.from.id, safeText, { parse_mode: 'MarkdownV2' });
      } catch (e) {
        // fallback if DM fails
        await bot.answerCallbackQuery(callbackQuery.id, { text: `Token: ${rec.token}`, show_alert: true });
      }
      return;
    }

    if (cmd === 'indexview') {
      const rec = key ? __callbackTokenMap[key] : null;
      const token = rec ? rec.token : null;
      const page = parts[2] ? Number(parts[2]) : 0;

      // stop spinner
      await bot.answerCallbackQuery(callbackQuery.id);

      if (!token) {
        return bot.sendMessage(callbackQuery.message.chat.id, 'Preview token expired or invalid. Try the index again.');
      }

      // 1) Prefer your existing preview function(s) if present
      if (typeof handleIndexView === 'function') {
        return handleIndexView(callbackQuery.message.chat.id, token, page, callbackQuery);
      }
      if (typeof showBatchPreview === 'function') {
        return showBatchPreview(callbackQuery.message.chat.id, token, page, callbackQuery);
      }
      if (typeof handleBrowse === 'function') {
        return handleBrowse(callbackQuery.message.chat.id, token, { fromCallback: callbackQuery });
      }

      // 2) Fallback: try to read the batch and send a first-file caption like /browse would
      try {
        const idx = readIndex();
        const filename = (idx && idx.tokens && idx.tokens[token]) ? idx.tokens[token] : null;
        let batch = null;
        if (filename) batch = readBatchFile(filename);
        // fallback: maybe token maps to display name; attempt to find matching filename
        if (!batch && idx && idx.order) {
          const candidate = idx.order.find(fn => {
            const b = readBatchFile(fn);
            const name = (b && b.display_name) ? String(b.display_name) : fn;
            return name === token || name === rec?.display;
          });
          if (candidate) batch = readBatchFile(candidate);
        }

        if (!batch) {
          // we can't construct a full preview — provide the deep link + caption fallback
          const openUrl = (typeof BOT_USERNAME !== 'undefined' && BOT_USERNAME) ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(token)}` : `https://t.me/?start=${encodeURIComponent(token)}`;
          return bot.sendMessage(callbackQuery.message.chat.id, `Preview not available here. Open using the link below:\n${openUrl}`);
        }

        // try to locate first file and its caption
        const first = (batch.files && batch.files[0]) || (batch.items && batch.items[0]) || null;
        const caption = first && (first.caption || first.title || first.text) ? String(first.caption || first.title || first.text) : (batch.caption || `Preview: ${batch.display_name || batch.filename || 'item'}`);

        // best-effort preview: if it's just a caption/text, send it
        if (!first || (!first.file_id && !first.fileId && !first.id)) {
          // no file id — send the caption + deep link
          const openUrl = (typeof BOT_USERNAME !== 'undefined' && BOT_USERNAME) ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(token)}` : `https://t.me/?start=${encodeURIComponent(token)}`;
          return bot.sendMessage(callbackQuery.message.chat.id, `${caption}\n\nOpen: ${openUrl}`);
        }

        // If we have a file id, attempt to send the file. We try common senders in order:
        const fileId = first.file_id || first.fileId || first.id;
        // try sendPhoto, sendVideo, sendDocument — choose by declared type if available
        if (first.mime_type && first.mime_type.startsWith('image')) {
          return bot.sendPhoto(callbackQuery.message.chat.id, fileId, { caption });
        }
        if (first.mime_type && first.mime_type.startsWith('video')) {
          return bot.sendVideo(callbackQuery.message.chat.id, fileId, { caption });
        }
        // default to document send
        return bot.sendDocument(callbackQuery.message.chat.id, fileId, {}, { caption });
      } catch (e) {
        console.error('indexview fallback error', e);
        return bot.sendMessage(callbackQuery.message.chat.id, 'Unable to show preview. Try opening the item using the Open link.');
      }
    }

    // keep other callback handlers (prev/next/page etc) in place
  } catch (err) {
    console.error('callback_query error', err);
    try { await bot.answerCallbackQuery(callbackQuery.id, { text: 'Error', show_alert: false }); } catch (e) {}
  }
});

// ---------- fuzzy helpers ----------
function levenshtein(a,b){ if(!a) return b?b.length:0; if(!b) return a.length; a=a.toLowerCase(); b=b.toLowerCase(); const m=a.length, n=b.length; const dp=Array.from({length:m+1},()=>new Array(n+1).fill(0)); for(let i=0;i<=m;i++) dp[i][0]=i; for(let j=0;j<=n;j++) dp[0][j]=j; for(let i=1;i<=m;i++) for(let j=1;j<=n;j++){ const cost = a[i-1]===b[j-1]?0:1; dp[i][j]=Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+cost); } return dp[m][n]; }
function similarity(a,b){ const maxLen=Math.max((a||'').length,(b||'').length,1); const dist=levenshtein(a||'', b||''); return 1-(dist/maxLen); }

// ---------- misc ----------
function exportBatchCsv(filename) { const batch = readBatchFile(filename); if (!batch) return null; const rows=['index,file_name,type,file_id']; batch.files.forEach((f,i)=>{ rows.push(`${i+1},"${(f.file_name||f.text||'').replace(/"/g,'""')}",${f.type},${f.file_id||''}`); }); return rows.join('\n'); }

console.log('Bot ready. Commands: /help, /sendfile, /doneadd, /addto <TOKEN>, /doneaddto, /edit_caption <TOKEN>, /listfiles, /deletefile <TOKEN>, /listusers, /getuser <id>, /start_<TOKEN>, /browse, /view_index');

