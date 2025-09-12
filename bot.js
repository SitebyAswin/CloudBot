// bot.js - Updated: full-first-line display_name + sanitized filename; added "View index" button in browse controls
// Requirements:
//   npm install dotenv node-telegram-bot-api
// .env:
//   BOT_TOKEN=123456:ABC-DEF...
//   ADMIN_ID=123456789
//   TELEGRAPH_TOKEN=<optional>

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const querystring = require('querystring');
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
const TELEGRAPH_TOKEN = process.env.TELEGRAPH_TOKEN || null;

if (!BOT_TOKEN) throw new Error('Please set BOT_TOKEN in your .env');
if (!ADMIN_ID) console.warn('ADMIN_ID not set — admin-restricted commands will not be enforced.');

process.on('unhandledRejection', (r) => console.error('[UNHANDLED REJECTION]', r));
process.on('uncaughtException', (e) => console.error('[UNCAUGHT EXCEPTION]', e));

/* ---------- utilities ---------- */
function atomicWriteFileSync(filepath, data) {
  const tmp = filepath + '.tmp';
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filepath);
}
function readIndex() {
  try { delete require.cache[require.resolve(INDEX_FILE)]; return require(INDEX_FILE); } catch { return { tokens: {}, order: [] }; }
}
function writeIndex(obj) { atomicWriteFileSync(INDEX_FILE, 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n'); }
function readMeta() { try { return fs.existsSync(META_FILE) ? JSON.parse(fs.readFileSync(META_FILE)) : {}; } catch { return {}; } }
function writeMeta(obj) { atomicWriteFileSync(META_FILE, JSON.stringify(obj, null, 2)); }

function generateToken(len = 12) {
  const CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  const rnd = crypto.randomBytes(len);
  let out = '';
  for (let i = 0; i < len; i++) out += CHARS[rnd[i] % CHARS.length];
  return out;
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

/* ---------- pending admin state (used for sendfile/edit flows) ---------- */
const pendingBatches = {}; // chatId -> { filename, token, files: [], awaitingFilename?, autoNamed?, editCaptionFlow? }
function startPendingBatch(adminChatId, filename) {
  const token = generateToken();
  const initialFilename = filename && String(filename).trim().length > 0 ? filename.trim() : (`batch_${token}`);
  pendingBatches[adminChatId] = { filename: initialFilename, token, files: [], createdAt: new Date().toISOString(), autoNamed: !filename || String(filename).trim().length===0 };
  createBatchFile(initialFilename, token, adminChatId);
  registerTokenInIndex(token, initialFilename);
  return pendingBatches[adminChatId];
}

/* ---------- bot startup ---------- */
const bot = new TelegramBot(BOT_TOKEN, { polling: true, filepath: true });
let BOT_USERNAME = null;
(async () => { try { const me = await bot.getMe(); BOT_USERNAME = me && me.username ? me.username : null; console.log('Bot username:', BOT_USERNAME); } catch (e) { console.warn('Could not get bot username', e && e.message); } })();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ---------- safe wrappers ---------- */
async function safeSendMessage(chatId, text, opts = {}) { try { return await bot.sendMessage(chatId, String(text || ''), opts); } catch (e) { console.warn('safeSendMessage failed', e && (e.response && e.response.body ? e.response.body : e.message)); return null; } }
async function safeAnswerCallbackQuery(id, opts = {}) { try { return await bot.answerCallbackQuery(id, opts); } catch (e) { console.warn('safeAnswerCallbackQuery failed', e && (e.response && e.response.body ? e.response.body : e.message)); return null; } }

/* ---------- caption formatting / escaping ---------- */
function escapeHtml(s) { if (s === undefined || s === null) return ''; return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

function formatCaptionHtmlForPreview(rawCaption) {
  if (!rawCaption) return '';
  const text = String(rawCaption);
  const linesRaw = text.split(/\r?\n/);
  const lines = linesRaw.map(l => l.replace(/\s+$/,'').replace(/^\s+/,''));
  // Detect storyline start: line that contains 'Story Line' or 'Storyline' (case-insensitive) or starts with 📖
  let storyIndex = -1;
  for (let i = 0; i < lines.length; i++) {
    const l = lines[i];
    if (/story\s*line\s*[:\-]?/i.test(l) || /storyline\s*[:\-]?/i.test(l) || /📖\s*story/i.test(l) || /^📖/i.test(l)) {
      storyIndex = i;
      break;
    }
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

/* ---------- fuzzy search helpers ---------- */
function levenshtein(a,b){ if(!a) return b?b.length:0; if(!b) return a.length; a=a.toLowerCase(); b=b.toLowerCase(); const m=a.length, n=b.length; const dp=Array.from({length:m+1},()=>new Array(n+1).fill(0)); for(let i=0;i<=m;i++) dp[i][0]=i; for(let j=0;j<=n;j++) dp[0][j]=j; for(let i=1;i<=m;i++) for(let j=1;j<=n;j++){ const cost = a[i-1]===b[j-1]?0:1; dp[i][j]=Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+cost); } return dp[m][n]; }
function similarity(a,b){ const maxLen=Math.max((a||'').length,(b||'').length,1); const dist=levenshtein(a||'', b||''); return 1-(dist/maxLen); }
function findMatches(query){
  const idx = readIndex();
  const candidates = [];
  for (const token of Object.keys(idx.tokens||{})) {
    const fname = idx.tokens[token];
    const batch = readBatchFile(fname);
    if (!batch) continue;
    candidates.push({ name: batch.display_name || batch.filename, filename: batch.filename, token, source: 'batch' });
    for (const f of (batch.files||[])) {
      const candidateName = f.file_name || f.caption || f.text || '';
      if (candidateName && candidateName.length>0) candidates.push({ name: candidateName, filename: batch.filename, token, source: 'file' });
    }
  }
  const qs = (query||'').trim();
  const scored = candidates.map(c=>({...c, score: similarity(qs, c.name)})).sort((a,b)=>b.score-a.score);
  const bestByFilename = {};
  for (const s of scored) { if (!bestByFilename[s.filename] || s.score > bestByFilename[s.filename].score) bestByFilename[s.filename] = s; }
  return Object.values(bestByFilename).sort((a,b)=>b.score-a.score);
}

/* ---------- detection & sanitization helpers ---------- */

/*
  sanitizeFilenameCandidate:
  - strips emojis/prefixes e.g. "🎬 Movie:" / "🎬 TV Series:"
  - removes year tokens like "[2025]" / "(2025)" / "- 2025"
  - keeps only file-safe chars and trims to a reasonable length (<=60)
*/
function sanitizeFilenameCandidate(name) {
  if (!name) return null;
  let s = String(name).trim();
  s = s.replace(/^[\u{1F300}-\u{1F9FF}\u2600-\u26FF\p{So}\s]+/u, '');
  s = s.replace(/^(?:🎬\s*)?(?:Movie|TV Series|TV|Series|Show|🎞️)\s*[:\-–—]\s*/i, '');
  s = s.replace(/\s*\[[0-9]{4}\]\s*$/,'');
  s = s.replace(/\s*\([0-9]{4}\)\s*$/,'');
  s = s.replace(/\s*[-–—]\s*[0-9]{4}\s*$/,'');
  s = s.replace(/\s*[,;:.]\s*$/,'');
  s = s.replace(/[\u{1F300}-\u{1F9FF}\u2600-\u26FF\p{So}]/gu, '').trim();
  const bracketNameMatch = s.match(/^(.+?)\s*\[[0-9]{4}\]$/);
  if (bracketNameMatch) s = bracketNameMatch[1].trim();
  s = s.split(/\r?\n/)[0].trim();
  s = s.replace(/[^a-zA-Z0-9 \-_.]/g, '');
  s = s.replace(/\s+/g, ' ').trim();
  if (s.length > 60) s = s.slice(0,60).trim();
  if (!s) return null;
  return s;
}

/*
  renameBatchFileOnDisk(oldFilename, newFilenameSanitized, token, displayNameFull)
  - newFilenameSanitized: safe filename (short) used on disk
  - displayNameFull: full first-line detected (kept as batch.display_name)
*/
function renameBatchFileOnDisk(oldFilename, newFilenameSanitized, token, displayNameFull) {
  try {
    const oldPath = filenameToPath(oldFilename);
    let finalNewFilename = newFilenameSanitized;
    let finalNewPath = filenameToPath(finalNewFilename);
    let suffix = 1;
    while (fs.existsSync(finalNewPath)) {
      finalNewFilename = `${newFilenameSanitized}_${suffix}`;
      finalNewPath = filenameToPath(finalNewFilename);
      suffix++;
    }
    const batch = readBatchFile(oldFilename);
    if (!batch) return null;
    batch.filename = finalNewFilename;
    // set display_name to the full first-line text (limited to e.g. 200 chars)
    const displayFull = displayNameFull ? String(displayNameFull).trim().slice(0,200) : finalNewFilename;
    batch.display_name = displayFull;
    atomicWriteFileSync(finalNewPath, 'module.exports = ' + JSON.stringify(batch, null, 2) + ';\n');
    try { if (fs.existsSync(oldPath)) fs.unlinkSync(oldPath); } catch (e) { console.warn('renameBatchFileOnDisk unlink failed', e && e.message); }
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

/*
 detectNameFromFile(fileMeta) -> { rawLine, sanitized }
 - rawLine: the entire first non-empty line (untruncated except to 200 chars)
 - sanitized: sanitized short filename version (used on disk)
*/
async function detectNameFromFile(fileMeta) {
  try {
    if (fileMeta && fileMeta.caption) {
      const firstLine = String(fileMeta.caption).split(/\r?\n/).map(l=>l.trim()).find(l=>l && l.length>0);
      if (firstLine) {
        const raw = firstLine.trim().slice(0,200);
        const sanitized = sanitizeFilenameCandidate(raw) || null;
        if (sanitized) return { rawLine: raw, sanitized };
        // return raw even if sanitized null (we can still use raw as display_name; fallback sanitized will be token)
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
            const sanitized = sanitizeFilenameCandidate(raw) || null;
            if (sanitized) return { rawLine: raw, sanitized };
            return { rawLine: raw, sanitized: null };
          }
        }
      } catch (e) { console.warn('detectNameFromFile: read failed', e && e.message); }
    }

    if (fileMeta && fileMeta.file_name) {
      const base = String(fileMeta.file_name).replace(/\.[^/.]+$/, '');
      const raw = base.trim().slice(0,200);
      const sanitized = sanitizeFilenameCandidate(raw) || null;
      if (sanitized) return { rawLine: raw, sanitized };
      return { rawLine: raw, sanitized: null };
    }

    return null;
  } catch (e) { console.warn('detectNameFromFile error', e && e.message); return null; }
}

/* ---------- addFileToPending (async) ---------- */
async function addFileToPending(adminChatId, fileMeta) {
  const cur = pendingBatches[adminChatId];
  if (!cur) return null;
  cur.files.push(fileMeta);
  let batch = readBatchFile(cur.filename) || createBatchFile(cur.filename, cur.token, adminChatId);
  batch.files.push(fileMeta);
  writeBatchFile(cur.filename, batch);

  // attempt detection for the first file if autoNamed
  if (cur.autoNamed && cur.files.length === 1) {
    try {
      const detected = await detectNameFromFile(fileMeta);
      if (detected) {
        const raw = detected.rawLine || null;
        const sanitized = detected.sanitized || null;
        const token = cur.token;
        // use sanitized filename if present; otherwise fall back to token based name
        const newFilename = sanitized || (`batch_${token}`);
        const finalName = renameBatchFileOnDisk(cur.filename, newFilename, token, raw || newFilename);
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

/* ---------- attemptSendWithRetry & sending ---------- */
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
          await safeSendMessage(chatId, '⚠️ One item could not be retrieved from the source channel. It may be private or deleted.');
          if (ADMIN_ID) try { await safeSendMessage(ADMIN_ID, `Failed to copy message for token ${batch.token} — source:${f.source_chat_id}, msg:${f.source_message_id}`); } catch(_) {}
          return;
        }
      }
    }
    await safeSendMessage(chatId, 'Unsupported file type or metadata missing.');
  } catch (e) { console.warn('send file fail', e && (e.response && e.response.body ? e.response.body : e.message)); }
}

/* ---------- browse session helpers ---------- */
const browseSessions = {}; // chatId -> { pos, order:[], messageId }

function makeBrowseKeyboardForIndex(pos, total, token) {
  const left = { text: '◀️', callback_data: 'browse_left' };
  const view = { text: '🔲 View files', callback_data: 'browse_view' };
  const right = { text: '▶️', callback_data: 'browse_right' };
  const random = { text: '🎲 Random', callback_data: 'browse_random' };
  const viewList = { text: '📃 View as list', callback_data: 'browse_list' };
  const viewIndex = { text: '🗂️ View index', callback_data: 'view_index' }; // NEW
  // row1: navigation, row2: random + viewList, row3: viewIndex
  return { inline_keyboard: [[left, view, right], [random, viewList], [viewIndex]] };
}
function buildFilesKeyboardForBatch(token, batch) {
  const buttons = [];
  for (let i = 0; i < batch.files.length; i++) {
    const small = batch.files[i].file_name ? (' — ' + batch.files[i].file_name.slice(0,20)) : '';
    buttons.push({ text: `${i+1}${small}`, callback_data: `browse_file_${token}_${i}` });
  }
  const rows = [];
  for (let i = 0; i < buttons.length; i += 3) rows.push(buttons.slice(i, i+3));
  rows.push([{ text: 'Close', callback_data: 'browse_files_close' }]);
  return { inline_keyboard: rows };
}
function buildListViewForBatch(token, batch) {
  const lines = [];
  for (let i = 0; i < batch.files.length; i++) {
    const f = batch.files[i];
    const title = (f.caption || f.text || f.file_name || '').split(/\r?\n/)[0] || `File ${i+1}`;
    const short = escapeHtml(String(title).slice(0, 200));
    lines.push(`${i+1}. ${short}`);
  }
  const text = `<b>${escapeHtml(batch.display_name || batch.filename)}</b>\n\n` + lines.join('\n');
  const kb = buildFilesKeyboardForBatch(token, batch);
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

/* ---------- per-user tracking ---------- */
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

/* ---------- main message handler ---------- */
bot.on('message', async (msg) => {
  try {
    const chatId = msg.chat.id;
    const from = msg.from || {};
    const fromId = from.id;
    const text = msg.text || '';

    // record user actions
    if (fromId) {
      const action = { type: (text && text.startsWith('/')) ? 'command' : 'message', text: text ? (text.slice(0, 1000)) : '', chat_type: msg.chat && msg.chat.type ? msg.chat.type : 'private' };
      recordUserAction({ id: fromId, username: from.username || null, first_name: from.first_name || null, last_name: from.last_name || null }, action);
    }

    /* ---------- Admin: start adding a batch ---------- */
    if (text && text.startsWith('/sendfile')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /sendfile.');
      pendingBatches[chatId] = pendingBatches[chatId] || {};
      pendingBatches[chatId].awaitingFilename = true;
      return safeSendMessage(chatId, 'Send the filename to save this batch as (no extension) — or just send files and the bot will auto-detect a name from the first file. Example: MyMovie 2025\n\nIf you want the bot to auto-detect, just start uploading/forwarding files now and finish with /doneadd');
    }
    if (fromId === ADMIN_ID && pendingBatches[chatId] && pendingBatches[chatId].awaitingFilename && text && !text.startsWith('/')) {
      const filename = text.trim();
      const pending = startPendingBatch(chatId, filename);
      pendingBatches[chatId].awaitingFilename = false;
      return safeSendMessage(chatId, `Batch started as "${filename}" with token: /start_${pending.token}\nNow upload files, send text, or forward messages. When finished, send /doneadd`);
    }

    /* ---------- Admin: finalize batch ---------- */
    if (text === '/doneadd') {
      if (fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may finish a batch.');
      const pending = pendingBatches[chatId];
      if (!pending) return safeSendMessage(chatId, 'No pending batch found. Start with /sendfile and then name the batch.');
      delete pendingBatches[chatId];
      const idx = readIndex();
      const filename = pending.filename;
      const batch = readBatchFile(filename);
      if (!batch) return safeSendMessage(chatId, 'Batch finalized but not found on disk.');
      const kb = { inline_keyboard: [] };
      const row = [];
      if (BOT_USERNAME) {
        const link = `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(batch.token)}`;
        row.push({ text: 'Preview (open batch)', url: link });
      }
      row.push({ text: 'Browse preview', callback_data: 'browse_open_from_done' });
      kb.inline_keyboard.push(row);
      kb.inline_keyboard.push([{ text: 'Contact Admin', url: 'https://t.me/aswinlalus' }]);
      const meta = readMeta();
      if (meta && meta.index_link) kb.inline_keyboard.push([{ text: 'View index', url: meta.index_link }]);
      const previewText = batch.display_name ? `Saved ${batch.filename}\n${batch.display_name}` : `Saved ${batch.filename}`;
      await safeSendMessage(chatId, `${previewText}\nPreview link available.`, { reply_markup: kb });
      return;
    }

    /* ---------- Admin edit caption flow (same as before) ---------- */
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

    /* ---------- listfiles / deletefile / help / browse / index commands - same as before ---------- */
    if (text && text.startsWith('/listfiles')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /listfiles.');
      const idx = readIndex();
      if (!idx.order || idx.order.length === 0) return safeSendMessage(chatId, 'No batches found.');
      let out = 'Batches (send order):\n';
      idx.order.forEach((fname,i)=>{ const token = Object.keys(idx.tokens).find(t=>idx.tokens[t]===fname); const batch = readBatchFile(fname); const name = batch && batch.display_name ? batch.display_name : fname; out += `${i+1}. ${name} — token: /start_${token}\n`; });
      return safeSendMessage(chatId, out);
    }
    if (text && text.startsWith('/deletefile')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may delete.');
      const parts = text.split(' ');
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /deletefile <TOKEN>');
      const token = parts[1].trim().replace(/^\/start_?/, '');
      const idx = readIndex();
      const filename = idx.tokens[token];
      if (!filename) return safeSendMessage(chatId, 'Token not found');
      const filePath = filenameToPath(filename);
      try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); delete idx.tokens[token]; idx.order = idx.order.filter(f=>f!==filename); writeIndex(idx); return safeSendMessage(chatId, `Deleted ${filename} (token ${token})`); } catch(e){ console.error(e); return safeSendMessage(chatId, 'Delete failed: '+(e && e.message)); }
    }

    if (text && (text === '/start' || text.trim() === `/start@${BOT_USERNAME}`)) {
      const kb = { inline_keyboard: [[ { text: 'Browse', callback_data: 'browse_open' }, { text: 'Search inline', switch_inline_query_current_chat: '' } ], [ { text: 'Contact Admin', url: 'https://t.me/aswinlalus' } ]] };
      return safeSendMessage(chatId, `Open batches via Browse or try inline search.`, { reply_markup: kb });
    }

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
        await sleep(150);
      }
      const row1=[], row2=[]; for(let s=1;s<=5;s++) row1.push({ text:`${s}⭐`, callback_data:`rate_${batch.token}_${s}` }); for(let s=6;s<=10;s++) row2.push({ text:`${s}⭐`, callback_data:`rate_${batch.token}_${s}` });
      return safeSendMessage(chatId, 'Rate this batch (1–10):', { reply_markup: { inline_keyboard: [row1, row2] } });
    }

    if (text && text === '/help') {
      const kb = { inline_keyboard: [[ { text:'User', callback_data:'help_user' }, { text:'Admin', callback_data:'help_admin' } ], [ { text:'Contact Admin', url:'https://t.me/aswinlalus' } ]] };
      return safeSendMessage(chatId, 'Choose User or Admin help. Admin button requires admin privileges.', { reply_markup: kb });
    }

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
      try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: baseMsg.message_id }); } catch (e) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch(_){} }
      return;
    }

    if (text && text.startsWith('/set_index_link')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may set index link.');
      const parts = text.split(' ');
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /set_index_link <url>');
      const url = parts[1].trim();
      const meta = readMeta(); meta.index_link = url; writeMeta(meta);
      return safeSendMessage(chatId, `Index link saved: ${url}`);
    }
    if (text && (text === '/index_link' || text === '/get_index_link')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use this.');
      const meta = readMeta();
      if (meta && meta.index_link) return safeSendMessage(chatId, `Stored index link: ${meta.index_link}`, { reply_markup: { inline_keyboard: [[{ text: 'Open index', url: meta.index_link }]] } });
      return safeSendMessage(chatId, 'No index link stored.');
    }

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

    /* ---------- handle uploaded media/text/forwards while admin has a pending batch ---------- */
    const isMedia = !!(msg.document || msg.photo || msg.video || msg.audio || msg.voice || msg.caption || msg.forward_from || msg.forward_from_chat);
    if (fromId === ADMIN_ID && isMedia) {
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

/* ---------- inline query handler ---------- */
bot.on('inline_query', async (q) => {
  try {
    const qid = q.id; const query = (q.query||'').trim();
    const idxObj = readIndex();
    let matches = [];
    if (!query) {
      for (const fname of (idxObj.order||[]).slice(0,24)) {
        const tok = Object.keys(idxObj.tokens||{}).find(t=>idxObj.tokens[t]===fname);
        const batch = readBatchFile(fname); if (!batch) continue;
        matches.push({ name: batch.display_name || batch.filename, filename: batch.filename, token: tok, source: 'batch', score: 1.0 });
      }
    } else matches = findMatches(query).slice(0,24);

    const results = [];
    if (matches.length === 0 && query.length > 1) {
      const id = `report_missing_${Date.now()}_${Math.floor(Math.random()*1000)}`;
      const title = `Report: "${query}" not found`;
      const desc = `Tap to insert a message saying "${query}" and notify admin`;
      const messageText = `No results found for "${query}".`;
      results.push({
        type: 'article', id, title, description: desc,
        input_message_content: { message_text: messageText },
        reply_markup: { inline_keyboard: [[ { text: 'Notify admin', callback_data: 'noop' } ]] }
      });
      await bot.answerInlineQuery(qid, results, { cache_time: 0, is_personal: true });
      return;
    }

    for (let i=0;i<Math.min(matches.length,24);i++){
      const m = matches[i];
      const id = `batch_${m.token}`;
      const title = m.name.length>80? m.name.slice(0,77)+'…':m.name;
      const desc = `${m.source==='batch'?'Batch':'File'} — ${m.filename}`;
      const messageText = `${m.name}\nBatch: ${m.filename}\nToken: /start_${m.token}`;
      const link = BOT_USERNAME ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(m.token)}` : null;
      const kb = link ? { inline_keyboard: [[{ text:'Open in bot', url: link }]] } : undefined;
      results.push({ type:'article', id, title, description:desc, input_message_content:{ message_text: messageText }, reply_markup: kb });
    }
    await bot.answerInlineQuery(qid, results, { cache_time:0, is_personal:true });
  } catch (e) { console.error('inline_query error', e && e.message); try{ await bot.answerInlineQuery(q.id, [], { cache_time:0 }); } catch(_){} }
});

bot.on('chosen_inline_result', async (res) => {
  try { const rid = res.result_id || ''; if (rid.startsWith('report_missing_')) { const queryText = (res.query || '').trim(); if (ADMIN_ID) { await safeSendMessage(ADMIN_ID, `User ${res.from && res.from.username ? '@'+res.from.username : res.from && res.from.id ? res.from.id : 'unknown'} reported missing query: "${queryText}"`); } } } catch (e) { console.warn('chosen_inline_result handler failed', e && e.message); }
});

/* ---------- callback_query handler (browse navigation, view files, view list, rating, help, view_index) ---------- */
bot.on('callback_query', async (q) => {
  try {
    const data = q.data || ''; const chatId = q.message && q.message.chat && q.message.chat.id; const msgId = q.message && q.message.message_id;
    // help
    if (data === 'help_user' || data === 'help_admin' || data === 'help_contact') {
      await safeAnswerCallbackQuery(q.id);
      if (data === 'help_user') {
        const text = `📚 User help — quick guide:\n• Open a batch with token: /start_<TOKEN>\n• Or type @${BOT_USERNAME} in any chat to search inline\n• Use /browse to preview latest uploads\n• After opening a batch you can rate it (1–10).`;
        const kb = { inline_keyboard: [[ { text:'Contact Admin', url:'https://t.me/aswinlalus' } ]] };
        return safeSendMessage(chatId, text, { reply_markup: kb });
      }
      if (data === 'help_admin') {
        if (q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin commands are restricted.' });
        const text = `🛠️ Admin help:\n• /sendfile — start new batch (then reply filename or upload files for auto-detect)\n• /doneadd — finish adding files\n• /edit_caption <TOKEN> — edit file captions\n• /listfiles — list batches\n• /deletefile <TOKEN> — delete batch\n• /set_index_link <url> — store index page link\n• /listusers — view recorded users\n• /getuser <id> — view a user's actions`;
        return safeSendMessage(chatId, text);
      }
      if (data === 'help_contact') return safeSendMessage(chatId, 'Contact admin: https://t.me/aswinlalus');
    }

    // browse controls & file view & random & file selection
    if (['browse_left','browse_right','browse_view','browse_random','browse_list','browse_back_to_preview'].includes(data) || data.startsWith('browse_file_') || data === 'browse_files_close' || data === 'browse_open_from_done' || data === 'browse_open' || data === 'view_index') {
      await safeAnswerCallbackQuery(q.id);

      if (data === 'view_index') {
        const meta = readMeta();
        if (meta && meta.index_link) {
          return safeSendMessage(chatId, 'Index link:', { reply_markup: { inline_keyboard: [[{ text: 'Open index', url: meta.index_link }]] } });
        } else {
          return safeSendMessage(chatId, 'No index link stored. Admin can set it with /set_index_link <url>.');
        }
      }

      if (data === 'browse_open' || data === 'browse_open_from_done') {
        const idx = readIndex();
        const order = idx.order || [];
        if (!order || order.length === 0) return safeSendMessage(chatId, 'No batches available.');
        const pos = order.length - 1;
        const filename = order[pos];
        const batch = readBatchFile(filename);
        if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Latest batch has no files.');
        const firstFile = batch.files[0];
        const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
        let baseMsg;
        try {
          if (firstFile.type === 'photo' && firstFile.file_id) baseMsg = await bot.sendPhoto(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
          else if (firstFile.type === 'document' && firstFile.file_id) baseMsg = await bot.sendDocument(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
          else baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml ? { parse_mode: 'HTML' } : {});
        } catch (e) {
          baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml ? { parse_mode: 'HTML' } : {});
        }
        browseSessions[chatId] = { pos, order, messageId: baseMsg.message_id };
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: baseMsg.message_id }); } catch (e) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch (_) {} }
        return;
      }

      const session = browseSessions[chatId];
      if (!session) return safeSendMessage(chatId, 'No active browse session. Use /browse to start.');

      const order = session.order || [];
      let pos = session.pos || 0;

      if (data === 'browse_left') {
        pos = Math.min(order.length - 1, pos + 1);
        session.pos = pos;
        const fname = order[pos];
        const batch = readBatchFile(fname);
        if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Batch empty.');
        const fileObj = batch.files[0];
        const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_right') {
        pos = Math.max(0, pos - 1);
        session.pos = pos;
        const fname = order[pos];
        const batch = readBatchFile(fname);
        if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Batch empty.');
        const fileObj = batch.files[0];
        const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_random') {
        if (!order || order.length === 0) return safeSendMessage(chatId, 'No batches.');
        const r = Math.floor(Math.random() * order.length);
        session.pos = r;
        const batch = readBatchFile(order[r]);
        if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Random batch empty.');
        const fileObj = batch.files[0];
        const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(r, order.length, batch.token);
        try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_view') {
        const fname = order[session.pos];
        const batch = readBatchFile(fname);
        if (!batch) return safeSendMessage(chatId, 'Batch missing.');
        const token = Object.keys(readIndex().tokens || {}).find(t => readIndex().tokens[t] === fname);
        const filesKb = buildFilesKeyboardForBatch(token, batch);
        try {
          await bot.editMessageReplyMarkup(filesKb.inline_keyboard, { chat_id: chatId, message_id: session.messageId });
        } catch (e) {
          await safeSendMessage(chatId, 'Files:', { reply_markup: filesKb });
        }
        return;
      }

      if (data === 'browse_list') {
        const fname = order[session.pos];
        const batch = readBatchFile(fname);
        if (!batch) return safeSendMessage(chatId, 'Batch missing.');
        const token = Object.keys(readIndex().tokens || {}).find(t => readIndex().tokens[t] === fname);
        const listView = buildListViewForBatch(token, batch);
        try {
          await bot.editMessageText(listView.text, { chat_id: chatId, message_id: session.messageId, parse_mode: 'HTML', reply_markup: listView.keyboard.inline_keyboard });
        } catch (e) {
          await safeSendMessage(chatId, listView.text, { parse_mode: 'HTML', reply_markup: listView.keyboard });
        }
        return;
      }

      if (data === 'browse_back_to_preview') {
        const s = browseSessions[chatId];
        if (!s) return safeSendMessage(chatId, 'No active browse session.');
        const fname = s.order[s.pos];
        const batch = readBatchFile(fname);
        if (!batch) return safeSendMessage(chatId, 'Batch missing.');
        const firstFile = batch.files[0];
        const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
        try {
          const res = await replaceBrowseMessage(chatId, s.messageId, firstFile, captionHtml);
          if (res && res.newMessage) s.messageId = res.newMessage.message_id;
          const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token);
          try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: s.messageId }); } catch (_) {}
        } catch (e) { const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token); try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: s.messageId }); } catch (_) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch (_) {} } }
        return;
      }

      if (data.startsWith('browse_file_')) {
        const parts = data.split('_');
        const token = parts[2];
        const indexStr = parts[3];
        const fileIdx = Number(indexStr);
        if (isNaN(fileIdx)) return safeSendMessage(chatId, 'Invalid file index');
        const idxObj = readIndex();
        const fname = idxObj.tokens[token];
        if (!fname) return safeSendMessage(chatId, 'Batch not found for that token');
        const batch = readBatchFile(fname);
        if (!batch) return safeSendMessage(chatId, 'Batch missing');
        const fileObj = batch.files[fileIdx];
        if (!fileObj) return safeSendMessage(chatId, 'File not found in batch');
        const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(session.pos, session.order.length, token);
        try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_files_close') {
        const s = browseSessions[chatId];
        if (!s) return safeSendMessage(chatId, 'No active browse session.');
        const fname = s.order[s.pos];
        const batch = readBatchFile(fname);
        if (!batch) return safeSendMessage(chatId, 'Batch missing');
        const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token);
        try { await bot.editMessageReplyMarkup(kb.inline_keyboard, { chat_id: chatId, message_id: s.messageId }); } catch (_) {}
        return;
      }
    }

    // ratings
    if (data && data.startsWith('rate_')) {
      const parts = data.split('_');
      const token = parts[1];
      const score = Number(parts[2] || 0);
      const idx = readIndex();
      const filename = idx.tokens[token];
      if (!filename) return safeAnswerCallbackQuery(q.id, { text: 'Batch not found' });
      const batch = readBatchFile(filename);
      if (!batch) return safeAnswerCallbackQuery(q.id, { text: 'Batch missing' });
      batch.ratings = batch.ratings || {};
      batch.ratings[q.from.id] = { score, ts: new Date().toISOString() };
      writeBatchFile(filename, batch);
      return safeAnswerCallbackQuery(q.id, { text: `Thanks — you rated ${score}⭐` });
    }

    return safeAnswerCallbackQuery(q.id, { text: 'Unknown action' });

  } catch (e) {
    console.error('callback_query handler error', e && (e.stack || e.message));
    try { await safeAnswerCallbackQuery(q.id, { text: 'Error handling action' }); } catch (_) {}
  }
});

/* ---------- telegraph helper (optional) ---------- */
function telegraCreatePage(accessToken, title, authorName, nodes) {
  return new Promise((resolve, reject) => {
    const payload = { access_token: accessToken, title, author_name: authorName||'', content: JSON.stringify(nodes), return_content: true };
    const postData = querystring.stringify(payload);
    const opts = { method: 'POST', hostname: 'api.telegra.ph', path: '/createPage', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postData) } };
    const req = https.request(opts, (res)=>{ let raw=''; res.on('data',d=>raw+=d); res.on('end', ()=>{ try{ const parsed = JSON.parse(raw); if (parsed && parsed.ok) return resolve(parsed.result); return reject(new Error('Telegraph error: '+(parsed && parsed.error?parsed.error:raw))); } catch(e){ return reject(e); } }); });
    req.on('error', err=>reject(err)); req.write(postData); req.end();
  });
}

/* ---------- small utilities ---------- */
function extractYearFromBatch(batch) { const yearRegex = /(19|20)\d{2}/; let text = batch.filename || ''; if (!yearRegex.test(text) && batch.files && batch.files.length>0) { const f = batch.files[0]; text += ' '+(f.file_name||f.caption||f.text||''); } const m = text.match(yearRegex); return m?m[0]:'Unknown'; }
function exportBatchCsv(filename) { const batch = readBatchFile(filename); if (!batch) return null; const rows=['index,file_name,type,file_id']; batch.files.forEach((f,i)=>{ rows.push(`${i+1},"${(f.file_name||f.text||'').replace(/"/g,'""')}",${f.type},${f.file_id||''}`); }); return rows.join('\n'); }

console.log('Bot ready. Commands available: /help, /sendfile, /doneadd, /edit_caption <TOKEN> (admin), /listfiles, /deletefile <TOKEN>, /set_index_link, /index_link, /listusers (admin), /getuser <id> (admin), /start_<TOKEN>, /browse');
console.log('Inline placeholder active: users can type "@' + (BOT_USERNAME||'YourBot') + ' query" to search batches inline.');
