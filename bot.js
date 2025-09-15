// bot.js — updated: metadata feature removed, user index paginated
// Requirements:
//   npm install dotenv node-telegram-bot-api
// .env should contain:
//   BOT_TOKEN=123:ABC...
//   ADMIN_ID=123456789

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const TelegramBot = require('node-telegram-bot-api');
const crypto = require('crypto');

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const USER_DIR = path.join(__dirname, 'userdata');
if (!fs.existsSync(USER_DIR)) fs.mkdirSync(USER_DIR, { recursive: true });

const INDEX_FILE = path.join(DATA_DIR, 'index.js');

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
  const obj = { token, filename, adminId, createdAt: new Date().toISOString(), files: [], display_name: filename };
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

/*
  Modified makeBrowseKeyboardForIndex to accept an isAdmin flag.
  If isAdmin === true -> show two view-index buttons (user-style + admin-style).
  If isAdmin === false -> show single user-style view action.
*/
function makeBrowseKeyboardForIndex(pos, total, token, isAdmin = false) {
  const left = { text: '◀️', callback_data: 'browse_left' };
  const view = { text: '🔲 View files', callback_data: 'browse_view' };
  const right = { text: '▶️', callback_data: 'browse_right' };
  const random = { text: '🎲 Random', callback_data: 'browse_random' };
  const viewList = { text: '📃 View as list', callback_data: 'browse_list' };

  const viewIndexUser = { text: '🗂️ View index (User)', callback_data: 'view_index_user' };
  const viewIndexAdmin = { text: '🗂️ View index (Admin)', callback_data: 'view_index_admin' };

  if (isAdmin) {
    return { inline_keyboard: [[left, view, right], [random, viewList], [viewIndexUser, viewIndexAdmin]] };
  } else {
    return { inline_keyboard: [[left, view, right], [random, viewList], [viewIndexUser]] };
  }
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

// ---------- user index pagination helper ----------
/**
 * Build a paginated user-friendly index page (HTML)
 * Returns: { text, keyboard, page, totalPages, pageSize }
 */
function buildUserIndexPage(page = 0, pageSize = 15) {
  const idx = readIndex();
  const order = Array.isArray(idx.order) ? idx.order.slice() : [];
  const total = order.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  let p = Number(page) || 0;
  if (p < 0) p = 0;
  if (p >= totalPages) p = totalPages - 1;

  const start = p * pageSize;
  const slice = order.slice(start, start + pageSize);

  const lines = [];
  for (let i = 0; i < slice.length; i++) {
    const globalIndex = start + i;
    const fname = slice[i];
    const batch = readBatchFile(fname) || { filename: fname, display_name: fname };
    const display = escapeHtml(batch.display_name || batch.filename);
    const token = Object.keys(idx.tokens || {}).find(t => idx.tokens[t] === fname) || '';
    const url = BOT_USERNAME
      ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(token)}`
      : `https://t.me/Testerforcloudbot?start=${encodeURIComponent(token)}`;

    // Bold title and bold link text (link itself is clickable)
    lines.push(`<b>${globalIndex + 1}. ${display} - </b><a href="${url}"><b>Click here to view 👈</b></a>`);
  }

  const text = lines.join('\n');

  // navigation row
  const navRow = [];
  if (p > 0) navRow.push({ text: '⬅️ Prev', callback_data: `view_index_user_page_${p - 1}` });
  navRow.push({ text: `Page ${p + 1}/${totalPages}`, callback_data: `view_index_user_page_${p}` });
  if (p < totalPages - 1) navRow.push({ text: 'Next ➡️', callback_data: `view_index_user_page_${p + 1}` });

  // admin access and close button
  const adminRow = [{ text: 'View index (Admin)', callback_data: 'view_index_admin' }];
  const closeRow = [{ text: 'Close', callback_data: 'close_index_view' }];

  const keyboard = {
    inline_keyboard: [
      navRow,
      adminRow,
      closeRow
    ]
  };

  return { text, keyboard, page: p, totalPages, pageSize };
}

// ---------- index builder (quick) ----------
// Note: metadata feature was removed; index shows display_name and counts only.
function buildIndexTextAndKeyboardQuick(page = 0, requesterIsAdmin = false) {
  const idx = readIndex();
  const order = Array.isArray(idx.order) ? idx.order.slice() : [];
  const pageSize = 8;
  const total = order.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  let p = Number(page) || 0;
  if (p < 0) p = 0;
  if (p >= totalPages) p = totalPages - 1;
  const start = p * pageSize;
  const slice = order.slice(start, start + pageSize);

  const lines = [];
  const keyboardRows = [];
  for (let i = 0; i < slice.length; i++) {
    const globalIndex = start + i;
    const fname = slice[i];
    const batch = readBatchFile(fname) || { filename: fname, display_name: fname, files: [] };
    const display = batch.display_name || fname;
    lines.push(`${globalIndex+1}. ${display}`);
    const token = Object.keys(idx.tokens || {}).find(t => idx.tokens[t] === fname) || '';
    const filesCount = (batch.files || []).length;
    const countBtn = { text: `${filesCount} files`, callback_data: `index_view_${token}_${p}` };
    if (requesterIsAdmin) {
      const upBtn = { text: '🔼', callback_data: `index_up_${token}_${p}` };
      const downBtn = { text: '🔽', callback_data: `index_down_${token}_${p}` };
      keyboardRows.push([countBtn, upBtn, downBtn]);
    } else {
      keyboardRows.push([countBtn]);
    }
  }

  const navRow = [];
  if (p > 0) navRow.push({ text: '⬅️ Prev', callback_data: `index_prev_${p-1}` });
  navRow.push({ text: `Page ${p+1}/${totalPages}`, callback_data: `index_page_${p}` });
  if (p < totalPages - 1) navRow.push({ text: 'Next ➡️', callback_data: `index_next_${p+1}` });
  keyboardRows.push(navRow);
  keyboardRows.push([{ text: '🔁 Refresh', callback_data: `index_refresh_${p}` }]);

  const text = `<b>Index</b> (showing ${start+1}–${Math.min(start + pageSize, total)} of ${total})\n\n` + lines.map(l => escapeHtml(l)).join('\n');
  return { text, keyboard: { inline_keyboard: keyboardRows }, page: p, totalPages, pageSize };
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

    // listfiles
    if (text && text.startsWith('/listfiles')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /listfiles.');
      const idx = readIndex();
      if (!idx.order || idx.order.length === 0) return safeSendMessage(chatId, 'No batches found.');
      let out = 'Batches (send order):\n';
      idx.order.forEach((fname,i)=>{ const token = Object.keys(idx.tokens).find(t=>idx.tokens[t]===fname); const batch = readBatchFile(fname); const name = batch && batch.display_name ? batch.display_name : fname; out += `${i+1}. ${name} — token: /start_${token}\n`; });
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
      try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); delete idx.tokens[token]; idx.order = idx.order.filter(f=>f!==filename); writeIndex(idx); return safeSendMessage(chatId, `Deleted ${filename} (token ${token})`); } catch(e){ console.error(e); return safeSendMessage(chatId, 'Delete failed: '+(e && e.message)); }
    }

    // index page size (we keep static page size now)
    if (text && text.startsWith('/set_index_page_size')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may set this.');
      return safeSendMessage(chatId, 'Index page size setting removed (feature disabled).');
    }

    // /refresh_metadata removed
    if (text && text.startsWith('/refresh_metadata')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use that.');
      return safeSendMessage(chatId, 'Metadata feature removed.');
    }

    // /start (no token) - present Browse + inline placeholder
    if (text && (text === '/start' || text.trim() === `/start@${BOT_USERNAME}`)) {
      const kb = { inline_keyboard: [[ { text: 'Browse', callback_data: 'browse_open' }, { text: 'Search inline', switch_inline_query_current_chat: '' } ], [ { text: 'Contact Admin', url: 'https://t.me/aswinlalus' } ]] };
      return safeSendMessage(chatId, `Use Browse to preview latest uploads or use inline search (type @${BOT_USERNAME} in any chat).`, { reply_markup: kb });
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
      const kb = { inline_keyboard: [[ { text:'User', callback_data:'help_user' }, { text:'Admin', callback_data:'help_admin' } ], [ { text:'Contact Admin', url: 'https://t.me/aswinlalus' } ]] };
      return safeSendMessage(chatId, 'Choose User or Admin help. Admin button requires admin privileges.', { reply_markup: kb });
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
      // pass isAdmin flag so keyboard shows user/admin options correctly
      const kb = makeBrowseKeyboardForIndex(lastIndex, order.length, batch.token, (from && from.id === ADMIN_ID));
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
    const data = q.data || ''; const chatId = q.message && q.message.chat && q.message.chat.id; const msgId = q.message && q.message.message_id;
    if (!data) return safeAnswerCallbackQuery(q.id);

    // help user/admin
    if (data === 'help_user' || data === 'help_admin') {
      await safeAnswerCallbackQuery(q.id);
      if (data === 'help_user') {
        await safeSendMessage(chatId, `User help — use @${BOT_USERNAME} inline or /browse or open a token with /start_<TOKEN>.\nExample: /start_OBQUMJSSK9YB`);
        return;
      }
      if (data === 'help_admin') {
        if (q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
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
        await safeSendMessage(chatId, helpText);
        return;
      }
    }

    // index & browse callbacks (including new view_index_user/admin handlers)
    if (data === 'view_index' || data.startsWith('index_') || data.startsWith('browse_') || data.startsWith('file_') || data.startsWith('browse_file_') || data === 'view_index_user' || data === 'view_index_admin' || data.startsWith('view_index_user_page_') || data === 'close_index_view') {
      await safeAnswerCallbackQuery(q.id);

      if (data === 'view_index') {
        const idxPayload = buildIndexTextAndKeyboardQuick(0, (q.from && q.from.id === ADMIN_ID));
        try { await bot.sendMessage(chatId, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        catch (e) { await safeSendMessage(chatId, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        return;
      }

      // handle user index view + pagination
      if (data === 'view_index_user' || data.startsWith('view_index_user_page_') || data === 'close_index_view') {
        await safeAnswerCallbackQuery(q.id);

        // Close: clear inline keyboard on the message
        if (data === 'close_index_view') {
          try {
            if (q.message && q.message.chat && q.message.message_id) {
              await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: q.message.chat.id, message_id: q.message.message_id });
            }
          } catch (e) {
            // ignore errors
          }
          return;
        }

        // Determine page
        let page = 0;
        if (data.startsWith('view_index_user_page_')) {
          const parts = data.split('_');
          const last = parts[parts.length - 1];
          page = Number(last) || 0;
        } else {
          page = 0; // default when user first presses view_index_user
        }

        const pageSize = 15; // tweak if you want more/less items per page
        const payload = buildUserIndexPage(page, pageSize);

        // Try to edit the existing message where the callback came from
        try {
          if (q.message && q.message.chat && q.message.message_id) {
            await bot.editMessageText(payload.text, {
              chat_id: q.message.chat.id,
              message_id: q.message.message_id,
              parse_mode: 'HTML',
              disable_web_page_preview: true,
              reply_markup: payload.keyboard
            });
            return;
          }
        } catch (e) {
          // if edit fails, fall back to sending a new message
        }

        // Fallback: send a fresh message
        try {
          await safeSendMessage(chatId, payload.text, {
            parse_mode: 'HTML',
            disable_web_page_preview: true,
            reply_markup: payload.keyboard
          });
        } catch (e) {
          await safeSendMessage(chatId, 'Unable to show index right now.');
        }

        return;
      }

      // New: view_index_admin -> send original admin index view with inline management keyboard
      if (data === 'view_index_admin') {
        if (q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
        const idxPayload = buildIndexTextAndKeyboardQuick(0, true);
        try { await bot.sendMessage(chatId, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        catch (e) { await safeSendMessage(chatId, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        return;
      }

      // index pagination handlers (admin index)
      if (data.startsWith('index_prev_') || data.startsWith('index_next_') || data.startsWith('index_page_') || data.startsWith('index_refresh_')) {
        const parts = data.split('_');
        const page = Number(parts[2]) || 0;
        const idxPayload = buildIndexTextAndKeyboardQuick(page, (q.from && q.from.id === ADMIN_ID));
        try { await bot.editMessageText(idxPayload.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        catch (e) { await safeSendMessage(q.message.chat.id, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        return;
      }

      // index actions with token (admin)
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
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token, (q.from && q.from.id === ADMIN_ID));
        try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: baseMsg.message_id }); } catch (e) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch(_){} }
        return;
      }

      // other browse/file callbacks will be handled by the remainder of your callback handling below
    }

    // rating and the rest of callbacks follow — ensure these are implemented in your original code.
    // If no handlers matched:
    await safeAnswerCallbackQuery(q.id, { text: 'Action handled.' });

  } catch (e) {
    console.error('callback_query handler error', e && (e.stack || e.message));
    try { await safeAnswerCallbackQuery(q.id, { text: 'Error handling action' }); } catch (_) {}
  }
});

// ---------- Final notes ----------
console.log('Bot ready. Commands: /help, /sendfile, /doneadd, /addto <TOKEN>, /doneaddto, /edit_caption <TOKEN>, /listfiles, /deletefile <TOKEN>, /listusers, /getuser <id>, /start_<TOKEN>, /browse, /view_index');
console.log('Metadata feature removed; bot runs without external metadata fetching.');
