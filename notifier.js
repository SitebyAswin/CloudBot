// notifier.js
const TelegramBot = require('node-telegram-bot-api');

const BOT_TOKEN = process.env.BOT_TOKEN;
const NOTIFY_CHAT = process.env.NOTIFY_CHAT || '@cloudbackup2025';

if (!BOT_TOKEN) throw new Error('BOT_TOKEN not set in env');

const bot = new TelegramBot(BOT_TOKEN, { polling: false });

async function notifyBatch(batchName, action) {
  if (!batchName) return;
  let text = '';
  if (action === 'new') text = `NEW BATCH: ${batchName} has been UPLOADED ✅✅`;
  else if (action === 'updated') text = `${batchName} has been UPDATED 🔥🔥`;
  else text = `${batchName} — ${action}`;

  try {
    await bot.sendMessage(NOTIFY_CHAT, text);
    console.log(`[notify] ${action.toUpperCase()} -> ${batchName}`);
  } catch (err) {
    console.error('[notify] failed to send message', err && err.message ? err.message : err);
  }
}

module.exports = { notifyBatch };
