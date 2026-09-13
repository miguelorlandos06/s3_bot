// index.js
const { Bot } = require('grammy');
const axios = require('axios');
const express = require('express');
const fs = require('fs-extra');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const { pipeline } = require('stream/promises');

const BOT_TOKEN = "8633754852:AAFI3Eesq3dy2MqSWFGndJ7MDrm4eVHHlpc";
const S3 = "https://s3.todus.cu/stream";
const DOWNLOAD_PATH = "/tmp/todus_uploads";

fs.ensureDirSync(DOWNLOAD_PATH);

const bot = new Bot(BOT_TOKEN);

// --- Cola de edits con throttle por chat ---
const editState = new Map(); // chatId -> { pending, lastSent, timer }
function queueEdit(api, chatId, msgId, text, minInterval = 1200) {
    let s = editState.get(chatId);
    if (!s) { s = { pending: null, lastSent: 0, timer: null }; editState.set(chatId, s); }
    s.pending = { msgId, text };

    const now = Date.now();
    const elapsed = now - s.lastSent;

    if (elapsed >= minInterval) {
        const p = s.pending; s.pending = null; s.lastSent = now;
        api.editMessageText(chatId, p.msgId, p.text).catch(() => {});
    } else if (!s.timer) {
        s.timer = setTimeout(() => {
            s.timer = null;
            const p = s.pending; s.pending = null;
            if (p) { s.lastSent = Date.now(); api.editMessageText(chatId, p.msgId, p.text).catch(() => {}); }
        }, minInterval - elapsed);
    }
}

function formatSize(b) {
    if (b < 1024) return `${b} B`;
    if (b < 1048576) return `${(b/1024).toFixed(1)} KB`;
    if (b < 1073741824) return `${(b/1048576).toFixed(1)} MB`;
    return `${(b/1073741824).toFixed(2)} GB`;
}
const progressBar = p => '⬢'.repeat(Math.round(15*p/100)) + '⬡'.repeat(15 - Math.round(15*p/100));

function getFilenameFromUrl(url) {
    try {
        const n = path.basename(new URL(url).pathname);
        if (n && n.length > 2) return decodeURIComponent(n);
    } catch {}
    return null;
}

// --- Cliente axios reutilizable con pool keep-alive ---
const http = require('http');
const https = require('https');
const agent = new https.Agent({ keepAlive: true, maxSockets: 64 });
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 64 });
const client = axios.create({ httpAgent, httpsAgent: agent, timeout: 0 });

// --- Cola global de trabajos (evita saturar disco/red) ---
const MAX_CONCURRENT = 3;
let running = 0;
const queue = [];
function enqueue(fn) {
    return new Promise((resolve, reject) => {
        queue.push({ fn, resolve, reject });
        pump();
    });
}
function pump() {
    while (running < MAX_CONCURRENT && queue.length) {
        const { fn, resolve, reject } = queue.shift();
        running++;
        fn().then(resolve, reject).finally(() => { running--; pump(); });
    }
}

async function descargarYSubir(ctx, url, statusMsg) {
    return enqueue(async () => {
        const filename = getFilenameFromUrl(url) || `file_${Date.now()}`;
        const ext = path.extname(filename) || '.bin';
        const tempPath = path.join(DOWNLOAD_PATH, `${crypto.randomBytes(8).toString('hex')}${ext}`);

        try {
            const res = await client.get(url, {
                responseType: 'stream',
                maxRedirects: 10,
                headers: { 'User-Agent': 'Mozilla/5.0' },
                validateStatus: s => s >= 200 && s < 400,
            });

            const total = Number(res.headers['content-length']) || 0;
            let downloaded = 0;
            let lastPct = -1;
            const chatId = ctx.chat.id;
            const msgId = statusMsg.message_id;

            res.data.on('data', chunk => {
                downloaded += chunk.length;
                if (!total) return;
                const pct = (downloaded / total) * 100 | 0;
                if (pct - lastPct >= 5 || pct === 100) {
                    lastPct = pct;
                    queueEdit(ctx.api, chatId, msgId,
                        `┎ DOWNLOADING\n┠ [${progressBar(pct)}]\n┠ PERCENTAGE: ${pct}%\n┖ SIZE: ${formatSize(downloaded)}/${formatSize(total)}`
                    );
                }
            });

            await pipeline(res.data, fs.createWriteStream(tempPath));

            const { size } = await fsp.stat(tempPath);
            queueEdit(ctx.api, chatId, msgId, "UPLOADING...", 0);
            await new Promise(r => setTimeout(r, 50)); // deja salir el edit

            const remote = `${crypto.randomBytes(4).toString('hex')}_${filename}`;
            const uploadUrl = `${S3}/${remote}`;

            await client.put(uploadUrl, fs.createReadStream(tempPath), {
                headers: { 'Content-Length': size, 'Content-Type': 'application/octet-stream' },
                duplex: 'half',           // requerido en Node 18+
                timeout: 1800000,
                maxBodyLength: Infinity,
                maxContentLength: Infinity,
            });

            const name = path.basename(filename, ext).replace(/_/g, ' ');
            await ctx.api.editMessageText(chatId, msgId,
                `┎ NAME: ${name}\n┠ EXTENSION: ${ext.replace('.', '')}\n┠ SIZE: ${formatSize(size)}\n┖ URL: ${uploadUrl}`
            );
        } catch (e) {
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
                `ERROR: ${(e.message || '').slice(0, 200)}`).catch(() => {});
        } finally {
            fsp.unlink(tempPath).catch(() => {});
        }
    });
}

bot.command('start', ctx => ctx.reply("Send me a direct download link."));

// Regex más estricta: excluye puntuación final común
const URL_RE = /(https?:\/\/[^\s<>"']+?)(?=[.,;:!?)\]]?(\s|$))/i;

bot.on('message:text', async ctx => {
    const m = ctx.message.text.trim().match(URL_RE);
    if (!m) return;
    const statusMsg = await ctx.reply("PROCESSING...");
    await descargarYSubir(ctx, m[1], statusMsg);
});

const app = express();
app.get('/', (_q, r) => r.json({ status: 'online' }));
app.get('/health', (_q, r) => r.json({ status: 'healthy' }));
app.listen(10000, () => console.log('Web on 10000'));

setInterval(() => client.get('https://s3-bot-pjpo.onrender.com/health').catch(() => {}), 300000);

bot.start();
console.log('BOT READY');