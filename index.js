// index.js
const { Bot } = require('grammy');
const axios = require('axios');
const express = require('express');
const fs = require('fs-extra');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const http = require('http');
const https = require('https');
const { pipeline } = require('stream/promises');
const { Transform } = require('stream');

const BOT_TOKEN = "8611512607:AAFYiZUGWn6r8Ehp9YWCHFUG2hZ2hA01CDw";
const S3 = "https://s3.todus.cu/stream";
const DOWNLOAD_PATH = "/tmp/todus_uploads";
const MAX_FILE_SIZE = 2000 * 1024 * 1024; // 2 GB (servidor local Bot API)

fs.ensureDirSync(DOWNLOAD_PATH);

// Apuntamos al servidor local de la Bot API (docker-compose)
const bot = new Bot(BOT_TOKEN, {
    client: { apiRoot: "http://tg-bot-api:8081" },
});

// ---------- Utilidades ----------
function formatSize(b) {
    if (b < 1024) return `${b} B`;
    if (b < 1048576) return `${(b / 1024).toFixed(1)} KB`;
    if (b < 1073741824) return `${(b / 1048576).toFixed(1)} MB`;
    return `${(b / 1073741824).toFixed(2)} GB`;
}
const progressBar = p =>
    '⬢'.repeat(Math.round(15 * p / 100)) + '⬡'.repeat(15 - Math.round(15 * p / 100));

function getFilenameFromUrl(url) {
    try {
        const n = path.basename(new URL(url).pathname);
        if (n && n.length > 2) return decodeURIComponent(n);
    } catch {}
    return null;
}

// ---------- Cola de edits con throttle por chat ----------
const editState = new Map();
function queueEdit(api, chatId, msgId, text, minInterval = 1200) {
    let s = editState.get(chatId);
    if (!s) {
        s = { pending: null, lastSent: 0, timer: null };
        editState.set(chatId, s);
    }
    s.pending = { msgId, text };

    const now = Date.now();
    const elapsed = now - s.lastSent;

    const flush = () => {
        const p = s.pending;
        s.pending = null;
        if (!p) return;
        s.lastSent = Date.now();
        api.editMessageText(chatId, p.msgId, p.text).catch(() => {});
    };

    if (elapsed >= minInterval) {
        flush();
    } else if (!s.timer) {
        s.timer = setTimeout(() => {
            s.timer = null;
            flush();
        }, minInterval - elapsed);
    }
}

// ---------- Cliente HTTP con keep-alive ----------
const agent = new https.Agent({ keepAlive: true, maxSockets: 64 });
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 64 });
const client = axios.create({ httpAgent, httpsAgent: agent, timeout: 0 });

// ---------- Cola global de trabajos ----------
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
        fn().then(resolve, reject).finally(() => {
            running--;
            pump();
        });
    }
}

// ---------- Subida a S3 con progreso ----------
async function subirAS3(tempPath, filename, size, onProgress) {
    const remote = `${crypto.randomBytes(4).toString('hex')}_${filename}`;
    const uploadUrl = `${S3}/${remote}`;

    let uploaded = 0;
    const progressStream = new Transform({
        transform(chunk, _enc, cb) {
            uploaded += chunk.length;
            if (onProgress) onProgress(uploaded, size);
            cb(null, chunk);
        },
    });

    const readStream = fs.createReadStream(tempPath);
    readStream.pipe(progressStream);

    await client.put(uploadUrl, progressStream, {
        headers: {
            'Content-Length': size,
            'Content-Type': 'application/octet-stream',
        },
        duplex: 'half',
        timeout: 1800000,
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
    });

    return uploadUrl;
}

// ---------- Descarga desde URL + subida ----------
async function descargarYSubir(ctx, url, statusMsg) {
    return enqueue(async () => {
        const filename = getFilenameFromUrl(url) || `file_${Date.now()}`;
        const ext = path.extname(filename) || '.bin';
        const tempPath = path.join(
            DOWNLOAD_PATH,
            `${crypto.randomBytes(8).toString('hex')}${ext}`
        );

        const chatId = ctx.chat.id;
        const msgId = statusMsg.message_id;
        let stream = null;

        try {
            const res = await client.get(url, {
                responseType: 'stream',
                maxRedirects: 10,
                headers: { 'User-Agent': 'Mozilla/5.0' },
                validateStatus: s => s >= 200 && s < 400,
            });
            stream = res.data;

            const total = Number(res.headers['content-length']) || 0;
            let downloaded = 0;
            let lastPct = -1;

            stream.on('data', chunk => {
                downloaded += chunk.length;
                if (!total) return;
                const pct = (downloaded / total) * 100 | 0;
                if (pct - lastPct >= 5 || pct === 100) {
                    lastPct = pct;
                    queueEdit(
                        ctx.api, chatId, msgId,
                        `┎ DOWNLOADING\n┠ [${progressBar(pct)}]\n┠ PERCENTAGE: ${pct}%\n┖ SIZE: ${formatSize(downloaded)}/${formatSize(total)}`
                    );
                }
            });

            stream.on('error', err => {
                console.error('download stream error:', err.message);
            });

            await pipeline(stream, fs.createWriteStream(tempPath));

            const { size } = await fsp.stat(tempPath);

            queueEdit(ctx.api, chatId, msgId, "┎ UPLOADING\n┖ Preparando...", 0);
            await new Promise(r => setTimeout(r, 60));

            let lastUpPct = -1;
            const uploadUrl = await subirAS3(tempPath, filename, size, (sent, total) => {
                if (!total) return;
                const pct = (sent / total) * 100 | 0;
                if (pct - lastUpPct >= 5 || pct === 100) {
                    lastUpPct = pct;
                    queueEdit(
                        ctx.api, chatId, msgId,
                        `┎ UPLOADING\n┠ [${progressBar(pct)}]\n┠ PERCENTAGE: ${pct}%\n┖ SIZE: ${formatSize(sent)}/${formatSize(total)}`
                    );
                }
            });

            const name = path.basename(filename, ext).replace(/_/g, ' ');
            await ctx.api
                .editMessageText(
                    chatId,
                    msgId,
                    `┎ NAME: ${name}\n┠ EXTENSION: ${ext.replace('.', '')}\n┠ SIZE: ${formatSize(size)}\n┖ URL: ${uploadUrl}`
                )
                .catch(() => {});
        } catch (e) {
            await ctx.api
                .editMessageText(
                    chatId,
                    msgId,
                    `ERROR: ${(e.message || '').slice(0, 200)}`
                )
                .catch(() => {});
        } finally {
            if (stream && typeof stream.destroy === 'function') {
                try { stream.destroy(); } catch {}
            }
            fsp.unlink(tempPath).catch(() => {});
        }
    });
}

// ---------- Procesar archivo recibido (vía servidor local Bot API) ----------
async function procesarArchivo(ctx, file, originalName, statusMsg) {
    return enqueue(async () => {
        const chatId = ctx.chat.id;
        const msgId = statusMsg.message_id;

        const filename = originalName || `file_${Date.now()}`;
        const ext = path.extname(filename) || '.bin';
        const tempPath = path.join(
            DOWNLOAD_PATH,
            `${crypto.randomBytes(8).toString('hex')}${ext}`
        );

        try {
            const fileInfo = await ctx.api.getFile(file.file_id);
            // En modo local, file_path es una ruta absoluta dentro del contenedor
            const localFilePath = fileInfo.file_path;

            const { size } = await fsp.stat(localFilePath);

            // Copia local instantánea (mismo volumen compartido)
            await fsp.copyFile(localFilePath, tempPath);

            queueEdit(ctx.api, chatId, msgId, "┎ UPLOADING\n┖ Preparando...", 0);
            await new Promise(r => setTimeout(r, 60));

            let lastUpPct = -1;
            const uploadUrl = await subirAS3(tempPath, filename, size, (sent, total) => {
                if (!total) return;
                const pct = (sent / total) * 100 | 0;
                if (pct - lastUpPct >= 5 || pct === 100) {
                    lastUpPct = pct;
                    queueEdit(
                        ctx.api, chatId, msgId,
                        `┎ UPLOADING\n┠ [${progressBar(pct)}]\n┠ PERCENTAGE: ${pct}%\n┖ SIZE: ${formatSize(sent)}/${formatSize(total)}`
                    );
                }
            });

            const name = path.basename(filename, ext).replace(/_/g, ' ');
            await ctx.api
                .editMessageText(
                    chatId,
                    msgId,
                    `┎ NAME: ${name}\n┠ EXTENSION: ${ext.replace('.', '')}\n┠ SIZE: ${formatSize(size)}\n┖ URL: ${uploadUrl}`
                )
                .catch(() => {});
        } catch (e) {
            await ctx.api
                .editMessageText(
                    chatId,
                    msgId,
                    `ERROR: ${(e.message || '').slice(0, 200)}`
                )
                .catch(() => {});
        } finally {
            fsp.unlink(tempPath).catch(() => {});
        }
    });
}

// ---------- Extraer archivo del mensaje ----------
function extraerArchivo(msg) {
    if (msg.document) return { file: msg.document, name: msg.document.file_name || `doc_${Date.now()}.bin` };
    if (msg.video) return { file: msg.video, name: msg.video.file_name || `video_${Date.now()}.mp4` };
    if (msg.audio) return { file: msg.audio, name: msg.audio.file_name || `audio_${Date.now()}.mp3` };
    if (msg.voice) return { file: msg.voice, name: `voice_${Date.now()}.ogg` };
    if (msg.video_note) return { file: msg.video_note, name: `video_note_${Date.now()}.mp4` };
    if (msg.animation) return { file: msg.animation, name: msg.animation.file_name || `animation_${Date.now()}.mp4` };
    if (msg.sticker) return { file: msg.sticker, name: `sticker_${Date.now()}.webp` };
    if (msg.photo && msg.photo.length) {
        const largest = msg.photo[msg.photo.length - 1];
        return { file: largest, name: `photo_${Date.now()}.jpg` };
    }
    return null;
}

// ---------- Handlers ----------
bot.command('start', ctx =>
    ctx.reply("Envíame un enlace de descarga directa o un archivo (hasta 2 GB).")
);

const URL_RE = /(https?:\/\/[^\s<>"']+?)(?=[.,;:!?)\]]?(\s|$))/i;

bot.on('message:text', async ctx => {
    const m = ctx.message.text.trim().match(URL_RE);
    if (!m) {
        try {
            await ctx.reply("Envíame un enlace de descarga directa o un archivo (hasta 2 GB).");
        } catch {}
        return;
    }
    let statusMsg;
    try {
        statusMsg = await ctx.reply("PROCESSING...");
    } catch { return; }
    descargarYSubir(ctx, m[1], statusMsg).catch(err => {
        console.error('job error:', err?.message || err);
    });
});

bot.on(
    ['message:document', 'message:video', 'message:audio', 'message:voice',
     'message:video_note', 'message:animation', 'message:sticker', 'message:photo'],
    async ctx => {
        const info = extraerArchivo(ctx.message);
        if (!info) return;

        const { file, name } = info;
        const fileSize = file.file_size || 0;

        if (fileSize && fileSize > MAX_FILE_SIZE) {
            try {
                await ctx.reply(
                    `❌ El archivo es demasiado grande (${formatSize(fileSize)}). El límite es ${formatSize(MAX_FILE_SIZE)}.`
                );
            } catch {}
            return;
        }

        let statusMsg;
        try {
            statusMsg = await ctx.reply("PROCESSING...");
        } catch { return; }

        procesarArchivo(ctx, file, name, statusMsg).catch(err => {
            console.error('file job error:', err?.message || err);
        });
    }
);

// ---------- Servidor web ----------
const app = express();
app.get('/', (_q, r) => r.json({ status: 'online' }));
app.get('/health', (_q, r) => r.json({ status: 'healthy' }));
app.listen(10000, () => console.log('Web on 10000'));

setInterval(
    () => client.get('https://s3-bot-pjpo.onrender.com/health').catch(() => {}),
    300000
);

// ---------- Arranque ----------
process.on('unhandledRejection', err => console.error('unhandledRejection:', err));
process.on('uncaughtException', err => console.error('uncaughtException:', err));

bot.start();
console.log('BOT READY');