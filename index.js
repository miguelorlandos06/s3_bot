// index.js
const { Bot } = require('grammy');
const axios = require('axios');
const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const crypto = require('crypto');

const BOT_TOKEN = "8633754852:AAFI3Eesq3dy2MqSWFGndJ7MDrm4eVHHlpc";
const S3 = "https://s3.todus.cu/stream";
const DOWNLOAD_PATH = "/tmp/todus_uploads";

fs.ensureDirSync(DOWNLOAD_PATH);

const bot = new Bot(BOT_TOKEN);

function formatSize(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024*1024) return `${(bytes/1024).toFixed(1)} KB`;
    if (bytes < 1024*1024*1024) return `${(bytes/(1024*1024)).toFixed(1)} MB`;
    return `${(bytes/(1024*1024*1024)).toFixed(1)} GB`;
}

function progressBar(pct) {
    const w = 15;
    const f = Math.round(w * pct / 100);
    return '⬢'.repeat(f) + '⬡'.repeat(w - f);
}

function getFilenameFromUrl(url) {
    try {
        const u = new URL(url);
        const name = path.basename(u.pathname);
        if (name && name.length > 2) return decodeURIComponent(name);
    } catch {}
    return null;
}

async function descargarYSubir(ctx, url, statusMsg) {
    try {
        const filename = getFilenameFromUrl(url) || `file_${Date.now()}`;
        const ext = path.extname(filename) || '.bin';
        const tempPath = path.join(DOWNLOAD_PATH, `${crypto.randomBytes(8).toString('hex')}${ext}`);

        const response = await axios({
            method: 'get',
            url: url,
            responseType: 'stream',
            timeout: 1800000,
            maxRedirects: 5,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });

        const totalSize = Number(response.headers['content-length']) || 0;
        let downloaded = 0;
        let lastPct = -10;

        const writer = fs.createWriteStream(tempPath);

        response.data.on('data', (chunk) => {
            downloaded += chunk.length;
            const pct = totalSize > 0 ? Math.floor((downloaded / totalSize) * 100) : 0;
            if (pct - lastPct >= 10 || pct === 100) {
                lastPct = pct;
                ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
                    `┎ DOWNLOADING\n┠ [${progressBar(pct)}]\n┠ PERCENTAGE: ${pct}%\n┖ SIZE: ${formatSize(downloaded)}/${totalSize ? formatSize(totalSize) : '?'}`
                ).catch(() => {});
            }
        });

        response.data.pipe(writer);
        await new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
        });

        const size = fs.statSync(tempPath).size;
        await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id, "UPLOADING...");

        const remote = `${crypto.randomBytes(4).toString('hex')}_${filename}`;
        const uploadUrl = `${S3}/${remote}`;
        const fileStream = fs.createReadStream(tempPath);

        await axios.put(uploadUrl, fileStream, {
            headers: { 'Content-Length': size },
            timeout: 1800000
        });

        const name = path.basename(filename, ext).replace(/_/g, ' ');
        await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
            `┎ NAME: ${name}\n┠ EXTENSION: ${ext.replace('.', '')}\n┠ SIZE: ${formatSize(size)}\n┖ URL: ${uploadUrl}`
        );

        fs.removeSync(tempPath);

    } catch (e) {
        await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id, `ERROR: ${e.message.slice(0, 200)}`);
    }
}

bot.command('start', async (ctx) => {
    await ctx.reply("Send me a direct download link.");
});

bot.on('message:text', async (ctx) => {
    const texto = ctx.message.text.trim();
    const urlRegex = /(https?:\/\/[^\s]+)/i;
    const match = texto.match(urlRegex);
    if (!match) return;

    const statusMsg = await ctx.reply("PROCESSING...");
    await descargarYSubir(ctx, match[1], statusMsg);
});

const app = express();
app.get('/', (req, res) => res.json({ status: 'online' }));
app.get('/health', (req, res) => res.json({ status: 'healthy' }));
app.listen(10000, () => console.log('Web on 10000'));

setInterval(() => {
    axios.get('https://s3-uploader.onrender.com/health', { timeout: 10000 }).catch(() => {});
}, 300000);

bot.start();
console.log('BOT READY');