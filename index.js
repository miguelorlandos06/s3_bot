import { Bot, InlineKeyboard } from "grammy";
import { hydrateFiles } from "@grammyjs/files";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { execFile } from "child_process";
import { promisify } from "util";

const execFileAsync = promisify(execFile);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

const BOT_TOKEN = process.env.BOT_TOKEN;
const S3_BASE_URL = process.env.S3_BASE_URL || "https://s3.todus.cu";
const TMP_DIR = process.env.TODUS_TMP || path.join(__dirname, "tmp");
const YTDLP_COOKIES = process.env.YTDLP_COOKIES || "";
const YTDLP_IMPERSONATE = process.env.YTDLP_IMPERSONATE || "chrome";

const PICTA_USER = process.env.PICTA_USER;
const PICTA_PASS = process.env.PICTA_PASS;

if (!fs.existsSync(TMP_DIR)) fs.mkdirSync(TMP_DIR, { recursive: true });

const bot = new Bot(BOT_TOKEN);
bot.api.config.use(hydrateFiles(BOT_TOKEN));
bot.api.config.use((prev, method, payload, signal) => {
  if (payload && typeof payload === "object" && !("parse_mode" in payload)) payload.parse_mode = "HTML";
  return prev(method, payload, signal);
});

const activeJobs = new Map();
const pendingPlatform = new Map();
const jobKey = (chatId, messageId) => `${chatId}:${messageId}`;

// --- Utilidades ---
function formatSize(bytes) { if (!bytes) return "0 B"; const u = ["B","KB","MB","GB"]; let i=0,s=bytes; while(s>=1024&&i<u.length-1){s/=1024;i++;} return `${s.toFixed(2)} ${u[i]}`; }
function formatETA(seconds) { if (!isFinite(seconds)||seconds<0) return "?"; const s=Math.round(seconds); if(s<60) return `${s}s`; if(s<3600) return `${Math.floor(s/60)}m ${s%60}s`; return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`; }
function progressBar(pct, width = 15) { const c=Math.max(0,Math.min(100,pct)); const f=Math.round((width*c)/100); return "✦".repeat(f)+"✧".repeat(width-f); }
function escapeHtml(s) { return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
function sanitize(name) { return name.replace(/[^a-zA-Z0-9._-]/g,"_").replace(/^_+|_+$/g,"").slice(0,120) || "file.bin"; }
function renderBox(title, fields) { let out = `┎ ${title}\n`; fields.forEach((f,i)=>{ const p=i===fields.length-1?"┖":"┠"; out += `${p} ${f[0]?f[0]+": ":""}${f[1]}\n`; }); return out.trimEnd(); }
function renderProgress(phase, current, total, startTime, filename) {
  const pct = total > 0 ? Math.min(100, Math.round((current/total)*100)) : 0;
  const bar = progressBar(pct);
  const elapsed = (Date.now() - startTime) / 1000;
  const speed = elapsed > 0 ? current / elapsed : 0;
  const eta = speed > 0 && total > current ? (total - current) / speed : 0;
  const fields = [];
  if (filename) fields.push(["NAME", `<code>${escapeHtml(filename)}</code>`]);
  fields.push(["", `[${bar}]`]);
  fields.push(["PERCENT", `${pct}%`]);
  fields.push(["SIZE", `${formatSize(current)}/${formatSize(total)}`]);
  if (speed > 1) { fields.push(["SPEED", `${formatSize(speed)}/s`]); fields.push(["ETA", formatETA(eta)]); }
  return renderBox(phase, fields);
}
function cancelKeyboard(k) { return new InlineKeyboard().text("Cancelar", `cancel:${k}`); }
async function safeEdit(ctx, chatId, messageId, text, keyboard) { try { await ctx.api.editMessageText(chatId, messageId, text, keyboard?{reply_markup:keyboard}:undefined); } catch {} }
function ytdlpArgs(extra = []) {
  const args = ["--js-runtimes", "node", "--remote-components", "ejs:github", ...extra];
  if (YTDLP_COOKIES && fs.existsSync(YTDLP_COOKIES)) args.push("--cookies", YTDLP_COOKIES);
  if (YTDLP_IMPERSONATE) args.push("--impersonate", YTDLP_IMPERSONATE);
  return args;
}

// --- Picta (usando picta-dlp con timeout) ---
async function getPictaInfo(url) {
  const { stdout } = await execFileAsync("picta-dlp", ["-u", PICTA_USER, "-p", PICTA_PASS, "-J", url], { maxBuffer: 64 * 1024 * 1024, timeout: 60000 });
  return JSON.parse(stdout);
}
async function getPictaQualities(url) {
  const { stdout } = await execFileAsync("picta-dlp", ["-u", PICTA_USER, "-p", PICTA_PASS, "--no-playlist", "-F", url], { maxBuffer: 10 * 1024 * 1024, timeout: 60000 });
  const formats = [];
  for (const line of stdout.split("\n")) {
    const match = line.match(/(\d+)\s+video\s+(\d{3,4})x(\d{3,4})/);
    if (match) formats.push({ id: match[1], height: parseInt(match[3], 10) });
  }
  return Array.from(new Map(formats.map(f => [f.height, f])).values()).sort((a, b) => b.height - a.height);
}
async function downloadPictaVideo(url, formatId, dest, ctx, chatId, statusMsgId, filename) {
  const args = ["-u", PICTA_USER, "-p", PICTA_PASS, "--no-playlist", "-f", formatId, "--audio-lang", "und", "-o", dest, url];
  return new Promise((resolve, reject) => {
    const child = execFile("picta-dlp", args, { maxBuffer: 64 * 1024 * 1024 });
    child.on("close", (code) => code === 0 ? resolve(dest) : reject(new Error(`picta-dlp falló (${code})`)));
    child.on("error", reject);
  });
}
function extractPictaMeta(info) {
  return {
    title: info.title,
    thumbnail: info.thumbnail,
    synopsis: info.description,
    duration: info.duration,
    year: info.ano ? String(info.ano) : null,
    genres: info.genero ? info.genero.map(g => '#' + g.nombre.replace(/\s+/g, '')).join(' ') : null,
    director: info.director ? info.director.map(d => d.nombre).join(', ') : null,
    cast: info.reparto ? info.reparto.slice(0, 5).map(a => a.nombre).join(', ') : null,
    season: info.temporada ? info.temporada.nombre : null,
    episode: info.capitulo ? String(info.capitulo) : null,
  };
}

// --- YouTube ---
async function getYoutubeQualities(url) {
  const { stdout } = await execFileAsync("yt-dlp", ytdlpArgs(["-F", url]), { maxBuffer: 10 * 1024 * 1024, timeout: 60000 });
  const heights = new Set();
  for (const line of stdout.split("\n")) {
    const match = line.match(/(\d{3,4})x(\d{3,4})/);
    if (match) { const h = parseInt(match[2], 10); if (h >= 144 && h <= 2160) heights.add(h); }
  }
  return Array.from(heights).sort((a, b) => a - b).map(h => ({ id: h, height: h }));
}
async function getYoutubeInfo(url) {
  const { stdout } = await execFileAsync("yt-dlp", ytdlpArgs(["-J", "--no-playlist", url]), { maxBuffer: 64 * 1024 * 1024, timeout: 60000 });
  return JSON.parse(stdout);
}
function extractYoutubeMeta(info) {
  return {
    title: info.title || "Video de YouTube",
    thumbnail: info.thumbnail,
    synopsis: info.description,
    duration: info.duration ? `${Math.floor(info.duration / 60)}:${String(info.duration % 60).padStart(2, '0')}` : null,
    year: info.upload_date ? info.upload_date.slice(0, 4) : null,
    genres: "YouTube",
    director: info.uploader || null,
    cast: null,
    season: null,
    episode: null,
  };
}

// --- Descarga y S3 ---
async function downloadYoutubeVideo(url, quality, dest, ctx, chatId, statusMsgId, filename) {
  const format = quality === "best" ? "bestvideo+bestaudio/best" : `bestvideo[height<=${quality}]+bestaudio/best[height<=${quality}]/best`;
  const args = ytdlpArgs(["-f", format, "--merge-output-format", "mp4", "--newline", "-o", dest, "--add-header", "User-Agent:Mozilla/5.0", url]);
  const startTime = Date.now(); const throttle = { last: 0 };
  return new Promise((resolve, reject) => {
    const child = execFile("yt-dlp", args, { maxBuffer: 64 * 1024 * 1024 });
    child.stderr.on("data", (data) => {
      const match = data.toString().match(/\[download\]\s+(\d+\.\d+)%\s+of\s+~?\s*([\d.]+)(KiB|MiB|GiB)/);
      if (match) {
        const pct = parseFloat(match[1]); const size = parseFloat(match[2]); const unit = match[3];
        const mult = unit === "KiB" ? 1024 : unit === "MiB" ? 1024**2 : 1024**3;
        const total = size * mult; const current = total * (pct / 100);
        const now = Date.now();
        if (now - throttle.last > 1500) {
          throttle.last = now;
          safeEdit(ctx, chatId, statusMsgId, renderProgress("DOWNLOADING", current, total, startTime, filename), cancelKeyboard(jobKey(chatId, statusMsgId)));
        }
      }
    });
    child.on("close", (code) => code === 0 ? resolve(dest) : reject(new Error(`yt-dlp falló (${code})`)));
    child.on("error", reject);
  });
}
async function uploadToS3(localPath, filename, ctx, chatId, statusMsgId) {
  const stat = fs.statSync(localPath); const total = stat.size; const startTime = Date.now();
  const key = `${Date.now()}_${sanitize(filename)}`; const url = `${S3_BASE_URL}/stream/${key}`;
  const throttle = { last: 0 }; let sent = 0;
  const fileStream = fs.createReadStream(localPath);
  const readable = Readable.from(fileStream);
  readable.on("data", (chunk) => {
    sent += chunk.length; const now = Date.now();
    if (now - throttle.last > 1500) {
      throttle.last = now;
      safeEdit(ctx, chatId, statusMsgId, renderProgress("UPLOADING", sent, total, startTime, filename), cancelKeyboard(jobKey(chatId, statusMsgId)));
    }
  });
  const res = await fetch(url, { method: "PUT", body: readable, duplex: "half", headers: { "Content-Type": "video/mp4" } });
  if (!res.ok) throw new Error(`S3 respondió ${res.status}`);
  return { url, size: total };
}
async function downloadDirect(url, destPath, total, ctx, statusMsgId, filename, throttle, signal) {
  const startTime = Date.now();
  const res = await fetch(url, { signal });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const contentLength = parseInt(res.headers.get("content-length") || "0", 10);
  const knownTotal = total || contentLength || 0;
  let received = 0;
  const fileStream = fs.createWriteStream(destPath);
  const readable = Readable.fromWeb(res.body);
  readable.on("data", (chunk) => {
    received += chunk.length;
    const now = Date.now();
    if (now - throttle.last < 1500) return;
    throttle.last = now;
    safeEdit(ctx, ctx.chat.id, statusMsgId, renderProgress("DOWNLOADING", received, knownTotal, startTime, filename), cancelKeyboard(jobKey(ctx.chat.id, statusMsgId)));
  });
  await pipeline(readable, fileStream);
  return { received, total: knownTotal || received };
}

// --- Tarjeta Final ---
async function sendFinalCard(ctx, chatId, statusMsgId, meta, s3Url, size, qualityLabel) {
  const kb = new InlineKeyboard().url("Descargar", s3Url);
  
  let text = `<b>${escapeHtml(meta.title)}</b>\n\n`;
  if (meta.season) text += `Temporada: ${meta.season}\n`;
  if (meta.episode) text += `Episodio: ${meta.episode}\n`;
  if (qualityLabel) text += `Incluye: ${qualityLabel}\n`;
  if (meta.genres) text += `Géneros: ${meta.genres}\n`;
  if (meta.year) text += `Estreno: ${meta.year}\n`;
  if (meta.director) text += `Director: ${meta.director}\n`;
  if (meta.duration) text += `Duración: ${meta.duration}\n`;
  if (meta.cast) text += `Reparto: ${meta.cast}\n`;
  if (meta.synopsis) text += `\nSinopsis:\n<blockquote>${escapeHtml(meta.synopsis)}</blockquote>\n`;
  text += `\nTamaño: ${formatSize(size)}`;

  if (meta.thumbnail) {
    try {
      let thumbUrl = meta.thumbnail;
      if (thumbUrl.includes("ytimg.com") || thumbUrl.includes("youtube.com")) {
        thumbUrl = thumbUrl.replace(/\.webp$/, ".jpg").replace(/\/webp\//, "/jpg/");
      }
      await ctx.api.sendPhoto(chatId, thumbUrl, { caption: text, parse_mode: "HTML", reply_markup: kb });
      await ctx.api.deleteMessage(chatId, statusMsgId).catch(() => {});
      return;
    } catch (e) {
      console.error("Error al enviar foto:", e.message);
    }
  }

  try {
    await ctx.api.editMessageText(chatId, statusMsgId, text, { parse_mode: "HTML", reply_markup: kb });
  } catch (e) {
    await ctx.api.sendMessage(chatId, text, { parse_mode: "HTML", reply_markup: kb }).catch(() => {});
  }
}

// --- Handlers ---
async function handlePicta(ctx, url) {
  const statusMsgId = (await ctx.reply(renderBox("NUVOLA", [["ORIGIN", "Picta"], ["STATUS", "Conectando..."]]))).message_id;
  try {
    await ctx.api.editMessageText(ctx.chat.id, statusMsgId, renderBox("NUVOLA", [["ORIGIN", "Picta"], ["STATUS", "Buscando video..."]]));
    const info = await getPictaInfo(url);
    
    await ctx.api.editMessageText(ctx.chat.id, statusMsgId, renderBox("NUVOLA", [["ORIGIN", "Picta"], ["STATUS", "Obteniendo calidades..."]]));
    const qualities = await getPictaQualities(url);
    
    const meta = extractPictaMeta(info);
    const kb = new InlineKeyboard();
    if (qualities.length > 0) {
      qualities.forEach((q, i) => { kb.text(`${q.height}p`, `picta_${q.id}_${q.height}`); if ((i+1)%3===0) kb.row(); });
    } else { kb.text("Descargar", `picta_best`); }
    pendingPlatform.set(jobKey(ctx.chat.id, statusMsgId), { type: 'picta', url, meta });
    await ctx.api.editMessageText(ctx.chat.id, statusMsgId, renderBox("PICTA CALIDADES", [["TITLE", `<code>${info.title}</code>`]]), { reply_markup: kb });
  } catch (err) {
    console.error("Error en handlePicta:", err);
    await ctx.api.editMessageText(ctx.chat.id, statusMsgId, renderBox("ERROR", [["CAUSE", escapeHtml(err.message)]])).catch(() => {});
  }
}

async function handleYouTube(ctx, url) {
  const statusMsgId = (await ctx.reply(renderBox("NUVOLA", [["ORIGIN", "YouTube"], ["STATUS", "Conectando..."]]))).message_id;
  try {
    const info = await getYoutubeInfo(url);
    await ctx.api.editMessageText(ctx.chat.id, statusMsgId, renderBox("NUVOLA", [["ORIGIN", "YouTube"], ["STATUS", "Obteniendo calidades..."]]));
    const qualities = await getYoutubeQualities(url);
    const meta = extractYoutubeMeta(info);
    const kb = new InlineKeyboard();
    if (qualities.length > 0) {
      qualities.forEach((q, i) => { kb.text(`${q.height}p`, `yt_${q.id}_${q.height}`); if ((i+1)%3===0) kb.row(); });
    } else { kb.text("Descargar", `yt_best`); }
    pendingPlatform.set(jobKey(ctx.chat.id, statusMsgId), { type: 'youtube', url, meta });
    await ctx.api.editMessageText(ctx.chat.id, statusMsgId, renderBox("YOUTUBE CALIDADES", [["TITLE", `<code>${info.title}</code>`]]), { reply_markup: kb });
  } catch (err) {
    console.error("Error en handleYouTube:", err);
    await ctx.api.editMessageText(ctx.chat.id, statusMsgId, renderBox("ERROR", [["CAUSE", escapeHtml(err.message)]])).catch(() => {});
  }
}

async function handleDirectLink(ctx, url, host, filename) {
  const status = await ctx.reply(renderBox("NUVOLA", [["ORIGIN", host], ["NAME", filename]]), { reply_markup: cancelKeyboard(jobKey(ctx.chat.id, 0)) });
  const key = jobKey(ctx.chat.id, status.message_id);
  await ctx.api.editMessageReplyMarkup(ctx.chat.id, status.message_id, { reply_markup: cancelKeyboard(key) }).catch(() => {});
  const dest = path.join(TMP_DIR, `${Date.now()}.bin`);
  const controller = new AbortController();
  activeJobs.set(key, controller);
  try {
    await downloadDirect(url, dest, 0, ctx, status.message_id, filename, { last: 0 }, controller.signal);
    const { url: s3Url, size } = await uploadToS3(dest, filename, ctx, ctx.chat.id, status.message_id);
    fs.unlinkSync(dest);
    const kb = new InlineKeyboard().url("Descargar", s3Url);
    await ctx.api.editMessageText(ctx.chat.id, status.message_id, renderBox("COMPLETED", [["NAME", filename], ["SIZE", formatSize(size)], ["URL", s3Url]]), { reply_markup: kb });
  } catch (err) {
    await ctx.api.editMessageText(ctx.chat.id, status.message_id, renderBox("ERROR", [["CAUSE", escapeHtml(err.message)]])).catch(() => {});
  }
}

async function handleTelegramFile(ctx) {
  const msg = ctx.message;
  const media = msg.document || msg.video || msg.audio || (msg.photo ? msg.photo[msg.photo.length - 1] : null) || msg.animation;
  if (!media) return;
  const filename = media.file_name || `file_${Date.now()}.bin`;
  const total = media.file_size || 0;
  const status = await ctx.reply(renderBox("NUVOLA", [["ORIGIN", "Telegram"], ["NAME", filename]]), { reply_markup: cancelKeyboard(jobKey(ctx.chat.id, 0)) });
  const key = jobKey(ctx.chat.id, status.message_id);
  await ctx.api.editMessageReplyMarkup(ctx.chat.id, status.message_id, { reply_markup: cancelKeyboard(key) }).catch(() => {});
  const dest = path.join(TMP_DIR, `${Date.now()}.bin`);
  const controller = new AbortController();
  activeJobs.set(key, controller);
  try {
    const file = await ctx.getFile();
    await downloadDirect(file.getUrl(), dest, total, ctx, status.message_id, filename, { last: 0 }, controller.signal);
    const { url: s3Url, size } = await uploadToS3(dest, filename, ctx, ctx.chat.id, status.message_id);
    fs.unlinkSync(dest);
    const kb = new InlineKeyboard().url("Descargar", s3Url);
    await ctx.api.editMessageText(ctx.chat.id, status.message_id, renderBox("COMPLETED", [["NAME", filename], ["SIZE", formatSize(size)], ["URL", s3Url]]), { reply_markup: kb });
  } catch (err) {
    await ctx.api.editMessageText(ctx.chat.id, status.message_id, renderBox("ERROR", [["CAUSE", escapeHtml(err.message)]])).catch(() => {});
  }
}

// --- Callbacks ---
bot.callbackQuery(/^picta_(\d+)_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Iniciando descarga..." });
  const formatId = ctx.match[1]; const height = ctx.match[2];
  const statusMsgId = ctx.callbackQuery.message.message_id;
  processPicta(ctx, statusMsgId, formatId, `${height}p`).catch(console.error);
});
bot.callbackQuery(/^picta_best$/, async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Iniciando descarga..." });
  const statusMsgId = ctx.callbackQuery.message.message_id;
  processPicta(ctx, statusMsgId, null, "Mejor").catch(console.error);
});
bot.callbackQuery(/^yt_(\d+)_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Iniciando descarga..." });
  const quality = ctx.match[1];
  const statusMsgId = ctx.callbackQuery.message.message_id;
  processYoutube(ctx, statusMsgId, quality).catch(console.error);
});
bot.callbackQuery(/^yt_best$/, async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Iniciando descarga..." });
  const statusMsgId = ctx.callbackQuery.message.message_id;
  processYoutube(ctx, statusMsgId, "best").catch(console.error);
});
bot.callbackQuery(/^cancel:(.+)$/, async (ctx) => {
  const key = ctx.match[1];
  const controller = activeJobs.get(key);
  if (controller) { controller.abort(); await ctx.answerCallbackQuery({ text: "Cancelando..." }); }
  else await ctx.answerCallbackQuery({ text: "Ya terminó", show_alert: true });
});

async function processPicta(ctx, statusMsgId, formatId, qualityLabel) {
  const key = jobKey(ctx.chat.id, statusMsgId);
  const pending = pendingPlatform.get(key);
  if (!pending || pending.type !== 'picta') return;
  pendingPlatform.delete(key);
  await ctx.api.deleteMessage(ctx.chat.id, statusMsgId).catch(() => {});
  const newStatusId = (await ctx.reply(`Iniciando descarga ${qualityLabel}...`)).message_id;
  const dest = path.join(TMP_DIR, `${Date.now()}.mp4`);
  try {
    await downloadPictaVideo(pending.url, formatId, dest, ctx, ctx.chat.id, newStatusId, pending.meta.title);
    const stat = fs.statSync(dest);
    const { url: s3Url } = await uploadToS3(dest, `${sanitize(pending.meta.title)}_${qualityLabel}.mp4`, ctx, ctx.chat.id, newStatusId);
    fs.unlinkSync(dest);
    await sendFinalCard(ctx, ctx.chat.id, newStatusId, pending.meta, s3Url, stat.size, qualityLabel);
  } catch (err) {
    await ctx.api.editMessageText(ctx.chat.id, newStatusId, `Error: ${escapeHtml(err.message)}`).catch(() => {});
  }
}
async function processYoutube(ctx, statusMsgId, quality) {
  const key = jobKey(ctx.chat.id, statusMsgId);
  const pending = pendingPlatform.get(key);
  if (!pending || pending.type !== 'youtube') return;
  pendingPlatform.delete(key);
  const label = quality === "best" ? "Mejor" : quality + "p";
  await ctx.api.deleteMessage(ctx.chat.id, statusMsgId).catch(() => {});
  const newStatusId = (await ctx.reply(`Iniciando descarga ${label}...`)).message_id;
  const dest = path.join(TMP_DIR, `${Date.now()}.mp4`);
  try {
    await downloadYoutubeVideo(pending.url, quality, dest, ctx, ctx.chat.id, newStatusId, pending.meta.title);
    const stat = fs.statSync(dest);
    const { url: s3Url } = await uploadToS3(dest, `${sanitize(pending.meta.title)}_${quality}.mp4`, ctx, ctx.chat.id, newStatusId);
    fs.unlinkSync(dest);
    await sendFinalCard(ctx, ctx.chat.id, newStatusId, pending.meta, s3Url, stat.size, label);
  } catch (err) {
    await ctx.api.editMessageText(ctx.chat.id, newStatusId, `Error: ${escapeHtml(err.message)}`).catch(() => {});
  }
}

// --- Router y Comandos ---
const URL_RE = /https?:\/\/[^\s<>"']+/i;
bot.hears(URL_RE, async (ctx) => {
  const text = ctx.message.text;
  const match = text.match(URL_RE);
  if (!match) return;
  const url = match[0].replace(/[.,;:!?)\]}'"]+$/, "");
  if (url.includes("picta.cu")) await handlePicta(ctx, url);
  else if (url.includes("youtube.com") || url.includes("youtu.be")) await handleYouTube(ctx, url);
  else await handleDirectLink(ctx, url, new URL(url).hostname, "file.bin");
});
bot.on(["message:document", "message:video", "message:audio", "message:photo", "message:animation"], async (ctx) => {
  await handleTelegramFile(ctx);
});
bot.command("start", async (ctx) => {
  await ctx.reply(renderBox("NUVOLA", [
    ["ENTRADA", "archivo · video · foto · enlace"],
    ["PICTA", "películas y series"],
    ["YOUTUBE", "videos y cortos"],
    ["OTROS", "descarga directa"],
    ["SALIDA", "URL pública en s3.todus.cu/stream"]
  ]));
});
bot.start();
console.log("Nuvola listo (con escapeHtml en errores)");
