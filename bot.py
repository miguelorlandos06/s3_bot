import asyncio
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse
import shutil

import aiohttp
import requests as sync_requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters,
)

# --- NUEVO: DraftStreamer para actualizaciones fluidas ---
class DraftStreamer:
    def __init__(self, bot, chat_id, message_id, render_func, cancel_kb_func):
        self.bot = bot
        self.chat_id = chat_id
        self.message_id = message_id
        self.render_func = render_func
        self.cancel_kb_func = cancel_kb_func
        self.draft_id = int(time.time() * 1000) % 2147483647
        self.last_update = 0.0
        self.use_draft = True
        self.last_pct = -1

    async def update(self, phase, current, total, start_time, filename=None, force=False):
        now = time.monotonic()
        pct = int((current / total) * 100) if total > 0 else 0

        if not force and (now - self.last_update < 0.25):
            if pct == self.last_pct: return
        if pct == self.last_pct and not force: return

        self.last_update = now
        self.last_pct = pct
        text = self.render_func(phase, current, total, start_time, filename)

        if self.use_draft:
            try:
                await self.bot.api.post("sendMessageDraft", {
                    "chat_id": self.chat_id, "draft_id": self.draft_id,
                    "text": text, "parse_mode": "HTML"
                })
                return
            except Exception as e:
                err = str(e).lower()
                if "flood" in err or "not found" in err or "method" in err:
                    self.use_draft = False
                else:
                    return

        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id, message_id=self.message_id,
                text=text, parse_mode="HTML", reply_markup=self.cancel_kb_func()
            )
        except Exception: pass

BOT_TOKEN         = os.environ["BOT_TOKEN"]
S3_BASE_URL       = os.environ.get("S3_BASE_URL", "https://s3.todus.cu")
TMP_DIR           = Path(os.environ.get("TODUS_TMP", Path(__file__).parent / "tmp"))
YTDLP_COOKIES     = os.environ.get("YTDLP_COOKIES", "")
YTDLP_IMPERSONATE = os.environ.get("YTDLP_IMPERSONATE", "chrome")

PICTA_USER      = os.environ.get("PICTA_USER")
PICTA_PASS      = os.environ.get("PICTA_PASS")

TMP_DIR.mkdir(parents=True, exist_ok=True)
active_jobs: dict[str, asyncio.subprocess.Process] = {}
pending_platform: dict[str, dict] = {}

def job_key(chat_id: int, message_id: int) -> str:
    return f"{chat_id}:{message_id}"

_UNITS = ["B", "KB", "MB", "GB"]
def format_size(n: float) -> str:
    if not n: return "0 B"
    i = 0
    while n >= 1024 and i < len(_UNITS) - 1:
        n /= 1024; i += 1
    return f"{n:.2f} {_UNITS[i]}"

def format_eta(s: float) -> str:
    if not s or s < 0 or s == float("inf"): return "?"
    s = int(round(s))
    return f"{s}s" if s < 60 else (f"{s//60}m {s%60}s" if s < 3600 else f"{s//3600}h {(s%3600)//60}m")

def progress_bar(pct: float, width: int = 15) -> str:
    f = round(width * max(0, min(100, pct)) / 100)
    return "✦" * f + "✧" * (width - f)

def escape_html(s) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", name).strip("_")[:120] or "file.bin"

def render_box(title: str, fields: list) -> str:
    lines = [f"┎ {title}"]
    for i, (k, v) in enumerate(fields):
        p = "┖" if i == len(fields) - 1 else "┠"
        lines.append(f"{p} {k + ': ' if k else ''}{v}")
    return "\n".join(lines)

def render_progress(phase, current, total, start_time, filename):
    pct = min(100, round(current / total * 100)) if total > 0 else 0
    elapsed = time.monotonic() - start_time
    speed = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / speed if speed > 0 and total > current else 0
    fields = [("NAME", f"<code>{escape_html(filename)}</code>")] if filename else []
    fields += [("", f"[{progress_bar(pct)}]"), ("PERCENT", f"{pct}%"), ("SIZE", f"{format_size(current)}/{format_size(total)}")]
    if speed > 1:
        fields += [("SPEED", f"{format_size(speed)}/s"), ("ETA", format_eta(eta))]
    return render_box(phase, fields)

def cancel_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancelar", callback_data=f"cancel:{key}")]])

async def safe_edit(bot, chat_id, message_id, text, keyboard=None):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except Exception: pass

def ytdlp_args(extra=None) -> list:
    args = ["yt-dlp", "--js-runtimes", "node", "--remote-components", "ejs:github"]
    if YTDLP_COOKIES and Path(YTDLP_COOKIES).exists(): args += ["--cookies", YTDLP_COOKIES]
    if YTDLP_IMPERSONATE: args += ["--impersonate", YTDLP_IMPERSONATE]
    return args + (extra or [])

async def run_capture(*cmd, timeout=120) -> str:
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill(); raise RuntimeError(f"{cmd[0]} timeout")
    if proc.returncode != 0:
        raise RuntimeError(f"{cmd[0]} falló: {err.decode(errors='ignore')[:400]}")
    return out.decode(errors="ignore")

class StreamToS3:
    def __init__(self, response, total, callback):
        self.response = response; self.total = total; self.callback = callback
        self.received = 0; self.last_update = 0.0; self.start = time.monotonic()

    def read(self, size=-1):
        chunk = self.response.raw.read(size if size > 0 else 64 * 1024)
        if not chunk: return b""
        self.received += len(chunk)
        now = time.monotonic()
        if now - self.last_update > 0.25:
            self.last_update = now
            self.callback(self.received, self.total)
        return chunk

def pipe_to_s3_sync(source_url, s3_url, total_hint, progress_cb):
    src = sync_requests.get(source_url, stream=True, timeout=30)
    if src.status_code >= 400:
        src.close(); raise RuntimeError(f"HTTP {src.status_code} al descargar")
    total = total_hint or int(src.headers.get("content-length") or 0)
    stream_obj = StreamToS3(src, total, progress_cb)
    headers = {"Content-Type": "application/octet-stream", "Content-Length": str(total)}
    put = sync_requests.put(s3_url, data=stream_obj, headers=headers, timeout=120)
    src.close()
    if put.status_code >= 400: raise RuntimeError(f"S3 respondió {put.status_code}")
    return stream_obj.received

async def pipe_to_s3(source_url, filename, bot, chat_id, status_id, total_hint=0):
    s3_url = f"{S3_BASE_URL}/stream/{int(time.time() * 1000)}_{sanitize(filename)}"
    start = time.monotonic()
    state = {"last": 0.0, "sent": 0, "total": total_hint}

    streamer = DraftStreamer(bot, chat_id, status_id, render_progress, lambda: cancel_keyboard(job_key(chat_id, status_id)))

    def progress_cb(received, total):
        state["sent"] = received; state["total"] = total
        asyncio.create_task(streamer.update("STREAMING", received, total, start, filename))

    try:
        await asyncio.to_thread(pipe_to_s3_sync, source_url, s3_url, total_hint, progress_cb)
        await streamer.update("STREAMING", state["sent"], state["total"], start, filename, force=True)
    except Exception as e:
        raise e
    return {"url": s3_url, "size": state["sent"]}

async def handle_picta(update, context, url):
    chat_id, bot = update.effective_chat.id, context.bot
    msg = await update.message.reply_text(render_box("NUVOLA", [("ORIGIN", "Picta"), ("STATUS", "Conectando...")]), parse_mode=ParseMode.HTML)
    status_id = msg.message_id
    try:
        await safe_edit(bot, chat_id, status_id, render_box("NUVOLA", [("ORIGIN", "Picta"), ("STATUS", "Obteniendo metadatos...")]))
        out = await run_capture("picta-dlp", "-u", PICTA_USER, "-p", PICTA_PASS, "-J", url)
        data = json.loads(out)
        meta = {
            "title": data.get("title"), "thumbnail": data.get("thumbnail"),
            "synopsis": data.get("description"), "duration": data.get("duration"),
            "uploader": data.get("uploader"), "tipologia": data.get("tipologia") or "Video",
        }
        rows, row = [], []
        for f in data.get("formatos", []):
            if f["tipo"] == "video":
                h = f["resolucion"].split("x")[1]
                row.append(InlineKeyboardButton(f"{h}p", callback_data=f"picta_vid_{f['id']}_{h}"))
                if len(row) == 3: rows.append(row); row = []
        if row: rows.append(row)
        pending_platform[job_key(chat_id, status_id)] = {"type": "picta", "url": url, "meta": meta}
        info_text = render_box("PICTA INFO", [
            ("TITLE", f"<code>{escape_html(meta['title'])}</code>"), ("TIPO", meta['tipologia']),
            ("DURACION", meta['duration'] or '-'), ("UPLOADER", meta['uploader'] or '-')
        ])
        await safe_edit(bot, chat_id, status_id, info_text, keyboard=InlineKeyboardMarkup(rows))
    except Exception as e:
        await safe_edit(bot, chat_id, status_id, render_box("ERROR", [("CAUSE", escape_html(str(e)))]))

async def process_picta(context, chat_id, status_id, fmt_id, quality_label):
    key = job_key(chat_id, status_id)
    pending = pending_platform.pop(key, None)
    if not pending or pending["type"] != "picta": return
    bot = context.bot
    try: await bot.delete_message(chat_id, status_id)
    except Exception: pass
    
    new_id = (await bot.send_message(chat_id, f"Iniciando descarga {quality_label}...")).message_id
    dest_dir = TMP_DIR / f"{int(time.time() * 1000)}"
    dest_dir.mkdir()
    streamer = DraftStreamer(bot, chat_id, new_id, render_progress, lambda: cancel_keyboard(key))
    
    try:
        args = ["picta-dlp", "-u", PICTA_USER, "-p", PICTA_PASS, "-o", str(dest_dir), "-f", fmt_id, pending["url"]]
        start = time.monotonic()
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        active_jobs[key] = proc
        
        async def reader():
            async for raw in proc.stdout:
                m = re.search(rb"PROGRESO:(\d+)/(\d+)", raw)
                if m:
                    done, total = int(m.group(1)), int(m.group(2))
                    await streamer.update("DESCARGANDO", done, total, start, pending["meta"]["title"])
        
        await asyncio.gather(reader(), proc.wait())
        if proc.returncode != 0: raise RuntimeError("Falló la descarga de Picta")
        
        files = list(dest_dir.glob("*.mp4"))
        if not files: raise RuntimeError("No se encontró el archivo descargado")
        file_path = files[0]
        
        async with aiohttp.ClientSession() as session:
            info = await upload_to_s3(session, file_path, f"{sanitize(pending['meta']['title'])}_{quality_label}.mp4", bot, chat_id, new_id)
        await send_final_card(bot, chat_id, new_id, pending["meta"], info["url"], file_path.stat().st_size, quality_label)
    except Exception as e:
        await safe_edit(bot, chat_id, new_id, f"Error: {escape_html(str(e))}")
    finally:
        active_jobs.pop(key, None); shutil.rmtree(dest_dir, ignore_errors=True)

async def handle_youtube(update, context, url):
    chat_id, bot = update.effective_chat.id, context.bot
    msg = await update.message.reply_text(render_box("NUVOLA", [("ORIGIN", "YouTube"), ("STATUS", "Conectando...")]), parse_mode=ParseMode.HTML)
    status_id = msg.message_id
    try:
        info = json.loads(await run_capture(*ytdlp_args(["-J", "--no-playlist", url])))
        meta = {
            "title": info.get("title"), "thumbnail": info.get("thumbnail"),
            "synopsis": info.get("description"), "duration": f"{info.get('duration',0)//60}:{info.get('duration',0)%60:02d}" if info.get("duration") else None,
            "uploader": info.get("uploader"), "tipologia": "YouTube"
        }
        qualities = set()
        for line in (await run_capture(*ytdlp_args(["-F", url]))).splitlines():
            m = re.search(r"(\d{3,4})x(\d{3,4})", line)
            if m and 144 <= int(m.group(2)) <= 2160: qualities.add(int(m.group(2)))
        rows, row = [], []
        for h in sorted(qualities, reverse=True):
            row.append(InlineKeyboardButton(f"{h}p", callback_data=f"yt_{h}"))
            if len(row) == 3: rows.append(row); row = []
        if row: rows.append(row)
        pending_platform[job_key(chat_id, status_id)] = {"type": "youtube", "url": url, "meta": meta}
        await safe_edit(bot, chat_id, status_id, render_box("YOUTUBE INFO", [("TITLE", f"<code>{escape_html(meta['title'])}</code>"), ("DURACION", meta['duration'] or '-')]), keyboard=InlineKeyboardMarkup(rows))
    except Exception as e:
        await safe_edit(bot, chat_id, status_id, render_box("ERROR", [("CAUSE", escape_html(str(e)))]))

async def process_youtube(context, chat_id, status_id, quality):
    key = job_key(chat_id, status_id)
    pending = pending_platform.pop(key, None)
    if not pending or pending["type"] != "youtube": return
    bot = context.bot
    label = "Mejor" if quality == "best" else f"{quality}p"
    try: await bot.delete_message(chat_id, status_id)
    except Exception: pass
    new_id = (await bot.send_message(chat_id, f"Iniciando descarga {label}...")).message_id
    dest = TMP_DIR / f"{int(time.time() * 1000)}.mp4"
    streamer = DraftStreamer(bot, chat_id, new_id, render_progress, lambda: cancel_keyboard(key))
    
    fmt = "bestvideo+bestaudio/best" if quality == "best" else f"bestvideo[height<={quality}]+bestaudio/best[height<={quality}]/best"
    args = ytdlp_args(["-f", fmt, "--merge-output-format", "mp4", "--newline", "-o", str(dest), "--add-header", "User-Agent:Mozilla/5.0", pending["url"]])
    start = time.monotonic()
    proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    active_jobs[key] = proc
    rx = re.compile(rb"\[download\]\s+(\d+\.\d+)%\s+of\s+~?\s*([\d.]+)(KiB|MiB|GiB)")
    async def reader():
        async for raw in proc.stderr:
            m = rx.search(raw)
            if m:
                pct, size, unit = float(m.group(1)), float(m.group(2)), m.group(3).decode()
                total = size * {"KiB":1024, "MiB":1024**2, "GiB":1024**3}[unit]
                await streamer.update("DOWNLOADING", total*pct/100, total, start, pending["meta"]["title"])
    try:
        await asyncio.gather(reader(), proc.wait())
        if proc.returncode != 0: raise RuntimeError("yt-dlp falló")
        async with aiohttp.ClientSession() as session:
            info = await upload_to_s3(session, dest, f"{sanitize(pending['meta']['title'])}_{quality}.mp4", bot, chat_id, new_id)
        await send_final_card(bot, chat_id, new_id, pending["meta"], info["url"], dest.stat().st_size, label)
    except Exception as e:
        await safe_edit(bot, chat_id, new_id, f"Error: {escape_html(str(e))}")
    finally:
        active_jobs.pop(key, None); dest.unlink(missing_ok=True)

async def upload_to_s3(session, local_path, filename, bot, chat_id, status_id):
    total = local_path.stat().st_size
    start, last, sent = time.monotonic(), 0.0, 0
    url = f"{S3_BASE_URL}/stream/{int(time.time() * 1000)}_{sanitize(filename)}"
    streamer = DraftStreamer(bot, chat_id, status_id, render_progress, lambda: cancel_keyboard(job_key(chat_id, status_id)))
    async def gen():
        nonlocal sent, last
        with open(local_path, "rb") as f:
            while chunk := f.read(64 * 1024):
                sent += len(chunk)
                if time.monotonic() - last > 0.25:
                    last = time.monotonic()
                    await streamer.update("UPLOADING", sent, total, start, filename)
                yield chunk
    async with session.put(url, data=gen(), headers={"Content-Type": "video/mp4"}) as r:
        if r.status >= 400: raise RuntimeError(f"S3 respondió {r.status}")
    return {"url": url, "size": total}

async def send_final_card(bot, chat_id, status_id, meta, s3_url, size, quality):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Descargar", url=s3_url)]])
    lines = [f"<b>{escape_html(meta['title'])}</b>", ""]
    if quality: lines.append(f"Incluye: {quality}")
    if meta.get("tipologia"): lines.append(f"Tipo: {meta['tipologia']}")
    if meta.get("duration"): lines.append(f"Duración: {meta['duration']}")
    if meta.get("uploader"): lines.append(f"Uploader: {meta['uploader']}")
    if meta.get("synopsis"): lines += ["", f"<blockquote expandable>{escape_html(meta['synopsis'])}</blockquote>"]
    lines += ["", f"Tamaño: {format_size(size)}"]
    
    thumb = meta.get("thumbnail")
    if thumb:
        if "ytimg.com" in thumb or "youtube.com" in thumb: thumb = thumb.replace(".webp", ".jpg").replace("/webp/", "/jpg/")
        try:
            await bot.send_photo(chat_id=chat_id, photo=thumb, caption="\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=kb)
            try: await bot.delete_message(chat_id, status_id)
            except Exception: pass
            return
        except Exception: pass
    try: await bot.edit_message_text(chat_id=chat_id, message_id=status_id, text="\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception:
        try: await bot.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception: pass

async def handle_direct_link(update, context, url, host, filename):
    chat_id, bot = update.effective_chat.id, context.bot
    status = await update.message.reply_text(render_box("NUVOLA", [("ORIGIN", host), ("NAME", filename)]), parse_mode=ParseMode.HTML)
    status_id = status.message_id
    key = job_key(chat_id, status_id)
    await safe_edit(bot, chat_id, status_id, render_box("NUVOLA", [("ORIGIN", host), ("NAME", filename)]), cancel_keyboard(key))
    try:
        info = await pipe_to_s3(url, filename, bot, chat_id, status_id)
        await safe_edit(bot, chat_id, status_id, render_box("COMPLETED", [("NAME", filename), ("SIZE", format_size(info["size"])), ("URL", info["url"])]), keyboard=InlineKeyboardMarkup([[InlineKeyboardButton("Descargar", url=info["url"])]]))
    except Exception as e:
        await safe_edit(bot, chat_id, status_id, render_box("ERROR", [("CAUSE", escape_html(str(e)))]))

async def handle_telegram_file(update, context):
    msg = update.message
    media = msg.document or msg.video or msg.audio or msg.animation or (msg.photo[-1] if msg.photo else None)
    if not media: return
    chat_id, bot = update.effective_chat.id, context.bot
    filename = getattr(media, "file_name", None) or f"file_{int(time.time())}.bin"
    total = getattr(media, "file_size", 0) or 0
    status = await msg.reply_text(render_box("NUVOLA", [("ORIGIN", "Telegram"), ("NAME", filename)]), parse_mode=ParseMode.HTML)
    status_id = status.message_id
    key = job_key(chat_id, status_id)
    await safe_edit(bot, chat_id, status_id, render_box("NUVOLA", [("ORIGIN", "Telegram"), ("NAME", filename)]), cancel_keyboard(key))
    try:
        tg_file = await media.get_file()
        url = tg_file.file_path if tg_file.file_path.startswith("http") else f"https://api.telegram.org/file/bot{BOT_TOKEN}/{tg_file.file_path}"
        info = await pipe_to_s3(url, filename, bot, chat_id, status_id, total_hint=total)
        await safe_edit(bot, chat_id, status_id, render_box("COMPLETED", [("NAME", filename), ("SIZE", format_size(info["size"])), ("URL", info["url"])]), keyboard=InlineKeyboardMarkup([[InlineKeyboardButton("Descargar", url=info["url"])]]))
    except Exception as e:
        await safe_edit(bot, chat_id, status_id, render_box("ERROR", [("CAUSE", escape_html(str(e)))]))

async def on_callback(update, context):
    q = update.callback_query
    data, chat_id, status_id = q.data, q.message.chat_id, q.message.message_id
    m = re.fullmatch(r"picta_vid_([\w-]+)_(\d+)", data)
    if m: await q.answer(); asyncio.create_task(process_picta(context, chat_id, status_id, m.group(1), f"{m.group(2)}p")); return
    m = re.fullmatch(r"yt_(\d+)", data)
    if m: await q.answer(); asyncio.create_task(process_youtube(context, chat_id, status_id, m.group(1))); return
    m = re.fullmatch(r"cancel:(.+)", data)
    if m:
        proc = active_jobs.get(m.group(1))
        if proc:
            try: proc.terminate()
            except ProcessLookupError: pass
            await q.answer("Cancelando...")
        else: await q.answer("Ya terminó", show_alert=True)

URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
async def on_text(update, context):
    m = URL_RE.search(update.message.text or "")
    if not m: return
    url = re.sub(r"[.,;:!?)\]}'\"]+$", "", m.group(0))
    if "picta.cu" in url: await handle_picta(update, context, url)
    elif "youtube.com" in url or "youtu.be" in url: await handle_youtube(update, context, url)
    else: await handle_direct_link(update, context, url, urlparse(url).hostname or "direct", "file.bin")

async def on_start(update, context):
    await update.message.reply_text(render_box("NUVOLA", [("ENTRADA", "archivo · video · foto · enlace"), ("PICTA", "películas y series"), ("YOUTUBE", "videos y cortos"), ("OTROS", "descarga directa"), ("SALIDA", "URL pública en s3.todus.cu/stream")]), parse_mode=ParseMode.HTML)

def main():
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()
    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(MessageHandler(filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.PHOTO | filters.ANIMATION, handle_telegram_file))
    print("Nuvola listo (Python + picta-dlp + JSON + Streaming)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
