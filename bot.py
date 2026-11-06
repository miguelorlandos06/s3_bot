import asyncio
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters,
)

BOT_TOKEN         = os.environ["BOT_TOKEN"]
S3_BASE_URL       = os.environ.get("S3_BASE_URL", "https://s3.todus.cu")
TMP_DIR           = Path(os.environ.get("TODUS_TMP", Path(__file__).parent / "tmp"))
YTDLP_COOKIES     = os.environ.get("YTDLP_COOKIES", "")
YTDLP_IMPERSONATE = os.environ.get("YTDLP_IMPERSONATE", "chrome")

PICTA_USER      = os.environ.get("PICTA_USER")
PICTA_PASS      = os.environ.get("PICTA_PASS")
PICTA_CLIENT_ID = "ebkU3YeFu3So9hesQHrS8AZjEa4v7TiYbS5QZIgO"

TMP_DIR.mkdir(parents=True, exist_ok=True)

active_jobs: dict[str, asyncio.subprocess.Process] = {}
pending_platform: dict[str, dict] = {}

def job_key(chat_id: int, message_id: int) -> str:
    return f"{chat_id}:{message_id}"

_UNITS = ["B", "KB", "MB", "GB"]

def format_size(n: float) -> str:
    if not n:
        return "0 B"
    i = 0
    while n >= 1024 and i < len(_UNITS) - 1:
        n /= 1024
        i += 1
    return f"{n:.2f} {_UNITS[i]}"

def format_eta(seconds: float) -> str:
    if seconds is None or seconds < 0 or seconds == float("inf"):
        return "?"
    s = int(round(seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m"

def progress_bar(pct: float, width: int = 15) -> str:
    c = max(0, min(100, pct))
    f = round(width * c / 100)
    return "✦" * f + "✧" * (width - f)

def escape_html(s) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def sanitize(name: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9._-]", "_", name)
    name = name.strip("_")[:120]
    return name or "file.bin"

def render_box(title: str, fields: list) -> str:
    lines = [f"┎ {title}"]
    for i, (k, v) in enumerate(fields):
        p = "┖" if i == len(fields) - 1 else "┠"
        lines.append(f"{p} {k + ': ' if k else ''}{v}")
    return "\n".join(lines)

def render_progress(phase, current, total, start_time, filename):
    pct = min(100, round(current / total * 100)) if total > 0 else 0
    bar = progress_bar(pct)
    elapsed = time.monotonic() - start_time
    speed = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / speed if speed > 0 and total > current else 0
    fields = []
    if filename:
        fields.append(("NAME", f"<code>{escape_html(filename)}</code>"))
    fields.append(("", f"[{bar}]"))
    fields.append(("PERCENT", f"{pct}%"))
    fields.append(("SIZE", f"{format_size(current)}/{format_size(total)}"))
    if speed > 1:
        fields.append(("SPEED", f"{format_size(speed)}/s"))
        fields.append(("ETA", format_eta(eta)))
    return render_box(phase, fields)

def cancel_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancelar", callback_data=f"cancel:{key}")]])

async def safe_edit(bot, chat_id, message_id, text, keyboard=None):
    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=text,
            parse_mode=ParseMode.HTML, reply_markup=keyboard,
        )
    except Exception:
        pass

def ytdlp_args(extra=None) -> list:
    args = ["yt-dlp", "--js-runtimes", "node", "--remote-components", "ejs:github"]
    if YTDLP_COOKIES and Path(YTDLP_COOKIES).exists():
        args += ["--cookies", YTDLP_COOKIES]
    if YTDLP_IMPERSONATE:
        args += ["--impersonate", YTDLP_IMPERSONATE]
    return args + (extra or [])

async def get_picta_token(session: aiohttp.ClientSession) -> str:
    form = aiohttp.FormData()
    form.add_field("grant_type", "password")
    form.add_field("client_id", PICTA_CLIENT_ID)
    form.add_field("username", PICTA_USER)
    form.add_field("password", PICTA_PASS)
    async with session.post("https://api.picta.cu/o/token/", data=form,
                            headers={"User-Agent": "Mozilla/5.0"}) as r:
        data = await r.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"Picta token inválido: {data}")
        return token

async def get_picta_api_data(session: aiohttp.ClientSession, slug: str) -> dict:
    token = await get_picta_token(session)
    async with session.get(
        "https://api.picta.cu/v2/publicacion/",
        params={"format": "json", "slug_url_raw": slug},
        headers={
            "Authorization": f"Bearer {token}",
            "X-Device-Id": "1165077",
            "Accept": "application/json",
        },
    ) as r:
        data = await r.json()
    results = data.get("results") or []
    if not results:
        raise RuntimeError("Película no encontrada en Picta.")
    return results[0]

def extract_picta_meta(video: dict) -> dict:
    cat = video.get("categoria") or {}
    pel = cat.get("pelicula") or {}
    cap = cat.get("capitulo") or {}
    info_serie = (cap.get("temporada") or {}).get("serie") or {}
    year = pel.get("ano") or info_serie.get("ano")
    genres = pel.get("genero") or info_serie.get("genero") or []
    directors = pel.get("director") or info_serie.get("director") or []
    cast = pel.get("reparto") or info_serie.get("reparto") or []
    return {
        "title": video.get("nombre"),
        "thumbnail": video.get("url_imagen"),
        "synopsis": video.get("descripcion") or info_serie.get("sinopsis"),
        "duration": video.get("duracion"),
        "year": str(year) if year else None,
        "genres": " ".join("#" + re.sub(r"\s+", "", g["nombre"]) for g in genres) or None,
        "director": ", ".join(d["nombre"] for d in directors) or None,
        "cast": ", ".join(a["nombre"] for a in cast[:5]) or None,
        "season": (cap.get("temporada") or {}).get("nombre"),
        "episode": str(cap["numero"]) if cap.get("numero") else None,
    }

async def run_capture(*cmd, timeout=60) -> str:
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError(f"{cmd[0]} timeout")
    if proc.returncode != 0:
        raise RuntimeError(f"{cmd[0]} falló ({proc.returncode}): {err.decode(errors='ignore')[:400]}")
    return out.decode(errors="ignore")

async def get_picta_qualities(manifest_url: str) -> list:
    out = await run_capture(
        "picta-dl", "-u", PICTA_USER, "-p", PICTA_PASS,
        "--no-playlist", "-F", manifest_url,
    )
    found = {}
    for line in out.splitlines():
        m = re.search(r"(dash-\d+-\d+)\s+mp4\s+(\d{3,4})x(\d{3,4})", line)
        if m:
            h = int(m.group(3))
            found[h] = {"id": m.group(1), "height": h}
    return sorted(found.values(), key=lambda x: -x["height"])

async def download_picta_video(url, format_id, dest, bot, chat_id, status_id, filename):
    args = ["picta-dl", "-u", PICTA_USER, "-p", PICTA_PASS,
            "--no-playlist", "-f", format_id, "-o", str(dest), url]
    start = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    active_jobs[job_key(chat_id, status_id)] = proc
    async def reader():
        last = 0.0
        async for raw in proc.stdout:
            m = re.search(rb"PROGRESO:(\d+)/(\d+)", raw)
            if not m:
                continue
            done, total = int(m.group(1)), int(m.group(2))
            now = time.monotonic()
            if now - last < 1.5:
                continue
            last = now
            elapsed = now - start
            speed = done / elapsed if elapsed else 0
            eta = (total - done) / speed if speed else 0
            text = render_box("DESCARGANDO", [
                ("NAME", escape_html(filename)),
                ("", f"[{progress_bar(done / total * 100)}]"),
                ("SEGMENTOS", f"{done}/{total}"),
                ("VELOCIDAD", f"{speed:.1f} seg/s"),
                ("ETA", format_eta(eta)),
            ])
            await safe_edit(bot, chat_id, status_id, text,
                            cancel_keyboard(job_key(chat_id, status_id)))
    try:
        await asyncio.gather(reader(), proc.wait())
        if proc.returncode != 0:
            raise RuntimeError(f"picta-dl falló ({proc.returncode})")
        return dest
    finally:
        active_jobs.pop(job_key(chat_id, status_id), None)

async def get_youtube_qualities(url: str) -> list:
    out = await run_capture(*ytdlp_args(["-F", url]), timeout=90)
    heights = set()
    for line in out.splitlines():
        m = re.search(r"(\d{3,4})x(\d{3,4})", line)
        if m:
            h = int(m.group(2))
            if 144 <= h <= 2160:
                heights.add(h)
    return [{"id": h, "height": h} for h in sorted(heights)]

async def get_youtube_info(url: str) -> dict:
    out = await run_capture(*ytdlp_args(["-J", "--no-playlist", url]), timeout=90)
    return json.loads(out)

def extract_youtube_meta(info: dict) -> dict:
    dur = info.get("duration")
    return {
        "title": info.get("title") or "Video de YouTube",
        "thumbnail": info.get("thumbnail"),
        "synopsis": info.get("description"),
        "duration": f"{dur // 60}:{dur % 60:02d}" if dur else None,
        "year": info["upload_date"][:4] if info.get("upload_date") else None,
        "genres": "YouTube",
        "director": info.get("uploader"),
        "cast": None,
        "season": None,
        "episode": None,
    }

async def download_youtube_video(url, quality, dest, bot, chat_id, status_id, filename):
    fmt = ("bestvideo+bestaudio/best" if quality == "best"
           else f"bestvideo[height<={quality}]+bestaudio/best[height<={quality}]/best")
    args = ytdlp_args(["-f", fmt, "--merge-output-format", "mp4",
                       "--newline", "-o", str(dest),
                       "--add-header", "User-Agent:Mozilla/5.0", url])
    start = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    active_jobs[job_key(chat_id, status_id)] = proc
    rx = re.compile(rb"\[download\]\s+(\d+\.\d+)%\s+of\s+~?\s*([\d.]+)(KiB|MiB|GiB)")
    last = 0.0
    async def reader():
        nonlocal last
        async for raw in proc.stderr:
            m = rx.search(raw)
            if not m:
                continue
            pct = float(m.group(1))
            size = float(m.group(2))
            mult = {"KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[m.group(3).decode()]
            total = size * mult
            current = total * pct / 100
            now = time.monotonic()
            if now - last < 1.5:
                continue
            last = now
            text = render_progress("DOWNLOADING", current, total, start, filename)
            await safe_edit(bot, chat_id, status_id, text,
                            cancel_keyboard(job_key(chat_id, status_id)))
    try:
        await asyncio.gather(reader(), proc.wait())
        if proc.returncode != 0:
            raise RuntimeError(f"yt-dlp falló ({proc.returncode})")
        return dest
    finally:
        active_jobs.pop(job_key(chat_id, status_id), None)

async def upload_to_s3(session, local_path, filename, bot, chat_id, status_id):
    total = local_path.stat().st_size
    start = time.monotonic()
    key = f"{int(time.time() * 1000)}_{sanitize(filename)}"
    url = f"{S3_BASE_URL}/stream/{key}"
    last = 0.0
    sent = 0
    async def gen():
        nonlocal sent, last
        with open(local_path, "rb") as f:
            while chunk := f.read(64 * 1024):
                sent += len(chunk)
                now = time.monotonic()
                if now - last > 1.5:
                    last = now
                    text = render_progress("UPLOADING", sent, total, start, filename)
                    await safe_edit(bot, chat_id, status_id, text,
                                    cancel_keyboard(job_key(chat_id, status_id)))
                yield chunk
    async with session.put(url, data=gen(), headers={"Content-Type": "video/mp4"}) as r:
        if r.status >= 400:
            raise RuntimeError(f"S3 respondió {r.status}")
    return {"url": url, "size": total}

async def download_direct(session, url, dest, total, bot, chat_id, status_id, filename, state):
    start = time.monotonic()
    async with session.get(url) as r:
        if r.status >= 400:
            raise RuntimeError(f"HTTP {r.status}")
        known = total or int(r.headers.get("content-length") or 0)
        received = 0
        with open(dest, "wb") as f:
            async for chunk in r.content.iter_chunked(64 * 1024):
                f.write(chunk)
                received += len(chunk)
                now = time.monotonic()
                if now - state["last"] < 1.5:
                    continue
                state["last"] = now
                text = render_progress("DOWNLOADING", received, known, start, filename)
                await safe_edit(bot, chat_id, status_id, text,
                                cancel_keyboard(job_key(chat_id, status_id)))
    return {"received": received, "total": known or received}

async def send_final_card(bot, chat_id, status_id, meta, s3_url, size, quality):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Descargar", url=s3_url)]])
    lines = [f"<b>{escape_html(meta['title'])}</b>", ""]
    if meta.get("season"):   lines.append(f"Temporada: {meta['season']}")
    if meta.get("episode"):  lines.append(f"Episodio: {meta['episode']}")
    if quality:              lines.append(f"Incluye: {quality}")
    if meta.get("genres"):   lines.append(f"Géneros: {meta['genres']}")
    if meta.get("year"):     lines.append(f"Estreno: {meta['year']}")
    if meta.get("director"): lines.append(f"Director: {meta['director']}")
    if meta.get("duration"): lines.append(f"Duración: {meta['duration']}")
    if meta.get("cast"):     lines.append(f"Reparto: {meta['cast']}")
    if meta.get("synopsis"):
        lines += ["", "Sinopsis:", f"<blockquote>{escape_html(meta['synopsis'])}</blockquote>"]
    lines += ["", f"Tamaño: {format_size(size)}"]
    text = "\n".join(lines)
    thumb = meta.get("thumbnail")
    if thumb:
        if "ytimg.com" in thumb or "youtube.com" in thumb:
            thumb = thumb.replace(".webp", ".jpg").replace("/webp/", "/jpg/")
        try:
            await bot.send_photo(chat_id=chat_id, photo=thumb, caption=text,
                                 parse_mode=ParseMode.HTML, reply_markup=kb)
            try:
                await bot.delete_message(chat_id, status_id)
            except Exception:
                pass
            return
        except Exception as e:
            print("send_photo falló:", e)
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=status_id,
                                    text=text, parse_mode=ParseMode.HTML,
                                    reply_markup=kb)
    except Exception:
        try:
            await bot.send_message(chat_id=chat_id, text=text,
                                   parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass

async def handle_picta(update, context, url):
    chat_id = update.effective_chat.id
    bot = context.bot
    msg = await update.message.reply_text(
        render_box("NUVOLA", [("ORIGIN", "Picta"), ("STATUS", "Conectando...")]),
        parse_mode=ParseMode.HTML,
    )
    status_id = msg.message_id
    try:
        slug = urlparse(url).path.rstrip("/").split("/")[-1]
        await safe_edit(bot, chat_id, status_id,
                        render_box("NUVOLA", [("ORIGIN", "Picta"), ("STATUS", "Buscando video...")]))
        async with aiohttp.ClientSession() as session:
            data = await get_picta_api_data(session, slug)
        meta = extract_picta_meta(data)
        await safe_edit(bot, chat_id, status_id,
                        render_box("NUVOLA", [("ORIGIN", "Picta"), ("STATUS", "Obteniendo calidades...")]))
        qualities = await get_picta_qualities(data["url_manifiesto"])
        rows, row = [], []
        if qualities:
            for q in qualities:
                row.append(InlineKeyboardButton(f"{q['height']}p",
                                                callback_data=f"picta_{q['id']}_{q['height']}"))
                if len(row) == 3:
                    rows.append(row); row = []
            if row: rows.append(row)
        else:
            rows = [[InlineKeyboardButton("Descargar", callback_data="picta_best")]]
        kb = InlineKeyboardMarkup(rows)
        pending_platform[job_key(chat_id, status_id)] = {
            "type": "picta", "url": url, "meta": meta,
            "manifest": data["url_manifiesto"], "qualities": qualities,
        }
        await safe_edit(bot, chat_id, status_id,
                        render_box("PICTA CALIDADES",
                                   [("TITLE", f"<code>{escape_html(data['nombre'])}</code>")]),
                        keyboard=kb)
    except Exception as e:
        print("handle_picta:", e)
        await safe_edit(bot, chat_id, status_id,
                        render_box("ERROR", [("CAUSE", escape_html(str(e)))]))

async def handle_youtube(update, context, url):
    chat_id = update.effective_chat.id
    bot = context.bot
    msg = await update.message.reply_text(
        render_box("NUVOLA", [("ORIGIN", "YouTube"), ("STATUS", "Conectando...")]),
        parse_mode=ParseMode.HTML,
    )
    status_id = msg.message_id
    try:
        info = await get_youtube_info(url)
        await safe_edit(bot, chat_id, status_id,
                        render_box("NUVOLA", [("ORIGIN", "YouTube"), ("STATUS", "Obteniendo calidades...")]))
        qualities = await get_youtube_qualities(url)
        meta = extract_youtube_meta(info)
        rows, row = [], []
        if qualities:
            for q in qualities:
                row.append(InlineKeyboardButton(f"{q['height']}p",
                                                callback_data=f"yt_{q['height']}"))
                if len(row) == 3:
                    rows.append(row); row = []
            if row: rows.append(row)
        else:
            rows = [[InlineKeyboardButton("Descargar", callback_data="yt_best")]]
        kb = InlineKeyboardMarkup(rows)
        pending_platform[job_key(chat_id, status_id)] = {
            "type": "youtube", "url": url, "meta": meta,
        }
        await safe_edit(bot, chat_id, status_id,
                        render_box("YOUTUBE CALIDADES",
                                   [("TITLE", f"<code>{escape_html(info['title'])}</code>")]),
                        keyboard=kb)
    except Exception as e:
        print("handle_youtube:", e)
        await safe_edit(bot, chat_id, status_id,
                        render_box("ERROR", [("CAUSE", escape_html(str(e)))]))

async def handle_direct_link(update, context, url, host, filename):
    chat_id = update.effective_chat.id
    bot = context.bot
    status = await update.message.reply_text(
        render_box("NUVOLA", [("ORIGIN", host), ("NAME", filename)]),
        parse_mode=ParseMode.HTML,
    )
    status_id = status.message_id
    key = job_key(chat_id, status_id)
    await safe_edit(bot, chat_id, status_id,
                    render_box("NUVOLA", [("ORIGIN", host), ("NAME", filename)]),
                    cancel_keyboard(key))
    dest = TMP_DIR / f"{int(time.time() * 1000)}.bin"
    try:
        async with aiohttp.ClientSession() as session:
            await download_direct(session, url, dest, 0, bot, chat_id, status_id,
                                  filename, {"last": 0.0})
            info = await upload_to_s3(session, dest, filename, bot, chat_id, status_id)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Descargar", url=info["url"])]])
        await safe_edit(bot, chat_id, status_id,
                        render_box("COMPLETED", [
                            ("NAME", filename),
                            ("SIZE", format_size(info["size"])),
                            ("URL", info["url"]),
                        ]), keyboard=kb)
    except Exception as e:
        await safe_edit(bot, chat_id, status_id,
                        render_box("ERROR", [("CAUSE", escape_html(str(e)))]))
    finally:
        dest.unlink(missing_ok=True)

async def handle_telegram_file(update, context):
    msg = update.message
    media = msg.document or msg.video or msg.audio or msg.animation or \
            (msg.photo[-1] if msg.photo else None)
    if not media:
        return
    chat_id = update.effective_chat.id
    bot = context.bot
    filename = getattr(media, "file_name", None) or f"file_{int(time.time())}.bin"
    total = getattr(media, "file_size", 0) or 0
    status = await msg.reply_text(
        render_box("NUVOLA", [("ORIGIN", "Telegram"), ("NAME", filename)]),
        parse_mode=ParseMode.HTML,
    )
    status_id = status.message_id
    key = job_key(chat_id, status_id)
    await safe_edit(bot, chat_id, status_id,
                    render_box("NUVOLA", [("ORIGIN", "Telegram"), ("NAME", filename)]),
                    cancel_keyboard(key))
    dest = TMP_DIR / f"{int(time.time() * 1000)}.bin"
    try:
        tg_file = await media.get_file()
        url = tg_file.file_path if tg_file.file_path.startswith("http") \
              else f"https://api.telegram.org/file/bot{BOT_TOKEN}/{tg_file.file_path}"
        async with aiohttp.ClientSession() as session:
            await download_direct(session, url, dest, total, bot, chat_id, status_id,
                                  filename, {"last": 0.0})
            info = await upload_to_s3(session, dest, filename, bot, chat_id, status_id)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Descargar", url=info["url"])]])
        await safe_edit(bot, chat_id, status_id,
                        render_box("COMPLETED", [
                            ("NAME", filename),
                            ("SIZE", format_size(info["size"])),
                            ("URL", info["url"]),
                        ]), keyboard=kb)
    except Exception as e:
        await safe_edit(bot, chat_id, status_id,
                        render_box("ERROR", [("CAUSE", escape_html(str(e)))]))
    finally:
        dest.unlink(missing_ok=True)

async def process_picta(context, chat_id, status_id, format_id, quality_label):
    key = job_key(chat_id, status_id)
    pending = pending_platform.pop(key, None)
    if not pending or pending["type"] != "picta":
        return
    bot = context.bot
    try:
        await bot.delete_message(chat_id, status_id)
    except Exception:
        pass
    new_msg = await bot.send_message(chat_id, f"Iniciando descarga {quality_label}...")
    new_id = new_msg.message_id
    dest = TMP_DIR / f"{int(time.time() * 1000)}.mp4"
    try:
        if not format_id:
            q = pending["qualities"]
            if not q:
                q = await get_picta_qualities(pending["manifest"])
            if not q:
                raise RuntimeError("No hay calidades disponibles")
            format_id = q[0]["id"]
            quality_label = f"{q[0]['height']}p"
        await download_picta_video(pending["manifest"], format_id, dest,
                                   bot, chat_id, new_id, pending["meta"]["title"])
        size = dest.stat().st_size
        async with aiohttp.ClientSession() as session:
            info = await upload_to_s3(
                session, dest, f"{sanitize(pending['meta']['title'])}_{quality_label}.mp4",
                bot, chat_id, new_id,
            )
        await send_final_card(bot, chat_id, new_id, pending["meta"],
                              info["url"], size, quality_label)
    except Exception as e:
        await safe_edit(bot, chat_id, new_id, f"Error: {escape_html(str(e))}")
    finally:
        dest.unlink(missing_ok=True)

async def process_youtube(context, chat_id, status_id, quality):
    key = job_key(chat_id, status_id)
    pending = pending_platform.pop(key, None)
    if not pending or pending["type"] != "youtube":
        return
    bot = context.bot
    label = "Mejor" if quality == "best" else f"{quality}p"
    try:
        await bot.delete_message(chat_id, status_id)
    except Exception:
        pass
    new_msg = await bot.send_message(chat_id, f"Iniciando descarga {label}...")
    new_id = new_msg.message_id
    dest = TMP_DIR / f"{int(time.time() * 1000)}.mp4"
    try:
        await download_youtube_video(pending["url"], quality, dest,
                                     bot, chat_id, new_id, pending["meta"]["title"])
        size = dest.stat().st_size
        async with aiohttp.ClientSession() as session:
            info = await upload_to_s3(
                session, dest, f"{sanitize(pending['meta']['title'])}_{quality}.mp4",
                bot, chat_id, new_id,
            )
        await send_final_card(bot, chat_id, new_id, pending["meta"],
                              info["url"], size, label)
    except Exception as e:
        await safe_edit(bot, chat_id, new_id, f"Error: {escape_html(str(e))}")
    finally:
        dest.unlink(missing_ok=True)

async def on_callback(update, context):
    q = update.callback_query
    data = q.data
    chat_id = q.message.chat_id
    status_id = q.message.message_id
    m = re.fullmatch(r"picta_([a-z0-9-]+)_(\d+)", data)
    if m:
        await q.answer("Iniciando descarga...")
        asyncio.create_task(process_picta(context, chat_id, status_id, m.group(1), f"{m.group(2)}p"))
        return
    if data == "picta_best":
        await q.answer("Iniciando descarga...")
        asyncio.create_task(process_picta(context, chat_id, status_id, None, "Mejor"))
        return
    m = re.fullmatch(r"yt_(\d+)", data)
    if m:
        await q.answer("Iniciando descarga...")
        asyncio.create_task(process_youtube(context, chat_id, status_id, m.group(1)))
        return
    if data == "yt_best":
        await q.answer("Iniciando descarga...")
        asyncio.create_task(process_youtube(context, chat_id, status_id, "best"))
        return
    m = re.fullmatch(r"cancel:(.+)", data)
    if m:
        key = m.group(1)
        proc = active_jobs.get(key)
        if proc:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            await q.answer("Cancelando...")
        else:
            await q.answer("Ya terminó", show_alert=True)

URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)

async def on_text(update, context):
    text = update.message.text or ""
    m = URL_RE.search(text)
    if not m:
        return
    url = re.sub(r"[.,;:!?)\]}'\"]+$", "", m.group(0))
    host = urlparse(url).hostname or "direct"
    if "picta.cu" in url:
        await handle_picta(update, context, url)
    elif "youtube.com" in url or "youtu.be" in url:
        await handle_youtube(update, context, url)
    else:
        await handle_direct_link(update, context, url, host, "file.bin")

async def on_start(update, context):
    await update.message.reply_text(
        render_box("NUVOLA", [
            ("ENTRADA", "archivo · video · foto · enlace"),
            ("PICTA", "películas y series"),
            ("YOUTUBE", "videos y cortos"),
            ("OTROS", "descarga directa"),
            ("SALIDA", "URL pública en s3.todus.cu/stream"),
        ]),
        parse_mode=ParseMode.HTML,
    )

def main():
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()
    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(MessageHandler(
        filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.PHOTO | filters.ANIMATION,
        handle_telegram_file,
    ))
    print("Nuvola listo (Python + picta-dl)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
