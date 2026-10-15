import os, re, time, uuid, signal, asyncio, logging, shutil
from html import escape as _html_escape
from urllib.parse import urlparse, unquote

import aiofiles, aiohttp
from aiohttp import web

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application, ApplicationBuilder, ContextTypes,
    CommandHandler, MessageHandler, CallbackQueryHandler, filters,
)
from telegram.request import HTTPXRequest

from todus.client import S3Client


# ============================================================
# CONFIGURACION
# ============================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

TELEGRAM_API_URL = os.environ.get(
    "TELEGRAM_API_URL", "http://localhost:8081/bot")
TELEGRAM_LOCAL_MODE = os.environ.get(
    "TELEGRAM_LOCAL_MODE", "true").lower() == "true"

_TMP_BASE = os.environ.get("TMPDIR") or os.path.expanduser("~")
DOWNLOAD_PATH = os.environ.get(
    "TODUS_TMP", os.path.join(_TMP_BASE, "todus_uploads"))

# Ruta donde telegram-bot-api descarga archivos (mismo host que el bot)
TG_BOT_API_DIR = os.environ.get(
    "TG_BOT_API_DIR", "/tmp/tg-data")

PORT = int(os.environ.get("PORT", 10000))

MAX_FILE_SIZE_URL = 5 * 1024 * 1024 * 1024
MAX_FILE_SIZE_TG  = 2 * 1024 * 1024 * 1024

QUEUE_WORKERS = 1
CHUNK_SIZE = 1024 * 1024
POSITION_POLL_INTERVAL = 3
WATCHDOG_LIFETIME = 13800
PARALLEL_URL_DOWNLOAD = True
PARALLEL_URL_PARTS = 4
PARALLEL_URL_MIN_SIZE = 50 * 1024 * 1024

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

os.makedirs(DOWNLOAD_PATH, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("bot")


# ============================================================
# HELPERS
# ============================================================

def h(text):
    return _html_escape(str(text), quote=False)


def format_size(b):
    if b < 1024: return f"{b} B"
    if b < 1048576: return f"{b / 1024:.1f} KB"
    if b < 1073741824: return f"{b / 1048576:.1f} MB"
    return f"{b / 1073741824:.2f} GB"


def speed_str(bps):
    if bps < 1024: return f"{bps:.0f} B/s"
    if bps < 1048576: return f"{bps / 1024:.1f} KB/s"
    return f"{bps / 1048576:.1f} MB/s"


def eta_str(seconds):
    if seconds < 60: return f"{int(seconds)}s"
    if seconds < 3600: return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"


def progress_bar(pct, width=16):
    filled = round(width * pct / 100)
    return "\u2726" * filled + "\u2727" * (width - filled)


def humanize_error(exc):
    s = str(exc).lower()
    if "no space left" in s: return "El servidor no tiene espacio temporal."
    if "http 403" in s or "forbidden" in s: return "El servidor rechazo la descarga (403)."
    if "http 404" in s or "not found" in s: return "La URL no existe o fue eliminada (404)."
    if "http 429" in s or "too many requests" in s: return "Servidor limitando peticiones (429)."
    if "timeout" in s or "timed out" in s: return "Tiempo de espera agotado."
    if "supera el limite" in s or "too large" in s: return "El archivo supera el limite."
    if "range" in s: return "El servidor no soporta descarga en paralelo."
    if "connection" in s or "connect" in s: return "No se pudo conectar con el origen."
    return f"Error inesperado: <code>{h(str(exc)[:150])}</code>"


PHASE_TITLES = {
    "Descargando": "DOWNLOADING",
    "Subiendo": "UPLOADING",
}


def _render_box(title, fields):
    keys = [k for k, _ in fields]
    max_key_len = max(len(k) for k in keys) if keys else 1
    lines = [f"\u2553 {title}"]
    total = len(fields)
    for i, (key, val) in enumerate(fields):
        prefix = "\u2559" if i == total - 1 else "\u2551"
        pad = " " * (max_key_len - len(key))
        val = str(val).replace("\n", " ").replace("\r", " ")
        lines.append(f"{prefix} {key}:{pad} {val}")
    return "\n".join(lines)


def render_progress(phase, current, total, tracker, filename="", extra=""):
    title = PHASE_TITLES.get(phase, phase.upper())
    pct = int(current / total * 100) if total else 0
    tracker.update(current, total)
    bar = progress_bar(pct, width=16)
    progress_val = f"<code>{bar}</code> <b>{pct}%</b>"
    fields = []
    if filename: fields.append(("NAME", f"<code>{h(filename)}</code>"))
    if extra:    fields.append(("MODE", f"<code>{extra}</code>"))
    fields.append(("SIZE", f"<code>{format_size(current)} / {format_size(total)}</code>"))
    fields.append(("PROGRESS", progress_val))
    if tracker.speed > 0:
        fields.append(("SPEED", f"<code>{speed_str(tracker.speed)}</code>"))
    eta = tracker.get_eta(current, total)
    if eta > 0:
        fields.append(("ETA", f"<code>{eta_str(eta)}</code>"))
    return _render_box(title, fields)


class ProgressTracker:
    def __init__(self):
        self.start = time.time()
        self.last_time = self.start
        self.last_bytes = 0
        self.speed = 0.0

    def update(self, current, total):
        now = time.time()
        dt = now - self.last_time
        if dt >= 0.5:
            db = current - self.last_bytes
            self.speed = db / dt if dt > 0 else 0.0
            self.last_time = now
            self.last_bytes = current

    def get_eta(self, current, total):
        if self.speed <= 0: return 0.0
        return (total - current) / self.speed

    def get_elapsed(self):
        return time.time() - self.start


URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


def extract_url(text):
    m = URL_RE.search(text.strip())
    if not m: return None
    return m.group(0).rstrip(".,;:!?)]}'\"")


def get_filename_from_url(url):
    try:
        name = os.path.basename(urlparse(url).path)
        if name and len(name) > 2:
            return unquote(name)
    except Exception:
        pass
    return None


def sanitize_filename(name):
    name = name.replace("/", "_").replace("\\", "_")
    name = re.sub(r"[\s?#&]+", "_", name)
    name = re.sub(r"[\x00-\x1f\x7f]", "_", name)
    return name.strip("._") or f"file_{int(time.time())}"


def check_disk_space(needed_bytes=0):
    try:
        free = shutil.disk_usage(DOWNLOAD_PATH).free
    except Exception:
        return
    min_free = max(512 * 1024 * 1024, int(needed_bytes * 1.1))
    if free < min_free:
        raise RuntimeError(f"Disco insuficiente: {format_size(free)} libres")


def cancel_button(uid):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Cancelar", callback_data=f"cancel:{uid}")
    ]])


class EditState:
    def __init__(self):
        self.last_sent = 0.0
        self.last_text = ""


_edit_states = {}
_edit_locks = {}


async def edit_status(bot, chat_id, msg_id, text, force=False):
    key = (chat_id, msg_id)
    state = _edit_states.setdefault(key, EditState())
    lock = _edit_locks.setdefault(key, asyncio.Lock())
    async with lock:
        now = time.time()
        if not force and now - state.last_sent < 1.2:
            return
        if text == state.last_text and not force:
            return
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id,
                text=text, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            state.last_sent = now
            state.last_text = text
        except BadRequest as e:
            if "not modified" in str(e).lower():
                state.last_text = text
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except Exception:
            pass


def _cleanup_edit_state(chat_id, msg_id):
    key = (chat_id, msg_id)
    _edit_states.pop(key, None)
    _edit_locks.pop(key, None)


async def notify_processing(bot, job):
    try:
        await bot.edit_message_text(
            chat_id=job.chat_id, message_id=job.msg_id,
            text=_render_box("PROCESSING", [("STATUS", "preparando")]),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_button(job.user_id),
        )
    except Exception:
        pass


async def subir_a_s3(temp_path, filename, size, on_progress=None):
    from todus.constants import guess_content_type

    safe_name = sanitize_filename(filename)
    remote_key = f"{uuid.uuid4().hex[:8]}_{safe_name}"
    loop = asyncio.get_running_loop()
    _last_cb = [0.0]

    def _cb(sent, total):
        now = time.time()
        if now - _last_cb[0] < 1.5 and sent < total:
            return
        _last_cb[0] = now
        if on_progress and loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(on_progress(sent, total), loop)
            def _log_exc(f):
                try: f.result()
                except Exception as e: log.warning(f"on_progress fallo: {e}")
            fut.add_done_callback(_log_exc)

    def _do_upload():
        with S3Client() as client:
            result = client.put(
                temp_path, remote_key,
                content_type=guess_content_type(filename),
                filename=filename,
                show_progress=False, inline=True,
                progress_cb=_cb if on_progress else None,
            )
            if not result["success"]:
                raise RuntimeError(result["error"])
            return client.build_url(remote_key)

    return await asyncio.to_thread(_do_upload)


class QueuedJob:
    __slots__ = ("job_id", "user_id", "chat_id", "msg_id", "kind", "url",
                 "original_name", "local_file_path", "task", "created_at",
                 "cancel_requested", "last_position")

    def __init__(self, user_id, chat_id, msg_id, kind, url=None,
                 original_name=None, local_file_path=None):
        self.job_id = uuid.uuid4().hex
        self.user_id = user_id
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.kind = kind
        self.url = url
        self.original_name = original_name
        self.local_file_path = local_file_path
        self.task = None
        self.created_at = time.time()
        self.cancel_requested = False
        self.last_position = None


class JobQueue:
    def __init__(self, n_workers):
        self.queue = asyncio.Queue()
        self.pending_order = []
        self.n_workers = n_workers
        self.workers = []
        self.user_pending = {}
        self.active_jobs = {}
        self._lock = asyncio.Lock()

    async def enqueue(self, job):
        async with self._lock:
            if job.user_id in self.user_pending:
                raise ValueError("user_already_queued")
            self.user_pending[job.user_id] = job
            self.pending_order.append(job)
            await self.queue.put(job)
            return len(self.pending_order)

    async def mark_active(self, job):
        async with self._lock:
            self.active_jobs[job.job_id] = job

    async def finish(self, job):
        async with self._lock:
            self.active_jobs.pop(job.job_id, None)
            self.pending_order = [j for j in self.pending_order if j.job_id != job.job_id]
            current = self.user_pending.get(job.user_id)
            if current is not None and current.job_id == job.job_id:
                self.user_pending.pop(job.user_id, None)
        _cleanup_edit_state(job.chat_id, job.msg_id)

    async def position_of(self, user_id):
        async with self._lock:
            job = self.user_pending.get(user_id)
            if job is None:
                return None
            if job.job_id in self.active_jobs:
                return 0
            for i, queued in enumerate(self.pending_order):
                if queued.job_id == job.job_id:
                    return i + 1
            return None

    def has_user_job(self, user_id):
        return user_id in self.user_pending

    async def cancel_user(self, user_id):
        async with self._lock:
            job = self.user_pending.get(user_id)
            if job is None:
                return "none"
            if job.job_id in self.active_jobs:
                job.cancel_requested = True
                task = job.task
                if task is not None and not task.done():
                    task.cancel()
                return "active"
            self.user_pending.pop(user_id, None)
            self.pending_order = [j for j in self.pending_order if j.job_id != job.job_id]
            return "queued"

    @property
    def queued_count(self):
        return len(self.pending_order)

    @property
    def active_count(self):
        return len(self.active_jobs)

    def start_workers(self, application, process_fn):
        for i in range(self.n_workers):
            self.workers.append(
                asyncio.create_task(self._worker_loop(i, application, process_fn)))
        self.workers.append(
            asyncio.create_task(self._notify_position_changes(application)))

    async def _worker_loop(self, worker_id, application, process_fn):
        while True:
            job = await self.queue.get()
            try:
                if job.cancel_requested:
                    continue
                if job.job_id not in [j.job_id for j in self.pending_order]:
                    continue
                await self.mark_active(job)
                await notify_processing(application.bot, job)
                job.task = asyncio.create_task(process_fn(application, job))
                try:
                    await job.task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log.exception(f"job {job.job_id} fallo: {e}")
            except asyncio.CancelledError:
                raise
            finally:
                await self.finish(job)
                self.queue.task_done()

    async def _notify_position_changes(self, application):
        while True:
            try:
                async with self._lock:
                    snapshot = list(self.pending_order)
                    active_ids = set(self.active_jobs.keys())
                for i, job in enumerate(snapshot):
                    if job.job_id in active_ids:
                        continue
                    pos = i + 1
                    if job.last_position == pos:
                        continue
                    job.last_position = pos
                    if pos == 1:
                        text = _render_box("NEXT", [("STATUS", "preparando")])
                    else:
                        text = _render_box("QUEUE", [
                            ("POSITION", f"#{pos}"),
                            ("AHEAD", f"{pos - 1} trabajos"),
                            ("ESTIMATE", "~4m"),
                        ])
                    try:
                        await application.bot.edit_message_text(
                            chat_id=job.chat_id, message_id=job.msg_id,
                            text=text, parse_mode=ParseMode.HTML,
                            reply_markup=cancel_button(job.user_id),
                        )
                    except Exception:
                        pass
            except Exception as e:
                log.exception(f"_notify_position_changes: {e}")
            await asyncio.sleep(POSITION_POLL_INTERVAL)

    async def stop_workers(self):
        for w in self.workers:
            w.cancel()
        for w in self.workers:
            try: await w
            except asyncio.CancelledError: pass
        self.workers.clear()


job_queue = JobQueue(QUEUE_WORKERS)


# ============================================================
# DESCARGA
# ============================================================

class RangeNotSupported(RuntimeError):
    pass


async def _download_sequential(session, url, temp_path, total, bot, job, filename):
    downloaded = 0
    last_pct = -1
    tracker = ProgressTracker()
    async with aiofiles.open(temp_path, "wb") as f:
        async with session.get(url, headers=BROWSER_HEADERS) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}")
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                await f.write(chunk)
                downloaded += len(chunk)
                if downloaded > MAX_FILE_SIZE_URL:
                    raise RuntimeError("Archivo supera el limite")
                if total:
                    pct = int(downloaded / total * 100)
                    if pct - last_pct >= 5 or pct == 100:
                        last_pct = pct
                        await edit_status(bot, job.chat_id, job.msg_id,
                            render_progress("Descargando", downloaded, total, tracker, filename))
    return downloaded


async def _download_parallel(session, url, temp_path, total, bot, job, filename):
    n_parts = PARALLEL_URL_PARTS
    part_size = total // n_parts
    parts = []
    for i in range(n_parts):
        start = i * part_size
        end = start + part_size - 1 if i < n_parts - 1 else total - 1
        parts.append((i, start, end))

    progress = {"total": 0}
    progress_lock = asyncio.Lock()
    last_pct = [-1]
    tracker = ProgressTracker()

    async def download_part(idx, start, end):
        path = f"{temp_path}.part{idx}"
        headers = dict(BROWSER_HEADERS)
        headers["Range"] = f"bytes={start}-{end}"
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                raise RangeNotSupported()
            if resp.status != 206:
                raise RuntimeError(f"HTTP {resp.status} en parte {idx}")
            async with aiofiles.open(path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    await f.write(chunk)
                    should_update = False
                    async with progress_lock:
                        progress["total"] += len(chunk)
                        pct = int(progress["total"] / total * 100)
                        if pct - last_pct[0] >= 5 or pct == 100:
                            last_pct[0] = pct
                            should_update = True
                    if should_update:
                        await edit_status(bot, job.chat_id, job.msg_id,
                            render_progress("Descargando", progress["total"], total,
                                            tracker, filename, extra=f"paralelo x{n_parts}"))
        return path

    part_paths = await asyncio.gather(*[download_part(i, s, e) for i, s, e in parts])

    async with aiofiles.open(temp_path, "wb") as out:
        for p in part_paths:
            async with aiofiles.open(p, "rb") as src:
                while True:
                    chunk = await src.read(8 * 1024 * 1024)
                    if not chunk: break
                    await out.write(chunk)
            try: os.unlink(p)
            except Exception: pass
    return total


# ============================================================
# PROCESAMIENTO
# ============================================================

async def process_job(application, job):
    if job.kind == "url":
        await _process_url(application, job)
    else:
        await _process_file(application, job)


def _success_message(filename, size, elapsed, upload_url):
    return _render_box("DONE", [
        ("NAME", f"<code>{h(filename)}</code>"),
        ("SIZE", f"<code>{format_size(size)}</code>"),
        ("TIME", f"<code>{eta_str(elapsed)}</code>"),
        ("URL",  f'<a href="{upload_url}">{h(upload_url)}</a>'),
    ])


def _error_message(filename, exc):
    return _render_box("ERROR", [
        ("NAME",  f"<code>{h(filename)}</code>"),
        ("CAUSE", humanize_error(exc)),
    ])


def _cancel_message(filename):
    return _render_box("CANCELLED", [
        ("NAME",   f"<code>{h(filename)}</code>"),
        ("REASON", "solicitado por ti"),
    ])


async def _process_url(application, job):
    bot = application.bot
    url = job.url
    filename = sanitize_filename(get_filename_from_url(url) or f"file_{int(time.time())}")
    ext = os.path.splitext(filename)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    try:
        check_disk_space()
        async with aiohttp.ClientSession() as session:
            async with asyncio.timeout(14400):
                total = 0
                accepts_ranges = False
                try:
                    async with session.head(url, headers=BROWSER_HEADERS,
                                            timeout=aiohttp.ClientTimeout(total=30)) as hp:
                        if hp.status < 400:
                            total = int(hp.headers.get("Content-Length", 0))
                            accepts_ranges = hp.headers.get("Accept-Ranges", "").lower() == "bytes"
                except Exception:
                    pass

                if total and total > MAX_FILE_SIZE_URL:
                    raise RuntimeError(f"Archivo {format_size(total)} supera el limite")
                check_disk_space(total)

                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        if (PARALLEL_URL_DOWNLOAD and accepts_ranges
                                and total >= PARALLEL_URL_MIN_SIZE):
                            await _download_parallel(session, url, temp_path, total, bot, job, filename)
                        else:
                            await _download_sequential(session, url, temp_path, total, bot, job, filename)
                        break
                    except RangeNotSupported:
                        log.warning("paralelo no soportado, reintentando secuencial")
                        accepts_ranges = False
                        try: os.unlink(temp_path)
                        except Exception: pass
                        continue
                    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as e:
                        if attempt < max_retries - 1:
                            log.warning(f"Descarga fallo (intento {attempt + 1}): {e}")
                            try: os.unlink(temp_path)
                            except Exception: pass
                            await asyncio.sleep(2 ** attempt)
                        else:
                            raise

        size = os.path.getsize(temp_path)
        tracker = ProgressTracker()
        await edit_status(bot, job.chat_id, job.msg_id,
                          render_progress("Subiendo", 0, size, tracker, filename),
                          force=True)

        async def on_up(sent, total):
            await edit_status(bot, job.chat_id, job.msg_id,
                              render_progress("Subiendo", sent, total, tracker, filename))

        upload_url = await subir_a_s3(temp_path, filename, size, on_up)

        await bot.edit_message_text(
            chat_id=job.chat_id, message_id=job.msg_id,
            text=_success_message(filename, size, tracker.get_elapsed(), upload_url),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Descargar", url=upload_url)
            ]]),
        )

    except asyncio.CancelledError:
        try:
            await bot.edit_message_text(chat_id=job.chat_id, message_id=job.msg_id,
                text=_cancel_message(filename), parse_mode=ParseMode.HTML)
        except Exception: pass
        raise
    except Exception as e:
        log.exception("error procesando URL")
        try:
            await bot.edit_message_text(chat_id=job.chat_id, message_id=job.msg_id,
                text=_error_message(filename, e), parse_mode=ParseMode.HTML)
        except Exception: pass
    finally:
        try: os.unlink(temp_path)
        except Exception: pass


async def _process_file(application, job):
    bot = application.bot
    original_name = sanitize_filename(job.original_name or f"file_{int(time.time())}")
    src_path = job.local_file_path

    if not src_path or not os.path.isfile(src_path):
        try:
            await bot.edit_message_text(chat_id=job.chat_id, message_id=job.msg_id,
                text=_error_message(original_name, RuntimeError("archivo local no encontrado")),
                parse_mode=ParseMode.HTML)
        except Exception: pass
        return

    try:
        size = os.path.getsize(src_path)
        tracker = ProgressTracker()
        await edit_status(bot, job.chat_id, job.msg_id,
                          render_progress("Subiendo", 0, size, tracker, original_name),
                          force=True)

        async def on_up(sent, total):
            await edit_status(bot, job.chat_id, job.msg_id,
                              render_progress("Subiendo", sent, total, tracker, original_name))

        upload_url = await subir_a_s3(src_path, original_name, size, on_up)

        await bot.edit_message_text(
            chat_id=job.chat_id, message_id=job.msg_id,
            text=_success_message(original_name, size, tracker.get_elapsed(), upload_url),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Descargar", url=upload_url)
            ]]),
        )

    except asyncio.CancelledError:
        try:
            await bot.edit_message_text(chat_id=job.chat_id, message_id=job.msg_id,
                text=_cancel_message(original_name), parse_mode=ParseMode.HTML)
        except Exception: pass
        raise
    except Exception as e:
        log.exception("error procesando archivo")
        try:
            await bot.edit_message_text(chat_id=job.chat_id, message_id=job.msg_id,
                text=_error_message(original_name, e), parse_mode=ParseMode.HTML)
        except Exception: pass
    finally:
        # Borrar el archivo del server local tras subirlo a S3 (es efimero)
        if src_path and os.path.isfile(src_path):
            try:
                os.unlink(src_path)
                log.info(f"borrado temporal: {src_path}")
            except Exception as e:
                log.warning(f"no se pudo borrar {src_path}: {e}")


# ============================================================
# HANDLERS
# ============================================================

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        _render_box("TODUS UPLOADER", [
            ("INFO",  "sube archivos y URLs a s3.todus.cu/stream"),
            ("USO",   "enviame un archivo (hasta 2 GB) o un enlace directo (hasta 5 GB)"),
            ("NOTA",  "solo enlaces directos. No soporto YouTube, Mega ni similares."),
        ])
        + "\n\n"
        + _render_box("COMANDOS", [
            ("/start",  "este mensaje"),
            ("/status", "ver tu posicion en la cola"),
            ("/cancel", "cancelar tu trabajo actual"),
        ]),
        parse_mode=ParseMode.HTML,
    )


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    result = await job_queue.cancel_user(uid)
    if result == "none":
        text = _render_box("CANCEL", [("INFO", "no tienes nada en curso ni en cola.")])
    elif result == "active":
        text = _render_box("CANCELLING", [("STATUS", "esperando confirmacion del proceso activo")])
    else:
        text = _render_box("CANCELLED", [("STATUS", "tu trabajo fue retirado de la cola")])
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def on_cancel_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = int(q.data.split(":")[1])
    if q.from_user.id != uid:
        await q.answer("Solo el dueno puede cancelar.", show_alert=True)
        return
    result = await job_queue.cancel_user(uid)
    if result == "none":
        await q.answer("No tienes trabajos.", show_alert=True)
    elif result == "active":
        await q.answer("Cancelando...")
    else:
        await q.answer("Removido de la cola.")


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    pos = await job_queue.position_of(uid)
    tq = job_queue.queued_count
    ta = job_queue.active_count

    if pos is None:
        text = _render_box("STATUS", [
            ("INFO",  "enviame un enlace o un archivo para empezar."),
            ("QUEUE", f"{tq} esperando, {ta} procesando"),
        ])
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    elif pos == 0:
        text = _render_box("STATUS", [
            ("STATE", "procesandose ahora mismo"),
            ("QUEUE", f"{tq} esperando, {ta} procesando"),
        ])
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    else:
        text = _render_box("QUEUE", [
            ("POSITION", f"#{pos}"),
            ("AHEAD",    f"{pos - 1} trabajos"),
            ("QUEUE",    f"{tq} esperando, {ta} procesando"),
        ])
        await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                        reply_markup=cancel_button(uid))


async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    message = update.message
    url = extract_url(message.text)
    if not url:
        await message.reply_text(
            _render_box("ERROR", [
                ("INFO", f"enviame un enlace directo o un archivo (maximo {format_size(MAX_FILE_SIZE_URL)})."),
            ]),
            parse_mode=ParseMode.HTML,
        )
        return

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        await message.reply_text("Solo acepto http/https.", parse_mode=ParseMode.HTML)
        return

    try:
        import ipaddress, socket
        ip = socket.gethostbyname(parsed.hostname)
        if ipaddress.ip_address(ip).is_private:
            await message.reply_text("URL privada no permitida.", parse_mode=ParseMode.HTML)
            return
    except Exception:
        pass

    if job_queue.has_user_job(uid):
        await message.reply_text(
            _render_box("WAIT", [("INFO", "ya tienes un trabajo en curso. Espera o usa /cancel.")]),
            parse_mode=ParseMode.HTML,
        )
        return

    status = await message.reply_text(
        _render_box("RECEIVED", [
            ("ORIGIN", f"<code>{h(parsed.netloc or 'origen')}</code>"),
            ("INFO",   "anadiendo a la cola..."),
        ]),
        parse_mode=ParseMode.HTML,
    )
    job = QueuedJob(user_id=uid, chat_id=message.chat.id, msg_id=status.message_id,
                    kind="url", url=url)
    try:
        position = await job_queue.enqueue(job)
    except ValueError:
        await status.edit_text(_render_box("WAIT", [("INFO", "ya tienes un trabajo en curso.")]),
                               parse_mode=ParseMode.HTML)
        return

    if position == 1:
        await status.edit_text(_render_box("NEXT", [("STATUS", "preparando")]),
                               parse_mode=ParseMode.HTML, reply_markup=cancel_button(uid))
    else:
        await status.edit_text(
            _render_box("QUEUE", [("POSITION", f"#{position}"), ("AHEAD", f"{position - 1} trabajos")]),
            parse_mode=ParseMode.HTML, reply_markup=cancel_button(uid))


async def handle_media(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    message = update.message

    media = None
    original_name = None
    file_size = 0

    if message.document:
        media = message.document
        original_name = media.file_name
        file_size = media.file_size or 0
    elif message.video:
        media = message.video
        original_name = media.file_name or f"video_{int(time.time())}.mp4"
        file_size = media.file_size or 0
    elif message.audio:
        media = message.audio
        original_name = media.file_name or f"audio_{int(time.time())}.mp3"
        file_size = media.file_size or 0
    elif message.voice:
        media = message.voice
        original_name = f"voice_{int(time.time())}.ogg"
        file_size = media.file_size or 0
    elif message.video_note:
        media = message.video_note
        original_name = f"video_note_{int(time.time())}.mp4"
        file_size = media.file_size or 0
    elif message.animation:
        media = message.animation
        original_name = media.file_name or f"animation_{int(time.time())}.mp4"
        file_size = media.file_size or 0
    elif message.sticker:
        media = message.sticker
        ext = ".tgs" if media.is_animated else (".webm" if media.is_video else ".webp")
        original_name = f"sticker_{int(time.time())}{ext}"
        file_size = media.file_size or 0
    elif message.photo:
        media = message.photo[-1]
        original_name = f"photo_{int(time.time())}.jpg"
        file_size = media.file_size or 0

    if media is None:
        return

    if file_size and file_size > MAX_FILE_SIZE_TG:
        await message.reply_text(
            _render_box("ERROR", [
                ("INFO", f"archivo demasiado grande: {format_size(file_size)} > 2 GB"),
            ]),
            parse_mode=ParseMode.HTML,
        )
        return

    if job_queue.has_user_job(uid):
        await message.reply_text(
            _render_box("WAIT", [("INFO", "ya tienes un trabajo en curso. Espera o usa /cancel.")]),
            parse_mode=ParseMode.HTML,
        )
        return

    # Obtener ruta local
    local_file_path = getattr(media, "file_path", None)
    if not local_file_path and TELEGRAM_LOCAL_MODE:
        try:
            tg_file = await ctx.bot.get_file(media.file_id)
            local_file_path = tg_file.file_path
        except Exception as e:
            log.warning(f"get_file fallo: {e}")

    fields = [("NAME", f"<code>{h(original_name)}</code>")]
    if file_size: fields.append(("SIZE", f"<code>{format_size(file_size)}</code>"))
    fields.append(("INFO", "anadiendo a la cola..."))

    status = await message.reply_text(_render_box("RECEIVED", fields), parse_mode=ParseMode.HTML)
    job = QueuedJob(user_id=uid, chat_id=message.chat.id, msg_id=status.message_id,
                    kind="file", original_name=original_name,
                    local_file_path=local_file_path)
    try:
        position = await job_queue.enqueue(job)
    except ValueError:
        await status.edit_text(_render_box("WAIT", [("INFO", "ya tienes un trabajo en curso.")]),
                               parse_mode=ParseMode.HTML)
        return

    if position == 1:
        await status.edit_text(_render_box("NEXT", [("STATUS", "preparando")]),
                               parse_mode=ParseMode.HTML, reply_markup=cancel_button(uid))
    else:
        await status.edit_text(
            _render_box("QUEUE", [("POSITION", f"#{position}"), ("AHEAD", f"{position - 1} trabajos")]),
            parse_mode=ParseMode.HTML, reply_markup=cancel_button(uid))


# ============================================================
# HEALTH SERVER
# ============================================================

START_TIME = time.time()

async def health_handler(request):
    return web.json_response({
        "status": "healthy",
        "uptime": round(time.time() - START_TIME, 1),
        "queue_queued": job_queue.queued_count,
        "queue_active": job_queue.active_count,
        "local_mode": TELEGRAM_LOCAL_MODE,
        "api_url": TELEGRAM_API_URL,
    })

async def root_handler(request):
    return web.json_response({"status": "online"})

def make_web_app():
    a = web.Application()
    a.router.add_get("/", root_handler)
    a.router.add_get("/health", health_handler)
    return a

async def run_web():
    a = make_web_app()
    runner = web.AppRunner(a)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    return runner


# ============================================================
# WATCHDOG
# ============================================================

async def watchdog():
    while True:
        await asyncio.sleep(30)
        uptime = time.time() - START_TIME
        if uptime > WATCHDOG_LIFETIME:
            log.warning(f"watchdog: {uptime:.0f}s, SIGTERM")
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.sleep(10)
            log.error("watchdog: forzando os._exit")
            os._exit(0)


# ============================================================
# ARRANQUE
# ============================================================

async def post_init(application: Application):
    await application.bot.set_my_commands([
        BotCommand("start", "este mensaje"),
        BotCommand("status", "ver tu posicion en la cola"),
        BotCommand("cancel", "cancelar tu trabajo actual"),
    ])


async def main_async():
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=3600,
        write_timeout=3600,
        connect_timeout=30,
    )

    builder = ApplicationBuilder().token(BOT_TOKEN).request(request)

    if TELEGRAM_LOCAL_MODE:
        builder = builder.base_url(TELEGRAM_API_URL).local_mode(True)
        log.info(f"LOCAL MODE activo -> {TELEGRAM_API_URL}")

    application = builder.post_init(post_init).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CallbackQueryHandler(on_cancel_callback, pattern=r"^cancel:"))
    application.add_handler(MessageHandler(
        (filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.VOICE |
         filters.VIDEO_NOTE | filters.ANIMATION | filters.Sticker.ALL | filters.PHOTO),
        handle_media,
    ))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    web_runner = await run_web()

    await application.initialize()
    await application.start()
    await application.updater.start_polling(
        poll_interval=1.0,
        timeout=30,
        drop_pending_updates=True,
    )

    job_queue.start_workers(application, process_job)
    asyncio.create_task(watchdog())

    log.info(
        f"BOT READY - PTB | {QUEUE_WORKERS} workers | "
        f"url<=5GB tg<=2GB | "
        f"watchdog {WATCHDOG_LIFETIME}s | "
        f"local_mode={TELEGRAM_LOCAL_MODE} | tmp={DOWNLOAD_PATH}"
    )

    try:
        await asyncio.Event().wait()
    finally:
        log.info("apagando limpiamente...")
        await job_queue.stop_workers()
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        await web_runner.cleanup()
        log.info("apagado completo")


def main():
    try:
        asyncio.run(main_async())
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
