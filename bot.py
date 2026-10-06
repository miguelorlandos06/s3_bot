"""Bot de subida a Todus S3 / Catbox / Litterbox - GitHub Actions Runner."""
import os, re, time, uuid, signal, asyncio, logging
from urllib.parse import urlparse, unquote, quote

import aiofiles, aiohttp, aioboto3
from aiohttp import web
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]

S3_ENDPOINT = "https://s3.todus.cu"
S3_BUCKET = "stream"
S3_REGION = "us-east-1"
LITTERBOX_URL = "https://litterbox.catbox.moe/resources/internals/api.php"

DOWNLOAD_PATH = "/tmp/todus_uploads"
PORT = 10000
MAX_FILE_SIZE = 2000 * 1024 * 1024
QUEUE_WORKERS = 1
CHUNK_SIZE = 16 * 1024 * 1024
POSITION_POLL_INTERVAL = 3
WATCHDOG_LIFETIME = 14100
PARALLEL_URL_DOWNLOAD = True
PARALLEL_URL_PARTS = 4
PARALLEL_URL_MIN_SIZE = 50 * 1024 * 1024

LITTERBOX_MAX = 1024 * 1024 * 1024
TODUS_MAX = 2000 * 1024 * 1024
BANNED_EXTENSIONS = {".exe", ".scr", ".cpl", ".jar", ".doc", ".docx", ".docm"}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

os.makedirs(DOWNLOAD_PATH, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
log = logging.getLogger("bot")

app = Client("todus_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, session_string=SESSION_STRING, in_memory=True, workers=8, sleep_threshold=60)

_S3_CONFIG = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=50,
    connect_timeout=30,
    read_timeout=600,
    s3={"addressing_style": "path"},
)
_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=16,
    use_threads=True,
)
_s3_session = aioboto3.Session()

_pending_cloud = {}

class QueuedJob:
    __slots__ = ("job_id","user_id","chat_id","msg_id","kind","url","original_name","original_msg_id","task","created_at","cancel_requested")
    def __init__(self, user_id, chat_id, msg_id, kind, url=None, original_name=None, original_msg_id=None):
        self.job_id = uuid.uuid4().hex
        self.user_id = user_id
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.kind = kind
        self.url = url
        self.original_name = original_name
        self.original_msg_id = original_msg_id
        self.task = None
        self.created_at = time.time()
        self.cancel_requested = False

class JobQueue:
    def __init__(self, n_workers):
        self.queue = asyncio.Queue()
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
            await self.queue.put(job)
            return self.queue.qsize()

    async def mark_active(self, job):
        async with self._lock:
            self.active_jobs[job.job_id] = job

    async def finish(self, job):
        async with self._lock:
            self.active_jobs.pop(job.job_id, None)
            current = self.user_pending.get(job.user_id)
            if current is not None and current.job_id == job.job_id:
                self.user_pending.pop(job.user_id, None)

    def position_of(self, user_id):
        job = self.user_pending.get(user_id)
        if job is None:
            return None
        if job.job_id in self.active_jobs:
            return 0
        for i, queued in enumerate(self.queue._queue):
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
            items = list(self.queue._queue)
            self.queue._queue.clear()
            for item in items:
                if item.job_id != job.job_id:
                    self.queue._queue.append(item)
            return "queued"

    @property
    def queued_count(self):
        return self.queue.qsize()

    @property
    def active_count(self):
        return len(self.active_jobs)

    def start_workers(self, process_fn):
        for i in range(self.n_workers):
            self.workers.append(asyncio.create_task(self._worker_loop(i, process_fn)))
        self.workers.append(asyncio.create_task(self._notify_position_changes()))
        self.workers.append(asyncio.create_task(self._cleanup_pending()))

    async def _worker_loop(self, worker_id, process_fn):
        while True:
            job = await self.queue.get()
            try:
                if job.cancel_requested:
                    continue
                await self.mark_active(job)
                await notify_processing(job)
                job.task = asyncio.create_task(process_fn(job))
                try:
                    await job.task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log.exception(f"job {job.job_id} falló: {e}")
            except asyncio.CancelledError:
                raise
            finally:
                await self.finish(job)
                self.queue.task_done()

    async def _notify_position_changes(self):
        last_positions = {}
        while True:
            try:
                current = {}
                for i, job in enumerate(self.queue._queue):
                    if job.job_id in self.active_jobs:
                        continue
                    current[job.user_id] = i + 1
                for uid, pos in current.items():
                    if last_positions.get(uid) == pos:
                        continue
                    job = self.user_pending.get(uid)
                    if not job:
                        continue
                    try:
                        await app.edit_message_text(
                            job.chat_id, job.msg_id,
                            f"⏳ En cola — posición #{pos}\nEsperando turno... ({pos - 1} delante de ti)",
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton("❌ Cancelar", callback_data=f"cancel:{uid}")
                            ]]),
                        )
                    except Exception:
                        pass
                last_positions = dict(current)
            except Exception as e:
                log.exception(f"_notify_position_changes: {e}")
            await asyncio.sleep(POSITION_POLL_INTERVAL)

    async def _cleanup_pending(self):
        while True:
            await asyncio.sleep(300)
            now = time.time()
            expired = [uid for uid, p in _pending_cloud.items() if now - p.get("created_at", 0) > 600]
            for uid in expired:
                p = _pending_cloud.pop(uid, None)
                if p:
                    try:
                        os.unlink(p["temp_path"])
                    except Exception:
                        pass

    async def stop_workers(self):
        for w in self.workers:
            w.cancel()
        for w in self.workers:
            try:
                await w
            except asyncio.CancelledError:
                pass
        self.workers.clear()

job_queue = JobQueue(QUEUE_WORKERS)

def format_size(b):
    if b < 1024: return f"{b} B"
    if b < 1048576: return f"{b / 1024:.1f} KB"
    if b < 1073741824: return f"{b / 1048576:.1f} MB"
    return f"{b / 1073741824:.2f} GB"

def progress_bar(p):
    filled = round(15 * p / 100)
    return "⬢" * filled + "⬡" * (15 - filled)

URL_RE = re.compile(r"(https?://[^\s<>\"']+?)(?=[.,;:!?)\]]?(\s|$))", re.IGNORECASE)

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
    return name.strip("._") or f"file_{int(time.time())}"

def check_disk_space():
    st = os.statvfs(DOWNLOAD_PATH)
    free = st.f_bavail * st.f_frsize
    min_free = 2 * 1024 * 1024 * 1024
    if free < min_free:
        raise RuntimeError(f"Disco insuficiente: {format_size(free)} libres")

def cancel_button(uid):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Cancelar", callback_data=f"cancel:{uid}")
    ]])

class EditState:
    def __init__(self):
        self.last_sent = 0.0
        self.last_text = ""

_edit_states = {}
_edit_locks = {}

async def edit_status(client, chat_id, msg_id, text, force=False):
    state = _edit_states.setdefault(chat_id, EditState())
    lock = _edit_locks.setdefault(chat_id, asyncio.Lock())
    async with lock:
        now = time.time()
        if not force and now - state.last_sent < 1.2:
            return
        if text == state.last_text and not force:
            return
        try:
            await client.edit_message_text(chat_id, msg_id, text)
            state.last_sent = now
            state.last_text = text
        except Exception:
            pass

async def notify_processing(job):
    try:
        await app.edit_message_text(job.chat_id, job.msg_id, "▶️ Procesando...", reply_markup=cancel_button(job.user_id))
    except Exception:
        pass

async def subir_a_s3(temp_path, filename, size, on_progress=None):
    safe_name = sanitize_filename(filename)
    remote_key = f"{uuid.uuid4().hex[:8]}_{safe_name}"
    loop = asyncio.get_running_loop()
    last_update = [0.0]

    def _progress_callback(bytes_transferred):
        if on_progress is None: return
        now = loop.time()
        if now - last_update[0] < 0.5 and bytes_transferred < size: return
        last_update[0] = now
        asyncio.run_coroutine_threadsafe(on_progress(bytes_transferred, size), loop)

    async with _s3_session.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id="public", aws_secret_access_key="public", region_name=S3_REGION, config=_S3_CONFIG) as s3:
        with open(temp_path, "rb") as f:
            await s3.upload_fileobj(f, S3_BUCKET, remote_key, ExtraArgs={"ContentType": "application/octet-stream"}, Config=_TRANSFER_CONFIG, Callback=_progress_callback)
    return f"{S3_ENDPOINT}/{S3_BUCKET}/{quote(remote_key)}"

async def subir_a_litterbox(temp_path, filename, duration="72h"):
    ext = os.path.splitext(filename)[1].lower()
    if ext in BANNED_EXTENSIONS:
        raise RuntimeError(f"Extensión no permitida en Litterbox: {ext}")
    size = os.path.getsize(temp_path)
    if size > LITTERBOX_MAX:
        raise RuntimeError(f"Archivo {format_size(size)} supera el límite de Litterbox (1 GB)")
    async with aiohttp.ClientSession() as session:
        form = aiohttp.FormData()
        form.add_field("reqtype", "fileupload")
        form.add_field("time", duration)
        with open(temp_path, "rb") as f:
            form.add_field("fileToUpload", f, filename=filename)
            async with session.post(LITTERBOX_URL, data=form, timeout=600) as resp:
                text = (await resp.text()).strip()
                if resp.status != 200 or not text.startswith("http"):
                    raise RuntimeError(f"Litterbox error {resp.status}: {text[:200]}")
                return text

async def process_job(job):
    if job.kind == "url":
        await _process_url(job)
    else:
        await _process_file(job)

async def _download_sequential(session, url, temp_path, total, job):
    downloaded = 0
    last_pct = -1
    async with aiofiles.open(temp_path, "wb") as f:
        async with session.get(url, headers=BROWSER_HEADERS) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}")
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                await f.write(chunk)
                downloaded += len(chunk)
                if downloaded > MAX_FILE_SIZE:
                    raise RuntimeError("Archivo supera el límite")
                if total:
                    pct = int(downloaded / total * 100)
                    if pct - last_pct >= 5 or pct == 100:
                        last_pct = pct
                        await edit_status(app, job.chat_id, job.msg_id,
                            f"┎ DOWNLOADING\n┠ [{progress_bar(pct)}]\n┠ PERCENTAGE: {pct}%\n┖ SIZE: {format_size(downloaded)}/{format_size(total)}")
    return downloaded

async def _download_parallel(session, url, temp_path, total, job):
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

    async def download_part(idx, start, end):
        path = f"{temp_path}.part{idx}"
        headers = dict(BROWSER_HEADERS)
        headers["Range"] = f"bytes={start}-{end}"
        async with session.get(url, headers=headers) as resp:
            if resp.status not in (200, 206):
                raise RuntimeError(f"HTTP {resp.status} en parte {idx}")
            async with aiofiles.open(path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    await f.write(chunk)
                    async with progress_lock:
                        progress["total"] += len(chunk)
                        pct = int(progress["total"] / total * 100)
                        if pct - last_pct[0] >= 5 or pct == 100:
                            last_pct[0] = pct
                            await edit_status(app, job.chat_id, job.msg_id,
                                f"┎ DOWNLOADING (parallel x{n_parts})\n┠ [{progress_bar(pct)}]\n┠ PERCENTAGE: {pct}%\n┖ SIZE: {format_size(progress['total'])}/{format_size(total)}")
        return path

    part_paths = await asyncio.gather(*[download_part(i, s, e) for i, s, e in parts])
    async with aiofiles.open(temp_path, "wb") as out:
        for p in part_paths:
            async with aiofiles.open(p, "rb") as src:
                while True:
                    chunk = await src.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    await out.write(chunk)
            try:
                os.unlink(p)
            except Exception:
                pass
    return total

async def _ask_cloud(job, temp_path, filename, size):
    uid = job.user_id
    _pending_cloud[uid] = {
        "job": job,
        "temp_path": temp_path,
        "filename": filename,
        "size": size,
        "chat_id": job.chat_id,
        "msg_id": job.msg_id,
        "stage": "cloud",
        "created_at": time.time(),
    }
    buttons = []
    if size <= TODUS_MAX:
        buttons.append([InlineKeyboardButton("📦 toDus S3 · 2GB · Permanente", callback_data=f"cloud:todus:{uid}")])
    if size <= LITTERBOX_MAX:
        buttons.append([InlineKeyboardButton("⏳ Litterbox · 1GB · Temporal", callback_data=f"cloud:litterbox:{uid}")])
    await app.edit_message_text(
        job.chat_id, job.msg_id,
        f"┎ ✅ DESCARGA COMPLETA\n┠ SIZE: {format_size(size)}\n┖ Elige la nube:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )

async def _process_url(job):
    url = job.url
    filename = sanitize_filename(get_filename_from_url(url) or f"file_{int(time.time())}")
    ext = os.path.splitext(filename)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")
    try:
        check_disk_space()
        async with aiohttp.ClientSession() as session:
            async with asyncio.timeout(1200):
                total = 0
                accepts_ranges = False
                try:
                    async with session.head(url, headers=BROWSER_HEADERS) as h:
                        if h.status < 400:
                            total = int(h.headers.get("Content-Length", 0))
                            accepts_ranges = h.headers.get("Accept-Ranges", "").lower() == "bytes"
                except Exception:
                    pass
                if total and total > MAX_FILE_SIZE:
                    raise RuntimeError(f"Archivo {format_size(total)} supera el límite")
                if PARALLEL_URL_DOWNLOAD and accepts_ranges and total >= PARALLEL_URL_MIN_SIZE:
                    await _download_parallel(session, url, temp_path, total, job)
                else:
                    await _download_sequential(session, url, temp_path, total, job)
        size = os.path.getsize(temp_path)
        await _ask_cloud(job, temp_path, filename, size)
    except asyncio.CancelledError:
        try:
            await app.edit_message_text(job.chat_id, job.msg_id, "❌ CANCELADO")
        except Exception:
            pass
        raise
    except Exception as e:
        log.exception("error procesando URL")
        try:
            await app.edit_message_text(job.chat_id, job.msg_id, f"ERROR: {str(e)[:200]}")
        except Exception:
            pass
        try:
            os.unlink(temp_path)
        except Exception:
            pass

async def _process_file(job):
    original_name = sanitize_filename(job.original_name or f"file_{int(time.time())}")
    ext = os.path.splitext(original_name)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")
    try:
        check_disk_space()

        async def on_dl(current, total):
            if total:
                pct = int(current / total * 100)
                await edit_status(app, job.chat_id, job.msg_id,
                    f"┎ DOWNLOADING FROM TELEGRAM\n┠ [{progress_bar(pct)}]\n┠ PERCENTAGE: {pct}%\n┖ SIZE: {format_size(current)}/{format_size(total)}")

        original_msg = await app.get_messages(job.chat_id, job.original_msg_id)
        await original_msg.download(file_name=temp_path, progress=on_dl)
        size = os.path.getsize(temp_path)
        await _ask_cloud(job, temp_path, original_name, size)
    except asyncio.CancelledError:
        try:
            await app.edit_message_text(job.chat_id, job.msg_id, "❌ CANCELADO")
        except Exception:
            pass
        raise
    except Exception as e:
        log.exception("error procesando archivo")
        try:
            await app.edit_message_text(job.chat_id, job.msg_id, f"ERROR: {str(e)[:200]}")
        except Exception:
            pass
        try:
            os.unlink(temp_path)
        except Exception:
            pass

async def _do_upload(chat_id, msg_id, uid):
    pending = _pending_cloud.get(uid)
    if not pending:
        return
    temp_path = pending["temp_path"]
    filename = pending["filename"]
    size = pending["size"]
    cloud = pending.get("cloud")
    try:
        await edit_status(app, chat_id, msg_id, "UPLOADING...", force=True)
        if cloud == "todus":
            async def on_up(sent, total):
                pct = int(sent / total * 100) if total else 0
                await edit_status(app, chat_id, msg_id,
                    f"┎ UPLOADING → toDus S3\n┠ [{progress_bar(pct)}]\n┠ {pct}%\n┖ {format_size(sent)}/{format_size(total)}")
            url = await subir_a_s3(temp_path, filename, size, on_up)
            cloud_label = "toDus S3"
        elif cloud == "litterbox":
            duration = pending.get("duration", "72h")
            url = await subir_a_litterbox(temp_path, filename, duration)
            cloud_label = f"Litterbox ({duration})"
        else:
            raise RuntimeError("Nube desconocida")
        name = os.path.splitext(filename)[0].replace("_", " ")
        ext = os.path.splitext(filename)[1].replace(".", "")
        await app.edit_message_text(
            chat_id, msg_id,
            f"┎ NAME: {name}\n┠ EXTENSION: {ext}\n┠ SIZE: {format_size(size)}\n┠ CLOUD: {cloud_label}\n┖ URL: {url}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📥 DESCARGAR", url=url)
            ]]),
        )
    except Exception as e:
        log.exception("error subiendo")
        try:
            await app.edit_message_text(chat_id, msg_id, f"ERROR: {str(e)[:200]}")
        except Exception:
            pass
    finally:
        _pending_cloud.pop(uid, None)
        try:
            os.unlink(temp_path)
        except Exception:
            pass

@app.on_message(filters.command("start"))
async def cmd_start(client, message):
    await message.reply_text(
        "**Bot de subida a múltiples nubes**\n\n"
        "Envíame un enlace o un archivo (máx 2 GB).\n"
        "Elige entre:\n"
        "📦 toDus S3 · 2GB · Permanente\n"
        "🐱 Catbox · 200MB · Permanente\n"
        "⏳ Litterbox · 1GB · Temporal\n\n"
        "**Comandos:**\n• /start — este mensaje\n• /cancel — cancelar tu trabajo\n• /status — ver tu posición en la cola")

@app.on_message(filters.command("cancel"))
async def cmd_cancel(client, message):
    uid = message.from_user.id
    if uid in _pending_cloud:
        p = _pending_cloud.pop(uid)
        try:
            os.unlink(p["temp_path"])
        except Exception:
            pass
        await message.reply_text("❌ Selección de nube cancelada.")
        return
    result = await job_queue.cancel_user(uid)
    if result == "none":
        await message.reply_text("ℹ️ No tienes trabajos activos ni en cola.")
    elif result == "active":
        await message.reply_text("❌ Cancelando tu trabajo activo...")
    else:
        await message.reply_text("✅ Tu trabajo fue removido de la cola.")

@app.on_callback_query(filters.regex(r"^cancel:(\d+)$"))
async def on_cancel_callback(client, callback_query):
    uid = int(callback_query.data.split(":")[1])
    if callback_query.from_user.id != uid:
        await callback_query.answer("⛔ Solo el dueño puede cancelar.", show_alert=True)
        return
    if uid in _pending_cloud:
        p = _pending_cloud.pop(uid)
        try:
            os.unlink(p["temp_path"])
        except Exception:
            pass
        await callback_query.answer("❌ Cancelado.")
        try:
            await callback_query.message.edit_text("❌ CANCELADO")
        except Exception:
            pass
        return
    result = await job_queue.cancel_user(uid)
    if result == "none":
        await callback_query.answer("ℹ️ No tienes trabajos.", show_alert=True)
    elif result == "active":
        await callback_query.answer("❌ Cancelando...")
    else:
        await callback_query.answer("✅ Removido de la cola.")

@app.on_callback_query(filters.regex(r"^cloud:(todus|catbox|litterbox):(\d+)$"))
async def on_cloud_choice(client, callback_query):
    match = re.match(r"^cloud:(todus|catbox|litterbox):(\d+)$", callback_query.data)
    cloud, uid = match.group(1), int(match.group(2))
    if callback_query.from_user.id != uid:
        await callback_query.answer("⛔ Solo el dueño puede elegir.", show_alert=True)
        return
    pending = _pending_cloud.get(uid)
    if not pending:
        await callback_query.answer("⏱️ Sesión expirada.", show_alert=True)
        return
    if cloud == "litterbox":
        pending["stage"] = "time"
        time_buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("1h", callback_data=f"time:1h:{uid}"),
             InlineKeyboardButton("12h", callback_data=f"time:12h:{uid}")],
            [InlineKeyboardButton("24h", callback_data=f"time:24h:{uid}"),
             InlineKeyboardButton("72h", callback_data=f"time:72h:{uid}")],
        ])
        await callback_query.message.edit_text(
            f"⏳ Litterbox — elige duración:\n\n📎 {pending['filename']}\n📊 {format_size(pending['size'])}",
            reply_markup=time_buttons,
        )
        await callback_query.answer()
        return
    await callback_query.answer(f"Subiendo a {cloud}...")
    pending["cloud"] = cloud
    await _do_upload(pending["chat_id"], pending["msg_id"], uid)

@app.on_callback_query(filters.regex(r"^time:(1h|12h|24h|72h):(\d+)$"))
async def on_time_choice(client, callback_query):
    match = re.match(r"^time:(1h|12h|24h|72h):(\d+)$", callback_query.data)
    duration, uid = match.group(1), int(match.group(2))
    if callback_query.from_user.id != uid:
        await callback_query.answer("⛔ Solo el dueño puede elegir.", show_alert=True)
        return
    pending = _pending_cloud.get(uid)
    if not pending:
        await callback_query.answer("⏱️ Sesión expirada.", show_alert=True)
        return
    pending["cloud"] = "litterbox"
    pending["duration"] = duration
    await callback_query.answer(f"Subiendo a Litterbox ({duration})...")
    await _do_upload(pending["chat_id"], pending["msg_id"], uid)

@app.on_message(filters.command("status"))
async def cmd_status(client, message):
    uid = message.from_user.id
    if uid in _pending_cloud:
        await message.reply_text("⏳ Esperando que elijas la nube...")
        return
    pos = job_queue.position_of(uid)
    tq = job_queue.queued_count
    ta = job_queue.active_count
    if pos is None:
        await message.reply_text(f"ℹ️ No tienes trabajos en curso.\n\n📊 Cola: {tq} esperando, {ta} procesando")
    elif pos == 0:
        await message.reply_text(f"▶️ Procesándose ahora.\n\n📊 Cola: {tq} esperando, {ta} procesando")
    else:
        await message.reply_text(
            f"⏳ Posición #{pos} de la cola.\n\n📊 Cola: {tq} esperando, {ta} procesando",
            reply_markup=cancel_button(uid))

@app.on_message(filters.text & ~filters.command(["start", "cancel", "status"]))
async def handle_text(client, message):
    uid = message.from_user.id
    match = URL_RE.search(message.text.strip())
    if not match:
        await message.reply_text(f"Envíame un enlace o un archivo (máx {format_size(MAX_FILE_SIZE)}).")
        return
    if job_queue.has_user_job(uid) or uid in _pending_cloud:
        await message.reply_text("⚠️ Ya tienes un trabajo en curso o pendiente.")
        return
    url = match.group(1)
    status = await message.reply_text("📥 Añadiendo a la cola...")
    job = QueuedJob(user_id=uid, chat_id=message.chat.id, msg_id=status.id, kind="url", url=url)
    try:
        position = await job_queue.enqueue(job)
    except ValueError:
        await status.edit_text("⚠️ Ya tienes un trabajo en curso o en cola.")
        return
    if position == 1:
        await status.edit_text("▶️ Eres el siguiente, procesando...", reply_markup=cancel_button(uid))
    else:
        await status.edit_text(f"⏳ En cola — posición #{position}\nEsperando turno... ({position - 1} delante de ti)", reply_markup=cancel_button(uid))

@app.on_message(filters.document | filters.video | filters.audio | filters.voice | filters.video_note | filters.animation | filters.sticker | filters.photo)
async def handle_media(client, message):
    uid = message.from_user.id
    media = (message.document or message.video or message.audio or message.voice or message.video_note or message.animation or message.sticker or (message.photo[-1] if message.photo else None))
    if media is None:
        return
    file_size = getattr(media, "file_size", 0) or 0
    if file_size and file_size > MAX_FILE_SIZE:
        await message.reply_text(f"❌ Archivo demasiado grande ({format_size(file_size)}).")
        return
    if job_queue.has_user_job(uid) or uid in _pending_cloud:
        await message.reply_text("⚠️ Ya tienes un trabajo en curso o pendiente.")
        return
    original_name = getattr(media, "file_name", None)
    if not original_name:
        ts = int(time.time())
        if message.photo: original_name = f"photo_{ts}.jpg"
        elif message.video: original_name = f"video_{ts}.mp4"
        elif message.audio: original_name = f"audio_{ts}.mp3"
        elif message.voice: original_name = f"voice_{ts}.ogg"
        elif message.video_note: original_name = f"video_note_{ts}.mp4"
        elif message.animation: original_name = f"animation_{ts}.mp4"
        elif message.sticker: original_name = f"sticker_{ts}.webp"
        else: original_name = f"file_{ts}.bin"
    status = await message.reply_text("📥 Añadiendo a la cola...")
    job = QueuedJob(user_id=uid, chat_id=message.chat.id, msg_id=status.id, kind="file", original_name=original_name, original_msg_id=message.id)
    try:
        position = await job_queue.enqueue(job)
    except ValueError:
        await status.edit_text("⚠️ Ya tienes un trabajo en curso o en cola.")
        return
    if position == 1:
        await status.edit_text("▶️ Eres el siguiente, procesando...", reply_markup=cancel_button(uid))
    else:
        await status.edit_text(f"⏳ En cola — posición #{position}\nEsperando turno... ({position - 1} delante de ti)", reply_markup=cancel_button(uid))

START_TIME = time.time()

async def health_handler(request):
    return web.json_response({"status": "healthy", "uptime": round(time.time() - START_TIME, 1), "queue_queued": job_queue.queued_count, "queue_active": job_queue.active_count, "pending_cloud": len(_pending_cloud)})

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

async def main():
    web_runner = await run_web()
    await app.start()
    job_queue.start_workers(process_job)
    asyncio.create_task(watchdog())
    log.info(f"BOT READY — {QUEUE_WORKERS} workers, límite {format_size(MAX_FILE_SIZE)}, watchdog {WATCHDOG_LIFETIME}s")
    try:
        await asyncio.Event().wait()
    finally:
        log.info("apagando limpiamente...")
        await job_queue.stop_workers()
        await app.stop()
        await web_runner.cleanup()
        log.info("apagado completo")

if __name__ == "__main__":
    app.run(main())
