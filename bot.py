"""
Bot de subida a Todus S3 con multipart upload - Cola global FIFO.

- Descarga desde URL (aiohttp) o desde Telegram (Pyrogram)
- Sube a s3.todus.cu/stream con aioboto3 (multipart automático)
- Cola global FIFO con 1 worker (evita saturar el bot)
- Límite de 500 MB por archivo, verificado antes y durante la descarga
- 1 job por usuario (no acapara la cola)
- Notificación en vivo de la posición en cola (throttle 3s)
- Botón inline "❌ Cancelar" en mensajes en cola y activos
- Cancelación individual (/cancel o botón)
- Health server async con aiohttp en 0.0.0.0:$PORT
- Keepalive cada 10 min (con backoff exponencial hasta 60 min si falla)
- Chunks de descarga: 1 MB (URL y Telegram)
- Escritura async a disco con aiofiles
"""

import os
import re
import time
import uuid
import asyncio
import logging
from urllib.parse import urlparse, unquote, quote

import aiofiles
import aiohttp
import aioboto3
from aiohttp import web
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)


# ============================================================
# Configuración (adaptada a Render)
# ============================================================

BOT_TOKEN = "8942582638:AAF1MQGDLVPqbLxwK3X2RbEeLDYfHzUzp5I"
API_ID = 32471788
API_HASH = "cb57130abda56877acf3b3027e569450"

S3_ENDPOINT = "https://s3.todus.cu"
S3_BUCKET = "stream"
S3_REGION = "us-east-1"

DOWNLOAD_PATH = "/tmp/todus_uploads"
SESSION_DIR = "/tmp/todus_session"
SESSION_NAME = "todus_bot"

SELF_URL = os.environ.get("SELF_URL", "https://s3-bot-y4ap.onrender.com")
PORT = int(os.environ.get("PORT", 10000))

MAX_FILE_SIZE = 500 * 1024 * 1024   # 500 MB
QUEUE_WORKERS = 1                    # 1 worker en Render free (512MB RAM)
CHUNK_SIZE = 1024 * 1024             # 1 MB
POSITION_POLL_INTERVAL = 3           # cada 3s revisa posiciones

os.makedirs(DOWNLOAD_PATH, exist_ok=True)
os.makedirs(SESSION_DIR, exist_ok=True)


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
log = logging.getLogger("bot")


# ============================================================
# Cliente Pyrogram
# ============================================================

app = Client(
    os.path.join(SESSION_DIR, SESSION_NAME),
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=2,
)


# ============================================================
# Configuración S3
# ============================================================

_S3_CONFIG = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=5,
    connect_timeout=30,
    read_timeout=600,
)

_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=8 * 1024 * 1024,
    max_concurrency=2,
    use_threads=True,
)

_s3_session = aioboto3.Session()


# ============================================================
# Cola global FIFO
# ============================================================

class QueuedJob:
    """Representa un job en la cola."""

    __slots__ = (
        "job_id", "user_id", "chat_id", "msg_id",
        "kind", "url", "original_name", "original_msg_id",
        "task", "created_at", "cancel_requested",
    )

    def __init__(
        self,
        user_id: int,
        chat_id: int,
        msg_id: int,
        kind: str,
        url: str | None = None,
        original_name: str | None = None,
        original_msg_id: int | None = None,
    ):
        self.job_id = uuid.uuid4().hex
        self.user_id = user_id
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.kind = kind
        self.url = url
        self.original_name = original_name
        self.original_msg_id = original_msg_id
        self.task: asyncio.Task | None = None
        self.created_at = time.time()
        self.cancel_requested = False


class JobQueue:
    """
    Cola global FIFO.
    - 1 job por usuario (activo o en cola)
    - Workers de fondo consumen la cola en orden
    - Notificación en vivo de posición (cada POSITION_POLL_INTERVAL segundos)
    """

    def __init__(self, n_workers: int):
        self.queue: asyncio.Queue[QueuedJob] = asyncio.Queue()
        self.n_workers = n_workers
        self.workers: list[asyncio.Task] = []
        self.user_pending: dict[int, QueuedJob] = {}
        self.active_jobs: dict[str, QueuedJob] = {}
        self._lock = asyncio.Lock()

    async def enqueue(self, job: QueuedJob) -> int:
        async with self._lock:
            if job.user_id in self.user_pending:
                raise ValueError("user_already_queued")
            self.user_pending[job.user_id] = job
            await self.queue.put(job)
            return self.queue.qsize()

    async def mark_active(self, job: QueuedJob):
        async with self._lock:
            self.active_jobs[job.job_id] = job

    async def finish(self, job: QueuedJob):
        async with self._lock:
            self.active_jobs.pop(job.job_id, None)
            current = self.user_pending.get(job.user_id)
            if current is not None and current.job_id == job.job_id:
                self.user_pending.pop(job.user_id, None)

    def position_of(self, user_id: int) -> int | None:
        job = self.user_pending.get(user_id)
        if job is None:
            return None
        if job.job_id in self.active_jobs:
            return 0
        for i, queued in enumerate(self.queue._queue):  # noqa: SLF001
            if queued.job_id == job.job_id:
                return i + 1
        return None

    def has_user_job(self, user_id: int) -> bool:
        return user_id in self.user_pending

    async def cancel_user(self, user_id: int) -> str:
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
            items = list(self.queue._queue)  # noqa: SLF001
            self.queue._queue.clear()        # noqa: SLF001
            for item in items:
                if item.job_id != job.job_id:
                    self.queue._queue.append(item)  # noqa: SLF001
            return "queued"

    @property
    def queued_count(self) -> int:
        return self.queue.qsize()

    @property
    def active_count(self) -> int:
        return len(self.active_jobs)

    # -------- Tareas de fondo --------

    def start_workers(self, process_fn):
        for i in range(self.n_workers):
            self.workers.append(asyncio.create_task(self._worker_loop(i, process_fn)))
        self.workers.append(asyncio.create_task(self._notify_position_changes()))
        log.info(
            f"{self.n_workers} worker(s) + notificador de posición arrancados"
        )

    async def _worker_loop(self, worker_id: int, process_fn):
        log.info(f"Worker #{worker_id} arrancado")
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
                    log.info(f"Job {job.job_id} cancelado")
                except Exception as e:
                    log.exception(f"job {job.job_id} falló: {e}")
            except asyncio.CancelledError:
                log.info(f"Worker #{worker_id} detenido")
                raise
            finally:
                await self.finish(job)
                self.queue.task_done()

    async def _notify_position_changes(self):
        """
        Tarea de fondo: notifica a usuarios en cola cuando cambia su posición.
        - Ignora usuarios con job activo (esos ya tienen barra de progreso).
        - Throttle implícito por el interval de POSITION_POLL_INTERVAL segundos.
        """
        last_positions: dict[int, int] = {}
        while True:
            try:
                current: dict[int, int] = {}
                for i, job in enumerate(self.queue._queue):  # noqa: SLF001
                    # Ignorar jobs activos: su mensaje lo maneja la barra de progreso
                    if job.job_id in self.active_jobs:
                        continue
                    current[job.user_id] = i + 1

                # Notificar solo a los que cambiaron de posición
                for uid, pos in current.items():
                    if last_positions.get(uid) == pos:
                        continue
                    job = self.user_pending.get(uid)
                    if not job:
                        continue
                    try:
                        await app.edit_message_text(
                            job.chat_id, job.msg_id,
                            f"⏳ En cola — posición #{pos}\n"
                            f"Esperando turno... ({pos - 1} delante de ti)",
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton(
                                    "❌ Cancelar",
                                    callback_data=f"cancel:{uid}"
                                )
                            ]]),
                        )
                    except Exception as e:
                        log.debug(f"notify position falló: {e}")

                # Actualizar snapshot
                last_positions = dict(current)
            except Exception as e:
                log.exception(f"_notify_position_changes: {e}")
            await asyncio.sleep(POSITION_POLL_INTERVAL)

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


# ============================================================
# Utilidades
# ============================================================

def format_size(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    if b < 1048576:
        return f"{b / 1024:.1f} KB"
    if b < 1073741824:
        return f"{b / 1048576:.1f} MB"
    return f"{b / 1073741824:.2f} GB"


def progress_bar(p: int) -> str:
    filled = round(15 * p / 100)
    return "⬢" * filled + "⬡" * (15 - filled)


URL_RE = re.compile(r"(https?://[^\s<>\"']+?)(?=[.,;:!?)\]]?(\s|$))", re.IGNORECASE)


def get_filename_from_url(url: str) -> str | None:
    try:
        name = os.path.basename(urlparse(url).path)
        if name and len(name) > 2:
            return unquote(name)
    except Exception:
        pass
    return None


def sanitize_filename(name: str) -> str:
    name = name.replace("/", "_").replace("\\", "_")
    name = re.sub(r"[\s?#&]+", "_", name)
    return name.strip("._") or f"file_{int(time.time())}"


def check_disk_space():
    st = os.statvfs(DOWNLOAD_PATH)
    free = st.f_bavail * st.f_frsize
    if free < MAX_FILE_SIZE:
        raise RuntimeError(
            f"Disco insuficiente: {format_size(free)} libres, "
            f"se necesitan {format_size(MAX_FILE_SIZE)}"
        )


def cancel_button(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Cancelar", callback_data=f"cancel:{uid}")
    ]])


# ============================================================
# Estado de edición con throttle
# ============================================================

class EditState:
    def __init__(self):
        self.last_sent = 0.0
        self.last_text = ""

_edit_states: dict[int, EditState] = {}
_edit_locks: dict[int, asyncio.Lock] = {}


async def edit_status(client: Client, chat_id: int, msg_id: int, text: str, force: bool = False):
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
        except Exception as e:
            log.debug(f"edit_status falló: {e}")


async def notify_processing(job: QueuedJob):
    """Avisa al usuario que su job salió de la cola y empieza a procesarse."""
    try:
        await app.edit_message_text(
            job.chat_id, job.msg_id,
            "▶️ Procesando...",
            reply_markup=cancel_button(job.user_id),
        )
    except Exception as e:
        log.debug(f"notify_processing falló: {e}")


# ============================================================
# Subida a S3 con aioboto3
# ============================================================

async def subir_a_s3(temp_path: str, filename: str, size: int, on_progress=None) -> str:
    safe_name = sanitize_filename(filename)
    remote_key = f"{uuid.uuid4().hex[:8]}_{safe_name}"

    loop = asyncio.get_running_loop()
    last_update = [0.0]

    def _progress_callback(bytes_transferred: int):
        if on_progress is None:
            return
        now = loop.time()
        if now - last_update[0] < 0.5 and bytes_transferred < size:
            return
        last_update[0] = now
        asyncio.run_coroutine_threadsafe(
            on_progress(bytes_transferred, size), loop
        )

    async with _s3_session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public",
        aws_secret_access_key="public",
        region_name=S3_REGION,
        config=_S3_CONFIG,
    ) as s3:
        with open(temp_path, "rb") as f:
            await s3.upload_fileobj(
                f,
                S3_BUCKET,
                remote_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
                Config=_TRANSFER_CONFIG,
                Callback=_progress_callback,
            )

    return f"{S3_ENDPOINT}/{S3_BUCKET}/{quote(remote_key)}"


# ============================================================
# Procesamiento de jobs (llamado por el worker)
# ============================================================

async def process_job(job: QueuedJob):
    if job.kind == "url":
        await _process_url(job)
    else:
        await _process_file(job)


async def _process_url(job: QueuedJob):
    url = job.url
    filename = get_filename_from_url(url) or f"file_{int(time.time())}"
    filename = sanitize_filename(filename)
    ext = os.path.splitext(filename)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    try:
        check_disk_space()

        async with aiohttp.ClientSession() as session:
            async with asyncio.timeout(3600):
                async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                    if resp.status >= 400:
                        raise RuntimeError(f"HTTP {resp.status}")

                    total = int(resp.headers.get("Content-Length", 0))
                    if total and total > MAX_FILE_SIZE:
                        raise RuntimeError(
                            f"Archivo {format_size(total)} supera el límite "
                            f"de {format_size(MAX_FILE_SIZE)}"
                        )

                    downloaded = 0
                    last_pct = -1

                    # ✅ Escritura async con aiofiles
                    async with aiofiles.open(temp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            await f.write(chunk)
                            downloaded += len(chunk)

                            if downloaded > MAX_FILE_SIZE:
                                raise RuntimeError(
                                    f"Archivo supera el límite de "
                                    f"{format_size(MAX_FILE_SIZE)} durante la descarga"
                                )

                            if total:
                                pct = int(downloaded / total * 100)
                                if pct - last_pct >= 5 or pct == 100:
                                    last_pct = pct
                                    await edit_status(
                                        app, job.chat_id, job.msg_id,
                                        f"┎ DOWNLOADING\n"
                                        f"┠ [{progress_bar(pct)}]\n"
                                        f"┠ PERCENTAGE: {pct}%\n"
                                        f"┖ SIZE: {format_size(downloaded)}/{format_size(total)}"
                                    )

        size = os.path.getsize(temp_path)
        if size > MAX_FILE_SIZE:
            raise RuntimeError(f"Archivo {format_size(size)} supera el límite")

        await edit_status(app, job.chat_id, job.msg_id, "UPLOADING...", force=True)

        async def on_up(sent, total):
            pct = int(sent / total * 100) if total else 0
            await edit_status(
                app, job.chat_id, job.msg_id,
                f"┎ UPLOADING\n"
                f"┠ [{progress_bar(pct)}]\n"
                f"┠ PERCENTAGE: {pct}%\n"
                f"┖ SIZE: {format_size(sent)}/{format_size(total)}"
            )

        upload_url = await subir_a_s3(temp_path, filename, size, on_up)

        name = os.path.splitext(filename)[0].replace("_", " ")
        ext_clean = ext.replace(".", "")
        await app.edit_message_text(
            job.chat_id, job.msg_id,
            f"┎ NAME: {name}\n"
            f"┠ EXTENSION: {ext_clean}\n"
            f"┠ SIZE: {format_size(size)}\n"
            f"┖ URL: {upload_url}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📥 DESCARGAR", url=upload_url)
            ]]),
        )

    except asyncio.CancelledError:
        log.info(f"Job {job.job_id} cancelado (URL)")
        try:
            await app.edit_message_text(job.chat_id, job.msg_id, "❌ CANCELADO")
        except Exception:
            pass
        raise
    except Exception as e:
        log.exception("error procesando URL")
        try:
            await app.edit_message_text(
                job.chat_id, job.msg_id, f"ERROR: {str(e)[:200]}"
            )
        except Exception:
            pass
    finally:
        try:
            os.unlink(temp_path)
        except Exception:
            pass


async def _process_file(job: QueuedJob):
    original_name = sanitize_filename(job.original_name or f"file_{int(time.time())}")
    ext = os.path.splitext(original_name)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    try:
        check_disk_space()

        async def on_dl(current, total):
            if total:
                pct = int(current / total * 100)
                await edit_status(
                    app, job.chat_id, job.msg_id,
                    f"┎ DOWNLOADING FROM TELEGRAM\n"
                    f"┠ [{progress_bar(pct)}]\n"
                    f"┠ PERCENTAGE: {pct}%\n"
                    f"┖ SIZE: {format_size(current)}/{format_size(total)}"
                )

        original_msg = await app.get_messages(job.chat_id, job.original_msg_id)
        await original_msg.download(
            file_name=temp_path,
            progress=on_dl,
            chunk_size=CHUNK_SIZE,
        )
        size = os.path.getsize(temp_path)

        await edit_status(app, job.chat_id, job.msg_id, "UPLOADING...", force=True)

        async def on_up(sent, total):
            pct = int(sent / total * 100) if total else 0
            await edit_status(
                app, job.chat_id, job.msg_id,
                f"┎ UPLOADING\n"
                f"┠ [{progress_bar(pct)}]\n"
                f"┠ PERCENTAGE: {pct}%\n"
                f"┖ SIZE: {format_size(sent)}/{format_size(total)}"
            )

        upload_url = await subir_a_s3(temp_path, original_name, size, on_up)

        name = os.path.splitext(original_name)[0].replace("_", " ")
        ext_clean = ext.replace(".", "")
        await app.edit_message_text(
            job.chat_id, job.msg_id,
            f"┎ NAME: {name}\n"
            f"┠ EXTENSION: {ext_clean}\n"
            f"┠ SIZE: {format_size(size)}\n"
            f"┖ URL: {upload_url}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📥 DESCARGAR", url=upload_url)
            ]]),
        )

    except asyncio.CancelledError:
        log.info(f"Job {job.job_id} cancelado (archivo)")
        try:
            await app.edit_message_text(job.chat_id, job.msg_id, "❌ CANCELADO")
        except Exception:
            pass
        raise
    except Exception as e:
        log.exception("error procesando archivo")
        try:
            await app.edit_message_text(
                job.chat_id, job.msg_id, f"ERROR: {str(e)[:200]}"
            )
        except Exception:
            pass
    finally:
        try:
            os.unlink(temp_path)
        except Exception:
            pass


# ============================================================
# Handlers
# ============================================================

@app.on_message(filters.command("start"))
async def cmd_start(client: Client, message: Message):
    await message.reply_text(
        "**Bot de subida a Todus S3**\n\n"
        f"Envíame un enlace o un archivo (máx {format_size(MAX_FILE_SIZE)}).\n"
        "El archivo se sube a `s3.todus.cu/stream` y te devuelvo el enlace público.\n\n"
        "Los archivos se procesan en **cola FIFO** (1 a la vez).\n"
        "Solo puedes tener **1 trabajo** activo o en cola.\n\n"
        "**Comandos:**\n"
        "• /start — este mensaje\n"
        "• /cancel — cancelar tu trabajo (activo o en cola)\n"
        "• /status — ver tu posición en la cola"
    )


@app.on_message(filters.command("cancel"))
async def cmd_cancel(client: Client, message: Message):
    uid = message.from_user.id
    result = await job_queue.cancel_user(uid)
    if result == "none":
        await message.reply_text("ℹ️ No tienes trabajos activos ni en cola.")
    elif result == "active":
        await message.reply_text("❌ Cancelando tu trabajo activo...")
    else:
        await message.reply_text("✅ Tu trabajo fue removido de la cola.")


@app.on_callback_query(filters.regex(r"^cancel:(\d+)$"))
async def on_cancel_callback(client: Client, callback_query: CallbackQuery):
    uid = int(callback_query.data.split(":")[1])
    if callback_query.from_user.id != uid:
        await callback_query.answer(
            "⛔ Solo el dueño del trabajo puede cancelarlo.",
            show_alert=True,
        )
        return

    result = await job_queue.cancel_user(uid)
    if result == "none":
        await callback_query.answer("ℹ️ No tienes trabajos.", show_alert=True)
        try:
            await callback_query.message.edit_text("ℹ️ Sin trabajo pendiente.")
        except Exception:
            pass
    elif result == "active":
        await callback_query.answer("❌ Cancelando...", show_alert=False)
        try:
            await callback_query.message.edit_text("❌ CANCELADO")
        except Exception:
            pass
    else:
        await callback_query.answer("✅ Removido de la cola.", show_alert=False)
        try:
            await callback_query.message.edit_text("❌ CANCELADO")
        except Exception:
            pass


@app.on_message(filters.command("status"))
async def cmd_status(client: Client, message: Message):
    uid = message.from_user.id
    pos = job_queue.position_of(uid)

    total_queued = job_queue.queued_count
    total_active = job_queue.active_count

    if pos is None:
        await message.reply_text(
            f"ℹ️ No tienes trabajos en curso.\n\n"
            f"📊 Cola global: {total_queued} esperando, {total_active} procesando"
        )
    elif pos == 0:
        await message.reply_text(
            f"▶️ Tu trabajo está **procesándose ahora**.\n\n"
            f"📊 Cola global: {total_queued} esperando, {total_active} procesando"
        )
    else:
        await message.reply_text(
            f"⏳ Estás en la **posición #{pos}** de la cola.\n\n"
            f"📊 Cola global: {total_queued} esperando, {total_active} procesando",
            reply_markup=cancel_button(uid),
        )


@app.on_message(filters.text & ~filters.command(["start", "cancel", "status"]))
async def handle_text(client: Client, message: Message):
    uid = message.from_user.id

    match = URL_RE.search(message.text.strip())
    if not match:
        await message.reply_text(
            f"Envíame un enlace de descarga directa o un archivo "
            f"(máx {format_size(MAX_FILE_SIZE)})."
        )
        return

    if job_queue.has_user_job(uid):
        await message.reply_text(
            "⚠️ Ya tienes un trabajo en curso o en cola. "
            "Usa /status para ver tu posición o /cancel para cancelarlo."
        )
        return

    url = match.group(1)
    status = await message.reply_text("📥 Añadiendo a la cola...")

    job = QueuedJob(
        user_id=uid,
        chat_id=message.chat.id,
        msg_id=status.id,
        kind="url",
        url=url,
    )

    try:
        position = await job_queue.enqueue(job)
    except ValueError:
        await status.edit_text("⚠️ Ya tienes un trabajo en curso o en cola.")
        return

    if position == 1:
        await status.edit_text(
            "▶️ Eres el siguiente, procesando...",
            reply_markup=cancel_button(uid),
        )
    else:
        await status.edit_text(
            f"⏳ En cola — posición #{position}\n"
            f"Esperando turno... ({position - 1} delante de ti)",
            reply_markup=cancel_button(uid),
        )


@app.on_message(
    filters.document | filters.video | filters.audio | filters.voice |
    filters.video_note | filters.animation | filters.sticker | filters.photo
)
async def handle_media(client: Client, message: Message):
    uid = message.from_user.id

    media = (
        message.document or message.video or message.audio or message.voice or
        message.video_note or message.animation or message.sticker or
        (message.photo[-1] if message.photo else None)
    )
    if media is None:
        return

    file_size = getattr(media, "file_size", 0) or 0
    if file_size and file_size > MAX_FILE_SIZE:
        await message.reply_text(
            f"❌ Archivo demasiado grande ({format_size(file_size)}). "
            f"El límite es {format_size(MAX_FILE_SIZE)}."
        )
        return

    if job_queue.has_user_job(uid):
        await message.reply_text(
            "⚠️ Ya tienes un trabajo en curso o en cola. "
            "Usa /status para ver tu posición o /cancel para cancelarlo."
        )
        return

    original_name = getattr(media, "file_name", None)
    if not original_name:
        ts = int(time.time())
        if message.photo:
            original_name = f"photo_{ts}.jpg"
        elif message.video:
            original_name = f"video_{ts}.mp4"
        elif message.audio:
            original_name = f"audio_{ts}.mp3"
        elif message.voice:
            original_name = f"voice_{ts}.ogg"
        elif message.video_note:
            original_name = f"video_note_{ts}.mp4"
        elif message.animation:
            original_name = f"animation_{ts}.mp4"
        elif message.sticker:
            original_name = f"sticker_{ts}.webp"
        else:
            original_name = f"file_{ts}.bin"

    status = await message.reply_text("📥 Añadiendo a la cola...")

    job = QueuedJob(
        user_id=uid,
        chat_id=message.chat.id,
        msg_id=status.id,
        kind="file",
        original_name=original_name,
        original_msg_id=message.id,
    )

    try:
        position = await job_queue.enqueue(job)
    except ValueError:
        await status.edit_text("⚠️ Ya tienes un trabajo en curso o en cola.")
        return

    if position == 1:
        await status.edit_text(
            "▶️ Eres el siguiente, procesando...",
            reply_markup=cancel_button(uid),
        )
    else:
        await status.edit_text(
            f"⏳ En cola — posición #{position}\n"
            f"Esperando turno... ({position - 1} delante de ti)",
            reply_markup=cancel_button(uid),
        )


# ============================================================
# Health server
# ============================================================

START_TIME = time.time()


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "healthy",
        "uptime": round(time.time() - START_TIME, 1),
        "queue_queued": job_queue.queued_count,
        "queue_active": job_queue.active_count,
        "workers": QUEUE_WORKERS,
        "max_file_size_mb": MAX_FILE_SIZE // (1024 * 1024),
        "chunk_size_kb": CHUNK_SIZE // 1024,
    })


async def root_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "online"})


def make_web_app() -> web.Application:
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
    log.info(f"Health server escuchando en 0.0.0.0:{PORT}")
    return runner


# ============================================================
# Keepalive
# ============================================================

async def keepalive():
    base_interval = 600
    max_interval = 3600

    async with aiohttp.ClientSession() as session:
        await asyncio.sleep(60)
        while True:
            try:
                async with session.get(f"{SELF_URL}/health", timeout=15) as r:
                    if r.status == 200:
                        base_interval = 600
                        log.info("keepalive OK")
                    else:
                        log.warning(f"keepalive HTTP {r.status}")
            except Exception as e:
                log.warning(f"keepalive falló: {e}")
                base_interval = min(base_interval * 2, max_interval)
            await asyncio.sleep(base_interval)


# ============================================================
# Arranque
# ============================================================

async def main():
    web_runner = await run_web()
    await app.start()
    job_queue.start_workers(process_job)
    keepalive_task = asyncio.create_task(keepalive())
    log.info(
        f"BOT READY — {QUEUE_WORKERS} worker(s), "
        f"límite {format_size(MAX_FILE_SIZE)}, "
        f"chunk {CHUNK_SIZE // 1024} KB"
    )
    try:
        await asyncio.Event().wait()
    finally:
        keepalive_task.cancel()
        await job_queue.stop_workers()
        await app.stop()
        await web_runner.cleanup()


if __name__ == "__main__":
    app.run(main())