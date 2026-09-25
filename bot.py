"""
Bot de subida a Todus S3 con multipart upload - Versión Render (plan gratis)

- Descarga desde URL (aiohttp) o desde Telegram (Pyrogram)
- Sube a s3.todus.cu/stream con aioboto3 (multipart automático)
- Progreso en vivo con throttle
- Cancelación de trabajos con /cancel
- Health server async con aiohttp en 0.0.0.0:$PORT
- Keepalive cada 10 min para evitar el sleep de Render
- Optimizado para Render free: 512MB RAM, CPU compartida
"""

import os
import re
import time
import uuid
import asyncio
import logging
from urllib.parse import urlparse, unquote, quote

import aiohttp
import aioboto3
from aiohttp import web
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton


# ============================================================
# Configuración (adaptada a Render)
# ============================================================

BOT_TOKEN = "8942582638:AAF1MQGDLVPqbLxwK3X2RbEeLDYfHzUzp5I"
API_ID = 32471788
API_HASH = "cb57130abda56877acf3b3027e569450"

S3_ENDPOINT = "https://s3.todus.cu"
S3_BUCKET = "stream"
S3_REGION = "us-east-1"

# Rutas: Render solo permite escribir en /tmp y en el working dir
DOWNLOAD_PATH = "/tmp/todus_uploads"
SESSION_DIR = "/tmp/todus_session"
SESSION_NAME = "todus_bot"

# URL pública del servicio en Render (para el keepalive)
SELF_URL = os.environ.get("SELF_URL", "https://s3-bot-y4ap.onrender.com")

# Render asigna el puerto dinámicamente
PORT = int(os.environ.get("PORT", 10000))

MAX_FILE_SIZE = 2000 * 1024 * 1024   # 2 GB
MAX_CONCURRENT_JOBS = 1              # Render free: 512MB RAM -> 1 job a la vez

os.makedirs(DOWNLOAD_PATH, exist_ok=True)
os.makedirs(SESSION_DIR, exist_ok=True)


# ============================================================
# Logging (solo a stdout, Render lo captura automáticamente)
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
log = logging.getLogger("bot")


# ============================================================
# Cliente Pyrogram (workers bajos por RAM limitada)
# ============================================================

app = Client(
    os.path.join(SESSION_DIR, SESSION_NAME),
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=2,
)


# ============================================================
# Configuración S3 (ajustada para RAM limitada de Render)
# ============================================================

_S3_CONFIG = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=5,
    connect_timeout=30,
    read_timeout=600,
)

_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,     # >=8MB -> multipart
    multipart_chunksize=8 * 1024 * 1024,     # 8MB por parte (mínimo válido)
    max_concurrency=2,                        # Poco paralelismo por RAM
    use_threads=True,
)

_s3_session = aioboto3.Session()


# ============================================================
# Job Manager
# ============================================================

class JobManager:
    """Maneja trabajos activos con límite de concurrencia."""

    def __init__(self):
        self._jobs: dict[str, dict] = {}
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

    async def register(self, task: asyncio.Task, chat_id: int, msg_id: int, temp_path: str) -> str:
        async with self._lock:
            job_id = uuid.uuid4().hex
            self._jobs[job_id] = {
                "task": task,
                "chat_id": chat_id,
                "msg_id": msg_id,
                "temp_path": temp_path,
                "created_at": time.time(),
            }
            return job_id

    async def unregister(self, job_id: str):
        async with self._lock:
            self._jobs.pop(job_id, None)

    async def cancel_all(self) -> list[dict]:
        async with self._lock:
            cancelled = []
            for job_id, info in list(self._jobs.items()):
                task = info["task"]
                if not task.done():
                    task.cancel()
                    cancelled.append(info)
                self._jobs.pop(job_id, None)
            return cancelled

    @property
    def active_count(self) -> int:
        return len(self._jobs)

    def semaphore(self) -> asyncio.Semaphore:
        return self._semaphore


job_manager = JobManager()


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
    """Evita caracteres que rompen la key de S3 y la URL del botón."""
    name = name.replace("/", "_").replace("\\", "_")
    # Limpia espacios, ?, #, & que rompen URLs
    name = re.sub(r"[\s?#&]+", "_", name)
    return name.strip("._") or f"file_{int(time.time())}"


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
    """Edita el mensaje de estado con throttle para no saturar Telegram."""
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


# ============================================================
# Subida a S3 con aioboto3
# ============================================================

async def subir_a_s3(temp_path: str, filename: str, size: int, on_progress=None) -> str:
    """
    Sube un archivo a Todus S3 (bucket público stream).

    - <8MB: PutObject simple
    - >=8MB: multipart con partes de 8MB y 2 en paralelo
    """
    safe_name = sanitize_filename(filename)
    remote_key = f"{uuid.uuid4().hex[:8]}_{safe_name}"

    loop = asyncio.get_running_loop()
    last_update = [0.0]

    def _progress_callback(bytes_transferred: int):
        # Llamado desde threads internos de boto3 (NO desde el event loop)
        if on_progress is None:
            return
        now = loop.time()
        if now - last_update[0] < 0.5 and bytes_transferred < size:
            return
        last_update[0] = now
        asyncio.run_coroutine_threadsafe(
            on_progress(bytes_transferred, size),
            loop,
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

    # ✅ ÚNICA CORRECCIÓN: quote() para que Telegram acepte la URL del botón
    return f"{S3_ENDPOINT}/{S3_BUCKET}/{quote(remote_key)}"


# ============================================================
# Trabajo: procesar URL
# ============================================================

async def procesar_url(client: Client, message: Message, url: str, status_id: int, job_id_holder: dict):
    filename = get_filename_from_url(url) or f"file_{int(time.time())}"
    filename = sanitize_filename(filename)
    ext = os.path.splitext(filename)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    job_id = await job_manager.register(
        asyncio.current_task(), message.chat.id, status_id, temp_path
    )
    job_id_holder["job_id"] = job_id

    async with job_manager.semaphore():
        try:
            # 1) Descarga desde URL externa
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

                        with open(temp_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(64 * 1024):
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total:
                                    pct = int(downloaded / total * 100)
                                    if pct - last_pct >= 5 or pct == 100:
                                        last_pct = pct
                                        await edit_status(
                                            client, message.chat.id, status_id,
                                            f"┎ DOWNLOADING\n"
                                            f"┠ [{progress_bar(pct)}]\n"
                                            f"┠ PERCENTAGE: {pct}%\n"
                                            f"┖ SIZE: {format_size(downloaded)}/{format_size(total)}"
                                        )

            size = os.path.getsize(temp_path)
            if size > MAX_FILE_SIZE:
                raise RuntimeError(f"Archivo {format_size(size)} supera el límite")

            # 2) Subida a S3
            await edit_status(client, message.chat.id, status_id, "UPLOADING...", force=True)

            async def on_up(sent, total):
                pct = int(sent / total * 100) if total else 0
                await edit_status(
                    client, message.chat.id, status_id,
                    f"┎ UPLOADING\n"
                    f"┠ [{progress_bar(pct)}]\n"
                    f"┠ PERCENTAGE: {pct}%\n"
                    f"┖ SIZE: {format_size(sent)}/{format_size(total)}"
                )

            upload_url = await subir_a_s3(temp_path, filename, size, on_up)

            # 3) Mensaje final con botón inline
            name = os.path.splitext(filename)[0].replace("_", " ")
            ext_clean = ext.replace(".", "")
            await client.edit_message_text(
                message.chat.id, status_id,
                f"┎ NAME: {name}\n"
                f"┠ EXTENSION: {ext_clean}\n"
                f"┠ SIZE: {format_size(size)}\n"
                f"┖ URL: {upload_url}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📥 DESCARGAR", url=upload_url)
                ]]),
            )

        except asyncio.CancelledError:
            log.info(f"Trabajo {job_id} cancelado (URL)")
            try:
                await client.edit_message_text(message.chat.id, status_id, "❌ CANCELADO")
            except Exception:
                pass
            raise
        except Exception as e:
            log.exception("error procesando URL")
            try:
                await client.edit_message_text(
                    message.chat.id, status_id, f"ERROR: {str(e)[:200]}"
                )
            except Exception:
                pass
        finally:
            try:
                os.unlink(temp_path)
            except Exception:
                pass
            await job_manager.unregister(job_id)


# ============================================================
# Trabajo: procesar archivo de Telegram
# ============================================================

async def procesar_archivo(client: Client, message: Message, original_name: str,
                           status_id: int, job_id_holder: dict):
    original_name = sanitize_filename(original_name)
    ext = os.path.splitext(original_name)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    job_id = await job_manager.register(
        asyncio.current_task(), message.chat.id, status_id, temp_path
    )
    job_id_holder["job_id"] = job_id

    async with job_manager.semaphore():
        try:
            # 1) Descarga desde Telegram
            async def on_dl(current, total):
                if total:
                    pct = int(current / total * 100)
                    await edit_status(
                        client, message.chat.id, status_id,
                        f"┎ DOWNLOADING FROM TELEGRAM\n"
                        f"┠ [{progress_bar(pct)}]\n"
                        f"┠ PERCENTAGE: {pct}%\n"
                        f"┖ SIZE: {format_size(current)}/{format_size(total)}"
                    )

            await message.download(file_name=temp_path, progress=on_dl)
            size = os.path.getsize(temp_path)

            # 2) Subida a S3
            await edit_status(client, message.chat.id, status_id, "UPLOADING...", force=True)

            async def on_up(sent, total):
                pct = int(sent / total * 100) if total else 0
                await edit_status(
                    client, message.chat.id, status_id,
                    f"┎ UPLOADING\n"
                    f"┠ [{progress_bar(pct)}]\n"
                    f"┠ PERCENTAGE: {pct}%\n"
                    f"┖ SIZE: {format_size(sent)}/{format_size(total)}"
                )

            upload_url = await subir_a_s3(temp_path, original_name, size, on_up)

            # 3) Mensaje final
            name = os.path.splitext(original_name)[0].replace("_", " ")
            ext_clean = ext.replace(".", "")
            await client.edit_message_text(
                message.chat.id, status_id,
                f"┎ NAME: {name}\n"
                f"┠ EXTENSION: {ext_clean}\n"
                f"┠ SIZE: {format_size(size)}\n"
                f"┖ URL: {upload_url}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📥 DESCARGAR", url=upload_url)
                ]]),
            )

        except asyncio.CancelledError:
            log.info(f"Trabajo {job_id} cancelado (archivo)")
            try:
                await client.edit_message_text(message.chat.id, status_id, "❌ CANCELADO")
            except Exception:
                pass
            raise
        except Exception as e:
            log.exception("error procesando archivo")
            try:
                await client.edit_message_text(
                    message.chat.id, status_id, f"ERROR: {str(e)[:200]}"
                )
            except Exception:
                pass
        finally:
            try:
                os.unlink(temp_path)
            except Exception:
                pass
            await job_manager.unregister(job_id)


# ============================================================
# Handlers
# ============================================================

@app.on_message(filters.command("start"))
async def cmd_start(client: Client, message: Message):
    await message.reply_text(
        "**Bot de subida a Todus S3**\n\n"
        "Envíame un enlace de descarga directa o un archivo (hasta 800 MB).\n"
        "El archivo se sube a `s3.todus.cu/stream` y te devuelvo el enlace público.\n\n"
        "Comandos:\n"
        "• /start — este mensaje\n"
        "• /cancel — detener los procesos en curso\n"
        "• /status — ver trabajos activos"
    )


@app.on_message(filters.command("cancel"))
async def cmd_cancel(client: Client, message: Message):
    cancelled = await job_manager.cancel_all()
    if not cancelled:
        await message.reply_text("ℹ️ No hay procesos en curso.")
        return
    await message.reply_text(f"❌ Cancelando {len(cancelled)} proceso(s)...")


@app.on_message(filters.command("status"))
async def cmd_status(client: Client, message: Message):
    n = job_manager.active_count
    if n == 0:
        await message.reply_text("ℹ️ No hay trabajos activos.")
    else:
        await message.reply_text(f"⚙️ {n} trabajo(s) activo(s) (límite: {MAX_CONCURRENT_JOBS}).")


@app.on_message(filters.text & ~filters.command(["start", "cancel", "status"]))
async def handle_text(client: Client, message: Message):
    match = URL_RE.search(message.text.strip())
    if not match:
        await message.reply_text(
            "Envíame un enlace de descarga directa o un archivo (hasta 2 GB)."
        )
        return

    url = match.group(1)
    status = await message.reply_text("PROCESSING...")

    holder: dict = {}
    task = asyncio.create_task(procesar_url(client, message, url, status.id, holder))
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.exception(f"tarea URL falló: {e}")


@app.on_message(
    filters.document | filters.video | filters.audio | filters.voice |
    filters.video_note | filters.animation | filters.sticker | filters.photo
)
async def handle_media(client: Client, message: Message):
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

    status = await message.reply_text("PROCESSING...")
    holder: dict = {}
    task = asyncio.create_task(
        procesar_archivo(client, message, original_name, status.id, holder)
    )
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.exception(f"tarea archivo falló: {e}")


# ============================================================
# Health server (Render requiere bind en 0.0.0.0 y $PORT)
# ============================================================

START_TIME = time.time()


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "healthy",
        "uptime": round(time.time() - START_TIME, 1),
        "jobs_active": job_manager.active_count,
        "jobs_limit": MAX_CONCURRENT_JOBS,
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
    site = web.TCPSite(runner, "0.0.0.0", PORT)   # Render requiere 0.0.0.0
    await site.start()
    log.info(f"Health server escuchando en 0.0.0.0:{PORT}")
    return runner


# ============================================================
# Keepalive para evitar el sleep de Render (plan gratis)
# ============================================================

async def keepalive():
    base_interval = 600       # 10 min
    max_interval = 3600       # 1h si falla repetido

    async with aiohttp.ClientSession() as session:
        # Espera inicial para que Render termine de levantar
        await asyncio.sleep(60)

        while True:
            try:
                async with session.get(f"{SELF_URL}/health", timeout=15) as r:
                    if r.status == 200:
                        base_interval = 600
                        log.info(f"keepalive OK")
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
    keepalive_task = asyncio.create_task(keepalive())
    log.info("BOT READY")
    try:
        await asyncio.Event().wait()
    finally:
        keepalive_task.cancel()
        await app.stop()
        await web_runner.cleanup()


if __name__ == "__main__":
    app.run(main())