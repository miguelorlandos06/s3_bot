"""
Bot de subida a Todus S3 con multipart upload - PÚBLICO - Render (plan gratis).

Requisitos:
    Python 3.11+

Notas Render:
    - Solo escribe en /tmp y working dir (todo efímero entre deploys)
    - Health server debe escuchar en 0.0.0.0:$PORT
    - Keepalive cada 10 min para evitar el sleep
    - 512 MB RAM -> concurrencia y chunks reducidos

Seguridad:
    - Cualquiera puede usar el bot (chats privados)
    - /cancel y /status son por usuario
    - Bloqueo de SSRF
    - Rate limit por usuario (jobs concurrentes + ventana temporal)
"""

from __future__ import annotations

import os
import re
import time
import uuid
import shutil
import socket
import asyncio
import ipaddress
import logging
from collections import defaultdict, deque
from urllib.parse import urlparse, unquote, quote

import aiofiles
import aiohttp
import aioboto3
from aiohttp import web
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton


# ============================================================
# Configuración
# ============================================================

BOT_TOKEN = "8942582638:AAF1MQGDLVPqbLxwKX3RbEeLDYfHzUzp5I"
API_ID = 32471788
API_HASH = "cb57130abda56877acf3b3027e569450"

S3_ENDPOINT = "https://s3.todus.cu"
S3_BUCKET = "stream"
S3_REGION = "us-east-1"

# Rutas (Render solo permite /tmp y working dir)
DOWNLOAD_PATH = "/tmp/todus_uploads"
SESSION_DIR = "/tmp/todus_session"
SESSION_NAME = "todus_bot"

# Render asigna el puerto dinámicamente y necesita bind en 0.0.0.0
PORT = int(os.environ.get("PORT", 10000))
SELF_URL = os.environ.get("SELF_URL", "https://s3-bot-r85n.onrender.com")

MAX_FILE_SIZE = 500 * 1024 * 1024     # 500 MB (Render free: /tmp limitado a ~512MB)
MAX_CONCURRENT_JOBS = 1                # 512 MB RAM
MAX_JOBS_PER_USER = 1
RATE_LIMIT_WINDOW = 3600               # 1 hora
RATE_LIMIT_MAX_JOBS = 10               # 10 por hora por usuario

os.makedirs(DOWNLOAD_PATH, exist_ok=True)
os.makedirs(SESSION_DIR, exist_ok=True)


# ============================================================
# Logging (solo stdout, Render lo captura)
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
# Configuración S3 (ajustada a RAM limitada de Render free)
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
    max_concurrency=2,                        # poco paralelismo por RAM
    use_threads=True,
)

_s3_session = aioboto3.Session()
_s3_client_cm = None
_s3_client = None


async def init_s3_client():
    global _s3_client_cm, _s3_client
    _s3_client_cm = _s3_session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        config=_S3_CONFIG,
    )
    _s3_client = await _s3_client_cm.__aenter__()
    log.info("Cliente S3 global inicializado")


async def close_s3_client():
    global _s3_client_cm, _s3_client
    if _s3_client_cm is not None:
        try:
            await _s3_client_cm.__aexit__(None, None, None)
        except Exception as e:
            log.warning(f"Cierre S3: {e}")
    _s3_client = None
    _s3_client_cm = None


# ============================================================
# Rate limiter por usuario (ventana deslizante)
# ============================================================

class RateLimiter:
    def __init__(self, window: int, max_jobs: int):
        self.window = window
        self.max_jobs = max_jobs
        self._events: dict[int, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def check_and_record(self, user_id: int) -> tuple[bool, int]:
        async with self._lock:
            now = time.time()
            dq = self._events[user_id]
            while dq and now - dq[0] > self.window:
                dq.popleft()
            if len(dq) >= self.max_jobs:
                retry_after = int(self.window - (now - dq[0])) + 1
                return False, max(retry_after, 1)
            dq.append(now)
            return True, 0


rate_limiter = RateLimiter(RATE_LIMIT_WINDOW, RATE_LIMIT_MAX_JOBS)


# ============================================================
# Job Manager
# ============================================================

class JobManager:
    def __init__(self):
        self._jobs: dict[str, dict] = {}
        self._user_jobs: dict[int, int] = {}
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

    async def can_accept(self, user_id: int) -> bool:
        async with self._lock:
            return self._user_jobs.get(user_id, 0) < MAX_JOBS_PER_USER

    async def register(self, task: asyncio.Task, user_id: int, chat_id: int,
                       msg_id: int, temp_path: str) -> str:
        async with self._lock:
            job_id = uuid.uuid4().hex
            self._jobs[job_id] = {
                "task": task,
                "user_id": user_id,
                "chat_id": chat_id,
                "msg_id": msg_id,
                "temp_path": temp_path,
                "created_at": time.time(),
            }
            self._user_jobs[user_id] = self._user_jobs.get(user_id, 0) + 1
            return job_id

    async def unregister(self, job_id: str):
        async with self._lock:
            info = self._jobs.pop(job_id, None)
            if info:
                uid = info["user_id"]
                self._user_jobs[uid] = max(0, self._user_jobs.get(uid, 1) - 1)
                if self._user_jobs[uid] == 0:
                    self._user_jobs.pop(uid, None)

    async def cancel_all_for_user(self, user_id: int) -> list[dict]:
        async with self._lock:
            cancelled = []
            for job_id, info in list(self._jobs.items()):
                if info["user_id"] != user_id:
                    continue
                task = info["task"]
                if not task.done():
                    task.cancel()
                    cancelled.append(info)
                self._jobs.pop(job_id, None)
            self._user_jobs.pop(user_id, None)
            return cancelled

    async def active_count_for_user(self, user_id: int) -> int:
        async with self._lock:
            return self._user_jobs.get(user_id, 0)

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
    name = name.replace("/", "_").replace("\\", "_")
    name = re.sub(r"[\s?#&]+", "_", name)
    name = name.strip("._") or f"file_{int(time.time())}"
    return name


def check_disk_space():
    free = shutil.disk_usage(DOWNLOAD_PATH).free
    if free < MAX_FILE_SIZE:
        raise RuntimeError(
            f"Disco insuficiente: {format_size(free)} libres, "
            f"se necesitan {format_size(MAX_FILE_SIZE)}"
        )


# ---- Validación anti-SSRF ----

_BLOCKED_NETS = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return any(ip in net for net in _BLOCKED_NETS)


async def validate_url_target(url: str):
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise RuntimeError("Esquema no permitido (solo http/https)")
    host = parsed.hostname
    if not host:
        raise RuntimeError("URL sin host")
    try:
        ip = ipaddress.ip_address(host)
        if _is_blocked_ip(ip):
            raise RuntimeError(f"Host bloqueado: {host}")
        return
    except ValueError:
        pass
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except Exception as e:
        raise RuntimeError(f"DNS falló para {host}: {e}")
    for info in infos:
        sockaddr = info[4]
        addr = sockaddr[0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if _is_blocked_ip(ip):
            raise RuntimeError(f"URL apunta a rango bloqueado ({ip})")


# ============================================================
# Estado de edición con throttle y descarte fuera de orden
# ============================================================

class EditState:
    def __init__(self):
        self.last_sent = 0.0
        self.last_text = ""
        self.last_pct = -1


_edit_states: dict[int, EditState] = {}


async def edit_status(client: Client, chat_id: int, msg_id: int, text: str,
                      pct: int = -1, force: bool = False):
    state = _edit_states.setdefault(chat_id, EditState())

    now = time.time()
    if not force:
        if now - state.last_sent < 1.2:
            return
        if pct != -1 and pct < state.last_pct:
            return
        if text == state.last_text:
            return

    try:
        await client.edit_message_text(chat_id, msg_id, text)
        state.last_sent = now
        state.last_text = text
        if pct != -1:
            state.last_pct = pct
    except FloodWait as e:
        log.warning(f"FloodWait {e.value}s en edit_status")
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        log.debug(f"edit_status falló: {e}")


def clear_edit_state(chat_id: int):
    _edit_states.pop(chat_id, None)


# ============================================================
# Subida a S3 (cliente global)
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

    async with aiofiles.open(temp_path, "rb") as f:
        await _s3_client.upload_fileobj(
            f,
            S3_BUCKET,
            remote_key,
            ExtraArgs={"ContentType": "application/octet-stream"},
            Config=_TRANSFER_CONFIG,
            Callback=_progress_callback,
        )

    return f"{S3_ENDPOINT}/{S3_BUCKET}/{quote(remote_key)}"


# ============================================================
# Trabajo: procesar URL
# ============================================================

async def procesar_url(client: Client, message: Message, url: str,
                       status_id: int, user_id: int):
    filename = sanitize_filename(get_filename_from_url(url) or f"file_{int(time.time())}")
    ext = os.path.splitext(filename)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    job_id = await job_manager.register(
        asyncio.current_task(), user_id, message.chat.id, status_id, temp_path
    )

    async with job_manager.semaphore():
        try:
            check_disk_space()
            await validate_url_target(url)

            async with aiohttp.ClientSession() as session:
                async with asyncio.timeout(1800):
                    async with session.get(
                        url,
                        headers={"User-Agent": "Mozilla/5.0"},
                        allow_redirects=False,
                    ) as resp:
                        if resp.status in (301, 302, 303, 307, 308):
                            loc = resp.headers.get("Location", "")
                            raise RuntimeError(f"Redirección no permitida: {loc[:80]}")
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

                        async with aiofiles.open(temp_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(64 * 1024):
                                await f.write(chunk)
                                downloaded += len(chunk)
                                if downloaded > MAX_FILE_SIZE:
                                    raise RuntimeError(
                                        "Límite de tamaño superado durante la descarga"
                                    )
                                if total:
                                    pct = int(downloaded / total * 100)
                                    if pct - last_pct >= 5 or pct == 100:
                                        last_pct = pct
                                        await edit_status(
                                            client, message.chat.id, status_id,
                                            f"┎ DOWNLOADING\n"
                                            f"┠ [{progress_bar(pct)}]\n"
                                            f"┠ PERCENTAGE: {pct}%\n"
                                            f"┖ SIZE: {format_size(downloaded)}/{format_size(total)}",
                                            pct=pct,
                                        )

            size = os.path.getsize(temp_path)
            if size > MAX_FILE_SIZE:
                raise RuntimeError(f"Archivo {format_size(size)} supera el límite")

            await edit_status(
                client, message.chat.id, status_id, "UPLOADING...", force=True
            )

            async def on_up(sent, total):
                pct = int(sent / total * 100) if total else 0
                await edit_status(
                    client, message.chat.id, status_id,
                    f"┎ UPLOADING\n"
                    f"┠ [{progress_bar(pct)}]\n"
                    f"┠ PERCENTAGE: {pct}%\n"
                    f"┖ SIZE: {format_size(sent)}/{format_size(total)}",
                    pct=pct,
                )

            upload_url = await subir_a_s3(temp_path, filename, size, on_up)

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
                await client.edit_message_text(
                    message.chat.id, status_id, "❌ CANCELADO"
                )
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
                await asyncio.to_thread(os.unlink, temp_path)
            except Exception:
                pass
            clear_edit_state(message.chat.id)
            await job_manager.unregister(job_id)


# ============================================================
# Trabajo: procesar archivo de Telegram
# ============================================================

async def procesar_archivo(client: Client, message: Message, original_name: str,
                           status_id: int, user_id: int):
    original_name = sanitize_filename(original_name)
    ext = os.path.splitext(original_name)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    job_id = await job_manager.register(
        asyncio.current_task(), user_id, message.chat.id, status_id, temp_path
    )

    async with job_manager.semaphore():
        try:
            check_disk_space()

            async def on_dl(current, total):
                if total:
                    pct = int(current / total * 100)
                    await edit_status(
                        client, message.chat.id, status_id,
                        f"┎ DOWNLOADING FROM TELEGRAM\n"
                        f"┠ [{progress_bar(pct)}]\n"
                        f"┠ PERCENTAGE: {pct}%\n"
                        f"┖ SIZE: {format_size(current)}/{format_size(total)}",
                        pct=pct,
                    )

            await message.download(file_name=temp_path, progress=on_dl)
            size = os.path.getsize(temp_path)

            await edit_status(
                client, message.chat.id, status_id, "UPLOADING...", force=True
            )

            async def on_up(sent, total):
                pct = int(sent / total * 100) if total else 0
                await edit_status(
                    client, message.chat.id, status_id,
                    f"┎ UPLOADING\n"
                    f"┠ [{progress_bar(pct)}]\n"
                    f"┠ PERCENTAGE: {pct}%\n"
                    f"┖ SIZE: {format_size(sent)}/{format_size(total)}",
                    pct=pct,
                )

            upload_url = await subir_a_s3(temp_path, original_name, size, on_up)

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
                await client.edit_message_text(
                    message.chat.id, status_id, "❌ CANCELADO"
                )
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
                await asyncio.to_thread(os.unlink, temp_path)
            except Exception:
                pass
            clear_edit_state(message.chat.id)
            await job_manager.unregister(job_id)


# ============================================================
# Handlers (públicos)
# ============================================================

@app.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    await message.reply_text(
        "**Bot de subida a Todus S3**\n\n"
        f"Envíame un enlace de descarga directa o un archivo (hasta {format_size(MAX_FILE_SIZE)}).\n"
        "El archivo se sube a `s3.todus.cu/stream` y te devuelvo el enlace público.\n\n"
        "**Comandos:**\n"
        "• /start — este mensaje\n"
        "• /cancel — detener tus procesos en curso\n"
        "• /status — ver tus trabajos activos\n\n"
        f"**Límites:** 1 trabajo concurrente · "
        f"{RATE_LIMIT_MAX_JOBS} subidas por hora"
    )


@app.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(client: Client, message: Message):
    uid = message.from_user.id
    cancelled = await job_manager.cancel_all_for_user(uid)
    if not cancelled:
        await message.reply_text("ℹ️ No hay procesos en curso.")
        return
    await message.reply_text(f"❌ Cancelando {len(cancelled)} proceso(s)...")


@app.on_message(filters.command("status") & filters.private)
async def cmd_status(client: Client, message: Message):
    uid = message.from_user.id
    n_user = await job_manager.active_count_for_user(uid)
    n_total = job_manager.active_count
    if n_user == 0 and n_total == 0:
        await message.reply_text("ℹ️ No hay trabajos activos.")
    else:
        await message.reply_text(
            f"⚙️ Tus trabajos activos: {n_user}/{MAX_JOBS_PER_USER}\n"
            f"🌐 Total en el bot: {n_total}/{MAX_CONCURRENT_JOBS}"
        )


@app.on_message(filters.text & filters.private & ~filters.command(["start", "cancel", "status"]))
async def handle_text(client: Client, message: Message):
    uid = message.from_user.id

    match = URL_RE.search(message.text.strip())
    if not match:
        await message.reply_text(
            f"Envíame un enlace de descarga directa o un archivo (hasta {format_size(MAX_FILE_SIZE)})."
        )
        return

    allowed, retry_after = await rate_limiter.check_and_record(uid)
    if not allowed:
        await message.reply_text(
            f"⏳ Has alcanzado el límite de {RATE_LIMIT_MAX_JOBS} subidas por hora. "
            f"Vuelve a intentarlo en ~{retry_after // 60} min."
        )
        return

    if not await job_manager.can_accept(uid):
        await message.reply_text(
            f"⚠️ Ya tienes {MAX_JOBS_PER_USER} trabajo(s) en curso. "
            f"Usa /cancel o espera a que termine."
        )
        return

    url = match.group(1)
    status = await message.reply_text("PROCESSING...")
    task = asyncio.create_task(procesar_url(client, message, url, status.id, uid))
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.exception(f"tarea URL falló: {e}")


@app.on_message(
    filters.private & (
        filters.document | filters.video | filters.audio | filters.voice |
        filters.video_note | filters.animation | filters.sticker | filters.photo
    )
)
async def handle_media(client: Client, message: Message):
    uid = message.from_user.id

    media = (
        message.document or message.video or message.audio or message.voice or
        message.video_note or message.animation or message.sticker or
        message.photo
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

    allowed, retry_after = await rate_limiter.check_and_record(uid)
    if not allowed:
        await message.reply_text(
            f"⏳ Has alcanzado el límite de {RATE_LIMIT_MAX_JOBS} subidas por hora. "
            f"Vuelve a intentarlo en ~{retry_after // 60} min."
        )
        return

    if not await job_manager.can_accept(uid):
        await message.reply_text(
            f"⚠️ Ya tienes {MAX_JOBS_PER_USER} trabajo(s) en curso. "
            f"Usa /cancel o espera a que termine."
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
    task = asyncio.create_task(
        procesar_archivo(client, message, original_name, status.id, uid)
    )
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.exception(f"tarea archivo falló: {e}")


# ============================================================
# Health server (Render: bind 0.0.0.0 + $PORT)
# ============================================================

START_TIME = time.time()


async def health_handler(request: web.Request) -> web.Response:
    try:
        disk = shutil.disk_usage(DOWNLOAD_PATH)
        disk_info = {
            "total": format_size(disk.total),
            "used": format_size(disk.used),
            "free": format_size(disk.free),
        }
    except Exception:
        disk_info = None

    return web.json_response({
        "status": "healthy",
        "uptime": round(time.time() - START_TIME, 1),
        "jobs_active": job_manager.active_count,
        "jobs_limit": MAX_CONCURRENT_JOBS,
        "jobs_per_user_limit": MAX_JOBS_PER_USER,
        "rate_limit_per_hour": RATE_LIMIT_MAX_JOBS,
        "disk": disk_info,
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
# Keepalive para evitar el sleep de Render
# ============================================================

async def keepalive():
    base_interval = 600
    max_interval = 3600

    async with aiohttp.ClientSession() as session:
        await asyncio.sleep(60)  # deja que Render termine de levantar
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
# Limpieza de huérfanos al arranque
# ============================================================

def cleanup_orphans():
    try:
        count = 0
        for fname in os.listdir(DOWNLOAD_PATH):
            fpath = os.path.join(DOWNLOAD_PATH, fname)
            if os.path.isfile(fpath):
                try:
                    os.unlink(fpath)
                    count += 1
                except Exception:
                    pass
        if count:
            log.info(f"Limpieza de arranque: {count} archivo(s) huérfano(s) borrado(s)")
    except Exception as e:
        log.warning(f"cleanup_orphans falló: {e}")


# ============================================================
# Arranque
# ============================================================

async def main():
    cleanup_orphans()
    await init_s3_client()
    web_runner = await run_web()
    await app.start()
    keepalive_task = asyncio.create_task(keepalive())
    log.info("BOT READY (público, Render free)")
    try:
        await asyncio.Event().wait()
    finally:
        keepalive_task.cancel()
        await app.stop()
        await close_s3_client()
        await web_runner.cleanup()
        log.info("BOT STOPPED")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupción manual")