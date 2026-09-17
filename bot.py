import os
import re
import time
import uuid
import asyncio
import logging
import threading

import aiohttp
from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message

# ----------------- Configuración -----------------
BOT_TOKEN = "8611512607:AAFYiZUGWn6r8Ehp9YWCHFUG2hZ2hA01CDw"
API_ID = 32471788
API_HASH = "cb57130abda56877acf3b3027e569450"

S3 = "https://s3.todus.cu/stream"
DOWNLOAD_PATH = "/tmp/todus_uploads"
MAX_FILE_SIZE = 2000 * 1024 * 1024  # 2 GB
SELF_URL = "https://s3-bot-r85n.onrender.com"  # cámbialo por tu URL real

os.makedirs(DOWNLOAD_PATH, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("bot")

# ----------------- Cliente Pyrogram (MTProto) -----------------
app = Client(
    "todus_bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=8,
)

# ----------------- Utilidades -----------------
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


def get_filename_from_url(url: str) -> str:
    try:
        from urllib.parse import urlparse, unquote
        name = os.path.basename(urlparse(url).path)
        if name and len(name) > 2:
            return unquote(name)
    except Exception:
        pass
    return None


# ----------------- Estado de los mensajes con throttle -----------------
class EditState:
    def __init__(self):
        self.last_sent = 0.0
        self.last_text = ""

edit_locks = {}

async def edit_status(client: Client, chat_id: int, msg_id: int, text: str, force: bool = False):
    """Edita el mensaje con throttle de ~1.2s para no chocar con el rate limit."""
    key = chat_id
    st = edit_locks.setdefault(key, EditState())
    now = time.time()
    if not force and now - st.last_sent < 1.2:
        return
    if text == st.last_text and not force:
        return
    try:
        await client.edit_message_text(chat_id, msg_id, text)
        st.last_sent = now
        st.last_text = text
    except Exception as e:
        log.warning(f"edit falló: {e}")


# ----------------- Subida a S3 -----------------
async def subir_a_s3(session: aiohttp.ClientSession, temp_path: str, filename: str, size: int,
                     on_progress=None) -> str:
    remote = f"{uuid.uuid4().hex[:8]}_{filename}"
    upload_url = f"{S3}/{remote}"

    sent = 0
    last_pct = [-1]

    async def file_reader():
        nonlocal sent
        chunk_size = 64 * 1024
        with open(temp_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sent += len(chunk)
                if on_progress:
                    pct = int(sent / size * 100) if size else 0
                    if pct - last_pct[0] >= 5 or pct == 100:
                        last_pct[0] = pct
                        await on_progress(sent, size)
                yield chunk

    headers = {
        "Content-Length": str(size),
        "Content-Type": "application/octet-stream",
    }

    async with session.put(upload_url, data=file_reader(), headers=headers) as resp:
        if resp.status >= 400:
            body = await resp.text()
            raise RuntimeError(f"S3 {resp.status}: {body[:200]}")

    return upload_url


# ----------------- Manejadores -----------------
@app.on_message(filters.command("start"))
async def cmd_start(client: Client, message: Message):
    await message.reply_text(
        "Envíame un enlace de descarga directa o un archivo (hasta 2 GB)."
    )


@app.on_message(filters.text & ~filters.command(["start"]))
async def handle_text(client: Client, message: Message):
    match = URL_RE.search(message.text.strip())
    if not match:
        await message.reply_text(
            "Envíame un enlace de descarga directa o un archivo (hasta 2 GB)."
        )
        return

    url = match.group(1)
    status = await message.reply_text("PROCESSING...")

    filename = get_filename_from_url(url) or f"file_{int(time.time())}"
    ext = os.path.splitext(filename)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}")
                total = int(resp.headers.get("Content-Length", 0))
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
                                    client, message.chat.id, status.id,
                                    f"┎ DOWNLOADING\n┠ [{progress_bar(pct)}]\n"
                                    f"┠ PERCENTAGE: {pct}%\n"
                                    f"┖ SIZE: {format_size(downloaded)}/{format_size(total)}"
                                )

            size = os.path.getsize(temp_path)
            await edit_status(client, message.chat.id, status.id, "UPLOADING...", force=True)

            async def on_up(sent, total):
                pct = int(sent / total * 100) if total else 0
                await edit_status(
                    client, message.chat.id, status.id,
                    f"┎ UPLOADING\n┠ [{progress_bar(pct)}]\n"
                    f"┠ PERCENTAGE: {pct}%\n"
                    f"┖ SIZE: {format_size(sent)}/{format_size(total)}"
                )

            upload_url = await subir_a_s3(session, temp_path, filename, size, on_up)

        name = os.path.splitext(filename)[0].replace("_", " ")
        await client.edit_message_text(
            message.chat.id, status.id,
            f"┎ NAME: {name}\n┠ EXTENSION: {ext.replace('.', '')}\n"
            f"┠ SIZE: {format_size(size)}\n┖ URL: {upload_url}"
        )
    except Exception as e:
        log.exception("error procesando URL")
        await client.edit_message_text(
            message.chat.id, status.id, f"ERROR: {str(e)[:200]}"
        )
    finally:
        try:
            os.unlink(temp_path)
        except Exception:
            pass


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
        # Nombre por defecto según el tipo
        if message.photo:
            original_name = f"photo_{int(time.time())}.jpg"
        elif message.video:
            original_name = f"video_{int(time.time())}.mp4"
        elif message.audio:
            original_name = f"audio_{int(time.time())}.mp3"
        elif message.voice:
            original_name = f"voice_{int(time.time())}.ogg"
        elif message.video_note:
            original_name = f"video_note_{int(time.time())}.mp4"
        elif message.animation:
            original_name = f"animation_{int(time.time())}.mp4"
        elif message.sticker:
            original_name = f"sticker_{int(time.time())}.webp"
        else:
            original_name = f"file_{int(time.time())}.bin"

    ext = os.path.splitext(original_name)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")

    status = await message.reply_text("PROCESSING...")

    try:
        # Descarga vía MTProto (hasta 2 GB)
        async def on_dl(current, total):
            if total:
                pct = int(current / total * 100)
                await edit_status(
                    client, message.chat.id, status.id,
                    f"┎ DOWNLOADING FROM TELEGRAM\n┠ [{progress_bar(pct)}]\n"
                    f"┠ PERCENTAGE: {pct}%\n"
                    f"┖ SIZE: {format_size(current)}/{format_size(total)}"
                )

        await message.download(file_name=temp_path, progress=on_dl)
        size = os.path.getsize(temp_path)

        await edit_status(client, message.chat.id, status.id, "UPLOADING...", force=True)

        async with aiohttp.ClientSession() as session:
            async def on_up(sent, total):
                pct = int(sent / total * 100) if total else 0
                await edit_status(
                    client, message.chat.id, status.id,
                    f"┎ UPLOADING\n┠ [{progress_bar(pct)}]\n"
                    f"┠ PERCENTAGE: {pct}%\n"
                    f"┖ SIZE: {format_size(sent)}/{format_size(total)}"
                )

            upload_url = await subir_a_s3(session, temp_path, original_name, size, on_up)

        name = os.path.splitext(original_name)[0].replace("_", " ")
        await client.edit_message_text(
            message.chat.id, status.id,
            f"┎ NAME: {name}\n┠ EXTENSION: {ext.replace('.', '')}\n"
            f"┠ SIZE: {format_size(size)}\n┖ URL: {upload_url}"
        )
    except Exception as e:
        log.exception("error procesando archivo")
        await client.edit_message_text(
            message.chat.id, status.id, f"ERROR: {str(e)[:200]}"
        )
    finally:
        try:
            os.unlink(temp_path)
        except Exception:
            pass


# ----------------- Servidor web para Render -----------------
flask_app = Flask(__name__)


@flask_app.route("/")
def root():
    return {"status": "online"}


@flask_app.route("/health")
def health():
    return {"status": "healthy"}


def run_web():
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port)


# ----------------- Auto-ping para Render -----------------
async def keepalive():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(f"{SELF_URL}/health", timeout=10) as r:
                    log.info(f"keepalive {r.status}")
            except Exception as e:
                log.warning(f"keepalive falló: {e}")
            await asyncio.sleep(10 * 60)


# ----------------- Arranque -----------------
async def main():
    # Servidor web en hilo aparte
    threading.Thread(target=run_web, daemon=True).start()

    # Cliente Pyrogram
    await app.start()

    # Tarea de keep-alive
    asyncio.create_task(keepalive())

    log.info("BOT READY")
    await asyncio.Event().wait()  # mantener vivo


if __name__ == "__main__":
    app.run(main())