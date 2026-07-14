"""
Telegram Userbot → Google Drive Streaming Uploader
===================================================
Streams files from Telegram directly to Google Drive without ever
writing to disk or loading an entire file into RAM.

Designed for Render.com free tier (512 MB RAM hard limit).

Architecture
------------
  Telegram ──iter_download(5 MB chunks)──▶ bytearray buffer
  buffer   ──resumable PUT──────────────▶ Google Drive API

Environment Variables
---------------------
  TELEGRAM_API_ID          – Telegram API ID (integer)
  TELEGRAM_API_HASH        – Telegram API Hash
  TELEGRAM_STRING_SESSION   – Telethon StringSession export
  GOOGLE_PARENT_FOLDER_ID  – Root Drive folder shared with SA
  GOOGLE_CREDENTIALS_JSON  – Full JSON content of SA key file
  PORT                     – (optional) HTTP port for health checks
"""

import os
import json
import asyncio
import logging
import mimetypes

import aiohttp
from aiohttp import web
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeFilename
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleAuthRequest


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
)
logger = logging.getLogger("drive-bot")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIGURATION (from environment variables)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_STR = os.environ["TELEGRAM_STRING_SESSION"]
PARENT_FOLDER_ID = os.environ["GOOGLE_PARENT_FOLDER_ID"]
SA_INFO = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])

CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB – must be a multiple of 256 KiB
DRIVE_API = "https://www.googleapis.com"
SCOPES = ["https://www.googleapis.com/auth/drive"]
MAX_RETRIES = 3


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FILE CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_VIDEO_EXT = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts"}
_AUDIO_EXT = {".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a", ".opus"}
_DOC_EXT = {
    ".pdf", ".docx", ".doc", ".txt", ".xlsx", ".xls",
    ".pptx", ".ppt", ".csv", ".rtf", ".odt", ".ods", ".epub",
}

# Maps category key → Hebrew subfolder name
CATEGORY_FOLDERS = {
    "video": "וידאו",
    "audio": "מוזיקה",
    "documents": "מסמכים",
    "other": "אחר",
}


def classify_file(filename: str | None, mime_type: str | None) -> str:
    """Return a category key based on MIME type or file extension."""
    # 1) Check MIME type first
    if mime_type:
        if mime_type.startswith("video/"):
            return "video"
        if mime_type.startswith("audio/"):
            return "audio"

    # 2) Fall back to extension
    if filename:
        ext = os.path.splitext(filename)[1].lower()
        if ext in _VIDEO_EXT:
            return "video"
        if ext in _AUDIO_EXT:
            return "audio"
        if ext in _DOC_EXT:
            return "documents"

    return "other"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  GOOGLE AUTH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_creds: service_account.Credentials | None = None


def _auth_headers() -> dict[str, str]:
    """Return Authorization headers, refreshing the token if needed."""
    global _creds
    if _creds is None:
        _creds = service_account.Credentials.from_service_account_info(
            SA_INFO, scopes=SCOPES
        )
    if not _creds.valid:
        _creds.refresh(GoogleAuthRequest())
    return {"Authorization": f"Bearer {_creds.token}"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  GOOGLE DRIVE HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_folder_cache: dict[str, str] = {}


async def ensure_subfolder(
    session: aiohttp.ClientSession,
    folder_name: str,
    parent_id: str,
) -> str:
    """Find a subfolder by name inside *parent_id*, creating it if absent."""
    if folder_name in _folder_cache:
        return _folder_cache[folder_name]

    headers = _auth_headers()

    # Search for existing folder
    query = (
        f"name='{folder_name}' and '{parent_id}' in parents "
        f"and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    async with session.get(
        f"{DRIVE_API}/drive/v3/files",
        headers=headers,
        params={"q": query, "fields": "files(id)", "pageSize": "1"},
    ) as resp:
        data = await resp.json()
        if data.get("files"):
            _folder_cache[folder_name] = data["files"][0]["id"]
            logger.info("Found folder '%s' → %s", folder_name, _folder_cache[folder_name])
            return _folder_cache[folder_name]

    # Create the folder
    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    async with session.post(
        f"{DRIVE_API}/drive/v3/files",
        headers={**headers, "Content-Type": "application/json"},
        json=metadata,
    ) as resp:
        resp.raise_for_status()
        folder_id = (await resp.json())["id"]
        _folder_cache[folder_name] = folder_id
        logger.info("Created folder '%s' → %s", folder_name, folder_id)
        return folder_id


async def _init_resumable_upload(
    session: aiohttp.ClientSession,
    filename: str,
    mime_type: str,
    folder_id: str,
    total_size: int,
) -> str:
    """Initiate a resumable upload and return the upload URI."""
    headers = _auth_headers()
    headers["Content-Type"] = "application/json; charset=UTF-8"
    headers["X-Upload-Content-Type"] = mime_type
    headers["X-Upload-Content-Length"] = str(total_size)

    async with session.post(
        f"{DRIVE_API}/upload/drive/v3/files?uploadType=resumable",
        headers=headers,
        json={"name": filename, "parents": [folder_id]},
    ) as resp:
        resp.raise_for_status()
        return resp.headers["Location"]


async def _upload_chunk(
    session: aiohttp.ClientSession,
    upload_url: str,
    data: bytes,
    offset: int,
    total_size: int,
) -> dict | None:
    """
    PUT a single chunk to the resumable upload URL.

    Returns the file metadata dict on the final chunk (HTTP 200/201),
    or None on an intermediate chunk (HTTP 308 Resume Incomplete).
    """
    end = offset + len(data) - 1
    headers = {
        "Content-Length": str(len(data)),
        "Content-Range": f"bytes {offset}-{end}/{total_size}",
    }
    async with session.put(upload_url, headers=headers, data=data) as resp:
        if resp.status in (200, 201):
            return await resp.json()
        if resp.status == 308:
            return None
        body = await resp.text()
        raise RuntimeError(f"Drive upload failed ({resp.status}): {body}")


async def _upload_chunk_with_retry(
    session: aiohttp.ClientSession,
    upload_url: str,
    data: bytes,
    offset: int,
    total_size: int,
) -> dict | None:
    """Wrapper around _upload_chunk with exponential-backoff retries."""
    for attempt in range(MAX_RETRIES):
        try:
            return await _upload_chunk(session, upload_url, data, offset, total_size)
        except Exception as exc:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = 2 ** attempt
            logger.warning(
                "Chunk upload failed (attempt %d/%d), retrying in %ds: %s",
                attempt + 1, MAX_RETRIES, wait, exc,
            )
            await asyncio.sleep(wait)
    return None  # unreachable, but keeps type checkers happy


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STREAMING PIPELINE: Telegram → Google Drive
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Limit concurrent uploads to keep memory predictable.
# 2 uploads × 5 MB buffer each = ~10 MB worst case
_upload_sem = asyncio.Semaphore(2)


async def stream_to_drive(
    tg_client: TelegramClient,
    message,
    filename: str,
    mime_type: str,
    file_size: int,
    folder_id: str,
) -> dict | None:
    """
    Stream a file from Telegram to Google Drive chunk-by-chunk.

    At any given moment, at most one CHUNK_SIZE buffer is held in RAM
    per upload, keeping total memory usage well under 512 MB.
    """
    async with _upload_sem:
        async with aiohttp.ClientSession() as http:
            upload_url = await _init_resumable_upload(
                http, filename, mime_type, folder_id, file_size,
            )
            logger.info("⬆  Resumable upload started: %s (%s bytes)", filename, f"{file_size:,}")

            buf = bytearray()
            offset = 0

            async for chunk in tg_client.iter_download(
                message.media, chunk_size=CHUNK_SIZE
            ):
                buf.extend(chunk)

                # Flush full chunks to Drive
                while len(buf) >= CHUNK_SIZE:
                    piece = bytes(buf[:CHUNK_SIZE])
                    del buf[:CHUNK_SIZE]
                    result = await _upload_chunk_with_retry(
                        http, upload_url, piece, offset, file_size,
                    )
                    offset += len(piece)
                    pct = offset * 100 // file_size
                    logger.info("   %s – %d%% (%s / %s bytes)",
                                filename, pct, f"{offset:,}", f"{file_size:,}")
                    if result:
                        return result

            # Flush remaining bytes (last chunk, may be < CHUNK_SIZE)
            if buf:
                result = await _upload_chunk_with_retry(
                    http, upload_url, bytes(buf), offset, file_size,
                )
                offset += len(buf)
                logger.info("   %s – 100%% (%s / %s bytes) [final]",
                            filename, f"{offset:,}", f"{file_size:,}")
                return result

    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TELEGRAM CLIENT & EVENT HANDLER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)


def extract_file_info(message) -> tuple[str | None, str | None, int | None]:
    """
    Extract (filename, mime_type, file_size) from a Telegram message.
    Returns (None, None, None) if the message has no downloadable file.
    """
    # ── Documents (includes video/audio sent as files) ──
    if message.document:
        doc = message.document
        mime = doc.mime_type or "application/octet-stream"
        size = doc.size

        # Try to find the original filename
        name = None
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                name = attr.file_name
                break
        if not name:
            ext = mimetypes.guess_extension(mime) or ""
            name = f"file_{doc.id}{ext}"

        return name, mime, size

    # ── Photos ──
    if message.photo:
        photo = message.photo
        # Approximate size from the largest photo size object
        size = 0
        for s in photo.sizes:
            s_size = getattr(s, "size", 0) or 0
            if s_size > size:
                size = s_size
        name = f"photo_{photo.id}.jpg"
        return name, "image/jpeg", size if size > 0 else None

    return None, None, None


@client.on(events.NewMessage(incoming=True))
async def on_new_file(event):
    """Handle every incoming message – upload its media to Drive."""
    message = event.message
    if not message.media:
        return

    filename, mime_type, file_size = extract_file_info(message)
    if not filename or not file_size:
        return  # skip non-file media (contacts, polls, etc.)

    category = classify_file(filename, mime_type)
    folder_name = CATEGORY_FOLDERS[category]

    logger.info(
        "📥 Received: %s (%s bytes) → category '%s'",
        filename, f"{file_size:,}", category,
    )

    try:
        # Resolve target subfolder (uses cache after first call)
        async with aiohttp.ClientSession() as http:
            target_folder_id = await ensure_subfolder(
                http, folder_name, PARENT_FOLDER_ID,
            )

        # Notify the user that the upload is starting
        status_msg = await event.reply(
            f"📤 מעלה את **{filename}** לתיקייה **{folder_name}**…"
        )

        # Stream from Telegram → Google Drive
        result = await stream_to_drive(
            client, message, filename, mime_type, file_size, target_folder_id,
        )

        if result:
            file_id = result.get("id", "?")
            logger.info("✅ Upload complete: %s → Drive ID %s", filename, file_id)
            await status_msg.edit(
                f"✅ **{filename}** הועלה בהצלחה לתיקייה **{folder_name}**"
            )
        else:
            logger.warning("⚠️ Upload finished without confirmation: %s", filename)
            await status_msg.edit(
                f"⚠️ העלאה של **{filename}** הסתיימה ללא אישור מגוגל"
            )

    except Exception as exc:
        logger.exception("❌ Upload failed for %s", filename)
        try:
            await event.reply(f"❌ שגיאה בהעלאת **{filename}**: `{exc}`")
        except Exception:
            pass  # If replying also fails, just log it


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HEALTH-CHECK HTTP SERVER (keeps Render web-service alive)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _health_handler(_request):
    return web.Response(text="OK")


async def start_health_server():
    """Start a minimal HTTP server for Render health checks."""
    app = web.Application()
    app.router.add_get("/", _health_handler)
    app.router.add_get("/health", _health_handler)
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info("🌐 Health-check server listening on port %d", port)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ENTRYPOINT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def main():
    # 1) Start health-check server (Render requires a listening port)
    await start_health_server()

    # 2) Connect to Telegram
    await client.start()
    me = await client.get_me()
    logger.info("🤖 Logged in as %s (ID: %d)", me.first_name, me.id)

    # 3) Pre-warm the subfolder cache so the first upload is fast
    async with aiohttp.ClientSession() as http:
        for _cat, folder_name in CATEGORY_FOLDERS.items():
            await ensure_subfolder(http, folder_name, PARENT_FOLDER_ID)
    logger.info("📂 Subfolder cache: %s", _folder_cache)

    # 4) Run forever, listening for incoming files
    logger.info("👂 Listening for incoming files…")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
