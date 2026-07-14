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
from google.oauth2.credentials import Credentials as GoogleCredentials
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

# Google User OAuth2 configuration
GOOGLE_CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]

# Password authorization configuration
BOT_PASSWORD = os.environ.get("BOT_PASSWORD", "1234")  # Default placeholder if not specified

CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB – must be a multiple of 256 KiB
DRIVE_API = "https://www.googleapis.com"
SCOPES = ["https://www.googleapis.com/auth/drive"]
MAX_RETRIES = 3

# In-memory auth databases
_authorized_users: set[int] = set()
_failed_attempts: dict[str, int] = {}  # user_id (string) -> count (int)
_system_folder_id: str | None = None
_auth_file_drive_id: str | None = None


async def load_auth_data_from_drive(session: aiohttp.ClientSession):
    """Load authorized users and failed attempts from Google Drive."""
    global _authorized_users, _failed_attempts, _system_folder_id, _auth_file_drive_id
    headers = _auth_headers()
    
    # 1. Ensure the system folder "מערכת הבוט" exists
    try:
        _system_folder_id = await ensure_subfolder(session, "מערכת הבוט", PARENT_FOLDER_ID)
    except Exception as e:
        logger.error("❌ Failed to ensure system folder: %s", e)
        return

    # 2. Search for authorized_users.json inside "מערכת הבוט"
    query = (
        f"name='authorized_users.json' and '{_system_folder_id}' in parents "
        f"and mimeType='application/json' and trashed=false"
    )
    
    try:
        async with session.get(
            f"{DRIVE_API}/drive/v3/files",
            headers=headers,
            params={"q": query, "fields": "files(id)", "pageSize": "1"},
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            files = data.get("files", [])
            if files:
                _auth_file_drive_id = files[0]["id"]
                logger.info("Found system config file on Drive: ID %s", _auth_file_drive_id)
    except Exception as e:
        logger.error("❌ Error searching system config file: %s", e)
        return

    # 3. If file exists, download it
    if _auth_file_drive_id:
        try:
            async with session.get(
                f"{DRIVE_API}/drive/v3/files/{_auth_file_drive_id}?alt=media",
                headers=headers
            ) as resp:
                resp.raise_for_status()
                payload = await resp.json()
                _authorized_users = set(payload.get("authorized_users", []))
                _failed_attempts = payload.get("failed_attempts", {})
                logger.info("👥 Successfully loaded system config: %d authorized, %d failed attempt records", 
                            len(_authorized_users), len(_failed_attempts))
        except Exception as e:
            logger.error("❌ Failed to download system config file: %s", e)
    else:
        logger.info("ℹ️ System config file authorized_users.json not found on Drive. Will create on first authorization change.")


async def save_auth_data_to_drive():
    """Upload/Update the authorized users configuration JSON on Google Drive."""
    global _auth_file_drive_id
    if not _system_folder_id:
        logger.error("❌ System folder ID is missing. Cannot save auth data to Drive.")
        return

    headers = _auth_headers()
    payload = {
        "authorized_users": list(_authorized_users),
        "failed_attempts": _failed_attempts
    }
    
    # We serialize payload as a JSON formatted byte stream
    json_bytes = json.dumps(payload, indent=2).encode('utf-8')
    
    # 1. Update existing file if drive ID is known
    if _auth_file_drive_id:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.patch(
                    f"{DRIVE_API}/upload/drive/v3/files/{_auth_file_drive_id}?uploadType=media",
                    headers={
                        **headers,
                        "Content-Type": "application/json",
                        "Content-Length": str(len(json_bytes))
                    },
                    data=json_bytes
                ) as resp:
                    resp.raise_for_status()
                    logger.info("💾 System config successfully updated on Google Drive")
                    return
        except Exception as e:
            logger.error("❌ Failed to update system config on Google Drive: %s. Retrying via rewrite...", e)

    # 2. Create new file on Drive
    try:
        metadata = {
            "name": "authorized_users.json",
            "parents": [_system_folder_id]
        }
        
        boundary = "system_config_boundary"
        headers_multipart = {
            **headers,
            "Content-Type": f"multipart/related; boundary={boundary}"
        }
        
        # Construct RFC 2387 multipart body manually for Google Drive API
        multipart_body = (
            f"--{boundary}\r\n"
            f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
            f"{json.dumps(metadata)}\r\n"
            f"--{boundary}\r\n"
            f"Content-Type: application/json\r\n\r\n"
            f"{json.dumps(payload, indent=2)}\r\n"
            f"--{boundary}--\r\n"
        ).encode('utf-8')

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{DRIVE_API}/upload/drive/v3/files?uploadType=multipart",
                headers=headers_multipart,
                data=multipart_body
            ) as resp:
                if resp.status >= 400:
                    err = await resp.text()
                    logger.error("❌ Create config failed (%d): %s", resp.status, err)
                    return
                res_data = await resp.json()
                _auth_file_drive_id = res_data.get("id")
                logger.info("💾 Created new system config file on Google Drive: ID %s", _auth_file_drive_id)
    except Exception as e:
        logger.error("❌ Failed to create system config on Google Drive: %s", e)


async def authorize_user_and_save(user_id: int):
    """Mark user as authorized and save changes to Drive."""
    _authorized_users.add(user_id)
    await save_auth_data_to_drive()


async def record_failed_attempt_and_save(user_str: str, attempts_count: int):
    """Update failed attempt counter and save changes to Drive."""
    _failed_attempts[user_str] = attempts_count
    await save_auth_data_to_drive()


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

_creds: GoogleCredentials | None = None


def _auth_headers() -> dict[str, str]:
    """Return Authorization headers, refreshing the token if needed."""
    global _creds
    if _creds is None:
        _creds = GoogleCredentials(
            token=None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            scopes=SCOPES
        )
        logger.info("🔑 OAuth2 Credentials initialized")
    if not _creds.valid:
        logger.info("🔄 Refreshing Google OAuth2 token...")
        _creds.refresh(GoogleAuthRequest())
        logger.info("✅ Token refreshed, valid until %s", _creds.expiry)
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
    logger.info("🔍 Searching for folder '%s' in parent '%s'...", folder_name, parent_id)
    async with session.get(
        f"{DRIVE_API}/drive/v3/files",
        headers=headers,
        params={"q": query, "fields": "files(id)", "pageSize": "1"},
    ) as resp:
        if resp.status >= 400:
            error_body = await resp.text()
            logger.error("❌ Folder search failed (%d): %s", resp.status, error_body)
            raise RuntimeError(f"Folder search failed ({resp.status}): {error_body}")
        data = await resp.json()
        if data.get("files"):
            _folder_cache[folder_name] = data["files"][0]["id"]
            logger.info("✅ Found folder '%s' → %s", folder_name, _folder_cache[folder_name])
            return _folder_cache[folder_name]

    # Create the folder
    logger.info("📁 Folder '%s' not found, creating...", folder_name)
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
        if resp.status >= 400:
            error_body = await resp.text()
            logger.error("❌ Folder creation failed (%d): %s", resp.status, error_body)
            raise RuntimeError(f"Folder creation failed ({resp.status}): {error_body}")
        folder_id = (await resp.json())["id"]
        _folder_cache[folder_name] = folder_id
        logger.info("✅ Created folder '%s' → %s", folder_name, folder_id)
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
        if resp.status >= 400:
            error_body = await resp.text()
            logger.error(
                "❌ Resumable upload init failed (%d): %s", resp.status, error_body
            )
            raise RuntimeError(
                f"Drive upload init failed ({resp.status}): {error_body}"
            )
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
        logger.error(
            "❌ Chunk upload failed (%d) at offset %d: %s",
            resp.status, offset, body,
        )
        raise RuntimeError(f"Drive chunk upload failed ({resp.status}): {body}")


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
    status_msg=None,
) -> dict | None:
    """
    Stream a file from Telegram to Google Drive chunk-by-chunk.
    At any given moment, at most one CHUNK_SIZE buffer is held in RAM
    per upload, keeping total memory usage well under 512 MB.
    """
    async with _upload_sem:
        async with aiohttp.ClientSession() as http:
            logger.info(
                "⬆  Starting resumable upload: file='%s', mime='%s', size=%s, folder='%s'",
                filename, mime_type, f"{file_size:,}", folder_id,
            )
            upload_url = await _init_resumable_upload(
                http, filename, mime_type, folder_id, file_size,
            )
            logger.info("✅ Resumable upload URL obtained for '%s'", filename)

            buf = bytearray()
            offset = 0
            last_reported_pct = 0
            import time
            last_edit_time = time.time()

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
                    
                    if status_msg and (pct >= last_reported_pct + 10 or time.time() - last_edit_time > 4.0):
                        last_reported_pct = pct
                        last_edit_time = time.time()
                        try:
                            await status_msg.edit(
                                f"📤 מעלה את **{filename}**...\n"
                                f"⏳ התקדמות: **{pct}%** (`{offset // (1024*1024)}MB` / `{file_size // (1024*1024)}MB`)"
                            )
                        except Exception as tg_err:
                            logger.warning("⚠️ Could not update progress: %s", tg_err)

                    if result:
                        return result

            # Flush remaining bytes (last chunk or small file)
            if buf:
                piece = bytes(buf)
                result = await _upload_chunk_with_retry(
                    http, upload_url, piece, offset, file_size,
                )
                offset += len(piece)
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
    """Handle incoming messages – authenticate users and process files."""
    # Only process messages from private/direct chats
    if not event.is_private:
        return

    message = event.message
    sender = await event.get_sender()
    if not sender:
        return
    
    sender_id = sender.id
    sender_str = str(sender_id)
    
    # 1. Check if user is locked out
    attempts = _failed_attempts.get(sender_str, 0)
    if attempts >= 5:
        # Ignore locked out users completely
        return

    # 2. Check if this is a password submission (text only)
    if message.text and not message.media:
        text = message.text.strip()
        
        # If user is already authorized, ignore text messages
        if sender_id in _authorized_users:
            return

        if text == BOT_PASSWORD:
            # Correct password!
            await authorize_user_and_save(sender_id)
            if sender_str in _failed_attempts:
                await record_failed_attempt_and_save(sender_str, 0)
            try:
                await event.reply("✅ הסיסמה נכונה! גישתך להעלאת קבצים אושרה בהצלחה.")
            except Exception as e:
                logger.warning("Could not reply: %s", e)
            logger.info("🔓 User %s successfully authorized via password", sender_id)
            return
        elif len(text) < 50:  # Simple check to avoid checking long random messages or spam
            # Wrong password!
            new_attempts = attempts + 1
            await record_failed_attempt_and_save(sender_str, new_attempts)
            remaining = 5 - new_attempts
            
            try:
                if remaining <= 0:
                    await event.reply("❌ סיסמה שגויה. נחסמת משימוש בבוט עקב ריבוי ניסיונות.")
                    logger.warning("🔒 User %s locked out after 5 failed attempts", sender_id)
                else:
                    await event.reply(f"❌ סיסמה שגויה. נותרו לך עוד {remaining} ניסיונות.")
            except Exception as e:
                logger.warning("Could not reply: %s", e)
            return

    # 3. Check for media upload
    if not message.media:
        return

    filename, mime_type, file_size = extract_file_info(message)
    if not filename or not file_size:
        return  # skip non-file media

    # 4. Check if user is authorized to upload
    if sender_id not in _authorized_users:
        logger.info("⚠️ Unauthorized user %s tried to upload file '%s'", sender_id, filename)
        try:
            await event.reply(
                "🔒 מערכת מזהה שזו הפעם הראשונה שאתה משתמש בבוט.\n"
                "אם יש לך אישור גישה אנא הכנס את הסיסמה שקיבלת כדי לאשר את החשבון שלך."
            )
        except Exception as e:
            logger.warning("Could not reply: %s", e)
        return

    # User is authorized, proceed with upload
    category = classify_file(filename, mime_type)
    folder_name = CATEGORY_FOLDERS[category]

    logger.info(
        "📥 Received: %s (%s bytes) → category '%s' from user %s",
        filename, f"{file_size:,}", category, sender_id
    )

    # Send Read Acknowledge (✓✓)
    try:
        await event.client.send_read_acknowledge(event.chat_id, max_id=message.id)
        logger.info("✓✓ Message marked as read")
    except Exception as ack_err:
        logger.warning("⚠️ Could not mark message as read: %s", ack_err)

    try:
        # Step 1: Resolve target subfolder
        logger.info("[Step 1/3] Resolving subfolder '%s'...", folder_name)
        async with aiohttp.ClientSession() as http:
            target_folder_id = await ensure_subfolder(
                http, folder_name, PARENT_FOLDER_ID,
            )
        logger.info("[Step 1/3] ✅ Target folder: %s → %s", folder_name, target_folder_id)

        # Step 2: Notify user
        logger.info("[Step 2/3] Sending status message to Telegram...")
        status_msg = None
        try:
            status_msg = await event.reply(
                f"📤 מעלה את **{filename}** לתיקייה **{folder_name}**…"
            )
        except Exception as tg_err:
            logger.warning("⚠️ Could not send status reply message: %s", tg_err)

        # Step 3: Stream from Telegram → Google Drive
        logger.info("[Step 3/3] Starting stream: Telegram → Google Drive...")
        result = await stream_to_drive(
            client, message, filename, mime_type, file_size, target_folder_id, status_msg
        )

        if result:
            file_id = result.get("id", "?")
            logger.info("✅ Upload complete: %s → Drive ID %s", filename, file_id)
            if status_msg:
                try:
                    await status_msg.edit(
                        f"✅ **{filename}** הועלה בהצלחה לתיקייה **{folder_name}**"
                    )
                except Exception as tg_err:
                    logger.warning("⚠️ Could not edit status message: %s", tg_err)
        else:
            logger.warning("⚠️ Upload finished without confirmation: %s", filename)
            if status_msg:
                try:
                    await status_msg.edit(
                        f"⚠️ העלאה של **{filename}** הסתיימה ללא אישור מגוגל"
                    )
                except Exception as tg_err:
                    logger.warning("⚠️ Could not edit status message: %s", tg_err)

    except Exception as exc:
        logger.exception("❌ Upload FAILED for '%s'. Error type: %s, Details: %s",
                         filename, type(exc).__name__, exc)
        if status_msg:
            try:
                await status_msg.edit(f"❌ שגיאה בהעלאת **{filename}**: `{exc}`")
            except Exception:
                pass
        else:
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
    # Redacted log config
    logger.info("="*60)
    logger.info("🚀 TELEGRAM DRIVE BOT STARTING")
    logger.info("="*60)
    logger.info("Config: API_ID=%s, PARENT_FOLDER=%s", API_ID, PARENT_FOLDER_ID)
    logger.info("Config: OAuth client_id=%s...", GOOGLE_CLIENT_ID[:15] if GOOGLE_CLIENT_ID else "None")
    logger.info("Config: CHUNK_SIZE=%s MB", CHUNK_SIZE // (1024*1024))

    # 1) Start health-check server (Render requires a listening port)
    await start_health_server()

    # 2) Pre-warm caches and load auth data from Google Drive
    logger.info("📂 Loading system databases from Google Drive...")
    async with aiohttp.ClientSession() as http:
        # Load authorized users config
        await load_auth_data_from_drive(http)
        
        # Pre-warm standard folders cache
        logger.info("📂 Pre-warming subfolder cache...")
        for _cat, folder_name in CATEGORY_FOLDERS.items():
            try:
                await ensure_subfolder(http, folder_name, PARENT_FOLDER_ID)
            except Exception as exc:
                logger.error("❌ Failed to ensure folder '%s': %s", folder_name, exc)
    logger.info("📂 Subfolder cache: %s", _folder_cache)

    # 3) Connect to Telegram
    logger.info("📡 Connecting to Telegram...")
    await client.start()
    me = await client.get_me()
    logger.info("🤖 Logged in as %s (ID: %d)", me.first_name, me.id)

    # 4) Run forever, listening for incoming files
    logger.info("👂 Listening for incoming files…")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
