"""Convert an uploaded TData archive / Telethon `.session` / Pyrogram
`.session` file into a live Telethon client authorized under THIS bot's own
API identity.

Why not just reuse the file's own auth_key as-is under our api_id/api_hash:
opentele's own docs warn that an auth_key should stick to the API that
created it — reusing it under a different, mismatched api_id is a known ban
signal. Every import here instead goes through Telegram's QR-login-approval
flow (opentele's `CreateNewSession`), which is the officially safe way to
hand a session to a different API identity: it asks the ALREADY-authorized
session to approve a brand-new login, exactly like scanning a QR code from a
device that's already logged in.
"""
import asyncio
import os
import sqlite3
import zipfile
from typing import Optional

from opentele.api import API, APIData, CreateNewSession, UseCurrentSession
from opentele.exception import NoPasswordProvided, OpenTeleException, PasswordIncorrect
from opentele.td import TDesktop
from opentele.tl import TelegramClient as OpenteleTelegramClient
from telethon.sessions import StringSession


class ImportRejected(Exception):
    """Any expected, non-recoverable failure (bad file, unauthorized
    session, wrong password on a final attempt). Callers show `str(exc)`
    straight to the user — it is always already a Russian, user-safe message."""


class ImportPasswordRequired(Exception):
    """The account's 2FA password is needed to finish the QR-login-approval
    step. Callers should prompt for it and retry with `password=`.

    `wrong=True` means a password WAS supplied but Telegram rejected it —
    callers should say so instead of asking as if for the first time."""

    def __init__(self, wrong: bool = False):
        self.wrong = wrong
        super().__init__("wrong password" if wrong else "password required")


def detect_import_kind(filename: str, head: bytes) -> Optional[str]:
    """Sniff an uploaded document's likely format from its name/first bytes,
    before it's even fully downloaded. Returns "tdata", "session" (Telethon
    or Pyrogram — disambiguated later, once the whole file is on disk, by
    `sqlite_session_kind`) or None if unrecognized."""
    name = (filename or "").lower()
    if name.endswith(".zip") or head[:4] == b"PK\x03\x04":
        return "tdata"
    if name.endswith(".session") or head[:16] == b"SQLite format 3\x00":
        return "session"
    return None


# Telegram Desktop writes every tdata file with a one-char version suffix and
# opentele's Storage.ReadFile tries exactly these — so the key file on disk
# is "key_datas" (or key_data1/key_data0), never a bare "key_data".
_TDATA_KEY_FILES = {"key_datas", "key_data1", "key_data0"}

# Cache folders Telegram Desktop keeps inside tdata — media/emoji/webview
# caches that can run to hundreds of MB and that opentele never reads.
_TDATA_SKIP_DIRS = {"user_data", "emoji", "dumps", "temp", "tdummy", "webview", "working"}
_TDATA_MAX_EXTRACT_BYTES = 100 * 1024 * 1024
_TDATA_MAX_ENTRIES = 5000


def _find_tdata_root(extracted_dir: str) -> Optional[str]:
    """TData zips show up in several shapes depending on what got zipped:
    the `tdata` folder itself, its contents with no wrapper folder, or
    `tdata` nested a level or two deeper. The key file is the one every
    TData folder always has, so find it and use its parent as basePath."""
    for root, _dirs, files in os.walk(extracted_dir):
        if _TDATA_KEY_FILES.intersection(files):
            return root
    return None


def _safe_extract_tdata(zf: zipfile.ZipFile, extract_dir: str) -> None:
    """Extract only what opentele needs, with a hard streamed size cap — a 20MB
    upload can otherwise decompress to gigabytes (zip bomb / a full media
    cache) and fill the disk. The cap counts real bytes read, not the
    archive's declared header sizes, so lying headers cannot bypass it."""
    members = [
        m for m in zf.infolist()
        if not m.is_dir()
        and not _TDATA_SKIP_DIRS.intersection(p.lower() for p in m.filename.replace("\\", "/").split("/")[:-1])
    ]
    if len(members) > _TDATA_MAX_ENTRIES or sum(m.file_size for m in members) > _TDATA_MAX_EXTRACT_BYTES:
        raise ImportRejected("❌ Архив слишком большой. Заархивируйте только папку tdata без кэша (user_data).")
    extracted = 0
    for m in members:
        parts = [
            p for p in m.filename.replace("\\", "/").split("/")
            if p not in ("", ".", "..") and ":" not in p
        ]
        if not parts:
            raise ImportRejected("❌ В архиве найден путь вне папки TData.")
        target = os.path.join(extract_dir, *parts)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        member_bytes = 0
        with zf.open(m) as src, open(target, "wb") as dst:
            while chunk := src.read(1024 * 1024):
                member_bytes += len(chunk)
                if extracted + member_bytes > _TDATA_MAX_EXTRACT_BYTES:
                    raise ImportRejected("❌ Архив слишком большой. Заархивируйте только папку tdata без кэша (user_data).")
                dst.write(chunk)
        extracted += member_bytes


def sqlite_session_kind(path: str) -> Optional[str]:
    """Telethon's and Pyrogram's `.session` files are both plain SQLite but
    with different `sessions` table columns: Telethon's has `takeout_id`,
    Pyrogram's has `user_id`/`is_bot`."""
    try:
        conn = sqlite3.connect(path)
        try:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(sessions)")}
        finally:
            conn.close()
    except sqlite3.Error:
        return None
    if "takeout_id" in cols:
        return "telethon_session"
    if "user_id" in cols and "is_bot" in cols:
        return "pyrogram_session"
    return None


def _load_tdata(file_path: str, work_dir: str) -> TDesktop:
    extract_dir = os.path.join(work_dir, "extracted")
    os.makedirs(extract_dir, exist_ok=True)
    try:
        with zipfile.ZipFile(file_path) as zf:
            _safe_extract_tdata(zf, extract_dir)
    except zipfile.BadZipFile:
        raise ImportRejected("❌ Файл повреждён или это не ZIP-архив с TData.")

    tdata_root = _find_tdata_root(extract_dir)
    if not tdata_root:
        raise ImportRejected("❌ В архиве не найдена папка TData (нет файла key_datas).")

    try:
        # The API used here only decrypts/reads the LOCAL tdata files — it
        # has no bearing on which API the new session gets created under,
        # so any custom api_id/hash typed earlier in the wizard is
        # correctly irrelevant and ignored.
        tdesk = TDesktop(tdata_root, api=API.TelegramDesktop)
    except OpenTeleException as e:
        raise ImportRejected(f"❌ Не удалось прочитать TData: {e}")

    if not tdesk.isLoaded() or tdesk.accountsCount == 0:
        raise ImportRejected("❌ TData не содержит авторизованного аккаунта.")
    return tdesk


async def _load_pyrogram_tdesktop(file_path: str, work_dir: str) -> TDesktop:
    try:
        import pyrogram
    except ImportError:
        raise ImportRejected("❌ Поддержка Pyrogram-сессий временно недоступна.")

    session_name = "imported"
    dest = os.path.join(work_dir, session_name + ".session")
    with open(file_path, "rb") as src, open(dest, "wb") as dst:
        dst.write(src.read())

    # api_id/hash here are never used for a real login — FromPyrogram() below
    # is forced to UseCurrentSession, which only reads the auth_key straight
    # out of this client's local SQLite storage.
    pyro_client = pyrogram.Client(
        session_name, workdir=work_dir,
        api_id=API.TelegramDesktop.api_id, api_hash=API.TelegramDesktop.api_hash,
    )
    try:
        tdesk = await TDesktop.FromPyrogram(pyro_client, flag=UseCurrentSession, api=API.TelegramDesktop)
    except OpenTeleException as e:
        raise ImportRejected(f"❌ Не удалось прочитать Pyrogram-сессию: {e}")
    except Exception as e:
        raise ImportRejected(f"❌ Сессия недействительна: {e}")

    if not tdesk.isLoaded() or tdesk.accountsCount == 0:
        raise ImportRejected("❌ Pyrogram-сессия не авторизована.")
    return tdesk


async def _load_telethon_old_client(file_path: str, work_dir: str, proxy=None, use_ipv6: bool = False, register_client=None) -> OpenteleTelegramClient:
    session_path = os.path.join(work_dir, "imported")
    with open(file_path, "rb") as src, open(session_path + ".session", "wb") as dst:
        dst.write(src.read())

    # Same reasoning as the Pyrogram branch: this client only makes one
    # cheap authorized call (is_user_authorized) before handing off to
    # QRLoginToNewClient, which is the step that actually re-authorizes
    # under a different, chosen API — so which API this transient client
    # itself connects with doesn't matter.
    old_client = OpenteleTelegramClient(session_path, api=API.TelegramDesktop, proxy=proxy, use_ipv6=use_ipv6)
    if register_client:
        register_client(old_client)
    try:
        await old_client.connect()
    except Exception as e:
        raise ImportRejected(f"❌ Не удалось открыть .session файл: {e}")

    if not await old_client.is_user_authorized():
        await old_client.disconnect()
        raise ImportRejected("❌ Эта сессия не авторизована — нужно заново войти в аккаунт.")
    return old_client


async def cleanup_import_source(source) -> None:
    """Disconnect the transient client behind a "telethon_session" `source`
    once it's no longer needed (success or final rejection). A `TDesktop`
    source (tdata / pyrogram_session) never held a live connection, so
    there's nothing to close."""
    if isinstance(source, TDesktop):
        return
    await source.disconnect()


async def load_import_source(kind: str, file_path: str, work_dir: str, proxy=None, use_ipv6: bool = False, register_client=None):
    """Read the uploaded file into whatever local, not-yet-network-authorized
    representation opentele needs for the next step: a `TDesktop` for
    "tdata"/"pyrogram_session", or an already-connected opentele
    `TelegramClient` for "telethon_session". Raises `ImportRejected` if the
    file is malformed or the session it contains isn't authorized.

    Kept separate from `create_new_session` so a caller that hits
    `ImportPasswordRequired` can retry just the network step, without
    re-parsing the uploaded file."""
    if kind == "tdata":
        # Zip extraction + TDesktop crypto parse are CPU/IO heavy and fully
        # synchronous: they must not freeze the live event loop.
        return await asyncio.to_thread(_load_tdata, file_path, work_dir)
    if kind == "pyrogram_session":
        return await _load_pyrogram_tdesktop(file_path, work_dir)
    if kind == "telethon_session":
        return await _load_telethon_old_client(file_path, work_dir, proxy=proxy, use_ipv6=use_ipv6, register_client=register_client)
    raise ImportRejected("❌ Неизвестный формат файла.")


async def create_new_session(
    source, target_api: APIData, password: Optional[str] = None,
    proxy=None, use_ipv6: bool = False, register_client=None,
) -> OpenteleTelegramClient:
    """Perform the actual safe re-authorization step (QR-login-approval)
    against either a `TDesktop` (tdata / converted-Pyrogram) or an already-
    connected opentele `TelegramClient` (plain Telethon `.session`) —
    `source`, as returned by `load_import_source`.

    Both paths bottom out in opentele's `QRLoginToNewClient`, which reports a
    missing/wrong 2FA password as `NoPasswordProvided`/`PasswordIncorrect` —
    NOT Telethon's own `SessionPasswordNeededError` — so those are what must
    be caught here. Raises `ImportPasswordRequired` in that case; the caller
    should prompt for the password and call this again with the same
    `source` (do not call `load_import_source` again — for a `TDesktop`
    that's harmless but wasteful, for a `telethon_session`'s already-
    connected client it would leak the old connection)."""
    try:
        if isinstance(source, TDesktop):
            new_client = await source.ToTelethon(
                session=StringSession(), flag=CreateNewSession, api=target_api, password=password,
                proxy=proxy, use_ipv6=use_ipv6,
            )
        else:
            new_client = await source.QRLoginToNewClient(
                session=StringSession(), api=target_api, password=password,
                proxy=proxy, use_ipv6=use_ipv6,
            )
    except NoPasswordProvided:
        raise ImportPasswordRequired(wrong=False)
    except PasswordIncorrect:
        raise ImportPasswordRequired(wrong=True)
    except OpenTeleException as e:
        raise ImportRejected(f"❌ Не удалось авторизовать новую сессию: {e}")

    if register_client:
        register_client(new_client)
    if not await new_client.is_user_authorized():
        await new_client.disconnect()
        raise ImportRejected("❌ Не удалось авторизовать новую сессию.")
    return new_client
