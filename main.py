import os
import json
import time
import asyncio
import logging
import shutil
from typing import Optional, Dict, Tuple
from datetime import datetime, timezone

from telethon import TelegramClient, events
from telethon.errors import ChatAdminRequiredError, SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.tl.functions.messages import (
    ExportChatInviteRequest,
    EditExportedChatInviteRequest,
    DeleteChatUserRequest,
)

# ============ Console logging ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("usher")
logging.getLogger("telethon").setLevel(logging.WARNING)

# ============ Load & Save Config ============
def load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def save_cfg(cfg):
    tmp = "config.json.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    shutil.move(tmp, "config.json")

CFG = load_cfg()

API_ID = CFG["telegram"]["api_id"]
API_HASH = CFG["telegram"]["api_hash"]
SESSION_FILE = CFG["telegram"]["session"]
STRING_SESSION = (CFG["telegram"].get("string_session") or "").strip()
PREFER_STRING = bool(CFG["telegram"].get("prefer_string_session", True))

ADMIN_IDS = set(CFG["admin"]["admin_ids"])

BOT_USERNAME = CFG["bot_target"]["username"]
CMD_MAP = CFG["bot_target"]["commands"]

BEHAV = CFG["behavior"]
COMBO_ON = BEHAV.get("combo_addv_plus_link", True)
COMBO_ORDER = BEHAV.get("combo_order", "relay_first")  # relay_first | parallel
WAIT_BOT_REPLY = int(BEHAV.get("wait_bot_reply_sec", 4))
DEDUPE_CACHE_SEC = int(BEHAV.get("dedupe_cache_sec", 60))
SILENT_DM = bool(BEHAV.get("silent_dm_to_user", True))
# Panjang preview isi; jumlah maksimal item saat list
NOTES_PREVIEW_LEN = int(BEHAV.get("notes_list_preview_len", 160))
NOTES_LIST_MAX = int(BEHAV.get("notes_list_max_items", 50))

VIP_INV = CFG["vip_invite"]
VIP_MAP = VIP_INV["map"]
TTL = int(VIP_INV["ttl_sec"])
LIMIT = int(VIP_INV["limit"])
TPL = VIP_INV["template"]

STORAGE = CFG["storage"]
NOTES_PATH = STORAGE["notes"]
INVITE_LOG = STORAGE["invite_log"]

os.makedirs("data", exist_ok=True)
os.makedirs(os.path.dirname(NOTES_PATH), exist_ok=True)
os.makedirs(os.path.dirname(INVITE_LOG), exist_ok=True)

# ========= In-memory caches =========
last_invites: Dict[Tuple[int, int], Dict[str, str]] = {}

# ========= Utils =========
def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def write_log(action: str, payload: dict):
    row = {"time": now_iso(), "action": action}
    row.update(payload)
    with open(INVITE_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    log.info("%s | %s", action, payload)

def load_notes() -> dict:
    if not os.path.exists(NOTES_PATH):
        return {}
    try:
        with open(NOTES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_notes(notes: dict):
    with open(NOTES_PATH, "w", encoding="utf-8") as f:
        json.dump(notes, f, ensure_ascii=False, indent=2)

def preview_text(s: str, limit: int) -> str:
    s = (s or "").strip().replace("\r", " ").replace("\n", " ")
    if len(s) <= limit:
        return s
    return s[:limit - 1].rstrip() + "…"

async def get_target_user(event: events.NewMessage.Event) -> Optional[int]:
    if event.is_reply:
        r = await event.get_reply_message()
        if r and r.sender_id and not r.is_channel:
            return r.sender_id
    parts = event.raw_text.strip().split()
    if len(parts) >= 2:
        arg = parts[1]
        if arg.isdigit():
            return int(arg)
        if arg.startswith("@"):
            try:
                ent = await client.get_entity(arg)
                return getattr(ent, "id", None)
            except Exception:
                return None
    return None

def addv_to_link_cmd(addv: str) -> str:
    return "link" + addv[-2:]  # addv1 -> linkv1

async def create_invite(peer: int, ttl_sec: int, limit: int) -> Optional[str]:
    expire_at = int(time.time()) + int(ttl_sec)
    try:
        invite = await client(ExportChatInviteRequest(peer=peer, expire_date=expire_at, usage_limit=limit))
        link = getattr(invite, "link", None)
        if not link and hasattr(invite, "exported_invite"):
            link = getattr(invite.exported_invite, "link", None)
        return link
    except ChatAdminRequiredError:
        return None

async def revoke_invite(peer: int, link: str) -> bool:
    try:
        await client(EditExportedChatInviteRequest(peer=peer, link=link, revoked=True))
        write_log("invite_revoked", {"peer": peer, "link": link})
        return True
    except Exception as e:
        write_log("invite_revoke_error", {"peer": peer, "link": link, "error": str(e)})
        return False

async def wait_bot_reply(timeout_sec: int) -> Optional[str]:
    try:
        bot = await client.get_entity(BOT_USERNAME)
        ev = await client.wait_for(events.NewMessage(from_users=bot.id), timeout=timeout_sec)
        return ev.raw_text or ""
    except Exception:
        return None

async def ensure_single_use_invite(peer: int, target: int, tier_cmd: str) -> str:
    now_ts = time.time()
    key = (peer, target)
    cached = last_invites.get(key)
    if cached and float(cached.get("expire_at", 0)) > now_ts:
        return cached["link"]

    link = await create_invite(peer, TTL, LIMIT)
    if not link:
        raise RuntimeError("Gagal membuat invite link (izin admin kurang?).")
    last_invites[key] = {"link": link, "expire_at": str(now_ts + DEDUPE_CACHE_SEC)}
    write_log("invite_sent", {"tier": tier_cmd, "peer": peer, "target": target, "link": link, "status": "pending"})
    return link

async def send_invite_to_user(target: int, tier_cmd: str, link: str):
    text = TPL.format(tier=tier_cmd.upper(), link=link)
    await client.send_message(target, text, silent=SILENT_DM)

# ============ Notes helpers (Mode A: tanpa emoji, tetap balas admin untuk notes) ============
def sanitize_title(title: str) -> str:
    return title.strip().lower()

def notes_media_dir(title: str) -> str:
    return os.path.join("data", "notes_media", title)

async def capture_note_from_reply(event, title: str, caption_override: Optional[str]) -> Optional[dict]:
    r = await event.get_reply_message()
    if not r:
        return None

    media = None
    media_type = None
    caption_text = (caption_override or r.message or "").strip()

    if r.photo:
        media = r.photo
        media_type = "photo"
    elif r.document and not r.video:
        media = r.document
        media_type = "document"
    elif r.video:
        media = r.video
        media_type = "video"

    if media:
        d = notes_media_dir(title)
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
        path = await client.download_media(media, file=d)
        size = os.path.getsize(path) if os.path.exists(path) else None
        note = {
            "type": media_type,
            "text": caption_text,
            "media": {"path": path, "size": size},
            "updated_at": now_iso()
        }
        return note

    if caption_text:
        return {
            "type": "text",
            "text": caption_text,
            "updated_at": now_iso()
        }
    return None

# ============ Client bootstrap (StringSession first) ============
def make_initial_client() -> TelegramClient:
    if PREFER_STRING:
        if STRING_SESSION:
            log.info("Login with StringSession from config.json")
            return TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
        else:
            log.info("No StringSession in config. Will generate one interactively.")
            return TelegramClient(StringSession(), API_ID, API_HASH)
    else:
        log.info("Login with FileSession: %s", SESSION_FILE)
        return TelegramClient(SESSION_FILE, API_ID, API_HASH)

client = make_initial_client()

async def interactive_login_and_persist_string():
    """Run only if we're using blank StringSession()."""
    await client.connect()
    if await client.is_user_authorized():
        session_str = client.session.save()
        CFG["telegram"]["string_session"] = session_str
        save_cfg(CFG)
        print("\n=== STRING SESSION (SIMPAN DENGAN AMAN) ===")
        print(session_str)
        print("=== END STRING SESSION ===\n")
        log.info("String session saved into config.json")
        return

    phone = os.getenv("USHER_PHONE") or input("Masukkan nomor HP (format internasional, contoh +628xx): ").strip()
    await client.send_code_request(phone)
    code = os.getenv("USHER_OTP") or input("Masukkan OTP (kode 5 digit): ").strip()
    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        pwd = os.getenv("USHER_2FA") or input("Akun ini aktif 2FA. Masukkan password: ").strip()
        await client.sign_in(password=pwd)

    session_str = client.session.save()
    print("\n=== STRING SESSION (SIMPAN DENGAN AMAN) ===")
    print(session_str)
    print("=== END STRING SESSION ===\n")
    CFG["telegram"]["string_session"] = session_str
    save_cfg(CFG)
    log.info("String session saved into config.json")

# ============ Command Handlers ============
@client.on(events.NewMessage(from_users=list(ADMIN_IDS)))
async def admin_handler(event: events.NewMessage.Event):
    text_raw = event.raw_text.strip()
    text = text_raw.lower()

    # /addv1|2|3  (silent to admin, combo relay+link)
    if text.startswith(("/addv1", "/addv2", "/addv3")):
        cmd = text.split()[0][1:]  # addv1
        tier_key = cmd[-2:]        # v1/v2/v3
        target = await get_target_user(event)
        if not target:
            return
        # relay
        payload = f"{CMD_MAP.get(tier_key, '/addv1')} {target}"
        await client.send_message(BOT_USERNAME, payload)
        write_log("relay_addv", {"tier": tier_key, "target": target, "to": BOT_USERNAME})

        # combo link
        if COMBO_ON:
            link_cmd = addv_to_link_cmd(cmd)
            peer = VIP_MAP.get(link_cmd)
            if peer:
                async def make_and_send():
                    try:
                        link = await ensure_single_use_invite(peer, target, link_cmd)
                        await send_invite_to_user(target, link_cmd, link)
                    except Exception as e:
                        write_log("invite_dm_failed", {"tier": link_cmd, "peer": peer, "target": target, "error": str(e)})
                if COMBO_ORDER == "relay_first":
                    if WAIT_BOT_REPLY > 0:
                        await wait_bot_reply(WAIT_BOT_REPLY)
                    await make_and_send()
                else:
                    asyncio.create_task(wait_bot_reply(WAIT_BOT_REPLY))
                    await make_and_send()
        return

    # /linkv1|2|3  (silent ke admin)
    if text.startswith(("/linkv1", "/linkv2", "/linkv3")):
        link_cmd = text.split()[0][1:]
        target = await get_target_user(event)
        if not target:
            return
        peer = VIP_MAP.get(link_cmd)
        if not peer:
            return
        try:
            link = await ensure_single_use_invite(peer, target, link_cmd)
            await send_invite_to_user(target, link_cmd, link)
        except Exception as e:
            write_log("invite_dm_failed", {"tier": link_cmd, "peer": peer, "target": target, "error": str(e)})
        return

    # /savenote <judul>  (Mode A: balas singkat tanpa emoji)
    if text.startswith("/savenote"):
        # format: /savenote judul | optional_text   (atau reply ke media/teks)
        parts = text_raw.split(maxsplit=1)
        if len(parts) < 2:
            await event.reply("Format tidak valid. Gunakan reply ke pesan, atau /savenote <judul> | <teks>.")
            return
        tail = parts[1]
        if "|" in tail:
            title, cap = [p.strip() for p in tail.split("|", 1)]
        else:
            title, cap = tail.strip(), None
        title_key = sanitize_title(title)
        if not title_key:
            await event.reply("Format tidak valid. Gunakan reply ke pesan, atau /savenote <judul> | <teks>.")
            return

        note = None
        if event.is_reply:
            note = await capture_note_from_reply(event, title_key, cap)
        elif cap:
            note = {"type": "text", "text": cap, "updated_at": now_iso()}
        if not note:
            await event.reply("Pesan tidak mengandung konten yang bisa disimpan.")
            return

        notes = load_notes()
        note["added_by"] = event.sender_id
        notes[title_key] = note
        save_notes(notes)
        write_log("note_saved", {"title": title_key, "type": note["type"], "by": event.sender_id})
        await event.reply(f"Note '{title_key}' disimpan.")
        return

    # /delnote <judul>  (Mode A)
    if text.startswith("/delnote"):
        parts = text_raw.split(maxsplit=1)
        if len(parts) < 2:
            await event.reply("Gunakan: /delnote <judul>")
            return
        title_key = sanitize_title(parts[1])
        notes = load_notes()
        if title_key in notes:
            d = notes_media_dir(title_key)
            if os.path.exists(d):
                shutil.rmtree(d)
            del notes[title_key]
            save_notes(notes)
            write_log("note_deleted", {"title": title_key})
            await event.reply(f"Note '{title_key}' dihapus.")
        else:
            await event.reply("Note tidak ditemukan.")
        return

    # /listnote — hanya judul + isi ringkas (tanpa waktu, tanpa tipe)
    if text.startswith("/listnote"):
        notes = load_notes()
        if not notes:
            await event.reply("Tidak ada note tersimpan.")
            write_log("note_list", {"count": 0, "shown": 0})
            return

        # Optional limit via arg: /listnote 10
        limit = NOTES_LIST_MAX
        parts = text_raw.split(maxsplit=1)
        if len(parts) == 2 and parts[1].isdigit():
            try:
                limit = max(1, min(int(parts[1]), NOTES_LIST_MAX))
            except Exception:
                limit = NOTES_LIST_MAX

        lines = ["Daftar note:"]
        titles = list(notes.keys())
        shown = 0

        for idx, title in enumerate(titles, start=1):
            if shown >= limit:
                break
            note = notes.get(title) or {}
            text_body = note.get("text", "") or ""
            prev = preview_text(text_body, NOTES_PREVIEW_LEN)
            lines.append(f"{idx}. {title}")
            lines.append(f"- {prev or '(kosong)'}")
            shown += 1

        if shown < len(titles):
            lines.append(f"(ditampilkan {shown} dari {len(titles)})")

        await event.reply("\n".join(lines))
        write_log("note_list", {"count": len(notes), "shown": shown})
        return

    # /getnote <judul>  (silent ke admin saat kirim; hanya log)
    if text.startswith("/getnote"):
        parts = text_raw.split(maxsplit=1)
        if len(parts) < 2:
            await event.reply("Gunakan: /getnote <judul>")
            return
        title_key = sanitize_title(parts[1])
        notes = load_notes()
        note = notes.get(title_key)
        if not note:
            await event.reply("Note tidak ditemukan.")
            return

        target_id = None
        if event.is_reply:
            r = await event.get_reply_message()
            target_id = r.sender_id if r else event.chat_id
        else:
            target_id = event.chat_id

        try:
            if note["type"] == "text":
                await client.send_message(target_id, note.get("text", "") or "")
            else:
                media = note.get("media") or {}
                path = media.get("path")
                if path and os.path.exists(path):
                    await client.send_file(
                        target_id,
                        path,
                        caption=note.get("text", "") or "",
                        force_document=(note["type"] == "document")
                    )
                else:
                    await client.send_message(target_id, note.get("text", "") or "")
            write_log("note_get_sent", {"title": title_key, "type": note["type"], "target": target_id})
        except Exception as e:
            write_log("note_get_send_error", {"title": title_key, "error": str(e)})
        return

# ============ Join enforcement: single-use ============
@client.on(events.ChatAction)
async def on_chat_action(event: events.ChatAction.Event):
    if not (event.user_joined or event.user_added):
        return
    chat_id = event.chat_id
    user = await event.get_user()
    user_id = user.id

    if not os.path.exists(INVITE_LOG):
        return

    pending = None
    with open(INVITE_LOG, "r", encoding="utf-8") as f:
        lines = f.readlines()
    for line in reversed(lines):
        try:
            data = json.loads(line)
        except Exception:
            continue
        if data.get("action") == "invite_sent" and data.get("peer") == chat_id and data.get("status") == "pending":
            pending = data
            break
    if not pending:
        return

    link = pending.get("link")
    target = int(pending.get("target", 0))

    if user_id == target:
        await revoke_invite(chat_id, link)
        write_log("invite_consumed", {"peer": chat_id, "target": user_id, "link": link})
        return

    try:
        await client(DeleteChatUserRequest(chat_id, user_id))
        write_log("invite_abused_kicked", {"peer": chat_id, "abuser": user_id, "intended_for": target, "link": link})
    except Exception as e:
        write_log("invite_abused_kick_failed", {"peer": chat_id, "abuser": user_id, "error": str(e)})
    await revoke_invite(chat_id, link)

# ============ Main ============
async def main():
    # If using blank StringSession(), perform interactive login and persist string
    if PREFER_STRING and not STRING_SESSION:
        await interactive_login_and_persist_string()
    else:
        await client.connect()
        if not await client.is_user_authorized():
            pass

    log.info("Bot started !!!")
    await client.run_until_disconnected()

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
