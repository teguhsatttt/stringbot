import os
import json
import time
import asyncio
import logging
import shutil
from typing import Optional
from datetime import datetime, timezone

from telethon import TelegramClient, events, functions
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("usher")

# ===================== CONFIG IO =====================
CFG_PATH = "config.json"

def load_cfg() -> dict:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_cfg(cfg: dict):
    tmp = CFG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CFG_PATH)

CFG = load_cfg()

# --- Telegram credentials & client mode ---
API_ID = int(CFG["telegram"]["api_id"])
API_HASH = CFG["telegram"]["api_hash"]
SESSION_FILE = CFG["telegram"]["session"]
STRING_SESSION = (CFG["telegram"].get("string_session") or "").strip()
PREFER_STRING = bool(CFG["telegram"].get("prefer_string_session", True))

# --- Admin & target bot ---
ADMIN_IDS = set(int(x) for x in CFG["admin"]["admin_ids"])
BOT_USERNAME = CFG["bot_target"]["username"]
CMD_MAP = CFG["bot_target"]["commands"]  # {"v1": "/addv1", ..., "v6": "/addv6"}

# --- Behavior ---
BEHAV = CFG["behavior"]
COMBO_ON = bool(BEHAV.get("combo_addv_plus_link", True))
COMBO_ORDER = BEHAV.get("combo_order", "relay_first")  # relay_first | parallel
WAIT_BOT_REPLY = int(BEHAV.get("wait_bot_reply_sec", 5))
SILENT_DM = bool(BEHAV.get("silent_dm_to_user", True))
ADDV_MIN = int(BEHAV.get("addv_req_min", 1))
ADDV_MAX = int(BEHAV.get("addv_req_max", 100))
NOTES_PREVIEW_LEN = int(BEHAV.get("notes_list_preview_len", 160))
NOTES_LIST_MAX = int(BEHAV.get("notes_list_max_items", 50))

# --- VIP invite config ---
VIP = CFG["vip_invite"]
VIP_MAP = VIP["map"]                      # {"linkv1": "-100...", ..., "linkv6": "-100..."}
TTL = int(VIP.get("ttl_sec", 86400))      # 24 jam
LIMIT = int(VIP.get("limit", 1))          # diabaikan saat join-request
INVITE_TPL = VIP.get("template", "Akses {tier} aktif.\nLink (berlaku 24 jam, 1x pakai): {link}")

# --- Storage ---
STO = CFG["storage"]
NOTES_PATH = STO["notes"]
INVITE_LOG = STO["invite_log"]

os.makedirs("data", exist_ok=True)
os.makedirs(os.path.dirname(NOTES_PATH), exist_ok=True)
os.makedirs(os.path.dirname(INVITE_LOG), exist_ok=True)

# ===================== CLIENT =====================
def make_client() -> TelegramClient:
    if PREFER_STRING:
        if STRING_SESSION:
            return TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
        # first-run: create empty StringSession then interactive login
        return TelegramClient(StringSession(), API_ID, API_HASH)
    return TelegramClient(SESSION_FILE, API_ID, API_HASH)

client = make_client()

# ===================== HELPERS =====================
ACTIVE_INVITES = {}  # { user_id: {"tier": "linkvX", "link": "https://t.me/+..."} }

def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def log_action(action: str, data: dict):
    rec = {"time": now_iso(), "action": action}
    rec.update(data)
    with open(INVITE_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    log.info("%s | %s", action, json.dumps(data, ensure_ascii=False))

def preview_text(s: str, limit: int) -> str:
    s = (s or "").strip().replace("\r", " ").replace("\n", " ")
    return s if len(s) <= limit else s[:limit - 1].rstrip() + "…"

def sanitize_title(title: str) -> str:
    return title.strip().lower()

def notes_media_dir(title: str) -> str:
    return os.path.join("data", "notes_media", title)

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

def extract_int_token(tok: str) -> Optional[int]:
    if not tok:
        return None
    digits = "".join(ch for ch in tok if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None

def clip_req(n: int) -> int:
    if n < ADDV_MIN:
        return ADDV_MIN
    if n > ADDV_MAX:
        return ADDV_MAX
    return n

def addv_to_link_cmd(addv_cmd: str) -> str:
    # "/addv1" -> "linkv1"
    base = addv_cmd.lstrip("/")
    return "link" + base[-2:]  # aman untuk 1..6

async def get_target_user_from_context(event: events.NewMessage.Event) -> Optional[int]:
    # Prioritas: reply user
    if event.is_reply:
        r = await event.get_reply_message()
        if r and r.sender_id and not r.is_channel:
            return r.sender_id
    # Alternatif: argumen kedua
    parts = event.raw_text.strip().split()
    if len(parts) >= 2:
        tok = parts[1].strip()
        if tok.startswith("@"):
            try:
                ent = await client.get_entity(tok)
                return getattr(ent, "id", None)
            except Exception:
                return None
        num = extract_int_token(tok)
        if num is not None:
            return num
    return None

def parse_req_no_or_none(text_raw: str, consumed_first_arg: bool) -> Optional[int]:
    parts = text_raw.strip().split()
    args = parts[1:] if len(parts) > 1 else []
    start = 1 if consumed_first_arg else 0
    while start < len(args):
        tok = args[start].strip()
        if tok.startswith("@"):
            start += 1
            continue
        num = extract_int_token(tok)
        if num is not None:
            return clip_req(num)
        start += 1
    return None

# ===================== LOGIN (STRING SESSION) =====================
async def interactive_login_and_persist_string():
    await client.connect()
    if await client.is_user_authorized():
        s = client.session.save()
        CFG["telegram"]["string_session"] = s
        save_cfg(CFG)
        print("\n=== STRING SESSION ===\n" + s + "\n======================\n")
        return
    phone = os.getenv("USHER_PHONE") or input("Masukkan nomor HP (+62xxxxxxxxxx): ").strip()
    await client.send_code_request(phone)
    code = os.getenv("USHER_OTP") or input("Masukkan OTP: ").strip()
    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        pwd = os.getenv("USHER_2FA") or input("Akun ini pakai 2FA. Password: ").strip()
        await client.sign_in(password=pwd)
    s = client.session.save()
    CFG["telegram"]["string_session"] = s
    save_cfg(CFG)
    print("\n=== STRING SESSION ===\n" + s + "\n======================\n")

# ===================== INVITE CORE =====================
async def ensure_entity_cached(peer_id: int):
    # Bantu Telethon mengenali entity jika session baru
    try:
        await client.get_entity(peer_id)
    except Exception:
        try:
            await client(functions.channels.GetChannelsRequest(id=[peer_id]))
        except Exception as e:
            log_action("entity_cache_error", {"peer": peer_id, "err": str(e)})

async def create_invite(link_cmd: str, require_approval: bool = True) -> Optional[str]:
    peer_cfg = VIP_MAP.get(link_cmd)
    if not peer_cfg:
        log_action("invite_error", {"tier": link_cmd, "err": "peer_not_configured"})
        return None
    try:
        peer_id = int(str(peer_cfg).strip())  # "-100xxxxxxxxxx"
        expire = int(time.time()) + TTL

        await ensure_entity_cached(peer_id)

        kwargs = dict(
            peer=peer_id,
            expire_date=expire,
            request_needed=require_approval
        )
        # usage_limit tidak valid bila request_needed=True
        if not require_approval:
            kwargs["usage_limit"] = LIMIT

        res = await client(functions.messages.ExportChatInviteRequest(**kwargs))

        link = getattr(res, "link", None)
        if not link and hasattr(res, "exported_invite"):
            link = getattr(res.exported_invite, "link", None)
        if not link:
            log_action("invite_error", {"tier": link_cmd, "peer": str(peer_cfg), "err": "no_link_returned"})
            return None
        return link
    except Exception as e:
        log_action("invite_error", {"tier": link_cmd, "peer": str(peer_cfg), "err": str(e)})
        return None

async def revoke_invite(peer_key: str, link: str):
    try:
        peer_id = int(str(VIP_MAP[peer_key]).strip())
        await client(functions.messages.EditExportedChatInviteRequest(
            peer=peer_id,
            link=link,
            revoked=True
        ))
        log_action("invite_revoked", {"tier": peer_key, "link": link})
    except Exception as e:
        log_action("revoke_err", {"tier": peer_key, "err": str(e)})

async def send_invite_to_user(user_id: int, tier: str, link: str):
    t = (tier or "").lower()
    if t.startswith("linkv"):
        n = t.replace("linkv", "")
    elif t.startswith("v"):
        n = t[1:]
    else:
        n = ""
    tier_label = f"VIP{n}" if n.isdigit() else "VIP"
    msg = INVITE_TPL.format(tier=tier_label, link=link)
    await client.send_message(user_id, msg, silent=SILENT_DM)

# ===================== ADMIN COMMANDS =====================
@client.on(events.NewMessage(from_users=list(ADMIN_IDS)))
async def admin_handler(event: events.NewMessage.Event):
    text_raw = event.raw_text.strip()
    if not text_raw:
        return
    text = text_raw.lower()

    # ---------- .linkv1..linkv6 (join-request) ----------
    if text_raw.startswith((".linkv1", ".linkv2", ".linkv3", ".linkv4", ".linkv5", ".linkv6")):
        link_cmd = text_raw.split()[0].lstrip("/.!").lower()  # 'linkv1'..'linkv6'
        target = await get_target_user_from_context(event)
        if not target:
            log_action("link_ignored", {"reason": "no_target", "cmd": link_cmd, "raw": event.raw_text})
            return

        link = await create_invite(link_cmd, True)
        if not link:
            log_action("link_error", {"tier": link_cmd, "target": target, "err": "create_invite_failed"})
            return

        await send_invite_to_user(target, link_cmd, link)
        log_action("invite_sent_manual", {"tier": link_cmd, "target": target, "link": link})
        ACTIVE_INVITES[target] = {"tier": link_cmd, "link": link}
        return

    # ---------- .addv1..6 atau /addv1..6 (relay ke bot utama + DM link) ----------
    if text.startswith(("/addv1", "/addv2", "/addv3", "/addv4", "/addv5", "/addv6",
                        ".addv1", ".addv2", ".addv3", ".addv4", ".addv5", ".addv6")):
        input_cmd = text.split()[0]                      # '.addvX' atau '/addvX'
        relay_cmd_token = input_cmd.replace(".", "/")    # selalu relay ke bot utama /addvX
        target = await get_target_user_from_context(event)

        consumed_first_arg = False
        if not event.is_reply:
            parts = text_raw.split()
            if len(parts) >= 2:
                a1 = parts[1]
                if a1.startswith("@") or extract_int_token(a1) is not None:
                    consumed_first_arg = True

        if not target:
            log_action("ignored_addv", {"reason": "no_target", "tier": relay_cmd_token.lstrip("/"), "raw": text_raw})
            return

        # Hanya .addv1 yang menerima request_no (1–100)
        req_no = None
        if relay_cmd_token.endswith("1"):
            req_no = parse_req_no_or_none(text_raw, consumed_first_arg)

        tier_key = f"v{relay_cmd_token[-1]}"                 # 'v1'..'v6'
        relay_cmd = CMD_MAP.get(tier_key, relay_cmd_token)    # map -> '/addvX'

        payload = f"{relay_cmd} {target}" if req_no is None else f"{relay_cmd} {target} {req_no}"
        await client.send_message(BOT_USERNAME, payload)

        lp = {"tier": tier_key, "target": target, "to": BOT_USERNAME}
        if req_no is not None:
            lp["req_no"] = req_no
        log_action("relay_addv", lp)

        # DM link setelah relay
        if COMBO_ON:
            link_cmd = addv_to_link_cmd(relay_cmd_token)  # 'linkvX'

            async def make_and_send():
                try:
                    if WAIT_BOT_REPLY > 0:
                        try:
                            bot_ent = await client.get_entity(BOT_USERNAME)
                            await client.wait_for(events.NewMessage(from_users=bot_ent.id), timeout=WAIT_BOT_REPLY)
                        except Exception:
                            pass

                    await asyncio.sleep(5)
                    link = await create_invite(link_cmd, True)
                    if not link:
                        raise RuntimeError("create_invite_failed")

                    await send_invite_to_user(target, link_cmd, link)
                    inv_log = {"tier": link_cmd, "target": target, "link": link, "status": "pending"}
                    if req_no is not None:
                        inv_log["req_no"] = req_no
                    log_action("invite_sent", inv_log)
                    ACTIVE_INVITES[target] = {"tier": link_cmd, "link": link}
                except Exception as e:
                    log_action("invite_dm_failed", {"tier": link_cmd, "target": target, "err": str(e)})

            if COMBO_ORDER == "relay_first":
                await make_and_send()
            else:
                asyncio.create_task(make_and_send())
        return

    # ---------- NOTES ----------
    if text.startswith("/savenote"):
        parts = text_raw.split(maxsplit=1)
        if len(parts) < 2 and not event.is_reply:
            await event.reply("Format: reply konten atau /savenote <judul> | <isi>")
            return
        tail = parts[1] if len(parts) > 1 else ""
        if "|" in tail:
            title, cap = [p.strip() for p in tail.split("|", 1)]
        else:
            title, cap = (tail.strip() or "untitled"), None
        title_key = sanitize_title(title)

        note = None
        if event.is_reply:
            note = await capture_note_from_reply(event, title_key, cap)
        elif cap:
            note = {"type": "text", "text": cap}

        if not note:
            await event.reply("Pesan tidak mengandung konten yang bisa disimpan.")
            return

        notes = load_notes()
        notes[title_key] = note
        save_notes(notes)
        log_action("note_saved", {"title": title_key, "type": note["type"]})
        await event.reply(f"Note '{title_key}' disimpan.")
        return

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
            log_action("note_deleted", {"title": title_key})
            await event.reply(f"Note '{title_key}' dihapus.")
        else:
            await event.reply("Note tidak ditemukan.")
        return

    if text.startswith("/listnote"):
        notes = load_notes()
        if not notes:
            await event.reply("Tidak ada note tersimpan.")
            log_action("note_list", {"count": 0, "shown": 0})
            return
        limit = NOTES_LIST_MAX
        parts = text_raw.split(maxsplit=1)
        if len(parts) == 2 and parts[1].isdigit():
            try:
                limit = max(1, min(int(parts[1]), NOTES_LIST_MAX))
            except Exception:
                limit = NOTES_LIST_MAX
        titles = list(notes.keys())
        lines = ["Daftar note:"]
        shown = 0
        for idx, title in enumerate(titles, start=1):
            if shown >= limit:
                break
            n = notes.get(title) or {}
            body = n.get("text", "") or ""
            prev = preview_text(body, NOTES_PREVIEW_LEN)
            lines.append(f"{idx}. {title}")
            lines.append(f"   > {prev or '(kosong)'}")
            shown += 1
        if shown < len(titles):
            lines.append(f"(ditampilkan {shown} dari {len(titles)})")
        await event.reply("\n".join(lines))
        log_action("note_list", {"count": len(notes), "shown": shown})
        return

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
        target_id = event.chat_id
        if event.is_reply:
            r = await event.get_reply_message()
            target_id = r.sender_id if r else event.chat_id
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
                        force_document=(note["type"] == "document"),
                    )
                else:
                    await client.send_message(target_id, note.get("text", "") or "")
            log_action("note_get_sent", {"title": title_key, "type": note["type"], "target": target_id})
        except Exception as e:
            log_action("note_get_send_error", {"title": title_key, "err": str(e)})
        return

    # ---------- /help ----------
    if text.startswith("/help"):
        help_text = (
            "Panduan Perintah Usher Bot\n"
            "\n"
            "ADD (relay ke Bot Utama):\n"
            "  .addv1  [reply user] [opsional: <request_no 1-100>]\n"
            "    - Contoh: reply ke user lalu kirim: .addv1 7\n"
            "    - Relay: /addv1 <user_id> 7\n"
            "  .addv2  [reply user] (tanpa request_no)  → /addv2 <user_id>\n"
            "  .addv3  [reply user] (tanpa request_no)  → /addv3 <user_id>\n"
            "  .addv4  [reply user] (tanpa request_no)  → /addv4 <user_id>\n"
            "  .addv5  [reply user] (tanpa request_no)  → /addv5 <user_id>\n"
            "  .addv6  [reply user] (tanpa request_no)  → /addv6 <user_id>\n"
            "\n"
            "LINK (DM link join-request, TTL 24 jam, 1x pakai via auto-revoke):\n"
            "  .linkv1  [reply user]\n"
            "  .linkv2  [reply user]\n"
            "  .linkv3  [reply user]\n"
            "  .linkv4  [reply user]\n"
            "  .linkv5  [reply user]\n"
            "  .linkv6  [reply user]\n"
            "\n"
            "NOTES:\n"
            "  /savenote <judul> | <isi>    atau reply konten lalu: /savenote <judul>\n"
            "  /delnote <judul>\n"
            "  /listnote [maks_item]\n"
            "  /getnote <judul>\n"
        )
        await event.reply(help_text)
        log_action("show_help", {"from": event.sender_id})
        return

# ===================== AUTO-REVOKE (1x pakai) =====================
@client.on(events.ChatAction)
async def on_chat_action(event: events.ChatAction.Event):
    if not (event.user_joined or event.user_added):
        return
    try:
        user = await event.get_user()
        uid = user.id
    except Exception:
        return

    rec = ACTIVE_INVITES.pop(uid, None)
    if rec:
        try:
            await revoke_invite(rec["tier"], rec["link"])
            log_action("invite_consumed", {"tier": rec["tier"], "target": uid, "link": rec["link"]})
        except Exception as e:
            log_action("invite_consume_err", {"tier": rec["tier"], "target": uid, "err": str(e)})
        return

    # Fallback kalau bot restart: scan log terakhir
    try:
        if not os.path.exists(INVITE_LOG):
            return
        with open(INVITE_LOG, "r", encoding="utf-8") as f:
            lines = f.readlines()
        link = None
        tier = None
        for line in reversed(lines):
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("action") in ("invite_sent", "invite_sent_manual") and int(d.get("target", 0)) == uid:
                link = d.get("link")
                tier = d.get("tier")
                break
        if link and tier:
            try:
                await revoke_invite(tier, link)
                log_action("invite_consumed", {"tier": tier, "target": uid, "link": link})
            except Exception as e:
                log_action("invite_consume_err", {"tier": tier, "target": uid, "err": str(e)})
    except Exception:
        pass

# ===================== STARTUP =====================
async def main():
    if PREFER_STRING and not STRING_SESSION:
        await interactive_login_and_persist_string()
    else:
        await client.connect()
    print("UsherBot started.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
