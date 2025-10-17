import os, json, time, asyncio, logging, shutil
from typing import Optional
from datetime import datetime, timezone
from telethon import TelegramClient, events, functions
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("usher")

CFG_PATH = "config.json"

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_cfg(cfg: dict):
    tmp = CFG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CFG_PATH)

CFG = load_cfg()

API_ID = int(CFG["telegram"]["api_id"])
API_HASH = CFG["telegram"]["api_hash"]
SESSION_FILE = CFG["telegram"]["session"]
STRING_SESSION = (CFG["telegram"].get("string_session") or "").strip()
PREFER_STRING = bool(CFG["telegram"].get("prefer_string_session", True))

ADMIN_IDS = set(int(x) for x in CFG["admin"]["admin_ids"])

BOT_USERNAME = CFG["bot_target"]["username"]
CMD_MAP = CFG["bot_target"]["commands"]

BEHAV = CFG["behavior"]
COMBO_ON = bool(BEHAV.get("combo_addv_plus_link", True))
COMBO_ORDER = BEHAV.get("combo_order", "relay_first")
WAIT_BOT_REPLY = int(BEHAV.get("wait_bot_reply_sec", 4))
DEDUPE_CACHE_SEC = int(BEHAV.get("dedupe_cache_sec", 60))
SILENT_DM = bool(BEHAV.get("silent_dm_to_user", True))
NOTES_PREVIEW_LEN = int(BEHAV.get("notes_list_preview_len", 160))
NOTES_LIST_MAX = int(BEHAV.get("notes_list_max_items", 50))
ADDV_MIN = int(BEHAV.get("addv_req_min", 1))
ADDV_MAX = int(BEHAV.get("addv_req_max", 10))

VIP = CFG["vip_invite"]
VIP_MAP = VIP["map"]
TTL = int(VIP.get("ttl_sec", 3600))
LIMIT = int(VIP.get("limit", 1))
INVITE_TPL = VIP.get("template", "Akses {tier} aktif.\nLink (berlaku 1 jam, 1x pakai): {link}")

STO = CFG["storage"]
NOTES_PATH = STO["notes"]
INVITE_LOG = STO["invite_log"]

os.makedirs("data", exist_ok=True)
os.makedirs(os.path.dirname(NOTES_PATH), exist_ok=True)
os.makedirs(os.path.dirname(INVITE_LOG), exist_ok=True)

def make_client() -> TelegramClient:
    if PREFER_STRING:
        if STRING_SESSION:
            return TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
        return TelegramClient(StringSession(), API_ID, API_HASH)
    return TelegramClient(SESSION_FILE, API_ID, API_HASH)

client = make_client()

def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def log_action(action: str, data: dict):
    rec = {"time": now_iso(), "action": action}
    rec.update(data)
    with open(INVITE_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    log.info("%s | %s", action, data)

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

async def interactive_login_and_persist_string():
    await client.connect()
    if await client.is_user_authorized():
        s = client.session.save()
        CFG["telegram"]["string_session"] = s
        save_cfg(CFG)
        print("\n=== STRING SESSION ===\n" + s + "\n======================\n")
        return
    phone = os.getenv("USHER_PHONE") or input("Masukkan nomor HP (+62xxxx): ").strip()
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

def addv_to_link_cmd(addv_cmd: str) -> str:
    base = addv_cmd.lstrip("/")
    return "link" + base[-2:]

async def create_invite(link_cmd: str, require_approval: bool = False) -> Optional[str]:
    peer_cfg = VIP_MAP.get(link_cmd)
    if not peer_cfg:
        log_action("invite_error", {"tier": link_cmd, "err": "peer_not_configured"})
        return None
    try:
        entity = await client.get_entity(peer_cfg)
        expire = int(time.time()) + TTL

        kwargs = dict(
            peer=entity,
            expire_date=expire,
            request_needed=require_approval
        )
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
        entity = await client.get_entity(VIP_MAP[peer_key])
        await client(functions.messages.EditExportedChatInviteRequest(peer=entity, link=link, revoked=True))
        log_action("invite_revoked", {"tier": peer_key, "link": link})
    except Exception as e:
        log_action("revoke_err", {"tier": peer_key, "err": str(e)})

async def send_invite_to_user(user_id: int, tier_cmd: str, link: str):
    msg = INVITE_TPL.format(tier=tier_cmd.upper(), link=link)
    await client.send_message(user_id, msg, silent=SILENT_DM)

async def get_target_user_from_context(event: events.NewMessage.Event) -> Optional[int]:
    if event.is_reply:
        r = await event.get_reply_message()
        if r and r.sender_id and not r.is_channel:
            return r.sender_id
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

def clip_req(n: int) -> int:
    if n < ADDV_MIN: return ADDV_MIN
    if n > ADDV_MAX: return ADDV_MAX
    return n

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

async def capture_note_from_reply(event, title: str, caption_override: Optional[str]) -> Optional[dict]:
    r = await event.get_reply_message()
    if not r:
        return None
    media = None
    media_type = None
    caption_text = (caption_override or r.message or "").strip()
    if r.photo:
        media = r.photo; media_type = "photo"
    elif r.document and not r.video:
        media = r.document; media_type = "document"
    elif r.video:
        media = r.video; media_type = "video"
    if media:
        d = notes_media_dir(title)
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
        path = await client.download_media(media, file=d)
        size = os.path.getsize(path) if os.path.exists(path) else None
        return {"type": media_type, "text": caption_text, "media": {"path": path, "size": size}}
    if caption_text:
        return {"type": "text", "text": caption_text}
    return None

@client.on(events.NewMessage(from_users=list(ADMIN_IDS)))
async def admin_handler(event: events.NewMessage.Event):
    text_raw = event.raw_text.strip()
    text = text_raw.lower()

    if text.startswith(("/linkv1", "/linkv2", "/linkv3")):
        link_cmd = text.split()[0].lstrip("/")
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
        return

    if text.startswith(("/addv1", "/addv2", "/addv3")):
        cmd_token = text.split()[0]
        target = await get_target_user_from_context(event)
        consumed_first_arg = False
        if not event.is_reply:
            parts = text_raw.split()
            if len(parts) >= 2:
                a1 = parts[1]
                if a1.startswith("@") or extract_int_token(a1) is not None:
                    consumed_first_arg = True
        if not target:
            log_action("ignored_addv", {"reason": "no_target", "tier": cmd_token.lstrip("/"), "raw": text_raw})
            return
        req_no = parse_req_no_or_none(text_raw, consumed_first_arg)
        tier_key = f"v{cmd_token[-1]}"
        relay_cmd = CMD_MAP.get(tier_key, cmd_token)
        payload = f"{relay_cmd} {target}" if req_no is None else f"{relay_cmd} {target} {req_no}"
        await client.send_message(BOT_USERNAME, payload)
        lp = {"tier": tier_key, "target": target, "to": BOT_USERNAME}
        if req_no is not None: lp["req_no"] = req_no
        log_action("relay_addv", lp)

        if COMBO_ON:
            link_cmd = addv_to_link_cmd(cmd_token)
            async def make_and_send():
                try:
                    if WAIT_BOT_REPLY > 0:
                        try:
                            bot = await client.get_entity(BOT_USERNAME)
                            await client.wait_for(events.NewMessage(from_users=bot.id), timeout=WAIT_BOT_REPLY)
                        except Exception:
                            pass
                    await asyncio.sleep(5)
                    link = await create_invite(link_cmd, True)
                    if not link:
                        raise RuntimeError("create_invite_failed")
                    await send_invite_to_user(target, link_cmd, link)
                    inv_log = {"tier": link_cmd, "target": target, "link": link, "status": "pending"}
                    if req_no is not None: inv_log["req_no"] = req_no
                    log_action("invite_sent", inv_log)
                except Exception as e:
                    log_action("invite_dm_failed", {"tier": link_cmd, "target": target, "err": str(e)})
            if COMBO_ORDER == "relay_first":
                await make_and_send()
            else:
                asyncio.create_task(make_and_send())
        return

    if text.startswith("/savenote"):
        parts = text_raw.split(maxsplit=1)
        if len(parts) < 2:
            await event.reply("Format: reply konten atau /savenote <judul> | <teks>")
            return
        tail = parts[1]
        if "|" in tail:
            title, cap = [p.strip() for p in tail.split("|", 1)]
        else:
            title, cap = tail.strip(), None
        title_key = sanitize_title(title)
        if not title_key:
            await event.reply("Format: reply konten atau /savenote <judul> | <teks>")
            return
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
                    await client.send_file(target_id, path, caption=note.get("text", "") or "", force_document=(note["type"] == "document"))
                else:
                    await client.send_message(target_id, note.get("text", "") or "")
            log_action("note_get_sent", {"title": title_key, "type": note["type"], "target": target_id})
        except Exception as e:
            log_action("note_get_send_error", {"title": title_key, "err": str(e)})
        return

@client.on(events.ChatAction)
async def on_chat_action(event: events.ChatAction.Event):
    if not (event.user_joined or event.user_added):
        return
    user = await event.get_user()
    user_id = user.id
    if not os.path.exists(INVITE_LOG):
        return
    link = None
    tier = None
    try:
        with open(INVITE_LOG, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for line in reversed(lines):
            d = json.loads(line)
            if d.get("action") == "invite_sent" and int(d.get("target", 0)) == user_id:
                link = d.get("link")
                tier = d.get("tier")
                break
            if d.get("action") == "invite_sent_manual" and int(d.get("target", 0)) == user_id:
                link = d.get("link")
                tier = d.get("tier")
                break
    except Exception:
        return
    if not link or not tier:
        return
    try:
        await revoke_invite(tier, link)
        log_action("invite_consumed", {"tier": tier, "target": user_id, "link": link})
    except Exception as e:
        log_action("invite_consumed_err", {"tier": tier, "target": user_id, "err": str(e)})

async def main():
    if PREFER_STRING and not STRING_SESSION:
        await interactive_login_and_persist_string()
    else:
        await client.connect()
    log.info("Usher started: addv relay + join-request link, manual /linkv1/2/3, notes.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
