import json
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Make repo root importable and import our downloader helpers
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Option 1: Put your Telegram bot token here (recommended for quick local use)
# Example: INLINE_BOT_TOKEN = "123456:ABC-DEF_your_bot_token_here"
INLINE_BOT_TOKEN = "7714227792:AAEGVV1ohshLUn3rGtkxuqXhs0wyOwqgTDo"

try:
    from tools.download_repo_debs import (
        try_fetch_packages,
        maybe_decompress,
        parse_filenames_from_packages,
        download_many,
    )
except Exception as ex:
    print("Failed to import downloader helpers:", ex)
    raise


def getenv(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"Missing environment variable: {name}")
    return val

def _parse_env_file(p: Path) -> Optional[str]:
    try:
        if not p.exists():
            return None
        for line in p.read_text(encoding='utf-8', errors='ignore').splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                if k.strip() == 'TELEGRAM_BOT_TOKEN':
                    return v.strip().strip('"').strip("'")
    except Exception:
        return None
    return None


def load_token(repo_root: Path) -> Tuple[Optional[str], Optional[str]]:
    # Priority: inline -> env var -> tools/bot_token.txt -> .env -> tools/.env
    if INLINE_BOT_TOKEN and INLINE_BOT_TOKEN.strip():
        return INLINE_BOT_TOKEN.strip(), 'inline'
    env_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    if env_token:
        return env_token, 'env'
    candidates = [
        repo_root / 'tools' / 'bot_token.txt',
        repo_root / '.env',
        repo_root / 'tools' / '.env',
    ]
    for fp in candidates:
        if fp.name == 'bot_token.txt' and fp.exists():
            try:
                tok = fp.read_text(encoding='utf-8').strip()
                if tok:
                    return tok, str(fp)
            except Exception:
                pass
        else:
            tok = _parse_env_file(fp)
            if tok:
                return tok, str(fp)
    return None, None


BOT_TOKEN, TOKEN_SRC = load_token(REPO_ROOT)
if not BOT_TOKEN:
    print("Set INLINE_BOT_TOKEN inside tg_bot_downloader.py, OR set TELEGRAM_BOT_TOKEN env var, OR put your token in tools/bot_token.txt (single line).")

API_BASE = None if not BOT_TOKEN else f"https://api.telegram.org/bot{BOT_TOKEN}"


def api_call(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not API_BASE:
        raise SystemExit("TELEGRAM_BOT_TOKEN not set.")
    url = f"{API_BASE}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read()
        obj = json.loads(body.decode("utf-8"))
        if not obj.get("ok", False):
            raise RuntimeError(f"Telegram API error: {obj}")
        return obj["result"]


def get_me() -> Dict[str, Any]:
    return api_call('getMe', {})


def send_message(chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> int:
    res = api_call("sendMessage", {"chat_id": chat_id, "text": text, "reply_to_message_id": reply_to_message_id})
    return res["message_id"]


def edit_message(chat_id: int, message_id: int, text: str):
    api_call("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": text})


def get_updates(offset: Optional[int], timeout: int = 50):
    payload: Dict[str, Any] = {"timeout": timeout}
    if offset is not None:
        payload["offset"] = offset
    return api_call("getUpdates", payload)


URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)


def parse_command(text: str) -> Tuple[Optional[str], Optional[int], Optional[float]]:
    # Extract first URL, optional max=N and delay=S options
    if not text:
        return None, None, None
    m = URL_RE.search(text)
    url = m.group(0) if m else None
    max_n = None
    delay_s = None
    m2 = re.search(r"\bmax\s*=\s*(\d+)", text, re.IGNORECASE)
    if m2:
        try:
            max_n = int(m2.group(1))
        except ValueError:
            pass
    m3 = re.search(r"\bdelay\s*=\s*([0-9]*\.?[0-9]+)", text, re.IGNORECASE)
    if m3:
        try:
            delay_s = float(m3.group(1))
        except ValueError:
            pass
    return url, max_n, delay_s


class Job:
    def __init__(self, chat_id: int, message_id: int, base_url: str, max_n: Optional[int], delay_s: Optional[float]):
        self.chat_id = chat_id
        self.message_id = message_id
        self.base_url = base_url
        self.max_n = max_n
        self.delay_s = delay_s
        self.cancel = False
        self.thread = threading.Thread(target=self.run, daemon=True)

    def start(self):
        self.thread.start()

    def run(self):
        start_ts = time.time()
        try:
            # Fetch and parse Packages
            files = []
            try:
                pk_url, data = try_fetch_packages(self.base_url, None, timeout=20.0, retries=2, delay=0.5, user_agent="RepoDebFetcher/1.0")
                text = maybe_decompress(data, pk_url)
                files = parse_filenames_from_packages(text)
            except Exception as ex:
                # Fallback to directory listing scrape
                html = urllib.request.urlopen(self.base_url, timeout=20).read().decode('utf-8', errors='replace')
                # Reuse downloader's simple parser via a tiny inline function
                files = []
                for m in re.finditer(r'href\s*=\s*["\']([^"\'#?]+)["\']', html, re.IGNORECASE):
                    href = m.group(1)
                    if href.lower().endswith('.deb'):
                        if href.startswith('http://') or href.startswith('https://'):
                            files.append(href)
                        else:
                            base = self.base_url if self.base_url.endswith('/') else self.base_url + '/'
                            files.append(urllib.parse.urljoin(base, href))
            if not files:
                edit_message(self.chat_id, self.message_id, f"لم أجد أي ملفات .deb في {self.base_url}.")
                return
            # Output folder: downloads/<host>/<timestamp>
            host = urllib.parse.urlparse(self.base_url).netloc.replace(':', '_') or 'repo'
            ts = time.strftime('%Y%m%d-%H%M%S', time.localtime(start_ts))
            dest = REPO_ROOT / 'downloads' / host / ts
            # Start download
            ok, skip, fail = download_many(
                self.base_url,
                files,
                dest_root=dest,
                timeout=20.0,
                retries=2,
                delay=self.delay_s if self.delay_s is not None else 0.5,
                user_agent="RepoDebFetcher/1.0",
                dry_run=False,
                max_items=self.max_n,
            )
            dur = time.time() - start_ts
            edit_message(self.chat_id, self.message_id, f"تم. ok={ok} skip={skip} fail={fail}\nالمجلد: {dest}\nالوقت: {dur:.1f}s")
        except Exception as ex:
            edit_message(self.chat_id, self.message_id, f"فشل: {ex}")


running: Dict[int, Job] = {}


def main():
    if not BOT_TOKEN:
        print("Usage: set TELEGRAM_BOT_TOKEN env var OR create tools/bot_token.txt with the token, then run this script.")
        return
    # Validate token via getMe for a clean error if invalid
    try:
        me = get_me()
        bot_name = me.get('username') or me.get('first_name') or 'bot'
        print(f"Bot started as @{bot_name}. Token source: {TOKEN_SRC}.")
        print("Send /start to your bot and paste a repo URL.")
    except Exception as ex:
        print("Invalid bot token or network issue:", ex)
        return
    offset = None
    while True:
        try:
            updates = get_updates(offset)
            for upd in updates:
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message")
                if not msg:
                    continue
                chat = msg.get("chat", {})
                chat_id = chat.get("id")
                text = msg.get("text") or ""
                if text.strip().lower().startswith("/start"):
                    send_message(chat_id, "أرسل رابط المستودع (https://...) ويمكن إضافة max=رقم و delay=ثانية، مثال:\nhttps://apt.example.com max=100 delay=0.2")
                    continue
                if text.strip().lower().startswith("/cancel"):
                    job = running.pop(chat_id, None)
                    if job:
                        job.cancel = True
                        send_message(chat_id, "تم طلب الإلغاء (قد يستغرق لحظات)")
                    else:
                        send_message(chat_id, "لا توجد مهمة قيد التنفيذ")
                    continue
                url, max_n, delay_s = parse_command(text)
                if not url:
                    send_message(chat_id, "لم أتعرف على رابط. أعد الإرسال بشكل: https://apt.example.com max=50")
                    continue
                if chat_id in running:
                    send_message(chat_id, "مهمة قيد التنفيذ بالفعل. أرسل /cancel للإلغاء أولًا أو انتظر انتهاءها.")
                    continue
                msg_id = send_message(chat_id, f"بدء التحميل من:\n{url}\nالخيارات: max={max_n or 'الكل'}, delay={delay_s or 0.5}s")
                job = Job(chat_id, msg_id, url, max_n, delay_s)
                running[chat_id] = job
                job.start()
        except Exception as ex:
            print("Loop error:", ex)
            time.sleep(2)


if __name__ == "__main__":
    main()
