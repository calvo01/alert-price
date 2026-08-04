"""Renova MERCADOLIVRE_COOKIE no Oracle a partir do painel de afiliado.

Fluxo:
1. Abre https://www.mercadolivre.com.br/afiliados/home em Chrome persistente (headless).
2. Se ainda logado -> pega cookies, sobe pro Oracle via SCP, restarta o systemd.
3. Se sessao caiu -> reabre em modo visivel, espera Felipe logar (ate 3min), repete.

Alertas de falha vao pro Telegram (mesmo bot/admin do alerta_bot).
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from urllib import parse, request

from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).resolve().parent
PROFILE_DIR = BASE_DIR / "chrome_profile"
COOKIE_TMP = BASE_DIR / "cookie.txt"
BOT_ENV = BASE_DIR.parent / ".env"

ORACLE_HOST = "ubuntu@163.176.219.29"
ORACLE_KEY = r"C:\Users\feter\.ssh\oracle_bot"
ENV_PATH = "/home/ubuntu/alerta_bot/.env"
SYSTEMD_UNIT = "price-alert-bot"

TARGET_URL = "https://www.mercadolivre.com.br/afiliados/home"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _read_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


ENV = _read_env(BOT_ENV)
TELEGRAM_TOKEN = ENV.get("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID = ENV.get("ADMIN_CHAT_ID", "")


def _notify(msg: str) -> None:
    print(msg, file=sys.stderr)
    if not TELEGRAM_TOKEN or not ADMIN_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = parse.urlencode(
            {"chat_id": ADMIN_CHAT_ID, "text": msg, "parse_mode": "HTML"}
        ).encode()
        request.urlopen(url, data=data, timeout=10).read()
    except Exception as exc:
        print(f"[WARN] telegram alert falhou: {exc}", file=sys.stderr)


def _cookies_to_header(ctx) -> str:
    cookies = ctx.cookies("https://www.mercadolivre.com.br")
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies)


def _is_logged_in(url: str) -> bool:
    return "/afiliados/" in url and "/hub/" not in url and "/jms" not in url


def _grab_cookie() -> str | None:
    with sync_playwright() as pw:
        for headless in (True, False):
            ctx = pw.chromium.launch_persistent_context(
                str(PROFILE_DIR),
                headless=headless,
                viewport={"width": 1280, "height": 800},
                user_agent=USER_AGENT,
            )
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                page.goto(TARGET_URL, timeout=30000, wait_until="domcontentloaded")
            except PWTimeout:
                pass

            if _is_logged_in(page.url):
                cookie = _cookies_to_header(ctx)
                ctx.close()
                if cookie and "_csrf" in cookie:
                    return cookie
                print("[WARN] cookie sem _csrf, tentando de novo", file=sys.stderr)
                continue

            if headless:
                print("[INFO] sessao expirou, reabrindo em modo visivel", file=sys.stderr)
                ctx.close()
                continue

            print(
                "[ACAO] faca login no ML. Detecto quando cair no painel (timeout 3min).",
                file=sys.stderr,
            )
            try:
                page.wait_for_url("**/afiliados/**", timeout=180000)
                page.goto(TARGET_URL, wait_until="domcontentloaded")
                cookie = _cookies_to_header(ctx)
                ctx.close()
                if cookie and "_csrf" in cookie:
                    return cookie
                print("[ERRO] cookie sem _csrf apos login", file=sys.stderr)
            except PWTimeout:
                print("[ERRO] timeout esperando login manual", file=sys.stderr)
                ctx.close()
    return None


def _sync_to_oracle(cookie: str) -> bool:
    COOKIE_TMP.write_text(cookie, encoding="utf-8")
    remote_cmd = (
        f"sed -i '/^MERCADOLIVRE_COOKIE=/d' {ENV_PATH} && "
        f"printf 'MERCADOLIVRE_COOKIE=%s\\n' \"$(cat /tmp/ml_cookie.txt)\" >> {ENV_PATH} && "
        f"rm /tmp/ml_cookie.txt && "
        f"sudo systemctl restart {SYSTEMD_UNIT}"
    )
    try:
        subprocess.run(
            ["scp", "-i", ORACLE_KEY, str(COOKIE_TMP), f"{ORACLE_HOST}:/tmp/ml_cookie.txt"],
            check=True,
        )
        subprocess.run(
            ["ssh", "-i", ORACLE_KEY, ORACLE_HOST, remote_cmd],
            check=True,
        )
        return True
    except subprocess.CalledProcessError as exc:
        _notify(
            "\u26a0\ufe0f <b>Refresher ML falhou</b>\n"
            f"Sync com Oracle deu erro: <code>{exc}</code>"
        )
        return False
    finally:
        COOKIE_TMP.unlink(missing_ok=True)


def main() -> int:
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        cookie = _grab_cookie()
    except Exception as exc:
        _notify(
            "\u26a0\ufe0f <b>Refresher ML crashou</b>\n"
            f"<code>{type(exc).__name__}: {exc}</code>"
        )
        raise

    if not cookie:
        _notify(
            "\ud83d\udd10 <b>Refresher ML: sessao expirou</b>\n"
            "Rode manualmente <code>run.bat</code> pra fazer login no ML.\n"
            "Enquanto isso, links saem sem afiliado."
        )
        return 1

    if _sync_to_oracle(cookie):
        print("[OK] cookie renovado, bot reiniciado no Oracle")
        _notify(
            "\u2705 <b>Cookie ML reconectado</b>\n"
            "Renovacao automatica concluida, bot reiniciado.\n"
            "Links do ML voltam a sair com afiliado."
        )
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
