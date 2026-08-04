"""Roda 24/7 no PC do Felipe. Faz polling SSH no Oracle a cada POLL_SECONDS.

Quando encontra a flag /home/ubuntu/alerta_bot/COOKIE_NEEDS_REFRESH:
1. remove a flag remotamente (evita disparar de novo)
2. chama refresh_cookie.main()
3. Se refresh der ruim, recria a flag pra tentar de novo no proximo ciclo
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

CREATE_NO_WINDOW = 0x08000000 if sys.platform == "win32" else 0

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import refresh_cookie  # noqa: E402

ORACLE_HOST = "ubuntu@163.176.219.29"
ORACLE_KEY = r"C:\Users\feter\.ssh\oracle_bot"
FLAG_PATH = "/home/ubuntu/alerta_bot/COOKIE_NEEDS_REFRESH"
POLL_SECONDS = 30

_ssh_broken_notified = False


def _check_and_clear_flag() -> bool:
    """Retorna True se a flag existia (e foi removida). False se nao existia."""
    global _ssh_broken_notified
    try:
        # -e testa se existe, retorna 0 se sim, 1 se nao. Removemos imediatamente.
        cmd = f"test -e {FLAG_PATH} && rm {FLAG_PATH} && echo present || echo absent"
        result = subprocess.run(
            ["ssh", "-i", ORACLE_KEY, "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=10", ORACLE_HOST, cmd],
            capture_output=True, text=True, timeout=20,
            creationflags=CREATE_NO_WINDOW,
        )
        _ssh_broken_notified = False
        return "present" in result.stdout
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        if not _ssh_broken_notified:
            refresh_cookie._notify(
                "\u26a0\ufe0f <b>Watcher ML: SSH falhou</b>\n"
                f"<code>{exc}</code>\n"
                "Sem conexao com Oracle nao consigo reagir se cookie cair."
            )
            _ssh_broken_notified = True
        return False


def _recreate_flag() -> None:
    """Se refresh falhou, recoloca a flag pra tentar de novo depois."""
    try:
        subprocess.run(
            ["ssh", "-i", ORACLE_KEY, "-o", "StrictHostKeyChecking=no",
             ORACLE_HOST, f"touch {FLAG_PATH}"],
            check=True, timeout=15,
            creationflags=CREATE_NO_WINDOW,
        )
    except Exception:
        pass


def main() -> int:
    print(f"[watcher] iniciado, polling {POLL_SECONDS}s no Oracle", flush=True)
    while True:
        try:
            if _check_and_clear_flag():
                print("[watcher] flag detectada, rodando refresh", flush=True)
                rc = refresh_cookie.main()
                if rc != 0:
                    print("[watcher] refresh falhou, recolocando flag", flush=True)
                    _recreate_flag()
        except KeyboardInterrupt:
            print("[watcher] parando", flush=True)
            return 0
        except Exception as exc:
            print(f"[watcher] erro no loop: {exc}", flush=True)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
