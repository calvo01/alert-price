"""Roda 24/7 na maquina local. Faz polling SSH no servidor remoto a cada POLL_SECONDS.

Quando encontra a flag REFRESHER_FLAG_PATH (default /home/ubuntu/alerta_bot/COOKIE_NEEDS_REFRESH):
1. remove a flag remotamente (evita disparar de novo)
2. chama refresh_cookie.main()
3. Se refresh der ruim, recria a flag pra tentar de novo no proximo ciclo

Config via .env do bot: REFRESHER_SSH_HOST, REFRESHER_SSH_KEY, REFRESHER_FLAG_PATH.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

CREATE_NO_WINDOW = 0x08000000 if sys.platform == "win32" else 0

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import refresh_cookie  # noqa: E402

PID_FILE = BASE_DIR / "watcher.pid"


def _acquire_single_instance() -> bool:
    """Evita 2 watchers rodando ao mesmo tempo (Startup+Task Scheduler duplicavam)."""
    if PID_FILE.exists():
        try:
            other_pid = int(PID_FILE.read_text(encoding="utf-8").strip())
        except (ValueError, OSError):
            other_pid = 0
        if other_pid and other_pid != os.getpid() and _pid_alive(other_pid):
            print(f"[watcher] outro watcher ja rodando (PID {other_pid}), saindo",
                  flush=True)
            return False
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
    return True


def _pid_alive(pid: int) -> bool:
    if sys.platform == "win32":
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=5,
                creationflags=CREATE_NO_WINDOW,
            )
            return str(pid) in out.stdout
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

SSH_HOST = refresh_cookie.SSH_HOST
SSH_KEY = refresh_cookie.SSH_KEY
FLAG_PATH = refresh_cookie.ENV.get(
    "REFRESHER_FLAG_PATH", "/home/ubuntu/alerta_bot/COOKIE_NEEDS_REFRESH"
)
POLL_SECONDS = int(refresh_cookie.ENV.get("REFRESHER_POLL_SECONDS", "30"))
PAUSED_LOG_EVERY = 20  # loga status "pausado" a cada N ciclos (~10 min com poll=30s)

_ssh_broken_notified = False
_paused_ticks = 0


def _check_and_clear_flag() -> bool:
    """Retorna True se a flag existia (e foi removida). False se nao existia."""
    global _ssh_broken_notified
    try:
        # -e testa se existe, retorna 0 se sim, 1 se nao. Removemos imediatamente.
        cmd = f"test -e {FLAG_PATH} && rm {FLAG_PATH} && echo present || echo absent"
        result = subprocess.run(
            ["ssh", "-i", SSH_KEY, "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=10", SSH_HOST, cmd],
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
                "Sem conexao com o servidor remoto nao consigo reagir se cookie cair."
            )
            _ssh_broken_notified = True
        return False


def _recreate_flag() -> None:
    """Se refresh falhou por erro tecnico, recoloca a flag pra tentar de novo depois."""
    try:
        subprocess.run(
            ["ssh", "-i", SSH_KEY, "-o", "StrictHostKeyChecking=no",
             SSH_HOST, f"touch {FLAG_PATH}"],
            check=True, timeout=15,
            creationflags=CREATE_NO_WINDOW,
        )
    except Exception:
        pass


def _drain_flag_silent() -> None:
    """Consome a flag remota sem disparar refresh (usado enquanto pausado)."""
    try:
        subprocess.run(
            ["ssh", "-i", SSH_KEY, "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=10", SSH_HOST, f"rm -f {FLAG_PATH}"],
            timeout=15, creationflags=CREATE_NO_WINDOW,
        )
    except Exception:
        pass


def main() -> int:
    if not _acquire_single_instance():
        return 0
    if not SSH_HOST or not SSH_KEY:
        print("[watcher] REFRESHER_SSH_HOST/REFRESHER_SSH_KEY nao configurados no .env, "
              "watcher nao tem o que monitorar. Saindo.", flush=True)
        return 0
    print(f"[watcher] iniciado (PID {os.getpid()}), polling {POLL_SECONDS}s no servidor remoto",
          flush=True)
    global _paused_ticks
    while True:
        try:
            if refresh_cookie.PAUSED_FLAG.exists():
                # Pausado ate voce rodar run.bat manualmente.
                # Drena a flag remota pro servidor nao acumular disparos.
                _drain_flag_silent()
                if _paused_ticks % PAUSED_LOG_EVERY == 0:
                    print("[watcher] PAUSADO — aguardando run.bat manual "
                          "(delete PAUSED_UNTIL_MANUAL pra retomar)", flush=True)
                _paused_ticks += 1
            elif _check_and_clear_flag():
                _paused_ticks = 0
                print("[watcher] flag detectada, rodando refresh", flush=True)
                rc = refresh_cookie.main()
                if rc == refresh_cookie.RC_NEEDS_MANUAL:
                    print("[watcher] refresh precisa login manual, pausando "
                          "(sem retentar)", flush=True)
                elif rc == refresh_cookie.RC_TECH_ERROR:
                    print("[watcher] refresh falhou por erro tecnico, "
                          "recolocando flag", flush=True)
                    _recreate_flag()
        except KeyboardInterrupt:
            print("[watcher] parando", flush=True)
            return 0
        except Exception as exc:
            print(f"[watcher] erro no loop: {exc}", flush=True)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
