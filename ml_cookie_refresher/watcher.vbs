' Roda o watcher sem abrir janela de console.
' Chamado pelo Task Scheduler ao logon.
Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "C:\Users\feter\alerta_bot\ml_cookie_refresher"
WshShell.Run "cmd /c venv\Scripts\pythonw.exe watcher.py > watcher.log 2>&1", 0, False
