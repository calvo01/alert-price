' Roda o watcher sem abrir janela de console.
' Chamado pelo Task Scheduler ao logon.
' Ajuste ScriptDir se movido pra outro path.
Set WshShell = CreateObject("WScript.Shell")
ScriptDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
WshShell.CurrentDirectory = ScriptDir
WshShell.Run "cmd /c venv\Scripts\pythonw.exe watcher.py > watcher.log 2>&1", 0, False
