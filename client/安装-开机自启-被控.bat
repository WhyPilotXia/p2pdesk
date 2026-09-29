@echo off
:: p2pdesk auto-start installer (ASCII only, CRLF) - registers share for current user
cd /d %~dp0

echo === p2pdesk : install auto-start for current user ===
echo.
echo This registers "share" (controlled side) to run at every logon:
echo   - No admin rights needed
echo   - Works even after reboot or crash-restart
echo   - Client auto-reconnects to server forever
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [!] python not found in PATH. Install Python 3.9+ first.
    pause & exit /b 1
)

python -c "import websockets, cv2, mss, numpy, pyautogui, aiortc, pywinpty" >nul 2>nul
if errorlevel 1 (
    echo [*] installing dependencies ...
    python -m pip install -r requirements.txt -i https://mirrors.tencent.com/pypi/simple
)

schtasks /Query /TN "p2pdesk-share" >nul 2>nul
if not errorlevel 1 (
    schtasks /Delete /TN "p2pdesk-share" /F >nul 2>nul
)

schtasks /Create /TN "p2pdesk-share" /TR "\"%CD%\run-share-guarded.bat\"" /SC ONLOGON /RL LIMITED /F
if errorlevel 1 (
    echo [!] schtasks failed, trying Startup folder shortcut instead ...
    goto :startupfolder
)

echo.
echo [+] Task "p2pdesk-share" created ^(ONLOGON^).
echo [i] It starts at EVERY logon, including after reboot or crash.
goto :done

:startupfolder
set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "TGT=%CD%\run-share-guarded.bat"
powershell -NoProfile -Command "$s = (New-Object -ComObject WScript.Shell).CreateShortcut('%STARTUP%\p2pdesk-share.lnk'); $s.TargetPath='%TGT%'; $s.WorkingDirectory='%CD%'; $s.Save()" >nul
if errorlevel 1 (
    echo [!] both schtasks and Startup folder failed. Please install manually.
    pause & exit /b 1
)
echo [+] Startup shortcut created.

:done
echo.
echo [*] starting share now in this window ...
call run-share-guarded.bat
