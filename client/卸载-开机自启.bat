@echo off
:: p2pdesk auto-start uninstaller (ASCII only, CRLF)
cd /d %~dp0

echo === p2pdesk : uninstall auto-start ===
echo.

schtasks /Query /TN "p2pdesk-share" >nul 2>nul
if not errorlevel 1 (
    schtasks /Delete /TN "p2pdesk-share" /F >nul 2>nul
    echo [+] scheduled task "p2pdesk-share" removed.
)

if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\p2pdesk-share.lnk" (
    del "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\p2pdesk-share.lnk"
    echo [+] startup shortcut removed.
)

taskkill /FI "WINDOWTITLE eq p2pdesk share*" /T >nul 2>nul
echo [+] done.
pause
