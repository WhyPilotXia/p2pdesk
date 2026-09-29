@echo off
:: p2pdesk share guardian loop (ASCII only, CRLF) - restarts python if it exits
:: Called by scheduled task / Startup shortcut, or run manually
cd /d %~dp0
title p2pdesk share guarded

:loop
python p2pdesk.py share
echo [!] p2pdesk exited (code %errorlevel%), restart in 10s ... press Ctrl+C to stop forever.
timeout /t 10 /nobreak >nul
goto loop
