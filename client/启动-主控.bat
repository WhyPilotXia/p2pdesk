@echo off
chcp 65001 >nul
cd /d %~dp0
echo === p2pdesk : control remote PC ===
where python >nul 2>nul
if errorlevel 1 (
    echo [!] python not found in PATH. Install Python 3.9+ first.
    pause & exit /b 1
)
python -c "import websockets, cv2, mss, numpy, pyautogui, aiortc" >nul 2>nul
if errorlevel 1 (
    echo [*] installing dependencies ...
    python -m pip install -r requirements.txt -i https://mirrors.tencent.com/pypi/simple
)
if "%~1"=="" (
    echo [*] no peer id given, listing online peers ...
    python p2pdesk.py list
    echo.
    set /p PEER=Enter peer id to control: 
    if "%PEER%"=="" ( echo [!] empty peer id & pause & exit /b 1 )
    set PEER_ARG=%PEER%
) else (
    set PEER_ARG=%~1
)
python p2pdesk.py watch %PEER_ARG%
pause
