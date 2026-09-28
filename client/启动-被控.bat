@echo off
chcp 65001 >nul
cd /d %~dp0
echo === p2pdesk : share this PC (be controlled) ===
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
python p2pdesk.py share
pause
