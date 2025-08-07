@echo off
REM Activate virtual environment
call "C:\Kritagya Folder\FileVineBI\filevineBIDashboardServer\venv\Scripts\activate.bat"

REM Start Uvicorn (no reload) on port 8000
start "" uvicorn app:app --host 0.0.0.0 --port 8000 --log-level debug

REM Give the server a few seconds to spin up
timeout /t 5 /nobreak

REM Start Cloudflare Tunnel
cd "C:\Program Files (x86)\cloudflared"
start "" cloudflared tunnel --url http://localhost:8000/webhook
