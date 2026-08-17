@echo off
setlocal
cd /d "%~dp0"
title Consulta Claro V2

if not exist ".venv\Scripts\python.exe" (
    echo Criando ambiente virtual independente...
    python -m venv .venv
    if errorlevel 1 goto :erro
)

".venv\Scripts\python.exe" -c "import fastapi, uvicorn, httpx, certifi, multipart" >nul 2>&1
if errorlevel 1 (
    echo Instalando dependencias da V2...
    ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -r requirements.txt
    if errorlevel 1 goto :erro
)

".venv\Scripts\python.exe" -c "import certifi, pathlib, sys; sys.exit(0 if pathlib.Path(certifi.where()).is_file() else 1)"
if errorlevel 1 (
    echo Reparando certificado HTTPS...
    ".venv\Scripts\python.exe" -m pip install --force-reinstall --no-cache-dir certifi
    if errorlevel 1 goto :erro
)

".venv\Scripts\python.exe" prepare_v2.py
if errorlevel 1 goto :erro

echo.
echo Iniciando em http://127.0.0.1:8520
".venv\Scripts\python.exe" run.py
goto :fim

:erro
echo.
echo Nao foi possivel iniciar a Consulta Claro V2.
pause

:fim
endlocal
