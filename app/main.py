import asyncio
import base64
import json
import secrets
import shutil
import sqlite3
import tempfile
import zlib
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import database
from .config import (
    API_KEYS,
    AUTH_ENABLED,
    DB_PATH,
    EXPORT_DIR,
    LOGIN_ADMIN,
    MIGRATION_SECRET_KEY,
    ROOT_DIR,
    SENHA_ADMIN,
    masked_keys,
)
from .imports import import_csv
from .schemas import ExportPayload, JobPayload, ManualQueryPayload, SettingsPayload
from .worker import job_manager


REAL_CONFIRMATION = "CONSULTAR API REAL"


@asynccontextmanager
async def lifespan(_: FastAPI):
    database.init_db()
    yield
    if job_manager.client and (not job_manager.task or job_manager.task.done()):
        await job_manager.client.close()


app = FastAPI(
    title="Consulta Claro V2",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url=None,
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if not AUTH_ENABLED or path in {"/api/health", "/api/admin/restore-database"}:
        return await call_next(request)

    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Basic "):
        try:
            encoded_credentials = auth_header[6:].strip()
            decoded = base64.b64decode(encoded_credentials).decode("utf-8")
            username, _, password = decoded.partition(":")
            if secrets.compare_digest(username, LOGIN_ADMIN) and secrets.compare_digest(password, SENHA_ADMIN):
                return await call_next(request)
        except Exception:
            pass

    return Response(
        content="Autenticação necessária.",
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="Consulta Claro V2"'},
        media_type="text/plain; charset=utf-8",
    )


app.mount("/static", StaticFiles(directory=ROOT_DIR / "static"), name="static")


@app.get("/")
async def index():
    return FileResponse(ROOT_DIR / "static" / "index.html")


@app.get("/api/health")
async def health():
    return {"ok": True, "database": database.DB_PATH.exists(), "api_keys": len(API_KEYS)}


@app.post("/api/admin/restore-database")
async def restore_database(request: Request):
    secret = request.headers.get("X-Migration-Secret", "").strip()
    if not MIGRATION_SECRET_KEY or not secrets.compare_digest(secret, MIGRATION_SECRET_KEY):
        raise HTTPException(status_code=401, detail="Chave de migração inválida ou não configurada.")

    if job_manager.task and not job_manager.task.done():
        raise HTTPException(status_code=409, detail="Existe um processamento ativo. Pause-o antes de restaurar o banco.")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_target = DB_PATH.with_suffix(".restoring")

    try:
        decompressor = zlib.decompressobj(31)
        with temp_target.open("wb") as output:
            async for chunk in request.stream():
                if chunk:
                    data = decompressor.decompress(chunk)
                    if data:
                        output.write(data)
            remaining = decompressor.flush()
            if remaining:
                output.write(remaining)

        conn = sqlite3.connect(temp_target)
        try:
            cur = conn.cursor()
            cur.execute("PRAGMA integrity_check")
            integrity = cur.fetchone()[0]
            if integrity != "ok":
                raise ValueError(f"Integridade do SQLite falhou: {integrity}")
            cur.execute("SELECT COUNT(*) FROM registros")
            count = cur.fetchone()[0]
        finally:
            conn.close()

        for ext in ["", "-wal", "-shm"]:
            target_file = DB_PATH.parent / f"{DB_PATH.name}{ext}" if ext else DB_PATH
            target_file.unlink(missing_ok=True)

        temp_target.replace(DB_PATH)
        database.init_db()

        return {
            "ok": True,
            "message": "Banco de dados restaurado com sucesso.",
            "total_registros": count,
            "size_bytes": DB_PATH.stat().st_size,
        }
    except Exception as exc:
        temp_target.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Erro ao restaurar banco de dados: {exc}") from exc



@app.get("/api/dashboard/summary")
async def dashboard_summary():
    return await asyncio.to_thread(database.dashboard_summary)


@app.get("/api/records")
async def records(
    status: str | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=200),
):
    return await asyncio.to_thread(database.list_records, status, search, page, page_size)


@app.get("/api/results/options")
async def result_options():
    return await asyncio.to_thread(database.result_filter_options)


@app.post("/api/imports")
async def upload_import(
    file: UploadFile = File(...),
    cep_column: str = Form(...),
    numero_column: str = Form(...),
):
    suffix = Path(file.filename or "upload.csv").suffix or ".csv"
    temp_path = Path(tempfile.gettempdir()) / f"claro_v2_{id(file)}{suffix}"
    try:
        with temp_path.open("wb") as output:
            shutil.copyfileobj(file.file, output)
        return await asyncio.to_thread(import_csv, temp_path, cep_column, numero_column)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        temp_path.unlink(missing_ok=True)
        await file.close()


@app.post("/api/manual-query")
async def manual_query(payload: ManualQueryPayload):
    if payload.confirmation != REAL_CONFIRMATION:
        raise HTTPException(status_code=400, detail="Confirme que a consulta utilizará a API real.")
    try:
        return await job_manager.manual_query(payload.cep, payload.numero)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/api/jobs")
async def start_job(payload: JobPayload):
    if payload.confirmation != REAL_CONFIRMATION:
        raise HTTPException(status_code=400, detail="Confirmação da API real ausente.")
    if not API_KEYS:
        raise HTTPException(status_code=503, detail="Nenhuma chave de API configurada.")
    try:
        database.save_settings(
            payload.threads,
            payload.requests_per_second,
            payload.per_robot_delay_ms,
            payload.adaptive,
        )
        return await job_manager.start(
            payload.threads,
            payload.requests_per_second,
            payload.per_robot_delay_ms,
            payload.adaptive,
            payload.include_retry,
        )
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/jobs/{job_id}/pause")
async def pause_job(job_id: str):
    try:
        return await job_manager.pause(job_id)
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    try:
        return await job_manager.resume(job_id)
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    try:
        return await job_manager.cancel(job_id)
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/events")
async def events():
    async def stream():
        while True:
            try:
                payload = job_manager.snapshot()
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/settings")
async def get_settings():
    settings = await asyncio.to_thread(database.get_settings)
    return {
        **settings,
        "keys": masked_keys(),
        "confirmation_text": REAL_CONFIRMATION,
    }


@app.put("/api/settings")
async def put_settings(payload: SettingsPayload):
    return await asyncio.to_thread(
        database.save_settings,
        payload.threads,
        payload.requests_per_second,
        payload.per_robot_delay_ms,
        payload.adaptive,
    )


@app.post("/api/exports")
async def create_export(payload: ExportPayload, background: BackgroundTasks):
    export_id = await asyncio.to_thread(database.create_export)
    background.add_task(database.generate_export, export_id, payload.model_dump())
    return {"id": export_id, "status": "CRIADO"}


@app.get("/api/exports/{export_id}")
async def get_export(export_id: str):
    result = await asyncio.to_thread(database.export_status, export_id)
    if not result:
        raise HTTPException(status_code=404, detail="Exportação não encontrada.")
    if result.get("arquivo"):
        result["download_url"] = f"/api/exports/{export_id}/download"
    return result


@app.get("/api/exports/{export_id}/download")
async def download_export(export_id: str):
    result = await asyncio.to_thread(database.export_status, export_id)
    if not result or result.get("status") != "CONCLUIDO":
        raise HTTPException(status_code=404, detail="Arquivo ainda não está disponível.")
    path = Path(result["arquivo"])
    if not path.exists() or EXPORT_DIR not in path.parents:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado.")
    return FileResponse(path, filename=path.name, media_type="text/csv")
