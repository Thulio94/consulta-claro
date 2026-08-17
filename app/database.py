from __future__ import annotations

import csv
import json
import re
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import DB_PATH, EXPORT_DIR, MAX_PAGE_SIZE


_schema_lock = threading.Lock()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_cep(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value).split(".")[0])
    return digits.zfill(8)[-8:] if digits else ""


def normalize_numero(value: Any) -> str:
    text = str(value).strip()
    if text.lower() in {"", "nan", "none"}:
        return ""
    return text[:-2] if text.endswith(".0") else text


def connect(path: Path | None = None, readonly: bool = False) -> sqlite3.Connection:
    path = Path(path or DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    else:
        conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def transaction(path: Path | None = None):
    conn = connect(path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, name: str, definition: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if name not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def init_db(path: Path | None = None) -> None:
    path = Path(path or DB_PATH)
    with _schema_lock:
        conn = connect(path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS registros (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cep TEXT NOT NULL,
                    numero TEXT NOT NULL,
                    status TEXT DEFAULT 'PENDENTE',
                    ultima_consulta DATETIME,
                    resultado_json TEXT,
                    UNIQUE(cep, numero)
                )
                """
            )
            _ensure_column(conn, "registros", "tentativas", "INTEGER NOT NULL DEFAULT 0")
            _ensure_column(conn, "registros", "proxima_tentativa", "TEXT")
            _ensure_column(conn, "registros", "ultimo_http", "INTEGER")
            _ensure_column(conn, "registros", "ultimo_erro", "TEXT")
            _ensure_column(conn, "registros", "job_id", "TEXT")
            _ensure_column(conn, "registros", "worker_id", "INTEGER")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    threads INTEGER NOT NULL,
                    delay_ms INTEGER NOT NULL,
                    adaptive INTEGER NOT NULL DEFAULT 1,
                    include_retry INTEGER NOT NULL DEFAULT 0,
                    criado_em TEXT NOT NULL,
                    iniciado_em TEXT,
                    finalizado_em TEXT,
                    total INTEGER NOT NULL DEFAULT 0,
                    processados INTEGER NOT NULL DEFAULT 0,
                    sucessos INTEGER NOT NULL DEFAULT 0,
                    erros INTEGER NOT NULL DEFAULT 0,
                    http_429 INTEGER NOT NULL DEFAULT 0,
                    pause_requested INTEGER NOT NULL DEFAULT 0,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    rate_scale REAL NOT NULL DEFAULT 1.0,
                    cooldown_until TEXT,
                    mensagem TEXT
                );

                CREATE TABLE IF NOT EXISTS resultados_endereco (
                    registro_id INTEGER PRIMARY KEY,
                    cep TEXT,
                    numero TEXT,
                    logradouro TEXT,
                    bairro TEXT,
                    cidade TEXT,
                    uf TEXT,
                    status_resultado TEXT,
                    atualizado_em TEXT,
                    FOREIGN KEY(registro_id) REFERENCES registros(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS tecnologias (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    registro_id INTEGER NOT NULL,
                    nome TEXT,
                    status TEXT,
                    internet INTEGER NOT NULL DEFAULT 0,
                    tv INTEGER NOT NULL DEFAULT 0,
                    fone INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(registro_id, nome, status),
                    FOREIGN KEY(registro_id) REFERENCES registros(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS settings (
                    chave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS exports (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    arquivo TEXT,
                    criado_em TEXT NOT NULL,
                    finalizado_em TEXT,
                    linhas INTEGER NOT NULL DEFAULT 0,
                    erro TEXT
                );

                CREATE TABLE IF NOT EXISTS metadata (
                    chave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_registros_status_id
                    ON registros(status, id);
                CREATE INDEX IF NOT EXISTS idx_registros_status_data
                    ON registros(status, ultima_consulta, id);
                CREATE INDEX IF NOT EXISTS idx_registros_retry
                    ON registros(status, proxima_tentativa, id);
                CREATE INDEX IF NOT EXISTS idx_registros_job
                    ON registros(job_id, status);
                CREATE INDEX IF NOT EXISTS idx_resultados_local
                    ON resultados_endereco(uf, cidade, status_resultado);
                CREATE INDEX IF NOT EXISTS idx_tecnologias_filtro
                    ON tecnologias(nome, status, registro_id);
                """
            )
            _ensure_column(
                conn,
                "jobs",
                "latency_total_ms",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(conn, "jobs", "requests_per_second", "REAL")
            conn.execute(
                """
                UPDATE jobs
                   SET requests_per_second =
                       MAX(0.1, threads * 1000.0 / MAX(1, delay_ms))
                 WHERE requests_per_second IS NULL
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO settings(chave, valor) VALUES ('threads', '2')"
            )
            conn.execute(
                "INSERT OR IGNORE INTO settings(chave, valor) VALUES ('delay_ms', '1000')"
            )
            conn.execute(
                "INSERT OR IGNORE INTO settings(chave, valor) VALUES ('adaptive', '1')"
            )
            threads_value = float(
                conn.execute(
                    "SELECT valor FROM settings WHERE chave='threads'"
                ).fetchone()[0]
            )
            delay_value = float(
                conn.execute(
                    "SELECT valor FROM settings WHERE chave='delay_ms'"
                ).fetchone()[0]
            )
            legacy_rps = max(0.1, threads_value * 1000.0 / max(100.0, delay_value))
            conn.execute(
                """
                INSERT OR IGNORE INTO settings(chave, valor)
                VALUES ('requests_per_second', ?)
                """,
                (str(round(legacy_rps, 3)),),
            )
            conn.execute(
                """
                UPDATE registros
                   SET status='PENDENTE', job_id=NULL, worker_id=NULL
                 WHERE status='EM_PROCESSAMENTO'
                """
            )
            conn.execute(
                """
                UPDATE jobs
                   SET status='FALHOU', finalizado_em=?,
                       mensagem='Servidor reiniciado; registros liberados para nova execução'
                 WHERE status='EXECUTANDO'
                """,
                (utc_now(),),
            )
            conn.commit()
        finally:
            conn.close()


def backup_database(source: Path, destination: Path, progress=None) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=60)
    destination_conn = sqlite3.connect(destination, timeout=60)
    try:
        def on_progress(status: int, remaining: int, total: int) -> None:
            if progress:
                progress(total - remaining, total)

        source_conn.backup(destination_conn, pages=2048, progress=on_progress, sleep=0.05)
    finally:
        destination_conn.close()
        source_conn.close()


def _parse_result(payload: dict[str, Any], cep_db: str, numero_db: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return (
            {
                "cep": cep_db,
                "numero": numero_db,
                "logradouro": "",
                "bairro": "",
                "cidade": "",
                "uf": "",
                "status_resultado": "RESPOSTA SEM DADOS",
            },
            [],
        )
    technologies = data.get("technologies") or []
    tech_rows: list[dict[str, Any]] = []
    statuses: list[str] = []
    for tech in technologies if isinstance(technologies, list) else []:
        if not isinstance(tech, dict):
            continue
        building = tech.get("building") if isinstance(tech.get("building"), dict) else {}
        status = str(building.get("status") or tech.get("hpStatus") or "DESCONHECIDO")
        statuses.append(status)
        tech_rows.append(
            {
                "nome": str(tech.get("name") or "N/A"),
                "status": status,
                "internet": int(bool(tech.get("internet"))),
                "tv": int(bool(tech.get("tv"))),
                "fone": int(bool(tech.get("phone"))),
            }
        )
    return (
        {
            "cep": str(data.get("cep") or cep_db),
            "numero": str(data.get("number") or numero_db),
            "logradouro": str(data.get("logradouro") or ""),
            "bairro": str(data.get("bairro") or ""),
            "cidade": str(data.get("cidade") or ""),
            "uf": str(data.get("uf") or ""),
            "status_resultado": ", ".join(sorted(set(statuses))) if statuses else "SEM TECNOLOGIA",
        },
        tech_rows,
    )


def _upsert_structured(conn: sqlite3.Connection, registro_id: int, cep: str, numero: str, payload: dict[str, Any]) -> None:
    address, technologies = _parse_result(payload, cep, numero)
    conn.execute(
        """
        INSERT INTO resultados_endereco(
            registro_id, cep, numero, logradouro, bairro, cidade, uf,
            status_resultado, atualizado_em
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(registro_id) DO UPDATE SET
            cep=excluded.cep, numero=excluded.numero, logradouro=excluded.logradouro,
            bairro=excluded.bairro, cidade=excluded.cidade, uf=excluded.uf,
            status_resultado=excluded.status_resultado,
            atualizado_em=excluded.atualizado_em
        """,
        (
            registro_id,
            address["cep"],
            address["numero"],
            address["logradouro"],
            address["bairro"],
            address["cidade"],
            address["uf"],
            address["status_resultado"],
            utc_now(),
        ),
    )
    conn.execute("DELETE FROM tecnologias WHERE registro_id=?", (registro_id,))
    conn.executemany(
        """
        INSERT OR IGNORE INTO tecnologias(
            registro_id, nome, status, internet, tv, fone
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                registro_id,
                tech["nome"],
                tech["status"],
                tech["internet"],
                tech["tv"],
                tech["fone"],
            )
            for tech in technologies
        ],
    )


def migrate_existing_results(path: Path | None = None, progress=None, batch_size: int = 2000) -> dict[str, int]:
    path = Path(path or DB_PATH)
    init_db(path)
    conn = connect(path)
    migrated = 0
    invalid = 0
    try:
        conn.execute(
            """
            UPDATE registros
               SET status='AGUARDANDO_RETRY',
                   ultimo_http=429,
                   ultimo_erro='HTTP 429 importado da V1'
             WHERE status='ERRO'
               AND resultado_json LIKE '%Erro HTTP 429%'
            """
        )
        conn.execute(
            """
            UPDATE registros
               SET status='ERRO_PERMANENTE',
                   ultimo_erro=COALESCE(ultimo_erro, 'Erro importado da V1')
             WHERE status='ERRO'
            """
        )
        conn.commit()
        total = conn.execute(
            """
            SELECT COUNT(*)
              FROM registros r
             WHERE r.status='CONSULTADO'
               AND r.resultado_json IS NOT NULL
               AND NOT EXISTS (
                    SELECT 1 FROM resultados_endereco e WHERE e.registro_id=r.id
               )
            """
        ).fetchone()[0]
        last_id = 0
        while True:
            rows = conn.execute(
                """
                SELECT id, cep, numero, resultado_json
                  FROM registros r
                 WHERE r.id>?
                   AND r.status='CONSULTADO'
                   AND r.resultado_json IS NOT NULL
                   AND NOT EXISTS (
                        SELECT 1 FROM resultados_endereco e WHERE e.registro_id=r.id
                   )
                 ORDER BY r.id
                 LIMIT ?
                """,
                (last_id, batch_size),
            ).fetchall()
            if not rows:
                break
            conn.execute("BEGIN")
            for row in rows:
                last_id = row["id"]
                try:
                    payload = json.loads(row["resultado_json"])
                    _upsert_structured(conn, row["id"], row["cep"], row["numero"], payload)
                    migrated += 1
                except Exception:
                    invalid += 1
            conn.commit()
            if progress:
                progress(migrated + invalid, total)
        conn.execute(
            "INSERT OR REPLACE INTO metadata(chave, valor) VALUES ('structured_migration', ?)",
            (json.dumps({"migrated": migrated, "invalid": invalid, "at": utc_now()}),),
        )
        conn.commit()
        return {"migrated": migrated, "invalid": invalid}
    finally:
        conn.close()


def get_settings() -> dict[str, Any]:
    conn = connect(readonly=True)
    try:
        values = {row["chave"]: row["valor"] for row in conn.execute("SELECT chave, valor FROM settings")}
        return {
            "threads": int(values.get("threads", "2")),
            "per_robot_delay_ms": int(values.get("delay_ms", "1000")),
            "requests_per_second": float(
                values.get(
                    "requests_per_second",
                    str(
                        int(values.get("threads", "2"))
                        * 1000
                        / max(100, int(values.get("delay_ms", "1000")))
                    ),
                )
            ),
            "adaptive": values.get("adaptive", "1") == "1",
        }
    finally:
        conn.close()


def save_settings(
    threads: int,
    requests_per_second: float,
    per_robot_delay_ms: int,
    adaptive: bool,
) -> dict[str, Any]:
    threads = max(1, min(10, int(threads)))
    requests_per_second = max(0.1, min(1000.0, float(requests_per_second)))
    per_robot_delay_ms = max(0, min(60_000, int(per_robot_delay_ms)))
    with transaction() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO settings(chave, valor) VALUES (?, ?)",
            [
                ("threads", str(threads)),
                ("delay_ms", str(per_robot_delay_ms)),
                ("requests_per_second", str(requests_per_second)),
                ("adaptive", "1" if adaptive else "0"),
            ],
        )
    return {
        "threads": threads,
        "requests_per_second": requests_per_second,
        "per_robot_delay_ms": per_robot_delay_ms,
        "adaptive": adaptive,
    }


def dashboard_summary() -> dict[str, Any]:
    conn = connect(readonly=True)
    try:
        counts = {row["status"]: row["n"] for row in conn.execute("SELECT status, COUNT(*) n FROM registros GROUP BY status")}
        job = conn.execute("SELECT * FROM jobs ORDER BY criado_em DESC LIMIT 1").fetchone()
        return {
            "total": sum(counts.values()),
            "counts": counts,
            "structured": conn.execute("SELECT COUNT(*) FROM resultados_endereco").fetchone()[0],
            "technologies": conn.execute("SELECT COUNT(*) FROM tecnologias").fetchone()[0],
            "latest_job": dict(job) if job else None,
        }
    finally:
        conn.close()


def list_records(status: str | None, search: str | None, page: int, page_size: int) -> dict[str, Any]:
    page = max(1, int(page))
    page_size = max(1, min(MAX_PAGE_SIZE, int(page_size)))
    where: list[str] = []
    params: list[Any] = []
    if status and status != "TODOS":
        where.append("r.status=?")
        params.append(status)
    if search:
        where.append("(r.cep LIKE ? OR r.numero LIKE ?)")
        term = f"%{search.strip()}%"
        params.extend([term, term])
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    conn = connect(readonly=True)
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM registros r {where_sql}", params).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT r.id, r.cep, r.numero, r.status, r.ultima_consulta,
                   r.ultimo_http, r.ultimo_erro,
                   e.cidade, e.uf, e.status_resultado
              FROM registros r
              LEFT JOIN resultados_endereco e ON e.registro_id=r.id
              {where_sql}
             ORDER BY r.id
             LIMIT ? OFFSET ?
            """,
            [*params, page_size, (page - 1) * page_size],
        ).fetchall()
        return {
            "items": [dict(row) for row in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max(1, (total + page_size - 1) // page_size),
        }
    finally:
        conn.close()


def result_filter_options() -> dict[str, list[str]]:
    conn = connect(readonly=True)
    try:
        def values(column: str, table: str) -> list[str]:
            return [
                str(row[0])
                for row in conn.execute(
                    f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL AND {column}<>'' ORDER BY {column} LIMIT 500"
                )
            ]
        return {
            "ufs": values("uf", "resultados_endereco"),
            "cidades": values("cidade", "resultados_endereco"),
            "tecnologias": values("nome", "tecnologias"),
            "status_resultados": values("status_resultado", "resultados_endereco"),
        }
    finally:
        conn.close()


def insert_records(rows: Iterable[tuple[str, str]], batch_size: int = 10_000) -> dict[str, int]:
    inserted = 0
    ignored = 0
    buffer: list[tuple[str, str]] = []
    conn = connect()
    try:
        for cep_raw, numero_raw in rows:
            cep = normalize_cep(cep_raw)
            numero = normalize_numero(numero_raw)
            if not cep or not numero:
                ignored += 1
                continue
            buffer.append((cep, numero))
            if len(buffer) >= batch_size:
                before = conn.total_changes
                conn.executemany("INSERT OR IGNORE INTO registros(cep, numero) VALUES (?, ?)", buffer)
                conn.commit()
                changed = conn.total_changes - before
                inserted += changed
                ignored += len(buffer) - changed
                buffer.clear()
        if buffer:
            before = conn.total_changes
            conn.executemany("INSERT OR IGNORE INTO registros(cep, numero) VALUES (?, ?)", buffer)
            conn.commit()
            changed = conn.total_changes - before
            inserted += changed
            ignored += len(buffer) - changed
        return {"inserted": inserted, "ignored": ignored}
    finally:
        conn.close()


def create_job(
    threads: int,
    requests_per_second: float,
    per_robot_delay_ms: int,
    adaptive: bool,
    include_retry: bool,
) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    requests_per_second = max(0.1, min(1000.0, float(requests_per_second)))
    per_robot_delay_ms = max(0, min(60_000, int(per_robot_delay_ms)))
    statuses = ["PENDENTE", "AGUARDANDO_RETRY"] if include_retry else ["PENDENTE"]
    placeholders = ",".join("?" for _ in statuses)
    conn = connect()
    try:
        active = conn.execute(
            "SELECT id FROM jobs WHERE status IN ('EXECUTANDO','PAUSADO') LIMIT 1"
        ).fetchone()
        if active:
            raise RuntimeError("Já existe um trabalho ativo ou pausado.")
        total = conn.execute(
            f"SELECT COUNT(*) FROM registros WHERE status IN ({placeholders})",
            statuses,
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO jobs(
                id, status, threads, delay_ms, requests_per_second,
                adaptive, include_retry,
                criado_em, total
            ) VALUES (?, 'CRIADO', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                threads,
                per_robot_delay_ms,
                requests_per_second,
                int(adaptive),
                int(include_retry),
                utc_now(),
                total,
            ),
        )
        conn.commit()
        return dict(conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
    finally:
        conn.close()


def update_job(job_id: str, **values: Any) -> None:
    if not values:
        return
    allowed = {
        "status", "iniciado_em", "finalizado_em", "processados", "sucessos",
        "erros", "http_429", "pause_requested", "cancel_requested",
        "rate_scale", "cooldown_until", "mensagem",
    }
    clean = {key: value for key, value in values.items() if key in allowed}
    if not clean:
        return
    assignments = ", ".join(f"{key}=?" for key in clean)
    with transaction() as conn:
        conn.execute(
            f"UPDATE jobs SET {assignments} WHERE id=?",
            [*clean.values(), job_id],
        )


def get_job(job_id: str) -> dict[str, Any] | None:
    conn = connect(readonly=True)
    try:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def latest_job() -> dict[str, Any] | None:
    conn = connect(readonly=True)
    try:
        row = conn.execute("SELECT * FROM jobs ORDER BY criado_em DESC LIMIT 1").fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def claim_batch(job_id: str, include_retry: bool, limit: int) -> list[dict[str, Any]]:
    statuses = ["PENDENTE", "AGUARDANDO_RETRY"] if include_retry else ["PENDENTE"]
    placeholders = ",".join("?" for _ in statuses)
    with transaction() as conn:
        rows = conn.execute(
            f"""
            SELECT id, cep, numero, tentativas
              FROM registros
             WHERE status IN ({placeholders})
               AND (proxima_tentativa IS NULL OR proxima_tentativa<=?)
             ORDER BY id
             LIMIT ?
            """,
            [*statuses, utc_now(), limit],
        ).fetchall()
        if rows:
            ids = [row["id"] for row in rows]
            marks = ",".join("?" for _ in ids)
            conn.execute(
                f"""
                UPDATE registros
                   SET status='EM_PROCESSAMENTO', job_id=?, worker_id=NULL
                 WHERE id IN ({marks})
                """,
                [job_id, *ids],
            )
        return [dict(row) for row in rows]


def release_claims(job_id: str) -> None:
    with transaction() as conn:
        conn.execute(
            """
            UPDATE registros
               SET status='PENDENTE', job_id=NULL, worker_id=NULL
             WHERE job_id=? AND status='EM_PROCESSAMENTO'
            """,
            (job_id,),
        )


def write_results(job_id: str, results: list[dict[str, Any]]) -> dict[str, int]:
    counters = {
        "processados": 0,
        "sucessos": 0,
        "erros": 0,
        "http_429": 0,
        "latency_total_ms": 0,
    }
    if not results:
        return counters
    with transaction() as conn:
        for result in results:
            raw_json = json.dumps(result.get("payload") or {}, ensure_ascii=False)
            conn.execute(
                """
                UPDATE registros
                   SET status=?, ultima_consulta=?, resultado_json=?,
                       tentativas=?, proxima_tentativa=?, ultimo_http=?,
                       ultimo_erro=?, job_id=NULL, worker_id=?
                 WHERE id=?
                """,
                (
                    result["status"],
                    utc_now(),
                    raw_json,
                    result.get("attempts", 1),
                    result.get("next_retry"),
                    result.get("http_status"),
                    result.get("error"),
                    result.get("worker_id"),
                    result["id"],
                ),
            )
            counters["processados"] += 1
            counters["latency_total_ms"] += max(0, int(result.get("latency_ms", 0)))
            if result["status"] == "CONSULTADO":
                counters["sucessos"] += 1
                _upsert_structured(
                    conn,
                    result["id"],
                    result["cep"],
                    result["numero"],
                    result.get("payload") or {},
                )
            elif result.get("http_status") == 429:
                counters["http_429"] += 1
            else:
                counters["erros"] += 1
        conn.execute(
            """
            UPDATE jobs
               SET processados=processados+?,
                   sucessos=sucessos+?,
                   erros=erros+?,
                   http_429=http_429+?,
                   latency_total_ms=latency_total_ms+?
             WHERE id=?
            """,
            (
                counters["processados"],
                counters["sucessos"],
                counters["erros"],
                counters["http_429"],
                counters["latency_total_ms"],
                job_id,
            ),
        )
    return counters


def get_or_create_record(cep: str, numero: str) -> int:
    cep = normalize_cep(cep)
    numero = normalize_numero(numero)
    if not cep or not numero:
        raise ValueError("CEP e número são obrigatórios.")
    with transaction() as conn:
        conn.execute("INSERT OR IGNORE INTO registros(cep, numero) VALUES (?, ?)", (cep, numero))
        return conn.execute("SELECT id FROM registros WHERE cep=? AND numero=?", (cep, numero)).fetchone()[0]


def write_manual_result(result: dict[str, Any]) -> None:
    with transaction() as conn:
        raw_json = json.dumps(result.get("payload") or {}, ensure_ascii=False)
        conn.execute(
            """
            UPDATE registros
               SET status=?, ultima_consulta=?, resultado_json=?,
                   tentativas=?, proxima_tentativa=?, ultimo_http=?,
                   ultimo_erro=?, job_id=NULL, worker_id=0
             WHERE id=?
            """,
            (
                result["status"],
                utc_now(),
                raw_json,
                result.get("attempts", 1),
                result.get("next_retry"),
                result.get("http_status"),
                result.get("error"),
                result["id"],
            ),
        )
        if result["status"] == "CONSULTADO":
            _upsert_structured(
                conn,
                result["id"],
                result["cep"],
                result["numero"],
                result.get("payload") or {},
            )


def create_export() -> str:
    export_id = uuid.uuid4().hex
    with transaction() as conn:
        conn.execute(
            "INSERT INTO exports(id, status, criado_em) VALUES (?, 'CRIADO', ?)",
            (export_id, utc_now()),
        )
    return export_id


def export_status(export_id: str) -> dict[str, Any] | None:
    conn = connect(readonly=True)
    try:
        row = conn.execute("SELECT * FROM exports WHERE id=?", (export_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def generate_export(export_id: str, filters: dict[str, str | None]) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"claro_resultados_{export_id[:8]}.csv"
    path = EXPORT_DIR / filename
    where = ["r.status='CONSULTADO'"]
    params: list[Any] = []
    for key, column in (
        ("uf", "e.uf"),
        ("cidade", "e.cidade"),
        ("tecnologia", "t.nome"),
        ("status_resultado", "e.status_resultado"),
    ):
        value = filters.get(key)
        if value:
            where.append(f"{column}=?")
            params.append(value)
    conn = connect()
    try:
        conn.execute("UPDATE exports SET status='PROCESSANDO' WHERE id=?", (export_id,))
        conn.commit()
        cursor = conn.execute(
            f"""
            SELECT r.cep AS CEP, r.numero AS NUMERO,
                   e.logradouro AS LOGRADOURO, e.bairro AS BAIRRO,
                   e.cidade AS CIDADE, e.uf AS UF,
                   COALESCE(t.nome, 'N/A') AS TECNOLOGIA,
                   COALESCE(t.status, e.status_resultado) AS STATUS,
                   COALESCE(t.internet, 0) AS INTERNET,
                   COALESCE(t.tv, 0) AS TV,
                   COALESCE(t.fone, 0) AS FONE
              FROM registros r
              JOIN resultados_endereco e ON e.registro_id=r.id
              LEFT JOIN tecnologias t ON t.registro_id=r.id
             WHERE {' AND '.join(where)}
             ORDER BY r.id
            """,
            params,
        )
        lines = 0
        with path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle, delimiter=";")
            writer.writerow([col[0] for col in cursor.description])
            while True:
                rows = cursor.fetchmany(5000)
                if not rows:
                    break
                writer.writerows([tuple(row) for row in rows])
                lines += len(rows)
        conn.execute(
            """
            UPDATE exports
               SET status='CONCLUIDO', arquivo=?, finalizado_em=?, linhas=?
             WHERE id=?
            """,
            (str(path), utc_now(), lines, export_id),
        )
        conn.commit()
    except Exception as exc:
        conn.execute(
            "UPDATE exports SET status='FALHOU', erro=?, finalizado_em=? WHERE id=?",
            (str(exc), utc_now(), export_id),
        )
        conn.commit()
    finally:
        conn.close()
