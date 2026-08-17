import argparse
import os
import sqlite3
import sys
import time
import uuid
import zlib
from pathlib import Path
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "consulta_claro_v2.db"
ENV_PATH = ROOT_DIR / ".env"


def load_env_migration_key() -> str:
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("MIGRATION_SECRET_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.getenv("MIGRATION_SECRET_KEY", "")


def checkpoint_wal(db_path: Path) -> None:
    print(f"Executando checkpoint WAL no banco local: {db_path}...")
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        print("Checkpoint WAL concluído com sucesso.")
    except Exception as exc:
        print(f"Aviso ao executar checkpoint WAL: {exc}")


def compress_in_chunks(source_path: Path, chunk_size_mb: int = 15):
    target_chunk_bytes = chunk_size_mb * 1024 * 1024
    compressor = zlib.compressobj(6, zlib.DEFLATED, 31)
    current_chunk = bytearray()
    raw_read = 0
    total_raw = source_path.stat().st_size

    with source_path.open("rb") as f:
        while True:
            raw_block = f.read(512 * 1024)
            if not raw_block:
                break
            raw_read += len(raw_block)
            data = compressor.compress(raw_block)
            if data:
                current_chunk.extend(data)
                if len(current_chunk) >= target_chunk_bytes:
                    yield bytes(current_chunk), raw_read, total_raw
                    current_chunk.clear()

        remaining = compressor.flush()
        if remaining:
            current_chunk.extend(remaining)
        if current_chunk:
            yield bytes(current_chunk), raw_read, total_raw


def main():
    parser = argparse.ArgumentParser(description="Upload resiliente do banco SQLite em chunks.")
    parser.add_argument(
        "--base-url",
        default="https://169.58.168.174",
        help="URL base do servidor",
    )
    parser.add_argument(
        "--host",
        default="claro.thconect.com.br",
        help="Header Host para o Traefik",
    )
    parser.add_argument(
        "--secret",
        default="",
        help="Chave secreta de migração",
    )
    parser.add_argument(
        "--db",
        default=str(DB_PATH),
        help="Caminho do arquivo do banco de dados",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=15,
        help="Tamanho de cada chunk comprimido em MB",
    )
    args = parser.parse_args()

    db_file = Path(args.db)
    if not db_file.exists():
        print(f"Erro: Banco de dados não encontrado em {db_file}")
        sys.exit(1)

    secret = args.secret or load_env_migration_key()
    if not secret:
        print("Erro: MIGRATION_SECRET_KEY não informada.")
        sys.exit(1)

    checkpoint_wal(db_file)

    total_size_mb = db_file.stat().st_size / (1024 * 1024)
    session_id = uuid.uuid4().hex[:12]
    print(f"Iniciando migração em chunks (Sessão: {session_id})")
    print(f"Banco original: {total_size_mb:.2f} MB | Destino: {args.base_url} (Host: {args.host})")

    headers = {
        "X-Migration-Secret": secret,
        "Host": args.host,
        "Content-Type": "application/octet-stream",
    }

    chunks_data = []
    print("Gerando chunks compactados...")
    for chunk_bytes, raw_read, total_raw in compress_in_chunks(db_file, args.chunk_size):
        chunks_data.append(chunk_bytes)
        pct = (raw_read / total_raw) * 100
        print(f"\rCompactado: {pct:5.1f}% | Total de chunks: {len(chunks_data)}", end="", flush=True)

    total_chunks = len(chunks_data)
    total_compressed_mb = sum(len(c) for c in chunks_data) / (1024 * 1024)
    print(f"\nTotal gerado: {total_chunks} chunks ({total_compressed_mb:.2f} MB comprimidos).")

    start_upload = time.time()
    for idx, chunk in enumerate(chunks_data):
        url = f"{args.base_url}/api/admin/upload-chunk?chunk_index={idx}&session_id={session_id}"
        success = False
        for attempt in range(3):
            try:
                r = requests.post(url, data=chunk, headers=headers, verify=False, timeout=60)
                if r.status_code == 200:
                    success = True
                    break
                else:
                    print(f"\nTentativa {attempt+1} falhou para chunk {idx+1}/{total_chunks}: HTTP {r.status_code}")
            except Exception as e:
                print(f"\nTentativa {attempt+1} falhou para chunk {idx+1}/{total_chunks}: {e}")
            time.sleep(1)

        if not success:
            print(f"\nErro fatal ao enviar chunk {idx+1}/{total_chunks}. Abortando.")
            sys.exit(1)

        elapsed = max(0.1, time.time() - start_upload)
        sent_mb = sum(len(chunks_data[i]) for i in range(idx + 1)) / (1024 * 1024)
        speed = sent_mb / elapsed
        pct = ((idx + 1) / total_chunks) * 100
        print(f"\rEnviando chunks: {idx+1}/{total_chunks} ({pct:5.1f}%) | {sent_mb:.1f}/{total_compressed_mb:.1f} MB | {speed:.2f} MB/s", end="", flush=True)

    print("\nTodos os chunks enviados! Solicitando restauração e verificação de integridade no servidor...")
    finish_url = f"{args.base_url}/api/admin/finish-restore?total_chunks={total_chunks}&session_id={session_id}"
    try:
        r = requests.post(finish_url, headers=headers, verify=False, timeout=120)
        print("Status code:", r.status_code)
        print("Resposta do servidor:", r.text)
        if r.status_code == 200:
            print("\n*** SUCESSO: Banco de dados restaurado e validado no Easypanel! ***")
        else:
            print("\n*** Erro na finalização da restauração ***")
            sys.exit(1)
    except Exception as exc:
        print(f"\nErro ao finalizar restauração: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
