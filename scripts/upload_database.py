import argparse
import gzip
import os
import sqlite3
import sys
import time
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


def generate_compressed_stream(source_path: Path, block_size: int = 256 * 1024):
    total_raw_bytes = source_path.stat().st_size
    raw_read = 0
    compressed_sent = 0
    start_time = time.time()
    last_print = 0

    with source_path.open("rb") as f:
        # We can use gzip compressor in memory
        compressor = gzip.compressobj(level=6, method=gzip.DEFLATED, wbits=31)
        while True:
            chunk = f.read(block_size)
            if not chunk:
                break
            raw_read += len(chunk)
            data = compressor.compress(chunk)
            if data:
                compressed_sent += len(data)
                now = time.time()
                if now - last_print >= 0.5 or raw_read >= total_raw_bytes:
                    pct = (raw_read / total_raw_bytes) * 100
                    elapsed = max(0.1, now - start_time)
                    speed_mb = (compressed_sent / (1024 * 1024)) / elapsed
                    print(
                        f"\rProgresso: {pct:5.1f}% | Lido: {raw_read / (1024*1024):.1f} MB | "
                        f"Enviado (Gzip): {compressed_sent / (1024*1024):.1f} MB | {speed_mb:.2f} MB/s",
                        end="",
                        flush=True,
                    )
                    last_print = now
                yield data

        data = compressor.flush()
        if data:
            compressed_sent += len(data)
            yield data

    print(f"\nCompactação e streaming finalizados: {compressed_sent / (1024*1024):.1f} MB enviados.")


def main():
    parser = argparse.ArgumentParser(description="Upload do banco de dados SQLite para o servidor remoto.")
    parser.add_argument(
        "--url",
        default="https://169.58.168.174/api/admin/restore-database",
        help="URL do endpoint de restore",
    )
    parser.add_argument(
        "--host",
        default="claro.thconect.com.br",
        help="Header Host para o Traefik",
    )
    parser.add_argument(
        "--secret",
        default="",
        help="Chave secreta de migração (se não informada, lê do .env)",
    )
    parser.add_argument(
        "--db",
        default=str(DB_PATH),
        help="Caminho do arquivo de banco de dados SQLite local",
    )
    args = parser.parse_args()

    db_file = Path(args.db)
    if not db_file.exists():
        print(f"Erro: Arquivo do banco de dados não encontrado em {db_file}")
        sys.exit(1)

    secret = args.secret or load_env_migration_key()
    if not secret:
        print("Erro: Chave MIGRATION_SECRET_KEY não informada. Defina no .env ou passe via --secret")
        sys.exit(1)

    checkpoint_wal(db_file)

    total_size = db_file.stat().st_size
    print(f"Tamanho original do banco: {total_size / (1024 * 1024):.2f} MB")
    print(f"Destino: {args.url} (Host: {args.host})")

    headers = {
        "X-Migration-Secret": secret,
        "Host": args.host,
        "Content-Type": "application/octet-stream",
    }

    try:
        r = requests.post(
            args.url,
            data=generate_compressed_stream(db_file),
            headers=headers,
            verify=False,
            timeout=1800,
        )
        print("Status code:", r.status_code)
        print("Resposta do servidor:", r.text)
        if r.status_code == 200:
            print("\n*** Banco de dados restaurado e verificado com sucesso no Easypanel! ***")
        else:
            print("\n*** Erro na restauração do banco de dados ***")
            sys.exit(1)
    except Exception as err:
        print(f"\nErro durante a transmissão: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
