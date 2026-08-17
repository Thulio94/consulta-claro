import argparse
import gzip
import io
import os
import sqlite3
import sys
import time
from pathlib import Path
import urllib.request
import urllib.error

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
        print("Checkpoint WAL concluido com sucesso.")
    except Exception as exc:
        print(f"Aviso ao executar checkpoint WAL: {exc}")


class GzipStreamReader(io.RawIOBase):
    """Streams a file compressed on-the-fly with gzip to avoid writing large temporary files to disk."""

    def __init__(self, source_path: Path, block_size: int = 256 * 1024):
        self.source_file = source_path.open("rb")
        self.total_raw_bytes = source_path.stat().st_size
        self.raw_read = 0
        self.block_size = block_size
        self.compressor = gzip.GzipFile(fileobj=self._BufferWriter(self), mode="wb")
        self.out_buffer = bytearray()
        self.eof_reached = False

    class _BufferWriter:
        def __init__(self, parent):
            self.parent = parent

        def write(self, data):
            self.parent.out_buffer.extend(data)

    def readable(self):
        return True

    def readinto(self, b):
        while len(self.out_buffer) < len(b) and not self.eof_reached:
            chunk = self.source_file.read(self.block_size)
            if not chunk:
                self.eof_reached = True
                self.compressor.close()
                break
            self.raw_read += len(chunk)
            self.compressor.write(chunk)
            self.compressor.flush()

        if not self.out_buffer:
            return 0

        to_read = min(len(b), len(self.out_buffer))
        b[:to_read] = self.out_buffer[:to_read]
        del self.out_buffer[:to_read]
        return to_read

    def close(self):
        self.source_file.close()
        super().close()


def main():
    parser = argparse.ArgumentParser(description="Upload do banco de dados SQLite para o servidor remoto.")
    parser.add_argument(
        "--url",
        default="https://claro.thconect.com.br/api/admin/restore-database",
        help="URL do endpoint de restore",
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
    print(f"Iniciando streaming comprimido para: {args.url}")

    stream_reader = GzipStreamReader(db_file)
    start_time = time.time()

    class ProgressStream(io.RawIOBase):
        def __init__(self, reader, total_raw):
            self.reader = reader
            self.total_raw = total_raw
            self.compressed_sent = 0
            self.last_print = 0

        def readable(self):
            return True

        def readinto(self, b):
            n = self.reader.readinto(b)
            if n:
                self.compressed_sent += n
                now = time.time()
                if now - self.last_print >= 0.5 or self.reader.raw_read >= self.total_raw:
                    pct = (self.reader.raw_read / self.total_raw) * 100
                    elapsed = max(0.1, now - start_time)
                    speed_mb = (self.compressed_sent / (1024 * 1024)) / elapsed
                    print(
                        f"\rProgresso: {pct:5.1f}% | Lido: {self.reader.raw_read / (1024*1024):.1f} MB | "
                        f"Enviado (Gzip): {self.compressed_sent / (1024*1024):.1f} MB | {speed_mb:.2f} MB/s",
                        end="",
                        flush=True,
                    )
                    self.last_print = now
            return n

    progress_stream = ProgressStream(stream_reader, total_size)
    req = urllib.request.Request(
        args.url,
        data=progress_stream,
        headers={
            "X-Migration-Secret": secret,
            "Content-Type": "application/octet-stream",
            "Content-Encoding": "gzip",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=600) as response:
            print("\n")
            print("Status code:", response.status)
            body = response.read().decode("utf-8")
            print("Resposta do servidor:", body)
            print("Migração concluída com sucesso!")
    except urllib.error.HTTPError as err:
        print(f"\nErro HTTP {err.code}: {err.read().decode('utf-8')}")
        sys.exit(1)
    except Exception as err:
        print(f"\nErro durante upload: {err}")
        sys.exit(1)
    finally:
        stream_reader.close()


if __name__ == "__main__":
    main()
