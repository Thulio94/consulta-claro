from __future__ import annotations

import ast
import sys
from pathlib import Path

from app.config import DB_PATH, ENV_PATH, SOURCE_DB_PATH, SOURCE_DIR
from app.database import backup_database, init_db, migrate_existing_results


def extract_api_keys() -> list[str]:
    source = (SOURCE_DIR / "api_client.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == "API_KEYS" for target in node.targets):
                value = ast.literal_eval(node.value)
                if isinstance(value, list) and all(isinstance(item, str) for item in value):
                    return value
    return []


def ensure_env() -> None:
    if ENV_PATH.exists():
        return
    keys = extract_api_keys()
    if not keys:
        raise RuntimeError("Não foi possível importar as chaves da V1.")
    ENV_PATH.write_text(
        "CLARO_API_KEYS=" + ",".join(keys) + "\n"
        "CLARO_API_URL=https://planos.claronet.com/api/api.php\n"
        "CLARO_V2_PORT=8520\n"
        "CLARO_THREADS=2\n"
        "CLARO_REQUESTS_PER_SECOND=2\n",
        encoding="utf-8",
    )


def main() -> None:
    print("Preparando Consulta Claro V2...")
    ensure_env()
    if not DB_PATH.exists():
        if not SOURCE_DB_PATH.exists():
            raise FileNotFoundError(f"Banco da V1 não encontrado: {SOURCE_DB_PATH}")
        print("Criando cópia consistente do banco atual...")
        last_percent = -1

        def backup_progress(done: int, total: int) -> None:
            nonlocal last_percent
            percent = int(done / max(total, 1) * 100)
            if percent // 10 != last_percent // 10:
                print(f"  Banco: {percent}%")
                last_percent = percent

        backup_database(SOURCE_DB_PATH, DB_PATH, backup_progress)
    init_db()
    print("Estruturando os resultados existentes...")
    last_percent = -1

    def migration_progress(done: int, total: int) -> None:
        nonlocal last_percent
        percent = int(done / max(total, 1) * 100)
        if percent // 5 != last_percent // 5:
            print(f"  Resultados: {percent}%")
            last_percent = percent

    result = migrate_existing_results(progress=migration_progress)
    print(f"Pronto: {result['migrated']} resultados estruturados; {result['invalid']} inválidos.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERRO: {exc}")
        sys.exit(1)
