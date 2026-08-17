from __future__ import annotations

import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
EXPORT_DIR = ROOT_DIR / "exports"
DB_PATH = DATA_DIR / "consulta_claro_v2.db"
ENV_PATH = ROOT_DIR / ".env"
SOURCE_DIR = Path(r"C:\Users\Thulio\Desktop\PROJETOS\consumir api\consulta_claro")
SOURCE_DB_PATH = SOURCE_DIR / "dados_viabilidade.db"


def _load_env_file() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_env_file()

API_URL = os.getenv("CLARO_API_URL", "https://planos.claronet.com/api/api.php")
API_KEYS = [
    value.strip()
    for value in os.getenv("CLARO_API_KEYS", "").split(",")
    if value.strip()
]
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", os.getenv("CLARO_V2_PORT", "8520")))
DEFAULT_THREADS = max(1, min(10, int(os.getenv("CLARO_THREADS", "2"))))
DEFAULT_REQUESTS_PER_SECOND = max(
    0.1,
    min(1000.0, float(os.getenv("CLARO_REQUESTS_PER_SECOND", "2"))),
)
MAX_PAGE_SIZE = 200

AUTH_ENABLED = os.getenv("AUTH_ENABLED", "false").strip().lower() in {"true", "1", "yes"}
LOGIN_ADMIN = os.getenv("LOGIN_ADMIN", os.getenv("login-admin", os.getenv("AUTH_USER", ""))).strip()
SENHA_ADMIN = os.getenv("SENHA_ADMIN", os.getenv("senha-admin", os.getenv("AUTH_PASSWORD", ""))).strip()
MIGRATION_SECRET_KEY = os.getenv("MIGRATION_SECRET_KEY", "").strip()


def masked_keys() -> list[str]:
    return [f"••••{key[-4:]}" if len(key) >= 4 else "••••" for key in API_KEYS]


def read_api_keys() -> list[str]:
    if ENV_PATH.exists():
        for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if line.startswith("CLARO_API_KEYS="):
                return [
                    value.strip()
                    for value in line.split("=", 1)[1].strip().strip('"').strip("'").split(",")
                    if value.strip()
                ]
    return API_KEYS.copy()
