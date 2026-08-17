from __future__ import annotations

import threading
import time
import webbrowser

import uvicorn

from app.config import HOST, PORT


def open_browser() -> None:
    time.sleep(1.5)
    webbrowser.open(f"http://{HOST}:{PORT}")


if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    uvicorn.run("app.main:app", host=HOST, port=PORT, reload=False, access_log=False)
