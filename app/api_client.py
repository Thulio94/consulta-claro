from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Awaitable, Callable

import certifi
import httpx

from .config import API_KEYS, API_URL, read_api_keys


StateCallback = Callable[[dict[str, Any]], Awaitable[None]]


class NoValidAPIKeysError(RuntimeError):
    pass


def iso_after(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def retry_after_seconds(value: str | None) -> float:
    if not value:
        return 60.0
    try:
        return max(1.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return max(1.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 60.0


@dataclass
class KeyState:
    value: str
    disabled: bool = False
    cooldown_until: float = 0.0

    @property
    def label(self) -> str:
        return f"••••{self.value[-4:]}"


class KeyPool:
    def __init__(self, keys: list[str]):
        self.keys = [KeyState(key) for key in keys]
        self._index = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> KeyState:
        while True:
            async with self._lock:
                now = time.monotonic()
                available = [key for key in self.keys if not key.disabled and key.cooldown_until <= now]
                if available:
                    for _ in range(len(self.keys)):
                        key = self.keys[self._index % len(self.keys)]
                        self._index += 1
                        if key in available:
                            return key
                enabled = [key for key in self.keys if not key.disabled]
                if not enabled:
                    raise NoValidAPIKeysError("Nenhuma chave de API válida está disponível.")
                wait_for = max(0.5, min(key.cooldown_until for key in enabled) - now)
            await asyncio.sleep(min(wait_for, 5.0))

    def cooldown(self, key: KeyState, seconds: float) -> None:
        key.cooldown_until = max(key.cooldown_until, time.monotonic() + seconds)

    def disable(self, key: KeyState) -> None:
        key.disabled = True

    def has_enabled(self) -> bool:
        return any(not key.disabled for key in self.keys)

    def snapshot(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        return [
            {
                "label": key.label,
                "disabled": key.disabled,
                "cooldown_seconds": max(0, round(key.cooldown_until - now)),
            }
            for key in self.keys
        ]


class AdaptiveLimiter:
    def __init__(self, requests_per_second: float, adaptive: bool = True):
        self.ceiling_rps = max(0.1, float(requests_per_second))
        self.minimum_scale = min(1.0, 0.1 / self.ceiling_rps)
        self.adaptive = adaptive
        self.scale = 1.0
        self.success_streak = 0
        self.cooldown_until = 0.0
        self._next_allowed = 0.0
        self._lock = asyncio.Lock()
        self._priority_waiters = 0

    @property
    def effective_rps(self) -> float:
        return self.ceiling_rps * self.scale

    async def acquire(self, priority: bool = False) -> None:
        if priority:
            self._priority_waiters += 1
        try:
            while True:
                deferred = False
                async with self._lock:
                    if not priority and self._priority_waiters:
                        deferred = True
                    else:
                        now = time.monotonic()
                        wait = max(
                            0.0,
                            self.cooldown_until - now,
                            self._next_allowed - now,
                        )
                        if wait > 0:
                            await asyncio.sleep(wait)
                        now = time.monotonic()
                        self._next_allowed = (
                            now + 1.0 / max(0.01, self.effective_rps)
                        )
                        return
                if deferred:
                    await asyncio.sleep(0.01)
        finally:
            if priority:
                self._priority_waiters -= 1

    def on_429(self, seconds: float) -> None:
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + seconds)
        self.success_streak = 0
        if self.adaptive:
            self.scale = max(self.minimum_scale, self.scale / 2.0)

    def on_success(self) -> None:
        self.success_streak += 1
        if self.adaptive and self.success_streak >= 100:
            self.scale = min(1.0, self.scale + 0.1)
            self.success_streak = 0

    def snapshot(self) -> dict[str, Any]:
        return {
            "ceiling_rps": round(self.ceiling_rps, 2),
            "effective_rps": round(self.effective_rps, 2),
            "rate_scale": round(self.scale, 3),
            "cooldown_seconds": max(0, round(self.cooldown_until - time.monotonic())),
            "adaptive_reduced": self.scale < 0.999,
        }


class ClaroAPIClient:
    BACKOFFS = [2, 4, 8, 16, 30]

    def __init__(
        self,
        threads: int,
        requests_per_second: float,
        adaptive: bool,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        if not API_KEYS:
            raise RuntimeError("Nenhuma chave de API foi configurada no arquivo .env.")
        self.key_pool = KeyPool(API_KEYS)
        self.limiter = AdaptiveLimiter(requests_per_second, adaptive)
        limits = httpx.Limits(max_connections=max(2, threads), max_keepalive_connections=max(2, threads))
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
            limits=limits,
            verify=certifi.where(),
            transport=transport,
            headers={"User-Agent": "ConsultaClaroV2/1.0"},
        )

    async def close(self) -> None:
        await self.client.aclose()

    def reload_keys(self) -> int:
        keys = read_api_keys()
        if not keys:
            raise NoValidAPIKeysError("Nenhuma chave foi encontrada no arquivo .env.")
        self.key_pool = KeyPool(keys)
        return len(keys)

    async def query(
        self,
        cep: str,
        numero: str,
        state_callback: StateCallback | None = None,
        priority: bool = False,
    ) -> dict[str, Any]:
        last_error = ""
        last_http: int | None = None
        for attempt in range(1, 6):
            key = await self.key_pool.acquire()
            limiter_state = self.limiter.snapshot()
            if state_callback:
                cooling = limiter_state["cooldown_seconds"] > 0
                await state_callback(
                    {
                        "state": "RESFRIANDO" if cooling else "AGUARDANDO",
                        "attempt": attempt,
                        "key": key.label,
                        "summary": (
                            f"Proteção ativa por {limiter_state['cooldown_seconds']}s"
                            if cooling
                            else "Aguardando o limitador"
                        ),
                    }
                )
            await self.limiter.acquire(priority=priority)
            if state_callback:
                await state_callback(
                    {
                        "state": "CONSULTANDO",
                        "attempt": attempt,
                        "key": key.label,
                        "effective_rps": self.limiter.effective_rps,
                    }
                )
            started = time.perf_counter()
            try:
                response = await self.client.get(
                    API_URL,
                    params={
                        "action": "viability",
                        "api_key": key.value,
                        "cep": cep,
                        "numero": numero,
                    },
                )
                latency_ms = int((time.perf_counter() - started) * 1000)
                last_http = response.status_code
                if response.status_code == 200:
                    try:
                        payload = response.json()
                    except ValueError:
                        return {
                            "status": "ERRO_PERMANENTE",
                            "payload": {"erro": "Resposta HTTP 200 não contém JSON válido"},
                            "http_status": 200,
                            "error": "JSON inválido",
                            "attempts": attempt,
                            "latency_ms": latency_ms,
                            "summary": "JSON inválido",
                        }
                    if isinstance(payload, dict) and "erro" not in payload:
                        self.limiter.on_success()
                        technologies = payload.get("data", {}).get("technologies", []) if isinstance(payload.get("data"), dict) else []
                        names = [
                            str(item.get("name"))
                            for item in technologies
                            if isinstance(item, dict) and item.get("name")
                        ]
                        return {
                            "status": "CONSULTADO",
                            "payload": payload,
                            "http_status": 200,
                            "error": None,
                            "attempts": attempt,
                            "latency_ms": latency_ms,
                            "summary": ", ".join(names[:2]) if names else "Sem tecnologia",
                        }
                    return {
                        "status": "ERRO_PERMANENTE",
                        "payload": payload if isinstance(payload, dict) else {"resposta": payload},
                        "http_status": 200,
                        "error": str(payload.get("erro", "Erro retornado pela API")) if isinstance(payload, dict) else "Resposta inválida",
                        "attempts": attempt,
                        "latency_ms": latency_ms,
                        "summary": "Erro da API",
                    }
                if response.status_code == 429:
                    wait_seconds = retry_after_seconds(response.headers.get("Retry-After"))
                    self.limiter.on_429(wait_seconds)
                    self.key_pool.cooldown(key, wait_seconds)
                    last_error = f"HTTP 429; proteção ativa por {int(wait_seconds)}s"
                    if state_callback:
                        await state_callback(
                            {
                                "state": "RESFRIANDO",
                                "http_status": 429,
                                "summary": last_error,
                                "cooldown_seconds": int(wait_seconds),
                            }
                        )
                    continue
                if response.status_code in {401, 403}:
                    self.key_pool.disable(key)
                    last_error = f"HTTP {response.status_code}; chave {key.label} desativada"
                    continue
                if response.status_code in {500, 502, 503, 504}:
                    last_error = f"HTTP {response.status_code}"
                    wait = self.BACKOFFS[attempt - 1] + random.uniform(0, 0.75)
                    await asyncio.sleep(wait)
                    continue
                return {
                    "status": "ERRO_PERMANENTE",
                    "payload": {"erro": f"HTTP {response.status_code}", "texto": response.text[:300]},
                    "http_status": response.status_code,
                    "error": f"HTTP {response.status_code}",
                    "attempts": attempt,
                    "latency_ms": latency_ms,
                    "summary": f"HTTP {response.status_code}",
                }
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = f"{type(exc).__name__}: {str(exc)[:160]}"
                if attempt < 5:
                    wait = self.BACKOFFS[attempt - 1] + random.uniform(0, 0.75)
                    await asyncio.sleep(wait)
                    continue
        return {
            "status": "AGUARDANDO_RETRY",
            "payload": {"erro": last_error},
            "http_status": last_http,
            "error": last_error or "Falha transitória após cinco tentativas",
            "attempts": 5,
            "next_retry": iso_after(900),
            "latency_ms": 0,
            "summary": (
                "HTTP 429 · aguardando retry"
                if last_http == 429
                else "Timeout · aguardando retry"
                if "Timeout" in last_error
                else "Erro transitório · aguardando retry"
            ),
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "limiter": self.limiter.snapshot(),
            "keys": self.key_pool.snapshot(),
        }
