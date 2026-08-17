from __future__ import annotations

from pydantic import BaseModel, Field


class SettingsPayload(BaseModel):
    threads: int = Field(ge=1, le=10)
    requests_per_second: float = Field(ge=0.1, le=1000)
    per_robot_delay_ms: int = Field(ge=0, le=60_000)
    adaptive: bool = True


class JobPayload(SettingsPayload):
    include_retry: bool = False
    confirmation: str


class ManualQueryPayload(BaseModel):
    cep: str = Field(min_length=1, max_length=20)
    numero: str = Field(min_length=1, max_length=100)
    confirmation: str


class ExportPayload(BaseModel):
    uf: str | None = None
    cidade: str | None = None
    tecnologia: str | None = None
    status_resultado: str | None = None
