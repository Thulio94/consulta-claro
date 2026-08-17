from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from . import database
from .api_client import ClaroAPIClient, NoValidAPIKeysError


@dataclass
class RobotState:
    id: int
    name: str
    state: str = "DISPONÍVEL"
    cep: str = ""
    numero: str = ""
    started_at: float | None = None
    latency_ms: int = 0
    http_status: int | None = None
    summary: str = "Aguardando"
    technology: str = ""
    successes: int = 0
    errors: int = 0
    http_429: int = 0
    attempt: int = 0
    key: str = ""

    def public(self) -> dict[str, Any]:
        data = asdict(self)
        data["elapsed_seconds"] = (
            round(time.monotonic() - self.started_at, 1)
            if self.started_at and self.state in {"CONSULTANDO", "AGUARDANDO", "RESFRIANDO"}
            else round(self.latency_ms / 1000.0, 1)
        )
        data.pop("started_at", None)
        return data


class JobManager:
    def __init__(self):
        self.task: asyncio.Task | None = None
        self.job_id: str | None = None
        self.robots: list[RobotState] = []
        self.client: ClaroAPIClient | None = None
        self.pause_event = asyncio.Event()
        self.pause_event.set()
        self.cancel_requested = False
        self.started_monotonic = 0.0

    def _build_robots(self, count: int) -> list[RobotState]:
        return [RobotState(id=index, name=f"Robô {index}") for index in range(1, count + 1)]

    async def start(
        self,
        threads: int,
        requests_per_second: float,
        per_robot_delay_ms: int,
        adaptive: bool,
        include_retry: bool,
    ) -> dict[str, Any]:
        if self.task and not self.task.done():
            raise RuntimeError("Já existe uma consulta massiva em execução.")
        job = await asyncio.to_thread(
            database.create_job,
            threads,
            requests_per_second,
            per_robot_delay_ms,
            adaptive,
            include_retry,
        )
        self.job_id = job["id"]
        self.robots = self._build_robots(threads)
        self.client = ClaroAPIClient(threads, requests_per_second, adaptive)
        self.pause_event.set()
        self.cancel_requested = False
        self.started_monotonic = time.monotonic()
        self.task = asyncio.create_task(self._run(job))
        return job

    async def _run(self, job: dict[str, Any]) -> None:
        assert self.client is not None
        job_id = job["id"]
        threads = int(job["threads"])
        per_robot_delay_ms = int(job["delay_ms"])
        include_retry = bool(job["include_retry"])
        work_queue: asyncio.Queue = asyncio.Queue(maxsize=max(threads * 4, threads))
        write_queue: asyncio.Queue = asyncio.Queue(maxsize=max(threads * 8, 50))
        database.update_job(
            job_id,
            status="EXECUTANDO",
            iniciado_em=database.utc_now(),
            mensagem="Consulta real em andamento",
        )

        async def producer() -> None:
            while not self.cancel_requested:
                await self.pause_event.wait()
                capacity = work_queue.maxsize - work_queue.qsize()
                if capacity <= 0:
                    await asyncio.sleep(0.1)
                    continue
                rows = await asyncio.to_thread(
                    database.claim_batch,
                    job_id,
                    include_retry,
                    min(capacity, max(threads * 2, 10)),
                )
                if not rows:
                    break
                for row in rows:
                    await work_queue.put(row)
            for _ in range(threads):
                await work_queue.put(None)

        async def update_robot(robot: RobotState, data: dict[str, Any]) -> None:
            robot.state = data.get("state", robot.state)
            robot.http_status = data.get("http_status", robot.http_status)
            robot.summary = data.get("summary", robot.summary)
            robot.attempt = data.get("attempt", robot.attempt)
            robot.key = data.get("key", robot.key)

        async def worker(robot: RobotState) -> None:
            while True:
                item = await work_queue.get()
                if item is None:
                    work_queue.task_done()
                    break
                if self.cancel_requested:
                    work_queue.task_done()
                    continue
                await self.pause_event.wait()
                robot.state = "CONSULTANDO"
                robot.cep = item["cep"]
                robot.numero = item["numero"]
                robot.started_at = time.monotonic()
                robot.http_status = None
                robot.summary = "Consultando API real"
                result = None
                while result is None and not self.cancel_requested:
                    try:
                        result = await self.client.query(
                            item["cep"],
                            item["numero"],
                            lambda data, current=robot: update_robot(current, data),
                        )
                    except NoValidAPIKeysError:
                        robot.state = "PAUSADO"
                        robot.summary = "Sem chave válida; corrija o .env e retome"
                        self.pause_event.clear()
                        await asyncio.to_thread(
                            database.update_job,
                            job_id,
                            status="PAUSADO",
                            pause_requested=1,
                            mensagem="Todas as chaves retornaram HTTP 401/403",
                        )
                        await self.pause_event.wait()
                if result is None:
                    work_queue.task_done()
                    continue
                result.update(
                    {
                        "id": item["id"],
                        "cep": item["cep"],
                        "numero": item["numero"],
                        "worker_id": robot.id,
                    }
                )
                robot.latency_ms = int(result.get("latency_ms", 0))
                robot.http_status = result.get("http_status")
                robot.summary = result.get("summary", "")
                robot.started_at = None
                if result["status"] == "CONSULTADO":
                    robot.state = "SUCESSO"
                    robot.successes += 1
                    robot.technology = result.get("summary", "")
                elif result.get("http_status") == 429:
                    robot.state = "RESFRIANDO"
                    robot.http_429 += 1
                    robot.errors += 1
                else:
                    robot.state = "ERRO"
                    robot.errors += 1
                await write_queue.put(result)
                work_queue.task_done()
                if per_robot_delay_ms:
                    await asyncio.sleep(per_robot_delay_ms / 1000.0)
            await write_queue.put(None)

        async def writer() -> None:
            finished_workers = 0
            buffer: list[dict[str, Any]] = []
            last_flush = time.monotonic()
            while finished_workers < threads:
                timeout = max(0.05, 1.0 - (time.monotonic() - last_flush))
                try:
                    item = await asyncio.wait_for(write_queue.get(), timeout=timeout)
                    if item is None:
                        finished_workers += 1
                    else:
                        buffer.append(item)
                    write_queue.task_done()
                except asyncio.TimeoutError:
                    pass
                if buffer and (len(buffer) >= 50 or time.monotonic() - last_flush >= 1.0 or finished_workers == threads):
                    await asyncio.to_thread(database.write_results, job_id, buffer.copy())
                    buffer.clear()
                    last_flush = time.monotonic()

        tasks = [
            asyncio.create_task(producer()),
            asyncio.create_task(writer()),
            *[asyncio.create_task(worker(robot)) for robot in self.robots],
        ]
        try:
            await asyncio.gather(*tasks)
            await asyncio.to_thread(database.release_claims, job_id)
            final_status = "CANCELADO" if self.cancel_requested else "CONCLUIDO"
            database.update_job(
                job_id,
                status=final_status,
                finalizado_em=database.utc_now(),
                mensagem="Cancelado pelo usuário" if self.cancel_requested else "Fila concluída",
            )
        except asyncio.CancelledError:
            self.cancel_requested = True
            await asyncio.to_thread(database.release_claims, job_id)
            # A chamada de leitura do banco iniciada pelo produtor pode terminar
            # poucos milissegundos após o cancelamento. Uma segunda liberação
            # garante que nenhum item fique marcado como EM_PROCESSAMENTO.
            await asyncio.sleep(0.05)
            await asyncio.to_thread(database.release_claims, job_id)
            database.update_job(
                job_id,
                status="CANCELADO",
                cancel_requested=1,
                finalizado_em=database.utc_now(),
                mensagem="Cancelado imediatamente pelo usuário",
            )
            for robot in self.robots:
                robot.state = "PAUSADO"
                robot.summary = "Cancelado"
                robot.started_at = None
            raise
        except Exception as exc:
            await asyncio.to_thread(database.release_claims, job_id)
            database.update_job(
                job_id,
                status="FALHOU",
                finalizado_em=database.utc_now(),
                mensagem=str(exc)[:500],
            )
            for robot in self.robots:
                robot.state = "ERRO"
                robot.summary = "Trabalho interrompido"
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.client.close()
            for robot in self.robots:
                if robot.state not in {"ERRO", "RESFRIANDO"}:
                    robot.state = "DISPONÍVEL"

    async def pause(self, job_id: str) -> dict[str, Any]:
        if job_id != self.job_id or not self.task or self.task.done():
            raise RuntimeError("Trabalho ativo não encontrado.")
        self.pause_event.clear()
        for robot in self.robots:
            if robot.state != "CONSULTANDO":
                robot.state = "PAUSADO"
        database.update_job(job_id, status="PAUSADO", pause_requested=1, mensagem="Pausado pelo usuário")
        return database.get_job(job_id) or {}

    async def resume(self, job_id: str) -> dict[str, Any]:
        if job_id != self.job_id or not self.task or self.task.done():
            raise RuntimeError("Trabalho pausado não encontrado.")
        assert self.client is not None
        if not self.client.key_pool.has_enabled():
            self.client.reload_keys()
        self.pause_event.set()
        for robot in self.robots:
            if robot.state == "PAUSADO":
                robot.state = "DISPONÍVEL"
        database.update_job(job_id, status="EXECUTANDO", pause_requested=0, mensagem="Consulta retomada")
        return database.get_job(job_id) or {}

    async def cancel(self, job_id: str) -> dict[str, Any]:
        if job_id != self.job_id or not self.task or self.task.done():
            raise RuntimeError("Trabalho ativo não encontrado.")
        self.cancel_requested = True
        self.pause_event.set()
        for robot in self.robots:
            robot.state = "PAUSADO"
            robot.summary = "Cancelando imediatamente"
            robot.started_at = None
        database.update_job(
            job_id,
            cancel_requested=1,
            mensagem="Cancelamento imediato solicitado",
        )
        # Cancelar a tarefa principal também interrompe espera de limitador,
        # retentativas e a requisição HTTP ainda pendente no cliente assíncrono.
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass
        return database.get_job(job_id) or {}

    async def manual_query(self, cep: str, numero: str) -> dict[str, Any]:
        record_id = await asyncio.to_thread(database.get_or_create_record, cep, numero)
        settings = database.get_settings()
        owned_client = self.client is None or self.task is None or self.task.done()
        client = self.client if not owned_client else ClaroAPIClient(
            settings["threads"],
            settings["requests_per_second"],
            settings["adaptive"],
        )
        assert client is not None
        try:
            result = await client.query(
                database.normalize_cep(cep),
                database.normalize_numero(numero),
                priority=True,
            )
            result.update(
                {
                    "id": record_id,
                    "cep": database.normalize_cep(cep),
                    "numero": database.normalize_numero(numero),
                    "worker_id": 0,
                }
            )
            await asyncio.to_thread(database.write_manual_result, result)
            return {
                "status": result["status"],
                "http_status": result.get("http_status"),
                "summary": result.get("summary"),
                "payload": result.get("payload"),
            }
        finally:
            if owned_client:
                await client.close()

    def snapshot(self) -> dict[str, Any]:
        job = database.get_job(self.job_id) if self.job_id else database.latest_job()
        elapsed = max(0.001, time.monotonic() - self.started_monotonic) if self.started_monotonic else 0
        processed = int(job.get("processados", 0)) if job else 0
        total = int(job.get("total", 0)) if job else 0
        rps = processed / elapsed if elapsed else 0
        remaining = max(0, total - processed)
        eta = int(remaining / rps) if rps > 0 else None
        average_latency_ms = (
            round(int(job.get("latency_total_ms", 0)) / processed)
            if job and processed
            else 0
        )
        return {
            "job": job,
            "robots": [robot.public() for robot in self.robots],
            "runtime": {
                "rps": round(rps, 2),
                "eta_seconds": eta,
                "elapsed_seconds": int(elapsed),
                "remaining": remaining,
                "average_latency_ms": average_latency_ms,
            },
            "api": self.client.snapshot() if self.client else None,
        }


job_manager = JobManager()
