from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from app import api_client, database, worker as worker_module
from app.api_client import AdaptiveLimiter, ClaroAPIClient
from app.worker import JobManager


class TemporaryDatabaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.original_db = database.DB_PATH
        self.original_exports = database.EXPORT_DIR
        database.DB_PATH = Path(self.temp.name) / "test.db"
        database.EXPORT_DIR = Path(self.temp.name) / "exports"
        database.init_db()

    def tearDown(self) -> None:
        database.DB_PATH = self.original_db
        database.EXPORT_DIR = self.original_exports
        self.temp.cleanup()

    def test_import_deduplicates_and_preserves_alphanumeric_number(self) -> None:
        result = database.insert_records(
            [
                ("01310-100", "123A"),
                ("01310100", "123A"),
                ("01310-100", "S/N"),
            ]
        )
        self.assertEqual(result, {"inserted": 2, "ignored": 1})
        page = database.list_records(None, None, 1, 200)
        self.assertEqual(page["total"], 2)
        self.assertEqual({row["numero"] for row in page["items"]}, {"123A", "S/N"})

    def test_pagination_is_limited_to_200(self) -> None:
        database.insert_records((("01001000", str(index)) for index in range(250)))
        page = database.list_records(None, None, 1, 9999)
        self.assertEqual(len(page["items"]), 200)
        self.assertEqual(page["page_size"], 200)

    def test_requests_per_second_setting_is_user_controlled(self) -> None:
        saved = database.save_settings(5, 7.5, 1200, True)
        loaded = database.get_settings()
        self.assertEqual(saved["requests_per_second"], 7.5)
        self.assertEqual(loaded["requests_per_second"], 7.5)
        self.assertEqual(loaded["per_robot_delay_ms"], 1200)
        job = database.create_job(5, 7.5, 1200, True, False)
        self.assertEqual(job["requests_per_second"], 7.5)
        self.assertEqual(job["delay_ms"], 1200)

    def test_structured_json_is_preserved_and_indexed(self) -> None:
        database.insert_records([("01001000", "10")])
        record = database.list_records(None, None, 1, 1)["items"][0]
        payload = {
            "data": {
                "cep": "01001000",
                "number": "10",
                "cidade": "São Paulo",
                "uf": "SP",
                "technologies": [
                    {
                        "name": "HFC",
                        "internet": True,
                        "tv": True,
                        "phone": False,
                        "building": {"status": "CABEADO"},
                    }
                ],
            }
        }
        database.write_manual_result(
            {
                "id": record["id"],
                "cep": "01001000",
                "numero": "10",
                "status": "CONSULTADO",
                "payload": payload,
                "attempts": 1,
                "http_status": 200,
                "error": None,
            }
        )
        conn = database.connect(readonly=True)
        try:
            raw = conn.execute(
                "SELECT resultado_json FROM registros WHERE id=?", (record["id"],)
            ).fetchone()[0]
            tech = conn.execute(
                "SELECT nome, status FROM tecnologias WHERE registro_id=?",
                (record["id"],),
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(json.loads(raw), payload)
        self.assertEqual(tuple(tech), ("HFC", "CABEADO"))

    def test_export_runs_in_chunks_and_produces_csv(self) -> None:
        database.insert_records([("01001000", "10")])
        record = database.list_records(None, None, 1, 1)["items"][0]
        database.write_manual_result(
            {
                "id": record["id"],
                "cep": "01001000",
                "numero": "10",
                "status": "CONSULTADO",
                "payload": {
                    "data": {
                        "cidade": "São Paulo",
                        "uf": "SP",
                        "technologies": [{"name": "FTTH", "internet": True}],
                    }
                },
                "attempts": 1,
                "http_status": 200,
                "error": None,
            }
        )
        export_id = database.create_export()
        database.generate_export(export_id, {"uf": "SP"})
        result = database.export_status(export_id)
        self.assertEqual(result["status"], "CONCLUIDO")
        self.assertEqual(result["linhas"], 1)
        self.assertTrue(Path(result["arquivo"]).is_file())

    def test_restart_releases_claimed_records_and_marks_job_failed(self) -> None:
        database.insert_records([("01001000", "10")])
        job = database.create_job(1, 2.5, 0, True, False)
        database.update_job(job["id"], status="EXECUTANDO")
        claimed = database.claim_batch(job["id"], False, 1)
        self.assertEqual(len(claimed), 1)
        database.init_db()
        restarted_job = database.get_job(job["id"])
        record = database.list_records(None, None, 1, 1)["items"][0]
        self.assertEqual(restarted_job["status"], "FALHOU")
        self.assertEqual(record["status"], "PENDENTE")


class RuntimeBehaviorTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.original_db = database.DB_PATH
        self.original_keys = api_client.API_KEYS
        database.DB_PATH = Path(self.temp.name) / "test.db"
        api_client.API_KEYS = ["test-key-never-sent"]
        database.init_db()

    async def asyncTearDown(self) -> None:
        database.DB_PATH = self.original_db
        api_client.API_KEYS = self.original_keys
        self.temp.cleanup()

    async def test_five_threads_create_exactly_five_robot_cards(self) -> None:
        manager = JobManager()
        robots = manager._build_robots(5)
        self.assertEqual(len(robots), 5)
        self.assertEqual([robot.name for robot in robots], [f"Robô {i}" for i in range(1, 6)])

    async def test_adaptive_limiter_reduces_and_recovers_without_exceeding_ceiling(self) -> None:
        limiter = AdaptiveLimiter(5, True)
        self.assertEqual(limiter.ceiling_rps, 5)
        limiter.on_429(0)
        self.assertEqual(limiter.scale, 0.5)
        for _ in range(100):
            limiter.on_success()
        self.assertAlmostEqual(limiter.scale, 0.6)
        for _ in range(1000):
            limiter.on_success()
        self.assertLessEqual(limiter.scale, 1.0)

    async def test_isolated_success_response(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.params["api_key"], "test-key-never-sent")
            return httpx.Response(
                200,
                json={"data": {"technologies": [{"name": "FTTH"}]}},
            )

        client = ClaroAPIClient(1, 10, True, transport=httpx.MockTransport(handler))
        try:
            result = await client.query("01001000", "10")
        finally:
            await client.close()
        self.assertEqual(result["status"], "CONSULTADO")
        self.assertEqual(result["summary"], "FTTH")

    async def test_isolated_429_and_timeout_are_queued_for_retry(self) -> None:
        states: list[dict] = []

        def too_many(_: httpx.Request) -> httpx.Response:
            return httpx.Response(429, headers={"Retry-After": "1"})

        client_429 = ClaroAPIClient(1, 10, True, transport=httpx.MockTransport(too_many))
        client_429.limiter.on_429 = lambda _: None
        client_429.key_pool.cooldown = lambda *_: None
        try:
            result_429 = await client_429.query(
                "01001000", "10", lambda state: _collect(states, state)
            )
        finally:
            await client_429.close()
        self.assertEqual(result_429["status"], "AGUARDANDO_RETRY")
        self.assertEqual(result_429["http_status"], 429)
        self.assertTrue(any(state.get("state") == "RESFRIANDO" for state in states))

        def timeout(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("isolated timeout", request=request)

        client_timeout = ClaroAPIClient(1, 10, True, transport=httpx.MockTransport(timeout))
        try:
            with patch.object(ClaroAPIClient, "BACKOFFS", [0, 0, 0, 0, 0]), patch(
                "app.api_client.random.uniform", return_value=0
            ):
                result_timeout = await client_timeout.query("01001000", "10")
        finally:
            await client_timeout.close()
        self.assertEqual(result_timeout["status"], "AGUARDANDO_RETRY")
        self.assertIn("ReadTimeout", result_timeout["error"])

    async def test_pause_resume_and_cancel_update_persistent_job(self) -> None:
        database.insert_records([("01001000", "10")])
        job = database.create_job(1, 2.5, 0, True, False)
        database.update_job(job["id"], status="EXECUTANDO")
        manager = JobManager()
        manager.job_id = job["id"]
        manager.robots = manager._build_robots(1)
        manager.client = ClaroAPIClient(
            1,
            2.5,
            True,
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"data": {}})
            ),
        )
        manager.task = asyncio.create_task(asyncio.sleep(60))
        try:
            paused = await manager.pause(job["id"])
            self.assertEqual(paused["status"], "PAUSADO")
            resumed = await manager.resume(job["id"])
            self.assertEqual(resumed["status"], "EXECUTANDO")
            cancelled = await manager.cancel(job["id"])
            self.assertEqual(cancelled["cancel_requested"], 1)
        finally:
            manager.task.cancel()
            await asyncio.gather(manager.task, return_exceptions=True)
            await manager.client.close()

    async def test_user_selected_rps_is_the_limiter_ceiling(self) -> None:
        client = ClaroAPIClient(
            5,
            7.5,
            True,
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"data": {}})
            ),
        )
        try:
            self.assertEqual(client.limiter.ceiling_rps, 7.5)
            client.limiter.on_429(0)
            self.assertEqual(client.limiter.effective_rps, 3.75)
        finally:
            await client.close()

    async def test_cancel_interrupts_in_flight_work_and_releases_records(self) -> None:
        database.insert_records([("01001000", "10")])
        request_started = asyncio.Event()

        class BlockingClient:
            def __init__(self, *_args, **_kwargs):
                self.key_pool = type("Pool", (), {"has_enabled": lambda _self: True})()

            async def query(self, *_args, **_kwargs):
                request_started.set()
                await asyncio.sleep(60)

            async def close(self):
                return None

            def snapshot(self):
                return {"limiter": {}, "keys": []}

        manager = JobManager()
        with patch.object(worker_module, "ClaroAPIClient", BlockingClient):
            job = await manager.start(1, 2, 0, True, False)
            await asyncio.wait_for(request_started.wait(), timeout=2)
            started = asyncio.get_running_loop().time()
            cancelled = await manager.cancel(job["id"])
            elapsed = asyncio.get_running_loop().time() - started

        self.assertLess(elapsed, 1.0)
        self.assertEqual(cancelled["status"], "CANCELADO")
        record = database.list_records(None, None, 1, 1)["items"][0]
        self.assertEqual(record["status"], "PENDENTE")


async def _collect(target: list[dict], value: dict) -> None:
    target.append(value)


class StaticApplicationTest(unittest.TestCase):
    def test_application_has_no_fake_mode_controls(self) -> None:
        root = Path(__file__).resolve().parents[1]
        app_source = "\n".join(
            path.read_text(encoding="utf-8")
            for folder in (root / "app", root / "static")
            for path in folder.glob("*")
            if path.suffix in {".py", ".js", ".html"}
        ).lower()
        self.assertNotIn("mock", app_source)
        self.assertNotIn("dados fictícios", app_source)

    def test_sse_is_throttled_to_two_updates_per_second(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn("await asyncio.sleep(0.5)", source)

    def test_http_application_starts_and_exposes_core_routes(self) -> None:
        from app.main import app

        with TestClient(app) as client:
            health = client.get("/api/health")
            dashboard = client.get("/api/dashboard/summary")
            records = client.get("/api/records", params={"page": 1, "page_size": 200})
            index = client.get("/")
        self.assertEqual(health.status_code, 200)
        self.assertTrue(health.json()["ok"])
        self.assertEqual(dashboard.status_code, 200)
        self.assertEqual(records.status_code, 200)
        self.assertLessEqual(len(records.json()["items"]), 200)
        self.assertIn('id="robotGrid"', index.text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
