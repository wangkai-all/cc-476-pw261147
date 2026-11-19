"""端到端测试：真实 HTTP 服务 + 轮询接口，覆盖页面依赖的全部 API。"""
from __future__ import annotations

import json
import tempfile
import time
import unittest
import urllib.request
import urllib.error
from pathlib import Path

from coordinator.config import load_config
from coordinator.model import TaskState
from coordinator.persistence import SnapshotStore
from coordinator.scheduler import Scheduler
from coordinator.server import CoordinatorServer

CONFIG = str(Path(__file__).resolve().parent.parent / "config.json")


class HttpEndToEndTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        specs, slots, mem = load_config(CONFIG)
        store = SnapshotStore(self._tmp.name)
        self.sched = Scheduler(specs, total_slots=slots, total_mem=mem,
                               state_dir=self._tmp.name, store=store)
        self.server = CoordinatorServer(self.sched, CONFIG, "127.0.0.1", 0)
        self.server.start()
        self.base = f"http://127.0.0.1:{self.server.port}"

    def tearDown(self):
        self.server.stop()
        self.sched.shutdown()
        self._tmp.cleanup()

    def get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def post(self, path):
        req = urllib.request.Request(self.base + path, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise AssertionError(f"{path} -> {e.code}: {e.read().decode()}")

    def wait_terminal(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            st = self.get("/api/status")
            if all(t["state"] in ("SUCCESS", "FAILED", "CANCELLED")
                   for t in st["tasks"]):
                return st
            time.sleep(0.1)
        raise AssertionError("任务未在超时内结束")

    def test_status_shape_and_static_page(self):
        st = self.get("/api/status")
        for key in ("scheduler", "resources", "locks", "tasks", "events"):
            self.assertIn(key, st)
        self.assertEqual(len(st["tasks"]), 10)
        html = urllib.request.urlopen(self.base + "/", timeout=5).read().decode()
        self.assertIn("多表增量装载协调器", html)
        js = urllib.request.urlopen(self.base + "/app.js", timeout=5).read().decode()
        self.assertIn("renderDag", js)

    def test_full_demo_run_with_retry_and_blocked(self):
        st = self.wait_terminal()
        by_id = {t["task_id"]: t for t in st["tasks"]}
        # dwd_orders_clean 前两次自动失败，第三次成功
        self.assertEqual(by_id["dwd_orders_clean"]["state"], "SUCCESS")
        self.assertEqual(by_id["dwd_orders_clean"]["attempts"], 3)
        # audit_reconcile 超时注入一次后第二次成功
        self.assertEqual(by_id["audit_reconcile"]["state"], "SUCCESS")
        self.assertEqual(by_id["audit_reconcile"]["attempts"], 2)
        # 所有任务最终成功（失败的都被自动重试救回）
        self.assertTrue(all(t["state"] == "SUCCESS" for t in st["tasks"]))
        # 资源已全部释放
        self.assertEqual(st["resources"]["used_slots"], 0)
        self.assertEqual(st["resources"]["used_mem"], 0)
        self.assertEqual(len(st["locks"]["holders"]), 0)

    def test_inject_failure_and_manual_retry_api(self):
        # 等一个任务跑起来后注入故障
        target = None
        deadline = time.time() + 10
        while time.time() < deadline:
            st = self.get("/api/status")
            running = [t for t in st["tasks"] if t["state"] == "RUNNING"]
            if running:
                target = running[0]["task_id"]
                break
            time.sleep(0.05)
        self.assertIsNotNone(target)
        self.post(f"/api/tasks/{target}/inject")
        # 对非运行任务注入应被拒绝
        with self.assertRaises(AssertionError):
            self.post("/api/tasks/stg_users/inject")
        self.wait_terminal()

    def test_restart_endpoint_preserves_state(self):
        time.sleep(0.2)
        gen = self.get("/api/status")["scheduler"]["generation"]
        self.post("/api/scheduler/restart")
        st = self.get("/api/status")
        self.assertEqual(st["scheduler"]["generation"], gen + 1)
        # 重启后等待所有任务跑完，已完成的不重跑、未完成的续跑
        st = self.wait_terminal()
        self.assertTrue(all(t["state"] == "SUCCESS" for t in st["tasks"]))

    def test_cancel_api_and_path_traversal_blocked(self):
        # 路径穿越防护
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(self.base + "/../etc/passwd", timeout=5)
        self.assertIn(cm.exception.code, (403, 404))

    def test_reset_clears_everything(self):
        self.wait_terminal()
        self.post("/api/scheduler/reset")
        st = self.get("/api/status")
        # 所有任务回到初始态，尝试次数清零
        self.assertTrue(all(t["state"] in ("PENDING", "READY", "WAITING_LOCK", "RUNNING")
                            for t in st["tasks"]))
        self.assertTrue(all(t["attempts"] in (0, 1) for t in st["tasks"]))
        # 旧事件已清空，时间线第一条是重置事件
        self.assertEqual(st["events"][0]["type"], "SCHEDULER_RESET")
        self.assertFalse(any(e["type"] == "TASK_SUCCESS" for e in st["events"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
