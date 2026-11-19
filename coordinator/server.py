"""HTTP API 与静态页面服务（仅依赖标准库）。

API:
  GET  /api/status                 全量状态（DAG/资源/锁/任务/事件）
  GET  /api/events                 SSE 事件流
  POST /api/scheduler/start
  POST /api/scheduler/pause
  POST /api/scheduler/restart      模拟崩溃后按快照重启
  POST /api/scheduler/reset        清空状态重新装载
  POST /api/tasks/{id}/cancel
  POST /api/tasks/{id}/retry       人工补齐（从检查点续跑）
  POST /api/tasks/{id}/inject      对运行中的任务注入故障
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional

from .config import load_config
from .model import TaskSpec
from .scheduler import Scheduler

STATIC_DIR = Path(__file__).resolve().parent / "static"


class CoordinatorServer:
    def __init__(self, scheduler: Scheduler, config_path: str, host: str, port: int):
        self.scheduler = scheduler
        self.config_path = config_path
        self.host = host
        self.port = port
        self._sse_clients: List[Any] = []
        self._sse_lock = threading.Lock()
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        scheduler.add_listener(self._broadcast)

    # ------------------------------------------------------------ SSE 推送
    def _broadcast(self, event: Dict[str, Any]) -> None:
        with self._sse_lock:
            clients = list(self._sse_clients)
        dead = []
        payload = f"data: {json.dumps(event, ensure_ascii=False)}\n\n".encode("utf-8")
        for q in clients:
            try:
                q.put_nowait(payload)
            except Exception:
                dead.append(q)
        if dead:
            with self._sse_lock:
                for q in dead:
                    if q in self._sse_clients:
                        self._sse_clients.remove(q)

    def start(self) -> None:
        server = self
        handler = _make_handler(server)
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        actual_port = self._httpd.server_address[1]
        self.port = actual_port
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name="http-server", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()

    def reset_with_config(self) -> None:
        specs, slots, mem = load_config(self.config_path)
        self.scheduler.reset(specs=specs, total_slots=slots, total_mem=mem)


def _make_handler(server: CoordinatorServer):
    class Handler(BaseHTTPRequestHandler):
        server_version = "IncrementalCoordinator/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # 静默默认访问日志
            pass

        def _json(self, code: int, body: Any) -> None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _error(self, code: int, message: str) -> None:
            self._json(code, {"error": message})

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/api/status":
                self._json(200, server.scheduler.status())
            elif path == "/api/events":
                self._serve_sse(server)
            else:
                self._serve_static(path)

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            try:
                if path == "/api/scheduler/start":
                    server.scheduler.start()
                elif path == "/api/scheduler/pause":
                    server.scheduler.pause()
                elif path == "/api/scheduler/restart":
                    server.scheduler.restart()
                elif path == "/api/scheduler/reset":
                    server.reset_with_config()
                elif path.startswith("/api/tasks/"):
                    self._task_action(server, path)
                else:
                    self._error(404, "未知接口")
                    return
                self._json(200, {"ok": True})
            except (RuntimeError, KeyError) as exc:
                self._error(409, str(exc))

        def _task_action(self, srv: CoordinatorServer, path: str) -> None:
            parts = [p for p in path.split("/") if p]
            # /api/tasks/{id}/action
            if len(parts) != 4 or parts[0] != "api" or parts[1] != "tasks":
                raise KeyError("任务接口格式应为 /api/tasks/{id}/{action}")
            task_id, action = parts[2], parts[3]
            if action == "cancel":
                srv.scheduler.cancel_task(task_id)
            elif action == "retry":
                srv.scheduler.retry_task(task_id)
            elif action == "inject":
                srv.scheduler.inject_failure(task_id)
            else:
                raise KeyError(f"未知操作 {action}")

        def _serve_sse(self, srv: CoordinatorServer) -> None:
            import queue
            q: "queue.Queue[bytes]" = queue.Queue(maxsize=1000)
            with srv._sse_lock:
                srv._sse_clients.append(q)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            try:
                self.wfile.write(b": connected\n\n")
                self.wfile.flush()
                while True:
                    try:
                        chunk = q.get(timeout=15)
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except Exception:
                        # 超时后发心跳保活
                        try:
                            self.wfile.write(b": keepalive\n\n")
                            self.wfile.flush()
                        except OSError:
                            break
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with srv._sse_lock:
                    if q in srv._sse_clients:
                        srv._sse_clients.remove(q)

        def _serve_static(self, path: str) -> None:
            if path == "/":
                path = "/index.html"
            # 防路径穿越：resolve 后必须仍在静态目录内
            target = (STATIC_DIR / path.lstrip("/")).resolve()
            if STATIC_DIR not in target.parents and target != STATIC_DIR:
                self._error(403, "禁止访问")
                return
            if not target.is_file():
                self._error(404, "页面不存在")
                return
            suffix = target.suffix
            ctype = {
                ".html": "text/html; charset=utf-8",
                ".js": "application/javascript; charset=utf-8",
                ".css": "text/css; charset=utf-8",
                ".svg": "image/svg+xml",
            }.get(suffix, "application/octet-stream")
            data = target.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return Handler
