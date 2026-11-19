"""命令行入口：python -m coordinator.main --config config.json"""
from __future__ import annotations

import argparse
import signal
import sys

from .config import load_config
from .persistence import SnapshotStore
from .scheduler import Scheduler
from .server import CoordinatorServer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="多表增量装载协调器")
    parser.add_argument("--config", default="config.json", help="装载配置 JSON")
    parser.add_argument("--state-dir", default=".state", help="快照与日志目录")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--fresh", action="store_true",
                        help="忽略旧快照，清空状态目录后重新开始")
    args = parser.parse_args(argv)

    specs, total_slots, total_mem = load_config(args.config)
    store = SnapshotStore(args.state_dir)
    if args.fresh:
        store.clear()

    scheduler = Scheduler(
        specs, total_slots=total_slots, total_mem=total_mem,
        state_dir=args.state_dir, store=store,
    )
    http_server = CoordinatorServer(scheduler, args.config, args.host, args.port)
    http_server.start()

    stopped = {"flag": False}

    def _shutdown(_signum: int, _frame: object) -> None:
        if stopped["flag"]:
            return
        stopped["flag"] = True
        print("\n正在停止调度器 ...")
        http_server.stop()
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"增量装载协调器已启动: http://{args.host}:{http_server.port}")
    print(f"状态目录: {args.state_dir}（snapshot.json + journal.jsonl）")
    print("按 Ctrl+C 停止")
    try:
        while not stopped["flag"]:
            signal.pause()
    except AttributeError:
        # Windows 没有 signal.pause
        import time
        while not stopped["flag"]:
            time.sleep(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
