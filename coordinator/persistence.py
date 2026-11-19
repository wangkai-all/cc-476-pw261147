"""快照与事件日志的磁盘持久化（全部标准库）。

- snapshot.json: 原子写入（tmp + os.replace），调度器重启后据此恢复
- journal.jsonl: 追加式事件流水，便于审计与页面时间线
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

SNAPSHOT_NAME = "snapshot.json"
JOURNAL_NAME = "journal.jsonl"


class SnapshotStore:
    def __init__(self, state_dir: str | os.PathLike[str]):
        self.dir = Path(state_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_path = self.dir / SNAPSHOT_NAME
        self.journal_path = self.dir / JOURNAL_NAME

    def write_snapshot(self, payload: Dict[str, Any]) -> None:
        # 确保目录存在（外部可能清理过状态目录），tmp 文件名加 pid 避免并发互踩
        self.dir.mkdir(parents=True, exist_ok=True)
        tmp = self.dir / f".{SNAPSHOT_NAME}.{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.snapshot_path)

    def read_snapshot(self) -> Optional[Dict[str, Any]]:
        if not self.snapshot_path.exists():
            return None
        with open(self.snapshot_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def clear(self) -> None:
        for p in (self.snapshot_path, self.journal_path):
            if p.exists():
                p.unlink()

    def append_journal(self, event: Dict[str, Any]) -> None:
        with open(self.journal_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
