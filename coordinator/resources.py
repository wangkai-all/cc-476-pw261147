"""资源预算：全局槽位 + 内存单位，任务整体预留、整体释放。"""
from __future__ import annotations

import threading
from typing import Dict, List, Optional

from .model import TaskSpec


class ResourceManager:
    def __init__(self, total_slots: int, total_mem: int,
                 lock: Optional[threading.RLock] = None):
        self.total_slots = total_slots
        self.total_mem = total_mem
        self._used_slots = 0
        self._used_mem = 0
        # 复用调度器的状态锁，避免"状态锁/资源锁"嵌套导致 AB-BA 死锁
        self._lock = lock or threading.RLock()
        self._allocated: Dict[str, TaskSpec] = {}

    def can_fit(self, spec: TaskSpec) -> bool:
        return (
            self._used_slots + spec.slots <= self.total_slots
            and self._used_mem + spec.mem <= self.total_mem
        )

    def try_allocate(self, spec: TaskSpec) -> bool:
        with self._lock:
            if spec.task_id in self._allocated:
                return True
            if not self.can_fit(spec):
                return False
            self._used_slots += spec.slots
            self._used_mem += spec.mem
            self._allocated[spec.task_id] = spec
            return True

    def release(self, task_id: str) -> None:
        with self._lock:
            spec = self._allocated.pop(task_id, None)
            if spec:
                self._used_slots -= spec.slots
                self._used_mem -= spec.mem

    def held_by(self) -> List[Dict[str, object]]:
        with self._lock:
            return [
                {
                    "task_id": tid,
                    "table": spec.table,
                    "slots": spec.slots,
                    "mem": spec.mem,
                }
                for tid, spec in self._allocated.items()
            ]

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            return {
                "total_slots": self.total_slots,
                "used_slots": self._used_slots,
                "total_mem": self.total_mem,
                "used_mem": self._used_mem,
                "allocations": self.held_by(),
            }
