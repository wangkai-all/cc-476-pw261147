"""共享分区排他锁。

锁键为 `表名/分区`，同一分区在同一时刻只允许一个任务持有；
冲突任务进入 FIFO 等待队列，页面可展示"谁在等谁"。
锁按分区键全局排序申请，从机制上避免死锁。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class LockWaiter:
    task_id: str
    requested: List[str]
    since: float = field(default_factory=time.time)


class PartitionLockManager:
    def __init__(self, lock: Optional[threading.RLock] = None) -> None:
        # 复用调度器的状态锁，避免多锁嵌套的 AB-BA 死锁
        self._cv = threading.Condition(lock or threading.RLock())
        self._holders: Dict[str, str] = {}      # partition_key -> task_id
        self._task_keys: Dict[str, List[str]] = {}
        self._waiters: Dict[str, LockWaiter] = {}

    @staticmethod
    def keys_for(table: str, partitions: List[str]) -> List[str]:
        # 全局唯一且有序，保证所有任务以相同顺序加锁
        return sorted(f"{table}/{p}" for p in partitions)

    def try_acquire(self, task_id: str, table: str,
                    partitions: List[str]) -> Tuple[bool, Optional[str]]:
        """非阻塞尝试。成功返回 (True, None)，冲突返回 (False, 持有方 task_id)。"""
        keys = self.keys_for(table, partitions)
        with self._cv:
            self._waiters.pop(task_id, None)
            for key in keys:
                owner = self._holders.get(key)
                if owner is not None and owner != task_id:
                    self._waiters[task_id] = LockWaiter(task_id, keys)
                    return False, owner
            for key in keys:
                self._holders[key] = task_id
            self._task_keys[task_id] = keys
            return True, None

    def release(self, task_id: str) -> None:
        with self._cv:
            keys = self._task_keys.pop(task_id, [])
            for key in keys:
                if self._holders.get(key) == task_id:
                    del self._holders[key]
            self._waiters.pop(task_id, None)
            self._cv.notify_all()

    def cancel_wait(self, task_id: str) -> None:
        with self._cv:
            self._waiters.pop(task_id, None)
            self._cv.notify_all()

    def snapshot(self) -> Dict[str, object]:
        with self._cv:
            return {
                "holders": [
                    {"key": k, "task_id": owner}
                    for k, owner in sorted(self._holders.items())
                ],
                "waiters": [
                    {
                        "task_id": w.task_id,
                        "keys": list(w.requested),
                        "blocked_by": next(
                            (self._holders[k] for k in w.requested
                             if k in self._holders and self._holders[k] != w.task_id),
                            None,
                        ),
                        "wait_seconds": round(time.time() - w.since, 1),
                    }
                    for w in sorted(self._waiters.values(), key=lambda x: x.since)
                ],
            }
