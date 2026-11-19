"""任务规格、运行时状态机与 DAG 校验。"""
from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


class TaskState(str, enum.Enum):
    PENDING = "PENDING"                # 依赖未满足或等待被调度
    READY = "READY"                    # 依赖已满足，等待资源
    WAITING_LOCK = "WAITING_LOCK"      # 等待冲突分区锁
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"          # 失败退避中，等待下一次尝试
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"                  # 重试耗尽，等待人工补齐
    CANCELLED = "CANCELLED"
    BLOCKED = "BLOCKED"                # 上游失败/取消，下游等待补齐


# 终态：重启后也不会再被自动调度（除非人工 retry）
TERMINAL_STATES = {
    TaskState.SUCCESS,
    TaskState.FAILED,
    TaskState.CANCELLED,
}

# 上游处于这些状态时，下游必须 BLOCKED
UPSTREAM_BAD_STATES = {
    TaskState.FAILED,
    TaskState.CANCELLED,
    TaskState.BLOCKED,
}


@dataclass(frozen=True)
class TaskSpec:
    """一个增量装载任务的静态规格（对应一张表的一次装载）。"""

    task_id: str
    table: str
    partitions: List[str]                      # 本次需要装载的分区键
    deps: List[str] = field(default_factory=list)
    slots: int = 1                             # 申请的执行槽位
    mem: int = 1                               # 申请的内存单位
    timeout: float = 30.0                      # 单次尝试的超时秒数
    max_attempts: int = 3                      # 最大自动尝试次数
    partition_seconds: float = 0.25            # 模拟：每个分区装载耗时
    chunk_seconds: float = 0.05                # 模拟：工作块粒度（取消/超时检测粒度）
    fail_times: int = 0                        # 前 N 次自动尝试注入失败
    fail_kind: str = "error"                   # error | timeout
    backoff_base: float = 0.8                  # 指数退避基数

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskSpec":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in data.items() if k in known})


class TaskRuntime:
    """任务的可变运行时状态，全部可序列化到快照。"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        self.state: TaskState = TaskState.PENDING
        self.attempts = 0                       # 已发起的尝试次数
        self.manual_recovered = False           # 是否经历过人工补齐（关闭自动故障注入）
        self.checkpoint = -1                    # 恢复点：已完成的最大分区下标，-1 表示无
        self.completed_partitions: List[int] = []
        self.current_partition: Optional[int] = None
        self.progress = 0.0                     # 总进度 0..1
        self.backoff_until: Optional[float] = None
        self.started_at: Optional[float] = None
        self.deadline: Optional[float] = None
        self.last_error: Optional[str] = None
        self.wait: Optional[Dict[str, Any]] = None   # 等待信息（资源/锁）
        self.cancel_requested = False
        self.inject_failure = False      # 自动注入（规格里的 fail_times）
        self.manual_failure = False      # 页面/API 手动注入
        self.timeout_signalled = False

    @property
    def done_count(self) -> int:
        return len(self.completed_partitions)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "attempts": self.attempts,
            "manual_recovered": self.manual_recovered,
            "checkpoint": self.checkpoint,
            "completed_partitions": list(self.completed_partitions),
            "current_partition": self.current_partition,
            "progress": round(self.progress, 4),
            "backoff_until": self.backoff_until,
            "started_at": self.started_at,
            "deadline": self.deadline,
            "last_error": self.last_error,
            "wait": self.wait,
            "cancel_requested": self.cancel_requested,
            "inject_failure": self.inject_failure,
            "manual_failure": self.manual_failure,
            "timeout_signalled": self.timeout_signalled,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskRuntime":
        rt = cls(data["task_id"])
        rt.state = TaskState(data["state"])
        for name in (
            "attempts", "manual_recovered", "checkpoint", "completed_partitions",
            "current_partition", "progress", "backoff_until", "started_at",
            "deadline", "last_error", "wait", "cancel_requested",
            "inject_failure", "manual_failure", "timeout_signalled",
        ):
            setattr(rt, name, data[name])
        return rt


class SpecValidationError(ValueError):
    pass


def validate_specs(specs: List[TaskSpec]) -> List[str]:
    """校验规格并返回拓扑序（Kahn 算法）；发现环或悬空依赖时抛异常。"""
    if not specs:
        raise SpecValidationError("至少需要一个任务")
    ids = [s.task_id for s in specs]
    if len(set(ids)) != len(ids):
        raise SpecValidationError("任务 id 重复")
    by_id = {s.task_id: s for s in specs}

    for s in specs:
        if not s.partitions:
            raise SpecValidationError(f"任务 {s.task_id} 没有分区")
        if s.slots <= 0 or s.mem <= 0:
            raise SpecValidationError(f"任务 {s.task_id} 资源申请必须为正数")
        if s.max_attempts <= 0:
            raise SpecValidationError(f"任务 {s.task_id} max_attempts 必须 >= 1")
        if s.fail_kind not in ("error", "timeout"):
            raise SpecValidationError(f"任务 {s.task_id} fail_kind 非法: {s.fail_kind}")
        for d in s.deps:
            if d not in by_id:
                raise SpecValidationError(f"任务 {s.task_id} 依赖了不存在的任务 {d}")
            if d == s.task_id:
                raise SpecValidationError(f"任务 {s.task_id} 不能依赖自己")

    indegree = {s.task_id: len(set(s.deps)) for s in specs}
    # 保持输入顺序，使调度顺序稳定
    queue = [tid for tid in ids if indegree[tid] == 0]
    order: List[str] = []
    while queue:
        tid = queue.pop(0)
        order.append(tid)
        for s in specs:
            if tid in s.deps:
                indegree[s.task_id] -= 1
                if indegree[s.task_id] == 0:
                    queue.append(s.task_id)
    if len(order) != len(specs):
        stuck = [tid for tid, deg in indegree.items() if deg > 0]
        raise SpecValidationError(f"DAG 存在环，涉及任务: {stuck}")
    return order


def now_ts() -> float:
    return time.time()
