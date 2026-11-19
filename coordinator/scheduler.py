"""调度核心。

门控顺序（每个调度 tick，按 DAG 拓扑序遍历）：
  1. 依赖门：所有上游必须 SUCCESS，否则 PENDING；上游失败/取消则下游 BLOCKED
  2. 资源门：槽位与内存预算足够，否则 READY（资源等待）
  3. 锁门：所有共享分区排他锁可用，否则 WAITING_LOCK（锁等待，FIFO 队列）
三道门全过才启动工作线程。失败任务按指数退避自动重试，从检查点续跑；
重试耗尽转 FAILED，其传递下游转 BLOCKED 等待人工补齐。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Set

from .locks import PartitionLockManager
from .model import (
    TaskRuntime,
    TaskSpec,
    TaskState,
    UPSTREAM_BAD_STATES,
    validate_specs,
)
from .persistence import SnapshotStore
from .resources import ResourceManager

EVENT_LIMIT = 500
TICK_SECONDS = 0.03


class Aborted(Exception):
    """调度器停止（模拟崩溃/重启），工作线程静默退出，状态保持崩溃前快照。"""


class Scheduler:
    def __init__(
        self,
        specs: List[TaskSpec],
        total_slots: int,
        total_mem: int,
        state_dir: str,
        store: Optional[SnapshotStore] = None,
        auto_start: bool = True,
    ):
        self._store = store or SnapshotStore(state_dir)
        self._specs: Dict[str, TaskSpec] = {s.task_id: s for s in specs}
        self._topo = validate_specs(specs)
        self._total_slots = total_slots
        self._total_mem = total_mem
        self._init_locks_and_state()
        self._events: deque[Dict[str, Any]] = deque(maxlen=EVENT_LIMIT)
        self._listeners: Set[Callable[[Dict[str, Any]], None]] = set()
        self._generation = 1
        self._stopping = threading.Event()
        self._running = auto_start
        self._workers: List[threading.Thread] = []

        restored = self._store.read_snapshot()
        if restored:
            self._restore(restored)
            self._generation += 1
            # auto_start=True（进程重启默认）：恢复后自动续跑；
            # False：保持暂停，便于先核验恢复点再手动 start()
            self._running = bool(auto_start)
            self._emit("SCHEDULER_RECOVERED", {
                "generation": self._generation,
                "auto_start": self._running,
                "note": "从磁盘快照恢复，崩溃前在跑的任务回到待调度并从检查点续跑",
            })
        self._persist()
        self._loop_thread = threading.Thread(
            target=self._dispatch_loop, name="scheduler-dispatch", daemon=True
        )
        self._loop_thread.start()
        if self._running:
            self._emit("SCHEDULER_STARTED", {"generation": self._generation})

    # ------------------------------------------------------------------ 基础
    def _init_locks_and_state(self) -> None:
        self._state_lock = threading.RLock()
        self._cv = threading.Condition(self._state_lock)
        # 资源/锁管理器复用同一把 RLock，整个调度器只有一把锁，杜绝嵌套死锁
        self.resources = ResourceManager(self._total_slots, self._total_mem,
                                         lock=self._state_lock)
        self.locks = PartitionLockManager(lock=self._state_lock)
        self._runtimes: Dict[str, TaskRuntime] = {
            tid: TaskRuntime(tid) for tid in self._topo
        }

    def add_listener(self, fn: Callable[[Dict[str, Any]], None]) -> None:
        with self._state_lock:
            self._listeners.add(fn)

    def remove_listener(self, fn: Callable[[Dict[str, Any]], None]) -> None:
        with self._state_lock:
            self._listeners.discard(fn)

    def _emit(self, event_type: str, payload: Optional[Dict[str, Any]] = None,
              task_id: Optional[str] = None) -> Dict[str, Any]:
        event = {
            "ts": round(time.time(), 3),
            "type": event_type,
            "task_id": task_id,
            "payload": payload or {},
        }
        with self._state_lock:
            self._events.append(event)
            listeners = list(self._listeners)
        self._store.append_journal(event)
        for fn in listeners:
            try:
                fn(event)
            except Exception:  # 监听器异常不影响调度
                pass
        return event

    def _persist(self) -> None:
        """调用方必须持有 _state_lock。"""
        self._store.write_snapshot({
            "version": 1,
            "saved_at": time.time(),
            "running": self._running,
            "generation": self._generation,
            "resources": {
                "total_slots": self._total_slots,
                "total_mem": self._total_mem,
            },
            "specs": [self._specs[tid].to_dict() for tid in self._topo],
            "topo": list(self._topo),
            "runtimes": {tid: rt.to_dict() for tid, rt in self._runtimes.items()},
        })

    def _restore(self, data: Dict[str, Any]) -> None:
        for tid, rt_data in data["runtimes"].items():
            rt = TaskRuntime.from_dict(rt_data)
            # 崩溃瞬间在跑的任务：线程已不存在，回到待调度，保留尝试次数与检查点
            if rt.state == TaskState.RUNNING:
                rt.state = TaskState.PENDING
            rt.deadline = None
            rt.started_at = None
            rt.current_partition = None
            rt.wait = None
            rt.cancel_requested = False
            rt.inject_failure = False
            rt.manual_failure = False
            rt.timeout_signalled = False
            self._runtimes[tid] = rt

    # ------------------------------------------------------------------ 控制
    def start(self) -> None:
        with self._state_lock:
            if self._stopping.is_set():
                raise RuntimeError("调度器已停止，请使用 restart")
            if self._running:
                raise RuntimeError("调度器已在运行")
            self._running = True
            self._cv.notify_all()
            self._persist()
        self._emit("SCHEDULER_STARTED", {"generation": self._generation})

    def pause(self) -> None:
        """暂停：不再启动新任务，已在跑的任务继续跑完。"""
        with self._state_lock:
            if self._stopping.is_set() or not self._running:
                raise RuntimeError("调度器未在运行")
            self._running = False
            self._persist()
        self._emit("SCHEDULER_PAUSED", {"note": "运行中的任务会继续执行完毕"})

    def simulate_crash(self) -> None:
        """模拟进程崩溃：中断所有工作线程并落盘最后状态，之后由 restart 恢复。"""
        with self._state_lock:
            if self._stopping.is_set():
                return
            self._stopping.set()
            self._running = False
            self._cv.notify_all()
            self._persist()
        self._emit("SCHEDULER_CRASH", {"note": "模拟调度器崩溃，状态已落盘"})
        self._loop_thread.join(timeout=5)
        for w in list(self._workers):
            w.join(timeout=5)

    def restart(self) -> None:
        """调度器重启：读快照重建内存状态，崩溃前在跑的任务从检查点续跑。"""
        if not self._stopping.is_set():
            self.simulate_crash()
        data = self._store.read_snapshot()
        if data is None:
            raise RuntimeError("没有可恢复的快照")
        self._stopping = threading.Event()
        self._init_locks_and_state()
        self._restore(data)
        self._generation = int(data.get("generation", 1)) + 1
        self._running = True
        self._workers = []
        self._loop_thread = threading.Thread(
            target=self._dispatch_loop, name="scheduler-dispatch", daemon=True
        )
        self._loop_thread.start()
        self._emit("SCHEDULER_RESTARTED", {
            "generation": self._generation,
            "note": "快照恢复完成，未完成任务按检查点继续",
        })
        with self._state_lock:
            self._persist()

    def shutdown(self) -> None:
        with self._state_lock:
            self._stopping.set()
            self._cv.notify_all()
        self._loop_thread.join(timeout=5)
        for w in list(self._workers):
            w.join(timeout=5)

    def reset(self, specs: Optional[List[TaskSpec]] = None,
              total_slots: Optional[int] = None,
              total_mem: Optional[int] = None) -> None:
        """清空状态，按（可能更新过的）规格重新开始。"""
        if not self._stopping.is_set():
            self.simulate_crash()
        if specs is not None:
            self._specs = {s.task_id: s for s in specs}
            self._topo = validate_specs(specs)
        if total_slots is not None:
            self._total_slots = total_slots
        if total_mem is not None:
            self._total_mem = total_mem
        self._store.clear()
        self._events.clear()
        self._stopping = threading.Event()
        self._init_locks_and_state()
        self._generation += 1
        self._running = True
        self._workers = []
        # 先发射 RESET 事件，再启动 dispatch，保证它是新时间线的第一条
        self._emit("SCHEDULER_RESET", {"generation": self._generation})
        self._loop_thread = threading.Thread(
            target=self._dispatch_loop, name="scheduler-dispatch", daemon=True
        )
        self._loop_thread.start()
        with self._state_lock:
            self._persist()

    def cancel_task(self, task_id: str) -> None:
        with self._state_lock:
            rt = self._require(task_id)
            spec = self._specs[task_id]
            if rt.state == TaskState.RUNNING:
                rt.cancel_requested = True
                self._persist()
                self._emit("TASK_CANCEL_REQUESTED", {"note": "协作式取消，等待工作线程响应"},
                           task_id=task_id)
                return
            if rt.state in (TaskState.SUCCESS, TaskState.FAILED, TaskState.CANCELLED):
                raise RuntimeError(f"任务 {task_id} 处于终态 {rt.state.value}，无法取消")
            rt.state = TaskState.CANCELLED
            rt.wait = None
            rt.backoff_until = None
            self.locks.cancel_wait(task_id)
            self._persist()
        self._emit("TASK_CANCELLED", {"table": spec.table}, task_id=task_id)
        with self._state_lock:
            self._recompute_blocked()
            self._persist()

    def retry_task(self, task_id: str) -> None:
        """人工补齐：失败/取消的任务重新进入调度，保留检查点，关闭自动故障注入。"""
        with self._state_lock:
            rt = self._require(task_id)
            if rt.state not in (TaskState.FAILED, TaskState.CANCELLED):
                raise RuntimeError(f"仅 FAILED/CANCELLED 可人工补齐，当前 {rt.state.value}")
            rt.state = TaskState.PENDING
            rt.manual_recovered = True
            rt.last_error = None
            rt.wait = None
            rt.backoff_until = None
            rt.cancel_requested = False
            rt.timeout_signalled = False
            rt.inject_failure = False
            rt.manual_failure = False
            self._cv.notify_all()
            self._persist()
        self._emit("TASK_RETRY_REQUESTED", {
            "note": "人工补齐，从检查点分区续跑",
            "checkpoint": rt.checkpoint,
        }, task_id=task_id)
        with self._state_lock:
            self._recompute_blocked()
            self._persist()

    def inject_failure(self, task_id: str) -> None:
        with self._state_lock:
            rt = self._require(task_id)
            if rt.state != TaskState.RUNNING:
                raise RuntimeError("只能对运行中的任务注入故障")
            rt.manual_failure = True
        self._emit("FAILURE_INJECTED", {"manual": True}, task_id=task_id)

    def _require(self, task_id: str) -> TaskRuntime:
        if task_id not in self._runtimes:
            raise KeyError(f"未知任务 {task_id}")
        return self._runtimes[task_id]

    # ------------------------------------------------------------------ 调度
    def _dispatch_loop(self) -> None:
        while not self._stopping.is_set():
            with self._state_lock:
                if self._running:
                    try:
                        self._tick()
                    except Exception as exc:  # 调度循环不能因为单次异常死掉
                        self._emit("SCHEDULER_ERROR", {"error": repr(exc)})
            self._stopping.wait(TICK_SECONDS)

    def _tick(self) -> None:
        self._recompute_blocked()
        now = time.time()

        # 退避结束的任务重新进入候选
        for rt in self._runtimes.values():
            if rt.state == TaskState.RETRY_WAIT and rt.backoff_until and now >= rt.backoff_until:
                rt.state = TaskState.PENDING
                rt.backoff_until = None

        for tid in self._topo:
            spec = self._specs[tid]
            rt = self._runtimes[tid]
            if rt.state not in (TaskState.PENDING, TaskState.READY,
                                TaskState.WAITING_LOCK):
                continue
            # 依赖门
            if not all(self._runtimes[d].state == TaskState.SUCCESS for d in spec.deps):
                rt.state = TaskState.PENDING
                continue
            # 资源门
            if not self.resources.can_fit(spec):
                rt.state = TaskState.READY
                rt.wait = {
                    "type": "resource",
                    "reason": f"需要 slots={spec.slots} mem={spec.mem}，当前预算不足",
                    "slots": spec.slots,
                    "mem": spec.mem,
                }
                continue
            # 锁门：冲突分区串行
            acquired, owner = self.locks.try_acquire(tid, spec.table, spec.partitions)
            if not acquired:
                rt.state = TaskState.WAITING_LOCK
                rt.wait = {
                    "type": "lock",
                    "blocked_by": owner,
                    "keys": self.locks.keys_for(spec.table, spec.partitions),
                }
                continue
            # 三门全过：预留资源并启动
            ok = self.resources.try_allocate(spec)
            if not ok:  # 理论上不会发生（同一 tick 内串行分配）
                self.locks.release(tid)
                continue
            self._start_attempt(spec, rt)

        self._check_timeouts()
        self._persist()

    def _start_attempt(self, spec: TaskSpec, rt: TaskRuntime) -> None:
        rt.attempts += 1
        rt.state = TaskState.RUNNING
        rt.wait = None
        rt.started_at = time.time()
        rt.deadline = rt.started_at + spec.timeout
        rt.timeout_signalled = False
        rt.cancel_requested = False
        total = len(spec.partitions)
        done = rt.done_count
        rt.progress = done / total

        # 自动故障注入（前 fail_times 次尝试）
        inject = (not rt.manual_recovered and spec.fail_times > 0
                  and rt.attempts <= spec.fail_times)
        if inject and spec.fail_kind == "timeout":
            # 缩短本次尝试的 deadline，模拟在跑完部分分区后于真实时间点超时，
            # 从而留下检查点，验证"超时后从检查点续跑"
            rt.deadline = rt.started_at + min(spec.timeout, 0.15)
        if inject:
            if spec.fail_kind == "error":
                rt.inject_failure = True
            self._emit("FAILURE_INJECTED", {
                "manual": False,
                "kind": spec.fail_kind,
                "attempt": rt.attempts,
            }, task_id=spec.task_id)

        self._emit("TASK_STARTED", {
            "attempt": rt.attempts,
            "resume_from": rt.checkpoint + 1,
            "timeout": spec.timeout,
        }, task_id=spec.task_id)
        worker = threading.Thread(
            target=self._run_task, args=(spec.task_id,),
            name=f"worker-{spec.task_id}-{rt.attempts}", daemon=True,
        )
        self._workers.append(worker)
        worker.start()

    def _check_timeouts(self) -> None:
        now = time.time()
        for rt in self._runtimes.values():
            if (rt.state == TaskState.RUNNING and rt.deadline
                    and now > rt.deadline and not rt.timeout_signalled):
                rt.timeout_signalled = True
                spec = self._specs[rt.task_id]
                self._emit("TIMEOUT_SIGNALLED", {
                    "timeout": spec.timeout,
                    "attempt": rt.attempts,
                }, task_id=rt.task_id)

    def _recompute_blocked(self) -> None:
        """上游失败/取消的传递下游进入 BLOCKED；上游补齐后自动解除。"""
        for tid in self._topo:
            rt = self._runtimes[tid]
            if rt.state in (TaskState.SUCCESS, TaskState.FAILED,
                            TaskState.CANCELLED, TaskState.RUNNING):
                continue
            bad = [d for d in self._specs[tid].deps
                   if self._runtimes[d].state in UPSTREAM_BAD_STATES]
            if bad:
                if rt.state != TaskState.BLOCKED:
                    rt.state = TaskState.BLOCKED
                    rt.wait = {"type": "dependency", "upstream": bad}
                    self._emit("TASK_BLOCKED", {"upstream": bad}, task_id=tid)
            elif rt.state == TaskState.BLOCKED:
                rt.state = TaskState.PENDING
                rt.wait = None
                self._emit("TASK_UNBLOCKED", {"note": "上游已补齐，重新进入调度"},
                           task_id=tid)

    # ------------------------------------------------------------------ 执行
    def _run_task(self, task_id: str) -> None:
        spec = self._specs[task_id]
        rt = self._runtimes[task_id]
        try:
            start_index = rt.checkpoint + 1
            total = len(spec.partitions)
            for idx in range(start_index, total):
                self._raise_if_interrupted(rt)
                with self._state_lock:
                    rt.current_partition = idx
                    self._persist()
                self._emit("PARTITION_STARTED", {
                    "index": idx,
                    "partition": spec.partitions[idx],
                }, task_id=task_id)

                # 模拟装载：分块睡眠，保证取消/超时能被及时感知
                remaining = spec.partition_seconds
                while remaining > 0:
                    self._raise_if_interrupted(rt)
                    if rt.manual_failure:
                        rt.manual_failure = False
                        raise RuntimeError("手动注入的故障")
                    step = min(spec.chunk_seconds, remaining)
                    time.sleep(step)
                    remaining -= step
                    with self._state_lock:
                        rt.progress = min(0.999, (idx + (spec.partition_seconds - remaining)
                                                  / spec.partition_seconds) / total)

                with self._state_lock:
                    rt.completed_partitions.append(idx)
                    rt.checkpoint = idx
                    rt.current_partition = None
                    rt.progress = (idx + 1) / total
                    self._persist()
                self._emit("PARTITION_DONE", {
                    "index": idx,
                    "partition": spec.partitions[idx],
                    "checkpoint": rt.checkpoint,
                }, task_id=task_id)

                # 自动注入的错误在完成一个分区后触发，以演示检查点续跑
                if rt.inject_failure:
                    rt.inject_failure = False
                    raise RuntimeError(
                        f"注入失败: {spec.table} 分区 {spec.partitions[idx]} 装载报错"
                    )

            self._finish_success(spec, rt)
        except Aborted:
            return  # 崩溃/重启：状态留在快照里，由恢复流程接管
        except TimeoutError:
            self._finish_failure(task_id, f"超过 {spec.timeout}s 超时限制",
                                 kind="timeout")
        except Exception as exc:  # noqa: BLE001 - 工作线程需要兜住所有异常
            self._finish_failure(task_id, str(exc),
                                 kind="cancel" if isinstance(exc, InterruptedError)
                                 else "error")

    def _raise_if_interrupted(self, rt: TaskRuntime) -> None:
        if self._stopping.is_set():
            raise Aborted()
        if rt.cancel_requested:
            raise InterruptedError("任务被用户取消")
        if rt.timeout_signalled:
            raise TimeoutError("任务超时")

    def _finish_success(self, spec: TaskSpec, rt: TaskRuntime) -> None:
        with self._state_lock:
            if self._stopping.is_set():
                raise Aborted()
            if rt.cancel_requested:
                raise InterruptedError("任务被用户取消")
            rt.state = TaskState.SUCCESS
            rt.progress = 1.0
            rt.current_partition = None
            rt.deadline = None
            rt.wait = None
            self.resources.release(spec.task_id)
            self.locks.release(spec.task_id)
            self._cv.notify_all()
            self._persist()
        self._emit("TASK_SUCCESS", {
            "table": spec.table,
            "attempts": rt.attempts,
            "partitions": len(spec.partitions),
        }, task_id=spec.task_id)
        with self._state_lock:
            self._recompute_blocked()
            self._persist()

    def _finish_failure(self, task_id: str, message: str, *, kind: str) -> None:
        spec = self._specs[task_id]
        rt = self._runtimes[task_id]
        with self._state_lock:
            if self._stopping.is_set():
                return
            self.resources.release(task_id)
            self.locks.release(task_id)
            rt.current_partition = None
            rt.deadline = None
            rt.wait = None
            rt.last_error = message

            if kind == "cancel":
                rt.state = TaskState.CANCELLED
                self._persist()
        if kind == "cancel":
            self._emit("TASK_CANCELLED", {"table": spec.table, "reason": message},
                       task_id=task_id)
            with self._state_lock:
                self._recompute_blocked()
                self._persist()
            return

        exhausted = rt.attempts >= spec.max_attempts
        self._emit("TASK_ERROR", {
            "error": message,
            "kind": kind,
            "attempt": rt.attempts,
            "max_attempts": spec.max_attempts,
            "checkpoint": rt.checkpoint,
            "will_retry": not exhausted,
        }, task_id=task_id)

        with self._state_lock:
            if exhausted:
                rt.state = TaskState.FAILED
                self._persist()
        if exhausted:
            self._emit("TASK_FAILED", {
                "error": message,
                "note": "重试已耗尽，下游进入 BLOCKED 等待人工补齐",
            }, task_id=task_id)
            with self._state_lock:
                self._recompute_blocked()
                self._persist()
            return

        backoff = round(spec.backoff_base * (2 ** (rt.attempts - 1)), 2)
        with self._state_lock:
            rt.state = TaskState.RETRY_WAIT
            rt.backoff_until = time.time() + backoff
            self._cv.notify_all()
            self._persist()
        self._emit("TASK_RETRY", {
            "retry_in": backoff,
            "next_attempt": rt.attempts + 1,
            "checkpoint": rt.checkpoint,
        }, task_id=task_id)

    # ------------------------------------------------------------------ 视图
    def status(self) -> Dict[str, Any]:
        with self._state_lock:
            tasks = []
            for tid in self._topo:
                spec = self._specs[tid].to_dict()
                rt = self._runtimes[tid].to_dict()
                merged = {**spec, **rt}
                merged["retry_in"] = (
                    round(rt["backoff_until"] - time.time(), 1)
                    if rt["state"] == TaskState.RETRY_WAIT.value and rt["backoff_until"]
                    else None
                )
                tasks.append(merged)
            return {
                "scheduler": {
                    "running": self._running,
                    "stopping": self._stopping.is_set(),
                    "generation": self._generation,
                    "topo": list(self._topo),
                },
                "resources": self.resources.snapshot(),
                "locks": self.locks.snapshot(),
                "tasks": tasks,
                "events": list(self._events),
            }

    def join_idle(self, timeout: float = 30.0) -> bool:
        """等待所有任务进入终态（测试用），返回是否全部结束。

        BLOCKED 也算稳定态：它在等人工补齐，没有自动动作会再发生。
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._state_lock:
                if all(rt.state in (TaskState.SUCCESS, TaskState.FAILED,
                                    TaskState.CANCELLED, TaskState.BLOCKED)
                       for rt in self._runtimes.values()):
                    return True
            time.sleep(0.05)
        return False
