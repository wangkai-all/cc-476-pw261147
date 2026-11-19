"""调度核心测试：依赖并行、分区锁串行、资源门控、失败重试、检查点、取消、超时、崩溃恢复。"""
from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from coordinator.model import TaskSpec, TaskState, validate_specs, SpecValidationError
from coordinator.persistence import SnapshotStore
from coordinator.scheduler import Scheduler


def make_spec(tid, parts=("p1", "p2"), deps=None, slots=1, mem=1,
              timeout=10, max_attempts=3, partition_seconds=0.08,
              chunk_seconds=0.02, **kw):
    return TaskSpec(
        task_id=tid, table=tid, partitions=list(parts), deps=list(deps or []),
        slots=slots, mem=mem, timeout=timeout, max_attempts=max_attempts,
        partition_seconds=partition_seconds, chunk_seconds=chunk_seconds, **kw,
    )


class SchedulerTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir = self._tmp.name

    def tearDown(self):
        if hasattr(self, "sched"):
            self.sched.shutdown()
        self._tmp.cleanup()

    def make(self, specs, slots=4, mem=8, auto_start=True):
        store = SnapshotStore(self.state_dir)
        self.sched = Scheduler(specs, total_slots=slots, total_mem=mem,
                               state_dir=self.state_dir, store=store,
                               auto_start=auto_start)
        return self.sched

    def state_of(self, tid):
        return self.sched._runtimes[tid].state

    def assert_all(self, states):
        for tid, st in states.items():
            self.assertEqual(self.state_of(tid), st,
                             f"{tid} 应为 {st}，实际 {self.state_of(tid)}")


class TestParallelismAndDeps(SchedulerTestBase):
    def test_independent_tasks_run_in_parallel(self):
        specs = [
            make_spec("a", slots=1, mem=1, partition_seconds=0.25),
            make_spec("b", slots=1, mem=1, partition_seconds=0.25),
            make_spec("c", slots=1, mem=1, partition_seconds=0.25),
        ]
        sched = self.make(specs, slots=3, mem=3)
        deadline = time.time() + 3
        while time.time() < deadline:
            running = [t for t in sched.status()["tasks"] if t["state"] == "RUNNING"]
            if len(running) >= 2:
                break
            time.sleep(0.02)
        running = [t for t in sched.status()["tasks"] if t["state"] == "RUNNING"]
        self.assertGreaterEqual(len(running), 2, "无依赖且资源足够时应并行")
        self.assertTrue(sched.join_idle(10))
        self.assert_all({"a": TaskState.SUCCESS, "b": TaskState.SUCCESS,
                         "c": TaskState.SUCCESS})

    def test_dependency_gates_downstream(self):
        specs = [make_spec("a", partition_seconds=0.2),
                 make_spec("b", deps=["a"], partition_seconds=0.1)]
        sched = self.make(specs)
        # b 在 a 完成前必须保持 PENDING
        self.assertEqual(self.state_of("b"), TaskState.PENDING)
        time.sleep(0.08)
        self.assertEqual(self.state_of("b"), TaskState.PENDING)
        self.assertTrue(sched.join_idle(10))
        self.assertEqual(self.state_of("b"), TaskState.SUCCESS)
        b = sched._runtimes["b"]
        self.assertGreaterEqual(b.started_at, sched._runtimes["a"].started_at)

    def test_dag_cycle_rejected(self):
        specs = [make_spec("a", deps=["b"]), make_spec("b", deps=["a"])]
        with self.assertRaises(SpecValidationError):
            validate_specs(specs)

    def test_dangling_dep_rejected(self):
        with self.assertRaises(SpecValidationError):
            validate_specs([make_spec("a", deps=["ghost"])])


class TestLocksAndResources(SchedulerTestBase):
    def test_shared_partition_is_serial(self):
        # 两个任务写同一张表的相同分区 -> 锁键相同，必须串行
        specs = [
            TaskSpec("a", table="shared_tbl", partitions=["p1", "p2"],
                     partition_seconds=0.6, chunk_seconds=0.02),
            TaskSpec("b", table="shared_tbl", partitions=["p1", "p2"],
                     partition_seconds=0.6, chunk_seconds=0.02),
        ]
        sched = self.make(specs, slots=4, mem=8)
        saw_lock_wait = False
        deadline = time.time() + 3
        while time.time() < deadline:
            st = sched.status()
            if any(t["state"] == "WAITING_LOCK" for t in st["tasks"]):
                saw_lock_wait = True
                locks = st["locks"]
                self.assertEqual(len(locks["holders"]), 2)  # 两个分区都被 a 持有
                self.assertEqual(len(locks["waiters"]), 1)
                break
            time.sleep(0.01)
        self.assertTrue(saw_lock_wait, "冲突分区应出现 WAITING_LOCK 等待队列")
        self.assertTrue(sched.join_idle(10))
        self.assert_all({"a": TaskState.SUCCESS, "b": TaskState.SUCCESS})
        # 任意时刻同一分区最多一个持有者（锁快照里 holders 的 key 不重复）
        keys = [h["key"] for h in sched.locks.snapshot()["holders"]]
        self.assertEqual(len(keys), len(set(keys)))

    def test_different_tables_run_parallel(self):
        specs = [
            TaskSpec("a", table="table_a", partitions=["p1"],
                     partition_seconds=0.2, chunk_seconds=0.02),
            TaskSpec("b", table="table_b", partitions=["p1"],
                     partition_seconds=0.2, chunk_seconds=0.02),
        ]
        sched = self.make(specs, slots=2, mem=2)
        saw_parallel = False
        deadline = time.time() + 3
        while time.time() < deadline:
            if sum(1 for t in sched.status()["tasks"]
                   if t["state"] == "RUNNING") == 2:
                saw_parallel = True
                break
            time.sleep(0.02)
        self.assertTrue(saw_parallel, "不同表无锁冲突，应并行")
        self.assertTrue(sched.join_idle(10))

    def test_resource_gate_keeps_tasks_ready(self):
        specs = [
            make_spec("big_a", slots=3, mem=6, partition_seconds=0.25),
            make_spec("big_b", slots=3, mem=6, partition_seconds=0.25),
        ]
        sched = self.make(specs, slots=4, mem=8)
        saw_ready = False
        deadline = time.time() + 3
        while time.time() < deadline:
            for t in sched.status()["tasks"]:
                if t["state"] == "READY":
                    saw_ready = True
                    self.assertEqual(t["wait"]["type"], "resource")
            if saw_ready:
                break
            time.sleep(0.02)
        self.assertTrue(saw_ready, "预算放不下第二个任务时应处于 READY")
        self.assertTrue(sched.join_idle(10))
        self.assert_all({"big_a": TaskState.SUCCESS, "big_b": TaskState.SUCCESS})


class TestFailureRetryCheckpoint(SchedulerTestBase):
    def test_retry_resumes_from_checkpoint(self):
        specs = [make_spec("a", parts=("p1", "p2", "p3"),
                           fail_times=2, fail_kind="error",
                           max_attempts=3, backoff_base=0.05,
                           partition_seconds=0.06)]
        sched = self.make(specs)
        self.assertTrue(sched.join_idle(15))
        rt = sched._runtimes["a"]
        self.assertEqual(rt.state, TaskState.SUCCESS)
        self.assertEqual(rt.attempts, 3)
        # 每次失败至少完成一个分区，检查点单调推进；最终全部完成
        self.assertEqual(rt.checkpoint, 2)
        self.assertEqual(sorted(rt.completed_partitions), [0, 1, 2])
        # 检查点保证不重复装载：完成分区记录不重复
        self.assertEqual(len(rt.completed_partitions), 3)

    def test_failure_blocks_downstream_but_keeps_independent_success(self):
        specs = [
            make_spec("independent", partition_seconds=0.08),
            make_spec("flaky", fail_times=9, max_attempts=2,
                      backoff_base=0.02, partition_seconds=0.05),
            make_spec("down", deps=["flaky"], partition_seconds=0.05),
        ]
        sched = self.make(specs)
        self.assertTrue(sched.join_idle(15))
        self.assertEqual(self.state_of("independent"), TaskState.SUCCESS,
                         "不相关的已完成表必须保留")
        self.assertEqual(self.state_of("flaky"), TaskState.FAILED)
        self.assertEqual(self.state_of("down"), TaskState.BLOCKED)
        self.assertEqual(sched._runtimes["down"].wait["type"], "dependency")

        # 人工补齐失败任务，下游自动解除阻塞并完成
        sched.retry_task("flaky")
        self.assertTrue(sched.join_idle(15))
        self.assertEqual(self.state_of("flaky"), TaskState.SUCCESS)
        self.assertEqual(self.state_of("down"), TaskState.SUCCESS)
        self.assertTrue(sched._runtimes["flaky"].manual_recovered)

    def test_manual_retry_does_not_reinject(self):
        specs = [make_spec("a", fail_times=9, max_attempts=1,
                           partition_seconds=0.05)]
        sched = self.make(specs)
        self.assertTrue(sched.join_idle(10))
        self.assertEqual(self.state_of("a"), TaskState.FAILED)
        cp_before = sched._runtimes["a"].checkpoint
        sched.retry_task("a")
        self.assertTrue(sched.join_idle(10))
        rt = sched._runtimes["a"]
        self.assertEqual(rt.state, TaskState.SUCCESS)
        self.assertEqual(rt.attempts, 2)
        self.assertGreaterEqual(rt.checkpoint, cp_before)


class TestCancelAndTimeout(SchedulerTestBase):
    def test_cancel_running_task_blocks_downstream(self):
        specs = [
            make_spec("long", parts=("p1", "p2", "p3", "p4"),
                      partition_seconds=0.3, chunk_seconds=0.02),
            make_spec("down", deps=["long"], partition_seconds=0.05),
        ]
        sched = self.make(specs)
        # 等到至少完成一个分区（检查点落地）再取消，验证检查点能被保留
        deadline = time.time() + 3
        while time.time() < deadline:
            if sched._runtimes["long"].checkpoint >= 0:
                break
            time.sleep(0.01)
        self.assertEqual(self.state_of("long"), TaskState.RUNNING)
        sched.cancel_task("long")
        self.assertTrue(sched.join_idle(10))
        self.assertEqual(self.state_of("long"), TaskState.CANCELLED)
        self.assertEqual(self.state_of("down"), TaskState.BLOCKED)
        # 已完成的分区检查点保留
        self.assertGreaterEqual(sched._runtimes["long"].checkpoint, 0)
        # 资源与锁均已释放
        self.assertEqual(sched.resources.snapshot()["used_slots"], 0)
        self.assertEqual(len(sched.locks.snapshot()["holders"]), 0)

    def test_cancel_waiting_task(self):
        # 同表同分区：b 必然在 a 持锁期间进入 WAITING_LOCK
        specs = [
            TaskSpec("a", table="shared_tbl", partitions=["p1", "p2"],
                     partition_seconds=1.0, chunk_seconds=0.02),
            TaskSpec("b", table="shared_tbl", partitions=["p1", "p2"],
                     partition_seconds=1.2, chunk_seconds=0.02),
        ]
        sched = self.make(specs, slots=4, mem=8)
        deadline = time.time() + 3
        while time.time() < deadline:
            if self.state_of("b") == TaskState.WAITING_LOCK:
                break
            time.sleep(0.005)
        self.assertEqual(self.state_of("b"), TaskState.WAITING_LOCK)
        sched.cancel_task("b")
        self.assertEqual(self.state_of("b"), TaskState.CANCELLED)
        self.assertTrue(sched.join_idle(10))
        self.assertEqual(self.state_of("a"), TaskState.SUCCESS)

    def test_timeout_then_retry_succeeds(self):
        # 第一次尝试：3 个分区共 0.36s 工作量 > 0.3s 超时，必然在途中超时；
        # 退避后第二次尝试从检查点续跑，剩余工作量在超时内可完成
        specs = [make_spec("a", parts=("p1", "p2", "p3"),
                           timeout=0.3, max_attempts=2, fail_times=1,
                           fail_kind="timeout", backoff_base=0.05,
                           partition_seconds=0.12, chunk_seconds=0.02)]
        sched = self.make(specs)
        self.assertTrue(sched.join_idle(15))
        rt = sched._runtimes["a"]
        self.assertEqual(rt.state, TaskState.SUCCESS)
        self.assertEqual(rt.attempts, 2)
        # 第一次超时前已完成部分分区并落检查点，第二次从检查点续跑到全部完成
        self.assertEqual(rt.checkpoint, 2)
        self.assertEqual(sorted(rt.completed_partitions), [0, 1, 2])
        types = [e["type"] for e in sched.status()["events"]]
        self.assertIn("TIMEOUT_SIGNALLED", types)
        self.assertIn("TASK_RETRY", types)


class TestCrashRecovery(SchedulerTestBase):
    def test_snapshot_survives_restart_and_resumes(self):
        specs = [
            make_spec("a", parts=("p1", "p2", "p3", "p4"),
                      partition_seconds=0.3, chunk_seconds=0.02),
            make_spec("b", deps=["a"], partition_seconds=0.1),
        ]
        sched = self.make(specs)
        # 让 a 跑起来并完成第一个分区，随后立即崩溃，避免再跨分区
        deadline = time.time() + 3
        while time.time() < deadline:
            if sched._runtimes["a"].checkpoint == 0:
                break
            time.sleep(0.01)
        cp = sched._runtimes["a"].checkpoint
        attempts = sched._runtimes["a"].attempts
        gen = sched._generation
        self.assertEqual(cp, 0)

        sched.simulate_crash()
        self.assertTrue(Path(self.state_dir, "snapshot.json").exists())

        # 用全新的 Scheduler 实例模拟进程重启；先暂停以便核验恢复瞬间状态
        sched.shutdown()
        store2 = SnapshotStore(self.state_dir)
        sched2 = Scheduler(specs, total_slots=4, total_mem=8,
                           state_dir=self.state_dir, store=store2,
                           auto_start=False)
        self.sched = sched2
        self.assertEqual(sched2._generation, gen + 1)
        rt = sched2._runtimes["a"]
        self.assertGreaterEqual(rt.checkpoint, cp, "检查点必须从快照恢复")
        self.assertGreaterEqual(rt.attempts, attempts, "尝试次数必须保留不丢失")
        self.assertEqual(rt.state, TaskState.PENDING, "崩溃时在跑的任务回到待调度")
        # 核验完毕，显式恢复调度
        sched2.start()
        self.assertTrue(sched2.join_idle(15))
        self.assert_all({"a": TaskState.SUCCESS, "b": TaskState.SUCCESS})
        types = [e["type"] for e in sched2.status()["events"]]
        self.assertIn("SCHEDULER_RECOVERED", types)

    def test_restart_method_in_same_process(self):
        specs = [make_spec("a", partition_seconds=0.08)]
        sched = self.make(specs)
        self.assertTrue(sched.join_idle(10))
        gen_before = sched._generation
        sched.simulate_crash()
        sched.restart()
        self.assertEqual(sched._generation, gen_before + 1)
        # 已成功的任务保持成功，不会重跑
        self.assertEqual(self.state_of("a"), TaskState.SUCCESS)
        self.assertEqual(sched._runtimes["a"].attempts, 1)


class TestPauseAndValidation(SchedulerTestBase):
    def test_pause_stops_dispatch_but_workers_finish(self):
        specs = [make_spec("a", partition_seconds=0.25),
                 make_spec("b", partition_seconds=0.1)]
        sched = self.make(specs, slots=1, mem=1)
        time.sleep(0.05)
        sched.pause()
        # 暂停期间没有新任务启动：a 可在跑或已完成，b 绝不允许运行
        time.sleep(0.4)
        statuses = {t["task_id"]: t["state"] for t in sched.status()["tasks"]}
        self.assertIn(statuses["a"], ("RUNNING", "SUCCESS"))
        self.assertNotEqual(statuses["b"], "RUNNING")
        self.assertNotEqual(statuses["b"], "SUCCESS")
        sched.start()
        self.assertTrue(sched.join_idle(15))
        self.assert_all({"a": TaskState.SUCCESS, "b": TaskState.SUCCESS})

    def test_snapshot_file_is_valid_json(self):
        sched = self.make([make_spec("a")])
        self.assertTrue(sched.join_idle(10))
        data = SnapshotStore(self.state_dir).read_snapshot()
        self.assertEqual(data["version"], 1)
        self.assertIn("runtimes", data)
        self.assertEqual(data["runtimes"]["a"]["state"], "SUCCESS")


if __name__ == "__main__":
    unittest.main(verbosity=2)
