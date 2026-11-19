# 多表增量装载协调器

一个带 DAG 依赖、共享分区锁和资源预算的多表增量装载调度器，纯 Python 标准库实现（后端 + 原生 HTML/JS 控制台，零第三方依赖）。

## 核心能力

| 需求 | 实现 |
|------|------|
| 任务依赖（DAG） | 拓扑排序 + 依赖门控，下游等所有上游 `SUCCESS` 才调度；环/悬空依赖启动即拒绝 |
| 不相关表并行 | 无依赖且资源足够的任务并发执行（线程池式 worker） |
| 冲突分区串行 | 锁键 `表/分区` 的排他锁 + FIFO 等待队列；按分区键全局排序加锁，机制上无死锁 |
| 资源上限 | 全局槽位 + 内存预算，任务整体预留/整体释放；不足时进入 `READY` 等待 |
| 失败保留独立表 | 某任务失败只影响其传递下游（`BLOCKED`），不相关的已完成表保持 `SUCCESS` |
| 下游等待补齐 | 失败任务重试耗尽转 `FAILED`，下游转 `BLOCKED`；人工补齐后下游自动解除阻塞 |
| 失败重试 | 指数退避自动重试，从**检查点**（已完成的最大分区）续跑，不重复装载 |
| 恢复点 | 每完成一个分区即落检查点并持久化到快照 |
| 取消 | 协作式中断，工作线程在 chunk 边界响应；运行中/等待锁/待调度均可取消 |
| 超时 | 每任务单次尝试截止时间，chunk 粒度检测；超时按失败处理并可重试 |
| 调度器重启 | 原子快照（tmp+fsync+rename）+ 追加事件日志；崩溃后新进程读快照恢复，在跑任务回到待调度、按检查点续跑 |

## 状态机

```
PENDING ──依赖满足+资源不足──> READY
   │  └──依赖满足+锁冲突──> WAITING_LOCK
   └──三门全过──> RUNNING ──成功──> SUCCESS
                    │
                    ├──失败(可重试)──> RETRY_WAIT ──退避结束──> PENDING
                    ├──失败(耗尽)──> FAILED
                    ├──取消──> CANCELLED
                    └──超时──> (按失败处理)
上游 FAILED/CANCELLED/BLOCKED ──> 下游 BLOCKED ──上游补齐──> PENDING
```

## 目录结构

```
coordinator/
  model.py        # TaskSpec/TaskRuntime 状态机、DAG 校验（Kahn 拓扑）
  locks.py        # 共享分区排他锁 + 等待队列
  resources.py    # 槽位/内存预算
  persistence.py  # 原子快照 + JSONL 事件日志
  scheduler.py    # 调度核心：三重门控、重试、超时、取消、崩溃恢复
  server.py       # 标准库 HTTP API + SSE + 静态页
  config.py       # 配置加载
  main.py         # 命令行入口
  static/         # 控制台（index.html / app.js / styles.css）
tests/
  test_scheduler.py   # 17 个单测：并行/锁/资源/重试/检查点/取消/超时/崩溃/暂停
  test_http_e2e.py    # 6 个端到端：真实 HTTP 服务全链路
config.json       # 演示装载配置（10 个任务、4 槽位/8 内存）
```

## 运行

```bash
python3 -m coordinator.main --config config.json --state-dir .state --port 8000 --fresh
# 打开 http://127.0.0.1:8000
```

- `--fresh`：忽略旧快照重新开始；不带该参数则从 `.state/snapshot.json` 恢复。
- 状态目录：`snapshot.json`（崩溃恢复快照）、`journal.jsonl`（事件审计流水）。

## 测试

```bash
python3 -m unittest tests.test_scheduler tests.test_http_e2e -v
```

## HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/status` | 全量状态：DAG、资源、锁、任务、事件 |
| GET | `/api/events` | SSE 实时事件流 |
| POST | `/api/scheduler/start` `/pause` `/restart` `/reset` | 继续/暂停/崩溃重启/重新装载 |
| POST | `/api/tasks/{id}/cancel` | 取消任务 |
| POST | `/api/tasks/{id}/retry` | 人工补齐（从检查点续跑，关闭自动故障注入） |
| POST | `/api/tasks/{id}/inject` | 对运行中任务手动注入故障 |

## 控制台展示

- **装载 DAG**：SVG 分层布局，节点按状态着色，边在失败时变红虚线、成功时变绿，含每任务进度条与当前分区。
- **资源占用**：槽位/内存实时用量条（≥80% 警戒、100% 满载变红）与各任务占用明细。
- **分区锁/等待队列**：每个锁键的持有者，以及等待方、被谁阻塞、已等待时长。
- **任务与恢复点**：状态、进度、尝试次数、检查点分区、等待原因、错误/重试倒计时，行内操作按钮。
- **事件时间线**：SSE 实时推送的完整事件流（分区开始/完成、重试、超时、取消、崩溃、恢复等）。

## 演示配置中的故障注入

- `dwd_orders_clean`：前 2 次尝试自动报错，第 3 次成功（演示错误重试 + 检查点续跑）。
- `audit_reconcile`：第 1 次尝试注入超时，第 2 次成功（演示超时重试）。
- 也可在页面上对任意运行中任务手动「注入故障」，或将任务重试次数调到 1 观察 `FAILED` → 下游 `BLOCKED` → 「人工补齐」闭环。
