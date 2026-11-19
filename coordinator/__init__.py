"""多表增量装载协调器（incremental-load coordinator）。

组件一览：
- model:       任务规格 / 运行时状态 / DAG 校验
- locks:       共享分区排他锁 + 等待队列
- resources:   槽位 / 内存预算管理
- persistence: 原子快照 + 追加式事件日志
- scheduler:   调度核心（依赖、锁、资源、重试、超时、取消、重启恢复）
- server:      标准库 HTTP API + 静态页面
"""

__version__ = "1.0.0"
