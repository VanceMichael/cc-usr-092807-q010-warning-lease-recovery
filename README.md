# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 目录

- `src/civicflow/`：领域服务、SQLite 持久化、权限和命令行入口。
- `tests/`：核心流程、边界条件和异常路径测试。
- `examples/`：本地演示输入。

## 配置

通过 `CIVICFLOW_DB` 指定 SQLite 文件路径；不设置时命令行使用当前目录下的 `civicflow.sqlite3`。所有时间使用带时区的 ISO 8601 字符串。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译或构建

```bash
PYTHONPATH=src python3 -m compileall -q src
```

## 使用

初始化数据库并运行离线演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 demo
```

查看当前案件：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 list-cases
```

## 通知发件箱与定时任务的租约语义

工作进程通过 `outbox.lease(owner=...)` / `jobs.claim_due(owner=...)` 领取消息和任务。领取时持久化 `lease_owner` 与单调递增的 `lease_version`（fencing 令牌）：

- 领取后崩溃的进程不会卡死消息：`leased`/`running` 状态的记录在 `lease_until` 过期后会被其他进程安全回收重领。
- `complete`/`fail`/`renew`（发件箱）与 `finish`/`retry`/`renew`（任务队列）必须携带当前租约的 `owner` 与 `lease_version`，且租约未过期；旧进程迟到的确认会因版本不匹配被拒绝，不会覆盖新进程的处理结果。
- `lease_until` 为 NULL 表示当前没有有效租约；`lease_owner` 保留最后一次领取人，便于值守界面追溯。
- 失败重试保持原有语义：发件箱 `attempts`、任务 `attempt` 达到 5 次后进入 `dead`/`failed` 终态，不再被领取；`last_error` 记录最近一次失败原因。
- 既有数据库在 `CivicFlow.open` 时自动就地升级（新增 `lease_owner`、`lease_version`、`last_error` 列），已有消息与任务语义不变，卡死记录随租约到期即可回收。

值守人员可用命令查询一条预警是否仍会送达（`will_deliver`）、由谁处理（`lease_owner`/`lease_active`）以及为何重试（`last_error`/`attempts`）：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 outbox-inspect <message_id>
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 job-inspect <job_id>
```
