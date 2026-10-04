# 多站卫星地面站排程系统

使用标准库与 SQLite 实现的独立排程原型。系统维护卫星、地面站、天线、维护时段、可见窗口、租户配额和数据请求，并检查速率、数据量、截止时间、设备重叠、卫星同时接收、天气和租户配额。

## 运行

```bash
python3 app.py --db satellite_scheduling.db
```

默认监听 `127.0.0.1:8204`，首页 `/`，健康检查 `/health`。

身份头为 `X-User-Id`、`X-Role`；`requester` 还需 `X-Tenant`。角色：`viewer`、`requester`、`operator`、`commander`、`auditor`。

## 主要接口

- `POST /api/satellites`、`/api/stations`、`/api/antennas`、`/api/maintenance`、`/api/visibility-windows`、`/api/quotas`：资源配置。
- `POST /api/requests`：创建数据接收请求。
- `POST /api/requests/{id}/schedule`、`/reschedule`：排程或重排被抢占请求。
- `POST /api/schedules/{id}/start`、`/complete`、`/cancel`、`/preempt`：接收状态和紧急抢占。
- `POST /api/visibility-windows/{id}/change`：窗口变化并返回受影响排程；已接收数据保留。
- `GET /api/state`、`GET /api/schedules/{id}`：权限化状态查询。
- `GET /api/schedules/{id}/journal`：排程执行流水（按 seq 升序）。

## 执行流水与崩溃恢复

排程生命周期全部以 `schedule_journal` 流水为准，事件：`scheduled`、`started`、`completed`、`canceled`、`preempted`、`interrupted`、`window_changed`。

- **先记流水再改状态**：开始、完成、取消、抢占、窗口变化都在同一事务内先追加流水，再更新排程/请求状态；事务未提交则崩溃无痕。
- **操作单幂等**：排程/开始/完成/取消可带 `operation_id`，抢占使用 `order_id`；同一操作单（全局唯一）只认一次，重复返回 `operation_already_recorded`。重复完成（含不带单号的裸重试）不会把用量记两遍。
- **配额按流水计费**：`schedules.billed_seconds` 记录实际占用秒数；完成按计划时段结算，取消/抢占全额退还，中断只计已消费时段。
- **崩溃恢复**：服务启动时 `recover()` 按流水逐条重放。接收中（有 `started`）查不到完成/取消/抢占/中断记录的排程转为 `interrupted`，按实际经过秒数计费，未消费时段退还租户配额，请求回到 `pending`；恢复幂等。
- **窗口变化**：写进受影响排程同一条流水（`window_changed`）；越界排程再追加 `preempted`；已接收排程只追加事件，状态与数据保留。
- **旧数据升级**：无流水的历史排程按现有状态回填起始流水（`backfilled=1`）；接收中缺完成记录、抢占前状态补不齐的，置 `schedules.reconcile_status='pending'` 待核对。
- **分层**：流水判定（`_expected_status`/`recover`）、状态改动（各业务方法）、接口入口（`Handler`）相互独立。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

速率和容量按静态 Mbps 与时长计算，不包含链路预算、调制编码、雨衰、天线跟踪和存储卸载策略。租户身份使用请求头模拟；SQLite 和单进程 HTTP 服务适用于原型，生产环境需要统一身份、共享数据库和分布式资源锁。
