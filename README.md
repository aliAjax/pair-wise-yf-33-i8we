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
- `GET /api/schedules/{id}/journal`：查询排程的执行流水。
- `GET /api/state`、`GET /api/schedules/{id}`：权限化状态查询。

## 执行流水与崩溃恢复

接收过程通过 `schedule_journal` 表记录追加式流水，流水判定、状态改动、接口入口三层分离（`journal.py` / `app.py` / `Handler`）。

- **先记流水再改状态**：排程开始、完成、取消、抢占、窗口变化都在同一事务内先落流水、再改状态。
- **操作单幂等**：每个操作可带 `op_key`（操作单号）；同一 `op_key` 只认一次，重复提交原样返回，重复完成不会把用量记两遍。未带 `op_key` 时按排程与事件派生唯一键。
- **崩溃恢复**：服务启动时自动 `recover()`——按流水逐条重放状态；接收中但查不到完成记录的排程转成 `interrupted`，未消费的时段不计入租户占用（配额自动归还），请求回到 `pending` 可重新排程。
- **窗口变化**：写进同一条流水（父流水 + 每条受影响排程一条）；已接收数据保留，状态不回滚。
- **旧数据兼容**：升级时对没有流水的排程按现有状态回填起始流水，全部标记 `needs_review`；状态推断不出的只补起始条并记入待核对。

恢复报告会在启动时打印，例如：

```
recovery report: {"backfill": {...}, "reconcile": {"interrupted": [{"schedule_id": 1, "request_id": 1}], ...}}
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

速率和容量按静态 Mbps 与时长计算，不包含链路预算、调制编码、雨衰、天线跟踪和存储卸载策略。租户身份使用请求头模拟；SQLite 和单进程 HTTP 服务适用于原型，生产环境需要统一身份、共享数据库和分布式资源锁。
