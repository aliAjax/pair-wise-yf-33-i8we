#!/usr/bin/env python3
"""Multi-station satellite scheduling service using only Python's standard library."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PORT = 8204
ROLES = {"viewer", "requester", "operator", "commander", "auditor"}

# 排程终态与流水事件：流水判定只读这两张映射，状态改动与接口入口各自独立。
SCHEDULE_STATES = {"scheduled", "receiving", "received", "canceled", "preempted", "interrupted"}
JOURNAL_EVENTS = {"scheduled", "started", "completed", "canceled", "preempted", "interrupted", "window_changed"}
# 事件重放后的期望状态；window_changed 不改状态（在判定时跳过）。
EVENT_STATUS = {"scheduled": "scheduled", "started": "receiving", "completed": "received", "canceled": "canceled", "preempted": "preempted", "interrupted": "interrupted"}
# 各状态占用的租户配额：interrupted 只计流水里实际消费的 billed_seconds。
QUOTA_STATES = ("scheduled", "receiving", "received", "interrupted")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message); self.status, self.code, self.message, self.details = status, code, message, details


def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso(value: datetime | None = None) -> str: return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def parse_time(value: str | None) -> datetime:
    if not value: raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool: return a_start < b_end and b_start < a_end


class Repository:
    def __init__(self, path: str | Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row; self.conn.execute("PRAGMA foreign_keys=ON"); self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS satellites(id TEXT PRIMARY KEY, name TEXT NOT NULL, data_rate_mbps REAL NOT NULL, priority INTEGER NOT NULL, storage_capacity_mb REAL NOT NULL, tenant TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS stations(id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', weather TEXT NOT NULL DEFAULT 'clear');
        CREATE TABLE IF NOT EXISTS antennas(id TEXT PRIMARY KEY, station_id TEXT NOT NULL REFERENCES stations(id), max_rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS maintenance(id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT REFERENCES antennas(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, reason TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS visibility_windows(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), station_id TEXT NOT NULL REFERENCES stations(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, max_rate_mbps REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), tenant TEXT NOT NULL, priority INTEGER NOT NULL, data_mb REAL NOT NULL, deadline TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_by TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS quotas(id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, station_id TEXT NOT NULL REFERENCES stations(id), daily_seconds INTEGER NOT NULL, UNIQUE(tenant,station_id));
        CREATE TABLE IF NOT EXISTS schedules(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL UNIQUE REFERENCES requests(id), window_id INTEGER NOT NULL REFERENCES visibility_windows(id), station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT NOT NULL REFERENCES antennas(id), satellite_id TEXT NOT NULL REFERENCES satellites(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'scheduled', revision INTEGER NOT NULL DEFAULT 1, disposition_reason TEXT, billed_seconds INTEGER NOT NULL DEFAULT 0, reconcile_status TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER, schedule_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS schedule_journal(id INTEGER PRIMARY KEY AUTOINCREMENT, schedule_id INTEGER NOT NULL REFERENCES schedules(id), request_id INTEGER NOT NULL, seq INTEGER NOT NULL, event TEXT NOT NULL, from_status TEXT, to_status TEXT, operation_id TEXT, actor TEXT NOT NULL, role TEXT NOT NULL, billed_seconds INTEGER, backfilled INTEGER NOT NULL DEFAULT 0, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_journal_operation ON schedule_journal(operation_id) WHERE operation_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_journal_schedule ON schedule_journal(schedule_id, seq);
        """)
        self._migrate()

    def _migrate(self) -> None:
        # 旧库没有流水与计费列：补列后由服务层 recover() 回填起始流水。
        for ddl in ("ALTER TABLE schedules ADD COLUMN billed_seconds INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE schedules ADD COLUMN reconcile_status TEXT"):
            try: self.conn.execute(ddl)
            except sqlite3.OperationalError: pass

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try: yield self.conn; self.conn.execute("COMMIT")
        except Exception: self.conn.execute("ROLLBACK"); raise

    @staticmethod
    def audit(conn: sqlite3.Connection, request_id: int | None, schedule_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(request_id,schedule_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                     (request_id, schedule_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class SatelliteSchedulingService:
    def __init__(self, path: str | Path, recover_on_start: bool = True):
        self.repo = Repository(path); self.recovery_report: dict[str, Any] | None = None
        if recover_on_start: self.recover()

    # ---------- 流水判定层：只从流水推导事实，不直接改状态 ----------
    @staticmethod
    def _next_seq(conn: sqlite3.Connection, schedule_id: int) -> int:
        row = conn.execute("SELECT COALESCE(MAX(seq),0)+1 FROM schedule_journal WHERE schedule_id=?", (schedule_id,)).fetchone()
        return int(row[0])

    @staticmethod
    def _ensure_fresh_operation(conn: sqlite3.Connection, operation_id: str | None) -> None:
        # 操作单预检：同一张单第二次提交直接拒绝；schedule_journal 的唯一索引兜底并发。
        if operation_id and conn.execute("SELECT 1 FROM schedule_journal WHERE operation_id=?", (operation_id,)).fetchone():
            raise ApiError(409, "operation_already_recorded", "同一张操作单只认一次，请勿重复提交", {"operation_id": operation_id})

    @staticmethod
    def _expected_status(events: list[sqlite3.Row]) -> tuple[str | None, bool]:
        """按 seq 重放流水，返回(期望终态, 是否有过开始接收)。window_changed 不参与状态判定。"""
        expected, started = None, False
        for ev in events:
            if ev["event"] == "started": started = True
            if ev["event"] == "window_changed": continue
            expected = EVENT_STATUS[ev["event"]]
        return expected, started

    @staticmethod
    def _elapsed_seconds(row: sqlite3.Row, at: datetime) -> int:
        start, end = parse_time(row["starts_at"]), parse_time(row["ends_at"])
        return max(0, min(int((end - start).total_seconds()), int((at - start).total_seconds())))

    def _journal(self, conn: sqlite3.Connection, row: sqlite3.Row, event: str, actor: str, role: str, *,
                 to_status: str | None = None, operation_id: str | None = None, billed_seconds: int | None = None,
                 backfilled: bool = False, detail: dict[str, Any] | None = None) -> int:
        """先写流水：所有生命周期事件的唯一落库入口，operation_id 重复时拒绝（同一张操作单只认一次）。"""
        seq = self._next_seq(conn, row["id"])
        try:
            cur = conn.execute("""INSERT INTO schedule_journal(schedule_id,request_id,seq,event,from_status,to_status,operation_id,actor,role,billed_seconds,backfilled,detail_json,created_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                               (row["id"], row["request_id"], seq, event, row["status"], to_status, operation_id, actor, role, billed_seconds, 1 if backfilled else 0, json.dumps(detail or {}, ensure_ascii=False, sort_keys=True), iso()))
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "operation_already_recorded", "同一张操作单只认一次，请勿重复提交", {"operation_id": operation_id}) from exc
        return int(cur.lastrowid)

    def _backfill_legacy(self, conn: sqlite3.Connection, row: sqlite3.Row) -> list[sqlite3.Row]:
        """升级兼容：旧排程没有流水，按现有状态回填一条起始流水链；补不齐的留待 recover 标记待核对。"""
        duration = int((parse_time(row["ends_at"]) - parse_time(row["starts_at"])).total_seconds())
        legacy_billed = {"scheduled": duration, "receiving": duration, "received": duration, "canceled": 0, "preempted": 0}
        chain: list[tuple[str, str | None, int | None, bool, dict[str, Any]]]
        pending = False
        if row["status"] == "scheduled":
            chain = [("scheduled", "scheduled", duration, False, {"legacy": "按现有状态回填起始流水"})]
        elif row["status"] == "receiving":
            # 有开始无完成记录，补不齐实际用量，先记全额，恢复时转中断并挂待核对。
            chain = [("scheduled", "scheduled", duration, False, {"legacy": "按现有状态回填起始流水"}),
                     ("started", "receiving", duration, False, {"legacy": "接收中无流水，实际用量待核对"})]
            pending = True
        elif row["status"] == "received":
            chain = [("scheduled", "scheduled", duration, False, {"legacy": "按现有状态回填起始流水"}),
                     ("started", "receiving", duration, False, {"legacy": "补回填"}),
                     ("completed", "received", duration, False, {"legacy": "完成时间补不齐，按计划时长回填用量"})]
        elif row["status"] == "canceled":
            chain = [("scheduled", "scheduled", 0, False, {"legacy": "按现有状态回填起始流水"}),
                     ("canceled", "canceled", 0, False, {"legacy": "取消时间与原因补不齐", "reason": row["disposition_reason"]})]
        else:
            # preempted：抢占前是 scheduled 还是 receiving 无法判定，补不齐，记待核对。
            chain = [("scheduled", "scheduled", duration, False, {"legacy": "按现有状态回填起始流水"}),
                     ("preempted", "preempted", 0, True, {"legacy": "抢占前状态补不齐，记待核对", "reason": row["disposition_reason"]})]
            pending = True
        billed_total = legacy_billed.get(row["status"], duration)
        for event, to_status, billed, _mark, detail in chain:
            self._journal(conn, row, event, "system-migration", "system", to_status=to_status, billed_seconds=billed, backfilled=True, detail=detail)
        conn.execute("UPDATE schedules SET billed_seconds=?, reconcile_status=? WHERE id=?",
                     (billed_total, "pending" if pending else None, row["id"]))
        return conn.execute("SELECT * FROM schedule_journal WHERE schedule_id=? ORDER BY seq", (row["id"],)).fetchall()

    def _interrupt_crashed(self, conn: sqlite3.Connection, row: sqlite3.Row, events: list[sqlite3.Row], legacy: bool) -> None:
        """接收中查不到完成记录：转 interrupted，未消费时段退还租户配额。"""
        duration = int((parse_time(row["ends_at"]) - parse_time(row["starts_at"])).total_seconds())
        charged = self._elapsed_seconds(row, utcnow())
        note = "升级旧数据：接收中缺完成记录，实际用量补不齐，按已过时段估算待核对" if legacy else "服务恢复：接收中无完成记录，按崩溃处理"
        self._journal(conn, row, "interrupted", "system-recovery", "system", to_status="interrupted", billed_seconds=charged,
                      backfilled=legacy, detail={"reason": "crash_recovery_no_completion", "planned_seconds": duration, "charged_seconds": charged, "released_seconds": max(0, duration - charged), "note": note})
        conn.execute("""UPDATE schedules SET status='interrupted',billed_seconds=?,reconcile_status=COALESCE(reconcile_status,?),
                        disposition_reason=COALESCE(disposition_reason,'crash_recovery_no_completion'),revision=revision+1,updated_at=? WHERE id=?""",
                     (charged, "pending" if legacy else None, iso(), row["id"]))
        conn.execute("UPDATE requests SET status='pending' WHERE id=?", (row["request_id"],))
        Repository.audit(conn, row["request_id"], row["id"], "system-recovery", "system", "crash_interrupted",
                         {"planned_seconds": duration, "charged_seconds": charged, "released_seconds": max(0, duration - charged), "reconcile_pending": legacy})

    def recover(self) -> dict[str, Any]:
        """服务启动恢复：逐条重放流水，旧数据先回填；状态与流水不一致时以流水为准修复。"""
        report: dict[str, Any] = {"backfilled": [], "interrupted": [], "repaired": []}
        with self.repo.tx() as conn:
            schedules = conn.execute("SELECT * FROM schedules ORDER BY id").fetchall()
            for srow in schedules:
                events = conn.execute("SELECT * FROM schedule_journal WHERE schedule_id=? ORDER BY seq", (srow["id"],)).fetchall()
                legacy = False
                if not events:
                    events = self._backfill_legacy(conn, srow); legacy = True; report["backfilled"].append(srow["id"])
                expected, started = self._expected_status(events)
                if srow["status"] == "receiving" and expected == "receiving":
                    # 有 started 流水但没有完成/取消/抢占/中断等终态记录 —— 上次服务在接收中崩溃。
                    self._interrupt_crashed(conn, srow, events, legacy); report["interrupted"].append(srow["id"]); continue
                if expected and srow["status"] != expected:
                    duration = int((parse_time(srow["ends_at"]) - parse_time(srow["starts_at"])).total_seconds())
                    billed = duration if expected in ("scheduled", "receiving") else srow["billed_seconds"] if expected == "interrupted" else 0
                    conn.execute("UPDATE schedules SET status=?,billed_seconds=?,revision=revision+1,updated_at=? WHERE id=?", (expected, billed, iso(), srow["id"]))
                    req_status = "received" if expected == "received" else "scheduled" if expected in ("scheduled", "receiving") else "preempted" if expected == "preempted" else "pending"
                    conn.execute("UPDATE requests SET status=? WHERE id=?", (req_status, srow["request_id"]))
                    report["repaired"].append({"schedule_id": srow["id"], "from_status": srow["status"], "to_status": expected})
        self.recovery_report = report; return report
    # ---------- 流水判定层结束 ----------

    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor, role, tenant = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip(), headers.get("X-Tenant", "").strip()
        if not actor or role not in ROLES: raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        if role == "requester" and not tenant: raise ApiError(401, "tenant_required", "requester 必须提供 X-Tenant")
        return actor, role, tenant

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None: return dict(row) if row else None

    def create_satellite(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护卫星")
        sid, name, tenant = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("tenant", "")).strip()
        rate, priority, capacity = body.get("data_rate_mbps"), body.get("priority"), body.get("storage_capacity_mb")
        if not sid or not name or not tenant or not isinstance(rate, (int, float)) or float(rate) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10 or not isinstance(capacity, (int, float)) or float(capacity) <= 0:
            raise ApiError(400, "invalid_satellite", "卫星参数不完整")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO satellites(id,name,data_rate_mbps,priority,storage_capacity_mb,tenant,status) VALUES(?,?,?,?,?,?,?)", (sid, name, float(rate), priority, float(capacity), tenant, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM satellites WHERE id=?", (sid,)).fetchone())

    def create_station(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护地面站")
        sid, name = str(body.get("id", "")).strip(), str(body.get("name", "")).strip()
        weather = str(body.get("weather", "clear")).lower()
        if not sid or not name or weather not in {"clear", "rain", "storm", "closed"}: raise ApiError(400, "invalid_station", "地面站名称或天气状态无效")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO stations(id,name,status,weather) VALUES(?,?,?,?)", (sid, name, body.get("status", "active"), weather))
            return dict(conn.execute("SELECT * FROM stations WHERE id=?", (sid,)).fetchone())

    def create_antenna(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护天线")
        aid, station, rate = str(body.get("id", "")).strip(), str(body.get("station_id", "")).strip(), body.get("max_rate_mbps")
        if not aid or not station or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_antenna", "天线参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT OR REPLACE INTO antennas(id,station_id,max_rate_mbps,status) VALUES(?,?,?,?)", (aid, station, float(rate), body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM antennas WHERE id=?", (aid,)).fetchone())

    def create_maintenance(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "maintenance_forbidden", "当前角色不能登记维护")
        station, start, end, reason = str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), str(body.get("reason", "")).strip()
        antenna = body.get("antenna_id")
        if not station or end <= start or not reason: raise ApiError(400, "invalid_maintenance", "维护参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            if antenna and not conn.execute("SELECT 1 FROM antennas WHERE id=? AND station_id=?", (antenna, station)).fetchone(): raise ApiError(400, "antenna_station_mismatch", "天线不属于该站")
            cur = conn.execute("INSERT INTO maintenance(station_id,antenna_id,starts_at,ends_at,reason) VALUES(?,?,?,?,?)", (station, antenna, iso(start), iso(end), reason))
            return dict(conn.execute("SELECT * FROM maintenance WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_window(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能维护可见窗口")
        satellite, station, start, end, rate = str(body.get("satellite_id", "")).strip(), str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("max_rate_mbps")
        if not satellite or not station or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_window", "可见窗口参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM satellites WHERE id=?", (satellite,)).fetchone(): raise ApiError(404, "satellite_not_found", "卫星不存在")
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            cur = conn.execute("INSERT INTO visibility_windows(satellite_id,station_id,starts_at,ends_at,max_rate_mbps) VALUES(?,?,?,?,?)", (satellite, station, iso(start), iso(end), float(rate)))
            return dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (cur.lastrowid,)).fetchone())

    def set_quota(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "quota_forbidden", "当前角色不能设置租户配额")
        tenant, station, seconds = str(body.get("tenant", "")).strip(), str(body.get("station_id", "")).strip(), body.get("daily_seconds")
        if not tenant or not station or not isinstance(seconds, int) or seconds <= 0: raise ApiError(400, "invalid_quota", "配额参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT INTO quotas(tenant,station_id,daily_seconds) VALUES(?,?,?) ON CONFLICT(tenant,station_id) DO UPDATE SET daily_seconds=excluded.daily_seconds", (tenant, station, seconds))
            return dict(conn.execute("SELECT * FROM quotas WHERE tenant=? AND station_id=?", (tenant, station)).fetchone())

    def create_request(self, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"requester", "operator", "commander"}: raise ApiError(403, "request_forbidden", "当前角色不能创建数据请求")
        satellite = str(body.get("satellite_id", "")).strip(); data_mb = body.get("data_mb"); priority = body.get("priority"); deadline = parse_time(body.get("deadline"))
        owner = tenant if role == "requester" else str(body.get("tenant", "")).strip()
        if not satellite or not owner or not isinstance(data_mb, (int, float)) or float(data_mb) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10:
            raise ApiError(400, "invalid_request", "请求参数无效")
        with self.repo.tx() as conn:
            satellite_row = conn.execute("SELECT * FROM satellites WHERE id=?", (satellite,)).fetchone()
            if not satellite_row: raise ApiError(404, "satellite_not_found", "卫星不存在")
            if owner != satellite_row["tenant"]: raise ApiError(403, "tenant_satellite_forbidden", "租户不能申请不属于自己的卫星")
            if deadline <= utcnow(): raise ApiError(409, "deadline_expired", "请求截止时间已经过去")
            cur = conn.execute("INSERT INTO requests(satellite_id,tenant,priority,data_mb,deadline,created_by,created_at) VALUES(?,?,?,?,?,?,?)", (satellite, owner, priority, float(data_mb), iso(deadline), actor, iso()))
            request_id = cur.lastrowid; Repository.audit(conn, request_id, None, actor, role, "request_created", {"data_mb": float(data_mb), "deadline": iso(deadline)})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def _used_quota(self, conn: sqlite3.Connection, tenant: str, station: str, day: str, exclude_schedule: int | None = None) -> int:
        states = ",".join("?" for _ in QUOTA_STATES)
        sql = f"""SELECT COALESCE(SUM(s.billed_seconds),0) FROM schedules s JOIN requests r ON r.id=s.request_id
                 WHERE r.tenant=? AND s.station_id=? AND substr(s.starts_at,1,10)=? AND s.status IN ({states})"""
        args: list[Any] = [tenant, station, day, *QUOTA_STATES]
        if exclude_schedule is not None: sql += " AND s.id!=?"; args.append(exclude_schedule)
        return int(conn.execute(sql, args).fetchone()[0])

    def schedule_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排接收")
        window_id, antenna_id = body.get("window_id"), str(body.get("antenna_id", "")).strip()
        start, end, rate = parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("rate_mbps")
        if not isinstance(window_id, int) or not antenna_id or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_schedule", "排程参数无效")
        operation_id = str(body.get("operation_id", "")).strip() or None
        with self.repo.tx() as conn:
            self._ensure_fresh_operation(conn, operation_id)
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
            if not request or not window or not antenna: raise ApiError(404, "schedule_ref_not_found", "请求、窗口或天线不存在")
            if request["status"] not in {"pending", "preempted"}: raise ApiError(409, "request_closed", "请求当前不能排程")
            if request["satellite_id"] != window["satellite_id"] or window["station_id"] != antenna["station_id"]: raise ApiError(409, "window_mismatch", "卫星、窗口和天线不匹配")
            station = conn.execute("SELECT * FROM stations WHERE id=?", (window["station_id"],)).fetchone()
            satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
            if satellite["status"] != "active" or station["status"] != "active" or antenna["status"] != "active": raise ApiError(409, "resource_inactive", "卫星、地面站或天线不可用")
            if station["weather"] != "clear": raise ApiError(409, "weather_blocked", "天气条件不允许接收")
            w_start, w_end = parse_time(window["starts_at"]), parse_time(window["ends_at"])
            if start < w_start or end > w_end: raise ApiError(409, "outside_visibility", "排程超出可见窗口")
            if end > parse_time(request["deadline"]): raise ApiError(409, "deadline_missed", "预计结束时间超过请求截止时间")
            max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
            if float(rate) > max_rate: raise ApiError(409, "rate_exceeded", "请求速率超过可用上限", {"max_rate_mbps": max_rate})
            transferred = (end - start).total_seconds() * float(rate) / 8
            if transferred < float(request["data_mb"]): raise ApiError(409, "insufficient_capacity", "窗口内可接收数据量不足", {"capacity_mb": transferred, "required_mb": request["data_mb"]})
            maintenance = conn.execute("""SELECT * FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?""", (station["id"], antenna_id, iso(end), iso(start))).fetchone()
            if maintenance: raise ApiError(409, "maintenance_conflict", "天线或地面站处于维护期", dict(maintenance))
            equipment = conn.execute("SELECT id,status FROM schedules WHERE station_id=? AND antenna_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (station["id"], antenna_id, iso(end), iso(start))).fetchone()
            if equipment: raise ApiError(409, "antenna_conflict", "天线时段已被占用", {"schedule_id": equipment["id"]})
            satellite_conflict = conn.execute("SELECT id,status FROM schedules WHERE satellite_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (request["satellite_id"], iso(end), iso(start))).fetchone()
            if satellite_conflict: raise ApiError(409, "satellite_conflict", "同一卫星时段已被其他站接收", {"schedule_id": satellite_conflict["id"]})
            quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station["id"])).fetchone()
            used = self._used_quota(conn, request["tenant"], station["id"], start.date().isoformat())
            duration = int((end - start).total_seconds())
            if quota and used + duration > quota["daily_seconds"]: raise ApiError(409, "tenant_quota_exceeded", "租户当日地面站配额不足", {"used_seconds": used, "requested_seconds": duration, "limit": quota["daily_seconds"]})
            cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,billed_seconds,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                               (request_id, window_id, station["id"], antenna_id, request["satellite_id"], iso(start), iso(end), float(rate), duration, actor, iso(), iso()))
            schedule_id = cur.lastrowid
            srow = conn.execute("SELECT * FROM schedules WHERE id=?", (schedule_id,)).fetchone()
            self._journal(conn, srow, "scheduled", actor, role, to_status="scheduled", operation_id=operation_id, billed_seconds=duration,
                          detail={"window_id": window_id, "antenna_id": antenna_id, "capacity_mb": transferred})
            conn.execute("UPDATE requests SET status='scheduled' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, schedule_id, actor, role, "schedule_created", {"window_id": window_id, "antenna_id": antenna_id, "capacity_mb": transferred})
            return self.get_schedule(schedule_id)

    def get_schedule(self, schedule_id: int) -> dict[str, Any]:
        row = self.repo.conn.execute("""SELECT s.*,r.tenant,r.data_mb,r.priority request_priority,r.deadline FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
        if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
        return dict(row)

    def transition(self, schedule_id: int, actor: str, role: str, tenant: str, target: str, body: dict[str, Any]) -> dict[str, Any]:
        if target not in {"receiving", "received"}: raise ApiError(400, "invalid_transition", "未知状态")
        operation_id = str(body.get("operation_id", "")).strip() or None
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant,r.status request_status FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能执行该接收操作")
            self._ensure_fresh_operation(conn, operation_id)
            duration = int((parse_time(row["ends_at"]) - parse_time(row["starts_at"])).total_seconds())
            if target == "receiving":
                if row["status"] != "scheduled": raise ApiError(409, "invalid_transition", "只有已排程任务可以开始接收")
                self._journal(conn, row, "started", actor, role, to_status="receiving", operation_id=operation_id, billed_seconds=duration, detail={})
                conn.execute("UPDATE schedules SET status='receiving',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
                Repository.audit(conn, row["request_id"], schedule_id, actor, role, "receive_receiving", {})
            else:
                if row["status"] == "received": raise ApiError(409, "already_completed", "排程已完成，重复完成不会把用量记两遍")
                if row["status"] != "receiving": raise ApiError(409, "invalid_transition", "只有接收中任务可以完成")
                # 完成流水先落库：按计划时段结算用量，之后再改状态。
                self._journal(conn, row, "completed", actor, role, to_status="received", operation_id=operation_id, billed_seconds=duration, detail={"billed_seconds": duration})
                conn.execute("UPDATE schedules SET status='received',billed_seconds=?,revision=revision+1,updated_at=? WHERE id=?", (duration, iso(), schedule_id))
                conn.execute("UPDATE requests SET status='received' WHERE id=?", (row["request_id"],))
                Repository.audit(conn, row["request_id"], schedule_id, actor, role, "receive_received", {"billed_seconds": duration})
            return self.get_schedule(schedule_id)

    def cancel_schedule(self, schedule_id: int, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        operation_id = str(body.get("operation_id", "")).strip() or None
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if role == "requester":
                if row["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能取消其他租户排程")
                if row["status"] != "scheduled": raise ApiError(409, "cancel_not_allowed", "接收开始后租户不能取消")
            elif role not in {"operator", "commander"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消排程")
            self._ensure_fresh_operation(conn, operation_id)
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据不能取消或删除")
            if row["status"] == "canceled": return self.get_schedule(schedule_id)
            # 取消先记流水（未消费时段全部退还配额），再改排程与请求状态。
            self._journal(conn, row, "canceled", actor, role, to_status="canceled", operation_id=operation_id, billed_seconds=0, detail={"reason": reason})
            conn.execute("UPDATE schedules SET status='canceled',billed_seconds=0,disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), schedule_id))
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (row["request_id"],))
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "schedule_canceled", {"reason": reason})
            return self.get_schedule(schedule_id)

    def emergency_preempt(self, schedule_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "commander": raise ApiError(403, "commander_required", "只有任务指挥官可以执行紧急抢占")
        order_id, reason = str(body.get("order_id", "")).strip(), str(body.get("reason", "")).strip()
        if not order_id or not reason: raise ApiError(400, "emergency_details_required", "order_id 和 reason 必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.priority,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            self._ensure_fresh_operation(conn, order_id)
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据的排程不能被抢占")
            if row["status"] not in {"scheduled", "receiving"}: raise ApiError(409, "invalid_transition", "当前排程不可抢占")
            # 同一张抢占操作单只认一次（operation_id 全局唯一）；抢占后未消费时段全部退还配额。
            self._journal(conn, row, "preempted", actor, role, to_status="preempted", operation_id=order_id, billed_seconds=0, detail={"order_id": order_id, "reason": reason, "displaced_priority": row["priority"]})
            conn.execute("UPDATE schedules SET status='preempted',billed_seconds=0,disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (f"{order_id}: {reason}", iso(), schedule_id))
            conn.execute("UPDATE requests SET status='preempted' WHERE id=?", (row["request_id"],))
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "emergency_preemption", {"order_id": order_id, "reason": reason, "displaced_priority": row["priority"]})
            return {"schedule": self.get_schedule(schedule_id), "reschedule_required": True, "order_id": order_id}

    def change_window(self, window_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能变更可见窗口")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_window", "窗口结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            if not window: raise ApiError(404, "window_not_found", "可见窗口不存在")
            rows = conn.execute("SELECT * FROM schedules WHERE window_id=? AND status IN ('scheduled','receiving','received')", (window_id,)).fetchall()
            impacts = []
            for row in rows:
                sched_start, sched_end = parse_time(row["starts_at"]), parse_time(row["ends_at"])
                invalid = sched_start < start or sched_end > end
                detail = {"window_id": window_id, "old_window": {"starts_at": window["starts_at"], "ends_at": window["ends_at"], "revision": window["revision"]},
                          "new_window": {"starts_at": iso(start), "ends_at": iso(end)}, "invalid": invalid}
                if row["status"] == "received":
                    # 已接收数据继续保留：窗口变化只追加同一条流水，不改状态、不动用量。
                    self._journal(conn, row, "window_changed", actor, role, to_status="received", billed_seconds=row["billed_seconds"], detail={**detail, "preserved": True})
                    impacts.append({"schedule_id": row["id"], "action": "preserve_received_data", "reason": "已接收数据不可回滚", "invalid": invalid})
                    continue
                if invalid:
                    # 窗口变化先记流水（window_changed 与抢占各一条），未消费时段退还配额，再改状态。
                    self._journal(conn, row, "window_changed", actor, role, to_status=row["status"], billed_seconds=row["billed_seconds"], detail=detail)
                    self._journal(conn, row, "preempted", actor, role, to_status="preempted", billed_seconds=0, detail={"reason": "visibility_window_changed", **detail})
                    conn.execute("UPDATE schedules SET status='preempted',billed_seconds=0,disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", ("visibility_window_changed", iso(), row["id"]))
                    conn.execute("UPDATE requests SET status='preempted' WHERE id=?", (row["request_id"],))
                    impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "preempted", "reason": "新窗口无法覆盖原排程", "old_start": row["starts_at"], "old_end": row["ends_at"]})
                else:
                    self._journal(conn, row, "window_changed", actor, role, to_status=row["status"], billed_seconds=row["billed_seconds"], detail=detail)
                    impacts.append({"schedule_id": row["id"], "action": "unchanged", "reason": "新窗口仍覆盖排程"})
            conn.execute("UPDATE visibility_windows SET starts_at=?,ends_at=?,revision=revision+1 WHERE id=?", (iso(start), iso(end), window_id))
            Repository.audit(conn, None, None, actor, role, "visibility_window_changed", {"window_id": window_id, "impacts": impacts})
            return {"window": dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()), "impacts": impacts}

    def reschedule(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] != "preempted": raise ApiError(409, "reschedule_not_needed", "只有被抢占请求需要重排")
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, None, actor, role, "reschedule_requested", {"reason": body.get("reason", "")})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def state(self, role: str, tenant: str) -> dict[str, Any]:
        conn = self.repo.conn
        if role == "requester":
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests WHERE tenant=? ORDER BY id DESC", (tenant,))]
            schedules = [dict(r) for r in conn.execute("SELECT s.* FROM schedules s JOIN requests r ON r.id=s.request_id WHERE r.tenant=? ORDER BY s.id DESC", (tenant,))]
            stations = []
        elif role == "viewer":
            requests, schedules, stations = [], [dict(r) for r in conn.execute("SELECT id,status,starts_at,ends_at FROM schedules ORDER BY id DESC")], []
        else:
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests ORDER BY id DESC")]
            schedules = [dict(r) for r in conn.execute("SELECT * FROM schedules ORDER BY id DESC")]
            stations = [dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id")]
        return {"requests": requests, "schedules": schedules, "stations": stations, "server_time": iso()}

    def get_journal(self, schedule_id: int, role: str, tenant: str) -> dict[str, Any]:
        conn = self.repo.conn
        row = conn.execute("""SELECT s.id,s.status,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
        if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
        if role == "requester" and row["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能查看其他租户排程流水")
        events = [dict(e) | {"detail": json.loads(e["detail_json"])} for e in conn.execute("SELECT * FROM schedule_journal WHERE schedule_id=? ORDER BY seq", (schedule_id,))]
        for ev in events: ev.pop("detail_json", None)
        return {"schedule_id": schedule_id, "status": row["status"], "events": events}


def send_json(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode(); handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: SatelliteSchedulingService; web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        if not size: return {}
        try: value = json.loads(self.rfile.read(size))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "satellite-scheduling"}
        actor, role, tenant = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state(role, tenant)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "schedules"] and parts[2].isdigit(): return 200, self.service.get_schedule(int(parts[2]))
        if len(parts) == 4 and parts[:2] == ["api", "schedules"] and parts[2].isdigit() and parts[3] == "journal": return 200, self.service.get_journal(int(parts[2]), role, tenant)
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role, tenant = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        actions = {
            "/api/satellites": lambda: (201, self.service.create_satellite(actor, role, body)),
            "/api/stations": lambda: (201, self.service.create_station(actor, role, body)),
            "/api/antennas": lambda: (201, self.service.create_antenna(actor, role, body)),
            "/api/maintenance": lambda: (201, self.service.create_maintenance(actor, role, body)),
            "/api/visibility-windows": lambda: (201, self.service.create_window(actor, role, body)),
            "/api/quotas": lambda: (200, self.service.set_quota(actor, role, body)),
            "/api/requests": lambda: (201, self.service.create_request(actor, role, tenant, body)),
        }
        if path in actions: return actions[path]()
        if len(parts) == 4 and parts[:2] == ["api", "requests"] and parts[2].isdigit():
            rid, action = int(parts[2]), parts[3]
            if action == "schedule": return 201, self.service.schedule_request(rid, actor, role, body)
            if action == "reschedule": return 200, self.service.reschedule(rid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "schedules"] and parts[2].isdigit():
            sid, action = int(parts[2]), parts[3]
            if action in {"start", "complete"}: return 200, self.service.transition(sid, actor, role, tenant, "receiving" if action == "start" else "received", body)
            if action == "cancel": return 200, self.service.cancel_schedule(sid, actor, role, tenant, body)
            if action == "preempt": return 200, self.service.emergency_preempt(sid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "visibility-windows"] and parts[2].isdigit() and parts[3] == "change": return 200, self.service.change_window(int(parts[2]), actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path); send_json(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            send_json(self, exc.status, payload)
        except Exception as exc: print(f"unhandled error: {exc!r}"); send_json(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = SatelliteSchedulingService(db_path); handler = type("SatelliteHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"}); return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("SAT_DB", "satellite_scheduling.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"satellite-scheduling listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
