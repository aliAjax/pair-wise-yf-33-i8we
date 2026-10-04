#!/usr/bin/env python3
"""执行流水（journal）：先记流水、再改状态，服务崩溃后按流水逐条恢复。

流水只回答"发生过什么"：排程开始、完成、取消、抢占、窗口变化、中断都先落流水。
状态改动由 SatelliteSchedulingService 在同一事务内依据流水判定完成，接口入口
（Handler）不直接写流水。流水是追加写（append-only）的，不允许 UPDATE/DELETE。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS schedule_journal(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    op_key TEXT NOT NULL UNIQUE,
    schedule_id INTEGER REFERENCES schedules(id),
    request_id INTEGER REFERENCES requests(id),
    seq INTEGER NOT NULL,
    event TEXT NOT NULL,                 -- schedule_created/start/complete/cancel/preempt/window_change/interrupt/backfill
    from_status TEXT,
    to_status TEXT,
    payload_json TEXT NOT NULL,
    needs_review INTEGER NOT NULL DEFAULT 0,   -- 1=回填或恢复产生，需人工核对
    backfill INTEGER NOT NULL DEFAULT 1,       -- 1=升级时按现有状态补记，非实时观测
    actor TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schedule_journal_schedule ON schedule_journal(schedule_id, seq);
"""

# 实时事件 -> (允许的来源状态, 目标状态, 请求目标状态)；window_change 由 payload.action 单独判定
EVENT_STATUS: dict[str, tuple[frozenset[str] | None, str, str | None]] = {
    "schedule_created": (None, "scheduled", "scheduled"),
    "start": (frozenset({"scheduled"}), "receiving", None),
    "complete": (frozenset({"receiving"}), "received", "received"),
    "cancel": (frozenset({"scheduled", "receiving"}), "canceled", "pending"),
    "preempt": (frozenset({"scheduled", "receiving"}), "preempted", "preempted"),
    "interrupt": (frozenset({"receiving"}), "interrupted", "pending"),
}


def iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class Journal:
    """流水判定与恢复。所有方法接收调用方事务内的 conn，不自行开事务。"""

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        # 逐条 execute，不用 executescript（后者会先 COMMIT 掉调用方事务）
        for stmt in SCHEMA.split(";"):
            if stmt.strip():
                conn.execute(stmt)

    # ---------- 判定 ----------

    @staticmethod
    def op_exists(conn: sqlite3.Connection, op_key: str) -> sqlite3.Row | None:
        return conn.execute("SELECT * FROM schedule_journal WHERE op_key=?", (op_key,)).fetchone()

    @staticmethod
    def has_event(conn: sqlite3.Connection, schedule_id: int, event: str) -> bool:
        return conn.execute("SELECT 1 FROM schedule_journal WHERE schedule_id=? AND event=? LIMIT 1", (schedule_id, event)).fetchone() is not None

    @staticmethod
    def events(conn: sqlite3.Connection, schedule_id: int) -> list[sqlite3.Row]:
        return list(conn.execute("SELECT * FROM schedule_journal WHERE schedule_id=? ORDER BY seq", (schedule_id,)))

    @staticmethod
    def next_seq(conn: sqlite3.Connection, schedule_id: int) -> int:
        row = conn.execute("SELECT COALESCE(MAX(seq),0) AS m FROM schedule_journal WHERE schedule_id=?", (schedule_id,)).fetchone()
        return int(row["m"]) + 1

    # ---------- 追加（唯一允许的写） ----------

    def append(self, conn: sqlite3.Connection, *, op_key: str, schedule_id: int | None, request_id: int | None,
               event: str, from_status: str | None, to_status: str | None, payload: dict[str, Any],
               actor: str, role: str, needs_review: bool = False, backfill: bool = False) -> dict[str, Any]:
        seq = self.next_seq(conn, schedule_id) if schedule_id is not None else 1
        conn.execute("""INSERT INTO schedule_journal(op_key,schedule_id,request_id,seq,event,from_status,to_status,
                                                    payload_json,needs_review,backfill,actor,role,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                     (op_key, schedule_id, request_id, seq, event, from_status, to_status,
                      json.dumps(payload, ensure_ascii=False, sort_keys=True),
                      1 if needs_review else 0, 1 if backfill else 0, actor, role, iso()))
        return {"op_key": op_key, "seq": seq, "event": event, "to_status": to_status}

    # ---------- 升级回填：旧数据没有流水，按现有状态补起始流水 ----------

    @staticmethod
    def _infer_events(status: str) -> list[str]:
        """按排程现有状态推断应补记的流水；推断不出的只补起始条，交人工核对。"""
        decisive = {
            "scheduled": [],
            "receiving": ["start"],
            "received": ["complete"],
            "canceled": ["cancel"],
            "preempted": ["preempt"],
            "interrupted": ["interrupt"],
        }.get(status)
        if decisive is None:  # 未知状态：补起始条，其余待核对
            return []
        return ["schedule_created", *decisive]

    def backfill(self, conn: sqlite3.Connection) -> dict[str, Any]:
        """对没有任何流水的排程按当前状态回填起始流水，全部标记 needs_review。"""
        report = {"schedules_seen": 0, "events_backfilled": 0, "needs_review": []}
        schedules = conn.execute("SELECT * FROM schedules ORDER BY id").fetchall()
        for sched in schedules:
            report["schedules_seen"] += 1
            if self.op_exists(conn, f"backfill:{sched['id']}:schedule_created") or \
               conn.execute("SELECT 1 FROM schedule_journal WHERE schedule_id=? LIMIT 1", (sched["id"],)).fetchone():
                continue
            events = self._infer_events(sched["status"])
            if not events:
                report["needs_review"].append({"schedule_id": sched["id"], "status": sched["status"], "reason": "无法从现有状态推断流水"})
            for ev in events:
                self.append(conn, op_key=f"backfill:{sched['id']}:{ev}", schedule_id=sched["id"],
                            request_id=sched["request_id"], event=ev, from_status=None,
                            to_status=EVENT_STATUS[ev][1],
                            payload={"backfill": True, "inferred_from_status": sched["status"],
                                     "reason": "upgrade_backfill", "schedule_created_at": sched["created_at"]},
                            actor="system", role="system", needs_review=True, backfill=True)
                report["events_backfilled"] += 1
            report["needs_review"].append({"schedule_id": sched["id"], "status": sched["status"],
                                            "reason": "回填流水为推断结果，需核对"})
        return report

    # ---------- 崩溃恢复：按流水逐条重放，接收中查不到完成记录的转中断 ----------

    @staticmethod
    def _expected_status(events: list[sqlite3.Row]) -> str | None:
        """流水判定：取最后一个决定性事件对应的状态。"""
        for ev in events:
            if ev["event"] == "complete":
                return "received"
        for ev in events:
            if ev["event"] == "cancel":
                return "canceled"
        for ev in events:
            if ev["event"] == "preempt":
                return "preempted"
            if ev["event"] == "window_change":
                try:
                    if json.loads(ev["payload_json"]).get("action") == "preempted":
                        return "preempted"
                except json.JSONDecodeError:
                    pass
        for ev in events:
            if ev["event"] == "interrupt":
                return "interrupted"
        for ev in events:
            if ev["event"] == "start":
                return "receiving"
        for ev in events:
            if ev["event"] in {"schedule_created", "backfill"}:
                return "scheduled"
        return None

    @staticmethod
    def _expected_request_status(events: list[sqlite3.Row]) -> str | None:
        for ev in events:
            if ev["event"] == "complete":
                return "received"
        for ev in events:
            if ev["event"] in {"cancel", "interrupt"}:
                return "pending"
            if ev["event"] == "preempt":
                return "preempted"
            if ev["event"] == "window_change":
                try:
                    if json.loads(ev["payload_json"]).get("action") == "preempted":
                        return "preempted"
                except json.JSONDecodeError:
                    pass
        for ev in events:
            if ev["event"] == "schedule_created":
                return "scheduled"
        return None

    def reconcile(self, conn: sqlite3.Connection) -> dict[str, Any]:
        """按流水逐条恢复状态。返回修复明细。"""
        report = {"checked": 0, "fixed": [], "interrupted": [], "needs_review": []}
        schedules = conn.execute("SELECT * FROM schedules ORDER BY id").fetchall()
        for sched in schedules:
            report["checked"] += 1
            events = self.events(conn, sched["id"])
            if not events:
                # 完全没有流水：补一条待核对的起始流水，状态不动
                self.append(conn, op_key=f"backfill:{sched['id']}:schedule_created", schedule_id=sched["id"],
                            request_id=sched["request_id"], event="schedule_created", from_status=None,
                            to_status="scheduled", payload={"backfill": True, "reason": "recovery_missing_journal"},
                            actor="system", role="system", needs_review=True, backfill=True)
                report["needs_review"].append({"schedule_id": sched["id"], "reason": "无流水，已补待核对起始条"})
                continue
            # 接收中但查不到完成记录：转中断，未消费时段还回租户配额
            if sched["status"] == "receiving" and not self.has_event(conn, sched["id"], "complete"):
                self.append(conn, op_key=f"recover:interrupt:{sched['id']}", schedule_id=sched["id"],
                            request_id=sched["request_id"], event="interrupt", from_status="receiving",
                            to_status="interrupted",
                            payload={"reason": "recovery_no_completion", "detail": "服务恢复时接收中无完成记录，按中断处理"},
                            actor="system", role="system", needs_review=True)
                conn.execute("UPDATE schedules SET status='interrupted', revision=revision+1, updated_at=? WHERE id=?",
                            (iso(), sched["id"]))
                conn.execute("UPDATE requests SET status='pending' WHERE id=?", (sched["request_id"],))
                report["interrupted"].append({"schedule_id": sched["id"], "request_id": sched["request_id"]})
                continue
            expected = self._expected_status(events)
            if expected and expected != sched["status"]:
                conn.execute("UPDATE schedules SET status=?, revision=revision+1, updated_at=? WHERE id=?",
                             (expected, iso(), sched["id"]))
                req_target = self._expected_request_status(events)
                if req_target:
                    conn.execute("UPDATE requests SET status=? WHERE id=?", (req_target, sched["request_id"]))
                report["fixed"].append({"schedule_id": sched["id"], "from": sched["status"], "to": expected})
        return report
