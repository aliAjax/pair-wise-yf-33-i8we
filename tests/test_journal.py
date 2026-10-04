import http.client
import json
import sys
import tempfile
import threading
import unittest
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import SatelliteSchedulingService, create_server, iso, utcnow


class JournalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "test.db"
        self.svc = SatelliteSchedulingService(self.db)
        self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 7200})

    def tearDown(self):
        self.tmp.cleanup()

    def request(self, mb=10000):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": 7, "deadline": iso(self.now + timedelta(days=1))})

    def schedule(self, req, start=None, end=None, rate=60):
        return self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1",
                                         "starts_at": iso(start or self.now), "ends_at": iso(end or self.now + timedelta(hours=1)), "rate_mbps": rate})

    def events(self, schedule_id):
        return self.svc.get_schedule_journal(schedule_id)

    def test_journal_records_every_lifecycle_event(self):
        req = self.request()
        sched = self.schedule(req)
        self.assertEqual([e["event"] for e in self.events(sched["id"])], ["schedule_created"])
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {})
        self.assertEqual([e["event"] for e in self.events(sched["id"])], ["schedule_created", "start"])
        self.svc.transition(sched["id"], "op", "operator", "", "received", {})
        self.assertEqual([e["event"] for e in self.events(sched["id"])], ["schedule_created", "start", "complete"])
        # 流水只追加，状态改动后流水不被修改
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "received")

    def test_duplicate_complete_does_not_double_count(self):
        req = self.request()
        sched = self.schedule(req)
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {"op_key": "form-001"})
        first = self.svc.transition(sched["id"], "op", "operator", "", "received", {"op_key": "form-002"})
        # 重复完成（不同操作单）被状态机挡住，不产生第二条 complete 流水
        with self.assertRaises(Exception):
            self.svc.transition(sched["id"], "op", "operator", "", "received", {"op_key": "form-003"})
        events = self.events(sched["id"])
        self.assertEqual([e["event"] for e in events].count("complete"), 1)
        self.assertEqual(first["status"], "received")

    def test_same_op_key_accepted_once(self):
        req = self.request()
        sched = self.schedule(req)
        # 两位排程员提交同一张操作单：只认一次
        r1 = self.svc.transition(sched["id"], "op", "operator", "", "receiving", {"op_key": "same-form-9"})
        r2 = self.svc.transition(sched["id"], "op2", "operator", "", "receiving", {"op_key": "same-form-9"})
        self.assertEqual(r1["status"], "receiving")
        self.assertEqual(r2["status"], "receiving")
        self.assertEqual([e["event"] for e in self.events(sched["id"])].count("start"), 1)
        # 操作单号被别的排程占用 -> 冲突
        req2 = self.request()
        sched2 = self.schedule(req2, start=self.now + timedelta(hours=1), end=self.now + timedelta(minutes=90))
        with self.assertRaises(Exception):
            self.svc.transition(sched2["id"], "op", "operator", "", "receiving", {"op_key": "same-form-9"})

    def test_recovery_interrupts_receiving_without_completion(self):
        req = self.request()
        sched = self.schedule(req)
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {})
        # 服务崩溃后重启：接收中有 start 流水、无 complete 流水 -> 转中断
        report = self.svc.recover()
        self.assertEqual(len(report["reconcile"]["interrupted"]), 1)
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "interrupted")
        self.assertEqual(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone()["status"], "pending")
        events = self.events(sched["id"])
        self.assertEqual(events[-1]["event"], "interrupt")
        self.assertEqual(events[-1]["needs_review"], 1)
        # 已完成的排程不被中断
        req2 = self.request()
        sched2 = self.schedule(req2, start=self.now + timedelta(hours=1), end=self.now + timedelta(minutes=90))
        self.svc.transition(sched2["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(sched2["id"], "op", "operator", "", "received", {})
        self.svc.recover()
        self.assertEqual(self.svc.get_schedule(sched2["id"])["status"], "received")

    def test_interrupted_schedule_returns_quota(self):
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 3600})
        req = self.request()
        sched = self.schedule(req)  # 占用 3600 秒
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {})
        # 中断前配额被占住，第二个排程下不了单
        req2 = self.request()
        with self.assertRaises(Exception):
            self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1",
                                            "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1)), "rate_mbps": 60})
        # 恢复后中断排程，时段还回租户配额
        self.svc.recover()
        sched2 = self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1",
                                            "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1)), "rate_mbps": 60})
        self.assertEqual(sched2["status"], "scheduled")

    def test_recovery_replays_journal_state(self):
        req = self.request()
        sched = self.schedule(req)
        # 模拟流水已写、状态未改（崩溃在流水与状态之间）：直接补一条 complete 流水
        with self.svc.repo.tx() as conn:
            self.svc.journal.append(conn, op_key="manual-complete", schedule_id=sched["id"], request_id=req["id"],
                                    event="complete", from_status="receiving", to_status="received",
                                    payload={"reason": "crash_simulation"}, actor="op", role="operator")
            conn.execute("UPDATE schedules SET status='scheduled' WHERE id=?", (sched["id"],))
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "scheduled")
        self.svc.recover()
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "received")

    def test_backfill_legacy_data_without_journal(self):
        # 旧数据：直接写库，没有流水
        req = self.request()
        now = iso()
        with self.svc.repo.tx() as conn:
            cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,status,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                               (req["id"], self.window["id"], "GS1", "ANT1", "SAT1", iso(self.now), iso(self.now + timedelta(hours=1)), 60, "received", "op", now, now))
            legacy_id = cur.lastrowid
        report = self.svc.recover()
        self.assertGreater(report["backfill"]["events_backfilled"], 0)
        events = self.events(legacy_id)
        self.assertEqual([e["event"] for e in events], ["schedule_created", "complete"])
        self.assertTrue(all(e["needs_review"] for e in events))
        self.assertTrue(all(e["backfill"] for e in events))
        # 状态与回填流水一致，不被恢复改动
        self.assertEqual(self.svc.get_schedule(legacy_id)["status"], "received")

    def test_backfill_unknown_status_marked_needs_review(self):
        req = self.request()
        now = iso()
        with self.svc.repo.tx() as conn:
            cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,status,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                               (req["id"], self.window["id"], "GS1", "ANT1", "SAT1", iso(self.now), iso(self.now + timedelta(hours=1)), 60, "weird_status", "op", now, now))
            weird_id = cur.lastrowid
        report = self.svc.recover()
        self.assertTrue(any(x["schedule_id"] == weird_id for x in report["backfill"]["needs_review"]))
        events = self.events(weird_id)
        self.assertEqual([e["event"] for e in events], ["schedule_created"])
        self.assertTrue(events[0]["needs_review"])

    def test_window_change_journaled_and_idempotent(self):
        req = self.request()
        sched = self.schedule(req)
        req2 = self.request(10000)
        sched2 = self.schedule(req2, start=self.now + timedelta(hours=1), end=self.now + timedelta(minutes=90))
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(sched["id"], "op", "operator", "", "received", {})
        # 同一张窗口变更单提交两次
        r1 = self.svc.change_window(self.window["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=10)), "op_key": "wc-2026-01"})
        r2 = self.svc.change_window(self.window["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=10)), "op_key": "wc-2026-01"})
        self.assertEqual(r2.get("idempotent"), True)
        # 已接收数据保留，且窗口变化写进了同一条流水
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "received")
        self.assertEqual([e["event"] for e in self.events(sched["id"])], ["schedule_created", "start", "complete", "window_change"])
        self.assertEqual([e["event"] for e in self.events(sched2["id"])], ["schedule_created", "window_change"])
        self.assertEqual(self.svc.get_schedule(sched2["id"])["status"], "preempted")

    def test_http_journal_endpoint(self):
        req = self.request()
        sched = self.schedule(req)
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(sched["id"], "op", "operator", "", "received", {})
        server = create_server(self.db, "127.0.0.1", 0)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port)
            conn.request("GET", f"/api/schedules/{sched['id']}/journal", headers={"X-User-Id": "aud", "X-Role": "auditor"})
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            payload = json.loads(resp.read())
            self.assertEqual([e["event"] for e in payload], ["schedule_created", "start", "complete"])
            resp.close(); conn.close()
        finally:
            server.shutdown(); server.server_close()


if __name__ == "__main__":
    unittest.main()
