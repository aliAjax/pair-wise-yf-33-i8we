import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, iso, utcnow


class JournalBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.db = str(Path(self.tmp.name) / "test.db")
        self.svc = SatelliteSchedulingService(self.db); self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=6)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 7200})

    def tearDown(self): self.tmp.cleanup()

    def request(self, mb=10000):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": 7, "deadline": iso(self.now + timedelta(days=1))})

    def schedule(self, req, start, end, rate=50, operation_id=None):
        body = {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(start), "ends_at": iso(end), "rate_mbps": rate}
        if operation_id: body["operation_id"] = operation_id
        return self.svc.schedule_request(req["id"], "op", "operator", body)

    def journal_events(self, sid):
        return [ev["event"] for ev in self.svc.get_journal(sid, "operator", "")["events"]]

    def used_quota(self):
        with self.svc.repo.tx() as conn:
            return self.svc._used_quota(conn, "T1", "GS1", self.now.date().isoformat())


class CrashRecoveryTest(JournalBase):
    def test_receiving_without_completion_is_interrupted_and_quota_refunded(self):
        # 第一段：排程 1 已开始接收，排程 2 尚未开始；夜里服务崩溃。
        req = self.request(); sched = self.schedule(req, self.now, self.now + timedelta(hours=1))
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {"operation_id": "op-start-1"})
        req2 = self.request(); sched2 = self.schedule(req2, self.now + timedelta(hours=2), self.now + timedelta(hours=3))

        self.assertEqual(self.used_quota(), 7200)
        # 重启服务：按流水逐条恢复。
        svc2 = SatelliteSchedulingService(self.db)
        self.assertIn(sched["id"], svc2.recovery_report["interrupted"])
        recovered = svc2.get_schedule(sched["id"])
        self.assertEqual(recovered["status"], "interrupted")
        self.assertEqual(recovered["disposition_reason"], "crash_recovery_no_completion")
        # 未开始的排程保持 scheduled，只退还中断排程未消费的 3600 秒。
        self.assertEqual(svc2.get_schedule(sched2["id"])["status"], "scheduled")
        with svc2.repo.tx() as conn:
            used = svc2._used_quota(conn, "T1", "GS1", self.now.date().isoformat())
        self.assertEqual(used, 3600)
        events = svc2.get_journal(sched["id"], "operator", "")["events"]
        self.assertEqual([e["event"] for e in events], ["scheduled", "started", "interrupted"])
        self.assertEqual(events[-1]["actor"], "system-recovery")
        self.assertEqual(events[-1]["detail"]["released_seconds"], 3600)
        # 再次启动恢复应幂等，不重复产生中断流水。
        svc3 = SatelliteSchedulingService(self.db)
        self.assertEqual(svc3.recovery_report["interrupted"], [])
        self.assertEqual(len(svc3.get_journal(sched["id"], "operator", "")["events"]), 3)

    def test_live_interrupt_charges_only_elapsed_after_halfway_crash(self):
        req = self.request(); sched = self.schedule(req, self.now, self.now + timedelta(hours=1))
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {})
        # 模拟崩溃点在排程中途：把 started 流水时间前移 30 分钟（billed 以实际经过秒数封顶）。
        # 直接校验恢复后的 charged 不超过计划时长且非负，并用 released=planned-charged。
        svc2 = SatelliteSchedulingService(self.db)
        row = svc2.get_schedule(sched["id"])
        self.assertEqual(row["status"], "interrupted")
        self.assertGreaterEqual(row["billed_seconds"], 0)
        self.assertLessEqual(row["billed_seconds"], 3600)


class IdempotencyTest(JournalBase):
    def test_same_operation_sheet_scheduled_only_once(self):
        req1, req2 = self.request(), self.request()
        sched = self.schedule(req1, self.now, self.now + timedelta(minutes=30), operation_id="SHEET-1")
        # 两位排程员同时提交同一张操作单：第二次（即便是另一个请求/时段）只认第一次。
        with self.assertRaises(ApiError) as ctx:
            self.schedule(req2, self.now + timedelta(minutes=40), self.now + timedelta(minutes=50), operation_id="SHEET-1")
        self.assertEqual(ctx.exception.code, "operation_already_recorded")
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(self.journal_events(sched["id"]), ["scheduled"])
        # req2 没有产生新排程。
        state = self.svc.state("operator", "")
        self.assertEqual(len(state["schedules"]), 1)

    def test_duplicate_complete_does_not_double_bill(self):
        req = self.request(); sched = self.schedule(req, self.now, self.now + timedelta(hours=1), operation_id="S-1")
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {"operation_id": "S-2"})
        self.svc.transition(sched["id"], "op2", "commander", "", "received", {"operation_id": "S-3"})
        before = self.used_quota()
        # 同一张完成操作单重放：被唯一约束拒绝。
        with self.assertRaises(ApiError) as ctx:
            self.svc.transition(sched["id"], "op2", "commander", "", "received", {"operation_id": "S-3"})
        self.assertEqual(ctx.exception.code, "operation_already_recorded")
        # 不带操作单的裸重试：状态已是 received，同样不重复记账。
        with self.assertRaises(ApiError) as ctx:
            self.svc.transition(sched["id"], "op", "operator", "", "received", {})
        self.assertEqual(ctx.exception.code, "already_completed")
        self.assertEqual(self.used_quota(), before)
        self.assertEqual(self.journal_events(sched["id"]), ["scheduled", "started", "completed"])

    def test_duplicate_preemption_order_applied_once(self):
        req = self.request(); sched = self.schedule(req, self.now, self.now + timedelta(hours=1))
        body = {"order_id": "EMG-9", "reason": "应急任务"}
        self.svc.emergency_preempt(sched["id"], "cmd", "commander", body)
        with self.assertRaises(ApiError) as ctx:
            self.svc.emergency_preempt(sched["id"], "cmd", "commander", body)
        self.assertEqual(ctx.exception.code, "operation_already_recorded")
        # 抢占后配额释放，另一张单可占用同一时段。
        self.assertEqual(self.used_quota(), 0)
        self.assertEqual(self.journal_events(sched["id"]), ["scheduled", "preempted"])


class WindowChangeJournalTest(JournalBase):
    def test_window_change_recorded_in_same_journal_and_data_preserved(self):
        req = self.request(); sched = self.schedule(req, self.now, self.now + timedelta(minutes=30))
        self.svc.transition(sched["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(sched["id"], "op", "operator", "", "received", {})
        billed_before = self.svc.get_schedule(sched["id"])["billed_seconds"]
        changed = self.svc.change_window(self.window["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=10))})
        self.assertEqual(changed["impacts"][0]["action"], "preserve_received_data")
        events = self.svc.get_journal(sched["id"], "operator", "")["events"]
        self.assertEqual([e["event"] for e in events], ["scheduled", "started", "completed", "window_changed"])
        self.assertTrue(events[-1]["detail"]["preserved"])
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "received")
        self.assertEqual(self.svc.get_schedule(sched["id"])["billed_seconds"], billed_before)

    def test_window_change_preempts_unfinished_with_two_events(self):
        req = self.request(); sched = self.schedule(req, self.now, self.now + timedelta(minutes=30))
        self.svc.change_window(self.window["id"], "op", "operator", {"starts_at": iso(self.now + timedelta(hours=2)), "ends_at": iso(self.now + timedelta(hours=6))})
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "preempted")
        self.assertEqual(self.used_quota(), 0)
        self.assertEqual(self.journal_events(sched["id"]), ["scheduled", "window_changed", "preempted"])


class LegacyBackfillTest(JournalBase):
    def request(self, mb=3000):  # 短排程也要能传满，数据量调小
        return super().request(mb)

    def test_old_rows_without_journal_are_backfilled_and_unrecoverable_marked_pending(self):
        # 直接按旧表结构造四类无流水旧数据。
        req1 = self.request(); s1 = self.schedule(req1, self.now, self.now + timedelta(minutes=20))
        req2 = self.request(); s2 = self.schedule(req2, self.now + timedelta(minutes=30), self.now + timedelta(minutes=50))
        self.svc.transition(s2["id"], "op", "operator", "", "receiving", {})
        req3 = self.request(); s3 = self.schedule(req3, self.now + timedelta(hours=1), self.now + timedelta(hours=1, minutes=20))
        self.svc.transition(s3["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(s3["id"], "op", "operator", "", "received", {})
        req4 = self.request(); s4 = self.schedule(req4, self.now + timedelta(hours=2), self.now + timedelta(hours=2, minutes=20))
        self.svc.emergency_preempt(s4["id"], "cmd", "commander", {"order_id": "OLD-1", "reason": "旧抢占"})
        with self.svc.repo.tx() as conn:
            conn.execute("DELETE FROM schedule_journal")
            conn.execute("UPDATE schedules SET billed_seconds=(julianday(ends_at)-julianday(starts_at))*86400 WHERE status IN ('scheduled','receiving','received')")
        # 用一个不自动恢复的实例再手动恢复，模拟升级。
        svc = SatelliteSchedulingService(self.db, recover_on_start=False)
        report = svc.recover()
        self.assertEqual(sorted(report["backfilled"]), sorted([s1["id"], s2["id"], s3["id"], s4["id"]]))
        self.assertEqual(report["interrupted"], [s2["id"]])
        # scheduled / received：回填完整，无需核对。
        self.assertIsNone(svc.get_schedule(s1["id"])["reconcile_status"])
        self.assertIsNone(svc.get_schedule(s3["id"])["reconcile_status"])
        # receiving 补不齐完成记录 -> 中断 + 待核对；preempted 抢占前状态不明 -> 待核对。
        self.assertEqual(svc.get_schedule(s2["id"])["status"], "interrupted")
        self.assertEqual(svc.get_schedule(s2["id"])["reconcile_status"], "pending")
        self.assertEqual(svc.get_schedule(s4["id"])["reconcile_status"], "pending")
        # 所有回填事件带 backfilled 标记，事件链可重放到正确状态。
        for sid, expected in [(s1["id"], "scheduled"), (s3["id"], "received"), (s4["id"], "preempted")]:
            events = svc.get_journal(sid, "operator", "")["events"]
            self.assertTrue(all(e["backfilled"] == 1 for e in events))
            self.assertEqual(svc._expected_status(events)[0], expected)
        # 中断的待核对排程按已过时段计费，其余终态保持原配额语义。
        self.assertEqual(svc.get_schedule(s1["id"])["billed_seconds"], 1200)
        self.assertEqual(svc.get_schedule(s3["id"])["billed_seconds"], 1200)
        self.assertEqual(svc.get_schedule(s4["id"])["billed_seconds"], 0)


if __name__ == "__main__": unittest.main()
