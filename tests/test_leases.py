"""预警发件箱与叫应任务队列的租约回收、租约凭证和旧库迁移验证。"""

from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, NotFoundError

T0 = "2026-10-01T06:00:00Z"  # 演练开始，30 秒租约至 06:00:30
T1 = "2026-10-01T06:00:31Z"  # 租约已过期
T2 = "2026-10-01T06:01:05Z"

# 升级前旧库结构：没有 lease_owner / lease_version / last_error 列。
LEGACY_SCHEMA = """
CREATE TABLE outbox_messages (
    message_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    delivered_at TEXT
);
CREATE TABLE scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    run_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);
"""


class LeaseCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "alert.sqlite3"

    def tearDown(self):
        self.temp.cleanup()

    def open(self, now: str) -> CivicFlow:
        return CivicFlow.open(self.path, fixed_now=now)


class OutboxLeaseTest(LeaseCase):
    def enqueue_alert(self, app: CivicFlow, aggregate: str = "alert:1") -> str:
        return app.outbox.enqueue(topic="alert.earthquake", aggregate_id=aggregate, payload={"level": "red", "channels": ["mobile", "tv", "campus", "radio"]})

    def test_expired_lease_is_reclaimed_and_old_owner_cannot_complete(self):
        first = self.open(T0)
        message = self.enqueue_alert(first)
        leased = first.outbox.lease(owner="worker-a", seconds=30)
        self.assertEqual([item["message_id"] for item in leased], [message])
        token = leased[0]["lease_version"]
        # worker-a 领取后崩溃，值守界面仍能看到当前持有人、消息仍会送达。
        snapshot = first.outbox.describe(message)
        self.assertEqual(snapshot["current_owner"], "worker-a")
        self.assertTrue(snapshot["will_deliver"])
        # 租约过期后 worker-b 回收同一条预警。
        second = self.open(T1)
        reclaimed = second.outbox.lease(owner="worker-b", seconds=30)
        self.assertEqual([item["message_id"] for item in reclaimed], [message])
        self.assertEqual(reclaimed[0]["lease_owner"], "worker-b")
        self.assertGreater(reclaimed[0]["lease_version"], token)
        # 旧进程迟到的确认与失败上报都不得覆盖新进程。
        with self.assertRaises(ConflictError):
            second.outbox.complete(message, owner="worker-a", lease_version=token)
        with self.assertRaises(ConflictError):
            second.outbox.fail(message, owner="worker-a", lease_version=token, error="迟到的失败")
        second.outbox.complete(message, owner="worker-b", lease_version=reclaimed[0]["lease_version"])
        done = second.outbox.describe(message)
        self.assertEqual(done["status"], "delivered")
        self.assertFalse(done["will_deliver"])
        self.assertIsNone(done["current_owner"])
        self.assertEqual(done["lease_owner"], "worker-b")
        # 重复确认得到一致的冲突结果，已送达状态不被改写。
        with self.assertRaises(ConflictError):
            second.outbox.complete(message, owner="worker-b", lease_version=reclaimed[0]["lease_version"])
        self.assertEqual(second.outbox.describe(message)["status"], "delivered")

    def test_active_lease_is_not_taken_over(self):
        first = self.open(T0)
        message = self.enqueue_alert(first)
        first.outbox.lease(owner="worker-a", seconds=30)
        other = self.open("2026-10-01T06:00:10Z")
        self.assertEqual(other.outbox.lease(owner="worker-b", seconds=30), [])
        self.assertEqual(other.outbox.describe(message)["current_owner"], "worker-a")

    def test_renew_extends_lease_only_for_holder(self):
        first = self.open(T0)
        message = self.enqueue_alert(first)
        leased = first.outbox.lease(owner="worker-a", seconds=30)
        token = leased[0]["lease_version"]
        with self.assertRaises(ConflictError):
            first.outbox.renew(message, owner="worker-b", lease_version=token, seconds=60)
        extended = first.outbox.renew(message, owner="worker-a", lease_version=token, seconds=60)
        self.assertEqual(extended, "2026-10-01T06:01:00Z")
        # 原租约时刻已过、续租仍有效，其他进程取不到。
        self.assertEqual(self.open("2026-10-01T06:00:45Z").outbox.lease(owner="worker-b"), [])
        # 续租也过期后才能回收。
        final = self.open("2026-10-01T06:01:01Z")
        self.assertEqual([item["message_id"] for item in final.outbox.lease(owner="worker-b")], [message])

    def test_failed_delivery_retries_then_dies_with_reason(self):
        message = self.enqueue_alert(self.open(T0))
        for round_ in range(1, 6):
            app = self.open(T0)
            leased = app.outbox.lease(owner="worker-a", seconds=30)
            self.assertEqual([item["message_id"] for item in leased], [message])
            app.outbox.fail(message, owner="worker-a", lease_version=leased[0]["lease_version"], error=f"第{round_}次: 短信网关超时")
            state = app.outbox.describe(message)
            if round_ < 5:
                self.assertEqual(state["status"], "failed")
                self.assertTrue(state["will_deliver"])
            else:
                self.assertEqual(state["status"], "dead")
                self.assertFalse(state["will_deliver"])
        self.assertEqual(state["attempts"], 5)
        self.assertEqual(state["last_error"], "第5次: 短信网关超时")
        self.assertEqual(self.open(T1).outbox.lease(owner="worker-b"), [])

    def test_lease_survives_process_restart(self):
        first = self.open(T0)
        message = self.enqueue_alert(first)
        first.outbox.lease(owner="worker-a", seconds=30)
        # 平台重启后重新打开同一数据库，持有人与租约版本仍然可查。
        restarted = self.open("2026-10-01T06:00:05Z")
        snapshot = restarted.outbox.describe(message)
        self.assertEqual(snapshot["current_owner"], "worker-a")
        self.assertEqual(snapshot["lease_version"], 1)
        self.assertEqual(restarted.outbox.lease(owner="worker-b"), [])
        # 重启后租约过期，其他进程正常回收并完成投递。
        expired = self.open(T1)
        reclaimed = expired.outbox.lease(owner="worker-b", seconds=30)
        self.assertEqual([item["message_id"] for item in reclaimed], [message])
        expired.outbox.complete(message, owner="worker-b", lease_version=reclaimed[0]["lease_version"])
        self.assertEqual(expired.outbox.describe(message)["status"], "delivered")

    def test_competing_workers_split_pending_alerts(self):
        app = self.open(T0)
        ids = {self.enqueue_alert(app, aggregate=f"alert:{index}") for index in range(6)}
        barrier = threading.Barrier(2)
        leased: dict[str, list[dict]] = {"worker-a": [], "worker-b": []}
        errors: list[Exception] = []

        def pump(owner: str) -> None:
            try:
                barrier.wait(timeout=10)
                leased[owner] = CivicFlow.open(self.path, fixed_now=T0).outbox.lease(owner=owner, seconds=30, limit=3)
            except Exception as exc:  # pragma: no cover - 失败时由断言报告
                errors.append(exc)

        threads = [threading.Thread(target=pump, args=(owner,)) for owner in leased]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertFalse(any(thread.is_alive() for thread in threads))
        held_a = {item["message_id"] for item in leased["worker-a"]}
        held_b = {item["message_id"] for item in leased["worker-b"]}
        self.assertFalse(held_a & held_b)
        self.assertEqual(held_a | held_b, ids)

    def test_describe_unknown_message_raises(self):
        with self.assertRaises(NotFoundError):
            self.open(T0).outbox.describe("msg:missing")


class JobLeaseTest(LeaseCase):
    def schedule_call(self, app: CivicFlow, subject: str = "alert:1") -> str:
        return app.jobs.schedule(job_type="call-response", subject_id=subject, run_at=T0, payload={"action": "叫应", "target": "duty-officer"})

    def test_running_job_is_reclaimed_after_crash_and_fenced(self):
        first = self.open(T0)
        job = self.schedule_call(first)
        claimed = first.jobs.claim_due(owner="worker-a", seconds=30)
        self.assertEqual([item["job_id"] for item in claimed], [job])
        token = claimed[0]["lease_version"]
        self.assertEqual(first.jobs.describe(job)["current_owner"], "worker-a")
        # worker-a 崩溃，任务停在 running；租约过期后 worker-b 接管。
        second = self.open(T1)
        reclaimed = second.jobs.claim_due(owner="worker-b", seconds=30)
        self.assertEqual([item["job_id"] for item in reclaimed], [job])
        self.assertGreater(reclaimed[0]["lease_version"], token)
        # 旧进程的迟到完成与重试上报都被拒绝。
        with self.assertRaises(ConflictError):
            second.jobs.finish(job, owner="worker-a", lease_version=token)
        with self.assertRaises(ConflictError):
            second.jobs.retry(job, owner="worker-a", lease_version=token, error="迟到", retry_at=T2)
        second.jobs.finish(job, owner="worker-b", lease_version=reclaimed[0]["lease_version"])
        done = second.jobs.describe(job)
        self.assertEqual(done["status"], "succeeded")
        self.assertFalse(done["will_run"])
        self.assertEqual(done["lease_owner"], "worker-b")
        with self.assertRaises(ConflictError):
            second.jobs.finish(job, owner="worker-b", lease_version=reclaimed[0]["lease_version"])

    def test_retry_waits_for_scheduled_time_and_keeps_reason(self):
        first = self.open(T0)
        job = self.schedule_call(first)
        claimed = first.jobs.claim_due(owner="worker-a", seconds=30)
        first.jobs.retry(job, owner="worker-a", lease_version=claimed[0]["lease_version"], error="叫应无人接听", retry_at="2026-10-01T06:05:00Z")
        state = first.jobs.describe(job)
        self.assertEqual(state["status"], "retry")
        self.assertEqual(state["last_error"], "叫应无人接听")
        self.assertTrue(state["will_run"])
        self.assertIsNone(state["current_owner"])
        # 未到重试时间不可领取，到点后由其他进程完成。
        self.assertEqual(self.open("2026-10-01T06:04:59Z").jobs.claim_due(owner="worker-b"), [])
        due = self.open("2026-10-01T06:05:00Z")
        reclaimed = due.jobs.claim_due(owner="worker-b", seconds=30)
        self.assertEqual([item["job_id"] for item in reclaimed], [job])
        due.jobs.finish(job, owner="worker-b", lease_version=reclaimed[0]["lease_version"])
        self.assertEqual(due.jobs.describe(job)["status"], "succeeded")

    def test_retry_exhaustion_marks_failed(self):
        job = self.schedule_call(self.open(T0))
        for _ in range(5):
            app = self.open(T0)
            claimed = app.jobs.claim_due(owner="worker-a", seconds=30)
            app.jobs.retry(job, owner="worker-a", lease_version=claimed[0]["lease_version"], error="叫应无人接听", retry_at=T0)
        state = self.open(T0).jobs.describe(job)
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["attempt"], 5)
        self.assertEqual(state["last_error"], "叫应无人接听")
        self.assertFalse(state["will_run"])
        self.assertEqual(self.open(T1).jobs.claim_due(owner="worker-b"), [])

    def test_job_lease_renew_and_restart(self):
        first = self.open(T0)
        job = self.schedule_call(first)
        claimed = first.jobs.claim_due(owner="worker-a", seconds=30)
        token = claimed[0]["lease_version"]
        extended = first.jobs.renew(job, owner="worker-a", lease_version=token, seconds=90)
        self.assertEqual(extended, "2026-10-01T06:01:30Z")
        # 重启后原租约时刻已过、续租仍有效，他人取不到也续不了。
        restarted = self.open("2026-10-01T06:01:00Z")
        self.assertEqual(restarted.jobs.describe(job)["current_owner"], "worker-a")
        self.assertEqual(restarted.jobs.claim_due(owner="worker-b"), [])
        with self.assertRaises(ConflictError):
            restarted.jobs.renew(job, owner="worker-b", lease_version=token, seconds=30)
        # 续租过期后任务被回收并完成。
        expired = self.open("2026-10-01T06:01:31Z")
        reclaimed = expired.jobs.claim_due(owner="worker-b", seconds=30)
        self.assertEqual([item["job_id"] for item in reclaimed], [job])
        expired.jobs.finish(job, owner="worker-b", lease_version=reclaimed[0]["lease_version"])

    def test_competing_workers_split_due_jobs(self):
        app = self.open(T0)
        ids = {self.schedule_call(app, subject=f"alert:{index}") for index in range(6)}
        barrier = threading.Barrier(2)
        claimed: dict[str, list[dict]] = {"worker-a": [], "worker-b": []}
        errors: list[Exception] = []

        def pump(owner: str) -> None:
            try:
                barrier.wait(timeout=10)
                claimed[owner] = CivicFlow.open(self.path, fixed_now=T0).jobs.claim_due(owner=owner, seconds=30, limit=3)
            except Exception as exc:  # pragma: no cover - 失败时由断言报告
                errors.append(exc)

        threads = [threading.Thread(target=pump, args=(owner,)) for owner in claimed]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertFalse(any(thread.is_alive() for thread in threads))
        held_a = {item["job_id"] for item in claimed["worker-a"]}
        held_b = {item["job_id"] for item in claimed["worker-b"]}
        self.assertFalse(held_a & held_b)
        self.assertEqual(held_a | held_b, ids)


class LegacyMigrationTest(LeaseCase):
    def setUp(self):
        super().setUp()
        connection = sqlite3.connect(self.path)
        connection.executescript(LEGACY_SCHEMA)
        # 既有数据：待发送、已送达、崩溃遗留的过期 leased；任务同理。
        connection.execute("INSERT INTO outbox_messages VALUES('msg:pending','alert.earthquake','alert:1','{}','2026-09-30T23:59:00Z',NULL,0,'pending',NULL)")
        connection.execute("INSERT INTO outbox_messages VALUES('msg:delivered','alert.earthquake','alert:2','{}','2026-09-30T23:59:00Z',NULL,1,'delivered','2026-09-30T23:59:30Z')")
        connection.execute("INSERT INTO outbox_messages VALUES('msg:stuck','alert.earthquake','alert:3','{}','2026-09-30T23:59:00Z','2026-10-01T00:00:30Z',1,'leased',NULL)")
        connection.execute("INSERT INTO scheduled_jobs VALUES('job:waiting','call-response','alert:1','2026-09-30T23:59:00Z','{}','waiting',0,NULL,'')")
        connection.execute("INSERT INTO scheduled_jobs VALUES('job:done','call-response','alert:2','2026-09-30T23:59:00Z','{}','succeeded',1,NULL,'')")
        connection.execute("INSERT INTO scheduled_jobs VALUES('job:stuck','call-response','alert:3','2026-09-30T23:59:00Z','{}','running',1,'2026-10-01T00:00:30Z','')")
        connection.commit()
        connection.close()

    def test_legacy_rows_keep_semantics_and_stuck_rows_are_reclaimable(self):
        app = self.open(T0)
        # 迁移后新列存在，既有行取默认值。
        with app.database.connect() as connection:
            outbox_columns = {row["name"] for row in connection.execute("PRAGMA table_info(outbox_messages)")}
            job_columns = {row["name"] for row in connection.execute("PRAGMA table_info(scheduled_jobs)")}
        self.assertTrue({"lease_owner", "lease_version", "last_error"} <= outbox_columns)
        self.assertTrue({"lease_owner", "lease_version"} <= job_columns)
        pending = app.outbox.describe("msg:pending")
        self.assertEqual(pending["status"], "pending")
        self.assertEqual(pending["lease_version"], 0)
        self.assertIsNone(pending["lease_owner"])
        self.assertTrue(pending["will_deliver"])
        delivered = app.outbox.describe("msg:delivered")
        self.assertEqual(delivered["status"], "delivered")
        self.assertFalse(delivered["will_deliver"])
        # 崩溃遗留的过期 leased 消息与 running 任务可被回收，已完结记录不受影响。
        leased = app.outbox.lease(owner="worker-b", seconds=30)
        self.assertEqual({item["message_id"] for item in leased}, {"msg:pending", "msg:stuck"})
        stuck = next(item for item in leased if item["message_id"] == "msg:stuck")
        self.assertEqual(stuck["lease_version"], 1)
        self.assertEqual(stuck["attempts"], 2)
        app.outbox.complete("msg:stuck", owner="worker-b", lease_version=stuck["lease_version"])
        claimed = app.jobs.claim_due(owner="worker-b", seconds=30)
        self.assertEqual({item["job_id"] for item in claimed}, {"job:waiting", "job:stuck"})
        stuck_job = next(item for item in claimed if item["job_id"] == "job:stuck")
        app.jobs.finish("job:stuck", owner="worker-b", lease_version=stuck_job["lease_version"])
        self.assertEqual(app.jobs.describe("job:done")["status"], "succeeded")
        # 再次打开数据库时迁移幂等，处理结果保持一致。
        again = self.open(T0)
        self.assertEqual(again.outbox.describe("msg:stuck")["status"], "delivered")
        self.assertEqual(again.jobs.describe("job:stuck")["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()
