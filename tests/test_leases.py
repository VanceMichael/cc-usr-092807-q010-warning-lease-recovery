"""租约回收、 fencing 令牌、旧库迁移与多进程竞争的自动化验证。

场景对应值守演练：预警投递（outbox）、叫应与避险跟进任务（jobs）、
旧库就地升级、双进程争抢同一批消息。
"""

from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, NotFoundError, ValidationError

T0 = "2026-10-01T08:00:00+08:00"
T0_PLUS_30S = "2026-10-01T08:00:30+08:00"
T0_PLUS_31S = "2026-10-01T08:00:31+08:00"
T0_PLUS_60S = "2026-10-01T08:01:00+08:00"
T0_PLUS_90S = "2026-10-01T08:01:30+08:00"

# 升级前旧库的表结构（没有 lease_owner / lease_version / last_error 列）。
OLD_SCHEMA = """
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


class LeaseTestBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "leases.sqlite3"

    def tearDown(self):
        self.temp.cleanup()

    def open_app(self, now: str) -> CivicFlow:
        return CivicFlow.open(self.path, fixed_now=now)


class OutboxLeaseTest(LeaseTestBase):
    def test_expired_lease_reclaimed_and_stale_ack_rejected(self):
        """演练复现：发送进程领取后崩溃，租约过期必须能被新进程安全回收。"""
        app_t0 = self.open_app(T0)
        message = app_t0.outbox.enqueue(topic="alert.earthquake", aggregate_id="alert:1", payload={"level": 5, "channels": ["手机", "电视", "校园终端", "应急广播"]})
        first = app_t0.outbox.lease(owner="sender-a", seconds=30)
        self.assertEqual([item["message_id"] for item in first], [message])
        token_a = first[0]["lease_version"]
        self.assertEqual(first[0]["lease_owner"], "sender-a")
        # sender-a 崩溃，不再确认；租约到期后新进程回收同一条预警
        app_t1 = self.open_app(T0_PLUS_31S)
        second = app_t1.outbox.lease(owner="sender-b", seconds=30)
        self.assertEqual([item["message_id"] for item in second], [message])
        self.assertEqual(second[0]["lease_owner"], "sender-b")
        self.assertGreater(second[0]["lease_version"], token_a)
        # 旧进程迟到的确认、失败上报和续租都不能覆盖新持有人
        with self.assertRaises(ConflictError):
            app_t1.outbox.complete(message, owner="sender-a", lease_version=token_a)
        with self.assertRaises(ConflictError):
            app_t1.outbox.fail(message, owner="sender-a", lease_version=token_a, error="迟到")
        with self.assertRaises(ConflictError):
            app_t1.outbox.renew(message, owner="sender-a", lease_version=token_a, seconds=30)
        # 新持有人正常完成，值守查询可见最后领取人与终态
        app_t1.outbox.complete(message, owner="sender-b", lease_version=second[0]["lease_version"])
        record = app_t1.outbox.inspect(message)
        self.assertEqual(record["status"], "delivered")
        self.assertEqual(record["lease_owner"], "sender-b")
        self.assertIsNone(record["lease_until"])
        self.assertFalse(record["lease_active"])
        self.assertFalse(record["will_deliver"])
        self.assertEqual(record["payload"]["level"], 5)

    def test_valid_lease_blocks_other_owners(self):
        app_t0 = self.open_app(T0)
        message = app_t0.outbox.enqueue(topic="alert.earthquake", aggregate_id="alert:2", payload={"level": 4})
        app_t0.outbox.lease(owner="sender-a", seconds=60)
        # 租约未过期，其他进程取不到
        app_t1 = self.open_app(T0_PLUS_30S)
        self.assertEqual(app_t1.outbox.lease(owner="sender-b"), [])
        record = app_t1.outbox.inspect(message)
        self.assertEqual(record["status"], "leased")
        self.assertEqual(record["lease_owner"], "sender-a")
        self.assertTrue(record["lease_active"])
        self.assertTrue(record["will_deliver"])

    def test_renew_extends_lease_and_requires_token(self):
        app_t0 = self.open_app(T0)
        message = app_t0.outbox.enqueue(topic="alert.earthquake", aggregate_id="alert:3", payload={"level": 3})
        leased = app_t0.outbox.lease(owner="sender-a", seconds=30)[0]
        renewed_until = app_t0.outbox.renew(message, owner="sender-a", lease_version=leased["lease_version"], seconds=90)
        self.assertEqual(renewed_until, "2026-10-01T00:01:30Z")
        # 原租约本已过期的时间点，续租后仍不可回收
        app_t1 = self.open_app(T0_PLUS_31S)
        self.assertEqual(app_t1.outbox.lease(owner="sender-b"), [])
        # 错误的持有人或版本不能续租
        with self.assertRaises(ConflictError):
            app_t1.outbox.renew(message, owner="sender-b", lease_version=leased["lease_version"], seconds=90)
        with self.assertRaises(ConflictError):
            app_t1.outbox.renew(message, owner="sender-a", lease_version=leased["lease_version"] + 99, seconds=90)
        # 持有人在续租后的有效期内仍可完成
        app_t1.outbox.complete(message, owner="sender-a", lease_version=leased["lease_version"])
        self.assertEqual(app_t1.outbox.inspect(message)["status"], "delivered")

    def test_fail_requeues_and_eventually_dead(self):
        app = self.open_app(T0)
        message = app.outbox.enqueue(topic="alert.earthquake", aggregate_id="alert:4", payload={"level": 2})
        for attempt in range(1, 6):
            leased = app.outbox.lease(owner="sender-a", seconds=30)
            self.assertEqual([item["message_id"] for item in leased], [message])
            self.assertEqual(leased[0]["attempts"], attempt)
            app.outbox.fail(message, owner="sender-a", lease_version=leased[0]["lease_version"], error="网关超时")
            record = app.outbox.inspect(message)
            if attempt < 5:
                self.assertEqual(record["status"], "failed")
                self.assertTrue(record["will_deliver"])
            else:
                self.assertEqual(record["status"], "dead")
                self.assertFalse(record["will_deliver"])
        # 进入 dead 后不再被领取；重试原因与最后领取人可供值守查询
        self.assertEqual(app.outbox.lease(owner="sender-b"), [])
        record = app.outbox.inspect(message)
        self.assertEqual(record["attempts"], 5)
        self.assertEqual(record["last_error"], "网关超时")
        self.assertEqual(record["lease_owner"], "sender-a")

    def test_duplicate_complete_and_fail_after_complete_rejected(self):
        app = self.open_app(T0)
        message = app.outbox.enqueue(topic="alert.earthquake", aggregate_id="alert:5", payload={"level": 1})
        leased = app.outbox.lease(owner="sender-a")[0]
        app.outbox.complete(message, owner="sender-a", lease_version=leased["lease_version"])
        with self.assertRaises(ConflictError):
            app.outbox.complete(message, owner="sender-a", lease_version=leased["lease_version"])
        with self.assertRaises(ConflictError):
            app.outbox.fail(message, owner="sender-a", lease_version=leased["lease_version"])
        record = app.outbox.inspect(message)
        self.assertEqual(record["status"], "delivered")
        self.assertIsNotNone(record["delivered_at"])

    def test_lease_survives_restart(self):
        """服务重启后租约与令牌来自数据库而非进程内存。"""
        app_t0 = self.open_app(T0)
        message = app_t0.outbox.enqueue(topic="alert.earthquake", aggregate_id="alert:6", payload={"level": 5})
        leased = app_t0.outbox.lease(owner="sender-a", seconds=30)[0]
        restarted = self.open_app(T0)
        self.assertEqual(restarted.outbox.lease(owner="sender-b"), [])
        restarted.outbox.complete(message, owner="sender-a", lease_version=leased["lease_version"])
        self.assertEqual(restarted.outbox.inspect(message)["status"], "delivered")

    def test_lease_argument_validation(self):
        app = self.open_app(T0)
        message = app.outbox.enqueue(topic="alert.earthquake", aggregate_id="alert:7", payload={})
        with self.assertRaises(ValidationError):
            app.outbox.lease(owner=" ")
        with self.assertRaises(ValidationError):
            app.outbox.lease(owner="sender-a", seconds=0)
        with self.assertRaises(ValidationError):
            app.outbox.lease(owner="sender-a", limit=0)
        with self.assertRaises(NotFoundError):
            app.outbox.complete("msg-missing", owner="sender-a", lease_version=1)
        with self.assertRaises(NotFoundError):
            app.outbox.fail("msg-missing", owner="sender-a", lease_version=1)
        with self.assertRaises(NotFoundError):
            app.outbox.renew("msg-missing", owner="sender-a", lease_version=1)
        with self.assertRaises(NotFoundError):
            app.outbox.inspect("msg-missing")
        # 从未被领取的消息没有可续租的租约
        with self.assertRaises(ConflictError):
            app.outbox.renew(message, owner="sender-a", lease_version=0)


class JobLeaseTest(LeaseTestBase):
    def _schedule(self, app: CivicFlow, subject: str) -> str:
        return app.jobs.schedule(job_type="叫应跟进", subject_id=subject, run_at="2026-10-01T00:00:00Z", payload={"kind": "call"})

    def test_expired_job_lease_reclaimed_and_stale_finish_rejected(self):
        """叫应任务执行进程崩溃后，租约过期必须能被新进程回收。"""
        app_t0 = self.open_app(T0)
        job = self._schedule(app_t0, "alert:1")
        first = app_t0.jobs.claim_due(owner="responder-a", seconds=30)
        self.assertEqual([item["job_id"] for item in first], [job])
        token_a = first[0]["lease_version"]
        # responder-a 崩溃；租约过期后新进程回收同一任务
        app_t1 = self.open_app(T0_PLUS_31S)
        second = app_t1.jobs.claim_due(owner="responder-b", seconds=30)
        self.assertEqual([item["job_id"] for item in second], [job])
        self.assertEqual(second[0]["lease_owner"], "responder-b")
        self.assertGreater(second[0]["lease_version"], token_a)
        # 旧进程迟到的完成、重试和续租都不生效
        with self.assertRaises(ConflictError):
            app_t1.jobs.finish(job, owner="responder-a", lease_version=token_a)
        with self.assertRaises(ConflictError):
            app_t1.jobs.retry(job, owner="responder-a", lease_version=token_a, error="迟到", retry_at=T0_PLUS_60S)
        with self.assertRaises(ConflictError):
            app_t1.jobs.renew(job, owner="responder-a", lease_version=token_a, seconds=30)
        app_t1.jobs.finish(job, owner="responder-b", lease_version=second[0]["lease_version"])
        record = app_t1.jobs.inspect(job)
        self.assertEqual(record["status"], "succeeded")
        self.assertEqual(record["lease_owner"], "responder-b")
        self.assertFalse(record["will_run"])

    def test_running_job_not_double_claimed(self):
        app_t0 = self.open_app(T0)
        job = self._schedule(app_t0, "alert:2")
        app_t0.jobs.claim_due(owner="responder-a", seconds=60)
        app_t1 = self.open_app(T0_PLUS_30S)
        self.assertEqual(app_t1.jobs.claim_due(owner="responder-b"), [])
        record = app_t1.jobs.inspect(job)
        self.assertEqual(record["status"], "running")
        self.assertEqual(record["lease_owner"], "responder-a")
        self.assertTrue(record["lease_active"])

    def test_retry_records_reason_and_terminal_failure(self):
        app = self.open_app(T0)
        job = self._schedule(app, "alert:3")
        for attempt in range(1, 6):
            run_time = T0 if attempt == 1 else T0_PLUS_60S
            claimed = self.open_app(run_time).jobs.claim_due(owner="responder-a", seconds=30)
            self.assertEqual([item["job_id"] for item in claimed], [job])
            self.assertEqual(claimed[0]["attempt"], attempt)
            self.open_app(run_time).jobs.retry(job, owner="responder-a", lease_version=claimed[0]["lease_version"], error="叫应无应答", retry_at=T0_PLUS_60S)
            record = self.open_app(run_time).jobs.inspect(job)
            if attempt < 5:
                self.assertEqual(record["status"], "retry")
                self.assertTrue(record["will_run"])
                # 未到重试时间不会被领取
                self.assertEqual(self.open_app(T0_PLUS_31S).jobs.claim_due(owner="responder-b"), [])
            else:
                self.assertEqual(record["status"], "failed")
                self.assertFalse(record["will_run"])
        # 终态后不再被领取；重试原因保留供值守查询
        self.assertEqual(self.open_app(T0_PLUS_90S).jobs.claim_due(owner="responder-b"), [])
        record = self.open_app(T0_PLUS_90S).jobs.inspect(job)
        self.assertEqual(record["attempt"], 5)
        self.assertEqual(record["last_error"], "叫应无应答")
        self.assertEqual(record["lease_owner"], "responder-a")

    def test_job_renew_extends_lease(self):
        app_t0 = self.open_app(T0)
        job = self._schedule(app_t0, "alert:4")
        claimed = app_t0.jobs.claim_due(owner="responder-a", seconds=30)[0]
        renewed_until = app_t0.jobs.renew(job, owner="responder-a", lease_version=claimed["lease_version"], seconds=90)
        self.assertEqual(renewed_until, "2026-10-01T00:01:30Z")
        app_t1 = self.open_app(T0_PLUS_31S)
        self.assertEqual(app_t1.jobs.claim_due(owner="responder-b"), [])
        with self.assertRaises(ConflictError):
            app_t1.jobs.renew(job, owner="responder-b", lease_version=claimed["lease_version"], seconds=90)
        app_t1.jobs.finish(job, owner="responder-a", lease_version=claimed["lease_version"])
        self.assertEqual(app_t1.jobs.inspect(job)["status"], "succeeded")

    def test_job_argument_validation(self):
        app = self.open_app(T0)
        job = self._schedule(app, "alert:5")
        with self.assertRaises(ValidationError):
            app.jobs.claim_due(owner="")
        with self.assertRaises(ValidationError):
            app.jobs.claim_due(owner="responder-a", seconds=0)
        with self.assertRaises(NotFoundError):
            app.jobs.finish("job-missing", owner="responder-a", lease_version=1)
        with self.assertRaises(NotFoundError):
            app.jobs.retry("job-missing", owner="responder-a", lease_version=1, error="x", retry_at=T0)
        with self.assertRaises(NotFoundError):
            app.jobs.inspect("job-missing")
        # 未领取的任务没有可续租的租约
        with self.assertRaises(ConflictError):
            app.jobs.renew(job, owner="responder-a", lease_version=0)


class MigrationTest(LeaseTestBase):
    def _build_old_database(self):
        connection = sqlite3.connect(self.path)
        try:
            connection.executescript(OLD_SCHEMA)
            # 待发送、卡死（ leased 且租约已过期）、有效租约、已送达
            connection.execute("INSERT INTO outbox_messages VALUES('msg-pending','alert','alert:1','{}','2026-09-30T23:00:00Z',NULL,0,'pending',NULL)")
            connection.execute("INSERT INTO outbox_messages VALUES('msg-stuck','alert','alert:2','{}','2026-09-30T23:00:00Z','2026-09-30T23:30:00Z',2,'leased',NULL)")
            connection.execute("INSERT INTO outbox_messages VALUES('msg-held','alert','alert:3','{}','2026-09-30T23:00:00Z','2026-10-01T01:00:00Z',1,'leased',NULL)")
            connection.execute("INSERT INTO outbox_messages VALUES('msg-done','alert','alert:4','{}','2026-09-30T23:00:00Z',NULL,1,'delivered','2026-09-30T23:05:00Z')")
            # 等待执行、卡死（ running 且租约已过期）、有效租约、已成功
            connection.execute("INSERT INTO scheduled_jobs VALUES('job-waiting','叫应','alert:1','2026-09-30T23:00:00Z','{}','waiting',0,NULL,'')")
            connection.execute("INSERT INTO scheduled_jobs VALUES('job-stuck','叫应','alert:2','2026-09-30T23:00:00Z','{}','running',1,'2026-09-30T23:30:00Z','')")
            connection.execute("INSERT INTO scheduled_jobs VALUES('job-held','叫应','alert:3','2026-09-30T23:00:00Z','{}','running',1,'2026-10-01T01:00:00Z','')")
            connection.execute("INSERT INTO scheduled_jobs VALUES('job-done','叫应','alert:4','2026-09-30T23:00:00Z','{}','succeeded',1,NULL,'')")
            connection.commit()
        finally:
            connection.close()

    def test_old_database_upgrades_in_place(self):
        self._build_old_database()
        app = self.open_app(T0)
        # 迁移补齐新列
        with app.database.connect() as connection:
            outbox_columns = {row["name"] for row in connection.execute("PRAGMA table_info(outbox_messages)")}
            job_columns = {row["name"] for row in connection.execute("PRAGMA table_info(scheduled_jobs)")}
        self.assertTrue({"lease_owner", "lease_version", "last_error"} <= outbox_columns)
        self.assertTrue({"lease_owner", "lease_version"} <= job_columns)
        # 既有行获得默认值：从未被新机制领取
        stuck = app.outbox.inspect("msg-stuck")
        self.assertEqual(stuck["lease_version"], 0)
        self.assertIsNone(stuck["lease_owner"])
        self.assertEqual(stuck["attempts"], 2)
        # 卡死的预警与任务立即可被回收；有效租约与终态不受影响
        leased = app.outbox.lease(owner="sender-new", seconds=30)
        self.assertEqual([item["message_id"] for item in leased], ["msg-pending", "msg-stuck"])
        reclaimed = next(item for item in leased if item["message_id"] == "msg-stuck")
        self.assertEqual(reclaimed["lease_version"], 1)
        self.assertEqual(reclaimed["attempts"], 3)
        claimed = app.jobs.claim_due(owner="responder-new", seconds=30)
        self.assertEqual([item["job_id"] for item in claimed], ["job-stuck", "job-waiting"])
        # 回收后可以正常完成；已送达记录保持原语义
        app.outbox.complete("msg-stuck", owner="sender-new", lease_version=1)
        self.assertEqual(app.outbox.inspect("msg-stuck")["status"], "delivered")
        self.assertEqual(app.outbox.inspect("msg-done")["status"], "delivered")
        self.assertEqual(app.outbox.inspect("msg-done")["delivered_at"], "2026-09-30T23:05:00Z")
        self.assertEqual(app.jobs.inspect("job-done")["status"], "succeeded")
        # 重复打开同一数据库，迁移幂等
        again = self.open_app(T0)
        self.assertEqual(again.outbox.inspect("msg-stuck")["status"], "delivered")


class ConcurrencyTest(LeaseTestBase):
    def _race(self, worker):
        errors = []
        threads = [threading.Thread(target=worker, args=(f"worker-{i}",), kwargs={"errors": errors}) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

    def test_concurrent_outbox_lease_race(self):
        """双进程（多线程模拟）争抢：每条预警只被一个进程领到，且不遗漏。"""
        app = self.open_app(T0)
        messages = [app.outbox.enqueue(topic="alert.earthquake", aggregate_id=f"alert:{i}", payload={"i": i}) for i in range(8)]
        collected, lock = [], threading.Lock()

        def worker(owner, errors):
            empties = 0
            while empties < 5:
                try:
                    batch = app.outbox.lease(owner=owner, limit=2)
                except Exception as exc:  # pragma: no cover - 失败时由断言报告
                    errors.append(exc)
                    return
                if not batch:
                    empties += 1
                    continue
                empties = 0
                with lock:
                    collected.extend(item["message_id"] for item in batch)

        self._race(worker)
        self.assertEqual(sorted(collected), sorted(messages))

    def test_concurrent_job_claim_race(self):
        """双进程争抢定时任务：每个任务只被一个进程领到，且不遗漏。"""
        app = self.open_app(T0)
        jobs = [app.jobs.schedule(job_type="叫应", subject_id=f"alert:{i}", run_at="2026-10-01T00:00:00Z", payload={"i": i}) for i in range(8)]
        collected, lock = [], threading.Lock()

        def worker(owner, errors):
            empties = 0
            while empties < 5:
                try:
                    batch = app.jobs.claim_due(owner=owner, limit=2)
                except Exception as exc:  # pragma: no cover - 失败时由断言报告
                    errors.append(exc)
                    return
                if not batch:
                    empties += 1
                    continue
                empties = 0
                with lock:
                    collected.extend(item["job_id"] for item in batch)

        self._race(worker)
        self.assertEqual(sorted(collected), sorted(jobs))

    def test_handover_then_old_owner_cannot_interfere(self):
        """租约交接后，旧持有人的任何迟到操作都得到一致的拒绝结果。"""
        app_t0 = self.open_app(T0)
        message = app_t0.outbox.enqueue(topic="alert.earthquake", aggregate_id="alert:handover", payload={})
        token_a = app_t0.outbox.lease(owner="sender-a", seconds=30)[0]["lease_version"]
        app_t1 = self.open_app(T0_PLUS_31S)
        token_b = app_t1.outbox.lease(owner="sender-b", seconds=30)[0]["lease_version"]
        app_t1.outbox.fail(message, owner="sender-b", lease_version=token_b, error="通道故障")
        # 旧持有人迟到确认不得把已回队列的消息标记为送达
        with self.assertRaises(ConflictError):
            app_t1.outbox.complete(message, owner="sender-a", lease_version=token_a)
        record = app_t1.outbox.inspect(message)
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["last_error"], "通道故障")
        self.assertTrue(record["will_deliver"])
        # 新进程可再次领取并完成
        token_c = app_t1.outbox.lease(owner="sender-c", seconds=30)[0]["lease_version"]
        app_t1.outbox.complete(message, owner="sender-c", lease_version=token_c)
        self.assertEqual(app_t1.outbox.inspect(message)["status"], "delivered")


if __name__ == "__main__":
    unittest.main()
