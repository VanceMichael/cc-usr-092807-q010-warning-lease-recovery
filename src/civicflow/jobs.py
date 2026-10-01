"""可恢复的定时任务队列。

租约语义与通知发件箱一致：
- claim_due 领取时持久化 lease_owner 与 lease_version（fencing 令牌，单调递增）。
- 状态为 running 且 lease_until 已过期的任务可以被其他工作进程回收重领。
- finish/retry/renew 必须携带当前租约的 owner 与 lease_version，且租约未过期；
  旧进程迟到的确认因版本不匹配而被拒绝，不会覆盖新进程的处理结果。
- lease_until 为 NULL 表示当前没有有效租约；lease_owner 保留最后一次领取人。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id
from .jsonutil import canonical_json, parse_object
from .timeutil import Clock, canonical_instant, parse_instant

MAX_ATTEMPTS = 5


def _validate_lease_args(owner: str, seconds: int, limit: int | None = None) -> None:
    if not isinstance(owner, str) or not owner.strip():
        raise ValidationError("租约持有人不能为空")
    if seconds < 1 or (limit is not None and limit < 1):
        raise ValidationError("租约参数不合法")


@dataclass(frozen=True)
class JobQueue:
    database: Database
    clock: Clock

    def schedule(self, *, job_type: str, subject_id: str, run_at: str, payload: dict) -> str:
        job_id = new_id("job"); run_at = canonical_instant(run_at)
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO scheduled_jobs(job_id,job_type,subject_id,run_at,payload_json,status) VALUES(?,?,?,?,?,'waiting')", (job_id, job_type, subject_id, run_at, canonical_json(payload)))
        return job_id

    def claim_due(self, *, owner: str, seconds: int = 30, limit: int = 20) -> list[dict]:
        """领取到期任务；租约过期的 running 任务会被安全回收。"""
        _validate_lease_args(owner, seconds, limit)
        now = self.clock.now(); lease_until = (parse_instant(now) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            rows = connection.execute("SELECT * FROM scheduled_jobs WHERE run_at<=? AND status IN ('waiting','retry','running') AND (lease_until IS NULL OR lease_until<?) ORDER BY run_at,job_id LIMIT ?", (now, now, limit)).fetchall()
            result = []
            for row in rows:
                changed = connection.execute("UPDATE scheduled_jobs SET status='running',lease_until=?,lease_owner=?,lease_version=lease_version+1,attempt=attempt+1 WHERE job_id=? AND status IN ('waiting','retry','running') AND (lease_until IS NULL OR lease_until<?)", (lease_until, owner, row["job_id"], now)).rowcount
                if changed:
                    item = dict(row)
                    item["status"] = "running"; item["lease_owner"] = owner; item["lease_until"] = lease_until
                    item["lease_version"] = row["lease_version"] + 1; item["attempt"] = row["attempt"] + 1
                    result.append(item)
            return result

    def renew(self, job_id: str, *, owner: str, lease_version: int, seconds: int = 30) -> str:
        """续租：仅当前租约持有人可延长 lease_until，返回新的到期时间。"""
        _validate_lease_args(owner, seconds)
        lease_until = (parse_instant(self.clock.now()) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            self._require_job(connection, job_id)
            changed = connection.execute("UPDATE scheduled_jobs SET lease_until=? WHERE job_id=? AND status='running' AND lease_owner=? AND lease_version=? AND lease_until IS NOT NULL AND lease_until>?", (lease_until, job_id, owner, lease_version, self.clock.now())).rowcount
            if changed != 1:
                raise ConflictError("任务没有有效运行租约")
        return lease_until

    def finish(self, job_id: str, *, owner: str, lease_version: int) -> None:
        with self.database.transaction() as connection:
            self._require_job(connection, job_id)
            changed = connection.execute("UPDATE scheduled_jobs SET status='succeeded',lease_until=NULL,last_error='' WHERE job_id=? AND status='running' AND lease_owner=? AND lease_version=? AND lease_until IS NOT NULL AND lease_until>?", (job_id, owner, lease_version, self.clock.now())).rowcount
            if changed != 1:
                raise ConflictError("任务没有有效运行租约")

    def retry(self, job_id: str, *, owner: str, lease_version: int, error: str, retry_at: str) -> None:
        retry_at = canonical_instant(retry_at)
        with self.database.transaction() as connection:
            row = self._require_job(connection, job_id)
            status = "failed" if row["attempt"] >= MAX_ATTEMPTS else "retry"
            changed = connection.execute("UPDATE scheduled_jobs SET status=?,run_at=?,lease_until=NULL,last_error=? WHERE job_id=? AND status='running' AND lease_owner=? AND lease_version=? AND lease_until IS NOT NULL AND lease_until>?", (status, retry_at, error[:500], job_id, owner, lease_version, self.clock.now())).rowcount
            if changed != 1:
                raise ConflictError("任务没有有效运行租约")

    def inspect(self, job_id: str) -> dict:
        """值守查询：是否仍会执行、由谁处理、为何进入重试。"""
        with self.database.transaction(immediate=False) as connection:
            row = connection.execute("SELECT * FROM scheduled_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not row:
            raise NotFoundError("任务不存在")
        record = dict(row)
        record["payload"] = parse_object(record.pop("payload_json"))
        lease_until = record["lease_until"]
        record["lease_active"] = bool(lease_until) and parse_instant(lease_until) > parse_instant(self.clock.now())
        record["will_run"] = record["status"] in ("waiting", "retry", "running")
        return record

    @staticmethod
    def _require_job(connection, job_id: str):
        row = connection.execute("SELECT * FROM scheduled_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not row:
            raise NotFoundError("任务不存在")
        return row
