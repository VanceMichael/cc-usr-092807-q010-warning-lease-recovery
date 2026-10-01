"""可恢复的定时任务队列。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id
from .jsonutil import canonical_json
from .timeutil import Clock, canonical_instant, parse_instant

MAX_ATTEMPTS = 5
TERMINAL_STATES = ("succeeded", "failed")
# 崩溃进程遗留的 running 任务在租约过期后重新可被领取。
CLAIMABLE = "(status IN ('waiting','retry') OR (status='running' AND (lease_until IS NULL OR lease_until<=?)))"


def _require_owner(owner: str) -> str:
    if not isinstance(owner, str) or not owner.strip():
        raise ValidationError("租约持有人不能为空")
    return owner.strip()


def _require_lease_version(lease_version: int) -> int:
    if isinstance(lease_version, bool) or not isinstance(lease_version, int) or lease_version < 0:
        raise ValidationError("租约版本不合法")
    return lease_version


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
        owner = _require_owner(owner)
        if seconds < 1 or limit < 1:
            raise ValidationError("租约参数不合法")
        now = self.clock.now(); lease_until = (parse_instant(now) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            rows = connection.execute(f"SELECT * FROM scheduled_jobs WHERE run_at<=? AND {CLAIMABLE} ORDER BY run_at,job_id LIMIT ?", (now, now, limit)).fetchall()
            result = []
            for row in rows:
                changed = connection.execute(f"UPDATE scheduled_jobs SET status='running',lease_owner=?,lease_until=?,lease_version=lease_version+1,attempt=attempt+1 WHERE job_id=? AND run_at<=? AND {CLAIMABLE}", (owner, lease_until, row["job_id"], now, now)).rowcount
                if changed:
                    result.append(dict(connection.execute("SELECT * FROM scheduled_jobs WHERE job_id=?", (row["job_id"],)).fetchone()))
            return result

    def finish(self, job_id: str, *, owner: str, lease_version: int) -> None:
        owner = _require_owner(owner); lease_version = _require_lease_version(lease_version)
        with self.database.transaction() as connection:
            self._require_valid_lease(connection, job_id, owner=owner, lease_version=lease_version)
            connection.execute("UPDATE scheduled_jobs SET status='succeeded',lease_until=NULL,last_error='' WHERE job_id=?", (job_id,))

    def retry(self, job_id: str, *, owner: str, lease_version: int, error: str, retry_at: str) -> None:
        owner = _require_owner(owner); lease_version = _require_lease_version(lease_version); retry_at = canonical_instant(retry_at)
        with self.database.transaction() as connection:
            row = self._require_valid_lease(connection, job_id, owner=owner, lease_version=lease_version)
            status = "failed" if row["attempt"] >= MAX_ATTEMPTS else "retry"
            connection.execute("UPDATE scheduled_jobs SET status=?,run_at=?,lease_until=NULL,last_error=? WHERE job_id=?", (status, retry_at, str(error).strip()[:500], job_id))

    def renew(self, job_id: str, *, owner: str, lease_version: int, seconds: int = 30) -> str:
        owner = _require_owner(owner); lease_version = _require_lease_version(lease_version)
        if seconds < 1:
            raise ValidationError("租约参数不合法")
        lease_until = (parse_instant(self.clock.now()) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            self._require_valid_lease(connection, job_id, owner=owner, lease_version=lease_version)
            connection.execute("UPDATE scheduled_jobs SET lease_until=? WHERE job_id=?", (lease_until, job_id))
        return lease_until

    def describe(self, job_id: str) -> dict:
        with self.database.transaction(immediate=False) as connection:
            row = connection.execute("SELECT * FROM scheduled_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not row:
            raise NotFoundError("任务不存在")
        item = dict(row)
        leased = item["status"] == "running" and item["lease_until"] is not None and item["lease_until"] > self.clock.now()
        item["current_owner"] = item["lease_owner"] if leased else None
        item["will_run"] = item["status"] not in TERMINAL_STATES
        return item

    def _require_valid_lease(self, connection, job_id: str, *, owner: str, lease_version: int):
        row = connection.execute("SELECT * FROM scheduled_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not row:
            raise NotFoundError("任务不存在")
        if not (row["status"] == "running" and row["lease_owner"] == owner and row["lease_version"] == lease_version and row["lease_until"] is not None and row["lease_until"] > self.clock.now()):
            raise ConflictError("任务没有有效运行租约")
        return row
