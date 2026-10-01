"""可靠通知发件箱。

租约语义：
- lease 领取时持久化 lease_owner 与 lease_version（ fencing 令牌，单调递增）。
- 状态为 leased 且 lease_until 已过期的消息可以被其他工作进程回收重领。
- complete/fail/renew 必须携带当前租约的 owner 与 lease_version，且租约未过期；
  旧进程迟到的确认因版本不匹配而被拒绝，不会覆盖新进程的处理结果。
- lease_until 为 NULL 表示当前没有有效租约；lease_owner 保留最后一次领取人，
  便于值守界面判断“谁最后领取”。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id
from .jsonutil import canonical_json, parse_object
from .timeutil import Clock, parse_instant

MAX_ATTEMPTS = 5


def _validate_lease_args(owner: str, seconds: int, limit: int | None = None) -> None:
    if not isinstance(owner, str) or not owner.strip():
        raise ValidationError("租约持有人不能为空")
    if seconds < 1 or (limit is not None and limit < 1):
        raise ValidationError("租约参数不合法")


@dataclass(frozen=True)
class Outbox:
    database: Database
    clock: Clock

    def enqueue(self, *, topic: str, aggregate_id: str, payload: dict, available_at: str | None = None) -> str:
        message_id = new_id("msg"); available_at = available_at or self.clock.now()
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO outbox_messages(message_id,topic,aggregate_id,payload_json,available_at,status) VALUES(?,?,?,?,?,?)", (message_id, topic, aggregate_id, canonical_json(payload), available_at, "pending"))
        return message_id

    def lease(self, *, owner: str, seconds: int = 30, limit: int = 20) -> list[dict]:
        """领取到期消息；租约过期的 leased 消息会被安全回收。"""
        _validate_lease_args(owner, seconds, limit)
        now = self.clock.now(); lease_until = (parse_instant(now) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            rows = connection.execute("SELECT * FROM outbox_messages WHERE status IN ('pending','failed','leased') AND available_at<=? AND (lease_until IS NULL OR lease_until<?) ORDER BY available_at,message_id LIMIT ?", (now, now, limit)).fetchall()
            result = []
            for row in rows:
                changed = connection.execute("UPDATE outbox_messages SET status='leased',lease_until=?,lease_owner=?,lease_version=lease_version+1,attempts=attempts+1 WHERE message_id=? AND status IN ('pending','failed','leased') AND (lease_until IS NULL OR lease_until<?)", (lease_until, owner, row["message_id"], now)).rowcount
                if changed:
                    item = dict(row)
                    item["status"] = "leased"; item["lease_owner"] = owner; item["lease_until"] = lease_until
                    item["lease_version"] = row["lease_version"] + 1; item["attempts"] = row["attempts"] + 1
                    result.append(item)
            return result

    def renew(self, message_id: str, *, owner: str, lease_version: int, seconds: int = 30) -> str:
        """续租：仅当前租约持有人可延长 lease_until，返回新的到期时间。"""
        _validate_lease_args(owner, seconds)
        lease_until = (parse_instant(self.clock.now()) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            self._require_message(connection, message_id)
            changed = connection.execute("UPDATE outbox_messages SET lease_until=? WHERE message_id=? AND status='leased' AND lease_owner=? AND lease_version=? AND lease_until IS NOT NULL AND lease_until>?", (lease_until, message_id, owner, lease_version, self.clock.now())).rowcount
            if changed != 1:
                raise ConflictError("消息没有有效租约")
        return lease_until

    def complete(self, message_id: str, *, owner: str, lease_version: int) -> None:
        with self.database.transaction() as connection:
            self._require_message(connection, message_id)
            changed = connection.execute("UPDATE outbox_messages SET status='delivered',delivered_at=?,lease_until=NULL,last_error='' WHERE message_id=? AND status='leased' AND lease_owner=? AND lease_version=? AND lease_until IS NOT NULL AND lease_until>?", (self.clock.now(), message_id, owner, lease_version, self.clock.now())).rowcount
            if changed != 1:
                raise ConflictError("消息没有有效租约")

    def fail(self, message_id: str, *, owner: str, lease_version: int, error: str = "") -> None:
        with self.database.transaction() as connection:
            row = self._require_message(connection, message_id)
            status = "dead" if row["attempts"] >= MAX_ATTEMPTS else "failed"
            changed = connection.execute("UPDATE outbox_messages SET status=?,lease_until=NULL,last_error=? WHERE message_id=? AND status='leased' AND lease_owner=? AND lease_version=? AND lease_until IS NOT NULL AND lease_until>?", (status, error[:500], message_id, owner, lease_version, self.clock.now())).rowcount
            if changed != 1:
                raise ConflictError("消息没有有效租约")

    def inspect(self, message_id: str) -> dict:
        """值守查询：是否仍会送达、由谁处理、为何重试。"""
        with self.database.transaction(immediate=False) as connection:
            row = connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
        if not row:
            raise NotFoundError("消息不存在")
        record = dict(row)
        record["payload"] = parse_object(record.pop("payload_json"))
        lease_until = record["lease_until"]
        record["lease_active"] = bool(lease_until) and parse_instant(lease_until) > parse_instant(self.clock.now())
        record["will_deliver"] = record["status"] in ("pending", "failed", "leased")
        return record

    @staticmethod
    def _require_message(connection, message_id: str):
        row = connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
        if not row:
            raise NotFoundError("消息不存在")
        return row
