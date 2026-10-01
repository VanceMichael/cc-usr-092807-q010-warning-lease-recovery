"""可靠通知发件箱。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id
from .jsonutil import canonical_json
from .timeutil import Clock, canonical_instant, parse_instant

MAX_ATTEMPTS = 5
TERMINAL_STATES = ("delivered", "dead")
# 崩溃进程遗留的 leased 记录在租约过期后重新可被领取。
CLAIMABLE = "(status IN ('pending','failed') OR (status='leased' AND (lease_until IS NULL OR lease_until<=?)))"


def _require_owner(owner: str) -> str:
    if not isinstance(owner, str) or not owner.strip():
        raise ValidationError("租约持有人不能为空")
    return owner.strip()


def _require_lease_version(lease_version: int) -> int:
    if isinstance(lease_version, bool) or not isinstance(lease_version, int) or lease_version < 0:
        raise ValidationError("租约版本不合法")
    return lease_version


@dataclass(frozen=True)
class Outbox:
    database: Database
    clock: Clock

    def enqueue(self, *, topic: str, aggregate_id: str, payload: dict, available_at: str | None = None) -> str:
        message_id = new_id("msg"); available_at = canonical_instant(available_at) if available_at else self.clock.now()
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO outbox_messages(message_id,topic,aggregate_id,payload_json,available_at,status) VALUES(?,?,?,?,?,'pending')", (message_id, topic, aggregate_id, canonical_json(payload), available_at))
        return message_id

    def lease(self, *, owner: str, seconds: int = 30, limit: int = 20) -> list[dict]:
        owner = _require_owner(owner)
        if seconds < 1 or limit < 1:
            raise ValidationError("租约参数不合法")
        now = self.clock.now(); lease_until = (parse_instant(now) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            rows = connection.execute(f"SELECT * FROM outbox_messages WHERE available_at<=? AND {CLAIMABLE} ORDER BY available_at,message_id LIMIT ?", (now, now, limit)).fetchall()
            result = []
            for row in rows:
                changed = connection.execute(f"UPDATE outbox_messages SET status='leased',lease_owner=?,lease_until=?,lease_version=lease_version+1,attempts=attempts+1 WHERE message_id=? AND available_at<=? AND {CLAIMABLE}", (owner, lease_until, row["message_id"], now, now)).rowcount
                if changed:
                    result.append(dict(connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (row["message_id"],)).fetchone()))
            return result

    def complete(self, message_id: str, *, owner: str, lease_version: int) -> None:
        owner = _require_owner(owner); lease_version = _require_lease_version(lease_version)
        with self.database.transaction() as connection:
            self._require_valid_lease(connection, message_id, owner=owner, lease_version=lease_version)
            connection.execute("UPDATE outbox_messages SET status='delivered',delivered_at=?,lease_until=NULL,last_error='' WHERE message_id=?", (self.clock.now(), message_id))

    def fail(self, message_id: str, *, owner: str, lease_version: int, error: str = "") -> None:
        owner = _require_owner(owner); lease_version = _require_lease_version(lease_version)
        with self.database.transaction() as connection:
            row = self._require_valid_lease(connection, message_id, owner=owner, lease_version=lease_version)
            status = "dead" if row["attempts"] >= MAX_ATTEMPTS else "failed"
            connection.execute("UPDATE outbox_messages SET status=?,lease_until=NULL,last_error=? WHERE message_id=?", (status, str(error).strip()[:500], message_id))

    def renew(self, message_id: str, *, owner: str, lease_version: int, seconds: int = 30) -> str:
        owner = _require_owner(owner); lease_version = _require_lease_version(lease_version)
        if seconds < 1:
            raise ValidationError("租约参数不合法")
        lease_until = (parse_instant(self.clock.now()) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            self._require_valid_lease(connection, message_id, owner=owner, lease_version=lease_version)
            connection.execute("UPDATE outbox_messages SET lease_until=? WHERE message_id=?", (lease_until, message_id))
        return lease_until

    def describe(self, message_id: str) -> dict:
        with self.database.transaction(immediate=False) as connection:
            row = connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
        if not row:
            raise NotFoundError("消息不存在")
        item = dict(row)
        leased = item["status"] == "leased" and item["lease_until"] is not None and item["lease_until"] > self.clock.now()
        item["current_owner"] = item["lease_owner"] if leased else None
        item["will_deliver"] = item["status"] not in TERMINAL_STATES
        return item

    def _require_valid_lease(self, connection, message_id: str, *, owner: str, lease_version: int):
        row = connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
        if not row:
            raise NotFoundError("消息不存在")
        if not (row["status"] == "leased" and row["lease_owner"] == owner and row["lease_version"] == lease_version and row["lease_until"] is not None and row["lease_until"] > self.clock.now()):
            raise ConflictError("消息没有有效租约")
        return row
