from __future__ import annotations

from dataclasses import dataclass


# 失败原因码：字段缺失、未知枚举、类型不合法、跨表引用未到达、业务键无效。
REASON_MISSING_FIELD = "missing_field"
REASON_UNKNOWN_ENUM = "unknown_enum"
REASON_INVALID_VALUE = "invalid_value"
REASON_REFERENCE_MISSING = "reference_missing"
REASON_INVALID_KEY = "invalid_key"
REASON_INVALID_REF = "invalid_reference"


class ContractError(ValueError):
    """记录不满足输入约定，携带稳定原因码与说明，供隔离落库。"""

    def __init__(self, reason_code: str, detail: str):
        super().__init__(detail)
        self.reason_code = reason_code
        self.reason_detail = detail


@dataclass(frozen=True)
class Event:
    source: str
    batch_id: str
    cursor: int
    business_key: str
    kind: str
    value: float
    occurred_at: str
    ref: str | None = None


def parse_event(source: str, batch_id: str, cursor: int, raw: dict) -> Event:
    required = {"business_key", "kind", "value", "occurred_at"}
    missing = required - raw.keys()
    if missing:
        raise ContractError(REASON_MISSING_FIELD, "缺少字段: " + ",".join(sorted(missing)))
    if not isinstance(raw["business_key"], str) or not raw["business_key"]:
        raise ContractError(REASON_INVALID_KEY, "业务键不能为空")
    if raw["kind"] not in {"upsert", "retract"}:
        raise ContractError(REASON_UNKNOWN_ENUM, "事件类型不受支持: " + repr(raw["kind"]))
    try:
        value = float(raw["value"])
    except (TypeError, ValueError):
        raise ContractError(REASON_INVALID_VALUE, "数值无法解析: " + repr(raw["value"]))
    occurred_at = raw["occurred_at"]
    if not isinstance(occurred_at, str) or not occurred_at:
        raise ContractError(REASON_INVALID_VALUE, "业务时间不能为空")
    ref = raw.get("ref")
    if ref is not None and (not isinstance(ref, str) or not ref):
        raise ContractError(REASON_INVALID_REF, "跨表引用不能为空")
    return Event(source, batch_id, int(cursor), raw["business_key"], raw["kind"], value, occurred_at, ref)
