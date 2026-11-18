from __future__ import annotations


def next_state(current: str, dependency_ready: bool, corrected: bool) -> str:
    if current == "隔离" and corrected and dependency_ready:
        return "可重放"
    if current == "隔离" and not dependency_ready:
        return "等待引用"
    return current


def partition_gate(unresolved: int, policy: str) -> str:
    if unresolved == 0:
        return "可用"
    return "延迟" if policy == "allow_delay" else "阻断"
