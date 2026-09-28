"""
Task priority. See docs/rabbitmq.md (added in Phase 4) for how this
maps onto queue routing, and docs/architecture.md for the
priority-vs-fairness tradeoff.
"""

from enum import StrEnum


class TaskPriority(StrEnum):
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"
