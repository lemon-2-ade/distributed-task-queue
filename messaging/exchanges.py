"""
Exchange names.

Two exchanges, two purposes:
- TASK_EXCHANGE: where the API publishes new/retried work. A direct
  exchange, routed by priority (see queues.py) -- not a fanout or
  topic exchange, because every message has exactly one queue it
  belongs in and routing is a simple exact match on priority.
- DEAD_LETTER_EXCHANGE: where messages land after a queue's
  dead-letter-exchange argument redirects them (see topology.py).
  Kept separate from TASK_EXCHANGE so "why did this message leave
  its original queue" is answerable by looking at which exchange
  routed it, rather than overloading one exchange for two purposes.
"""

TASK_EXCHANGE = "task.exchange"
DEAD_LETTER_EXCHANGE = "dead_letter.exchange"
