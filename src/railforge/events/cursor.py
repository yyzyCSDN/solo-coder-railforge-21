from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class FeedEvent:
    partition: str
    sequence: int
    event_id: str
    payload: dict


class CursorConsumer:

    def __init__(self):
        self.cursor = {}
        self.seen = set()

    def _check(self, event: FeedEvent, last: int):
        if event.event_id in self.seen:
            return False
        if event.sequence <= last:
            return False
        if event.sequence != last + 1:
            raise ValueError('cursor gap')
        return True

    def peek(self, event: FeedEvent) -> bool:
        """Return whether accept() would take this event, without moving the cursor."""
        return self._check(event, self.cursor.get(event.partition, 0))

    def accept(self, event: FeedEvent):
        last = self.cursor.get(event.partition, 0)
        if not self._check(event, last):
            return False
        self.cursor[event.partition] = event.sequence
        self.seen.add(event.event_id)
        return True

    def snapshot(self):
        return dict(self.cursor), set(self.seen)

    def restore(self, state) -> None:
        cursor, seen = state
        self.cursor = dict(cursor)
        self.seen = set(seen)
