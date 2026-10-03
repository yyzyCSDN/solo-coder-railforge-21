from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from collections import defaultdict


class MeetConflictError(ValueError):
    """Raised when two opposing trains cannot be kept apart on a single-track section."""

    def __init__(self, section: str, trains: tuple[str, str], delay_minutes: int):
        self.section = section
        self.trains = tuple(sorted(trains))
        self.delay_minutes = delay_minutes
        super().__init__(
            f"meet conflict on single-track section {section}: trains "
            f"{self.trains[0]} and {self.trains[1]} still oppose after {delay_minutes} delay minutes"
        )


@dataclass(frozen=True)
class Section:
    section_id: str
    run_minutes: int
    single_track: bool
    capacity_per_hour: int


@dataclass(frozen=True)
class PathRequest:
    train: str
    origin_time: datetime
    sections: tuple[str, ...]
    priority: int
    max_delay_minutes: int
    direction: int = 1


@dataclass(frozen=True)
class Slot:
    train: str
    section: str
    start: datetime
    end: datetime
    direction: int = 1


def _overlap(a, b, c, d):
    return max(a, c) < min(b, d)


def build(requests: list[PathRequest], sections: dict[str, Section], existing: list[Slot], headway_minutes: int):
    """Build a publishable path plan.

    Trains moving in the same direction may occupy the same window in
    parallel (headway only separates opposing traversals). On a single-track
    section an opposing occupancy is a meet: the lower-priority request is
    shifted minute by minute until the windows clear; if that needs more than
    its delay tolerance a MeetConflictError is raised and no slot is published
    anywhere, so the caller is never left with a half-built plan.
    """
    if headway_minutes < 0:
        raise ValueError('headway')
    planned = list(existing)
    result = {}
    for req in sorted(requests, key=lambda r: (-r.priority, r.origin_time, r.train)):
        if req.direction not in (1, -1):
            raise ValueError('direction must be 1 or -1')
        cursor = req.origin_time.astimezone(timezone.utc)
        delay = 0
        slots = []
        for sid in req.sections:
            section = sections[sid]
            start = cursor
            while True:
                end = start + timedelta(minutes=section.run_minutes)
                opposing = None
                # Only single-track sections forbid opposing windows; same
                # direction followers are allowed to run in parallel.
                if section.single_track:
                    for row in planned + slots:
                        if row.section != sid or row.direction == req.direction:
                            continue
                        guard = timedelta(minutes=headway_minutes)
                        if _overlap(start, end, row.start - guard, row.end + guard):
                            opposing = row
                            break
                if opposing is None:
                    break
                start += timedelta(minutes=1)
                delay += 1
                if delay > req.max_delay_minutes:
                    raise MeetConflictError(sid, (req.train, opposing.train), delay)
            slot = Slot(req.train, sid, start, end, req.direction)
            slots.append(slot)
            cursor = end
        planned.extend(slots)
        result[req.train] = slots
    return result


def section_load(plan):
    out = defaultdict(float)
    for slots in plan.values():
        for s in slots:
            out[s.section] += (s.end - s.start).total_seconds() / 3600
    return dict(out)


def train_delay(plan, requests):
    rm = {r.train: r for r in requests}
    out = {}
    for train, slots in plan.items():
        out[train] = 0 if not slots else max(0, (slots[0].start - rm[train].origin_time.astimezone(timezone.utc)).total_seconds() / 60)
    return out
