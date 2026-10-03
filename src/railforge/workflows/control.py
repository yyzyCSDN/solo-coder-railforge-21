from __future__ import annotations

from dataclasses import dataclass, is_dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping, TypeVar

from railforge.audit.trail import Trail
from railforge.braking.brake_percentage import brake_percentage
from railforge.billing.demurrage import charge
from railforge.cargo.segregation import violations
from railforge.consist.integrity import validate
from railforge.customs.holds import active
from railforge.energy.regen import net_energy
from railforge.eta.event_fusion import fused_delay
from railforge.events.cursor import CursorConsumer, FeedEvent
from railforge.events.stream import EventStream
from railforge.intermodal.connections import feasible
from railforge.inventory.wagon_pool import allocate
from railforge.locomotive.tractive_effort import available_effort
from railforge.maintenance.release import releasable
from railforge.planning.train_path_engine import (
    MeetConflictError,
    build as build_paths,
    section_load,
    train_delay,
)
from railforge.possession.windows import conflicts as possession_conflicts
from railforge.routing.clearance import route_ok
from railforge.signaling.block_occupancy import reserve
from railforge.storage.ops_store import VersionedStore
from railforge.timetable.meets import conflicts as meet_conflicts
from railforge.wagon.axle_load import route_ok as axle_route_ok
from railforge.yard.hump import classify
from railforge.yard.switching import lock_route
from railforge.crew.duty import legal as duty_legal

T = TypeVar("T")


def _safe(value: Any) -> Any:
    if is_dataclass(value):
        return {k: _safe(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_safe(v) for v in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


@dataclass(frozen=True)
class OperationReceipt:
    operation_id: str
    topic: str
    subject: str
    result: Any
    event: Any
    audit_index: int
    version: int


class RailForgeWorkflowControl:
    """Atomic domain -> event -> audit workflow used by every feature surface."""

    def __init__(self, stream: EventStream | None = None, trail: Trail | None = None,
                 store: VersionedStore | None = None):
        self.stream = stream or EventStream()
        self.trail = trail or Trail()
        self.store = store or VersionedStore()
        self._receipts: dict[str, OperationReceipt] = {}
        self._consumers: dict[str, CursorConsumer] = {}

    @property
    def receipts(self) -> Mapping[str, OperationReceipt]:
        return dict(self._receipts)

    def _commit(self, operation_id: str, topic: str, subject: str, at: datetime,
                action: Callable[[], T], evidence: Mapping[str, Any] | None = None,
                staged: Callable[[], list[tuple[Any, Callable[[Any], None]]]] | None = None,
                expected_version: int | None = None) -> OperationReceipt:
        """Run one atomic commit.

        The domain decision runs first; when it rejects, nothing is published.
        Only then do the side effects run, as a staged transaction: if any
        later effect fails (including the optimistic version check), every
        earlier effect is rolled back, so a rejected operation can never leave
        a half-written plan, event or cursor.
        """
        previous = self._receipts.get(operation_id)
        if previous is not None:
            if previous.topic != topic or previous.subject != subject:
                raise ValueError("operation id reused for a different workflow")
            return previous

        # 1. Pure domain decision. Any exception here leaves the world untouched.
        result = action()

        # 2. Capture rollback state for every staged mutating effect.
        effects = list(staged()) if staged is not None else []
        snapshots = [target.snapshot() for target, _apply in effects]
        prior_value = self.store.read(subject)

        payload = {
            "operation_id": operation_id,
            "status": "committed",
            "control_path": "atomic",
            "expected_version": expected_version,
            "result": _safe(result),
            **dict(evidence or {}),
        }

        # 3. Versioned publication: optimistic check + state + event run as one
        #    staged transaction. Any failure unwinds every effect already taken,
        #    so a rejected operation never leaves a partial plan behind.
        event = None
        try:
            version = self.store.write(subject, _safe(result), expected_version=expected_version)
            payload["version"] = version
            event = self.stream.publish(operation_id, at, topic, subject, payload)
            for target, apply_effect in effects:
                apply_effect(target)
        except BaseException:
            for (target, _apply), state in zip(effects, snapshots):
                target.restore(state)
            if event is not None:
                self.stream.retract_tail(event)
            self.store.restore(subject, prior_value)
            raise

        audit = self.trail.append("railforge-control", topic, subject, at)
        receipt = OperationReceipt(operation_id, topic, subject, result, event, audit.index, version)
        self._receipts[operation_id] = receipt
        return receipt

    def run(self, operation_id: str, topic: str, subject: str, at: datetime,
            action: Callable[[], T], evidence: Mapping[str, Any] | None = None) -> OperationReceipt:
        return self._commit(operation_id, topic, subject, at, action, evidence)

    def reserve_block(self, operation_id, candidate, existing, clearance, at):
        return self._commit(operation_id, "dispatch.block.reserved", candidate.train, at,
                            lambda: reserve(candidate, existing, clearance))

    def validate_consist(self, operation_id, rows, subject, at):
        return self._commit(operation_id, "consist.validated", subject, at, lambda: validate(rows))

    def assess_brakes(self, operation_id, rows, gradient, subject, at):
        return self._commit(operation_id, "braking.assessed", subject, at,
                            lambda: brake_percentage(rows, gradient))

    def plan_traction(self, operation_id, rows, adhesion_factor, subject, at):
        return self._commit(operation_id, "traction.planned", subject, at,
                            lambda: available_effort(rows, adhesion_factor))

    def assign_duty(self, operation_id, rows, max_duty, max_drive, max_span, subject, at):
        def action():
            result = duty_legal(rows, max_duty, max_drive, max_span)
            if not result:
                raise ValueError("duty is not legal")
            return result
        return self._commit(operation_id, "crew.duty.assigned", subject, at, action)

    def book_meet(self, operation_id, rows, subject, at):
        """Direction-constrained meet gate: an opposing overlap is a rejection.

        The decision is made (and raised) before any commit, so a refused meet
        publishes no event, no version and no audit record.
        """
        def action():
            found = meet_conflicts(rows)
            if found:
                raise MeetConflictError(
                    found[0][0], (found[0][1], found[0][2]), 0
                )
            return []
        return self._commit(operation_id, "timetable.meet.booked", subject, at, action,
                            evidence={"meet_conflicts": []})

    def publish_single_line_plan(self, operation_id, requests, sections, existing,
                                 headway_minutes, at, feed_events=(), expected_version=None):
        """Joint publish gate for a single-track line.

        Publishability is decided by all four planes together:
          * path plan   - build() shifts opposing trains within tolerance;
          * direction   - any remaining opposing overlap is a hard meet reject;
          * event cursor- feed events must be gap-free to be consumed;
          * version     - expected_version must match the current plan version.
        Same-direction followers parallelize; an opposing meet is refused and
        no half-built plan is left behind anywhere.
        """
        subject = operation_id

        def action():
            plan = build_paths(requests, sections, existing, headway_minutes)
            single = {sid for sid, s in sections.items() if s.single_track}
            traversals = [
                _traversal(slot)
                for slots in plan.values()
                for slot in slots
                if slot.section in single
            ]
            meets = meet_conflicts(traversals)
            if meets:
                section_id, a, b = meets[0]
                raise MeetConflictError(section_id, (a, b), 0)
            # Cursor gate: every feed event must be accepted in strict order;
            # peek moves nothing yet, so a gap rejects the whole publish.
            accepted = []
            for event in feed_events:
                consumer = self._consumers.get(event.partition)
                if consumer is None:
                    accepted.append(event)
                    continue
                if consumer.peek(event):
                    accepted.append(event)
                elif event.event_id in consumer.seen or \
                        event.sequence <= consumer.cursor.get(event.partition, 0):
                    continue
                # A gap raises ValueError('cursor gap') from peek -> reject.
            return {
                "plan": plan,
                "load": section_load(plan),
                "delay": train_delay(plan, requests),
                "meet_conflicts": meets,
                "events_accepted": [e.event_id for e in accepted],
                "publishable": True,
            }

        def staged():
            effects = []
            for event in feed_events:
                consumer = self._consumers.setdefault(event.partition, CursorConsumer())
                effects.append((consumer, lambda c, ev=event: c.accept(ev)))
            return effects

        return self._commit(
            operation_id, "timetable.singleline.published", subject, at,
            action, staged=staged, expected_version=expected_version,
        )

    def classify_yard(self, operation_id, cuts, tracks, occupied, subject, at):
        return self._commit(operation_id, "yard.classified", subject, at,
                            lambda: classify(cuts, tracks, occupied))

    def check_axles(self, operation_id, wagon, max_axle, max_gross, subject, at):
        return self._commit(operation_id, "wagon.axle.checked", subject, at,
                            lambda: axle_route_ok(wagon, max_axle, max_gross))

    def book_intermodal(self, operation_id, inbound, outbound, unload, load, cutoff, subject, at):
        return self._commit(operation_id, "intermodal.booked", subject, at,
                            lambda: feasible(inbound, outbound, unload, load, cutoff))

    def protect_possession(self, operation_id, candidate, rows, subject, at):
        return self._commit(operation_id, "possession.protected", subject, at,
                            lambda: possession_conflicts(candidate, rows))

    def release_maintenance(self, operation_id, evidence, policy, check_at, subject, at):
        def action():
            result = releasable(evidence, policy, check_at)
            if not result:
                raise ValueError("maintenance evidence incomplete")
            return result
        return self._commit(operation_id, "maintenance.released", subject, at, action)

    def consume_event(self, operation_id, event: FeedEvent, subject, at):
        consumer = self._consumers.setdefault(event.partition, CursorConsumer())

        def staged():
            return [(consumer, lambda c: c.accept(event))]

        def action():
            return consumer.peek(event)

        return self._commit(operation_id, "events.cursor.consumed", subject, at,
                            action, staged=staged)

    def charge_demurrage(self, operation_id, arrival, release, tariff, subject, at):
        return self._commit(operation_id, "billing.demurrage.charged", subject, at,
                            lambda: charge(arrival, release, tariff))

    def check_customs(self, operation_id, rows, scope, check_at, subject, at):
        return self._commit(operation_id, "customs.hold.checked", subject, at,
                            lambda: active(rows, scope, check_at))

    def check_segregation(self, operation_id, rows, rules, subject, at):
        return self._commit(operation_id, "cargo.segregation.checked", subject, at,
                            lambda: violations(rows, rules))

    def calculate_energy(self, operation_id, rows, subject, at):
        return self._commit(operation_id, "energy.calculated", subject, at,
                            lambda: net_energy(rows))

    def lock_switch_route(self, operation_id, route, switches, owner, subject, at):
        return self._commit(operation_id, "yard.route.locked", subject, at,
                            lambda: lock_route(route, switches, owner))

    def check_clearance(self, operation_id, profile, segments, subject, at):
        return self._commit(operation_id, "routing.clearance.checked", subject, at,
                            lambda: route_ok(profile, segments))

    def allocate_wagons(self, operation_id, rows, wagon_type, location, on, count, required_days, forbidden, subject, at):
        return self._commit(operation_id, "inventory.wagons.allocated", subject, at,
                            lambda: allocate(rows, wagon_type, location, on, count, required_days, forbidden))

    def fuse_eta(self, operation_id, rows, max_lateness, subject, at):
        return self._commit(operation_id, "eta.fused", subject, at,
                            lambda: fused_delay(rows, max_lateness))


def _traversal(slot) -> object:
    from railforge.timetable.meets import Traversal
    return Traversal(slot.train, slot.section, slot.start, slot.end, slot.direction)
