import unittest
from datetime import datetime, timezone, timedelta

from railforge.api import RailForgeService
from railforge.events.cursor import FeedEvent
from railforge.planning.train_path_engine import (
    Section, PathRequest, Slot, build, MeetConflictError,
)
from railforge.timetable.meets import Traversal, conflicts as meet_conflicts
from railforge.workflows.control import RailForgeWorkflowControl

U = timezone.utc
AT = datetime(2026, 1, 1, 8, tzinfo=U)


def sections(single=True, run=30):
    return {'A': Section('A', run, single, 2)}


class MeetDetectionTests(unittest.TestCase):
    def test_opposing_overlap_is_a_meet(self):
        rows = [
            Traversal('T1', 'A', datetime(2026, 1, 1, 8, tzinfo=U), datetime(2026, 1, 1, 9, tzinfo=U), 1),
            Traversal('T2', 'A', datetime(2026, 1, 1, 8, 30, tzinfo=U), datetime(2026, 1, 1, 9, 30, tzinfo=U), -1),
        ]
        self.assertEqual(meet_conflicts(rows), [('A', 'T1', 'T2')])

    def test_same_direction_followers_are_not_a_meet(self):
        rows = [
            Traversal('T1', 'A', datetime(2026, 1, 1, 8, tzinfo=U), datetime(2026, 1, 1, 9, tzinfo=U), 1),
            Traversal('T2', 'A', datetime(2026, 1, 1, 8, 5, tzinfo=U), datetime(2026, 1, 1, 9, 5, tzinfo=U), 1),
        ]
        self.assertEqual(meet_conflicts(rows), [])

    def test_opposing_windows_touching_do_not_meet(self):
        rows = [
            Traversal('T1', 'A', datetime(2026, 1, 1, 8, tzinfo=U), datetime(2026, 1, 1, 9, tzinfo=U), 1),
            Traversal('T2', 'A', datetime(2026, 1, 1, 9, tzinfo=U), datetime(2026, 1, 1, 10, tzinfo=U), -1),
        ]
        self.assertEqual(meet_conflicts(rows), [])


class PathEngineDirectionTests(unittest.TestCase):
    def test_same_direction_runs_in_parallel_without_delay(self):
        reqs = [
            PathRequest('T1', AT, ('A',), 1, 10, direction=1),
            PathRequest('T2', AT + timedelta(minutes=2), ('A',), 1, 10, direction=1),
        ]
        plan = build(reqs, sections(), [], headway_minutes=3)
        self.assertEqual(plan['T2'][0].start, AT + timedelta(minutes=2))

    def test_double_track_opposing_runs_in_parallel(self):
        reqs = [
            PathRequest('T1', AT, ('A',), 1, 10, direction=1),
            PathRequest('T2', AT, ('A',), 1, 10, direction=-1),
        ]
        plan = build(reqs, sections(single=False), [], headway_minutes=3)
        self.assertEqual(plan['T2'][0].start, AT)

    def test_opposing_meet_is_delayed_within_tolerance(self):
        existing = [Slot('T1', 'A', AT, AT + timedelta(minutes=30), 1)]
        req = PathRequest('T2', AT, ('A',), 1, 35, direction=-1)
        plan = build([req], sections(), existing, headway_minutes=3)
        # T2 window must clear T1 plus the 3-minute guard: starts at 08:33.
        self.assertEqual(plan['T2'][0].start, AT + timedelta(minutes=33))

    def test_opposing_meet_beyond_tolerance_rejected_without_partial_plan(self):
        existing = [Slot('T1', 'A', AT, AT + timedelta(minutes=30), 1)]
        # Only two sections: if the first section were tentatively appended the
        # caller would see a half-built plan. Nothing must mutate.
        two = {'A': Section('A', 30, True, 2), 'B': Section('B', 30, True, 2)}
        ex2 = [Slot('T1', 'A', AT, AT + timedelta(minutes=30), 1),
               Slot('T1', 'B', AT + timedelta(minutes=30), AT + timedelta(minutes=60), 1)]
        req = PathRequest('T2', AT, ('A', 'B'), 1, 5, direction=-1)
        with self.assertRaises(MeetConflictError):
            build([req], two, ex2, headway_minutes=0)
        # existing plan untouched
        self.assertEqual(ex2, [Slot('T1', 'A', AT, AT + timedelta(minutes=30), 1),
                               Slot('T1', 'B', AT + timedelta(minutes=30), AT + timedelta(minutes=60), 1)])


class PublishGateTests(unittest.TestCase):
    def _opposing_requests(self):
        return [
            PathRequest('T1', AT, ('A',), 1, 10, direction=1),
            PathRequest('T2', AT, ('A',), 1, 0, direction=-1),
        ]

    def test_publishable_plan_commits_all_four_planes(self):
        svc = RailForgeService()
        feed = [FeedEvent('line-1', 1, 'evt-1', {'kind': 'line-open'})]
        reqs = [PathRequest('T1', AT, ('A',), 1, 10, direction=1),
                PathRequest('T2', AT + timedelta(minutes=2), ('A',), 1, 10, direction=1)]
        receipt = svc.publish_single_line('op-1', reqs, sections(), [], 3, AT, feed_events=feed)
        self.assertTrue(receipt.result['publishable'])
        self.assertEqual(receipt.result['events_accepted'], ['evt-1'])
        self.assertEqual(receipt.event.payload['status'], 'committed')
        self.assertEqual(receipt.event.payload['control_path'], 'atomic')
        self.assertEqual(receipt.version, 1)
        self.assertTrue(svc.control.stream.verify_chain())
        self.assertTrue(svc.control.trail.verify())
        # cursor advanced
        self.assertEqual(svc.control._consumers['line-1'].cursor, {'line-1': 1})

    def test_opposing_meet_rejected_leaves_no_trace(self):
        svc = RailForgeService()
        feed = [FeedEvent('line-1', 1, 'evt-1', {})]
        with self.assertRaises(MeetConflictError):
            svc.publish_single_line('op-bad', self._opposing_requests(), sections(), [], 3, AT,
                                    feed_events=feed)
        self.assertEqual(svc.control.stream.head, 0)
        self.assertEqual(svc.control.store.snapshot(), {})
        self.assertTrue(svc.control.trail.verify())
        self.assertEqual(len(svc.control.trail._rows), 0)
        # cursor must not have advanced
        self.assertNotIn('line-1', svc.control._consumers)
        self.assertEqual(svc.control.receipts, {})

    def test_cursor_gap_rejects_publish(self):
        ctrl = RailForgeWorkflowControl()
        feed = [FeedEvent('line-1', 2, 'evt-2', {})]  # gap: expected seq 1
        reqs = [PathRequest('T1', AT, ('A',), 1, 10, direction=1)]
        with self.assertRaises(ValueError):
            ctrl.publish_single_line_plan('op-gap', reqs, sections(), [], 3, AT, feed_events=feed)
        self.assertEqual(ctrl.stream.head, 0)
        self.assertEqual(ctrl.store.snapshot(), {})
        self.assertEqual(ctrl._consumers.get('line-1').cursor, {})

    def test_stale_version_rejects_without_side_effects(self):
        ctrl = RailForgeWorkflowControl()
        reqs = [PathRequest('T1', AT, ('A',), 1, 10, direction=1)]
        receipt = ctrl.publish_single_line_plan('op-v1', reqs, sections(), [], 3, AT)
        self.assertEqual(receipt.version, 1)
        with self.assertRaises(RuntimeError):
            ctrl.publish_single_line_plan('op-v2', reqs, sections(), [], 3, AT, expected_version=1)
        # only the first commit exists; failed commit did not publish/retract
        self.assertEqual(ctrl.stream.head, 1)
        self.assertTrue(ctrl.stream.verify_chain())

    def test_book_meet_gate_direction_aware(self):
        ctrl = RailForgeWorkflowControl()
        opposing = [
            Traversal('T1', 'A', AT, AT + timedelta(minutes=60), 1),
            Traversal('T2', 'A', AT + timedelta(minutes=10), AT + timedelta(minutes=70), -1),
        ]
        with self.assertRaises(MeetConflictError):
            ctrl.book_meet('meet-1', opposing, 'A', AT)
        self.assertEqual(ctrl.stream.head, 0)
        same = [
            Traversal('T1', 'A', AT, AT + timedelta(minutes=60), 1),
            Traversal('T2', 'A', AT + timedelta(minutes=10), AT + timedelta(minutes=70), 1),
        ]
        receipt = ctrl.book_meet('meet-2', same, 'A', AT)
        self.assertEqual(receipt.event.payload['status'], 'committed')
        self.assertEqual(receipt.event.payload['control_path'], 'atomic')
        self.assertEqual(ctrl.stream.head, 1)

    def test_rejected_meet_does_not_block_later_commit(self):
        svc = RailForgeService()
        with self.assertRaises(MeetConflictError):
            svc.publish_single_line('op-bad', self._opposing_requests(), sections(), [], 3, AT)
        # a valid publish afterwards still starts cleanly
        reqs = [PathRequest('T1', AT, ('A',), 1, 10, direction=1)]
        receipt = svc.publish_single_line('op-good', reqs, sections(), [], 3, AT)
        self.assertEqual(receipt.version, 1)
        self.assertEqual(svc.control.stream.head, 1)


if __name__ == '__main__':
    unittest.main()
