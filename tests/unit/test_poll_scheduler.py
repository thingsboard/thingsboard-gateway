import pytest
from time import monotonic
from datetime import datetime
from unittest.mock import patch

from thingsboard_gateway.tb_utility.poll_scheduler import (
    PollScheduleEntry, PollScheduler, compute_next_poll
)


# --- Config parsing ---

class TestPollSchedulerParsing:
    def test_parse_string(self):
        s = PollScheduler("*/5 * * * *")
        assert s.is_active
        assert len(s.entries) == 1
        assert s.entries[0].label == "*/5 * * * *"

    def test_parse_list_of_strings(self):
        s = PollScheduler(["*/5 * * * *", "*/10 * * * *"])
        assert s.is_active
        assert len(s.entries) == 2

    def test_parse_list_of_dicts(self):
        s = PollScheduler([{"cron": "*/5 * * * *", "label": "prod"}])
        assert s.is_active
        assert len(s.entries) == 1
        assert s.entries[0].label == "prod"

    def test_parse_mixed_list(self):
        s = PollScheduler([
            "*/5 * * * *",
            {"cron": "*/10 * * * *", "label": "slow"}
        ])
        assert len(s.entries) == 2
        assert s.entries[0].label == "*/5 * * * *"
        assert s.entries[1].label == "slow"

    def test_parse_none(self):
        s = PollScheduler(None)
        assert not s.is_active
        assert len(s.entries) == 0

    def test_parse_empty_list(self):
        s = PollScheduler([])
        assert not s.is_active

    def test_invalid_cron(self):
        with pytest.raises(ValueError, match="Invalid cron expression"):
            PollScheduler("not a cron")

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="pollSchedule must be str, list, or None"):
            PollScheduler(12345)


# --- Schedule computation ---

class TestScheduleComputation:
    def test_next_fire_single(self):
        s = PollScheduler("*/1 * * * *")
        result = s.next_poll_monotonic()
        assert result > monotonic()

    def test_next_fire_multiple_nearest_wins(self):
        # Every minute should fire sooner than every 30 minutes
        s = PollScheduler(["*/30 * * * *", "*/1 * * * *"])
        result = s.next_poll_monotonic()
        now = monotonic()
        # Should be within ~60 seconds (the every-minute schedule)
        assert result - now <= 61.0

    def test_next_fire_distant(self):
        # Schedule for a specific unlikely time — still returns a valid future value
        s = PollScheduler("0 3 1 1 *")  # Jan 1 at 3am
        result = s.next_poll_monotonic()
        assert result > monotonic()


# --- compute_next_poll() ---

class TestComputeNextPoll:
    def test_compute_with_active_scheduler(self):
        s = PollScheduler("*/1 * * * *")
        now = monotonic()
        result = compute_next_poll(now, 5.0, s)
        # Should NOT be now + 5.0 (that's the fallback)
        assert result != now + 5.0
        assert result > now

    def test_compute_without_scheduler(self):
        now = monotonic()
        result = compute_next_poll(now, 5.0, None)
        assert result == now + 5.0

    def test_compute_with_inactive_scheduler(self):
        s = PollScheduler(None)
        now = monotonic()
        result = compute_next_poll(now, 5.0, s)
        assert result == now + 5.0


# --- PollScheduleEntry ---

class TestPollScheduleEntry:
    def test_next_fire_time(self):
        entry = PollScheduleEntry("*/5 * * * *")
        base = datetime(2025, 1, 1, 12, 3, 0)
        nxt = entry.next_fire_time(base)
        assert nxt == datetime(2025, 1, 1, 12, 5, 0)

    def test_label_defaults_to_cron(self):
        entry = PollScheduleEntry("*/5 * * * *")
        assert entry.label == "*/5 * * * *"

    def test_custom_label(self):
        entry = PollScheduleEntry("*/5 * * * *", label="fast")
        assert entry.label == "fast"


class TestSchedulerWallClockDelta:
    def test_scheduler_wall_clock_delta(self):
        """Verify next_poll_monotonic() - monotonic() produces a positive delta
        suitable for wall-clock offset (used by Request connector pattern)."""
        s = PollScheduler("*/1 * * * *")
        delta = s.next_poll_monotonic() - monotonic()
        assert delta > 0
        assert delta <= 61.0
