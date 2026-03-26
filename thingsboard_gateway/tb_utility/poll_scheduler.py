from time import time, monotonic
from datetime import datetime
from typing import Union, List, Optional
import logging

from croniter import croniter

log = logging.getLogger(__name__)


class PollScheduleEntry:
    """Wraps a single cron expression with validation and fire-time computation."""

    def __init__(self, cron: str, label: Optional[str] = None):
        self.cron = cron
        self.label = label or cron
        if not croniter.is_valid(cron):
            raise ValueError(f"Invalid cron expression: '{cron}'")

    def next_fire_time(self, base: datetime) -> datetime:
        """Return the next fire time after the given base datetime."""
        return croniter(self.cron, base).get_next(datetime)


class PollScheduler:
    """
    Manages one or more cron schedules and computes the next poll
    time as a monotonic() value.

    Accepts three config formats:
    - str:  single cron expression
    - list: list of strings or dicts with 'cron' and optional 'label'
    - None: inactive scheduler (falls back to pollPeriod)
    """

    def __init__(self, config: Union[str, list, None]):
        self.entries: List[PollScheduleEntry] = []
        self._parse_config(config)
        if self.entries:
            labels = ", ".join(e.label for e in self.entries)
            log.info(
                "PollScheduler initialized with %d schedule(s): %s",
                len(self.entries), labels
            )

    def _parse_config(self, config):
        if config is None:
            return

        if isinstance(config, str):
            self.entries.append(PollScheduleEntry(config))
            return

        if isinstance(config, list):
            for item in config:
                if isinstance(item, str):
                    self.entries.append(PollScheduleEntry(item))
                elif isinstance(item, dict):
                    self.entries.append(PollScheduleEntry(
                        cron=item['cron'],
                        label=item.get('label')
                    ))
                else:
                    raise ValueError(
                        f"Invalid pollSchedule entry: {item}"
                    )
            return

        raise ValueError(
            f"pollSchedule must be str, list, or None — "
            f"got {type(config).__name__}"
        )

    @property
    def is_active(self) -> bool:
        """Return True if at least one schedule entry is configured."""
        return len(self.entries) > 0

    def next_poll_monotonic(self) -> float:
        """
        Compute the next poll time across all schedule entries and
        return it as a monotonic() timestamp.

        When multiple entries are configured, the nearest upcoming
        fire time wins.
        """
        now = datetime.now()
        now_mono = monotonic()

        best_delta = None
        best_entry = None

        for entry in self.entries:
            next_fire = entry.next_fire_time(now)
            delta = (next_fire - now).total_seconds()
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_entry = entry

        target_mono = now_mono + best_delta

        log.debug(
            "Poll schedule [%s]: next poll in %.1fs at %s",
            best_entry.label,
            best_delta,
            datetime.fromtimestamp(time() + best_delta).strftime("%H:%M:%S")
        )

        return target_mono


def compute_next_poll(current_monotonic: float,
                      poll_period_sec: float,
                      scheduler: Optional[PollScheduler] = None) -> float:
    """
    Unified next-poll-time computation. Drop-in replacement for all connectors.

    Args:
        current_monotonic: The current monotonic() timestamp.
        poll_period_sec:   The configured poll period in seconds (fallback).
        scheduler:         Optional PollScheduler instance.

    Returns:
        The monotonic() timestamp for the next poll.
    """
    if scheduler is not None and scheduler.is_active:
        return scheduler.next_poll_monotonic()
    else:
        return current_monotonic + poll_period_sec
