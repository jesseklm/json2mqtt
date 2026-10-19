import math
import random
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from croniter import croniter


class RequestSchedule:
    """Keep one sampled deadline per request, using the appropriate clock."""

    def __init__(self, config: dict, request: dict):
        if 'cron' in request:
            self.cron = request['cron']
        elif 'update_rate' in request:
            self.cron = None
        else:
            self.cron = config.get('cron')

        self.jitter = self._seconds(request.get('jitter', config.get('jitter', 0)), 'jitter')
        if self.jitter < 0:
            raise ValueError('jitter must be non-negative (seconds)')

        self.last_cron_time: float | None = None
        if self.cron is not None:
            if not isinstance(self.cron, str) or not croniter.is_valid(self.cron):
                raise ValueError('cron must be a valid cron expression')
            timezone = request.get('timezone', config.get('timezone'))
            self.timezone = ZoneInfo(timezone) if timezone is not None else None
            self.advance()
        else:
            self.update_rate = self._seconds(request.get('update_rate', config.get('update_rate', 600)), 'update_rate')
            if self.update_rate <= 0 or self.jitter >= self.update_rate:
                raise ValueError('update_rate must be positive and greater than jitter (seconds)')
            # Interval requests still fetch immediately on startup.
            self.next_run = time.monotonic()

    @staticmethod
    def _seconds(value, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f'{name} must be a finite number of seconds')
        return float(value)

    def seconds_until_due(self) -> float:
        now = time.time() if self.cron is not None else time.monotonic()
        return self.next_run - now

    def advance(self) -> None:
        if self.cron is None:
            self.next_run = time.monotonic() + self.update_rate + self._offset()
            return

        now = time.time()
        initial = self.last_cron_time is None
        # Include nominal times whose positive jitter could still put them in
        # the future, but never revisit an already sampled cron occurrence.
        base_time = now - self.jitter
        if self.last_cron_time is not None:
            base_time = max(base_time, self.last_cron_time)
        base = datetime.fromtimestamp(base_time, self.timezone)
        if initial:
            base -= timedelta(microseconds=1)
        iterator = croniter(self.cron, base)
        while True:
            self.last_cron_time = iterator.get_next(datetime).timestamp()
            self.next_run = self.last_cron_time + self._offset()
            if self.next_run > now or (initial and self.next_run == now):
                return

    def _offset(self) -> float:
        return random.uniform(-self.jitter, self.jitter) if self.jitter else 0.0
