import asyncio
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from json2mqtt import Json2Mqtt
from scheduling import RequestSchedule


def timestamp(day=19, hour=12, minute=0, *, month=10, timezone='UTC'):
    return datetime(2026, month, day, hour, minute, tzinfo=ZoneInfo(timezone)).timestamp()


class ClockMixin:
    def setUp(self):
        self.now = timestamp(hour=10)
        self.monotonic = 100.0
        self.clock = self.enterContext(patch('scheduling.time'))
        self.clock.time.side_effect = lambda: self.now
        self.clock.monotonic.side_effect = lambda: self.monotonic
        self.random = self.enterContext(patch('scheduling.random.uniform', return_value=0))

    def schedule(self, request=None, **defaults):
        return RequestSchedule({'timezone': 'UTC', **defaults}, request or {})


class RequestScheduleTests(ClockMixin, unittest.TestCase):
    def test_interval_runs_immediately_then_uses_default_rate(self):
        schedule = self.schedule()
        self.assertEqual(schedule.seconds_until_due(), 0)
        schedule.advance()
        self.assertEqual(schedule.seconds_until_due(), 600)
        self.monotonic += 600
        self.assertEqual(schedule.seconds_until_due(), 0)

    def test_interval_uses_monotonic_clock(self):
        schedule = self.schedule(update_rate=60)
        schedule.advance()
        self.now += 86400
        self.assertEqual(schedule.seconds_until_due(), 60)
        self.monotonic += 30
        self.assertEqual(schedule.seconds_until_due(), 30)

    def test_interval_jitter_is_sampled_once_per_run(self):
        self.random.side_effect = [-10, 10]
        schedule = self.schedule(update_rate=60, jitter=10)
        self.random.assert_not_called()
        schedule.advance()
        self.assertEqual(schedule.seconds_until_due(), 50)
        self.assertEqual(schedule.seconds_until_due(), 50)
        self.random.assert_called_once_with(-10, 10)
        self.monotonic += 50
        schedule.advance()
        self.assertEqual(schedule.seconds_until_due(), 70)

    def test_daily_cron_waits_until_noon(self):
        schedule = self.schedule(cron='0 12 * * *')
        self.assertEqual(schedule.next_run, timestamp())
        self.assertEqual(schedule.seconds_until_due(), 7200)
        self.now = timestamp()
        schedule.advance()
        self.assertEqual(schedule.next_run, timestamp(day=20))
        self.random.assert_not_called()

    def test_starting_at_exact_cron_time_is_due(self):
        self.now = timestamp()
        self.assertEqual(self.schedule(cron='0 12 * * *').seconds_until_due(), 0)

    def test_cron_jitter_bounds_and_no_duplicate_occurrence(self):
        for offset in (-600, 600):
            with self.subTest(offset=offset):
                self.now = timestamp(hour=10)
                self.random.return_value = offset
                schedule = self.schedule(cron='0 12 * * *', jitter=600)
                self.assertEqual(schedule.next_run, timestamp() + offset)
                self.now = schedule.next_run
                schedule.advance()
                self.assertEqual(schedule.next_run, timestamp(day=20) + offset)
                self.assertGreater(schedule.seconds_until_due(), 0)

    def test_cron_jitter_is_stable_and_does_not_accumulate(self):
        self.random.side_effect = [-300, 420]
        schedule = self.schedule(cron='0 12 * * *', jitter=600)
        self.assertEqual(schedule.next_run, timestamp() - 300)
        self.assertEqual(schedule.seconds_until_due(), 6900)
        self.assertEqual(schedule.seconds_until_due(), 6900)
        self.random.assert_called_once_with(-600, 600)
        self.now = schedule.next_run
        schedule.advance()
        self.assertEqual(schedule.next_run, timestamp(day=20) + 420)

    def test_startup_after_nominal_time_can_use_positive_jitter(self):
        self.now = timestamp(minute=5)
        self.random.return_value = 600
        schedule = self.schedule(cron='0 12 * * *', jitter=600)
        self.assertEqual(schedule.next_run, timestamp(minute=10))

    def test_startup_skips_past_sampled_deadline(self):
        self.now = timestamp(hour=11, minute=55)
        self.random.return_value = -600
        schedule = self.schedule(cron='0 12 * * *', jitter=600)
        self.assertEqual(schedule.next_run, timestamp(day=20, hour=11, minute=50))

    def test_overlapping_windows_skip_past_deadlines(self):
        self.now = timestamp(hour=11, minute=59)
        self.random.return_value = 90
        schedule = self.schedule(cron='* * * * *', jitter=90)
        self.assertEqual(schedule.next_run, timestamp(hour=11, minute=59) + 30)
        self.now = schedule.next_run
        self.random.return_value = -90
        schedule.advance()
        self.assertEqual(schedule.next_run, timestamp() + 30)

    def test_delayed_run_skips_missed_days(self):
        schedule = self.schedule(cron='0 12 * * *')
        self.now = timestamp(day=23, hour=14)
        self.assertLess(schedule.seconds_until_due(), 0)
        schedule.advance()
        self.assertEqual(schedule.next_run, timestamp(day=24))

    def test_global_and_request_schedule_precedence(self):
        schedule = self.schedule(cron='0 12 * * *', update_rate=600)
        self.assertEqual(schedule.next_run, timestamp())
        schedule = self.schedule({'update_rate': 60}, cron='0 12 * * *')
        schedule.advance()
        self.assertEqual(schedule.seconds_until_due(), 60)
        schedule = self.schedule({'cron': '0 14 * * *', 'update_rate': 60}, cron='0 12 * * *')
        self.assertEqual(schedule.next_run, timestamp(hour=14))
        schedule = self.schedule({'cron': None}, cron='0 12 * * *', update_rate=120)
        schedule.advance()
        self.assertEqual(schedule.seconds_until_due(), 120)

    def test_global_jitter_can_be_overridden_and_disabled(self):
        self.random.return_value = 10
        schedule = self.schedule(jitter=10)
        schedule.advance()
        self.assertEqual(schedule.seconds_until_due(), 610)
        schedule = self.schedule({'jitter': 0}, jitter=10)
        schedule.advance()
        self.assertEqual(schedule.seconds_until_due(), 600)

    def test_timezone_override(self):
        self.now = timestamp(hour=0)
        schedule = self.schedule({'timezone': 'Europe/Berlin'}, cron='0 12 * * *')
        self.assertEqual(schedule.next_run, timestamp(hour=10))

    def test_daily_cron_across_daylight_saving_changes(self):
        for month, day, hours in [(3, 28, 23), (10, 24, 25)]:
            with self.subTest(month=month):
                self.now = timestamp(month=month, day=day, hour=10, timezone='Europe/Berlin')
                schedule = self.schedule(cron='0 12 * * *', timezone='Europe/Berlin')
                self.now = schedule.next_run
                schedule.advance()
                self.assertEqual(schedule.next_run, timestamp(month=month, day=day + 1, timezone='Europe/Berlin'))
                self.assertEqual(schedule.seconds_until_due(), hours * 3600)

    def test_invalid_scheduling_options_fail_at_startup(self):
        invalid = [
            {'cron': ''}, {'cron': 'not a cron'}, {'cron': 12},
            {'cron': '0 0 31 2 *'},
            {'update_rate': 0}, {'update_rate': -1}, {'update_rate': True},
            {'update_rate': '60'}, {'update_rate': float('inf')},
            {'jitter': -1}, {'jitter': '10m'}, {'jitter': None},
            {'jitter': True}, {'jitter': float('nan')},
            {'update_rate': 60, 'jitter': 60},
        ]
        for request in invalid:
            with self.subTest(request=request), self.assertRaises(ValueError):
                self.schedule(request)


class Json2MqttSchedulingTests(ClockMixin, unittest.IsolatedAsyncioTestCase):
    def make_app(self, requests, **defaults):
        config = {'requests': requests, 'timezone': 'UTC', **defaults}
        self.enterContext(patch('json2mqtt.get_first_config', return_value=config))
        self.enterContext(patch('json2mqtt.MqttHandler'))
        app = Json2Mqtt()
        app.fetch = AsyncMock(return_value='{"value": 42}')
        return app

    async def test_mixed_schedules_fetch_and_publish_only_when_due(self):
        topics = {'value': {'path': ['value']}}
        app = self.make_app({
            'interval': {'update_rate': 60, 'topics': topics},
            'daily': {'cron': '0 12 * * *', 'jitter': 600, 'topics': topics},
        })
        self.random.return_value = -600
        # The initial cron deadline remains stable despite subsequent random values.
        await app.loop_iteration()
        app.fetch.assert_awaited_once_with('interval', None)
        app.mqtt_handler.publish.assert_called_once_with('value', 42, False)
        app.fetch.reset_mock()
        await app.loop_iteration()
        app.fetch.assert_not_awaited()

        self.now = timestamp()
        await app.loop_iteration()
        app.fetch.assert_awaited_once_with('daily', None)
        self.assertEqual(app.schedules['daily'].next_run, timestamp(day=20) - 600)
        app.fetch.reset_mock()
        await app.loop_iteration()
        app.fetch.assert_not_awaited()

    async def test_failed_and_invalid_responses_advance_schedule(self):
        for response in ('', 'invalid json'):
            with self.subTest(response=response):
                app = self.make_app({'url': {'topics': {}}}, update_rate=60)
                app.fetch.return_value = response
                with patch('json2mqtt.logging.warning'):
                    await app.loop_iteration()
                await app.loop_iteration()
                app.fetch.assert_awaited_once()
                self.assertEqual(app.schedules['url'].seconds_until_due(), 60)

    async def test_sleep_accounts_for_fetch_duration_once(self):
        app = self.make_app({'url': {'topics': {}}}, update_rate=60)

        async def fetch(*args):
            self.monotonic += 10
            return '{}'

        app.fetch.side_effect = fetch
        with patch('json2mqtt.asyncio.sleep', new_callable=AsyncMock) as sleep:
            sleep.side_effect = asyncio.CancelledError
            with self.assertRaises(asyncio.CancelledError):
                await app.loop()
            sleep.assert_awaited_once_with(50)

    async def test_cron_sleep_rechecks_wall_clock(self):
        app = self.make_app({'url': {'cron': '0 12 * * *', 'topics': {}}})
        with patch('json2mqtt.asyncio.sleep', new_callable=AsyncMock) as sleep:
            sleep.side_effect = asyncio.CancelledError
            with self.assertRaises(asyncio.CancelledError):
                await app.loop()
            sleep.assert_awaited_once_with(60)
        app.fetch.assert_not_awaited()

    async def test_empty_requests_sleep(self):
        app = self.make_app({})
        with patch('json2mqtt.asyncio.sleep', new_callable=AsyncMock) as sleep:
            sleep.side_effect = asyncio.CancelledError
            with self.assertRaises(asyncio.CancelledError):
                await app.loop()
            sleep.assert_awaited_once_with(600)

    def test_invalid_schedule_is_rejected_before_mqtt_connection(self):
        with patch('json2mqtt.get_first_config', return_value={'requests': {'url': {'cron': 'invalid'}}}):
            with patch('json2mqtt.MqttHandler') as mqtt, self.assertRaises(ValueError):
                Json2Mqtt()
            mqtt.assert_not_called()


if __name__ == '__main__':
    unittest.main()
