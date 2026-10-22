import asyncio
import json
import logging
import signal
import time
import tomllib
from functools import reduce
from pathlib import Path

import httpcore
from httpcore import ConnectError
from selectolax.lexbor import LexborHTMLParser

from config import get_first_config
from mqtt_handler import MqttHandler
from scheduling import RequestSchedule

with (Path(__file__).parent / 'pyproject.toml').open('rb') as f:
    __version__ = tomllib.load(f)['project']['version']


def html_table_to_list(html: str, table_index: int = 0) -> list[list[str]]:
    tree = LexborHTMLParser(html)
    return [
        [cell.text(strip=True) for cell in row.css('th, td')]
        for row in tree.css('table')[table_index].css('tr')
        if row.css('th, td')
    ]


class Json2Mqtt:
    def __init__(self):
        config: dict = get_first_config()
        self.setup_logging(config)
        self.headers: dict = config.get('headers')
        # requests structure:
        # {
        #     "<URL>": {
        #         "update_rate": 60,                                # Optional: update interval in seconds
        #         "cron": "0 12 * * *",                             # Optional: cron instead of update_rate
        #         "jitter": 600,                                    # Optional: random +/- seconds per run
        #         "timezone": "Europe/Berlin",                      # Optional: cron timezone (default: system)
        #         "html_table": 0,                                  # Optional: Index of HTML table to parse
        #         "headers": {                                      # Optional: HTTP headers specific to this URL
        #             "Authorization": "Bearer xxx",
        #             "Accept": "application/json"
        #         },
        #         "topics": {
        #             "<MQTT topic>": {
        #                 "path": ["json", "keys", "to", "value"],  # Path to the value in the JSON response
        #                 "if": {                                   # Optional: only publish if this value matches
        #                     "path": ["status", "enabled"],         # Path in the same response
        #                     "equals": 1
        #                 },
        #                 "retain": True,                           # Optional: MQTT retain flag
        #                 "offset": 1.0,                            # Optional: Value offset
        #                 "factor": 0.1                             # Optional: Value multiplier
        #             }
        #         }
        #     }
        # }
        self.requests: dict = config['requests']
        for request in self.requests.values():
            for topic, options in request.get('topics', {}).items():
                if 'if' not in options:
                    continue
                condition = options['if']
                if (
                        not isinstance(condition, dict)
                        or not isinstance(condition.get('path'), list)
                        or 'equals' not in condition
                        or any(isinstance(key, bool) or not isinstance(key, (str, int))
                               for key in condition['path'])
                ):
                    raise ValueError(
                        f'invalid if condition for topic {topic!r}: expected a path list and equals value'
                    )
        self.schedules = {url: RequestSchedule(config, request) for url, request in self.requests.items()}
        self.has_cron = any(schedule.cron is not None for schedule in self.schedules.values())
        self.mqtt_handler = MqttHandler(config)

    async def exit(self):
        await self.mqtt_handler.disconnect()

    @staticmethod
    def setup_logging(config):
        if 'logging' in config:
            logging_level_name: str = config['logging'].upper()
            logging_level: int = logging.getLevelNamesMapping().get(logging_level_name, logging.NOTSET)
            if logging_level != logging.NOTSET:
                logging.getLogger().setLevel(logging_level)
            else:
                logging.warning('unknown logging level: %s.', logging_level)

    async def loop_iteration(self) -> None:
        for url, request in self.requests.items():
            schedule = self.schedules[url]
            if schedule.seconds_until_due() > 0:
                continue
            schedule.advance()

            response = await self.fetch(url, request.get('headers'))
            if not response:
                continue
            if (html_table := request.get('html_table')) is not None:
                try:
                    js_content = html_table_to_list(response, html_table)
                except IndexError as e:
                    logging.warning('failed to parse HTML from %s: %s', url, e)
                    continue
            else:
                try:
                    js_content = json.loads(response)
                except json.JSONDecodeError as e:
                    logging.warning('invalid JSON response from %s: %s', url, e)
                    continue
            for topic, options in request['topics'].items():
                try:
                    if 'if' in options:
                        condition = options['if']
                        condition_value = reduce(lambda d, key: d[key], condition['path'], js_content)
                        if condition_value != condition['equals']:
                            continue
                    value = reduce(lambda d, key: d[key], options['path'], js_content)
                except (KeyError, IndexError, TypeError) as e:
                    logging.warning('unexpected response for topic %s: %s. %s', topic, js_content, str(e))
                    continue
                offset = 'offset' in options
                factor = 'factor' in options
                if offset or factor:
                    try:
                        value = float(value)
                        if offset:
                            value += options['offset']
                        if factor:
                            value *= options['factor']
                        value = round(value, 10)
                    except ValueError as e:
                        logging.warning('failed to convert: %s. %s', value, str(e))
                        continue
                self.mqtt_handler.publish(topic, value, options.get('retain', False))

    async def loop(self) -> None:
        try:
            while True:
                start_time: float = time.perf_counter()
                await self.loop_iteration()
                time_taken: float = time.perf_counter() - start_time
                next_updates = [schedule.seconds_until_due() for schedule in self.schedules.values()]
                time_to_sleep: float = max(0.0, min(next_updates, default=600))
                if self.has_cron:
                    # Recheck wall-clock deadlines after system clock changes.
                    time_to_sleep = min(time_to_sleep, 60)
                logging.debug('looped in %.2fms, sleeping %.2fs.', time_taken * 1000, time_to_sleep)
                await asyncio.sleep(time_to_sleep)
        except KeyboardInterrupt:
            await self.mqtt_handler.mqttc.disconnect()

    async def fetch(self, url: str, headers: dict | None) -> str:
        async with httpcore.AsyncConnectionPool() as http:
            try:
                response = await http.request('GET', url, headers=headers or self.headers)
                return response.content.decode()
            except ConnectError as e:
                logging.warning(f'{e=}, {url=}')
            except Exception as e:
                logging.error(f'{e=}, {url=}')
            return ''


async def main():
    app = Json2Mqtt()
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    def shutdown_handler():
        if not main_task.done():
            main_task.cancel()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)
    except NotImplementedError:
        pass
    try:
        await app.loop()
    except asyncio.CancelledError:
        logging.info('exiting.')
    finally:
        await app.exit()
        logging.info('exited.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('gmqtt').setLevel(logging.ERROR)
    logging.info('starting Json2Mqtt v%s.', __version__)
    asyncio.run(main())
