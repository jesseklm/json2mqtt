import asyncio
import json
import logging
import signal
import time
from functools import reduce

import httpcore
from httpcore import ConnectError

from config import get_first_config
from mqtt_handler import MqttHandler

__version__ = '1.0.6'


class Json2Mqtt:
    def __init__(self):
        config = get_first_config()
        self.setup_logging(config)
        self.mqtt_handler = MqttHandler(config)
        self.headers: dict = config.get('headers')
        self.requests: dict = config['requests']
        self.update_rate: int = config.get('update_rate', 600)

    async def exit(self):
        await self.mqtt_handler.disconnect()

    @staticmethod
    def setup_logging(config):
        if 'logging' in config:
            logging_level_name = config['logging'].upper()
            logging_level = logging.getLevelNamesMapping().get(logging_level_name, logging.NOTSET)
            if logging_level != logging.NOTSET:
                logging.getLogger().setLevel(logging_level)
            else:
                logging.warning('unknown logging level: %s.', logging_level)

    async def loop_iteration(self) -> None:
        if not await self.mqtt_handler.connect():
            return
        for url, topics in self.requests.items():
            js_content = json.loads(await self.fetch(url))
            for topic, options in topics.items():
                try:
                    value = reduce(lambda d, key: d[key], options['path'], js_content)
                except KeyError as e:
                    logging.warning('unexpected response: %s. %s', js_content, str(e))
                    continue
                if 'factor' in options:
                    try:
                        value = float(value)
                        value = value * options['factor']
                    except ValueError as e:
                        logging.warning('failed to convert: %s. %s', value, str(e))
                        continue
                self.mqtt_handler.publish(topic, value, options.get('retain', False))

    async def loop(self) -> None:
        while True:
            start_time: float = time.perf_counter()
            await self.loop_iteration()
            time_taken: float = time.perf_counter() - start_time
            time_to_sleep: float = self.update_rate - time_taken
            logging.debug('looped in %.2fms, sleeping %.2fs.', time_taken * 1000, time_to_sleep)
            if time_to_sleep > 0:
                await asyncio.sleep(time_to_sleep)

    async def fetch(self, url: str) -> str:
        async with httpcore.AsyncConnectionPool() as http:
            try:
                response = await http.request("GET", url, headers=self.headers)
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
