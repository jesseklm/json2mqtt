import json
import logging
import signal
import sys
import time
from functools import reduce

import requests
from urllib3.exceptions import MaxRetryError, NameResolutionError

from config import get_first_config
from mqtt_handler import MqttHandler

__version__ = '1.0.1'


class Json2Mqtt:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_handler)
        signal.signal(signal.SIGTERM, self.exit_handler)
        config = get_first_config()
        if 'logging' in config:
            logging_level_name = config['logging'].upper()
            logging_level = logging.getLevelNamesMapping().get(logging_level_name, logging.NOTSET)
            if logging_level != logging.NOTSET:
                logging.getLogger().setLevel(logging_level)
            else:
                logging.warning('unknown logging level: %s.', logging_level)
        self.mqtt_handler = MqttHandler(config)
        self.headers: dict = config.get('headers')
        self.requests: dict = config['requests']
        self.update_rate: int = config.get('update_rate', 600)

    @staticmethod
    def exit_handler(signum, frame):
        logging.info('stopping Json2Mqtt.')
        sys.exit(0)

    def loop(self):
        while True:
            start_time = time.perf_counter()
            for url, request in self.requests.items():
                try:
                    response = requests.get(url, headers=self.headers)
                    value = json.loads(response.text)
                    value = reduce(lambda d, key: d[key], request['path'], value)
                    self.mqtt_handler.publish(request['topic'], value, request.get('retain', False))
                except MaxRetryError as e:
                    logging.error('%s max retries exceeded: %s', url, e)
                except NameResolutionError as e:
                    logging.error('%s failed to resolve hostname: %s', url, e)
                except ConnectionError as e:
                    logging.error('%s connection failed: %s', url, e)
                except Exception as e:
                    logging.error('%s failed: %s', url, e)
            time_taken = time.perf_counter() - start_time
            time_to_sleep = self.update_rate - time_taken
            logging.debug('looped in %.2fms, sleeping %.2fs.', time_taken * 1000, time_to_sleep)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('starting Json2Mqtt v%s.', __version__)
    app = Json2Mqtt()
    app.loop()
