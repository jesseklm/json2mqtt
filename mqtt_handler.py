import hashlib
import logging

from gmqtt import Client as MQTTClient, Message

from background_tasks import run_in_background


class MqttHandler:
    def __init__(self, config: dict) -> None:
        self.topic_prefix: str = config.get('mqtt_topic', 'json2mqtt/').rstrip('/') + '/'
        self.host: str = config['mqtt_server']
        self.port: int = config.get('mqtt_port', 1883)

        client_id = hashlib.md5(f'j2mqtt-{self.host}{self.port}{self.topic_prefix}'.encode()).hexdigest()
        will_message: Message = Message(self.topic_prefix + 'available', 'offline', will_delay_interval=5, retain=True)
        self.mqttc: MQTTClient = MQTTClient(client_id=client_id, will_message=will_message)
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_disconnect = self.on_disconnect
        self.mqttc.set_auth_credentials(config['mqtt_username'], config['mqtt_password'])
        run_in_background(self.connect())

    def on_connect(self, client: MQTTClient, flags, rc, properties):
        self.publish('available', 'online', retain=True)
        logging.info('mqtt connected.')

    def publish(self, topic: str, payload: str | int | float, retain: bool = False) -> None:
        try:
            self.mqttc.publish(self.topic_prefix + topic, payload, retain=retain)
        except AttributeError:
            pass

    async def connect(self) -> bool:
        if self.mqttc.is_connected:
            return True
        try:
            await self.mqttc._connection.close()
        except AttributeError:
            pass
        except Exception as e:
            logging.warning(f'mqtt close: {self.host=}, {e=}')
        try:
            await self.mqttc.connect(self.host, self.port)
            return True
        except ConnectionRefusedError as e:
            logging.warning(f'mqtt: {self.host=}, {e=}')
        except Exception as e:
            logging.error(f'mqtt: {self.host=}, {e=}')
        return False

    async def disconnect(self):
        if self.mqttc.is_connected:
            await self.mqttc.disconnect(reason_code=4)

    @staticmethod
    def on_disconnect(packet, exc=None):
        logging.info('mqtt disconnected.')
