import json
import unittest
from unittest.mock import AsyncMock, call, patch

import yaml

from json2mqtt import Json2Mqtt


class Json2MqttConditionTests(unittest.IsolatedAsyncioTestCase):
    def make_app(self, topics, content, **request_options):
        config = {'requests': {'url': {'topics': topics, **request_options}}}
        with patch('json2mqtt.get_first_config', return_value=config), patch('json2mqtt.MqttHandler'):
            app = Json2Mqtt()
        app.fetch = AsyncMock(return_value=json.dumps(content))
        return app

    async def test_matching_values_publish(self):
        for expected in (0, 1, False, True, 'online', '', None):
            with self.subTest(expected=expected):
                app = self.make_app({
                    'value': {'path': ['value'], 'if': {'path': ['status'], 'equals': expected}},
                }, {'status': expected, 'value': 42})
                await app.loop_iteration()
                app.mqtt_handler.publish.assert_called_once_with('value', 42, False)

    async def test_nonmatching_values_skip_before_processing_topic_value(self):
        for actual, expected in ((0, 1), (1, 0), ('1', 1), (1, '1'), (None, 0)):
            with self.subTest(actual=actual, expected=expected):
                app = self.make_app({
                    'value': {
                        'path': ['missing'], 'offset': 1,
                        'if': {'path': ['status'], 'equals': expected},
                    },
                }, {'status': actual})
                with self.assertNoLogs(level='WARNING'):
                    await app.loop_iteration()
                app.mqtt_handler.publish.assert_not_called()

    async def test_topics_have_independent_conditions_and_keep_value_options(self):
        topics = yaml.safe_load('''
            active:
              path: [measurement]
              if:
                path: [devices, 0, enabled]
                equals: 1
              offset: 2
              factor: 0.5
              retain: true
            inactive:
              path: [measurement]
              if:
                path: [devices, 0, enabled]
                equals: 0
            unconditional:
              path: [measurement]
        ''')
        app = self.make_app(topics, {'devices': [{'enabled': 1}], 'measurement': '40'})
        await app.loop_iteration()
        self.assertEqual(app.mqtt_handler.publish.call_args_list, [
            call('active', 21.0, True), call('unconditional', '40', False),
        ])

    async def test_unreadable_condition_path_skips_only_affected_topic(self):
        for content, path in (
            ({}, ['missing']),
            ({'status': None}, ['status', 'enabled']),
            ({'status': 1}, ['status', 'enabled']),
            ({'status': []}, ['status', 0]),
            ({'status': []}, ['status', 'enabled']),
        ):
            with self.subTest(content=content, path=path):
                app = self.make_app({
                    'conditional': {'path': ['value'], 'if': {'path': path, 'equals': None}},
                    'unconditional': {'path': ['value']},
                }, {**content, 'value': 42})
                with self.assertLogs(level='WARNING') as logs:
                    await app.loop_iteration()
                self.assertIn('conditional', logs.output[0])
                app.mqtt_handler.publish.assert_called_once_with('unconditional', 42, False)

    async def test_condition_is_reevaluated_for_each_response(self):
        app = self.make_app({
            'value': {'path': ['value'], 'if': {'path': ['enabled'], 'equals': 1}},
        }, {})
        app.fetch.side_effect = [
            '{"enabled": 1, "value": 10}',
            '{"enabled": 0, "value": 20}',
            '{"enabled": 1, "value": 30}',
        ]
        with patch.object(app.schedules['url'], 'seconds_until_due', return_value=0):
            for _ in range(3):
                await app.loop_iteration()
        self.assertEqual(app.mqtt_handler.publish.call_args_list, [
            call('value', 10, False), call('value', 30, False),
        ])

    async def test_empty_paths_select_response_root(self):
        app = self.make_app({
            'value': {'path': [], 'if': {'path': [], 'equals': 0}},
        }, 0)
        await app.loop_iteration()
        app.mqtt_handler.publish.assert_called_once_with('value', 0, False)

    async def test_html_table_conditions_use_string_cells(self):
        for expected in ('1', 1):
            with self.subTest(expected=expected):
                app = self.make_app({
                    'value': {'path': [0, 1], 'if': {'path': [0, 0], 'equals': expected}},
                }, None, html_table=0)
                app.fetch.return_value = '<table><tr><td>1</td><td>42</td></tr></table>'
                await app.loop_iteration()
                if expected == '1':
                    app.mqtt_handler.publish.assert_called_once_with('value', '42', False)
                else:
                    app.mqtt_handler.publish.assert_not_called()

    def test_invalid_conditions_fail_before_mqtt_connection(self):
        for condition in (
            None, False, [], 'enabled', {}, {'equals': 1}, {'path': ['enabled']},
            {'path': 'enabled', 'equals': 1},
            {'path': [None], 'equals': 1},
            {'path': [True], 'equals': 1},
            {'path': [0.5], 'equals': 1},
        ):
            with self.subTest(condition=condition):
                config = {'requests': {'url': {'topics': {
                    'value': {'path': ['value'], 'if': condition},
                }}}}
                with patch('json2mqtt.get_first_config', return_value=config):
                    with patch('json2mqtt.MqttHandler') as mqtt:
                        with self.assertRaisesRegex(ValueError, 'invalid if condition.*value'):
                            Json2Mqtt()
                        mqtt.assert_not_called()


if __name__ == '__main__':
    unittest.main()
