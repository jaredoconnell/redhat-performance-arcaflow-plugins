#!/usr/bin/env python3.9

import unittest
from bm_control_plugin import NodeAction
import bm_control_plugin
from arcaflow_plugin_sdk import plugin


class HelloWorldTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            bm_control_plugin.NodeActionParams(
                action = NodeAction.START,
                addr="127.0.0.1",
                user="user",
                password="password",
                wait=True,
                wait_timeout=30
            )
        )

        plugin.test_object_serialization(
            bm_control_plugin.SuccessOutput(
                ms_duration=50,
                final_power_state_on=True
            )
        )

        plugin.test_object_serialization(
            bm_control_plugin.ErrorOutput(
                error="This is an error"
            )
        )

    def test_functional(self):
        input = bm_control_plugin.NodeActionParams(
            action = NodeAction.START,
            addr="127.0.0.1:12345",
            user="user",
            password="password",
            wait=True,
            wait_timeout=30
        )

        output_id, output_data = bm_control_plugin.ipmi_action(input)

        # Error due to no server running at the address.
        self.assertEqual("error", output_id)
        self.assertIn("Failed to connect", output_data.error)


if __name__ == '__main__':
    unittest.main()
