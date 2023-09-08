#!/usr/bin/env python3.9

from lib2to3.pytree import Node
import unittest
from unittest.mock import patch
from aws_control_plugin import NodeAction
import aws_control_plugin
from arcaflow_plugin_sdk import plugin
import boto3
from moto import mock_ec2

mock_region = "us-east-2"

class HelloWorldTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            aws_control_plugin.NodeActionParams(
                action = NodeAction.START,
                instance_id="none",
                aws_access_key_id="none",
                aws_access_private_key="none",
                aws_region="none",
                wait=True,
                wait_timeout=30
            )
        )

        plugin.test_object_serialization(
            aws_control_plugin.SuccessOutput(
                ms_duration=50,
                final_power_state_on=True
            )
        )

        plugin.test_object_serialization(
            aws_control_plugin.ErrorOutput(
                error="This is an error"
            )
        )


    #@patch("boto3.session.session.resource.Instance.wait_until_running")
    #@patch("boto3.session.client.start_instances")
    #@patch("boto3.session.session.resource.Instance")
    #@patch("aws_control_plugin.boto3.client")
    #@patch("aws_control_plugin.boto3")
    @mock_ec2
    def test_functional(self):

        # Setup the instance in mock
        client = boto3.client("ec2", region_name=mock_region)
        ec2 = boto3.resource("ec2", region_name=mock_region)
        instance = ec2.create_instances(ImageId="ami-12c6146b", MinCount=1, MaxCount=1)[0]

        input = aws_control_plugin.NodeActionParams(
            action = NodeAction.START,
            instance_id=instance.id,
            aws_access_key_id="test",
            aws_access_private_key="test",
            aws_region=mock_region,
            wait=True,
            wait_timeout=1
        )

        # Test start
        output_id, output_data = aws_control_plugin.aws_action(input)

        # Verify that it completed successfully
        self.assertEqual("success", output_id)
        self.assertEqual(True, output_data.final_power_state_on)
        # Verify that it did what it says it did
        instance.reload()
        self.assertEqual("running", instance.state['Name'])

        # Test stop
        input.action = NodeAction.STOP
        output_id, output_data = aws_control_plugin.aws_action(input)

        self.assertEqual("success", output_id)
        self.assertEqual(False, output_data.final_power_state_on)
        # Verify that it did what it says it did
        instance.reload()
        self.assertNotEqual("running", instance.state['Name'])

        # Start again
        input.action = NodeAction.START
        output_id, output_data = aws_control_plugin.aws_action(input)

        self.assertEqual("success", output_id)
        self.assertEqual(True, output_data.final_power_state_on)
        # Verify that it did what it says it did
        instance.reload()
        self.assertEqual("running", instance.state['Name'])

        # Test force stop
        input.action = NodeAction.FORCE_STOP
        output_id, output_data = aws_control_plugin.aws_action(input)

        self.assertEqual("success", output_id)
        self.assertEqual(False, output_data.final_power_state_on)
        # Verify that it did what it says it did
        instance.reload()
        self.assertNotEqual("running", instance.state['Name'])

        # Test reboot when node is off
        input.action = NodeAction.REBOOT
        output_id, output_data = aws_control_plugin.aws_action(input)

        self.assertEqual("error", output_id)

        # Start again
        input.action = NodeAction.START
        output_id, output_data = aws_control_plugin.aws_action(input)

        self.assertEqual("success", output_id)
        self.assertEqual(True, output_data.final_power_state_on)
        # Verify that it did what it says it did
        instance.reload()
        self.assertEqual("running", instance.state['Name'])

        # Test reboot when node is on
        input.action = NodeAction.REBOOT
        output_id, output_data = aws_control_plugin.aws_action(input)

        self.assertEqual("success", output_id)
        self.assertEqual(True, output_data.final_power_state_on)
        # Verify that it did what it says it did
        instance.reload()
        self.assertEqual("running", instance.state['Name'])

        # Test terminate
        input.action = NodeAction.TERMINATE
        output_id, output_data = aws_control_plugin.aws_action(input)

        self.assertEqual("success", output_id)
        self.assertEqual(False, output_data.final_power_state_on)
        # Verify that it did what it says it did
        instance.reload()
        self.assertEqual("terminated", instance.state['Name'])


if __name__ == '__main__':
    unittest.main()
