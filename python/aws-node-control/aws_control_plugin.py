#!/usr/bin/env python3.9

from curses import meta
import sys
import enum
import typing
import time
from dataclasses import dataclass, field
import boto3
import botocore
from arcaflow_plugin_sdk import plugin

@dataclass
class NodeAction(enum.Enum):
    STOP = "stop"
    FORCE_STOP = "force_stop"
    START = "start"
    REBOOT = "reboot"
    TERMINATE = "terminate"


@dataclass
class NodeActionParams:
    """
    The structure that has info needed to control the nodes.
    """
    action: NodeAction = field(metadata={
        "name": "action",
        "description": "The action to execute."
    })
    instance_id: str = field(metadata={
        "name": "addr",
        "description": "The AWS instance ID."
    })
    aws_access_key_id: str = field(metadata={
        "name": "aws_access_public_key",
        "description": "The AWS public key/key id."
    })
    aws_access_private_key: str = field(metadata={
        "name": "aws_access_private_key",
        "description": "The AWS private/access key."
    })
    aws_region: str = field(metadata={
        "name": "aws_region",
        "description": "The AWS region the instance is in."
    })
    wait: bool = field(metadata={
        "name": "wait",
        "description": "Whether to wait for it to complete its action."
    })
    wait_timeout: int = field(
        default=30,
        metadata={
            "name": "wait_timeout",
            "description": "The amount of time in seconds to spend waiting for the desired state."
        }
    )


@dataclass
class SuccessOutput:
    """
    The output when the operation succeeds.
    """
    ms_duration: int
    final_power_state_on: bool


@dataclass
class ErrorOutput:
    """
    This is the output data structure in the error  case.
    """
    error: str = field(metadata={"name": "Error", "description": "An explanation why the execution failed."})


def get_boto3_ec2_client(instance_id, key_id, access_key, region):
    session = boto3.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=access_key,
        region_name=region
    )
    return session.client('ec2'), session.resource('ec2').Instance(instance_id)


def wait_for_power_state(resource, desired_power_state_on, timeout=600):
    if desired_power_state_on:
        resource.wait_until_running(timeout)
    else:
        resource.wait_until_stopped(timeout)

# The following is a decorator (starting with @). We add this in front of our function to define the metadata for our
# step.
@plugin.step(
    id="aws-ec2-action",
    name="Run AWs ec2 action",
    description="Runs IPMI node actions",
    outputs={"success": SuccessOutput, "error": ErrorOutput},
)
def aws_action(params: NodeActionParams) -> typing.Tuple[str, typing.Union[SuccessOutput, ErrorOutput]]:
    """
    The function runs the specified IPMI/BMC commands, and optionally waits for them to complete.
    Note: Timeout is not supported for terminate

    :param params:

    :return: the string identifying which output it is, as well the output structure
    """
    start_time = time.time()
    client, resource = get_boto3_ec2_client(params.instance_id, params.aws_access_key_id, params.aws_access_private_key, params.aws_region)
    if params.action.value == NodeAction.REBOOT.value:
        if resource.state['Name'] == "stopped" or resource.state['Name'] == "terminated":
            return "error", ErrorOutput("Node must be running to reboot.")
 
    if params.action.value == NodeAction.START.value:
        print(client.start_instances(InstanceIds=[params.instance_id]))
    elif params.action.value == NodeAction.STOP.value:
        print(client.stop_instances(InstanceIds=[params.instance_id], Force=False))
    elif params.action.value == NodeAction.FORCE_STOP.value:
        print(client.stop_instances(InstanceIds=[params.instance_id], Force=True))
    elif params.action.value == NodeAction.REBOOT.value:
        print(client.reboot_instances(InstanceIds=[params.instance_id]))
    elif params.action.value == NodeAction.TERMINATE.value:
        print(client.terminate_instances(InstanceIds=[params.instance_id]))
    else:
        return "error", ErrorOutput("Unknown action " + str(params.action.value))

    intended_final_power_state = params.action.value == NodeAction.REBOOT.value \
        or params.action.value == NodeAction.START.value
    if params.wait:
        print("Waiting for final state")
        try:
            if params.action.value != NodeAction.TERMINATE.value:
                wait_for_power_state(resource, intended_final_power_state, \
                    params.wait_timeout)
            else:
                # Timeout not supported by wait_until_terminated
                resource.wait_until_terminated()
        except botocore.exceptions.WaiterError as e:
            return "error", ErrorOutput("Failed due to timeout while waiting for final state.")

    resource.reload() # Ensure that the state is up to date
    final_power_state_on = resource.state['Name'] == "running"

    # If not waiting, successful because it issued the command without error
    # If waiting, 
    if not params.wait or final_power_state_on == intended_final_power_state:
        return "success", SuccessOutput(int(1000 * (time.time() - start_time)), final_power_state_on)
    else:
        return "error", ErrorOutput("Did not reach desired final state within timeout period. Power state is " \
            + str(resource.state['Name']))


if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        # List your step functions here:
        aws_action,
    )))
