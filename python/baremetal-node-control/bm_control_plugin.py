#!/usr/bin/env python3.9

from curses import meta
import sys
import enum
import typing
import time
from dataclasses import dataclass, field
import pyipmi
import pyipmi.interfaces
from arcaflow_plugin_sdk import plugin

@dataclass
class NodeAction(enum.Enum):
    STOP = "stop"
    START = "start"
    REBOOT = "reboot"
    HARD_RESET = "hard_reset"
    DIAGNOSTIC_INTERRUPT = "diagnostic_interrupt"
    SOFT_STOP = "soft_stop"

nodeActionEnumToInt = {
    NodeAction.STOP.value: 0,
    NodeAction.START.value: 1,
    NodeAction.REBOOT.value: 2,
    NodeAction.HARD_RESET.value: 3,
    NodeAction.DIAGNOSTIC_INTERRUPT.value: 4,
    NodeAction.SOFT_STOP.value: 5
}


@dataclass
class NodeActionParams:
    """
    The structure that has info needed to control the nodes.
    """
    action: NodeAction = field(metadata={
        "name": "action",
        "description": "The action to execute."
    })
    addr: str = field(metadata={
        "name": "addr",
        "description": "The address of the IPMI/BMC interface."
    })
    user: str = field(metadata={
        "name": "user",
        "description": "The user to authenticate the IPMI/BMC interface."
    })
    password: str = field(metadata={
        "name": "password",
        "description": "The password to authenticate the IPMI/BMC interface."
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


def get_ipmi_connection(bmc_addr, user, passwd):
    type_position = bmc_addr.find("://")
    if type_position == -1:
        host = bmc_addr
    else:
        host = bmc_addr[type_position + 3 :]
    port_position = host.find(":")
    if port_position == -1:
        port = 623
    else:
        port = int(host[port_position + 1 :])
        host = host[0:port_position]

    if user is None or passwd is None:
        return (False, "Missing IPMI BMI user and/or password for baremetal cloud. " + \
            "Please specify either a global or per-machine user and pass"
        )

    # Establish connection
    interface = pyipmi.interfaces.create_interface("ipmitool", interface_type="lanplus")

    connection = pyipmi.create_connection(interface)

    connection.target = pyipmi.Target(ipmb_address=0x20)
    connection.session.set_session_type_rmcp(host, port)
    connection.session.set_auth_type_user(user, passwd)
    connection.session.establish()
    return (True, connection)


def wait_for_power_state(connection, desired_power_state_on, timeout):
    timeout_time = time.time() + timeout
    while not connection.get_chassis_status().power_on == desired_power_state_on \
        and time.time() < timeout_time:
        time.sleep(0.5)


@plugin.step(
    id="ipmi-node-action",
    name="Run IPMI node action",
    description="Runs IPMI node actions",
    outputs={"success": SuccessOutput, "error": ErrorOutput},
)
def ipmi_action(params: NodeActionParams) -> typing.Tuple[str, typing.Union[SuccessOutput, ErrorOutput]]:
    """
    The function runs the specified IPMI/BMC commands, and optionally waits for them to complete.

    :param params:

    :return: the string identifying which output it is, as well the output structure
    """
    is_restart = params.action.value == NodeAction.REBOOT.value or params.action == NodeAction.HARD_RESET.value
    is_diagnostic_interrupt = params.action.value == NodeAction.DIAGNOSTIC_INTERRUPT
    print("Requested action:", params.action, "Reboot:", is_restart)
    start_time = time.time()
    try:
        success, conn = get_ipmi_connection(params.addr, params.user, params.password)
        if not success:
            return "error", ErrorOutput(conn) # conn is an error message

        # For restart waiting to work properly, we need to see if the server is on,
        # and if it is, wait for it to turn off before waiting for it to turn on again.
        if is_restart:
            original_power_state_on = conn.get_chassis_status().power_on
            print("Chassis originally on?", original_power_state_on)

        conn.chassis_control(nodeActionEnumToInt[params.action.value])
    except pyipmi.errors.IpmiConnectionError as e:
        return "error", ErrorOutput("Failed to connect: " + str(e))

    if params.wait:
        if is_restart and original_power_state_on:
            # Wait to turn off if restarting/resetting
            print("Waiting for power off")
            wait_for_power_state(conn, False)
        
        if not is_diagnostic_interrupt:
            print("Waiting for final state")
            intended_final_power_state = is_restart or params.action.value == NodeAction.START.value
            wait_for_power_state(conn, intended_final_power_state, params.wait_timeout)
    final_power_state_on = conn.get_chassis_status().power_on

    # Success if not waiting for a state, and success if reached state when waiting.
    if is_diagnostic_interrupt or not params.wait or final_power_state_on == intended_final_power_state:
        return "success", SuccessOutput(int(1000 * (time.time() - start_time)), final_power_state_on)
    else:
        return "error", ErrorOutput("Did not reach desired final state within timeout period")


if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        # List your step functions here:
        ipmi_action,
    )))
