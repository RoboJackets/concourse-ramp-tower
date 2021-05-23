"""
Synchronize Concourse worker state with AWS auto scaling groups
"""

import logging
import sys
from json import dumps, loads
from logging import getLogger
from os import environ
from typing import Dict

from concourse_ramp_tower.autoscaling import (
    AUTO_SCALING_LAUNCHING,
    AUTO_SCALING_TERMINATING,
    HEALTHY,
    UNHEALTHY,
    complete_lifecycle_action,
    get_instances,
    get_public_ips,
    instance_is_healthy,
    record_lifecycle_action_heartbeat,
    set_health,
)
from concourse_ramp_tower.concourse import AccessTokenManager, get_worker_states
from concourse_ramp_tower.config import get_role_for_instance
from concourse_ramp_tower.ssm import get_termination_command_state, send_termination_command

CONCOURSE_HOSTNAME = environ["CONCOURSE_HOSTNAME"]
CONCOURSE_CLIENT_ID = environ["CONCOURSE_CLIENT_ID"]
CONCOURSE_CLIENT_SECRET = environ["CONCOURSE_CLIENT_SECRET"]

AUTO_SCALING_GROUPS = loads(environ["AUTO_SCALING_GROUPS"])

SSM_SNS_TOPIC_ARN = environ["SSM_SNS_TOPIC_ARN"]

access_token_manager = AccessTokenManager(CONCOURSE_HOSTNAME, CONCOURSE_CLIENT_ID, CONCOURSE_CLIENT_SECRET)


def handler(  # pylint: disable=too-many-branches,too-many-statements
    event: Dict[str, str], context: None  # pylint: disable=unused-argument
) -> None:
    """
    Receives events from CloudWatch, Auto Scaling, and SSM, and updates state as appropriate

    :param event: event that was received
    :param context: ignored
    """
    print(dumps(event))

    logger = getLogger()
    logger.setLevel(logging.INFO)

    if event["source"] == "aws.events":
        logger.info("Invoked by cron")

        web_is_healthy = False

        instances = get_instances()
        public_ips = get_public_ips(instances)

        for instance in instances:
            role = get_role_for_instance(instance, AUTO_SCALING_GROUPS)

            if role is None:
                continue

            if role == "worker":
                continue

            if instance["LifecycleState"] == "Pending:Wait":
                if instance_is_healthy(instance, public_ips, AUTO_SCALING_GROUPS, CONCOURSE_HOSTNAME):
                    logger.info(
                        f"Completing launch lifecycle action for instance {instance['InstanceId']} in group {instance['AutoScalingGroupName']}"  # noqa
                    )
                    complete_lifecycle_action(instance, AUTO_SCALING_LAUNCHING)
                    if role == "web":
                        web_is_healthy = True
            elif instance["LifecycleState"] in ("Terminating", "Terminating:Wait"):
                termination_command_state = get_termination_command_state(instance)
                logger.info(
                    f"Instance {instance['InstanceId']} in group {instance['AutoScalingGroupName']} is about to be terminated, command state is {termination_command_state}"  # noqa
                )
                if termination_command_state is None:
                    logger.info("Sending termination command")
                    send_termination_command(instance, AUTO_SCALING_GROUPS, SSM_SNS_TOPIC_ARN)
                    continue
                if termination_command_state in ("Pending", "InProgress"):
                    logger.info("Recording heartbeat for lifecycle hook")
                    record_lifecycle_action_heartbeat(instance)
                else:
                    logger.info("Marking lifecycle action as complete")
                    complete_lifecycle_action(instance, AUTO_SCALING_TERMINATING)
            elif instance["LifecycleState"] == "InService":
                actually_healthy = instance_is_healthy(instance, public_ips, AUTO_SCALING_GROUPS, CONCOURSE_HOSTNAME)
                if (instance["HealthStatus"] == UNHEALTHY and actually_healthy) or (
                    instance["HealthStatus"] == HEALTHY and not actually_healthy
                ):
                    logger.info(
                        f"Instance {instance['InstanceId']} in group {instance['AutoScalingGroupName']} is marked as {instance['HealthStatus']} but actually_healthy is {actually_healthy}"  # noqa
                    )
                    set_health(instance, actually_healthy)
                print(actually_healthy)
                if actually_healthy and role == "web":
                    web_is_healthy = True

        if not web_is_healthy:
            return

        logger.info("Web appears to be healthy, checking workers")

        worker_states = get_worker_states(CONCOURSE_HOSTNAME, access_token_manager.get_access_token())

        for instance in instances:
            role = get_role_for_instance(instance, AUTO_SCALING_GROUPS)

            if role is None:
                continue

            if role != "worker":
                continue

            instance_id = instance["InstanceId"]

            if instance["LifecycleState"] == "Pending:Wait":
                if instance_id in worker_states and worker_states[instance_id] == "running":
                    logger.info(
                        f"Completing launch lifecycle action for instance {instance['InstanceId']} in group {instance['AutoScalingGroupName']}"  # noqa
                    )
                    complete_lifecycle_action(instance, AUTO_SCALING_LAUNCHING)
            elif instance["LifecycleState"] in ("Terminating", "Terminating:Wait"):
                termination_command_state = get_termination_command_state(instance)
                logger.info(
                    f"Instance {instance['InstanceId']} in group {instance['AutoScalingGroupName']} is about to be terminated, command state is {termination_command_state}"  # noqa
                )
                if termination_command_state is None:
                    logger.info("Sending termination command")
                    send_termination_command(instance, AUTO_SCALING_GROUPS, SSM_SNS_TOPIC_ARN)
                    continue
                if termination_command_state in ("Pending", "InProgress"):
                    logger.info("Recording heartbeat for lifecycle hook")
                    record_lifecycle_action_heartbeat(instance)
                else:
                    logger.info("Marking lifecycle action as complete")
                    complete_lifecycle_action(instance, AUTO_SCALING_TERMINATING)
            elif instance["LifecycleState"] == "InService":
                actually_healthy = instance_id in worker_states
                if (instance["HealthStatus"] == UNHEALTHY and actually_healthy) or (
                    instance["HealthStatus"] == HEALTHY and not actually_healthy
                ):
                    logger.info(
                        f"Instance {instance['InstanceId']} in group {instance['AutoScalingGroupName']} is marked as {instance['HealthStatus']} but actually_healthy is {actually_healthy}"  # noqa
                    )
                    set_health(instance, actually_healthy)


if __name__ == "__main__":
    stream_handler = logging.StreamHandler(sys.stdout)

    main_logger = logging.getLogger()
    main_logger.addHandler(stream_handler)

    handler({"source": "aws.events"}, None)
