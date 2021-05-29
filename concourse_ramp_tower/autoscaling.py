"""
Wraps the Auto Scaling client with a simpler interface
"""
from json import JSONDecodeError
from logging import getLogger
from typing import Dict, List

from boto3 import client  # type: ignore

from concourse_ramp_tower.config import get_health_check_endpoint_for_instance, get_role_for_instance

from forcediphttpsadapter.adapters import ForcedIPHTTPSAdapter  # type: ignore

from requests import RequestException, Session, get


HEALTHY = "HEALTHY"
UNHEALTHY = "UNHEALTHY"

AUTO_SCALING_LAUNCHING = "autoscaling:EC2_INSTANCE_LAUNCHING"
AUTO_SCALING_TERMINATING = "autoscaling:EC2_INSTANCE_TERMINATING"

autoscaling = client("autoscaling")
ec2 = client("ec2")


def get_instances() -> List[Dict[str, str]]:
    """
    Returns all instances in any auto scaling group in the account

    :return: list of instances
    """
    return autoscaling.describe_auto_scaling_instances()["AutoScalingInstances"]  # type: ignore


def get_public_ips(instances: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Looks up public IP addresses for provided instances

    :param instances: list of instances
    :return: dict of instance IDs to public IP addresses
    """
    public_ips = {}

    ec2_response = ec2.describe_instances(InstanceIds=[instance["InstanceId"] for instance in instances])

    for reservation in ec2_response["Reservations"]:
        for instance in reservation["Instances"]:
            if "PublicIpAddress" in instance:
                public_ips[instance["InstanceId"]] = instance["PublicIpAddress"]

    return public_ips


def instance_is_healthy(
    instance: Dict[str, str], public_ips: Dict[str, str], groups: Dict[str, Dict[str, str]], concourse_hostname: str
) -> bool:
    """
    Determines if a (non-worker) instance is healthy

    :param instance: instance to check
    :param public_ips: dict from get_public_ips()
    :param groups: configuration dict
    :param concourse_hostname: hostname for concourse web node
    :return: whether the instance is healthy
    """
    instance_id = instance["InstanceId"]

    if instance_id not in public_ips:
        return False

    public_ip = public_ips[instance_id]

    if get_role_for_instance(instance, groups) == "web":
        try:
            # This mess is needed for Python to send the correct hostname during SNI
            # The ACME magic on the Concourse side requires it, otherwise the handshake fails
            session = Session()
            session.mount(f"https://{concourse_hostname}", ForcedIPHTTPSAdapter(dest_ip=public_ip))
            health_check_response = session.get(
                f"https://{concourse_hostname}/api/v1/info", headers={"Host": concourse_hostname}, verify=False
            )
            if health_check_response.status_code == 200 and isinstance(health_check_response.json(), dict):
                return True
        except (ConnectionError, RequestException, JSONDecodeError) as e:
            print(e)
    else:
        endpoint = get_health_check_endpoint_for_instance(instance, groups)
        try:
            health_check_response = get(f"https://{public_ip}{endpoint}", verify=False)
            if health_check_response.status_code == 200:
                if len(health_check_response.text) > 0:
                    if isinstance(health_check_response.json(), dict):
                        return True
                else:
                    return True
        except (ConnectionError, RequestException) as e:
            print(e)

    return False


def get_hook_name(instance: Dict[str, str], lifecycle_transition: str) -> str:
    """
    Get the name of the lifecycle hook for a given instance and transition

    :param instance: instance to look up
    :param lifecycle_transition: one of autoscaling:EC2_INSTANCE_LAUNCHING or autoscaling:EC2_INSTANCE_TERMINATING
    :return: name of the lifecycle hook
    """
    asg_name = instance["AutoScalingGroupName"]
    hooks = autoscaling.describe_lifecycle_hooks(AutoScalingGroupName=asg_name)["LifecycleHooks"]

    matching_hooks = [
        hook["LifecycleHookName"] for hook in hooks if hook["LifecycleTransition"] == lifecycle_transition
    ]

    if len(matching_hooks) != 1:
        raise ValueError(f"Found {len(matching_hooks)} {lifecycle_transition} hooks for {asg_name}, expected exactly 1")

    return matching_hooks[0]  # type: ignore


def instance_is_waiting(instance: Dict[str, str]) -> bool:
    """
    Determines if the given instance is waiting on a lifecycle hook

    :param instance: instance to check
    :return: whether the instance is waiting on a lifecycle hook
    """
    instance_id = instance["InstanceId"]
    autoscaling_response = autoscaling.describe_auto_scaling_instances(InstanceIds=[instance_id])

    for found_instance in autoscaling_response["AutoScalingInstances"]:
        if found_instance["InstanceId"] == instance_id and found_instance["LifecycleState"].endswith(":Wait"):
            return True

    return False


def complete_lifecycle_action(instance: Dict[str, str], lifecycle_transition: str) -> None:
    """
    Simple wrapper around the boto3 equivalent

    :param instance: instance to mark completed
    :param lifecycle_transition: one of autoscaling:EC2_INSTANCE_LAUNCHING or autoscaling:EC2_INSTANCE_TERMINATING
    """
    if not instance_is_waiting(instance):
        return

    try:
        autoscaling.complete_lifecycle_action(
            LifecycleHookName=get_hook_name(instance, lifecycle_transition),
            AutoScalingGroupName=instance["AutoScalingGroupName"],
            LifecycleActionResult="CONTINUE",
            InstanceId=instance["InstanceId"],
        )
    except autoscaling.exceptions.ResourceContentionFault:
        logger = getLogger()
        logger.info("Lifecycle action already updated")


def record_lifecycle_action_heartbeat(instance: Dict[str, str]) -> None:
    """
    Simple wrapper around the boto3 equivalent

    :param instance: instance to heartbeat
    """
    if not instance_is_waiting(instance):
        return

    try:
        autoscaling.record_lifecycle_action_heartbeat(
            LifecycleHookName=get_hook_name(instance, AUTO_SCALING_TERMINATING),
            AutoScalingGroupName=instance["AutoScalingGroupName"],
            InstanceId=instance["InstanceId"],
        )
    except autoscaling.exceptions.ResourceContentionFault:
        logger = getLogger()
        logger.info("Lifecycle action already updated")


def set_health(instance: Dict[str, str], healthy: bool) -> None:
    """
    Simple wrapper around the boto3 equivalent

    :param instance: instance to update
    :param healthy: whether it is healthy or not
    """
    autoscaling.set_instance_health(
        InstanceId=instance["InstanceId"], HealthStatus=HEALTHY.capitalize() if healthy else UNHEALTHY.capitalize()
    )
