"""
Handles plucking values from the configuration dict
"""
from typing import Dict, Optional


def get_role_for_instance(instance: Dict[str, str], groups: Dict[str, Dict[str, str]]) -> Optional[str]:
    """
    Returns the "role," e.g. web, worker

    :param instance: instance to look up
    :param groups: configuration dict
    :return: role string for the instance, or None if the auto scaling group is not in the configuration
    """
    asg_name = instance["AutoScalingGroupName"]
    if asg_name in groups:
        return groups[asg_name]["role"]

    return None


def get_health_check_endpoint_for_instance(instance: Dict[str, str], groups: Dict[str, Dict[str, str]]) -> str:
    """
    Returns the endpoint that should be used for health checks

    :param instance: instance to look up
    :param groups: configuration dict
    :return: endpoint to use for health checks
    """
    return groups[instance["AutoScalingGroupName"]]["health_check_endpoint"]
