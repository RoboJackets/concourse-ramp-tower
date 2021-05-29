"""
Wraps the Systems Manager client with a simpler interface
"""
from logging import getLogger
from typing import Dict, Optional

from boto3 import client  # type: ignore

ssm = client("ssm")


SSM_TERMINATION_DOCUMENT_NAME = "AWS-RunShellScript"


def get_termination_command_state(instance: Dict[str, str]) -> Optional[str]:
    """
    Returns the state of the most recent termination command, or None if one has not been sent

    :param instance: instance to terminate
    :return: state of most recent termination command
    """
    invocations = ssm.list_command_invocations(
        InstanceId=instance["InstanceId"], Filters=[{"key": "DocumentName", "value": SSM_TERMINATION_DOCUMENT_NAME}]
    )["CommandInvocations"]

    if len(invocations) == 0:
        return None

    invocations.sort(key=lambda invocation: invocation["RequestedDateTime"], reverse=True)

    return invocations[0]["Status"]  # type: ignore


def send_termination_command(
    instance: Dict[str, str], groups: Dict[str, Dict[str, str]], topic_arn: str, service_role_arn: str
) -> None:
    """
    Sends a termination command to an instance

    :param instance: instance to terminate
    :param groups: configuration dict
    :param topic_arn: SNS topic to provide for notifications
    """
    try:
        ssm.send_command(
            InstanceIds=[
                instance["InstanceId"],
            ],
            DocumentName=SSM_TERMINATION_DOCUMENT_NAME,
            DocumentVersion="$LATEST",
            Comment="Instance is about to be terminated",
            Parameters={"commands": [groups[instance["AutoScalingGroupName"]]["stop_command"]]},
            NotificationConfig={
                "NotificationArn": topic_arn,
                "NotificationEvents": ["All"],
                "NotificationType": "Invocation",
            },
            ServiceRoleArn=service_role_arn,
        )
    except ssm.exceptions.InvalidInstanceId:
        logger = getLogger()
        logger.info("Instance is not registered with SSM, skipping")
