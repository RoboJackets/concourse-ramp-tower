"""
Wraps the Concourse REST API
"""
from time import time
from typing import Dict

from requests import get, post


class AccessTokenManager:
    """
    Manages retrieval and caching of access tokens used for the Concourse REST API
    """

    def __init__(self, hostname: str, client_id: str, client_secret: str):
        self.access_token = None
        self.expires_at = 0

        self.hostname = hostname
        self.client_id = client_id
        self.client_secret = client_secret

    def get_fresh_access_token(self) -> str:
        """
        Get a fresh access token from Skymarshal

        :return: access token
        """
        response = post(
            f"https://{self.hostname}/sky/issuer/token",
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
        )

        status_code = response.status_code
        json = response.json()

        if status_code != 200:
            raise ValueError(f"Unexpected response code {status_code}: {json}")

        self.access_token = json["access_token"]
        self.expires_at = time() + json["expires_in"]

        return self.access_token  # type: ignore

    def get_access_token(self) -> str:
        """
        Return a cached access token if it is good, or call get_fresh_access_token

        :return: access token
        """
        if (time() - 60) > self.expires_at:
            return self.get_fresh_access_token()

        response = get(f"https://{self.hostname}/api/v1/user", headers={"Authorization": f"Bearer {self.access_token}"})

        if response.status_code != 200:
            raise ValueError(f"Unexpected response code {response.status_code}: {response.json()}")

        return self.get_fresh_access_token()


def get_worker_states(hostname: str, access_token: str) -> Dict[str, str]:
    """
    Get the state of each worker

    :param hostname: Concourse web node hostname
    :param access_token: access token to use for authentication
    :return: dict of worker names to state
    """
    workers = {}

    response = get(f"https://{hostname}/api/v1/workers", headers={"Authorization": f"Bearer {access_token}"})

    status_code = response.status_code
    json = response.json()

    if status_code != 200:
        raise ValueError(f"Unexpected response code {status_code}: {json}")

    for worker in json:
        workers[worker["name"]] = worker["state"]

    return workers
