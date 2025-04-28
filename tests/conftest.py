import os
from typing import List, Tuple

import pytest


@pytest.fixture
def valkey_url() -> str:
    """
    URL to connect to valkey.

    It tries to get it from environ,
    and return default one if the variable is
    not set.

    :return: URL string.
    """
    return os.environ.get("TEST_VALKEY_URL", "valkey://localhost:7000")


@pytest.fixture
def valkey_cluster_url() -> str:
    """
    URL to connect to valkey cluster.

    It tries to get it from environ,
    and return default one if the variable is
    not set.

    :return: URL string.
    """
    return os.environ.get("TEST_VALKEY_CLUSTER_URL", "valkey://localhost:7001")


@pytest.fixture
def valkey_sentinels() -> List[Tuple[str, int]]:
    """
    List of valkey sentinel hosts.

    It tries to get it from environ,
    and return default one if the variable is
    not set.

    :return: list of host and port pairs.
    """
    sentinels = os.environ.get("TEST_VALKEY_SENTINELS", "localhost:7002")
    host, _, port = sentinels.partition(":")
    return [(host, int(port))]


@pytest.fixture
def valkey_sentinel_master_name() -> str:
    """
    Valkey sentinel master name.

    It tries to get it from environ,
    and return default one if the variable is
    not set.

    :return: valkey sentinel master name string.
    """
    return os.environ.get("TEST_VALKEY_SENTINEL_MASTER_NAME", "myprimary")
