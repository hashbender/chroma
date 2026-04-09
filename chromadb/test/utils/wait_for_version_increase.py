import time
from typing import Callable, TypeVar

from chromadb.api import ClientAPI
from chromadb.test.conftest import COMPACTION_SLEEP

TIMEOUT_INTERVAL = 1

T = TypeVar("T")


def get_collection_version(client: ClientAPI, collection_name: str) -> int:
    coll = client.get_collection(collection_name)
    return coll.get_model()["version"]


def wait_for_version_increase(
    client: ClientAPI,
    collection_name: str,
    initial_version: int,
    additional_time: int = 0,
) -> int:
    timeout = COMPACTION_SLEEP
    deadline = time.time() + timeout + additional_time
    target_version = initial_version + 1

    curr_version = get_collection_version(client, collection_name)
    if curr_version == initial_version:
        print(
            "[wait_for_version_increase] "
            f"collection={collection_name} "
            f"waiting for version >= {target_version} "
            f"(current={curr_version}, timeout={timeout + additional_time}s)"
        )
    while curr_version == initial_version:
        time.sleep(TIMEOUT_INTERVAL)
        if time.time() > deadline:
            collection_id = client.get_collection(collection_name).id
            raise TimeoutError(
                "Model was not updated in time for "
                f"{collection_id}; waited for version >= {target_version}, "
                f"last seen version {curr_version}"
            )
        curr_version = get_collection_version(client, collection_name)

    return curr_version


def poll_for_condition(
    condition_fn: Callable[[], T],
    timeout: int = 120,
    interval: float = 2,
    description: str = "condition",
) -> T:
    """Poll until condition_fn returns a truthy value, or raise TimeoutError.

    This replaces fixed time.sleep() calls for waiting on eventually-consistent
    state (e.g. query cache invalidation after compaction). Instead of sleeping
    for a fixed duration that may be too short under load or wastefully long,
    this polls at a regular interval until the condition is met.

    Args:
        condition_fn: A callable that returns a truthy value when the condition is met.
        timeout: Maximum time in seconds to wait.
        interval: Time in seconds between polls.
        description: Human-readable description for timeout error messages.

    Returns:
        The truthy return value from condition_fn.
    """
    deadline = time.time() + timeout
    last_result = None
    while time.time() < deadline:
        try:
            result = condition_fn()
            if result:
                return result
            last_result = result
        except Exception:
            pass
        time.sleep(interval)
    raise TimeoutError(
        f"Timed out after {timeout}s waiting for {description}. "
        f"Last result: {last_result}"
    )
