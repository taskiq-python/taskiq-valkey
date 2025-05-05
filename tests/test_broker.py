import asyncio
import uuid
from typing import List, Tuple, Union

import pytest
from taskiq import AckableMessage, AsyncBroker, BrokerMessage

from taskiq_valkey import (
    ListQueueClusterBroker,
    ListQueueSentinelBroker,
    PubSubBroker,
    PubSubSentinelBroker,
    ValkeyStreamClusterBroker,
    ValkeyStreamSentinelBroker,
)
from taskiq_valkey.valkey_broker import ValkeyStreamBroker


def test_no_url_should_raise_typeerror() -> None:
    """Test that url is expected."""
    with pytest.raises(TypeError):
        PubSubBroker()  # type: ignore


async def get_message(
    broker: AsyncBroker,
) -> Union[bytes, AckableMessage]:
    """
    Get a message from the broker.

    :param broker: async message broker.
    :return: first message from listen method.
    """
    async for message in broker.listen():
        return message
    return b""


@pytest.fixture
def valid_broker_message() -> BrokerMessage:
    """
    Generate valid broker message for tests.

    :returns: broker message.
    """
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=b"my_msg",
        labels={
            "label1": "val1",
        },
    )


async def test_pub_sub_broker(
    valid_broker_message: BrokerMessage,
    valkey_url: str,
) -> None:
    """
    Test that messages are published and read correctly by PubSubBroker.

    We create two workers that listen and send a message to them.
    Expect both workers to receive the same message we sent.
    """
    broker = PubSubBroker(url=valkey_url, queue_name=uuid.uuid4().hex)
    worker1_task = asyncio.create_task(get_message(broker))
    worker2_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    message1 = worker1_task.result()
    message2 = worker2_task.result()
    assert message1 == valid_broker_message.message
    assert message1 == message2
    await broker.shutdown()


async def test_pub_sub_broker_max_connections(
    valid_broker_message: BrokerMessage,
    valkey_url: str,
) -> None:
    """Test PubSubBroker with connection limit set."""
    broker = PubSubBroker(
        url=valkey_url,
        queue_name=uuid.uuid4().hex,
        max_connection_pool_size=4,
        timeout=1,
    )
    worker_tasks = [asyncio.create_task(get_message(broker)) for _ in range(3)]
    await asyncio.sleep(0.3)

    await asyncio.gather(*[broker.kick(valid_broker_message) for _ in range(50)])
    await asyncio.sleep(0.3)

    for worker in worker_tasks:
        worker.cancel()
    await broker.shutdown()


async def test_stream_broker(
    valid_broker_message: BrokerMessage,
    valkey_url: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = ValkeyStreamBroker(
        url=valkey_url,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
    )
    await broker.startup()

    worker1_task = asyncio.create_task(get_message(broker))
    worker2_task = asyncio.create_task(get_message(broker))

    await broker.kick(valid_broker_message)

    await asyncio.wait(
        [worker1_task, worker2_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    assert worker1_task.done() != worker2_task.done()
    message = worker1_task.result() if worker1_task.done() else worker2_task.result()
    assert isinstance(message, AckableMessage)
    assert message.data == valid_broker_message.message
    await message.ack()  # type: ignore
    worker1_task.cancel()
    worker2_task.cancel()
    await broker.shutdown()


async def test_list_queue_cluster_broker(
    valid_broker_message: BrokerMessage,
    valkey_cluster_url: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueClusterBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = ListQueueClusterBroker(
        url=valkey_cluster_url,
        queue_name=uuid.uuid4().hex,
    )
    worker_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    assert worker_task.done()
    assert worker_task.result() == valid_broker_message.message
    worker_task.cancel()
    await broker.shutdown()


async def test_stream_cluster_broker(
    valid_broker_message: BrokerMessage,
    valkey_cluster_url: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueClusterBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = ValkeyStreamClusterBroker(
        url=valkey_cluster_url,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
    )
    await broker.startup()

    worker_task = asyncio.create_task(get_message(broker))

    await broker.kick(valid_broker_message)

    result = await worker_task

    assert isinstance(result, AckableMessage)
    assert result.data == valid_broker_message.message
    await result.ack()  # type: ignore
    worker_task.cancel()
    await broker.shutdown()


async def test_pub_sub_sentinel_broker(
    valid_broker_message: BrokerMessage,
    valkey_sentinels: List[Tuple[str, int]],
    valkey_sentinel_master_name: str,
) -> None:
    """
    Test that messages are published and read correctly by PubSubSentinelBroker.

    We create two workers that listen and send a message to them.
    Expect both workers to receive the same message we sent.
    """
    broker = PubSubSentinelBroker(
        sentinels=valkey_sentinels,
        master_name=valkey_sentinel_master_name,
        queue_name=uuid.uuid4().hex,
    )
    worker1_task = asyncio.create_task(get_message(broker))
    worker2_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    message1 = worker1_task.result()
    message2 = worker2_task.result()
    assert message1 == valid_broker_message.message
    assert message1 == message2
    await broker.shutdown()


async def test_list_queue_sentinel_broker(
    valid_broker_message: BrokerMessage,
    valkey_sentinels: List[Tuple[str, int]],
    valkey_sentinel_master_name: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueSentinelBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = ListQueueSentinelBroker(
        sentinels=valkey_sentinels,
        master_name=valkey_sentinel_master_name,
        queue_name=uuid.uuid4().hex,
    )
    worker_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    assert worker_task.done()
    assert worker_task.result() == valid_broker_message.message
    worker_task.cancel()
    await broker.shutdown()


async def test_streams_sentinel_broker(
    valid_broker_message: BrokerMessage,
    valkey_sentinels: List[Tuple[str, int]],
    valkey_sentinel_master_name: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueSentinelBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = ValkeyStreamSentinelBroker(
        sentinels=valkey_sentinels,
        master_name=valkey_sentinel_master_name,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
    )
    await broker.startup()
    worker_task = asyncio.create_task(get_message(broker))

    await broker.kick(valid_broker_message)

    result = await worker_task
    assert isinstance(result, AckableMessage)
    assert result.data == valid_broker_message.message
    await result.ack()  # type: ignore
    worker_task.cancel()
    await broker.shutdown()
