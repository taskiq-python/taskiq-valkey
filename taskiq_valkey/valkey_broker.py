import sys
import uuid
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Optional,
    TypeVar,
)

from taskiq import AckableMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage
from valkey.asyncio import BlockingConnectionPool, Connection, ResponseError, Valkey

_T = TypeVar("_T")

logger = getLogger("taskiq.valkey_broker")

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool[Connection]  # type: ignore
else:
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool


class BaseValkeyBroker(AsyncBroker):
    """Base broker that works with Valkey."""

    def __init__(
        self,
        url: str,
        task_id_generator: Optional[Callable[[], str]] = None,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        queue_name: str = "taskiq",
        max_connection_pool_size: Optional[int] = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new broker.

        :param url: url to valkey.
        :param task_id_generator: custom task_id generator.
        :param result_backend: custom result backend.
        :param queue_name: name for a list in valkey.
        :param max_connection_pool_size: maximum number of connections in pool.
            Each worker opens its own connection. Therefore this value has to be
            at least number of workers + 1.
        :param connection_kwargs: additional arguments for valkey BlockingConnectionPool.
        """  # noqa: E501
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )

        self.connection_pool: _BlockingConnectionPool = BlockingConnectionPool.from_url(
            url=url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        self.queue_name = queue_name

    async def shutdown(self) -> None:
        """Closes valkey connection pool."""
        await super().shutdown()
        await self.connection_pool.disconnect()


class PubSubBroker(BaseValkeyBroker):
    """Broker that works with Valkey and broadcasts tasks to all workers."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        Publish message over PUBSUB channel.

        :param message: message to send.
        """
        queue_name = message.labels.get("queue_name") or self.queue_name
        async with Valkey(connection_pool=self.connection_pool) as valkey_conn:
            await valkey_conn.publish(queue_name, message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen valkey queue for new messages.

        This function listens to the pubsub channel
        and yields all messages with proper types.

        :yields: broker messages.
        """
        async with Valkey(connection_pool=self.connection_pool) as valkey_conn:
            valkey_pubsub_channel = valkey_conn.pubsub()
            await valkey_pubsub_channel.subscribe(self.queue_name)
            async for message in valkey_pubsub_channel.listen():
                if not message:
                    continue
                if message["type"] != "message":
                    logger.debug("Received non-message from valkey: %s", message)
                    continue
                yield message["data"]


class ValkeyStreamBroker(BaseValkeyBroker):
    """
    Valkey broker that uses streams for task distribution.

    You can read more about streams here:
    https://valkey.io/docs/latest/develop/data-types/streams

    This broker supports acknowledgment of messages.
    """

    def __init__(
        self,
        url: str,
        queue_name: str = "taskiq",
        max_connection_pool_size: Optional[int] = None,
        consumer_group_name: str = "taskiq",
        consumer_name: Optional[str] = None,
        consumer_id: str = "$",
        mkstream: bool = True,
        xread_block: int = 2000,
        maxlen: Optional[int] = None,
        idle_timeout: int = 600000,  # 10 minutes
        unacknowledged_batch_size: int = 100,
        xread_count: Optional[int] = 100,
        additional_streams: Optional[Dict[str, str]] = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new broker that uses streams.

        :param url: url to valkey.
        :param queue_name: name for a key with stream in valkey.
        :param max_connection_pool_size: maximum number of connections in pool.
            Each worker opens its own connection. Therefore this value has to be
            at least number of workers + 1.
        :param consumer_group_name: name for a consumer group.
            Valkey will keep track of acked messages for this group.
        :param consumer_name: name for a consumer. By default it is a random uuid.
        :param consumer_id: id for a consumer. ID of a message to start reading from.
            $ means start from the latest message.
        :param mkstream: create stream if it does not exist.
        :param xread_block: block time in ms for xreadgroup.
            Better to set it to a bigger value, to avoid unnecessary calls.
        :param maxlen: sets the maximum length of the stream
            trims (the old values of) the stream each time a new element is added
        :param xread_count: number of messages to fetch from the stream at once.
        :param additional_streams: additional streams to read from.
            Each key is a stream name, value is a consumer id.
        :param redeliver_timeout: time in ms to wait before redelivering a message.
        :param unacknowledged_batch_size: number of unacknowledged messages to fetch.
        """
        super().__init__(
            url,
            task_id_generator=None,
            result_backend=None,
            queue_name=queue_name,
            max_connection_pool_size=max_connection_pool_size,
            **connection_kwargs,
        )
        self.consumer_group_name = consumer_group_name
        self.consumer_name = consumer_name or str(uuid.uuid4())
        self.consumer_id = consumer_id
        self.mkstream = mkstream
        self.block = xread_block
        self.maxlen = maxlen
        self.additional_streams = additional_streams or {}
        self.idle_timeout = idle_timeout
        self.unacknowledged_batch_size = unacknowledged_batch_size
        self.count = xread_count

    async def _declare_consumer_group(self) -> None:
        """
        Declare consumber group.

        Required for proper work of the broker.
        """
        streams = {self.queue_name, *self.additional_streams.keys()}
        async with Valkey(connection_pool=self.connection_pool) as valkey_conn:
            for stream_name in streams:
                try:
                    await valkey_conn.xgroup_create(
                        stream_name,
                        self.consumer_group_name,
                        id=self.consumer_id,
                        mkstream=self.mkstream,
                    )
                except ResponseError as err:
                    logger.debug(err)

    async def startup(self) -> None:
        """Declare consumer group on startup."""
        await super().startup()
        await self._declare_consumer_group()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Put a message in a list.

        This method appends a message to the list of all messages.

        :param message: message to append.
        """
        async with Valkey(connection_pool=self.connection_pool) as valkey_conn:
            await valkey_conn.xadd(
                self.queue_name,
                {b"data": message.message},
                maxlen=self.maxlen,
            )

    def _ack_generator(self, id: str) -> Callable[[], Awaitable[None]]:
        async def _ack() -> None:
            async with Valkey(connection_pool=self.connection_pool) as valkey_conn:
                await valkey_conn.xack(
                    self.queue_name,
                    self.consumer_group_name,
                    id,
                )

        return _ack

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Listen to incoming messages."""
        async with Valkey(connection_pool=self.connection_pool) as valkey_conn:
            while True:
                logger.debug("Starting fetching new messages")
                fetched = await valkey_conn.xreadgroup(
                    self.consumer_group_name,
                    self.consumer_name,
                    {
                        self.queue_name: ">",
                        **self.additional_streams,  # type: ignore
                    },
                    block=self.block,
                    noack=False,
                    count=self.count,
                )
                for _, msg_list in fetched:
                    for msg_id, msg in msg_list:
                        logger.debug("Received message: %s", msg)
                        yield AckableMessage(
                            data=msg[b"data"],
                            ack=self._ack_generator(msg_id),
                        )
                logger.debug("Starting fetching unacknowledged messages")
                for stream in [self.queue_name, *self.additional_streams.keys()]:
                    lock = valkey_conn.lock(
                        f"autoclaim:{self.consumer_group_name}:{stream}",
                    )
                    if await lock.locked():
                        continue
                    async with lock:
                        pending = await valkey_conn.xautoclaim(
                            name=stream,
                            groupname=self.consumer_group_name,
                            consumername=self.consumer_name,
                            min_idle_time=self.idle_timeout,
                            count=self.unacknowledged_batch_size,
                        )
                        logger.debug(
                            "Found %d pending messages in stream %s",
                            len(pending),
                            stream,
                        )
                        for msg_id, msg in pending[1]:
                            logger.debug("Received message: %s", msg)
                            yield AckableMessage(
                                data=msg[b"data"],
                                ack=self._ack_generator(msg_id),
                            )
