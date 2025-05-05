import sys
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, List, Optional, Tuple

from taskiq import ScheduleSource
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.serializers import PickleSerializer
from valkey.asyncio import (
    BlockingConnectionPool,
    Connection,
    Sentinel,
    Valkey,
    ValkeyCluster,
)

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    _Valkey: TypeAlias = Valkey[bytes]  # type: ignore
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool[Connection]  # type: ignore
else:
    _Valkey: TypeAlias = Valkey
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool  # noqa: PYI047


class ValkeyClusterScheduleSource(ScheduleSource):
    """
    Source of schedules for valkey cluster.

    This class allows you to store schedules in valkey.
    Also it supports dynamic schedules.

    :param url: url to valkey cluster.
    :param prefix: prefix for valkey schedule keys.
    :param buffer_size: buffer size for valkey scan.
        This is how many keys will be fetched at once.
    :param max_connection_pool_size: maximum number of connections in pool.
    :param serializer: serializer for data.
    :param connection_kwargs: additional arguments for ValkeyCluster.
    """

    def __init__(
        self,
        url: str,
        prefix: str = "schedule",
        serializer: Optional[TaskiqSerializer] = None,
        **connection_kwargs: Any,
    ) -> None:
        self.prefix = prefix
        self.valkey: "ValkeyCluster" = ValkeyCluster.from_url(
            url,
            **connection_kwargs,
        )
        if serializer is None:
            serializer = PickleSerializer()
        self.serializer = serializer

    async def delete_schedule(self, schedule_id: str) -> None:
        """Remove schedule by id."""
        await self.valkey.delete(f"{self.prefix}:{schedule_id}")

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """
        Add schedule to valkey.

        :param schedule: schedule to add.
        :param schedule_id: schedule id.
        """
        await self.valkey.set(
            f"{self.prefix}:{schedule.schedule_id}",
            self.serializer.dumpb(model_dump(schedule)),
        )

    async def get_schedules(self) -> List[ScheduledTask]:
        """
        Get all schedules from valkey.

        This method is used by scheduler to get all schedules.

        :return: list of schedules.
        """
        schedules = []
        async for key in self.valkey.scan_iter(f"{self.prefix}:*"):
            raw_schedule = await self.valkey.get(key)
            parsed_schedule = model_validate(
                ScheduledTask,
                self.serializer.loadb(raw_schedule),
            )
            schedules.append(parsed_schedule)
        return schedules

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)

    async def shutdown(self) -> None:
        """Shut down the schedule source."""
        await self.valkey.aclose()


class ValkeySentinelScheduleSource(ScheduleSource):
    """
    Source of schedules for valkey cluster.

    This class allows you to store schedules in valkey.
    Also it supports dynamic schedules.

    :param sentinels: list of sentinel host and ports pairs.
    :param master_name: sentinel master name.
    :param prefix: prefix for valkey schedule keys.
    :param buffer_size: buffer size for valkey scan.
        This is how many keys will be fetched at once.
    :param max_connection_pool_size: maximum number of connections in pool.
    :param serializer: serializer for data.
    :param connection_kwargs: additional arguments for ValkeyCluster.
    """

    def __init__(
        self,
        sentinels: List[Tuple[str, int]],
        master_name: str,
        prefix: str = "schedule",
        buffer_size: int = 50,
        serializer: Optional[TaskiqSerializer] = None,
        min_other_sentinels: int = 0,
        sentinel_kwargs: Optional[Any] = None,
        **connection_kwargs: Any,
    ) -> None:
        self.prefix = prefix
        self.sentinel = Sentinel(
            sentinels=sentinels,
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connection_kwargs,
        )
        self.master_name = master_name
        self.buffer_size = buffer_size
        if serializer is None:
            serializer = PickleSerializer()
        self.serializer = serializer

    @asynccontextmanager
    async def _acquire_master_conn(self) -> AsyncIterator[_Valkey]:
        async with self.sentinel.master_for(self.master_name) as valkey_conn:
            yield valkey_conn

    async def delete_schedule(self, schedule_id: str) -> None:
        """Remove schedule by id."""
        async with self._acquire_master_conn() as valkey:
            await valkey.delete(f"{self.prefix}:{schedule_id}")

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """
        Add schedule to valkey.

        :param schedule: schedule to add.
        :param schedule_id: schedule id.
        """
        async with self._acquire_master_conn() as valkey:
            await valkey.set(
                f"{self.prefix}:{schedule.schedule_id}",
                self.serializer.dumpb(model_dump(schedule)),
            )

    async def get_schedules(self) -> List[ScheduledTask]:
        """
        Get all schedules from valkey.

        This method is used by scheduler to get all schedules.

        :return: list of schedules.
        """
        schedules = []
        async with self._acquire_master_conn() as valkey:
            buffer = []
            async for key in valkey.scan_iter(f"{self.prefix}:*"):
                buffer.append(key)
                if len(buffer) >= self.buffer_size:
                    schedules.extend(await valkey.mget(buffer))
                    buffer = []
            if buffer:
                schedules.extend(await valkey.mget(buffer))
        return [
            model_validate(ScheduledTask, self.serializer.loadb(schedule))
            for schedule in schedules
            if schedule
        ]

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)

    async def shutdown(self) -> None:
        """Shut down the schedule source."""
        for sentinel in self.sentinel.sentinels:
            await sentinel.aclose()
