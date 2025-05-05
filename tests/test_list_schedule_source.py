import datetime
import uuid

from freezegun import freeze_time
from taskiq import ScheduledTask

from taskiq_valkey.list_schedule_source import ListValkeyScheduleSource


@freeze_time("2025-01-01 00:00:00")
async def test_schedule_cron(valkey_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListValkeyScheduleSource(valkey_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]


@freeze_time("2025-01-01 00:00:00")
async def test_schedule_from_past(valkey_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListValkeyScheduleSource(valkey_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(minutes=4),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    for schedule in schedules:
        await source.post_send(schedule)
    # After getting the schedules for the second time,
    # all the schedules in the past are ignored.
    schedules = await source.get_schedules()
    assert schedules == []


@freeze_time("2025-01-01 00:00:00")
async def test_schedule_removal(valkey_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListValkeyScheduleSource(valkey_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(minutes=4),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    scehdules = await source.get_schedules()
    assert scehdules == []
    # Assert that we will get the schedule after the time has passed.
    with freeze_time("2025-01-01 00:04:00"):
        scehdules = await source.get_schedules()
        assert scehdules == [schedule]


@freeze_time("2025-01-01 00:00:00")
async def test_deletion(valkey_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListValkeyScheduleSource(valkey_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]
    await source.delete_schedule(schedule.schedule_id)
    scehdules = await source.get_schedules()
    assert scehdules == []
