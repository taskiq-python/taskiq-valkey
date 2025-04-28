# TaskIQ-Valkey

Taskiq-valkey is a plugin for taskiq that adds a new broker and result backend based on valkey.

# Installation

To use this project you must have installed core taskiq library:
```bash
pip install taskiq
```
This project can be installed using pip:
```bash
pip install taskiq-valkey
```

# Usage

Let's see the example with the valkey broker and valkey async result:

```python
# broker.py
import asyncio

from taskiq_valkey import ValkeyAsyncResultBackend, ValkeyStreamBroker

result_backend = ValkeyAsyncResultBackend(
    valkey_url="valkey://localhost:6379",
)

# Or you can use PubSubBroker if you need broadcasting
# Or ListQueueBroker if you don't want acknowledges
broker = ValkeyStreamBroker(
    valkey_url="valkey://localhost:6379",
).with_result_backend(result_backend)


@broker.task
async def best_task_ever() -> None:
    """Solve all problems in the world."""
    await asyncio.sleep(5.5)
    print("All problems are solved!")


async def main():
    task = await best_task_ever.kiq()
    print(await task.wait_result())


if __name__ == "__main__":
    asyncio.run(main())
```

Launch the workers:
`taskiq worker broker:broker`
Then run the main code:
`python3 broker.py`


## Brokers

This package contains 6 broker implementations. We have two broker types: `PubSub` and `Stream`.

Each of type is implemented for each valkey architecture:
* Single node
* Cluster
* Sentinel

Here's a small breakdown of how they differ from eachother.


### PubSub

By default on old valkey versions PUBSUB was the way of making valkey into a queue.
But using PUBSUB means that all messages delivered to all subscribed consumers.

> [!WARNING]
> This broker doesn't support acknowledgements. If during message processing
> Worker was suddenly killed the message is going to be lost.

### Stream

Stream brokers use valkey [stream type](https://valkey.io/topics/streams-intro/) to store and fetch messages.

> [!TIP]
> This broker **supports** acknowledgements and therefore is fine to use in cases when data durability is
> required.

## ValkeyAsyncResultBackend configuration

ValkeyAsyncResultBackend parameters:
* `valkey_url` - url to valkey.
* `keep_results` - flag to not remove results from Valkey after reading.
* `result_ex_time` - expire time in seconds (by default - not specified)
* `result_px_time` - expire time in milliseconds (by default - not specified)
* Any other keyword arguments are passed to `valkey.asyncio.BlockingConnectionPool`.
  Notably, you can use `timeout` to set custom timeout in seconds for reconnects
  (or set it to `None` to try reconnects indefinitely).

> [!WARNING]
> **It is highly recommended to use expire time in ValkeyAsyncResultBackend**
> If you want to add expiration, either `result_ex_time` or `result_px_time` must be set.
> ```python
> # First variant
> valkey_async_result = ValkeyAsyncResultBackend(
>     valkey_url="valkey://localhost:6379",
>     result_ex_time=1000,
> )
>
> # Second variant
> valkey_async_result = ValkeyAsyncResultBackend(
>     valkey_url="valkey://localhost:6379",
>     result_px_time=1000000,
> )
> ```


## Schedule sources


You can use this package to add dynamic schedule sources. They are used to store
schedules for taskiq scheduler.

The advantage of using schedule sources from this package over default `LabelBased` source is that you can
dynamically add schedules in it.

For now we have only one type of schedules - `ListValkeyScheduleSource`.

### ListValkeyScheduleSource

This source holds values in lists.

* For cron tasks it uses key `{prefix}:cron`.
* For timed schedules it uses key `{prefix}:time:{time}` where `{time}` is actually time where schedules should run.

The main advantage of this approach is that we only fetch tasks we need to run at a given time and do not perform any excesive calls to valkey.
