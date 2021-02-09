# RedisXchange

Just a few Python convenience/wrapper methods and classes for setting up FIFO message queues and/or pub-sub message brokers for clients to use for exchanging messages through [Redis](https://redis.io/). The convenience/wrapper methods and classes are built on top of [redis-py](https://github.com/andymccurdy/redis-py/) and [msgpack](https://github.com/msgpack/msgpack-python).

The library implements:

- Redis server reconnection logic.
- Convenience methods:
  - For sending/receiving messages using a publish/subscribe messaging pattern.
  - For sending messages using a FIFO (First In First Out) message queue and Redis' Key-Value store for receiving messages. 

## Usage

Install the library through pip:
```bash
pip install git+https://github.com/nicklasxyz/redisxchange
```

For a couple of simple examples and use-cases see the [example](https://github.com/NicklasXYZ/redisxchange/tree/main/examples) directory.