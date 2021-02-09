#------------------------------------------------------------------------------#
#               Import packages from the python standard library               #
#------------------------------------------------------------------------------#
from datetime import datetime
import asyncio
import time
#------------------------------------------------------------------------------#
#                          Import local libraries/code                         #
#------------------------------------------------------------------------------#
from redisxchange.RedisMessageExchange import (
    RedisQueueMessageExchange,
    FutureSet,
    load,
    dump,
)
#------------------------------------------------------------------------------#
#                      Import third-party libraries: Others                    #
#------------------------------------------------------------------------------#
from asgiref.sync import sync_to_async # pip install asgiref
from asgiref.sync import async_to_sync # pip install asgiref


# TODO: Add additional tests:
#       - RedisQueueMessageExchange
#           - publish_get() together with consume_set()
#       - RedisPubSubMessageExchange
#------------------------------------------------------------------------------#
def main():
    print("\n----> Running RedisQueueMessageExchange test 1: ")
    test_redis_queue_exchange_1()
    print("\n----> Running FutureSet test 1                : ")
    test_futureset_1()

#------------------------------------------------------------------------------#
def test_redis_queue_exchange_1():
    # Generate some test data which we are going to put in the queue
    messages_in = [{"int": i} for i in range(100)]
    name = "test012345"; namespace = "queue"; key = f"{namespace}:{name}"
    # Create a Redis queue client object
    client = RedisQueueMessageExchange(namespace = "queue")
    
    # Make sure the key is correct
    assert client._get_key(name = name) == key
    
    # Delete all data in the queue
    client.client.delete(key)

    # Make sure the queue is empty
    client.queue_size(name = name) == 0
    client.queue_empty(name = name) == True

    # Add data to the queue
    for i in range(len(messages_in)):
        # Use low-level methods
        if i % 2 == 0:
            # Convert dict --> bytes
            data = dump(messages_in[i])
            # Add the data to the queue
            client.queue_push(name = name, data = data)
        # Use convenience/wrapper method with disconnection handling
        else:
            client.publish(message = messages_in[i], name = name)
        # Make sure the queue is growing
        assert client.queue_size(name = name) == (i + 1)
        assert client.queue_empty(name = name) == False
    
    # Retrieve all the messages in the queue
    messages_out = client.queue_lrange(name = name, start = 0, end = -1)
    assert len(messages_out) == len(messages_in)
    
    # Make sure all the data added to the queue is retrieved
    for i in range(len(messages_out)):
        data = load(messages_out[i])
        assert data == messages_in[i]

    # Retrieve and remove each item in the queue.
    # Make sure the right item is retrieved and removed.    
    for i in range(client.queue_size(name = name)):
        message = client.queue_pop(name = name)
        if not message is None:
            data = load(message)
            assert messages_in[i] == data
        else:
            assert False # Message was None!

    print("\n----> RedisQueueMessageExchange 1 tests passed!\n")


#------------------------------------------------------------------------------#
async def print_input(i):
    rnd = 2.5
    await asyncio.sleep(rnd)
    now = await sync_to_async(datetime.utcnow)()
    print("Value:", i, "Time: ", now)

async def make_clients(nb_clients, limit = 0):
    futures = FutureSet(maxsize = limit)
    for client_id in range(nb_clients):
        await futures.add(print_input(client_id))
    await futures.wait()

def test_futureset_1():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(make_clients(15, limit = 5))

    print("\n----> FutureSet 1 tests passed!\n")


#------------------------------------------------------------------------------#
if __name__ == "__main__":
    main()