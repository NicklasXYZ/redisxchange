 #------------------------------------------------------------------------------#
#                     Author     : Nicklas Sindlev Andersen                    #
#                     Website    : Nicklas.xyz                                 #
#                     Github     : github.com/NicklasXYZ                       #
#------------------------------------------------------------------------------#
#                                                                              #
#------------------------------------------------------------------------------#
#               Import packages from the python standard library               #
#------------------------------------------------------------------------------#
from datetime import timedelta
from datetime import datetime
import asyncio
import logging
import time
import uuid 
import abc
from typing import (
    Union,
    Dict,
    Any,
)
#------------------------------------------------------------------------------#
#                          Import local libraries/code                         #
#------------------------------------------------------------------------------#
#                                                                              #
#------------------------------------------------------------------------------#
#                      Import third-party libraries: Others                    #
#------------------------------------------------------------------------------#
from asgiref.sync import sync_to_async # pip install asgiref
from asgiref.sync import async_to_sync # pip install asgiref
import msgpack # pip install msgpack
import redis   # pip install redis-py


#------------------------------------------------------------------------------#
#                         GLOBAL SETTINGS AND VARIABLES                        #
#------------------------------------------------------------------------------#
logging.basicConfig(level = logging.DEBUG)


#------------------------------------------------------------------------------#
#                                HELPER METHODS                                #
#------------------------------------------------------------------------------#
def load(message: Union[None, bytes]) -> Union[None, dict]:
    """ Convert the given input from bytes to dict: bytes --> dict.

    Args:
        message (Union[None, bytes]): A message that should converted to a 
            python dict.

    Raises:
        TypeError: If the input is not of type 'bytes' or 'None'.

    Returns:
        Union[None, dict]: The input message as a dict or None.
    """
    if not message is None:
        if isinstance(message, bytes):
            # Convert: bytes --> dict
            data = msgpack.loads(message)
        else:
            error = "The given value has the wrong type! " + \
                "The given input needs to be of type 'bytes'."
            raise TypeError(error)
    else:
        data = None
    return data

def dump(message: Union[None, dict]) -> bytes:
    """ Convert given input from dict to bytes: dict --> bytes.

    Args:
        message (Union[None, bytes]): A message that should be converted to bytes.

    Raises:
        TypeError: If the input is not of type 'bytes' or 'None'.

    Returns:
        bytes: The input message as a bytes.
    """
    if isinstance(message, dict) or message is None:
        # Convert: dict --> bytes
        data = msgpack.dumps(message)
    else:
        error = "The given value has the wrong type! " + \
            "The given input needs to be of type 'dict'."
        raise TypeError(error)
    return data


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
class RedisMessageExchange:
    def __init__(
            self, host = "localhost", port = "6379", db = 1,
            timeout = 5, max_try = 5, wait_time = 0.001, ttl = 5,
        ) -> None:
        """Initialize and set given class variables on class instantiation. 

        Args:
            host (str, optional): The service name or address of where the
                Redis KV store is running. Defaults to "localhost".
            port (str, optional): The port number of the address where the 
                Redis KV store is running. Defaults to "6379".
            db (int, optional): The index of the Redis database that should
                be used. Defaults to 1.
            timeout (int, optional): An upper bound on the amount of time we
                are prepared to wait on the retrieval of a value before we
                return with an error. Defaults to 5.
            max_try (int, optional): An upper bound on the number of 
                connection retries. Defaults to 5.
            wait_time (float, optional): The time between trying to SET or 
                GET values in the Redis KV store. Defaults to 0.001.
            ttl (int, optional): The amount of time after which a KV pair is
                deleted from the Redis KV store. Defaults to 10.
        """
        # Redis KV store connection info
        self.host = host
        self.port = port
        self.db = db
        # Redis connection status
        self.server_down = True
        # The upper bound on number of re-tries
        self.max_try = max_try
        # The waiting time between re-tries, when processing data
        self.wait_time = wait_time
        # The expiry time of key-value pairs
        self.ttl = ttl
        # The timeout value used when trying to retrieve a value from Redis
        self.timeout = timeout
        # The backoff counter used to increase the waiting time 
        # between re-triees, when trying to re-connect to Redis
        self.backoff = 0
        # The name of the queue or named channel from which we should process data
        self.name = None
        # The client connection
        self.client = None
        # Check that the right args were given
        self.check_base_args()
        # Finally, connect to the Redis server
        self.connect()
    
    def check_base_args(self):
        """ Check and validate all class arguments on calss instantiation.

        Raises:
            TypeError: Given input is NOT of type 'float'.
            TypeError: Given input is NOT of type 'int'.
            TypeError: Given input is NOT of type 'float'.
            TypeError: Given input is NOT of type 'int'.
        """
        if not isinstance(self.timeout, float):
            error = f"ARG 'timeout' is of type {type(self.timeout)} " + \
                "but should be of type 'float'!"
            try:
                self.timeout = float(self.timeout)
            except:
                raise TypeError(error)
        if not isinstance(self.max_try, int):
            error = f"ARG 'max_try' is of type {type(self.max_try)} " + \
                "but should be of type 'int'!"
            try:
                self.max_try = int(self.max_try)
            except:
                raise TypeError(error)
        if not isinstance(self.wait_time, float):
            error = f"ARG 'wait_time' is of type {type(self.wait_time)} " + \
                "but should be of type 'float'!"
            try:
                self.wait_time = float(self.wait_time)
            except:
                raise TypeError(error)
        if not isinstance(self.ttl, int):
            error = f"ARG 'ttl' is of type {type(self.ttl)} " + \
                "but should be of type 'int'!"
            try:
                self.ttl = int(self.ttl)
            except:
                raise TypeError(error)

    def connect(self) -> bool:
        """ Connect to the Redis KV store and handle connection errors.

        Returns:
            bool: A boolean value that signals whether it was possible
                to connect to the Redis KV store.
        """
        connection_string = str(self.host) + ":" + str(self.port) + \
            "/" + str(self.db) 
        # Assume we are not able to connect.
        # This flag is set to 'True' if we are able to connect successfully.
        return_value = False
        while self.server_down:
            try:
                self._connect()
                self.server_down = False
                self.backoff = 0 # Reset the backoff value
                logging.debug(
                    f"Client connection to {connection_string} is up!"
                )
                return_value = True
                break
            except:
                self.server_down = True
                # After each connection retry increase the backoff value
                self.backoff += 1
                # Increase the waiting time before we try to reconnect to the
                # Redis KV store.
                sleep_time = 3 * self.backoff
                time.sleep(sleep_time)
                logging.debug(
                    f"Cannot connect to {connection_string} trying again " + \
                    f"in {sleep_time} seconds..."
                )
            if self.backoff == self.max_try:
                logging.debug(
                    f"No connection could be established to " + \
                    f"{connection_string} after {self.backoff} re-tries!"
                )
                break
        return return_value

    def reset_connection(self) -> bool:
        """ Try reconecting to the Redis KV store.

        Raises:
            redis.exceptions.ConnectionError: The reconnection attempt did 
                not work so raise a Redis 'ConnectionError'.

        Returns:
            bool: In case the reconnection attempt succeeded then return 'True'. 
        """
        # After 3 seconds try to re-connect...
        time.sleep(3)
        self.server_down = True
        is_connected = self.connect()
        if not is_connected:
            connection_string = str(self.host) + ":" + str(self.port) + \
                "/" + str(self.db) 
            logging.debug(
                f"Server is down. No connection could be established to " + \
                f"{connection_string}!"
            )
            raise redis.exceptions.ConnectionError
        else:
            return True

    def kv_set(self, message: dict, namespace: str) -> None:
        """ SET a value in the Redis KV store by supplying a 'namespace' and
        the 'message' which should be SET in the Redis KV store. The 'message'
        should contain a unique identifier which can be used by a PRODUCER to 
        eventually retrieve the response in the Redis KV store.

        Args:
            message (dict): The data that should be SET in the Redis KV store
                and that also contains a unique identifier.
            namespace (str): A namespace used for building a unique identifier
                which is used as a key to access a value in the Redis KV store. 
        """
        start_time = datetime.utcnow() + timedelta(seconds = self.timeout)
        while True:
            if datetime.utcnow() - start_time > timedelta(seconds = self.timeout):
                logging.debug(
                    f"Waited {self.timeout} seconds. No message was returned!"
                )
                break
            try:
                data = dump(message)
                return_code = self.client.set(
                    namespace + ":" + message["message_uuid"],
                    data,
                    ex = int(self.ttl),
                )
                if return_code:
                    break
            except redis.exceptions.ConnectionError:
                # Try to fix the connection
                self.reset_connection()
            time.sleep(self.wait_time)

    def kv_get(self, namespace: str, message: dict) -> Union[None, dict]:
        """ GET a value in the Redis KV store by supplying a 'namespace' and
        the 'message' which was initially sent (it should contain a unique 
        identifier which can be used to retrieve the response in the Redis
        KV store).

        Args:
            namespace (str): A namespace used for building a unique identifier
                which is used as a key to access a value in the Redis KV store. 
            message (dict): The data that we should GET in the Redis KV store
                and that also contains a unique identifier.

        Returns:
            dict: The data retrieved from the Redis KV store.
        """
        start_time = datetime.utcnow() + timedelta(seconds = self.timeout)
        response_data = None
        # Get response data
        while response_data is None:                
            if datetime.utcnow() - start_time > timedelta(seconds = self.timeout):
                logging.debug(
                    f"Waited {self.timeout} seconds. No message was returned!"
                )
                response_data = None
                break
            try:
                response = self.client.get(
                    namespace + ":" + message["message_uuid"],
                )
                response_data = load(response)
            except redis.exceptions.ConnectionError:
                # Try to fix the connection
                self.reset_connection()
            time.sleep(self.wait_time)
        return response_data
    
    def _connect(self) -> None:
        """ An internal method that is used to instantiate a Redis KV store
        client object and check if the connection to Redis is working. This
        is done by pinging the address and port where the Redis KV store is
        running.  
        """
        self.client = redis.Redis(
            host = self.host,
            port = self.port,
            db = self.db,
        )
        logging.debug(
            "Pinging the Redis server..."
        )
        while True:
            return_value = self.client.ping()
            if return_value == True:
                logging.debug(
                    f"The Redis server responded with {return_value}!"
                )
                break
            else:
                logging.debug(
                    "The Redis server did not respond..."
                )
            time.sleep(self.wait_time)
    
    def check_input_data(self, data: dict) -> bool:
        """ Check/validate data before it is passed on for further processing. 

        Args:
            data (dict): Input data to be handled by a consumer.

        Returns:
            bool: Returns 'True' if all checks have passed, otherwise 'False'
                is returned.
        """
        # This method should be implemented in a subclass
        return True

    def check_output_data(self, data: dict) -> bool:
        """ Check/validate data before it is passed on for further processing. 

        Args:
            data (dict): Output data to be returned to a producer.

        Returns:
            bool: Returns 'True' if all checks have passed, otherwise 'False'
                is returned.
        """
        # This method should be implemented in a subclass
        return True

    def _check_input_data(self, data: dict) -> bool:
        """ Run a number of internal and external user-defined checks on the
        given 'data' to determine whether it complies with the required format.
        Make sure that required fields are contained in the  given 'data' passed
        to this function and other subsequent functions down the line. 

        Args:
            data (dict): Input data to be handled by a consumer.

        Raises:
            TypeError: If the external user-defined checks do not return a 
                boolean value. The user-defined checks implemented in the 
                'check_input_data()' function should return a boolean value.

        Returns:
            bool: Returns 'True' if all checks have passed, otherwise 'False'
                is returned.
        """
        # Perform a number of internal checks
        v1 = True
        if not "message_uuid" in data:
            logging.debug(
                "No 'message_uuid' field in given data!"
            )
            v1 = False
        if not "message_timestamp" in data:
            logging.debug(
                "No 'message_timestamp' field in given data!"
            )
            v1 = False
        # Also run the additional checks that have been implemented in a 
        # subclass.
        v2 = self.check_input_data(data)
        if not v2 is None:
            return v1 and v2
        else:
            error = "The 'check_input_data()' function returned None, " + \
                " but a boolean value 'True/False' was expected."
            raise TypeError(error)

    def _check_output_data(self, data: dict) -> bool:
        """ Run a number of internal and external user-defined checks on the
        given 'data' to determine whether it complies with the required format.
        Make sure that required fields are contained in the given 'data' passed
        to this function and other subsequent functions down the line. 

        Args:
            data (dict): Output data to be returned to a producer.

        Raises:
            TypeError: If the external user-defined checks do not return a 
                boolean value. The user-defined checks implemented in the 
                'check_output_data()' function should return a boolean value.

        Returns:
            bool: Returns 'True' if all checks have passed, otherwise 'False'
                is returned.
        """
        # Perform a number of internal checks
        v1 = True
        if not "message_uuid" in data:
            logging.debug(
                "No 'message_uuid' field in given data!"
            )
            v1 = False
        if not "message_timestamp" in data:
            logging.debug(
                "No 'message_timestamp' field in given data!"
            )
            v1 = False
        # Also run the additional checks that have been implemented in a subclass.
        v2 = self.check_output_data(data)
        if not v2 is None:
            return v1 and v2
        else:
            error = "The 'check_output_data()' function returned None, " + \
                " but a boolean value 'True/False' was expected."
            raise TypeError(error)

    @abc.abstractmethod
    async def receive(self):
        # This method should be implemented in a subclass
        pass

    @abc.abstractmethod
    async def handle(self):
        # This method should be implemented in a subclass
        pass


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
class RedisPubSubMessageExchange(RedisMessageExchange):
    def __init__(self, *args, **kwargs) -> None:
        """Initialize and set given class variables on class instantiation. 

        Returns:
            None: None.
        """
        self.pubsub = None
        self.namespaces = [] # (topics/named channels)
        super().__init__(*args, **kwargs)
    
    def _connect(self) -> None:
        """ Connect the Redis client to the Redis KV store and create
        a Redis pub-sub message broker.

        Returns:
            None: None.
        """
        super()._connect()
        self.pubsub = self.client.pubsub()

    def subscribe(self, namespace: str) -> None:
        """ Subscribe to namespaces (topics/named channels). 

        Args:
            namespace (str): The namespace (usually called topic/named channel).
                to subscribe to.

        Returns:
            None: None.
        """
        if not self.pubsub is None:
            if not namespace in self.namespaces:
                self.pubsub.subscribe(namespace)
                self.namespaces.append(namespace)
            else:
                logging.warning("Already subscribed to this topic/named channel!")
        else:
            logging.warning("A pubsub object has not been initialized!")

    def reset_connection(self) -> None:
        """ Try to reconnect and re-subscribe to namespaces (topics/named channels).

        Returns:
            None: None.
        """
        is_connected = super().connect()
        if is_connected:
            namespaces_copy = self.namespaces.copy()
            self.namespaces = []
            for v in namespaces_copy:
                self.subscribe(v)

    def publish(self, message: dict, namespace: str) -> None:
        """ Convert the input message (a dict) to bytes and publish it to
        a certain namespace (usually called topic/named channel).

        Args:
            message (dict): The message to publish to a certain namespace
                (usually called topic/named channel).
            namespace (str): The namespace (usually called topic/named channel) to
                publish a message on.

        Returns:
            None: None.
        """
        if not self.client is None:
            while True:
                try:
                    # Dump the data: dict --> bytes
                    data = dump(message)
                    return_code = self.client.publish(
                        namespace,
                        data,
                    )
                    if return_code > 0:
                        break
                except redis.exceptions.ConnectionError:
                    # Try to fix the connection
                    self.reset_connection()
        else:
            logging.warning(
                "No connection to the client has been established yet! " + \
                "Try calling the RedisPubSubMessageExchange.connect() method."
            )
    
    def consume(self) -> Dict[str, Any]:
        """ Pick up the next message in the stream...
        """
        message = None
        # Consume messages
        while message is None:                
            try:
                message = self.pubsub.get_message()
            except redis.exceptions.ConnectionError:
                # Try to fix the connection
                self.reset_connection()
            time.sleep(self.wait_time)
        return message

    @abc.abstractmethod
    async def receive(self, data):
        # This method should be implemented in a subclass
        pass

    @abc.abstractmethod
    async def pre_handle(self):
        # This method should be implemented in a subclass
        pass

    @abc.abstractmethod
    async def post_handle(self):
        # This method should be implemented in a subclass
        pass

    async def handle(self) -> None:
        """ Keep handling incoming messages...

        Returns:
            None: None.
        """
        await self.pre_handle()
        while True:
            message_data = await sync_to_async(self.consume)()
            if not message_data is None:
                try:
                    # Load the data: bytes --> dict
                    data = await sync_to_async(load)(message_data["data"])
                    # Process the data
                    await self.receive(data)
                except AttributeError as e:
                    print("Exception: ", e)
                await asyncio.sleep(self.wait_time)
            else:
                logging.warning(
                    "The data recived from the message broker had value None!"
                )
        await self.post_handle()


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
class RedisQueueMessageExchange(RedisMessageExchange):
    def __init__(self, namespace: str = "queue", *args, **kwargs) -> None:
        """Initialize and set given class variables on class instantiation. 

        Args:
            namespace (str, optional): The namespace used to form a unique key
                for the queue. The unique key is used to access the queue.
                Defaults to "queue".
        """
        # Set the queue namespace
        self.namespace = namespace
        super().__init__(*args, **kwargs)
    
    def _get_key(self, name: str) -> str:
        """ Construct the key to access a certain queue based on the given name 
        and the initially set namespace.  

        Args:
            name (str): The name of the queue.

        Returns:
            str: The key to access a queue.
        """
        key = f"{self.namespace}:{name}"
        return key

    def queue_size(self, name: str) -> int:
        """ Retrieve the size of a queue.

        Args:
            name (str): The name of the queue.

        Returns:
            int: The size of the queue.
        """
        key = self._get_key(name) 
        return self.client.llen(key)

    def queue_empty(self, name: str) -> bool:
        """Check whether a queue is empty or not.

        Args:
            name (str): The name of the queue.

        Returns:
            bool: True if the queue is empty, otherwise False.
        """
        return self.queue_size(name) == 0

    def queue_push(self, data: bytes, name: str) -> int:
        """ Add data to the end of the queue.

        Args:
            item (bytes): The data to add to the queue. 
            name (str): The name of the queue.

        Returns:
            int: The number of items currently in the queue.
        """
        key = self._get_key(name) 
        return self.client.rpush(key, data)

    def queue_lrange(self, name: str, start = 0, end = -1) -> list:
        """ Access and return a certain range of values from the queue. 

        Args:
            name (str): The name of the queue.
            start (int, optional): The start index of the range to
                access in the queue. Defaults to 0.
            end (int, optional): The end index of the range to access
                in the queue. Defaults to -1.

        Returns:
            list: Return a certain range of values from the queue.
        """
        key = self._get_key(name) 
        return self.client.lrange(key, start, end)

    def queue_pop(self, name: str, timeout: Union[None, int] = None):
        """ Retrieve and remove an item at the head of the queue. 

        Args:
            name (str): The name of the queue.
            timeout (Union[None, str], optional): The max amount of time to.
                Defaults to 5.

        Returns:
            dict: The item removed from the queue.
        """
        key = self._get_key(name)
        # Blocking pop
        item = self.client.blpop(key, timeout = timeout)
        if item:
            item = item[1]
        return item

    def publish(self, message: dict, name: str) -> None:
        """ Serialize the input message and put it in a queue. Handle any 
        connection problems.

        Args:
            message (dict): The message to add to the end of the queue.
            name (str): The name of the queue. 
        """
        if not self.client is None:
            while True:
                try:
                    # Dump the data: dict --> bytes
                    data = dump(message)
                    return_code = self.queue_push(data, name)
                    if return_code > 0:
                        break
                except redis.exceptions.ConnectionError:
                    # Try to fix the connection
                    self.reset_connection()
                time.sleep(self.wait_time)
        else:
            logging.warning(
                "No connection to the client has been established yet! " + \
                "Try calling the RedisQueueMessageExchange.connect() method."
            )

    def consume_set(self, message: dict, name: str) -> None:
        """ As a consumer, set the message data, then return.

        Args:
            message (dict): The message to set in the Redis KV store. The message
                is a result that should be returned to a producer. 
            name (str): The name of the queue. 
        """
        v = self.check_output_data(message)
        if v == True:
            # Publish the message
            self.kv_set(message, name)
        else:
            logging.warning(
                "The given data does not follow the correct format!"
            )
      
    def publish_get(self, message: dict, name: str) -> Union[None, dict]:
        """ As a producer, publish a message on a topic/named channel, then 
        retrieve the result and return.

        Args:
            message (dict): The message to add to a certain queue, then 
                wait for a result to be made available in the Redis KV store.  
            name (str): The name of the queue. 

        Returns:
            Union[None, dict]: Return data.
        """
        self.message_uuid = str(uuid.uuid4())
        self.message_timestamp = str(datetime.utcnow())
        message["message_uuid"] = self.message_uuid
        message["message_timestamp"] = self.message_timestamp
        # Publish the message
        self.publish(message, name)
        # Get and return the result
        return self.kv_get(name, message)

    @abc.abstractmethod
    async def receive(self, data):
        logging.debug(f"Received data: {data}")
        # This method should be inplemented in a subclass
        pass

    @abc.abstractmethod
    async def pre_handle(self):
        pass

    @abc.abstractmethod
    async def post_handle(self):
        pass

    async def handle(self) -> None:
        """ Keep handling incoming messages...
        """
        await self.pre_handle()
        if not self.name is None:
            while True:
                data = await sync_to_async(self.queue_pop)(self.name)
                if not data is None:
                    # Load the data: bytes --> dict
                    data = await sync_to_async(load)(data)
                    # Process the data
                    await self.receive(data)
                else:
                    logging.warning(
                        "The data taken from the queue had value None!"
                    )
        else:
            logging.warning(
                "No topic/named channel has been set! No data can thus be " + \
                "read from the queue."
            )
        await self.post_handle()


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
class FutureSet:
    def __init__(self, maxsize: int, wait_time: float = 0.001) -> None:
        """Initialize and set given class variables on class instantiation. 

        Args:
            maxsize (int): Number of asynchronous tasks to be running.
            wait_time (float, optional): The time between starting new 
                async tasks. Defaults to 0.001.
        """
        self._set = set()
        self._maxsize = maxsize
        self._wait_time = wait_time

    async def add(self, item) -> None:
        """ Start a new async task. If this is not possible, then block
        until we can start running the task.

        Args:
            item (coroutine, future): A task to be run.

        Raises:
            ValueError: If input is not a coroutine or future.

        Returns:
            None: None.
        """
        if not asyncio.iscoroutine(item) and \
            not asyncio.isfuture(item):
            error = "Expecting a coroutine or a Future"
            raise ValueError(error)
        if item in self._set:
            return None
        while True:
            if len(self._set) >= self._maxsize:
                await sync_to_async(time.sleep)(self._wait_time)
            else:
                break
        logging.debug(
            "Number of tasks currently running: : " + str(len(self._set))
        )
        item = asyncio.create_task(item)    
        self._set.add(item)
        item.add_done_callback(self._remove)

    def _remove(self, item) -> None:
        """ Remove an async task from the set of running tasks.

        Args:
            item (coroutine, future): A task completed to be removed.

        Raises:
            ValueError: If the task (coroutine or future) is still pending.
        """
        if not item.done():
            error = "Cannot remove a pending Future"
            raise ValueError(error)
        self._set.remove(item)

    async def wait(self):
        """ Wait for an async task to complete.

        Returns:
            future: A completed async task.
        """
        return await asyncio.wait(self._set)