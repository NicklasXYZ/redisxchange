#------------------------------------------------------------------------------#
#               Import packages from the python standard library               #
#------------------------------------------------------------------------------#
from threading import Thread
import logging
import random
#------------------------------------------------------------------------------#
#                          Import local libraries/code                         #
#------------------------------------------------------------------------------#
#                                                                              #
#------------------------------------------------------------------------------#
#                      Import third-party libraries: Others                    #
#------------------------------------------------------------------------------#
from redisxchange.xchanges import (
    RedisQueueMessageExchange,
)


#------------------------------------------------------------------------------#
#                         GLOBAL SETTINGS AND VARIABLES                        #
#------------------------------------------------------------------------------#
logging.basicConfig(level = logging.DEBUG)


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
class Consumer(RedisQueueMessageExchange):
    def __init__(self, name, **kwargs) -> None:
        # IMPORTANT: Call parent methods!
        super().__init__(**kwargs)
        # Set the name of the queue from which we want to process messages
        self.name = name

    def pre_handle(self) -> None:
        logging.debug("")
        logging.debug("This is a message before we start processing messages!")

    def post_handle(self) -> None:
        logging.debug("")
        logging.debug("This is a message after we have processed messages!")

    def check_input_data(self, data: dict) -> bool:
        """ 
        Description:
           Make sure the input data follows the correct format. If implemented
            this method is automatically called before the data is passed on to
            the 'receive()' method.
        """
        v = True
        if not "type" in data: # Message type.
            logging.debug(
                "No 'type' field in given data!"
            )
            v = False
        if not "data" in data:  # Main data.
            logging.debug(
                "No 'data' field in given data!"
            )
            v = False
        return v

    def receive(self, data: dict) -> None:
        """
        Description:
            Default method for receiving and processing incoming messages.
            Whenever a new message is received, then do some pre-processing and 
            and repond.

        Args:
            None

        Returns:
            None
        """
        # Make message type lowercase
        data["type"] = data["type"].lower()
        # Respond to the received message
        self.respond(data)

    def respond(self, data: dict) -> None:
        """ Respond to the received message. List all message handlers here.
        """
        # Message handler example 1:
        if data["type"] == "shuffle-list":
            self.shuffle_list(data)
        # Message handler example 2:
        elif data["type"] == "sort-list":
            self.sort_list(data)
    
    def shuffle_list(self, data: dict) -> None:
        """
        Given a list as input, suffle and return the list.
        """
        return_data = data["data"].copy()
        random.shuffle(return_data)
        message = {
            "data": return_data,
            "message_uuid": data["message_uuid"],
        }
        # Set the response in the Redis Key-Value store such that it can be
        # retrieved by the producer that initially sent the message.
        # We use the 'message_uuid' as the key.
        self.consume_set(
            message, self.name,
        )

    def sort_list(self, data: dict) -> None:
        """
        Given a list as input, sort and return the list.
        """
        return_data = data["data"].copy()
        return_data.sort()
        message = {
            "data": return_data,
            "message_uuid": data["message_uuid"],
        }
        # Set the response in the Redis Key-Value store such that it can be
        # retrieved by the producer that initially sent the message.
        # We use the 'message_uuid' as the key.
        self.consume_set(
            message, self.name,
        )


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
class Producer:
    def __init__(self, name: str) -> None:
        # Set the name of the queue from which we want to process messages from 
        self.name = name
        # Create a connection to the Redis server
        self.exchange = RedisQueueMessageExchange()#host = "localhost:6377")

    def start(self) -> None:
        # Example 1. Let the consumer shuffle the given list:
        list_1 = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        logging.debug("")
        logging.debug("Example 1. Input list : " + str(list_1))
        response_1 = self.exchange.publish_get(
            message = {
                "type": "shuffle-list",
                "data": list_1,
            },
            name = self.name,
        )
        if not response_1 is None:
            logging.debug("Example 1. Output list: " + str(response_1["data"]))
        else:
            logging.debug("Example 1. Something went wrong. Nothing was returned...")

        # Example 2. Let the consumer sort the given list:
        list_2 = [9, 8, 7, 6, 5, 4, 3, 2, 1]
        logging.debug("")
        logging.debug("Example 2. Input list : " + str(list_2))
        response_2 = self.exchange.publish_get(
            message = {
                "type": "sort-list",
                "data": list_2,
            },
            name = self.name,
        )
        if not response_2 is None:
            logging.debug("Example 2. Output list: " + str(response_2["data"]))
        else:
            logging.debug("Example 2. Something went wrong. Nothing was returned...")


if __name__ == "__main__":       
    # Create and start consumer
    consumer = Consumer("queue-one")

    # Create and start producer
    producer = Producer("queue-one")

    # Start the consumer and producer in each of their own threads.
    thr1 = Thread(target = consumer.handle)
    thr1.start()
    thr2 = Thread(target = producer.start)
    thr2.start()