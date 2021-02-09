#!/usr/bin/env python

import os
import logging
import json
import yaml
import pika
from pymongo import MongoClient


class MongoDBConnector():
    def __init__(self):
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        # MongoDB connection string
        self.mongodb_uri = os.getenv('MONGODB_URI', None)

        self.client = None

    def connect(self):
        self.client = MongoClient(self.mongodb_uri)
        self.eventsink = self.client.eventsink

    def insert(self, event_json):
        logger = logging.getLogger("__main__")
        try:
            self.eventsink.events.insert_one(event_json)
            return True
        except Exception as e:
            try:
                self.connect()
                self.eventsink.events.insert_one(event_json)
                return True
            except Exception as e2:
                logger.debug("MongoDB Error: "+str(e2))
                return False

class ActivityMonitorDaemon():
    def __init__(self):
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        self.rabbitmq_uri = os.getenv('RABBITMQ_URI', None)
        self.rabbitmq_exchange = os.getenv('RABBITMQ_EXCHANGE', None)
        self.rabbitmq_key = os.getenv('RABBITMQ_KEY', None)
        self.rabbitmq_queue = os.getenv('RABBITMQ_QUEUE', None)

        self.channel = None
        self.connection = None
        self.consumer_tag = None
        self.worker = None
        self.announcer = None

        self.mongoclient = MongoDBConnector()

    # Connect to message broker
    def connect(self):
        """connect to rabbitmq using URL parameters"""

        parameters = pika.URLParameters(self.rabbitmq_uri)
        self.connection = pika.BlockingConnection(parameters)

        # connect to channel
        self.channel = self.connection.channel()

        # setting prefetch count to 1 so we only take 1 message of the bus at a time,
        # so other extractors of the same type can take the next message.
        self.channel.basic_qos(prefetch_count=1)

        # declare the queue in case it does not exist
        self.channel.queue_declare(queue=self.rabbitmq_queue, durable=True)

        # register with an exchange
        if self.rabbitmq_exchange:
            # declare the exchange in case it does not exist
            self.channel.exchange_declare(exchange=self.rabbitmq_exchange, exchange_type='topic',
                                          durable=True)

            # connect queue and exchange
            if self.rabbitmq_key:
                if isinstance(self.rabbitmq_key, str):
                    self.channel.queue_bind(queue=self.rabbitmq_queue,
                                            exchange=self.rabbitmq_exchange,
                                            routing_key=self.rabbitmq_key)
                else:
                    for key in self.rabbitmq_key:
                        self.channel.queue_bind(queue=self.rabbitmq_queue,
                                                exchange=self.rabbitmq_exchange,
                                                routing_key=key)

    # Listen for next available message
    def listen(self):
        """Listen for messages coming from RabbitMQ"""

        # check for connection
        if not self.channel:
            self.connect()

        # create listener
        self.consumer_tag = self.channel.basic_consume(queue=self.rabbitmq_queue,
                                                       on_message_callback=self.on_message,
                                                       auto_ack=False)

        # start listening
        logging.getLogger(__name__).info("Starting to listen for messages.")
        try:
            # pylint: disable=protected-access
            while self.channel and self.channel.is_open and self.channel._consumer_infos:
                self.channel.connection.process_data_events(time_limit=1)  # 1 second
                if self.worker:
                    self.worker.process_messages(self.channel, self.rabbitmq_queue)
                    if self.worker.is_finished():
                        self.worker = None
        except SystemExit:
            raise
        except KeyboardInterrupt:
            raise
        except GeneratorExit:
            raise
        except Exception:  # pylint: disable=broad-except
            logging.getLogger(__name__).exception("Error while consuming messages.")
        finally:
            logging.getLogger(__name__).info("Stopped listening for messages.")
            if self.channel and self.channel.is_open:
                try:
                    self.channel.close()
                except Exception:
                    logging.getLogger(__name__).exception("Error while closing channel.")
            self.channel = None
            if self.connection and self.connection.is_open:
                try:
                    self.connection.close()
                except Exception:
                    logging.getLogger(__name__).exception("Error while closing connection.")
            if self.announcer:
                self.announcer.stop_thread()

            self.connection = None

    def on_message(self, channel, method, header, body):
        """When the message is received this will call the generic _process_message in
        the connector class. Any message will only be acked if the message is processed,
        or there is an exception (except for SystemExit and SystemError exceptions).
        """

        try:
            json_body = yaml.safe_load(body)
            if 'routing_key' not in json_body and method.routing_key:
                json_body['routing_key'] = method.routing_key

            result = self.process(json_body)
            if result:
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                channel.basic_nack(delivery_tag=method.delivery_tag)


        except ValueError:
            # something went wrong, move message to error queue and give up on this message immediately
            logging.exception("Error processing message")

    # Process a specific message
    def process(self, message):
        return self.mongoclient.insert(message)

    def start(self):
        logger = logging.getLogger("__main__")
        logger.debug("Connecting to RabbitMQ...")
        self.connect()
        logger.debug("Connecting to MongoDB...")
        self.mongoclient.connect()

        self.listen()


logging.basicConfig()
if __name__ == "__main__":
    service = ActivityMonitorDaemon()
    service.start()
