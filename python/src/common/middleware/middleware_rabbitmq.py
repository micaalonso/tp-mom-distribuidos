import pika
import random
import string
import logging
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        response = self.channel.queue_declare(queue=self.queue_name, durable=True)
        if response:
            logging.info(f"Queue declared successfully")
            print("Queue declared successfully")
    
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            try:
                logging.info(f"Processing message: {body}")
                print(f"Processing message: {body}")
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError(f"Internal error | error: {e}")
        
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ChannelError,
            pika.exceptions.ConnectionClosed
        ) as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost | error: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Internal error | error: {e}")
    
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost | error: {e}")

    def send(self, message):
        try:
            print(f"Sending message: {message}")
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ChannelError,
            pika.exceptions.ConnectionClosed
        ) as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost | error: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Internal error | error: {e}")

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Internal error | error: {e}")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # El flag exclusive es para que una vez que la conexión cierre, la queue se elimine
        queue_response = self.channel.queue_declare(queue='', durable=True, exclusive=True)
        self.queue_name = queue_response.method.queue

        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            logging.info(f"Processing message: {body}")
            print(f"Processing message: {body}")
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        for rounting_key in self.routing_keys:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=rounting_key,
                body=message
            )

    def close(self):
        self.connection.close()
