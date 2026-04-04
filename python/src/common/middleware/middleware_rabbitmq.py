import pika
import random
import string
import logging
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

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
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
        self.channel.start_consuming()
    
    
    def callback(self, ch, method, properties, body):
        logging.info(f"Processing message: {body}")
        print(f"Processing message: {body}")
    
    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        # Caso feliz (no considero errores por ahora)
        print(f"Sending message: {message}")
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
