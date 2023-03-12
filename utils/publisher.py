from pika import BlockingConnection, ConnectionParameters, BasicProperties, spec
from pika.exchange_type import ExchangeType


class Publisher:

    def __init__(self, name: str, exchange_type: ExchangeType = ExchangeType.fanout):
        self.name = name
        self.connection = BlockingConnection(ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.name, exchange_type=exchange_type)

    def publish(self, message: str):
        self.channel.basic_publish(
            exchange=self.name,
            routing_key=self.name,
            body=message,
            properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE)
        )
        print("Message published.")
