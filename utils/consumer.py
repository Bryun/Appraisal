from pika import BlockingConnection, ConnectionParameters
from pika.exchange_type import ExchangeType
from scripts.queues import DLQ


class Consumer:

    def __init__(self, name: str, exchange: str = None, exchange_type: ExchangeType = ExchangeType.fanout):
        self.name = name
        self.connection = BlockingConnection(ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.name if exchange is None else exchange,
            exchange_type=exchange_type
        )

        result = self.channel.queue_declare(queue=self.name,
                                            durable=True,
                                            arguments={
                                                'x-dead-letter-exchange': f"{self.name}_{DLQ}",
                                                'x-dead-letter-routing-key': DLQ
                                            })
        queue = result.method.queue

        self.channel.queue_bind(
            exchange=self.name if exchange is None else exchange,
            queue=queue
        )
        self.channel.basic_consume(queue=queue, on_message_callback=self.callback)
        self.channel.start_consuming()

    def dispose(self):
        if self.channel.is_open:
            self.channel.stop_consuming()
            self.channel.close()

    def callback(self, ch, method, properties, body):
        print(" %r" % body)
        print('Message consumed.')
        self.channel.basic_ack(delivery_tag=method.delivery_tag)


"""
from flopsy import Connection, Consumer, Publisher

// Globals
AMQP_SERVER = 'localhost'
AMQP_PORT = 5672
AMQP_USER = 'guest'
AMQP_PASSWORD = 'guest'
AMQP_VHOST = '/'

// Create a consumer
consumer = Consumer(connection=Connection())
consumer.declare(queue='books', exchange='readernaut', routing_key='importer', auto_delete=False)

def message_callback(message):
     print 'Recieved: ' + message.body
     consumer.channel.basic_ack(message.delivery_tag)

consumer.register(message_callback)
consumer.wait()

// Create a publisher
publisher = Publisher(connection=Connection(), exchange='readernaut', routing_key='importer')
publisher.publish('Test message!')
publisher.close()
"""
