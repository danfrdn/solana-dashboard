from confluent_kafka import Producer
import json
from loguru import logger # Using loguru for better logging here too

class KafkaProducerService:
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initializes the Kafka Producer service.

        Args:
            bootstrap_servers: Kafka broker host and port (e.g., 'localhost:9092').
            topic: The Kafka topic to produce messages to.
        """
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'solana-websocket-producer'
        })
        logger.info(f"Kafka Producer initialized for topic: {self.topic} on servers: {bootstrap_servers}")

    def produce_message(self, message: dict):
        """
        Produces a message to the configured Kafka topic.
        The message should be a dictionary that will be serialized to JSON.
        """
        try:
            value_bytes = json.dumps(message).encode('utf-8')
            self.producer.produce(self.topic, value=value_bytes, callback=self.delivery_report)
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to produce message to Kafka: {e}")

    def delivery_report(self, err, msg):
        """
        Callback function for Kafka message delivery.
        This is called once for each message produced to indicate success or failure.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def flush(self):
        """
        Flushes any outstanding messages to the Kafka broker.
        This blocks until all messages are delivered or the timeout is reached.
        Crucial to call before application shutdown to prevent data loss.
        """
        logger.info("Flushing remaining Kafka messages...")
        pending_messages = self.producer.flush(timeout=10)
        if pending_messages > 0:
            logger.warning(f"WARNING: {pending_messages} messages still pending after flush timeout.")
        else:
            logger.info("All Kafka messages flushed successfully.")