import os
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

class KafkaConsumerService:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.consumer = self._create_consumer()
        logger.info(f"Kafka Consumer initialized for topic: {self.topic}, group_id: {self.group_id} on servers: {self.bootstrap_servers}")

    def _create_consumer(self):
        """
        Creates and configures a Confluent Kafka Consumer.
        """
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest', 
            'enable.auto.commit': False,     
        }
        return Consumer(conf)

    def consume_messages(self, timeout: float = 1.0):
        """
        Polls Kafka for a single message.
        Args:
            timeout (float): Maximum time to wait for a message (in seconds).
        Returns:
            confluent_kafka.Message: The raw message object, or None if no message is received within the timeout.
                                     Returns None if there's an error or no message.
        """
        try:
            self.consumer.subscribe([self.topic])
            msg = self.consumer.poll(timeout)

            if msg is None:
                logger.debug("No message received within timeout.")
                return None
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    logger.debug(f"{self.group_id} reached end of partition {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    return None
                else:
                    logger.error(f"Kafka error while consuming: {msg.error()}")
                    return None
            else:
                return msg 
        except Exception as e:
            logger.error(f"An unexpected error occurred during message consumption: {e}")
            return None

    def commit_offsets(self, message):
        """
        Commits the offset for a given message.
        Call this after a message has been successfully processed.
        """
        if message:
            try:
                self.consumer.commit(message=message)
                logger.debug(f"Committed offset for topic {message.topic()} partition {message.partition()} at offset {message.offset()}")
            except KafkaException as e:
                logger.error(f"Failed to commit offset: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred during offset commit: {e}")

    def close(self):
        """
        Closes the consumer, unsubscribing from topics and committing final offsets.
        """
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka Consumer closed.")

if __name__ == "__main__":
    logger.info("Starting Kafka Consumer Service example (press Ctrl+C to stop)...")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_RAW_TRANSACTIONS_TOPIC = os.getenv("KAFKA_RAW_TRANSACTIONS_TOPIC", "solana_raw_transactions")
    KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "solana-liquidity-consumer-group") 

    consumer_service = KafkaConsumerService(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP_ID,
        topic=KAFKA_RAW_TRANSACTIONS_TOPIC
    )

    try:
        while True:
            msg = consumer_service.consume_messages() 
            if msg:
                msg_value = msg.value().decode('utf-8')
                logger.info(f"Received message: {msg_value[:100]}...")
                consumer_service.commit_offsets(msg) 

    except KeyboardInterrupt:
        logger.info("Kafka Consumer Service example stopped.")
    finally:
        consumer_service.close()
