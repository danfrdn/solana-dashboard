import os
import asyncio
from dotenv import load_dotenv
from loguru import logger
import time # For time.sleep in synchronous contexts, though asyncio.sleep is used here

from backend.services.kafka_consumer import KafkaConsumerService
from backend.services.solana_decoder import SolanaLogDecoder
from backend.db.db_writer import DatabaseWriter
from backend.db.models import Base, engine # Import Base and engine to ensure tables are created

# Configure loguru logger
logger.remove() # Remove default handler to avoid duplicate output if already configured
logger.add(os.sys.stderr, level="INFO") # Add back stderr handler for console output

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TRANSACTIONS_TOPIC = os.getenv("KAFKA_RAW_TRANSACTIONS_TOPIC", "solana_raw_transactions")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "solana-liquidity-processor-group")

async def process_solana_logs():
    """
    Main function to consume raw Solana logs from Kafka, decode them,
    and write the structured data to PostgreSQL.
    """
    logger.info("Starting Solana log processor...")

    # Ensure database tables are created (idempotent operation)
    Base.metadata.create_all(bind=engine)
    logger.info("Ensured database tables exist.")

    consumer_service = None
    try:
        consumer_service = KafkaConsumerService(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP_ID,
            topic=KAFKA_RAW_TRANSACTIONS_TOPIC
        )
        decoder = SolanaLogDecoder()
        db_writer = DatabaseWriter()

        logger.info(f"Listening for messages on topic: {KAFKA_RAW_TRANSACTIONS_TOPIC} with group ID: {KAFKA_CONSUMER_GROUP_ID}")

        while True:
            # Poll for a message from Kafka (timeout 1 second)
            msg = consumer_service.consume_messages(timeout=1.0)
            
            if msg is None:
                # No message received within the timeout, continue polling
                # logger.debug("No new message from Kafka.")
                await asyncio.sleep(0.1) # Small delay to avoid busy-waiting
                continue

            raw_kafka_message_value = msg.value().decode('utf-8', errors='ignore')
            logger.info(f"Processor received raw message from Kafka: {raw_kafka_message_value[:500]}...")
            # Decode the message
            decoded_data = decoder.decode_message(msg.value())

            if decoded_data:
                # Write to database
                written_transaction = db_writer.write_transaction(decoded_data)
                if written_transaction:
                    # Only commit Kafka offset if the message was successfully processed AND written to DB
                    consumer_service.commit_offsets(msg)
                else:
                    logger.warning(f"Failed to write decoded transaction {decoded_data.get('transaction_signature')} to DB. Offset NOT committed. Message will be reprocessed.")
            else:
                logger.warning(f"Message from offset {msg.offset()} could not be decoded or was deemed irrelevant. Committing offset to move forward.")
                consumer_service.commit_offsets(msg) # Commit offset for non-processable messages (e.g., RPC responses, malformed logs)

            # Small delay to prevent busy-looping if there's a quick succession of messages
            await asyncio.sleep(0.01) # Small async sleep

    except KeyboardInterrupt:
        logger.info("Solana log processor stopped by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in the processor: {e}")
    finally:
        if consumer_service:
            consumer_service.close()
        logger.info("Solana log processor shut down.")


if __name__ == "__main__":
    logger.info("Starting Solana log processor as main entry point...")
    asyncio.run(process_solana_logs())
