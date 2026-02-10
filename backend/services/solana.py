import os
import asyncio
import websockets
from dotenv import load_dotenv
import json
from loguru import logger
from backend.services.kafka_producer import KafkaProducerService # Import our new producer

from services.kafka_producer import KafkaProducerService


logger.remove()
logger.add(os.sys.stderr, level="INFO")


load_dotenv()

# Public Solana Devnet WebSocket URL
WSS_URL = "wss://api.devnet.solana.com/"

# Kafka Configuration from .env, with local defaults for development
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_TRANSACTIONS_TOPIC = os.getenv("KAFKA_RAW_TRANSACTIONS_TOPIC", "solana_raw_transactions")

async def connect_to_solana_websocket_and_produce_to_kafka():
    """
    Connects to the public Solana Devnet WebSocket, subscribes to logs,
    and produces raw messages to Kafka.
    """
    logger.info(f"Initializing Kafka Producer for topic: '{KAFKA_RAW_TRANSACTIONS_TOPIC}' on servers: '{KAFKA_BOOTSTRAP_SERVERS}'")
    kafka_producer = KafkaProducerService(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_RAW_TRANSACTIONS_TOPIC
    )

    logger.info(f"Connecting to public Solana Devnet WebSocket: {WSS_URL}")
    # The `async for websocket` loop automatically handles reconnection attempts
    async for websocket in websockets.connect(WSS_URL):
        try:
            subscription_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "logsSubscribe",
                "params": ["all"]
            }
            await websocket.send(json.dumps(subscription_request))
            logger.info("Subscribed to all transaction logs on public Solana devnet.")

            while True:
                message = await websocket.recv()
                # Parse the JSON string into a Python dictionary
                message_dict = json.loads(message)

                # Produce the raw message (as a dictionary) to Kafka
                kafka_producer.produce_message(message_dict)
                # For high volume, avoid verbose debug logging unless necessary
                # logger.debug(f"Produced message to Kafka (first 100 chars): {message[:100]}...")

        except websockets.exceptions.ConnectionClosed:
            logger.warning("Solana WebSocket connection closed. Reconnecting in 1 second...")
            await asyncio.sleep(1) 
            continue
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}. Reconnecting in 1 second...")
            await asyncio.sleep(1)
            continue
        finally:
            # Ensure any buffered messages are sent to Kafka before the WebSocket potentially reconnects
            kafka_producer.flush()

if __name__ == "__main__":
    logger.info("Starting Solana WebSocket client and Kafka producer (press Ctrl+C to stop)...")
    try:
        asyncio.run(connect_to_solana_websocket_and_produce_to_kafka())
    except KeyboardInterrupt:
        logger.info("Solana WebSocket client and Kafka producer stopped.")
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
