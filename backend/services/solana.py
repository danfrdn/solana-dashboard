import os
import asyncio
import websockets
from dotenv import load_dotenv
import json

load_dotenv()

# Public Solana Devnet WebSocket URL
WSS_URL = "wss://api.devnet.solana.com/"

async def connect_to_solana_websocket():
    """
    Connects to the public Solana Devnet WebSocket and subscribes to logs.
    Prints incoming messages to the console.
    """
    print(f"Connecting to public Solana Devnet WebSocket: {WSS_URL}")
    async for websocket in websockets.connect(WSS_URL):
        try:
            # Subscribe to all logs for all transactions.
            # This is a standard Solana RPC method, compatible with public endpoints.

            subscription_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "logsSubscribe",
                "params": ["all"]
            }
            await websocket.send(json.dumps(subscription_request))
            print("Subscribed to all transaction logs on public Solana devnet.")

            while True:
                message = await websocket.recv()
                # For initial testing, just print the message
                print("Received Solana log message:")
                print(message[:500] + "..." if len(message) > 500 else message)

        except websockets.exceptions.ConnectionClosed:
            print("Solana WebSocket connection closed. Reconnecting...")
            continue
        except Exception as e:
            print(f"An error occurred: {e}. Reconnecting...")
            continue

if __name__ == "__main__":
    print("Starting Solana WebSocket client (press Ctrl+C to stop)...")
    try:
        asyncio.run(connect_to_solana_websocket())
    except KeyboardInterrupt:
        print("Solana WebSocket client stopped.")