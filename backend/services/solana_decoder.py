import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from loguru import logger

class SolanaLogDecoder:
    """
    Decodes raw Solana transaction log messages from Kafka into a structured format
    suitable for the DecodedSolanaTransaction model.

    For MVP, this provides basic extraction and placeholders for more complex fields
    that would typically require fetching full transaction metadata via RPC.
    """
    def __init__(self):
        logger.info("SolanaLogDecoder initialized.")

    def decode_message(self, raw_message: bytes) -> Optional[Dict[str, Any]]:
        """
        Parses a raw Kafka message (bytes) containing Solana log data
        and extracts relevant features.

        Args:
            raw_message (bytes): The raw message value from Kafka.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing decoded transaction data,
                                      or None if the message cannot be decoded or is not a relevant log.
        """
        try:
            message_str = raw_message.decode('utf-8')
            message_dict = json.loads(message_str)

            # We are primarily interested in 'logsNotification' messages from our subscription
            if (message_dict.get("jsonrpc") == "2.0" and
                message_dict.get("method") == "logsNotification" and
                "params" in message_dict and
                "result" in message_dict["params"] and
                "value" in message_dict["params"]["result"]):

                log_value = message_dict["params"]["result"]["value"]
                
                if not isinstance(log_value, dict):
                    logger.error(f"logsNotification 'value' is not a dictionary. Skipping. Raw message (value portion): {str(log_value)[:500]}...")
                    return None

                context_slot = message_dict["params"]["result"]["context"].get("slot")
                if context_slot is None:
                    logger.error(f"logsNotification 'context.slot' is missing. Skipping. Raw message (context portion): {str(message_dict['params']['result']['context'])[:500]}...")
                    return None

                transaction_signature = log_value.get("signature")
                if transaction_signature is None:
                    logger.error(f"logsNotification 'value.signature' is missing. Skipping. Raw message (value portion): {str(log_value)[:500]}...")
                    return None
                
                err = log_value.get("err")
                logs = log_value.get("logs", [])

                if transaction_signature is None:
                    logger.warning(f"Logs notification message missing transaction signature. Skipping: {message_str[:200]}...")
                    return None

                # Initialize with placeholder/default values mapping to DecodedSolanaTransaction model
                decoded_data = {
                    "transaction_signature": transaction_signature,
                    "block_slot": context_slot,
                    "block_time": datetime.now(timezone.utc), # Placeholder: Actual block time requires separate RPC call or full block data
                    "program_id": "Unknown", # Will attempt to extract from logs
                    "event_type": "Generic Invocation", # Default event type for MVP
                    "token_mint_address_in": None, # Requires advanced parsing or full transaction details
                    "amount_in": None,             # Requires advanced parsing or full transaction details
                    "token_mint_address_out": None, # Requires advanced parsing or full transaction details
                    "amount_out": None,            # Requires advanced parsing or full transaction details
                    "signer_address": None,        # Requires advanced parsing or full transaction details
                    "raw_log_message": message_dict # Store the entire message for debugging/reprocessing
                }

                # --- Basic Log Parsing Heuristics for MVP ---
                # Attempt to find the first invoked program ID
                first_program_id = None
                for log_entry in logs:
                    if "Program " in log_entry and " invoke [" in log_entry:
                        parts = log_entry.split(" ")
                        if len(parts) >= 2:
                            # Example: "Program ComputeBudget111111... invoke [1]" -> extract "ComputeBudget111111..."
                            first_program_id = parts[1]
                            break # Get the first invoked program for simplicity

                if first_program_id:
                    decoded_data["program_id"] = first_program_id
                    # Simple event type inference based on known program IDs for MVP
                    if "TokenkegQfe" in first_program_id: # Solana SPL Token Program
                        decoded_data["event_type"] = "SPL Token Transfer"
                    elif "ComputeBudget" in first_program_id:
                        decoded_data["event_type"] = "Compute Budget"
                    # Add more heuristics here for specific DEXs (Raydium, Orca, Jupiter) if their program IDs are known and logs provide clues
                    # e.g., if first_program_id == "EcfK8...": # Example Raydium AMM program ID
                    #     decoded_data["event_type"] = "Raydium Swap"


                if err: # Check if the transaction itself had an error
                    decoded_data["event_type"] += " (Error)"
                    logger.warning(f"Transaction {transaction_signature} in slot {context_slot} has an error: {err}")

                logger.debug(f"Decoded transaction {transaction_signature} for slot {context_slot} - Event: {decoded_data['event_type']}")
                return decoded_data
            else:
                # Handle other RPC messages that are not logsNotification (e.g., subscription confirmation)
                if message_dict.get("jsonrpc") == "2.0" and ("id" in message_dict and "result" in message_dict):
                    logger.debug(f"Received RPC response (likely subscription confirmation): {message_str[:200]}...")
                else:
                    logger.warning(f"Received unrecognized message format. Skipping: {message_str[:200]}...")
                return None

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}. Raw message: {raw_message.decode('utf-8', errors='ignore')[:500]}...")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during message decoding: {e}. Raw message: {raw_message.decode('utf-8', errors='ignore')[:500]}...")
            return None
