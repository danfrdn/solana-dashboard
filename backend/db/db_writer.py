from sqlalchemy.orm import Session
from backend.db.database import SessionLocal # Import SessionLocal to create sessions
from backend.db.models import DecodedSolanaTransaction
from datetime import datetime, timezone # For example data timestamp
from typing import Dict, Any, Optional
from loguru import logger

class DatabaseWriter:
    """
    Handles writing decoded Solana transaction data to the PostgreSQL database.
    """
    def __init__(self):
        logger.info("DatabaseWriter initialized.")

    def write_transaction(self, decoded_data: Dict[str, Any]) -> Optional[DecodedSolanaTransaction]:
        """
        Writes a single decoded Solana transaction to the database.

        Args:
            decoded_data (Dict[str, Any]): A dictionary containing the decoded
                                            transaction data, matching the
                                            DecodedSolanaTransaction model fields.

        Returns:
            Optional[DecodedSolanaTransaction]: The created ORM object if successful, else None.
        """
        # Ensure we don't try to write if essential data is missing
        if not decoded_data.get("transaction_signature") or not decoded_data.get("block_slot"):
            logger.error(f"Attempted to write incomplete data to DB: Missing signature or block_slot. Data: {decoded_data}")
            return None

        db: Optional[Session] = None
        try:
            db = SessionLocal() # Manually get a session
            
            # Create a new DecodedSolanaTransaction object
            # Filter out any keys from decoded_data that are not attributes of DecodedSolanaTransaction
            # to prevent SQLAlchemy from complaining about unexpected arguments.
            model_fields = {c.name for c in DecodedSolanaTransaction.__table__.columns}
            filtered_data = {k: v for k, v in decoded_data.items() if k in model_fields}

            db_transaction = DecodedSolanaTransaction(**filtered_data)
            
            db.add(db_transaction)
            db.commit() # Commit the transaction to save to DB
            db.refresh(db_transaction) # Refresh to load any generated fields (like 'id', 'created_at')
            logger.info(f"Successfully wrote transaction {db_transaction.transaction_signature} to DB.")
            return db_transaction
        except Exception as e:
            if db:
                db.rollback() # Rollback in case of error
            
            # Check for unique constraint violation (transaction_signature)
            # This is a common error if the same transaction comes through again.
            # The specific error message might vary slightly based on DB driver/version.
            error_message = str(e)
            if "duplicate key value violates unique constraint" in error_message or "UniqueViolation" in error_message:
                 logger.warning(f"Duplicate transaction signature '{decoded_data.get('transaction_signature')}' already exists. Skipping write.")
            else:
                logger.error(f"Failed to write transaction to DB: {e}. Data: {decoded_data}")
            return None
        finally:
            if db:
                db.close() # Ensure session is closed

# Example usage (for testing purposes, not part of the main application flow)
if __name__ == "__main__":
    logger.info("Starting DatabaseWriter example (press Ctrl+C to stop)...")
    writer = DatabaseWriter()

    # Example decoded data
    example_data_1 = {
        "transaction_signature": "5pW5jSzSM9iRA8azgEgLJ4dXkPtBS7jEcfK81hDxhe",
        "block_slot": 123456789,
        "block_time": datetime.now(timezone.utc),
        "program_id": "TokenkegQfe",
        "event_type": "SPL Token Transfer",
        "token_mint_address_in": "So11111111111111111111111111111111111111112",
        "amount_in": 1.5,
        "token_mint_address_out": "EPjFWdd5AufqSSqeM2qN1xzybapT8Ga6WgKtEke9NAtg",
        "amount_out": 1000.0,
        "signer_address": "8pW5jSzSM9iRA8azgEgLJ4dXkPtBS7j",
        "raw_log_message": {"some": "raw", "json": "data"}
    }

    written_transaction_1 = writer.write_transaction(example_data_1)

    if written_transaction_1:
        logger.info(f"Example transaction 1 written successfully with ID: {written_transaction_1.id}")
        # Test writing a duplicate to see the warning
        logger.info("Attempting to write a duplicate transaction...")
        duplicate_transaction = writer.write_transaction(example_data_1)
        if duplicate_transaction is None:
            logger.info("Successfully handled duplicate transaction attempt for example 1.")

    # Example for a second unique transaction
    example_data_2 = {
        "transaction_signature": "UNIQUE_SIG_4dXkPtBS7jEcfK81hDxhe5pW5jSzSM9iRA8azgEgLJ",
        "block_slot": 123456790,
        "block_time": datetime.now(timezone.utc),
        "program_id": "ComputeBudget",
        "event_type": "Compute Budget",
        "raw_log_message": {"another": "unique", "message": "here"}
    }
    written_transaction_2 = writer.write_transaction(example_data_2)
    if written_transaction_2:
        logger.info(f"Example transaction 2 written successfully with ID: {written_transaction_2.id}")

    logger.info("DatabaseWriter example finished.")
