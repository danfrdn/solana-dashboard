from sqlalchemy import Column, Integer, String, BigInteger, Numeric, DateTime, func, JSON
from sqlalchemy.ext.declarative import declarative_base
from backend.db.database import engine
from loguru import logger

# Base class for our declarative models
# All ORM models will inherit from this Base.
Base = declarative_base()

class DecodedSolanaTransaction(Base):
    """
    SQLAlchemy model for storing decoded Solana transaction data.
    """
    __tablename__ = "decoded_solana_transactions" # Name of the table in the database

    id = Column(Integer, primary_key=True, index=True)
    transaction_signature = Column(String(100), unique=True, nullable=False, index=True)
    block_slot = Column(BigInteger, nullable=False, index=True)
    block_time = Column(DateTime(timezone=True), nullable=False) # Store as timezone-aware for consistency
    program_id = Column(String(100), nullable=False, index=True)
    event_type = Column(String(50), nullable=False)
    token_mint_address_in = Column(String(100), index=True)
    amount_in = Column(Numeric(28, 9)) # Precision for Solana token amounts (up to 28 digits total, 9 after decimal)
    token_mint_address_out = Column(String(100), index=True)
    amount_out = Column(Numeric(28, 9)) # Precision for Solana token amounts
    signer_address = Column(String(100)) # Wallet address that initiated the transaction
    raw_log_message = Column(JSON, nullable=False) # Store the entire raw JSON log as a JSON type in Postgres
    created_at = Column(DateTime(timezone=True), server_default=func.now()) # Automatically set creation timestamp

    def __repr__(self):
        """
        String representation for debugging and logging.
        """
        return (f"<DecodedSolanaTransaction(signature='{self.transaction_signature}', "
                f"event_type='{self.event_type}', program_id='{self.program_id}')>")

# Create tables in the database if they don't already exist.
# This line will connect to the database using the engine from database.py
# and create all tables defined as subclasses of Base.
Base.metadata.create_all(bind=engine)
logger.info("Database models defined and tables ensured to exist.")
