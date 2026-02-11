import os
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func # Import func for aggregation queries
from dotenv import load_dotenv
from loguru import logger

from backend.db.database import get_db
from backend.db.models import DecodedSolanaTransaction
from backend.schemas.transaction import TransactionResponse, AggregationResult # Import Pydantic models

# Load environment variables
load_dotenv()

# Configure loguru logger
logger.remove() # Remove default handler to avoid duplicate output if already configured
logger.add(os.sys.stderr, level="INFO") # Add back stderr handler for console output

app = FastAPI(
    title="Solana Liquidity Dashboard API",
    description="API for providing decoded Solana transaction data for the liquidity sniper dashboard.",
    version="0.1.0",
)

@app.get("/")
async def read_root():
    """
    Root endpoint for the API.
    """
    logger.info("Root endpoint accessed.")
    return {"message": "Welcome to the Solana Liquidity Dashboard API!"}

@app.get("/transactions/recent", response_model=List[TransactionResponse])
async def get_recent_transactions(
    limit: int = Query(100, ge=1, le=1000, description="Number of recent transactions to return."),
    offset: int = Query(0, ge=0, description="Offset for pagination."),
    db: Session = Depends(get_db)
):
    """
    Retrieves a list of the most recent decoded Solana transactions.
    Ordered by block_slot in descending order.
    """
    logger.info(f"Fetching recent transactions: limit={limit}, offset={offset}")
    transactions = db.query(DecodedSolanaTransaction).order_by(DecodedSolanaTransaction.block_slot.desc()).offset(offset).limit(limit).all()
    if not transactions:
        logger.warning("No recent transactions found.")
    return transactions

@app.get("/analytics/event_type_counts", response_model=List[AggregationResult])
async def get_event_type_counts(
    db: Session = Depends(get_db)
):
    """
    Retrieves the count of transactions grouped by event type.
    """
    logger.info("Fetching event type counts.")
    result = db.query(
        DecodedSolanaTransaction.event_type.label("name"),
        func.count(DecodedSolanaTransaction.id).label("count")
    ).group_by(DecodedSolanaTransaction.event_type).order_by(func.count(DecodedSolanaTransaction.id).desc()).all()
    return result

@app.get("/analytics/program_id_counts", response_model=List[AggregationResult])
async def get_program_id_counts(
    db: Session = Depends(get_db)
):
    """
    Retrieves the count of transactions grouped by program ID.
    """
    logger.info("Fetching program ID counts.")
    result = db.query(
        DecodedSolanaTransaction.program_id.label("name"),
        func.count(DecodedSolanaTransaction.id).label("count")
    ).group_by(DecodedSolanaTransaction.program_id).order_by(func.count(DecodedSolanaTransaction.id).desc()).all()
    return result

@app.get("/transactions/{transaction_signature}", response_model=TransactionResponse)
async def get_transaction_by_signature(
    transaction_signature: str,
    db: Session = Depends(get_db)
):
    """
    Retrieves the full details of a single decoded Solana transaction by its signature.
    """
    logger.info(f"Fetching transaction by signature: {transaction_signature}")
    transaction = db.query(DecodedSolanaTransaction).filter(DecodedSolanaTransaction.transaction_signature == transaction_signature).first()
    if not transaction:
        logger.warning(f"Transaction with signature {transaction_signature} not found.")
        raise HTTPException(status_code=404, detail="Transaction not found")
    return transaction

logger.info("FastAPI application initialized.")