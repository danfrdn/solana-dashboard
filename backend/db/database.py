import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Database connection details from .env
# Default to our docker-compose PostgreSQL setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/solana_liquidity")

# SQLAlchemy Engine
# The `pool_pre_ping=True` option will enable a "pre-ping" feature that tests the database connection
# before using it, helping to catch stale connections and ensure connection health.
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# SQLAlchemy SessionLocal class
# Each instance of the SessionLocal class will be a database session.
# `autocommit=False` ensures we explicitly commit transactions.
# `autoflush=False` prevents session from flushing pending changes to the DB automatically before query.
# `bind=engine` connects the session to our database engine.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Dependency to get a database session.
    Yields a session that will be automatically closed after use.
    This pattern is common in FastAPI for managing database sessions.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

logger.info("Database engine and session configured.")
