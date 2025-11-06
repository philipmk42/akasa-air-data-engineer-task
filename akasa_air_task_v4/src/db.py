from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .config import db_url

engine = create_engine(db_url(), pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def execute(sql: str, params: dict | None = None):
    with engine.begin() as conn:
        return conn.execute(text(sql), params or {})
