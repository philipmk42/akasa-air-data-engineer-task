from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Numeric, DateTime, Index, UniqueConstraint
from datetime import datetime
from decimal import Decimal

class Base(DeclarativeBase):
    pass

class Customer(Base):
    __tablename__ = "customers"
    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    customer_name: Mapped[str] = mapped_column(String(200), nullable=False)
    mobile_number: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    region: Mapped[str] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        UniqueConstraint("mobile_number", name="uq_customers_mobile"),
        Index("ix_customers_region", "region"),
    )

class Order(Base):
    __tablename__ = "orders"
    order_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    mobile_number: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    order_date_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    sku_id: Mapped[str] = mapped_column(String(64), nullable=False)
    sku_count: Mapped[int] = mapped_column(Integer, nullable=False)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    __table_args__ = (
        Index("ix_orders_mobiledatetime", "mobile_number", "order_date_time"),
    )
