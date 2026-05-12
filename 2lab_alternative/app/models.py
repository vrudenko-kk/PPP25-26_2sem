from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base

class Source(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True) # Например: "MOEX", "CoinGecko"
    
    # Связь 1-ко-многим с активами
    assets = relationship("Asset", back_populates="source", cascade="all, delete-orphan")

class Asset(Base):
    __tablename__ = "assets"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, unique=True, index=True)
    currency = Column(String)
    asset_type = Column(String) # 'Stock' или 'Crypto'
    source_id = Column(Integer, ForeignKey("sources.id"))

    source = relationship("Source", back_populates="assets")
    # Связь 1-ко-многим с котировками
    quotes = relationship("Quote", back_populates="asset", cascade="all, delete-orphan")

class Quote(Base):
    __tablename__ = "quotes"

    id = Column(Integer, primary_key=True, index=True)
    asset_id = Column(Integer, ForeignKey("assets.id"))
    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)

    asset = relationship("Asset", back_populates="quotes")