from pydantic import BaseModel, ConfigDict
from typing import List, Optional
from datetime import datetime

# --- Source ---
class SourceBase(BaseModel):
    name: str

class SourceCreate(SourceBase):
    pass

class SourceResponse(SourceBase):
    id: int
    model_config = ConfigDict(from_attributes=True)
# --- Asset ---
class AssetBase(BaseModel):
    ticker: str
    currency: str
    asset_type: str
    source_id: int

class AssetCreate(AssetBase):
    pass

class AssetUpdate(AssetBase):
    pass # Для PUT (полная замена)

class AssetPatch(BaseModel):
    # Для PATCH (частичное обновление, все поля опциональны)
    ticker: Optional[str] = None
    currency: Optional[str] = None
    asset_type: Optional[str] = None
    source_id: Optional[int] = None

class AssetResponse(AssetBase):
    id: int
    model_config = ConfigDict(from_attributes=True)

# --- Quote ---
class QuoteBase(BaseModel):
    price: float

class QuoteCreate(QuoteBase):
    pass

class QuoteResponse(QuoteBase):
    id: int
    asset_id: int
    timestamp: datetime
    model_config = ConfigDict(from_attributes=True)