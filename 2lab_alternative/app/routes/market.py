from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List
from app import models, schemas
from app.db import get_db

router = APIRouter()

@router.get("/sources", response_model=List[schemas.SourceResponse], tags=["Sources"])
def get_sources(db: Session = Depends(get_db)):
    return db.query(models.Source).all()

@router.post("/sources", response_model=schemas.SourceResponse, status_code=201, tags=["Sources"])
def create_source(source: schemas.SourceCreate, db: Session = Depends(get_db)):
    db_source = models.Source(name=source.name)
    try:
        db.add(db_source)
        db.commit()
        db.refresh(db_source)
        return db_source
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Source already exists")

@router.get("/assets", response_model=List[schemas.AssetResponse], tags=["Assets"])
def get_assets(db: Session = Depends(get_db)):
    return db.query(models.Asset).all()

@router.get("/assets/{asset_id}", response_model=schemas.AssetResponse, tags=["Assets"])
def get_asset(asset_id: int, db: Session = Depends(get_db)):
    asset = db.query(models.Asset).filter(models.Asset.id == asset_id).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    return asset

@router.post("/assets", response_model=schemas.AssetResponse, status_code=201, tags=["Assets"])
def create_asset(asset: schemas.AssetCreate, db: Session = Depends(get_db)):
    try:
        new_asset = models.Asset(**asset.model_dump())
        db.add(new_asset)
        db.commit()
        db.refresh(new_asset)
        return new_asset
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Integrity error (duplicate ticker or invalid source_id)")

@router.put("/assets/{asset_id}", response_model=schemas.AssetResponse, tags=["Assets"])
def update_asset(asset_id: int, asset_in: schemas.AssetUpdate, db: Session = Depends(get_db)):
    asset = db.query(models.Asset).filter(models.Asset.id == asset_id).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    for key, value in asset_in.model_dump().items():
        setattr(asset, key, value)
    db.commit()
    db.refresh(asset)
    return asset

@router.patch("/assets/{asset_id}", response_model=schemas.AssetResponse, tags=["Assets"])
def patch_asset(asset_id: int, asset_in: schemas.AssetPatch, db: Session = Depends(get_db)):
    asset = db.query(models.Asset).filter(models.Asset.id == asset_id).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    for key, value in asset_in.model_dump(exclude_unset=True).items():
        setattr(asset, key, value)
    db.commit()
    db.refresh(asset)
    return asset

@router.delete("/assets/{asset_id}", status_code=204, tags=["Assets"])
def delete_asset(asset_id: int, db: Session = Depends(get_db)):
    asset = db.query(models.Asset).filter(models.Asset.id == asset_id).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    db.delete(asset)
    db.commit()

@router.get("/assets/{asset_id}/quotes", response_model=List[schemas.QuoteResponse], tags=["Quotes"])
def get_quotes(asset_id: int, db: Session = Depends(get_db)):
    asset = db.query(models.Asset).filter(models.Asset.id == asset_id).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    return asset.quotes

@router.post("/assets/{asset_id}/quotes", response_model=schemas.QuoteResponse, status_code=201, tags=["Quotes"])
def create_quote(asset_id: int, quote: schemas.QuoteCreate, db: Session = Depends(get_db)):
    # Проверяем, существует ли актив
    asset = db.query(models.Asset).filter(models.Asset.id == asset_id).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    new_quote = models.Quote(**quote.model_dump(), asset_id=asset_id)
    db.add(new_quote)
    db.commit()
    db.refresh(new_quote)
    return new_quote
