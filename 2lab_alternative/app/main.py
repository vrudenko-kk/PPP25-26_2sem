from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.db import engine
from app import models
from app.routes import market
import etl

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Создаем таблицы
    models.Base.metadata.create_all(bind=engine)
    # Запуск ETL
    print("Running startup ETL...")
    etl.run_etl()
    yield

app = FastAPI(title="Market API", lifespan=lifespan)

# Подключаем роутер
app.include_router(market.router)