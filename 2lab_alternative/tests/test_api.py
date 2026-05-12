import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.main import app
from app.db import Base, get_db

# Создаем тестовую БД в памяти
SQLALCHEMY_DATABASE_URL = "sqlite:///./test_market.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Переопределяем зависимость БД
def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)

@pytest.fixture(autouse=True, scope="function")
def setup_db():
    # Создаем таблицы перед каждым тестом
    Base.metadata.create_all(bind=engine)
    yield
    # Удаляем таблицы после каждого теста, чтобы тесты были изолированы
    Base.metadata.drop_all(bind=engine)

# --- ТЕСТЫ ---

def test_create_source_and_asset():
    """POST + GET (CRUD)"""
    # Создаем источник
    resp_s = client.post("/sources", json={"name": "TEST_EXCHANGE"})
    assert resp_s.status_code == 201
    source_id = resp_s.json()["id"]

    # Создаем актив (POST)
    resp_a = client.post("/assets", json={
        "ticker": "TST", "currency": "USD", "asset_type": "Stock", "source_id": source_id
    })
    assert resp_a.status_code == 201
    assert resp_a.json()["ticker"] == "TST"

def test_get_assets_list():
    """GET List"""
    response = client.get("/assets")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_update_asset_put():
    """PUT (Полное обновление)"""
    client.post("/sources", json={"name": "S1"})
    client.post("/assets", json={"ticker": "A1", "currency": "RUB", "asset_type": "Stock", "source_id": 1})
    
    response = client.put("/assets/1", json={
        "ticker": "A1_UPDATED", "currency": "USD", "asset_type": "Crypto", "source_id": 1
    })
    assert response.status_code == 200
    assert response.json()["currency"] == "USD"

def test_patch_asset():
    """PATCH (Частичное обновление)"""
    client.post("/sources", json={"name": "S1"})
    client.post("/assets", json={"ticker": "A1", "currency": "RUB", "asset_type": "Stock", "source_id": 1})
    
    response = client.patch("/assets/1", json={"currency": "EUR"})
    assert response.status_code == 200
    assert response.json()["currency"] == "EUR"
    assert response.json()["ticker"] == "A1" # Остался прежним

def test_delete_asset():
    """DELETE"""
    client.post("/sources", json={"name": "S1"})
    client.post("/assets", json={"ticker": "DEL", "currency": "RUB", "asset_type": "Stock", "source_id": 1})
    
    response = client.delete("/assets/1")
    assert response.status_code == 204
    
    # Проверяем 404 после удаления
    check = client.get("/assets/1")
    assert check.status_code == 404

def test_validation_error():
    """422 Unprocessable Entity (невалидные данные)"""
    # Отправляем строку там, где ожидается int (source_id)
    response = client.post("/assets", json={"ticker": "ERR", "source_id": "NOT_AN_INT"})
    assert response.status_code == 422

def test_asset_not_found():
    """404 Not Found"""
    response = client.get("/assets/999")
    assert response.status_code == 404