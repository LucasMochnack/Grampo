"""
Test fixtures.

Database strategy: one in-memory SQLite engine per test session; each test
function runs inside a transaction that is rolled back on teardown.  This
gives clean state per test with no table truncation overhead.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.dependencies import get_db
from app.main import create_app

_TEST_DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture(scope="session")
def _engine():
    engine = create_engine(
        _TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def db_session(_engine):
    """Yield a DB session whose transaction is rolled back after the test."""
    connection = _engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture()
def client(db_session):
    """TestClient wired to the per-test database session."""
    app = create_app()

    def _override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db

    with TestClient(app) as c:
        yield c
