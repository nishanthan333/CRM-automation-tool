import pytest

from crm.db import connect, migrate
from tests.fakes import FakeClient


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "watchdog.db")
    migrate(connection)
    yield connection
    connection.close()


@pytest.fixture
def fake_client():
    return FakeClient()
