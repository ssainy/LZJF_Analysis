import pytest

@pytest.fixture(autouse=1,scope="session")
def fix1():
    print("session")