import pytest

from workflow.app import build_app
from workflow.clock import FakeClock


@pytest.fixture
def app():
    return build_app(FakeClock())
