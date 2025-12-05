import shutil

import pytest


@pytest.fixture
def store_path(tmp_path):
    yield tmp_path
    shutil.rmtree(tmp_path, ignore_errors=True)
