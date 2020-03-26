from elasticmagic.compiler import Compiler_6_0

import pytest


@pytest.fixture
def compiler():
    return Compiler_6_0
