from elasticmagic.compiler import Compiler_6_0

import pytest


def assert_search_query(sq, expected, compiler):
    # import pprint
    # pprint.pprint(sq.to_dict(compiler))
    # pprint.pprint(expected.to_dict(compiler))
    assert sq.to_dict(compiler) == expected.to_dict(compiler)


@pytest.fixture
def compiler():
    return Compiler_6_0
