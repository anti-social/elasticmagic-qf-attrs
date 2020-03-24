import pytest

from elasticmagic.ext.queryfilter import QueryFilter

from elasticmagic_qf_attrs import AttrBoolSimpleFilter
from elasticmagic_qf_attrs import AttrIntSimpleFilter
from elasticmagic_qf_attrs import AttrFloatSimpleFilter

from .docs import ProductDoc


class AttrsQueryFilter(QueryFilter):
    attrs = AttrIntSimpleFilter(ProductDoc.attrs, alias='a')
    attrs_bool = AttrBoolSimpleFilter(ProductDoc.attrs_bool, alias='a')
    attrs_range = AttrFloatSimpleFilter(ProductDoc.attrs_range, alias='a')


@pytest.mark.asyncio
async def test_int_attrs(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(es_index.search_query(), {'a1': '1'})
    assert (await sq.count()) == 1

    sq = qf.apply(es_index.search_query(), {'a2': '1'})
    assert (await sq.count()) == 3

    sq = qf.apply(es_index.search_query(), {'a2': '1', 'a1': '3'})
    assert (await sq.count()) == 2

    sq = qf.apply(es_index.search_query(), {'a2': '1', 'a1': '1'})
    assert (await sq.count()) == 0


@pytest.mark.asyncio
async def test_bool_attrs(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(es_index.search_query(), {'a3': 'true'})
    assert (await sq.count()) == 1

    sq = qf.apply(es_index.search_query(), {'a3': 'false'})
    assert (await sq.count()) == 3

    sq = qf.apply(es_index.search_query(), {'a3': ['true', 'false']})
    assert (await sq.count()) == 4


@pytest.mark.asyncio
async def test_range_attrs(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(es_index.search_query(), {'a4__gte': '6.5'})
    assert (await sq.count()) == 2

    sq = qf.apply(es_index.search_query(), {'a4__gte': '6.55'})
    assert (await sq.count()) == 1

    sq = qf.apply(es_index.search_query(), {'a4__lte': '6.5'})
    assert (await sq.count()) == 4

    sq = qf.apply(
        es_index.search_query(),
        {'a4__gte': '6.2', 'a4__lte': '6.4'}
    )
    assert (await sq.count()) == 2


@pytest.mark.asyncio
async def test_all_attrs(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {
            'a1': ['2', '3', '4'],
            'a2': '1',
            'a3': 'true',
        }
    )
    assert (await sq.count()) == 1

    sq = qf.apply(
        es_index.search_query(),
        {
            'a1': ['1', '3', '4'],
            'a2': '1',
            'a4__gte': '6.45',
            'a3': ['false', 'true'],
        }
    )
    assert (await sq.count()) == 2

