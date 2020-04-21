import pytest

from elasticmagic.ext.queryfilter import QueryFilter

from elasticmagic_qf_attrs import AttrBoolSimpleFilter
from elasticmagic_qf_attrs import AttrIntSimpleFilter
from elasticmagic_qf_attrs import AttrRangeSimpleFilter

from .attrs import Country
from .attrs import Display
from .attrs import Manufacturer
from .attrs import Waterproof
from .docs import ProductDoc


class AttrsQueryFilter(QueryFilter):
    attrs = AttrIntSimpleFilter(ProductDoc.attrs, alias='a')
    attrs_bool = AttrBoolSimpleFilter(ProductDoc.attrs_bool, alias='a')
    attrs_range = AttrRangeSimpleFilter(ProductDoc.attrs_range, alias='a')


@pytest.mark.asyncio
async def test_int_attrs(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Manufacturer.attr_id}': f'{Manufacturer.Values.apple}'
        }
    )
    assert (await sq.count()) == 1

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Country.attr_id}': f'{Country.Values.china}'
        }
    )
    assert (await sq.count()) == 4

    sq = qf.apply(
        es_index .search_query(),
        {
            f'a{Country.attr_id}': f'{Country.Values.china}',
            f'a{Manufacturer.attr_id}': f'{Manufacturer.Values.huawei}'
        }
    )
    assert (await sq.count()) == 2

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Country.attr_id}': f'{Country.Values.china}',
            f'a{Manufacturer.attr_id}': f'{Manufacturer.Values.apple}'
        }
    )
    assert (await sq.count()) == 0

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Country.attr_id}': [
                f'{Country.Values.china}', f'{Country.Values.usa}',
            ],
            f'a{Manufacturer.attr_id}': [
                f'{Manufacturer.Values.apple}',
                f'{Manufacturer.Values.xiaomi}',
            ]
        }
    )
    assert (await sq.count()) == 3


@pytest.mark.asyncio
async def test_bool_attrs(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Waterproof.attr_id}': 'true'
        }
    )
    assert (await sq.count()) == 3

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Waterproof.attr_id}': 'false'
        }
    )
    assert (await sq.count()) == 3

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Waterproof.attr_id}': ['true', 'false']
        }
    )
    assert (await sq.count()) == 6


@pytest.mark.asyncio
async def test_range_attrs(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Display.attr_id}__gte': '6.5'
        }
    )
    assert (await sq.count()) == 2

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Display.attr_id}__gte': '6.55'
        }
    )
    assert (await sq.count()) == 1

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Display.attr_id}__lte': '6.5'
        }
    )
    assert (await sq.count()) == 6

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Display.attr_id}__gte': '6.2',
            f'a{Display.attr_id}__lte': '6.4'
        }
    )
    assert (await sq.count()) == 3


@pytest.mark.asyncio
async def test_all_attrs(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Manufacturer.attr_id}': [
                f'{Manufacturer.Values.huawei}',
                f'{Manufacturer.Values.samsung}',
                f'{Manufacturer.Values.xiaomi}',
            ],
            f'a{Country.attr_id}': f'{Country.Values.china}',
            f'a{Waterproof.attr_id}': 'true',
        }
    )
    assert (await sq.count()) == 2

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Manufacturer.attr_id}': [
                f'{Manufacturer.Values.apple}',
                f'{Manufacturer.Values.huawei}',
                f'{Manufacturer.Values.xiaomi}',
            ],
            f'a{Country.attr_id}': f'{Country.Values.china}',
            f'a{Display.attr_id}__gte': '6.45',
            f'a{Waterproof.attr_id}': ['false', 'true'],
        }
    )
    assert (await sq.count()) == 2
