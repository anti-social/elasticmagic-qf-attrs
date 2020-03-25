import pytest

from elasticmagic.ext.queryfilter import QueryFilter

from elasticmagic_qf_attrs import AttrIntFacetFilter

from .attrs import Country
from .attrs import Manufacturer
from .docs import ProductDoc


class IntAttrsQueryFilter(QueryFilter):
    attrs = AttrIntFacetFilter(ProductDoc.attrs, alias='a')


@pytest.mark.asyncio
async def test_int_attrs_empty(es_index, products):
    qf = IntAttrsQueryFilter()

    sq = qf.apply(es_index.search_query(), {})
    qf_res = qf.process_result(await sq.get_result())

    manufacturer_facet = qf_res.attrs.get_facet(Manufacturer.attr_id)
    assert len(manufacturer_facet.all_values) == 4
    apple = manufacturer_facet.get_value(Manufacturer.Values.apple)
    assert apple.count == 1
    assert apple.selected is False
    samsung = manufacturer_facet.get_value(Manufacturer.Values.samsung)
    assert samsung.count == 1
    assert samsung.selected is False
    huawei = manufacturer_facet.get_value(Manufacturer.Values.huawei)
    assert huawei.count == 2
    assert huawei.selected is False
    xiaomi = manufacturer_facet.get_value(Manufacturer.Values.xiaomi)
    assert xiaomi.count == 1
    assert xiaomi.selected is False

    country_facet = qf_res.attrs.get_facet(Country.attr_id)
    assert len(country_facet.all_values) == 3
    china = country_facet.get_value(Country.Values.china)
    assert china.count == 3
    assert china.selected is False
    usa = country_facet.get_value(Country.Values.usa)
    assert usa.count == 1
    assert usa.selected is False
    korea = country_facet.get_value(Country.Values.korea)
    assert korea.count == 1
    assert korea.selected is False


@pytest.mark.asyncio
async def test_int_attrs_single_selected_facet(es_index, products):
    qf = IntAttrsQueryFilter()

    sq = qf.apply(es_index.search_query(), {'a2': '1'})
    qf_res = qf.process_result(await sq.get_result())

    manufacturer_facet = qf_res.attrs.get_facet(Manufacturer.attr_id)
    assert len(manufacturer_facet.all_values) == 2
    assert len(manufacturer_facet.selected_values) == 0
    huawei = manufacturer_facet.get_value(Manufacturer.Values.huawei)
    assert huawei.count == 2
    assert huawei.selected is False
    xiaomi = manufacturer_facet.get_value(Manufacturer.Values.xiaomi)
    assert xiaomi.count == 1
    assert xiaomi.selected is False

    country_facet = qf_res.attrs.get_facet(Country.attr_id)
    assert len(country_facet.all_values) == 3
    assert len(country_facet.selected_values) == 1
    china = country_facet.get_value(Country.Values.china)
    assert china.count == 3
    assert china.selected is True
    usa = country_facet.get_value(Country.Values.usa)
    assert usa.count == 1
    assert usa.selected is False
    korea = country_facet.get_value(Country.Values.korea)
    assert korea.count == 1
    assert korea.selected is False


@pytest.mark.asyncio
async def test_int_attrs_multiple_selected_facets(es_index, products):
    qf = IntAttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Country.attr_id}': f'{Country.Values.china}',
            f'a{Manufacturer.attr_id}': [
                f'{Manufacturer.Values.apple}',
                f'{Manufacturer.Values.huawei}',
            ]
        }
    )
    qf_res = qf.process_result(await sq.get_result())

    manufacturer_facet = qf_res.attrs.get_facet(Manufacturer.attr_id)
    assert len(manufacturer_facet.all_values) == 2
    huawei = manufacturer_facet.get_value(Manufacturer.Values.huawei)
    assert huawei.count == 2
    assert huawei.selected is True
    xiaomi = manufacturer_facet.get_value(Manufacturer.Values.xiaomi)
    assert xiaomi.count == 1
    assert xiaomi.selected is False

    country_facet = qf_res.attrs.get_facet(Country.attr_id)
    assert len(country_facet.all_values) == 2
    china = country_facet.get_value(Country.Values.china)
    assert china.count == 2
    assert china.selected is True
    usa = country_facet.get_value(Country.Values.usa)
    assert usa.count == 1
    assert usa.selected is False
