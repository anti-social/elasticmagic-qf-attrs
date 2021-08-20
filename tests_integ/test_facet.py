import pytest

from elasticmagic.ext.queryfilter import QueryFilter

from elasticmagic_qf_attrs import AttrBoolFacetFilter
from elasticmagic_qf_attrs import AttrRangeFacetFilter
from elasticmagic_qf_attrs import AttrIntFacetFilter

from .attrs import Battery
from .attrs import Country
from .attrs import Display
from .attrs import Manufacturer
from .attrs import Waterproof
from .docs import ProductDoc


class AttrsQueryFilter(QueryFilter):
    attrs = AttrIntFacetFilter(ProductDoc.attrs, alias='a')
    bools = AttrBoolFacetFilter(ProductDoc.attrs_bool, alias='a')
    ranges = AttrRangeFacetFilter(ProductDoc.attrs_range, alias='a')


@pytest.mark.asyncio
async def test_facets(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(es_index.search_query(), {})

    res = await sq.get_result()
    assert res.total == 7

    qf_res = qf.process_result(res)

    manufacturer_facet = qf_res.attrs.get_facet(Manufacturer.attr_id)
    assert len(manufacturer_facet.all_values) == 4
    apple = manufacturer_facet.get_value(Manufacturer.Values.apple)
    assert apple.count == 1
    assert apple.count_text == '1'
    assert apple.selected is False
    samsung = manufacturer_facet.get_value(Manufacturer.Values.samsung)
    assert samsung.count == 2
    assert samsung.count_text == '2'
    assert samsung.selected is False
    huawei = manufacturer_facet.get_value(Manufacturer.Values.huawei)
    assert huawei.count == 2
    assert huawei.count_text == '2'
    assert huawei.selected is False
    xiaomi = manufacturer_facet.get_value(Manufacturer.Values.xiaomi)
    assert xiaomi.count == 2
    assert xiaomi.count_text == '2'
    assert xiaomi.selected is False

    country_facet = qf_res.attrs.get_facet(Country.attr_id)
    assert len(country_facet.all_values) == 3
    china = country_facet.get_value(Country.Values.china)
    assert china.count == 4
    assert china.count_text == '4'
    assert china.selected is False
    usa = country_facet.get_value(Country.Values.usa)
    assert usa.count == 1
    assert usa.count_text == '1'
    assert usa.selected is False
    korea = country_facet.get_value(Country.Values.korea)
    assert korea.count == 2
    assert korea.count_text == '2'
    assert korea.selected is False

    waterprof_facet = qf_res.bools.get_facet(Waterproof.attr_id)
    assert len(waterprof_facet.all_values) == 2
    is_waterproof = waterprof_facet.get_value(True)
    assert is_waterproof.count == 3
    assert is_waterproof.count_text == '3'
    assert is_waterproof.selected is False
    is_not_waterproof = waterprof_facet.get_value(False)
    assert is_not_waterproof.count == 3
    assert is_not_waterproof.count_text == '3'
    assert is_not_waterproof.selected is False

    display_facet = qf_res.ranges.get_facet(Display.attr_id)
    assert display_facet.attr_id == Display.attr_id
    assert display_facet.count == 7
    assert display_facet.selected is False

    battery_facet = qf_res.ranges.get_facet(Battery.attr_id)
    assert battery_facet.attr_id == Battery.attr_id
    assert battery_facet.count == 6
    assert battery_facet.selected is False


@pytest.mark.asyncio
async def test_int_attrs__single_int_facet(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {f'a{Country.attr_id}': f'{Country.Values.china}'}
    )

    res = await sq.get_result()
    assert res.total == 4

    qf_res = qf.process_result(res)

    manufacturer_facet = qf_res.attrs.get_facet(Manufacturer.attr_id)
    assert len(manufacturer_facet.all_values) == 2
    assert len(manufacturer_facet.selected_values) == 0
    huawei = manufacturer_facet.get_value(Manufacturer.Values.huawei)
    assert huawei.count == 2
    assert huawei.count_text == '2'
    assert huawei.selected is False
    xiaomi = manufacturer_facet.get_value(Manufacturer.Values.xiaomi)
    assert xiaomi.count == 2
    assert xiaomi.count_text == '2'
    assert xiaomi.selected is False

    country_facet = qf_res.attrs.get_facet(Country.attr_id)
    assert len(country_facet.all_values) == 3
    assert len(country_facet.selected_values) == 1
    china = country_facet.get_value(Country.Values.china)
    assert china.count == 4
    assert china.count_text == '4'
    assert china.selected is True
    usa = country_facet.get_value(Country.Values.usa)
    assert usa.count == 1
    assert usa.count_text == '+1'
    assert usa.selected is False
    korea = country_facet.get_value(Country.Values.korea)
    assert korea.count == 2
    assert korea.count_text == '+2'
    assert korea.selected is False

    waterprof_facet = qf_res.bools.get_facet(Waterproof.attr_id)
    assert len(waterprof_facet.all_values) == 2
    is_waterproof = waterprof_facet.get_value(True)
    assert is_waterproof.count == 2
    assert is_waterproof.count_text == '2'
    assert is_waterproof.selected is False
    is_not_waterproof = waterprof_facet.get_value(False)
    assert is_not_waterproof.count == 1
    assert is_not_waterproof.count_text == '1'
    assert is_not_waterproof.selected is False

    display_facet = qf_res.ranges.get_facet(Display.attr_id)
    assert display_facet.attr_id == Display.attr_id
    assert display_facet.count == 4
    assert display_facet.selected is False

    battery_facet = qf_res.ranges.get_facet(Battery.attr_id)
    assert battery_facet.attr_id == Battery.attr_id
    assert battery_facet.count == 4
    assert battery_facet.selected is False


@pytest.mark.asyncio
async def test_facet__multiple_selected_int_values(es_index, products):
    qf = AttrsQueryFilter()

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

    res = await sq.get_result()
    assert res.total == 2

    qf_res = qf.process_result(res)

    manufacturer_facet = qf_res.attrs.get_facet(Manufacturer.attr_id)
    assert len(manufacturer_facet.all_values) == 2
    huawei = manufacturer_facet.get_value(Manufacturer.Values.huawei)
    assert huawei.count == 2
    assert huawei.count_text == '2'
    assert huawei.selected is True
    xiaomi = manufacturer_facet.get_value(Manufacturer.Values.xiaomi)
    assert xiaomi.count == 2
    assert xiaomi.count_text == '+2'
    assert xiaomi.selected is False

    country_facet = qf_res.attrs.get_facet(Country.attr_id)
    assert len(country_facet.all_values) == 2
    china = country_facet.get_value(Country.Values.china)
    assert china.count == 2
    assert china.count_text == '2'
    assert china.selected is True
    usa = country_facet.get_value(Country.Values.usa)
    assert usa.count == 1
    assert usa.count_text == '+1'
    assert usa.selected is False

    waterprof_facet = qf_res.bools.get_facet(Waterproof.attr_id)
    assert len(waterprof_facet.all_values) == 2
    is_waterproof = waterprof_facet.get_value(True)
    assert is_waterproof.count == 1
    assert is_waterproof.count_text == '1'
    assert is_waterproof.selected is False
    is_not_waterproof = waterprof_facet.get_value(False)
    assert is_not_waterproof.count == 1
    assert is_not_waterproof.count_text == '1'
    assert is_not_waterproof.selected is False

    display_facet = qf_res.ranges.get_facet(Display.attr_id)
    assert display_facet.attr_id == Display.attr_id
    assert display_facet.count == 2
    assert display_facet.selected is False

    battery_facet = qf_res.ranges.get_facet(Battery.attr_id)
    assert battery_facet.attr_id == Battery.attr_id
    assert battery_facet.count == 2
    assert battery_facet.selected is False


@pytest.mark.asyncio
async def test_facet__selected_range_facet(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Display.attr_id}__gte': '6.5'
        }
    )
    print(await sq.to_dict())

    res = await sq.get_result()
    assert res.total == 2

    qf_res = qf.process_result(res)

    manufacturer_facet = qf_res.attrs.get_facet(Manufacturer.attr_id)
    assert len(manufacturer_facet.all_values) == 2
    huawei = manufacturer_facet.get_value(Manufacturer.Values.huawei)
    assert huawei.count == 1
    assert huawei.count_text == '1'
    assert huawei.selected is False
    apple = manufacturer_facet.get_value(Manufacturer.Values.apple)
    assert apple.count == 1
    assert apple.count_text == '1'
    assert apple.selected is False

    country_facet = qf_res.attrs.get_facet(Country.attr_id)
    assert len(country_facet.all_values) == 2
    china = country_facet.get_value(Country.Values.china)
    assert china.count == 1
    assert china.count_text == '1'
    assert china.selected is False
    usa = country_facet.get_value(Country.Values.usa)
    assert usa.count == 1
    assert usa.count_text == '1'
    assert usa.selected is False

    waterprof_facet = qf_res.bools.get_facet(Waterproof.attr_id)
    assert len(waterprof_facet.all_values) == 1
    is_not_waterproof = waterprof_facet.get_value(False)
    assert is_not_waterproof.count == 2
    assert is_not_waterproof.count_text == '2'
    assert is_not_waterproof.selected is False

    display_facet = qf_res.ranges.get_facet(Display.attr_id)
    assert display_facet.attr_id == Display.attr_id
    assert display_facet.count == 7
    assert display_facet.selected is True

    battery_facet = qf_res.ranges.get_facet(Battery.attr_id)
    assert battery_facet.attr_id == Battery.attr_id
    assert battery_facet.count == 1
    assert battery_facet.selected is False


@pytest.mark.asyncio
async def test_facets__different_selected_facets(es_index, products):
    qf = AttrsQueryFilter()

    sq = qf.apply(
        es_index.search_query(),
        {
            f'a{Manufacturer.attr_id}': [
                f'{Manufacturer.Values.huawei}',
                f'{Manufacturer.Values.samsung}',
            ],
            f'a{Waterproof.attr_id}': 'true',
            f'a{Display.attr_id}__lte': '6.5',
        }
    )

    res = await sq.get_result()
    assert res.total == 2

    qf_res = qf.process_result(res)

    manufacturer_facet = qf_res.attrs.get_facet(Manufacturer.attr_id)
    assert len(manufacturer_facet.all_values) == 3
    huawei = manufacturer_facet.get_value(Manufacturer.Values.huawei)
    assert huawei.count == 1
    assert huawei.selected is True
    samsung = manufacturer_facet.get_value(Manufacturer.Values.samsung)
    assert samsung.count == 1
    assert samsung.selected is True
    samsung = manufacturer_facet.get_value(Manufacturer.Values.xiaomi)
    assert samsung.count == 1
    assert samsung.selected is False

    country_facet = qf_res.attrs.get_facet(Country.attr_id)
    assert len(country_facet.all_values) == 2
    china = country_facet.get_value(Country.Values.china)
    assert china.count == 1
    assert china.selected is False
    korea = country_facet.get_value(Country.Values.korea)
    assert korea.count == 1
    assert korea.selected is False

    waterprof_facet = qf_res.bools.get_facet(Waterproof.attr_id)
    assert len(waterprof_facet.all_values) == 2
    is_waterproof = waterprof_facet.get_value(True)
    assert is_waterproof.count == 2
    assert is_waterproof.count_text == '2'
    assert is_waterproof.selected is True
    is_not_waterproof = waterprof_facet.get_value(False)
    assert is_not_waterproof.count == 1
    assert is_not_waterproof.count_text == '+1'
    assert is_not_waterproof.selected is False

    display_facet = qf_res.ranges.get_facet(Display.attr_id)
    assert display_facet.attr_id == Display.attr_id
    assert display_facet.count == 2
    assert display_facet.selected is True

    battery_facet = qf_res.ranges.get_facet(Battery.attr_id)
    assert battery_facet.attr_id == Battery.attr_id
    assert battery_facet.count == 2
    assert battery_facet.selected is False
