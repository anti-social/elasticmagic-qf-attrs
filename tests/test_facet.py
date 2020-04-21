from elasticmagic import agg
from elasticmagic import Bool, Field, Range, Script, Term, Terms
from elasticmagic import SearchQuery
from elasticmagic.ext.queryfilter import QueryFilter
from elasticmagic.result import SearchResult

from elasticmagic_qf_attrs.facet import AttrBoolFacetFilter
from elasticmagic_qf_attrs.facet import AttrRangeFacetFilter
from elasticmagic_qf_attrs.facet import AttrIntFacetFilter

import pytest

from .conftest import assert_search_query


@pytest.fixture
def int_qf():
    qf = QueryFilter()
    qf.add_filter(AttrIntFacetFilter('attr_int', Field('attr.int'), alias='a'))
    yield qf


@pytest.fixture
def int_qf_with_values():
    qf = QueryFilter()
    qf.add_filter(
        AttrIntFacetFilter(
            'attr_int', Field('attr.int'), alias='a',
            attrs_values_getter=lambda _: {18: [0xe2e4, 0xe7e5]}
        )
    )
    yield qf


@pytest.fixture
def bool_qf():
    qf = QueryFilter()
    qf.add_filter(
        AttrBoolFacetFilter('attr_bool', Field('attr.bool'), alias='a')
    )
    yield qf


@pytest.fixture
def range_qf():
    qf = QueryFilter()
    qf.add_filter(
        AttrRangeFacetFilter('attr_range', Field('attr.float'), alias='a')
    )
    yield qf


def test_attr_int_facet_filter__unknown_param(int_qf, compiler):
    sq = int_qf.apply(SearchQuery(), {'b18': '224'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000)
        })
        .to_dict(compiler=compiler)
    )


def test_attr_int_facet_filter__existing_post_filter(int_qf, compiler):
    sq = int_qf.apply(
        SearchQuery().post_filter(Range('price', lt=100)),
        {}
    )
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int.filter': agg.Filter(
                Range('price', lt=100),
                aggs={
                    'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000)
                }
            )
        })
        .post_filter(Range('price', lt=100))
        .to_dict(compiler=compiler)
    )

    sq = int_qf.apply(
        SearchQuery()
        .post_filter(Range('price', lt=100), meta={'price': True}),
        {}
    )
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int.filter': agg.Filter(
                Range('price', lt=100),
                aggs={
                    'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000)
                }
            )
        })
        .post_filter(Range('price', lt=100))
        .to_dict(compiler=compiler)
    )


def test_attr_int_facet_filter__empty_params(int_qf, compiler):
    sq = int_qf.apply(SearchQuery(), {})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000)
        })
        .to_dict(compiler=compiler)
    )
    qf_res = int_qf.process_result(SearchResult(
        {
            'aggregations': {
                'qf.attr_int': {
                    'buckets': [
                        {
                            'key': 0x12_00000001,
                            'doc_count': 123,
                        },
                        {
                            'key': 0x144_0000dead,
                            'doc_count': 99
                        },
                        {
                            'key': 0x12_f0000000,
                            'doc_count': 1
                        }
                    ]
                }
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    facet = qf_res.attr_int.get_facet(18)
    assert len(facet.all_values) == 2
    assert facet.all_values[0].value == 1
    assert facet.all_values[0].count == 123
    assert facet.all_values[0].count_text == '123'
    assert facet.all_values[0].selected is False
    assert facet.all_values[1].value == 4026531840
    assert facet.all_values[1].count == 1
    assert facet.all_values[1].count_text == '1'
    assert facet.all_values[1].selected is False
    facet = qf_res.attr_int.get_facet(324)
    assert len(facet.all_values) == 1
    assert facet.all_values[0].value == 57005
    assert facet.all_values[0].count == 99
    assert facet.all_values[0].count_text == '99'
    assert facet.all_values[0].selected is False


def test_attr_int_facet_filter__single_selected_value(int_qf, compiler):
    params = {'a18': '58084'}
    sq = int_qf.apply(SearchQuery(), params)
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int.filter': agg.Filter(
                Term('attr.int', 0x12_0000e2e4),
                aggs={
                    'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000),
                }
            ),
            'qf.attr_int:18': agg.Terms(Field('attr.int'), size=100),
        })
        .post_filter(Term('attr.int', 0x12_0000e2e4))
        .to_dict(compiler=compiler)
    )
    qf_res = int_qf.process_result(SearchResult(
        {
            'aggregations': {
                'qf.attr_int.filter': {
                    'doc_count': 201,
                    'qf.attr_int': {
                        'buckets': [
                            {
                                'key': 0x144_0000dead,
                                'doc_count': 123,
                            },
                            {
                                'key': 0x12_0000e2e4,
                                'doc_count': 119
                            },
                            {
                                'key': 0x144_0000beef,
                                'doc_count': 1
                            }
                        ]
                    }
                },
                'qf.attr_int:18': {
                    'buckets': [
                        {
                            'key': 0x12_0000e2e4,
                            'doc_count': 99
                        },
                        {
                            'key': 0x12_0000e7e5,
                            'doc_count': 88
                        },
                    ]
                }
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    facet = qf_res.attr_int.get_facet(18)
    assert facet.attr_id == 18
    assert len(facet.all_values) == 2
    assert len(facet.selected_values) == 1
    assert len(facet.values) == 1
    assert facet.all_values[0] is facet.selected_values[0]
    assert facet.all_values[1] is facet.values[0]
    assert facet.all_values[0].value == 58084
    assert facet.all_values[0].count == 99
    assert facet.all_values[0].count_text == '99'
    assert facet.all_values[0].selected is True
    assert facet.all_values[1].value == 59365
    assert facet.all_values[1].count == 88
    assert facet.all_values[1].count_text == '+88'
    assert facet.all_values[1].selected is False

    facet = qf_res.attr_int.get_facet(324)
    assert facet.attr_id == 324
    assert len(facet.all_values) == 2
    assert len(facet.selected_values) == 0
    assert len(facet.values) == 2
    assert facet.all_values[0] is facet.values[0]
    assert facet.all_values[1] is facet.values[1]
    assert facet.all_values[0].value == 57005
    assert facet.all_values[0].count == 123
    assert facet.all_values[0].count_text == '123'
    assert facet.all_values[0].selected is False
    assert facet.all_values[1].value == 48879
    assert facet.all_values[1].count == 1
    assert facet.all_values[1].count_text == '1'
    assert facet.all_values[1].selected is False


def test_attr_int_facet_filter__multiple_selected_values(int_qf, compiler):
    sq = int_qf.apply(
        SearchQuery(),
        {'a18': '58084', 'a324': ['57005', '48879']}
    )
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int.filter': agg.Filter(
                Bool.must(
                    Term('attr.int', 0x12_0000e2e4),
                    Terms('attr.int', [0x144_0000dead, 0x144_0000beef]),
                ),
                aggs={
                    'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000),
                }
            ),
            'qf.attr_int.filter:18': agg.Filter(
                Terms('attr.int', [0x144_0000dead, 0x144_0000beef]),
                aggs={
                    'qf.attr_int:18': agg.Terms(Field('attr.int'), size=100),
                }
            ),
            'qf.attr_int.filter:324': agg.Filter(
                Term('attr.int', 0x12_0000e2e4),
                aggs={
                    'qf.attr_int:324': agg.Terms(Field('attr.int'), size=100),
                }
            )
        })
        .post_filter(Term('attr.int', 0x12_0000e2e4))
        .post_filter(Terms('attr.int', [0x144_0000dead, 0x1440000beef]))
        .to_dict(compiler=compiler)
    )
    qf_res = int_qf.process_result(SearchResult(
        {
            'aggregations': {
                'qf.attr_int.filter': {
                    'doc_count': 404,
                    'qf.attr_int': {
                        'buckets': [
                            {
                                'key': 0x144_0000dead,
                                'doc_count': 1
                            },
                            {
                                'key': 0x12_0000e2e4,
                                'doc_count': 1
                            },
                            {
                                'key': 0x144_0000beef,
                                'doc_count': 1
                            }
                        ]
                    }
                },
                'qf.attr_int.filter:18': {
                    'doc_count': 200,
                    'qf.attr_int:18': {
                        'buckets': [
                            {
                                'key': 0x12_0000e2e4,
                                'doc_count': 99
                            },
                            {
                                'key': 0x12_0000e7e5,
                                'doc_count': 88
                            },
                        ]
                    }
                },
                'qf.attr_int.filter:324': {
                    'doc_count': 200,
                    'qf.attr_int:324': {
                        'buckets': [
                            {
                                'key': 0x144_0000dead,
                                'doc_count': 123
                            },
                            {
                                'key': 0x144_0000beef,
                                'doc_count': 1
                            },
                        ]
                    }
                },
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    facet = qf_res.attr_int.get_facet(18)
    assert facet.attr_id == 18
    assert len(facet.all_values) == 2
    assert len(facet.selected_values) == 1
    assert len(facet.values) == 1
    assert facet.all_values[0].value == 58084
    assert facet.all_values[0].count == 99
    assert facet.all_values[0].count_text == '99'
    assert facet.all_values[0].selected is True
    assert facet.all_values[1].value == 59365
    assert facet.all_values[1].count == 88
    assert facet.all_values[1].count_text == '+88'
    assert facet.all_values[1].selected is False

    facet = qf_res.attr_int.get_facet(324)
    assert facet.attr_id == 324
    assert len(facet.all_values) == 2
    assert len(facet.selected_values) == 2
    assert len(facet.values) == 0
    assert facet.all_values[0].value == 57005
    assert facet.all_values[0].count == 123
    assert facet.all_values[0].count_text == '123'
    assert facet.all_values[0].selected is True
    assert facet.all_values[1].value == 48879
    assert facet.all_values[1].count == 1
    assert facet.all_values[1].count_text == '1'
    assert facet.all_values[1].selected is True


def test_attr_int_facet_filter__include_values(int_qf_with_values, compiler):
    sq = int_qf_with_values.apply(SearchQuery(), {'a18': '58084'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int.filter': agg.Filter(
                Term('attr.int', 0x12_0000e2e4),
                aggs={
                    'qf.attr_int': agg.Terms(
                        Field('attr.int'), size=10_000
                    )
                }
            ),
            'qf.attr_int:18': agg.Terms(
                Field('attr.int'), size=100,
                include=[0x12_0000e2e4, 0x12_0000e7e5]
            )
        })
        .post_filter(Term('attr.int', 0x12_0000e2e4))
        .to_dict(compiler=compiler)
    )


def test_attr_bool_facet_filter__unknown_param(bool_qf, compiler):
    sq = bool_qf.apply(SearchQuery(), {'b18': 'true'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_bool': agg.Terms(Field('attr.bool'), size=100)
        })
        .to_dict(compiler=compiler)
    )


def test_attr_bool_facet_filter__empty_params(bool_qf, compiler):
    sq = bool_qf.apply(SearchQuery(), {})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_bool': agg.Terms(Field('attr.bool'), size=100)
        })
        .to_dict(compiler=compiler)
    )
    qf_res = bool_qf.process_result(SearchResult(
        {
            'aggregations': {
                'qf.attr_bool': {
                    'buckets': [
                        {
                            'key': 0b11,
                            'doc_count': 123,
                        },
                        {
                            'key': 0b10,
                            'doc_count': 99
                        },
                        {
                            'key': 0b101,
                            'doc_count': 1
                        }
                    ]
                }
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    assert len(qf_res.attr_bool.facets) == 2
    facet = qf_res.attr_bool.get_facet(1)
    assert len(facet.all_values) == 2
    assert facet.all_values[0].value is True
    assert facet.all_values[0].count == 123
    assert facet.all_values[0].count_text == '123'
    assert facet.all_values[0].selected is False
    assert facet.all_values[1].value is False
    assert facet.all_values[1].count == 99
    assert facet.all_values[1].count_text == '99'
    assert facet.all_values[1].selected is False
    facet = qf_res.attr_bool.get_facet(2)
    assert len(facet.all_values) == 1
    assert facet.all_values[0].value is True
    assert facet.all_values[0].count == 1
    assert facet.all_values[0].count_text == '1'
    assert facet.all_values[0].selected is False


def test_attr_bool_facet_filter__single_selected_value(bool_qf, compiler):
    sq = bool_qf.apply(SearchQuery(), {
        'a1': 'true',
    })
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_bool.filter': agg.Filter(
                Term('attr.bool', 0b11),
                aggs={
                    'qf.attr_bool': agg.Terms(Field('attr.bool'), size=100)
                }
            ),
            'qf.attr_bool:1': agg.Terms(
                Field('attr.bool'), size=2, include=[0b10, 0b11]
            ),
        })
        .post_filter(Term('attr.bool', 0b11))
        .to_dict(compiler=compiler)
    )
    qf_res = bool_qf.process_result(SearchResult(
        {
            'aggregations': {
                'qf.attr_bool.filter': {
                    'doc_count': 200,
                    'qf.attr_bool': {
                        'buckets': [
                            {
                                'key': 0b11,
                                'doc_count': 123,
                            },
                            {
                                'key': 0b101,
                                'doc_count': 1
                            },
                        ]
                    }
                },
                'qf.attr_bool:1': {
                    'buckets': [
                        {
                            'key': 0b11,
                            'doc_count': 123,
                        },
                        {
                            'key': 0b10,
                            'doc_count': 99
                        },
                    ]
                }
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    assert len(qf_res.attr_bool.facets) == 2
    facet = qf_res.attr_bool.get_facet(1)
    assert len(facet.all_values) == 2
    assert len(facet.selected_values) == 1
    assert len(facet.values) == 1
    assert facet.all_values[0] is facet.selected_values[0]
    assert facet.all_values[0].value is True
    assert facet.all_values[0].count == 123
    assert facet.all_values[0].count_text == '123'
    assert facet.all_values[0].selected is True
    assert facet.all_values[1].value is False
    assert facet.all_values[1].count == 99
    assert facet.all_values[1].count_text == '+99'
    assert facet.all_values[1].selected is False
    facet = qf_res.attr_bool.get_facet(2)
    assert len(facet.all_values) == 1
    assert len(facet.selected_values) == 0
    assert len(facet.values) == 1
    assert facet.all_values[0].value is True
    assert facet.all_values[0].count == 1
    assert facet.all_values[0].count_text == '1'
    assert facet.all_values[0].selected is False


def test_attr_bool_facet_filter__multiple_selected_values(bool_qf, compiler):
    sq = bool_qf.apply(SearchQuery(), {
        'a1': ['true', 'false'],
        'a2': 'true'
    })
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_bool.filter': agg.Filter(
                Bool.must(
                    Terms('attr.bool', [0b11, 0b10]),
                    Term('attr.bool', 0b101),
                ),
                aggs={
                    'qf.attr_bool': agg.Terms(Field('attr.bool'), size=100)
                }
            ),
            'qf.attr_bool.filter:1': agg.Filter(
                Term('attr.bool', 0b101),
                aggs={
                    'qf.attr_bool:1': agg.Terms(
                        Field('attr.bool'), size=2, include=[0b10, 0b11]
                    )
                }
            ),
            'qf.attr_bool.filter:2': agg.Filter(
                Terms('attr.bool', [0b11, 0b10]),
                aggs={
                    'qf.attr_bool:2': agg.Terms(
                        Field('attr.bool'), size=2, include=[0b100, 0b101]
                    )
                }
            ),
        })
        .post_filter(
            Bool.must(
                Terms('attr.bool', [0b11, 0b10]),
                Term('attr.bool', 0b101),
            )
        )
        .to_dict(compiler=compiler)
    )
    qf_res = bool_qf.process_result(SearchResult(
        {
            'aggregations': {
                'qf.attr_bool.filter': {
                    'doc_count': 200,
                    'qf.attr_bool': {
                        'buckets': [
                            {
                                'key': 0b11,
                                'doc_count': 123,
                            },
                            {
                                'key': 0b101,
                                'doc_count': 1
                            },
                        ]
                    }
                },
                'qf.attr_bool.filter:1': {
                    'doc_count': 163,
                    'qf.attr_bool:1': {
                        'buckets': [
                            {
                                'key': 0b11,
                                'doc_count': 123,
                            },
                            {
                                'key': 0b10,
                                'doc_count': 99
                            },
                        ]
                    }
                },
                'qf.attr_bool.filter:2': {
                    'doc_count': 144,
                    'qf.attr_bool:2': {
                        'buckets': [
                            {
                                'key': 0b101,
                                'doc_count': 1
                            },
                        ]
                    }
                },
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    assert len(qf_res.attr_bool.facets) == 2
    facet = qf_res.attr_bool.get_facet(1)
    assert len(facet.all_values) == 2
    assert len(facet.selected_values) == 2
    assert len(facet.values) == 0
    assert facet.all_values[0] is facet.selected_values[0]
    assert facet.all_values[1] is facet.selected_values[1]
    assert facet.all_values[0].value is True
    assert facet.all_values[0].count == 123
    assert facet.all_values[0].count_text == '123'
    assert facet.all_values[0].selected is True
    assert facet.all_values[1].value is False
    assert facet.all_values[1].count == 99
    assert facet.all_values[1].count_text == '99'
    assert facet.all_values[1].selected is True
    facet = qf_res.attr_bool.get_facet(2)
    assert len(facet.all_values) == 1
    assert len(facet.selected_values) == 1
    assert len(facet.values) == 0
    assert facet.all_values[0] is facet.selected_values[0]
    assert facet.all_values[0].value is True
    assert facet.all_values[0].count == 1
    assert facet.all_values[0].count_text == '1'
    assert facet.all_values[0].selected is True


def test_attr_float_facet_filter__empty(range_qf, compiler):
    sq = range_qf.apply(SearchQuery(), {})
    assert_search_query(
        sq,
        SearchQuery().aggs({
            'qf.attr_range': agg.Terms(
                script=Script(
                    'doc[params.field].value >>> 32',
                    lang='painless',
                    params={
                        'field': 'attr.float',
                    }
                ),
                size=100
            ),
        }),
        compiler
    )

    qf_res = range_qf.process_results(SearchResult(
        {
            'aggregations': {
                'qf.attr_range': {
                    'buckets': [
                        {
                            'key': '8',
                            'doc_count': 84
                        },
                        {
                            'key': '439',
                            'doc_count': 28
                        }
                    ]
                }
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    assert qf_res.attr_range.name == 'attr_range'
    assert qf_res.attr_range.alias == 'a'
    f = qf_res.attr_range.get_facet(8)
    assert f.attr_id == 8
    assert f.count == 84
    assert f.selected is False
    f = qf_res.attr_range.get_facet(439)
    assert f.attr_id == 439
    assert f.count == 28
    assert f.selected is False


def test_attr_float_facet_filter__single_selected_filter(range_qf, compiler):
    sq = range_qf.apply(SearchQuery(), {'a8__gte': 2.71})
    assert_search_query(
        sq,
        SearchQuery()
        .aggs({
            'qf.attr_range.filter': agg.Filter(
                Range('attr.float', gte=0x8_402d70a4, lte=0x8_7f800000),
                aggs={
                    'qf.attr_range': agg.Terms(
                        script=Script(
                            'doc[params.field].value >>> 32',
                            lang='painless',
                            params={
                                'field': 'attr.float',
                            }
                        ),
                        size=100
                    )
                }
            ),
            'qf.attr_range:8': agg.Filter(
                Range('attr.float', gte=0x8_00000000, lte=0x8_ffffffff),
            )
        })
        .post_filter(Range('attr.float', gte=0x8_402d70a4, lte=0x8_7f800000)),
        compiler
    )

    qf_res = range_qf.process_results(SearchResult(
        {
            'aggregations': {
                'qf.attr_range.filter': {
                    'doc_count': 32,
                    'qf.attr_range': {
                        'buckets': [
                            {
                                'key': 8,
                                'doc_count': 32
                            },
                            {
                                'key': 439,
                                'doc_count': 18
                            }
                        ]
                    }
                },
                'qf.attr_range:8': {
                    'doc_count': 84
                }
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    assert qf_res.attr_range.name == 'attr_range'
    assert qf_res.attr_range.alias == 'a'
    f = qf_res.attr_range.get_facet(8)
    assert f.attr_id == 8
    assert f.count == 84
    assert f.selected is True
    f = qf_res.attr_range.get_facet(439)
    assert f.attr_id == 439
    assert f.count == 18
    assert f.selected is False


def test_attr_float_facet_filter__multiple_selected_filters(
        range_qf, compiler
):
    sq = range_qf.apply(SearchQuery(), {'a8__gte': 2.71, 'a99__lte': 3.14})
    assert_search_query(
        sq,
        SearchQuery()
        .aggs({
            'qf.attr_range.filter': agg.Filter(
                Bool.must(
                    Range('attr.float', gte=0x8_402d70a4, lte=0x8_7f800000),
                    Bool.should(
                        Range(
                            'attr.float', gte=0x63_00000000, lte=0x63_4048f5c3
                        ),
                        Range(
                            'attr.float', gte=0x63_80000000, lte=0x63_ff800000
                        )
                    )
                ),
                aggs={
                    'qf.attr_range': agg.Terms(
                        script=Script(
                            'doc[params.field].value >>> 32',
                            lang='painless',
                            params={
                                'field': 'attr.float',
                            }
                        ),
                        size=100
                    )
                }
            ),
            'qf.attr_range:8': agg.Filter(
                Bool.must(
                    Bool.should(
                        Range(
                            'attr.float', gte=0x63_00000000, lte=0x63_4048f5c3
                        ),
                        Range(
                            'attr.float', gte=0x63_80000000, lte=0x63_ff800000
                        )
                    ),
                    Range('attr.float', gte=0x8_00000000, lte=0x8_ffffffff)
                )
            ),
            'qf.attr_range:99': agg.Filter(
                Bool.must(
                    Range('attr.float', gte=0x8_402d70a4, lte=0x8_7f800000),
                    Range('attr.float', gte=0x63_00000000, lte=0x63_ffffffff)
                )
            ),
        })
        .post_filter(Range('attr.float', gte=0x8_402d70a4, lte=0x8_7f800000))
        .post_filter(Bool.should(
            Range('attr.float', gte=0x63_00000000, lte=0x63_4048f5c3),
            Range('attr.float', gte=0x63_80000000, lte=0x63_ff800000)
        )),
        compiler
    )

    qf_res = range_qf.process_results(SearchResult(
        {
            'aggregations': {
                'qf.attr_range.filter': {
                    'doc_count': 32,
                    'qf.attr_range': {
                        'buckets': [
                            {
                                'key': 8,
                                'doc_count': 32
                            },
                            {
                                'key': 99,
                                'doc_count': 18
                            }
                        ]
                    }
                },
                'qf.attr_range:8': {
                    'doc_count': 84
                },
                'qf.attr_range:99': {
                    'doc_count': 33
                }
            }
        },
        aggregations=sq.get_context().aggregations
    ))
    assert qf_res.attr_range.name == 'attr_range'
    assert qf_res.attr_range.alias == 'a'
    f = qf_res.attr_range.get_facet(8)
    assert f.attr_id == 8
    assert f.count == 84
    assert f.selected is True
    f = qf_res.attr_range.get_facet(99)
    assert f.attr_id == 99
    assert f.count == 33
    assert f.selected is True
