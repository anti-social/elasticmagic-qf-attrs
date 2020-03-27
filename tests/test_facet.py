from elasticmagic import agg
from elasticmagic import Bool, Field, Range, Term, Terms
from elasticmagic import SearchQuery
from elasticmagic.ext.queryfilter import QueryFilter
from elasticmagic.result import SearchResult

from elasticmagic_qf_attrs.facet import AttrBoolFacetFilter
from elasticmagic_qf_attrs.facet import AttrIntFacetFilter

import pytest


@pytest.fixture
def int_qf():
    qf = QueryFilter()
    qf.add_filter(AttrIntFacetFilter('attr_int', Field('attr.int'), alias='a'))
    yield qf


@pytest.fixture
def bool_qf():
    qf = QueryFilter()
    qf.add_filter(
        AttrBoolFacetFilter('attr_bool', Field('attr.bool'), alias='a')
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
    assert facet.all_values[0].selected is False
    assert facet.all_values[1].value == 4026531840
    assert facet.all_values[1].count == 1
    assert facet.all_values[1].selected is False
    facet = qf_res.attr_int.get_facet(324)
    assert len(facet.all_values) == 1
    assert facet.all_values[0].value == 57005
    assert facet.all_values[0].count == 99
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
    assert facet.all_values[0].selected is True
    assert facet.all_values[1].value == 59365
    assert facet.all_values[1].count == 88
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
    assert facet.all_values[0].selected is False
    assert facet.all_values[1].value == 48879
    assert facet.all_values[1].count == 1
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
    assert facet.all_values[0].selected is True
    assert facet.all_values[1].value == 59365
    assert facet.all_values[1].count == 88
    assert facet.all_values[1].selected is False

    facet = qf_res.attr_int.get_facet(324)
    assert facet.attr_id == 324
    assert len(facet.all_values) == 2
    assert len(facet.selected_values) == 2
    assert len(facet.values) == 0
    assert facet.all_values[0].value == 57005
    assert facet.all_values[0].count == 123
    assert facet.all_values[0].selected is True
    assert facet.all_values[1].value == 48879
    assert facet.all_values[1].count == 1
    assert facet.all_values[1].selected is True


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
    assert facet.all_values[0].selected is False
    assert facet.all_values[1].value is False
    assert facet.all_values[1].count == 99
    assert facet.all_values[1].selected is False
    facet = qf_res.attr_bool.get_facet(2)
    assert len(facet.all_values) == 1
    assert facet.all_values[0].value is True
    assert facet.all_values[0].count == 1
    assert facet.all_values[0].selected is False
