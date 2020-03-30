from elasticmagic import agg
from elasticmagic import Bool, Field, Range, Term
from elasticmagic import SearchQuery
from elasticmagic.ext.queryfilter import QueryFilter

from elasticmagic_qf_attrs.facet import AttrBoolFacetFilter
from elasticmagic_qf_attrs.facet import AttrIntFacetFilter
from elasticmagic_qf_attrs.simple import AttrFloatSimpleFilter

import pytest


@pytest.fixture
def qf():
    qf = QueryFilter()
    qf.add_filter(
        AttrBoolFacetFilter('attr_bool', Field('attr.bool'), alias='a')
    )
    qf.add_filter(
        AttrIntFacetFilter('attr_int', Field('attr.int'), alias='a')
    )
    qf.add_filter(
        AttrFloatSimpleFilter('attr_float', Field('attr.float'), alias='a')
    )
    yield qf


def test_combined_facet_filters(qf, compiler):
    sq = qf.apply(SearchQuery(), {
        'a1': 'true',
        'a18': '58084',
        'a324': '57005',
        'a8__gte': '2.71',
    })
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_bool.filter': agg.Filter(
                Bool.must(
                    Term('attr.bool', 0b11),
                    Term('attr.int', 0x12_0000e2e4),
                    Term('attr.int', 0x144_0000dead),
                ),
                aggs={
                    'qf.attr_bool': agg.Terms(Field('attr.bool'), size=100),
                }
            ),
            'qf.attr_bool.filter:1': agg.Filter(
                Bool.must(
                    Term('attr.int', 0x12_0000e2e4),
                    Term('attr.int', 0x144_0000dead),
                ),
                aggs={
                    'qf.attr_bool:1': agg.Terms(
                        Field('attr.bool'),
                        size=2,
                        include=[0b10, 0b11],
                    ),
                }
            ),
            'qf.attr_int.filter': agg.Filter(
                Bool.must(
                    Term('attr.bool', 0b11),
                    Term('attr.int', 0x12_0000e2e4),
                    Term('attr.int', 0x144_0000dead),
                ),
                aggs={
                    'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000),
                }
            ),
            'qf.attr_int.filter:18': agg.Filter(
                Bool.must(
                    Term('attr.bool', 0b11),
                    Term('attr.int', 0x144_0000dead),
                ),
                aggs={
                    'qf.attr_int:18': agg.Terms(Field('attr.int'), size=100),
                }
            ),
            'qf.attr_int.filter:324': agg.Filter(
                Bool.must(
                    Term('attr.bool', 0b11),
                    Term('attr.int', 0x12_0000e2e4),
                ),
                aggs={
                    'qf.attr_int:324': agg.Terms(Field('attr.int'), size=100),
                }
            )
        })
        .post_filter(Term('attr.bool', 0b11))
        .post_filter(Term('attr.int', 0x12_0000e2e4))
        .post_filter(Term('attr.int', 0x144_0000dead))
        .filter(Range('attr.float', gte=0x8_402d70a4, lte=0x8_7f800000))
        .to_dict(compiler=compiler)
    )
