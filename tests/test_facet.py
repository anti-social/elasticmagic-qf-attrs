from elasticmagic import agg
from elasticmagic import Bool, Field, Term, Terms
from elasticmagic import SearchQuery
from elasticmagic.ext.queryfilter import QueryFilter

from elasticmagic_qf_attrs.facet import AttrIntFacetFilter


def test_attr_int_simple_filter(compiler):
    qf = QueryFilter()
    qf.add_filter(AttrIntFacetFilter('attr_int', Field('attr.int'), alias='a'))

    sq = qf.apply(SearchQuery(), {})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000)
        })
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'b18': '224'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000)
        })
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a18': '1234'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .aggs({
            'qf.attr_int.filter': agg.Filter(
                Term('attr.int', 0x12_000004d2),
                aggs={
                    'qf.attr_int': agg.Terms(Field('attr.int'), size=10_000),
                }
            ),
            'qf.attr_int:18': agg.Terms(Field('attr.int'), size=100),
        })
        .post_filter(Term('attr.int', 0x12_000004d2))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a18': '58084', 'a324': ['57005', '48879']})
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
