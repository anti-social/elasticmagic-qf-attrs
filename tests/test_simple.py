from elasticmagic import Bool, Field, Range, Term, Terms
from elasticmagic import SearchQuery
from elasticmagic.ext.queryfilter import QueryFilter

from elasticmagic_qf_attrs import AttrBoolSimpleFilter
from elasticmagic_qf_attrs import AttrRangeSimpleFilter
from elasticmagic_qf_attrs import AttrIntSimpleFilter


def test_attr_int_simple_filter(compiler):
    qf = QueryFilter()
    qf.add_filter(
        AttrIntSimpleFilter('attr_int', Field('attr.int'), alias='a')
    )

    sq = qf.apply(SearchQuery(), {})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'b18': '224'})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'a18': '1234'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Term('attr.int', 0x12_000004d2))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a18': ['1234', '5678']})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Terms('attr.int', [0x12_000004d2, 0x12_0000162e]))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a18': ['1234', '5678'], 'a324': '90'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Terms('attr.int', [0x12_000004d2, 0x12_0000162e]))
        .filter(Term('attr.int', 0x144_0000005a))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a18': '0x1234'})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'a18-19': '1234'})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'a2147483648': '1'})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'a1': '2147483648'})
    assert sq.to_dict(compiler=compiler) == {}


def test_attr_range_simple_filter(compiler):
    qf = QueryFilter()
    qf.add_filter(
        AttrRangeSimpleFilter('attr_float', Field('attr.float'), alias='a')
    )

    sq = qf.apply(SearchQuery(), {})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'a8__gte': '3.14'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Range('attr.float', gte=0x8_4048f5c3, lte=0x8_7f800000))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a8__gte': '-3.14'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Bool.should(
            Range('attr.float', gte=0x8_80000000, lte=0x8_c048f5c3),
            Range('attr.float', gte=0x8_00000000, lte=0x8_7f800000)
        ))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a8__lte': '-2.71'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Range('attr.float', gte=0x8_c02d70a4, lte=0x8_ff800000))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a8__gte': ['1', '2.71'], 'a8__lte': '3.14'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Range('attr.float', gte=0x8_402d70a4, lte=0x8_4048f5c3))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a8__gte': '-3.14', 'a8__lte': '-2.71'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Range('attr.float', gte=0x8_c02d70a4, lte=0x8_c048f5c3))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a8__gte': '-3.14', 'a8__lte': '3.14'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Bool.should(
            Range('attr.float', gte=0x8_80000000, lte=0x8_c048f5c3),
            Range('attr.float', gte=0x8_00000000, lte=0x8_4048f5c3)
        ))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a8__gte': '3.14', 'a8__lte': '-3.14'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Bool.must(
            Range('attr.float', gte=0x8_4048f5c3, lte=0x8_7f800000),
            Range('attr.float', gte=0x8_c048f5c3, lte=0x8_ff800000)
        ))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(
        SearchQuery(),
        {'a8__gte': '2.71', 'a8__lte': '3.14', 'a99__lte': '99'}
    )
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Range('attr.float', gte=0x8_402d70a4, lte=0x8_4048f5c3))
        .filter(Bool.should(
            Range('attr.float', gte=0x63_00000000, lte=0x63_42c60000),
            Range('attr.float', gte=0x63_80000000, lte=0x63_ff800000)
        ))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a99.9__gte': '99.9'})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'a99__gte': '100ee2'})
    assert sq.to_dict(compiler=compiler) == {}


def test_attr_bool_simple_filter(compiler):
    qf = QueryFilter()
    qf.add_filter(
        AttrBoolSimpleFilter('attr_bool', Field('attr.bool'), alias='a')
    )

    sq = qf.apply(SearchQuery(), {})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'a1': 'true'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Term('attr.bool', 0x3))
        .to_dict(compiler=compiler)
    )

    # FIXME
    # sq = qf.apply(SearchQuery(), {'a1': [True]})
    # assert sq.to_dict(compiler=compiler) == (
    #     SearchQuery()
    #     .filter(Term('attr.bool', 0x3))
    #     .to_dict(compiler=compiler)
    # )

    sq = qf.apply(SearchQuery(), {'a1': ['true', 'false']})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Terms('attr.bool', [0x3, 0x2]))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a1': ['true', 'false'], 'a2': 'false'})
    assert sq.to_dict(compiler=compiler) == (
        SearchQuery()
        .filter(Terms('attr.bool', [0x3, 0x2]))
        .filter(Term('attr.bool', 0x4))
        .to_dict(compiler=compiler)
    )

    sq = qf.apply(SearchQuery(), {'a2147483648': '1'})
    assert sq.to_dict(compiler=compiler) == {}

    sq = qf.apply(SearchQuery(), {'a1': 'True'})
    assert sq.to_dict(compiler=compiler) == {}
