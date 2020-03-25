from elasticmagic import agg
from elasticmagic import Bool

from .simple import AttrIntSimpleFilter


class AttrIntFacetFilter(AttrIntSimpleFilter):
    def __init__(
            self, name, field, alias=None,
            full_agg_size=10_000, single_agg_size=100,
    ):
        super().__init__(name, field, alias=alias)
        self.full_agg_size = full_agg_size
        self.single_agg_size = single_agg_size

    def _apply_filter_expression(self, search_query, expr, attr_id):
        return search_query.post_filter(
            expr,
            meta={
                'tags': {self.name, f'{self.alias}:{attr_id}'},
                'attr_id': attr_id
            }
        )

    @property
    def _agg_name(self):
        return f'{self.qf._name}.{self.name}'

    @property
    def _filter_agg_name(self):
        return f'{self.qf._name}.{self.name}.filter'

    def _apply_agg(self, search_query):
        aggs = {}

        exclude_tags = {self.qf._name}
        filters = self._get_agg_filters(
            search_query.get_context().iter_post_filters_with_meta(),
            exclude_tags
        )

        full_terms_agg = agg.Terms(
            self.field, size=self.full_agg_size
        )
        if filters:
            aggs[self._filter_agg_name] = agg.Filter(
                Bool.must(*filters),
                aggs={self._agg_name: full_terms_agg}
            )
        else:
            aggs[self._agg_name] = full_terms_agg

        post_filters = list(
            search_query.get_context().iter_post_filters_with_meta()
        )
        for filt, tags in post_filters:
            attr_id = tags.get('attr_id')
            if attr_id is None:
                continue
            attr_aggs = {
                f'{self._agg_name}:{attr_id}': agg.Terms(
                    self.field, size=self.single_agg_size
                )
            }
            filters = [
                f for f, m in post_filters if m.get('attr_id') != attr_id
            ]
            if filters:
                aggs[f'{self._filter_agg_name}:{attr_id}'] = agg.Filter(
                    Bool.must(*filters),
                    aggs=attr_aggs
                )
            else:
                aggs.update(attr_aggs)

        return search_query.aggs(aggs)
