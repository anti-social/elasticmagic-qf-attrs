import typing as t

from elasticmagic import agg
from elasticmagic import Bool
from elasticmagic import Field
from elasticmagic import SearchQuery
from elasticmagic.expression import Expression
from elasticmagic.ext.queryfilter.queryfilter import BaseFilterResult
from elasticmagic.result import SearchResult

from .simple import AttrIntSimpleFilter
from .util import split_attr_value_int


class AttrIntFacetFilter(AttrIntSimpleFilter):
    def __init__(
            self, name: str, field: Field, alias: t.Optional[str] = None,
            full_agg_size: int = 10_000, single_agg_size: int = 100,
    ):
        super().__init__(name, field, alias=alias)
        self.full_agg_size = full_agg_size
        self.single_agg_size = single_agg_size

    def _apply_filter_expression(
            self, search_query: SearchQuery, expr: Expression, attr_id: int
    ) -> None:
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

    def _apply_agg(self, search_query: SearchQuery):
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
        for filt, meta in post_filters:
            if not meta:
                continue
            attr_id = meta.get('attr_id')
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

    @staticmethod
    def _parse_attr_id_from_agg_name(agg_name: str) -> t.Optional[int]:
        try:
            return int(agg_name.rpartition(':')[2])
        except ValueError:
            pass

    def _process_result(
        self, result: SearchResult, params: t.Dict
    ) -> 'AttrIntFacetFilterResult':
        facet_result = AttrIntFacetFilterResult(self.name, self.alias)

        selected_attr_values = {}
        for attr_id, w in self._iter_attr_values(params):
            selected_attr_values[attr_id] = set(self._parse_values(w))

        processed_attr_ids = set()
        for agg_name, attr_agg in result.aggregations.items():
            if agg_name.startswith(f'{self._filter_agg_name}:'):
                attr_id = self._parse_attr_id_from_agg_name(agg_name)
                if attr_id is not None:
                    agg_name = f'{self._agg_name}:{attr_id}'
                    attr_agg = attr_agg.get_aggregation(agg_name)

            if not agg_name.startswith(f'{self._agg_name}:'):
                continue

            attr_id = self._parse_attr_id_from_agg_name(agg_name)
            selected_values = selected_attr_values.get(attr_id) or set()
            processed_attr_ids.add(attr_id)
            for bucket in attr_agg.buckets:
                found_attr_id, value_id = split_attr_value_int(bucket.key)
                if found_attr_id != attr_id:
                    continue
                fv = AttrIntFacetValue(
                    value_id,
                    bucket.doc_count,
                    value_id in selected_values
                )
                facet_result.add_attr_value(attr_id, fv)

        main_agg = result.get_aggregation(self._agg_name)
        if main_agg is None:
            main_agg = result.get_aggregation(self._filter_agg_name) \
                .get_aggregation(self._agg_name)
        for bucket in main_agg.buckets:
            attr_id, value_id = split_attr_value_int(bucket.key)
            if attr_id in processed_attr_ids:
                continue
            fv = AttrIntFacetValue(value_id, bucket.doc_count, False)
            facet_result.add_attr_value(attr_id, fv)

        return facet_result


class AttrIntFacetValue:
    def __init__(self, value: int, count: int, selected: bool):
        self.value = value
        self.count = count
        self.selected = selected

    @property
    def count_text(self):
        raise NotImplementedError


class AttrIntFacet:
    def __init__(self, attr_id: int):
        self.attr_id = attr_id
        self.values: t.List[AttrIntFacetValue] = []
        self.selected_values: t.List[AttrIntFacetValue] = []
        self.all_values: t.List[AttrIntFacetValue] = []
        self._values_map: t.Dict[int, AttrIntFacetValue] = {}

    def add_value(self, facet_value: AttrIntFacetValue) -> None:
        if facet_value.selected:
            self.selected_values.append(facet_value)
        else:
            self.values.append(facet_value)
        self.all_values.append(facet_value)
        self._values_map[facet_value.value] = facet_value

    def get_value(self, value: int) -> AttrIntFacetValue:
        return self._values_map.get(value)


class AttrIntFacetFilterResult(BaseFilterResult):
    def __init__(self, name, alias):
        super().__init__(name, alias)
        self.attr_facets: t.Dict[int, AttrIntFacet] = {}

    def add_attr_value(
        self, attr_id: int, facet_value: AttrIntFacetValue
    ) -> None:
        facet = self.attr_facets.get(attr_id)
        if facet is None:
            facet = AttrIntFacet(attr_id)
            self.attr_facets[attr_id] = facet
        facet.add_value(facet_value)

    def get_facet(self, attr_id: int) -> AttrIntFacet:
        return self.attr_facets.get(attr_id)
