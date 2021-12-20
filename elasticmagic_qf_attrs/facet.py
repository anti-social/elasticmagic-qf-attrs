import typing as t

from elasticmagic import agg
from elasticmagic import Bool
from elasticmagic import Range
from elasticmagic import Script
from elasticmagic import SearchQuery
from elasticmagic.agg import SingleValueMetricsAggResult
from elasticmagic.expression import Expression
from elasticmagic.expression import FieldOperators
from elasticmagic.result import SearchResult

from .facet_result import AttrFacetFilterResult, TMaxValue, TMinValue
from .facet_result import AttrRangeFacet
from .facet_result import AttrRangeFacetFilterResult
from .facet_result import AttrFacetValue
from .simple import AttrBoolSimpleFilter
from .simple import AttrRangeSimpleFilter
from .simple import AttrIntSimpleFilter
from .simple import BaseAttrSimpleFilter
from .simple import Params
from .util import merge_attr_value_bool
from .util import merge_attr_value_int
from .util import split_attr_value_bool
from .util import split_attr_value_int


T = t.TypeVar('T')


RANGE_ATTR_SCRIPT = '''
int attrsLen = doc[params.field].size();
List values = doc[params.field];

int[] attrIds = new int[attrsLen];
for (int i = 0; i < attrsLen; i++) {
    attrIds[i] = (int) (values[i] >>> 32);
}
return attrIds;
'''

RANGE_ATTR_MINMAX_MAP_SCRIPT = '''
for (v in doc[params.field]) {
    String attr_id = (v >>> 32).toString();
    float float_val = Float.intBitsToFloat((int) v);
    float[] min_max = state.get(attr_id);
    if (min_max == null) {
        state[attr_id] = new float[] {float_val, float_val};
        continue;
    }
    if (float_val < min_max[0]){
        min_max[0] = float_val;
    }
    if (float_val > min_max[1]){
        min_max[1] = float_val;
    }
}
'''

RANGE_ATTR_MINMAX_REDUCE_SCRIPT = '''
Map reduced = new HashMap();
for (state in states) {
    for (entry in state.entrySet()) {
        String attr_id = entry.getKey();
        float[] min_max = entry.getValue();
        if (!reduced.containsKey(attr_id)) {
            reduced[attr_id] = min_max;
            continue;
        }
        if (min_max[0] < reduced[attr_id][0]) {
            reduced[attr_id][0] = min_max[0];
        }
        if (min_max[1] > reduced[attr_id][1]) {
            reduced[attr_id][1] = min_max[1];
        }
    }
}
return reduced;
'''

RANGE_ATTR_MINMAX_COMBINE_SCRIPT = 'return state;'


def _parse_attr_id_from_agg_name(agg_name: str) -> t.Optional[int]:
    try:
        return int(agg_name.rpartition(':')[2])
    except ValueError:
        return None


class BaseAttrFacetFilter(BaseAttrSimpleFilter, t.Generic[T]):
    full_agg_size: int
    single_agg_size: int

    _result_cls: t.Type[AttrFacetFilterResult[T]]

    _facet_value_cls: t.Type[AttrFacetValue[T]]

    _attr_id_meta_key: str

    def _split_bucket_key(self, key: int) -> t.Tuple[int, T]:
        raise NotImplementedError  # pragma: no cover

    def _include_attrs_values(
            self, attr_ids: t.Iterable[int]
    ) -> t.Dict[int, t.List[int]]:
        raise NotImplementedError  # pragma: no cover

    def _apply_filter_expression(
            self, search_query: SearchQuery, expr: Expression, attr_id: int
    ) -> None:
        return search_query.post_filter(
            expr,
            meta={
                'tags': {self.name, f'{self.alias}:{attr_id}'},
                self._attr_id_meta_key: attr_id
            }
        )

    @property
    def _agg_name(self) -> str:
        return f'{self.qf._name}.{self.name}'

    @property
    def _filter_agg_name(self) -> str:
        return f'{self.qf._name}.{self.name}.filter'

    def _apply_agg(self, search_query: SearchQuery) -> SearchQuery:
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
        selected_attr_ids = []
        for filt, meta in post_filters:
            if not meta:
                continue
            attr_id = meta.get(self._attr_id_meta_key)
            if attr_id is None:
                continue
            selected_attr_ids.append(attr_id)
        include_attrs_values = self._include_attrs_values(selected_attr_ids)
        for attr_id in selected_attr_ids:
            attr_aggs = {
                f'{self._agg_name}:{attr_id}': agg.Terms(
                    self.field,
                    size=self.single_agg_size,
                    include=include_attrs_values.get(attr_id),
                )
            }
            filters = [
                f for f, m in post_filters
                if m.get(self._attr_id_meta_key) != attr_id
            ]
            if filters:
                aggs[f'{self._filter_agg_name}:{attr_id}'] = agg.Filter(
                    Bool.must(*filters),
                    aggs=attr_aggs
                )
            else:
                aggs.update(attr_aggs)

        return search_query.aggs(aggs)

    def _process_result(
            self, result: SearchResult, params: Params
    ) -> AttrFacetFilterResult[T]:
        facet_result = self._result_cls(self.name, self.alias)

        selected_attr_values = {}
        for selected_attr_id, w in self._iter_attr_values(params):
            selected_attr_values[selected_attr_id] = set(
                self._parse_values(w, 'exact')
            )

        processed_attr_ids = set()
        for agg_name, attr_agg in result.aggregations.items():
            if agg_name.startswith(f'{self._filter_agg_name}:'):
                attr_id = _parse_attr_id_from_agg_name(agg_name)
                if attr_id is not None:
                    agg_name = f'{self._agg_name}:{attr_id}'
                    attr_agg = attr_agg.get_aggregation(agg_name)

            if not agg_name.startswith(f'{self._agg_name}:'):
                continue

            attr_id = _parse_attr_id_from_agg_name(agg_name)
            if attr_id is None:
                continue
            selected_values = selected_attr_values.get(attr_id) or set()
            processed_attr_ids.add(attr_id)
            for bucket in attr_agg.buckets:
                found_attr_id, value_id = self._split_bucket_key(bucket.key)
                if found_attr_id != attr_id:
                    continue
                fv = self._facet_value_cls(
                    value_id,
                    bucket.doc_count,
                    value_id in selected_values,
                    bool(selected_values),
                )
                facet_result.add_attr_value(attr_id, fv)

        main_agg = result.get_aggregation(self._agg_name)
        if main_agg is None:
            main_agg = result.get_aggregation(self._filter_agg_name) \
                .get_aggregation(self._agg_name)
        for bucket in main_agg.buckets:
            attr_id, value_id = self._split_bucket_key(bucket.key)
            if attr_id in processed_attr_ids:
                continue
            fv = self._facet_value_cls(
                value_id, bucket.doc_count, False, False
            )
            facet_result.add_attr_value(attr_id, fv)

        return facet_result


AttrsValuesGetter = t.Callable[[t.Iterable[int]], t.Dict[int, t.List[int]]]


class AttrIntFacetFilter(AttrIntSimpleFilter, BaseAttrFacetFilter[int]):
    _result_cls = AttrFacetFilterResult[int]
    _facet_value_cls = AttrFacetValue[int]

    _attr_id_meta_key = 'int_attr_id'

    def __init__(
            self, name: str, field: FieldOperators,
            alias: t.Optional[str] = None,
            full_agg_size: int = 10_000,
            single_agg_size: int = 100,
            attrs_values_getter: t.Optional[AttrsValuesGetter] = None,
    ):
        super().__init__(name, field, alias=alias)
        self.full_agg_size = full_agg_size
        self.single_agg_size = single_agg_size
        self._attrs_values_getter = attrs_values_getter

    def _split_bucket_key(self, key: int) -> t.Tuple[int, int]:
        return split_attr_value_int(key)

    def _include_attrs_values(
            self, attr_ids: t.Iterable[int]
    ) -> t.Dict[int, t.List[int]]:
        if self._attrs_values_getter is None:
            return {}
        attrs_values = {}
        for attr_id, values in self._attrs_values_getter(attr_ids).items():
            attrs_values[attr_id] = [
                merge_attr_value_int(attr_id, v) for v in values
            ]
        return attrs_values


class AttrBoolFacetFilter(AttrBoolSimpleFilter, BaseAttrFacetFilter[bool]):
    _result_cls = AttrFacetFilterResult[bool]
    _facet_value_cls = AttrFacetValue[bool]

    _attr_id_meta_key = 'bool_attr_id'

    def __init__(
            self, name: str, field: FieldOperators,
            alias: t.Optional[str] = None,
            full_agg_size: int = 100,
            single_agg_size: int = 2,
    ):
        super().__init__(name, field, alias=alias)
        self.full_agg_size = full_agg_size
        self.single_agg_size = single_agg_size

    def _split_bucket_key(self, key: int) -> t.Tuple[int, bool]:
        return split_attr_value_bool(key)

    def _include_attrs_values(
            self, attr_ids: t.Iterable[int]
    ) -> t.Dict[int, t.List[int]]:
        attrs_values = {}
        for attr_id in attr_ids:
            attrs_values[attr_id] = [
                merge_attr_value_bool(attr_id, False),
                merge_attr_value_bool(attr_id, True),
            ]
        return attrs_values


class AttrRangeFacetFilter(AttrRangeSimpleFilter):
    _attr_id_meta_key = 'float_attr_id'

    def __init__(
            self,
            name: str,
            field: FieldOperators,
            alias: t.Optional[str] = None,
            compute_min_max: bool = False,
    ):
        super().__init__(name, field, alias=alias)
        self._compute_min_max = compute_min_max

    def _apply_filter_expression(
            self, search_query: SearchQuery, expr: Expression, attr_id: int
    ) -> None:
        return search_query.post_filter(
            expr,
            meta={
                'tags': {self.name, f'{self.alias}:{attr_id}'},
                self._attr_id_meta_key: attr_id
            }
        )

    def _agg_name(self, attr_id: t.Optional[int] = None) -> str:
        agg_name = f'{self.qf._name}.{self.name}'
        if attr_id is not None:
            return f'{agg_name}:{attr_id}'
        return agg_name

    def _filter_agg_name(self) -> str:
        return f'{self._agg_name()}.filter'

    def _min_max_agg_name(self) -> str:
        return f'{self._agg_name()}.min_max'

    def _filter_min_max_agg_name(self) -> str:
        return f'{self._min_max_agg_name()}.filter'

    def _process_min_max_agg_result(
            self,
            attr_id: int,
            min_max_agg: SingleValueMetricsAggResult,
    ) -> t.Tuple[TMinValue, TMaxValue]:

        min_ = None
        max_ = None

        if (
            self._compute_min_max
            and min_max_agg.value
            and str(attr_id) in min_max_agg.value
        ):
            min_, max_ = min_max_agg.value[str(attr_id)]

        return min_, max_

    def _apply_agg(self, search_query: SearchQuery) -> SearchQuery:
        aggs = {}

        post_filters_with_meta = list(
            search_query.get_context().iter_post_filters_with_meta()
        )
        exclude_tags = {self.qf._name}
        agg_filters = self._get_agg_filters(
            post_filters_with_meta,
            exclude_tags,
        )

        full_terms_agg = agg.Terms(
            script=Script(
                RANGE_ATTR_SCRIPT,
                lang='painless',
                params={
                    'field': self.field,
                }
            ),
            size=100
        )
        if agg_filters:
            aggs[self._filter_agg_name()] = agg.Filter(
                Bool.must(*agg_filters),
                aggs={self._agg_name(): full_terms_agg}
            )
        else:
            aggs[self._agg_name()] = full_terms_agg

        for filt, meta in post_filters_with_meta:
            if not meta:
                continue
            selected_attr_id = meta.get(self._attr_id_meta_key)
            if selected_attr_id is None:
                continue

            filters = [
                f for f, m in post_filters_with_meta
                if m.get(self._attr_id_meta_key) != selected_attr_id
            ]
            filters.append(
                Range(
                    self.field,
                    gte=merge_attr_value_int(selected_attr_id, 0),
                    lte=merge_attr_value_int(selected_attr_id, 0xffff_ffff)
                )
            )
            aggs[self._agg_name(selected_attr_id)] = agg.Filter(
                Bool.must(*filters)
            )

        if self._compute_min_max:
            min_max_agg = agg.ScriptedMetric(
                map_script=RANGE_ATTR_MINMAX_MAP_SCRIPT,
                reduce_script=RANGE_ATTR_MINMAX_REDUCE_SCRIPT,
                combine_script=RANGE_ATTR_MINMAX_COMBINE_SCRIPT,
                params={
                    'field': self.field,
                },
            )
            min_max_filters = [
                f for f, m in post_filters_with_meta
                if m.get(self._attr_id_meta_key) is None
                and not m.get('tags', set()).intersection(exclude_tags)
            ]
            if min_max_filters:
                aggs[self._filter_min_max_agg_name()] = agg.Filter(
                    Bool.must(*min_max_filters),
                    aggs={self._min_max_agg_name(): min_max_agg}
                )
            else:
                aggs[self._min_max_agg_name()] = min_max_agg

        return search_query.aggs(aggs)

    def _process_result(
            self, result: SearchResult, params: t.Dict
    ) -> AttrRangeFacetFilterResult:
        facet_result = AttrRangeFacetFilterResult(self.name, self.alias)

        selected_attr_ids = set()
        for selected_attr_id, w in self._iter_attr_values(params):
            if (
                self._parse_last_value(w, 'gte') is not None or
                self._parse_last_value(w, 'lte') is not None
            ):
                selected_attr_ids.add(selected_attr_id)

        main_agg = result.get_aggregation(self._agg_name())
        if main_agg is None:
            main_agg = result.get_aggregation(self._filter_agg_name()) \
                .get_aggregation(self._agg_name())

        min_max_agg = None
        if self._compute_min_max:
            min_max_agg = result.get_aggregation(self._min_max_agg_name())
            if not min_max_agg:
                min_max_agg = (
                    result
                    .get_aggregation(self._filter_min_max_agg_name())
                    .get_aggregation(self._min_max_agg_name())
                )

        for bucket in main_agg.buckets:
            attr_id = int(bucket.key)
            min_, max_ = self._process_min_max_agg_result(
                attr_id, min_max_agg
            )
            facet_result.add_facet(
                AttrRangeFacet(
                    attr_id=attr_id,
                    count=bucket.doc_count,
                    selected=attr_id in selected_attr_ids,
                    min_=min_,
                    max_=max_,
                )
            )

        for selected_attr_id in selected_attr_ids:
            selected_agg = result.get_aggregation(
                self._agg_name(selected_attr_id)
            )
            min_, max_ = self._process_min_max_agg_result(
                selected_attr_id, min_max_agg
            )
            facet_result.add_facet(
                AttrRangeFacet(
                    attr_id=selected_attr_id,
                    count=selected_agg.doc_count,
                    selected=True,
                    min_=min_,
                    max_=max_,
                )
            )

        return facet_result
