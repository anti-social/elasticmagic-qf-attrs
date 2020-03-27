import typing as t

from elasticmagic.ext.queryfilter.queryfilter import BaseFilterResult


T = t.TypeVar('T')


class AttrFacetValue(t.Generic[T]):
    def __init__(self, value: T, count: int, selected: bool):
        self.value = value
        self.count = count
        self.selected = selected

    @property
    def count_text(self) -> str:
        raise NotImplementedError


class AttrFacet(t.Generic[T]):
    def __init__(self, attr_id: int):
        self.attr_id = attr_id
        self.values: t.List[AttrFacetValue[T]] = []
        self.selected_values: t.List[AttrFacetValue[T]] = []
        self.all_values: t.List[AttrFacetValue[T]] = []
        self._values_map: t.Dict[T, AttrFacetValue[T]] = {}

    def add_value(self, facet_value: AttrFacetValue[T]) -> None:
        if facet_value.selected:
            self.selected_values.append(facet_value)
        else:
            self.values.append(facet_value)
        self.all_values.append(facet_value)
        self._values_map[facet_value.value] = facet_value

    def get_value(self, value: T) -> t.Optional[AttrFacetValue[T]]:
        return self._values_map.get(value)


class AttrFacetFilterResult(BaseFilterResult, t.Generic[T]):
    def __init__(self, name: str, alias: str):
        super().__init__(name, alias)
        self.facets: t.Dict[int, AttrFacet[T]] = {}

    def add_attr_value(
            self, attr_id: int, facet_value: AttrFacetValue[T]
    ) -> None:
        facet = self.facets.get(attr_id)
        if facet is None:
            facet = AttrFacet(attr_id)
            self.facets[attr_id] = facet
        facet.add_value(facet_value)

    def get_facet(self, attr_id: int) -> t.Optional[AttrFacet[T]]:
        return self.facets.get(attr_id)
