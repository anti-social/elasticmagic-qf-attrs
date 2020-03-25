from elasticmagic import Bool, Range, Term, Terms
from elasticmagic import types

from elasticmagic.ext.queryfilter.queryfilter import BaseFilter

from elasticmagic.ext.queryfilter.codec import BoolCodec
from elasticmagic.ext.queryfilter.codec import FloatCodec
from elasticmagic.ext.queryfilter.codec import IntCodec

from .util import merge_attr_value_bool
from .util import merge_attr_value_float
from .util import merge_attr_value_int


bool_codec = BoolCodec()
float_codec = FloatCodec()
int_codec = IntCodec()


class AttrSimpleFilter(BaseFilter):
    def __init__(self, name, field, alias=None):
        super().__init__(name, alias=alias)
        self.field = field

    def _iter_attr_values(self, params):
        for k, w in params.items():
            if not k.startswith(self.alias):
                continue
            try:
                attr_id = int_codec.decode(k[1:], es_type=types.Integer)
            except ValueError:
                continue
            yield (attr_id, w)

    def _apply_filter(self, search_query, params):
        for attr_id, w in self._iter_attr_values(params):
            expr = self._get_filter_expression(attr_id, w)
            if not expr:
                continue
            search_query = self._apply_filter_expression(
                search_query, expr, attr_id
            )
        return search_query

    def _apply_filter_expression(self, search_query, expr, attr_id):
        return search_query.filter(expr)

    def _get_filter_expression(self, attr_id, values):
        raise NotImplementedError  # pragma: no cover


class AttrIntSimpleFilter(AttrSimpleFilter):
    def _get_filter_expression(self, attr_id, values):
        w = []
        for v in values.get('exact', []):
            try:
                value_id = int_codec.decode(v, es_type=types.Integer)
            except ValueError:
                continue
            w.append(merge_attr_value_int(attr_id, value_id))
        if not w:
            return None
        if len(w) == 1:
            return Term(self.field, w[0])
        return Terms(self.field, w)


#
#             -Inf                 +0.0
#    0x{attr_id}_ff800000 0x{attr_id}_00000000
#                       | |
#                    *********
#                 **           **
#                *               *
#    negative   *                 *    positive
#    floats     *                 *    floats
#             ⤹ *               * ⤸
#                 **           **
#                    *********
#                       | |
#    0x{attr_id}_80000000 0x{attr_id}_7f800000
#             -0.0                 +Inf
#
class AttrFloatSimpleFilter(AttrSimpleFilter):
    @staticmethod
    def _convert_last_value(values):
        if not values:
            return None
        try:
            return float_codec.decode(values[-1])
        except ValueError:
            return None

    @staticmethod
    def _plus_zero(attr_id):
        return merge_attr_value_float(attr_id, 0.0)

    @staticmethod
    def _minus_zero(attr_id):
        return merge_attr_value_float(attr_id, -0.0)

    @staticmethod
    def _plus_inf(attr_id):
        return merge_attr_value_float(attr_id, float('inf'))

    @staticmethod
    def _minus_inf(attr_id):
        return merge_attr_value_float(attr_id, float('-inf'))

    def _get_filter_expression(self, attr_id, values):
        gte = self._convert_last_value(values.get('gte', []))
        gte_value = None
        if gte is not None:
            gte_value = merge_attr_value_float(attr_id, gte)

        lte = self._convert_last_value(values.get('lte', []))
        lte_value = None
        if lte is not None:
            lte_value = merge_attr_value_float(attr_id, lte)

        if gte is not None and lte is not None:
            if gte >= 0.0 and lte >= 0.0:
                return Range(self.field, gte=gte_value, lte=lte_value)
            elif gte < 0.0 and lte < 0.0:
                return Range(self.field, gte=lte_value, lte=gte_value)
            elif gte < 0.0 and lte >= 0:
                return Bool.should(
                    Range(
                        self.field,
                        gte=self._minus_zero(attr_id), lte=gte_value
                    ),
                    Range(
                        self.field, gte=self._plus_zero(attr_id), lte=lte_value
                    ),
                )
            else:
                return Bool.must(
                    Range(
                        self.field, gte=gte_value, lte=self._plus_inf(attr_id)
                    ),
                    Range(
                        self.field, gte=lte_value, lte=self._minus_inf(attr_id)
                    ),
                )

        if gte is not None:
            if gte >= 0.0:
                return Range(
                    self.field, gte=gte_value, lte=self._plus_inf(attr_id)
                )
            else:
                return Bool.should(
                    Range(
                        self.field,
                        gte=self._minus_zero(attr_id), lte=gte_value
                    ),
                    Range(
                        self.field,
                        gte=self._plus_zero(attr_id),
                        lte=self._plus_inf(attr_id)
                    ),
                )

        if lte is not None:
            if lte < 0.0:
                return Range(
                    self.field, gte=lte_value, lte=self._minus_inf(attr_id)
                )
            else:
                return Bool.should(
                    Range(
                        self.field, gte=self._plus_zero(attr_id), lte=lte_value
                    ),
                    Range(
                        self.field,
                        gte=self._minus_zero(attr_id),
                        lte=self._minus_inf(attr_id)
                    ),
                )


class AttrBoolSimpleFilter(AttrSimpleFilter):
    def _get_filter_expression(self, attr_id, values):
        w = []
        for v in values.get('exact', []):
            try:
                value = bool_codec.decode(v)
            except ValueError:
                continue
            w.append(merge_attr_value_bool(attr_id, value))
        if not w:
            return None
        if len(w) == 1:
            return Term(self.field, w[0])
        return Terms(self.field, w)
