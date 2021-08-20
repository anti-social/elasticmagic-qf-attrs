from enum import IntEnum, unique

from elasticmagic_qf_attrs.util import merge_attr_value_bool
from elasticmagic_qf_attrs.util import merge_attr_value_int
from elasticmagic_qf_attrs.util import merge_attr_value_float


def attr(cls):
    for v in cls.Values:
        setattr(cls, v.name, merge_attr_value_int(cls.attr_id, v))
    return cls


@attr
class Manufacturer:
    attr_id = 1

    @unique
    class Values(IntEnum):
        apple = 1
        samsung = 2
        huawei = 3
        xiaomi = 4


@attr
class Country:
    attr_id = 2

    @unique
    class Values(IntEnum):
        china = 1
        usa = 2
        korea = 3


class BoolAttr:
    attr_id = None

    @classmethod
    def yes(cls):
        return merge_attr_value_bool(cls.attr_id, True)

    @classmethod
    def no(cls):
        return merge_attr_value_bool(cls.attr_id, False)


class Waterproof(BoolAttr):
    attr_id = 3


class RangeAttr:
    attr_id = None

    @classmethod
    def value(cls, v):
        return merge_attr_value_float(cls.attr_id, v)


class Display(RangeAttr):
    attr_id = 4


class Battery(RangeAttr):
    attr_id = 5
