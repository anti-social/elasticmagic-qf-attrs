import struct
import typing


def merge_attr_value_int(attr_id: int, value_id: int) -> int:
    return (attr_id << 32) | (value_id & 0xffff_ffff)


def split_attr_value_int(merged_attr: int) -> typing.Tuple[int, int]:
    return merged_attr >> 32, merged_attr & 0xffff_ffff


def merge_attr_value_bool(attr_id: int, value: bool) -> int:
    return (attr_id << 1) | value


def split_attr_value_bool(merged_attr: int) -> typing.Tuple[int, bool]:
    return merged_attr >> 1, bool(merged_attr & 1)


def merge_attr_value_float(attr_id: int, value: float) -> int:
    return (attr_id << 32) | struct.unpack('=I', struct.pack('=f', value))[0]
