import struct


def merge_attr_value_int(attr_id: int, value_id: int) -> int:
    return (attr_id << 32) | (value_id & 0xffff_ffff)


def merge_attr_value_bool(attr_id: int, value: bool) -> int:
    return (attr_id << 1) | value


def merge_attr_value_float(attr_id: int, value: float) -> int:
    return (attr_id << 32) | struct.unpack('=I', struct.pack('=f', value))[0]
