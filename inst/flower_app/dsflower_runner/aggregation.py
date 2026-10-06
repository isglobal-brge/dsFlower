"""Deterministic ordering of already released public contribution contents."""
import hashlib
import struct

import numpy as np


def _frame(value):
    return struct.pack(">Q", len(value)) + value


def arrays_wire(arrays):
    values = list(arrays)
    pieces = [_frame(b"dsflower-public-contribution-v1"), struct.pack(">Q", len(values))]
    for value in values:
        array = np.asarray(value)
        if array.dtype.kind not in "biuf" or not np.isfinite(array).all():
            raise ValueError("released contribution must contain finite numeric arrays")
        dtype = array.dtype.newbyteorder("<")
        array = np.array(array, dtype=dtype, order="C", copy=True)
        if dtype.kind == "f":
            array[array == 0] = 0.0
        pieces.extend((_frame(dtype.str.encode("ascii")),
                       _frame(b"".join(struct.pack(">Q", int(n)) for n in array.shape)),
                       _frame(array.tobytes(order="C"))))
    return b"".join(pieces)


def bytes_key(value):
    wire = bytes(value)
    return hashlib.sha256(wire).digest(), wire


def arrays_key(arrays):
    return bytes_key(arrays_wire(arrays))


def vector_key(vector):
    return arrays_key([vector])


def reply_key(reply):
    return arrays_key(reply.content["arrays"].to_numpy_ndarrays())
