import bkstore_client as bkc
import numpy as np


def to_u128_arr(bytes_arr):
    return [
        int.from_bytes(item, byteorder="little", signed=False) for item in bytes_arr
    ]


if __name__ == "__main__":
    bkc.init()
    dim_size = 100
    # dim_size = 10
    rd1 = np.random.rand(dim_size, dim_size, dim_size)
    rd2 = np.random.rand(dim_size, dim_size, dim_size)
    arr4 = bkc.polymorphic_add(rd1, rd2)
    arr5 = bkc.array_slicing(arr4, [slice(0, 1), slice(0, 1), slice(0, 10, -2)])
    arr6 = bkc.times_two(arr5, arr5.dtype)
    result = bkc.create_objects(
        "abc",
        metadata=[{"type": "container"}, {"type": "regular"}],
        array_data=[arr6, arr4],
    )
    print("{}".format(to_u128_arr(result)))
