import bkstore_client as bkc
import numpy as np

if __name__ == "__main__":
    bkc.init()
    dim_size = 100
    # dim_size = 10
    rd1 = np.random.rand(dim_size, dim_size, dim_size)
    rd2 = np.random.rand(dim_size, dim_size, dim_size)
    arr4 = bkc.polymorphic_add(rd1, rd2)
    arr5 = bkc.array_slicing(arr4, [slice(0, 1), slice(0, 1), slice(0, 10, -2)])
    arr6 = bkc.times_two(arr5, arr5.dtype)
    bkc.create_objects("abc", metadata={"type": "container"}, array_data=[arr6, arr4])
    print("done")
