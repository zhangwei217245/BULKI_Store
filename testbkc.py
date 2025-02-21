import bkstore_client as bkc
import numpy as np
import time

if __name__ == "__main__":
    bkc.init()
    shape = 10
    dtype1 = np.float64
    dtype2 = np.int64

    arr1 = np.random.rand(100, 100, 100)
    print("arr1.shape=", arr1.shape)
    print(arr1)
    arr2 = np.random.rand(100, 100, 100)
    print("arr2.shape=", arr2.shape)
    print(arr2)

    arr3 = bkc.polymorphic_add(arr1, arr2)
    print("arr3.shape=", arr3.shape)
    print("arr3:")
    print(arr3)

    arr4 = bkc.head(arr3)
    print("arr4.shape=", arr4.shape)
    print("arr4:")
    print(arr4)

    arr5 = bkc.array_slicing(arr3, [slice(0, 2), slice(0, 2), slice(0, 2, -1)])
    print("arr5.shape=", arr5.shape)
    print("arr5:")
    print(arr5)

    start_time = time.time()
    for i in range(1000):
        arr6 = bkc.times_two(arr5, arr5.dtype)
    print("arr6.shape=", arr6.shape)
    print("arr6:")
    print(arr6)
    print(
        "time:{}, throughput:{}".format(
            time.time() - start_time, 1000 / (time.time() - start_time)
        )
    )
