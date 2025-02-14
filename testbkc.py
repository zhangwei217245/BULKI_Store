import bkstore_client as bkc
import numpy as np
import time

if __name__ == "__main__":
    bkc.init()
    shape = 10
    dtype1 = np.float64
    dtype2 = np.int64

    arr1 = np.full(shape, 3.14, dtype1)
    print("arr1:")
    print(arr1)
    arr2 = np.full(shape, 1, dtype2)
    print("arr2:")
    print(arr2)
    arr3 = bkc.polymorphic_add(arr1, arr2)
    print("arr3:")
    print(arr3)

    arr4 = bkc.times_two(arr3)
    print("arr4:")
    print(arr4)

    start_time = time.time()
    for i in range(1000):
        arr4 = bkc.times_two(arr3)
    print("arr4:")
    print(arr4)
    print(
        "time:{}, throughput:{}".format(
            time.time() - start_time, 1000 / (time.time() - start_time)
        )
    )
