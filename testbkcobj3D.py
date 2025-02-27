from ast import Slice
import bkstore_client as bkc
import numpy as np
import time

if __name__ == "__main__":
    bkc.init()
    dim_size = 100
    # dim_size = 10
    rd1 = np.random.rand(dim_size, dim_size, dim_size)
    print("rd1.shape=", rd1.shape)
    print("rd1:")
    print(rd1)
    rd2 = np.random.rand(dim_size, dim_size, dim_size)
    print("rd2.shape=", rd2.shape)
    print("rd2:")
    print(rd2)
    arr4 = bkc.polymorphic_add(rd1, rd2)
    print("arr4.shape=", arr4.shape)
    print("arr4:")
    print(arr4)
    arr5 = bkc.array_slicing(arr4, [slice(0, 1), slice(0, 1), slice(0, 10, -2)])
    print("arr5.shape=", arr5.shape)
    print("arr5:")
    print(arr5)
    arr6 = bkc.times_two(arr5)
    print("arr6.shape=", arr6.shape)
    print("arr6:")
    print(arr6)
    obj_ids = bkc.create_objects(
        "name",
        metadata={
            "name": "container",
            "type": "container",
            "keys": ["arr5", "arr4"],
            "ranges": [(1, 100), (100, 200)],
            "part_num": 1,
            "part_size": 100,
        },
        parent_id=None,
        array_meta_list=[
            {
                "name": "arr5",
                "type": "array",
                "shape": arr5.shape,
                "vcount": 100,
                "voffset": 0,
                "vdim": 0,
            },
            {
                "name": "arr4",
                "type": "array",
                "shape": arr4.shape,
                "vcount": 100,
                "voffset": 0,
                "vdim": 0,
            },
        ],
        array_data_list=[arr5, arr4],
    )
    print(obj_ids)

    result = bkc.get_object_metadata(
        obj_ids[0],
        meta_keys=["type", "keys", "ranges"],
        sub_meta_keys=["name", "shape", "vcount", "voffset", "vdim"],
    )
    print(result)

    result = bkc.get_object_metadata(
        obj_ids[0],
        meta_keys=["type", "keys", "ranges"],
        sub_meta_keys={"arr5": ["name", "shape", "vcount", "voffset", "vdim"]},
    )
    print(result)

    result = bkc.get_object_metadata(
        obj_ids[0],
        meta_keys=["type", "keys", "ranges"],
        sub_meta_keys={"arr4": ["name", "shape", "vcount", "voffset", "vdim"]},
    )
    print(result)

    result = bkc.get_object_data(
        obj_ids[0],
        region=None,
        sub_obj_regions=[
            (
                "arr5",
                [slice(0, 1), slice(0, 1), slice(0, 2, -1)],
            ),
            (
                "arr4",
                [slice(0, 1), slice(0, 1), slice(0, 10, -2)],
            ),
        ],
    )
    print(result)

    # Stop for user input from console
    input("Start benchmarking... Press Enter to continue...")

    start_time = time.time()
    num_iters = 1000
    for i in range(num_iters):
        result = bkc.get_object_data(
            obj_ids[0],
            region=None,
            sub_obj_regions=[
                (
                    "arr5",
                    [
                        slice(0, 1),
                        slice(0, 1),
                        slice(0, 2, -1),
                    ],
                ),
                (
                    "arr4",
                    [
                        slice(0, 1),
                        slice(0, 1),
                        slice(0, 10, -2),
                    ],
                ),
            ],
        )
    print(
        "time:{}, throughput:{}".format(
            time.time() - start_time, num_iters / (time.time() - start_time)
        )
    )
    print(result)
