from ast import Slice
import bkstore_client as bkc
import numpy as np

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
    arr6 = bkc.times_two(arr5, arr5.dtype)
    print("arr6.shape=", arr6.shape)
    print("arr6:")
    print(arr6)
    obj_ids = bkc.create_objects(
        "name",
        metadata={"name": "container", "type": "container"},
        parent_id=None,
        array_meta_list=[
            {"name": "arr6", "type": "array"},
            {"name": "arr4", "type": "array"},
        ],
        array_data_list=[arr6, arr4],
    )
    print(obj_ids)

    result = bkc.get_object_data(
        obj_ids[0],
        region=[Slice(0, 1), Slice(0, 1), Slice(0, 10, -2)],
        sub_obj_regions=[
            ("arr6", [Slice(0, 1), Slice(0, 1), Slice(0, 10, -2)]),
            ("arr4", [Slice(0, 1), Slice(0, 1), Slice(0, 2, -1)]),
        ],
    )
    print(result)
