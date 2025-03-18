from mpi4py import MPI

world = MPI.COMM_WORLD
rank = world.Get_rank()
size = world.Get_size()

print(f"Rank {rank}, Size {size}")

world.Barrier()

obj_name = f"test_obj_{rank}"

import numpy as np

obj_data = np.random.rand(4, 12)

import bkstore_client as client

client.init()

obj_ids = client.create_objects(
    obj_name_key="name",
    parent_id=None,
    metadata={"name": obj_name, "rank": rank, "size": size},
    data=obj_data,
)

print(f"Rank {rank} created object with ID {obj_ids}")
obj_metadata = client.get_object_metadata(
    obj_ids[0], meta_keys=["name", "rank", "size"]
)
print(f"Rank {rank} object metadata: {obj_metadata}")
obj_data = client.get_object_data(obj_ids[0], region=[slice(0, rank), slice(0, rank)])
print(f"Rank {rank} read object data: {obj_data}")
