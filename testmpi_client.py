from mpi4py import MPI
import sys
import os

world = MPI.COMM_WORLD
rank = world.Get_rank()
size = world.Get_size()

print(f"Rank {rank}, Size {size}")
print(os.getcwd(), flush=True)

world.Barrier()


import numpy as np

obj_data = np.random.rand(4000, 1200)

import bkstore_client as client

client.init()


# check the first console argument and see if it is --create
if len(sys.argv) > 1 and sys.argv[1] == "--create":
    print(f"=========CREATE+READING=========== \n  Rank {rank} creating object")
    for i in range(100):
        obj_name = f"test_obj_{rank}_{i}"
        obj_ids = client.create_objects(
            obj_name_key=f"name",
            parent_id=None,
            metadata={"name": obj_name, "rank": rank, "size": size},
            data=obj_data,
        )

    print(f"Rank {rank} created object with ID {obj_ids}")
    for i in range(100):
        obj_metadata = client.get_object_metadata(
            f"test_obj_{rank}_{i}", meta_keys=["name", "rank", "size"]
        )
        print(f"Rank {rank} object metadata: {obj_metadata}")
        obj_data = client.get_object_data(
            f"test_obj_{rank}_{i}", region=[slice(0, rank), slice(0, rank)]
        )
        print(f"Rank {rank} read object data: {obj_data}")

    if rank == 0:
        print(f"called force_checkpointing")
        rst = client.force_checkpointing()
        print(f"Rank {rank} force_checkpointing result: {rst}")
else:
    print(f"=========READING=========== \n  Rank {rank} reading object")
    for i in range(100):
        obj_metadata = client.get_object_metadata(
            f"test_obj_{rank}_{i}", meta_keys=["name", "rank", "size"]
        )
        print(f"Rank {rank} object metadata: {obj_metadata}")
        obj_data = client.get_object_data(
            f"test_obj_{rank}_{i}", region=[slice(0, rank), slice(0, rank)]
        )
        print(f"Rank {rank} read object data: {obj_data}")
