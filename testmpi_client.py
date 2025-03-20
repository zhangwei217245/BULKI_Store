from mpi4py import MPI
import sys

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

# check the first console argument and see if it is --create
if len(sys.argv) > 1 and sys.argv[1] == "--create":
    print(f"=========CREATE+READING=========== \n  Rank {rank} creating object")
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
    obj_data = client.get_object_data(
        obj_ids[0], region=[slice(0, rank), slice(0, rank)], sim_data=True
    )
    print(f"Rank {rank} read object data: {obj_data}")

    if rank == 0:
        print(f"called force_checkpointing")
        rst = client.force_checkpointing()
        print(f"Rank {rank} force_checkpointing result: {rst}")
else:
    print(f"=========READING=========== \n  Rank {rank} reading object")
    obj_metadata = client.get_object_metadata(
        f"test_obj_{rank}", meta_keys=["name", "rank", "size"]
    )
    print(f"Rank {rank} object metadata: {obj_metadata}")
    obj_data = client.get_object_data(
        f"test_obj_{rank}", region=[slice(0, rank), slice(0, rank)], sim_data=True
    )
    print(f"Rank {rank} read object data: {obj_data}")
