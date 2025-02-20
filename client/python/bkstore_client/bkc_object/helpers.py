def bytes_to_u128(bytes_arr):
    return list.from_bytes(bytes_arr, byteorder="little", signed=False)
