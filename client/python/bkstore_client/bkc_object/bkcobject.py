from typing import List, Any, Union, Tuple, Dict, Optional
import numpy as np
from numpy.typing import ArrayLike


class BKCObject:
    """Wrapper class for BulkiStore Client objects."""

    def __init__(
        self,
        name: str,
        parent_id: int = None,
        metadata: Dict[str, Any] = None,
        array: ArrayLike = None,
    ):
        """Initialize a new BKCObject instance."""
        self.name = name
        self.parent_id = parent_id
        self.metadata = metadata
        self.array = array
        self.obj_id = self.__class__.new(name, parent_id, [array], [metadata])[0].obj_id

    @classmethod
    def new(
        cls,
        name: str,
        parent_id: int = None,
        numpy_arr: List[ArrayLike] = None,
        metadata: List[Dict[str, Any]] = None,
    ) -> List[int]:
        """Create a new BKCObject, or a series of new ones, optionally initialized with numpy array data and metadata.

        Args:
            name: Name of the object
            parent_id: Optional parent object ID
            numpy_arr: Optional list of numpy arrays to initialize the object with
            metadata: Optional list of metadata dictionaries

        Returns:
            List of BKCObject instances
        """
        # Convert arrays to proper Option format
        array_data = None
        if numpy_arr is not None:
            array_data = [arr if arr is not None else None for arr in numpy_arr]

        # Convert metadata to proper Option format
        meta_data = None
        if metadata is not None and array_data is not None:
            meta_data = [meta if meta is not None else None for meta in metadata]

        result = create_objects(
            name, parent_id, metadata=meta_data, array_data=array_data
        )
        return result

    def append_metadata(self, metadata: List[Tuple[str, Any]]) -> bool:
        """Append metadata to the object.

        Args:
            metadata: List of metadata key-value pairs

        Returns:
            bool: True if successful, False otherwise
        """
        if self._obj_id is None:
            raise RuntimeError("Object not initialized")
        # Return rust_append_metadata(self._obj_id, key, value)
        return True

    def get_metadata(self, keys: List[str]) -> List[Any]:
        """Retrieve metadata for given keys.

        Args:
            keys: List of metadata keys to retrieve

        Returns:
            List of metadata values corresponding to the keys
        """
        if self._obj_id is None:
            raise RuntimeError("Object not initialized")
        # Return rust_get_metadata(self._obj_id, keys)
        return []

    def get_region(self, *slices) -> np.ndarray:
        """Get a region of the data using slice notation.

        Args:
            *slices: Slice objects specifying the region

        Returns:
            numpy.ndarray: The requested data region
        """
        if self._obj_id is None:
            raise RuntimeError("Object not initialized")
        # Return rust_get_region(self._obj_id, slices)
        return np.array([])

    def __getitem__(self, key: Union[slice, Tuple[slice, ...]]) -> np.ndarray:
        """Support for Python slice notation.

        Args:
            key: A slice or tuple of slices

        Returns:
            numpy.ndarray: The requested data region
        """
        if isinstance(key, slice):
            return self.get_region(key)
        elif isinstance(key, tuple):
            return self.get_region(*key)
        else:
            raise TypeError(f"Invalid key type: {type(key)}")
