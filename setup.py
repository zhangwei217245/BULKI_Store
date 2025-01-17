from setuptools import setup
from setuptools_rust import Binding, RustExtension

setup(
    name="PyBulkiStoreClient",
    version="0.1.0",
    rust_extensions=[
        RustExtension("bulkistore_client.bulkistore_client", binding=Binding.PyO3)
    ],
    packages=["PyBulkiStoreClient"],
    zip_safe=False,
    python_requires=">=3.7",
)
