from argus.argus import Argus
from argus.store.metadata_store import MetadataStore
from argus.store.version_store import VersionStore


def test_argus(argus):
    assert isinstance(argus, Argus)


def test_library(library):
    assert isinstance(library, VersionStore)
    assert library._argus_lib.get_library_type() == "VersionStore"


def test_ms_lib(ms_lib):
    assert isinstance(ms_lib, MetadataStore)
    assert ms_lib._argus_lib.get_library_type() == "MetadataStore"
