import pytest

from argus.tickstore import tickstore
from argus.tickstore import toplevel


def pytest_generate_tests(metafunc):
    if "tickstore_lib" in metafunc.fixturenames:
        metafunc.parametrize("tickstore_lib", ["tickstore"], indirect=True)


@pytest.fixture(scope="function")
def tickstore_lib(argus, request):
    if request.param == "tickstore":
        store = tickstore
    argus.initialize_library("test.tickstore", store.TICK_STORE_TYPE)
    return argus["test.tickstore"]


@pytest.fixture(scope="function")
def toplevel_tickstore(argus):
    argus.initialize_library("test.toplevel_tickstore", toplevel.TICK_STORE_TYPE)
    return argus["test.toplevel_tickstore"]
