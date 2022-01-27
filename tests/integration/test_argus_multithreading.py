import os
import time
from multiprocessing import Process
from random import random

import pytest

from argus import Argus, VERSION_STORE
from argus.exceptions import LibraryNotFoundException
from argus.hooks import register_get_auth_hook
from argus.store.version_store import VersionStore

MY_ARGUS = None  # module-level Argus singleton
AUTH_COUNT = 0


def f(library_name, total_writes, do_reset):
    my_pid = os.getpid()
    data = [str(my_pid)] * 100
    if do_reset:
        global AUTH_COUNT
        AUTH_COUNT = 0
        MY_ARGUS.reset()
        assert AUTH_COUNT > 0
    while True:
        try:
            vstore = MY_ARGUS[library_name]  # wait for parent to initialize
            break
        except LibraryNotFoundException:
            pass
        time.sleep(random() * 0.2)
    for i in range(total_writes):
        if i % 20 == 0:  # add some randomisation, make sure that processes are multiplexed across time
            time.sleep(random())
        key = f"{my_pid}_{i}"
        vstore.write(key, data + [key])
    for i in range(total_writes):
        key = f"{my_pid}_{i}"
        assert vstore.read(key).data == data + [key]


def my_auth_hook(host, app_name, database_name):
    global AUTH_COUNT
    AUTH_COUNT += 1


@pytest.mark.timeout(600)
def test_multiprocessing_safety(mongo_host, library_name):
    # Create/initialize library at the parent process, then spawn children, and start them aligned in time
    total_processes = 64
    total_writes_per_child = 100

    register_get_auth_hook(my_auth_hook)

    global MY_ARGUS
    MY_ARGUS = Argus(mongo_host=mongo_host)

    MY_ARGUS.initialize_library(library_name, VERSION_STORE)
    assert isinstance(MY_ARGUS.get_library(library_name), VersionStore)

    processes = [Process(target=f, args=(library_name, total_writes_per_child, True)) for _ in range(total_processes)]

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    for p in processes:
        assert p.exitcode == 0

    assert isinstance(MY_ARGUS.get_library(library_name), VersionStore)


@pytest.mark.timeout(600)
def test_multiprocessing_safety_parent_children_race(mongo_host, library_name):
    # Create Argus and directly fork/start children (no wait)
    total_iterations = 12
    total_processes = 6
    total_writes_per_child = 20

    global MY_ARGUS

    for i in range(total_iterations):
        processes = list()

        MY_ARGUS = Argus(mongo_host=mongo_host)
        for j in range(total_processes):
            p = Process(target=f, args=(library_name, total_writes_per_child, False))
            p.start()  # start directly, don't wait to create first all children procs
            processes.append(p)

        MY_ARGUS.initialize_library(library_name, VERSION_STORE)  # this will unblock spinning children

        for p in processes:
            p.join()

        for p in processes:
            assert p.exitcode == 0

        MY_ARGUS.reset()

    assert isinstance(MY_ARGUS.get_library(library_name), VersionStore)
