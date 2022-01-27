import time
from datetime import datetime as dt

import pytest
from mock import patch, MagicMock
from pandas.util.testing import assert_frame_equal
from pymongo.errors import OperationFailure

from argus.argus import Argus, VERSION_STORE
from argus.exceptions import LibraryNotFoundException, QuotaExceededException
from ..util import get_large_ts


def test_connect_to_Argus_string(mongo_host):
    argus = Argus(mongo_host=mongo_host)
    assert argus.list_libraries() == []
    assert argus.mongo_host == mongo_host


def test_connect_to_Argus_connection(mongo_server, mongo_host):
    argus = Argus(mongo_server.api)
    assert argus.list_libraries() == []
    assert argus.mongo_host == mongo_host


def test_reset_Argus(mongo_host, library_name):
    argus = Argus(mongo_host=mongo_host)
    argus.list_libraries()
    argus.initialize_library(library_name, VERSION_STORE)
    c = argus._conn
    assert argus[library_name]._argus_lib._curr_conn is c
    argus.reset()
    assert c is not argus._conn
    assert len(c.nodes) == 0
    assert argus[library_name]._argus_lib._curr_conn is argus._conn


def test_re_authenticate_on_argus_reset(mongo_host, library_name):
    from collections import namedtuple

    Cred = namedtuple("Cred", "user, password")
    with patch("argus.argus.authenticate") as auth_mock, patch("argus.argus.get_auth") as get_auth_mock:
        auth_mock.return_value = True
        get_auth_mock.return_value = Cred(user="a_username", password="a_passwd")
        argus = Argus(mongo_host=mongo_host)
        argus.initialize_library(library_name, VERSION_STORE)
        vstore = argus[library_name]
        vstore.list_symbols()
        auth_mock.reset_mock()
        argus.reset()
        assert auth_mock.call_count > 0
        auth_mock.reset_mock()
        vstore.list_symbols()
        assert auth_mock.call_count == 0


def test_simple(library):
    sym = "symbol"
    data = get_large_ts(100)

    library.write(sym, data)
    orig = dt.now()
    time.sleep(1)  # Move the timestamp on 1ms
    data2 = get_large_ts(100)
    library.write(sym, data2, prune_previous_version=False)

    # Get the timeseries, it should be the same
    read2 = library.read(sym).data
    assert_frame_equal(read2, data2)

    # Ensure we can get the previous version
    read = library.read(sym, as_of=orig).data
    assert_frame_equal(read, data)


def test_indexes(argus):
    c = argus._conn
    argus.initialize_library("library", VERSION_STORE, segment="month")
    chunk = c.argus.library.index_information()
    index_version = chunk["_id_"][
        "v"
    ]  # Mongo 3.2 has index v1, 3.4 and 3.5 have v2 (3.4 can run in compabitility mode with v1)
    assert chunk == {
        "_id_": {"key": [("_id", 1)], "ns": "argus.library", "v": index_version},
        "symbol_1_parent_1_segment_1": {
            "background": True,
            "key": [("symbol", 1), ("parent", 1), ("segment", 1)],
            "ns": "argus.library",
            "unique": True,
            "v": index_version,
        },
        "symbol_1_sha_1": {
            "background": True,
            "key": [("symbol", 1), ("sha", 1)],
            "ns": "argus.library",
            "unique": True,
            "v": index_version,
        },
        "symbol_hashed": {"background": True, "key": [("symbol", "hashed")], "ns": "argus.library", "v": index_version},
        "symbol_1_sha_1_segment_1": {
            "background": True,
            "key": [("symbol", 1), ("sha", 1), ("segment", 1)],
            "ns": "argus.library",
            "unique": True,
            "v": index_version,
        },
    }
    snapshots = c.argus.library.snapshots.index_information()
    assert snapshots == {
        "_id_": {"key": [("_id", 1)], "ns": "argus.library.snapshots", "v": index_version},
        "name_1": {
            "background": True,
            "key": [("name", 1)],
            "ns": "argus.library.snapshots",
            "unique": True,
            "v": index_version,
        },
    }
    versions = c.argus.library.versions.index_information()
    assert versions == {
        "_id_": {"key": [("_id", 1)], "ns": "argus.library.versions", "v": index_version},
        "symbol_1__id_-1": {
            "background": True,
            "key": [("symbol", 1), ("_id", -1)],
            "ns": "argus.library.versions",
            "v": index_version,
        },
        "symbol_1_version_-1": {
            "background": True,
            "key": [("symbol", 1), ("version", -1)],
            "ns": "argus.library.versions",
            "unique": True,
            "v": index_version,
        },
        "versionstore_idx": {
            "background": True,
            "key": [("symbol", 1), ("version", -1), ("metadata.deleted", 1)],
            "ns": "argus.library.versions",
            "v": index_version,
        },
    }
    version_nums = c.argus.library.version_nums.index_information()
    assert version_nums == {
        "_id_": {"key": [("_id", 1)], "ns": "argus.library.version_nums", "v": index_version},
        "symbol_1": {
            "background": True,
            "key": [("symbol", 1)],
            "ns": "argus.library.version_nums",
            "unique": True,
            "v": index_version,
        },
    }


def test_delete_library(argus, library, library_name):
    mongo = argus._conn
    # create a library2 library too - ensure that this isn't deleted
    argus.initialize_library("user.library2", VERSION_STORE, segment="month")
    library.write("asdf", get_large_ts(1))
    assert "TEST" in mongo.argus_test.list_collection_names()
    assert "TEST.versions" in mongo.argus_test.list_collection_names()
    assert "library2" in mongo.argus_user.list_collection_names()
    assert "library2.versions" in mongo.argus_user.list_collection_names()

    argus.delete_library(library_name)
    assert "TEST" not in mongo.argus_user.list_collection_names()
    assert "TEST.versions" not in mongo.argus_user.list_collection_names()
    with pytest.raises(LibraryNotFoundException):
        argus[library_name]
    with pytest.raises(LibraryNotFoundException):
        argus["argus_{}".format(library_name)]
    assert "library2" in mongo.argus_user.list_collection_names()
    assert "library2.versions" in mongo.argus_user.list_collection_names()


def test_quota(argus, library, library_name):
    thing = list(range(100))
    library._argus_lib.set_quota(10)
    assert argus.get_quota(library_name) == 10
    assert library._argus_lib.get_quota() == 10
    library.write("thing", thing)
    with pytest.raises(QuotaExceededException):
        library.write("ts", thing)
        library.write("ts", thing)
        library.write("ts", thing)
        library.write("ts", thing)
    with pytest.raises(QuotaExceededException):
        argus.check_quota(library_name)


def test_check_quota(argus, library, library_name):
    with patch("argus.argus.logger.info") as info:
        argus.check_quota(library_name)
    assert info.call_count == 1


def test_default_mongo_retry_timout():
    now = time.time()
    with pytest.raises(LibraryNotFoundException):
        Argus("unresolved-host", serverSelectionTimeoutMS=0)["some.lib"]
    assert time.time() - now < 1.0


def test_lib_rename(argus):
    argus.initialize_library("test")
    l = argus["test"]
    l.write("test_data", "abc")
    argus.rename_library("test", "new_name")
    l = argus["new_name"]
    assert l.read("test_data").data == "abc"
    with pytest.raises(LibraryNotFoundException) as e:
        l = argus["test"]
    assert "Library test" in str(e.value)
    assert "test" not in argus.list_libraries()


def test_lib_rename_namespace(argus):
    argus.initialize_library("namespace.test")
    l = argus["namespace.test"]
    l.write("test_data", "abc")

    with pytest.raises(ValueError) as e:
        argus.rename_library("namespace.test", "new_namespace.test")
    assert "Collection can only be renamed in the same database" in str(e.value)

    argus.rename_library("namespace.test", "namespace.newlib")
    l = argus["namespace.newlib"]
    assert l.read("test_data").data == "abc"

    with pytest.raises(LibraryNotFoundException) as e:
        l = argus["namespace.test"]
    assert "Library namespace.test" in str(e.value)
    assert "namespace.test" not in argus.list_libraries()


def test_lib_type(argus):
    argus.initialize_library("test")
    assert argus.get_library_type("test") == VERSION_STORE


def test_library_exists(argus):
    argus.initialize_library("test")
    assert argus.library_exists("test")
    assert not argus.library_exists("nonexistentlib")


def test_library_exists_no_auth(argus):
    argus.initialize_library("test")
    with patch("argus.argus.ArgusLibraryBinding") as AB:
        AB.return_value = MagicMock(
            get_library_type=MagicMock(side_effect=OperationFailure("not authorized on argus to execute command"))
        )
        assert argus.library_exists("test")
        assert AB.return_value.get_library_type.called
        assert not argus.library_exists("nonexistentlib")


def test_list_libraries_cached(argus):
    # default in argus is to cache list_libraries.
    libs = ["test1", "test2"]
    for lib in libs:
        argus.initialize_library(lib)

    # Cached data should have been appended to cache.
    assert sorted(libs) == sorted(argus.list_libraries()) == sorted(argus._list_libraries())

    # Should default to uncached list_libraries if cache is empty.
    with patch("argus.argus.Argus._list_libraries", return_value=libs) as uncached_list_libraries:
        # Empty cache manually.
        argus._conn.meta_db.cache.remove({})
        assert argus._list_libraries_cached() == libs
        uncached_list_libraries.assert_called()

    # Reload cache and check that it has data
    argus.reload_cache()
    assert sorted(argus._cache.get("list_libraries")) == sorted(libs)

    # Should fetch it from cache the second time.
    with patch("argus.argus.Argus._list_libraries", return_value=libs) as uncached_list_libraries:
        assert sorted(argus._list_libraries_cached()) == sorted(libs)
        uncached_list_libraries.assert_not_called()


def test_initialize_library_adds_to_cache(argus):
    libs = ["test1", "test2"]

    for lib in libs:
        argus.initialize_library(lib)

    argus.reload_cache()
    assert argus._list_libraries_cached() == argus._list_libraries()

    # Add another lib
    argus.initialize_library("test3")

    assert sorted(argus._cache.get("list_libraries")) == ["test1", "test2", "test3"]


def test_cache_does_not_return_stale_data(argus):
    libs = ["test1", "test2"]

    for lib in libs:
        argus.initialize_library(lib)

    argus.reload_cache()
    assert argus._list_libraries_cached() == argus._list_libraries()

    time.sleep(0.2)

    # Should call uncached list_libraries if the data is stale according to caller.
    with patch("argus.argus.Argus._list_libraries", return_value=libs) as uncached_list_libraries:
        assert argus._list_libraries_cached(newer_than_secs=0.1) == libs
        uncached_list_libraries.assert_called()


def test_renaming_returns_new_name_in_cache(argus):
    libs = ["test1", "test2"]

    for lib in libs:
        argus.initialize_library(lib)

    assert sorted(argus._list_libraries_cached()) == sorted(argus._list_libraries())

    argus.rename_library("test1", "test3")

    assert sorted(argus._list_libraries_cached()) == sorted(["test2", "test3"])


def test_deleting_library_removes_it_from_cache(argus):
    libs = ["test1", "test2"]

    for lib in libs:
        argus.initialize_library(lib)

    argus.delete_library("test1")

    assert argus._list_libraries_cached() == argus._list_libraries() == argus.list_libraries() == ["test2"]


def test_disable_cache_by_settings(argus):
    lib = "test1"
    argus.initialize_library(lib)

    # Should be enabled by default
    assert argus._list_libraries_cached() == argus._list_libraries()

    argus._cache.set_caching_state(enabled=False)

    # Should not return cached results now.
    with patch("argus.argus.Argus._list_libraries", return_value=[lib]) as uncached_list_libraries:
        with patch("argus.argus.Argus._list_libraries_cached", return_value=[lib]) as cached_list_libraries:
            argus.list_libraries()
            uncached_list_libraries.assert_called()
            cached_list_libraries.assert_not_called()

    argus._cache.set_caching_state(enabled=True)

    # Should used cached data again.
    with patch("argus.argus.Argus._list_libraries", return_value=[lib]) as uncached_list_libraries_e:
        with patch("argus.argus.Argus._list_libraries_cached", return_value=[lib]) as cached_list_libraries_e:
            argus.list_libraries()
            uncached_list_libraries_e.assert_not_called()
            cached_list_libraries_e.assert_called()
