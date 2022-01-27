try:
    import cPickle as pickle
except ImportError:
    import pickle
import pytest
import six
from mock import patch, MagicMock, sentinel, create_autospec, Mock, call
from pymongo.errors import OperationFailure, AutoReconnect
from pymongo.mongo_client import MongoClient

from argus.argus import Argus, ArgusLibraryBinding, register_library_type, LIBRARY_TYPES
from argus.auth import Credential
from argus.exceptions import LibraryNotFoundException, ArgusException, QuotaExceededException
from argus._cache import Cache


def test_argus_lazy_init():
    with patch("pymongo.MongoClient", return_value=MagicMock(), autospec=True) as mc, patch(
        "argus.argus.mongo_retry", side_effect=lambda x: x, autospec=True
    ), patch("argus._cache.Cache._is_not_expired", return_value=True), patch(
        "argus.argus.get_auth", autospec=True
    ) as ga:
        store = Argus("cluster")
        assert not mc.called
        # do something to trigger lazy argus init
        store.list_libraries()
        assert mc.called


def test_argus_lazy_init_ssl_true():
    with patch("pymongo.MongoClient", return_value=MagicMock(), autospec=True) as mc, patch(
        "argus.argus.mongo_retry", side_effect=lambda x: x, autospec=True
    ), patch("argus._cache.Cache._is_not_expired", return_value=True), patch(
        "argus.argus.get_auth", autospec=True
    ) as ga:
        store = Argus("cluster", ssl=True)
        assert not mc.called
        # do something to trigger lazy argus init
        store.list_libraries()
        assert mc.called
        assert len(mc.mock_calls) == 1
        assert mc.mock_calls[0] == call(
            connectTimeoutMS=2000,
            host="cluster",
            maxPoolSize=4,
            serverSelectionTimeoutMS=30000,
            socketTimeoutMS=600000,
            ssl=True,
        )


def test_connection_passed_warning_raised():
    with patch("pymongo.MongoClient", return_value=MagicMock(), autospec=True), patch(
        "argus.argus.mongo_retry", side_effect=lambda x: x, autospec=True
    ), patch("argus._cache.Cache._is_not_expired", return_value=True), patch(
        "argus.argus.get_auth", autospec=True
    ), patch(
        "argus.argus.logger"
    ) as lg:
        magic_mock = MagicMock(nodes={("host", "port")})
        store = Argus(magic_mock, ssl=True)
        # Increment _pid to simulate forking the process
        store._pid += 1
        _ = store._conn
        assert lg.mock_calls[0] == call.warn(
            "Forking process. Argus was passed a pymongo connection during init, "
            "the new pymongo connection may have different parameters."
        )


def test_argus_auth():
    with patch("pymongo.MongoClient", return_value=MagicMock(), autospec=True), patch(
        "argus.argus.mongo_retry", autospec=True
    ), patch("argus._cache.Cache._is_not_expired", return_value=True), patch(
        "argus.argus.get_auth", autospec=True
    ) as ga:
        ga.return_value = Credential("db", "admin_user", "admin_pass")
        store = Argus("cluster")
        # do something to trigger lazy argus init
        store.list_libraries()
        ga.assert_called_once_with("cluster", "argus", "admin")
        store._adminDB.authenticate.assert_called_once_with("admin_user", "admin_pass")
        ga.reset_mock()

        # Get a 'missing' library
        with pytest.raises(LibraryNotFoundException):
            with patch("argus.argus.ArgusLibraryBinding.get_library_type", return_value=None, autospec=True):
                ga.return_value = Credential("db", "user", "pass")
                store._conn["argus_jblackburn"].name = "argus_jblackburn"
                store["jblackburn.library"]

        # Creating the library will have attempted to auth against it
        ga.assert_called_once_with("cluster", "argus", "argus_jblackburn")
        store._conn["argus_jblackburn"].authenticate.assert_called_once_with("user", "pass")


def test_argus_auth_custom_app_name():
    with patch("pymongo.MongoClient", return_value=MagicMock(), autospec=True), patch(
        "argus.argus.mongo_retry", autospec=True
    ), patch("argus._cache.Cache._is_not_expired", return_value=True), patch(
        "argus.argus.get_auth", autospec=True
    ) as ga:
        ga.return_value = Credential("db", "admin_user", "admin_pass")
        store = Argus("cluster", app_name=sentinel.app_name)
        # do something to trigger lazy argus init
        store.list_libraries()
        assert ga.call_args_list == [call("cluster", sentinel.app_name, "admin")]
        ga.reset_mock()

        # Get a 'missing' library
        with pytest.raises(LibraryNotFoundException):
            with patch("argus.argus.ArgusLibraryBinding.get_library_type", return_value=None, autospec=True):
                ga.return_value = Credential("db", "user", "pass")
                store._conn["argus_jblackburn"].name = "argus_jblackburn"
                store["jblackburn.library"]

        # Creating the library will have attempted to auth against it
        assert ga.call_args_list == [call("cluster", sentinel.app_name, "argus_jblackburn")]


def test_argus_connect_hostname():
    with patch("pymongo.MongoClient", return_value=MagicMock(), autospec=True) as mc, patch(
        "argus.argus.mongo_retry", autospec=True
    ) as ar, patch("argus._cache.Cache._is_not_expired", return_value=True), patch(
        "argus.argus.get_mongodb_uri", autospec=True
    ) as gmu:
        store = Argus(
            "hostname",
            socketTimeoutMS=sentinel.socket_timeout,
            connectTimeoutMS=sentinel.connect_timeout,
            serverSelectionTimeoutMS=sentinel.select_timeout,
        )
        # do something to trigger lazy argus init
        store.list_libraries()
        mc.assert_called_once_with(
            host=gmu("hostname"),
            maxPoolSize=4,
            socketTimeoutMS=sentinel.socket_timeout,
            connectTimeoutMS=sentinel.connect_timeout,
            serverSelectionTimeoutMS=sentinel.select_timeout,
        )


def test_argus_connect_with_environment_name():
    with patch("pymongo.MongoClient", return_value=MagicMock(), autospec=True) as mc, patch(
        "argus.argus.mongo_retry", autospec=True
    ) as ar, patch("argus.argus.get_auth", autospec=True), patch(
        "argus._cache.Cache._is_not_expired", return_value=True
    ), patch(
        "argus.argus.get_mongodb_uri"
    ) as gmfe:
        store = Argus(
            "live",
            socketTimeoutMS=sentinel.socket_timeout,
            connectTimeoutMS=sentinel.connect_timeout,
            serverSelectionTimeoutMS=sentinel.select_timeout,
        )
        # do something to trigger lazy argus init
        store.list_libraries()
    assert gmfe.call_args_list == [call("live")]
    assert mc.call_args_list == [
        call(
            host=gmfe.return_value,
            maxPoolSize=4,
            socketTimeoutMS=sentinel.socket_timeout,
            connectTimeoutMS=sentinel.connect_timeout,
            serverSelectionTimeoutMS=sentinel.select_timeout,
        )
    ]


@pytest.mark.parametrize(
    ["library", "expected_library", "expected_database"],
    [
        ("library", "library", "argus"),
        ("user.library", "library", "argus_user"),
    ],
)
def test_database_library_specifier(library, expected_library, expected_database):
    mongo = MagicMock()
    with patch("argus.argus.ArgusLibraryBinding._auth"):
        ml = ArgusLibraryBinding(mongo, library)

    assert ml.library == expected_library
    mongo._conn.__getitem__.assert_called_with(expected_database)


def test_argus_repr():
    with patch("pymongo.MongoClient", return_value=MagicMock(), autospec=True):
        with patch("argus.argus.mongo_retry", autospec=True):
            with patch("argus.argus.get_auth", autospec=True) as ga:
                ga.return_value = Credential("db", "admin_user", "admin_pass")
                store = Argus("cluster")
                assert str(store) == repr(store)


def test_lib_repr():
    mongo = MagicMock()
    with patch("argus.argus.ArgusLibraryBinding._auth"):
        ml = ArgusLibraryBinding(mongo, "asdf")
        assert str(ml) == repr(ml)


def test_register_library_type():
    class DummyType:
        pass

    register_library_type("new_dummy_type", DummyType)
    assert LIBRARY_TYPES["new_dummy_type"] == DummyType

    with pytest.raises(ArgusException) as e:
        register_library_type("new_dummy_type", DummyType)
    assert "Library new_dummy_type already registered" in str(e.value)


def test_set_quota():
    m = Mock(spec=ArgusLibraryBinding)
    ArgusLibraryBinding.set_quota(m, 10000)
    m.set_library_metadata.assert_called_once_with("QUOTA", 10000)
    assert m.quota_countdown == 0
    assert m.quota == 10000


def test_get_quota():
    m = Mock(spec=ArgusLibraryBinding)
    m.get_library_metadata.return_value = 42
    assert ArgusLibraryBinding.get_quota(m) == 42
    m.get_library_metadata.assert_called_once_with("QUOTA")


def test_check_quota_Zero():
    self = create_autospec(ArgusLibraryBinding)
    self.get_library_metadata.return_value = 0
    self.quota_countdown = 0
    ArgusLibraryBinding.check_quota(self)


def test_check_quota_None():
    m = Mock(spec=ArgusLibraryBinding)
    m.quota = None
    m.quota_countdown = 0
    m.get_library_metadata.return_value = None
    ArgusLibraryBinding.check_quota(m)
    m.get_library_metadata.assert_called_once_with("QUOTA")
    assert m.quota == 0


def test_check_quota_Zero2():
    m = Mock(spec=ArgusLibraryBinding)
    m.quota = None
    m.quota_countdown = 0
    m.get_library_metadata.return_value = 0
    ArgusLibraryBinding.check_quota(m)
    m.get_library_metadata.assert_called_once_with("QUOTA")
    assert m.quota == 0


def test_check_quota_countdown():
    self = create_autospec(ArgusLibraryBinding)
    self.get_library_metadata.return_value = 10
    self.quota_countdown = 10
    ArgusLibraryBinding.check_quota(self)
    assert self.quota_countdown == 9


def test_check_quota():
    self = create_autospec(ArgusLibraryBinding, database_name="argus_db", library="lib")
    self.argus = create_autospec(Argus)
    self.get_library_metadata.return_value = 1024 * 1024 * 1024
    self.quota_countdown = 0
    self.argus.__getitem__.return_value = Mock(
        stats=Mock(
            return_value={
                "totals": {
                    "size": 900 * 1024 * 1024,
                    "count": 100,
                }
            }
        )
    )
    with patch("argus.argus.logger.warning") as warn:
        ArgusLibraryBinding.check_quota(self)
    self.argus.__getitem__.assert_called_once_with(self.get_name.return_value)
    warn.assert_called_once_with("Mongo Quota: argus_db.lib 0.879 / 1 GB used")
    assert self.quota_countdown == 6


def test_check_quota_90_percent():
    self = create_autospec(ArgusLibraryBinding, database_name="argus_db", library="lib")
    self.argus = create_autospec(Argus)
    self.get_library_metadata.return_value = 1024 * 1024 * 1024
    self.quota_countdown = 0
    self.argus.__getitem__.return_value = Mock(
        stats=Mock(
            return_value={
                "totals": {
                    "size": 0.91 * 1024 * 1024 * 1024,
                    "count": 1000000,
                }
            }
        )
    )
    with patch("argus.argus.logger.warning") as warn:
        ArgusLibraryBinding.check_quota(self)
    self.argus.__getitem__.assert_called_once_with(self.get_name.return_value)
    warn.assert_called_once_with("Mongo Quota: argus_db.lib 0.910 / 1 GB used")


def test_check_quota_info():
    self = create_autospec(ArgusLibraryBinding, database_name="argus_db", library="lib")
    self.argus = create_autospec(Argus)
    self.get_library_metadata.return_value = 1024 * 1024 * 1024
    self.quota_countdown = 0
    self.argus.__getitem__.return_value = Mock(
        stats=Mock(
            return_value={
                "totals": {
                    "size": 1 * 1024 * 1024,
                    "count": 100,
                }
            }
        )
    )
    with patch("argus.argus.logger.info") as info:
        ArgusLibraryBinding.check_quota(self)
    self.argus.__getitem__.assert_called_once_with(self.get_name.return_value)
    info.assert_called_once_with("Mongo Quota: argus_db.lib 0.001 / 1 GB used")
    assert self.quota_countdown == 51153


def test_check_quota_exceeded():
    self = create_autospec(ArgusLibraryBinding, database_name="argus_db", library="lib")
    self.argus = create_autospec(Argus)
    self.get_library_metadata.return_value = 1024 * 1024 * 1024
    self.quota_countdown = 0
    self.argus.__getitem__.return_value = Mock(
        stats=Mock(
            return_value={
                "totals": {
                    "size": 1024 * 1024 * 1024,
                    "count": 100,
                }
            }
        )
    )
    with pytest.raises(QuotaExceededException) as e:
        ArgusLibraryBinding.check_quota(self)
    assert "Quota Exceeded: argus_db.lib 1.000 / 1 GB used" in str(e.value)


def test_initialize_library():
    self = create_autospec(Argus)
    self._conn = create_autospec(MongoClient)
    self._cache = create_autospec(Cache)
    lib = create_autospec(ArgusLibraryBinding)
    lib.database_name = sentinel.db_name
    lib.get_quota.return_value = None
    lib_type = Mock()
    with patch.dict("argus.argus.LIBRARY_TYPES", {sentinel.lib_type: lib_type}), patch(
        "argus.argus.ArgusLibraryBinding", return_value=lib, autospec=True
    ) as ML:
        Argus.initialize_library(self, sentinel.lib_name, sentinel.lib_type, thing=sentinel.thing)
    assert ML.call_args_list == [call(self, sentinel.lib_name)]
    assert ML.return_value.set_library_type.call_args_list == [call(sentinel.lib_type)]
    assert ML.return_value.set_quota.call_args_list == [call(10 * 1024 * 1024 * 1024)]
    assert lib_type.initialize_library.call_args_list == [call(ML.return_value, thing=sentinel.thing)]


def test_initialize_library_too_many_ns():
    self = create_autospec(Argus)
    self._conn = create_autospec(MongoClient)
    self._cache = create_autospec(Cache)
    lib = create_autospec(ArgusLibraryBinding)
    lib.database_name = sentinel.db_name
    self._conn.__getitem__.return_value.list_collection_names.return_value = [x for x in six.moves.range(5001)]
    lib_type = Mock()
    with pytest.raises(ArgusException) as e:
        with patch.dict("argus.argus.LIBRARY_TYPES", {sentinel.lib_type: lib_type}), patch(
            "argus.argus.ArgusLibraryBinding", return_value=lib, autospec=True
        ) as ML:
            Argus.initialize_library(self, sentinel.lib_name, sentinel.lib_type, thing=sentinel.thing)
    assert self._conn.__getitem__.call_args_list == [call(sentinel.db_name), call(sentinel.db_name)]
    assert lib_type.initialize_library.call_count == 0
    assert "Too many namespaces 5001, not creating: sentinel.lib_name" in str(e.value)


def test_initialize_library_with_list_coll_names():
    self = create_autospec(Argus)
    self._conn = create_autospec(MongoClient)
    self._cache = create_autospec(Cache)
    lib = create_autospec(ArgusLibraryBinding)
    lib.database_name = sentinel.db_name
    lib.get_quota.return_value = None
    self._conn.__getitem__.return_value.list_collection_names.return_value = [x for x in six.moves.range(5001)]
    lib_type = Mock()
    with patch.dict("argus.argus.LIBRARY_TYPES", {sentinel.lib_type: lib_type}), patch(
        "argus.argus.ArgusLibraryBinding", return_value=lib, autospec=True
    ) as ML:
        Argus.initialize_library(
            self, sentinel.lib_name, sentinel.lib_type, thing=sentinel.thing, check_library_count=False
        )
    assert ML.call_args_list == [call(self, sentinel.lib_name)]
    assert ML.return_value.set_library_type.call_args_list == [call(sentinel.lib_type)]
    assert ML.return_value.set_quota.call_args_list == [call(10 * 1024 * 1024 * 1024)]
    assert lib_type.initialize_library.call_args_list == [call(ML.return_value, thing=sentinel.thing)]


def test_library_exists():
    self = create_autospec(Argus)
    self.get_library.return_value = "not an exception"
    assert Argus.library_exists(self, "mylib")


def test_library_doesnt_exist():
    self = create_autospec(Argus)
    self.get_library.side_effect = LibraryNotFoundException("not found")
    assert not Argus.library_exists(self, "mylib")


def test_get_library():
    self = create_autospec(Argus)
    self._library_cache = {}
    library_type = Mock()
    register_library_type(sentinel.lib_type, library_type)
    with patch("argus.argus.ArgusLibraryBinding", autospec=True) as ML:
        ML.return_value.get_library_type.return_value = sentinel.lib_type
        library = Argus.get_library(self, sentinel.lib_name)
    del LIBRARY_TYPES[sentinel.lib_type]
    assert ML.call_args_list == [call(self, sentinel.lib_name)]
    assert library_type.call_args_list == [call(ML.return_value)]
    assert library == library_type.return_value


def test_get_library_not_initialized():
    self = create_autospec(Argus, mongo_host=sentinel.host)
    self._library_cache = {}
    with pytest.raises(LibraryNotFoundException) as e, patch("argus.argus.ArgusLibraryBinding", autospec=True) as ML:
        ML.return_value.get_library_type.return_value = None
        Argus.get_library(self, sentinel.lib_name)
    assert f"Library {sentinel.lib_name} was not correctly initialized in {self}." in str(e.value)


def test_get_library_auth_issue():
    self = create_autospec(Argus, mongo_host=sentinel.host)
    self._library_cache = {}
    with pytest.raises(LibraryNotFoundException) as e, patch("argus.argus.ArgusLibraryBinding", autospec=True) as ML:
        ML.return_value.get_library_type.side_effect = OperationFailure(
            "database error: not authorized for query on argus_marketdata.index.ARGUS"
        )
        Argus.get_library(self, sentinel.lib_name)
    assert f"Library {sentinel.lib_name} was not correctly initialized in {self}." in str(e.value)


def test_get_library_not_registered():
    self = create_autospec(Argus)
    self._library_cache = {}
    with pytest.raises(LibraryNotFoundException) as e, patch("argus.argus.ArgusLibraryBinding", autospec=True) as ML:
        ML.return_value.get_library_type.return_value = sentinel.lib_type
        Argus.get_library(self, sentinel.lib_name)
    assert (
        "Couldn't load LibraryType '%s' for '%s' (has the class been registered?)"
        % (sentinel.lib_type, sentinel.lib_name)
    ) in str(e.value)


def test_mongo_host_get_set():
    sentinel.mongo_host = Mock(nodes={("host", "port")})
    with patch("argus._cache.Cache.__init__", autospec=True, return_value=None):
        argus = Argus(sentinel.mongo_host)
        assert argus.mongo_host == "host:port"


def test_argus_set_get_state():
    sentinel.mongo_host = Mock(nodes={("host", "port")})
    with patch("argus._cache.Cache.__init__", autospec=True, return_value=None):
        store = Argus(
            sentinel.mongo_host,
            allow_secondary="allow_secondary",
            app_name="app_name",
            socketTimeoutMS=1234,
            connectTimeoutMS=2345,
            serverSelectionTimeoutMS=3456,
        )
        buff = pickle.dumps(store)
        mnew = pickle.loads(buff)
        assert mnew.mongo_host == "host:port"
        assert mnew._allow_secondary == "allow_secondary"
        assert mnew._application_name == "app_name"
        assert mnew._socket_timeout == 1234
        assert mnew._connect_timeout == 2345
        assert mnew._server_selection_timeout == 3456


def test__conn_auth_issue():
    auth_timeout = [0]

    a = Argus("host:12345")
    sentinel.creds = Mock()

    def flaky_auth(*args, **kwargs):
        if not auth_timeout[0]:
            auth_timeout[0] = 1
            raise AutoReconnect()

    with patch("argus.argus.authenticate", flaky_auth), patch(
        "argus.argus.get_auth", return_value=sentinel.creds
    ), patch("argus._cache.Cache.__init__", autospec=True, return_value=None), patch(
        "argus.decorators._handle_error"
    ) as he:
        a._conn
        assert he.call_count == 1
        assert auth_timeout[0]


def test_reset():
    c = MagicMock()
    with patch("pymongo.MongoClient", return_value=c, autospec=True) as mc, patch(
        "argus._cache.Cache._is_not_expired", return_value=True
    ):
        store = Argus("hostname")
        # do something to trigger lazy argus init
        store.list_libraries()
        store.reset()
        # Doesn't matter how many times we call it:
        store.reset()
        c.close.assert_called_once()


def test_ArgusLibraryBinding_db():
    argus = create_autospec(Argus)
    argus._conn = create_autospec(MongoClient)
    alb = ArgusLibraryBinding(argus, "sentinel.library")
    with patch.object(alb, "_auth") as _auth:
        # connection is cached during __init__
        alb._db
        assert _auth.call_count == 0

        # Change the argus connection
        argus._conn = create_autospec(MongoClient)
        alb._db
        assert _auth.call_count == 1

        # connection is still cached
        alb._db
        assert _auth.call_count == 1
