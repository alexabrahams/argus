import pytest
from mock import patch, call

try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError
from argus.hosts import get_argus_lib


def test_get_argus_lib_with_known_host():
    with patch("argus.argus.Argus") as Argus:
        get_argus_lib("foo@bar")
        assert Argus.call_args_list == [call("bar")]


def test_get_argus_lib_with_unknown_host():
    with patch("argus.argus.Argus") as Argus:
        with patch("pymongo.MongoClient") as MongoClient:
            get_argus_lib("foo@bar:123")
            assert Argus.call_args_list == [call("bar:123")]


def test_get_argus_connection_strings():
    with patch("argus.argus.Argus") as Argus:
        with patch("pymongo.MongoClient") as MongoClient:
            get_argus_lib("foo@bar")
            get_argus_lib("foo.sheep@bar")
            get_argus_lib("foo.sheep@bar:123")
            get_argus_lib("foo.sheep@127.0.0.1:123")


@pytest.mark.parametrize(["string"], [("donkey",), ("donkey:ride@blackpool",), ("donkey:ride",)])
def test_get_argus_malformed_connection_strings(string):
    with pytest.raises(ValueError):
        get_argus_lib(string)
