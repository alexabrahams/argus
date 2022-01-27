from mock import patch, sentinel, call

from argus.scripts.argus_fsck import main
from ...util import run_as_main


def test_main():
    with patch("argus.scripts.argus_fsck.Argus") as Argus, patch(
        "argus.scripts.argus_fsck.get_mongodb_uri"
    ) as get_mongodb_uri, patch("argus.scripts.argus_fsck.do_db_auth") as do_db_auth:
        run_as_main(
            main,
            "--host",
            f"{sentinel.host}:{sentinel.port}",
            "-v",
            "--library",
            "sentinel.library",
            "lib2",
            "-f",
        )
    get_mongodb_uri.assert_called_once_with("sentinel.host:sentinel.port")
    Argus.assert_called_once_with(get_mongodb_uri.return_value)
    assert do_db_auth.call_args_list == [
        call(f"{sentinel.host}:{sentinel.port}", Argus.return_value._conn, "argus_sentinel"),
        call(f"{sentinel.host}:{sentinel.port}", Argus.return_value._conn, "argus"),
    ]
    assert Argus.return_value.__getitem__.return_value._fsck.call_args_list == [
        call(False),
        call(False),
    ]


def test_main_dry_run():
    with patch("argus.scripts.argus_fsck.Argus") as Argus, patch(
        "argus.scripts.argus_fsck.get_mongodb_uri"
    ) as get_mongodb_uri, patch("argus.scripts.argus_fsck.do_db_auth") as do_db_auth:
        run_as_main(
            main,
            "--host",
            f"{sentinel.host}:{sentinel.port}",
            "-v",
            "--library",
            "sentinel.library",
            "sentinel.lib2",
        )
    get_mongodb_uri.assert_called_once_with("sentinel.host:sentinel.port")
    Argus.assert_called_once_with(get_mongodb_uri.return_value)
    assert do_db_auth.call_count == 0
    assert Argus.return_value.__getitem__.return_value._fsck.call_args_list == [
        call(True),
        call(True),
    ]
