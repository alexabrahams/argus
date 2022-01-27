import argus.asynchronous as aasync


def test_async_argus():
    print(aasync.ASYNC_ARGUS.total_alive_tasks())
