import abc
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_EXCEPTION
from threading import RLock, Event

from six import iteritems, itervalues

from argus._config import ARGUS_ASYNC_NWORKERS
from argus.exceptions import AsyncArgusException

ABC = abc.ABCMeta("ABC", (object,), {})


def _looping_task(shutdown_flag, fun, *args, **kwargs):
    while not shutdown_flag.is_set():
        try:
            fun(*args, **kwargs)
        except Exception as e:
            logging.exception(f"Task failed {fun}")
            raise e


def _exec_task(fun, *args, **kwargs):
    try:
        fun(*args, **kwargs)
    except Exception as e:
        logging.exception(f"Task failed {fun}")
        raise e


class LazySingletonTasksCoordinator(ABC):
    """
    A Thread-Safe singleton lazily initialized thread pool class (encapsulating concurrent.futures.ThreadPoolExecutor)
    """

    _instance = None
    _SINGLETON_LOCK = RLock()
    _POOL_LOCK = RLock()

    @classmethod
    def is_initialized(cls):
        with cls._POOL_LOCK:
            is_init = cls._instance is not None and cls._instance._pool is not None
        return is_init

    @classmethod
    def get_instance(cls, pool_size=None):
        if cls._instance is not None:
            return cls._instance

        # Lazy init
        with cls._SINGLETON_LOCK:
            if cls._instance is None:
                cls._instance = cls(ARGUS_ASYNC_NWORKERS if pool_size is None else pool_size)
        return cls._instance

    @property
    def _workers_pool(self):
        if self._pool is not None:
            return self._pool

        # lazy init the workers pool
        got_initialized = False
        with type(self)._POOL_LOCK:
            if self._pool is None:
                self._pool = ThreadPoolExecutor(max_workers=self._pool_size, thread_name_prefix="AsyncArgusWorker")
                got_initialized = True

        # Call hooks outside the lock, to minimize time-under-lock
        if got_initialized:
            for hook in self._pool_update_hooks:
                hook(self._pool_size)

        return self._pool

    def __init__(self, pool_size):
        # Only allow creation via get_instance
        if not type(self)._SINGLETON_LOCK._is_owned():
            raise AsyncArgusException(f"{type(self)} is a singleton, can't create a new instance")

        pool_size = int(pool_size)
        if pool_size < 1:
            raise ValueError(f"{type(self)} can't be instantiated with a pool_size of {pool_size}")

        # Enforce the singleton pattern
        with type(self)._SINGLETON_LOCK:
            if type(self)._instance is not None:
                raise AsyncArgusException("LazySingletonTasksCoordinator is a singleton, can't create a new instance")
            self._lock = RLock()
            self._pool = None
            self._pool_size = int(pool_size)
            self._pool_update_hooks = []
            self.alive_tasks = {}
            self.is_shutdown = False

    def reset(self, pool_size=None, timeout=None):
        pool_size = ARGUS_ASYNC_NWORKERS if pool_size is None else int(pool_size)
        with type(self)._POOL_LOCK:
            self.shutdown(timeout=timeout)
            pool_size = max(pool_size, 1)
            self._pool = None
            self._pool_size = pool_size
            self.is_shutdown = False
            # pool will be lazily initialized with pool_size on next request submission

    def stop_all_running_tasks(self):
        with type(self)._POOL_LOCK:
            for fut, ev in (v for v in itervalues(self.alive_tasks) if not v[0].done()):
                if ev:
                    ev.set()
                fut.cancel()

    @staticmethod
    def wait_tasks(futures, timeout=None, return_when=ALL_COMPLETED, raise_exceptions=True):
        running_futures = [fut for fut in futures if not fut.done()]
        done, _ = wait(running_futures, timeout=timeout, return_when=return_when)
        if raise_exceptions:
            [f.result() for f in done if not f.cancelled() and f.exception() is not None]  # raises the exception

    @staticmethod
    def wait_tasks_or_abort(futures, timeout=60, kill_switch_ev=None):
        try:
            LazySingletonTasksCoordinator.wait_tasks(futures, return_when=FIRST_EXCEPTION, raise_exceptions=True)
        except Exception as e:
            if kill_switch_ev is not None:
                # Used when we want to keep both raise the exception and wait for all tasks to finish
                kill_switch_ev.set()
                LazySingletonTasksCoordinator.wait_tasks(
                    futures, return_when=ALL_COMPLETED, raise_exceptions=False, timeout=timeout
                )
            raise e

    def register_update_hook(self, fun):
        with type(self)._POOL_LOCK:
            self._pool_update_hooks.append(fun)

    def submit_task(self, is_looping, fun, *args, **kwargs):
        new_id = uuid.uuid4()
        shutdown_flag = Event() if is_looping else None
        with type(self)._POOL_LOCK:
            if self.is_shutdown:
                raise AsyncArgusException("The worker pool has been shutdown and can no longer accept new requests.")

            if is_looping:
                new_future = self._workers_pool.submit(_looping_task, shutdown_flag, fun, *args, **kwargs)
            else:
                new_future = self._workers_pool.submit(_exec_task, fun, *args, **kwargs)
            self.alive_tasks = {k: v for k, v in iteritems(self.alive_tasks) if not v[0].done()}
            self.alive_tasks[new_id] = (new_future, shutdown_flag)
        return new_id, new_future

    def total_alive_tasks(self):
        with type(self)._POOL_LOCK:
            self.alive_tasks = {k: v for k, v in iteritems(self.alive_tasks) if not v[0].done()}
            total = len(self.alive_tasks)
        return total

    def shutdown(self, timeout=None):
        if self.is_shutdown:
            return
        with type(self)._POOL_LOCK:
            self.is_shutdown = True
        if timeout is not None:
            self.await_termination(timeout=timeout)
        self._workers_pool.shutdown(wait=timeout is not None)

    def await_termination(self, timeout=None):
        with type(self)._POOL_LOCK:
            if not self.is_shutdown:
                raise AsyncArgusException("The workers pool has not been shutdown, please call shutdown() first.")
        LazySingletonTasksCoordinator.wait_tasks(
            [v[0] for v in itervalues(self.alive_tasks)],
            timeout=timeout,
            return_when=ALL_COMPLETED,
            raise_exceptions=False,
        )
        with type(self)._POOL_LOCK:
            self.alive_tasks = {}

    @property
    def actual_pool_size(self):
        return self._workers_pool._max_workers

    @abc.abstractmethod
    def __reduce__(self):
        pass
