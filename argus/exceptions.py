class ArgusException(Exception):
    pass


class NoDataFoundException(ArgusException):
    pass


class UnhandledDtypeException(ArgusException):
    pass


class LibraryNotFoundException(ArgusException):
    pass


class DuplicateSnapshotException(ArgusException):
    pass


class StoreNotInitializedException(ArgusException):
    pass


class OptimisticLockException(ArgusException):
    pass


class QuotaExceededException(ArgusException):
    pass


class UnsupportedPickleStoreVersion(ArgusException):
    pass


class DataIntegrityException(ArgusException):
    """
    Base class for data integrity issues.
    """

    pass


class ArgusSerializationException(ArgusException):
    pass


class ConcurrentModificationException(DataIntegrityException):
    pass


class UnorderedDataException(DataIntegrityException):
    pass


class OverlappingDataException(DataIntegrityException):
    pass


class AsyncArgusException(ArgusException):
    pass


class RequestDurationException(AsyncArgusException):
    pass
