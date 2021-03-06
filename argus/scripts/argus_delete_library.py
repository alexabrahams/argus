from __future__ import print_function

import logging
import optparse

import pymongo

from .utils import do_db_auth, setup_logging
from ..argus import Argus
from ..hooks import get_mongodb_uri

logger = logging.getLogger(__name__)


def main():
    usage = """usage: %prog [options]

    Deletes the named library from a user's database.

    Example:
        %prog --host=hostname --library=argus_jblackburn.my_library
    """
    setup_logging()

    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--host", default="localhost", help="Hostname, or clustername. Default: localhost")
    parser.add_option("--library", help="The name of the library. e.g. 'argus_jblackburn.lib'")

    (opts, _) = parser.parse_args()

    if not opts.library:
        parser.error("Must specify the full path of the library e.g. argus_jblackburn.lib!")

    print(f"Deleting: {opts.library} on mongo {opts.host}")
    c = pymongo.MongoClient(get_mongodb_uri(opts.host))

    db_name = opts.library[: opts.library.index(".")] if "." in opts.library else None
    do_db_auth(opts.host, c, db_name)
    store = Argus(c)
    store.delete_library(opts.library)

    logger.info(f"Library {opts.library} deleted")


if __name__ == "__main__":
    main()
