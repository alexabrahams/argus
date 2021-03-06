from __future__ import print_function

import logging
import optparse

import pymongo

from .utils import do_db_auth, setup_logging
from ..argus import Argus, ArgusLibraryBinding
from ..hooks import get_mongodb_uri

logger = logging.getLogger(__name__)


def prune_versions(lib, symbols, keep_mins):
    logger.info("Fixing snapshot pointers")
    lib._cleanup_orphaned_versions(dry_run=False)
    for symbol in symbols:
        logger.info(f"Pruning {symbol}")
        lib._prune_previous_versions(symbol, keep_mins=keep_mins)


def main():
    usage = """usage: %prog [options]

    Prunes (i.e. deletes) versions of data that are not the most recent, and are older than 10 minutes,
    and are not in use by snapshots. Must be used on a Argus VersionStore library instance.

    Example:
        argus_prune_versions --host=hostname --library=argus_jblackburn.my_library
    """
    setup_logging()

    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--host", default="localhost", help="Hostname, or clustername. Default: localhost")
    parser.add_option("--library", help="The name of the library. e.g. 'argus_jblackburn.library'")
    parser.add_option("--symbols", help="The symbols to prune - comma separated (default all)")
    parser.add_option("--keep-mins", default=10, help="Ensure there's a version at least keep-mins old. Default:10")

    (opts, _) = parser.parse_args()

    if not opts.library:
        parser.error("Must specify the Argus library e.g. argus_jblackburn.library!")
    db_name, _ = ArgusLibraryBinding._parse_db_lib(opts.library)

    print(f"Pruning (old) versions in : {opts.library} on mongo {opts.host}")
    print(f"Keeping all versions <= {opts.keep_mins} mins old")
    c = pymongo.MongoClient(get_mongodb_uri(opts.host))

    if not do_db_auth(opts.host, c, db_name):
        logger.error("Authentication Failed. Exiting.")
        return
    lib = Argus(c)[opts.library]

    if opts.symbols:
        symbols = opts.symbols.split(",")
    else:
        symbols = lib.list_symbols(all_symbols=True)
        logger.info(f"Found {len(symbols)} symbols")

    prune_versions(lib, symbols, opts.keep_mins)
    logger.info("Done")


if __name__ == "__main__":
    main()
