from __future__ import absolute_import

import logging

from ..auth import get_auth, authenticate

logger = logging.getLogger(__name__)


def do_db_auth(host, connection, db_name):
    """
    Attempts to authenticate against the mongo instance.

    Tries:
      - Auth'ing against admin as 'admin' ; credentials: <host>/argus/admin/admin
      - Auth'ing against db_name (which may be None if auth'ing against admin above)

    returns True if authentication succeeded.
    """
    admin_creds = get_auth(host, "admin", "admin")
    user_creds = get_auth(host, "argus", db_name)

    # Attempt to authenticate the connection
    # Try at 'admin level' first as this allows us to enableSharding, which we want
    if admin_creds is None:
        # Get ordinary credentials for authenticating against the DB
        if user_creds is None:
            logger.error(f"You need credentials for db '{db_name}' on '{host}', or admin credentials")
            return False
        if not authenticate(connection[db_name], user_creds.user, user_creds.password):
            logger.error(f"Failed to authenticate to db '{db_name}' on '{host}', using user credentials")
            return False
        return True
    elif not authenticate(connection.admin, admin_creds.user, admin_creds.password):
        logger.error(f"Failed to authenticate to '{host}' as Admin. Giving up.")
        return False
    # Ensure we attempt to auth against the user DB, for non-priviledged users to get access
    authenticate(connection[db_name], user_creds.user, user_creds.password)
    return True


def setup_logging():
    """Logging setup for console scripts"""
    logging.basicConfig(format="%(asctime)s %(message)s", level="INFO")
