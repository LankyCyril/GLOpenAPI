from os import path, access, W_OK
from genefab3.common.exceptions import GeneFabLogger
from contextlib import contextmanager, closing
from filelock import FileLock
from genefab3.common.exceptions import GeneFabConfigurationException
from sqlite3 import connect, OperationalError
from genefab3.common.utils import timestamp36
from genefab3.common.exceptions import GeneFabDatabaseException


def check_database_validity(filename, desc):
    """Make sure database exists and can be written to"""
    if filename is None:
        msg = f"SQLite database ({desc!r}) was not specified"
        raise GeneFabConfigurationException(msg)
    else:
        access_warning = f"SQLite database {filename!r} may not be writable"
        if path.exists(filename) and (not access(filename, W_OK)):
            GeneFabLogger.warning(f"{access_warning}: path.access()")
            potential_access_warning = None
        else:
            potential_access_warning = access_warning
        return potential_access_warning


def apply_pragma(execute, pragma, value, filename, potential_access_warning):
    """Apply PRAGMA, test result, warn if unable to apply"""
    try:
        execute(f"PRAGMA {pragma} = {value}")
        status = (execute(f"PRAGMA {pragma}").fetchone() or [None])[0]
    except (OSError, FileNotFoundError, OperationalError) as e:
        if potential_access_warning:
            GeneFabLogger.warning(f"{potential_access_warning}", exc_info=e)
    else:
        if str(status) != str(value):
            msg = f"Could not set {pragma} = {value} for database"
            GeneFabLogger.warning(f"{msg} {filename!r}")


@contextmanager
def nullcontext():
    yield


@contextmanager
def SQLTransaction(filename, desc=None, *, locking_tier=False, timeout=600):
    """Preconfigure `filename` if new, allow long timeout (for tasks sent to background), expose connection and execute()"""
    desc, _tid = desc or filename, timestamp36()
    potential_access_warning = check_database_validity(filename, desc)
    if locking_tier:
        GeneFabLogger.debug(f"{desc} @ {_tid}: acquiring lock...")
        lock = FileLock(f"{filename}.lock")
    else:
        lock = nullcontext()
    with lock:
        if locking_tier:
            GeneFabLogger.debug(f"{desc} @ {_tid}: acquired lock!")
        try:
            with closing(connect(filename, timeout=timeout)) as connection:
                GeneFabLogger.debug(f"{desc} @ {_tid}: begin transaction")
                execute = connection.cursor().execute
                args = filename, potential_access_warning
                busy_timeout = str(timeout*1000)
                apply_pragma(execute, "auto_vacuum", "1", *args)
                apply_pragma(execute, "journal_mode", "wal", *args)
                apply_pragma(execute, "wal_autocheckpoint", "0", *args)
                apply_pragma(execute, "busy_timeout", busy_timeout, *args)
                try:
                    yield connection, execute
                except Exception as e:
                    connection.rollback()
                    msg = "rolling back transaction due to"
                    GeneFabLogger.warning(f"{desc} @ {_tid}: {msg} {e!r}")
                    raise
                else:
                    msg = "committing transaction"
                    GeneFabLogger.debug(f"{desc} @ {_tid}: {msg}")
                    connection.commit()
        except (OSError, FileNotFoundError, OperationalError) as e:
            msg = "Data could not be retrieved"
            raise GeneFabDatabaseException(msg, debug_info=repr(e))
        finally:
            if locking_tier:
                GeneFabLogger.debug(f"{desc} @ {_tid}: released lock")
