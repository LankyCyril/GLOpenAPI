from contextlib import contextmanager, closing
from genefab3.common.exceptions import GeneFabConfigurationException
from os import path, access, W_OK
from sqlite3 import connect, OperationalError
from genefab3.common.exceptions import GeneFabLogger


def apply_pragma(execute, pragma, value, filename, access_warning):
    """Apply PRAGMA, test result, warn if unable to apply"""
    try:
        execute(f"PRAGMA {pragma} = {value}")
        status = (execute(f"PRAGMA {pragma}").fetchone() or [None])[0]
    except (OSError, FileNotFoundError, OperationalError) as e:
        if access_warning:
            GeneFabLogger(warning=f"{access_warning}: {e!r}")
    else:
        if str(status) != str(value):
            msg = f"Could not set {pragma} = {value} for database"
            GeneFabLogger(warning=f"{msg} {filename!r}")


@contextmanager
def SQLTransaction(filename, desc=None, *, timeout=600):
    """Preconfigure `filename` if new, allow long timeout (for tasks sent to background), expose connection and execute()"""
    if filename is None:
        msg = f"SQLite database ({desc!r}) was not specified"
        raise GeneFabConfigurationException(msg)
    else:
        access_warning = f"SQLite database {filename!r} may not be writable"
        if path.exists(filename) and (not access(filename, W_OK)):
            GeneFabLogger(warning=f"{access_warning}: path.access()")
            access_warning = None
        try:
            with closing(connect(filename, timeout=timeout)) as connection:
                execute = connection.cursor().execute
                args = filename, access_warning
                apply_pragma(execute, "auto_vacuum", "1", *args)
                apply_pragma(execute, "journal_mode", "wal", *args)
                apply_pragma(execute, "wal_checkpoint", "0", *args)
                try:
                    yield connection, execute
                except:
                    connection.rollback()
                    raise
                else:
                    connection.commit()
        except (OSError, FileNotFoundError, OperationalError) as e:
            msg = f"Could not connect to SQLite database {filename!r}"
            raise GeneFabConfigurationException(msg, debug_info=repr(e))
