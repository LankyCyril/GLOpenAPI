from contextlib import contextmanager, closing
from genefab3.common.exceptions import GeneFabConfigurationException
from os import path, access, W_OK
from sqlite3 import connect, OperationalError
from genefab3.common.logger import GeneFabLogger


@contextmanager
def sql_connection(filename, desc=None, *, none_ok=False, timeout=600):
    """Preconfigure `filename` if new, allow long timeout (for tasks sent to background), expose connection and execute()"""
    if (filename is None) and none_ok:
        yield None, None
    elif filename is None:
        msg = f"SQLite database ({desc!r}) was not specified"
        raise GeneFabConfigurationException(msg)
    else:
        access_warning = f"SQLite database {filename!r} may not be writable"
        if path.exists(filename) and (not access(filename, W_OK)):
            GeneFabLogger().warning(f"{access_warning}: path.access()")
            access_warning = None
        try:
            with closing(connect(filename, timeout=timeout)) as connection:
                cursor, pragma = connection.cursor(), "PRAGMA auto_vacuum"
                try:
                    cursor.execute(f"{pragma} = 1")
                    _avac = (cursor.execute(pragma).fetchone() or [0])[0]
                except (OSError, FileNotFoundError, OperationalError) as e:
                    if access_warning:
                        GeneFabLogger().warning(f"{access_warning}: {e!r}")
                else:
                    if int(_avac) != 1:
                        msg = f"Could not set auto_vacuum = 1 for database"
                        GeneFabLogger().warning(f"{msg} {filename!r}")
                finally:
                    try:
                        yield connection, connection.cursor().execute
                    except:
                        connection.rollback()
                        raise
                    else:
                        connection.commit()
        except (OSError, FileNotFoundError, OperationalError) as e:
            msg = f"Could not connect to SQLite database {filename!r}"
            raise GeneFabConfigurationException(msg, debug_info=repr(e))
