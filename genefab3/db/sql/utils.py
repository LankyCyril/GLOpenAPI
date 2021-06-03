from genefab3.common.exceptions import GeneFabConfigurationException
from os import path, access, W_OK, stat, remove
from genefab3.common.exceptions import GeneFabLogger
from hashlib import md5
from datetime import datetime
from filelock import FileLock
from contextlib import contextmanager, closing
from sqlite3 import connect, OperationalError
from genefab3.common.utils import timestamp36
from genefab3.common.exceptions import GeneFabDatabaseException


def check_database_validity(filename, desc):
    """Make sure database exists and can be written to"""
    if filename is None:
        msg = f"SQLite database ({desc!r}) was not specified"
        raise GeneFabConfigurationException(msg)
    else:
        access_warning = f"SQLite file {filename!r} may not be writable"
        if path.exists(filename) and (not access(filename, W_OK)):
            GeneFabLogger.warning(f"{access_warning}: path.access()")
            potential_access_warning = None
        else:
            potential_access_warning = access_warning
        return potential_access_warning


def get_filelock(filename, identifier, locking_tier, potential_access_warning, max_filelock_age_seconds=3600):
    """If `locking_tier`, stage lockfile and activate DEBUG-level logger"""
    if locking_tier:
        directory, name = path.split(filename)
        if identifier is None:
            lockfilename = path.join(directory, f".{name}.lock")
        else:
            id_hash = md5(identifier.encode()).hexdigest()
            lockfilename = path.join(directory, f".{name}.{id_hash}.lock")
        try:
            lockfile_ctime = datetime.fromtimestamp(stat(lockfilename).st_ctime)
        except FileNotFoundError:
            lockfile_ctime = datetime.now()
        except Exception as e:
            msg = f"{lockfilename} is inaccessible"
            raise GeneFabConfigurationException(msg, debug_info=repr(e))
        if (not access(lockfilename, W_OK)) and potential_access_warning:
            GeneFabLogger.warning(f"{lockfilename} may not be writable")
        lock_age_seconds = (datetime.now() - lockfile_ctime).total_seconds()
        if lock_age_seconds > max_filelock_age_seconds:
            try:
                GeneFabLogger.debug(f"Releasing stale lock {lockfilename}")
                remove(lockfilename)
            except FileNotFoundError:
                pass
            except Exception as e:
                msg = f"{lockfilename} is inaccessible"
                raise GeneFabConfigurationException(msg, debug_info=repr(e))
        lock = FileLock(lockfilename)
        log_if_lock = GeneFabLogger.debug
    else:
        @contextmanager
        def nullcontext():
            yield
        lock = nullcontext()
        log_if_lock = lambda *a, **k: None
    return lock, log_if_lock


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


def apply_all_pragmas(filename, execute, timeout, potential_access_warning):
    """Apply all relevant PRAGMAs at once: auto_vacuum, WAL, wal_autocheckpoint, busy_timeout"""
    args = filename, potential_access_warning
    apply_pragma(execute, "auto_vacuum", "1", *args)
    apply_pragma(execute, "journal_mode", "wal", *args)
    apply_pragma(execute, "wal_autocheckpoint", "0", *args)
    apply_pragma(execute, "busy_timeout", str(int(timeout*1000)), *args)


@contextmanager
def SQLTransaction(filename, desc=None, *, identifier=None, locking_tier=False, timeout=600):
    """Preconfigure `filename` if new, allow long timeout (for tasks sent to background), expose connection and execute()"""
    desc, _tid = desc or filename, timestamp36()
    potential_access_warning = check_database_validity(filename, desc)
    lock, log_if_lock = get_filelock(
        filename, identifier, locking_tier, potential_access_warning,
    )
    log_if_lock(f"{desc} @ {_tid}: acquiring lock...")
    with lock:
        log_if_lock(f"{desc} @ {_tid}: acquired lock!")
        try:
            _kw = dict(timeout=timeout, isolation_level=None)
            with closing(connect(filename, **_kw)) as connection:
                log_if_lock(f"{desc} @ {_tid}: begin transaction")
                execute = connection.execute
                apply_all_pragmas(
                    filename, execute, timeout, potential_access_warning,
                )
                execute("BEGIN")
                try:
                    yield connection, execute
                except Exception as e:
                    connection.rollback()
                    msg = "rolling back transaction due to"
                    GeneFabLogger.warning(f"{desc} @ {_tid}: {msg} {e!r}")
                    raise
                else:
                    msg = f"{desc} @ {_tid}: committing transaction"
                    GeneFabLogger.debug(msg)
                    connection.commit()
        except (OSError, FileNotFoundError, OperationalError) as e:
            msg = "Data could not be retrieved"
            raise GeneFabDatabaseException(msg, debug_info=repr(e))
        finally:
            log_if_lock(f"{desc} @ {_tid}: released lock")
            connection.close()


def reraise_operational_error(obj, e):
    """If OperationalError is due to too many columns in request, tell user; otherwise, raise generic error"""
    if "too many columns" in str(e).lower():
        msg = "Too many columns requested"
        sug = "Limit request to fewer than 2000 columns"
        raise GeneFabDatabaseException(msg, suggestion=sug)
    else:
        msg = "Data could not be retrieved"
        try:
            debug_info = [repr(e), obj.query]
        except AttributeError:
            debug_info = repr(e)
        raise GeneFabDatabaseException(msg, debug_info=debug_info)
