from genefab3.common.exceptions import GeneFabConfigurationException
from os import path, access, W_OK, stat, remove, makedirs
from genefab3.common.exceptions import GeneFabLogger, GeneFabDatabaseException
from datetime import datetime
from filelock import Timeout as FileLockTimeoutError, FileLock
from glob import iglob
from hashlib import md5
from contextlib import contextmanager, closing
from genefab3.common.utils import timestamp36
from sqlite3 import connect, OperationalError
from threading import Thread


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


def clear_lock_if_stale(lockfilename, max_filelock_age_seconds=7200, raise_errors=True):
    """If lockfile has not been accessed in `max_filelock_age_seconds`, assume junk and remove"""
    try:
        lockfile_ctime = datetime.fromtimestamp(stat(lockfilename).st_ctime)
    except FileNotFoundError:
        lockfile_ctime = datetime.now()
    except Exception as e:
        msg = f"{lockfilename} is inaccessible"
        if raise_errors:
            raise GeneFabConfigurationException(msg, debug_info=repr(e))
        else:
            GeneFabLogger.error(msg, exc_info=e)
            return
    else:
        if not access(lockfilename, W_OK):
            GeneFabLogger.warning(f"{lockfilename} may not be writable")
    lock_age_seconds = (datetime.now() - lockfile_ctime).total_seconds()
    if lock_age_seconds > max_filelock_age_seconds:
        try:
            msg = f"{lockfilename} ({lock_age_seconds} seconds old)"
            GeneFabLogger.debug(f"Clearing stale lock:\n  {msg}")
            try: # intercept if possible, prevent other instances stealing lock
                with FileLock(lockfilename, timeout=1e-10):
                    remove(lockfilename)
            except FileLockTimeoutError: # it is junked (locked and abandoned)
                remove(lockfilename)
        except FileNotFoundError:
            pass
        except Exception as e:
            msg = f"{lockfilename} is inaccessible"
            if raise_errors:
                raise GeneFabConfigurationException(msg, debug_info=repr(e))
            else:
                GeneFabLogger.error(msg, exc_info=e)


def clear_stale_locks(filename, raise_errors=False):
    """Clear abandoned (junked) lockfiles that are in the same directory as `filename`"""
    directory, _ = path.split(filename)
    for lockfilename in iglob(f"{directory}/.*.lock"):
        clear_lock_if_stale(lockfilename, raise_errors=raise_errors)


def get_filelock(filename, identifier, locking_tier):
    """If `locking_tier`, stage lockfile and activate DEBUG-level logger"""
    if locking_tier:
        directory, name = path.split(filename)
        if identifier is None:
            lockfilename = path.join(directory, f".{name}.lock")
        else:
            id_hash = md5(identifier.encode()).hexdigest()
            lockfilename = path.join(directory, f".{name}.{id_hash}.lock")
        clear_lock_if_stale(lockfilename, raise_errors=True)
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


def apply_all_pragmas(filename, execute, timeout, potential_access_warning=None):
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
    lock, log_if_lock = get_filelock(filename, identifier, locking_tier)
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
            if locking_tier:
                Thread(target=lambda: clear_stale_locks(filename)).start()


class SQLT():
 
    def __init__(self, sqlite_db, timeout=600, identifier=None, cwd="/tmp/genefab3", max_filelock_age_seconds=7200):
        try:
            makedirs(cwd, exist_ok=True)
        except OSError:
            raise GeneFabConfigurationException(f"{cwd} is not writable")
        if sqlite_db is None:
            raise GeneFabConfigurationException("`sqlite_db` cannot be None")
        elif call(["touch", path.join(cwd, ".check")]) != 0:
            raise GeneFabConfigurationException(f"{cwd} is not writable")
        else:
            self.sqlite_db, self.timeout = sqlite_db, timeout
            self.max_filelock_age_seconds = max_filelock_age_seconds
            self.cwd, self.identifier = cwd, identifier
            _, name = path.split(sqlite_db)
            if identifier is None:
                self.prefix = path.join(cwd, name)
            else:
                id_hash = md5(identifier.encode()).hexdigest()
                self.prefix = path.join(cwd, f"{name}.{id_hash}")
 
    @contextmanager
    def lock(self, postfix, *, exclusive):
        lockfilename = f"{self.prefix}.{postfix}.lock"
        clear_lock_if_stale(
            lockfilename, raise_errors=True,
            max_filelock_age_seconds=self.max_filelock_age_seconds,
        )
        if exclusive:
            with FileLock(lockfilename) as lock:
                yield lock
        else:
            watch = True
            def _keep_acquiring(timeout=.05):
                while watch:
                    try:
                        with FileLock(lockfilename, timeout=timeout):
                            pass
                    except FileLockTimeoutError:
                        pass
            watcher = Thread(target=_keep_acquiring)
            watcher.start()
            yield
            watch = False
 
    @contextmanager
    def _connect(self, desc=None):
        desc, _tid = desc or self.sqlite_db, timestamp36()
        try:
            _kw = dict(timeout=self.timeout, isolation_level=None)
            with closing(connect(self.sqlite_db, **_kw)) as connection:
                GeneFabLogger.debug(f"{desc} @ {_tid}: begin transaction")
                execute = connection.execute
                apply_all_pragmas(self.sqlite_db, execute, self.timeout)
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
            #log_if_lock(f"{desc} @ {_tid}: released lock")
            connection.close()
            #if locking_tier:
            #    Thread(target=lambda: clear_stale_locks(filename)).start()
 
    @contextmanager
    def readable(self, desc=None):
        read_lock = self.lock("r", exclusive=False)
        write_lock = self.lock("w", exclusive=True, release_immediately=True)
        with read_lock: # prevents NEW self.writable from locking on
            with write_lock: # waits for EXISTING self.writable to finish
                with self.connect(desc) as (connection, execute):
                    yield connection, execute
 
    @contextmanager
    def writable(self, desc=None):
        read_lock = self.lock("r", exclusive=True)
        write_lock = self.lock("w", exclusive=True)
        with read_lock: # waits for EXISTING self.readable to finish
            with write_lock: # prevents all other operations from locking on
                with self.connect(desc) as (connection, execute):
                    yield connection, execute


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
