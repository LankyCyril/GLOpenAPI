from genefab3.common.exceptions import GeneFabLogger, GeneFabDatabaseException
from genefab3.common.exceptions import GeneFabConfigurationException
from os import path, access, W_OK, stat, remove, makedirs
from datetime import datetime
from filelock import Timeout as FileLockTimeoutError, FileLock
from glob import iglob
from hashlib import md5
from contextlib import contextmanager, closing
from genefab3.common.utils import timestamp36
from sqlite3 import connect, OperationalError
from threading import Thread
from subprocess import call
from time import sleep


_logd = GeneFabLogger.debug
_logw = GeneFabLogger.warning
_loge = GeneFabLogger.error


def apply_pragma(execute, pragma, value, sqlite_db):
    """Apply PRAGMA, test result, warn if unable to apply"""
    try:
        execute(f"PRAGMA {pragma} = {value}")
        status = (execute(f"PRAGMA {pragma}").fetchone() or [None])[0]
    except (OSError, FileNotFoundError, OperationalError) as e:
        msg = f"Database {sqlite_db!r} may not be writable"
        _logw(msg, exc_info=e)
    else:
        if str(status) != str(value):
            msg = f"Could not set {pragma} = {value} for database {sqlite_db!r}"
            _logw(msg)


def apply_all_pragmas(sqlite_db, execute, timeout):
    """Apply all relevant PRAGMAs at once: auto_vacuum, WAL, wal_autocheckpoint, busy_timeout"""
    apply_pragma(execute, "auto_vacuum", "1", sqlite_db)
    apply_pragma(execute, "journal_mode", "wal", sqlite_db)
    apply_pragma(execute, "wal_autocheckpoint", "0", sqlite_db)
    apply_pragma(execute, "busy_timeout", str(int(timeout*1000)), sqlite_db)


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
            _loge(msg, exc_info=e)
            return
    else:
        if not access(lockfilename, W_OK):
            _logw(f"{lockfilename} may not be writable")
    lock_age_seconds = (datetime.now() - lockfile_ctime).total_seconds()
    if lock_age_seconds > max_filelock_age_seconds:
        try:
            msg = f"{lockfilename} ({lock_age_seconds} seconds old)"
            _logd(f"Clearing stale lock:\n  {msg}")
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
                _loge(msg, exc_info=e)


def fds_exceed(filename, maxcount):
    """Check if number of open file descriptors for `filename` exceeds `maxcount`"""
    from sys import platform
    from tempfile import NamedTemporaryFile
    global fds_exceed # this function bootstraps itself on first call
    if not getattr(fds_exceed, "bootstrapped", None):
        if platform.startswith("linux") and path.isdir("/proc"):
            def _fds_exceed(filename, maxcount):
                realpath, n = path.realpath(filename), 0
                for fd in iglob("/proc/[0-9]*/fd/*"):
                    if path.realpath(fd) == realpath:
                        n += 1
                        if n > maxcount:
                            return True
                else:
                    return False
            with NamedTemporaryFile(mode="w") as tf:
                with open(tf.name):
                    if (not _fds_exceed(tf.name, 1)) or _fds_exceed(tf.name, 2):
                        problem = "test poll of /proc returned unexpected value"
                        msg = f"Cannot set up ReadLock methods: {problem}"
                        raise GeneFabConfigurationException(msg)
                    else:
                        fds_exceed = _fds_exceed
                        fds_exceed.bootstrapped = True
                        return _fds_exceed(filename, maxcount)
        else:
            problem = "/proc not available and/or not a Linux/POSIX system"
            msg = f"Cannot set up read lock methods: {problem}"
            raise GeneFabConfigurationException(msg)


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
                self._lockfilename = path.join(cwd, f"{name}.lock")
            else:
                id_hash = md5(identifier.encode()).hexdigest()
                self._lockfilename = path.join(cwd, f"{name}.{id_hash}.lock")
            clear_lock_if_stale(self._lockfilename, raise_errors=True)
 
    @contextmanager
    def _connect(self, desc, _tid):
        prelude = f"SQLT._connect ({desc} @ {_tid})"
        try:
            _kw = dict(timeout=self.timeout, isolation_level=None)
            with closing(connect(self.sqlite_db, **_kw)) as connection:
                _logd(f"{prelude}: begin transaction")
                execute = connection.execute
                apply_all_pragmas(self.sqlite_db, execute, self.timeout)
                execute("BEGIN")
                try:
                    yield connection, execute
                except Exception as e:
                    connection.rollback()
                    msg = f"{prelude}: transaction failed, rolling back"
                    _logw(msg, exc_info=e)
                    raise
                else:
                    _logd(f"{prelude}: committing transaction")
                    connection.commit()
        except (OSError, FileNotFoundError, OperationalError) as e:
            msg = "Data could not be retrieved"
            raise GeneFabDatabaseException(msg, debug_info=repr(e))
        finally:
            connection.close()
            def _clear_stale_locks():
                for lockfilename in iglob(f"{self.cwd}/*.lock"):
                    clear_lock_if_stale(lockfilename, raise_errors=False)
            Thread(target=_clear_stale_locks).start()
 
    @contextmanager
    def readable(self, desc=None):
        """2PL: lock that is non-exclusive w.r.t. other `SQLT.readable`s, but exclusive w.r.t. `SQLT.writable`"""
        desc, _tid = desc or self.sqlite_db, timestamp36()
        prelude = f"SQLT.readable ({desc} @ {_tid})"
        _logd(f"{prelude}: waiting for write locks to release...")
        with FileLock(self._lockfilename) as lock:
            # grow handle count; prevents new write locks:
            _logd(f"{prelude}: increasing read lock count (++)")
            with open(self._lockfilename):
                # release hard lock immediately; new `WriteLock`s will not be
                # able to lock on, because read handle count is above zero:
                lock.release()
                # finally, initiate SQL connection:
                with self._connect(desc, _tid) as (connection, execute):
                    yield connection, execute
                _logd(f"{prelude}: decreasing read lock count (--)")
 
    @contextmanager
    def writable(self, desc=None, poll_interval=.1):
        """2PL: lock that is exclusive w.r.t. both `SQLT.readable`s and `SQLT.writable`s"""
        desc, _tid = desc or self.sqlite_db, timestamp36()
        prelude = f"SQLT.writable ({desc} @ {_tid})"
        _logd(f"{prelude}: obtaining write lock...")
        # obtain hard lock; prevents all other locks:
        with FileLock(self._lockfilename):
            # wait for old `ReadLock`s to finish:
            _logd(f"{prelude}: write lock obtained!")
            _logd(f"{prelude}: waiting for all read locks to release...")
            while fds_exceed(self._lockfilename, 1):
                sleep(poll_interval)
            # finally, initiate SQL connection:
            with self._connect(desc) as (connection, execute):
                yield connection, execute
                _logd(f"{prelude}: releasing write lock")


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
