from glopenapi.common.exceptions import GLOpenAPILogger
from glopenapi.common.exceptions import GLOpenAPIDatabaseException
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
from functools import partial
from os import path, access, W_OK, stat, remove, makedirs
from datetime import datetime
from filelock import Timeout as FileLockTimeoutError, FileLock
from glob import iglob
from hashlib import sha1
from contextlib import contextmanager, closing
from glopenapi.common.utils import timestamp36
from sqlite3 import connect, OperationalError
from threading import Thread
from subprocess import call
from time import sleep


_logd = GLOpenAPILogger.debug
_logw = GLOpenAPILogger.warning
_loge = GLOpenAPILogger.error


def validate_no_special_character(identifier, desc, c):
    """Pass through `identifier` if contains no `c`, raise GLOpenAPIConfigurationException otherwise"""
    if (not isinstance(identifier, str)) or (c not in identifier):
        return identifier
    else:
        msg = f"{repr(c)} in {desc} name"
        raise GLOpenAPIConfigurationException(msg, **{desc: identifier})
validate_no_backtick = partial(validate_no_special_character, c="`")
validate_no_doublequote = partial(validate_no_special_character, c='"')


def apply_pragma(execute, pragma, value, sqlite_db):
    """Apply PRAGMA, test result, warn if unable to apply"""
    try:
        execute(f"PRAGMA {pragma} = {value}")
        status = (execute(f"PRAGMA {pragma}").fetchone() or [None])[0]
    except (OSError, FileNotFoundError, OperationalError) as exc:
        msg = f"Database {sqlite_db!r} may not be writable"
        _logw(msg, exc_info=exc)
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
    except Exception as exc:
        msg = f"{lockfilename} is inaccessible"
        if raise_errors:
            raise GLOpenAPIConfigurationException(msg, debug_info=repr(exc))
        else:
            _loge(msg, exc_info=exc)
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
        except Exception as exc:
            msg = f"{lockfilename} is inaccessible"
            if raise_errors:
                raise GLOpenAPIConfigurationException(msg, debug_info=repr(exc))
            else:
                _loge(msg, exc_info=exc)


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
                        raise GLOpenAPIConfigurationException(msg)
                    else:
                        fds_exceed = _fds_exceed
                        fds_exceed.bootstrapped = True
                        return _fds_exceed(filename, maxcount)
        else:
            problem = "/proc not available and/or not a Linux/POSIX system"
            msg = f"Cannot set up read lock methods: {problem}"
            raise GLOpenAPIConfigurationException(msg)


class SQLTransactions():
 
    def __init__(self, sqlite_db, identifier=None, timeout=600, cwd="/tmp/GLOpenAPI", max_filelock_age_seconds=7200):
        try:
            makedirs(cwd, exist_ok=True)
        except OSError:
            raise GLOpenAPIConfigurationException(f"{cwd} is not writable")
        if sqlite_db is None:
            raise GLOpenAPIConfigurationException("`sqlite_db` cannot be None")
        elif call(["touch", path.join(cwd, ".check")]) != 0:
            raise GLOpenAPIConfigurationException(f"{cwd} is not writable")
        else:
            self.sqlite_db, self.timeout = sqlite_db, timeout
            self.max_filelock_age_seconds = max_filelock_age_seconds
            self.cwd, self.identifier = cwd, identifier
            _, name = path.split(sqlite_db)
            if identifier is None:
                self._lockfilename = path.join(cwd, f"{name}.lock")
            else:
                id_hash = sha1(identifier.encode()).hexdigest()
                self._lockfilename = path.join(cwd, f"{name}.{id_hash}.lock")
            clear_lock_if_stale(
                self._lockfilename, raise_errors=True,
                max_filelock_age_seconds=max_filelock_age_seconds,
            )
 
    @contextmanager
    def _connect(self, fulldesc, _tid):
        prelude = f"SQLTransactions._connect @ {_tid} ({fulldesc})"
        try:
            _kw = dict(timeout=self.timeout, isolation_level=None)
            with closing(connect(self.sqlite_db, **_kw)) as connection:
                _logd(f"{prelude}: begin transaction")
                execute = connection.execute
                apply_all_pragmas(self.sqlite_db, execute, self.timeout)
                execute("BEGIN")
                try:
                    yield connection, execute
                except Exception as exc:
                    connection.rollback()
                    msg = f"{prelude}: transaction failed, rolling back"
                    _logw(msg, exc_info=exc)
                    raise
                else:
                    _logd(f"{prelude}: committing transaction")
                    connection.commit()
        except (OSError, FileNotFoundError, OperationalError) as exc:
            msg = "Data could not be retrieved"
            raise GLOpenAPIDatabaseException(msg, debug_info=repr(exc))
        finally:
            try:
                connection.close()
            except UnboundLocalError:
                pass
            def _clear_stale_locks():
                for lockfilename in iglob(f"{self.cwd}/*.lock"):
                    clear_lock_if_stale(
                        lockfilename, raise_errors=False,
                        max_filelock_age_seconds=self.max_filelock_age_seconds,
                    )
            Thread(target=_clear_stale_locks).start()
 
    @contextmanager
    def unconditional(self, desc=None):
        """2PL bypass: ignore read and write locks, initiate transaction immediately"""
        fulldesc = f"{desc or ''}:{self.sqlite_db}:{self.identifier or ''}"
        _tid = timestamp36()
        prelude = f"SQLTransactions.unconditional @ {_tid} ({fulldesc})"
        _logd(f"{prelude}: staging transaction immediately")
        with self._connect(fulldesc, _tid) as (connection, execute):
            yield connection, execute
 
    @contextmanager
    def concurrent(self, desc=None):
        """2PL: lock that is non-exclusive w.r.t. other `SQLTransactions.concurrent`s, but exclusive w.r.t. `SQLTransactions.exclusive`"""
        fulldesc = f"{desc or ''}:{self.sqlite_db}:{self.identifier or ''}"
        _tid = timestamp36()
        prelude = f"SQLTransactions.concurrent @ {_tid} ({fulldesc})"
        _logd(f"{prelude}: waiting for write locks to release...")
        with FileLock(self._lockfilename) as lock:
            # grow handle count; prevents new write locks:
            _logd(f"{prelude}: increasing read lock count (++)")
            with open(self._lockfilename):
                # release hard lock immediately; new `WriteLock`s will not be
                # able to lock on, because read handle count is above zero:
                lock.release()
                # finally, initiate SQL connection:
                with self._connect(fulldesc, _tid) as (connection, execute):
                    yield connection, execute
                _logd(f"{prelude}: decreasing read lock count (--)")
 
    @contextmanager
    def exclusive(self, desc=None, poll_interval=.1):
        """2PL: lock that is exclusive w.r.t. both `SQLTransactions.concurrent`s and `SQLTransactions.exclusive`s"""
        fulldesc = f"{desc or ''}:{self.sqlite_db}:{self.identifier or ''}"
        _tid = timestamp36()
        prelude = f"SQLTransactions.exclusive @ {_tid} ({fulldesc})"
        _logd(f"{prelude}: obtaining write lock...")
        # obtain hard lock; prevents all other locks:
        with FileLock(self._lockfilename):
            # wait for old `ReadLock`s to finish:
            _logd(f"{prelude}: write lock obtained!")
            _logd(f"{prelude}: waiting for all read locks to release...")
            while fds_exceed(self._lockfilename, 1):
                sleep(poll_interval)
            # finally, initiate SQL connection:
            with self._connect(fulldesc, _tid) as (connection, execute):
                yield connection, execute
                _logd(f"{prelude}: releasing write lock")


def reraise_operational_error(obj, exc):
    """If OperationalError is due to too many columns in request, tell user; otherwise, raise generic error"""
    if "too many columns" in str(exc).lower():
        msg = "Too many columns requested"
        sug = "Limit request to fewer than 2000 columns"
        raise GLOpenAPIDatabaseException(msg, suggestion=sug)
    else:
        msg = "Data could not be retrieved"
        try:
            debug_info = [repr(exc), obj.query]
        except AttributeError:
            debug_info = repr(exc)
        raise GLOpenAPIDatabaseException(msg, debug_info=debug_info)
