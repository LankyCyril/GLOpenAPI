from genefab3.common.logger import GeneFabLogger
from genefab3.db.sql.utils import sql_connection
from functools import wraps
from genefab3.common.types import ResponseContainer
from sqlite3 import Binary, OperationalError
from datetime import datetime
from zlib import compress, decompress, error as ZlibError
from threading import Thread
from flask import Response
from os import path


RESPONSE_CACHE_SCHEMAS = (
    ("response_cache", """(
        `context_identity` TEXT, `response` BLOB, `mimetype` TEXT,
        `retrieved_at` INTEGER
    )"""),
    ("accessions_used", """(
        `context_identity` TEXT, `accession` TEXT
    )"""),
)

_logger = GeneFabLogger()
_logi, _logw, _loge = _logger.info, _logger.warning, _logger.error


class ResponseCache():
    """LRU response cache; responses are identified by context.identity, dropped if underlying (meta)data changed"""
 
    def __init__(self, sqlite_dbs):
        self.sqlite_db = sqlite_dbs.response_cache["db"]
        self.maxdbsize = sqlite_dbs.response_cache["maxsize"] or float("inf")
        if self.sqlite_db is None:
            _logw("ResponseCache():\n  LRU SQL cache DISABLED by client")
        else:
            _kw = dict(desc="response_cache", timeout=5)
            with sql_connection(self.sqlite_db, **_kw) as (connection, execute):
                for table, schema in RESPONSE_CACHE_SCHEMAS:
                    query = f"CREATE TABLE IF NOT EXISTS `{table}` {schema}"
                    execute(query)
 
    bypass_if_disabled = lambda f: wraps(f)(lambda self, *args, **kwargs:
        ResponseContainer(content=None) if self.sqlite_db is None
        else f(self, *args, **kwargs)
    )
 
    @bypass_if_disabled
    def put(self, context, obj, response):
        """Store response object blob in response_cache table, if possible; this will happen in a parallel thread"""
        desc = "ResponseCache(),"
        if not obj.accessions:
            msg = "none or unrecognized accession names in object"
            _logw(f"{desc} did not store:\n  {context.identity}\n  {msg}")
            return
        def _put():
            _kw = dict(desc="response_cache")
            with sql_connection(self.sqlite_db, **_kw) as (_, execute):
                blob = Binary(compress(response.get_data()))
                retrieved_at = int(datetime.now().timestamp())
                try:
                    execute("""DELETE FROM `response_cache` WHERE
                        `context_identity` == ?""", [context.identity])
                    execute("""INSERT INTO `response_cache`
                        (context_identity, response, mimetype, retrieved_at)
                        VALUES (?,?,?,?)""", [
                        context.identity, blob, response.mimetype, retrieved_at])
                    for accession in obj.accessions:
                        execute("""DELETE FROM `accessions_used` WHERE
                            `accession` == ? AND `context_identity` == ?""", [
                            accession, context.identity])
                        execute("""INSERT INTO `accessions_used`
                            (accession, context_identity) VALUES (?, ?)""", [
                            accession, context.identity])
                except OperationalError:
                    _loge(f"{desc} could not store:\n  {context.identity}")
                    raise
                else:
                    _logi(f"{desc} stored:\n  {context.identity}")
        Thread(target=_put).start()
 
    def _drop_by_context_identity(self, execute, context_identity):
        """Drop responses with given context.identity"""
        execute("""DELETE FROM `accessions_used`
            WHERE `context_identity` == ?""", [context_identity])
        execute("""DELETE FROM `response_cache`
            WHERE `context_identity` == ?""", [context_identity])
 
    @bypass_if_disabled
    def drop(self, accession):
        """Drop responses for given accession"""
        with sql_connection(self.sqlite_db, "response_cache") as (_, execute):
            try:
                query = """SELECT `context_identity` FROM `accessions_used`
                    WHERE `accession` == ?"""
                identity_entries = execute(query, [accession]).fetchall()
                for context_identity, *_ in identity_entries:
                    self._drop_by_context_identity(execute, context_identity)
            except OperationalError as e:
                msg = "ResponseCache():\n  could not drop responses for %s: %s"
                _loge(msg, accession, repr(e))
                raise
            else:
                msg = "ResponseCache():\n  dropped %s cached response(s) for %s"
                _logi(msg, len(identity_entries), accession)
 
    @bypass_if_disabled
    def drop_all(self):
        """Drop all cached responses"""
        with sql_connection(self.sqlite_db, "response_cache") as (_, execute):
            try:
                execute("DELETE FROM `accessions_used`")
                execute("DELETE FROM `response_cache`")
            except OperationalError as e:
                _loge(f"ResponseCache().drop_all():\n  failed with {e!r}")
                raise
            else:
                _logi("ResponseCache():\n  dropped all cached Flask responses")
 
    @bypass_if_disabled
    def get(self, context):
        """Retrieve cached response object blob from response_cache table if possible; otherwise return None"""
        with sql_connection(self.sqlite_db, "response_cache") as (_, execute):
            try:
                cid = context.identity
                query = """SELECT `response`, `mimetype` FROM `response_cache`
                    WHERE `context_identity` == ? LIMIT 1"""
                row, mimetype = execute(query, [cid]).fetchone() or [None, None]
            except OperationalError:
                row, mimetype = None, None
        if row and mimetype:
            try:
                response = Response(
                    response=decompress(row[0]), mimetype=row[1],
                )
            except ZlibError:
                msg = "ResponseCache() could not decompress, staging deletion"
                _logw(f"{msg}:\n  {context.identity}")
                return ResponseContainer(content=None)
            else:
                _logi(f"ResponseCache(), retrieved:\n  {context.identity}")
                return ResponseContainer(content=response)
        else:
            _logi(f"ResponseCache(), nothing yet for:\n  {context.identity}")
            return ResponseContainer(content=None)
 
    @bypass_if_disabled
    def shrink(self, max_iter=100, max_skids=20):
        """Drop oldest cached responses to keep file size on disk `self.maxdbsize`"""
        # TODO: DRY: very similar to genefab3.db.sql.core SQLiteTable.cleanup()
        n_dropped, n_skids = 0, 0
        for _ in range(max_iter):
            current_size = path.getsize(self.sqlite_db)
            if (n_skids < max_skids) and (current_size > self.maxdbsize):
                _logi(f"ResponseCache():\n  is being shrunk")
                _kw = dict(filename=self.sqlite_db, desc="response_cache")
                with sql_connection(**_kw) as (connection, execute):
                    query_oldest = f"""SELECT `context_identity`
                        FROM `response_cache` ORDER BY `retrieved_at` ASC"""
                    try:
                        cid = (execute(query_oldest).fetchone() or [None])[0]
                        if cid is None:
                            break
                        else:
                            self._drop_by_context_identity(execute, cid)
                    except OperationalError:
                        connection.rollback()
                        break
                    else:
                        connection.commit()
                        n_dropped += 1
                n_skids += (path.getsize(self.sqlite_db) >= current_size)
            else:
                break
        if n_dropped:
            _logi(f"ResponseCache():\n  shrunk by {n_dropped} entries")
        elif path.getsize(self.sqlite_db) > self.maxdbsize:
            _logw(f"ResponseCache():\n  could not drop entries to shrink")
        if n_skids:
            _logw(f"ResponseCache():\n  file did not shrink {n_skids} times")
