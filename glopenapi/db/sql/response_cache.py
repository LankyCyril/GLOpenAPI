from glopenapi.common.exceptions import GLOpenAPILogger
from glopenapi.db.sql.utils import SQLTransactions
from functools import wraps
from glopenapi.api.types import ResponseContainer
from flask import Response
from collections.abc import Callable
from zlib import compressobj, decompressobj, Z_FINISH, error as ZlibError
from sqlite3 import Binary, OperationalError
from datetime import datetime
from threading import Thread
from glopenapi.common.hacks import apply_hack, bypass_uncached_views
from os import path


RESPONSE_CACHE_SCHEMAS = (
    ("response_cache", """(
        `context_identity` TEXT, `i` INTEGER, `chunk` BLOB,
        `mimetype` TEXT, `retrieved_at` INTEGER
    )"""),
    ("accessions_used", """(
        `context_identity` TEXT, `accession` TEXT
    )"""),
)

_logi, _logw = GLOpenAPILogger.info, GLOpenAPILogger.warning
_loge = GLOpenAPILogger.error


class ResponseCache():
    """LRU response cache; responses are identified by context.identity, dropped if underlying (meta)data changed"""
 
    def __init__(self, sqlite_dbs):
        self.sqlite_db = sqlite_dbs.response_cache["db"]
        self.maxdbsize = sqlite_dbs.response_cache["maxsize"] or float("inf")
        if self.sqlite_db is None:
            msg = "LRU SQL cache DISABLED by client parameter"
            _logw(f"ResponseCache():\n  {msg}")
        else:
            self.sqltransactions = SQLTransactions(self.sqlite_db)
            desc = "response_cache/ensure_schema"
            with self.sqltransactions.concurrent(desc) as (_, execute):
                for table, schema in RESPONSE_CACHE_SCHEMAS:
                    execute(f"CREATE TABLE IF NOT EXISTS `{table}` {schema}")
 
    bypass_if_disabled = lambda f: wraps(f)(lambda self, *args, **kwargs:
        ResponseContainer(content=None) if self.sqlite_db is None
        else f(self, *args, **kwargs)
    )
 
    def _validate_content_type(self, response_container):
        """Check if type of passed content is supported by ResponseCache"""
        _is = lambda _type: isinstance(response_container.content, _type)
        if not response_container.obj.accessions:
            return "none or unrecognized accession names in object"
        elif _is(Response) or (not _is(Callable)):
            return "only data iterators are supported"
        else:
            return None
 
    def _itercompress(self, content, isinstance=isinstance, str=str, bytes=bytes, Binary=Binary):
        """Iteratively compress chunks generated by callable `content`"""
        compressor = compressobj()
        for uncompressed_chunk in content():
            if isinstance(uncompressed_chunk, str):
                chunk = compressor.compress(uncompressed_chunk.encode())
            elif isinstance(uncompressed_chunk, bytes):
                chunk = compressor.compress(uncompressed_chunk)
            else:
                _type = type(uncompressed_chunk).__name__
                raise TypeError("Content chunk is not str or bytes", _type)
            if chunk:
                yield Binary(chunk)
        chunk = compressor.flush(Z_FINISH)
        if chunk:
            yield Binary(chunk)
 
    @bypass_if_disabled
    def put(self, response_container, context):
        """Store response object blob in response_cache table, if possible; this will happen in a parallel thread"""
        problem = self._validate_content_type(response_container)
        if problem:
            msg = f"{context.identity}\n  {problem}"
            _logw(f"ResponseCache(), did not store:\n  {msg}")
            return
        def _put(desc="response_cache/put"):
            retrieved_at = int(datetime.now().timestamp())
            with self.sqltransactions.exclusive(desc) as (_, execute):
                try:
                    execute("""DELETE FROM `response_cache` WHERE
                        `context_identity` == ?""", [context.identity])
                    _it = self._itercompress
                    for i, chunk in enumerate(_it(response_container.content)):
                        execute("""INSERT INTO `response_cache`
                            (context_identity, i, chunk, mimetype, retrieved_at)
                            VALUES (?,?,?,?,?)""", [context.identity, i, chunk,
                            response_container.mimetype, retrieved_at])
                    for accession in response_container.obj.accessions:
                        execute("""DELETE FROM `accessions_used` WHERE
                            `accession` == ? AND `context_identity` == ?""", [
                            accession, context.identity])
                        execute("""INSERT INTO `accessions_used`
                            (accession, context_identity) VALUES (?, ?)""", [
                            accession, context.identity])
                except (OperationalError, ZlibError, TypeError) as e:
                    msg = f"{context.identity}, {e!r}"
                    _loge(f"ResponseCache(), could not store:\n  {msg}")
                    raise
                else:
                    _logi(f"ResponseCache(), stored:\n  {context.identity}")
        Thread(target=_put).start()
 
    def _drop_by_context_identity(self, execute, context_identity):
        """Drop responses with given context.identity"""
        execute("""DELETE FROM `accessions_used`
            WHERE `context_identity` == ?""", [context_identity])
        execute("""DELETE FROM `response_cache`
            WHERE `context_identity` == ?""", [context_identity])
 
    @bypass_if_disabled
    def drop_by_context(self, *, identity, desc="response_cache/drop_by_context"):
        with self.sqltransactions.exclusive(desc) as (_, execute):
            try:
                self._drop_by_context_identity(execute, identity)
            except OperationalError as e:
                msg = "ResponseCache():\n  could not drop responses for %s: %s"
                _loge(msg, identity, repr(e))
                raise
            else:
                msg = "ResponseCache():\n  dropped cached response(s) for %s"
                _logi(msg, identity)
 
    @bypass_if_disabled
    def drop_by_accession(self, accession, desc="response_cache/drop"):
        """Drop responses for given accession"""
        with self.sqltransactions.exclusive(desc) as (_, execute):
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
    def drop_all(self, desc="response_cache/drop_all"):
        """Drop all cached responses"""
        with self.sqltransactions.exclusive(desc) as (_, execute):
            try:
                execute("DELETE FROM `accessions_used`")
                execute("DELETE FROM `response_cache`")
            except OperationalError as e:
                _loge(f"ResponseCache().drop_all():\n  failed with {e!r}")
                raise
            else:
                _logi("ResponseCache():\n  dropped all cached Flask responses")
 
    def _iterdecompress(self, cid, desc="response_cache/_iterdecompress"):
        """Iteratively decompress chunks retrieved from database by `context_identity`"""
        decompressor = decompressobj()
        with self.sqltransactions.concurrent(desc) as (_, execute):
            query = """SELECT `mimetype` FROM `response_cache`
                WHERE `context_identity` == ? LIMIT 1"""
            mimetype, = execute(query, [cid]).fetchone() or [None]
            if mimetype is None:
                raise EOFError("No chunks found in response_cache")
            else:
                yield mimetype
            query = """SELECT `chunk`,`mimetype` FROM `response_cache`
                WHERE `context_identity` == ? ORDER BY `i` ASC"""
            for chunk, _mimetype in execute(query, [cid]):
                if mimetype != _mimetype:
                    raise OperationalError("Stored mimetypes of chunks differ")
                decompressed_chunk = decompressor.decompress(chunk)
                if decompressed_chunk:
                    yield decompressed_chunk.decode()
            decompressed_chunk = decompressor.flush()
            if decompressed_chunk:
                yield decompressed_chunk.decode()
 
    @apply_hack(bypass_uncached_views)
    @bypass_if_disabled
    def get(self, context):
        """Retrieve cached response object blob from response_cache table if possible; otherwise return empty ResponseContainer()"""
        try:
            for value in self._iterdecompress(context.identity):
                pass # test retrieval and decompression before returning
        except EOFError:
            _logi(f"ResponseCache(), nothing yet for:\n  {context.identity}")
            return ResponseContainer(content=None)
        except OperationalError as e:
            msg = "could not retrieve, staging replacement"
            _logw(f"ResponseCache() {msg}:\n  {context.identity}, {e!r}")
            return ResponseContainer(content=None)
        except ZlibError as e:
            msg = "could not decompress, staging replacement"
            _logw(f"ResponseCache() {msg}:\n  {context.identity}, {e!r}")
            return ResponseContainer(content=None)
        else:
            _logi(f"ResponseCache(), retrieving:\n  {context.identity}")
            iterator = self._iterdecompress(context.identity) # second pass # TODO PhoenixIterator?
            mimetype = next(iterator)
            return ResponseContainer(lambda: iterator, mimetype)
 
    @bypass_if_disabled
    def shrink(self, max_iter=100, max_skids=20, desc="response_cache/shrink"):
        """Drop oldest cached responses to keep file size on disk under `self.maxdbsize`"""
        # TODO: DRY: very similar to glopenapi.db.sql.core SQLiteTable.cleanup()
        n_dropped, n_skids = 0, 0
        for _ in range(max_iter):
            current_size = path.getsize(self.sqlite_db)
            if (n_skids < max_skids) and (current_size > self.maxdbsize):
                with self.sqltransactions.concurrent(desc) as (_, execute):
                    query_oldest = """SELECT `context_identity`
                        FROM `response_cache` ORDER BY `retrieved_at` ASC"""
                    cid = (execute(query_oldest).fetchone() or [None])[0]
                    if cid is None:
                        break
                with self.sqltransactions.exclusive(desc) as (connection, execute):
                    try:
                        msg = f"ResponseCache.shrink():\n  dropping {cid}"
                        GLOpenAPILogger.info(msg)
                        self._drop_by_context_identity(execute, cid)
                    except OperationalError as e:
                        msg= f"Rolling back shrinkage due to {e!r}"
                        GLOpenAPILogger.error(msg, exc_info=e)
                        connection.rollback() # explicit, to be able to continue
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
            _logw("ResponseCache():\n  could not drop entries to shrink")
        if n_skids:
            _logw(f"ResponseCache():\n  file did not shrink {n_skids} times")