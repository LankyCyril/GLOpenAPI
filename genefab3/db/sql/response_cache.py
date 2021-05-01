from werkzeug.datastructures import ImmutableDict
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError
from genefab3.common.logger import GeneFabLogger
from urllib.request import quote
from datetime import datetime
from zlib import compress, decompress, error as ZlibError
from os import path
from flask import Response


RESPONSE_CACHE_SCHEMAS = ImmutableDict({
    "response_cache": """(
        `context_identity` TEXT, `api_path` TEXT, `stored_at` INTEGER,
        `response` BLOB, `nbytes` INTEGER, `mimetype` TEXT
    )""",
    "accessions_used": """(
        `context_identity` TEXT, `accession` TEXT
    )""",
})

_logger = GeneFabLogger()
_logi, _logw, _loge = _logger.info, _logger.warning, _logger.error


def sane_sql_repr(accession):
    """Enclose accession in single or double quotes, fail if contains both"""
    if '"' not in accession:
        return f'"{accession}"'
    elif "'" not in accession:
        return f"'{accession}'"
    else:
        _r = repr(accession)
        _loge(f"ResponseCache():\n  accession contains '\"', \"'\": {_r}")
        raise OperationalError


class ResponseCache():
    """LRU response cache; responses are identified by context.identity, dropped if underlying (meta)data changed"""
 
    def __init__(self, sqlite_dbs):
        self.sqlite_db = sqlite_dbs.response_cache["db"]
        self.maxdbsize = sqlite_dbs.response_cache["maxsize"]
        if self.sqlite_db is not None:
            with closing(connect(self.sqlite_db)) as connection:
                for table, schema in RESPONSE_CACHE_SCHEMAS.items():
                    query = f"CREATE TABLE IF NOT EXISTS `{table}` {schema}"
                    connection.cursor().execute(query)
        else:
            _logw("ResponseCache():\n  LRU SQL cache DISABLED by client")
 
    def put(self, context, obj, response):
        """Store response object blob in response_cache table, if possible"""
        if self.sqlite_db is None:
            return
        api_path = quote(context.full_path)
        blob = Binary(compress(response.get_data()))
        stored_at = int(datetime.now().timestamp())
        delete_blob_command = f"""DELETE FROM `response_cache`
            WHERE `context_identity` == "{context.identity}" """
        insert_blob_command = f"""INSERT INTO `response_cache`
            (context_identity, api_path, stored_at, response, nbytes, mimetype)
            VALUES ("{context.identity}", "{api_path}", {stored_at},
                ?, {blob.nbytes}, "{response.mimetype}")"""
        make_delete_accession_entry_command = """DELETE FROM `accessions_used`
            WHERE `accession` == {} AND `context_identity` == "{}" """.format
        make_insert_accession_entry_command = """INSERT INTO `accessions_used`
            (accession, context_identity) VALUES ({}, "{}")""".format
        with closing(connect(self.sqlite_db)) as connection:
            try:
                cursor = connection.cursor()
                cursor.execute(delete_blob_command)
                cursor.execute(insert_blob_command, [blob])
                for acc_repr in (sane_sql_repr(a) for a in obj.accessions):
                    args = acc_repr, context.identity
                    cursor.execute(make_delete_accession_entry_command(*args))
                    cursor.execute(make_insert_accession_entry_command(*args))
            except OperationalError:
                connection.rollback()
                _cid = context.identity
                _loge(f"ResponseCache(), could not store:\n  {_cid}")
            else:
                connection.commit()
                _logi(f"ResponseCache(), stored:\n  {context.identity}")
 
    def shrink(self, max_iter=100, max_skids=20):
        """Drop oldest cached responses to keep file size on disk `self.maxdbsize`"""
        if self.sqlite_db is None:
            return
        target_size, n_dropped, n_skids = self.maxdbsize or float("inf"), 0, 0
        for _ in range(max_iter):
            current_size = path.getsize(self.sqlite_db)
            if (n_skids < max_skids) and (current_size > target_size):
                _logi(f"ResponseCache():\n  is being shrunk")
                with closing(connect(self.sqlite_db)) as connection:
                    query_oldest = f"""SELECT `context_identity`,`stored_at`
                        FROM `response_cache` WHERE `stored_at` ==
                        (SELECT MIN(`stored_at`) FROM `response_cache`) LIMIT 1
                    """
                    try:
                        cursor = connection.cursor()
                        entries = cursor.execute(query_oldest).fetchall()
                        if len(entries) and (len(entries[0]) == 2):
                            context_identity = entries[0][0]
                        else:
                            break
                        cursor.execute(f"""DELETE FROM `accessions_used` WHERE
                            `context_identity` == "{context_identity}" """)
                        cursor.execute(f"""DELETE FROM `response_cache` WHERE
                            `context_identity` == "{context_identity}" """)
                    except OperationalError:
                        connection.rollback()
                        break
                    else:
                        connection.commit()
                        n_dropped += 1
                n_skids += (path.getsize(self.sqlite_db) >= current_size)
            else:
                break
        self._report_shrinkage(n_dropped, n_skids)
 
    def _report_shrinkage(self, n_dropped, n_skids):
        if n_dropped:
            _logi(f"ResponseCache():\n  shrunk by {n_dropped} entries")
        else:
            _logw(f"ResponseCache():\n  could not drop entries to shrink")
        if n_skids:
            _logw(f"ResponseCache():\n  file did not shrink {n_skids} times")
 
    def drop_all(self):
        """Drop all cached responses"""
        if self.sqlite_db is None:
            return
        with closing(connect(self.sqlite_db)) as connection:
            try:
                cursor = connection.cursor()
                cursor.execute("DELETE FROM `accessions_used`")
                cursor.execute("DELETE FROM `response_cache`")
            except OperationalError as e:
                connection.rollback()
                _loge("ResponseCache().drop_all():\n  failed with %s", repr(e))
            else:
                connection.commit()
                _logi("ResponseCache():\n  dropped all cached Flask responses")
 
    def drop(self, accession):
        """Drop responses for given accession"""
        if self.sqlite_db is None:
            return
        with closing(connect(self.sqlite_db)) as connection:
            try:
                cursor = connection.cursor()
                acc_repr = sane_sql_repr(accession)
                query = f"""SELECT `context_identity` FROM `accessions_used`
                    WHERE `accession` == {acc_repr}"""
                identity_entries = cursor.execute(query).fetchall()
                for entry in identity_entries:
                    context_identity = entry[0]
                    cursor.execute(f"""DELETE FROM `accessions_used`
                        WHERE `context_identity` == "{context_identity}" """)
                    cursor.execute(f"""DELETE FROM `response_cache`
                        WHERE `context_identity` == "{context_identity}" """)
            except OperationalError as e:
                connection.rollback()
                msg = "ResponseCache():\n  could not drop responses for %s: %s"
                _loge(msg, accession, repr(e))
            else:
                connection.commit()
                msg = "ResponseCache():\n  dropped %s cached response(s) for %s"
                _logi(msg, len(identity_entries), accession)
 
    def get(self, context):
        """Retrieve cached response object blob from response_cache table if possible; otherwise return None"""
        if self.sqlite_db is None:
            return
        query = f"""SELECT `response`, `mimetype` FROM `response_cache`
            WHERE `context_identity` == "{context.identity}" LIMIT 1"""
        try:
            with closing(connect(self.sqlite_db)) as connection:
                row = connection.cursor().execute(query).fetchone()
        except OperationalError:
            row = None
        if isinstance(row, tuple) and (len(row) == 2):
            try:
                response = Response(
                    response=decompress(row[0]), mimetype=row[1],
                )
            except ZlibError:
                msg = "ResponseCache() could not decompress, staging deletion"
                _logw(f"{msg}:\n  {context.identity}")
                return None
            else:
                _logi(f"ResponseCache(), retrieved:\n  {context.identity}")
                return response
        else:
            _logi(f"ResponseCache(), nothing yet for:\n  {context.identity}")
            return None
