from werkzeug.datastructures import ImmutableDict
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError
from genefab3.common.logger import GeneFabLogger
from urllib.request import quote
from datetime import datetime
from zlib import compress, decompress, error as ZlibError
from genefab3.common.utils import get_attribute
from flask import Response


RESPONSE_CACHE_SCHEMAS = ImmutableDict({
    "response_cache": """(
        'context_identity' TEXT, 'api_path' TEXT, 'timestamp' INTEGER,
        'response' BLOB, 'nbytes' INTEGER, 'mimetype' TEXT
    )""",
    "accessions_used": """(
        'context_identity' TEXT, 'accession' TEXT
    )""",
})


class ResponseCache():
    """LRU response cache; responses are identified by context.identity, dropped if underlying (meta)data changed"""
 
    def __init__(self, sqlite_dbs, maxsize=24*1024*1024*1024):
        self.sqlite_dbs = sqlite_dbs
        self.maxsize = maxsize
        if sqlite_dbs.cache is not None:
            # if not path.exists(response_cache): # TODO: auto_vacuum
            #     with closing(connect(response_cache)) as sql_connection:
            #         sql_connection.cursor().execute("PRAGMA auto_vacuum = 1")
            with closing(connect(sqlite_dbs.cache)) as connection:
                for table, schema in RESPONSE_CACHE_SCHEMAS.items():
                    connection.cursor().execute(
                        f"CREATE TABLE IF NOT EXISTS '{table}' {schema}",
                    )
        else:
            GeneFabLogger().warning("Not using LRU response SQL cache")
 
    def put(self, context, obj, response):
        """Store response object blob in response_cache table, if possible"""
        if self.sqlite_dbs.cache is None:
            return
        obj_accessions = get_attribute(obj, "accessions", set())
        accessions = obj_accessions | set(context.accessions_and_assays)
        api_path = quote(context.full_path)
        blob = Binary(compress(response.get_data()))
        timestamp = int(datetime.now().timestamp())
        delete_blob_command = f"""DELETE FROM 'response_cache'
            WHERE context_identity = '{context.identity}'"""
        insert_blob_command = f"""INSERT INTO 'response_cache'
            (context_identity, api_path, timestamp, response, nbytes, mimetype)
            VALUES ('{context.identity}', '{api_path}', {timestamp},
                ?, {blob.nbytes}, '{response.mimetype}')"""
        make_delete_accession_entry_command = """DELETE FROM 'accessions_used'
            WHERE accession = '{}' AND context_identity = '{}'""".format
        make_insert_accession_entry_command = """ INSERT INTO 'accessions_used'
            (accession, context_identity) VALUES ('{}', '{}')""".format
        with closing(connect(self.sqlite_dbs.cache)) as connection:
            try:
                cursor = connection.cursor()
                cursor.execute(delete_blob_command)
                cursor.execute(insert_blob_command, [blob])
                for accession in accessions:
                    args = accession, context.identity
                    cursor.execute(make_delete_accession_entry_command(*args))
                    cursor.execute(make_insert_accession_entry_command(*args))
            except OperationalError:
                connection.rollback()
                msg = f"LRU response cache: could not store {context.identity}"
                GeneFabLogger().error(msg)
            else:
                connection.commit()
                msg = f"LRU response cache: stored {context.identity}"
                GeneFabLogger().info(msg)
 
    def drop(self, accession):
        """Drop responses for given accession"""
        with closing(connect(self.sqlite_dbs.cache)) as connection:
            try:
                cursor = connection.cursor()
                query = f"""SELECT context_identity FROM 'accessions_used'
                    WHERE accession = '{accession}'"""
                identity_entries = cursor.execute(query).fetchall()
                for entry in identity_entries:
                    context_identity = entry[0]
                    cursor.execute(f"""DELETE FROM 'accessions_used'
                        WHERE context_identity = '{context_identity}'""")
                    cursor.execute(f"""DELETE FROM 'response_cache'
                        WHERE context_identity = '{context_identity}'""")
            except OperationalError as e:
                connection.rollback()
                msg = "Could not drop cached Flask responses for %s: %s"
                GeneFabLogger().error(msg, accession, repr(e))
            else:
                connection.commit()
                msg = "Dropped %s cached Flask responses for %s"
                GeneFabLogger().info(msg, len(identity_entries), accession)
 
    def shrink(self, to=None):
        """Drop oldest cached responses to keep file size on disk under `to` or `self.maxsize`"""
        msg = "Shrinking Flask response cache not implemented yet" # TODO
        GeneFabLogger().warning(msg)
 
    def get(self, context):
        """Retrieve cached response object blob from response_cache table if possible; otherwise return None"""
        query = f"""SELECT response, mimetype FROM 'response_cache'
            WHERE context_identity = '{context.identity}' LIMIT 1"""
        try:
            with closing(connect(self.sqlite_dbs.cache)) as connection:
                row = connection.cursor().execute(query).fetchone()
        except OperationalError:
            row = None
        if isinstance(row, tuple) and (len(row) == 2):
            try:
                response = Response(
                    response=decompress(row[0]), mimetype=row[1],
                )
            except ZlibError:
                msg = "Could not decompress cached response, staging deletion"
                GeneFabLogger().warning(msg)
                return None
            else:
                msg = f"LRU response cache: retrieved {context.identity}"
                GeneFabLogger().info(msg)
                return response
        else:
            msg = f"LRU response cache: nothing yet for {context.identity}"
            GeneFabLogger().info(msg)
            return None
