from genefab3.config import RESPONSE_CACHE, ZLIB_COMPRESS_RESPONSE_CACHE
from contextlib import closing
from sqlite3 import connect, OperationalError
from flask import Response

if ZLIB_COMPRESS_RESPONSE_CACHE:
    from zlib import decompress
    from zlib import error as ZlibError
else:
    decompress = lambda _:_
    ZlibError = NotImplementedError


def retrieve_cached_response(context, response_cache=RESPONSE_CACHE):
    """Retrieve cached response object blob from response_cache table if possible; otherwise return None"""
    query = f"""
        SELECT response, mimetype FROM 'response_cache'
        WHERE context_identity = '{context.identity}'
        LIMIT 1
    """
    try:
        with closing(connect(response_cache)) as sql_connection:
            cursor = sql_connection.cursor()
            row = cursor.execute(query).fetchone()
    except OperationalError:
        row = None
    if isinstance(row, tuple) and (len(row) == 2):
        try:
            return Response(response=decompress(row[0]), mimetype=row[1])
        except ZlibError: # maybe the cached version wasn't compressed, but...
            return None # cannot guarantee validity; this stages for replacement
    else:
        return None
