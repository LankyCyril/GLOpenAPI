from genefab3.config import RESPONSE_CACHE
from urllib.request import quote
from json import dumps
from contextlib import closing
from sqlite3 import connect, OperationalError
from flask import Response
from zlib import decompress


def retrieve_cached_response(context, response_cache=RESPONSE_CACHE, table="response_cache"):
    """Retrieve cached response object blob from response_cache table if possible; otherwise return None"""
    api_args = quote(dumps(context.complete_args, sort_keys=True))
    query = f"""
        SELECT response, mimetype FROM '{table}'
        WHERE api_args = '{api_args}'
        LIMIT 1
    """
    try:
        with closing(connect(response_cache)) as sql_connection:
            cursor = sql_connection.cursor()
            row = cursor.execute(query).fetchone()
    except OperationalError:
        row = None
    if isinstance(row, tuple) and (len(row) == 2):
        return Response(response=decompress(row[0]), mimetype=row[1])
    else:
        return None
