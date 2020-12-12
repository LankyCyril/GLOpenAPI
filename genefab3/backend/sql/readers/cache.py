from genefab3.config import RESPONSE_CACHE
from contextlib import closing
from sqlite3 import connect, OperationalError
from urllib.request import quote
from flask import Response


def retrieve_cached_response(request, response_cache=RESPONSE_CACHE, table="response_cache"):
    """Retrieve cached response object blob from response_cache table if possible; otherwise return None"""
    api_path = quote(request.full_path)
    query = f"""SELECT * FROM '{table}' WHERE api_path = '{api_path}'"""
    try:
        with closing(connect(response_cache)) as sql_connection:
            cursor = sql_connection.cursor()
            row = cursor.execute(query).fetchone()
    except OperationalError:
        row = None
    if isinstance(row, tuple) and (len(row) >= 3):
        return Response(response=row[1], mimetype=row[2])
    else:
        return None
