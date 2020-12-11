from pandas import read_csv
from contextlib import closing
from sqlite3 import connect


def recache_table(sqlite_db, name, file, sample_names, logger):
    """Update local table from remote file"""
    try:
        data = read_csv(file.url, sep=file.sep)
        if data.columns[0] not in sample_names:
            data.set_index(data.columns[0], inplace=True)
            data.index.name = None
        data = data[sample_names]
    except Exception as e:
        return None, False, e
    else:
        with closing(connect(sqlite_db)) as sql_connection:
            try:
                data.to_sql(name, sql_connection, if_exists="replace")
            except Exception as e:
                sql_connection.rollback()
                return data, False, e
            else:
                sql_connection.commit()
                return data, True, None
