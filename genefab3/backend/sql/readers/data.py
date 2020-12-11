from contextlib import closing
from sqlite3 import connect
from pandas import read_sql, concat
from pandas.io.sql import DatabaseError as PandasDatabaseError
from genefab3.common.exceptions import GeneLabFileException
from genefab3.backend.sql.table import CachedTable
from genefab3.config import ROW_TYPES


NO_FILES_ERROR = "No data files found for datatype"
AMBIGUOUS_FILES_ERROR = "Multiple (ambiguous) data files found for datatype"


def read_raw_sql_table(sqlite_db, table_name):
    """Read a table from SQL directly by name"""
    with closing(connect(sqlite_db)) as sql_connection:
        try:
            return read_sql(
                f"SELECT * FROM '{table_name}'",
                sql_connection, index_col="index",
            )
        except PandasDatabaseError:
            return None


def get_sql_data(dbs, raw_annotation, datatype, rows=None):
    """Based on `raw_annotation` (accessions, assays, sample names, file descriptors), update/retrieve data from SQL database"""
    groupby = raw_annotation.groupby(
        ["info.accession", "info.assay"], as_index=False, sort=False,
    )
    agg = groupby.agg(list).iterrows()
    tables = []
    for _, (accession, assay_name, sample_names, _, file_descriptors) in agg:
        file_descriptors_as_set = set(file_descriptors)
        if len(file_descriptors_as_set) == 0:
            raise GeneLabFileException(
                NO_FILES_ERROR, accession, assay_name, datatype=datatype,
            )
        elif len(file_descriptors_as_set) > 1:
            raise GeneLabFileException(
                AMBIGUOUS_FILES_ERROR, accession, assay_name, datatype=datatype,
            )
        else:
            tables.append(CachedTable(
                dbs=dbs,
                file_descriptor=file_descriptors_as_set.pop(),
                datatype=datatype,
                accession=accession,
                assay_name=assay_name,
                sample_names=sample_names,
                read_raw_sql_table=read_raw_sql_table,
            ))
    joined_table = concat( # this is in-memory and faster than sqlite3:
        # wesmckinney.com/blog/high-performance-database-joins-with-pandas-dataframe-more-benchmarks
        [table.dataframe(rows=rows) for table in tables], axis=1, sort=False,
    )
    joined_table.index.name = ("info", "info", ROW_TYPES[datatype])
    return joined_table.reset_index()
