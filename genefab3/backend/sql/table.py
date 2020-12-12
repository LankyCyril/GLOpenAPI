from types import SimpleNamespace
from genefab3.backend.mongo.readers.file_descriptors import (
    get_cached_file_descriptor_timestamp, set_cached_file_descriptor_timestamp,
    drop_cached_file_descriptor_timestamp,
)
from genefab3.backend.sql.writers.data import recache_table
from logging import getLogger, DEBUG
from urllib.request import quote
from genefab3.backend.mongo.utils import infer_file_separator
from genefab3.common.exceptions import GeneLabDatabaseException
from pandas import DataFrame, MultiIndex


CACHED_TABLE_LOGGER_SUCCESS_MASK, CACHED_TABLE_LOGGER_ERROR_MASK = (
    "CachedTable: updated accession %s, assay %s, datatype '%s', file url '%s'",
    "CachedTable: '%s' at accession %s, assay %s, datatype '%s', file url '%s'",
)
MISSING_SQL_TABLE_ERROR = (
    "Missing data table in GeneFab database "
    "(will attempt to re-cache on next request)"
)
MISSING_SAMPLE_NAMES_ERROR = "Missing sample names in GeneFab database"


class CachedTable():
    """Abstracts SQL table generated from a CSV/TSV file"""
    file = SimpleNamespace(name=None, url=None, timestamp=None, sep=None)
    is_fresh, status = None, None
    accession, assay_name, sample_names = None, None, None
    data = None
    logger = None
 
    def __init__(self, dbs, file_descriptor, datatype, accession, assay_name, sample_names, read_raw_sql_table):
        """Check cold storage and cached timestamps for file, update cache if remote file was updated"""
        self.name = quote(f"{datatype}/{accession}/{assay_name}")
        self.logger = getLogger("genefab3")
        self.logger.setLevel(DEBUG)
        self.mongo_db = dbs.mongo_db
        self.sqlite_db = dbs.sqlite_db
        self.read_raw_sql_table = read_raw_sql_table
        self.datatype, self.accession, self.assay_name, self.sample_names = (
            datatype, accession, assay_name, sample_names,
        )
        self.file = SimpleNamespace(
            name=file_descriptor.name, url=file_descriptor.url,
            timestamp=file_descriptor.timestamp,
            sep=infer_file_separator(file_descriptor.name),
        )
        cached_timestamp = get_cached_file_descriptor_timestamp(
            self.mongo_db, self.file,
        )
        if cached_timestamp >= self.file.timestamp:
            self.is_fresh, self.file.timestamp = True, cached_timestamp
        else:
            self.data, self.is_fresh, error = recache_table(
                self.sqlite_db, self.name,
                self.file, self.sample_names, self.logger,
            )
            if self.is_fresh:
                set_cached_file_descriptor_timestamp(self.mongo_db, self.file)
                self.logger.info(
                    CACHED_TABLE_LOGGER_SUCCESS_MASK, self.accession,
                    self.assay_name, self.datatype, self.file.url,
                )
            elif error is not None:
                self.logger.error(
                    CACHED_TABLE_LOGGER_ERROR_MASK, repr(error),
                    self.accession, self.assay_name, self.datatype,
                    self.file.url, stack_info=True,
                )
 
    def dataframe(self, rows=None):
        """Render retrieved or cached data as pandas.DataFrame"""
        if rows is not None:
            raise NotImplementedError("Selecting rows from a table")
        if self.data is None:
            data_subset = self.read_raw_sql_table(self.sqlite_db, self.name)
            if data_subset is None:
                drop_cached_file_descriptor_timestamp(
                    self.mongo_db, self.file,
                )
                raise GeneLabDatabaseException(
                    MISSING_SQL_TABLE_ERROR, self.accession,
                    self.assay_name, datatype=self.datatype,
                )
        else:
            data_subset = self.data if (rows is None) else self.data.loc[rows]
        if not (set(self.sample_names) <= set(data_subset.columns)):
            raise GeneLabDatabaseException(
                MISSING_SAMPLE_NAMES_ERROR, self.accession, self.assay_name,
                names=sorted(set(self.sample_names) - set(data_subset.columns)),
            )
        else:
            return DataFrame(
                data=data_subset.values, index=data_subset.index,
                columns=MultiIndex.from_tuples(
                    (self.accession, self.assay_name, sample_name)
                    for sample_name in list(data_subset.columns)
                )
            )
