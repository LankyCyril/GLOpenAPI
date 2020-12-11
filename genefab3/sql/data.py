from types import SimpleNamespace
from genefab3.config import COLLECTION_NAMES, ROW_TYPES
from logging import getLogger, DEBUG
from os import path, makedirs
from re import sub
from hashlib import md5
from genefab3.backend.mongo.utils import infer_file_separator
from pymongo import DESCENDING
from genefab3.backend.mongo.writers.metadata import run_mongo_transaction
from genefab3.common.exceptions import GeneLabDatabaseException, GeneLabFileException
from pandas import read_csv, read_sql, DataFrame, MultiIndex, concat
from pandas.io.sql import DatabaseError as PandasDatabaseError
from contextlib import closing
from sqlite3 import connect


NO_FILES_ERROR = "No data files found for datatype"
AMBIGUOUS_FILES_ERROR = "Multiple (ambiguous) data files found for datatype"
MISSING_SAMPLE_NAMES_ERROR = "Missing sample names in GeneFab database"
MISSING_SQL_TABLE_ERROR = (
    "Missing data table in GeneFab database "
    "(will attempt to re-cache on next request)"
)
CACHED_TABLE_LOGGER_SUCCESS_MASK, CACHED_TABLE_LOGGER_ERROR_MASK = (
    "CachedTable: updated accession %s, assay %s, datatype '%s', file url '%s'",
    "CachedTable: '%s' at accession %s, assay %s, datatype '%s', file url '%s'",
)
CACHED_TABLE_LOGGER_DROP_WARNING = (
    "CachedTable: dropping timestamp for %s, assay %s, datatype '%s'"
)


class CachedTable():
    """Abstracts SQL table generated from a CSV/TSV file"""
    file = SimpleNamespace(name=None, url=None, timestamp=None, sep=None)
    is_fresh, status = None, None
    accession, assay_name, sample_names = None, None, None
    data = None
    logger = None
 
    def __init__(self, dbs, file_descriptor, datatype, accession, assay_name, sample_names, cname=COLLECTION_NAMES.FILE_DESCRIPTORS):
        """Check cold storage JSON and cache, update cache if remote file was updated"""
        self.name = f"{accession}/{assay_name}"
        self.logger = getLogger("genefab3")
        self.logger.setLevel(DEBUG)
        self.mongo_db = dbs.mongo_db
        self.sqlite_db = self._get_unambiguous_path(dbs.sqlite_dir, datatype)
        self.datatype, self.accession, self.assay_name, self.sample_names = (
            datatype, accession, assay_name, sample_names,
        )
        self.file = SimpleNamespace(
            name=file_descriptor.name,
            url=file_descriptor.url,
            timestamp=file_descriptor.timestamp,
            sep=infer_file_separator(file_descriptor.name),
        )
        cache_entry = getattr(self.mongo_db, cname).find_one(
            {"name": self.file.name, "url": self.file.url},
            {"_id": False, "timestamp": True}, sort=[("timestamp", DESCENDING)],
        )
        is_cache_fresh = (
            (cache_entry is not None) and
            (cache_entry.get("timestamp", -1) >= self.file.timestamp)
        )
        if is_cache_fresh:
            self.is_fresh = True
            self.file.timestamp = cache_entry.get("timestamp", -1)
        else:
            self.is_fresh = self._recache()
            if self.is_fresh:
                run_mongo_transaction(
                    action="replace", collection=getattr(self.mongo_db, cname),
                    query={"name": self.file.name, "url": self.file.url},
                    data={"timestamp": self.file.timestamp},
                )
 
    def _get_unambiguous_path(self, sqlite_db, datatype):
        """Generate SQLite3 filename for datatype; will fail with generic Python exceptions here or downstream if not writable"""
        if not path.exists(sqlite_db):
            makedirs(sqlite_db)
        return path.join(
            sqlite_db, (
                sub(r'\s+', "_", datatype) + "-" +
                md5(datatype.encode("utf-8")).hexdigest()
            ),
        )
 
    def _drop_mongo_entry(self, cname=COLLECTION_NAMES.FILE_DESCRIPTORS):
        """Erase Mongo DB entry for file descriptor"""
        self.logger.warning(
            CACHED_TABLE_LOGGER_DROP_WARNING,
            self.accession, self.assay_name, self.datatype,
        )
        run_mongo_transaction(
            action="delete_many", collection=getattr(self.mongo_db, cname),
            query={"name": self.file.name, "url": self.file.url},
        )
 
    def _recache(self):
        """Update local table from remote file"""
        try:
            self.data = read_csv(self.file.url, sep=self.file.sep)
            if self.data.columns[0] not in self.sample_names:
                self.data.set_index(self.data.columns[0], inplace=True)
                self.data.index.name = None
            self.data = self.data[self.sample_names]
        except Exception as e:
            self.logger.error(
                CACHED_TABLE_LOGGER_ERROR_MASK, repr(e), self.accession,
                self.assay_name, self.datatype, self.file.url, stack_info=True,
            )
            return False
        else:
            with closing(connect(self.sqlite_db)) as sql_connection:
                try:
                    self.data.to_sql(
                        self.name, sql_connection, if_exists="replace",
                    )
                except Exception as e:
                    self.logger.error(
                        CACHED_TABLE_LOGGER_ERROR_MASK, repr(e), self.accession,
                        self.assay_name, self.datatype, self.file.url,
                        stack_info=True,
                    )
                    sql_connection.rollback()
                    return False
                else:
                    self.logger.info(
                        CACHED_TABLE_LOGGER_SUCCESS_MASK, self.accession,
                        self.assay_name, self.datatype, self.file.url,
                    )
                    sql_connection.commit()
                    return True
 
    def dataframe(self, rows=None):
        """Render retrieved or cached data as pandas.DataFrame"""
        if rows is not None:
            raise NotImplementedError("Selecting rows from a table")
        if self.data is None:
            with closing(connect(self.sqlite_db)) as sql_connection:
                try:
                    data_subset = read_sql(
                        f"SELECT * FROM '{self.name}'",
                        sql_connection, index_col="index",
                    )
                except PandasDatabaseError:
                    self._drop_mongo_entry()
                    raise GeneLabDatabaseException(
                        MISSING_SQL_TABLE_ERROR, self.accession,
                        self.assay_name, datatype=self.datatype,
                    )
                except Exception as e:
                    raise GeneLabDatabaseException(
                        type(e).__name__+ ": " + str(e),
                        self.accession, self.assay_name, datatype=self.datatype,
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
            ))
    joined_table = concat( # this is in-memory and faster than sqlite3:
        # wesmckinney.com/blog/high-performance-database-joins-with-pandas-dataframe-more-benchmarks
        [table.dataframe(rows=rows) for table in tables], axis=1, sort=False,
    )
    joined_table.index.name = ("info", "info", ROW_TYPES[datatype])
    return joined_table.reset_index()
