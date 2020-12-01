from logging import getLogger, DEBUG
from argparse import Namespace
from genefab3.utils import infer_file_separator
from pymongo import DESCENDING
from genefab3.exceptions import GeneLabFileException, GeneLabDatabaseException
from genefab3.mongo.utils import replace_doc
from pandas import read_csv, read_sql, DataFrame, MultiIndex, concat
from contextlib import closing
from sqlite3 import connect
from collections import defaultdict
from genefab3.mongo.dataset import CachedDataset


NO_FILES_ERROR = "No data files found for"
AMBIGUOUS_FILES_ERROR = "Multiple (ambiguous) data files found for"


class CachedTable():
    """Abstracts SQL table generated from a CSV/TSV file"""
    file = Namespace(name=None, url=None, timestamp=None, sep=None)
    is_fresh, status = None, None
    accession, assay_name, sample_names = None, None, None
    data = None
    logger = None
 
    def __init__(self, mongo_db, sqlite_db_location, file_descriptor, datatype, accession, assay_name, sample_names):
        self.name = f"{datatype}/{accession}/{assay_name}"
        self.logger = getLogger("genefab3")
        self.logger.setLevel(DEBUG)
        self.mongo_db, self.sqlite_db_location = mongo_db, sqlite_db_location
        self.datatype, self.accession, self.assay_name, self.sample_names = (
            datatype, accession, assay_name, sample_names,
        )
        self.file = Namespace(
            name=file_descriptor.filename,
            url=file_descriptor.url,
            timestamp=file_descriptor.timestamp,
            sep=infer_file_separator(file_descriptor.filename),
        )
        cache_entry = self.mongo_db.file_descriptors.find_one(
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
                replace_doc(
                    collection=self.mongo_db.file_descriptors,
                    query={"name": self.file.name, "url": self.file.url},
                    doc={"timestamp": self.file.timestamp},
                )
 
    def _recache(self):
        try:
            self.data = read_csv(self.file.url, sep=self.file.sep, index_col=0)
            # TODO: test index name and make index "only if"; avoid "Unnamed"
        except Exception as e:
            self.logger.error(
                ("CachedTable: %s at accession %s, assay %s, "
                "datatype %s, file url '%s'"),
                repr(e), self.accession, self.assay_name, self.datatype,
                self.file.url, stack_info=True,
            )
            return False
        else:
            with closing(connect(self.sqlite_db_location)) as sql_connection:
                try:
                    self.data.to_sql(
                        self.name, sql_connection, if_exists="replace",
                    )
                except Exception as e:
                    self.logger.error(
                        ("CachedTable: %s at accession %s, assay %s, "
                        "datatype %s, file url '%s'"),
                        repr(e), self.accession, self.assay_name, self.datatype,
                        self.file.url, stack_info=True,
                    )
                    sql_connection.rollback()
                    return False
                else:
                    self.logger.info(
                        ("CachedTable: updated accession %s, assay %s, "
                        "datatype %s, file url '%s'"),
                        self.accession, self.assay_name, self.datatype,
                        self.file.url,
                    )
                    sql_connection.commit()
                    return True
 
    def dataframe(self, rows=None):
        if self.data is None:
            with closing(connect(self.sqlite_db_location)) as sql_connection:
                try:
                    self.data = read_sql(
                        f"SELECT * FROM '{self.name}'",
                        sql_connection, index_col="index",
                    )
                except Exception as e:
                    raise GeneLabDatabaseException(
                        str(e), self.accession, self.assay_name, self.datatype,
                    )
        if not (set(self.sample_names) <= set(self.data.columns)):
            raise GeneLabDatabaseException(
                "Missing sample names in GeneFab database",
                self.accession, self.assay_name,
                sorted(set(self.sample_names) - set(self.data.columns)),
            )
        else:
            return DataFrame(
                data=self.data.values, index=self.data.index,
                columns=MultiIndex.from_tuples(
                    (self.accession, self.assay_name, sample_name)
                    for sample_name in list(self.data.columns)
                )
            )


def sample_index_to_dict(sample_index):
    """Convert a MultiIndex of form (accession, assay_name, sample_name) to a nested dictionary"""
    sample_dict = defaultdict(lambda: defaultdict(set))
    for accession, assay_name, sample_name in sample_index:
        sample_dict[accession][assay_name].add(sample_name)
    return sample_dict


def get_sql_data(mongo_db, sqlite_db_location, sample_index, datatype, target_file_locator, rows=None, index_name="Entry"):
    """Based on a MultiIndex of form (accession, assay_name, sample_name), retrieve data from files in `target_file_locator`"""
    sample_dict = sample_index_to_dict(sample_index)
    tables = []
    for accession in sample_dict:
        glds = CachedDataset(mongo_db, accession, init_assays=False)
        for assay_name, sample_names in sample_dict[accession].items():
            fileinfo = glds.assays[assay_name].get_file_descriptors( # TODO everywhere: file_descriptors, maybe List
                regex=target_file_locator.regex,
                projection={target_file_locator.key: True},
            )
            if len(fileinfo) == 0:
                raise FileNotFoundError(
                    NO_FILES_ERROR, accession, assay_name,
                )
            elif len(fileinfo) > 1:
                raise GeneLabFileException(
                    AMBIGUOUS_FILES_ERROR, accession, assay_name,
                )
            else:
                tables.append(CachedTable(
                    mongo_db=mongo_db,
                    sqlite_db_location=sqlite_db_location,
                    file_descriptor=next(iter(fileinfo.values())),
                    datatype=datatype,
                    accession=accession,
                    assay_name=assay_name,
                    sample_names=sample_names,
                ))
    joined_table = concat( # this is in-memory and faster than sqlite3:
        # wesmckinney.com/blog/high-performance-database-joins-with-pandas-dataframe-more-benchmarks
        [table.dataframe(rows=rows) for table in tables], axis=1, sort=False,
    )
    joined_table.index.name = ("Index", "Index", index_name)
    return joined_table.reset_index()
