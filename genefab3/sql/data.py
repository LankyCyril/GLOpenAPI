from pandas import Series, read_csv, concat, MultiIndex
from numpy import nan
from collections import defaultdict
from genefab3.exceptions import GeneLabFileException
from genefab3.mongo.dataset import CachedDataset


NO_FILES_ERROR = "No data files found for"
AMBIGUOUS_FILES_ERROR = "Multiple (ambiguous) data files found for"


def sample_index_to_dict(sample_index):
    sample_dict = defaultdict(lambda: defaultdict(set))
    for accession, assay_name, sample_name in sample_index:
        sample_dict[accession][assay_name].add(sample_name)
    return sample_dict # {k: {v: w for v, w in d.items()} for k, d in sample_dict.items()}


def get_sql_data(db, sample_index, target_file_locator, gene_rows=None):
    sample_dict = sample_index_to_dict(sample_index)
    tables = []
    for accession in sample_dict:
        glds = CachedDataset(db, accession, init_assays=False)
        for assay_name, sample_names in sample_dict[accession].items():
            fileinfo = glds.assays[assay_name].get_file_descriptors(
                regex=target_file_locator.regex,
                projection={target_file_locator.key: True},
            )
            if len(fileinfo) == 0:
                raise GeneLabFileException(
                    NO_FILES_ERROR, accession, assay_name,
                )
            elif len(fileinfo) > 1:
                raise GeneLabFileException(
                    AMBIGUOUS_FILES_ERROR, accession, assay_name,
                )
            else: # TODO well this is a complete kludge but will do for today as a concept
                table = read_csv(next(iter(fileinfo.values())).url, index_col=0)
                table = table[sorted(sample_names)]
                table.index.name = "Entry"
                table.columns = MultiIndex.from_tuples(
                    (accession, assay_name, sample_name)
                    for sample_name in sorted(sample_names)
                )
                tables.append(table)
    return concat(tables, axis=0, sort=False).reset_index()[::-20]


def get_data_placeholder(sample_info, target_file_locator, gene_rows=None):
    accession, assay_name, sample_name = sample_info
    return Series([nan]*16)
