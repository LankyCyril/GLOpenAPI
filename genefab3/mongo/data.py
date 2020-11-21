from pandas import Series
from numpy import nan


def get_single_sample_data(sample_info, target_file_regex, gene_rows=None):
    accession, assay_name, sample_name = sample_info
    return Series([nan]*16)
