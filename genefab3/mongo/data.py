from pandas import Series
from numpy import nan


def get_single_sample_data(sample_info, gene_rows=None):
    accession, assay_name, sample_name = sample_info
    print("Getting data for", sample_name)
    return Series([nan]*16)
