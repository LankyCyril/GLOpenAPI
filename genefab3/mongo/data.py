from pandas import Series, concat


def get_single_sample_data(triple):
    return Series(map(str.upper, triple))


def query_data(sample_columns, gene_rows=None):
    sample_data = concat(sample_columns.map(get_single_sample_data), axis=1)
    sample_data.columns = sample_columns
    return sample_data
