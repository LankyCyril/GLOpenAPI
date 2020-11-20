from genefab3.flask.meta import get_samples_by_metas
from pandas import DataFrame, concat
from functools import partial
from genefab3.mongo.data import get_single_sample_data


def get_data_by_metas(db, context):
    """Select data based on annotation filters"""
    annotation_by_metas = get_samples_by_metas(db, context)
    info_cols = list(annotation_by_metas["info"].columns)
    sample_index = annotation_by_metas["info"].set_index(info_cols).index
    gene_rows = None
    return DataFrame(
        data=concat(
            sample_index.map(
                partial(get_single_sample_data, gene_rows=gene_rows),
            ),
            axis=1,
        ),
        columns=sample_index,
    )
