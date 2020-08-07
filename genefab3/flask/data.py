from genefab3.flask.meta import get_info_cols, get_samples_by_metas
from genefab3.mongo.data import query_data


def get_data_by_metas(db, context):
    """Select data based on annotation filters"""
    _, info_multicols = get_info_cols(sample_level=True)
    annotation_by_metas = get_samples_by_metas(db, context)
    sample_columns = annotation_by_metas[info_multicols].set_index(
        info_multicols
    ).index
    return query_data(sample_columns)
