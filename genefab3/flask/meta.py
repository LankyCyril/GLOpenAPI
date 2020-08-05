from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from genefab3.mongo.utils import get_collection_fields
from pandas import DataFrame
from natsort import natsorted


def get_meta_names(db, meta, rargs=None):
    """List names of particular meta"""
    if meta not in ASSAY_METADATALIKES:
        raise GeneLabException("Unknown request: '{}'".format(meta))
    else:
        return DataFrame(
            data=natsorted(
                get_collection_fields(
                    collection=getattr(db, meta),
                    skip={"accession", "assay name", "sample name"},
                )
            ),
            columns=[meta],
        )
