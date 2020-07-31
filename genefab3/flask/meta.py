from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException
from genefab3.mongo.utils import get_collection_keys
from pandas import DataFrame
from natsort import natsorted
from werkzeug.datastructures import ImmutableMultiDict


def get_meta_names(db, meta, rargs={}):
    """List names of particular meta"""
    if meta not in ASSAY_METADATALIKES:
        raise GeneLabException("Unknown request: '{}'".format(meta))
    else:
        meta_names = DataFrame(
            data=natsorted(
                get_collection_keys(
                    collection=getattr(db, meta),
                    skip={"accession", "assay name", "sample name"},
                )
            ),
            columns=[meta],
        )
        return meta_names, ImmutableMultiDict(rargs)
