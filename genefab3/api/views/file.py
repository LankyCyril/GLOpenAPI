from genefab3.api.views import metadata
from re import sub
from functools import reduce
from operator import or_
from genefab3.common.exceptions import GeneFabFileException


def get_descriptor(mongo_collections, locale, context):
    """Return file descriptor if annotation filters constrain search to unique file entity"""
    context.update("file.filename")
    ff_ = ("file.filename", "_")
    annotation = metadata.get(
        mongo_collections, locale=locale, context=context,
        drop_trailing_fields=False, aggregate=False,
    )
    if "file.filename" not in annotation:
        raise FileNotFoundError("No file found matching specified constraints")
    elif "file.datatype" in annotation:
        ix = reduce(or_, (
            (annotation[("file.datatype", target)] == annotation[ff_])
            for target in {
                sub(r'\.[^.]+$', "", c) for c in
                annotation[["file.datatype"]].columns.get_level_values(1)
            }
        ))
        dataframe = annotation[ix].drop_duplicates(ff_)[["file.filename"]]
    else:
        dataframe = annotation.drop_duplicates(ff_)[["file.filename"]]
    if dataframe.empty:
        raise FileNotFoundError("No file found matching specified constraints")
    elif len(dataframe) > 1:
        msg = "Multiple files match search criteria"
        raise GeneFabFileException(msg, filenames=dataframe[ff_].tolist())
    else:
        return dataframe["file.filename"].iloc[0].to_dict()


def get(mongo_collections, *, locale, context):
    """Return file if annotation filters constrain search to unique file entity"""
    descriptor = get_descriptor(mongo_collections, locale, context)
    if context.kwargs.get("format") == "json":
        return descriptor
    else:
        raise NotImplementedError(f"Redirect to file {descriptor}")
