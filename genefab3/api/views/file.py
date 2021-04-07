from genefab3.api.views import metadata
from re import sub
from functools import reduce
from operator import or_
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.utils import copy_and_drop


def get_descriptor_dataframe(mongo_collections, *, locale, context, include=()):
    """Return DataFrame of file descriptors that match user query"""
    _cols = [*{k[0] for k in include if k != "file.filename"}, "file.filename"]
    for key in ["file.filename", *map(".".join, include)]:
        context.update(key)
    annotation = metadata.get(
        mongo_collections, locale=locale, context=context,
        drop_trailing_fields=False, aggregate=False,
    )
    if "file.filename" not in annotation:
        raise FileNotFoundError("No file found matching specified constraints")
    if ("file.filename", "urls") in annotation:
        annotation[("file.filename", "urls")] = (
            annotation[("file.filename", "urls")].apply(sorted).apply(tuple)
        )
    if "file.datatype" in annotation:
        ix = reduce(or_, (
            (annotation[("file.datatype",t)]==annotation[("file.filename","*")])
            for t in {
                sub(r'\.[^.]+$', "", c) for c in
                annotation[["file.datatype"]].columns.get_level_values(1)
            }
        ))
        return annotation[ix][_cols].drop_duplicates()
    else:
        return annotation[_cols].drop_duplicates()


def get(mongo_collections, *, locale, context):
    """Return file if annotation filters constrain search to unique file entity"""
    descriptor_dataframe = get_descriptor_dataframe(
        mongo_collections, locale=locale, context=context,
    )
    if descriptor_dataframe.empty:
        raise FileNotFoundError("No file found matching specified constraints")
    elif len(descriptor_dataframe) > 1:
        raise GeneFabFileException(
            "Multiple files match search criteria",
            filenames=descriptor_dataframe[("file.filename", "*")].tolist(),
        )
    else:
        descriptor = descriptor_dataframe["file.filename"].iloc[0].to_dict()
    if context.format == "json":
        return {"filename": descriptor["*"], **copy_and_drop(descriptor, {"*"})}
    else:
        raise NotImplementedError(f"Redirect to file {descriptor}")
