from genefab3.api.views import metadata
from genefab3.common.types import UniversalSet
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.db.mongo.types import HashableDocument


def get_descriptor(mongo_collections, *, locale, context):
    """Return file descriptor if annotation filters constrain search to unique file entity"""
    annotation = metadata.get(
        mongo_collections, locale=locale, context=context,
        include={"info.sample name", "file.filename"}, aggregate=False,
    )
    if "file.datatype" in annotation:
        target_filenames = set(annotation[["file.datatype"]].values.flatten())
    else:
        target_filenames = UniversalSet()
    if len(target_filenames) > 1:
        msg = "Multiple files match search criteria"
        raise GeneFabFileException(msg, filenames=target_filenames)
    elif "file.filename" not in annotation:
        msg = "No file information recovered for filename"
        raise GeneFabDatabaseException(msg, filename=target_filenames.pop())
    else:
        descriptor_tuples = (
            annotation[["file.filename"]]
            .applymap(lambda vv: tuple(sorted(HashableDocument(v) for v in vv)))
            .drop_duplicates().values.flatten()
        )
        descriptors = {
            d for dt in descriptor_tuples for d in dt
            if d.name in target_filenames
        }
        if len(descriptors) == 0:
            raise GeneFabFileException("No files match search criteria")
        elif len(descriptors) > 1:
            raise GeneFabFileException(
                "Multiple files match search criteria",
                filenames={d.name for d in descriptors},
            )
        else:
            return descriptors.pop()


def get(mongo_collections, *, locale, context):
    """Return file if annotation filters constrain search to unique file entity"""
    descriptor = get_descriptor(
        mongo_collections, locale=locale, context=context,
    )
    if context.kwargs.get("format") == "json":
        return descriptor.as_dict
    else:
        raise NotImplementedError(f"Redirect to file {descriptor.as_dict}")
