from functools import lru_cache
from pandas import Series
from genefab3.api.views.file import get_descriptor_dataframe
from genefab3.common.exceptions import GeneFabFileException


TECH_TYPE_LOCATOR = "investigation.study assays", "study assay technology type"


def validate_joinable_files(descriptor_dataframe):
    """Check for ability to join data from requested files"""
    getset = lru_cache(maxsize=None)(lambda col, default=None:
        set(descriptor_dataframe.get(col, Series(default)).drop_duplicates())
    )
    if len(getset(("file.filename", "datatype"))) > 1:
        msg = "Cannot combine data of multiple datatypes"
        return msg, dict(datatypes=getset(("file.filename", "datatype")))
    elif len(getset(TECH_TYPE_LOCATOR)) > 1:
        msg = "Cannot combine data for multiple technology types"
        return msg, dict(technology_types=getset(TECH_TYPE_LOCATOR))
    elif getset(("file.filename", "joinable")) != {True}:
        return "Cannot combine multiple files of this datatype", dict(
            datatype=getset(("file.filename", "datatype")).pop(),
            filenames=getset(("file.filename", "*")),
        )
    elif getset(("file.filename", "type")) != {"table"}:
        msg = "Cannot combine non-table files"
        return msg, dict(types=getset(("file.filename", "type")))
    elif len(getset(("file.filename", "index_name"))) > 1:
        msg = "Cannot combine tables with conflicting index names"
        return msg, dict(index_names=getset(("file.filename", "index_name")))
    else:
        return None, {}


def get(mongo_collections, *, locale, context):
    """Return data corresponding to search parameters; merged if multiple underlying files are same type and joinable"""
    descriptor_dataframe = get_descriptor_dataframe(
        mongo_collections, locale=locale, context=context,
        include={TECH_TYPE_LOCATOR},
    )
    if descriptor_dataframe.empty:
        raise FileNotFoundError("No file found matching specified constraints")
    elif ("file.filename", "*") not in descriptor_dataframe:
        raise FileNotFoundError("No file found matching specified constraints")
    elif len(descriptor_dataframe) > 1:
        msg, _kw = validate_joinable_files(descriptor_dataframe)
        if msg:
            raise GeneFabFileException(msg, **_kw)
    files = descriptor_dataframe["file.filename"].to_dict(orient="records")
    return files
