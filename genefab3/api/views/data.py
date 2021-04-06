from genefab3.api.views.file import get_descriptor_dataframe


TECH_TYPE_LOCATOR = "investigation.study assays", "study assay technology type"


def get(mongo_collections, *, locale, context):
    """Return file if annotation filters constrain search to unique file entity"""
    return get_descriptor_dataframe(
        mongo_collections, locale, context, include={TECH_TYPE_LOCATOR},
    )
