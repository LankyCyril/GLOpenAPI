from marshal import dumps as marsh
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.db.mongo.utils import aggregate_entries_by_context
from pymongo.errors import OperationFailure as MongoOperationError
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.common.types import StreamedAnnotationTable, NaN


def squash(cursor):
    """Condense sequential entries from same assay into entries where existing fields resolve to True"""
    hashes, current_id, squashed_entry = set(), None, {}
    def _booleanize(entry):
        if isinstance(entry, dict):
            for k, v in entry.items():
                if k != "id":
                    if isinstance(v, dict):
                        _booleanize(v)
                    else:
                        entry[k] = True
        return entry
    for entry in cursor:
        if entry["id"] != current_id:
            if current_id is not None:
                yield _booleanize(squashed_entry)
                squashed_entry = {}
            current_id, _hash = entry["id"], marsh(entry["id"], 4)
            if _hash in hashes:
                msg = "Retrieved metadata was not sorted"
                raise GeneFabConfigurationException(msg)
            else:
                hashes.add(_hash)
        squashed_entry.update(entry)
    if current_id is not None:
        yield _booleanize(squashed_entry)


def get(*, mongo_collections, locale, context, id_fields, condensed=False):
    """Select assays/samples based on annotation filters"""
    try:
        cursor = aggregate_entries_by_context(
            mongo_collections.metadata, locale=locale, context=context,
            id_fields=id_fields,
        )
    except MongoOperationError as e:
        errmsg = getattr(e, "details", {}).get("errmsg", "").lower()
        has_index = ("id" in mongo_collections.metadata.index_information())
        index_reason = ("index" in errmsg)
        if index_reason and (not has_index):
            msg = "Metadata is not indexed yet; this is a temporary error"
        else:
            msg = "Could not retrieve sorted metadata"
        raise GeneFabDatabaseException(msg, locale=locale, reason=str(e))
    else:
        annotation = StreamedAnnotationTable(
            cursor=squash(cursor) if condensed else cursor,
            na_rep=False if condensed else NaN,
        )
    if annotation.shape[0]:
        return annotation
    else:
        return annotation.placeholder(n_column_levels=2)
