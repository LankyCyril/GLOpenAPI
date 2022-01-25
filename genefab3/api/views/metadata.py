from marshal import dumps as marsh
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.db.mongo.utils import aggregate_entries_by_context
from pymongo.errors import OperationFailure as MongoOperationError
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.api.renderers.types import StreamedAnnotationTable
from genefab3.common.types import NaN


def squash(cursor):
    """Condense sequential entries from same assay into entries where existing fields resolve to True"""
    hashes, current_id, squashed_entry = set(), None, {}
    def _booleanize(entry):
        if isinstance(entry, dict):
            for key, value in entry.items():
                if key != "id":
                    if isinstance(value, dict):
                        _booleanize(value)
                    else:
                        entry[key] = True
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


def as_leaf_counts(cursor):
    """Aggregate and count metadata values""" # TODO: this is slow-ish, however, it's cached; but MongoDB native aggregation is WIP
    class MetadataValueCounts(dict): # TODO: genefab3.api.renderers.types
        default_format = "json"
        def add(self, keyseq, value):
            leafpile = self
            for key in keyseq:
                if isinstance(leafpile, dict):
                    leafpile = leafpile.setdefault(key, {})
                else:
                    msg = "Sister branches of document have different lengths"
                    raise ValueError(msg)
            try:
                leafpile[value] = leafpile.setdefault(value, 0) + 1
            except TypeError: # unhashable values nested in `value`
                value = "<complex object>"
                leafpile[value] = leafpile.setdefault(value, 0) + 1
    def _iterate_branches(entry, keyseq=(), _is=isinstance):
        if isinstance(entry, dict):
            if "" in entry:
                yield keyseq, str(entry[""])
            else:
                for key, value in entry.items():
                    yield from _iterate_branches(value, (*keyseq, str(key)))
        else:
            yield keyseq, str(entry)
    leafcounter = MetadataValueCounts()
    for entry in cursor:
        for keyseq, value in _iterate_branches(entry):
            leafcounter.add(keyseq, value)
    return leafcounter


def get_raw(*, mongo_collections, locale, context, id_fields):
    """Select assays/samples based on annotation filters; return raw MongoDB cursor and projection"""
    try:
        cursor, full_projection = aggregate_entries_by_context(
            mongo_collections.metadata, context=context, id_fields=id_fields,
            locale=locale,
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
        return cursor, full_projection


def get(*, mongo_collections, locale, context, id_fields, condensed=False, unique_counts=False):
    """Select assays/samples based on annotation filters; optionally count terminal leaf values"""
    cursor, full_projection = get_raw(
        mongo_collections=mongo_collections, id_fields=id_fields,
        context=context, locale=locale,
    )
    if unique_counts:
        return as_leaf_counts(cursor)
    else:
        annotation = StreamedAnnotationTable(
            cursor=squash(cursor) if condensed else cursor,
            full_projection=full_projection, # TODO: exclude sample, study name from /assays/
            na_rep=False if condensed else NaN,
        )
        if annotation.shape[0]:
            return annotation
        else:
            return annotation.placeholder(n_column_levels=2)
