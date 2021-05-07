from marshal import dumps as marsh
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.types import StreamedAnnotationTable, NaN
from genefab3.db.mongo.utils import aggregate_entries_by_context
from genefab3.common.utils import ForkableIterator, blackjack, KeyToPosition
from pymongo.errors import OperationFailure as MongoOperationError
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.api.renderers import Placeholders


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
                        entry[k] = "True"
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


class _StreamedAnnotationTable(StreamedAnnotationTable):
    _prefix_order = "id", "investigation", "study", "assay", "file", ""
    _accession_key = "id.accession"
 
    def __init__(self, *, mongo_collections, locale, context, id_fields, condense):
        """Make and retain forked MongoDB aggregation cursors, infer index names and columns in order `self._prefix_order`"""
        cursor = aggregate_entries_by_context(
            mongo_collections.metadata, locale=locale, context=context,
            id_fields=id_fields,
        )
        if condense:
            cursor, self._na_rep = squash(cursor), "False"
        else:
            self._na_rep = NaN
        self._cursors = ForkableIterator(cursor)
        self.accessions, _key_pool, _nrows = set(), set(), 0
        for _nrows, entry in enumerate(self._cursors(), 1):
            for key, value in blackjack(entry, max_level=2):
                _key_pool.add(key)
                if key == self._accession_key:
                    self.accessions.add(value)
        _key_order = {p: set() for p in self._prefix_order}
        for key in _key_pool:
            for prefix in self._prefix_order:
                if key.startswith(prefix):
                    _key_order[prefix].add(key)
                    break
        self._index_key_dispatcher = KeyToPosition(sorted(_key_order["id"]))
        self._column_key_dispatcher = KeyToPosition(
            *(sorted(_key_order[p]) for p in self._prefix_order[1:]),
        )
        self.n_index_levels = len(self._index_key_dispatcher)
        self.shape = (_nrows, len(self._column_key_dispatcher))
 
    def move_index_boundary(self, *, to):
        """Like pandas methods reset_index() and set_index(), but by numeric position"""
        keys = iter([*self._index_key_dispatcher, *self._column_key_dispatcher])
        index_keys = (next(keys) for _ in range(to))
        self._index_key_dispatcher = KeyToPosition(index_keys)
        self._column_key_dispatcher = KeyToPosition(keys)
        self.shape = (self.shape[0], len(self._column_key_dispatcher))
        self.n_index_levels = len(self._index_key_dispatcher)
 
    def _iter_header_levels(self, dispatcher):
        fields_and_bounds = [
            (ff, 2 - (ff[0] in {"id", "file"}))
            for ff in (c.split(".") for c in dispatcher)
        ]
        yield [".".join(ff[:b]) or "*" for ff, b in fields_and_bounds]
        yield [".".join(ff[b:]) or "*" for ff, b in fields_and_bounds]
 
    @property
    def index_levels(self):
        """Iterate index level line by line"""
        yield from self._iter_header_levels(self._index_key_dispatcher)
 
    @property
    def index_names(self):
        """Iterate index names column by column, like in pandas"""
        yield from zip(*list(self.index_levels))
 
    @property
    def column_levels(self):
        """Iterate column levels line by line"""
        yield from self._iter_header_levels(self._column_key_dispatcher)
 
    @property
    def columns(self):
        """Iterate column names column by column, like in pandas"""
        yield from zip(*list(self.column_levels))
 
    @property
    def metadata_columns(self):
        """List columns under any ISA category"""
        return [c for c in self.columns if c[0] not in {"id", "file"}]
 
    @property
    def cls_valid(self):
        """Test if valid for CLS, i.e. has exactly one metadata column"""
        return len(self.metadata_columns) == 1
 
    def _iter_body_levels(self, cursor, dispatcher):
        for entry in cursor:
            level = [self._na_rep] * len(dispatcher)
            for key, value in blackjack(entry, max_level=2):
                if key in dispatcher:
                    level[dispatcher[key]] = value
            yield level
 
    @property
    def index(self):
        dispatcher = self._index_key_dispatcher
        yield from self._iter_body_levels(self._cursors(), dispatcher)
 
    @property
    def values(self):
        dispatcher = self._column_key_dispatcher
        yield from self._iter_body_levels(self._cursors(), dispatcher)


def get(*, mongo_collections, locale, context, id_fields, condense=False):
    """Select assays/samples based on annotation filters"""
    try:
        annotation = _StreamedAnnotationTable(
            mongo_collections=mongo_collections, locale=locale,
            context=context, id_fields=id_fields, condense=condense,
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
    if annotation.shape[0]:
        return annotation
    else:
        return Placeholders.EmptyStreamedAnnotationTable(id_fields=id_fields)
