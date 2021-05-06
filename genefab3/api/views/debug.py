from genefab3.common.types import StreamedTable, NaN
from genefab3.db.mongo.utils import aggregate_entries_by_context
from itertools import tee, count, chain
from genefab3.common.utils import blackjack
from collections import OrderedDict
from pymongo.errors import OperationFailure as MongoOperationError
from genefab3.common.exceptions import GeneFabDatabaseException


class AnnotationRowIterator(StreamedTable):
    prefix_order = "id", "investigation", "study", "assay", "file", ""
 
    def __init__(self, *, mongo_collections, locale, context, id_fields):
        """Make and retain forked MongoDB aggregation cursors, infer index names and columns in order `self.prefix_order`"""
        cursor, _ = aggregate_entries_by_context(
            mongo_collections.metadata, locale=locale, context=context,
            id_fields=id_fields,
        )
        self._cursors = dict(zip(
            ("keys", "index", "values", "rows"),
            tee(cursor, 4),
        ))
        self.accessions, key_pool, _nrows = set(), set(), 0
        for _nrows, entry in enumerate(self._cursors["keys"], 1):
            for key, value in blackjack(entry, max_level=2):
                key_pool.add(key)
                if key == "id.accession":
                    self.accessions.add(value)
        key_order = {p: set() for p in self.prefix_order}
        for key in key_pool:
            for prefix in self.prefix_order:
                if key.startswith(prefix):
                    key_order[prefix].add(key)
                    break
        self._index_key_dispatcher = OrderedDict(zip(
            sorted(key_order["id"]), count(),
        ))
        self._column_key_dispatcher = OrderedDict(zip(
            sum((sorted(key_order[p]) for p in self.prefix_order[1:]), []),
            count(),
        ))
        self._index_nlevels = len(self._index_key_dispatcher)
        self.shape = (_nrows, len(self._column_key_dispatcher))
 
    @property
    def empty(self):
        return len(self._column_key_dispatcher) == 0
 
    def _iter_header_levels(self, dispatcher):
        fields_and_bounds = [
            (ff, 2 - (ff[0] in {"id", "file"}))
            for ff in (c.split(".") for c in dispatcher)
        ]
        yield [".".join(ff[:b]) or "*" for ff, b in fields_and_bounds]
        yield [".".join(ff[b:]) or "*" for ff, b in fields_and_bounds]
 
    @property
    def index_levels(self):
        yield from self._iter_header_levels(self._index_key_dispatcher)
 
    @property
    def column_levels(self):
        yield from self._iter_header_levels(self._column_key_dispatcher)
 
    def _iter_body_levels(self, cursor, dispatcher):
        for entry in cursor:
            level = [NaN] * len(dispatcher)
            for key, value in blackjack(entry, max_level=2):
                if key in dispatcher:
                    level[dispatcher[key]] = value
            yield level
 
    @property
    def index(self):
        dispatcher = self._index_key_dispatcher
        yield from self._iter_body_levels(self._cursors["index"], dispatcher)
 
    @property
    def values(self):
        dispatcher = self._column_key_dispatcher
        yield from self._iter_body_levels(self._cursors["values"], dispatcher)
 
    @property
    def rows(self):
        dispatcher = OrderedDict(zip(
            chain(self._index_key_dispatcher, self._column_key_dispatcher),
            count(),
        ))
        yield from self._iter_body_levels(self._cursors["rows"], dispatcher)


def get(*, mongo_collections, locale, context, id_fields, aggregate=False):
    """Select assays/samples based on annotation filters"""
    try:
        annotation = AnnotationRowIterator(
            mongo_collections=mongo_collections, locale=locale,
            context=context, id_fields=id_fields,
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
    if annotation.empty:
        return None # TODO (["id"] * len(id_fields)), (); id_fields, ()
    else:
        return annotation
