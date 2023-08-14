from functools import wraps
from types import SimpleNamespace
from glopenapi.common.types import NaN
from glopenapi.api.renderers.types import StreamedDataTable
from sqlite3 import OperationalError, connect
from glopenapi.common.exceptions import GLOpenAPIDatabaseException
from pandas import DataFrame
from pandas import concat, Index
from glopenapi.common.exceptions import GLOpenAPIFormatException
from glopenapi.common.exceptions import GLOpenAPILogger
from collections import OrderedDict
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
from re import search


def apply_hack(hack):
    """Wraps `method` with function `hack`"""
    def outer(method):
        @wraps(method)
        def inner(*args, **kwargs):
            return hack(method, *args, **kwargs)
        return inner
    return outer


def convert_legacy_material_type(self, _name="convert_legacy_material_type"):
    """Convert 'material type' legacy fields in MongoDB database from as-in-GeneLab to as-supposed-to-be-in-GeneLab """
    collection = self.mongo_collections.metadata
    updated_accessions = set()
    source = "study.material type"
    destination = "study.characteristics.material type"
    get_source_value = lambda e: e.get("study", {}).get("material type", None)
    query = {source: {"$exists": True}, destination: {"$exists": False}}
    for entry in collection.find(query):
        prefix = f"apply_hack({_name}) on {entry['id']!r}:\n "
        value = get_source_value(entry)
        if isinstance(value, (list, tuple)):
            if len(value) == 1:
                value = value[0]
            else:
                msg = f"{prefix} unexpected value of '{source}': {value!r}"
                GLOpenAPILogger.warning(msg)
                value = None
        if isinstance(value, str):
            value = {"": value}
        if (not isinstance(value, dict)) or ("" not in value):
            msg = f"{prefix} unexpected value of '{source}': {value!r}"
            GLOpenAPILogger.warning(msg)
            value = None
        if value is not None:
            if search(r'\s+$', value[""]):
                msg = f"{prefix} removing empty '{source}' value"
                collection.update_one(
                    {"_id": entry["_id"]}, {"$unset": {source: True}},
                )
                GLOpenAPILogger.info(msg)
            else:
                msg = f"{prefix} setting '{destination}' to {value}"
                GLOpenAPILogger.info(msg)
                collection.update_one(
                    {"_id": entry["_id"]}, {"$set": {destination: value}},
                )
            updated_accessions.add(entry["id"]["accession"])
    return updated_accessions


def remove_legacy_metadata_empty_values(self, _name="remove_legacy_metadata_empty_values"):
    """Remove 'study.characteristics.material type' entries where the value is entirely made of space-like characters"""
    collection = self.mongo_collections.metadata
    updated_accessions = set()
    source = "study.material type"
    source_query = {source: {"$elemMatch": {"": {"$exists": False}}}}
    for entry in collection.find(source_query):
        prefix = f"apply_hack({_name}) on {entry['id']!r}:\n "
        GLOpenAPILogger.info(f"{prefix} removing empty '{source}'")
        collection.update_one(
            {"_id": entry["_id"]}, {"$unset": {source: True}},
        )
        updated_accessions.add(entry["id"]["accession"])
    destination = "study.characteristics.material type"
    destination_query = {f"{destination}.": {"$regex": '^\s+$'}}
    for entry in collection.find(destination_query):
        prefix = f"apply_hack({_name}) on {entry['id']!r}:\n "
        GLOpenAPILogger.info(f"{prefix} removing empty '{destination}'")
        collection.update_one(
            {"_id": entry["_id"]}, {"$unset": {destination: True}},
        )
        updated_accessions.add(entry["id"]["accession"])
    return updated_accessions


def precache_metadata_counts(recache_metadata, self):
    """Precache the rather large /metadata-counts/?[...] JSON before users start asking for it"""
    from glopenapi.api import views
    from glopenapi.api.parser import Context
    for fmt in None, "json":
        prefix = "apply_hack(precache_metadata_counts):\n "
        GLOpenAPILogger.info(f"{prefix} rendering + storing with format={fmt}")
        context = SimpleNamespace(
            view="metadata-counts", query={}, unwind=set(), projection={
                "id.accession": True,
                "id.assay name": True,
                "investigation.study assays.study assay technology type": True,
                "investigation.study.comment.mission name": True,
                "investigation.study.comment.project type": True,
                "investigation.study.study title": True,
                "study.characteristics.material type": True,
                "study.characteristics.organism": True,
                "study.factor value": True,
            },
            data_columns=[], data_comparisons=[], schema="0", debug="0",
            sort_by=["id.accession", "id.assay name"], format=fmt,
        )
        context.identity = Context._make_identity(context)
        self.glopenapi_client.renderer._get_response_container_via_cache(
            context=context, method=views.metadata.get, args=(), kwargs=dict(
                mongo_collections=self.glopenapi_client.mongo_collections,
                id_fields=["accession", "assay name", "sample name"],
                condensed=False, unique_counts=True,
                locale=self.glopenapi_client.locale,
            ),
        )
        GLOpenAPILogger.info(f"{prefix} completed for format={fmt}")
    # Note that `recache_metadata` happens *after* we precache the counts JSON;
    # while yes, technically, this makes the cached counts lag by one cycle, but
    # 1) that is true for all cached responses (!!),
    # 2) if any accessions are dropped/updated/failed, the /metadata-counts/
    #    response would immediately be invalidated and deleted from the cache,
    # 3) if no accessions are dropped/updated/failed after `recache_metadata`,
    #    the /metadata-counts/ will remain in the cache and be valid.
    # See note in glopenapi.client.GLOpenAPIClient._ensure_cacher_loop_thread()
    # for details and rationalization of this approach in general.
    return recache_metadata(self)


def convert_legacy_metadata_pre(recache_metadata, self):
    """Run legacy metadata converters before recache_metadata()"""
    updated_accessions = (
        convert_legacy_material_type(self) |
        remove_legacy_metadata_empty_values(self)
    )
    accessions, success = recache_metadata(self)
    accessions["updated"] |= updated_accessions
    accessions["fresh"] -= updated_accessions
    return accessions, success


def convert_legacy_metadata_post(recache_single_dataset_metadata, self, accession, has_cache):
    """Run legacy metadata converters after recache_single_dataset_metadata()"""
    collection = self.mongo_collections.metadata
    with collection.database.client.start_session() as session:
        with session.start_transaction():
            result = recache_single_dataset_metadata(self, accession, has_cache)
            convert_legacy_material_type(self)
            remove_legacy_metadata_empty_values(self)
    return result


def get_sub_df(obj, partname, partcols):
    """Retrieve only informative values from single part of table as pandas.DataFrame"""
    from glopenapi.db.sql.streamed_tables import SQLiteIndexName
    found = lambda v: v is not None
    index_name, data = None, {}
    with obj.sqltransactions.concurrent("hacks/get_sub_df") as (_, execute):
        try:
            fetch = lambda query: execute(query).fetchone()
            mktargets = lambda f: ",".join(f"{f}(`{c}`)" for c in partcols)
            mkquery = lambda t: f"SELECT {t} FROM `{partname}` LIMIT 1"
            minima = fetch(mkquery(mktargets("MIN")))
            maxima = fetch(mkquery(mktargets("MAX")))
            counts = fetch(mkquery(mktargets("COUNT")))
            n_rows = fetch(f"SELECT COUNT(*) FROM `{partname}` LIMIT 1")[0]
            hasnan = [(n_rows - c) > 0 for c in counts]
            for c, m, M, h in zip(partcols, minima, maxima, hasnan):
                _min = m if found(m) else M if found(M) else NaN
                _max = M if found(M) else _min
                _nan = NaN if h else _max
                if isinstance(c, SQLiteIndexName):
                    index_name = str(c)
                data[c] = [_min, _max, _nan]
        except OperationalError as exc:
            msg = "Data could not be retrieved"
            _kw = dict(table=obj.name, debug_info=repr(exc))
            raise GLOpenAPIDatabaseException(msg, **_kw)
    dataframe = DataFrame(data)
    if index_name is not None:
        dataframe.set_index(index_name, inplace=True)
    return dataframe


def get_part_index(obj, partname):
    """Retrieve index values (row name) of part of `obj`"""
    index_query = f"SELECT `{obj._index_name}` FROM `{partname}`"
    with obj.sqltransactions.concurrent("hacks/get_part_index") as (_, execute):
        return {ix for ix, *_ in execute(index_query)}


def merge_subs(self, sub_dfs, sub_indices):
    """Merge subs 'by hand,' focing NaNs into subs whose full indices are smaller than full index pool"""
    index_pool = set.union(set(), *sub_indices.values())
    def _inject_NaN_if_outer(partname, sub_df):
        if sub_indices.get(partname, set()) != index_pool:
            mod_sub_df = sub_df.copy()
            mod_sub_df.iloc[-1] = NaN
            return mod_sub_df
        else:
            return sub_df
    sub_merged = concat(axis=1, objs=[
        _inject_NaN_if_outer(partname, sub_df).reset_index(drop=True)
        for partname, sub_df in sub_dfs.items()
    ])
    ixs = sum([list(sub_df.index) for sub_df in sub_dfs.values()], [])
    min_ix = min(ix for ix in ixs if ix == ix)
    max_ix = max(ix for ix in ixs if ix == ix)
    nan_ix = NaN if any(ix != ix for ix in ixs) else max_ix
    sub_merged.index = Index([min_ix, max_ix, nan_ix], name=self._index_name)
    return sub_merged


class StreamedDataTableSub(StreamedDataTable):
    """StreamedDataTable-like class that streams from underlying pandas.DataFrame"""
 
    def __init__(self, sub_merged, sub_columns, na_rep=None):
        self.shape, self.n_index_levels = tuple(sub_merged.shape), 1
        self._dataframe = sub_merged
        self._index_name = sub_merged.index.name
        self._columns = sub_columns
        self.na_rep = na_rep
        self.accessions = {c[0] for c in self._columns}
        self.datatypes, self.gct_validity_set = set(), set()
 
    @property
    def index(self):
        """Iterate index line by line, like in pandas"""
        if self.n_index_levels:
            if self.na_rep is None:
                for value in self._dataframe.index:
                    yield (value,)
            else:
                _na_tup = (self.na_rep,)
                for value in self._dataframe.index:
                    yield _na_tup if value is None else (value,)
        else:
            yield from ([] for _ in range(self.shape[0]))
 
    @property
    def values(self):
        """Iterate values line by line, like in pandas"""
        if self.na_rep is None:
            if self.n_index_levels:
                for _, vv in self._dataframe.iterrows():
                    yield vv.tolist()
            else:
                for r, vv in self._dataframe.iterrows():
                    yield (r, *vv)
        else:
            if self.n_index_levels:
                for _, vv in self._dataframe.iterrows():
                    yield [self.na_rep if v is None else v for v in vv.tolist()]
            else:
                for r, vv in self._dataframe.iterrows():
                    yield [self.na_rep if v is None else v for v in (r, *vv)]


def speed_up_data_schema(get, self, *, context, limit=None, offset=0):
    """If context.schema == '1', replaces underlying query with quick retrieval of just values informative for schema"""
    if context.schema != "1":
        return get(self, context=context, limit=limit, offset=offset)
    elif context.data_columns or context.data_comparisons:
        msg = "Data schema does not support column subsetting / comparisons"
        sug = "Remove comparisons and/or column, row slicing from query"
        raise GLOpenAPIFormatException(msg, suggestion=sug)
    else:
        from glopenapi.db.sql.streamed_tables import (
            SQLiteIndexName,
            StreamedDataTableWizard_Single, StreamedDataTableWizard_OuterJoined,
        )
        msg = f"apply_hack(speed_up_data_schema) for {self.name}"
        GLOpenAPILogger.info(msg)
        sub_dfs, sub_indices = OrderedDict(), {}
        sub_columns, index_name = [], []
        def _extend_parts(obj):
            for partname, partcols in obj._inverse_column_dispatcher.items():
                if isinstance(partcols[0], SQLiteIndexName):
                    index_name.clear()
                    index_name.append(partcols[0])
                    sub_df = get_sub_df(obj, partname, partcols)
                else:
                    sub_df = get_sub_df(obj, partname, [*index_name, *partcols])
                sub_indices[partname] = get_part_index(obj, partname)
                sub_dfs[partname] = sub_df
                _ocr2f = obj._columns_raw2full
                sub_columns.extend(_ocr2f[c] for c in sub_df.columns)
        if isinstance(self, StreamedDataTableWizard_Single):
            _extend_parts(self)
        elif isinstance(self, StreamedDataTableWizard_OuterJoined):
            for obj in self.objs:
                _extend_parts(obj)
        else:
            msg = "Schema speedup applied to unsupported object type"
            raise GLOpenAPIConfigurationException(msg, type=type(self))
        sub_merged = merge_subs(self, sub_dfs, sub_indices)
        return StreamedDataTableSub(sub_merged, sub_columns)


def bypass_uncached_views(get, self, context, desc="hacks/bypass_uncached_views"):
    """If serving images, static libs, bypass checking response_cache"""
    is_image = search(r'^images\/', context.view)
    is_favicon = search(r'^favicon\.[A-Za-z0-9]+$', context.view)
    is_static_lib = search(r'^libs\/', context.view)
    if is_image or is_favicon or is_static_lib:
        GLOpenAPILogger.info(f"{desc}: bypass caching of {context.view!r}")
        from glopenapi.api.types import ResponseContainer
        return ResponseContainer(content=None)
    else:
        return get(self, context)


def TEMP_force_recache_datasets_with_RSEM(recache_metadata, self):
    """Drop everything cached before 2023-08-13 for accessions OSD-502, OSD-508, OSD-510"""
    _name = "TEMP_force_recache_datasets_with_RSEM"
    BREAKPOINT_TS = 1692048434
    blobs_db = self.DatasetConstructor.keywords["sqlite_dbs"].blobs["db"]
    with connect(blobs_db, isolation_level=None) as conn:
        for acc in 502, 508, 510:
            conn.execute("BEGIN")
            oacc, gacc = f"OSD-{acc}", f"GLDS-{acc}"
            blob_name = f"BLOB:{oacc}/ISA/{oacc}_metadata_{gacc}-ISA.zip"
            tss = conn.execute(f"""
                SELECT `retrieved_at` FROM `BLOBS:blobs`
                WHERE `identifier` = "{blob_name}"
                AND `retrieved_at` < {BREAKPOINT_TS}
            """)
            if min((float("inf"), *(row[0] for row in tss))) < BREAKPOINT_TS:
                prefix = f"apply_hack({_name}) on {oacc}:\n "
                conn.execute(f"""
                    DELETE FROM `BLOBS:blobs` WHERE `identifier` = "{blob_name}"
                """)
                GLOpenAPILogger.info(f"{prefix} dropped ISA blob")
                self.mongo_collections.metadata.delete_many(
                    {"id.accession": oacc},
                )
                GLOpenAPILogger.info(f"{prefix} dropped MongoDB entries")
            conn.execute("COMMIT")
    return recache_metadata(self)


class NoCommitConnection():
    """Wrapper for sqlite3 connection that forcefully prevents commits (for pandas.to_sql)"""
    def __init__(self, connection):
        self.__connection = connection
    def __getattr__(self, attr):
        if attr == "commit":
            return lambda: None
        else:
            return getattr(self.__connection, attr)


def ExecuteMany(partname, width):
    """Generate a `method` function for `pandas.to_sql` that uses `connection.executemany`"""
    qmarks = ",".join(["?"]*(width+1)) # include index
    def mkinsert(pd_table, conn, keys, data_iter, name=partname):
        conn.executemany(f"INSERT INTO `{name}` VALUES({qmarks})", data_iter)
    return mkinsert


class ForceShowAllDataColumns():
    """Sentinel"""
    pass


class HackyBlackHoleList(list):
    """Empty list that cannot be filled, but does not generate errors on attempts"""
    def append(self, *a, **k): pass
    def extend(self, *a, **k): pass
    def __iadd__(self, *a, **k): return self
    def __imul__(self, *a, **k): return self
