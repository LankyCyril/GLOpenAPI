from collections import defaultdict
from natsort import natsorted
from functools import lru_cache
from pathlib import Path
from genefab3.common.utils import map_replace
from json import dumps
from genefab3.api.utils import is_debug


def get_metadata_equals_json(mongo_collections):
    """Generate JSON for documentation section 'meta-equals'"""
    equals_json = defaultdict(dict)
    for entry in mongo_collections.metadata_aux.find():
        equals_json[entry["isa_category"]][entry["subkey"]] = {
            key: {value: True for value in values}
            for key, values in entry["content"].items()
        }
    return dict(equals_json)


def get_metadata_existence_json(equals_json):
    """Generate JSON for documentation section 'meta-existence'"""
    existence_json = defaultdict(dict)
    for isa_category in equals_json:
        for subkey in equals_json[isa_category]:
            existence_json[isa_category][subkey] = {
                key: True for key in equals_json[isa_category][subkey]
            }
    return dict(existence_json)


def get_metadata_wildcards(existence_json):
    """Generate JSON for documentation section 'meta-wildcard'"""
    wildcards = defaultdict(dict)
    for isa_category in existence_json:
        for subkey in existence_json[isa_category]:
            wildcards[isa_category][subkey] = True
    return dict(wildcards)


def get_metadata_assays(mongo_collections):
    """Generate JSON for documentation section 'meta-assay'"""
    metadata_assays = defaultdict(set)
    for entry in mongo_collections.metadata.distinct("info"):
        metadata_assays[entry["accession"]].add(entry["assay"])
    return {
        k: {**{v: True for v in natsorted(metadata_assays[k])}, "": True}
        for k in natsorted(metadata_assays)
    }


def get_metadata_datatypes(mongo_collections):
    """Generate JSON for documentation section 'meta-file-datatype'"""
    # TODO: should be cached at index stage
    cursor = mongo_collections.metadata.aggregate([
        {"$unwind": "$file"},
        {"$project": {"k": "$file.datatype", "_id": False}},
    ])
    return {e["k"]: True for e in cursor if "k" in e}


@lru_cache(maxsize=None)
def _get_root_html():
    """Return text of HTML template"""
    return (Path(__file__).parent / "root.html").read_text()


def get(mongo_collections, context):
    """Serve an interactive documentation page"""
    equals_json = get_metadata_equals_json(mongo_collections)
    existence_json = get_metadata_existence_json(equals_json)
    wildcards = get_metadata_wildcards(existence_json)
    metadata_assays = get_metadata_assays(mongo_collections)
    metadata_datatypes = get_metadata_datatypes(mongo_collections)
    dumps_sorted = lambda j: dumps(j, sort_keys=True)
    return map_replace(
        _get_root_html(), {
            "%URL_ROOT%": context.url_root,
            "/* METADATA_WILDCARDS */": dumps_sorted(wildcards),
            "/* METADATA_EXISTENCE */": dumps_sorted(existence_json),
            "/* METADATA_EQUALS */": dumps_sorted(equals_json),
            "/* METADATA_ASSAYS */": dumps_sorted(metadata_assays),
            "/* METADATA_DATATYPES */": dumps_sorted(metadata_datatypes),
            "<!--DEBUG ": "" if is_debug() else "<!--",
            " DEBUG-->": "" if is_debug() else "-->",
        },
    )