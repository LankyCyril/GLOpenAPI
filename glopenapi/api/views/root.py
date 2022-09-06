from collections import defaultdict
from natsort import natsorted
from pathlib import Path
from json import dumps
from glopenapi.common.exceptions import is_debug, GLOpenAPIConfigurationException
from glopenapi.api.renderers.BrowserStreamedTableRenderers import _iter_html_chunks
from glopenapi.api.renderers.types import StreamedString


def get_metadata_equals_json(mongo_collections):
    """Generate JSON for documentation section 'meta-equals'"""
    equals_json = defaultdict(dict)
    for entry in mongo_collections.metadata_aux.find():
        equals_json[entry["isa_category"]][entry["subkey"]] = {
            key: {v.replace("\n", r'\n').replace("\t", r'\t'): True for v in vv}
            for key, vv in entry["content"].items()
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
    for entry in mongo_collections.metadata.distinct("id"):
        metadata_assays[entry["accession"]].add(entry["assay name"])
    return {
        k: {**{f"/{v}": True for v in natsorted(metadata_assays[k])}, "": True}
        for k in natsorted(metadata_assays)
    }


def get_metadata_datatypes(mongo_collections): # TODO: should be cached at index stage
    """Generate JSON for documentation section 'meta-file-datatype'"""
    cursor = mongo_collections.metadata.aggregate([
        {"$unwind": "$file"},
        {"$project": {"k": "$file.datatype", "_id": False}},
    ])
    return {e["k"]: True for e in cursor if "k" in e}


def get(*, glopenapi_client, mongo_collections, context):
    """Serve an interactive documentation page"""
    replacements = {
        "$APPNAME": context.app_name,
        "$APP_VERSION": glopenapi_client.app_version,
        "$URL_ROOT": context.url_root,
        "<!--DEBUG ": "" if is_debug() else "<!--",
        " DEBUG-->": "" if is_debug() else "-->",
    }
    if context.view == "":
        template_file = Path(__file__).parent / "root.html"
        default_format = "html"
    elif context.view == "root.js":
        equals_json = get_metadata_equals_json(mongo_collections)
        existence_json = get_metadata_existence_json(equals_json)
        wildcards = get_metadata_wildcards(existence_json)
        metadata_assays = get_metadata_assays(mongo_collections)
        metadata_datatypes = get_metadata_datatypes(mongo_collections)
        dumps_as_is = lambda j: dumps(j, separators=(",", ":"))
        dumps_sorted = lambda j: dumps(j, separators=(",", ":"), sort_keys=True)
        replacements = {
            **replacements,
            "$METADATA_WILDCARDS": dumps_sorted(wildcards),
            "$METADATA_EXISTENCE": dumps_sorted(existence_json),
            "$METADATA_EQUALS": dumps_sorted(equals_json),
            "$METADATA_ASSAYS": dumps_as_is(metadata_assays),
            "$METADATA_DATATYPES": dumps_sorted(metadata_datatypes),
        }
        template_file = Path(__file__).parent / "root.js"
        default_format = "raw" # TODO: format should be javascript
    else:
        _kw = dict(view=context.view)
        raise GLOpenAPIConfigurationException("Unconfigured view", **_kw)
    return StreamedString(
        lambda: _iter_html_chunks(template_file, replacements),
        default_format=default_format, cacheable=True,
    )
