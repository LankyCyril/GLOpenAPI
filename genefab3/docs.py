from json import dumps
from copy import copy
from os.path import join, split, abspath
from genefab3.utils import map_replace


JSON_TEMPLATE = {
    "investigation": {
        "study": "true",
        "study assays": "true",
        "investigation": "true",
    },
    "study": {
        "characteristics": "true",
        "factor value": "true",
        "parameter value": "true",
    },
    "assay": {
        "characteristics": "true",
        "factor value": "true",
        "parameter value": "true",
    },
}

FINAL_KEY_BLACKLIST = {"comment"}


def get_metadata_wildcard_json(db):
    """Generate JSON for documentation section 'meta-wildcard'"""
    return dumps(JSON_TEMPLATE)


def get_metadata_existence_json(db):
    """Generate JSON for documentation section 'meta-existence'"""
    json = copy(JSON_TEMPLATE)
    for isa_category in JSON_TEMPLATE:
        for subkey in JSON_TEMPLATE[isa_category]:
            raw_next_level_keyset = set.union(*(
                set(entry[isa_category][subkey].keys()) for entry in
                db.metadata.find(
                    {isa_category+"."+subkey: {"$exists": True}},
                    {isa_category+"."+subkey: True},
                )
            ))
            json[isa_category][subkey] = {
                next_level_key: True for next_level_key in
                raw_next_level_keyset - FINAL_KEY_BLACKLIST
            }
    return dumps(json)


def interactive_doc(db, html_path=None, document="docs.html", url_root="/"):
    """Serve an interactive documentation page"""
    if html_path is None:
        html_path = join(
            split(split(abspath(__file__))[0])[0],
            "html", document
        )
    try:
        with open(html_path, mode="rt") as handle:
            template = handle.read()
        documentation_exists = True
    except (FileNotFoundError, OSError, IOError):
        template = "Hello, Space! (No documentation at %URL_ROOT%)"
        documentation_exists = False
    if documentation_exists:
        return map_replace(
            template, {
                "%URL_ROOT%": url_root,
                "/* METADATA_WILDCARDS */": get_metadata_wildcard_json(db),
                "/* METADATA_EXISTENCE */": get_metadata_existence_json(db),
            },
        )
    else:
        return map_replace(template, {"%URL_ROOT%": url_root})
