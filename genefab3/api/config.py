ANNOTATION_CATEGORIES = {"factor value", "parameter value", "characteristics"}

DEFAULT_FORMATS = {
    "/assays/": "tsv", "/samples/": "tsv", "/data/": "tsv", "/files/": "tsv",
}

from operator import eq, ne, gt, getitem, contains, length_hint
not_in = lambda v, s: v not in s
listlen = lambda d, k: len(d.getlist(k))
leaf_count = lambda d, h: sum(length_hint(v, h) for v in d.values())

DISALLOWED_CONTEXTS = [
    dict(_="at least one dataset or annotation category must be specified",
        view=(eq, "/status/", eq, False), # TODO FIXME allow for favicon
        projection=(length_hint, 0, eq, 0), # no projection
        accessions_and_assays=(length_hint, 0, eq, 0), # no datasets
    ),
    dict(_="metadata queries are not valid for /status/",
        view=(eq, "/status/", eq, True), query=(leaf_count, 0, gt, 0),
    ),
    dict(_="'format=cls' is only valid for /samples/",
        view=(eq, "/samples/", eq, False), kwargs=(getitem, "format", eq, "cls"),
    ),
    dict(_="/data/ requires a 'datatype=' argument",
        view=(eq, "/data/", eq, True), kwargs=(contains, "datatype", eq, False),
    ),
    dict(_="'format=gct' is only valid for /data/",
        view=(eq, "/data/", eq, False), kwargs=(getitem, "format", eq, "gct"),
    ),
    dict(_="'format=gct' is not valid for the requested datatype",
        kwargs=[
            (getitem, "format", eq, "gct"),
            (getitem, "datatype", not_in, {"unnormalized counts"}),
        ],
    ),
    dict(_="/file/ only accepts 'format=raw'",
        view=(eq, "/file/", eq, True), kwargs=(getitem, "format", ne, "raw"),
    ),
    dict(_="/file/ requires at most one 'filename=' argument",
        view=(eq, "/file/", eq, True), kwargs=(listlen, "filename", gt, 1),
    ),
    dict(_="/file/ requires a single dataset in the 'from=' argument",
        view=(eq, "/file/", eq, True),
        accessions_and_assays=(length_hint, 0, ne, 1), # no. of datasets != 1
    ),
    dict(_="/file/ metadata categories are only valid for lookups in assays",
        view=(eq, "/file/", eq, True),
        accessions_and_assays=(leaf_count, 0, eq, 0), # no. of assays == 0
        projection=(length_hint, 0, gt, 0), # projection present
    ),
    dict(_="/file/ accepts at most one metadata category for lookups in assays",
        view=(eq, "/file/", eq, True),
        accessions_and_assays=(leaf_count, 0, eq, 1), # no. of assays == 1
        projection=(length_hint, 0, gt, 1), # many fields to look in
    ),
    dict(_="/file/ requires at most one assay in the 'from=' argument",
        view=(eq, "/file/", eq, True),
        accessions_and_assays=(leaf_count, 0, gt, 1), # no. of assays > 1
    ),
]
