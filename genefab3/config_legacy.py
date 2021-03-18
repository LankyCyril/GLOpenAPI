## Flask config


COMPRESSIBLE_MIMETYPES = [
    "text/plain", "text/html", "text/css", "text/xml",
    "application/json", "application/javascript"
]

DEFAULT_FORMATS = {
    "/assays/": "tsv", "/samples/": "tsv", "/data/": "tsv", "/files/": "tsv",
}


## Databases

MONGO_CLIENT_PARAMETERS = dict(
    serverSelectionTimeoutMS=2000,
)
MONGO_DB_NAME = "genefab3_testing"
MONGO_DB_LOCALE = "en_US"

MAX_JSON_AGE = 10800 # 3 hours (in seconds)
CACHER_THREAD_CHECK_INTERVAL = 1800 # 30 minutes (in seconds)
CACHER_THREAD_RECHECK_DELAY = 300 # 5 minutes (in seconds)
METADATA_INDEX_WAIT_DELAY = 60 # 1 minute (in seconds)
METADATA_INDEX_WAIT_STEP = 5

from types import SimpleNamespace
COLLECTION_NAMES = SimpleNamespace(
    STATUS="status",
    LOG="log",
    JSON_CACHE="json_cache",
    DATASET_TIMESTAMPS="dataset_timestamps",
    METADATA="metadata",
    METADATA_VALUE_LOOKUP="metadata_value_lookup",
    FILE_DESCRIPTORS="file_descriptors",
)

SQLITE_DB = "./.sqlite3/data_testing.db"

RESPONSE_CACHE = "./.sqlite3/response-cache_testing.db"
USE_RESPONSE_CACHE = True
ZLIB_COMPRESS_RESPONSE_CACHE = True
RESPONSE_CACHE_MAX_SIZE = 24*1024*1024*1024 # 24 GiB

from werkzeug.datastructures import ImmutableDict
RESPONSE_CACHE_SCHEMAS = ImmutableDict({
    "response_cache": """(
        'context_identity' TEXT, 'api_path' TEXT, 'timestamp' INTEGER,
        'response' BLOB, 'nbytes' INTEGER, 'mimetype' TEXT
    )""",
    "accessions_used": """(
        'context_identity' TEXT, 'accession' TEXT
    )""",
})


## GeneFab API parser parameters

from operator import eq, ne, gt, getitem, contains, length_hint
not_in = lambda v, s: v not in s
listlen = lambda d, k: len(d.getlist(k))
leaf_count = lambda d, h: sum(length_hint(v, h) for v in d.values())

DISALLOWED_CONTEXTS = [
    dict(_="at least one dataset or annotation category must be specified",
        view=(eq, "/status/", eq, False),
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


## (Meta)data parameters

ANNOTATION_CATEGORIES = {"factor value", "parameter value", "characteristics"}
UNITS_FORMAT = "{value} {{{unit}}}"
ISA_TECH_TYPE_LOCATOR = "investigation.study assays.study assay technology type"

from collections import namedtuple
Locator = namedtuple("Locator", ["keys", "regex"])

TECHNOLOGY_FILE_LOCATORS = {
    "rna sequencing (rna-seq)": {
        "unnormalized counts": Locator(
            keys=(
                "assay.parameter value.raw counts data file..",
                "assay.characteristics.raw counts data file..",
            ),
            regex=r'rna_seq_Unnormalized_Counts\.csv$',
        ),
    }
}

RAW_FILE_REGEX = r'file|plot'
