## Flask config

DEBUG_MARKERS = {"development", "staging", "stage", "debug", "debugging"}

COMPRESSIBLE_MIMETYPES = [
    "text/plain", "text/html", "text/css", "text/xml",
    "application/json", "application/javascript"
]

DEFAULT_FORMATS = {
    "/assays/": "tsv", "/samples/": "tsv", "/data/": "tsv", "/files/": "tsv",
}


## Databases and external APIs

MONGO_CLIENT_PARAMETERS = dict(
    serverSelectionTimeoutMS=2000,
)
MONGO_DB_NAME = "genefab3"
MONGO_DB_LOCALE = "en_US"
SQLITE_DIR = "./.sqlite3/"

GENELAB_ROOT = "https://genelab-data.ndc.nasa.gov"
COLD_API_ROOT = "https://genelab-data.ndc.nasa.gov/genelab"
COLD_SEARCH_MASK = COLD_API_ROOT + "/data/search/?term=GLDS&type=cgene&size={}"
COLD_GLDS_MASK = COLD_API_ROOT + "/data/study/data/{}/"
COLD_FILEURLS_MASK = COLD_API_ROOT + "/data/glds/files/{}"
COLD_FILEDATES_MASK = COLD_API_ROOT + "/data/study/filelistings/{}"

TIMESTAMP_FMT = "%a %b %d %H:%M:%S %Z %Y"

MAX_JSON_AGE = 10800 # 3 hours (in seconds)
CACHER_THREAD_CHECK_INTERVAL = 1800 # 30 minutes (in seconds)
CACHER_THREAD_RECHECK_DELAY = 300 # 5 minutes (in seconds)
METADATA_INDEX_WAIT_DELAY = 60 # 1 minute (in seconds)
METADATA_INDEX_WAIT_STEP = 5


## API parser parameters

from operator import eq, ne, gt, getitem, contains, length_hint
leaf_count = lambda d, h: sum(length_hint(v, h) for v in d.values())
listlen = lambda d, k: len(d.getlist(k))

DISALLOWED_CONTEXTS = [
    dict(_="at least one dataset or annotation category must be specified",
        projection=(length_hint, 0, eq, 0), # no projection
        accessions_and_assays=(length_hint, 0, eq, 0), # no datasets
    ),
    dict(_="'fmt=cls' is only valid for /samples/",
        view=(eq, "/samples/", eq, False), kwargs=(getitem, "fmt", eq, "cls"),
    ),
    dict(_="'fmt=gct' is only valid for /data/",
        view=(eq, "/data/", eq, False), kwargs=(getitem, "fmt", eq, "gct"),
    ),
    dict(_="/data/ requires a 'datatype=' argument",
        view=(eq, "/data/", eq, True), kwargs=(contains, "datatype", eq, False),
    ),
    dict(_="/file/ only accepts 'fmt=raw'",
        view=(eq, "/file/", eq, True), kwargs=(getitem, "fmt", ne, "raw"),
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

ISA_ZIP_REGEX = r'.*_metadata_.*[_-]ISA\.zip$'
ANNOTATION_CATEGORIES = {"factor value", "parameter value", "characteristics"}
METADATA_UNITS_FORMAT = "{value} {{{unit}}}"
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

from collections import defaultdict
ROW_TYPES = defaultdict(lambda: "entry", {
    "unnormalized counts": "entry",
})

RAW_FILE_REGEX = r'file|plot'
DEG_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression.csv$'
VIZ_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table.csv$'
PCA_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$'
