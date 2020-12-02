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
SQLITE_DB = "./.sqlite3/"

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


## (Meta)data parameters

ISA_ZIP_REGEX = r'.*_metadata_.*[_-]ISA\.zip$'
ANNOTATION_CATEGORIES = {"factor value", "parameter value", "characteristics"}
INFO = "info"
METADATA_UNITS_FORMAT = "{value} {{{unit}}}"
ISA_TECHNOLOGY_TYPE_LOCATOR = (
    "investigation.study assays", "study assay technology type",
)

from collections import namedtuple
Locator = namedtuple("Locator", ["keys", "regex", "row_type"])
TECHNOLOGY_FILE_LOCATORS = {
    "rna sequencing (rna-seq)": {
        "unnormalized counts": Locator(
            keys=(
                "assay.parameter value.raw counts data file..",
                "assay.characteristics.raw counts data file..",
            ),
            regex=r'rna_seq_Unnormalized_Counts\.csv$',
            row_type="entry",
        ),
    }
}

RAW_FILE_REGEX = r'file|plot'
DEG_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression.csv$'
VIZ_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table.csv$'
PCA_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$'
