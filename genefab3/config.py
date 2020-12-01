from collections import namedtuple


MONGO_DB_NAME = "genefab3"
SQLITE_DB_LOCATION = "./.sqlite3/genefab3.sqlite"

DEBUG_MARKERS = {"development", "staging", "stage", "debug", "debugging"}

COMPRESSIBLE_MIMETYPES = [
    "text/plain", "text/html", "text/css", "text/xml",
    "application/json", "application/javascript"
]

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

ISA_ZIP_REGEX = r'.*_metadata_.*[_-]ISA\.zip$'

ANNOTATION_CATEGORIES = {"factor value", "parameter value", "characteristics"}

DEFAULT_FORMATS = {
    "/assays/": "tsv", "/samples/": "tsv", "/data/": "tsv", "/files/": "tsv",
}

ISA_TECHNOLOGY_TYPE_LOCATOR = (
    "investigation.study assays", "study assay technology type",
)

TECHNOLOGY_FILE_LOCATORS = {
    "rna sequencing (rna-seq)": {
        "unnormalized counts": namedtuple("Locator", ["keys", "regex"])(
            keys=(
                "assay.parameter value.raw counts data file..",
                "assay.characteristics.raw counts data file..",
            ),
            regex=r'rna_seq_Unnormalized_Counts\.csv$',
        ),
    }
}

RAW_FILE_REGEX = r'file|plot'
DEG_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression.csv$'
VIZ_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table.csv$'
PCA_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$'
