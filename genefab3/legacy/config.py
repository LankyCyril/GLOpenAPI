## Databases

MAX_JSON_AGE = 10800 # 3 hours (in seconds)
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


## (Meta)data parameters

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
