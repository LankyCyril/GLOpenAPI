## Databases

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

ISA_TECH_TYPE_LOCATOR = "investigation.study assays.study assay technology type"
