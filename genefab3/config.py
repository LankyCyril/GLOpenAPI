MONGO_DB_NAME = "genefab3"

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

INDEX_BY = "Sample Name"
TIMESTAMP_FMT = "%a %b %d %H:%M:%S %Z %Y"

MAX_JSON_AGE = 10800 # 3 hours (in seconds)

DEG_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression.csv$'
VIZ_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table.csv$'
PCA_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$'
