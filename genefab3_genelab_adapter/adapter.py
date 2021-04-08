from urllib.request import urlopen
from json import loads
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabDataManagerException
from pandas import Timestamp, json_normalize
from urllib.parse import quote
from re import search, sub
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.types import Adapter
from natsort import natsorted
from genefab3.common.exceptions import GeneFabJSONException
from warnings import catch_warnings, filterwarnings
from dateutil.parser import UnknownTimezoneWarning
from functools import lru_cache


GENELAB_ROOT = "https://genelab-data.ndc.nasa.gov"
COLD_API_ROOT = GENELAB_ROOT + "/genelab"
COLD_SEARCH_MASK = COLD_API_ROOT + "/data/search/?term=GLDS&type=cgene&size={}"
COLD_GLDS_MASK = COLD_API_ROOT + "/data/study/data/{}/"
COLD_FILELISTINGS_MASK = COLD_API_ROOT + "/data/study/filelistings/{}"
ALT_FILEPATH = "/genelab/static/media/dataset/"

datatype = lambda t, **kwargs: dict(datatype=t, **kwargs)
CACHEABLE_TABLE = dict(cacheable=True, type="table")

sample_name_matches = lambda sample, filename: (
    sample.name and (sample.name in filename)
)
get_tech_type = lambda sample: (sample
    .get("Investigation", {}).get("Study Assays", {})
    .get("Study Assay Technology Type", "").lower()
)
is_expression_profiling = lambda sample, filename: get_tech_type(sample) in {
    "rna sequencing (rna-seq)", "microarray", "dna microarray",
}

KNOWN_DATATYPES = {
    r'.*_metadata_.*[_-]ISA\.zip$': datatype("isa", internal=True),
    r'^GLDS-[0-9]+_.*annotReport\.txt$': datatype("annotation report"),
    r'^GLDS-[0-9]+_.*_raw\.fastq(\.gz)?$':
        datatype("raw reads", condition=sample_name_matches),
    r'^GLDS-[0-9]+_.*_trimmed\.fastq(\.gz)?$':
        datatype("trimmed reads", condition=sample_name_matches),
    r'^GLDS-[0-9]+_.*\.bam$':
        datatype("alignment", condition=sample_name_matches),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_raw_multiqc_data\.zip$':
        datatype("raw multiqc data"),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_raw_multiqc_report\.htm(l)?$':
        datatype("raw multiqc report"),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_trimmed_multiqc_data\.zip$':
        datatype("trimmed multiqc data"),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_trimmed_multiqc_report\.htm(l)?$':
        datatype("trimmed multiqc report"),
    r'^GLDS-[0-9]+_array(_all-samples)?_normalized[_-]annotated\.rda$':
        datatype("processed microarray data (rda)"),
    r'^GLDS-[0-9]+_array(_all-samples)?_normalized[_-]annotated\.txt$':
        datatype("processed microarray data", **CACHEABLE_TABLE),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_Normalized_Counts\.csv$':
        datatype("normalized counts", **CACHEABLE_TABLE,
            column_subset="sample name"),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_Unnormalized_Counts\.csv$':
        datatype("unnormalized counts", **CACHEABLE_TABLE,
            joinable=True, column_subset="sample name"),
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression\.csv$':
        datatype("differential expression", **CACHEABLE_TABLE),
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_contrasts\.csv$':
        datatype("differential expression contrasts"),
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table\.csv$':
        datatype("visualization table", **CACHEABLE_TABLE,
            internal=True, condition=is_expression_profiling),
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table\.csv$':
        datatype("pca", **CACHEABLE_TABLE, index_name="sample name",
            internal=True, condition=is_expression_profiling),
}


def read_json(url):
    """Get parsed JSON from URL"""
    try:
        with urlopen(url) as response:
            return loads(response.read().decode())
    except URLError:
        raise GeneFabDataManagerException("Not found", url=url)


def as_timestamp(dataframe, column, default=-1):
    """Convert dataframe[column] to Unix timestamp, filling in `default` where not available"""
    if column not in dataframe:
        return default
    else:
        def safe_timestamp(value):
            try:
                return int(Timestamp(value).timestamp())
            except ValueError:
                return default
        return dataframe[column].apply(safe_timestamp)


def format_file_entry(row):
    """Format filelisting dataframe row to include URLs, timestamp, datatype, rules"""
    filename = row["file_name"]
    version_info = "?version={}".format(row["version"])
    entry = {
        "urls": [
            GENELAB_ROOT + quote(row["remote_url"]) + version_info,
            GENELAB_ROOT + ALT_FILEPATH + quote(filename) + version_info,
        ],
        "timestamp": row["timestamp"],
    }
    matched_patterns = set()
    for pattern, metadata in KNOWN_DATATYPES.items():
        if search(pattern, filename):
            entry.update(metadata)
            matched_patterns.add(pattern)
    if len(matched_patterns) > 1:
        raise GeneFabConfigurationException(
            "File name matches more than one predefined pattern",
            filename=filename, patterns=sorted(matched_patterns),
        )
    return entry


class GeneLabAdapter(Adapter):
 
    def get_accessions(self):
        """Return list of dataset accessions available through genelab.nasa.gov/genelabAPIs"""
        try:
            n_datasets = read_json(COLD_SEARCH_MASK.format(0))["hits"]["total"]
            return natsorted(
                entry["_id"] for entry in
                read_json(COLD_SEARCH_MASK.format(n_datasets))["hits"]["hits"]
            )
        except (KeyError, TypeError):
            raise GeneFabJSONException("Malformed GeneLab search JSON")
 
    def get_files_by_accession(self, accession):
        """Get dictionary of files for dataset available through genelab.nasa.gov/genelabAPIs"""
        try:
            url = COLD_GLDS_MASK.format(accession)
            glds_json = read_json(url)
            assert len(glds_json) == 1
            _id = glds_json[0]["_id"]
        except (AssertionError, IndexError, KeyError, TypeError):
            raise GeneFabJSONException(
                "Malformed GLDS JSON", accession,
                url=url, object_type=type(glds_json).__name__,
                length=getattr(glds_json, "__len__", lambda: None)(),
                target="[0]['_id']",
            )
        try:
            url = COLD_FILELISTINGS_MASK.format(_id)
            filelisting_json = read_json(url)
            assert isinstance(filelisting_json, list)
        except AssertionError:
            raise GeneFabJSONException(
                "Malformed 'filelistings' JSON", accession, _id=_id,
                url=url, object_type=type(filelisting_json).__name__,
                expected_type="list",
            )
        else:
            files = json_normalize(filelisting_json)
        with catch_warnings():
            filterwarnings("ignore", category=UnknownTimezoneWarning)
            files["date_created"] = as_timestamp(files, "date_created")
            files["date_modified"] = as_timestamp(files, "date_modified")
        files["timestamp"] = files[["date_created", "date_modified"]].max(axis=1)
        return {
            row["file_name"]: format_file_entry(row)
            for _, row in files.sort_values(by="timestamp").iterrows()
        }
 
    def best_sample_name_matches(self, name, names):
        """Match ISA sample names to their variants in data files (R-like dot-separated, postfixed)"""
        dotted = lru_cache(maxsize=None)(lambda s: sub(r'[._-]', ".", s))
        matches = [ns for ns in names if ns == name]
        if matches:
            return matches
        else:
            matches = [ns for ns in names if dotted(ns) == dotted(name)]
            if matches:
                return matches
            else:
                return [
                    ns for ns in names if
                    dotted(ns).startswith(dotted(name)) or
                    dotted(name).startswith(dotted(ns))
                ]
