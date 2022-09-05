from requests import get as request_get
from glopenapi.common.exceptions import GLOpenAPILogger
from urllib.error import URLError
from json.decoder import JSONDecodeError
from glopenapi.common.exceptions import GLOpenAPIDataManagerException
from pandas import Timestamp, json_normalize
from glopenapi.common.types import Adapter
from glopenapi.common.utils import pick_reachable_url
from types import SimpleNamespace
from urllib.parse import quote
from re import search, sub
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
from warnings import catch_warnings, filterwarnings
from dateutil.parser import UnknownTimezoneWarning
from functools import lru_cache


datatype = lambda t, **kw: dict(datatype=t, **kw)
tabletype = lambda t, **kw: dict(datatype=t, **kw, cacheable=True, type="table")

get_tech_type = lambda sample: (sample
    .get("Investigation", {}).get("Study Assays", {})
    .get("Study Assay Technology Type", "").lower()
)
is_microarray = lambda sample, filename: get_tech_type(sample) in {
    "microarray", "dna microarray",
}
is_rna_seq = lambda sample, filename: get_tech_type(sample) in {
    "rna sequencing (rna-seq)",
}
is_expression_profiling = lambda sample, filename: (
    is_microarray(sample, filename) or is_rna_seq(sample, filename)
)

KNOWN_DATATYPES = {
    r'.*_metadata_.*\.zip$': datatype("isa", internal=True),
    r'^GLDS-[0-9]+_.*annotReport\.txt$': datatype("annotation report"),
    r'^GLDS-[0-9]+_.*_raw\.fastq(\.gz)?$': datatype("raw reads"),
    r'^GLDS-[0-9]+_.*_trimmed\.fastq(\.gz)?$': datatype("trimmed reads"),
    r'^GLDS-[0-9]+_.*\.bam$': datatype("alignment"),
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
        tabletype("processed microarray data",
            column_subset="sample name", gct_valid=True),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_Normalized_Counts\.csv$':
        tabletype("normalized counts",
            column_subset="sample name", gct_valid=True),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_Unnormalized_Counts\.csv$':
        tabletype("unnormalized counts", joinable=True,
            column_subset="sample name", gct_valid=True),
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression\.csv$':
        tabletype("differential expression"),
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_contrasts\.csv$':
        datatype("differential expression contrasts"),
    r'^GLDS-[0-9]+_array(_all-samples)?_visualization_output_table\.csv$':
        tabletype("visualization table",
            internal=True, condition=is_microarray),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_visualization_output_table\.csv$':
        tabletype("visualization table",
            internal=True, condition=is_rna_seq),
    r'^GLDS-[0-9]+_array(_all-samples)?_visualization_PCA_table\.csv$':
        tabletype("pca", index_name="sample name", index_subset="sample name",
            internal=True, condition=is_microarray),
    r'^GLDS-[0-9]+_rna_seq(_all-samples)?_visualization_PCA_table\.csv$':
        tabletype("pca", index_name="sample name", index_subset="sample name",
            internal=True, condition=is_rna_seq),
}


def read_json(url):
    """Get parsed JSON from URL"""
    try:
        with request_get(url) as response:
            GLOpenAPILogger.debug(f"Reading from URL: {url}")
            return response.json()
    except (URLError, OSError):
        raise GLOpenAPIDataManagerException("Not found", url=url)
    except JSONDecodeError:
        raise GLOpenAPIDataManagerException("Malformed data returned", url=url)


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


class GeneLabAdapter(Adapter):
 
    def __init__(self, root_urls=["https://genelab-data.ndc.nasa.gov"]):
        with pick_reachable_url(root_urls) as genelab_root:
            self.constants = SimpleNamespace(
                GENELAB_ROOT=genelab_root,
                COLD_SEARCH_MASK=(
                    genelab_root +
                    "/genelab/data/search?term=GLDS&type=cgene&size={}"
                ),
                COLD_GLDS_MASK=genelab_root + "/genelab/data/study/data/{}/",
                COLD_FILELISTINGS_MASK=(
                    genelab_root + "/genelab/data/study/filelistings/{}"
                ),
                SHORT_MEDIA_PATH="/genelab/static/media/dataset/",
            )
        super().__init__()
 
    def get_accessions(self):
        """Return list of dataset accessions available through genelab.nasa.gov/genelabAPIs"""
        try:
            n_datasets_url = self.constants.COLD_SEARCH_MASK.format(0)
            n_datasets = read_json(n_datasets_url)["hits"]["total"]
            datasets_url = self.constants.COLD_SEARCH_MASK.format(n_datasets)
            return {e["_id"] for e in read_json(datasets_url)["hits"]["hits"]}
        except (KeyError, TypeError):
            raise GLOpenAPIDataManagerException("Malformed GeneLab search JSON")
 
    def _format_file_entry(self, row):
        """Format filelisting dataframe row to include URLs, timestamp, datatype, rules"""
        filename = row["file_name"]
        version_info = "?version={}".format(row["version"])
        entry = {
            "urls": [
                (self.constants.GENELAB_ROOT + self.constants.SHORT_MEDIA_PATH +
                    quote(filename) + version_info),
                (self.constants.GENELAB_ROOT + quote(row["remote_url"]) +
                    version_info),
            ],
            "timestamp": row["timestamp"],
        }
        matched_patterns = set()
        for pattern, metadata in KNOWN_DATATYPES.items():
            if search(pattern, filename):
                entry.update(metadata)
                matched_patterns.add(pattern)
        if len(matched_patterns) > 1:
            msg = "File name matches more than one predefined pattern"
            _kw = dict(filename=filename, debug_info=sorted(matched_patterns))
            raise GLOpenAPIConfigurationException(msg, **_kw)
        return entry
 
    def get_files_by_accession(self, accession):
        """Get dictionary of files for dataset available through genelab.nasa.gov/genelabAPIs"""
        try:
            url = self.constants.COLD_GLDS_MASK.format(accession)
            glds_json = read_json(url)
            assert len(glds_json) == 1
            _id = glds_json[0]["_id"]
        except (AssertionError, IndexError, KeyError, TypeError):
            raise GLOpenAPIDataManagerException(
                "Malformed GLDS JSON", accession=accession,
                url=url, object_type=type(glds_json).__name__,
                length=getattr(glds_json, "__len__", lambda: None)(),
                target="[0]['_id']",
            )
        try:
            url = self.constants.COLD_FILELISTINGS_MASK.format(_id)
            filelisting_json = read_json(url)
            assert isinstance(filelisting_json, list)
        except AssertionError:
            raise GLOpenAPIDataManagerException(
                "Malformed 'filelistings' JSON", accession=accession, _id=_id,
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
            row["file_name"]: self._format_file_entry(row)
            for _, row in files.sort_values(by="timestamp").iterrows()
        }
 
    def best_sample_name_matches(self, name, names, return_positions=False):
        """Match ISA sample names to their variants in data files (R-like dot-separated, postfixed)"""
        dotted = lru_cache(maxsize=None)(
            lambda s: sub(r'[._-]', ".", s) if isinstance(s, str) else None
        )
        positions_and_matches = [
            (p, ns) for p, ns in enumerate(names) if ns == name
        ]
        if not positions_and_matches:
            positions_and_matches = [
                (p, ns) for p, ns in enumerate(names)
                if dotted(ns) == dotted(name)
            ]
        if not positions_and_matches:
            positions_and_matches = [
                (p, ns) for p, ns in enumerate(names)
                if dotted(ns) and dotted(name) and (
                    dotted(ns).startswith(dotted(name)) or
                    dotted(name).startswith(dotted(ns))
                )
            ]
        if return_positions:
            return (
                [ns for p, ns in positions_and_matches],
                [p for p, ns in positions_and_matches],
            )
        else:
            return [ns for p, ns in positions_and_matches]
