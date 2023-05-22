from requests import get as request_get
from glopenapi.common.exceptions import GLOpenAPILogger
from urllib.error import URLError
from json import dumps
from json.decoder import JSONDecodeError
from glopenapi.common.exceptions import GLOpenAPIDataManagerException
from pandas import json_normalize
from glopenapi.common.types import Adapter
from glopenapi.common.utils import pick_reachable_url
from types import SimpleNamespace
from re import search, sub
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
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


class GeneLabAdapter(Adapter):
 
    def __init__(self, root_urls=["https://osdr.nasa.gov", "https://genelab-data.ndc.nasa.gov"]):
        with pick_reachable_url(root_urls) as root:
            self.constants = SimpleNamespace(
                GENELAB_ROOT=root,
                SEARCH_MASK=root+"/genelab/data/search?type=cgene&size={}",
                FILES_MASK=root+"/genelab/data/glds/files/{}?all_files=True",
            )
            self._legacy_constants = SimpleNamespace(
                GLDS_MASK=root+"/genelab/data/study/data/{}/",
                FILELISTINGS_MASK=root+"/genelab/data/study/filelistings/{}",
                SHORT_MEDIA_PATH=root+"/genelab/static/media/dataset/",
            )
        super().__init__()
 
    def get_accessions(self):
        """Return list of dataset accessions available through genelab.nasa.gov/genelabAPIs"""
        # return ["OSD-42", "OSD-4", "OSD-168", "OSD-48"] # hehe
        try:
            n_datasets_url = self.constants.SEARCH_MASK.format(0)
            n_datasets = read_json(n_datasets_url)["hits"]["total"]
            datasets_url = self.constants.SEARCH_MASK.format(n_datasets)
            return {
                entry["_source"]["Accession"]
                for entry in read_json(datasets_url)["hits"]["hits"]
            }
        except (KeyError, TypeError):
            raise GLOpenAPIDataManagerException("Malformed GeneLab search JSON")
 
    def _format_file_entry(self, row, accession):
        """Format filelisting dataframe row to include URLs, timestamp, datatype, rules"""
        filename, entry = row["file_name"], {
            "urls": [(
                self.constants.GENELAB_ROOT + row["remote_url"] +
                (f"?version={row['version']}" if "version" in row else "")
            )],
            "timestamp": row["timestamp"],
        }
        matched_patterns = set()
        for pattern, metadata in KNOWN_DATATYPES.items():
            if search(pattern, filename):
                entry.update(metadata)
                matched_patterns.add(pattern)
                GLOpenAPILogger.debug(
                    f"File pattern match: accession={accession}, " +
                    f"urls={entry['urls']}, filename={filename}, " +
                    f"pattern={pattern}, metadata={metadata}"
                )
        if len(matched_patterns) > 1:
            msg = "File name matches more than one predefined pattern"
            _kw = dict(filename=filename, debug_info=sorted(matched_patterns))
            raise GLOpenAPIConfigurationException(msg, **_kw)
        return filename, entry
 
    def get_files_by_accession(self, accession):
        """Get dictionary of files for dataset available through genelab.nasa.gov/genelabAPIs"""
        _id_match = search(r'[0-9]+', accession)
        if not _id_match:
            GLOpenAPILogger.warning(f"Not a numeric accession: {accession}")
            return {}
        else:
            _id = _id_match.group()
            try:
                url = self.constants.FILES_MASK.format(_id)
                files_json = read_json(url)
                assert files_json["hits"] == 1
                study_files = files_json["studies"][accession]["study_files"]
                assert isinstance(study_files, list)
            except (AssertionError, KeyError, TypeError):
                raise GLOpenAPIDataManagerException(
                    "Malformed 'files' JSON", accession=accession, _id=_id,
                    url=url, object_type=type(files_json).__name__,
                    expected_type="list",
                )
            else:
                files = json_normalize(study_files)
            if "date_updated" not in files:
                GLOpenAPILogger.warning(f"No 'date_updated' fields: {url}")
                files["date_updated"] = float("nan")
            files["timestamp"] = (
                files[["date_created", "date_updated"]].max(axis=1).astype(int)
            )
            files_entries = dict((
                self._format_file_entry(row, accession)
                for _, row in files.sort_values(by="timestamp").iterrows()
            ))
            files_entries_repr = dumps(files_entries, indent=4)
            msg = f"Files ({accession}) (url={url}): {files_entries_repr}"
            GLOpenAPILogger.debug(msg)
            return files_entries
 
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
