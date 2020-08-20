from pandas import read_csv
from re import search, sub
from genefab3.exceptions import GeneLabISAException
from argparse import Namespace
from urllib.request import urlopen
from os import path
from zipfile import ZipFile
from io import BytesIO, StringIO
from logging import getLogger, CRITICAL
from isatools.isatab import load_investigation
from collections import defaultdict


INVESTIGATION_KEYS = {
    "Ontology Source Reference": "ontology_sources",
    "Investigation": "investigation",
    "Investigation Publications": "i_publications",
    "Investigation Contacts": "i_contacts",
    "Study": "studies",
    "Studies": "studies",
    "Study Design Descriptors": "s_design_descriptors",
    "Study Publications": "s_publications",
    "Study Factors": "s_factors",
    "Study Assays": "s_assays",
    "Study Protocols": "s_protocols",
    "Study Contacts": "s_contacts",
}


class Investigation(dict):
    """Stores GLDS ISA Tab 'investigation' in accessible formats"""
 
    def __init__(self, raw_investigation):
        for key, internal_key in INVESTIGATION_KEYS.items():
            if internal_key in raw_investigation:
                content = raw_investigation[internal_key]
                if isinstance(content, list):
                    json = [self._jsonify(df) for df in content]
                else:
                    json = self._jsonify(content)
                if isinstance(json, list):
                    if (len(json) == 1) and isinstance(json[0], list):
                        json = json[0]
                super().__setitem__(key, json)
 
    def _jsonify(self, df):
        nn = range(0, df.shape[1])
        return df.drop(columns=nn, errors="ignore").to_dict(orient="records")


class StudyEntries(list):
    """Stores GLDS ISA Tab 'studies' records as a multilevel JSON"""
    _self_identifier = "Study"
    _by_sample_name = {}
 
    def __init__(self, raw_dataframes):
        """Convert tables to multilevel JSONs"""
        for name, raw_dataframe in raw_dataframes.items():
            sample_names = set()
            for _, row in raw_dataframe.iterrows():
                if "Sample Name" not in row:
                    error = "Table entry must have 'Sample Name'"
                    raise GeneLabISAException(error)
                else:
                    sample_name = row["Sample Name"]
                if sample_name in sample_names:
                    error = "Table file contains duplicate Sample Names"
                    raise GeneLabISAException(error)
                else:
                    sample_names.add(sample_name)
                json = self._row_to_json(row, name)
                super().append(json)
                if self._self_identifier == "Study":
                    if sample_name in self._by_sample_name:
                        error = "Duplicate Sample Name in studies"
                        raise GeneLabISAException(error)
                    else:
                        self._by_sample_name[sample_name] = json
 
    def _row_to_json(self, row, name):
        """Convert single row of table to multilevel JSON"""
        json, qualifiable = {"": {self._self_identifier: name}}, None
        for column, value in row.items():
            field, subfield, extra = self._parse_field(column)
            if not self._is_known_qualifier(column): # top-level field
                if not subfield: # e.g. "Source Name"
                    if field in json:
                        error = "Duplicate field '{}'".format(field)
                        raise GeneLabISAException(error)
                    else: # make {"Source Name": {"": "ABC"}}
                        json[field] = {"": value}
                        qualifiable = json[field]
                else: # e.g. "Characteristics[Age]"
                    if field not in json:
                        json[field] = {}
                    if subfield in json[field]:
                        raise GeneLabISAException(
                            "Duplicate field '{}[{}]'".format(field, subfield),
                        )
                    else: # make {"Characteristics": {"Age": {"": "36"}}}
                        json[field][subfield] = {"": value}
                        qualifiable = json[field][subfield]
            else: # qualify entry at pointer with second-level field
                if qualifiable is None:
                    raise GeneLabISAException("Qualifier before main field")
                if field == "Comment": # make {"Comment": {"mood": "cheerful"}}
                    if "Comment" not in qualifiable:
                        qualifiable["Comment"] = {"": None}
                    qualifiable["Comment"][subfield or ""] = value
                elif subfield:
                    ... # TODO
                    # log(f"Ignoring extra info past qualifier '{field}'")
                else: # make {"Unit": "percent"}
                    qualifiable[field] = value
        return json
 
    def _parse_field(self, column):
        """Interpret field like 'Source Name' or 'Characteristics[sex,http://purl.obolibrary.org/obo/PATO_0000047,EFO]"""
        matcher = search(r'(.+)\s*\[\s*(.+)\s*\]\s*$', column)
        if matcher:
            field = matcher.group(1)
            subfield, *extra = matcher.group(2).split(",")
            return field, subfield, extra
        else:
            return column, None, []
 
    def _is_known_qualifier(self, field):
        """Check if `field` is one of 'Term Accession Number', 'Unit', any 'Comment[.+]', or any '.*REF'"""
        return (
            (field == "Term Accession Number") or (field == "Unit") or
            field.endswith(" REF") or search(r'^Comment\s*\[.+\]\s*$', field)
        )


class AssayEntries(StudyEntries):
    """Stores GLDS ISA Tab 'assays' records as a multilevel JSON"""
    def abort_on_by_sample_name():
        error = "Unique look up by sample name within AssayEntries not allowed"
        raise GeneLabISAException(error)
    _self_identifier = "Assay"
    _by_sample_name = defaultdict(abort_on_by_sample_name)


def parse_investigation(handle):
    """Load investigation tab with isatools, safeguard input for engine='python', suppress logger"""
    safe_handle = StringIO( # make number of double quotes inside fields even:
        sub(r'([^\n\t])\"([^\n\t])', r'\1""\2', handle.read().decode()),
    )
    getLogger("isatools").setLevel(CRITICAL+1)
    return load_investigation(safe_handle)


def read_table(handle):
    """Read TSV file, allowing for duplicate column names"""
    raw_dataframe = read_csv(handle, sep="\t", comment="#", header=None)
    raw_dataframe.columns = raw_dataframe.iloc[0,:]
    raw_dataframe.columns.name = None
    return raw_dataframe.drop(index=[0]).reset_index(drop=True)


class ISA:
    """Stores GLDS ISA information retrieved from ISA ZIP file URL"""
 
    def __init__(self, isa_zip_url):
        """Unpack ZIP from URL and delegate to sub-parsers"""
        self.raw = self.ingest_raw_isa(isa_zip_url)
        self.investigation = Investigation(self.raw.investigation)
        self.studies = StudyEntries(self.raw.studies)
        self.assays = AssayEntries(self.raw.assays)
 
    def ingest_raw_isa(self, isa_zip_url):
        """Unpack ZIP from URL and delegate to top-level parsers"""
        raw = Namespace(
            investigation=None, studies={}, assays={},
        )
        with urlopen(isa_zip_url) as response:
            with ZipFile(BytesIO(response.read())) as archive:
                for relpath in archive.namelist():
                    _, filename = path.split(relpath)
                    matcher = search(r'^([isa])_(.+)\.txt$', filename)
                    if matcher:
                        kind, name = matcher.groups()
                        with archive.open(relpath) as handle:
                            if kind == "i":
                                raw.investigation = parse_investigation(handle)
                            elif kind == "s":
                                raw.studies[name] = read_table(handle)
                            elif kind == "a":
                                raw.assays[name] = read_table(handle)
        archive_name = isa_zip_url.split("/")[-1]
        for tab, value in raw._get_kwargs():
            if not value:
                raise GeneLabISAException("{}: missing ISA tab '{}'".format(
                    archive_name, tab,
                ))
        return raw
