from pandas import read_csv
from pandas.errors import ParserError
from numpy import nan
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


INVESTIGATION_KEYS = { # "Real Name In Mixed Case" -> "as_reported_by_isatools"
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
    """Stores GLDS ISA Tab 'investigation' in accessible formats""" # NOTE: work in progress
 
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
    """Stores GLDS ISA Tab 'studies' records as a nested JSON"""
    _self_identifier = "Study"
 
    def _abort_lookup(self):
        """Prevents ambiguous lookup through `self._by_sample_name` in inherited classes"""
        error_mask = "Unique look up by sample name within {} not allowed"
        raise GeneLabISAException(error_mask.format(type(self).__name__))
 
    def __init__(self, raw_tabs):
        """Convert tables to nested JSONs"""
        if self._self_identifier == "Study":
            self._by_sample_name = {}
        else: # lookup in classes like AssayEntries would be ambiguous
            self._by_sample_name = defaultdict(self._abort_lookup)
        for name, raw_tab in raw_tabs.items():
            for _, row in raw_tab.iterrows():
                if "Sample Name" not in row:
                    error = "Table entry must have 'Sample Name'"
                    raise GeneLabISAException(error)
                else:
                    sample_name = row["Sample Name"]
                json = self._row_to_json(row, name)
                super().append(json)
                if self._self_identifier == "Study":
                    if sample_name in self._by_sample_name:
                        error_mask = "Duplicate Sample Name '{}' in studies"
                        error = error_mask.format(sample_name)
                        raise GeneLabISAException(error)
                    else:
                        self._by_sample_name[sample_name] = json
 
    def _row_to_json(self, row, name):
        """Convert single row of table to nested JSON"""
        json = {"": {self._self_identifier: name}}
        protocol_ref, qualifiable = nan, None
        for column, value in row.items():
            field, subfield, extra = self._parse_field(column)
            if field == "Protocol REF":
                protocol_ref = value
            elif self._is_not_qualifier(field): # top-level field
                if not subfield: # e.g. "Source Name"
                    qualifiable = self._INPLACE_add_toplevel_field(
                        json, field, value, protocol_ref,
                    )
                else: # e.g. "Characteristics[Age]"
                    qualifiable = self._INPLACE_add_metadatalike(
                        json, field, subfield, value, protocol_ref,
                    )
            else: # qualify entry at pointer with second-level field
                if qualifiable is None:
                    raise GeneLabISAException("Qualifier before main field")
                else:
                    self._INPLACE_qualify(qualifiable, field, subfield, value)
        return json
 
    def _parse_field(self, column):
        """Interpret field like 'Source Name' or 'Characteristics[sex,http://purl.obolibrary.org/obo/PATO_0000047,EFO]"""
        matcher = search(r'(.+[^\s])\s*\[\s*(.+[^\s])\s*\]\s*$', column)
        if matcher:
            field = matcher.group(1)
            subfield, *extra = matcher.group(2).split(",")
            return field, subfield, extra
        else:
            return column, None, []
 
    def _is_not_qualifier(self, field):
        """Check if `column` is none of 'Term Accession Number', 'Unit', 'Comment', any '.* REF'"""
        return (
            (field not in {"Term Accession Number", "Unit", "Comment"}) and
            (not field.endswith(" REF"))
        )
 
    def _INPLACE_add_toplevel_field(self, json, field, value, protocol_ref):
        """Add top-level key-value to json ('Source Name', 'Material Type',...), qualify with 'Protocol REF', point to resulting field"""
        value_with_protocol_ref = {"": value, "Protocol REF": protocol_ref}
        if field in json:
            json[field].append(value_with_protocol_ref)
        else: # make {"Source Name": [{"": "ABC"}]}
            json[field] = [value_with_protocol_ref]
        qualifiable = json[field][-1]
        return qualifiable
 
    def _INPLACE_add_metadatalike(self, json, field, subfield, value, protocol_ref):
        """Add metadatalike to json (e.g. 'Characteristics' -> 'Age'), qualify with 'Protocol REF', point to resulting field"""
        if field not in json:
            json[field] = {}
        if subfield in json[field]:
            error = "Duplicate '{}[{}]'".format(field, subfield)
            raise GeneLabISAException(error)
        else: # make {"Characteristics": {"Age": {"": "36"}}}
            json[field][subfield] = {"": value}
            qualifiable = json[field][subfield]
            if field == "Parameter Value":
                qualifiable["Protocol REF"] = protocol_ref
            return qualifiable
 
    def _INPLACE_qualify(self, qualifiable, field, subfield, value):
        """Add qualifier to field at pointer (qualifiable)"""
        if field == "Comment": # make {"Comment": {"mood": "cheerful"}}
            if "Comment" not in qualifiable:
                qualifiable["Comment"] = {"": nan}
            qualifiable["Comment"][subfield or ""] = value
        else: # make {"Unit": "percent"}
            if subfield:
                warning = "Extra info past qualifier '{}'".format(field)
                getLogger("genefab3").warning(warning)
            qualifiable[field] = value


class AssayEntries(StudyEntries):
    """Stores GLDS ISA Tab 'assays' records as a nested JSON"""
    _self_identifier = "Assay"


def parse_investigation(handle):
    """Load investigation tab with isatools, safeguard input for engine='python', suppress isatools' logger"""
    safe_handle = StringIO(
        sub( # make number of double quotes inside fields even:
            r'([^\n\t])\"([^\n\t])', r'\1""\2',
            handle.read().decode(errors="replace")
        ),
    )
    getLogger("isatools").setLevel(CRITICAL+1)
    return load_investigation(safe_handle)


def read_tab(handle, isa_zip_url, filename):
    """Read TSV file, absorbing encoding errors, and allowing for duplicate column names"""
    byte_tee = BytesIO(handle.read())
    reader_kwargs = dict(sep="\t", comment="#", header=None, index_col=False)
    try:
        raw_tab = read_csv(byte_tee, **reader_kwargs)
    except (UnicodeDecodeError, ParserError) as e:
        byte_tee.seek(0)
        string_tee = StringIO(byte_tee.read().decode(errors="replace"))
        raw_tab = read_csv(string_tee, **reader_kwargs)
        warning_mask = "{}: absorbing {} when reading from {}"
        warning = warning_mask.format(isa_zip_url, repr(e), filename)
        getLogger("genefab3").warning(warning)
    raw_tab.columns = raw_tab.iloc[0,:]
    raw_tab.columns.name = None
    return raw_tab.drop(index=[0]).drop_duplicates().reset_index(drop=True)


class IsaZip:
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
                for filepath in archive.namelist():
                    _, filename = path.split(filepath)
                    matcher = search(r'^([isa])_(.+)\.txt$', filename)
                    if matcher:
                        kind, name = matcher.groups()
                        with archive.open(filepath) as handle:
                            if kind == "i":
                                raw.investigation = parse_investigation(handle)
                            elif kind == "s":
                                raw.studies[name] = read_tab(
                                    handle, isa_zip_url, filename,
                                )
                            elif kind == "a":
                                raw.assays[name] = read_tab(
                                    handle, isa_zip_url, filename,
                                )
        for tab, value in raw._get_kwargs():
            if not value:
                error = "{}: missing ISA tab '{}'".format(isa_zip_url, tab)
                raise GeneLabISAException(error)
        return raw
