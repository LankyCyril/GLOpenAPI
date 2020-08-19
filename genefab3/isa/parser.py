from pandas import DataFrame, concat, read_csv
from re import search, sub
from genefab3.exceptions import GeneLabISAException
from argparse import Namespace
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO, StringIO
from logging import getLogger, CRITICAL
from isatools.isatab import load_investigation


INVESTIGATION_KEYS = {
    "ontology_sources", "s_factors", "s_contacts", "s_publications",
    "investigation", "i_publications", "s_assays", "s_protocols",
    "s_design_descriptors", "studies", "i_contacts",
}


class InvestigationSection(DataFrame):
    """Stores single section of GLDS ISA 'Investigation' Tab"""
 
    def __init__(self, dataframe):
        """Split dataframe into DataFrames of main and comments"""
        comment_column_mapper = {}
        for col in dataframe.columns:
            matcher = search(r'^Comment\s*\[(.+)\]$', col)
            if matcher:
                comment_column_mapper[col] = matcher.group(1)
        super().__init__(dataframe[[
            col for col in dataframe.columns if col not in comment_column_mapper
        ]])
        self.columns.name = None
        comment_columns = [
            col for col in dataframe.columns if col in comment_column_mapper
        ]
        self._metadata = ["comments"]
        self.comments = dataframe[comment_columns].rename(
            columns=comment_column_mapper,
        )
        for obj in (self, self.comments):
            obj.columns.name = None
            obj.reset_index(drop=True, inplace=True)
 
    def to_json(self):
        """Return as JSON of all levels"""
        return {
            "main": DataFrame.to_json(self, orient="records"),
            "comments": DataFrame.to_json(self, orient="records"),
        }
 
    def to_frame(self, mode=None, append=None):
        """Return as DataFrame with values from certain level"""
        if (mode is not None) or (append is not None):
            error_mask = "{}() cannot be modified with `mode`, `append`"
            raise GeneLabISAException(error_mask.format("InvestigationSection"))
        else:
            return self


class Investigation:
    """Stores GLDS ISA Tab 'investigation' in accessible formats"""
 
    def __init__(self, raw_investigation):
        for field, content in raw_investigation.items():
            if isinstance(content, list):
                dataframe = concat(content)
            else:
                dataframe = content
            dataframe.drop(
                columns=range(0, dataframe.shape[1]),
                errors="ignore", inplace=True,
            )
            setattr(self, field, InvestigationSection(dataframe))
 
    def __getattr__(self, key):
        putative_key = sub(r'^(.).*_', r'\1_', key.lower().replace(" ", "_"))
        if putative_key in dir(self):
            return getattr(self, putative_key)
        else:
            raise AttributeError("Investigation has no field '{}'".format(key))
 
    def __getitem__(self, key):
        return getattr(self, key)


class Studies(list):
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


class Assays(Studies):
    """Stores GLDS ISA Tab 'assays' records as a multilevel JSON"""
    _self_identifier = "Assay"


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
        self.studies = Studies(self.raw.studies)
        self.assays = Assays(self.raw.assays)
 
    def ingest_raw_isa(self, isa_zip_url):
        """Unpack ZIP from URL and delegate to top-level parsers"""
        raw = Namespace(
            investigation=None, studies={}, assays={},
        )
        with urlopen(isa_zip_url) as response:
            with ZipFile(BytesIO(response.read())) as archive:
                for filename in archive.namelist():
                    matcher = search(r'^([isa])_(.+)\.txt$', filename)
                    if matcher:
                        kind, name = matcher.groups()
                        with archive.open(filename) as handle:
                            if kind == "i":
                                sio = StringIO(handle.read().decode())
                                getLogger("isatools").setLevel(CRITICAL+1)
                                raw.investigation = load_investigation(sio)
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
        if set(raw.investigation.keys()) != INVESTIGATION_KEYS:
            error = "{}: malformed ISA tab 'investigation'".format(archive_name)
            raise GeneLabISAException(error)
        return raw
