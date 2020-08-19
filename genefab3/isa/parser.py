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


class Section:
    """Stores single section of GLDS ISA 'Studies' or 'Assays' Tab"""
    def __init__(self, dataframe): pass
    def to_json(self): pass
    def to_frame(self, mode=None, append=None): pass


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


class Assays(dict):
    """Stores GLDS ISA Tab 'assays' in accessible formats"""
 
    def __init__(self, raw_assays):
        for assay_name, raw_assay in raw_assays.items():
            super().__setitem__(assay_name, Section(raw_assay))


class Studies(Assays):
    """Stores GLDS ISA Tab 'studies' in accessible formats"""
 
    def __init__(self, raw_studies):
        if len(raw_studies) > 1:
            raise GeneLabISAException("Multi-study datasets are not supported")
        else:
            super().__init__(raw_studies)


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
        read_tsv = lambda f: read_csv(f, sep="\t", comment="#")
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
                                raw.studies[name] = read_tsv(handle)
                            elif kind == "a":
                                raw.assays[name] = read_tsv(handle)
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
