from pandas import DataFrame, concat
from re import search, sub
from argparse import Namespace
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO, StringIO
from logging import getLogger, CRITICAL
from isatools.isatab import load_investigation
from pandas import read_csv
from genefab3.exceptions import GeneLabISAException


INVESTIGATION_KEYS = {
    "ontology_sources", "s_factors", "s_contacts", "s_publications",
    "investigation", "i_publications", "s_assays", "s_protocols",
    "s_design_descriptors", "studies", "i_contacts",
}


class Section(DataFrame):
    """Stores single section of GLDS ISA Tab"""
 
    def __init__(self, dataframe, kind=None):
        """Parse DataFrame into subsections"""
        if kind is Investigation: # simple, just move Comments
            self.__init__from__investigation__(dataframe)
        else:
            super().__init__()
 
    def __init__from__investigation__(self, dataframe):
        """Split dataframe into main and comments"""
        comment_column_mapper = {}
        for col in dataframe.columns:
            matcher = search(r'^Comment\s*\[(.+)\]$', col)
            if matcher:
                comment_column_mapper[col] = matcher.group(1)
        super().__init__(dataframe[[
            col for col in dataframe.columns if col not in comment_column_mapper
        ]])
        comment_columns = [
            col for col in dataframe.columns if col in comment_column_mapper
        ]
        self.comments = dataframe[comment_columns].rename(
            columns=comment_column_mapper,
        )


class Investigation:
    """Stores GLDS ISA Tab 'investigation' in an accessible format"""
 
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
            setattr(self, field, Section(dataframe, kind=Investigation))
 
    def __getattr__(self, key):
        putative_key = sub(r'^(.).*_', r'\1_', key.lower().replace(" ", "_"))
        if putative_key in dir(self):
            return getattr(self, putative_key)
        else:
            raise AttributeError("Investigation has no field '{}'".format(key))
 
    def __getitem__(self, key):
        return getattr(self, key)


class ISA:
    """Stores GLDS ISA information retrieved from ISA ZIP file URL"""
 
    def __init__(self, isa_zip_url):
        """Unpack ZIP from URL and delegate to sub-parsers"""
        self.raw = self.ingest_raw_isa(isa_zip_url)
        self.investigation = Investigation(self.raw.investigation)
        self.samples = self.process_samples(self.raw.samples)
        self.assays = self.process_assays(self.raw.assays)
 
    def ingest_raw_isa(self, isa_zip_url):
        """Unpack ZIP from URL and delegate to top-level parsers"""
        read_tsv = lambda f: read_csv(f, sep="\t", comment="#")
        raw = Namespace(
            investigation=None, samples={}, assays={},
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
                                raw.samples[name] = read_tsv(handle)
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
 
    def process_samples(self, raw_samples): pass
    def process_assays(self, raw_assays): pass
