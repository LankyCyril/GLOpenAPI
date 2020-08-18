from logging import getLogger, CRITICAL
from argparse import Namespace
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO, StringIO
from re import search
from isatools.isatab import load_investigation
from pandas import read_csv
from genefab3.exceptions import GeneLabISAException


getLogger("isatools").setLevel(CRITICAL+1)


class ISA:
    """Stores GLDS ISA information retrieved from ISA ZIP file URL"""
 
    def __init__(self, isa_zip_url):
        """Unpack ZIP from URL and delegate to sub-parsers"""
        self.raw = self.ingest_raw_isa(isa_zip_url)
        self.investigation = self.process_investigation(self.raw.investigation)
        self.samples = self.process_samples(self.raw.samples)
        self.assays = self.process_assays(self.raw.assays)
 
    def ingest_raw_isa(self, isa_zip_url):
        """Unpack ZIP from URL and delegate to top-level parsers"""
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
                                raw.investigation = load_investigation(sio)
                            elif kind == "s":
                                raw.samples[name] = read_csv(handle, sep="\t")
                            elif kind == "a":
                                raw.assays[name] = read_csv(handle, sep="\t")
        for tab, value in raw._get_kwargs():
            if not value:
                raise GeneLabISAException("{}: missing ISA tab '{}'".format(
                    isa_zip_url.split("/")[-1], tab,
                ))
        return raw
 
    def process_investigation(self, raw_investigation): pass
    def process_samples(self, raw_samples): pass
    def process_assays(self, raw_assays): pass
