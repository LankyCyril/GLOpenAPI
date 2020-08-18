from urllib.request import urlopen
from io import BytesIO, StringIO
from zipfile import ZipFile
from re import search
from isatools.isatab import load_investigation
from pandas import read_csv


class ISA:
    """Stores GLDS ISA information retrieved from ISA ZIP file URL"""
    investigation = None
    samples = {}
    assays = {}
 
    def __init__(self, isa_zip_url):
        """Unpack ZIP from URL and delegate to sub-parsers"""
        with urlopen(isa_zip_url) as response:
            with ZipFile(BytesIO(response.read())) as archive:
                for filename in archive.namelist():
                    matcher = search(r'^([isa])_(.+)\.txt$', filename)
                    if matcher:
                        kind, name = matcher.groups()
                        with archive.open(filename) as handle:
                            if kind == "i":
                                self.investigation = self.parse_i(handle)
                            elif kind == "s":
                                self.samples[name] = self.parse_s(handle)
                            elif kind == "a":
                                self.assays[name] = self.parse_a(handle)
 
    def parse_i(self, handle):
        """Load 'investigation' data from file buffer"""
        sio = StringIO(handle.read().decode())
        return load_investigation(sio)
 
    def parse_s(self, handle):
        """Load 'samples' data from file buffer"""
        return read_csv(handle, sep="\t")
 
    def parse_a(self, handle):
        """Load 'assays' data from file buffer"""
        return read_csv(handle, sep="\t")
