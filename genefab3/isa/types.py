from genefab3.common.exceptions import GeneLabFileException
from genefab3.common.utils import WithEitherURL
from genefab3.db.sql.file import CachedBinaryFile
from genefab3.isa.parser import IsaZip
from memoized_property import memoized_property


class Dataset():
 
    def __init__(self, accession, files, mongo_db, sqlite_blobs):
        self.accession, self.files = accession, files
        self.mongo_db, self.sqlite_blobs = mongo_db, sqlite_blobs
        isa_files = {
            filename: descriptor for filename, descriptor in files.items()
            if descriptor.get("datatype") == "isa"
        }
        if len(isa_files) != 1:
            raise GeneLabFileException(
                "ISA descriptor must contain exactly one file",
                accession, filenames=sorted(isa_files),
            )
        else:
            isa_name, isa_desc = next(iter(isa_files.items()))
            isa_file = WithEitherURL(
                CachedBinaryFile, isa_desc["urls"],
                name=isa_name, sqlite_db=self.sqlite_blobs,
                timestamp=isa_desc["timestamp"],
            )
            self.isa = IsaZip(
                data=isa_file.data,
                logger_info=dict(filename=isa_file.name, url=isa_file.url),
            )
            self.changed = None # TODO: detect any changes in `files`
            self.changed = self.changed or isa_file.changed
 
    @memoized_property
    def sample_records(self):
        pass
        # TODO: sample_records (converted from self.isa) to be cached into mongo
        # TODO: file_descriptors to be cached into mongo


class Assay():
    pass
