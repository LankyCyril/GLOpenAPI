from genefab3.common.exceptions import GeneLabConfigurationException
from genefab3.db.sql.file import CachedBinaryFile
from genefab3.isa.parser import IsaZip


ATTRIBUTE_ERROR_MASK = "Classes inheriting from {} must define attribute '{}'"


class Dataset():
 
    def __init__(self, accession, mongo_db, sqlite_blobs):
        self.accession = accession
        class_name = "genefab3.db.types.Dataset"
        if not hasattr(self, "file_descriptors"):
            raise GeneLabConfigurationException(
                ATTRIBUTE_ERROR_MASK.format(class_name, "file_descriptors"),
            )
        elif not hasattr(self, "isa_file_descriptor"):
            raise GeneLabConfigurationException(
                ATTRIBUTE_ERROR_MASK.format(class_name, "isa_file_descriptor"),
            )
        elif len(self.isa_file_descriptor) != 1:
            raise GeneLabConfigurationException(
                "ISA descriptor must contain exactly one file",
                accession, filenames=sorted(set(self.isa_file_descriptor)),
            )
        else:
            isa_name, isa_desc = next(iter(self.isa_file_descriptor.items()))
            isa_file = CachedBinaryFile(
                name=isa_name, sqlite_db=sqlite_blobs,
                url=isa_desc["url"], timestamp=isa_desc["timestamp"],
            )
            self.isa = IsaZip(isa_file)
