from genefab3.common.exceptions import GeneLabConfigurationException


class Dataset():
 
    def __init__(self, accession):
        self.accession = accession
        class_name = "genefab3.db.types.Dataset"
        error_mask = "Classes inheriting from {} must define attribute '{}'"
        if not hasattr(self, "file_descriptors"):
            raise GeneLabConfigurationException(
                error_mask.format(class_name, "file_descriptors"),
            )
        if not hasattr(self, "isa_file_descriptor"):
            raise GeneLabConfigurationException(
                error_mask.format(class_name, "isa_file_descriptor"),
            )
