from genefab3.common.types import AssayBaseClass
from genefab3.config import COLLECTION_NAMES
from genefab3.common.exceptions import GeneLabDatabaseException
from re import escape
from genefab3.common.types import UniversalSet
from genefab3.utils import iterate_terminal_leaf_filenames


class CachedAssay(AssayBaseClass):
    """Exposes individual assay information and metadata"""
 
    def __init__(self, dataset, assay_name):
        self.dataset = dataset
        self.name = assay_name
        self.mongo_db = dataset.mongo_db
 
    def _iterate_filenames_from_projection(self, projection, cname=COLLECTION_NAMES.METADATA):
        """Match filenames from end leaves of query in metadata"""
        metadata_collection = getattr(self.mongo_db, cname)
        query = {
            "info.accession": self.dataset.accession,
            "info.assay": self.name,
        }
        proj = {"_id": False, **projection}
        for entry in metadata_collection.find(query, proj):
            try:
                yield from iterate_terminal_leaf_filenames(entry)
            except GeneLabDatabaseException:
                raise GeneLabDatabaseException(
                    "Could not retrieve filenames from metadata fields",
                    fields=list(projection.keys()),
                )
 
    def get_file_descriptors(self, name=None, regex=None, glob=None, projection=None):
        """Given mask and/or target field, find filenames, urls, and datestamps"""
        if projection:
            metadata_candidates = set(
                self._iterate_filenames_from_projection(projection),
            )
        else:
            metadata_candidates = UniversalSet()
        if name or regex or glob:
            dataset_candidates = {
                file_descriptor.name for file_descriptor in
                self.dataset.get_file_descriptors(name, regex, glob)
            }
        else:
            dataset_candidates = UniversalSet()
        candidate_filenames = metadata_candidates & dataset_candidates
        if isinstance(candidate_filenames, UniversalSet):
            raise ValueError(
                "get_file_descriptors() accepts exactly one "
                "of `filename`, `regex`, `glob`"
            )
        elif candidate_filenames:
            return self.dataset.get_file_descriptors(
                regex=r'^({})$'.format(
                    r'|'.join(map(escape, candidate_filenames)),
                ),
            )
        else:
            return {}
