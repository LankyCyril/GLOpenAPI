from genefab3.exceptions import GeneLabJSONException
from collections import defaultdict


def parse_assay_json_fields(json):
    """Parse fields and titles from assay json 'header'"""
    try:
        header = json["header"]
        field2title = {entry["field"]: entry["title"] for entry in header}
    except KeyError:
        raise GeneLabJSONException("Malformed assay JSON: header")
    if len(field2title) != len(header):
        raise GeneLabJSONException("Conflicting IDs of data fields")
    fields = defaultdict(set)
    for field, title in field2title.items():
        fields[title].add(field)
    return dict(fields), field2title


class ColdStorageAssay():
    """Stores individual assay information and metadata in raw form"""
 
    def __init__(self, dataset, name, json):
        """Parse assay JSON reported by cold storage"""
        from genefab3.coldstoragedataset import ColdStorageDataset
        if isinstance(dataset, ColdStorageDataset):
            self.dataset = dataset
        else:
            self.dataset = ColdStorageDataset(dataset)
        self.fileurls = self.dataset.fileurls
        self.filedates = self.dataset.filedates
        self.fields, self.field2title = parse_assay_json_fields(json)
