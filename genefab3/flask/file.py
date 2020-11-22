from genefab3.exceptions import GeneLabFileException
from genefab3.mongo.dataset import CachedDataset
from urllib.request import urlopen


def get_file(db, context):
    """Patch through to cold storage file based on `from=` and `filename=`"""
    glds = CachedDataset(
        db=db, accession=context.accessions[0], init_assays=False,
    )
    filename = context.kwargs["filename"]
    if filename not in glds.fileurls:
        raise GeneLabFileException("File not found")
    else:
        with urlopen(glds.fileurls[filename]) as response:
            return response.read()
