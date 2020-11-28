from genefab3.exceptions import GeneLabFileException
from genefab3.mongo.dataset import CachedDataset
from urllib.request import urlopen


def get_file(db, context):
    """Patch through to cold storage file based on `from=` and `filename=`"""
    glds = CachedDataset(
        db=db, accession=context.accessions[0], init_assays=False,
    )
    mask = context.kwargs["filename"]
    if (mask[0] == "/") and (mask[-1] == "/"): # regular expression passed
        fileinfo = glds.get_file_descriptors(regex=mask[1:-1])
    else: # simple filename passed, match full
        fileinfo = glds.get_file_descriptors(name=mask)
    if not fileinfo:
        raise GeneLabFileException("Requested file not found")
    elif len(fileinfo) > 1:
        raise GeneLabFileException("Multiple files match search criteria")
    else:
        with urlopen(next(iter(fileinfo.values())).url) as response:
            return response.read()
