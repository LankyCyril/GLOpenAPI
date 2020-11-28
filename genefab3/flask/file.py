from genefab3.exceptions import GeneLabFileException
from genefab3.mongo.dataset import CachedDataset
from urllib.request import urlopen


def get_file(db, context):
    """Patch through to cold storage file based on `from=` and `filename=`"""
    accession = next(iter(context.accessions_and_assays))
    if context.accessions_and_assays[accession]:
        assay_name = context.accessions_and_assays[accession][0]
    else:
        assay_name = None
    glds = CachedDataset(db=db, accession=accession, init_assays=False)
    mask = context.kwargs["filename"]
    if (mask[0] == "/") and (mask[-1] == "/"): # regular expression passed
        lookup_kwargs = dict(regex=mask[1:-1])
    else: # simple filename passed, match full
        lookup_kwargs = dict(name=mask)
    if assay_name: # search within specific assay
        raise NotImplementedError
    else: # search in entire dataset
        fileinfo = glds.get_file_descriptors(**lookup_kwargs)
    if not fileinfo:
        raise GeneLabFileException("Requested file not found")
    elif len(fileinfo) > 1:
        raise GeneLabFileException("Multiple files match search criteria")
    else:
        with urlopen(next(iter(fileinfo.values())).url) as response:
            return response.read()
