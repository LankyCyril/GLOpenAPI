from genefab3.exceptions import GeneLabFileException
from genefab3.mongo.dataset import CachedDataset
from urllib.request import urlopen


def get_file(db, context):
    """Patch through to cold storage file based on `from=` and `filename=`"""
    accession = next( # assume only one accession (validated in parser)
        iter(context.accessions_and_assays),
    )
    if context.accessions_and_assays[accession]:
        # assume only one assay_name (validated in parser):
        assay_name = context.accessions_and_assays[accession][0]
    else:
        assay_name = None
    glds = CachedDataset(db=db, accession=accession, init_assays=False)
    mask = context.kwargs.get("filename")
    if mask is None: # nothing passed, assume target field specified
        lookup_kwargs = {}
    elif (mask[0] == "/") and (mask[-1] == "/"): # regular expression passed
        lookup_kwargs = dict(regex=mask[1:-1])
    else: # simple filename passed, match full
        lookup_kwargs = dict(name=mask)
    if assay_name: # search within specific assay
        fileinfo = glds.assays[assay_name].get_file_descriptors(
            **lookup_kwargs, projection=context.projection,
        )
    else: # search in entire dataset
        fileinfo = glds.get_file_descriptors(**lookup_kwargs)
    if not fileinfo:
        raise GeneLabFileException("Requested file not found")
    elif len(fileinfo) > 1:
        raise GeneLabFileException("Multiple files match search criteria")
    else:
        with urlopen(next(iter(fileinfo.values())).url) as response:
            return response.read()
