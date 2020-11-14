from genefab3.exceptions import GeneLabException, GeneLabFileException
from re import findall
from argparse import Namespace
from genefab3.mongo.dataset import CachedDataset
from urllib.request import urlopen


FMT_ERROR = "/file/ only accepts 'fmt=raw'"
FILENAME_ERROR = "/file/ requires a single 'filename=' argument"
SELECTS_ERROR = "/file/ requires a single dataset as 'select=' argument"


def get_file(db, context):
    """Patch through to cold storage file based on `select=` and `filename=`"""
    if context.args.get("fmt", "raw") != "raw":
        raise GeneLabException(FMT_ERROR)
    elif len(context.args.getlist("filename")) != 1:
        raise GeneLabException(FILENAME_ERROR)
    elif len(context.args.getlist("select")) != 1:
        raise GeneLabException(SELECTS_ERROR)
    else:
        target_accessions = set(findall(r'GLDS-[0-9]+', context.args["select"]))
        if len(target_accessions) > 1:
            raise GeneLabException(SELECTS_ERROR)
        else:
            glds = CachedDataset(
                db=db, accession=target_accessions.pop(), init_assays=False,
                logger=Namespace(warning=lambda *args, **kwargs: None),
            )
            filename = context.args["filename"]
            if filename not in glds.fileurls:
                raise GeneLabFileException("File not found")
            else:
                with urlopen(glds.fileurls[filename]) as response:
                    return response.read()
