from genefab3.exceptions import GeneLabException, GeneLabFileException
from re import findall
from argparse import Namespace
from genefab3.mongo.dataset import CachedDataset
from urllib.request import urlopen


ARGUMENTS_ERROR = "/file/ only accepts arguments 'select=', 'filename=', 'fmt='"
FMT_ERROR = "/file/ only accepts 'fmt=raw'"
FILENAME_ERROR = "/file/ requires a single 'filename=' argument"
SELECTS_ERROR = "/file/ requires a single dataset as 'select=' argument"


def get_file(db, context):
    """Patch through to cold storage file based on `select=` and `filename=`"""
    if not (set(context.complete_args) <= {"filename", "select", "fmt"}):
        raise GeneLabException(ARGUMENTS_ERROR)
    if context.kwargs.get("fmt", "raw") != "raw":
        # break early to avoid downloading a file and breaking during display:
        raise GeneLabException(FMT_ERROR)
    elif len(context.kwargs.getlist("filename")) != 1:
        raise GeneLabException(FILENAME_ERROR)
    # TODO: change 'select' logic after parser is updated:
    elif len(context.complete_args.getlist("select")) != 1:
        raise GeneLabException(SELECTS_ERROR)
    else:
        target_accessions = set(
            findall(r'GLDS-[0-9]+', context.complete_args["select"]),
        )
        if len(target_accessions) > 1:
            raise GeneLabException(SELECTS_ERROR)
        else:
            glds = CachedDataset(
                db=db, accession=target_accessions.pop(), init_assays=False,
                logger=Namespace(warning=lambda *args, **kwargs: None),
            )
            filename = context.kwargs["filename"]
            if filename not in glds.fileurls:
                raise GeneLabFileException("File not found")
            else:
                with urlopen(glds.fileurls[filename]) as response:
                    return response.read()
