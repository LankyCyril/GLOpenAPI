from genefab3.config import ISA_TECHNOLOGY_TYPE_LOCATOR, TECHNOLOGY_FILE_MASKS
from genefab3.exceptions import GeneLabMetadataException, GeneLabFileException
from genefab3.flask.parser import INPLACE_update_context
from genefab3.flask.meta import get_samples_by_metas
from pandas import DataFrame, concat
from functools import partial
from genefab3.mongo.data import get_single_sample_data


ISA_TECHNOLOGY_NOT_SPECIFIED_ERROR = "{} requires a '{}.{}=' argument".format(
    "/data/", *ISA_TECHNOLOGY_TYPE_LOCATOR,
)
ALL_TARGET_FILES_MISSING, SOME_TARGET_FILES_MISSING = (
    "No data files found for any of requested technology types",
    "No data files found for some of requested technology types",
)
MULTIPLE_TECHNOLOGIES_ERROR = "Multiple incompatible technology types requested"


def get_target_file_regex(annotation_by_metas, context):
    """Infer regex to look up data file(s)"""
    if ISA_TECHNOLOGY_TYPE_LOCATOR not in annotation_by_metas:
        raise GeneLabMetadataException(ISA_TECHNOLOGY_NOT_SPECIFIED_ERROR)
    else:
        target_file_regexes = {
            TECHNOLOGY_FILE_MASKS.get(technology.lower(), {}).get(
                context.kwargs["datatype"].lower(), None,
            )
            for technology in
            annotation_by_metas[ISA_TECHNOLOGY_TYPE_LOCATOR].drop_duplicates()
        }
        if (None in target_file_regexes) and (len(target_file_regexes) > 1):
            raise GeneLabFileException(SOME_TARGET_FILES_MISSING)
        elif None in target_file_regexes:
            raise GeneLabFileException(ALL_TARGET_FILES_MISSING)
        elif len(target_file_regexes) > 1:
            raise GeneLabMetadataException(MULTIPLE_TECHNOLOGIES_ERROR)
        else:
            return target_file_regexes.pop()


def get_data_by_metas(db, context):
    """Select data based on annotation filters"""
    if ".".join(ISA_TECHNOLOGY_TYPE_LOCATOR) not in context.complete_args:
        # inject "investigation.study assays.study assay technology type":
        INPLACE_update_context(
            context, {".".join(ISA_TECHNOLOGY_TYPE_LOCATOR): ""},
        )
    # get samples view and parse out per-sample accession and assay names:
    annotation_by_metas = get_samples_by_metas(db, context)
    info_cols = list(annotation_by_metas["info"].columns)
    sample_index = annotation_by_metas["info"].set_index(info_cols).index
    # infer target data file masks:
    target_file_regex = get_target_file_regex(annotation_by_metas, context)
    # constrain to gene lists (this is for the future...):
    gene_rows = None
    # retrieve data per-sample and merge:
    return DataFrame(
        data=concat(
            sample_index.map(
                partial(
                    get_single_sample_data, gene_rows=gene_rows,
                    target_file_regex=target_file_regex,
                ),
            ),
            axis=1,
        ),
        columns=sample_index,
    )
