from genefab3.flask.meta import get_samples_by_metas
from genefab3.config import ISA_TECHNOLOGY_TYPE_LOCATOR, TECHNOLOGY_FILE_MASKS
from genefab3.exceptions import GeneLabException, GeneLabFileException
from pandas import DataFrame, concat
from functools import partial
from genefab3.mongo.data import get_single_sample_data


ISA_TECHNOLOGY_NOT_SPECIFIED_ERROR = "{} requires a '{}.{}=' argument".format(
    "/data/", *ISA_TECHNOLOGY_TYPE_LOCATOR,
)
DATATYPE_NOT_SPECIFIED_ERROR = "/data/ requires a 'datatype=' argument"
NO_TARGET_FILES_ERROR = "No files found for some or all of requested data"
MULTIPLE_TECHNOLOGIES_ERROR = "Multiple incompatible technology types requested"


def get_target_file_regex(annotation_by_metas, context):
    """Infer regex to look up data file(s)"""
    if ISA_TECHNOLOGY_TYPE_LOCATOR not in annotation_by_metas:
        raise GeneLabException(ISA_TECHNOLOGY_NOT_SPECIFIED_ERROR)
    elif "datatype" not in context.kwargs:
        raise GeneLabException(DATATYPE_NOT_SPECIFIED_ERROR)
    else:
        target_file_regexes = {
            TECHNOLOGY_FILE_MASKS.get(technology.lower(), {}).get(
                context.kwargs["datatype"].lower(), None,
            )
            for technology in
            annotation_by_metas[ISA_TECHNOLOGY_TYPE_LOCATOR].drop_duplicates()
        }
        if None in target_file_regexes:
            raise GeneLabException(NO_TARGET_FILES_ERROR)
        elif len(target_file_regexes) == 0:
            raise GeneLabFileException(NO_TARGET_FILES_ERROR)
        elif len(target_file_regexes) > 1:
            raise GeneLabFileException(MULTIPLE_TECHNOLOGIES_ERROR)
        else:
            return target_file_regexes.pop()


def get_data_by_metas(db, context):
    """Select data based on annotation filters"""
    annotation_by_metas = get_samples_by_metas(db, context)
    target_file_regex = get_target_file_regex(annotation_by_metas, context)
    info_cols = list(annotation_by_metas["info"].columns)
    sample_index = annotation_by_metas["info"].set_index(info_cols).index
    gene_rows = None
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
