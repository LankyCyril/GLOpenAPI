from genefab3.config import ISA_TECHNOLOGY_TYPE_LOCATOR, TECHNOLOGY_FILE_LOCATORS
from genefab3.exceptions import GeneLabMetadataException, GeneLabFileException
from genefab3.flask.parser import INPLACE_update_context
from genefab3.flask.meta import get_samples_by_metas
from genefab3.sql.data import get_sql_data
from genefab3.config import SQLITE_DB_LOCATION


ISA_TECHNOLOGY_NOT_SPECIFIED_ERROR = "{} requires a '{}.{}=' argument".format(
    "/data/", *ISA_TECHNOLOGY_TYPE_LOCATOR,
)
NO_FILES_FOR_TECHNOLOGY_ERROR, NO_FILES_FOR_DATATYPE_ERROR = (
    "No data files found for technology type '{}' and datatype '{}'",
    "No data files found for datatype '{}'",
)
MULTIPLE_TECHNOLOGIES_ERROR = "Multiple incompatible technology types requested"


def get_target_file_locator(annotation_by_metas, context):
    """Infer regex to look up data file(s)"""
    if ISA_TECHNOLOGY_TYPE_LOCATOR not in annotation_by_metas:
        raise GeneLabMetadataException(ISA_TECHNOLOGY_NOT_SPECIFIED_ERROR)
    else:
        target_file_locators = set()
        technologies = annotation_by_metas[ISA_TECHNOLOGY_TYPE_LOCATOR]
        for technology in technologies.drop_duplicates():
            locator = TECHNOLOGY_FILE_LOCATORS.get(technology.lower(), {}).get(
                context.kwargs["datatype"].lower(), None,
            )
            if locator is None:
                raise GeneLabFileException(
                    NO_FILES_FOR_TECHNOLOGY_ERROR.format(
                        technology, context.kwargs["datatype"],
                    ),
                )
            else:
                target_file_locators.add(locator)
        if len(target_file_locators) == 0:
            raise GeneLabFileException(
                NO_FILES_FOR_DATATYPE_ERROR.format(context.kwargs["datatype"])
            )
        elif len(target_file_locators) > 1:
            raise GeneLabMetadataException(MULTIPLE_TECHNOLOGIES_ERROR)
        else:
            return target_file_locators.pop()


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
    # infer target data file locator and update/retrieve from SQL:
    return get_sql_data(
        mongo_db=db, sqlite_db_location=SQLITE_DB_LOCATION,
        sample_index=sample_index, rows=None,
        datatype=context.kwargs["datatype"].lower(),
        target_file_locator=get_target_file_locator(
            annotation_by_metas, context,
        ),
    )
