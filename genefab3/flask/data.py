from genefab3.mongo.dataset import CachedDataset
from genefab3.exceptions import GeneLabMetadataException, GeneLabFileException
from genefab3.config import ISA_TECH_TYPE_LOCATOR, TECHNOLOGY_FILE_LOCATORS
from pandas import merge
from genefab3.flask.meta import get_raw_meta_dataframe
from genefab3.flask.display import Placeholders
from genefab3.sql.data import get_sql_data


NO_FILES_ERROR = "No data files found for datatype"
AMBIGUOUS_FILES_ERROR = "Multiple (ambiguous) data files found for datatype"
MULTIPLE_TECHNOLOGIES_ERROR = "Incompatible technology types in request"


def get_file_descriptor(mongo_db, accession, assay_name, target_file_locator, datatype):
    """Match one unique file per assay based on `target_file_locator`"""
    glds = CachedDataset(mongo_db, accession, init_assays=False)
    file_descriptors = glds.assays[assay_name].get_file_descriptors(
        regex=target_file_locator.regex,
        projection={key: True for key in target_file_locator.keys},
    )
    if len(file_descriptors) == 0:
        raise GeneLabFileException(
            NO_FILES_ERROR, accession, assay_name, datatype=datatype,
        )
    elif len(file_descriptors) > 1:
        raise GeneLabFileException(
            AMBIGUOUS_FILES_ERROR, accession, assay_name, datatype=datatype,
        )
    else:
        return file_descriptors[0]


def infer_target_file_locator(raw_annotation, datatype):
    """Based on technology types in `raw_annotation` and target `datatype`, pick appropriate file locator (keys, regex)"""
    technologies = set(
        raw_annotation[ISA_TECH_TYPE_LOCATOR].str.lower().drop_duplicates()
    )
    target_file_locators = set()
    for technology in technologies:
        try:
            locator = TECHNOLOGY_FILE_LOCATORS[technology][datatype]
            target_file_locators.add(locator)
        except (KeyError, TypeError, IndexError):
            raise GeneLabFileException(
                NO_FILES_ERROR, technology=technology, datatype=datatype,
            )
    if len(target_file_locators) == 0:
        raise GeneLabFileException(NO_FILES_ERROR, datatype=datatype)
    elif len(target_file_locators) > 1:
        raise GeneLabMetadataException(
            MULTIPLE_TECHNOLOGIES_ERROR, technologies=technologies,
        )
    else:
        return target_file_locators.pop()


def add_file_descriptors_to_raw_annotation(mongo_db, raw_annotation, datatype):
    """Based on technology types in `raw_annotation` and target `datatype`, look up per-assay file descriptors"""
    target_file_locator = infer_target_file_locator(raw_annotation, datatype)
    info_cols = ["info.accession", "info.assay"]
    per_assay = raw_annotation[info_cols].drop_duplicates()
    per_assay["file descriptor"] = per_assay.apply(
        lambda row: get_file_descriptor(
            mongo_db, row["info.accession"], row["info.assay"],
            target_file_locator, datatype,
        ),
        axis=1,
    )
    return merge(raw_annotation, per_assay)


def get_data_by_metas(dbs, context):
    """Select data based on annotation filters"""
    raw_annotation = get_raw_meta_dataframe(
        dbs.mongo_db, context.query, include={"info.sample name"},
        projection={
            "_id": False, "info.accession": True, "info.assay": True,
            "info.sample name": True, ISA_TECH_TYPE_LOCATOR: True,
        }
    )
    if len(raw_annotation) == 0:
        return Placeholders.data_dataframe()
    else:
        raw_annotation = add_file_descriptors_to_raw_annotation(
            dbs.mongo_db, raw_annotation, context.kwargs["datatype"],
        )
        return get_sql_data(
            dbs=dbs,
            raw_annotation=raw_annotation,
            datatype=context.kwargs["datatype"],
            rows=None,
        )
