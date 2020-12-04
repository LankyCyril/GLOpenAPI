from genefab3.config import ISA_TECHNOLOGY_TYPE_LOCATOR, TECHNOLOGY_FILE_LOCATORS
from genefab3.exceptions import GeneLabMetadataException, GeneLabFileException
from collections import defaultdict
from genefab3.mongo.dataset import CachedDataset
from genefab3.flask.parser import INPLACE_update_context
from functools import reduce
from operator import getitem
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


def get_target_trees(mongo_db, query, datatype):
    """Retrieve `accession -> assay -> samples` and `accession -> assay -> file_descriptor` dictionaries"""
    sample_tree = defaultdict(lambda: defaultdict(set))
    technologies = set()
    file_descriptor_tree = defaultdict(dict)
    projection = {
        "_id": False, ".accession": True, ".assay": True, ".sample name": True,
        ".".join(ISA_TECHNOLOGY_TYPE_LOCATOR): True,
    }
    for entry in mongo_db.metadata.find(query, projection):
        accession, assay_name = entry[""]["accession"], entry[""]["assay"]
        sample_tree[accession][assay_name].add(entry[""]["sample name"])
        technology = "*"
        try:
            technology = reduce(getitem, ISA_TECHNOLOGY_TYPE_LOCATOR, entry)
            technologies.add(technology)
            locator = TECHNOLOGY_FILE_LOCATORS[technology.lower()][datatype]
        except (KeyError, TypeError):
            raise GeneLabFileException(
                NO_FILES_ERROR, accession, assay_name,
                technology=technology, datatype=datatype,
            )
        if len(technologies) > 1:
            raise GeneLabMetadataException(
                MULTIPLE_TECHNOLOGIES_ERROR, technologies=technologies,
            )
        else:
            file_descriptor_tree[accession][assay_name] = get_file_descriptor(
                mongo_db, accession, assay_name, locator, datatype,
            )
    return sample_tree, file_descriptor_tree


def get_data_by_metas(dbs, context):
    """Select data based on annotation filters"""
    if ".".join(ISA_TECHNOLOGY_TYPE_LOCATOR) not in context.complete_args:
        # inject "investigation.study assays.study assay technology type":
        INPLACE_update_context(
            context, {".".join(ISA_TECHNOLOGY_TYPE_LOCATOR): ""},
        )
    sample_tree, file_descriptor_tree = get_target_trees(
        dbs.mongo_db, context.query, context.kwargs["datatype"],
    )
    return get_sql_data(
        dbs=dbs,
        sample_tree=sample_tree,
        file_descriptor_tree=file_descriptor_tree,
        datatype=context.kwargs["datatype"],
        rows=None,
    )
