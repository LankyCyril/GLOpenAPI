from genefab3.config import ISA_TECHNOLOGY_TYPE_LOCATOR, TECHNOLOGY_FILE_LOCATORS
from genefab3.exceptions import GeneLabMetadataException, GeneLabFileException
from collections import defaultdict
from genefab3.mongo.dataset import CachedDataset
from genefab3.flask.parser import INPLACE_update_context
from functools import reduce
from operator import getitem
from genefab3.sql.data import get_sql_data


NO_FILES_ERROR = "No data files found for"
AMBIGUOUS_FILES_ERROR = "Multiple (ambiguous) data files found for"
NO_FILES_FOR_DATATYPE_ERROR = "No data files found for datatype"
MULTIPLE_TECHNOLOGIES_ERROR = "Multiple incompatible technology types requested"


def get_file_descriptor(mongo_db, accession, assay_name, target_file_locator):
    """Match one unique file per assay based on `target_file_locator`"""
    glds = CachedDataset(mongo_db, accession, init_assays=False)
    file_descriptors = glds.assays[assay_name].get_file_descriptors(
        regex=target_file_locator.regex,
        projection={key: True for key in target_file_locator.keys},
    )
    if len(file_descriptors) == 0:
        raise FileNotFoundError(NO_FILES_ERROR, accession, assay_name)
    elif len(file_descriptors) > 1:
        raise GeneLabFileException(AMBIGUOUS_FILES_ERROR, accession, assay_name)
    else:
        return file_descriptors[0]


def get_target_trees(mongo_db, query, datatype):
    """...""" # TODO refactor me
    sample_tree = defaultdict(lambda: defaultdict(set))
    file_locators = set()
    file_descriptor_tree = defaultdict(dict)
    projection = {
        "_id": False, ".accession": True, ".assay": True, ".sample name": True,
        ".".join(ISA_TECHNOLOGY_TYPE_LOCATOR): True,
    }
    for entry in mongo_db.metadata.find(query, projection):
        accession, assay_name, sample_name = (
            entry[""]["accession"], entry[""]["assay"],
            entry[""]["sample name"],
        )
        sample_tree[accession][assay_name].add(sample_name)
        technology = "*"
        try:
            technology = reduce(getitem, ISA_TECHNOLOGY_TYPE_LOCATOR, entry)
            file_locators.add(
                TECHNOLOGY_FILE_LOCATORS[technology.lower()][datatype],
            )
        except (KeyError, TypeError):
            raise GeneLabFileException(
                NO_FILES_FOR_DATATYPE_ERROR,
                f"accession={accession}", f"assay={assay_name}",
                f"technology={technology}", f"datatype={datatype}",
            )
        if len(file_locators) > 1:
            raise GeneLabMetadataException(MULTIPLE_TECHNOLOGIES_ERROR)
        else:
            file_descriptor_tree[accession][assay_name] = get_file_descriptor(
                mongo_db, accession, assay_name,
                next(iter(file_locators)),
            )
    return sample_tree, file_descriptor_tree, file_locators.pop().row_type


def get_data_by_metas(dbs, context):
    """Select data based on annotation filters"""
    if ".".join(ISA_TECHNOLOGY_TYPE_LOCATOR) not in context.complete_args:
        # inject "investigation.study assays.study assay technology type":
        INPLACE_update_context(
            context, {".".join(ISA_TECHNOLOGY_TYPE_LOCATOR): ""},
        )
    sample_tree, file_descriptor_tree, row_type = get_target_trees(
        dbs.mongo_db, context.query, context.kwargs["datatype"],
    )
    return get_sql_data(
        dbs=dbs,
        sample_tree=sample_tree,
        file_descriptor_tree=file_descriptor_tree,
        datatype=context.kwargs["datatype"],
        rows=None,
        row_type=row_type,
    )
