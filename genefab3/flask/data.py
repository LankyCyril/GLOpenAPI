from genefab3.config import ISA_TECHNOLOGY_TYPE_LOCATOR
from genefab3.config import TECHNOLOGY_FILE_LOCATORS, INFO
from genefab3.exceptions import GeneLabMetadataException, GeneLabFileException
from collections import defaultdict
from genefab3.mongo.dataset import CachedDataset
from genefab3.flask.parser import INPLACE_update_context
from genefab3.flask.meta import get_samples_by_metas
from genefab3.flask.display import Placeholders
from genefab3.sql.data import get_sql_data


ISA_TECHNOLOGY_NOT_SPECIFIED_ERROR = "{} requires a '{}.{}=' argument".format(
    "/data/", *ISA_TECHNOLOGY_TYPE_LOCATOR,
)
NO_FILES_FOR_TECHNOLOGY_ERROR, NO_FILES_FOR_DATATYPE_ERROR = (
    "No data files found for technology type '{}' and datatype '{}'",
    "No data files found for datatype '{}'",
)
MULTIPLE_TECHNOLOGIES_ERROR = "Multiple incompatible technology types requested"
NO_FILES_ERROR = "No data files found for"
AMBIGUOUS_FILES_ERROR = "Multiple (ambiguous) data files found for"


def get_target_file_locator(annotation_by_metas, context):
    """Infer regex to look up data file(s)"""
    if len(annotation_by_metas) == 0:
        return None # no samples, therefore no data files
    elif ISA_TECHNOLOGY_TYPE_LOCATOR not in annotation_by_metas:
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


def get_sample_tree(annotation_by_metas):
    """Convert annotation INFO to a nested dictionary"""
    info_cols = list(annotation_by_metas[INFO].columns)
    sample_index = annotation_by_metas[INFO].set_index(info_cols).index
    tree = defaultdict(lambda: defaultdict(set))
    for accession, assay_name, sample_name in sample_index:
        tree[accession][assay_name].add(sample_name)
    return tree


def get_file_descriptor_tree(mongo_db, sample_tree, target_file_locator):
    """Match one unique file per assay in `sample_tree`"""
    tree = defaultdict(dict)
    for accession in sample_tree:
        glds = CachedDataset(mongo_db, accession, init_assays=False)
        for assay_name, sample_names in sample_tree[accession].items():
            file_descriptors = glds.assays[assay_name].get_file_descriptors(
                regex=target_file_locator.regex,
                projection={key: True for key in target_file_locator.keys},
            )
            if len(file_descriptors) == 0:
                raise FileNotFoundError(
                    NO_FILES_ERROR, accession, assay_name,
                )
            elif len(file_descriptors) > 1:
                raise GeneLabFileException(
                    AMBIGUOUS_FILES_ERROR, accession, assay_name,
                )
            else:
                tree[accession][assay_name] = file_descriptors[0]
    return tree


def get_data_by_metas(dbs, context):
    """Select data based on annotation filters"""
    if ".".join(ISA_TECHNOLOGY_TYPE_LOCATOR) not in context.complete_args:
        # inject "investigation.study assays.study assay technology type":
        INPLACE_update_context(
            context, {".".join(ISA_TECHNOLOGY_TYPE_LOCATOR): ""},
        )
    # get samples view and parse out per-sample accession and assay names:
    annotation_by_metas = get_samples_by_metas(dbs.mongo_db, context)
    sample_tree = get_sample_tree(annotation_by_metas)
    # infer target data files per assay:
    target_file_locator = get_target_file_locator(annotation_by_metas, context)
    if target_file_locator is None:
        return Placeholders.dataframe([INFO], [INFO], ["entry"])
    else:
        file_descriptor_tree = get_file_descriptor_tree(
            dbs.mongo_db, sample_tree, target_file_locator,
        )
        # update/retrieve from SQL:
        return get_sql_data(
            dbs=dbs,
            sample_tree=sample_tree,
            file_descriptor_tree=file_descriptor_tree,
            datatype=context.kwargs["datatype"].lower(),
            rows=None,
            row_type=target_file_locator.row_type,
        )
