from genefab3.config import ASSAY_METADATALIKES
from genefab3.exceptions import GeneLabException


def get_by_meta(db, meta=None, rargs={}):
    unknown_args = set(rargs) - ASSAY_METADATALIKES
    if unknown_args:
        raise GeneLabException("Unrecognized arguments: {}".format(
            ", ".join(sorted(unknown_args))
        ))
    try:
        datasets_and_assays = set.intersection(*(
            {
                (entry["accession"], entry["assay_name"]) for entry in
                db.assay_meta.find({
                    "meta": "factors",
                    "field": {"$in": factors_any.split("|")},
                })
            }
            for factors_any in rargs.getlist("factors")
        ))
    except TypeError:
        datasets_and_assays = {}
    return "<pre>Dataset  \tAssay\n" + "\n".join([
        "{} \t{}".format(accession, assay_name)
        for accession, assay_name in sorted(datasets_and_assays)
    ])
