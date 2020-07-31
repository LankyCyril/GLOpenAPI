from os import environ
from genefab3.config import DEBUG_MARKERS
from genefab3.mongo.meta import refresh_database_metadata


def debug(db):
    if environ.get("FLASK_ENV", None) not in DEBUG_MARKERS:
        return "Production server, debug disabled"
    else:
        all_accessions, fresh, stale, auf = refresh_database_metadata(db)
        return "<hr>".join([
            "All accessions:<br>" + ", ".join(sorted(all_accessions)),
            "Fresh accessions:<br>" + ", ".join(sorted(fresh)),
            "Stale accessions:<br>" + ", ".join(sorted(stale)),
            "Assays updated for:<br>" + ", ".join(sorted(auf)),
        ])
