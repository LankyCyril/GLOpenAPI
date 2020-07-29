from genefab3.mongo.meta import refresh_database_metadata


def debug(db):
    all_accessions, fresh, stale, auf = refresh_database_metadata(db)
    return "<hr>".join([
        "All accessions:<br>" + ", ".join(sorted(all_accessions)),
        "Fresh accessions:<br>" + ", ".join(sorted(fresh)),
        "Stale accessions:<br>" + ", ".join(sorted(stale)),
        "Assays updated for:<br>" + ", ".join(sorted(auf)),
    ])
