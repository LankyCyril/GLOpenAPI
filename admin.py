#!/usr/bin/env python
from sys import argv
from pymongo import MongoClient
from genefab3.config import MONGO_DB_NAME
from genefab3.mongo.meta import CachedDataset


def confirm(prompt):
    mask = "Are you sure? Type '{}' without quotes to confirm:\n"
    if input(mask.format(prompt)) != prompt:
        raise ValueError


if len(argv) == 3:
    mongo = MongoClient()
    db = getattr(mongo, MONGO_DB_NAME)
    if argv[1] == "drop":
        confirm("Yes, drop " + argv[2])
        if argv[2] != "ALL":
            CachedDataset.drop_cache(db=db, accession=argv[2])
        else:
            confirm("Yes, I am really sure I want to drop ALL")
            collection_names = {
                "dataset_timestamps", "json_cache", "annotations", "metadata",
            }
            for cn in collection_names:
                getattr(db, cn).delete_many({})
    elif argv[1] == "recache":
        if argv[2] == "ALL":
            raise NotImplementedError
        else:
            confirm("Yes, recache " + argv[2])
            CachedDataset.drop_cache(db=db, accession=argv[2])
            CachedDataset(argv[2], db)
    else:
        raise NotImplementedError
else:
    raise NotImplementedError
