#!/usr/bin/env python
from pymongo import MongoClient
from os import path
from pickle import load, dump
from json import dumps
from genefab3.db.mongo.types import ValueCheckedRecord
from genefab3_genelab_adapter import GeneLabAdapter
from genefab3.isa.types import Dataset


def dmp(j):
    print(dumps(j, indent=4, sort_keys=True))


mongo_client = MongoClient()
db = mongo_client.test_db


if True:
    PKL = "sandbox/GLDS-48-files.pkl"
    if path.isfile(PKL):
        with open(PKL, mode="rb") as pkl:
            files = load(pkl)
    else:
        files = ValueCheckedRecord(
            identifier=dict(kind="dataset files", accession="GLDS-48"),
            collection=db.records,
            value=GeneLabAdapter().get_files_by_accession("GLDS-48"),
        )
        with open(PKL, mode="wb") as pkl:
            dump(files, pkl)
    ...
    glds = Dataset("GLDS-48", files.value, "sandbox/testblobs.db")
    #print(files.base64value)
    print(files.changed, glds.isa.changed)
    sample = next(glds.samples)
    ...
    if False:
        from bson import BSON
        from sys import getsizeof
        print(getsizeof(BSON.encode(sample)))
        ...
        from copy import deepcopy
        db.tests.insert_one(deepcopy(sample))
        ...
        dmp(db.tests.find_one(
            {"Files.datatype": "pca"},
            {"Files": {"$elemMatch": {"datatype": "pca"}}, "_id": False},
        ))
    ...
    from genefab3.db.mongo.utils import harmonize_document
    dmp(harmonize_document(
        sample["File"], units_formatter="{value} {{{unit}}}".format,
    ))


if False:
    from genefab3.common.types import Adapter
    func = Adapter.best_sample_name_matches
    print(func(None, "boba", {"buba", "bebebe", "boba"}))
