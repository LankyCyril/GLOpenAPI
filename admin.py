#!/usr/bin/env python
from logging import getLogger, INFO
from sys import argv
from pymongo import MongoClient, DESCENDING
from bson.objectid import ObjectId
from genefab3.config import MONGO_DB_NAME
from genefab3.mongo.meta import CachedDataset
from datetime import datetime


TIME_FMT = "%Y-%m-%d %H:%M:%S"
TYPE_OPTS = {True: "Exception", False: "Warning", None: "Unknown"}


logger = getLogger("genefab3")
logger.setLevel(INFO)


def format_timestamp(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime(TIME_FMT)


def confirm(prompt):
    mask = "Are you sure? Type '{}' without quotes to confirm:\n"
    if input(mask.format(prompt)) != prompt:
        raise ValueError


def drop(db, what):
    confirm("Yes, drop " + what)
    if what == "ALLMETA":
        confirm("Yes, I am really sure I want to drop ALLMETA")
        collection_names = {
            "dataset_timestamps", "json_cache", "annotations", "metadata",
        }
        for cn in collection_names:
            getattr(db, cn).delete_many({})
    elif what == "log":
        getattr(db, "log").delete_many({})
    else:
        CachedDataset.drop_cache(db=db, accession=what)


def recache(db, what):
    if what.startswith("ALL"):
        raise NotImplementedError
    else:
        confirm("Yes, recache " + what)
        CachedDataset.drop_cache(db=db, accession=what)
        CachedDataset(db, what, logger=logger)


def showlog(db, how):
    if how.isdigit() or ((not how.startswith("_id")) and ("=" in how)):
        if how.isdigit():
            max_i = int(how) - 1
            query = {}
        else:
            max_i = float("inf")
            k, v = how.split("=", 1)
            if v == "True":
                v = True
            elif v == "False":
                v = False
            query = {k: v}
        sort = [("timestamp", DESCENDING), ("_id", DESCENDING)]
        for i, entry in enumerate(db.log.find(query, sort=sort)):
            fields = [
                entry["_id"],
                format_timestamp(entry["timestamp"]),
                TYPE_OPTS[entry.get("is_exception")][0],
                "type={}".format(entry.get("type")),
                entry.get("value", "(no message)"),
                "from={}".format(entry.get("remote_addr")),
                "has_stack_info" if entry.get("stack") else "no_stack_info",
            ]
            print(*fields[:3], sep="; ", end=" ")
            print(*fields[3:], sep="\t")
            if i > max_i:
                break
    elif how.startswith("_id="):
        entry = db.log.find_one({"_id": ObjectId(how.lstrip("_id="))})
        fields = [
            "_id  = {}".format(entry["_id"]),
            "time = {}".format(format_timestamp(entry["timestamp"])),
            "what = {}".format(TYPE_OPTS[entry.get("is_exception")]),
            "type = {}".format(entry.get("type")),
            "from = {}".format(entry.get("remote_addr")),
            "path = {}".format(entry.get("full_path")),
            "mess = {}".format(entry.get("value")),
            "---",
            entry.get("stack", ""),
        ]
        print(*fields, sep="\n")
    else:
        raise NotImplementedError


if len(argv) == 3:
    mongo = MongoClient()
    db = getattr(mongo, MONGO_DB_NAME)
    if argv[1] == "drop":
        drop(db, argv[2])
    elif argv[1] == "recache":
        recache(db, argv[2])
    elif argv[1] == "log":
        showlog(db, argv[2])
    else:
        raise NotImplementedError
else:
    raise NotImplementedError
