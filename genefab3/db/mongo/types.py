from json import dumps
from base64 import encodebytes
from zlib import compress
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.db.mongo.utils import run_mongo_transaction
from genefab3.common.types import HashableEnough
from copy import deepcopy


class ValueCheckedRecord():
    """Universal wrapper for cached objects; defined by identifier and value; re-cached if value has changed"""
 
    def __init__(self, identifier, collection, value):
        """Match existing documents by base64-encoded `value`, update if changed, report state in self.changed"""
        if not isinstance(identifier, dict):
            raise GeneFabConfigurationException(
                "ValueCheckedRecord(): `identifier` is not a dictionary",
                identifier=identifier,
            )
        elif "base64value" in identifier:
            raise GeneFabConfigurationException(
                "ValueCheckedRecord(): `identifier` uses a reserved key",
                identifier=identifier, key="base64value",
            )
        else:
            self.identifier, self.value = identifier, value
            try:
                self.base64value = compress(
                    encodebytes(dumps(value, sort_keys=True).encode()),
                )
            except TypeError as e:
                raise GeneFabConfigurationException(
                    "ValueCheckedRecord(): " + str(e),
                    identifier=identifier, value=value,
                )
            else:
                self.changed, n_stale_entries = True, 0
                for entry in collection.find(identifier):
                    if entry["base64value"] == self.base64value:
                        self.changed = False
                    else:
                        n_stale_entries += 1
                if (n_stale_entries != 0) or self.changed:
                    run_mongo_transaction(
                        "replace", collection, query=identifier,
                        data={"base64value": self.base64value},
                    )


class HashableDocument(HashableEnough):
    """Hashable, sortable representation of MongoDB document"""
 
    def __init__(self, entry):
        self.name = entry.get("")
        self.as_dict = deepcopy(entry)
        self.identity = dumps(entry, sort_keys=True)
        HashableEnough.__init__(self, ("identity",))
 
    def __lt__(self, other):
        if isinstance(other, HashableDocument):
            return self.identity < other.identity
        else:
            return self < other
