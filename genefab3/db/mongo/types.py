from hashlib import md5
from json import dumps
from base64 import encodebytes
from zlib import compress
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.exceptions import GeneFabLogger
from genefab3.db.mongo.utils import run_mongo_action


def funcdump(f):
    """One-way encoder of functions; only used for identity checks; code is never executed"""
    code = f.__code__
    return {"vars": code.co_varnames, "hash": md5(code.co_code).hexdigest()}


class ValueCheckedRecord():
    """Universal wrapper for cached objects; defined by identifier and value; re-cached if value has changed"""
 
    def __init__(self, identifier, collection, value):
        """Match existing documents by base64-encoded `value`, update if changed, report state in self.changed"""
        if not isinstance(identifier, dict):
            msg = "ValueCheckedRecord(): `identifier` is not a dictionary"
            raise GeneFabConfigurationException(msg, identifier=identifier)
        elif "base64value" in identifier:
            msg = "ValueCheckedRecord(): `identifier` uses a reserved key"
            _kw = dict(identifier=identifier, key="base64value")
            raise GeneFabConfigurationException(msg, **_kw)
        else:
            self.identifier, self.value = identifier, value
            try:
                dumped = dumps(value, sort_keys=True, default=funcdump)
                self.base64value = compress(encodebytes(dumped.encode()))
            except TypeError as e:
                msg = "ValueCheckedRecord(): " + str(e)
                _kw = dict(identifier=identifier, value=value)
                raise GeneFabConfigurationException(msg, **_kw)
            else:
                self.changed, n_stale_entries = True, 0
                for entry in collection.find(identifier):
                    if entry["base64value"] == self.base64value:
                        self.changed = False
                    else:
                        n_stale_entries += 1
                if (n_stale_entries != 0) or self.changed:
                    msg = f"ValueCheckedRecord updated:\n  {identifier}"
                    GeneFabLogger(info=msg)
                    with collection.database.client.start_session() as session:
                        with session.start_transaction():
                            run_mongo_action(
                                "replace", collection, query=identifier,
                                data={"base64value": self.base64value},
                            )
