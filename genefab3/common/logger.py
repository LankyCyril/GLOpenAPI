from flask import request
from datetime import datetime
from logging import getLogger, Handler
from collections.abc import Callable


def log_to_mongo_collection(collection, et=None, ev=None, stack=None, is_exception=False, **kwargs):
    try:
        remote_addr, full_path = request.remote_addr, request.full_path
    except RuntimeError:
        remote_addr, full_path = None, None
    collection.insert_one({
        "is_exception": is_exception, "type": et, "value": ev, "stack": stack,
        "remote_addr": remote_addr, "full_path": full_path,
        "timestamp": int(datetime.now().timestamp()), **kwargs,
    })


def GeneFabLogger(*, call=None, message=None, with_side_effects=None):
    logger = getLogger("genefab3")
    if call is None:
        if with_side_effects or message:
            from genefab3.common.exceptions import GeneFabConfigurationException
            raise GeneFabConfigurationException(
                "No direct call to GeneFabLogger(), but kwargs passed",
                message=message, with_side_effects=with_side_effects,
            )
    else:
        if with_side_effects:
            side_effect_methods, printable_kwargs = [], {}
            for key, value in with_side_effects.items():
                if isinstance(key, Callable) and value:
                    side_effect_methods.append(key)
                else:
                    printable_kwargs[key] = value
            if isinstance(call, str) and message:
                printable_kwargs[call] = message
            for method in side_effect_methods:
                method(**printable_kwargs)
        else:
            printable_kwargs = {}
        method = getattr(logger, call, None)
        if not isinstance(method, Callable):
            from genefab3.common.exceptions import GeneFabConfigurationException
            raise GeneFabConfigurationException(
                "Incorrect call method for GeneFabLogger()", call=call,
            )
        else:
            method(f"{message or 'No message'}; {repr(printable_kwargs)}")
    return logger


class MongoDBLogger(Handler):
    def __init__(self, collection):
        self.collection = collection
        super().__init__()
    def emit(self, record):
        if self.collection:
            log_to_mongo_collection(
                self.collection,
                et=record.levelname, ev=record.getMessage(),
                stack=record.stack_info, is_exception=False,
            )
