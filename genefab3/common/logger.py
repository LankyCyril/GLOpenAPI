from functools import lru_cache
from logging import getLogger, DEBUG


@lru_cache(maxsize=None)
def GeneFabLogger():
    logger = getLogger("genefab3")
    logger.setLevel(DEBUG)
    return logger
