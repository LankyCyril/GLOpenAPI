from os import environ
from genefab3.config import DEBUG_MARKERS


def is_flask_reloaded():
    """https://stackoverflow.com/a/9476701/590676"""
    return (environ.get("WERKZEUG_RUN_MAIN", None) == "true")


def is_debug():
    """Determine if app is running in debug mode"""
    return (environ.get("FLASK_ENV", None) in DEBUG_MARKERS)
