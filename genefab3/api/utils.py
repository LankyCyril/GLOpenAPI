from os import environ


def is_debug():
    """Determine if app is running in debug mode"""
    return (
        environ.get("FLASK_ENV", None)
        in {"development", "staging", "stage", "debug", "debugging"}
    )


def is_flask_reloaded():
    """https://stackoverflow.com/a/9476701/590676"""
    return (environ.get("WERKZEUG_RUN_MAIN", None) == "true")
