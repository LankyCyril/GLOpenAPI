from functools import wraps


def refresh_json_store(db):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            print(repr(db))
            return func(*args, **kwargs)
        return wrapper
    return decorator
