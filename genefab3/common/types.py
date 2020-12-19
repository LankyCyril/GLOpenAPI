from werkzeug.datastructures import ImmutableDict
from numpy import nan
from collections.abc import Hashable
from itertools import zip_longest


def ImmutableTree(d, step_tracker=1, max_steps=256):
    """Converts nested dictionaries, lists, tuples into immutable equivalents"""
    if step_tracker >= max_steps:
        raise ValueError("Tree exceeded nestedness threshold", max_steps)
    elif isinstance(d, dict):
        return ImmutableDict({
            k: ImmutableTree(v, step_tracker+i)
            for i, (k, v) in enumerate(d.items(), start=1)
        })
    elif isinstance(d, (list, tuple)):
        return tuple(
            ImmutableTree(v, step_tracker+i)
            for i, v in enumerate(d, start=1)
        )
    else:
        return d


class HashableEnough():
    """Provides facilities to describe equality within a class based on a subset of fields"""

    def __init__(self, identity_fields, as_strings=()):
        """Describe equality within a class based on a subset of fields"""
        self.__identity_fields = tuple(identity_fields)
        self.__as_strings = set(as_strings)

    def __iter_identity_values__(self):
        """Iterate values of identity fields in a hash-friendly manner"""
        for field in self.__identity_fields:
            value = getattr(self, field, nan)
            if field in self.__as_strings:
                value = str(value)
            if not isinstance(value, Hashable):
                raise TypeError(
                    "{}: unhashable field value".format(type(self).__name__),
                    f"{field}={repr(value)}",
                )
            else:
                yield value

    def __eq__(self, other):
        """Compare values of identity fields between self and other"""
        return all(s == o for s, o in zip_longest(
            self.__iter_identity_values__(),
            getattr(other, "__iter_identity_values__", lambda: ())(),
            fillvalue=nan,
        ))

    def __hash__(self):
        """Hash values of identity fields as a tuple"""
        return hash(tuple(self.__iter_identity_values__()))
