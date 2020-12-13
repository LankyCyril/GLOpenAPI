from os import path
from re import sub, escape
from copy import deepcopy
from pandas import DataFrame


def walk_up(from_path, n_steps):
    if n_steps >= 1:
        return walk_up(path.split(from_path)[0], n_steps-1)
    else:
        return from_path


def map_replace(string, mappings):
    """Perform multiple replacements in one go"""
    return sub(
        r'|'.join(map(escape, mappings.keys())),
        lambda m: mappings[m.group()],
        string,
    )


def copy_and_drop(d, keys):
    """Deepcopy dictionary `d`, delete `d[key] for key in keys`"""
    d_copy = deepcopy(d)
    for key in keys:
        del d_copy[key]
    return d_copy


def INPLACE_set_attributes(dataframe, **kwargs):
    """Add custom attributes to dataframe"""
    if not isinstance(dataframe, DataFrame):
        raise TypeError("Not a DataFrame")
    else:
        for a, v in kwargs.items():
            try:
                dataframe._metadata.append(a)
                setattr(dataframe, a, v)
            except AttributeError:
                raise AttributeError(f"Cannot set attribute {a} of DataFrame")


def get_attribute(dataframe, a):
    """Retrieve custom attribute of dataframe"""
    if not isinstance(dataframe, DataFrame):
        raise TypeError("Not a DataFrame")
    elif a not in dataframe._metadata:
        raise AttributeError(f"DataFrame does not have attribute {a}")
    else:
        return getattr(dataframe, a)
