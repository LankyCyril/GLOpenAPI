from os import path
from re import sub, escape
from copy import deepcopy


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
