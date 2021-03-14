from pandas import isnull
from functools import partial
from bson.errors import InvalidDocument as InvalidDocumentError
from collections.abc import ValuesView


def isempty(v):
    """Check if terminal leaf value is a null value or an empty string"""
    return isnull(v) or (v == "")


def is_unit_formattable(entry, unit_key):
    """Check if entry contains keys "" and unit_key and that entry[unit_key] is not empty"""
    return (
        ("" in entry) and (unit_key in entry) and
        (not isempty(entry[unit_key]))
    )


def format_units(entry, unit_key, units_format):
    """Replace entry[""] with value with formatted entry[unit_key], discard entry[unit_key]"""
    return {
        k: units_format.format(value=v, unit=entry[unit_key]) if k == "" else v
        for k, v in entry.items()
        if k != unit_key
    }


def harmonize_document(query, lowercase=True, units_format=None, dropna=True, depth_tracker=0, *, max_depth=32):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.'), normalize case, format units, drop terminal NaNs"""
    unit_key = "unit" if lowercase else "Unit"
    harmonizer_function = partial(
        harmonize_document, lowercase=lowercase, units_format=units_format,
        dropna=dropna, depth_tracker=depth_tracker+1,
    )
    if depth_tracker >= max_depth:
        raise InvalidDocumentError("Document exceeds maximum depth", max_depth)
    elif isinstance(query, dict):
        harmonized = {}
        for key, branch in query.items():
            harmonized_key = key.replace("$", "_").replace(".", "_")
            if lowercase:
                harmonized_key = harmonized_key.lower()
            if harmonized_key == "_id":
                harmonized_key = "__id"
            if harmonized_key not in harmonized:
                harmonized_branch = harmonizer_function(branch)
                if harmonized_branch:
                    harmonized[harmonized_key] = harmonized_branch
            else:
                raise InvalidDocumentError("Harmonized keys conflict")
        if units_format and is_unit_formattable(harmonized, unit_key):
            return format_units(harmonized, unit_key, units_format)
        else:
            return harmonized
    elif isinstance(query, (list, ValuesView)):
        return [hq for hq in (harmonizer_function(q) for q in query) if hq]
    elif (not dropna) or (not isempty(query)):
        return query
    else:
        return {}
