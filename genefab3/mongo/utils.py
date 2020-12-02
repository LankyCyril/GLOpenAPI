from bson import Code
from bson.errors import InvalidDocument as InvalidDocumentError
from pandas import isnull
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


def harmonize_document(query, lowercase=True, units_format=None, dropna=True):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.'), normalize case, format units, drop terminal NaNs"""
    unit_key = "unit" if lowercase else "Unit"
    if isinstance(query, dict):
        harmonized = {}
        for key, branch in query.items():
            harmonized_key = key.replace("$", "_").replace(".", "_")
            if lowercase:
                harmonized_key = harmonized_key.lower()
            if harmonized_key == "_id":
                harmonized_key = "__id"
            if harmonized_key not in harmonized:
                harmonized_branch = harmonize_document(
                    branch, lowercase, units_format, dropna,
                )
                if harmonized_branch:
                    harmonized[harmonized_key] = harmonized_branch
            else:
                raise InvalidDocumentError("Harmonized keys conflict")
        if units_format and is_unit_formattable(harmonized, unit_key):
            return format_units(harmonized, unit_key, units_format)
        else:
            return harmonized
    elif isinstance(query, (list, ValuesView)):
        return [
            harmonize_document(q, lowercase, units_format, dropna)
            for q in query
        ]
    elif (not dropna) or (not isempty(query)):
        return query
    else:
        return {}


def replace_document(collection, query, doc):
    """Shortcut to drop all instances and replace with updated instance"""
    collection.delete_many(query)
    collection.insert_one({**query, **doc})


def get_collection_fields(collection, skip=set()):
    """Parse collection for keys, except for `skip`; see: https://stackoverflow.com/a/48117846/590676"""
    reduced = collection.map_reduce(
        Code("function() {for (var key in this) {emit(key, null);}}"),
        Code("function(key, stuff) {return null;}"), "_",
    )
    return set(reduced.distinct("_id")) - {"_id"} - skip
