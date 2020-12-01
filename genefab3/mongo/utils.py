from bson import Code
from bson.errors import InvalidDocument as InvalidDocumentError
from pandas import isnull
from collections.abc import ValuesView


def format_units(entry, unit_key, units_format):
    """Replace entry[""] with value with formatted entry[unit_key], discard entry[unit_key]"""
    return {
        k: units_format.format(value=v, unit=entry[unit_key]) if k == "" else v
        for k, v in entry.items()
        if k != unit_key
    }


def harmonize_query(query, lowercase=True, units_format=None):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.')"""
    unit_key = "unit" if lowercase else "Unit"
    if isinstance(query, dict):
        harmonized = {}
        for k, v in query.items():
            harmonized_key = k.replace("$", "_").replace(".", "_")
            if lowercase:
                harmonized_key = harmonized_key.lower()
            if harmonized_key == "_id":
                harmonized_key = "__id"
            if harmonized_key not in harmonized:
                if isinstance(v, (list, dict)):
                    harmonized[harmonized_key] = harmonize_query(
                        v, lowercase=lowercase, units_format=units_format,
                    )
                else:
                    harmonized[harmonized_key] = v
            else:
                raise InvalidDocumentError("Harmonized keys conflict")
        is_unit_formattable = (
            units_format and ("" in harmonized) and (unit_key in harmonized) and
            (harmonized[unit_key] != "") and (not isnull(harmonized[unit_key]))
        )
        if is_unit_formattable:
            return format_units(
                harmonized, unit_key=unit_key, units_format=units_format,
            )
        else:
            return harmonized
    elif isinstance(query, (list, ValuesView)):
        return [
            harmonize_query(q, lowercase=lowercase, units_format=units_format)
            for q in query
        ]
    else:
        return query


def replace_doc(collection, query, doc, harmonize=False):
    """Shortcut to drop all instances and replace with updated instance"""
    collection.delete_many(query)
    if harmonize:
        insert_query = harmonize_query({**query, **doc})
    else:
        insert_query = {**query, **doc}
    collection.insert_one(insert_query)


def get_collection_fields(collection, skip=set()):
    """Parse collection for keys, except for `skip`; see: https://stackoverflow.com/a/48117846/590676"""
    reduced = collection.map_reduce(
        Code("function() {for (var key in this) {emit(key, null);}}"),
        Code("function(key, stuff) {return null;}"), "_",
    )
    return set(reduced.distinct("_id")) - {"_id"} - skip
