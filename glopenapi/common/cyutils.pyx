#cython: language_level=3
cimport cython


def blazing_json_normalize_itertuples(dict source, int maxlevel=128, tuple keys=()):
    cdef:
        str k
        object v
    for k, v in source.items():
        if isinstance(v, dict):
            if len(keys) < maxlevel:
                yield from blazing_json_normalize_itertuples(
                    v, maxlevel, keys if (k == "") else keys+(k,),
                )
            else:
                break
        else:
            yield ((keys, v) if (k == "") else (keys+(k,), v))


def blazing_json_normalize_iterbranches(dict source, int maxlevel=128, tuple keys=()):
    cdef:
        str k
        object v
    for k, v in source.items():
        if isinstance(v, dict):
            if len(keys) < maxlevel:
                yield from blazing_json_normalize_iterbranches(
                    v, maxlevel, keys if (k == "") else keys+(k,),
                )
            else:
                break
        else:
            yield ((keys+(v,)) if (k == "") else (keys+(k, v)))


cpdef list blazing_json_normalize_tolist(dict source: dict, list sink: list, tuple keys: tuple):
    cdef:
        str k
        object v
    for k, v in source.items():
        if isinstance(v, dict):
            blazing_json_normalize_tolist(
                v, sink, keys if (k == "") else keys+(k,),
            )
        else:
            sink.append((keys+(v,) if (k == "") else keys+(k,v)))
    return sink
