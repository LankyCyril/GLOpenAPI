#cython: language_level=3
cimport cython


cpdef list blazing_json_normalize(dict source: dict, list sink: list, tuple keyseq: tuple):
    cdef:
        str k
        object v
    for k, v in source.items():
        if isinstance(v, dict):
            blazing_json_normalize(v, sink, keyseq if (k=="") else keyseq+(k,))
        else:
            sink.append((keyseq+(v,) if (k=="") else keyseq+(k,v)))
    return sink


cpdef list blazing_json_normalize_to_tuples(dict source: dict, list sink: list, tuple keyseq: tuple):
    cdef:
        str k
        object v
    for k, v in source.items():
        if isinstance(v, dict):
            blazing_json_normalize_to_tuples(
                v, sink, keyseq if (k=="") else keyseq+(k,),
            )
        else:
            sink.append((keyseq, v) if (k=="") else (keyseq+(k,), v))
    return sink
