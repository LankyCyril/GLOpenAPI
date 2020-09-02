from re import sub
from pandas import Series


def to_cls(dataframe, target, continuous="infer", space_sub=lambda s: sub(r'\s', "", s)):
    """Convert a presumed annotation/factor dataframe to CLS format"""
    sample_count = dataframe.shape[0]
    if continuous == "infer":
        try:
            _ = dataframe[target].astype(float)
            continuous = True
        except ValueError:
            continuous = False
    elif not isinstance(continuous, bool):
        if continuous == "0":
            continuous = False
        elif continuous == "1":
            continuous = True
        else:
            error_message = "`continuous` can be either boolean-like or 'infer'"
            raise TypeError(error_message)
    if continuous:
        cls_data = [
            ["#numeric"], ["#" + target],
            dataframe[target].astype(float)
        ]
    else:
        if space_sub is None:
            space_sub = lambda s: s
        classes = dataframe[target].unique()
        class2id = Series(index=classes, data=range(len(classes)))
        cls_data = [
            [sample_count, len(classes), 1],
            ["# "+space_sub(classes[0])] + [space_sub(c) for c in classes[1:]],
            [class2id[v] for v in dataframe[target]]
        ]
    return "\n".join([
        "\t".join([str(f) for f in fields]) for fields in cls_data
    ])
