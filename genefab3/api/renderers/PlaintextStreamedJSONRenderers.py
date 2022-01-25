from json import dumps
from genefab3.common.utils import json_permissive_default


def json(obj, context=None, indent=None):
    """Display StreamedAnnotationValueCounts as JSON"""
    def content():
        yield '{'
        for i, (k, v) in enumerate(obj.items()):
            if i > 0:
                yield ','
            yield f'"{k}":'
            yield dumps(v, indent=indent, default=json_permissive_default)
        yield '}'
    return content, "application/json"
