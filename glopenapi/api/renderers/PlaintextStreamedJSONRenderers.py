from glopenapi.common.utils import pdumps


def json(obj, context=None, indent=None):
    """Display StreamedAnnotationValueCounts as JSON"""
    def content():
        yield '{'
        for i, (k, v) in enumerate(obj.items()):
            if i > 0:
                yield ','
            yield f'"{k}":'
            yield pdumps(v, indent=indent)
        yield '}'
    return content, "application/json"
