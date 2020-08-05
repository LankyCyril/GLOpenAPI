from os.path import join, split, abspath
from re import sub


def interactive_doc(html_path=None, document="docs.html", url_root="/"):
    """Serve an interactive documentation page"""
    if html_path is None:
        html_path = join(
            split(split(abspath(__file__))[0])[0],
            "html", document
        )
    try:
        with open(html_path, mode="rt") as handle:
            template = handle.read()
    except (FileNotFoundError, OSError, IOError):
        template = "Hello, Space! (No documentation at %URL_ROOT%)"
    html = sub(r'%URL_ROOT%', url_root, template)
    return html
