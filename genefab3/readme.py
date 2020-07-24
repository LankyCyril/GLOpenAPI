from os.path import join, split, abspath
from re import sub


def readme(html_path=None, document="index.html", url_root="/"):
    """Serve readme HTML file(s)"""
    if html_path is None:
        html_path = join(
            split(split(abspath(__file__))[0])[0],
            "readme", document
        )
    try:
        with open(html_path, mode="rt") as handle:
            template = handle.read()
    except (FileNotFoundError, OSError, IOError):
        template = "Hello, Space! (No readme at %URL_ROOT%)"
    html = sub(r'%URL_ROOT%', url_root, template)
    return html
