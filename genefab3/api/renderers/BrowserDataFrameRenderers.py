from flask import Response


TABLE_CSS = """
    table {table-layout: fixed; white-space: nowrap;}
    th, td {text-align: left;}
"""


def twolevel(obj, indent=None):
    """Placeholder method""" # TODO
    return Response(
        f"<style>{TABLE_CSS}</style>" +
        obj.fillna("").to_html(index=False, col_space="1in"),
        mimetype="text/html",
    )


def threelevel(obj, indent=None):
    """Placeholder method""" # TODO
    return Response(
        f"<style>{TABLE_CSS}</style>" +
        obj.fillna("").to_html(index=False, col_space="1in"),
        mimetype="text/html",
    )
