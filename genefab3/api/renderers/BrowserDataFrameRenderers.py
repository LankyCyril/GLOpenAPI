from flask import Response


def twolevel(obj, indent=None):
    """Placeholder method""" # TODO
    TABLE_CSS = "table {table-layout: fixed; white-space: nowrap}"
    return Response(
        f"<style>{TABLE_CSS}</style>" +
        obj.fillna("").to_html(index=False, col_space="1in"),
        mimetype="text/html",
    )


def threelevel(obj, indent=None):
    """Placeholder method""" # TODO
    return None
