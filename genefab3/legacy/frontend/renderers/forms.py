from genefab3.common.exceptions import GeneFabParserException
from flask import Response


DATATYPE_DROPDOWN_HTML = """
<style>
    * {font-size: 13pt; font-family: "Roboto", sans-serif; font-weight: 300}
    b {font-weight: 500}
    strong {color: #00A; font-weight: 500}
</style>
<script>
function follow() {
    window.location.href += "&datatype=" + escape(
        document.getElementById("datatype").value
    );
}
</script>
<b>Data <strong>type</strong> to retrieve:</b><br>
&datatype=
<select id='datatype'>
    <option value='unnormalized counts' selected>unnormalized counts</option>
</select>
<input type='button' value='Go' onclick='follow()'>
"""


def needs_datatype(e):
    return isinstance(e, GeneFabParserException) and ("datatype=" in str(e))


def render_dropdown(obj, context):
    if obj == "datatype":
        return Response(DATATYPE_DROPDOWN_HTML.strip(), mimetype="text/html")
    else:
        raise NotImplementedError(f"render_dropdown(obj='{obj}'")
