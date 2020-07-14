from datetime import datetime


GENELAB_ROOT = "https://genelab-data.ndc.nasa.gov"
API_ROOT = "https://genelab-data.ndc.nasa.gov/genelab"
INDEX_BY = "Sample Name"
DEFAULT_NAME_DELIMITER = "-"


def date2stamp(fd, key="date_modified", fallback_key="date_created", fallback_value=-1):
    """Convert date like 'Fri Oct 11 22:02:48 EDT 2019' to timestamp"""
    strdate = fd.get(key)
    if strdate is None:
        strdate = fd.get(fallback_key)
    if strdate is None:
        return fallback_value
    else:
        try:
            dt = datetime.strptime(strdate, "%a %b %d %H:%M:%S %Z %Y")
        except ValueError:
            return fallback_value
        else:
            return int(dt.timestamp())
