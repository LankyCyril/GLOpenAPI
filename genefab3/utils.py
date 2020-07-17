from datetime import datetime
from re import sub
from numpy import zeros
from functools import lru_cache


GENELAB_ROOT = "https://genelab-data.ndc.nasa.gov"
COLD_API_ROOT = "https://genelab-data.ndc.nasa.gov/genelab"
INDEX_BY = "Sample Name"


def date2timestamp(fd, key="date_modified", fallback_key="date_created", fallback_value=-1):
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


@lru_cache(maxsize=None)
def force_default_name_delimiter(string):
    """Replace variable delimiters (._-) with '-' (default)"""
    return sub(r'[._-]', "-", string)


@lru_cache(maxsize=None)
def levenshtein_distance(v, w):
    """Calculate levenshtein distance between two sequences"""
    m, n = len(v), len(w)
    dp = zeros((m+1, n+1), dtype=int)
    for i in range(m+1):
        for j in range(n+1):
            if i == 0:
                dp[i, j] = j
            elif j == 0:
                dp[i, j] = i
            elif v[i-1] == w[j-1]:
                dp[i, j] = dp[i-1, j-1]
            else:
                dp[i, j] = 1 + min(dp[i, j-1], dp[i-1, j], dp[i-1, j-1])
    return dp[m, n]
