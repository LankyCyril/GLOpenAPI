#!/usr/bin/env python
from contextlib import closing
from sqlite3 import connect
from pandas import read_sql, read_csv

# https://stackoverflow.com/a/12760070 (UNION ALL)
# https://stackoverflow.com/a/27709823 (UNION)
# !!! Must specify columns in the correct order, otherwise inverse joins will do wrong things

N = 5
COLUMNS = [["B", "C"], ["D", "E"], ["F", "G"], ["H", "J"], ["K", "A"]]

with closing(connect("test.db")) as connection:
    views = []
    cols = ",".join(COLUMNS[0])
    for i in range(N-1):
        views.append(f"VIEW:upto:{i+1}")
        left = ("TABLE:part" if i == 0 else "VIEW:upto") + f":{i}"
        right = f"TABLE:part:{i+1}"
        cols = ",".join([cols] + COLUMNS[i+1])
        condition = f"'{left}'.[entry] == '{right}'.[entry]"
        connection.cursor().execute(f"DROP VIEW IF EXISTS '{views[-1]}'")
        query = f"""CREATE VIEW '{views[-1]}' AS
            SELECT '{left}'.[entry],{cols} FROM
                '{left}' LEFT JOIN '{right}' ON {condition}
            UNION
            SELECT '{right}'.entry,{cols} FROM '{right}'
                LEFT JOIN '{left}' ON {condition}
            WHERE '{left}'.[entry] IS NULL
        """
        print(f"Making {views[-1]} from \t{left}\t{right}")
        connection.cursor().execute(query)
    df = read_sql(f"SELECT * FROM '{views[-1]}'", connection)
    for view in views:
        connection.cursor().execute(f"DROP VIEW '{view}'")
    df.set_index("entry", inplace=True)

orig = read_csv("D.tsv", sep="\t", index_col=0)

assert (df.columns == orig.columns).all()
assert (df.index == orig.index).all()
assert (df.fillna("NA") == orig.fillna("NA")).all().all()

print(df)
