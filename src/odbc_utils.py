import pandas as pd, pyodbc

def getDataFrameODBC(cursor, sql=None):
    # can also get DataFrame from previously executed cursor
    if sql is not None:
        cursor.execute(sql)

    col_names = []
    for col in cursor.description:
        col_names.append(col[0])
    data = cursor.fetchall()
    return pd.DataFrame.from_records(data=data, columns=col_names if col_names else None)