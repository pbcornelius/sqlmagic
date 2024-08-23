import pandas as pd
import pyodbc
import struct
from datetime import datetime, timedelta, timezone

def getDataFrameODBC(cursor, sql=None):
    # can also get DataFrame from previously executed cursor
    if sql is not None:
        cursor.execute(sql)

    col_names = []
    for col in cursor.description:
        col_names.append(col[0])
    data = cursor.fetchall()
    return pd.DataFrame.from_records(data=data, columns=col_names if col_names else None)


def convert_datetimeoffset(dto_value):
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    # ref: https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
    if dto_value is None:
        return None
    else:
        tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
        return datetime(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000,
                        timezone(timedelta(hours=tup[7], minutes=tup[8])))


def add_datetimeoffset_converter(connection):
    connection.add_output_converter(-155, convert_datetimeoffset)
