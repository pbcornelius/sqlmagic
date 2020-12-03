import numpy as np, pandas as pd, jaydebeapi
import jpype, jpype.imports
from jpype.types import *

datadict = {
    'CHAR': lambda o: np.array(o.getStr()[:]),
    'NCHAR': lambda o: np.array(o.getStr()[:]),
    'VARCHAR': lambda o: np.array(o.getStr()[:]),
    'NVARCHAR': lambda o: np.array(o.getStr()[:]),
    'LONGVARCHAR': lambda o: np.array(o.getStr()[:]),
    'LONGNVARCHAR': lambda o: np.array(o.getStr()[:]),
    'OTHER': lambda o: o.getObj()[:],
    'TINYINT': lambda o: o.getByteObj()[:] if o.nullable else o.getByte()[:],
    'SMALLINT': lambda o: o.getShortObj()[:] if o.nullable else o.getShort()[:],
    'INTEGER': lambda o: o.getIntObj()[:] if o.nullable else o.getInt()[:],
    'BIGINT': lambda o: o.getLongObj()[:] if o.nullable else o.getLong()[:],
    'REAL': lambda o: o.getFloatObj()[:] if o.nullable else o.getFloat()[:],
    'FLOAT': lambda o: o.getDouble()[:],
    'DOUBLE': lambda o: o.getDouble()[:],
    'BOOLEAN': lambda o: o.getBooleanObj()[:] if o.nullable else o.getBoolean()[:],
    'TIME': lambda o: np.array(list(map(str, o.getTime()[:]))),
    'DATE': lambda o: np.array(o.getLongObj()[:]).astype('datetime64[D]') if o.nullable else np.array(
        o.getLong()[:]).astype('datetime64[D]'),
    'TIMESTAMP': lambda o: np.array(o.getLongObj()[:]).astype('datetime64[s]') if o.nullable else np.array(
        o.getLong()[:]).astype('datetime64[s]')
}


def getDataFrameJDBC(cursor, sql=None):
    # can also get DataFrame from previously executed cursor
    if sql is not None:
        cursor.execute(sql)

    if cursor._rs is not None:
        # if there are results
        rsagg = jpype.JClass("rsagg.RsAgg")(cursor._rs)

        import java.lang.Exception
        try:
            rsagg.agg()
        except java.lang.Exception as ex:
            print(str(ex))
            print(ex.stacktrace())

        df = pd.DataFrame(columns=[col.name for col in rsagg.cols])
        for col in rsagg.cols:
            df[col.name] = datadict[col.typeName](col)
        return df
    else:
        # otherwise return empty DataFrame
        return pd.DataFrame()