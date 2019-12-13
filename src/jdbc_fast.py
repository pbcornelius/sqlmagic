from IPython.core.magic import (magics_class, line_magic, cell_magic, line_cell_magic)
import pandas as pd, numpy as np
from jdbc_magic import JdbcMagic
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
    'TINYINT': lambda o: o.getByte()[:],
    'SMALLINT': lambda o: o.getShort()[:],
    'INTEGER': lambda o: o.getInt()[:],
    'BIGINT': lambda o: o.getLong()[:],
    'REAL': lambda o: o.getFloat()[:],
    'FLOAT': lambda o: o.getDouble()[:],
    'DOUBLE': lambda o: o.getDouble()[:],
    'BOOLEAN': lambda o: o.getBoolean()[:],
    'TIME': lambda o: np.array(list(map(str, o.getTime()[:]))),
    'DATE': lambda o: o.getLong()[:].astype('datetime64[D]'),
    'TIMESTAMP': lambda o: o.getLong()[:].astype('datetime64[s]')
}


@magics_class
class JdbcFast(JdbcMagic):

    @cell_magic
    def connect(self, line, cell):
        super().connect(line, cell)

    @line_magic
    def close(self, line):
        super().close(line)

    @line_cell_magic
    def sqlm(self, line, cell=None):
        super().sqlm(line, cell)

    def getDataFrame(self):
        rsagg = jpype.JClass("rsagg.RsAgg")(self.cursor._rs)

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


# Register magic with a running IPython.
def load_ipython_extension(ipython):
    ipython.register_magics(JdbcFast)
