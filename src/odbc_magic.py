from IPython.core.magic import (magics_class, line_magic, cell_magic, line_cell_magic)
import pyodbc, pandas as pd, numpy as np
from sql_magic import SqlMagic


@magics_class
class OdbcMagic(SqlMagic):
    CONNECTION_PARS = ['DRIVER', 'SERVER', 'DATABASE', 'UID', 'PWD']
    REQ_PARS = CONNECTION_PARS

    @cell_magic
    def connect(self, line, cell):
        con_pars = {}
        for cell_line in cell.strip().split('\n'):
            par, val = cell_line.split('=', maxsplit=1)
            if par in self.CONNECTION_PARS:
                con_pars[par] = val
            else:
                print(f'not a valid parameter (ignored): {par}')

        if all(par in con_pars for par in self.REQ_PARS):
            url = ';'.join([f'{x[0]}={x[1]}' for x in con_pars.items()])
            self.con = pyodbc.connect(url, autocommit=True)

            url = url.replace(con_pars['PWD'], '****')
            print(f'connected to {url}')

            self.cursor = self.con.cursor()
            self.shell.user_ns.update({'cursor': self.cursor})
        else:
            print(f'to connect, at least {self.REQ_PARS} need to be specified')

    @line_magic
    def close(self, line):
        super().close(line)

    @line_cell_magic
    def sqlm(self, line, cell=None):
        super().sqlm(line, cell)

    def cancel_statement(self):
        self.cursor.cancel()

    def has_results(self):
        return self.cursor.description is not None

    def getDataFrame(self):
        col_names = []
        for col in self.cursor.description:
            col_names.append(col[0])
        data = self.cursor.fetchall()
        return pd.DataFrame.from_records(data=data, columns=col_names if col_names else None)


# Register magic with a running IPython.
def load_ipython_extension(ipython):
    ipython.register_magics(OdbcMagic)
