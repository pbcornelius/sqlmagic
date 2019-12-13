from IPython.core.magic import (magics_class, line_magic, cell_magic, line_cell_magic)
import jaydebeapi, pandas as pd
from sql_magic import SqlMagic


@magics_class
class JdbcMagic(SqlMagic):
    CONNECTION_PARS = ['jclassname', 'url', 'driver_args', 'jars', 'libs']
    REQ_PARS = ['jclassname', 'url']

    @cell_magic
    def connect(self, line, cell):
        con_pars = {}
        for cell_line in cell.strip().split('\n'):
            par, val = cell_line.split('=', maxsplit=1)
            if par in ('driver_args', 'jars'):
                con_pars[par] = val.split(';')
                if len(con_pars[par]) == 1:
                    con_pars[par] = con_pars[par][0]
            elif par in self.CONNECTION_PARS:
                con_pars[par] = val
            else:
                print(f'not a valid parameter (ignored): {par}')

        if all(par in con_pars for par in self.REQ_PARS):
            self.con = jaydebeapi.connect(**con_pars)
            print(f'connected to {self.con.jconn.getMetaData()}')
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
        self.cursor._prep.cancel()

    def has_results(self):
        return self.cursor._rs is not None

    def getDataFrame(self):
        col_names = []
        for col in range(1, self.cursor._meta.getColumnCount() + 1):
            col_names.append(self.cursor._meta.getColumnName(col))
        data = self.cursor.fetchall()
        return pd.DataFrame(data=data, columns=col_names if col_names else None)


# Register magic with a running IPython.
def load_ipython_extension(ipython):
    ipython.register_magics(JdbcMagic)
