from IPython.core.magic import (Magics, magics_class, cell_magic, line_magic, line_cell_magic)
from IPython.display import display
from jupyter_ui_poll import ui_events
from datetime import timedelta
import concurrent.futures, threading, ipywidgets, time, sys

@magics_class
class SqlMagic(Magics):

    def __init__(self, shell):
        # must call the parent constructor
        super(SqlMagic, self).__init__(shell)
        print('sqlmagic loaded')

        # connection vars
        self.con = None
        self.cursor = None

        # thread pool
        self.pool = concurrent.futures.ThreadPoolExecutor(2)

        # completion info
        self.event_stop_counter = None
        self.event_cell_waiting = None
        self.start_time = None
        self.exc_info = None
        self.cancelled = False

        # widgets & layout
        self.noprint = None
        self.output_varname = None
        self.lbl_counter = None
        self.btn_cancel = None
        self.stmt_info = None

    @cell_magic
    def connect(self, line, cell):
        """Establishes database connection"""

        raise NotImplementedError

    @line_magic
    def close(self, line):
        """Close current connection (if any)"""

        if self.cursor:
            self.cursor.close()
        if self.con:
            self.con.close()

        print('connection closed')

    @line_cell_magic
    def sqlm(self, line, cell=None):
        """
        Execute SQL code (either from line, cell, or file),
        print returned DataFrame (if not noprint in first line; only for cell),
        and store DataFrame in local variable.
        """

        # check connection
        if not self.cursor:
            print('not connected')
            return

        # prepare sql
        if cell is None:
            sql = line

            self.noprint = False
            self.output_varname = 'df'
        else:
            # line parsing
            line = line.split(' ')

            # file or cell SQL
            if 'file' in line:
                with open(file=cell.strip(), encoding='utf-8') as f:
                    sql = f.read()
            else:
                sql = cell

            # check noprint
            self.noprint = 'noprint' in line

            # output variable name from line
            self.output_varname = next(filter(lambda x: x.startswith('var='), line), None)
            if self.output_varname:
                self.output_varname = self.output_varname.split('=')[1]
            else:
                self.output_varname = 'df'

        # counter, button, layout, & display
        self.lbl_counter = ipywidgets.Label(value='initialising ...',
                                            layout=ipywidgets.Layout(height='initial',
                                                                     margin='0'))
        self.btn_cancel = ipywidgets.Button(description='Cancel', button_style='danger',
                                            layout=ipywidgets.Layout(width='initial',
                                                                     margin='0 0 0 10px'))
        self.stmt_info = ipywidgets.HBox(children=[self.lbl_counter, self.btn_cancel],
                                         layout=ipywidgets.Layout(margin='0 0 5px 0'))
        display(self.stmt_info)

        # reset vars
        self.event_stop_counter = threading.Event()
        self.event_cell_waiting = threading.Event()
        self.cancelled = False
        self.start_time = time.perf_counter()

        # start statement in new thread
        future = self.pool.submit(self.exec_statement, sql)
        future.add_done_callback(self.statement_done)

        # update counter
        self.pool.submit(self.update_counter)

        # cancel button
        self.btn_cancel.on_click(self.onclick_cancel_statement)

        # using the fantastic https://github.com/Kirill888/jupyter-ui-poll to process UI events, but block further cell execution
        with ui_events() as poll:
            while not self.event_cell_waiting.is_set():
                poll(1)
                time.sleep(0.1)

    def exec_statement(self, sql):
        self.exc_info = None
        try:
            self.cursor.execute(sql)
        except:
            self.exc_info = sys.exc_info()

    def onclick_cancel_statement(self, btn):
        self.btn_cancel.close()
        self.cancelled = True
        self.cancel_statement()

    def cancel_statement(self):
        """Cancel current statement"""

        raise NotImplementedError

    def update_counter(self):
        """Update the counter during the execution of a statement."""

        while True:
            if self.event_stop_counter.is_set():
                return
            elif self.start_time:
                td = timedelta(seconds=time.perf_counter() - self.start_time)
                if not self.cancelled:
                    self.lbl_counter.value = f'Statement running for {td}'
                else:
                    self.lbl_counter.value = f'Statement awaiting cancellation ({td})'
            time.sleep(1)

    def statement_done(self, future):
        """Run after a statement has completed (normally, failed, or cancelled)."""

        # remove stop button & stop counter
        self.btn_cancel.close()
        self.event_stop_counter.set()

        # statement time
        td = timedelta(seconds=time.perf_counter() - self.start_time)

        # widget states are not cleared after a cell is re-run, thus only growing in size and never decreasing.
        # thus, when saving, it is better not to lose widget states and instead print out the last information
        # to the console
        try:
            if self.cancelled:
                print(f'Statement cancelled after {td}')
            elif self.exc_info:
                raise self.exc_info[1].with_traceback(self.exc_info[2])
            else:
                print(f'Statement completed in {td}')

                if self.has_results():
                    self.lbl_counter.value = 'Parsing response, creating DataFrame ...'

                    df = self.getDataFrame()
                    self.shell.user_ns.update({self.output_varname: df})

                    if not self.noprint:
                        display(df)
                    else:
                        print(f'{len(df.index):,} rows returned to {self.output_varname}')
                else:
                    print(f'{self.cursor.rowcount:,} rows updated')
        finally:
            # remove dynamic statement info + cell release
            # do after all is done, as some operations take a while
            self.stmt_info.close()
            self.event_cell_waiting.set()

    def has_results(self):
        """
        Return True if the cursor is bearing results.
        The statement can be assumed to not have been cancelled or failed.
        """

        raise NotImplementedError

    def getDataFrame(self):
        """
        Construct and return the result DataFrame.
        This is called after has_results().
        """

        raise NotImplementedError


# In order to actually use these magics, you must register them with a
# running IPython. Implement in child classes.
def load_ipython_extension(ipython):
    raise NotImplementedError
