# sqlmagic
IPython SQL Magic

This started as a simple magic implementation for JDBC ([ipython-sql](https://github.com/catherinedevlin/ipython-sql) does not work with JDBC, b/c JDBC is not supported by SQL Alchemy). I ended up adding a few functionalities that I thought were missing from [ipython-sql](https://github.com/catherinedevlin/ipython-sql) (such as a timer and a cancel button), so I also implemented it for ODBC. The main drawback to [ipython-sql](https://github.com/catherinedevlin/ipython-sql) is that it (currently) only supports one connection at a time. This can easily be changed per driver, but will be more difficult if connections to more than one driver need to be maintained (due to the subclassing).

`sql_magic` contains the main methods for statement execution, display, scheduling, etc, and `jdbc/odbc_magic` contain the driver specific implementations. The direct driver access allows easy extensions (e.g., support for additional data types).

Connections are driver-specific. See the test files for MSSQL ODBC/H2 JDBC examples.

Connections and cursors are kept alive until `%close` is called. The cursor can also be accessed from Python through `cursor`.

Just like in [ipython-sql](https://github.com/catherinedevlin/ipython-sql), statements are executed with `%sqlm` or `%%sqlm`. I used `sqlm` instead of `sql` to not break interoperability with [ipython-sql](https://github.com/catherinedevlin/ipython-sql). It should be possible to use both at the same time.

To enable the timer and cancellation functionality, I use the fantastic https://github.com/Kirill888/jupyter-ui-poll to process UI events, but block further cell execution. The UI elements are powered by https://github.com/jupyter-widgets/ipywidgets.

Big thanks to the [ipython-sql](https://github.com/catherinedevlin/ipython-sql) for the starting point and inspiration.