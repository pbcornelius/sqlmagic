# sqlmagic
IPython SQL Magic

This started as a simple magic implementation for JDBC ([ipython-sql](https://github.com/catherinedevlin/ipython-sql) does not work with JDBC, b/c JDBC is not supported by SQL Alchemy). I ended up adding a few functionalities that I thought were missing from [ipython-sql](https://github.com/catherinedevlin/ipython-sql) (such as a timer and a cancel button), so I also implemented it for ODBC. The main drawback to [ipython-sql](https://github.com/catherinedevlin/ipython-sql) is that it (currently) only supports one connection at a time. This can easily be changed per driver, but will be more difficult if connections to more than one driver need to be maintained (due to the subclassing).

#### Implementation

`sql_magic.py` contains the main methods for statement execution, display, scheduling, etc, and `jdbc/odbc_magic.py` contain the driver specific implementations. `jdbc_fast.py` is a significantly faster implementation using [`java-rsagg`](https://github.com/pbcornelius/java-rsagg) and JPype's ndarrays to by-pass the slow `jaydebeapi.fetchall()`. In a 1,000,000-rows table with all data types, the performance improvement on my laptop is 4min 26s -> 24.1s. The performance improvement is larger for literal types than for Objects/Strings. The direct driver access allows easy extensions (e.g., support for additional data types).

#### Use

Connections are driver-specific. See the test files for MSSQL ODBC/H2 JDBC examples. For `jdbc_fast`, the [`java-rsagg`](https://github.com/pbcornelius/java-rsagg) JAR needs to be added to the class path (if you add it to the `jar` connection parameter, jaydebeapi will take care of that).

Connections and cursors are kept alive until `%close` is called. The cursor can also be accessed from Python through `cursor`.

Just like in [ipython-sql](https://github.com/catherinedevlin/ipython-sql), statements are executed with `%sqlm` or `%%sqlm`. I used `sqlm` instead of `sql` to not break interoperability with [ipython-sql](https://github.com/catherinedevlin/ipython-sql). It should be possible to use both at the same time.

A simple example:
```
%%sqlm
SELECT *
FROM test
```

The cell magic `%%sqlm` comes with a few advanced options (in the first line, space separated):

* Instead of a SQL cmd, a SQL file can also be specified: 
```
%%sqlm file
path_to_a_sql_file
```

* `%%sqlm noprint` -> the result set is not printed.

* `%sqlm var=var_name` -> the result set (as a `DataFrame`) is stored in `var_name` (the default is `df`).

The rendering of the widgets doesn't completely work on Github, but looks fine with Jupyter itself.

#### Thanks

To enable the timer and cancellation functionality, I use the fantastic https://github.com/Kirill888/jupyter-ui-poll to process UI events, but block further cell execution. The UI elements are powered by https://github.com/jupyter-widgets/ipywidgets. I use Pandas DataFrames for the returned result sets, although this can be changed per driver implementation.

Big thanks to the [ipython-sql](https://github.com/catherinedevlin/ipython-sql) for the starting point and inspiration.
