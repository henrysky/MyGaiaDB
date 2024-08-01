import contextlib
import os
import re
import sqlite3
import stat
import sys
import sysconfig
from typing import List, Optional

import pandas as pd
from tqdm import tqdm

from mygaiadb import (
    __version__,
    allwise_sql_db_path,
    catwise_sql_db_path,
    gaia_sql_db_path,
    mygaiadb_default_db,
    mygaiadb_usertable_db,
    tmass_sql_db_path,
    mygaiadb_path,
)
from mygaiadb.query.callbacks import QueryCallback


class LocalGaiaSQL:
    """
    Class for local Gaia SQL database

    Parameters
    ----------
    load_tmass : bool, optional (default=True)
        Whether to load 2mass table
    load_allwise : bool, optional (default=True)
        Whether to load allwise table
    load_ext : bool, optional (default=True)
        Whether to load sqlite extension
    readonly_guard : bool, optional (default=True)
        Whether to ensure the databases are read-only
    """
    def __init__(
        self,
        load_tmass: bool=True,
        load_allwise: bool=True,
        load_catwise: bool=True,
        load_ext: bool=True,
        readonly_guard: bool=True,
    ):
        self.load_tmass = load_tmass
        self.load_allwise = load_allwise
        self.load_catwise = load_catwise
        self.load_ext = load_ext
        self.readonly_guard = readonly_guard
        self.attached_db_name = []

        # flag for windows or not
        self.win32 = sys.platform.startswith("win32")

        self.conn, self.cursor = self._load_db()

        # ipython Auto-completion
        try:
            from IPython import get_ipython
        except ImportError:
            pass
        else:
            if (ipy := get_ipython()) is not None:

                def list_all_tables_completer(ipython, event):
                    out = self.list_all_tables()
                    out.extend(
                        list(f"user_table.{i}" for i in self.list_user_tables().keys())
                    )
                    return out

                def usertable_completer(ipython, event):
                    out = self.list_user_tables()
                    return out

                ipy.set_hook(
                    "complete_command",
                    list_all_tables_completer,
                    re_key=".*get_table_column",
                )
                ipy.set_hook(
                    "complete_command",
                    usertable_completer,
                    re_key=".*remove_user_table",
                )

    def _check_callbacks_header(self, headers: List[str], callbacks: List[QueryCallback]):
        """
        Helper function to check if all columns required by all callbacks are presented in query

        Parameters
        ----------
        headers: List[str]
            List of query result header
        callbacks: List[QueryCallback]
            List of callbacks used in a query
        """
        for i in callbacks:
            for j in i.required_col:
                if j not in headers:
                    raise NameError(
                        f"Callback for new column {i.new_col_name} requires column {j} but not presented in your query"
                    )

    def _result_after_callbacks(self, df: pd.DataFrame, callbacks: List[QueryCallback]):
        for i in callbacks:
            func_dist = {}
            for j in i.required_col:
                func_dist[j] = df[j]
            df[i.new_col_name] = i.func(**func_dist)
        return df

    def _read_only(self, file_path):
        # set read only premission for all loaded dataset to prevent accidental change
        os.chmod(file_path, stat.S_IREAD if self.win32 else 0o444)

    def _file_exist(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Database at {path} does not exist. You should set loading that database to False."
            )

    def preprocess_query(func):
        """
        Decoractor to preprocess query. Mostly to increase compatability with Gaia ADQL
        """

        def preprocess_logic(self, *args, **kwargs):
            # either it is keyworded or the first arguement
            if "query" in kwargs:
                query = kwargs["query"]
            else:
                query = args[0]

            # turn TOP * to LIMIT *
            m = re.search(r"TOP (\d+)", query, re.IGNORECASE)
            if m is not None:
                limit_num = m.group(1)
                query = re.sub(r"\s+[TOP]+\s+[0-9]+", "", query, flags=re.IGNORECASE)
                query += f""" LIMIT {limit_num}"""

            # turn true to 1
            query = re.sub(r"[']\s?\b(t)\b\s?[']", "1 ", query, flags=re.IGNORECASE)
            query = re.sub(r"[']\s?\b(f)\b\s?[']", "0 ", query, flags=re.IGNORECASE)
            query = re.sub(r"[']\s?\b(true)\b\s?[']", "1 ", query, flags=re.IGNORECASE)
            query = re.sub(r"[']\s?\b(false)\b\s?[']", "0 ", query, flags=re.IGNORECASE)
            if "query" in kwargs:
                kwargs["query"] = query
            else:
                args = list(args)
                args[0] = query
                args = tuple(args)
            return func(self, *args, **kwargs)

        return preprocess_logic

    def _load_db(self):
        """
        Get SQL database connection and cursor used in this work for Gaia DR3 as the main table; 2MASS, ALLWISE and gaia astrophysical parameters as virtual tables
        """
        self._file_exist(mygaiadb_default_db)
        conn = sqlite3.connect(gaia_sql_db_path)
        conn.create_function(
            "mygaiadb_version", 0, lambda: __version__, deterministic=True
        )
        c = conn.cursor()
        if self.load_ext:
            self._load_sqlite3_ext(conn)

        # ======================= must load table =======================
        self._file_exist(gaia_sql_db_path)
        if self.readonly_guard:
            self._read_only(gaia_sql_db_path)  # set read-only before loading it
        c.execute(f"""ATTACH DATABASE '{gaia_sql_db_path}' AS gaiadr3""")
        self.attached_db_name.append("gaiadr3")
        self._file_exist(mygaiadb_usertable_db)
        c.execute(
            f"""ATTACH DATABASE '{mygaiadb_usertable_db}' AS user_table"""
        )  # don't read-only
        # ======================= must load table =======================

        # ======================= optional table =======================
        if self.load_tmass:
            self._file_exist(tmass_sql_db_path)
            if self.readonly_guard:
                self._read_only(tmass_sql_db_path)  # set read-only before loading it
            c.execute(f"""ATTACH DATABASE '{tmass_sql_db_path}' AS tmass""")
            self.attached_db_name.append("tmass")
        if self.load_allwise:
            self._file_exist(allwise_sql_db_path)
            if self.readonly_guard:
                self._read_only(allwise_sql_db_path)  # set read-only before loading it
            c.execute(f"""ATTACH DATABASE '{allwise_sql_db_path}' AS allwise""")
            self.attached_db_name.append("allwise")
        if self.load_catwise:
            self._file_exist(catwise_sql_db_path)
            if self.readonly_guard:
                self._read_only(catwise_sql_db_path)  # set read-only before loading it
            c.execute(f"""ATTACH DATABASE '{catwise_sql_db_path}' AS catwise""")
            self.attached_db_name.append("catwise")
        # ======================= optional table =======================
        return conn, c

    @staticmethod
    def _load_sqlite3_ext(c):
        c.enable_load_extension(True)
        # Find and load the library
        _ext_suffix = sysconfig.get_config_var("EXT_SUFFIX")
        _lib = mygaiadb_path.joinpath(f"astroqlite_c{_ext_suffix}")
        if _lib.exists():
            c.load_extension(_lib.as_posix())
        else:
            raise ImportError("MyGaiaDB SQL C extension not found")

    @preprocess_query
    def save_csv(
        self,
        query: str,
        filename: str,
        chunksize: int=50000,
        overwrite: bool=True,
        callbacks: Optional[List[QueryCallback]]=None,
        comments: bool=True,
    ):
        """
        Given query, save the fetchall() result to csv, "chunksize" number of rows at each time until finished

        Parameters
        ----------
        query : str
            Query string
        filename : str
            Filename (*.csv) to be saved
        chunksize : int, optional, default=50000
            Number of rows to do in one batch
        overwrite : bool, optional, default=True
            Whether to overwrite csv file if it already exists
        callbacks : List[QueryCallback], optional, default=None
            List of mygaiadb callbacks
        comments : bool, optional, default=True
            Whether to save the query as comment lines in csv file

        Returns
        -------
        None
        """
        if callbacks is not None and not isinstance(callbacks, list):
            raise TypeError("callbacks must be a list")

        self.cursor.execute(query)
        if os.path.exists(filename) and not overwrite:
            raise SystemError(f"{os.path.abspath(filename)} already existed!")
        f = open(filename, "w")
        if comments:
            comment_char = "# "
            query_commented = query.replace("\n", "\n" + comment_char)
            if "\n" in query_commented[:1]:
                # in case the first character is new line
                query_commented = query_commented[1:]
            else:
                # in case the first character is NOT new line so we need to add comment in the first line
                query_commented = comment_char + query_commented
            if "\n" in query_commented[-3:-2]:
                # in case the last character is new line, so will mess up the csv header
                query_commented = query_commented[:-2]
            else:
                # if not we need to add a new line so header wont be in the comment line
                query_commented = query_commented + "\n"
            f.write(query_commented)
        # write header rows
        header_og = [d[0] for d in self.cursor.description]
        if callbacks is not None:
            header_big = [d[0] for d in self.cursor.description]
            header_big.extend([i.new_col_name for i in callbacks])
            self._check_callbacks_header(header_og, callbacks)
        else:
            header_big = header_og
        first_flag = True
        with tqdm(unit=" rows") as pbar:
            pbar.set_description_str("Rows written: ")
            while True:  # looping until the end
                results = self.cursor.fetchmany(chunksize)
                if results == []:
                    break
                _df = pd.DataFrame(results, columns=header_og)
                if callbacks is not None:
                    _df = self._result_after_callbacks(_df, callbacks)
                _df.to_csv(
                    f,
                    mode="a" if not first_flag else "w",
                    index=False,
                    header=False if not first_flag else True,
                    lineterminator="\n",
                )
                first_flag = False
                pbar.update(len(_df))
        return None

    @preprocess_query
    def query(self, query: str, callbacks: Optional[List[QueryCallback]]=None):
        """
        Get result from query to pandas dataframe, ONLY USE THIS FOR SMALL QUERY

        Parameters
        ----------
        query : str
            Query string
        callbacks : List[QueryCallback], optional, default=None
            List of mygaiadb callbacks

        Returns
        -------
        df: pandas.Dataframe
        """
        _df = pd.read_sql_query(query, self.conn)
        if callbacks is not None:
            self._check_callbacks_header(_df.columns, callbacks)
            _df = self._result_after_callbacks(_df, callbacks)
        return _df

    @preprocess_query
    def execution_plan(self, query: str):
        """
        Get execution plan for a query, to debug to improve indexing

        Parameters
        ----------
        query : str
            Query string
        """
        query = (
            """
        EXPLAIN QUERY PLAN
        """
            + query
        )
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def xmatch(self, user_id, db_id, query):
        """
        cross-matching given a list of source_id
        """
        raise NotImplementedError()

    def upload_user_table(self, df: pd.DataFrame, tablename: str):
        """
        Add a custom user table

        Parameters
        ----------
        df : pandas.Dataframe
            pandas dataframe to be added
        tablename: str
            Table name
        """
        with contextlib.closing(sqlite3.connect(mygaiadb_usertable_db)) as conn:
            df.to_sql(f"{tablename}", conn, if_exists="fail", index=False)

    def remove_user_table(self, tablename: str, reclaim: bool=False):
        """
        Remove a custom user table

        Parameters
        ----------
        tablename: str
            Table name
        reclaim: bool, optional, default=False
            Whether to reclaim disk space after removing a table
        """
        with contextlib.closing(sqlite3.connect(mygaiadb_usertable_db)) as conn:
            conn.execute(f"""DROP TABLE {tablename}""")
            if reclaim:
                conn.execute("""VACUUM""")

    def list_user_tables(self):
        """
        Get the list of user table name

        Returns
        -------
        result: dict
            dictionary of all user table name and columns
        """
        self.cursor.execute(
            """
        SELECT name
        FROM user_table.sqlite_schema
        WHERE type ='table'
        """
        )
        a = self.cursor.fetchall()
        result = {}

        # TODO: add table length??
        for i in a:
            name = i[0]
            cols = self.get_table_column(name=f"user_table.{name}")
            result[name] = cols

        return result

    def list_all_tables(self):
        """
        Get the list of all table name

        Returns
        -------
        result: list
            list of tables with the format of DATABASE_NAME.TABLE_NAME
        """
        result = []
        for i in self.attached_db_name:
            self.cursor.execute(
                f"""
            SELECT name FROM {i}.sqlite_schema
            WHERE type ='table'
            """
            )
            a = self.cursor.fetchall()
            result.extend(list(f"{i}.{_a[0]}" for _a in a))
        return result

    def get_table_column(self, name: str):
        """
        Get the list of column from a table

        Parameters
        ----------
        name: str
            Table name with the format of DATABASE_NAME.TABLE_NAME

        Returns
        -------
        result: list
            list of columns of DATABASE_NAME.TABLE_NAME
        """
        if "." not in name:
            raise NameError(
                "Table name need to be with the format of DATABASE_NAME.TABLE_NAME"
            )
        result = pd.read_sql_query(f"""SELECT * FROM {name} LIMIT 1""", self.conn)
        return [i for i in result.columns]
