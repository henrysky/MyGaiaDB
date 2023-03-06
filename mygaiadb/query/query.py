import os
import sys
import stat
import inspect
import sqlite3
import numpy as np
import pandas as pd
from tqdm import tqdm
from mygaiadb import gaia_sql_db_path, tmass_sql_db_path, allwise_sql_db_path, gaia_astro_param_sql_db_path


class QueryCallback():
    """
    Callback to add new column to SQL query on the fly
    """
    def __init__(self, new_col_name, func):
        """
        INPUT:
            new_col_name (string): Name of the new column you wan to add
            func (function): function that maps query columns to new columns, arguements of this function need to have \
                the same names as columns in query
        """
        self.new_col_name = new_col_name
        self.func = func
        self.required_col = list(inspect.getfullargspec(self.func))[0]

class LocalGaiaSQL():
    """
    Class for local Gaia SQL database
    """
    def __init__(self, load_tmass=True, load_allwise=True, load_gaia_astro_params=True, readonly_guard=True):
        """
        Parameters
        ----------
        load_tmass : bool
            whether to load 2mass table
        load_allwise : bool
            whether to load allwise table
        load_gaia_astro_params : bool
            whether to load gaia astrophysical parameters table
        readonly_guard : bool
            whether to ensure the databases are read-only
        """
        self.load_tmass = load_tmass
        self.load_allwise = load_allwise
        self.load_gaia_astro_params = load_gaia_astro_params
        self.readonly_guard = readonly_guard

        # flag for windows or not
        self.win32 = sys.platform.startswith("win32")
        
        self.conn, self.cursor = self._load_db()

    def _check_callbacks_header(self, headers, callbacks):
        """
        Helper function to check if all columns required by all callbacks are presented in query

        Parameters
        ----------
        headers : list of string
            SQL query result header
        callbacks : QueryCallback object
            callbacks used in a query
        """
        for i in callbacks:
            for j in i.required_col:
                if not j in headers:
                    raise NameError(f"Callback for new column {i.new_col_name} requires column {j} but not presented in your query")

    def _result_after_callbacks(self, df, callbacks):
        for i in callbacks:
            func_dist = {}
            for j in i.required_col:
                func_dist[j] = df[j]
            df[i.new_col_name] = i.func(**func_dist)
        return df

    def _read_only(self, file_path):
        # set read only premission for all loaded dataset to prevent mess-up
        if self.win32:
            os.chmod(file_path, stat.S_IREAD)
        else:
            os.chmod(file_path, 0o444)

    def _load_db(self):
        """
        Get SQL database connection and cursor used in this work for Gaia DR3 as the main table; 2MASS, ALLWISE and gaia astrophysical parameters as virtual tables
        """
        conn = sqlite3.connect(gaia_sql_db_path)
        c = conn.cursor()
        if self.load_tmass:
            if self.readonly_guard:
                self._read_only(tmass_sql_db_path)  # set read-only before loading it
            c.execute(f"""ATTACH DATABASE '{tmass_sql_db_path}' AS tmass""")
        if self.load_allwise:
            if self.readonly_guard:
                self._read_only(allwise_sql_db_path)  # set read-only before loading it
            c.execute(f"""ATTACH DATABASE '{allwise_sql_db_path}' AS allwise""")
        if self.load_gaia_astro_params:
            if self.readonly_guard:
                self._read_only(gaia_astro_param_sql_db_path)  # set read-only before loading it
            c.execute(f"""ATTACH DATABASE '{gaia_astro_param_sql_db_path}' AS gastrophysical_params""")
        return conn, c

    def save_csv(self, query, filename, chunchsize=50000, overwrite=True, callbacks=None):
        """
        Given query, save the fetchall() result to csv, "chunchsize" number of rows at each time until finished

        Parameters
        ----------
        query : string
            Query string
        filename : string
            filename (*.csv) to be saved
        Returns
        -------
        None
        """
        if callbacks is not None and not isinstance(callbacks, list):
            raise TypeError("callbacks must be a list")

        self.cursor.execute(query)
        if os.path.exists(filename) and not overwrite:
            raise SystemError(f"{os.path.abspath(filename)} already existed!")
        # write header rows
        header_og = [d[0] for d in self.cursor.description]
        if callbacks is not None:
            header_big = [d[0] for d in self.cursor.description]
            header_big.extend([i.new_col_name for i in callbacks])
            self._check_callbacks_header(header_og, callbacks)
        else: 
            header_big = header_og
        pd.DataFrame(columns=header_big).to_csv(filename, index=False)
        # csv_out.writerow(header)
        first_flag = True
        with tqdm(unit=" rows") as pbar:
            pbar.set_description_str("Rows written: ")
            while True:  # looping until the end
                results = self.cursor.fetchmany(chunchsize)
                if results == []:
                    break
                _df = pd.DataFrame(results, columns=header_og)
                if callbacks is not None:
                    _df = self._result_after_callbacks(_df, callbacks)
                _df.to_csv(filename, mode="a" if not first_flag else "w", index=False, header=False if not first_flag else True)
                first_flag = False
                pbar.update(len(_df))
        return None

    def query(self, query, callbacks=None):
        """
        Get result from query to pandas dataframe, ONLY USE THIS FOR SMALL QUERY

        Parameters
        ----------
        query : string
            Query string

        Returns
        -------
        pandas.Dataframe
        """
        _df = pd.read_sql_query(query, self.conn)
        if callbacks is not None:
            self._check_callbacks_header(_df.columns, callbacks)
            _df = self._result_after_callbacks(_df, callbacks)
        return _df

    def execution_plan(self, query):
        """
        Get execution plan for a query, to debug to improve indexing
        """
        query = """
        EXPLAIN QUERY PLAN
        """ + query
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def xmatch(user_id, db_id, query):
        """
        cross-matching
        """
