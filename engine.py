import pandas as pd
import numpy as np
from datadough.query import TableQuery
from datadough.hangar import DataHangar

hangar = DataHangar("c:/temp/temp_hdf.h5")


class DataBase(object):
    """docstring for DataBase."""
    def __init__(self):
        """
        """
        self._data_object = DataObject()
        self._data_type = DataType()
        self._data_provider = DataProvider()
        self._currency = Currency()
        self._data_version = DataVersion()

        self._timeseries = Timeseries()

    def get_data(self, header, date_from, date_to, freq):
        """

        Parameters
        ----------
        header : pandas.Series or pandas.DataFrame
            if Series, dtype is int and index contains names to use on the
            retrieved data; if DataFrame, index is 'data_object_id',
            'data_type_id' etc. and columns contain names to use on the
            retrieved data, dtype either int or str
        date_from
        date_to
        freq

        Returns
        -------

        """
        if isinstance(header, pd.Series):
            dh = header.copy()
        else:
            dh = ConceptHeader().get_id(header)

        data = Timeseries().get_data(header, date_from, date_to)

        # implement freq
        data = data.resample(freq).last()

        return data

    def save_data(self, data_to_save):
        """

        Parameters
        ----------
        data_to_save : pandas.Series or pandas.DataFrame

        Returns
        -------
        None

        """
        df = data_to_save.copy()

        # deal with the columns: whatever columns are now, they need to be
        # integers (header ids)
        if not isinstance(df.columns, pd.MultiIndex):
            cols = df.columns
        else:
            cols = ConceptHeader().get_id(df.columns, create=True)

        df.columns = cols

        # deal with the index: index name better be set here to avoid
        # renaming later
        df.index.name = "obs_date"

        # melt: 'obs_date' column is repeated the same number of times as
        # there are columns other than 'obs_date'
        df = df.reset_index().melt(id_vars="obs_date",
                                   var_name="header_id",
                                   value_name="obs_value")

        # ready to save
        Timeseries().save(data=df)

        return


class Table(object):
    """docstring for Table.

    Parameters
    ----------
    key : str
        key to locate the table in the DataHangar
    schema : dict or None
        if None, the value is taken from HDFStore attributes from `hangar`
    """
    def __init__(self, key, schema=None):
        """
        """
        self.key = key

        if schema is None:
            with pd.HDFStore(hangar.path_to_hdf, mode='r') as h:
                for k, v in h.root._v_attrs[key].items():
                    setattr(self, ('_' + k), v)
        else:
            self._required_columns = schema.get(
                "required_columns", tuple())
            # TODO: watch out for empty list of keys!
            self._optional_columns = schema.get(
                "optional_columns", tuple())
            self._default_column = schema.get(
                "default_column", tuple())

        # columns
        columns = dict()
        for p in (list(self._required_columns) + list(self._optional_columns)):
            columns[p] = Column(p, self)

        self.columns = columns

    # def get_id(self, query):
    #     """Retrieve index values of rows where `query` condition holds.
    #
    #     Convenient to use in subsequent queries
    #
    #     Parameters
    #     ----------
    #     query : TableQuery
    #
    #     Returns
    #     -------
    #     qry : TableQuery
    #         of kind 'currency_id in (10, 20, 22)', ready to be parsed to
    #         pandas.HDFStore.select().
    #     """
    #     # TODO: implement this select!
    #     idx = self.filter(query=query).index
    #
    #     return idx

    @property
    def index(self):
        res = hangar.select_column(self.key, "index").index
        return res

    def filter(self, query):
        """Find the row(s) meeting the search criterion in `query`.

        Parameters
        ----------
        query : TableQuery

        Returns
        -------
        res : pandas.Series
            row(s) meeting the criterion
        """
        # TODO integrate pandas.HDFStore start, stop things?
        res = hangar.select(key=self.key, where=query.expression)

        return res

    def get_unique_id(self, query):
        """Find one row that meets the search criterion in `query`.

        If more than one rows are found, an error is thrown urging user to
        narrow the search terms.

        Parameters
        ----------
        query : TableQuery

        """
        # first, get all
        res = self.filter(query)

        if len(res) > 1:
            raise ValueError("More than one rows are found with these " +
                             "criteria; narrow down your search!")
        elif len(res) < 1:
            return []
        else:
            return res.index[0]

    def add_rows(self, new):
        """
        Parameters
        ----------
        new : pandas.Series or pandas.DataFrame
            with new data, columned with required columns of respective class
        """
        # to a DataFrame, as columns are required
        if isinstance(new, pd.Series):
            new = new.to_frame().T

        # check if some columns are missing
        new_reix = new.reindex(columns=self._required_columns)

        if new_reix.isnull().any().any():
            missing = new_reix.columns[new_reix.isnull().any(axis=0)]
            msg = ("The following required columns are missing: " +
                   "'{}', " * (len(missing)-1) + "'{}'.").format(*missing)
            raise ValueError(msg)

        # rows that are not in db already
        not_in_db = ~self.rows_in_db(new_reix)

        # add truly new rows
        # need to reindex first: watch out that the index in the HDFStore is
        #   unique!
        max_idx = self.index.max()

        # construct new index with unique values
        n_rows_to_save = not_in_db.sum()
        unq_idx = range(max_idx+1, max_idx+1+n_rows_to_save)

        # combine to a DataFrame
        df_to_save = new.loc[not_in_db, :]
        df_to_save.index = unq_idx

        # append
        hangar.append(self.key, df_to_save)

        return

    def rows_in_db(self, rows):
        """
        Parameters
        ----------
        rows : pandas.DataFrame or pandas.Series
            if Series, will be converetedcoerced to a DataFrame

        Returns
        -------
        res : pandas.Series
            of dtype bool indicating where a row of `rows` is already in db
        """
        if isinstance(rows, pd.Series):
            rows = rows.to_frame()

        # function to apply: will load rows where the entries match perfectly
        #   and calculate the number of such rows
        def apply_fun(row):
            res = len(self.filter(TableQuery(row)))
            return res

        # apply the function over rows
        flag = rows.apply(apply_fun, axis=1)

        # whenever there are more than 0 rows matching with `rows`, return True
        res = flag.gt(0)

        return res

    def construct_query_by_id(self, idx):
        """

        Parameters
        ----------
        idx : list-like
            of integer ids

        Returns
        -------

        """
        # if nothing was returned, output empty list
        if len(idx) < 1:
            expr = []

        elif len(idx) == 1:
            expr = self.key + "_id == " + str(idx.values[0])

        elif (np.diff(np.array(sorted(idx))) < 2).all():
            # check if possible to run a shortcut with between
            expr = (
                self.key + "_id" + " > {} and " +
                self.key + "_id" + " < {}").format(min(idx), max(idx))

        else:
            expr = self.key + "_id" + "in {}".format(tuple(idx))
            
        return expr


class DataObject(Table):
    """docstring for DataObject."""

    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("long_name", ),
            "default_column": "long_name"
        }
        super(DataObject, self).__init__(key="data_object", schema=schema)


class DataType(Table):
    """docstring for DataType."""

    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("short_name", "long_name", "nature", ),
            "default_column": "short_name",
        }
        super(DataType, self).__init__(key="data_type", schema=schema)


class DataProvider(Table):
    """docstring for DataProvider."""

    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("long_name", ),
            "default_column": "long_name"
        }
        super(DataProvider, self).__init__(key="data_provider", schema=schema)


class Currency(Table):
    """docstring for Currency."""

    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("iso", ),
            "optional_columns": ("long_name", ),
            "default_column": "iso"
        }

        super(Currency, self).__init__(key="currency", schema=schema)


class DataVersion(Table):
    """docstring for DataVersion."""

    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("header_id", "date_created", "description"),
            "default_column": "description"
        }

        super(DataVersion, self).__init__(key="data_version", schema=schema)

        self.default_version = self.get_unique_id(
            query=TableQuery("description == 'default'")
        )

    def add_new(self, new=None):
        """Convenience function able to generate new DataVersions on the fly.

        Parameters
        ----------
        new :

        Returns
        -------
        None

        """
        if new is None:
            new = pd.Series({"date_created": pd.Timestamp.today,
                             "description": "randomly generated version"})

        super(DataVersion, self).add_rows(new)

        return

    def get_newest(self, header_id):
        """

        Parameters
        ----------
        header_id : int or pandas.Series

        Returns
        -------

        """
        pass


class ConceptHeader(Table):
    """
    data_object, data_type, data_currency, data_provider 4-tuple.
    """
    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("data_object_id", "data_type_id",
                                 "data_provider_id", "currency_id"),
            "default_column": "data_object_id"
        }

        super(ConceptHeader, self).__init__(key="concept_header",
                                            schema=schema)

    def get_id(self, info, create=False):
        """Fetch integer header ids based on query in `info`.

        Parameters
        ----------
        info : pandas.Series or pandas.DataFrame or dict
        create : bool
            True to create a Header if not found

        Returns
        -------
        res : int or pandas.Series

        """
        # to ndframe
        info_ndframe = self._coerce_to_ndframe(info)

        # fetch
        info_int = self._from_ndframe(info_ndframe)

        # add a new entry if asked
        if create:
            self.add_rows(info_int)

        # call to db
        query = TableQuery(info_int)
        header = self.get_unique_id(query=query)

        return header

    @staticmethod
    def _coerce_to_ndframe(what):
        """

        Parameters
        ----------
        what

        Returns
        -------

        """
        if isinstance(what, (pd.Series, pd.DataFrame)):
            return what
        elif isinstance(what, pd.MultiIndex):
            res = pd.DataFrame.from_dict(
                {k: pd.Series(what.get_level_values(k)) for k in what.names},
                orient="index"
            )
        elif isinstance(what, dict):
            res = pd.Series(what)
        else:
            raise ValueError("Invalid type of input to Header().get_id()!")

        return res

    def _from_ndframe(self, info_ndframe):
        """Helper function operating on dictionaries.

        Parameters
        ----------
        info_ndframe : pandas.Series

        Returns
        -------
        info_series_int : int or pandas.Series
            of integers corresponding to ids of the header constituents

        """
        # assert all requred columns are in the index entries
        assert pd.Index(self._required_columns).isin(info_ndframe.index).all()

        # if a series -------------------------------------------------------
        # if string -> fetch by default columns
        if info_ndframe.dtype == 'O':
            # function to apply to (idx, row) pairs in the Series: construct
            #  a query + get unique id from each query
            def apply_fun(tup):
                tbl = Table(key=tup[0], schema=None)
                q = TableQuery(
                    ("{} == %r" % tup[1]).format(tbl._default_column))
                temp_res = tbl.get_unique_id(q)
                return temp_res

            # fetch entries of the series one by one
            info_series_int = pd.Series(
                {k: apply_fun((k, v)) for k, v in info_ndframe.iteritems()}
            )

            # convert to integers
            info_series_int = info_series_int.astype(int)

        else:
            info_series_int = info_ndframe.copy()

        return info_series_int


class TSHeader(Table):
    """
    """
    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("header_id", "data_version_id"),
            "default_column": "header_id"
        }

        super(TSHeader, self).__init__(key="ts_header", schema=schema)

        # # add revision if missing
        # # TODO: add this default_version!
        # if "data_version" not in info_series_int.index:
        #     info_series_int.loc["version"] = DataVersion().default_version


class Timeseries(Table):
    """Time series representation."""

    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("header_id", "obs_date", "obs_value", ),
            "optional_columns": ("date_inserted", ),
            "default_column": "header_id"
        }

        super(Timeseries, self).__init__(key="timeseries", schema=schema)

    def save(self, data):
        """
        """
        pass

    def get_data(self, header, date_from=None, date_to=None):
        """Load data based on header and dates."""
        cond_header = "index == {}".format(list(header.values))
        cond_date = ("obs_date > Timestamp('{}') and " +
                     "obs_date < Timestamp('{}')").format(date_from, date_to)

        query = TableQuery(cond_header).and_(TableQuery(cond_date))

        # call to db
        res = self.filter(query)

        return res


class Column:
    """
    """
    def __init__(self, name, table):
        """
        """
        self.table = table
        self.name = name

    def _operator_generator(self, expr):
        """

        Parameters
        ----------
        expr

        Returns
        -------

        """
        # get ids of rows in self.table where `expr` holds
        idx = self.table.filter(TableQuery(expr)).index

        # wrap ids in a TableQuery instance
        res = self.table.construct_query_by_id(idx)

        return res

    def __lt__(self, other):
        """
        Returns a TableQuery by comparing the values in column of `self` to
        the value in `other`.

        Parameters
        ----------
        other : coercible to str
            value to compare the default column values of `self` to

        Returns
        -------
        res : TableQuery

        """
        # expression
        expr = "{} < {}".format(self.name, str(other))

        # generate TableQuery
        res = self._operator_generator(expr)

        return res

    def __eq__(self, other):
        """
        Returns a TableQuery by comparing the default columns of `self` to
        the value in `other`.

        Parameters
        ----------
        other : coercible to str
            value to compare the default column values of `self` to

        Returns
        -------
        res : TableQuery

        """
        expr = "{} == {}".format(self.name, str(other))

        # generate TableQuery
        res = self._operator_generator(expr)

        return res

    def __gt__(self, other):
        """
        Returns a TableQuery by comparing the default columns of `self` to
        the value in `other`.

        Parameters
        ----------
        other : coercible to str
            value to compare the default column values of `self` to

        Returns
        -------
        res : TableQuery

        """
        expr = "{} > {}".format(self.name, str(other))

        # generate TableQuery
        res = self._operator_generator(expr)

        return res

    def like(self, other):
        """
        Parameters
        ----------
        other : any
            of stuff

        Unlike other operators, this one first fetches the whole table from
        the store, so beware of memory issues!
        """
        # fetch the whole table (assuming it is not too large)
        tbl = hangar.get(self.table.key)

        # get index of rows where `query` is fulfilled
        idx = tbl.loc[tbl[self.name].str.contains(other), :].index

        # wrap it in a TableQuery
        res = self.table.construct_query_by_id(idx)

        return res

    def in_(self, other):
        """

        Parameters
        ----------
        other : iterable

        Unlike other operators, this one first fetches the whole table from
        the store, so beware of memory issues!

        """
        # fetch the whole table (assuming it is not too large)
        tbl = hangar.get(self.table.key)

        # query
        idx = tbl.loc[tbl[self.name].isin(other), :].index

        # wrap in a TableQuery
        res = self.table.construct_query_by_id(idx)

        return res
