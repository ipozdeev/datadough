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
        self._data_object = DataObject(hangar)
        self._data_type = DataType(hangar)
        self._data_provider = DataProvider(hangar)
        self._currency = Currency(hangar)
        self._data_version = DataVersion(hangar)

        self._timeseries = Timeseries(hangar)

    def get_data(self, header, date_from, date_to, freq):
        """
        """
        pass

    def save_data(self, df_to_save):
        """
        """
        pass


class Table(object):
    """docstring for Table.

    Parameters
    ----------
    key : str
        key to locate the table in the DataHangar
    schema : str or dict
        if 'infer', the value is taken from HDFStore attributes from `hangar`
    """
    def __init__(self, key, schema="infer"):
        """
        """
        self.key = key

        if schema == "infer":
            with pd.HDFStore("c:/temp/temp_hdf.h5", mode='r') as hangar:
                for k, v in hangar.root._v_attrs[key].items():
                    setattr(self, ('_' + k), v)
        else:
            self._required_columns = schema.get("required_columns", tuple())
            # TODO: watch out for empty list of keys!
            self._optional_columns = schema.get("optional_columns", tuple())
            self._default_column = schema.get("default_column", tuple())

    def query_id(self, query):
        """Retrieve index values of rows where `condition` holds.

        Convenient to use in subsequent queries

        Parameters
        ----------
        query : TableQuery

        Returns
        -------
        qry : TableQuery
            of kind 'currency_id in (10, 20, 22)', ready to be parsed to
            pandas.HDFStore.select().
        """
        qry_0 = TableQuery(query)

        # TODO: implement this select!
        idx = self.get_all(query=qry_0).index

        # if nothing was returned, output empty list
        if len(idx) < 1:
            expression = []

        elif len(idx) == 1:
            expression = idx

        elif (np.diff(np.array(sorted(idx))) == 1).all():
            # check if possible to run a shortcut with between
            expression = (
                self.key + "_id" + " > {} and " +
                self.key + "_id" + " < {}").format(min(idx), max(idx))

        else:
            expression = self.key + "_id" + "in {}".format(tuple(idx))

        return TableQuery(condition=expression)

    def get_all(self, query):
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
        res = self.get_all(query)

        if len(res) > 1:
            raise ValueError("More than one rows are found with these " +
                             "criteria; narrow down your search!")
        elif len(res) < 1:
            return []
        else:
            return res.index[0]

    def add_new(self, new):
        """
        Parameters
        ----------
        new : pandas.Series or pandas.DataFrame
            with new data, columned with required columns of respective class
        """
        # to a Series, as columns are required
        if isinstance(new, pd.Series):
            new = new.to_frame().T

        # check if some columns are missing
        new_reix = new.reindex(columns=self._required_columns)

        if new_reix.isnull().any().any():
            missing = new_reix.columns[new_reix.isnull().any(axis=0)]
            msg = ("The following required columns are missing: " +
                   "'{}', " * (len(missing)-1) + "'{}'.").format(*missing)
            raise ValueError(msg)

        # check if such rows exist already
        rows_exist = self.check_exist(new_reix)

        # add truly new rows
        # need to reindex first: watch out that the index in the HDFStore is
        #   unique!
        def lambda_get_max_index(h):
            res = h[self.key].index.max()
            return res

        max_idx = hangar.do(lambda_get_max_index)

        # construct new index with unique values
        n_rows_to_save = (~rows_exist).sum()
        unq_idx = range(max_idx+1, max_idx+1+n_rows_to_save)

        # combine to a DataFrame
        df_to_save = new.loc[~rows_exist, :]
        df_to_save.index = unq_idx

        def lambda_add_row(h):
            return h.append(self.key, df_to_save)

        hangar.do(lambda_add_row)

    def check_exist(self, rows):
        """
        """
        def apply_fun(row):
            res = len(self.get_all(TableQuery(row)))
            return res

        flag = rows.apply(apply_fun, axis=1)

        res = flag > 0

        return res

    def __lt__(self, other):
        """
        """
        return self.query_id(query=TableQuery(
            "{} < {}".format(self._default_column, str(other))))

    def __eq__(self, other):
        """
        """
        return self.query_id(query=TableQuery(
            "{} == {}".format(self._default_column, str(other))))

    def __gt__(self, other):
        """
        """
        return self.query_id(query=TableQuery(
            "{} > {}".format(self._default_column, str(other))))


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
            "required_columns": ("long_name", "date_created"),
            "default_column": "long_name"
        }

        super(DataVersion, self).__init__(key="data_version", schema=schema)


class Header(Table):
    """docstring for Header."""
    def __init__(self):
        """
        """
        schema = {
            "required_columns": ("data_object_id", "data_type_id",
                                 "data_provider_id", "currency_id",
                                 "data_version_id"),
            "default_column": "data_object_id"
        }

        super(Header, self).__init__(key="data_version", schema=schema)

    def get_id(self, info, create=False):
        """

        Parameters
        ----------
        info : pandas.Series or pandas.DataFrame or dict

        Returns
        -------

        """
        if isinstance(info, pd.Series):
            info_int = self._get_int_representation(info)

        elif isinstance(info, pd.DataFrame):
            info_int = pd.DataFrame.from_dict(
                {
                    k: self._get_int_representation(v)
                    for k, v in info.iteritems()
                }
            )

        elif isinstance(info, dict):
            info_int = self._get_int_representation(pd.Series(info))

        else:
            raise ValueError("Invalid type of info!")

        # add a new entry if asked
        if create:
            self.add_new(info_int)

        # call to db
        query = TableQuery(info_int)
        header = self.get_unique_id(query=query)

        return header

    def _get_int_representation(self, info_series):
        """Helper function operating on dictionaries.

        Parameters
        ----------
        info_series : pandas.Series

        Returns
        -------
        header : any
            integer header id or an empty list
        info_series_int : pandas.Series
            of integers corresponding to ids of the header constituents

        """
        assert pd.Index(self._required_columns).isin(info_series.index).all()

        # query_dict = dict()

        if info_series.dtype == 'O':
            # function to apply to (idx, row) pairs in the Series
            def apply_fun(tup):
                tbl = Table(key=tup[0], schema="infer")
                q = TableQuery(
                    ("{} == %r" % tup[1]).format(tbl._default_column))
                temp_res = tbl.get_unique_id(q)
                return temp_res

            info_series_int = pd.Series({k: apply_fun((k, v))
                                         for k, v in info_series.iteritems()})
            info_series_int = info_series_int.astype(int)

        else:
            info_series_int = info_series.copy()

        return info_series_int

class Timeseries(Table):
    """
    """
    schema = {
        "required_columns": ("header_id", "obs_date", "obs_value", ),
        "optional_columns": ("date_inserted", ),
        "default_column": "header_id"
    }

    def __init__(self, hangar):
        """
        """
        super(Timeseries, self).__init__(key="timeseries")
