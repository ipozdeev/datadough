import pandas as pd
import numpy as np
from datadough.hangar import DataHangar
from datadough.engine import *
from datadough.query import TableQuery


def create_example_database():
    """
    """
    # data object -----------------------------------------------------------
    data_object = pd.DataFrame(
        {
            "long_name": ["1-month eur ois", "euro"],
            "short_name": ["ois_1m_eur", "eur"]
        },
    )

    # data type -------------------------------------------------------------
    data_type = pd.DataFrame(
        {
            "long_name": ["interest_rate, in percent p.a.",
                          "closing price, mid"],
            "short_name": ["interest_rate", "p_close_mid"],
            "nature": ["float", "float"]
        }
    )

    # data provider ---------------------------------------------------------
    data_provider = pd.DataFrame(
        {
            "long_name": ["datastream"]
        }
    )

    # currency --------------------------------------------------------------
    currency = pd.DataFrame(
        {
            "long_name": ["euro"],
            "iso": ["eur"]
        }
    )

    # data version ----------------------------------------------------------
    data_version = pd.DataFrame(
        {
            "description": ["using splines"],
            "date_created": ["2018-01-01"]
        }
    )

    # data header -----------------------------------------------------------
    data_header = pd.DataFrame(
        columns=["data_object_id", "data_type_id", "data_provider_id",
                 "currency_id", "data_version_id"],
        data=np.array([[0, 0, 0, 0, 0]])
    )

    # timeseries ------------------------------------------------------------
    timeseries = pd.DataFrame(columns=["header_id", "obs_date", "obs_value"],
                              data=[[0, pd.Timestamp("2011-01-01"), 2.9909]],
                              index=[0, ])

    # populate
    with pd.HDFStore("c:/temp/temp_hdf.h5", mode='w') as hangar:
        # tables ------------------------------------------------------------
        # columns=True ensures that columns are indexed and can be used in
        #   select expressions
        hangar.put("data_object", data_object, format='t', data_columns=True)
        hangar.put("data_type", data_type, format='t', data_columns=True)
        hangar.put("data_provider", data_provider, format='t',
                   data_columns=True)
        hangar.put("currency", currency, format='t', data_columns=True)
        hangar.put("data_version", data_version, format='t', data_columns=True)
        hangar.put("data_header", data_header, format='t', data_columns=True)
        hangar.put("timeseries", timeseries, format='t', data_columns=True)

        # attributes --------------------------------------------------------
        # schem and the like
        # TODO: possible to use the class-specific attributes here?
        hangar.root._v_attrs.data_object = {
            "required_columns": ("long_name", "short_name"),
            "default_column": "short_name"
        }
        hangar.root._v_attrs.data_type = {
            "required_columns": ("long_name", "short_name", "nature"),
            "default_column": "short_name"
        }
        hangar.root._v_attrs.data_provider = {
            "required_columns": ("long_name", ),
            "default_column": "long_name"
        }
        hangar.root._v_attrs.currency = {
            "required_columns": ("iso", ),
            "default_column": "iso"
        }
        hangar.root._v_attrs.data_version = {
            "required_columns": ("date_created", ),
            "default_column": "description"
        }
        hangar.root._v_attrs.data_header = {
            "required_columns": ("data_object_id", "data_type_id",
                                 "data_provider_id", "currency_id",
                                 "data_version_id"),
            "default_column": "data_object_id"
        }
        hangar.root._v_attrs.timeseries = {
            "required_columns": ("header_id", "obs_date", "obs_value", ),
            "optional_columns": ("date_inserted", ),
            "default_column": "header_id",
        }


        # for k in hangar.keys():
        #     hangar.create_table_index(k, columns=hangar, kind="full")


def seed_timeseries():
    """
    """
    # data
    data = pd.DataFrame(data=np.random.normal(size=(6, 2)),
                        index=pd.date_range("2000-12-31", periods=6, freq='M'))

    # header
    hdr = pd.Series({"data_object_id": 0, "data_type_id": 0,
                     "data_provider_id": 0, "currency_id": 0,
                     "data_version_id": 0})


    db = DataBase()
    db.save_data(data)

    db.get_data(pd.Series({"lol": 0, "wut": 1}), "2000-01-01", "2017-01-31",
                'M')

    with pd.HDFStore(hangar.path_to_hdf, mode='r') as h:
        print(h.keys())


if __name__ == "__main__":
    # create_example_database()
    seed_timeseries()