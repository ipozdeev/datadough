import pandas as pd
from datadough.hangar import DataHangar
from datadough.engine import *
from datadough.query import TableQuery


def create_example_database(path_to_dump):
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
            "long_name": ["using splines"],
            "date_created": ["2018-01-01"]
        }
    )

    # data header -----------------------------------------------------------
    data_header = pd.DataFrame(
        columns=["data_object_id", "data_type_id", "data_provider_id",
                 "currency_id", "data_version_id"]
    )

    # populate
    with pd.HDFStore("c:/temp/temp_hdf.h5", mode='w') as hangar:
        hangar.put("data_object", data_object, format='t', data_columns=True)
        hangar.put("data_type", data_type, format='t', data_columns=True)
        hangar.put("data_provider", data_provider, format='t',
                   data_columns=True)
        hangar.put("currency", currency, format='t', data_columns=True)
        hangar.put("data_version", data_version, format='t', data_columns=True)
        hangar.put("data_header", data_header, format='t', data_columns=True)

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
            "required_columns": ("long_name", "date_created"),
            "default_column": "long_name"
        }
        hangar.root._v_attrs.data_header = {
            "required_columns": ("data_object_id", "data_type_id",
                                 "data_provider_id", "currency_id",
                                 "data_version_id"),
            "default_column": "data_object_id"
        }

        # for k in hangar.keys():
        #     hangar.create_table_index(k, columns=hangar, kind="full")


if __name__ == "__main__":
    # # create database
    create_example_database("c:/temp/temp_hdf.h5")

    # query
    # hangar = DataHangar("c:/temp/temp_hdf.h5")
    # Currency().get_unique_id(TableQuery("iso == eur"))
    #
    # TableQuery({"and": {"<": ("lol", "wut"), "==": {"A", "k"}}}).expression
    #
    # Currency()._default_column

    # (Currency() == "eur").expression

    info = pd.Series(index=Header()._required_columns,
                     data=np.zeros(5).astype(int))
    Header().get_id(info, create=True)