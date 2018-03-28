import pandas as pd
import numpy as np
import unittest
import os

from datadough.engine import Table
from datadough.query import TableQuery


class TestTable(unittest.TestCase):
    """
    """
    def setUp(self):
        """
        """
        required_columns = ("col_1", "col_2")
        optional_columns = ("col_opt_1", )
        default_column = "col_2"

        schema = {
            "required_columns": required_columns,
            "optional_columns": optional_columns,
            "default_column": default_column
        }

        test_df = pd.DataFrame(data=np.eye(3),
                               columns=required_columns+optional_columns,
                               index=np.arange(3))

        table = Table("test_table", schema=schema)

        with pd.HDFStore("c:/temp/test_hdf.h5", mode='w') as hangar:
            hangar.put("test_table", test_df, format="table",
                       data_columns=True)

        # with pd.HDFStore("c:/temp/test_hdf.h5", mode='a') as hangar:
        #     hangar.create_table_index(
        #         "test_table", columns=list(required_columns+optional_columns),
        #         kind="full")

        self.req_cols = required_columns
        self.opt_cols = optional_columns
        self.def_col = default_column

        self.test_df = test_df
        self.schema = schema
        self.table = table

    def tearDown(self):
        """
        """
        os.remove("c:/temp/test_hdf.h5")

    def test_init(self):
        """
        """
        self.assertEqual(self.table._required_columns, self.req_cols)

    def test_filter(self):
        """
        """
        qry_1 = TableQuery(condition="index < -3")
        qry_2 = TableQuery(condition="col_1 > 0").or_(
            TableQuery("col_2 > 0"))
        qry_3 = TableQuery(condition=self.opt_cols[0] + " == 1")
        # qry_4 = TableQuery("index == 2").or_(TableQuery("index == 1"))

        self.assertTrue(self.table.filter(qry_1).empty)
        self.assertTrue(self.test_df.columns.equals(
            self.table.filter(qry_2).columns))
        self.assertEqual(
            self.table.filter(qry_3).loc[:, self.opt_cols[0]].iloc[0], 1)
        # self.assertEqual(self.table.filter(qry_4).loc[2, :].iloc[-1], 1)

    def test_add_new_no_dupl_w_optional(self):
        """Test addition of new rows."""
        new_df = pd.DataFrame(np.eye(3) * 2, index=range(3, 6),
                              columns=self.req_cols + self.opt_cols)
        self.table.add_new(new=new_df)
        self.assertEqual(len(self.table.index), 6)


if __name__ == "__main__":
    unittest.main()
