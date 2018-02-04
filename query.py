import pandas as pd


class TableQuery(object):
    """docstring for Query."""
    def __init__(self, condition=None):
        """
        """
        self.expression = self.parse_expression(condition)

    def __str__(self):
        return self.expression

    @staticmethod
    def parse_expression(condition):
        """
        """
        if condition is None:
            # default value of arg `where` in pandas.HDFStore.select(),
            #   to be able to return all rows from a table
            return None

        elif isinstance(condition, str):
            res = condition

        # elif isinstance(condition, dict):
        #     res = ' and '.join([(' ' + str(k) + ' ').join(v)
        #                         for k, v in condition.items()])

        elif isinstance(condition, (dict, pd.Series)):
            res = ' and '.join(['(' + str(k) + ' == ' + str(v) + ')'
                               for k, v in condition.items()])

        elif isinstance(condition, (list, tuple)) and (len(condition) < 1):
            res = "index < 0"

        else:
            raise ValueError("Not implemented!")

        return res

    def or_(self, other):
        """
        """
        new_expression = '(' + self.expression + ')' + " or " + \
            '(' + other.expression + ')'

        return TableQuery(condition=new_expression)

    def and_(self, other):
        """
        """
        new_expression = '(' + self.expression + ')' + " and " + \
            '(' + other.expression + ')'

        return TableQuery(condition=new_expression)

    # @classmethod
    # def by_id(cls, idx):
    #     """
    #
    #     Parameters
    #     ----------
    #     idx
    #
    #     Returns
    #     -------
    #
    #     """
    #     if len(idx) < 3:
    #         expr = self.key + "_id" + "in {}".format(tuple(idx))

if __name__ == "__main__":
    qry_1 = TableQuery("data_object == 'ois_1m_eur'").and_(
        TableQuery("data_type == 'interest_rate'"))
    print(qry_1.expression)

    qry_2 = TableQuery({">": ("A", "B"), "<": ("C", "D")}).expression

    qry_3 = TableQuery(
        pd.Series({">": ("A", "B"), "<": ("C", "D")})).expression
