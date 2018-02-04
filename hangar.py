import pandas as pd


class DataHangar(object):
    """Implement context managers for a bunch of pandas.HDFStore methods.

    Parameters
    ----------
    path_to_hdf : str
        path to the hdf storage

    """
    def __init__(self, path_to_hdf):
        """
        """
        self.path_to_hdf = path_to_hdf

    def select_column(self, *args, **kwargs):
        """Wrapper for pandas.HDFStore.get.select_column.

        Parameters
        ----------
        args
        kwargs

        Returns
        -------
        res

        """
        with pd.HDFStore(self.path_to_hdf, mode='r') as h:
            res = h.select_column(*args, **kwargs)

        return res

    def get(self, *args, **kwargs):
        """Wrapper for pandas.HDFStore.get.
        """
        with pd.HDFStore(self.path_to_hdf, mode='r') as h:
            return h.get(*args, **kwargs)

    def select(self, *args, **kwargs):
        """Wrapper for pandas.HDFStore.select.
        """
        with pd.HDFStore(self.path_to_hdf, mode='r') as h:
            return h.select(*args, **kwargs)

    def append(self, *args, **kwargs):
        """Wrapper for pandas.HDFStore.append.
        """
        with pd.HDFStore(self.path_to_hdf, mode='a') as h:
            return h.append(*args, **kwargs)
