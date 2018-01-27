import pandas as pd


class DataHangar(object):
    """docstring for DataHangar.

    Parameters
    ----------
    path_to_hdf : str
        path to the hdf storage

    """
    def __init__(self, path_to_hdf):
        """
        """
        self.path_to_hdf = path_to_hdf

    # @property
    # def hangar_read(self):
    #     with pd.HDFStore(self.path_to_hdf, mode='r') as h:
    #         return h

    def read_do(self, action):
        """Perform an action on a read-only open HDFStore.

        TODO: change to a decorator!

        Parameters
        ----------
        action : callable
            operating on an open pandas.HDFStore instance

        Example
        -------
        hangar = DataHangar(...)
        one_row = hangar.read_do(lambda x: x.select('df', 'index == 5'))

        Returns
        -------
        res : any

        """
        with pd.HDFStore(self.path_to_hdf, mode='r') as h:
            res = action(h)

        return res

    def select(self, **kwargs):
        """
        """
        with pd.HDFStore(self.path_to_hdf, mode='r') as h:
            return h.select(**kwargs)

    def append(self, **kwargs):
        """
        """
        with pd.HDFStore(self.path_to_hdf, mode='a') as h:
            return h.append(**kwargs)
