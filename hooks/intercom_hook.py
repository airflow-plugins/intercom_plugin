from intercom.client import Client
from airflow.hooks.base_hook import BaseHook


class IntercomHook(BaseHook):
    def __init__(
            self,
            conn_id,
            *args,
            **kwargs):
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        self.connection = None
        self.extras = None
        self.intercom = None

    def get_conn(self):
        """
        Initialize a python-intercom instance.
        """
        if self.intercom:
            return self.intercom

        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson
        intercom = Client(
            personal_access_token=self.extras['personal_access_token'])
        self.intercom = intercom

        return intercom

    def run_query(self, model, method='all', **kwargs):
        """
        Run a query against intercom.
        :param model:           name of the Intercom model
        :param method:          name of the python-intercom method
                                to call
        :param \**kwargs:       extra args required by the intercom method
        """
        intercom = self.get_conn()
        intercom_model = getattr(intercom, model)
        result = getattr(intercom_model, method)(**kwargs)

        return result

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
