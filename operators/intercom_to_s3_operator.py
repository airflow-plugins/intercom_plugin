import logging
import json
import collections
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from intercom_plugin.hooks.intercom_hook import IntercomHook
from tempfile import NamedTemporaryFile


# TODO: Inherit from BaseOperator
class IntercomToS3Operator(BaseOperator):
    """
    Make a query against Intercom and write the resulting data to s3.
    """

    @apply_defaults
    def __init__(
            self,
            intercom_conn_id,
            intercom_obj,
            intercom_method='all',
            s3_conn_id='',
            s3_bucket='',
            s3_key='',
            output='',
            fields=None,
            replication_key_name=None,
            replication_key_value=0,
            *args,
            **kwargs
    ):
        """ 
        Initialize the operator
        :param intercom_conn_id:        name of the Airflow connection that has
                                        your Intercom tokens
        :param intercom_obj:            name of the Intercom object we are
                                        fetching data from
        :param s3_conn_id:              name of the Airflow connection that has
                                        your Amazon S3 conection params
        :param s3_bucket:               name of the destination S3 bucket
        :param s3_key:                  name of the destination file from bucket                            
        :param output:                  name of the temporary file where the results
                                        should be saved
        :param fields:                  *(optional)* list of fields that you want
                                        to get from the object.
                                        If *None*, then this will get all fields
                                        for the object
        :param replication_key_name:     *(optional)* name of the replication key, 
                                        if needed.
        :param replication_key_value:   *(optional)* value of the replication key,
                                        if needed. The operator will import only 
                                        results with the property from replication_key
                                        grater than the value of this param.
        :param intercom_method          *(optional)* method to call from python-intercom 
                                        Default to "all". 
        :param \**kwargs:               Extra params for the intercom query, based on python
                                        intercom module
        """

        super().__init__(*args, **kwargs)

        # TODO: update with get_conn(intercom_conn_id)
        self.intercom_conn_id = intercom_conn_id
        self.intercom_obj = intercom_obj
        self.intercom_method = intercom_method

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.output = output

        self.fields = fields
        self.replication_key_name = replication_key_name
        self.replication_key_value = replication_key_value
        self._kwargs = kwargs

    def filter_fields(self, result):
        """
        Filter the fields from an resulting object.

        This will return a object only with fields given
        as parameter in the constructor.

        All fields are returned when "fields" param is None.
        """
        if not self.fields:
            return result
        obj = {}
        for field in self.fields:
            obj[field] = result[field]
        return obj

    def filter(self, results):
        """
        Filter the results.
        This will filter the results if there's a replication key given as param.
        """
        if not isinstance(results, collections.Iterable):
            return json.loads((json.dumps(results, default=lambda o: o.__dict__)))

        filtered = []
        for result in results:
            result_json = json.loads((json.dumps(result,
                                                 default=lambda o: o.__dict__)))

            if not self.replication_key_name or \
                    int(result_json[self.replication_key_name]) >= int(self.replication_key_value):
                filtered.append(self.filter_fields(result_json))
        logging.info(filtered)

        return filtered

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Intercom model
        and write it to a file.
        """
        logging.info("Prepping to gather data from Intercom")
        hook = IntercomHook(
            conn_id=self.intercom_conn_id,
        )

        # attempt to login to Intercom
        # if this process fails, it will raise an error and die right here
        # we could wrap it
        hook.get_conn()

        logging.info(
            "Making request for"
            " {0} object".format(self.intercom_obj)
        )

        # fetch the results from intercom and filter them

        results = hook.run_query(self.intercom_obj, self.intercom_method)
        filterd_results = self.filter(results)

        # write the results to a temporary file and save that file to s3
        with NamedTemporaryFile("w") as tmp:
            for result in filterd_results:
                tmp.write(json.dumps(result) + '\n')

            tmp.flush()

            dest_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
            dest_s3.load_file(
                filename=tmp.name,
                key=self.output,
                bucket_name=self.s3_bucket,
                replace=True

            )
            dest_s3.connection.close()
            tmp.close()

    logging.info("Query finished!")
