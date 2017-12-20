from airflow.plugins_manager import AirflowPlugin
from intercom_plugin.operators.intercom_to_s3_operator import IntercomToS3Operator
from intercom_plugin.hooks.intercom_hook import IntercomHook


class IntercomToS3Plugin(AirflowPlugin):
    name = "intercom_plugin"
    hooks = [IntercomHook]
    operators = [IntercomToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
