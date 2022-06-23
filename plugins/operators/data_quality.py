from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks_list = checks_list

    def execute(self, context):
        self.log.info(f'Running quality checks...')
        redshift = PostgresHook(self.redshift_conn_id)
        
        for check in self.checks_list:
            sql = check.get('quality_check')
            exp_result = check.get('expected_result')

            records = redshift.get_records(sql)[0]

            if exp_result != records[0]:
                self.log.info(f'Data quality check failed => {sql}')
                self.log.info(f'Actual: {records[0]}, Expected result: {exp_result}')
                raise ValueError(f'Data quality check failed => {sql}')
                