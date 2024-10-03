from airflow.decorators import dag, task 
from datetime import datetime 
# local to gcp imports 
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# big query dataset creator 
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
# astro sdk to load data to the local from external
from astro import sql as aql 
from astro.files import File
from astro.sql.table import Table, Metadata 
from astro.constants import FileType
# imports for dbt to be able to be in the airflow 
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
# finally to have a streamline/precendence on the task execution, import chain 
from airflow.models.baseoperator import chain


# the task with decorator tag 
@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None, 
    catchup=False, 
    tags=['retail']
)
def retail():

    # first task - Ingest local csv to bucket. 
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_csv_to_gcs',
        src = '/usr/local/airflow/include/dataset/online_retail.csv',
        dst = 'raw/online_retail.csv',
        bucket = 'cedie_online_retail', 
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv',
    )
    
    # second task - create schema for big query 
    create_detail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = 'create_detail_dataset',
        dataset_id = 'retail',
        gcp_conn_id = 'gcp',
    )
    
    ## this task is to load the data to the local from the external location. 
    ## in this case this will be loaded to big query. 
    gcs_to_raw = aql.load_file(
        task_id = 'gcs_to_raw',
        input_file = File(
            'gs://cedie_online_retail/raw/online_retail.csv',
            conn_id='gcp',
            filetype=FileType.CSV, 
        ),
        output_table = Table(
            name='raw_invoices',
            conn_id='gcp',
            metadata=Metadata(
                schema='retail'
            )
        ),
        # when set to true, the process will utilize the source features for efficiency and overall improved performance. 
        # when set to false, the process will run down everything, leaving it to have more overhead. 
        use_native_support = False
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        # import the function quality checker 
        from include.soda.checks.check_function import check 
        # rcall the function and then return its result. 
        return check(scan_name, checks_subpath)
    
    
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']    
        ),
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.checks.check_function import check

        return check(scan_name, checks_subpath)
    
    
    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.checks.check_function import check

        return check(scan_name, checks_subpath)
    
    
    # chain all the tasks in this dag so it will streamline the tasks 
    chain(
        upload_csv_to_gcs, # 1
        create_detail_dataset, # 2
        gcs_to_raw, # 3
        # check_load is a function call since it is a external python task. 
        check_load(), # 4
        # transform is an inclusion to chain since this will create the dbt group task.
        transform, # 5.0
        # check_transform is a function call since it is a external python task. 
        check_transform(), # 5.1
        # report is an inclusion to chain since this will create the dbt group task.
        report, # 6.0
        # check_report is a function call since it is a external python task. 
        check_report(), # 6.1
    )
    
    
# instansiate the task 
retail()