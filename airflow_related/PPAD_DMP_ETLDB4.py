from datetime import datetime, timedelta
from airflow_utils.utils.dag_master_functions import create_dag

MAIN_DB = ""
FREQUENCY = 30
DAG_ID = ''
DEFAULT_ARGS = {
    'owner': '',
   # 'start_date': s_date,
    # 'pool':''
    'start_date': datetime(2023, 11, 15, 10, 0, 0),
    'email': [''],
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue':'',
    'pool': ''
}
SCHEDULE = "*/30 * * * *"


dag = create_dag(FREQUENCY, MAIN_DB, DAG_ID, DEFAULT_ARGS, SCHEDULE)

dag
