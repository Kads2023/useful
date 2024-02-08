import MySQLdb
import os


class MySQLDatabase(object):
    host = os.environ['METADATA_MYSQL_HOST']
    user = os.environ['METADATA_MYSQL_USER']
    pwd = os.environ['METADATA_MYSQL_PASS']
    port = int(os.environ['METADATA_MYSQL_PORT'])
    db = os.environ['METADATA_MYSQL_DB']
    #ssl = {'ssl': {'ca': os.environ['AIRFLOW_MYSQL_SSL_CERT']}}

    def __init__(self):
        self.connection = MySQLdb.connect(host=self.host, user=self.user, passwd=self.pwd, port=self.port, db=self.db )
        self.cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)

    def query(self, query):
        try:
            print(query)
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except (MySQLdb.Error, MySQLdb.Warning, TypeError):
            raise
        finally:
            self.cursor.close()
            self.connection.close()


def get_application_job_with_dependency_for_dag(database, dag_id, conn):
    """Get all the jobs from the DATABASE. This generates the main DAGS."""
    print(dag_id)
    sql = ( "select a1.job_key as job_key, a1.job_name as job_name, a1.application_system_key as application_system_key,a1.job_frequency_code as job_frequency_code,ad.data_entity_key as data_entity_key,ad.source_application_server_key as source_application_server_key,"
           + "ad.target_application_server_key as target_application_server_key ,ajd.related_job_key as related_job_key, a1.is_active, ajd.is_active, a1.scheduler_job_name, a1.job_attributes job_attributes, ass.server_attributes as source_server_attributes, ast.server_attributes as target_server_attributes, aps.system_attributes as system_attributes, lm.logic_module_type_code as job_type, logic_body as logic_attributes from application_job a1 "
           + "left join app_job_dependency ajd on a1.job_key=ajd.job_key left join app_job_details ad on a1.job_key=ad.job_key join application_server ass on ad.source_application_server_key=ass.application_server_key join application_server ast on ad.target_application_server_key=ast.application_server_key join logic_module lm on a1.logic_module_key=lm.logic_module_key join application_system as aps on a1.application_system_key=aps.application_system_key  where a1.scheduler_chain_name = '{}' and a1.is_active='Y' "
           + "and (ajd.is_active='Y' or ajd.is_active is NULL);")
    sql = sql.format(dag_id)
    print(sql)
    job_data = conn.query(sql)
    return job_data


def is_application_system_on_hold(system_ids, conn):
    """Find application_system is on hold or not"""
    sql = "select 1 as is_hold from application_system where application_system_key in ({}) and is_active='N'".format(system_ids)
    print(sql)
    is_hold = conn.query(sql)
    print(is_hold)
    return is_hold


def is_application_server_on_hold(system_ids, conn):
    """Find application server is on hold or not """
    sql = "select 1 as is_hold from application_server where application_server_key in ({}) and is_active='N'".format(system_ids)
    print(sql)
    is_hold = conn.query(sql)
    print(is_hold)
    return is_hold


def is_application_job_on_hold(job_key):
    """Find application job is on hold or not """
    sql = "select 1 as is_hold from application_job where job_key={} and is_hold='Y'".format(job_key)
    print(sql)
    conn = MySQLDatabase()
    is_hold = conn.query(sql)
    print(is_hold)
    return is_hold


def is_skip_application_job(job_key):
    """Find application job is skip or not """
    sql = "select 1 as is_skip from application_job where job_key={} and is_skip='Y'".format(job_key)
    print(sql)
    conn = MySQLDatabase()
    is_skip = conn.query(sql)
    print(is_skip)
    return is_skip


def check_job_run_time_execution_status(job_key, job_run_time_ms):
    """Find application job is skip or not """
    sql = "select aj.job_status_code, aj.job_runtime_ms, aj.job_run_sequence from app_job_run_stats aj where aj.job_key={0} and aj.job_runtime_ms={1} and aj.job_status_code='COMMITTED'".format(job_key, job_run_time_ms)
    conn = MySQLDatabase()
    job_run_stats = conn.query(sql)
    print(job_run_stats)
    return job_run_stats