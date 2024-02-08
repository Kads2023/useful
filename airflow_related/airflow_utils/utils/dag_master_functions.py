import json
import functools
import socket
import ssl
import sys
import urllib
import pendulum
import time
from datetime import datetime, timedelta
import logging
import json
import requests
from airflow import DAG
import subprocess

from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow_utils.utils.metadata_functions import (MySQLDatabase, is_application_system_on_hold,
                                                  is_application_server_on_hold,
                                                  get_application_job_with_dependency_for_dag,
                                                  is_skip_application_job, is_application_job_on_hold,
                                                  check_job_run_time_execution_status)

from airflow_utils.utils.service_utils import ServiceUtils, read_json, merge_configs
from airflow_utils.utils.constants import JobConfigs, VariableKeyConstants

local_tz = pendulum.timezone("America/Los_Angeles")
iris_url = "http://iris-proxy-vip-a.paypalinc.com/api/v1/event/"
DATE_TIME_FORMAT = '%Y%m%d%H%M%S'
DATE_FORMAT = '%Y%m%d'


def seq_job(frequency, **kwargs):
    """Generates Sequence for DAG RUN """
    exec_date = local_tz.convert(kwargs['data_interval_start']) + timedelta(minutes=frequency)
    day_start = datetime.combine(exec_date, datetime.min.time())
    day_end = datetime.combine(exec_date, datetime.max.time()) + timedelta(minutes=1)
    for count, dt_stamp in enumerate(datetime_range(day_start, day_end, {'minutes': frequency})):
        if exec_date < dt_stamp.replace(tzinfo=local_tz):
            kwargs['ti'].xcom_push(key='sequence', value=count)
            kwargs['ti'].xcom_push(key='run_date', value=exec_date.strftime(DATE_FORMAT))
            kwargs['ti'].xcom_push(key='run_datetime', value=exec_date.strftime(DATE_TIME_FORMAT))
            if count and exec_date.strftime(DATE_FORMAT) and exec_date.strftime(DATE_TIME_FORMAT):
                return count, exec_date.strftime(DATE_FORMAT), exec_date.strftime(DATE_TIME_FORMAT)
            else:
                sys.exit(1)


def wait_job(frequency, job_frequency, buffer_wait_time, main_db, **kwargs):
    """ Waits till the sequence end time"""

    run_date = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='run_date')
    run_datetime = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='run_datetime')
    seq_num = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='sequence')

    if run_date and run_datetime and seq_num:
        if job_frequency < 30:
            logging.info("Skipping wait task as job frequencies are lesser than 30 min")
            return
        wait_for_job_end_time(run_date, run_datetime, seq_num, job_frequency, buffer_wait_time)


def wait_for_job_end_time(run_date, run_date_time, seq_num, frequency, wait_buffer):
    conv_run_datetime = datetime.strptime(str(run_date_time), DATE_TIME_FORMAT)
    delta_time = timedelta(minutes=frequency) + timedelta(minutes=wait_buffer)
    job_exec_time = conv_run_datetime + delta_time
    logging.info(f"conv_run_datetime             is           {conv_run_datetime} ")
    logging.info(f"job_exec_time      is                             {job_exec_time}")
    logging.info(timedelta(minutes=frequency))
    logging.info(f"Date time now: {datetime.now()}")
    job_run_id = job_exec_time.strftime(DATE_TIME_FORMAT)
    logging.info(f"job_run_id-----:{job_run_id}")
    end_time = datetime.strptime(str(job_run_id), DATE_TIME_FORMAT)
    curr_time = datetime.strptime(str(datetime.now(tz=local_tz).strftime(DATE_TIME_FORMAT)), DATE_TIME_FORMAT)
    logging.info(f"Sequence End Time is {end_time}")
    logging.info(f"Current system time is {curr_time}")
    if end_time > curr_time:
        wait_time = end_time - curr_time
        wait_secs = wait_time.seconds
        logging.info(f"sequence end time great than curr time..so waiting for {wait_secs}")
        time.sleep(wait_secs)
    else:
        logging.info("curr time greater than end time..So continuing the job")
        time.sleep(0)


def datetime_range(start, end, delta):
    """Generates the date range with time separation"""
    if not isinstance(delta, timedelta):
        delta = timedelta(**delta)
    while start < end:
        yield start
        start += delta


def hold_job(job_data, main_db, **kwargs):
    """ Puts dag run on hold in case application system or server is on hold"""
    application_system_keys = set()
    application_server_keys = set()
    hold_loop = 1
    run_date = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='run_date')
    run_datetime = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='run_datetime')
    seq_num = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='sequence')
    print(run_date, run_datetime, seq_num)
    for job in job_data:
        if job["application_system_key"]:
            application_system_keys.add(str(job["application_system_key"]))
            application_server_keys.add(str(job["source_application_server_key"]))
            application_server_keys.add(str(job["target_application_server_key"]))

    print(application_system_keys)
    while hold_loop <= 30:

        if len(application_system_keys) > 0:
            MYSQL_CONN = MySQLDatabase()
            is_application_system_hold = is_application_system_on_hold(','.join(application_system_keys), MYSQL_CONN)
            MYSQL_CONN = MySQLDatabase()
            is_application_server_hold = is_application_server_on_hold(','.join(application_server_keys), MYSQL_CONN)
            if (is_application_server_hold and len(is_application_server_hold) > 0) or (
                    is_application_system_hold and len(is_application_system_hold) > 0):
                if hold_loop < 30:
                    print("system on hold")
                    time.sleep(120)
                    hold_loop = hold_loop + 1
                    print("HOLD_LOOP:", hold_loop)
                else:
                    print("Waited for 60 min..still hold in place..Please restart once hold file is removed")
                    sys.exit(1)
                    break
            else:
                print("system not on hold")
                break


def alert_to_iris(main_db, context):
    groupid = 'Data_Movement'
    origin = 'Data_Movement_Airflow_Failure_Alert'
    suborigin = 'airflow_utils_Task_Failure_Alert'
    # component = contextDict['dag'].dag_id
    component = context['task'].task_id
    run_datetime = context['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='run_datetime')
    seq_num = context['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='sequence')
    exception_msg = context.get('exception')
    if exception_msg:
        exception_msg = str(exception_msg)[-450:]
    error_msg = " There is an error in the DAG = {}  from TASK = {} for execution date = {}, execution_run_date_time = {}, execution_run_sequence= {}, exception:{} ".format(
        context['dag'].dag_id, context['task'].task_id, context.get('ds'), run_datetime, seq_num, exception_msg)

    logging.info(f"Alert Info :  {socket.gethostbyname(socket.getfqdn())}, {socket.getfqdn()}")
    try:
        iris_alert_data = {
            "groupid": groupid,
            "origin": origin,
            "suborigin": suborigin,
            "hostname": socket.getfqdn(),
            "message": error_msg,
            "severity": "MAJOR",
            "component": component,
            "senderip": socket.gethostbyname(socket.getfqdn()),
            "sendername": socket.getfqdn()
        }
        cert_reqs = ssl.CERT_NONE
        encoded_body = json.dumps(iris_alert_data)
        # http = urllib.PoolManager(cert_reqs=cert_reqs)
        iris_call_response = requests.post(url=iris_url,
                                           headers={'Content-Type': 'application/json'},
                                           data=encoded_body)

        if iris_call_response.status != 200:
            sys.stderr.write(
                "EXCEPTION: got non 200 response from IRIS while creating alert for error_msg: {0}".format(error_msg))
        else:
            logging.info(f"Iris alert generated{iris_call_response}")
    except Exception as e:
        sys.stderr.write("EXCEPTION: error creating IRIS alert for error_msg: {0}, Error: {1}".format(error_msg, e))


def raise_email_alert(main_db, context):
    component = context['task'].task_id
    dag_id = context['dag'].dag_id
    run_datetime = context['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='run_datetime')
    seq_num = context['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='sequence')

    error_msg = " There is an error in the DAG = {}  from TASK = {} for execution date = {}, execution_run_date_time = {}, execution_run_sequence= {}".format(
        context['dag'].dag_id, context['task'].task_id, context.get('ds'), run_datetime, seq_num)
    logging.info(local_tz.convert(context['data_interval_start']))
    logging.info("There is failure")
    try:
        ServiceUtils.send_email(error_msg, component, dag_id)
    except Exception as err:
        logging.error("Error while generating email{}".format(str(err)))


def raise_alert(main_db, context):
    require_iris_alert = True
    raise_email_alert(main_db, context)
    if require_iris_alert:
        alert_to_iris(main_db, context)


def main(main_db, job_details, dag_frequency, wait_buffer, **kwargs):
    """Main method to for job tasks"""
    print(job_details)
    job_properties = {}
    run_date = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='run_date')
    run_datetime = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='run_datetime')
    seq_num = kwargs['ti'].xcom_pull(task_ids='SEQ_SENSOR_' + main_db, key='sequence')
    job_frequency = int(job_details["job_frequency_code"])
    check_for_hold_execute_job(job_details)
    check_for_skip_job(job_details)
    if job_frequency < 30:
        if dag_frequency % job_frequency != 0:
            raise Exception(
                f"Job cannot be triggered at frequency of {job_frequency} with dag frequency {dag_frequency}")
        else:
            conv_run_datetime = datetime.strptime(str(run_datetime), DATE_TIME_FORMAT)
            conv_run_end_datetime = conv_run_datetime + timedelta(minutes=dag_frequency)
            exec_date = datetime.strptime(run_date, DATE_FORMAT)
            day_start = datetime.combine(exec_date, datetime.min.time())
            catch_up_required = check_for_catch_up(conv_run_datetime, job_details, job_frequency)
            if catch_up_required:
                execute_job(run_date, run_datetime, seq_num, job_details, job_properties, dag_frequency)
                return
            sequence_till_now = total_sequences_in_range(day_start, conv_run_datetime, timedelta(minutes=job_frequency))
            for count, dt_stamp in enumerate(
                    datetime_range(conv_run_datetime, conv_run_end_datetime, {'minutes': job_frequency})):
                current_seq = int(sequence_till_now + count + 1)
                current_run_date_time = dt_stamp.strftime(DATE_TIME_FORMAT)
                logging.info(f"Going for Run data time: {current_run_date_time}, seq num: {current_seq}")
                if not job_details["related_job_key"]:
                    wait_for_job_end_time(run_date, current_run_date_time, current_seq, job_frequency, wait_buffer)
                execute_job(run_date, current_run_date_time, current_seq, job_details, job_properties, job_frequency)
    else:
        execute_job(run_date, run_datetime, seq_num, job_details, job_properties, job_frequency)

    return


def execute_job(run_date, run_date_time, seq_num, job_details, job_properties, job_frequency):
    """ Executes job """
    system_attributes = json.loads(read_json(job_details, ["system_attributes"], "{}"))
    source_server_attributes = json.loads(read_json(job_details, ["source_server_attributes"], "{}"))
    target_server_attributes = json.loads(read_json(job_details, ["target_server_attributes"], "{}"))
    job_attributes = json.loads(read_json(job_details, ["job_attributes"], "{}"))
    logic_attributes = json.loads(read_json(job_details, ["logic_attributes"], "{}"))
    runtime_in_ms_enabled = read_json(logic_attributes, ["runtime_in_ms_enabled"], "true")
    run_date_time_epoch = int(datetime.strptime(run_date_time, DATE_TIME_FORMAT).replace(tzinfo=local_tz).timestamp())
    if runtime_in_ms_enabled.lower() == "true":
        run_date_time_epoch = int(
            (datetime.strptime(run_date_time, DATE_TIME_FORMAT) + timedelta(minutes=job_frequency)).replace(
                tzinfo=local_tz).timestamp()) * 1000

    job_run_attributes = {
        "jobRuntimeMs": run_date_time_epoch,
        "jobRunSequence": seq_num,
        "submittedBy": JobConfigs.JOB_SUBMITTED_BY_VALUE,
        "jobKey": job_details["job_key"],
        "jobName": job_details["job_name"],
        "jobFrequencyCode": job_frequency,
        "app": {"systemKey": job_details["application_system_key"]},
        "target": {"dataEntityKey": job_details["data_entity_key"]}
    }
    if job_details["related_job_key"] and runtime_in_ms_enabled.lower() == "true":
        job_run_attributes["parentJobKey"] = job_details["related_job_key"]
    logging.info(f"Source Server Properties {source_server_attributes}")
    print(f"Target Server properties {target_server_attributes}")
    print(f"Job Properties {job_attributes}")
    print(f"System attributes: {system_attributes}")
    if system_attributes and len(system_attributes) != 0:
        job_properties = merge_configs(job_properties, system_attributes)
    if source_server_attributes and len(source_server_attributes) != 0:
        job_properties = merge_configs(job_properties, source_server_attributes)
    if target_server_attributes and len(target_server_attributes) != 0:
        job_properties = merge_configs(job_properties, target_server_attributes)
    if logic_attributes and len(logic_attributes) != 0:
        job_properties = merge_configs(job_properties, logic_attributes)
    if job_attributes and len(job_attributes) != 0:
        job_properties = merge_configs(job_properties, job_attributes)
    job_properties = merge_configs(job_properties, job_run_attributes)

    logging.info(f"Merged Job Properties:{job_properties}")
    if job_details['job_type'] and job_details['job_type'].lower() in ["spark", "script"]:
        spark_job_command = job_properties["trigger_command"]
        job_properties["trigger_command"] = ""
        spark_job_command = spark_job_command + " '" + json.dumps(job_properties) + "'"

        if VariableKeyConstants.LOG_FILE_NAME in spark_job_command:
            spark_job_command = spark_job_command.replace(VariableKeyConstants.LOG_FILE_NAME,
                                                          generate_log_file(job_details, run_date_time, seq_num))
        logging.info(f"Command executed :{spark_job_command}")
        output, error, exitcode = run_command(spark_job_command)
        if exitcode != 0:
            logging.error(error)
            raise Exception(error)

    else:
        logging.error("Empty job type or implementation not available")


def generate_log_file(job_details, run_date_time, seq):
    current_timestamp = datetime.now(local_tz).strftime(DATE_TIME_FORMAT)
    log_file = VariableKeyConstants.LOG_FILE_NAME_FORMAT.format(job_details["job_name"], str(run_date_time), str(seq),
                                                                str(current_timestamp))
    return log_file


def check_for_skip_job(job_details):
    skip_application_job = is_skip_application_job(job_details['job_key'])
    if skip_application_job and len(skip_application_job) > 0:
        logging.info("Skip Flag is true for job")
        raise AirflowSkipException("Skipping job as skip flag is enabled for job")
    else:
        logging.info("Not skipping job")


def check_for_catch_up(run_date_time, job_details, job_frequency):
    delta_time = timedelta(hours=2)
    catch_up_time = run_date_time + delta_time
    catch_up_run_id = catch_up_time.strftime(DATE_TIME_FORMAT)
    catch_up_end_time = datetime.strptime(str(catch_up_run_id), DATE_TIME_FORMAT)
    curr_time = datetime.strptime(str(datetime.now(tz=local_tz).strftime(DATE_TIME_FORMAT)), DATE_TIME_FORMAT)
    logging.info(f"Catch Up End Time: {catch_up_run_id}")
    if curr_time > catch_up_end_time:
        job_attributes = json.loads(read_json(job_details, ["job_attributes"], "{}"))
        is_catch_up_enabled = read_json(job_attributes, ["catch_up"], "true")
        if is_catch_up_enabled.lower() == "true" and check_for_start_sequence_execution(run_date_time,
                                                                                        job_details["job_key"],
                                                                                        job_frequency):
            logging.info(
                "Current time greater than catch up end time and it is a first run of the sequence.. Going for catch up:")
            return True
    return False


def check_for_start_sequence_execution(run_date_time, job_key, job_frequency):
    run_date_time_epoch = int(
        (run_date_time + timedelta(minutes=job_frequency)).replace(tzinfo=local_tz).timestamp()) * 1000
    job_run_stats = check_job_run_time_execution_status(job_key, run_date_time_epoch)
    if job_run_stats and len(job_run_stats) > 0:
        return False

    return True


def check_for_hold_execute_job(job_details):
    hold_loop = 1
    while hold_loop <= 30:
        hold_application_job = is_application_job_on_hold(job_details['job_key'])
        if hold_application_job and len(hold_application_job) > 0:
            if hold_loop < 30:
                print("system on hold")
                time.sleep(120)
                hold_loop = hold_loop + 1
                print("HOLD_LOOP:", hold_loop)
            else:
                print("Waited for 60 min..still hold in place..Please restart once hold file is removed")
                sys.exit(1)
                break
        else:
            print("application job not on hold")
            break


def generate_tasks(database, scheduler_job_name, job_details, frequency, wait_buffer, dag, execution_timeout=60):
    task_name = scheduler_job_name
    job_attributes = json.loads(read_json(job_details, ["job_attributes"], "{}"))
    execution_timeout = int(read_json(job_attributes, ["execution_timeout"],
                                      execution_timeout))
    retries = int(read_json(job_attributes, ["retries"],
                            "5"))
    retry_delay = int(read_json(job_details["job_attributes"], ["retry_delay"],
                                "5"))

    return PythonOperator(
        task_id=task_name,
        depends_on_past=False,
        provide_context=True,
        python_callable=main,
        retries=retries,
        retry_delay=timedelta(minutes=retry_delay),
        execution_timeout=timedelta(minutes=execution_timeout),
        on_failure_callback=functools.partial(raise_alert, database),
        op_kwargs={'main_db': database, 'job_details': job_details, "dag_frequency": frequency,
                   'wait_buffer': wait_buffer},
        dag=dag)


def create_dag(frequency, job_frequency, buffer_wait_time, database, dag_id, default_args, schedule, catchup=True,
               max_active_runs=5, execution_timeout=60):
    dag = DAG(dag_id=dag_id, catchup=catchup, default_args=default_args, max_active_runs=max_active_runs,
              schedule_interval=schedule)

    mysql_conn = MySQLDatabase()
    job_list = get_application_job_with_dependency_for_dag(database, dag_id, mysql_conn)
    print(job_list)
    freq_table_list = []
    job_id_task_dic = {}
    dependent_tasks = set()
    job_dependencies = []
    for table_data in job_list:
        job_key = table_data['job_key']
        related_job_key = table_data['related_job_key']
        if job_key not in job_id_task_dic:
            task = generate_tasks(database, table_data['scheduler_job_name'], table_data, frequency, buffer_wait_time,
                                  dag, execution_timeout)
            job_id_task_dic[job_key] = task
        if related_job_key:
            job_dependencies.append((job_key, related_job_key))

    print(job_id_task_dic)
    print(job_dependencies)
    if job_dependencies:
        for edge in job_dependencies:
            job_id_task_dic[edge[1]] >> job_id_task_dic[edge[0]]
            dependent_tasks.add(edge[0])
    print(dependent_tasks)
    for job_id, task_obj in job_id_task_dic.items():
        if job_id not in dependent_tasks:
            freq_table_list.append(task_obj)

    print(freq_table_list)

    hold_tasks = PythonOperator(
        task_id='HOLD_JOBS_' + database,
        provide_context=True,
        python_callable=hold_job,
        on_failure_callback=functools.partial(raise_alert, database),
        op_kwargs={'job_data': job_list, 'main_db': database},
        dag=dag)

    seq_sensor = PythonOperator(
        task_id='SEQ_SENSOR_' + database,
        provide_context=True,
        python_callable=seq_job,
        op_kwargs={'frequency': frequency},
        on_failure_callback=functools.partial(raise_alert, database),
        dag=dag)

    wait_task = PythonOperator(
        task_id='WAIT_TASK',
        provide_context=True,
        python_callable=wait_job,
        on_failure_callback=functools.partial(raise_alert, database),
        op_kwargs={'frequency': frequency, 'main_db': database, 'job_frequency': job_frequency,
                   'buffer_wait_time': buffer_wait_time},
        dag=dag)

    if freq_table_list:
        seq_sensor >> hold_tasks >> wait_task >> freq_table_list

    return dag


def run_command(command_with_params, dry_run=False):
    def gather_response(std_out_or_err):
        output = []
        while True:
            line = std_out_or_err.readline()
            if not line:
                break
            output.append(line.decode('utf-8').strip())
        return output

    if isinstance(command_with_params, list):
        logging.info('running query: ' + ' '.join(command_with_params))
    else:
        logging.info('running query: ' + command_with_params)

    if dry_run:
        return "no-op", "no-op"

    result = subprocess.Popen(command_with_params,
                              shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    logging.info(f"Running Command: {command_with_params}")
    stdout, stderr = result.communicate()
    exitcode = result.returncode

    # logging.info('query stdout: %s', str(stdout))
    # logging.info('query stderr: %s', str(stderr))
    return stdout, stderr, exitcode


def total_sequences_in_range(start, end, delta):
    if not isinstance(delta, timedelta):
        delta = timedelta(**delta)
    elapsed = end - start
    return elapsed / delta