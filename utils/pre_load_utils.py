import global_constants
import datetime
import pprint


class PreLoadUtils:
    def __init__(self, passed_job_params: JobParam, passed_common_operations: CommonOperations):
        self.__job_params = passed_job_params
        self.__common_operations = passed_common_operations
        self.__base_path = self.__job_params.get_params(global_constants.base_path_key_name)
        self.__xp_base_path = self.__job_params.get_params(global_constants.xp_base_path_key_name)
        self.__common_base_path = self.__job_params.get_params(global_constants.common_base_path_key_name)
        self.__tenant = self.__job_params.tenant
        self.__job_id = passed_job_params.get_params("id")
        self.__unwanted_keys = self.__job_params.get_params(global_constants.common_section.strip() +
                                                            "," + global_constants.unwanted_keys.strip())

    def validate_seq_num(self):
        now_seq_num = int(self.__job_params.get_params("seq_num"))
        max_seq = int(self.__job_params.get_params("seq_max"))
        if now_seq_num > max_seq:
            error_msg = f'In validate_seq_num of PreLoadUtils, ' \
                        f'Seq number {now_seq_num} is not valid, ' \
                        f'expected value should be less than {max_seq}'
            self.__common_operations.raise_value_error(error_msg)

    def validate_run_date(self):
        now_run_date = self.__job_params.get_params("run_date")
        if not datetime.datetime.strptime(now_run_date, '%Y%m%d'):
            error_msg = f'In validate_run_date of PreLoadUtils, ' \
                        f'Run date is not valid {now_run_date}'
            self.__common_operations.raise_value_error(error_msg)
        else:
            current_date = datetime.datetime.now().strftime('%Y%m%d')
            if now_run_date < current_date:
                catchup = True
            elif now_run_date == current_date:
                catchup = False
            else:
                catchup = False
                error_msg = f'In validate_run_date of PreLoadUtils, ' \
                            f'now_run_date --> {now_run_date}, ' \
                            f'current_date --> {current_date}, ' \
                            f'now_run_date has to be less than or equal to current_date, ' \
                            f'cannot be greater, seems like running for future date'
                self.__common_operations.raise_value_error(error_msg)
            self.__common_operations.call_set_job_details_params("catchup", catchup)

    def check_skip_job(self):
        skip_job = self.__job_params.get_params(
            global_constants.common_section.strip() +
            "," + global_constants.skip_job_file.strip()).\
            format(base_path=self.__base_path,
                   common_base_path=self.__common_base_path,
                   xp_base_path=self.__xp_base_path,
                   tenant=self.__tenant)
        now_job_id = self.__job_params.id
        self.__common_operations.log_and_print(f"In check_skip_job of PreLoadUtils, "
                                               f"skip_job --> {skip_job}, "
                                               f"now_job_id --> {now_job_id}", print_msg=True)
        with open(skip_job, 'r') as fileobject:
            lines = fileobject.read().splitlines()
            for job_id_item in lines:
                if now_job_id == job_id_item:
                    self.__common_operations.log_and_print(f'In check_skip_job of PreLoadUtils, '
                                                           f'Job found in skip list {now_job_id}',
                                                           logger_type="error")
                    exit(0)
                else:
                    continue
            self.__common_operations.log_and_print("In check_skip_job of PreLoadUtils, "
                                                   "Job not in skip list, "
                                                   "Continuing with the job", print_msg=True)
        fileobject.close()

    # Added this to remove unwanted keys from a dictionary:
    def recursive_remover_and_print(self, local_params_display=None):
        if local_params_display is None:
            local_params_display = {k: v for k, v in self.__job_params.local_params.items()}
        for key, value in list(local_params_display.items()):
            if isinstance(value, dict):
                self.recursive_remover_and_print(value)
            if key in self.__unwanted_keys:
                del local_params_display[key]
        # pprint.pprint(local_params_display)
        # self.__common_operations.log_and_print(f"In recursive_remover_and_print of PreLoadUtils, "
        #                                        f"local_params --> {local_params_display}", print_msg=True)

    def get_run_id_list(self):
        self.__common_operations.log_and_print(f"In get_run_id_list of PreLoadUtils", print_msg=True)
        max_seq = int(self.__job_params.get_params("seq_max"))
        seq_num = int(self.__job_params.get_params("seq_num"))
        refresh_frequency = int(self.__job_params.get_params("job_details,Refresh_Frequency"))
        run_date = self.__job_params.get_params("run_date")

        try:
            # since mandatory params are set later, directly getting it from data_store object
            target_table = self.__job_params.get_params("job_data_store,TARGET")[0]
            target_table_keys = self.__job_params.safe_get_params(target_table) or {}

            # Added to support weekly schedules as well
            schedule = global_constants.daily_schedule
            if "JOB_SCHEDULE" in target_table_keys and "WEEKLY" == target_table_keys["JOB_SCHEDULE"]:
                schedule = global_constants.weekly_schedule

            # added to support fixed timing schedules like 075959 instead of 235959
            run_id_format = target_table_keys.get("JOB_RUN_ID_FORMAT", global_constants.default_run_id_format)
        except Exception as ex:
            self.__common_operations.log_and_print("In get_run_id_list of PreLoadUtils, "
                                                   "Exception in getting schedule details. "
                                                   f"So reverting to older flow. {ex}")
            schedule = global_constants.daily_schedule
            run_id_format = global_constants.default_run_id_format

        max_loop_count = ((24 * schedule * 60) / max_seq) / refresh_frequency
        start_time = datetime.datetime.strptime(run_date, '%Y%m%d') + datetime.timedelta(minutes=((24 * 60) / max_seq) *
                                                                                                 (seq_num - 1))
        self.__common_operations.log_and_print(f"In get_run_id_list of PreLoadUtils, "
                                               f"max_seq --> {max_seq}, "
                                               f"refresh_frequency --> {refresh_frequency}, "
                                               f"run_date --> {run_date}, "
                                               f"max_loop_count --> {max_loop_count}, "
                                               f"start_time --> {start_time}", print_msg=True)
        loop_count = 1
        run_id_list = []
        while loop_count <= max_loop_count:
            run_id = start_time + datetime.timedelta(
                minutes=refresh_frequency * loop_count) - datetime.timedelta(
                seconds=1)
            run_id_list.append(run_id.strftime(run_id_format))
            loop_count += 1
        if max_seq == 1:
            current_run_id = ''
            for each_run_id in run_id_list:
                if each_run_id > datetime.datetime.now().strftime(global_constants.default_run_id_format):
                    break
                else:
                    current_run_id = each_run_id
            if current_run_id == '':
                current_run_id = run_id_list[0]
            run_id_list = [current_run_id]

        self.__common_operations.log_and_print(f"In get_run_id_list of PreLoadUtils, "
                                               f"run_id_list --> {run_id_list}")
        self.__common_operations.call_set_job_details_params("run_id_list", run_id_list, _check=True)

    def get_pending_run_id_list(self):
        pending_run_id_list = []
        self.__common_operations.log_and_print(f"In get_pending_run_id_list of PreLoadUtils", print_msg=True)
        run_id_list = self.__job_params.get_params("run_id_list")
        last_run = self.__common_operations.safe_get_params("last_run")
        last_run_status = self.__common_operations.safe_get_params("last_run,status")
        last_run_id = self.__common_operations.safe_get_params("last_run,last_run_id")
        min_run_id_to_process = self.__job_params.get_params("min_run_id_to_process")
        if last_run and last_run_status and last_run_id:
            start_index = run_id_list.index(last_run_id)
            self.__common_operations.log_and_print(f"In get_pending_run_id_list of PreLoadUtils, "
                                                   f"start_index {start_index}", print_msg=True)
            if start_index is None:
                error_str = "In get_pending_run_id_list of PreLoadUtils, " \
                            "Undefined last run id processed in same sequence"
                self.__common_operations.log_and_print(error_str, logger_type="error")
                self.__common_operations.create_stats_capture_dict(run_id_list[0], "FAILED", error_str,
                                                                   run_id_list[0], run_id_list[0], 0, 0, 'F')
            else:
                if last_run_status == "FAILED":
                    last_run_status_msg = self.__common_operations.safe_get_params("last_run,status_msg")
                    # if last_run_status_msg != "JOB|Dropzone delete failed":
                    pending_run_id_list = run_id_list[start_index:]
                    # Added this as the rerun job is rerun from the scheduler
                    self.__common_operations.call_set_job_details_params("rerun_indicator", 'Y')
                    self.__common_operations.log_and_print(f'In get_pending_run_id_list of PreLoadUtils, '
                                                           f'Job execution for '
                                                           f'Job_Id --> {self.__job_id}, '
                                                           f'last_run_id --> {last_run_id} '
                                                           f'has already failed due to {last_run_status_msg}, '
                                                           f'rerunning now', print_msg=True)
                elif last_run_status == "COMPLETED" and start_index == len(run_id_list) - 1:
                    self.__common_operations.log_and_print(f'In get_pending_run_id_list of PreLoadUtils, '
                                                           f'Job execution for '
                                                           f'Job_Id --> {self.__job_id}, '
                                                           f'last_run_id --> {last_run_id} '
                                                           f'is already completed. Exiting with code 0',
                                                           print_msg=True)
                    exit(0)
                elif last_run_status == "COMPLETED":
                    if start_index < (len(run_id_list) - 1):
                        pending_run_id_list = run_id_list[start_index + 1:]
                    self.__common_operations.log_and_print(f'In get_pending_run_id_list of PreLoadUtils, '
                                                           f'Job execution for '
                                                           f'Job_Id --> {self.__job_id}, '
                                                           f'last_run_id --> {last_run_id} '
                                                           f'is already completed, hence updating the '
                                                           f'pending_run_id_list --> {pending_run_id_list}',
                                                           print_msg=True)
                else:
                    error_str = "In get_pending_run_id_list of PreLoadUtils, " \
                                "Undefined last run id status in same sequence. " \
                                "Expected values FAILED or COMPLETED"
                    self.__common_operations.create_stats_capture_dict(run_id_list[0], "FAILED", error_str,
                                                                       run_id_list[0], run_id_list[0], 0, 0, 'F')
        elif last_run and (
                not last_run_status or last_run_id):
            error_str = "In get_pending_run_id_list of PreLoadUtils, " \
                        "Last Processed Run ID in same sequence " \
                        "is missing status and run id"
            self.__common_operations.create_stats_capture_dict(run_id_list[0], "FAILED", error_str,
                                                               run_id_list[0], run_id_list[0], 0, 0, 'F')
        elif not last_run:
            pending_run_id_list = run_id_list

        self.__common_operations.log_and_print(f"In get_pending_run_id_list of PreLoadUtils, "
                                               f"Script will process run ids, "
                                               f"pending_run_id_list --> {pending_run_id_list}", print_msg=True)
        pprint.pprint(pending_run_id_list)
        if not pending_run_id_list:
            error_str = "In get_pending_run_id_list of PreLoadUtils, " \
                        "Error in Calculating Run IDs to process"
            self.__common_operations.create_stats_capture_dict(run_id_list[0], "FAILED", error_str,
                                                               run_id_list[0], run_id_list[0], 0, 0, 'F')
        # Skipping current sequence if min_run_id_to_process is already completed
        if pending_run_id_list[0] > min_run_id_to_process:
            self.__common_operations.log_and_print(f"In get_pending_run_id_list of PreLoadUtils, "
                                                   f"Job already completed past min_run_id_to_process: "
                                                   f"{min_run_id_to_process}. "
                                                   f"Next run id to process is {pending_run_id_list[0]} . "
                                                   f"Exiting with code 0",
                                                   logger_type="error")
            exit(0)
        self.__common_operations.call_set_job_details_params("pending_run_id_list", pending_run_id_list, _check=True)
