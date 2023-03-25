import time
import mysql
import os
import subprocess
import calendar
import json
from datetime import datetime, timedelta
from mysql.connector import connect, errorcode
from PRISMA_SDK.simpleClass import UserConfiguration, CorePerson, CalibrationExecutionHistory, Camera, SystemConfiguration
from PRISMA_SDK import UserConfigurationFactory, CorePersonFactory, CalibrationExecutionHistoryFactory, CameraFactory, IDLConfigFileHandler, SystemConfigurationFactory
from PRISMA_SDK.LogProgramFileFactory import LogProgramFileFactory as lpff
from PRISMA_SDK.simpleClass.LogProgramFile import LogProgramFile as lpf

# load default configuration from json file
with open('../procedures_config.json') as pc:
    default_config = json.load(pc)

default_user = default_config['process_calibration']['default_user']

config = default_config['process_calibration']['db_config']

LOG_MESSAGE_PREFIX = default_config['process_calibration']['LOG_MESSAGE_PREFIX']

db_connection_attempts = default_config['process_calibration']['db_connection_attempts']


class ProcessCalibration:
    @staticmethod
    def __format_d(d, is_m):
        '''
        Returns string cotaining formatted date.  
        E.g.  
        `19991120` -> `20-11-1999`  
        `199911`   -> `11-1999`  
        '''
        return f'{"" if is_m else f"{d[6:8]}-"}{d[4:6]}-{d[:4]}'

    @staticmethod
    def start(cameraId, userId, date, is_monthly, db, loggingUserId=False):
        '''
        Runs procedure calibration.pro for camera cameraId on date date.  
        If is_monthly is set to 1 both daily and monthly processing will be done.  
        '''

        # if loggingUserId was not changed do logging for user userId
        if loggingUserId is False:
            loggingUserId = userId

        def log_info_with_level(text, level, db):
            '''
            Creates log entry in entity pr_log_program_file of type INFO, text = text, verbosity level = level.  
            '''
            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'INFO', level, LOG_MESSAGE_PREFIX + text, loggingUserId, loggingUserId, loggingUserId), db)

        def log_warning_with_level(text, level, db):
            '''
            Creates log entry in entity pr_log_program_file of type WARNING, text = text, verbosity level = level.  
            '''
            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'WARNING', level, LOG_MESSAGE_PREFIX + text, loggingUserId, loggingUserId, loggingUserId), db)

        def log_error_with_level(text, level, db):
            '''
            Creates log entry in entity pr_log_program_file of type ERROR, text = text, verbosity level = level.  
            '''
            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'ERROR', level, LOG_MESSAGE_PREFIX + text, loggingUserId, loggingUserId, loggingUserId), db)

        # Check if date is valid
        try:
            if len(date) > 8:
                raise Exception('Date seems a bit too long')
            elif len(date) <= 6:
                is_monthly = 1
                datetime.strptime(date, '%Y%m')
            else:
                datetime.strptime(date, '%Y%m%d')
        except:
            log_error_with_level('Error: The date in your input is incorrect, make sure it is in the format YYYYmmdd or YYYYmm.', 1, db)
            return False

        # Create new CalibrationExecutionHistory entry
        history_entry = CalibrationExecutionHistory.CalibrationExecutionHistory().create(cameraId, date, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), is_monthly, '', '', '', userId, userId, userId)
        if not CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().insert(history_entry, db):
            # Error couldn't create CalibrationExecutionHistory entry on the db
            log_error_with_level('Error: Couldn\'t create CalibrationExecutionHistory entry on the db.', 1, db)
            return False
        history_entry.id = CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraIdForUser(cameraId, userId, db)[-1].id
        log_info_with_level(f'Successfully created CalibrationExecutionHistory entry with id {history_entry.id} on the db.', 5, db)

        # Fetch system configuration parameters
        sys_config = SystemConfigurationFactory.SystemConfigurationFactory().getList(db)
        for parameter in sys_config:
            if parameter.parameter_name == 'root_path':
                root_path = parameter.parameter_value
            elif parameter.parameter_name == 'cp_dir_captures':
                captures_dir_path = parameter.parameter_value
            elif parameter.parameter_name == 'cp_dir_astrometry':
                astrometry_dir_path = parameter.parameter_value
            elif parameter.parameter_name == 'cp_tmp_user_config_path':
                cp_config_dir_path = parameter.parameter_value

        # Find camera code for this calibration
        camera_code = CameraFactory.CameraFactory().getById(cameraId, db).code

        # Create symbolic links to '.fit' files in captures folder
        if os.path.exists(f'{root_path}/{camera_code}'):
            folder_base_name = os.listdir(f'{root_path}/{camera_code}')[0].split('_')[0]
            if len(date) > 6:
                process_dates = [date, (datetime.strptime(date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")]
                for pd in process_dates:
                    if not os.path.exists(f'{captures_dir_path}/{camera_code}/{pd[:6]}'):
                        os.makedirs(os.path.dirname(f'{captures_dir_path}/{camera_code}/{pd[:6]}/'))
                    if os.path.exists(f'{root_path}/{camera_code}/{folder_base_name}_{pd}/captures'):
                        fit_files = os.listdir(f'{root_path}/{camera_code}/{folder_base_name}_{pd}/captures')
                        for fit in fit_files:
                            if not os.path.exists(f'{captures_dir_path}/{camera_code}/{pd[:6]}/{fit}'):
                                os.symlink(f'{root_path}/{camera_code}/{folder_base_name}_{pd}/captures/{fit}', f'{captures_dir_path}/{camera_code}/{pd[:6]}/{fit}')
            else:
                if not os.path.exists(f'{captures_dir_path}/{camera_code}/{date}'):
                    os.makedirs(os.path.dirname(f'{captures_dir_path}/{camera_code}/{date}/'))
                for date_dir in os.listdir(f'{root_path}/{camera_code}'):
                    if os.path.exists(f'{root_path}/{camera_code}/{date_dir}/captures'):
                        fit_files = os.listdir(f'{root_path}/{camera_code}/{date_dir}/captures')
                        for fit in fit_files:
                            if not os.path.exists(f'{captures_dir_path}/{camera_code}/{date}/{fit}'):
                                os.symlink(f'{root_path}/{camera_code}/{date_dir}/captures/{fit}', f'{captures_dir_path}/{camera_code}/{date}/{fit}')

        # Find if data for this calibration exists on disk
        exists_on_disk = False
        if os.path.exists(f'{captures_dir_path}/{camera_code}/{date[:6]}'):
            filenames_with_full_date_for_camera = os.listdir(f'{captures_dir_path}/{camera_code}/{date[:6]}')
            if is_monthly:
                if len(filenames_with_full_date_for_camera) > 1:
                    exists_on_disk = True
            else:
                filenames_with_full_date_for_camera.pop(0)
                for filename in filenames_with_full_date_for_camera:
                    fit_date = filename.split("_")[1].split("T")[0]
                    fit_time = filename.split("_")[1].split("T")[1]
                    if (int(fit_time[:2]) > 12 and fit_date == date) or (int(fit_time[:2]) < 12 and (datetime.strptime(fit_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d") == date):
                        exists_on_disk = True
                        break
        if exists_on_disk is False:
            # Error calibration not found in filesystem
            history_entry.ceh_stderr = history_entry.ceh_stderr + (f'Error: Unable to find captures from camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)} in the filesystem.\n')
            CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().update(history_entry, db)
            log_error_with_level(f'Error: Unable to find captures from camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)} in the filesystem.', 2, db)
            return False
        else:
            log_info_with_level(f'Found capture from camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)} in the filesystem at {captures_dir_path}/{camera_code}/{date[:6]}/.', 1, db)

        # Find config_parameters for this user
        usr_config = UserConfigurationFactory.UserConfigurationFactory().getDictForUser(userId, db)
        if len(usr_config) == 0:
            log_warning_with_level(f'Warning: No configuration found for user {userId}, proceeding with default configuration.', 1, db)

        # Create file configuration_userId.ini and update CalibrationExecutionHistory entry with user config_parameters
        config_json = IDLConfigFileHandler.IDLConfigFileHandler().create(userId, usr_config, sys_config)
        if config_json is False:
            # Error couldn't create config file
            history_entry.ceh_stderr = history_entry.ceh_stderr + ('Error: Unable to create configuration.ini file for this user.\n')
            CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().update(history_entry, db)
            log_error_with_level(f'Error: Unable to create configuration_{userId}.ini file for this user.', 1, db)
            return False
        else:
            # Successfully created file
            history_entry.config_parameters = config_json
            CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().update(history_entry, db)
            log_info_with_level(f'Successfully created configuration_{userId}.ini for this user.', 1, db)

        # IDL execution and update CalibrationExecutionHistory entry with new information (stdout, stderr)
        if is_monthly:
            log_info_with_level(f'Starting monthly{" and daily" if len(date) > 6 else ""} IDL procedure for camera {camera_code} with configuration_{userId}.ini.', 1, db)
        else:
            log_info_with_level(f'Starting daily IDL procedure for camera {camera_code} with configuration_{userId}.ini.', 1, db)
        cmd = ['bash', '-c', f'idl -e "calibration, \'{camera_code}\', \'{date}\', process_image=1, process_day={1 if len(date) > 6 else 0}, process_month={is_monthly}, config_file=\'{cp_config_dir_path}/configuration_{userId}.ini\'"']
        pipes = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = pipes.communicate()

        if pipes.returncode != 0:
            # Error unable to run idl
            history_entry.ceh_stderr = history_entry.ceh_stderr + (f'Error: Unable to run IDL procedure. Return code: {pipes.returncode}.\n')
            CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().update(history_entry, db)
            log_error_with_level(f'Error: Unable to run IDL procedure. Return code: {pipes.returncode}.', 1, db)
            return False
        else:
            # Update stdout and stderr attributes
            history_entry.ceh_stdout = history_entry.ceh_stdout + (std_out.decode("utf-8"))
            history_entry.ceh_stderr = history_entry.ceh_stderr + ("\n".join(std_err.decode("utf-8").split("\n")[8:]))  # Remove first 8 lines as they include IDL license information
            CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().update(history_entry, db)
            if is_monthly:
                log_info_with_level(f'Monthly{" and daily" if len(date) > 6 else ""} calibration finished processing camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)}', 1, db)
            else:
                log_info_with_level(f'Daily calibration finished processing camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)}', 1, db)

        # Determine if execution was successful by testing presence of new files in astrometry/cameraId/month_date directory
        if not os.path.exists(f'{astrometry_dir_path}/{camera_code}/{date[:6]}/{camera_code}_{date}_astro_solution.txt'):
            history_entry.ceh_stderr = history_entry.ceh_stderr + (f'Error: Unable to generate monthly{" and daily" if len(date) > 6 else ""} astrometry for camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)}.\n' if is_monthly else f'Error: Unable to generate daily astrometry for camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)}.\n')
            CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().update(history_entry, db)
            log_error_with_level(f'Error: Unable to generate monthly{" and daily" if len(date) > 6 else ""} astrometry for camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)}.' if is_monthly else f'Error: Unable to generate daily astrometry for camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)}.', 1, db)
            return False
        else:
            if is_monthly:
                log_info_with_level(f'Camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)} was successfully monthly{" and daily" if len(date) > 6 else ""} processed.', 1, db)
            else:
                log_info_with_level(f'Camera {camera_code} on date {ProcessCalibration.__format_d(date, is_monthly)} was successfully daily processed.', 1, db)
        
        # Delete configuration_userId.ini
        if not IDLConfigFileHandler.IDLConfigFileHandler().delete(userId, sys_config):
            # Error unable to delete config file for this user
            history_entry.ceh_stderr = history_entry.ceh_stderr(f'Error: Unable to delete configuration{userId}.ini file for this user after successful IDL procedure execution.\n')
            CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().update(history_entry, db)
            log_error_with_level(f'Error: Unable to delete configuration{userId}.ini file for this user after successful IDL procedure execution.', 1, db)
            return False
        log_info_with_level(f'File configuration{userId}.ini successfully deleted.', 1, db)

        return True

    @staticmethod
    def bulkProcess(launcherId, date, db):
        '''
        This method finds what day to process, whether it needs to process for a month or only a day and starts processing all calibrations.
        '''
        def fetch_cameras_to_process(db):
            '''
            Returns list of active cameras and how many they are.  
            '''
            camera_list = CameraFactory.CameraFactory.getListActiveCameras(db)
            return camera_list, len(camera_list)

        max_failed_retry_attempts = eval(SystemConfigurationFactory.SystemConfigurationFactory().getParameterValueByParameterName('calibration_max_failed_retry_attempts', db))

        # Check if we failed calibration of some cameras in previous runs
        with open('./failed_calibrations.json', 'r') as f:
            failed_calibrations = json.load(f)
        num_previously_failed = len(failed_calibrations)
        if num_previously_failed > 0:
            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'INFO', 1, f'{LOG_MESSAGE_PREFIX}Found previously failed calibrations. Re-attempting calibration.', launcherId, launcherId, launcherId), db)
            # Retry executing previously failed calibrations
            num_previously_failed = 0
            success = 0
            for failed_date in list(failed_calibrations):
                num_previously_failed += len(list(failed_calibrations[failed_date]["camera_data"]))
                is_monthly = 0
                if failed_calibrations[failed_date]['is_monthly'] == 1:
                    is_monthly = 1
                lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'INFO', 5, f'{LOG_MESSAGE_PREFIX}Re attempting {len(list(failed_calibrations[failed_date]["camera_data"]))} calibrations in date {ProcessCalibration.__format_d(failed_date, is_monthly)}, [{max_failed_retry_attempts - failed_calibrations[failed_date]["attempts"]} attempts left].', launcherId, launcherId, launcherId), db)
                index = 0
                for camera_data in list(failed_calibrations[failed_date]['camera_data']):
                    if CameraFactory.CameraFactory().isCameraActive(camera_data[0], db):
                        if ProcessCalibration.start(camera_data[0], camera_data[1], failed_date, is_monthly, db, loggingUserId=launcherId):
                            del failed_calibrations[failed_date]['camera_data'][index]
                            success += 1
                    index += 1
                failed_calibrations[failed_date]['attempts'] += 1
                if len(failed_calibrations[failed_date]['camera_data']) == 0 or failed_calibrations[failed_date]['attempts'] >= max_failed_retry_attempts:
                    del failed_calibrations[failed_date]

            with open('./failed_calibrations.json', 'w') as f:
                json.dump(failed_calibrations, f, sort_keys=True, indent=4)
            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'INFO', 1, f'{LOG_MESSAGE_PREFIX}Managed to complete {success} of {num_previously_failed} previously failed calibrations.', launcherId, launcherId, launcherId), db)

        # Find the date to calibrate
        now = date - timedelta(days=1)

        # Find if it's last day of the month
        is_monthly = 0
        if now.day == calendar.monthrange(now.year, now.month)[1]:
            is_monthly = 1
        camera_list, n_cameras = fetch_cameras_to_process(db)

        if n_cameras > 0:
            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'INFO', 1, f'{LOG_MESSAGE_PREFIX}Successfully fetched {n_cameras} camera(s).', launcherId, launcherId, launcherId), db)
            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'INFO', 1, f'{LOG_MESSAGE_PREFIX}Started bulk calibration processing {n_cameras} camera(s).', launcherId, launcherId, launcherId), db)
            n_success = 0
            n_failure = 0

            for camera in camera_list:
                success = ProcessCalibration.start(camera.id, camera.modified_by, now.strftime("%Y%m%d"), is_monthly, db, loggingUserId=launcherId)
                lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "INFO" if success else "ERROR", 4 if success else 1, f'{LOG_MESSAGE_PREFIX}({n_success + n_failure + 1}/{n_cameras}) Camera {camera.code} for user {camera.modified_by} {"was successfully processed." if success else "could not be daily processed."}', launcherId, launcherId, launcherId), db)
                if success:
                    n_success += 1
                else:
                    # Save failed calibration data in failed_calibrations.json
                    with open('./failed_calibrations.json', 'r') as f:
                        failed_calibrations = json.load(f)
                    try:
                        failed_calibrations[now.strftime("%Y%m%d")]['camera_data'].append((camera.id, camera.modified_by))
                    except:
                        failed_calibrations[now.strftime("%Y%m%d")] = {
                            'is_monthly': is_monthly,
                            'camera_data': [(camera.id, camera.modified_by)],
                            'attempts': 1
                        }
                    with open('./failed_calibrations.json', 'w') as f:
                        json.dump(failed_calibrations, f, indent=4)
                    n_failure += 1

            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'INFO', 1, f'{LOG_MESSAGE_PREFIX}Finishied daily {"and monthly " if is_monthly else ""}bulk processing {n_cameras} camera(s) [{n_success} success(es), {n_failure} failure(s)].', launcherId, launcherId, launcherId), db)
        else:
            lpff().insert(lpf().create(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'INFO', 1, f'{LOG_MESSAGE_PREFIX}No cameras to process.', launcherId, launcherId, launcherId), db)

        return n_success, n_failure


if __name__ == '__main__':
    db = None
    a = 0
    while not db:
        try:
            db = connect(**config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something is wrong with your username or password')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print('Database does not exist')
            else:
                print(err)
            a += 1
            if a == db_connection_attempts:
                print('Unable to connect to the database, exiting')
                exit()
            time.sleep(1)

    launcher_id = CorePersonFactory.CorePersonFactory().login(default_user["username"], default_user["password"], db)
    if launcher_id is not False:
        lpff().insert(lpf().create(datetime.now(), 'INFO', 4, f'{LOG_MESSAGE_PREFIX}Successfully logged in user {default_user["username"]}', launcher_id, launcher_id, launcher_id), db)
        ProcessCalibration.bulkProcess(launcher_id, datetime.now(), db)
    else:
        lpff().insert(lpf().create(datetime.now(), 'ERROR', 1, f'{LOG_MESSAGE_PREFIX}Error: Unable to login user {default_user["username"]}', 1, 1, 1), db)
