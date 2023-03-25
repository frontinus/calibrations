import time
import unittest
import mysql
import os
import json
import numpy as np
import calendar
from mysql.connector import connect, errorcode
import ProcessCalibration
from os import path
from PRISMA_SDK import CameraFactory, CalibrationExecutionHistoryFactory, SystemConfigurationFactory
from PRISMA_SDK.simpleClass import Camera, CalibrationExecutionHistory, SystemConfiguration
from datetime import datetime, timedelta
from PRISMA_SDK.LogProgramFileFactory import LogProgramFileFactory as lpff
from PRISMA_SDK.simpleClass.LogProgramFile import LogProgramFile as lpf


class TestProcessCalibration(unittest.TestCase):

    def setUp(self):
        with open('../procedures_config.json') as pc:
            default_config = json.load(pc)
        config = default_config['process_calibration']['db_config']

        self.db = None
        while not self.db:
            try:
                self.db = connect(**config)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your username or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                else:
                    print(err)
            time.sleep(1)

    def tearDown(self):
        if self.db is not None and self.db.is_connected():
            self.db.close()

    # Tests the bulkProcess function
    def test_01_process_calibration_bulk_process(self):
        camera_list = CameraFactory.CameraFactory.getList(self.db)
        n_logs = lpff().getList(self.db)[-1].id
        cameras_dir_path = SystemConfigurationFactory.SystemConfigurationFactory().getParameterValueByParameterName('cp_dir_captures', self.db)
        camera_codes_in_filesystem = os.listdir(cameras_dir_path)
        if len(camera_codes_in_filesystem) < 1:
            self.fail('No cameras found in the filesystem.')
        with open('../procedures_config.json') as pc:
            default_config = json.load(pc)
        LOG_MESSAGE_PREFIX = default_config['process_calibration']['LOG_MESSAGE_PREFIX']

        cmr = CameraFactory.CameraFactory().getByCameraCode(camera_codes_in_filesystem[0], self.db)
        if type(cmr) != type(Camera.Camera().create(None, None, None, None, None, None, None, None)):
            cmr = Camera.Camera().create( None, camera_codes_in_filesystem[0], None, None, None, 1, 1, 1)
            CameraFactory.CameraFactory().insert(cmr, self.db)
        else:
            CameraFactory.CameraFactory().update(cmr, self.db)

        with open('./failed_calibrations.json', 'r') as f:
            failed_calibrations = json.load(f)
        nmf = len(failed_calibrations)

        n_success, n_failure = ProcessCalibration.ProcessCalibration().bulkProcess(4, datetime.now(), self.db)
        if not len(camera_list) > 0:
            self.assertTrue(lpff().getList(self.db)[-1].text.decode() == f'{LOG_MESSAGE_PREFIX}No cameras to process.')
        else:
            n_s_l = 0
            nl = lpff().getList(self.db)[-1].id
            for log in lpff().getList(self.db)[n_logs:nl]:
                if log.level == 4:
                    n_s_l += 1
            self.assertIsNotNone(n_success)
            self.assertIsNotNone(n_failure)
            self.assertTrue(n_s_l == n_success)
            nl = lpff().getList(self.db)[-1].id
            for ll in lpff().getList(self.db)[n_logs:nl]:
                if ll.text.decode() == f'{LOG_MESSAGE_PREFIX}Successfully fetched {len(camera_list)} camera(s).':
                    nl = ll.id
            self.assertTrue(lpff().getList(self.db)[nl].text.decode() == f'{LOG_MESSAGE_PREFIX}Started bulk calibration processing {len(camera_list)} camera(s).')
            ns = 0
            for lgs in lpff().getList(self.db)[n_logs+1:nl]:
                if lgs.text.decode() == f'{LOG_MESSAGE_PREFIX}File configuration1.ini successfully deleted.':
                    ns += 1
            self.assertTrue(lpff().getList(self.db)[nl-2].text.decode() == f'{LOG_MESSAGE_PREFIX}Managed to complete {ns} of {nmf} previously failed calibrations.')
            if(os.stat('./failed_calibrations.json').st_size > 0):
                self.assertTrue(lpff().getList(self.db)[n_logs].text.decode() == f'{LOG_MESSAGE_PREFIX}Found previously failed calibrations. Re-attempting calibration.')
            if (datetime.now() - timedelta(days=1)).day == calendar.monthrange((datetime.now() - timedelta(days=1)).year, (datetime.now() - timedelta(days=1)).month)[1]:
                self.assertTrue(lpff().getList(self.db)[-1].text.decode() == f'{LOG_MESSAGE_PREFIX}Finishied daily and monthly bulk processing {len(camera_list)} camera(s) [{n_success} success(es), {n_failure} failure(s)].')
            else:
                self.assertTrue(lpff().getList(self.db)[-1].text.decode() == f'{LOG_MESSAGE_PREFIX}Finishied daily bulk processing {len(camera_list)} camera(s) [{n_success} success(es), {n_failure} failure(s)].')

    # Tests the start function
    def test_02_process_calibration_start(self):
        camera_list = CameraFactory.CameraFactory.getList(self.db)
        cameras_dir_path = SystemConfigurationFactory.SystemConfigurationFactory().getParameterValueByParameterName('cp_dir_captures', self.db)
        camera_codes_in_filesystem = os.listdir(cameras_dir_path)
        if len(camera_codes_in_filesystem) < 1:
            self.fail('No cameras found in the filesystem.')

        with open('../procedures_config.json') as pc:
            default_config = json.load(pc)
        LOG_MESSAGE_PREFIX = default_config['process_calibration']['LOG_MESSAGE_PREFIX']

        system_config = SystemConfigurationFactory.SystemConfigurationFactory().getList(self.db)
        for setting in system_config:
            if setting.parameter_name == 'cp_tmp_user_config_path':
                cp_tmp_user_config_path = setting.parameter_value
        ccl = []
        for c in camera_list:
            ccl.append(c.code)
        specam = list(set(ccl).difference(camera_codes_in_filesystem))[0]
        cam = CameraFactory.CameraFactory().getByCameraCode(specam, self.db)

        cmr = CameraFactory.CameraFactory().getByCameraCode(camera_codes_in_filesystem[0], self.db)
        if type(cmr) != type(Camera.Camera().create(None, None, None, None, None, None, None, None)):
            self.fail(f'Please insert camera {camera_codes_in_filesystem[0]} in the database.')

        dates_for_camera_dir_path = cameras_dir_path+"/"+camera_codes_in_filesystem[0]
        dates_for_camera_in_filesystem = os.listdir(dates_for_camera_dir_path)
        if len(dates_for_camera_in_filesystem) < 1:
            self.fail('No dates found for that camera in the filesystem.')

        dates_with_day_for_camera_dir_path = cameras_dir_path+"/"+camera_codes_in_filesystem[0]+"/"+dates_for_camera_in_filesystem[0]
        filenames_with_full_date_for_camera = os.listdir(dates_with_day_for_camera_dir_path)
        if len(filenames_with_full_date_for_camera) < 1:
            self.fail('No files found for that month for that camera in the filesystem.')

        x1 = (filenames_with_full_date_for_camera[1]).split("_")
        x2 = x1[1].split("T")
        x = x2[0]
        if int(x2[1][:2]) < 12:
            date = datetime.strptime(x, "%Y%m%d")
            date = date - timedelta(days=1)
        else:
            date = datetime.strptime(x, "%Y%m%d")
        m_o_d = 0
        if date.day == calendar.monthrange(date.year, date.month)[1]:
            m_o_d = 1
        date = date.strftime("%Y%m%d")
        if m_o_d == 1:
            len2 = len(CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraId(cmr.id, self.db))
            ProcessCalibration.ProcessCalibration().start(cmr.id, cmr.modified_by, date, 1, self.db, 4)
            self.assertTrue(len(CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraId(cmr.id, self.db)) > len2)
            self.assertTrue(lpff().getList(self.db)[-1].text.decode() == f'{LOG_MESSAGE_PREFIX}File configuration{cmr.modified_by}.ini successfully deleted.')
            self.assertTrue(lpff().getList(self.db)[-2].text.decode() == f'{LOG_MESSAGE_PREFIX}Camera {cmr.code} on date {date} was successfully monthly and daily processed.')
            self.assertTrue(lpff().getList(self.db)[-3].text.decode() == f'{LOG_MESSAGE_PREFIX}Monthly and daily calibration finished processing camera {cmr.code} on date {date}')
            self.assertTrue(lpff().getList(self.db)[-4].text.decode() == f'{LOG_MESSAGE_PREFIX}Starting monthly and daily IDL procedure for camera {cmr.code} with configuration_{cmr.modified_by}.ini.')
            self.assertTrue(lpff().getList(self.db)[-5].text.decode() == f'{LOG_MESSAGE_PREFIX}Successfully created configuration_{cmr.modified_by}.ini for this user.')
            self.assertTrue(lpff().getList(self.db)[-6].text.decode() == f'{LOG_MESSAGE_PREFIX}Found capture from camera {cmr.code} on date {date[:6]} in the filesystem at {cameras_dir_path}/{cmr.code}/{date[:6]}.')
            self.assertTrue(lpff().getList(self.db)[-7].text.decode() == f'{LOG_MESSAGE_PREFIX}Successfully created CalibrationExecutionHistory entry with id {CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraIdForUser(cmr.id, cmr.modified_by, self.db)[-1].id} on the db.')
            self.assertFalse(path.exists(f'{cp_tmp_user_config_path}/configuration_{cmr.modified_by}.ini'))
            self.assertTrue(len(CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraId(cmr.id, self.db)) > 0)
            self.assertTrue(ProcessCalibration.ProcessCalibration().start(cmr.id, cmr.modified_by, date[:6], 1, self.db, 4))
            self.assertTrue(len(CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraId(cmr.id, self.db)) > 1)
            self.assertTrue(lpff().getList(self.db)[-2].text.decode() == f'{LOG_MESSAGE_PREFIX}Camera {cmr.code} on date {date} was successfully monthly processed.')
            self.assertTrue(lpff().getList(self.db)[-3].text.decode() == f'{LOG_MESSAGE_PREFIX}Monthly calibration finished processing camera {cmr.code} on date {date}')
            self.assertTrue(lpff().getList(self.db)[-4].text.decode() == f'{LOG_MESSAGE_PREFIX}Starting monthly IDL procedure for camera {cmr.code} with configuration_{cmr.modified_by}.ini.')
        else:
            ProcessCalibration.ProcessCalibration().start(cmr.id, cmr.modified_by, str(123513135), 0, self.db, 4)
            self.assertTrue(lpff().getList(self.db)[-1].text.decode() == f'{LOG_MESSAGE_PREFIX}Error: The date in your input is not correct, make sure it is in the format YYYYmmdd or YYYYmm.')
            ProcessCalibration.ProcessCalibration().start(cmr.id, cmr.modified_by, date, 1515, self.db, 4)
            self.assertTrue(lpff().getList(self.db)[-1].text.decode() == f'{LOG_MESSAGE_PREFIX}Error: Couldn\'t create CalibrationExecutionHistory entry on the db.')
            ProcessCalibration.ProcessCalibration().start(cam.id, cam.modified_by, date, 0, self.db, 4)
            self.assertTrue(CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getList(self.db)[-1].ceh_stderr.decode() == f'Error: Unable to find capture with camera {cam.code} on date {date[:6]} in the filesystem.\n')
            self.assertTrue(lpff().getList(self.db)[-1].text.decode() == f'{LOG_MESSAGE_PREFIX}Error: Unable to find capture from camera {cam.code} on date {date[:6]} in the filesystem.')
            ProcessCalibration.ProcessCalibration().start(cmr.id, 42, date, 0, self.db, 4)
            print(lpff().getList(self.db)[-6].text.decode())
            print(f'{LOG_MESSAGE_PREFIX}Warning: No configuration found for user 42, proceeding with default configuration.')
            self.assertTrue(lpff().getList(self.db)[-6].text.decode() == f'{LOG_MESSAGE_PREFIX}Warning: No configuration found for user 42, proceeding with default configuration.')
            len1 = len(CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraId(cmr.id, self.db))
            ProcessCalibration.ProcessCalibration().start(cmr.id, cmr.modified_by, date, 0, self.db, 4)
            self.assertTrue(len(CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraId(cmr.id, self.db)) > len1)
            self.assertTrue(lpff().getList(self.db)[-1].text.decode() == f'{LOG_MESSAGE_PREFIX}File configuration{cmr.modified_by}.ini successfully deleted.')
            self.assertTrue(lpff().getList(self.db)[-2].text.decode() == f'{LOG_MESSAGE_PREFIX}Camera {cmr.code} on date {date} was successfully daily processed.')
            self.assertTrue(lpff().getList(self.db)[-3].text.decode() == f'{LOG_MESSAGE_PREFIX}Daily calibration finished processing camera {cmr.code} on date {date}')
            self.assertTrue(lpff().getList(self.db)[-4].text.decode() == f'{LOG_MESSAGE_PREFIX}Starting daily IDL procedure for camera {cmr.code} with configuration_{cmr.modified_by}.ini.')
            self.assertTrue(lpff().getList(self.db)[-5].text.decode() == f'{LOG_MESSAGE_PREFIX}Successfully created configuration_{cmr.modified_by}.ini for this user.')
            self.assertTrue(lpff().getList(self.db)[-6].text.decode() == f'{LOG_MESSAGE_PREFIX}Found capture from camera {cmr.code} on date {date[:6]} in the filesystem at {cameras_dir_path}/{cmr.code}/{date[:6]}.')
            self.assertTrue(lpff().getList(self.db)[-7].text.decode() == f'{LOG_MESSAGE_PREFIX}Successfully created CalibrationExecutionHistory entry with id {CalibrationExecutionHistoryFactory.CalibrationExecutionHistoryFactory().getByCameraIdForUser(cmr.id, cmr.modified_by, self.db)[-1].id} on the db.')
            self.assertFalse(path.exists(f'{cp_tmp_user_config_path}/configuration_{cmr.modified_by}.ini'))


if __name__ == '__main__':
    unittest.main()
