<p align="center">
  <img src="https://i.imgur.com/IHjGJBk.png" />
</p>

# PRISMA Calibration
This repo contains the logic necessary to run the IDL procedure `calibration.pro`  

## Installation

```console
# clone the repo
$ git clone https://github.com/n3srl/PRISMA_CALIBRATION.git

# change the working directory to PRISMA_CALIBRATION
$ cd PRISMA_CALIBRATION

# initialize SDK submodule
$ git submodule update --init --recursive
```
## Files Description
- `ProcessCalibration.py` contains the logic for processing a calibration, the bulk processing function and the main that calls it
- `test/` is the folder that contains the 'test_process_calibration.py' file that runs unittests on 'ProcessCalibration.py'
- `PRISMA_SDK/` is the folder that contains all the files that define useful functions and classes used in 'ProcessCalibration.py'
- `failed_calibrations.json` is a json file which gets populated with failed calibrations in order to re-attempt their processing when the method bulkProcess() is called

## What it does
'ProcessCalibration.py' executes the calibration for every single camera in the system **of which station's `active` attribute is set to 1** for the last julian day.  
If we are the last day of a month we also execute the monthly calibration for each camera.  
To do so the process picks every capture in the 'captures' folder for each julian day and puts the results of the calibration in the 'astrometry' folder.  
If a calibration is failed, it's details are inserted in the `failed_calibrations.json` file and its processing is re-attempted in the next run.  

## How to use
your captures path must be like the following : ./captures/camera/date/file.  
The camera field being the code of the camera that took those captures, the date field being the date without the day in which the captures were taken and file being the capture in .fit.gz format.  
the results of the calibration are found on the path ./astrometry/camera/date/file camera and date having the same meaning as before and file being the various results of the calibration.  
Since the IDL process runs the calibration for a camera on a julian day the ProcessCalibration program should be called at `12:00` of every day.  

Notes:
- If `ProcessCalibration.py` is run directly from console, `ProcessCalibration().bulkProcess(db)` will start with default database and launcher parameters (parameters contained in the file `../procedures_config.json`)  

## Logic stages
This is a brief step by step explanation of how the procedure works:
- logs the user to the database
- Determines if previous runs failed the execution of one or more calibrations by checking the file `failed_calibrations.json`, in case it is populated tries to process them. 
- It determines what day it is going to process
- calls the bulkProcess function
- in the bulkProcess function determines wether it's the last day of the month
- fetches the cameras to process from the database
- for each camera it calls the start function, it calls it twice in case it is processing on the last day of a month
- Creates a new `CalibrationExecutionHistory` entry
- Finds camera code for this calibration
- Asserts if data for this calibration exists on disk
- Finds `config_parameters` for this user
- Creates file `configuration_userId.ini` and updates `CalibrationExecutionHistory` entry with user config_parameters
- Executes IDL procedure `calibration.pro` and updates `CalibrationExecutionHistory` entry with new information (*stdout*, *stderr*)
- Determines if execution was successful by testing presence of new files in `astrometry/camera/date` directory
- Deletes `configuration_userId.ini`
- Back in bulkProcess it counts the number of failures and success
