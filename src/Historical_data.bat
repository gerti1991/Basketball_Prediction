@echo off

REM Go up one directory from the current script's location
cd %~dp0..
SET "current_dir=%cd%"

REM Activate the virtual environment
CALL "%current_dir%\env\Scripts\activate"

REM Run the Python script and optionally capture output
python "%current_dir%\src\Update_Historical_Data.py"

REM Deactivate the virtual environment
deactivate

REM Pause the script to see any messages before it closes (optional)
pause