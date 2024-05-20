@echo off

REM
cd /d "C:\Users\tiran\Basketball_Predictions"
REM
call .\env\Scripts\activate
REM
uvicorn src.FastApi.API:app --port 8000

pause