@echo off
setlocal
REM Go to repo root (two levels up from this script if scripts is directly in repo root)
cd /d %~dp0..

REM Activate virtual environment
call .venv\Scripts\activate
call scripts\env_template.cmd .env


REM Create logs folder if it doesn't exist
set LOGDIR=logs
if not exist %LOGDIR% mkdir %LOGDIR%

REM Build timestamped log filename
set LOGFILE=%LOGDIR%\discover_hubs_%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%_%TIME:~0,2%%TIME:~3,2%.log
set LOGFILE=%LOGFILE: =0%

REM Run the job
python -m jobs.discover_hubs >> %LOGFILE% 2>&1

REM Pass exit code back to Task Scheduler
set EXITCODE=%ERRORLEVEL%
exit /b %EXITCODE%
