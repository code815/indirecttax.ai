@echo off
setlocal
cd /d %~dp0..

call .venv\Scripts\activate
call scripts\env_template.cmd .env


set LOGDIR=logs
if not exist %LOGDIR% mkdir %LOGDIR%

set LOGFILE=%LOGDIR%\daily_crawl_%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%_%TIME:~0,2%%TIME:~3,2%.log
set LOGFILE=%LOGFILE: =0%

python -m jobs.daily_crawl >> %LOGFILE% 2>&1

set EXITCODE=%ERRORLEVEL%
exit /b %EXITCODE%
