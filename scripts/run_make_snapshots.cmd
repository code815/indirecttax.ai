@echo off
setlocal
cd /d %~dp0..
call .venv\Scripts\activate
python -m jobs.make_snapshots
