@echo off
REM scripts\env_template.cmd
REM Usage:
REM   call scripts\env_template.cmd
REM   call scripts\env_template.cmd C:\path\to\.env

setlocal EnableExtensions EnableDelayedExpansion

REM Resolve repo root (assumes this file lives in scripts\)
cd /d %~dp0..
set "ENV_FILE=%~1"
if "%ENV_FILE%"=="" set "ENV_FILE=.env"

if not exist "%ENV_FILE%" (
  echo [env] No .env found at "%ENV_FILE%". Skipping.
  goto :eof
)

echo [env] Loading variables from "%ENV_FILE%"

for /f "usebackq tokens=* delims=" %%L in ("%ENV_FILE%") do (
  set "line=%%L"
  REM Trim BOM from first line if present
  if "!line:~0,1!"=="ÿ" set "line=!line:~3!"
  REM Skip blank lines and comments
  if not "!line!"=="" if not "!line:~0,1!"=="#" (
    REM Split on first '='
    for /f "tokens=1* delims==" %%K in ("!line!") do (
      set "key=%%K"
      set "val=%%L"
      REM Strip surrounding quotes if present
      if "!val:~0,1!"=="\"" if "!val:~-1!"=="\"" set "val=!val:~1,-1!"
      if "!val:~0,1!"=="'" if "!val:~-1!"=="'" set "val=!val:~1,-1!"
      REM Export to current process
      set "!key!=!val!"
      REM Optional: echo loaded key (comment out if noisy)
      REM echo   set !key!=*** (hidden)
    )
  )
)

endlocal & goto :eof
