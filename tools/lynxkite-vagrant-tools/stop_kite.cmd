@echo off
setlocal
call :GetUnixTime UNIX_TIME
SET LOGFILE=logs/vagrant_log_stop_%UNIX_TIME%.txt
echo stop_kite.cmd log at time %UNIX_TIME% > %LOGFILE%
echo Stopping kite VM >> %LOGFILE%
echo Stopping kite VM

vagrant halt
IF ERRORLEVEL 1 (
color C
echo.
echo ---------------------------------------------------------------
echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
echo.
echo Something went wrong. 
echo Press any key to exit
echo Stop failed >> %LOGFILE%
) ELSE (
color A
echo Stop successful, your virtual machine is stopped. 
echo Stop successful >> %LOGFILE%
)
echo Press any key to close this window.
pause > nul

:GetUnixTime
setlocal enableextensions
for /f %%x in ('wmic path win32_utctime get /format:list ^| findstr "="') do (
    set %%x)
set /a z=(14-100%Month%%%100)/12, y=10000%Year%%%10000-z
set /a ut=y*365+y/4-y/100+y/400+(153*(100%Month%%%100+12*z-3)+2)/5+Day-719469
set /a ut=ut*86400+100%Hour%%%100*3600+100%Minute%%%100*60+100%Second%%%100
endlocal & set "%1=%ut%" & goto :EOF