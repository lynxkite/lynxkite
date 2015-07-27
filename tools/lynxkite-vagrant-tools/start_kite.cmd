@echo off
vagrant up
IF ERRORLEVEL 1 (
color C
echo.
echo ---------------------------------------------------------------
echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
echo.
echo Something went wrong. Try the troubleshooting methods described in the instructions.
echo Press any key to exit
) ELSE (
color A
echo Startup successful. You should be able to access kite by going to localhost:9000
)
echo Press any key to close this window.
pause > nul