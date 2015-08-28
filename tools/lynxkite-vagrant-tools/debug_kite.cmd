@echo off
setlocal

vagrant halt
copy Vagrantfile Vagrantfile.kite > nul

call :GetUnixTime UNIX_TIME


IF NOT EXIST logs mkdir logs
SET LOGFILE=logs/vagrant_log_debug_%UNIX_TIME%.txt
echo debug_kite.cmd log at time %UNIX_TIME% > %LOGFILE%


( echo # -*- mode: ruby -*-) > Vagrantfile
( echo # vi: set ft=ruby :) >> Vagrantfile
( echo Vagrant^.configure(2^) do ^|config^|) >> Vagrantfile
findstr "config.vm.box" Vagrantfile.kite >> Vagrantfile
( echo   config.vm.network "forwarded_port", guest: 9000, host: 9000) >> Vagrantfile
( echo   config.vm.network "forwarded_port", guest: 4040, host: 4040) >> Vagrantfile
( echo   config.vm.synced_folder "uploads", "/home/vagrant/kite_data/uploads", create: true) >> Vagrantfile
( echo   config.vm.provider "virtualbox" do ^|vb^|) >> Vagrantfile
findstr "vb.name" Vagrantfile.kite >> Vagrantfile
findstr "vb.memory" Vagrantfile.kite >> Vagrantfile
findstr "vb.cpus" Vagrantfile.kite >> Vagrantfile
( echo   end) >> Vagrantfile
( echo $script = ^<^<SCRIPT) >> Vagrantfile
( echo    echo "df:" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    df -h  2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "-------------" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "du:" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    du -h -d 1 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "-------------" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "du kite_data:" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    du -h -d 1 kite_data  2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "-------------" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "ls:" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    ls -l /home/vagrant  2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "-------------" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "initctl status:" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    initctl status lynxkite  2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "-------------" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "initctl restart:" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    initctl restart lynxkite  2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "-------------" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    echo "initctl status:" 2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    initctl status lynxkite  2^>^&1 ^| tee -a /vagrant/logs/vagrant_log_debug_linux_%UNIX_TIME%.txt) >> Vagrantfile
( echo    tar czf /vagrant/logs/vagrant_log_debug_kitelogs_%UNIX_TIME%.tgz $(ls -d $(ls -d /home/vagrant/lynxkite*^)/kite*^)/logs) >> Vagrantfile
( echo SCRIPT) >> Vagrantfile
( echo   config.vm.provision "shell", inline: $script) >> Vagrantfile
( echo end) >> Vagrantfile




vagrant up --provision
	IF ERRORLEVEL 1 (
		color C
		echo.
		echo ---------------------------------------------------------------
		echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		echo.
		echo Something went wrong. Cannot run debugging.
		echo Press any key to exit
		echo Debug failed >> %LOGFILE%
        copy Vagrantfile.kite Vagrantfile > nul
        del /F Vagrantfile.kite > nul
		pause > nul
		exit /b 1
	) ELSE (
		color A
		echo Debugging Finished.
		echo Check the log folder for log files starting with vagrant_log_debug
		echo Press any key to exit
		echo Debug successful >> %LOGFILE%
        copy Vagrantfile.kite Vagrantfile > nul
        del /F Vagrantfile.kite > nul
		pause > nul
		exit /b 0
	)
)

:GetUnixTime
setlocal enableextensions
for /f %%x in ('wmic path win32_utctime get /format:list ^| findstr "="') do (
    set %%x)
set /a z=(14-100%Month%%%100)/12, y=10000%Year%%%10000-z
set /a ut=y*365+y/4-y/100+y/400+(153*(100%Month%%%100+12*z-3)+2)/5+Day-719469
set /a ut=ut*86400+100%Hour%%%100*3600+100%Minute%%%100*60+100%Second%%%100
endlocal & set "%1=%ut%" & goto :EOF