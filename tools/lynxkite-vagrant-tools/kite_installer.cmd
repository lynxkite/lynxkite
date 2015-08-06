@echo off

IF [%1] == [install] (
	SET MODE=install
) ELSE IF [%1] == [upgrade] (
	SET MODE=upgrade
) ELSE (
echo Usage: kite_installer mode version size
exit /b 1
)
SET KITE_VERSION=%2
SET KITE_SIZE=%3
SET BOX_BITS=64
IF [%2] == [] SET KITE_VERSION=stable
IF [%3] == [] SET KITE_SIZE=small
IF [%4] == [-32] SET BOX_BITS=32
IF [%KITE_SIZE%] == [tiny] (
SET VM_MEMORY=512
SET VM_CPUS=1
)
IF [%KITE_SIZE%] == [small] (
SET VM_MEMORY=1256
SET VM_CPUS=1
)
IF [%KITE_SIZE%] == [medium] (
SET VM_MEMORY=3328
SET VM_CPUS=2
)
IF [%KITE_SIZE%] == [large] (
SET VM_MEMORY=6400
SET VM_CPUS=3
)
IF [%MODE%] == [install] (
echo Installing kite version %KITE_VERSION%, size %KITE_SIZE%
) ELSE IF [%MODE%] == [update] (
echo Updating to kite version %KITE_VERSION%, size %KITE_SIZE%
)
echo Machine settings: %VM_CPUS% CPUs with %VM_MEMORY%Mb RAM

( echo # -*- mode: ruby -*-) > Vagrantfile
( echo # vi: set ft=ruby :) >> Vagrantfile
( echo Vagrant^.configure(2^) do ^|config^|) >> Vagrantfile
( echo   config.vm.box = "ubuntu/trusty%BOX_BITS%") >> Vagrantfile
( echo   config.vm.network "forwarded_port", guest: 9000, host: 9000) >> Vagrantfile
( echo   config.vm.network "forwarded_port", guest: 4040, host: 4040) >> Vagrantfile
( echo   config.vm.synced_folder "uploads", "/home/vagrant/kite_data/uploads", create: true) >> Vagrantfile
( echo   config.vm.provider "virtualbox" do ^|vb^|) >> Vagrantfile
( echo      vb.name = "lynxkite-%KITE_VERSION%-%KITE_SIZE%-%BOX_BITS%bit") >> Vagrantfile
( echo      vb.memory = "%VM_MEMORY%") >> Vagrantfile
( echo      vb.cpus = "%VM_CPUS%") >> Vagrantfile
( echo   end) >> Vagrantfile
( echo $script = ^<^<SCRIPT) >> Vagrantfile
( echo    chown -R vagrant /home/vagrant/kite_data) >> Vagrantfile
( echo    cp /vagrant/install_scripts.tgz ~vagrant) >> Vagrantfile
( echo    cd ~vagrant) >> Vagrantfile
( echo    tar xzf install_scripts.tgz) >> Vagrantfile
( echo    sed -i '/URL/ s/wget/wget --progress=bar:force/' ~vagrant/install_scripts/*.sh) >> Vagrantfile
( echo    sudo  ~vagrant/install_scripts/single-machine-install.sh vagrant %KITE_VERSION% %KITE_SIZE% --vagrant) >> Vagrantfile
( echo    rm ~vagrant/install_scripts.tgz) >> Vagrantfile
( echo    rm -R ~vagrant/install_scripts) >> Vagrantfile
( echo SCRIPT) >> Vagrantfile
( echo   config.vm.provision "shell", inline: $script) >> Vagrantfile
( echo end) >> Vagrantfile

color E

IF [%MODE%] == [install] (
	vagrant up
	IF ERRORLEVEL 1 (
		color C
		echo.
		echo ---------------------------------------------------------------
		echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		echo.
		echo Something went wrong. Try the troubleshooting methods described in the instructions.
		echo.
		echo If you see a lot of unsuccessful attempts to connect to the virtual machine,
		echo there is a good chance your computer does not support a 64-bit virtual machine.
		echo To install a 32-bit machine, first run destroy_kite.cmd then run the 32bit install script.
		echo.
		echo Press any key to exit
		pause > nul
		exit /b 1
	) ELSE (
		color A
		echo Install successful. You should be able to access kite by going to localhost:9000
		echo Press any key to exit
		pause > nul
		exit /b 0
	)
) ELSE IF [%MODE%] == [upgrade] (
	vagrant halt
	vagrant up --provision
	IF ERRORLEVEL 1 (
		color C
		echo.
		echo ---------------------------------------------------------------
		echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		echo.
		echo Something went wrong. Try the troubleshooting methods described in the instructions.
		echo Press any key to exit
		pause > nul
		exit /b 1
	) ELSE (
		color A
		echo Update successful. You should be able to access kite by going to localhost:9000
		echo If there is problem try to run the upgrade script again.
		echo Press any key to exit
		pause > nul
		exit /b 0
	)
)

