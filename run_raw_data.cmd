@echo off

set DATAFILE_PATH=C:\data\Thesis\events_db\petrovic\
set DATAFILE_PATH_AWS_BASE=s3://magnet-fwm/home/LiveU/joint_scenario
set DATAFILE_PATH_AWS_INPUT=%DATAFILE_PATH_AWS_BASE%/raw_data/
set DATAFILE_PATH_AWS_OUTPUT=%DATAFILE_PATH_AWS_BASE%/Bodof_Group/

set DATAFILE_LOCAL=%DATAFILE_PATH%\tweets

del -Recurse -Confirm "%DATAFILE_LOCAL%"

IF EXIST "%DATAFILE_LOCAL%" (
	echo exists	
) else (
	echo Downloading folder %DATAFILE_PATH_AWS_INPUT% from AWS ...
	mkdir %DATAFILE_LOCAL%
	aws s3 cp "%DATAFILE_PATH_AWS_INPUT%" "%DATAFILE_LOCAL%" --recursive --include "*.json"
)

IF not EXIST "%DATAFILE_LOCAL%" (
	echo something went wrong downloading the file
	exit 1
)

echo "%DATAFILE_LOCAL%

java -XX:+UseG1GC -Xms50000m -classpath ".;./bin;./lib/gson-2.6.2.jar;./lib/commons-pool2-2.4.2.jar;./lib/jedis-2.9.0.jar;./lib/spring-core-4.3.7.RELEASE.jar;./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain2


AWS s3 cp C:/temp/threads_1000000_0/ %DATAFILE_PATH_AWS_OUTPUT% --recursive --include "*.csv"
AWS s3 cp C:/temp/threads_1000000_0/ %DATAFILE_PATH_AWS_OUTPUT% --recursive --include "*.txt"
