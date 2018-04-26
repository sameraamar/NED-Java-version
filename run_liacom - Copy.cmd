@echo off

set AWS_SOURCE_NAME=liacom_tweets_1.41m.txt
set DATAFILE_PATH=C:\data\Thesis\events_db\petrovic\
set DATAFILE_PATH_AWS_BASE=s3://magnet-fwm/home/LiveU/joint_scenario
set DATAFILE_PATH_AWS_INPUT=%DATAFILE_PATH_AWS_BASE%/Liacom/
set DATAFILE_PATH_AWS_OUTPUT=%DATAFILE_PATH_AWS_BASE%/Bodof_Group/

set DATAFILE_LOCAL=%DATAFILE_PATH%demo.txt
set DATAFILE_AWS=%DATAFILE_PATH_AWS_INPUT%%AWS_SOURCE_NAME%


echo Looking for file: "%DATAFILE_LOCAL%"

del "%DATAFILE_LOCAL%"

IF EXIST "%DATAFILE_LOCAL%" (
	echo exists
	
) else (
	echo Downloading file %AWS_SOURCE_NAME% from AWS ...
	aws s3 cp "%DATAFILE_AWS%" "%DATAFILE_LOCAL%"
)

IF not EXIST "%DATAFILE_LOCAL%" (
	echo something went wrong downloading the file
	exit 1
)

echo "%DATAFILE_LOCAL%.txt"

java -XX:+UseG1GC -Xms50000m -classpath ".;./bin;./lib/gson-2.6.2.jar;./lib/commons-pool2-2.4.2.jar;./lib/jedis-2.9.0.jar;./lib/spring-core-4.3.7.RELEASE.jar;./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.ReformatInputJson "%DATAFILE_LOCAL%" "%DATAFILE_LOCAL%.txt"
java -XX:+UseG1GC -Xms50000m -classpath ".;./bin;./lib/gson-2.6.2.jar;./lib/commons-pool2-2.4.2.jar;./lib/jedis-2.9.0.jar;./lib/spring-core-4.3.7.RELEASE.jar;./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain2


AWS s3 cp C:/temp/threads_1000000_0/ %DATAFILE_PATH_AWS_OUTPUT% --recursive --include "*.csv"
AWS s3 cp C:/temp/threads_1000000_0/ %DATAFILE_PATH_AWS_OUTPUT% --recursive --include "*.txt"
