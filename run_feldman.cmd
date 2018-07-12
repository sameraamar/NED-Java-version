@echo off

set DATAFILE_LOCAL=C:\data\Thesis\events_db\petrovic\tweets
set DATAFILE_PATH_AWS_BASE=s3://magnet-fwm/home/LiveU/joint_scenario
set DATAFILE_PATH_AWS_INPUT=%DATAFILE_PATH_AWS_BASE%/Feldman_Group/tweets_feldman_2000_loc_2706.txt
set DATAFILE_PATH_AWS_OUTPUT=%DATAFILE_PATH_AWS_BASE%/Bodof_Group/



set /p input="Download from AWS? <y / n>: "
if %input%==n  goto no


echo Looking for file: "%DATAFILE_LOCAL%"

echo Delete folder: "%DATAFILE_LOCAL%" ?
del /q "%DATAFILE_LOCAL%"\*.* 

echo aws s3 cp "%DATAFILE_PATH_AWS_INPUT%" "%DATAFILE_LOCAL%" --recursive --exclude "*" --include "*.*"
aws s3 cp "%DATAFILE_PATH_AWS_INPUT%" "%DATAFILE_LOCAL%" --recursive --exclude "*" --include "*.*"
echo aws s3 cp "%DATAFILE_PATH_AWS_INPUT%" "%DATAFILE_LOCAL%" 
aws s3 cp "%DATAFILE_PATH_AWS_INPUT%" "%DATAFILE_LOCAL%" 

:no

IF not EXIST "%DATAFILE_LOCAL%" (
	echo something went wrong downloading the file
	exit 1
)

echo "%DATAFILE_LOCAL%"


java1 -XX:+UseG1GC -Xms50000m -classpath ".;./bin;./lib/gson-2.6.2.jar;./lib/commons-pool2-2.4.2.jar;./lib/jedis-2.9.0.jar;./lib/spring-core-4.3.7.RELEASE.jar;./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.ReformatInputJson "%DATAFILE_LOCAL%" "%DATAFILE_LOCAL%.txt"
java -XX:+UseG1GC -Xms50000m -classpath ".;./bin;./lib/gson-2.6.2.jar;./lib/commons-pool2-2.4.2.jar;./lib/jedis-2.9.0.jar;./lib/spring-core-4.3.7.RELEASE.jar;./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain2


AWS s3 cp C:/temp/threads_1000000_0/ %DATAFILE_PATH_AWS_OUTPUT% --recursive --include "*0.csv"
AWS s3 cp C:/temp/threads_1000000_0/ %DATAFILE_PATH_AWS_OUTPUT% --recursive --include "*.txt"



