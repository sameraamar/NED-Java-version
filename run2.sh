@echo off

set DATAFILE_PATH=../data/demo
set DATAFILE_PATH_AWS_BASE=s3://magnet-fwm/home/LiveU/joint_scenario
set DATAFILE_PATH_AWS_INPUT=%DATAFILE_PATH_AWS_BASE%/raw_data/
set DATAFILE_PATH_AWS_OUTPUT=%DATAFILE_PATH_AWS_BASE%/Bodof_Group/

set DATAFILE_LOCAL=%DATAFILE_PATH%

echo Downloading folder %DATAFILE_PATH_AWS_INPUT% from AWS ...
mkdir %DATAFILE_LOCAL%
aws s3 cp "%DATAFILE_PATH_AWS_INPUT%" "%DATAFILE_LOCAL%" --recursive --include "*.json"

echo "%DATAFILE_LOCAL%

#java -XX:+UseG1GC -Xms50000m -classpath ".;./bin;./lib/gson-2.6.2.jar;./lib/commons-pool2-2.4.2.jar;./lib/jedis-2.9.0.jar;./lib/spring-core-4.3.7.RELEASE.jar;./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain2

java --Xmx120G  -Xss1024m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=7896  -classpath ".:./bin:./lib/gson-2.6.2.jar:./lib/commons-pool2-2.4.2.jar:./lib/jedis-2.9.0.jar:./lib/spring-core-4.3.7.RELEASE.jar:./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain2



AWS s3 cp ../temp/threads_1000000_0/ %DATAFILE_PATH_AWS_OUTPUT% --recursive --include "*.csv"
AWS s3 cp C:/temp/threads_1000000_0/ %DATAFILE_PATH_AWS_OUTPUT% --recursive --include "*.txt"


