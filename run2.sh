@echo off


# run me without 'sudo'

DATAFILE_PATH=../data/demo
DATAFILE_PATH_AWS_BASE=s3://magnet-fwm/home/LiveU/joint_scenario
DATAFILE_PATH_AWS_INPUT=$DATAFILE_PATH_AWS_BASE/Feldman_Group/
FILE_NAME=tweets_feldman_loc.txt
DATAFILE_PATH_AWS_INPUT=$DATAFILE_PATH_AWS_BASE/Feldman_Group/$FILE_NAME
DATAFILE_PATH_AWS_OUTPUT=$DATAFILE_PATH_AWS_BASE/Bodof_Group/

DATAFILE_LOCAL=$DATAFILE_PATH

sudo rm -rf $DATAFILE_LOCAL


echo Downloading folder $DATAFILE_PATH_AWS_INPUT from AWS ...
mkdir $DATAFILE_LOCAL

echo aws s3 cp "$DATAFILE_PATH_AWS_INPUT" "$DATAFILE_LOCAL" --recursive --exclude "*" --include "$FILE_NAME"
sudo aws s3 cp "$DATAFILE_PATH_AWS_INPUT" "$DATAFILE_LOCAL" --recursive --exclude "*" --include "$FILE_NAME"

echo s3 cp "$DATAFILE_PATH_AWS_INPUT" "$DATAFILE_LOCAL/$FILE_NAME" 
sudo aws s3 cp "$DATAFILE_PATH_AWS_INPUT" "$DATAFILE_LOCAL/$FILE_NAME" 

echo "DATAFILE_LOCAL"

##sudo java -XX:+UseG1GC -Xms50000m -classpath ".;./bin;./lib/gson-2.6.2.jar;./lib/commons-pool2-2.4.2.jar;./lib/jedis-2.9.0.jar;./lib/spring-core-4.3.7.RELEASE.jar;./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain2

sudo java -Xmx120G  -Xss1024m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=7896  -classpath ".:./bin:./lib/gson-2.6.2.jar:./lib/commons-pool2-2.4.2.jar:./lib/jedis-2.9.0.jar:./lib/spring-core-4.3.7.RELEASE.jar:./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain2



echo aws s3 cp ../temp/threads_1000000_0/ $DATAFILE_PATH_AWS_OUTPUT --recursive --exclude "\"*\"" --include "\"*.csv\""
aws s3 cp ../temp/threads_1000000_0/ $DATAFILE_PATH_AWS_OUTPUT --recursive --exclude "*" --include "*.csv"

echo aws s3 cp ../temp/threads_1000000_0/ $DATAFILE_PATH_AWS_OUTPUT --recursive --exclude "\"*\"" --include "\"*.txt\""
aws s3 cp ../temp/threads_1000000_0/ $DATAFILE_PATH_AWS_OUTPUT --recursive --exclude "*" --include "*.txt"


