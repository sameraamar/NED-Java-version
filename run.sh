

if [[ -f ./out.log ]] ; then
    echo "output file exists. Maybe some other process is running already"
    exit
fi

rm err.log

java -classpath ".:./bin:./lib/gson-2.6.2.jar:./lib/commons-pool2-2.4.2.jar:./lib/jedis-2.9.0.jar:./lib/spring-core-4.3.7.RELEASE.jar:./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain -max_doc 2000000 -ifolder  /Users/sameraamar/data -ofolder ~/temp -threads threads11.txt >out.log 2>err.log 
