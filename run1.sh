java -Xmx6000m  -Xss64m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=7896  -classpath ".:./bin:./lib/gson-2.6.2.jar:./lib/commons-pool2-2.4.2.jar:./lib/jedis-2.9.0.jar:./lib/spring-core-4.3.7.RELEASE.jar:./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain -ifolder /Users/sameraamar/data -ofolder ~/temp -threads threads.txt -conf /Users/sameraamar/Thesis/NED-Java-version/conf.json


