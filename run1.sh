java -Xmx200G   -Xss1024m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=7896  -classpath ".:./bin:./lib/gson-2.6.2.jar:./lib/commons-pool2-2.4.2.jar:./lib/jedis-2.9.0.jar:./lib/spring-core-4.3.7.RELEASE.jar:./lib/spring-data-redis-1.8.1.RELEASE.jar" ned.main.AppMain2  


