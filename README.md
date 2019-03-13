# logsense-opentracing

The project provides a Tracer implementation that sends-out spans to LogSense

The token must be provided in config either via:
* Environment variable `LOGSENSE_TOKEN`, e.g. 
```
$ LOGSENSE_TOKEN=aaa-123-bbb-999 java -javaagent:....
```
* Runtime propety `logsense.token`, e.g.:
 ```
 $ java -Dlogsense.token=aaa-123-bbb-999 -javaagent:....
 ```
* Via a file specified as a runtime property, e.g.:
```
$ cat logsense.properties
logsense.token=aaa-123-bbb-999
$ java -Dlogsense.config=logsense.properties -javaagent:...
```