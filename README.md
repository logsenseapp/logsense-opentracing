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

##Parameters

| Name         | Required  | Runtime property                | Environment variable               | Default value        | 
|-----------------|---|--------------------------------------|------------------------------------|----------------------|
| LogSense token  | Y | `-Dlogsense.token=aa-1213-bb...`     | `LOGSENSE_TOKEN=aa-1213-bb...`     |                      | 
| LogSense host   | N | `-Dlogsense.host=logs.logsense.com`  | `LOGSENSE_HOST=logs.logsense.com`  | `logs.logsense.com`  |
| LogSense port   | N | `-Dlogsense.port=32714`              | `LOGSENSE_PORT=32714`              |  `32714`             |
| Service name    | N | `-Dlogsense.service.name=foo`        | `LOGSENSE_SERVICE_NAME=foo`        |  ``                  |