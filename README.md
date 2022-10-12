# Dependencies

JDK-17

## Deployment

```sql
create user 'u1'@'%' identified by 'abc123';

grant all on *.* to 'u1'@'%' ;

create table default.test_table(
    userId VARCHAR NULL,
    fvideoId VARCHAR NULL,
    userIp VARCHAR NULL,
    requestChannel VARCHAR NULL,
    requestMethod VARCHAR NULL,
    requestUrl VARCHAR NULL,
    requestTime BIGINT UNSIGNED NULL,
    responseTime BIGINT UNSIGNED NULL,
    timeZone VARCHAR NULL,
    systemLangShort VARCHAR NULL,
    sessionId VARCHAR NULL,
    responseStatus VARCHAR NULL,
    deviceId VARCHAR NULL,
    deviceName VARCHAR NULL,
    systemVersion VARCHAR NULL,
    systemLang VARCHAR NULL,
    clientType VARCHAR NULL,
    screenResolution VARCHAR NULL,
    longitude VARCHAR NULL,
    latitude VARCHAR NULL,
    responseHeader VARCHAR NULL,
    requestHeader VARCHAR NULL,
    requestBody VARCHAR NULL,
    responseBody VARCHAR NULL,
    partitionDate VARCHAR NULL
);

```

## Usage

```shell
mvn compile
mvn exec:java -Dexec.mainClass="org.example.Main"
```


## Note

**mock total rows = CONCURRENCY * MOCK_ROWS * QUEUE_CAPACITY**

stream load addr: private static final String ADDRESS = "127.0.0.1:8000";

one batch mock rows: private static int MOCK_ROWS = 1000;

nums of consumer thread: private static final int CONCURRENCY = 2;
