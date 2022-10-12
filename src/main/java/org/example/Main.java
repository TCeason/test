package org.example;

import cn.hutool.core.text.csv.CsvUtil;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
How to use:

 1. create user in Databend

    create user 'u1'@'%' identified by 'abc123';

    grant all on *.* to 'u1'@'%' ;

 2. create table in Databend

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
**/

public class Main {

    // nums of consumer thread
    private static final int CONCURRENCY = 2;
    private static final int QUEUE_CAPACITY = 10;
    private static  final byte[] DATA_END = new byte[0];
    // one queue mock rows nums
    private static int MOCK_ROWS = 1000;

    public static void main(String[] args) throws Exception {

        List<LinkedBlockingQueue<byte[]>> queues = new ArrayList<>();
        for (int i = 0; i < CONCURRENCY; i++) {
            queues.add(new LinkedBlockingQueue<>(QUEUE_CAPACITY));
        }

        // Start a producer thread
        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                byte[] data = mockData();
                for (LinkedBlockingQueue<byte[]> queue : queues) {
                    try {
                        queue.put(data);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            for (LinkedBlockingQueue<byte[]> queue : queues) {
                try {
                    queue.put(DATA_END);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Long start1 = System.currentTimeMillis();

        // Start CONCURRENCY nums consumer thread
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);

        for (int i = 0; i < CONCURRENCY; i++) {
            final LinkedBlockingQueue<byte[]> queue = queues.get(i);
            pool.execute(() -> {
                Long one_start = System.currentTimeMillis();
                StreamingLoad loader = new StreamingLoad();
                while (true) {
                    byte[] data;
                    try {
                        data = queue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        continue;
                    }
                    if (data == DATA_END) {
                        break;
                    }
                    try {
                        Long start = System.currentTimeMillis();
                        loader.sendData(data);
                        Long end = System.currentTimeMillis();
                        System.out.printf("one batch cost %d ms.\n", end-start);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                System.out.printf("one thread cost %d ms.\n", System.currentTimeMillis()-one_start);
            });
        }
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.MINUTES);
        Long end = System.currentTimeMillis();
        System.out.printf("Total cost %d ms.\n", end - start1);
    }

    private static byte[] mockData() {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i <= MOCK_ROWS; i++) {
            Object[] row = new Object[] {
                    "240000","b7662279-890f-4b97-aa8a-381eedb792fb","172.21.243.229","gateway","POST",RandomStringUtils.randomAlphanumeric(45),null,null,"GMT+7",
                    "en",null,"200",null,RandomStringUtils.randomAlphanumeric(30),"iOS 15.4","en","web","667,375",
                    RandomStringUtils.randomAlphanumeric(251),
                    RandomStringUtils.randomAlphanumeric(4032),
                    "0.0","0.0",
                    null, RandomStringUtils.randomAlphanumeric(12), RandomStringUtils.randomAlphanumeric(10)
            };
            rows.add(row);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Writer writer = new BufferedWriter(new OutputStreamWriter(output));
        CsvUtil.getWriter(writer).write(rows);
        return output.toByteArray();
    }
}
