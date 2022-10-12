package org.example;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StreamingLoad {

    // stream load addr
    private static final String ADDRESS = "127.0.0.1:8000";
    private static final String USER = "u1";
    private static final String PASSWORD = "abc123";

    private final String url;
    private final String auth;
    private final CloseableHttpClient client;

    public StreamingLoad() {
        this.url = String.format("http://%s/v1/streaming_load", ADDRESS);
        this.auth = basicAuthHeader();
        this.client = HttpClients.custom().build();
    }

    private String basicAuthHeader() {
        final String tobeEncode = USER + ":" + PASSWORD;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public void sendData(byte[] data) throws Exception {
        HttpPut put = new HttpPut(url);
        put.setHeader(HttpHeaders.AUTHORIZATION, auth);
        put.setHeader("insert_sql", "insert into default.test_table format CSV");

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addBinaryBody("upfile", data, ContentType.DEFAULT_BINARY, "test_file");

        HttpEntity entity = builder.build();
        put.setEntity(entity);

        try (CloseableHttpResponse response = client.execute(put)) {
            String loadResult = "";
            if (response.getEntity() != null) {
                loadResult = EntityUtils.toString(response.getEntity());
            }
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                throw new IOException(
                        String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
            }
            System.out.println(loadResult);
        }
    }
}
