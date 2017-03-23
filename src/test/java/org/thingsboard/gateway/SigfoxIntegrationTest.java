package org.thingsboard.gateway;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SigfoxIntegrationTest {
    
    private static final String DEVICE_TYPE_ID = "DEVICE_TYPE_ID";
    private static final String SECURITY_TOKEN = "SECUTIRY_TOKEN";
    private static final String GATEWAY_URL = "http://localhost:9090/sigfox/" + DEVICE_TYPE_ID + "/";

    public static void main(String[] args) throws IOException {

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", SECURITY_TOKEN);
        headers.setContentType(MediaType.APPLICATION_JSON);

        String postJson = new String(Files.readAllBytes(Paths.get("src/test/resources/post.json")));

        new RestTemplate().exchange(GATEWAY_URL, HttpMethod.POST, new HttpEntity<>(postJson, headers), String.class);
    }
}
