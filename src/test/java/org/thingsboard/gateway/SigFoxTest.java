package org.thingsboard.gateway;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SigfoxTest {

    public static void main(String[] args) throws IOException {

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "SECURITY_TOKEN");
        headers.setContentType(MediaType.APPLICATION_JSON);

        String postJson = new String(Files.readAllBytes(Paths.get("src/test/resources/post.json")));

        new RestTemplate().exchange("http://localhost:9090/sigfox/0430b556622f4b7aa3270059d7987b/", HttpMethod.POST, new HttpEntity<>(postJson, headers), String.class);
    }
}
