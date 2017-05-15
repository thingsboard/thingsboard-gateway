/**
 * Copyright © 2017 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    private static final String SECURITY_TOKEN = "Basic U0lHRk9YX1RFU1RfVE9LRU4=";
    private static final String GATEWAY_URL = "http://localhost:9090/sigfox/%s/";

    public static void main(String[] args) throws IOException {

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", SECURITY_TOKEN);
        headers.setContentType(MediaType.APPLICATION_JSON);

        doPost(headers, "58ac4b889058c24616a43b3b");
    }

    private static void doPost(HttpHeaders headers, String deviceTypeId) throws IOException {
        String postJson = new String(Files.readAllBytes(Paths.get(String.format("src/test/resources/%s.json", deviceTypeId))));

        new RestTemplate().exchange(String.format(GATEWAY_URL, deviceTypeId), HttpMethod.POST, new HttpEntity<>(postJson, headers), String.class);
    }
}
