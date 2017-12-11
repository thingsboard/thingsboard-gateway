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

public class HttpExtensionIntegrationTest {

    private static final String GATEWAY_URL = "http://localhost:9090/sigfox/%s/";

    public static void main(String[] args) throws IOException {
        doPost("YOUR_DEVICE_TYPE_ID", "TOKEN");
    }

    private static void doPost(String deviceTypeId, String token) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        String postJson = new String(Files.readAllBytes(Paths.get(String.format("src/test/resources/%s.json", deviceTypeId))));
        new RestTemplate().exchange(String.format(GATEWAY_URL, deviceTypeId), HttpMethod.POST, new HttpEntity<>(postJson, headers), String.class);
    }
}
